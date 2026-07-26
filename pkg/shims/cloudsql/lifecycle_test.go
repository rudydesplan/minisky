package cloudsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

type fakeCloudSQLBackend struct {
	mu          sync.Mutex
	createErr   error
	deleteErr   error
	deleteCalls int
	created     bool
	deleted     []string
}

type failingCloudSQLStore struct {
	mu       sync.Mutex
	saves    int
	failFrom int
}

type failingOperationStore struct{}

func (failingOperationStore) Load(string, any) error { return state.ErrNotFound }
func (failingOperationStore) Save(string, any) error { return errors.New("operation save failed") }

func (*failingCloudSQLStore) Load(string, any) error { return state.ErrNotFound }
func (s *failingCloudSQLStore) Save(string, any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saves >= s.failFrom {
		return errors.New("save failed")
	}
	return nil
}

func (b *fakeCloudSQLBackend) Create(context.Context, string, string, string, string) (string, bool, error) {
	return "", b.created, b.createErr
}

func (b *fakeCloudSQLBackend) Delete(_ context.Context, project, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleteCalls++
	b.deleted = append(b.deleted, project+":"+name)
	return b.deleteErr
}

func (b *fakeCloudSQLBackend) deletes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deleteCalls
}

func TestCreateBackendFailureFailsOperationAndRollsBack(t *testing.T) {
	backend := &fakeCloudSQLBackend{createErr: errors.New("docker create failed"), created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db","databaseVersion":"POSTGRES_15"}`))
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
	}
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}

	op := waitCloudSQLOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Message != "docker create failed" {
		t.Fatalf("terminal operation = %#v", op)
	}
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/v1/projects/demo/operations/"+initial.Name, nil))
	var polled SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &polled); err != nil {
		t.Fatal(err)
	}
	if polled.Status != "DONE" || polled.Error == nil || polled.Error.Message != "docker create failed" {
		t.Fatalf("polled operation = %#v", polled)
	}
	api.mu.RLock()
	_, exists := api.instances[instanceKey("demo", "db")]
	api.mu.RUnlock()
	if exists {
		t.Fatal("failed create left instance metadata")
	}
	if backend.deletes() != 1 {
		t.Fatalf("backend cleanup calls = %d, want 1", backend.deletes())
	}
}

func TestDeleteBackendFailureFailsOperationAndRetainsMetadata(t *testing.T) {
	backend := &fakeCloudSQLBackend{deleteErr: errors.New("docker delete failed")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)
	key := instanceKey("demo", "db")
	api.instances[key] = &DatabaseInstance{
		Name: "db", Project: "demo", State: "RUNNABLE",
		SelfLink: "https://sqladmin.googleapis.com/v1/projects/demo/instances/db",
	}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body %s", rec.Code, rec.Body.String())
	}
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}

	op := waitCloudSQLOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Message != "docker delete failed" {
		t.Fatalf("terminal operation = %#v", op)
	}
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[key])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" || instance.BackendStatus != "docker delete failed" {
		t.Fatalf("instance after failed delete = %#v", instance)
	}
}

func TestCreateRollbackSaveFailureKeepsDegradedMetadata(t *testing.T) {
	store := &failingCloudSQLStore{failFrom: 2}
	backend := &fakeCloudSQLBackend{createErr: errors.New("create failed")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[instanceKey("demo", "db")])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" || instance.BackendStatus == "" {
		t.Fatalf("degraded instance = %#v", instance)
	}
}

func TestPostDeleteSaveFailureKeepsTombstone(t *testing.T) {
	store := &failingCloudSQLStore{failFrom: 2}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeCloudSQLBackend{}, store)
	key := instanceKey("demo", "db")
	api.instances[key] = &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[key])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" ||
		!strings.Contains(instance.BackendStatus, "backend deleted") {
		t.Fatalf("delete tombstone = %#v", instance)
	}
}

func TestMethodNotAllowedUsesJSONEnvelope(t *testing.T) {
	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeCloudSQLBackend{}, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch,
		"/v1/projects/demo/instances/db", nil))
	if rec.Code != http.StatusMethodNotAllowed ||
		!strings.Contains(rec.Body.String(), `"status":"METHOD_NOT_ALLOWED"`) {
		t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
	}
}

func TestOperationRegistrationFailureKeepsDegradedInstance(t *testing.T) {
	opMgr, err := orchestrator.NewOperationManagerWithStore(failingOperationStore{})
	if err != nil {
		t.Fatal(err)
	}
	api := newAPIWithBackend(opMgr, &fakeCloudSQLBackend{}, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[instanceKey("demo", "db")])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" {
		t.Fatalf("instance = %#v", instance)
	}
}

func TestFinalRunningSaveFailureCompensatesOwnedProjectBackend(t *testing.T) {
	store := &failingCloudSQLStore{failFrom: 2}
	backend := &fakeCloudSQLBackend{created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/project-a/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	backend.mu.Lock()
	deleted := append([]string(nil), backend.deleted...)
	backend.mu.Unlock()
	if len(deleted) != 1 || deleted[0] != "project-a:db" {
		t.Fatalf("backend compensation = %v", deleted)
	}
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[instanceKey("project-a", "db")])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" {
		t.Fatalf("degraded tombstone = %#v", instance)
	}
}

func TestCreateFailureWithoutOwnershipDoesNotCompensate(t *testing.T) {
	backend := &fakeCloudSQLBackend{createErr: errors.New("collision"), created: false}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/project-a/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	if backend.deletes() != 0 {
		t.Fatalf("unowned backend compensation calls = %d", backend.deletes())
	}
}

func waitCloudSQLOperation(t *testing.T, manager *orchestrator.OperationManager, name string) *orchestrator.Operation {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if op := manager.Get(name); op != nil && op.Done {
			return op
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("operation %q did not finish", name)
	return nil
}
