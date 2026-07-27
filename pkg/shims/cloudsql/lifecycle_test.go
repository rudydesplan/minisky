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
	adminErr    error
	adminErrors map[string]error
	deleteCalls int
	created     bool
	deleted     []string
	admin       []string
}

type failingCloudSQLStore struct {
	mu       sync.Mutex
	saves    int
	failFrom int
}

type failingOperationStore struct{}

type flakyCloudSQLStore struct {
	mu     sync.Mutex
	saves  int
	failOn map[int]bool
	data   []byte
}

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

func (s *flakyCloudSQLStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *flakyCloudSQLStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.failOn[s.saves] {
		return errors.New("injected transient save failure")
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	return err
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

func (b *fakeCloudSQLBackend) ExecuteAdmin(
	_ context.Context,
	project, instance, version, action, name, password string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.admin = append(b.admin, strings.Join([]string{project, instance, version, action, name, password}, ":"))
	if b.adminErrors[action] != nil {
		return b.adminErrors[action]
	}
	return b.adminErr
}

func (b *fakeCloudSQLBackend) deletes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deleteCalls
}

func TestCreateInstanceReturnsProviderDefaultSettings(t *testing.T) {
	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeCloudSQLBackend{}, nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db","databaseVersion":"POSTGRES_18","settings":{"tier":"db-f1-micro"}}`))
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, operation.Name)

	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/instances/db", nil))
	var instance DatabaseInstance
	if err := json.Unmarshal(rec.Body.Bytes(), &instance); err != nil {
		t.Fatal(err)
	}
	if instance.Settings.AvailabilityType != "ZONAL" || instance.Settings.PricingPlan != "PER_USE" {
		t.Fatalf("provider defaults = %#v", instance.Settings)
	}
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

func TestDeletePrecommitSaveFailureDoesNotTouchBackend(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{1: true}}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	key := instanceKey("demo", "db")
	api.instances[key] = &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if backend.deletes() != 0 {
		t.Fatalf("backend delete calls=%d", backend.deletes())
	}
	if api.instances[key].State != "RUNNABLE" {
		t.Fatalf("precommit state=%q", api.instances[key].State)
	}
}

func TestInstanceDeleteSerializesWithAdminMutations(t *testing.T) {
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)
	key := instanceKey("demo", "db")
	api.instances[key] = &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"}
	api.adminMu.Lock()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
		done <- response
	}()
	select {
	case <-done:
		t.Fatal("instance deletion bypassed the admin mutation lock")
	case <-time.After(20 * time.Millisecond):
	}
	if backend.deletes() != 0 {
		t.Fatal("backend deletion started while an admin mutation held the lock")
	}
	api.adminMu.Unlock()
	response := <-done
	var operation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, operation.Name)
}

func TestPostDeleteSaveFailurePersistsReconciliationTombstoneAcrossRestart(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{3: true}}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	key := instanceKey("demo", "db")
	api.instances[key] = &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	var initial SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)

	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	instance := restarted.instances[key]
	if instance == nil || instance.State != "ERROR" ||
		!strings.Contains(instance.BackendStatus, "backend deleted") {
		t.Fatalf("restarted tombstone=%#v", instance)
	}
}

func TestCreateCompensationFailurePersistsDivergenceAcrossRestart(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{2: true}}
	backend := &fakeCloudSQLBackend{created: true, deleteErr: errors.New("compensation failed")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)

	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	instance := restarted.instances[instanceKey("demo", "db")]
	if instance == nil || instance.State != "ERROR" ||
		!strings.Contains(instance.BackendStatus, "compensation failed") {
		t.Fatalf("restarted divergence=%#v", instance)
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

func TestDatabaseAndUserMutationsExecuteAgainstBackendBeforeMetadata(t *testing.T) {
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)
	key := instanceKey("demo", "db")
	api.instances[key] = &DatabaseInstance{
		Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_15", State: "RUNNABLE",
	}

	for _, request := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/projects/demo/instances/db/databases", `{"name":"app"}`},
		{http.MethodPost, "/v1/projects/demo/instances/db/users", `{"name":"app_user","password":"secret"}`},
		{http.MethodDelete, "/v1/projects/demo/instances/db/databases/app", ""},
		{http.MethodDelete, "/v1/projects/demo/instances/db/users?name=app_user", ""},
	} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(request.method, request.path, strings.NewReader(request.body)))
		if response.Code != http.StatusOK {
			t.Fatalf("%s %s status=%d body=%s", request.method, request.path, response.Code, response.Body.String())
		}
	}

	backend.mu.Lock()
	got := append([]string(nil), backend.admin...)
	backend.mu.Unlock()
	want := []string{
		"demo:db:POSTGRES_15:CREATE_DATABASE:app:",
		"demo:db:POSTGRES_15:CREATE_USER:app_user:secret",
		"demo:db:POSTGRES_15:DELETE_DATABASE:app:",
		"demo:db:POSTGRES_15:DELETE_USER:app_user:",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("backend admin calls = %#v, want %#v", got, want)
	}
}

func TestChildCreateCompensationFailurePersistsParentReconciliationAcrossRestart(t *testing.T) {
	tests := []struct {
		name               string
		path               string
		body               string
		compensationAction string
		assertChild        func(*testing.T, *API, string)
	}{
		{
			name: "database", path: "/v1/projects/demo/instances/db/databases",
			body: `{"name":"app"}`, compensationAction: "DELETE_DATABASE",
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.databases[key]; len(got) != 1 || got[0].Name != "app" {
					t.Fatalf("database reconciliation metadata=%#v", got)
				}
			},
		},
		{
			name: "user", path: "/v1/projects/demo/instances/db/users",
			body: `{"name":"app_user"}`, compensationAction: "DELETE_USER",
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.users[key]; len(got) != 1 || got[0].Name != "app_user" {
					t.Fatalf("user reconciliation metadata=%#v", got)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &flakyCloudSQLStore{failOn: map[int]bool{2: true}}
			backend := &fakeCloudSQLBackend{adminErrors: map[string]error{
				test.compensationAction: errors.New("compensation failed"),
			}}
			api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
			key := instanceKey("demo", "db")
			api.instances[key] = &DatabaseInstance{
				Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_15", State: "RUNNABLE",
			}
			if err := api.persistMetadata(); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body)))
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
			if err != nil {
				t.Fatal(err)
			}
			instance := restarted.instances[key]
			if instance == nil || instance.State != "ERROR" ||
				!strings.Contains(instance.BackendStatus, "compensation failed") {
				t.Fatalf("parent reconciliation=%#v", instance)
			}
			test.assertChild(t, restarted, key)
		})
	}
}

func TestChildPostDeleteSaveFailurePersistsParentReconciliationAcrossRestart(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		seed        func(*API, string)
		assertChild func(*testing.T, *API, string)
	}{
		{
			name: "database", path: "/v1/projects/demo/instances/db/databases/app",
			seed: func(api *API, key string) {
				api.databases[key] = []*Database{{Name: "app", Project: "demo", Instance: "db"}}
			},
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.databases[key]; len(got) != 1 || got[0].Name != "app" {
					t.Fatalf("database phantom evidence=%#v", got)
				}
			},
		},
		{
			name: "user", path: "/v1/projects/demo/instances/db/users?name=app_user",
			seed: func(api *API, key string) {
				api.users[key] = []*User{{Name: "app_user", Project: "demo", Instance: "db"}}
			},
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.users[key]; len(got) != 1 || got[0].Name != "app_user" {
					t.Fatalf("user phantom evidence=%#v", got)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &flakyCloudSQLStore{failOn: map[int]bool{2: true}}
			api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeCloudSQLBackend{}, store)
			key := instanceKey("demo", "db")
			api.instances[key] = &DatabaseInstance{
				Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_15", State: "RUNNABLE",
			}
			test.seed(api, key)
			if err := api.persistMetadata(); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, test.path, nil))
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
			if err != nil {
				t.Fatal(err)
			}
			instance := restarted.instances[key]
			if instance == nil || instance.State != "ERROR" ||
				!strings.Contains(instance.BackendStatus, "backend deleted") {
				t.Fatalf("parent reconciliation=%#v", instance)
			}
			test.assertChild(t, restarted, key)
		})
	}
}

func TestDatabaseMutationFailsClosedWhenBackendUnavailable(t *testing.T) {
	backend := &fakeCloudSQLBackend{adminErr: errors.New("database backend unavailable")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)
	api.instances[instanceKey("demo", "db")] = &DatabaseInstance{
		Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_15", State: "RUNNABLE",
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/instances/db/databases", strings.NewReader(`{"name":"app"}`)))
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(api.databases[instanceKey("demo", "db")]) != 0 {
		t.Fatal("backend failure created database metadata")
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
