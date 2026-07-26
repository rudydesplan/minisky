package gke

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

type fakeGKEBackend struct {
	mu          sync.Mutex
	createErr   error
	deleteErr   error
	deleteCalls int
	created     bool
}

type failingGKEStore struct {
	mu       sync.Mutex
	saves    int
	failFrom int
}

type failingOperationStore struct{}

type staticGKEBackend struct{}

func (staticGKEBackend) Enabled() bool { return false }
func (staticGKEBackend) CreateClusterContext(context.Context, ClusterIdentity) (gkeBackendCreateResult, error) {
	return gkeBackendCreateResult{}, nil
}
func (staticGKEBackend) DeleteClusterContext(context.Context, ClusterIdentity) error {
	return errors.New("static backend must not receive Kind cleanup")
}

func (failingOperationStore) Load(string, any) error { return state.ErrNotFound }
func (failingOperationStore) Save(string, any) error { return errors.New("operation save failed") }

func (*failingGKEStore) Load(string, any) error { return state.ErrNotFound }
func (s *failingGKEStore) Save(string, any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saves >= s.failFrom {
		return errors.New("save failed")
	}
	return nil
}

func (*fakeGKEBackend) Enabled() bool { return true }

func (b *fakeGKEBackend) CreateClusterContext(context.Context, ClusterIdentity) (gkeBackendCreateResult, error) {
	return gkeBackendCreateResult{Created: b.created}, b.createErr
}

func (b *fakeGKEBackend) DeleteClusterContext(context.Context, ClusterIdentity) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleteCalls++
	return b.deleteErr
}

func (b *fakeGKEBackend) deletes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deleteCalls
}

func TestCreateBackendFailureFailsOperationAndRemovesCluster(t *testing.T) {
	backend := &fakeGKEBackend{createErr: errors.New("kind create failed"), created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, "", http.DefaultClient, nil)

	initial := createTestCluster(t, api, "cluster")
	op := waitGKEOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Message != "kind create failed" {
		t.Fatalf("terminal operation = %#v", op)
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/v1/projects/demo/zones/us-central1-c/operations/"+initial.Name, nil))
	var polled GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &polled); err != nil {
		t.Fatal(err)
	}
	if polled.Status != "DONE" || polled.Error == nil || polled.Error.Message != "kind create failed" {
		t.Fatalf("polled operation = %#v", polled)
	}
	api.mu.RLock()
	_, exists := api.clusters[clusterKey("demo", "us-central1-c", "cluster")]
	api.mu.RUnlock()
	if exists {
		t.Fatal("failed create left cluster metadata")
	}
	if backend.deletes() != 1 {
		t.Fatalf("backend cleanup calls = %d, want 1", backend.deletes())
	}
}

func TestDeleteBackendFailureFailsOperationAndRetainsCluster(t *testing.T) {
	backend := &fakeGKEBackend{deleteErr: errors.New("kind delete failed")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, "", http.DefaultClient, nil)
	key := clusterKey("demo", "us-central1-c", "cluster")
	api.clusters[key] = &Cluster{
		Name: "cluster", Status: "RUNNING", InitialNodeCount: 1,
		SelfLink: "https://container.googleapis.com/v1/projects/demo/zones/us-central1-c/clusters/cluster",
	}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/demo/zones/us-central1-c/clusters/cluster", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body %s", rec.Code, rec.Body.String())
	}
	var initial GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	op := waitGKEOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Message != "kind delete failed" {
		t.Fatalf("terminal operation = %#v", op)
	}
	api.mu.RLock()
	cluster := cloneCluster(api.clusters[key])
	api.mu.RUnlock()
	if cluster == nil || cluster.Status != "ERROR" || cluster.StatusMessage != "kind delete failed" {
		t.Fatalf("cluster after failed delete = %#v", cluster)
	}
}

func TestStaticClusterDeleteDoesNotRequireKindCleanup(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer gateway.Close()
	api := newAPIWithBackend(
		orchestrator.NewOperationManager(), staticGKEBackend{}, gateway.URL, gateway.Client(), nil)
	key := clusterKey("demo", "us-central1-c", "cluster")
	api.clusters[key] = &Cluster{
		Name: "cluster", Status: "RUNNING",
		SelfLink: "https://container.googleapis.com/v1/projects/demo/zones/us-central1-c/clusters/cluster",
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/demo/zones/us-central1-c/clusters/cluster", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("response=%d %s", rec.Code, rec.Body.String())
	}
	var initial GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	if op := waitGKEOperation(t, api.opMgr, initial.Name); op.Error != nil {
		t.Fatalf("operation=%#v", op.Error)
	}
	if api.clusters[key] != nil {
		t.Fatal("static cluster metadata remains after successful delete")
	}
}

func TestNodeRegistrationUsesConfiguredGateway(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer gateway.Close()

	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeGKEBackend{}, gateway.URL, gateway.Client(), nil)
	initial := createTestCluster(t, api, "cluster")
	op := waitGKEOperation(t, api.opMgr, initial.Name)
	if op.Error != nil {
		t.Fatalf("create operation failed: %#v", op.Error)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 4 {
		t.Fatalf("gateway requests = %v, want four managed nodes", paths)
	}
	for _, path := range paths {
		if len(path) < len("/compute/v1/") || path[:len("/compute/v1/")] != "/compute/v1/" {
			t.Fatalf("unexpected configured gateway path %q", path)
		}
	}
}

func TestGatewayConfigurationComesFromBootstrap(t *testing.T) {
	t.Setenv("MINISKY_GATEWAY_URL", "http://wrong.example")
	api := newAPI(orchestrator.NewOperationManager(), nil)
	api.configMu.RLock()
	defer api.configMu.RUnlock()
	if api.gatewayURL != "" || api.httpClient != nil {
		t.Fatalf("constructor derived early gateway config: %q %#v", api.gatewayURL, api.httpClient)
	}
}

func TestMethodNotAllowedUsesJSONEnvelope(t *testing.T) {
	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeGKEBackend{}, "", nil, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch,
		"/v1/projects/demo/zones/us-central1-c/clusters/cluster", nil))
	if rec.Code != http.StatusMethodNotAllowed ||
		!strings.Contains(rec.Body.String(), `"status":"METHOD_NOT_ALLOWED"`) {
		t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
	}
}

func TestCreateDoesNotDeletePreexistingKindCluster(t *testing.T) {
	backend := &fakeGKEBackend{createErr: errors.New("existing cluster"), created: false}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, "", nil, nil)
	initial := createTestCluster(t, api, "cluster")
	waitGKEOperation(t, api.opMgr, initial.Name)
	if backend.deletes() != 0 {
		t.Fatalf("delete calls = %d, want no compensation for unowned cluster", backend.deletes())
	}
}

func TestPartialNodeRegistrationIsCompensated(t *testing.T) {
	var mu sync.Mutex
	var posts, deletes []string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPost {
			posts = append(posts, r.URL.Path)
			if len(posts) == 3 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else if r.Method == http.MethodDelete {
			deletes = append(deletes, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer gateway.Close()
	backend := &fakeGKEBackend{created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, gateway.URL, gateway.Client(), nil)
	initial := createTestCluster(t, api, "cluster")
	op := waitGKEOperation(t, api.opMgr, initial.Name)
	if op.Error == nil {
		t.Fatal("partial registration unexpectedly succeeded")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deletes) != 2 {
		t.Fatalf("node cleanup requests = %v, want two", deletes)
	}
	if backend.deletes() != 1 {
		t.Fatalf("backend cleanup calls = %d, want one", backend.deletes())
	}
}

func TestPostDeleteSaveFailureKeepsClusterTombstone(t *testing.T) {
	store := &failingGKEStore{failFrom: 2}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer gateway.Close()
	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeGKEBackend{},
		gateway.URL, gateway.Client(), store)
	key := clusterKey("demo", "us-central1-c", "cluster")
	api.clusters[key] = &Cluster{Name: "cluster", Status: "RUNNING",
		SelfLink: "https://container.googleapis.com/v1/projects/demo/zones/us-central1-c/clusters/cluster"}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/demo/zones/us-central1-c/clusters/cluster", nil))
	var initial GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitGKEOperation(t, api.opMgr, initial.Name)
	api.mu.RLock()
	cluster := cloneCluster(api.clusters[key])
	api.mu.RUnlock()
	if cluster == nil || cluster.Status != "ERROR" {
		t.Fatalf("delete tombstone = %#v", cluster)
	}
}

func TestOperationRegistrationFailureKeepsDegradedCluster(t *testing.T) {
	opMgr, err := orchestrator.NewOperationManagerWithStore(failingOperationStore{})
	if err != nil {
		t.Fatal(err)
	}
	api := newAPIWithBackend(opMgr, &fakeGKEBackend{}, "", nil, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/zones/us-central1-c/clusters",
		bytes.NewBufferString(`{"cluster":{"name":"cluster"}}`)))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	api.mu.RLock()
	cluster := cloneCluster(api.clusters[clusterKey("demo", "us-central1-c", "cluster")])
	api.mu.RUnlock()
	if cluster == nil || cluster.Status != "ERROR" {
		t.Fatalf("cluster = %#v", cluster)
	}
}

func TestFinalRunningSaveFailureCompensatesNodesAndBackend(t *testing.T) {
	store := &failingGKEStore{failFrom: 2}
	var mu sync.Mutex
	var deletes int
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			mu.Lock()
			deletes++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer gateway.Close()
	backend := &fakeGKEBackend{created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, gateway.URL, gateway.Client(), store)
	initial := createTestCluster(t, api, "cluster")
	op := waitGKEOperation(t, api.opMgr, initial.Name)
	if op.Error == nil {
		t.Fatal("final save failure unexpectedly succeeded")
	}
	mu.Lock()
	gotDeletes := deletes
	mu.Unlock()
	if gotDeletes != 4 || backend.deletes() != 1 {
		t.Fatalf("cleanup nodes=%d backend=%d", gotDeletes, backend.deletes())
	}
	api.mu.RLock()
	cluster := cloneCluster(api.clusters[clusterKey("demo", "us-central1-c", "cluster")])
	api.mu.RUnlock()
	if cluster == nil || cluster.Status != "ERROR" {
		t.Fatalf("degraded tombstone = %#v", cluster)
	}
}

func createTestCluster(t *testing.T, api *API, name string) GkeOperation {
	t.Helper()
	rec := httptest.NewRecorder()
	body := `{"cluster":{"name":"` + name + `","initialNodeCount":3}}`
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/zones/us-central1-c/clusters", bytes.NewBufferString(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
	}
	var operation GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	return operation
}

func waitGKEOperation(t *testing.T, manager *orchestrator.OperationManager, name string) *orchestrator.Operation {
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
