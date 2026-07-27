package alloydb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

type fakeBackend struct {
	endpoint       string
	exists         bool
	provisionCalls int
	deleteCalls    int
	provisionErr   error
	deleteErr      error
}

func (backend *fakeBackend) Provision(context.Context, orchestrator.AlloyDBIdentity) (string, bool, error) {
	backend.provisionCalls++
	return backend.endpoint, true, backend.provisionErr
}

func (backend *fakeBackend) Reconcile(context.Context, orchestrator.AlloyDBIdentity) (string, bool, error) {
	return backend.endpoint, backend.exists, backend.provisionErr
}

func (backend *fakeBackend) Delete(context.Context, orchestrator.AlloyDBIdentity) error {
	backend.deleteCalls++
	return backend.deleteErr
}

func TestCreateCluster(t *testing.T) {
	api := newTestAPI()
	body := `{"network":"projects/test/global/networks/default","databaseVersion":"POSTGRES_15"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters?clusterId=my-cluster", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented || !strings.Contains(w.Body.String(), `"status":"UNIMPLEMENTED"`) {
		t.Fatalf("expected canonical 501, got %d: %s", w.Code, w.Body.String())
	}
	api.mu.RLock()
	cluster := api.clusters["projects/test/locations/us-central1/clusters/my-cluster"]
	api.mu.RUnlock()
	if cluster != nil {
		t.Fatal("unsupported create mutated cluster state")
	}
}

func TestCreateInstanceIsCanonicalUnsupportedBeforeMutation(t *testing.T) {
	api := newTestAPI()
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/clusters/c1/instances?instanceId=primary",
		bytes.NewBufferString(`{"instanceType":"PRIMARY"}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusNotImplemented ||
		!strings.Contains(response.Body.String(), `"status":"UNIMPLEMENTED"`) {
		t.Fatalf("expected canonical 501, got %d: %s", response.Code, response.Body.String())
	}
	api.mu.RLock()
	count := len(api.clusters)
	api.mu.RUnlock()
	if count != 0 {
		t.Fatalf("cluster state mutated: %d entries", count)
	}
}

func TestClusterToPrimaryInstanceLifecycleUsesBackend(t *testing.T) {
	backend := &fakeBackend{endpoint: "127.0.0.1:49152", exists: true}
	api := newTestAPI()
	api.backend = backend

	clusterRequest := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/clusters?clusterId=c1",
		bytes.NewBufferString(`{"network":"projects/test/global/networks/default","databaseVersion":"POSTGRES_15"}`))
	clusterResponse := httptest.NewRecorder()
	api.ServeHTTP(clusterResponse, clusterRequest)
	if clusterResponse.Code != http.StatusOK {
		t.Fatalf("create cluster = %d: %s", clusterResponse.Code, clusterResponse.Body.String())
	}

	instanceRequest := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/clusters/c1/instances?instanceId=primary",
		bytes.NewBufferString(`{"instanceType":"PRIMARY","displayName":"Primary"}`))
	instanceResponse := httptest.NewRecorder()
	api.ServeHTTP(instanceResponse, instanceRequest)
	if instanceResponse.Code != http.StatusOK {
		t.Fatalf("create instance = %d: %s", instanceResponse.Code, instanceResponse.Body.String())
	}
	if backend.provisionCalls != 1 {
		t.Fatalf("provision calls = %d, want 1", backend.provisionCalls)
	}

	getRequest := httptest.NewRequest(http.MethodGet,
		"/v1/projects/test/locations/us-central1/clusters/c1/instances/primary", nil)
	getResponse := httptest.NewRecorder()
	api.ServeHTTP(getResponse, getRequest)
	var instance Instance
	if err := json.Unmarshal(getResponse.Body.Bytes(), &instance); err != nil {
		t.Fatal(err)
	}
	if instance.State != "READY" || instance.IPAddress != "127.0.0.1" || instance.PublicIPAddress != "" {
		t.Fatalf("instance endpoint fields = %#v", instance)
	}
	if strings.Contains(getResponse.Body.String(), "49152") {
		t.Fatalf("ephemeral Docker port leaked into AlloyDB resource: %s", getResponse.Body.String())
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete,
		"/v1/projects/test/locations/us-central1/clusters/c1/instances/primary", nil)
	deleteResponse := httptest.NewRecorder()
	api.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusOK || backend.deleteCalls != 1 {
		t.Fatalf("delete = %d calls=%d: %s", deleteResponse.Code, backend.deleteCalls, deleteResponse.Body.String())
	}
}

func TestInstanceProvisionFailureCleansBackendAndMetadata(t *testing.T) {
	backend := &fakeBackend{endpoint: "127.0.0.1:49152", provisionErr: errors.New("startup failed")}
	api := newTestAPI()
	api.backend = backend
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name: "projects/test/locations/us-central1/clusters/c1", State: "READY",
	}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/clusters/c1/instances?instanceId=primary",
		bytes.NewBufferString(`{"instanceType":"PRIMARY"}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"error"`) {
		t.Fatalf("create failure = %d: %s", response.Code, response.Body.String())
	}
	if backend.deleteCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", backend.deleteCalls)
	}
	if len(api.instances) != 0 {
		t.Fatalf("failed instance metadata retained: %#v", api.instances)
	}
}

func TestInstanceCleanupFailureRetainsRecoveryIntent(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	backend := &fakeBackend{
		endpoint:     "127.0.0.1:49152",
		provisionErr: errors.New("startup failed"),
		deleteErr:    errors.New("Docker unavailable"),
	}
	api := &API{
		opMgr: orchestrator.NewOperationManager(), backend: backend, stateStore: store,
		clusters: map[string]*Cluster{"projects/test/locations/us-central1/clusters/c1": {
			Name: "projects/test/locations/us-central1/clusters/c1", State: "READY",
		}},
		instances: make(map[string]*Instance),
	}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/clusters/c1/instances?instanceId=primary",
		bytes.NewBufferString(`{"instanceType":"PRIMARY"}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	name := "projects/test/locations/us-central1/clusters/c1/instances/primary"
	if instance := api.instances[name]; instance == nil || instance.State != "ERROR" {
		t.Fatalf("cleanup recovery intent = %#v", instance)
	}
	var persisted alloydbMetadata
	if err := store.Load(alloydbStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if instance := persisted.Instances[name]; instance == nil || instance.State != "ERROR" {
		t.Fatalf("persisted cleanup recovery intent = %#v", instance)
	}
}

func TestRestartReconcilesWithoutProvisioningDuplicate(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	backend := &fakeBackend{endpoint: "127.0.0.1:55432", exists: true}
	name := "projects/p/locations/l/clusters/c/instances/i"
	api := &API{
		opMgr: orchestrator.NewOperationManager(), backend: backend, stateStore: store,
		clusters: map[string]*Cluster{"projects/p/locations/l/clusters/c": {
			Name: "projects/p/locations/l/clusters/c", State: "READY",
		}},
		instances: map[string]*Instance{name: {Name: name, State: "READY", IPAddress: "127.0.0.1"}},
	}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	restarted := &API{
		opMgr: orchestrator.NewOperationManager(), backend: backend, stateStore: store,
		clusters: make(map[string]*Cluster), instances: make(map[string]*Instance),
	}
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	restarted.reconcileBackends()
	if backend.provisionCalls != 0 {
		t.Fatalf("restart provisioned duplicate backend %d times", backend.provisionCalls)
	}
	if got := restarted.instances[name]; got == nil || got.State != "READY" ||
		got.backendEndpoint != backend.endpoint || got.IPAddress != "127.0.0.1" {
		t.Fatalf("reconciled instance = %#v", got)
	}
}

func TestCreateClusterMissingClusterId(t *testing.T) {
	api := newTestAPI()
	body := `{"network":"projects/test/global/networks/default"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateClusterMissingNetwork(t *testing.T) {
	api := newTestAPI()
	body := `{"databaseVersion":"POSTGRES_15"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters?clusterId=c1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateClusterDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/dup"] = &Cluster{
		Name:    "projects/test/locations/us-central1/clusters/dup",
		UID:     "existing-uid",
		Network: "projects/test/global/networks/default",
	}
	api.mu.Unlock()

	body := `{"network":"projects/test/global/networks/default"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters?clusterId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetCluster(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:            "projects/test/locations/us-central1/clusters/c1",
		UID:             "uid-123",
		CreateTime:      "2024-01-01T00:00:00Z",
		UpdateTime:      "2024-01-01T00:00:00Z",
		State:           "READY",
		DatabaseVersion: "POSTGRES_15",
		Network:         "projects/test/global/networks/default",
		DisplayName:     "My Cluster",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/c1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var cluster Cluster
	_ = json.Unmarshal(w.Body.Bytes(), &cluster)
	if cluster.Name != "projects/test/locations/us-central1/clusters/c1" {
		t.Fatalf("unexpected name: %s", cluster.Name)
	}
	if cluster.State != "READY" {
		t.Fatalf("unexpected state: %s", cluster.State)
	}
	if cluster.Network != "projects/test/global/networks/default" {
		t.Fatalf("unexpected network: %s", cluster.Network)
	}
}

func TestGetClusterNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListClusters(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/alpha"] = &Cluster{Name: "projects/test/locations/us-central1/clusters/alpha", UID: "u1", Network: "n"}
	api.clusters["projects/test/locations/us-central1/clusters/beta"] = &Cluster{Name: "projects/test/locations/us-central1/clusters/beta", UID: "u2", Network: "n"}
	api.clusters["projects/test/locations/us-central1/clusters/gamma"] = &Cluster{Name: "projects/test/locations/us-central1/clusters/gamma", UID: "u3", Network: "n"}
	api.mu.Unlock()

	// First page: pageSize=2
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	clusters := resp["clusters"].([]any)
	if len(clusters) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(clusters))
	}
	first := clusters[0].(map[string]any)["name"].(string)
	second := clusters[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Verify unreachable field present
	if _, ok := resp["unreachable"]; !ok {
		t.Fatal("expected unreachable field in response")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	clusters = resp["clusters"].([]any)
	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster on second page, got %d", len(clusters))
	}
}

func TestListClustersEmpty(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	clusters := resp["clusters"].([]any)
	if len(clusters) != 0 {
		t.Fatalf("expected 0 clusters, got %d", len(clusters))
	}
}

func TestDeleteCluster(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:    "projects/test/locations/us-central1/clusters/c1",
		UID:     "uid-1",
		Network: "n",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/clusters/c1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for delete")
	}

	api.mu.RLock()
	_, exists := api.clusters["projects/test/locations/us-central1/clusters/c1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("cluster should have been deleted")
	}
}

func TestDeleteClusterNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/clusters/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPatchCluster(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:            "projects/test/locations/us-central1/clusters/c1",
		UID:             "uid-1",
		CreateTime:      "2024-01-01T00:00:00Z",
		UpdateTime:      "2024-01-01T00:00:00Z",
		State:           "READY",
		DatabaseVersion: "POSTGRES_15",
		Network:         "projects/test/global/networks/default",
		DisplayName:     "Old Name",
	}
	api.mu.Unlock()

	body := `{"displayName":"New Name"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/clusters/c1?updateMask=displayName", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for patch")
	}

	api.mu.RLock()
	cluster := api.clusters["projects/test/locations/us-central1/clusters/c1"]
	api.mu.RUnlock()
	if cluster.DisplayName != "New Name" {
		t.Fatalf("expected updated displayName, got %s", cluster.DisplayName)
	}
	if cluster.UID != "uid-1" {
		t.Fatalf("uid should be preserved, got %s", cluster.UID)
	}
	if cluster.CreateTime != "2024-01-01T00:00:00Z" {
		t.Fatalf("createTime should be preserved, got %s", cluster.CreateTime)
	}
	if cluster.UpdateTime == "2024-01-01T00:00:00Z" {
		t.Fatal("updateTime should have been updated")
	}
	if cluster.Network != "projects/test/global/networks/default" {
		t.Fatal("network should not have changed")
	}
}

func TestPatchClusterNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"displayName":"x"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/clusters/missing?updateMask=displayName", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	op, err := api.opMgr.RegisterScopedTargetDurable("alloydb#operation", "update",
		"projects/test/locations/us-central1/clusters/op-test")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/"+op.Name, nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var opResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &opResp)
	meta := opResp["metadata"].(map[string]any)
	if meta["verb"] != "update" {
		t.Fatalf("expected verb=update, got %v", meta["verb"])
	}
}

func TestGetOperationNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/operations/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		clusters:   make(map[string]*Cluster),
	}

	api.mu.Lock()
	api.clusters["projects/p/locations/l/clusters/c1"] = &Cluster{
		Name:            "projects/p/locations/l/clusters/c1",
		UID:             "uid-persist",
		CreateTime:      "2024-06-01T00:00:00Z",
		UpdateTime:      "2024-06-01T00:00:00Z",
		State:           "READY",
		Network:         "projects/p/global/networks/default",
		DatabaseVersion: "POSTGRES_15",
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		clusters:   make(map[string]*Cluster),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	cluster, ok := api2.clusters["projects/p/locations/l/clusters/c1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("cluster not found after reload")
	}
	if cluster.UID != "uid-persist" {
		t.Fatalf("expected uid-persist, got %s", cluster.UID)
	}
	if cluster.Network != "projects/p/global/networks/default" {
		t.Fatal("network lost after reload")
	}
}

func TestConcurrentCreateAndGet(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"network":"projects/test/global/networks/default"}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/projects/test/locations/us-central1/clusters?clusterId=c-%d", idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusNotImplemented {
				t.Errorf("unexpected status %d for create %d", w.Code, idx)
			}
		}(i)
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return fmt.Errorf("not found: %w", state.ErrNotFound)
	}
	return json.Unmarshal(raw, target)
}

func (m *mockStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
