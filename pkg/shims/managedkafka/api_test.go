package managedkafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

// ─────────────────────────────────────────────────────────────────────────────
// Cluster Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCreateCluster(t *testing.T) {
	api := newTestAPI()
	body := `{"capacity":{"vcpuCount":"3","memoryBytes":"3221225472"},"gcpConfig":{"accessConfig":{"networkConfigs":[{"subnet":"projects/test/regions/us-central1/subnetworks/default"}]}}}`
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

func TestCreateClusterMissingClusterId(t *testing.T) {
	api := newTestAPI()
	body := `{"capacity":{"vcpuCount":"3"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters", bytes.NewBufferString(body))
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
		Name:  "projects/test/locations/us-central1/clusters/dup",
		State: "ACTIVE",
	}
	api.mu.Unlock()

	body := `{}`
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
		Name:       "projects/test/locations/us-central1/clusters/c1",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		State:      "ACTIVE",
		Capacity:   &Capacity{VcpuCount: "3", MemoryBytes: "3221225472"},
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
	if cluster.State != "ACTIVE" {
		t.Fatalf("unexpected state: %s", cluster.State)
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
	api.clusters["projects/test/locations/us-central1/clusters/alpha"] = &Cluster{Name: "projects/test/locations/us-central1/clusters/alpha", State: "ACTIVE"}
	api.clusters["projects/test/locations/us-central1/clusters/beta"] = &Cluster{Name: "projects/test/locations/us-central1/clusters/beta", State: "ACTIVE"}
	api.clusters["projects/test/locations/us-central1/clusters/gamma"] = &Cluster{Name: "projects/test/locations/us-central1/clusters/gamma", State: "ACTIVE"}
	api.mu.Unlock()

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
		t.Fatal("expected nextPageToken")
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

func TestDeleteCluster(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:  "projects/test/locations/us-central1/clusters/c1",
		State: "ACTIVE",
	}
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/t1"] = &Topic{
		Name:           "projects/test/locations/us-central1/clusters/c1/topics/t1",
		PartitionCount: 3,
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/clusters/c1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, clusterExists := api.clusters["projects/test/locations/us-central1/clusters/c1"]
	_, topicExists := api.topics["projects/test/locations/us-central1/clusters/c1/topics/t1"]
	api.mu.RUnlock()
	if !clusterExists {
		t.Fatal("unsupported cluster delete mutated state")
	}
	if !topicExists {
		t.Fatal("unsupported cluster delete mutated topic state")
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
		Name:       "projects/test/locations/us-central1/clusters/c1",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		State:      "ACTIVE",
		Capacity:   &Capacity{VcpuCount: "3", MemoryBytes: "3221225472"},
		Labels:     map[string]string{"env": "dev"},
	}
	api.mu.Unlock()

	body := `{"labels":{"env":"prod"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/clusters/c1?updateMask=labels", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	cluster := api.clusters["projects/test/locations/us-central1/clusters/c1"]
	api.mu.RUnlock()
	if cluster.Labels["env"] != "dev" {
		t.Fatalf("unsupported patch mutated labels: %v", cluster.Labels)
	}
	if cluster.CreateTime != "2024-01-01T00:00:00Z" {
		t.Fatalf("createTime should be preserved, got %s", cluster.CreateTime)
	}
	if cluster.UpdateTime != "2024-01-01T00:00:00Z" {
		t.Fatal("unsupported patch mutated updateTime")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCreateTopic(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:  "projects/test/locations/us-central1/clusters/c1",
		State: "ACTIVE",
	}
	api.mu.Unlock()

	body := `{"partitionCount":3,"replicationFactor":3,"configs":{"retention.ms":"604800000"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters/c1/topics?topicId=my-topic", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if len(api.topics) != 0 {
		t.Fatal("unsupported topic create mutated state")
	}
}

func TestCreateTopicMissingTopicId(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:  "projects/test/locations/us-central1/clusters/c1",
		State: "ACTIVE",
	}
	api.mu.Unlock()

	body := `{"partitionCount":3}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters/c1/topics", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateTopicParentClusterNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"partitionCount":3}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters/missing/topics?topicId=t1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateTopicDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:  "projects/test/locations/us-central1/clusters/c1",
		State: "ACTIVE",
	}
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/dup"] = &Topic{
		Name:           "projects/test/locations/us-central1/clusters/c1/topics/dup",
		PartitionCount: 1,
	}
	api.mu.Unlock()

	body := `{"partitionCount":3}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters/c1/topics?topicId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetTopic(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/t1"] = &Topic{
		Name:              "projects/test/locations/us-central1/clusters/c1/topics/t1",
		PartitionCount:    6,
		ReplicationFactor: 3,
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/c1/topics/t1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var topic Topic
	_ = json.Unmarshal(w.Body.Bytes(), &topic)
	if topic.PartitionCount != 6 {
		t.Fatalf("expected partitionCount=6, got %d", topic.PartitionCount)
	}
}

func TestGetTopicNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/c1/topics/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListTopics(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.clusters["projects/test/locations/us-central1/clusters/c1"] = &Cluster{
		Name:  "projects/test/locations/us-central1/clusters/c1",
		State: "ACTIVE",
	}
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/alpha"] = &Topic{Name: "projects/test/locations/us-central1/clusters/c1/topics/alpha", PartitionCount: 1}
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/beta"] = &Topic{Name: "projects/test/locations/us-central1/clusters/c1/topics/beta", PartitionCount: 2}
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/gamma"] = &Topic{Name: "projects/test/locations/us-central1/clusters/c1/topics/gamma", PartitionCount: 3}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/c1/topics?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	topics := resp["topics"].([]any)
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
	first := topics[0].(map[string]any)["name"].(string)
	second := topics[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken")
	}
}

func TestListTopicsParentNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/missing/topics", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPatchTopic(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/t1"] = &Topic{
		Name:              "projects/test/locations/us-central1/clusters/c1/topics/t1",
		PartitionCount:    3,
		ReplicationFactor: 3,
		Configs:           map[string]string{"retention.ms": "604800000"},
	}
	api.mu.Unlock()

	body := `{"partitionCount":6}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/clusters/c1/topics/t1?updateMask=partitionCount", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if api.topics["projects/test/locations/us-central1/clusters/c1/topics/t1"].PartitionCount != 3 {
		t.Fatal("unsupported topic patch mutated state")
	}
}

func TestPatchTopicNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"partitionCount":6}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/clusters/c1/topics/missing?updateMask=partitionCount", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteTopic(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.topics["projects/test/locations/us-central1/clusters/c1/topics/t1"] = &Topic{
		Name:           "projects/test/locations/us-central1/clusters/c1/topics/t1",
		PartitionCount: 3,
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/clusters/c1/topics/t1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, exists := api.topics["projects/test/locations/us-central1/clusters/c1/topics/t1"]
	api.mu.RUnlock()
	if !exists {
		t.Fatal("unsupported topic delete mutated state")
	}
}

func TestDeleteTopicNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/clusters/c1/topics/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	op, err := api.opMgr.RegisterScopedTargetDurable("managedkafka#operation", "update",
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

// ─────────────────────────────────────────────────────────────────────────────
// Persistence Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		clusters:   make(map[string]*Cluster),
		topics:     make(map[string]*Topic),
	}

	api.mu.Lock()
	api.clusters["projects/p/locations/l/clusters/c1"] = &Cluster{
		Name:       "projects/p/locations/l/clusters/c1",
		CreateTime: "2024-06-01T00:00:00Z",
		State:      "ACTIVE",
		Capacity:   &Capacity{VcpuCount: "3"},
	}
	api.topics["projects/p/locations/l/clusters/c1/topics/t1"] = &Topic{
		Name:           "projects/p/locations/l/clusters/c1/topics/t1",
		PartitionCount: 6,
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		clusters:   make(map[string]*Cluster),
		topics:     make(map[string]*Topic),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	cluster, ok := api2.clusters["projects/p/locations/l/clusters/c1"]
	if !ok {
		t.Fatal("cluster not found after reload")
	}
	if cluster.Capacity == nil || cluster.Capacity.VcpuCount != "3" {
		t.Fatal("capacity lost after reload")
	}
	if cluster.State != "FAILED" {
		t.Fatalf("rehydrated cluster must not claim a live broker backend, got %q", cluster.State)
	}
	topic, ok := api2.topics["projects/p/locations/l/clusters/c1/topics/t1"]
	if !ok {
		t.Fatal("topic not found after reload")
	}
	if topic.PartitionCount != 6 {
		t.Fatalf("expected partitionCount=6, got %d", topic.PartitionCount)
	}
	api2.mu.RUnlock()
}

func TestConcurrentCreateAndGet(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"capacity":{"vcpuCount":"3"}}`
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
