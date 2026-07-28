package managedkafka

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
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

// ─────────────────────────────────────────────────────────────────────────────
// Cluster Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCreateCluster(t *testing.T) {
	api := newTestAPI()
	api.backend = &fakeKafkaBackend{bootstrap: "127.0.0.1:19092"}
	body := `{"capacityConfig":{"vcpuCount":"3","memoryBytes":"3221225472"},"gcpConfig":{"accessConfig":{"networkConfigs":[{"subnet":"projects/test/regions/us-central1/subnetworks/default"}]}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/clusters?clusterId=my-cluster", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	waitForClusterState(t, api, "projects/test/locations/us-central1/clusters/my-cluster", "ACTIVE")
	api.mu.RLock()
	cluster := api.clusters["projects/test/locations/us-central1/clusters/my-cluster"]
	api.mu.RUnlock()
	if cluster == nil || cluster.BootstrapAddress != "127.0.0.1:19092" {
		t.Fatalf("missing executable broker address: %+v", cluster)
	}
}

func TestCreateClusterMissingClusterId(t *testing.T) {
	api := newTestAPI()
	body := `{"capacityConfig":{"vcpuCount":"3"}}`
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

func TestCreateClusterMetadataSaveFailureRollsBackDurableOperation(t *testing.T) {
	operationStore := &mockStore{data: make(map[string][]byte)}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &mockStore{
		data:       make(map[string][]byte),
		failOnSave: map[int]error{1: errors.New("metadata save failed")},
	}
	api := &API{
		opMgr:      opMgr,
		stateStore: metadataStore,
		clusters:   make(map[string]*Cluster),
		topics:     make(map[string]*Topic),
		backend:    &fakeKafkaBackend{},
	}

	req := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/clusters?clusterId=save-fails",
		bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if len(api.opMgr.List()) != 0 {
		t.Fatalf("failed create left an in-memory operation: %+v", api.opMgr.List())
	}
	restarted, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.List()) != 0 {
		t.Fatalf("failed create left a durable operation: %+v", restarted.List())
	}
}

func TestPatchClusterOperationRegistrationFailureDoesNotMutate(t *testing.T) {
	operationStore := &mockStore{
		data:       make(map[string][]byte),
		failOnSave: map[int]error{1: errors.New("operation save failed")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &mockStore{data: make(map[string][]byte)}
	name := "projects/test/locations/us-central1/clusters/c1"
	api := &API{
		opMgr:      opMgr,
		stateStore: metadataStore,
		clusters: map[string]*Cluster{name: {
			Name:       name,
			CreateTime: "2024-01-01T00:00:00Z",
			UpdateTime: "2024-01-01T00:00:00Z",
			State:      "ACTIVE",
			Labels:     map[string]string{"env": "dev"},
		}},
		topics:  make(map[string]*Topic),
		backend: &fakeKafkaBackend{},
	}

	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/clusters/c1?updateMask=labels",
		bytes.NewBufferString(`{"labels":{"env":"prod"}}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/c1", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("cluster was not externally visible after failed patch: %d: %s", get.Code, get.Body.String())
	}
	var cluster Cluster
	if err := json.Unmarshal(get.Body.Bytes(), &cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.Labels["env"] != "dev" || cluster.UpdateTime != "2024-01-01T00:00:00Z" {
		t.Fatalf("operation registration failure mutated cluster: %+v", &cluster)
	}
	if metadataStore.saveCalls != 0 {
		t.Fatalf("operation registration failure persisted metadata %d times", metadataStore.saveCalls)
	}
}

func TestDeleteClusterOperationRegistrationFailureDoesNotMutateBackend(t *testing.T) {
	operationStore := &mockStore{
		data:       make(map[string][]byte),
		failOnSave: map[int]error{1: errors.New("operation save failed")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &mockStore{data: make(map[string][]byte)}
	backend := &fakeKafkaBackend{}
	name := "projects/test/locations/us-central1/clusters/c1"
	topicName := name + "/topics/t1"
	api := &API{
		opMgr:      opMgr,
		stateStore: metadataStore,
		clusters:   map[string]*Cluster{name: {Name: name, State: "ACTIVE"}},
		topics:     map[string]*Topic{topicName: {Name: topicName}},
		backend:    backend,
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/clusters/c1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	getCluster := httptest.NewRecorder()
	api.ServeHTTP(getCluster, httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/c1", nil))
	if getCluster.Code != http.StatusOK {
		t.Fatalf("operation registration failure hid cluster: %d: %s", getCluster.Code, getCluster.Body.String())
	}
	getTopic := httptest.NewRecorder()
	api.ServeHTTP(getTopic, httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/clusters/c1/topics/t1", nil))
	if getTopic.Code != http.StatusOK {
		t.Fatalf("operation registration failure hid topic: %d: %s", getTopic.Code, getTopic.Body.String())
	}
	if backend.deleteCalls != 0 {
		t.Fatalf("operation registration failure deleted backend %d times", backend.deleteCalls)
	}
	if metadataStore.saveCalls != 0 {
		t.Fatalf("operation registration failure persisted metadata %d times", metadataStore.saveCalls)
	}
}

func TestClusterMutationTerminalOperationSaveFailureReturnsErrorAndKeepsMetadataTruth(t *testing.T) {
	const name = "projects/test/locations/us-central1/clusters/c1"
	tests := []struct {
		name       string
		method     string
		body       string
		wantExists bool
		wantLabel  string
	}{
		{
			name:       "patch",
			method:     http.MethodPatch,
			body:       `{"labels":{"env":"prod"}}`,
			wantExists: true,
			wantLabel:  "prod",
		},
		{
			name:       "delete",
			method:     http.MethodDelete,
			wantExists: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operationStore := &mockStore{
				data:       make(map[string][]byte),
				failOnSave: map[int]error{2: errors.New("terminal operation save failed")},
			}
			opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			metadataStore := &mockStore{data: make(map[string][]byte)}
			topicName := name + "/topics/t1"
			api := &API{
				opMgr:      opMgr,
				stateStore: metadataStore,
				clusters: map[string]*Cluster{name: {
					Name:       name,
					CreateTime: "2024-01-01T00:00:00Z",
					UpdateTime: "2024-01-01T00:00:00Z",
					State:      "ACTIVE",
					Labels:     map[string]string{"env": "dev"},
				}},
				topics:  map[string]*Topic{topicName: {Name: topicName}},
				backend: &fakeKafkaBackend{},
			}

			path := "/v1/projects/test/locations/us-central1/clusters/c1"
			if test.method == http.MethodPatch {
				path += "?updateMask=labels"
			}
			req := httptest.NewRequest(test.method, path, bytes.NewBufferString(test.body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)

			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), `"done":true`) {
				t.Fatalf("terminal persistence failure fabricated success: %s", w.Body.String())
			}
			var response struct {
				Error struct {
					Status string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Error.Status != "UNAVAILABLE" {
				t.Fatalf("error status = %q, want UNAVAILABLE", response.Error.Status)
			}

			operations := opMgr.List()
			if len(operations) != 1 {
				t.Fatalf("operations = %+v, want one pending operation", operations)
			}
			inProcess := operations[0]
			if inProcess.Done || inProcess.Status != orchestrator.StatusPending || inProcess.Error != nil {
				t.Fatalf("uncommitted terminal result was published in process: %+v", inProcess)
			}

			restartedOps, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			durable := restartedOps.Get(inProcess.Name)
			if durable == nil || !durable.Done || durable.Error == nil ||
				!strings.Contains(durable.Error.Message, "interrupted by MiniSky restart") {
				t.Fatalf("pending operation was not reconciled across restart: in-process=%+v restarted=%+v", inProcess, durable)
			}

			restarted := &API{
				opMgr:      orchestrator.NewOperationManager(),
				stateStore: metadataStore,
				clusters:   make(map[string]*Cluster),
				topics:     make(map[string]*Topic),
			}
			if err := restarted.loadState(); err != nil {
				t.Fatal(err)
			}
			restarted.mu.RLock()
			cluster, exists := restarted.clusters[name]
			_, topicExists := restarted.topics[topicName]
			restarted.mu.RUnlock()
			if exists != test.wantExists {
				t.Fatalf("persisted cluster existence = %v, want %v", exists, test.wantExists)
			}
			if exists && cluster.Labels["env"] != test.wantLabel {
				t.Fatalf("persisted labels = %v, want env=%q", cluster.Labels, test.wantLabel)
			}
			if test.method == http.MethodDelete && topicExists {
				t.Fatal("persisted cluster deletion retained a child topic")
			}
		})
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
		Name:             "projects/p/locations/l/clusters/c1",
		CreateTime:       "2024-06-01T00:00:00Z",
		State:            "ACTIVE",
		Capacity:         &Capacity{VcpuCount: "3"},
		BootstrapAddress: "127.0.0.1:19092",
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
		backend:    &fakeKafkaBackend{bootstrap: "127.0.0.1:19092"},
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
	if cluster.State != "ACTIVE" {
		t.Fatalf("rehydrated exact-owned broker must be active, got %q", cluster.State)
	}
	if cluster.BootstrapAddress != "127.0.0.1:19092" {
		t.Fatalf("rehydrated cluster has broker address %q", cluster.BootstrapAddress)
	}
	backend := api2.backend.(*fakeKafkaBackend)
	if backend.provisionCalls != 0 || backend.reconcileCalls != 1 {
		t.Fatalf("restart provision calls = %d, reconcile calls = %d; want 0 and 1",
			backend.provisionCalls, backend.reconcileCalls)
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

func TestReloadFailsClosedWithoutExactOwnedKafkaBackend(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	name := "projects/p/locations/l/clusters/c1"
	topicName := name + "/topics/t1"
	if err := store.Save(managedKafkaStateEntry, managedKafkaMetadata{
		Clusters: map[string]*Cluster{name: {
			Name:             name,
			State:            "ACTIVE",
			BootstrapAddress: "127.0.0.1:19092",
		}},
		Topics: map[string]*Topic{topicName: {Name: topicName}},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeKafkaBackend{}
	api := &API{
		opMgr:      orchestrator.NewOperationManager(),
		stateStore: store,
		clusters:   make(map[string]*Cluster),
		topics:     make(map[string]*Topic),
		backend:    backend,
	}
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	cluster := api.clusters[name]
	if cluster == nil || cluster.State != "FAILED" || cluster.BootstrapAddress != "" {
		t.Fatalf("rehydrated cluster = %+v, want fail-closed metadata without endpoint", cluster)
	}
	if api.topics[topicName] == nil {
		t.Fatal("fail-closed restart lost durable topic metadata")
	}
	if backend.provisionCalls != 0 || backend.reconcileCalls != 1 {
		t.Fatalf("restart provision calls = %d, reconcile calls = %d; want 0 and 1",
			backend.provisionCalls, backend.reconcileCalls)
	}
}

func TestReloadGivesEachKafkaResourceAFairReconcileBudget(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	clusters := make(map[string]*Cluster)
	for _, id := range []string{"a", "b", "c"} {
		name := "projects/p/locations/l/clusters/" + id
		clusters[name] = &Cluster{Name: name, State: "ACTIVE"}
	}
	if err := store.Save(managedKafkaStateEntry, managedKafkaMetadata{Clusters: clusters}); err != nil {
		t.Fatal(err)
	}
	backend := &budgetKafkaBackend{}
	api := &API{
		opMgr:            orchestrator.NewOperationManager(),
		stateStore:       store,
		clusters:         make(map[string]*Cluster),
		topics:           make(map[string]*Topic),
		backend:          backend,
		reconcileTimeout: 10 * time.Millisecond,
	}
	start := time.Now()
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if backend.reconcileCalls != 3 {
		t.Fatalf("reconcile calls = %d, want 3", backend.reconcileCalls)
	}
	if elapsed < 20*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("reconciliation elapsed = %v, want independent bounded budgets", elapsed)
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
			body := `{"capacityConfig":{"vcpuCount":"3"}}`
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
	mu         sync.Mutex
	data       map[string][]byte
	failOnSave map[int]error
	saveCalls  int
}

type fakeKafkaBackend struct {
	bootstrap      string
	provisionCalls int
	reconcileCalls int
	deleteCalls    int
}

type budgetKafkaBackend struct {
	reconcileCalls int
}

func (*budgetKafkaBackend) Provision(context.Context, string) (string, error) {
	return "", nil
}
func (b *budgetKafkaBackend) Reconcile(ctx context.Context, _ string) (string, bool, error) {
	b.reconcileCalls++
	<-ctx.Done()
	return "", false, ctx.Err()
}
func (*budgetKafkaBackend) Delete(context.Context, string) error { return nil }
func (*budgetKafkaBackend) CreateTopic(context.Context, string, *Topic) error {
	return nil
}
func (*budgetKafkaBackend) UpdateTopic(context.Context, string, *Topic) error {
	return nil
}
func (*budgetKafkaBackend) DeleteTopic(context.Context, string, string) error {
	return nil
}

func (b *fakeKafkaBackend) Provision(context.Context, string) (string, error) {
	b.provisionCalls++
	return b.bootstrap, nil
}
func (b *fakeKafkaBackend) Reconcile(context.Context, string) (string, bool, error) {
	b.reconcileCalls++
	return b.bootstrap, b.bootstrap != "", nil
}
func (b *fakeKafkaBackend) Delete(context.Context, string) error {
	b.deleteCalls++
	return nil
}
func (b *fakeKafkaBackend) CreateTopic(context.Context, string, *Topic) error {
	return nil
}
func (b *fakeKafkaBackend) UpdateTopic(context.Context, string, *Topic) error {
	return nil
}
func (b *fakeKafkaBackend) DeleteTopic(context.Context, string, string) error {
	return nil
}

func waitForClusterState(t *testing.T, api *API, name, want string) {
	t.Helper()
	for i := 0; i < 5000; i++ {
		api.mu.RLock()
		state := ""
		if cluster := api.clusters[name]; cluster != nil {
			state = cluster.State
		}
		api.mu.RUnlock()
		if state == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cluster did not reach %s", want)
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
	m.saveCalls++
	if err := m.failOnSave[m.saveCalls]; err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
