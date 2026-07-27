package batch

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

func TestCreateJob(t *testing.T) {
	api := newTestAPI()
	body := `{"taskGroups":[{"taskSpec":{},"taskCount":"1","parallelism":"1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var job Job
	if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if job.Name != "projects/test/locations/us-central1/jobs/j1" {
		t.Fatalf("unexpected name: %s", job.Name)
	}
	if job.UID == "" {
		t.Fatal("expected uid to be generated")
	}
	if job.CreateTime == "" {
		t.Fatal("expected createTime to be set")
	}
	if job.UpdateTime == "" {
		t.Fatal("expected updateTime to be set")
	}
	if job.Status == nil || job.Status.State != "QUEUED" {
		t.Fatalf("expected status.state=QUEUED, got %+v", job.Status)
	}
	if len(job.TaskGroups) != 1 {
		t.Fatalf("expected 1 taskGroup, got %d", len(job.TaskGroups))
	}
}

func TestCreateJobGeneratesOptionalId(t *testing.T) {
	api := newTestAPI()
	body := `{"taskGroups":[{"taskSpec":{}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var job Job
	if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(job.Name, "/jobs/job-") {
		t.Fatalf("generated job name = %q", job.Name)
	}
}

func TestCreateJobMissingTaskGroups(t *testing.T) {
	api := newTestAPI()
	body := `{"labels":{"env":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateExecutableJobReturns501WithoutFakeSuccess(t *testing.T) {
	api := newTestAPI()
	body := `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"img"}}]}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
	if len(api.jobs) != 0 {
		t.Fatal("unsupported executable job must not be stored")
	}
}

func TestCancelJobReturnsLROAndPersistsCancelledState(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us-central1/jobs/j1"
	api.jobs[name] = &Job{Name: name, TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "QUEUED"}}

	req := httptest.NewRequest(http.MethodPost, "/v1/"+name+":cancel", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var operation map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if operation["done"] != true {
		t.Fatalf("operation = %#v", operation)
	}
	if api.jobs[name].Status.State != "CANCELLED" {
		t.Fatalf("state = %s", api.jobs[name].Status.State)
	}
}

func TestCreateJobDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/dup"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/dup",
		UID:        "existing-uid",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{}}},
		Status:     &JobStatus{State: "QUEUED"},
	}
	api.mu.Unlock()

	body := `{"taskGroups":[{"taskSpec":{}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/j1"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/j1",
		UID:        "uid-123",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{Runnables: []Runnable{{Container: &Container{ImageURI: "img"}}}}}},
		Status:     &JobStatus{State: "RUNNING"},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs/j1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var job Job
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	if job.Name != "projects/test/locations/us-central1/jobs/j1" {
		t.Fatalf("unexpected name: %s", job.Name)
	}
	if job.UID != "uid-123" {
		t.Fatalf("unexpected uid: %s", job.UID)
	}
	if job.Status == nil || job.Status.State != "RUNNING" {
		t.Fatalf("unexpected status: %+v", job.Status)
	}
}

func TestGetJobNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListJobs(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/alpha"] = &Job{Name: "projects/test/locations/us-central1/jobs/alpha", UID: "u1", CreateTime: "2024-01-01T00:00:00Z", UpdateTime: "2024-01-01T00:00:00Z", TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "QUEUED"}}
	api.jobs["projects/test/locations/us-central1/jobs/beta"] = &Job{Name: "projects/test/locations/us-central1/jobs/beta", UID: "u2", CreateTime: "2024-01-01T00:00:00Z", UpdateTime: "2024-01-01T00:00:00Z", TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "RUNNING"}}
	api.jobs["projects/test/locations/us-central1/jobs/gamma"] = &Job{Name: "projects/test/locations/us-central1/jobs/gamma", UID: "u3", CreateTime: "2024-01-01T00:00:00Z", UpdateTime: "2024-01-01T00:00:00Z", TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "SUCCEEDED"}}
	api.mu.Unlock()

	// First page: pageSize=2
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	jobs := resp["jobs"].([]any)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	// Verify sorted order
	first := jobs[0].(map[string]any)["name"].(string)
	second := jobs[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	jobs = resp["jobs"].([]any)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job on second page, got %d", len(jobs))
	}
}

func TestDeleteJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/j1"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/j1",
		UID:        "uid-1",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{}},
		Status:     &JobStatus{State: "SUCCEEDED"},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/jobs/j1", nil)
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
	name, _ := resp["name"].(string)
	if name == "" {
		t.Fatal("expected operation name in response")
	}
	meta, _ := resp["metadata"].(map[string]any)
	if meta == nil {
		t.Fatal("expected metadata in response")
	}
	if meta["verb"] != "delete" {
		t.Fatalf("expected verb=delete, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/jobs/j1" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}

	// Verify job was removed
	api.mu.RLock()
	_, exists := api.jobs["projects/test/locations/us-central1/jobs/j1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("job should have been deleted")
	}
}

func TestDeleteJobNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/jobs/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPatchJobNotAllowed(t *testing.T) {
	api := newTestAPI()
	body := `{"labels":{"env":"prod"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/jobs/j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMetadataOnlyJobNeverFakesTerminalSuccess(t *testing.T) {
	api := newTestAPI()
	body := `{"taskGroups":[{"taskSpec":{},"taskCount":"1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=trans", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Initially QUEUED
	api.mu.RLock()
	job := api.jobs["projects/test/locations/us-central1/jobs/trans"]
	initialState := job.Status.State
	api.mu.RUnlock()
	if initialState != "QUEUED" {
		t.Fatalf("expected initial state QUEUED, got %s", initialState)
	}

	api.mu.RLock()
	job = api.jobs["projects/test/locations/us-central1/jobs/trans"]
	finalState := job.Status.State
	api.mu.RUnlock()
	if finalState != "QUEUED" {
		t.Fatalf("expected final state QUEUED, got %s", finalState)
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	// Create and delete a job to generate an operation
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/op-test"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/op-test",
		UID:        "uid-op",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{}},
		Status:     &JobStatus{State: "SUCCEEDED"},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/jobs/op-test", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("delete failed: %d: %s", w.Code, w.Body.String())
	}

	var deleteResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &deleteResp)
	opPath := deleteResp["name"].(string)

	// Get the operation
	req = httptest.NewRequest(http.MethodGet, "/v1/"+opPath, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var opResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &opResp)
	if opResp["done"] != true {
		t.Fatal("expected operation done=true")
	}
	meta := opResp["metadata"].(map[string]any)
	if meta["verb"] != "delete" {
		t.Fatalf("expected verb=delete, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/jobs/op-test" {
		t.Fatalf("unexpected target: %v", meta["target"])
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
		jobs:       make(map[string]*Job),
	}

	// Create a job
	api.mu.Lock()
	api.jobs["projects/p/locations/l/jobs/j1"] = &Job{
		Name:       "projects/p/locations/l/jobs/j1",
		UID:        "uid-persist",
		CreateTime: "2024-06-01T00:00:00Z",
		UpdateTime: "2024-06-01T00:00:00Z",
		TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{Runnables: []Runnable{{Container: &Container{ImageURI: "img"}}}}}},
		Status:     &JobStatus{State: "SUCCEEDED"},
	}
	api.mu.Unlock()

	// Persist
	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Create a new API and reload
	api2 := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		jobs:       make(map[string]*Job),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	job, ok := api2.jobs["projects/p/locations/l/jobs/j1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("job not found after reload")
	}
	if job.UID != "uid-persist" {
		t.Fatalf("expected uid-persist, got %s", job.UID)
	}
	if job.Status == nil || job.Status.State != "SUCCEEDED" {
		t.Fatal("status lost after reload")
	}
	if len(job.TaskGroups) == 0 || job.TaskGroups[0].TaskSpec == nil {
		t.Fatal("taskGroups lost after reload")
	}
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	// Concurrent creates
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"taskGroups":[{"taskSpec":{},"taskCount":"1"}]}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/projects/test/locations/us-central1/jobs?jobId=job-%d", idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("unexpected status %d for create %d", w.Code, idx)
			}
		}(i)
	}

	// Concurrent gets/lists
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs", nil)
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

// mockStore is a simple in-memory state store for testing.
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
