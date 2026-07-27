package dataflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestCreateJob(t *testing.T) {
	api := newTestAPI()
	body := `{"name":"my-batch-job","type":"JOB_TYPE_BATCH","environment":{"tempStoragePrefix":"gs://bucket/tmp"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1b3/projects/test-project/locations/us-central1/jobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"status":"UNIMPLEMENTED"`)) {
		t.Fatalf("expected UNIMPLEMENTED error, got %s", w.Body.String())
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if len(api.jobs) != 0 || api.nextID != 1 {
		t.Fatalf("unsupported create mutated state: jobs=%d nextID=%d", len(api.jobs), api.nextID)
	}
}

func TestGetJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["123"] = &Job{
		ID:           "123",
		Name:         "test-job",
		ProjectID:    "test-project",
		Location:     "us-central1",
		CurrentState: "JOB_STATE_RUNNING",
		CreateTime:   "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1b3/projects/test-project/locations/us-central1/jobs/123", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var job Job
	if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job.ID != "123" {
		t.Fatalf("expected id '123', got %q", job.ID)
	}
	if job.CurrentState != "JOB_STATE_RUNNING" {
		t.Fatalf("expected JOB_STATE_RUNNING, got %s", job.CurrentState)
	}
}

func TestGetJobNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1b3/projects/test-project/locations/us-central1/jobs/999", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListJobs(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("%d", i)
		api.jobs[id] = &Job{ID: id, ProjectID: "test-project", Location: "us-central1", CurrentState: "JOB_STATE_RUNNING"}
	}
	api.mu.Unlock()

	// List with pageSize=2.
	req := httptest.NewRequest(http.MethodGet, "/v1b3/projects/test-project/locations/us-central1/jobs?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp struct {
		Jobs          []Job  `json:"jobs"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(resp.Jobs))
	}
	if resp.NextPageToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Fetch next page.
	req2 := httptest.NewRequest(http.MethodGet, "/v1b3/projects/test-project/locations/us-central1/jobs?pageSize=2&pageToken="+resp.NextPageToken, nil)
	w2 := httptest.NewRecorder()
	api.ServeHTTP(w2, req2)

	var resp2 struct {
		Jobs          []Job  `json:"jobs"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("decode page 2: %v", err)
	}
	if len(resp2.Jobs) != 1 {
		t.Fatalf("expected 1 job on page 2, got %d", len(resp2.Jobs))
	}
	if resp2.NextPageToken != "" {
		t.Fatalf("expected empty nextPageToken on last page, got %q", resp2.NextPageToken)
	}
}

func TestCancelJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["123"] = &Job{ID: "123", ProjectID: "test-project", Location: "us-central1", CurrentState: "JOB_STATE_RUNNING"}
	api.mu.Unlock()

	// Cancel via PUT with requestedState.
	cancelBody := `{"requestedState":"JOB_STATE_CANCELLED"}`
	req2 := httptest.NewRequest(http.MethodPut, "/v1b3/projects/test-project/locations/us-central1/jobs/123", bytes.NewBufferString(cancelBody))
	w2 := httptest.NewRecorder()
	api.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("cancel: expected 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var cancelled Job
	_ = json.Unmarshal(w2.Body.Bytes(), &cancelled)
	if cancelled.CurrentState != "JOB_STATE_CANCELLED" {
		t.Fatalf("expected JOB_STATE_CANCELLED, got %s", cancelled.CurrentState)
	}
	if cancelled.RequestedState != "JOB_STATE_CANCELLED" {
		t.Fatalf("expected requestedState JOB_STATE_CANCELLED, got %s", cancelled.RequestedState)
	}
}

func TestDeleteNotAllowed(t *testing.T) {
	api := newTestAPI()
	// Try DELETE on a job path.
	req := httptest.NewRequest(http.MethodDelete, "/v1b3/projects/test-project/locations/us-central1/jobs/123", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDrainJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["123"] = &Job{ID: "123", ProjectID: "test-project", Location: "us-central1", CurrentState: "JOB_STATE_RUNNING"}
	api.mu.Unlock()

	req2 := httptest.NewRequest(http.MethodPut, "/v1b3/projects/test-project/locations/us-central1/jobs/123",
		bytes.NewBufferString(`{"requestedState":"JOB_STATE_DRAINED"}`))
	w2 := httptest.NewRecorder()
	api.ServeHTTP(w2, req2)
	var drained Job
	_ = json.Unmarshal(w2.Body.Bytes(), &drained)
	if drained.CurrentState != "JOB_STATE_DRAINED" {
		t.Fatalf("expected JOB_STATE_DRAINED, got %s", drained.CurrentState)
	}
}

func TestPersistAndReload(t *testing.T) {
	// Use a mock state store to verify persist/reload cycle.
	store := &mockStateStore{data: make(map[string][]byte)}
	api := &API{
		jobs:       make(map[string]*Job),
		nextID:     1,
		stateStore: store,
	}

	api.mu.Lock()
	api.jobs["123"] = &Job{ID: "123", Name: "persist-test", ProjectID: "test-project", Location: "us-central1", CurrentState: "JOB_STATE_RUNNING"}
	api.mu.Unlock()
	req := httptest.NewRequest(http.MethodPut, "/v1b3/projects/test-project/locations/us-central1/jobs/123", bytes.NewBufferString(`{"requestedState":"JOB_STATE_CANCELLED"}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("cancel: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify state was persisted.
	if _, ok := store.data[dataflowStateEntry]; !ok {
		t.Fatal("expected state to be persisted")
	}

	// Create a new API instance and reload from the same store.
	api2 := &API{
		jobs:       make(map[string]*Job),
		nextID:     1,
		stateStore: store,
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("loadState: %v", err)
	}

	if len(api2.jobs) != 1 {
		t.Fatalf("expected 1 job after reload, got %d", len(api2.jobs))
	}
	reloaded, ok := api2.jobs["123"]
	if !ok {
		t.Fatal("expected job 123 after reload")
	}
	if reloaded.Name != "persist-test" {
		t.Fatalf("expected name 'persist-test', got %q", reloaded.Name)
	}
	if reloaded.CurrentState != "JOB_STATE_CANCELLED" {
		t.Fatalf("expected durable cancellation after reload, got %s", reloaded.CurrentState)
	}
}

func TestReloadStopsNonterminalLegacyJob(t *testing.T) {
	store := &mockStateStore{data: make(map[string][]byte)}
	store.data[dataflowStateEntry], _ = json.Marshal(dataflowMetadata{Jobs: map[string]*Job{
		"123": {ID: "123", ProjectID: "p", Location: "l", CurrentState: "JOB_STATE_RUNNING"},
	}})
	api := &API{jobs: make(map[string]*Job), nextID: 1, stateStore: store}
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	if api.jobs["123"].CurrentState != "JOB_STATE_STOPPED" {
		t.Fatalf("rehydrated job must not claim execution, got %q", api.jobs["123"].CurrentState)
	}
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	const n = 50

	api.mu.Lock()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%d", i)
		api.jobs[id] = &Job{ID: id, ProjectID: "test-project", Location: "us-central1", CurrentState: "JOB_STATE_RUNNING"}
	}
	api.mu.Unlock()

	// Concurrent durable updates.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/v1b3/projects/test-project/locations/us-central1/jobs/%d", i), bytes.NewBufferString(`{"requestedState":"JOB_STATE_CANCELLED"}`))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("concurrent cancel: expected 200, got %d", w.Code)
			}
		}(i)
	}
	wg.Wait()

	// Verify all jobs updated.
	api.mu.RLock()
	if len(api.jobs) != n {
		t.Fatalf("expected %d jobs, got %d", n, len(api.jobs))
	}
	for id, job := range api.jobs {
		if job.CurrentState != "JOB_STATE_CANCELLED" {
			t.Fatalf("job %s not cancelled: %s", id, job.CurrentState)
		}
	}
	api.mu.RUnlock()

	// Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1b3/projects/test-project/locations/us-central1/jobs", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("concurrent list: expected 200, got %d", w.Code)
			}
		}()
	}
	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Mock state store for testing
// ─────────────────────────────────────────────────────────────────────────────

type mockStateStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *mockStateStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}

func (m *mockStateStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}
