package dataflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestCreateJobRunsBoundedPipelineToCompletion(t *testing.T) {
	runner := &blockingRunner{started: make(chan struct{}), release: make(chan struct{})}
	api := newTestAPI()
	api.runner = runner
	body := `{"name":"my-batch-job","type":"JOB_TYPE_BATCH","steps":[{"name":"create","kind":"Create","properties":{"elements":["a","b"]}},{"name":"count","kind":"Count"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1b3/projects/test-project/locations/us-central1/jobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var created Job
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.CurrentState != "JOB_STATE_PENDING" {
		t.Fatalf("unexpected create response: %+v", created)
	}
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner was not started")
	}
	waitForJobState(t, api, created.ID, "JOB_STATE_RUNNING")
	close(runner.release)
	waitForJobState(t, api, created.ID, "JOB_STATE_DONE")
}

func TestTerminalPersistenceFailureRecordsStoppedState(t *testing.T) {
	root := t.TempDir()
	const profile = "terminal-failure"
	durableStore, err := state.New(root, profile)
	if err != nil {
		t.Fatal(err)
	}
	delegate := &failNthDataflowStore{delegate: durableStore, failAt: 3}
	guarded := state.NewGuardedEntryStore(delegate, nil)
	api := newTestAPI()
	api.stateStore = guarded
	api.readState = freshDataflowReader(t, root, profile)
	body := `{"name":"my-batch-job","type":"JOB_TYPE_BATCH","steps":[{"name":"create","kind":"Create","properties":{"elements":["a"]}}]}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1b3/projects/p/locations/l/jobs", bytes.NewBufferString(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var job Job
	if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	waitForJobState(t, api, job.ID, "JOB_STATE_STOPPED")
	if guarded.Degraded() == nil {
		t.Fatal("terminal save failure did not degrade GuardedEntryStore")
	}

	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet,
		"/v1b3/projects/p/locations/l/jobs/"+job.ID, nil))
	if get.Code != http.StatusServiceUnavailable {
		t.Fatalf("degraded GET status = %d, want 503: %s", get.Code, get.Body.String())
	}

	freshStore, err := state.New(root, profile)
	if err != nil {
		t.Fatal(err)
	}
	restarted := newTestAPI()
	restarted.stateStore = state.NewGuardedEntryStore(freshStore, nil)
	restarted.readState = freshDataflowReader(t, root, profile)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if got := restarted.jobs[job.ID].CurrentState; got != "JOB_STATE_STOPPED" {
		t.Fatalf("restart state = %q, want STOPPED", got)
	}
	if got := api.jobs[job.ID].CurrentState; got != restarted.jobs[job.ID].CurrentState {
		t.Fatalf("in-memory state = %q, restart state = %q", got, restarted.jobs[job.ID].CurrentState)
	}
}

func TestCreateJobRejectsUnsupportedPipelineBeforeMutation(t *testing.T) {
	api := newTestAPI()
	body := `{"name":"streaming","type":"JOB_TYPE_STREAMING","steps":[{"name":"read","kind":"Create"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1b3/projects/p/locations/l/jobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
	if len(api.jobs) != 0 {
		t.Fatal("unsupported pipeline mutated state")
	}
}

func TestCreateJobPersistenceFailureDoesNotConsumeID(t *testing.T) {
	api := newTestAPI()
	api.stateStore = state.NewGuardedEntryStore(
		&failingDataflowStore{saveErr: fmt.Errorf("disk full")}, nil)
	body := `{"name":"first","type":"JOB_TYPE_BATCH","steps":[{"name":"create","kind":"Create","properties":{"elements":["a"]}}]}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1b3/projects/p/locations/l/jobs", bytes.NewBufferString(body)))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", response.Code, response.Body.String())
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if api.nextID != 1 {
		t.Fatalf("nextID = %d, want 1 after rolled-back create", api.nextID)
	}
	if len(api.jobs) != 0 {
		t.Fatalf("jobs = %d, want no committed job", len(api.jobs))
	}
}

func TestCancelStopsRunningLocalJobWithoutTerminalOverwrite(t *testing.T) {
	runner := &blockingRunner{started: make(chan struct{}), release: make(chan struct{})}
	api := newTestAPI()
	api.runner = runner
	create := httptest.NewRequest(http.MethodPost, "/v1b3/projects/p/locations/l/jobs",
		bytes.NewBufferString(`{"name":"cancel-me","type":"JOB_TYPE_BATCH","steps":[{"name":"create","kind":"Create","properties":{"elements":["a"]}}]}`))
	createResponse := httptest.NewRecorder()
	api.ServeHTTP(createResponse, create)
	var job Job
	if err := json.Unmarshal(createResponse.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner was not started")
	}
	cancel := httptest.NewRequest(http.MethodPut, "/v1b3/projects/p/locations/l/jobs/"+job.ID,
		bytes.NewBufferString(`{"requestedState":"JOB_STATE_CANCELLED"}`))
	cancelResponse := httptest.NewRecorder()
	api.ServeHTTP(cancelResponse, cancel)
	if cancelResponse.Code != http.StatusOK {
		t.Fatalf("cancel: expected 200, got %d: %s", cancelResponse.Code, cancelResponse.Body.String())
	}
	waitForJobState(t, api, job.ID, "JOB_STATE_CANCELLED")
}

func TestCancelPersistenceFailureKeepsRunningJobCancellable(t *testing.T) {
	api := newTestAPI()
	api.stateStore = state.NewGuardedEntryStore(
		&failingDataflowStore{saveErr: fmt.Errorf("disk full")}, nil)
	cancelled := make(chan struct{})
	api.jobs["1"] = &Job{
		ID:           "1",
		ProjectID:    "p",
		Location:     "l",
		CurrentState: "JOB_STATE_RUNNING",
	}
	api.cancels["1"] = func() { close(cancelled) }

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut,
		"/v1b3/projects/p/locations/l/jobs/1",
		bytes.NewBufferString(`{"requestedState":"JOB_STATE_CANCELLED"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", response.Code, response.Body.String())
	}
	select {
	case <-cancelled:
		t.Fatal("failed persistence cancelled the running job")
	default:
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if api.jobs["1"].CurrentState != "JOB_STATE_RUNNING" || api.cancels["1"] == nil {
		t.Fatalf("rollback lost running job control: job=%+v cancel=%v", api.jobs["1"], api.cancels["1"] != nil)
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

type blockingRunner struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingRunner) Run(ctx context.Context, _ *Job) error {
	r.once.Do(func() { close(r.started) })
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForJobState(t *testing.T, api *API, id, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		api.mu.RLock()
		state := api.jobs[id].CurrentState
		api.mu.RUnlock()
		if state == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", id, want)
}

// ─────────────────────────────────────────────────────────────────────────────
// Mock state store for testing
// ─────────────────────────────────────────────────────────────────────────────

type mockStateStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

type failingDataflowStore struct {
	saveErr error
}

func (s *failingDataflowStore) Save(string, any) error { return s.saveErr }
func (s *failingDataflowStore) Load(string, any) error { return state.ErrNotFound }

type failNthDataflowStore struct {
	delegate *state.Store
	failAt   int
	saves    int
}

func (s *failNthDataflowStore) Save(name string, value any) error {
	s.saves++
	if s.saves == s.failAt {
		return fmt.Errorf("disk full")
	}
	return s.delegate.Save(name, value)
}

func (s *failNthDataflowStore) Load(name string, target any) error {
	return s.delegate.Load(name, target)
}

func freshDataflowReader(t *testing.T, root, profile string) func(*dataflowMetadata) error {
	t.Helper()
	return func(metadata *dataflowMetadata) error {
		store, err := state.New(root, profile)
		if err != nil {
			return err
		}
		return store.Load(dataflowStateEntry, metadata)
	}
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
