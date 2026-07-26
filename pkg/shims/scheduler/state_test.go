package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestSchedulerMetadataSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "scheduler-profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}

	path := "/v1/projects/test/locations/us-central1/jobs"
	response := schedulerRequest(api, http.MethodPost, path,
		`{"name":"nightly","schedule":"0 0 * * *","httpTarget":{"uri":"http://example.invalid"}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	name := "projects/test/locations/us-central1/jobs/nightly"
	response = schedulerRequest(api, http.MethodPost, "/v1/"+name+":pause", "")
	if response.Code != http.StatusOK {
		t.Fatalf("pause status = %d, body = %s", response.Code, response.Body.String())
	}
	api.Close()

	restarted, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	response = schedulerRequest(restarted, http.MethodGet, "/v1/"+name, "")
	if response.Code != http.StatusOK {
		t.Fatalf("get after restart status = %d, body = %s", response.Code, response.Body.String())
	}
	var job Job
	if err := jsonDecode(response, &job); err != nil {
		t.Fatal(err)
	}
	if job.State != "PAUSED" {
		t.Fatalf("restarted state = %q, want PAUSED", job.State)
	}
}

func TestSchedulerMissingStateAndCorruptState(t *testing.T) {
	store, err := state.New(t.TempDir(), "scheduler-profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatalf("missing state: %v", err)
	}
	api.Close()

	if err := store.Save(schedulerStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	if corruptAPI, err := NewAPIWithConfigAndStore(nil, Config{}, store); err == nil {
		corruptAPI.Close()
		t.Fatal("corrupt state was not reported")
	}
	var persisted string
	if err := store.Load(schedulerStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != "corrupt" {
		t.Fatalf("corrupt state was overwritten with %q", persisted)
	}
}

func TestSchedulerPersistenceDoesNotHoldAPILock(t *testing.T) {
	store := &checkingSchedulerStore{}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	store.api = api

	response := schedulerRequest(api, http.MethodPost, "/v1/projects/test/locations/us-central1/jobs",
		`{"name":"nightly","schedule":"0 0 * * *","httpTarget":{"uri":"http://example.invalid"}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !store.saved {
		t.Fatal("mutation did not save state")
	}
}

func TestSchedulerMutationsLeaveStateAndCronUnchangedWhenSaveFails(t *testing.T) {
	store := &failingSchedulerStore{fail: true}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()

	collection := "/v1/projects/test/locations/us-central1/jobs"
	name := "projects/test/locations/us-central1/jobs/nightly"
	body := `{"name":"nightly","schedule":"0 0 * * *","httpTarget":{"uri":"http://example.invalid"}}`
	response := schedulerRequest(api, http.MethodPost, collection, body)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(api.jobs) != 0 || len(api.cronIDs) != 0 || len(store.snapshot()) != 0 {
		t.Fatalf("failed create changed state: jobs=%#v cron=%#v persisted=%s", api.jobs, api.cronIDs, store.snapshot())
	}

	store.setFail(false)
	if response = schedulerRequest(api, http.MethodPost, collection, body); response.Code != http.StatusOK {
		t.Fatalf("seed create: %d: %s", response.Code, response.Body.String())
	}
	enabledBaseline := store.snapshot()
	store.setFail(true)
	response = schedulerRequest(api, http.MethodPost, "/v1/"+name+":pause", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("pause status = %d, body = %s", response.Code, response.Body.String())
	}
	if api.jobs[name].State != "ENABLED" || len(api.cronIDs) != 1 ||
		string(store.snapshot()) != string(enabledBaseline) {
		t.Fatal("failed pause changed live, cron, or persisted state")
	}
	store.setFail(false)
	if response = schedulerRequest(api, http.MethodPost, "/v1/"+name+":pause", ""); response.Code != http.StatusOK {
		t.Fatalf("seed pause: %d: %s", response.Code, response.Body.String())
	}
	baseline := store.snapshot()
	store.setFail(true)

	response = schedulerRequest(api, http.MethodPost, "/v1/"+name+":resume", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("resume status = %d, body = %s", response.Code, response.Body.String())
	}
	if api.jobs[name].State != "PAUSED" || len(api.cronIDs) != 0 || string(store.snapshot()) != string(baseline) {
		t.Fatal("failed resume changed live, cron, or persisted state")
	}

	response = schedulerRequest(api, http.MethodDelete, "/v1/"+name, "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	if api.jobs[name] == nil || string(store.snapshot()) != string(baseline) {
		t.Fatal("failed delete changed live or persisted state")
	}
}

func TestSchedulerAmbiguousPostCommitReadbackFailureFailsClosed(t *testing.T) {
	store := &sequencedSchedulerStore{
		failAfterCommit: map[int]error{1: errors.New("post-rename sync failure")},
	}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	store.loadErr = errors.New("readback unavailable")

	response := schedulerRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs",
		`{"name":"ambiguous","schedule":"0 0 * * *","httpTarget":{"uri":"http://example.invalid"}}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(api.jobs) != 0 || len(api.cronIDs) != 0 {
		t.Fatalf("rollback failure exposed live state: jobs=%#v cron=%#v", api.jobs, api.cronIDs)
	}
	degraded := api.persistenceFailure()
	if degraded == nil || !strings.Contains(degraded.Error(), "post-rename sync failure") ||
		!strings.Contains(degraded.Error(), "readback unavailable") {
		t.Fatalf("degraded error lost causal details: %v", degraded)
	}
	blocked := schedulerRequest(api, http.MethodGet,
		"/v1/projects/test/locations/us-central1/jobs", "")
	if blocked.Code != http.StatusServiceUnavailable ||
		!strings.Contains(blocked.Body.String(), "readback unavailable") {
		t.Fatalf("degraded API did not fail closed: %d: %s", blocked.Code, blocked.Body.String())
	}
}

func TestSchedulerInvalidCronHasNoPersistenceOrCronSideEffects(t *testing.T) {
	store := &sequencedSchedulerStore{}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()

	response := schedulerRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs",
		`{"name":"invalid","schedule":"not a cron expression","httpTarget":{"uri":"http://example.invalid"}}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if store.saveCount != 0 || len(api.jobs) != 0 || len(api.cronIDs) != 0 {
		t.Fatalf("invalid cron had side effects: saves=%d jobs=%#v cron=%#v",
			store.saveCount, api.jobs, api.cronIDs)
	}
}

func TestSchedulerInvalidCronResumeHasNoPersistenceOrCronSideEffects(t *testing.T) {
	store := &sequencedSchedulerStore{}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	name := "projects/test/locations/us-central1/jobs/invalid"
	api.jobs[name] = &Job{Name: name, State: "PAUSED", Schedule: "not a cron expression"}

	response := schedulerRequest(api, http.MethodPost, "/v1/"+name+":resume", "")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if store.saveCount != 0 || api.jobs[name].State != "PAUSED" || len(api.cronIDs) != 0 {
		t.Fatalf("invalid resume had side effects: saves=%d job=%#v cron=%#v",
			store.saveCount, api.jobs[name], api.cronIDs)
	}
}

func TestSchedulerCreateReconcilesPostCommitSaveError(t *testing.T) {
	store := &sequencedSchedulerStore{
		failAfterCommit: map[int]error{1: errors.New("post-rename sync failure")},
	}
	api, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	response := schedulerRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs",
		`{"name":"committed","schedule":"0 0 * * *","httpTarget":{"uri":"http://example.invalid"}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	api.Close()

	restarted, err := NewAPIWithConfigAndStore(nil, Config{}, store)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	name := "projects/test/locations/us-central1/jobs/committed"
	if restarted.jobs[name] == nil || restarted.cronIDs[name] == 0 {
		t.Fatalf("post-commit state did not reconcile across restart: jobs=%#v cron=%#v",
			restarted.jobs, restarted.cronIDs)
	}
}

type checkingSchedulerStore struct {
	api   *API
	saved bool
}

type failingSchedulerStore struct {
	mu   sync.Mutex
	data []byte
	fail bool
}

type sequencedSchedulerStore struct {
	mu              sync.Mutex
	data            []byte
	saveCount       int
	failOnSave      map[int]error
	failAfterCommit map[int]error
	loadErr         error
}

func (s *sequencedSchedulerStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *sequencedSchedulerStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCount++
	if err := s.failOnSave[s.saveCount]; err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	if err == nil {
		if postCommitErr := s.failAfterCommit[s.saveCount]; postCommitErr != nil {
			return postCommitErr
		}
	}
	return err
}

func (s *failingSchedulerStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *failingSchedulerStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("injected Scheduler save failure")
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	return err
}

func (s *failingSchedulerStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *failingSchedulerStore) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}

func (s *checkingSchedulerStore) Load(string, any) error { return state.ErrNotFound }

func (s *checkingSchedulerStore) Save(string, any) error {
	if !s.api.mu.TryRLock() {
		return errors.New("Scheduler API lock held during save")
	}
	s.api.mu.RUnlock()
	s.saved = true
	return nil
}

func schedulerRequest(api *API, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func jsonDecode(response *httptest.ResponseRecorder, target any) error {
	return json.NewDecoder(response.Body).Decode(target)
}
