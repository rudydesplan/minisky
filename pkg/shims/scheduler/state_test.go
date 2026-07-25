package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

type checkingSchedulerStore struct {
	api   *API
	saved bool
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
