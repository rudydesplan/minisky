package aiplatform

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

type fakeStore struct {
	mu      sync.Mutex
	payload []byte
	loadErr error
	saveErr error
}

func (s *fakeStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	if s.payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.payload, target)
}

func (s *fakeStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.payload, _ = json.Marshal(value)
	return nil
}

func perform(api *API, method, path, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
	return response
}

func TestIndexAndOperationSurviveRestart(t *testing.T) {
	store := &fakeStore{}
	api := NewAPIWithStore(store)
	created := perform(api, http.MethodPost, "/v1/projects/p/locations/us/indexes",
		`{"displayName":"metadata-only"}`)
	if created.Code != 200 {
		t.Fatalf("create status = %d, body = %s", created.Code, created.Body.String())
	}
	var operation Operation
	if err := json.NewDecoder(created.Body).Decode(&operation); err != nil {
		t.Fatal(err)
	}
	if !operation.Done || operation.Name == "" {
		t.Fatalf("operation = %#v", operation)
	}
	restarted := NewAPIWithStore(store)
	polled := perform(restarted, http.MethodGet, "/v1/"+operation.Name, "")
	if polled.Code != 200 {
		t.Fatalf("poll after restart = %d, body = %s", polled.Code, polled.Body.String())
	}
	listed := perform(restarted, http.MethodGet, "/v1/projects/p/locations/us/indexes?pageSize=1", "")
	if listed.Code != 200 || !strings.Contains(listed.Body.String(), "metadata-only") {
		t.Fatalf("list after restart = %d %s", listed.Code, listed.Body.String())
	}
}

func TestMutationSaveFailureRollsBackAndSticks(t *testing.T) {
	store := &fakeStore{saveErr: errors.New("injected")}
	api := NewAPIWithStore(store)
	for attempt := 0; attempt < 2; attempt++ {
		response := perform(api, http.MethodPost, "/v1/projects/p/locations/us/indexes",
			`{"displayName":"lost"}`)
		if response.Code != 503 {
			t.Fatalf("attempt %d status = %d", attempt, response.Code)
		}
		store.mu.Lock()
		store.saveErr = nil
		store.mu.Unlock()
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if len(api.indexes) != 0 || len(api.ops) != 0 {
		t.Fatalf("failed save leaked state: indexes=%d operations=%d", len(api.indexes), len(api.ops))
	}
}

func TestExecutionMethodsFailHonestly(t *testing.T) {
	response := perform(NewAPIWithStore(nil), http.MethodPost,
		"/v1/projects/p/locations/us/indexEndpoints/e:findNeighbors", `{}`)
	if response.Code != 501 || strings.Contains(response.Body.String(), `"distance":`) {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestUploadModelRejectsCredentialURLBeforeMutation(t *testing.T) {
	api := NewAPIWithStore(nil)
	response := perform(api, http.MethodPost, "/v1/projects/p/locations/us/models:upload",
		`{"model":{"displayName":"unsafe","artifactUri":"https://user:secret@example.com/model"}}`)
	if response.Code != 400 {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if len(api.models) != 0 || len(api.ops) != 0 {
		t.Fatal("unsafe upload mutated state")
	}
}

func TestCorruptStateFailsClosed(t *testing.T) {
	api := NewAPIWithStore(&fakeStore{loadErr: errors.New("corrupt")})
	response := perform(api, http.MethodPost, "/v1/projects/p/locations/us/indexes",
		`{"displayName":"blocked"}`)
	if response.Code != 503 {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}
