package dialogflowcx

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

func createAgent(t *testing.T, api *API, display string) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"displayName":"` + display + `","defaultLanguageCode":"en","timeZone":"UTC"}`
	request := httptest.NewRequest(http.MethodPost, "/v3/projects/p/locations/us/agents", strings.NewReader(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func TestAgentRestartSaveFailureAndConcurrency(t *testing.T) {
	store := &fakeStore{}
	api := NewAPIWithStore(store)
	if response := createAgent(t, api, "first"); response.Code != 200 {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	restarted := NewAPIWithStore(store)
	get := httptest.NewRecorder()
	restarted.ServeHTTP(get, httptest.NewRequest(http.MethodGet,
		"/v3/projects/p/locations/us/agents/agent-1", nil))
	if get.Code != 200 {
		t.Fatalf("restart get status = %d, body = %s", get.Code, get.Body.String())
	}

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if response := createAgent(t, restarted, "concurrent"); response.Code != 200 {
				t.Errorf("concurrent create status = %d", response.Code)
			}
		}()
	}
	wg.Wait()

	failing := &fakeStore{saveErr: errors.New("injected")}
	failedAPI := NewAPIWithStore(failing)
	if response := createAgent(t, failedAPI, "lost"); response.Code != 503 {
		t.Fatalf("save failure status = %d", response.Code)
	}
	failedAPI.mu.RLock()
	defer failedAPI.mu.RUnlock()
	if len(failedAPI.agents) != 0 {
		t.Fatal("failed save left an acknowledged in-memory agent")
	}
}

func TestDetectIntentDoesNotFabricateConfidence(t *testing.T) {
	api := NewAPIWithStore(nil)
	api.agents["projects/p/locations/us/agents/a"] = &Agent{
		Name: "projects/p/locations/us/agents/a", DisplayName: "a",
		DefaultLanguageCode: "en", TimeZone: "UTC",
	}
	body := `{"queryInput":{"text":{"text":"hello"},"languageCode":"en"}}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v3/projects/p/locations/us/agents/a/sessions/s:detectIntent", strings.NewReader(body)))
	if response.Code != 501 || strings.Contains(response.Body.String(), `"confidence"`) {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestCorruptStateFailsClosed(t *testing.T) {
	api := NewAPIWithStore(&fakeStore{loadErr: errors.New("corrupt")})
	if response := createAgent(t, api, "blocked"); response.Code != 503 {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}
