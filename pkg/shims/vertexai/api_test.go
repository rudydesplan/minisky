package vertexai

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestVertexMockGenerateAndPredictAreDeterministic(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	generateBody := `{"contents":[{"role":"user","parts":[{"text":"hello local model"}]}]}`
	first := vertexRequest(api, http.MethodPost,
		"/v1/projects/p/locations/us-central1/publishers/google/models/gemini:generateContent", generateBody)
	second := vertexRequest(api, http.MethodPost,
		"/v1/projects/p/locations/us-central1/publishers/google/models/gemini:generateContent", generateBody)
	if first.Code != http.StatusOK || second.Code != http.StatusOK || first.Body.String() != second.Body.String() {
		t.Fatalf("generate responses differ: %d %q / %d %q", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if !strings.Contains(first.Body.String(), "hello local model") {
		t.Fatalf("generate response = %s", first.Body.String())
	}

	predictBody := `{"instances":[{"feature":2},{"feature":1}],"parameters":{"temperature":0}}`
	predict := vertexRequest(api, http.MethodPost,
		"/v1/projects/p/locations/us-central1/endpoints/test:predict", predictBody)
	if predict.Code != http.StatusOK || !strings.Contains(predict.Body.String(), `"predictions"`) {
		t.Fatalf("predict status = %d, body = %s", predict.Code, predict.Body.String())
	}
}

func TestVertexPersistsNonSecretConfigOnly(t *testing.T) {
	store, err := state.New(t.TempDir(), "vertex")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	update := vertexRequest(api, http.MethodPost, "/v1/internal/config",
		`{"provider":"mock","model":"gemini-test","mockResponse":"configured","apiKey":"do-not-persist"}`)
	if update.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", update.Code, update.Body.String())
	}
	var raw json.RawMessage
	if err := store.Load(vertexStateEntry, &raw); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("do-not-persist")) {
		t.Fatalf("secret persisted in state: %s", raw)
	}
	restarted, err := NewAPIWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := vertexRequest(restarted, http.MethodPost,
		"/v1/projects/p/locations/us-central1/publishers/google/models/gemini:generateContent",
		`{"contents":[{"parts":[{"text":"ignored"}]}]}`)
	if !strings.Contains(response.Body.String(), "configured") {
		t.Fatalf("configured mock response did not survive restart: %s", response.Body.String())
	}
}

func TestVertexNormalizesDefaultsBeforePersisting(t *testing.T) {
	store, err := state.New(t.TempDir(), "vertex-defaults")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := vertexRequest(api, http.MethodPost, "/v1/internal/config", `{}`)
	if response.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", response.Code, response.Body.String())
	}
	var saved vertexConfig
	if err := store.Load(vertexStateEntry, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Provider != "mock" || saved.Model != "gemini-minisky" {
		t.Fatalf("persisted config = %#v", saved)
	}
}

func TestCorruptStateDisablesVertexRuntime(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-vertex")
	store, err := state.New(root, "corrupt-vertex")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(vertexStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(nil)
	response := vertexRequest(api, http.MethodPost, "/v1/internal/config", `{}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(vertexStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

func TestVertexOllamaIsDependencyGatedAndLoopbackOnly(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	unsafe := vertexRequest(api, http.MethodPost, "/v1/internal/config",
		`{"provider":"ollama","endpoint":"http://example.com:11434","model":"test"}`)
	assertVertexError(t, unsafe, http.StatusBadRequest, "INVALID_ARGUMENT")

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message": map[string]string{"role": "assistant", "content": "ollama response"},
		})
	}))
	defer backend.Close()
	configure := vertexRequest(api, http.MethodPost, "/v1/internal/config",
		`{"provider":"ollama","endpoint":"`+backend.URL+`","model":"test"}`)
	if configure.Code != http.StatusOK {
		t.Fatalf("configure status = %d, body = %s", configure.Code, configure.Body.String())
	}
	generate := vertexRequest(api, http.MethodPost,
		"/v1/projects/p/locations/us-central1/publishers/google/models/test:generateContent",
		`{"contents":[{"parts":[{"text":"hello"}]}]}`)
	if generate.Code != http.StatusOK || !strings.Contains(generate.Body.String(), "ollama response") {
		t.Fatalf("ollama status = %d, body = %s", generate.Code, generate.Body.String())
	}
}

func TestVertexUnsupportedSurfacesReturn501(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/v1/projects/p/locations/us-central1/batchPredictionJobs",
		"/v1/projects/p/locations/us-central1/featurestores",
	} {
		response := vertexRequest(api, http.MethodPost, path, `{}`)
		assertVertexError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
	}
}

func vertexRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertVertexError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Status != status {
		t.Fatalf("status = %q, want %q", envelope.Error.Status, status)
	}
}
