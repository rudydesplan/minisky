package vertexai

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/router"
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

func TestVertexPredictExactDeterminismAcrossRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "vertex-predict")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/projects/p/locations/us-central1/endpoints/test:predict"
	body := `{"instances":[{"feature":2,"name":"alpha"},[1,2,3]],"parameters":{"scale":2,"temperature":0},"labels":{"billing":"ignored"}}`

	first := vertexRequest(api, http.MethodPost, path, body)
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	assertPredictResponse(t, first.Body.Bytes(), []predictWant{
		{instance: `{"feature":2,"name":"alpha"}`, score: 0.4920141316233236},
		{instance: `[1,2,3]`, score: 0.9634117816955344},
	})

	restarted, err := NewAPIWithStore(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	second := vertexRequest(restarted, http.MethodPost, path, body)
	if second.Code != http.StatusOK {
		t.Fatalf("restart status=%d body=%s", second.Code, second.Body.String())
	}
	if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatalf("restart response differs:\nfirst:  %s\nsecond: %s", first.Body.Bytes(), second.Body.Bytes())
	}

	semanticallyEquivalent := vertexRequest(restarted, http.MethodPost, path, `{
		"labels": {"billing": "different"},
		"parameters": {"temperature": 0, "scale": 2},
		"instances": [{"name": "alpha", "feature": 2}, [1, 2, 3]]
	}`)
	if !bytes.Equal(first.Body.Bytes(), semanticallyEquivalent.Body.Bytes()) {
		t.Fatalf("JSON serialization or labels affected response:\nfirst: %s\nother: %s",
			first.Body.Bytes(), semanticallyEquivalent.Body.Bytes())
	}

	differentParameters := vertexRequest(restarted, http.MethodPost, path,
		`{"instances":[{"feature":2,"name":"alpha"},[1,2,3]],"parameters":{"scale":3,"temperature":0}}`)
	if differentParameters.Code != http.StatusOK {
		t.Fatalf("different parameters status=%d body=%s", differentParameters.Code, differentParameters.Body.String())
	}
	assertPredictResponse(t, differentParameters.Body.Bytes(), []predictWant{
		{instance: `{"feature":2,"name":"alpha"}`, score: 0.6696577078359336},
		{instance: `[1,2,3]`, score: 0.5217423235349689},
	})
	if bytes.Equal(first.Body.Bytes(), differentParameters.Body.Bytes()) {
		t.Fatal("different parameters produced an identical response")
	}
}

func TestVertexPredictCanonicalizesSemanticJSONBeforeHashing(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/projects/p/locations/us-central1/endpoints/test:predict"
	first := vertexRequest(api, http.MethodPost, path,
		`{"instances":[{"feature":2,"name":"alpha"},[1,2,3]],"parameters":{"scale":2,"temperature":0}}`)
	equivalent := vertexRequest(api, http.MethodPost, path, `{
		"parameters": {"temperature": 0, "scale": 2},
		"instances": [{"name": "alpha", "feature": 2}, [1, 2, 3]]
	}`)
	if first.Code != http.StatusOK || equivalent.Code != http.StatusOK {
		t.Fatalf("statuses=%d/%d bodies=%s/%s", first.Code, equivalent.Code, first.Body.String(), equivalent.Body.String())
	}
	if !bytes.Equal(first.Body.Bytes(), equivalent.Body.Bytes()) {
		t.Fatalf("semantically equivalent JSON differs:\nfirst: %s\nother: %s",
			first.Body.Bytes(), equivalent.Body.Bytes())
	}
}

func TestVertexPredictFramesInstanceAndParametersBeforeHashing(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/projects/p/locations/us-central1/endpoints/test:predict"
	first := vertexRequest(api, http.MethodPost, path, `{"instances":[1],"parameters":23}`)
	second := vertexRequest(api, http.MethodPost, path, `{"instances":[12],"parameters":3}`)
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses=%d/%d bodies=%s/%s", first.Code, second.Code, first.Body.String(), second.Body.String())
	}
	var firstResponse, secondResponse struct {
		Predictions []struct {
			Score float64 `json:"score"`
		} `json:"predictions"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstResponse); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondResponse); err != nil {
		t.Fatal(err)
	}
	if len(firstResponse.Predictions) != 1 || len(secondResponse.Predictions) != 1 {
		t.Fatalf("unexpected prediction counts: %s / %s", first.Body.String(), second.Body.String())
	}
	if firstResponse.Predictions[0].Score == secondResponse.Predictions[0].Score {
		t.Fatalf("ambiguous hash framing produced equal score %v", firstResponse.Predictions[0].Score)
	}
}

func TestVertexPredictRequiresCanonicalPath(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"instances":[{"value":1}]}`
	for _, path := range []string{
		"/v1/projects//locations/us-central1/endpoints/test:predict",
		"/v1/projects/p/locations//endpoints/test:predict",
		"/v1/projects/p/locations/us-central1/endpoints/:predict",
		"/v1/projects/p/locations/us-central1/models/test:predict",
		"/v1/projects/p/locations/us-central1/publishers//models/test:predict",
		"/v1/projects/p/locations/us-central1/publishers/google/models/:predict",
		"/v1/projects/p/locations/us-central1/publishers/google/models/test:predict/extra",
		"/not-a-vertex-path:predict",
		"/v1/projects/p/locations/us-central1/endpoints/test:predict/extra",
	} {
		t.Run(path, func(t *testing.T) {
			response := vertexRequest(api, http.MethodPost, path, body)
			assertVertexError(t, response, http.StatusNotFound, "NOT_FOUND")
		})
	}

	proxy := router.NewProxyRouterWithManager(nil)
	proxy.RegisterShim("aiplatform.googleapis.com", api)
	response := vertexRequest(proxy, http.MethodPost,
		"http://localhost/_minisky/aiplatform/v1/projects/p/locations/us-central1/endpoints/test:predict", body)
	if response.Code != http.StatusOK {
		t.Fatalf("gateway status=%d body=%s", response.Code, response.Body.String())
	}

	publisher := vertexRequest(api, http.MethodPost,
		"/v1/projects/p/locations/us-central1/publishers/google/models/test:predict", body)
	if publisher.Code != http.StatusOK {
		t.Fatalf("publisher model status=%d body=%s", publisher.Code, publisher.Body.String())
	}
}

func TestVertexPredictValidationAndBounds(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/projects/p/locations/us-central1/endpoints/test:predict"
	instances := make([]string, 101)
	for i := range instances {
		instances[i] = `{"value":1}`
	}
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "empty", body: `{"instances":[]}`},
		{name: "over limit", body: `{"instances":[` + strings.Join(instances, ",") + `]}`},
		{name: "malformed", body: `{"instances":[}`},
		{name: "unknown field", body: `{"instances":[1],"unsupported":true}`},
		{name: "trailing JSON", body: `{"instances":[1]}{"instances":[2]}`},
		{name: "over one MiB", body: `{"instances":["` + strings.Repeat("x", (1<<20)+1) + `"]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := vertexRequest(api, http.MethodPost, path, test.body)
			assertVertexError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
		})
	}
}

func TestVertexGenerateContentStillRejectsPredictLabels(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := vertexRequest(api, http.MethodPost,
		"/v1/projects/p/locations/us-central1/publishers/google/models/gemini:generateContent",
		`{"contents":[{"parts":[{"text":"hello"}]}],"labels":{"not":"allowed"}}`)
	assertVertexError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
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

type predictWant struct {
	instance string
	score    float64
}

func assertPredictResponse(t *testing.T, body []byte, want []predictWant) {
	t.Helper()
	var response struct {
		Predictions []struct {
			Instance json.RawMessage `json:"instance"`
			Score    float64         `json:"score"`
		} `json:"predictions"`
		DeployedModelID string         `json:"deployedModelId"`
		Model           string         `json:"model"`
		ModelDisplay    string         `json:"modelDisplayName"`
		ModelVersionID  string         `json:"modelVersionId"`
		Metadata        map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode predict response: %v; body=%s", err, body)
	}
	if len(response.Predictions) != len(want) {
		t.Fatalf("prediction count=%d want=%d body=%s", len(response.Predictions), len(want), body)
	}
	for i, expected := range want {
		if string(response.Predictions[i].Instance) != expected.instance || response.Predictions[i].Score != expected.score {
			t.Fatalf("prediction[%d]=%s score=%v, want instance=%s score=%v",
				i, response.Predictions[i].Instance, response.Predictions[i].Score, expected.instance, expected.score)
		}
	}
	if response.DeployedModelID != "minisky-deterministic" ||
		response.Model != "projects/p/locations/us-central1/models/minisky-deterministic" ||
		response.ModelDisplay != "MiniSky deterministic predictor" ||
		response.ModelVersionID != "1" ||
		response.Metadata["simulation"] != "deterministic-local" {
		t.Fatalf("model metadata=%+v body=%s", response, body)
	}
}
