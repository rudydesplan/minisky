package vertexai

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const (
	vertexStateEntry    = "vertexai/config"
	maxVertexJSONBody   = 1 << 20
	maxPredictInstances = 100
)

func init() {
	registry.Register("aiplatform.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.SvcMgr)
	})
}

type VertexRequest struct {
	Contents []Content `json:"contents"`
}

type Content struct {
	Role  string `json:"role,omitempty"`
	Parts []Part `json:"parts"`
}

type Part struct {
	Text string `json:"text"`
}

type VertexResponse struct {
	Candidates    []Candidate   `json:"candidates"`
	UsageMetadata UsageMetadata `json:"usageMetadata,omitempty"`
}

type Candidate struct {
	Content      Content `json:"content"`
	FinishReason string  `json:"finishReason,omitempty"`
	Index        int     `json:"index"`
}

type UsageMetadata struct {
	PromptTokenCount     int `json:"promptTokenCount"`
	CandidatesTokenCount int `json:"candidatesTokenCount"`
	TotalTokenCount      int `json:"totalTokenCount"`
}

type vertexConfig struct {
	Provider     string `json:"provider"`
	Endpoint     string `json:"endpoint,omitempty"`
	Model        string `json:"model,omitempty"`
	MockResponse string `json:"mockResponse,omitempty"`
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type API struct {
	mu         sync.RWMutex
	svcMgr     *orchestrator.ServiceManager
	store      stateStore
	httpClient *http.Client
	config     vertexConfig
	apiKey     string
	initErr    error
}

func NewAPI(sm *orchestrator.ServiceManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Vertex AI] state disabled: %v", err)
		return newAPI(sm, nil)
	}
	api, err := NewAPIWithStore(store, sm)
	if err != nil {
		log.Printf("[Shim: Vertex AI] state rehydration failed: %v", err)
		disabled := newAPI(sm, nil)
		disabled.initErr = err
		return disabled
	}
	return api
}

func NewAPIWithStore(store stateStore, sm *orchestrator.ServiceManager) (*API, error) {
	api := newAPI(sm, store)
	if store == nil {
		return api, nil
	}
	var saved vertexConfig
	if err := store.Load(vertexStateEntry, &saved); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Vertex AI config: %w", err)
	}
	saved = normalizeVertexConfig(saved)
	if err := validateConfig(saved); err != nil {
		return nil, fmt.Errorf("load Vertex AI config: %w", err)
	}
	api.config = saved
	return api, nil
}

func newAPI(sm *orchestrator.ServiceManager, store stateStore) *API {
	return &API{
		svcMgr: sm,
		store:  store,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		config: vertexConfig{Provider: "mock", Model: "gemini-minisky"},
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if api.initErr != nil {
		writeError(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION", "Vertex AI state is unavailable")
		return
	}
	path := r.URL.Path
	scope, predictPath := predictScopeFromPath(path)
	switch {
	case path == "/v1/internal/config" && r.Method == http.MethodPost:
		api.handleConfigUpdate(w, r)
	case path == "/v1/internal/config" && r.Method == http.MethodGet:
		api.handleConfigGet(w)
	case path == "/v1/internal/models" && r.Method == http.MethodGet:
		api.handleListModels(w)
	case strings.Contains(path, ":streamGenerateContent"):
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "streamGenerateContent is not implemented")
	case strings.Contains(path, ":generateContent") && r.Method == http.MethodPost:
		api.handleGenerateContent(w, r)
	case predictPath && r.Method == http.MethodPost:
		api.handlePredict(w, r, scope)
	case strings.Contains(path, "/batchPredictionJobs") || strings.Contains(path, "/featurestores"):
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Vertex AI batch prediction and feature store APIs are not implemented")
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Vertex AI resource not found")
	}
}

func (api *API) handleConfigUpdate(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Provider     string `json:"provider"`
		Endpoint     string `json:"endpoint"`
		APIKey       string `json:"apiKey"`
		Model        string `json:"model"`
		MockResponse string `json:"mockResponse"`
	}
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid Vertex AI config JSON")
		return
	}
	next := normalizeVertexConfig(vertexConfig{
		Provider:     strings.ToLower(strings.TrimSpace(request.Provider)),
		Endpoint:     strings.TrimSuffix(strings.TrimSpace(request.Endpoint), "/"),
		Model:        strings.TrimSpace(request.Model),
		MockResponse: request.MockResponse,
	})
	if err := validateConfig(next); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	api.mu.Lock()
	previous := api.config
	previousKey := api.apiKey
	api.config = next
	api.apiKey = request.APIKey
	api.mu.Unlock()
	if api.store != nil {
		if err := api.store.Save(vertexStateEntry, next); err != nil {
			api.mu.Lock()
			api.config = previous
			api.apiKey = previousKey
			api.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Vertex AI config")
			return
		}
	}
	_ = json.NewEncoder(w).Encode(next)
}

func (api *API) handleConfigGet(w http.ResponseWriter) {
	api.mu.RLock()
	current := api.config
	api.mu.RUnlock()
	_ = json.NewEncoder(w).Encode(current)
}

func (api *API) handleGenerateContent(w http.ResponseWriter, r *http.Request) {
	var request VertexRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid generateContent JSON")
		return
	}
	prompt := promptText(request)
	if prompt == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "contents.parts.text is required")
		return
	}

	api.mu.RLock()
	current := api.config
	api.mu.RUnlock()
	var responseText string
	var err error
	switch current.Provider {
	case "mock":
		responseText = current.MockResponse
		if responseText == "" {
			responseText = "MiniSky mock response: " + prompt
		}
	case "ollama":
		responseText, err = api.generateWithOllama(r, current, request)
	default:
		err = fmt.Errorf("provider %q is unavailable", current.Provider)
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Vertex AI dependency unavailable: "+err.Error())
		return
	}

	promptTokens := tokenCount(prompt)
	responseTokens := tokenCount(responseText)
	response := VertexResponse{
		Candidates: []Candidate{{
			Content:      Content{Role: "model", Parts: []Part{{Text: responseText}}},
			FinishReason: "STOP",
			Index:        0,
		}},
		UsageMetadata: UsageMetadata{
			PromptTokenCount:     promptTokens,
			CandidatesTokenCount: responseTokens,
			TotalTokenCount:      promptTokens + responseTokens,
		},
	}
	_ = json.NewEncoder(w).Encode(response)
}

type predictScope struct {
	project  string
	location string
}

func (api *API) handlePredict(w http.ResponseWriter, r *http.Request, scope predictScope) {
	var request struct {
		Instances  []json.RawMessage `json:"instances"`
		Parameters json.RawMessage   `json:"parameters,omitempty"`
		Labels     map[string]string `json:"labels,omitempty"`
	}
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid predict JSON")
		return
	}
	if len(request.Instances) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instances must contain at least one value")
		return
	}
	if len(request.Instances) > maxPredictInstances {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instances may contain at most 100 values")
		return
	}
	var canonicalParameters []byte
	if len(request.Parameters) > 0 {
		var parameters any
		if err := json.Unmarshal(request.Parameters, &parameters); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "parameters must contain a valid JSON value")
			return
		}
		encodedParameters, err := json.Marshal(parameters)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "parameters must contain a valid JSON value")
			return
		}
		canonicalParameters = encodedParameters
	}
	type prediction struct {
		Instance any     `json:"instance"`
		Score    float64 `json:"score"`
	}
	predictions := make([]prediction, len(request.Instances))
	for i, instance := range request.Instances {
		var decoded any
		if err := json.Unmarshal(instance, &decoded); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instances must contain valid JSON values")
			return
		}
		canonicalInstance, err := json.Marshal(decoded)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instances must contain valid JSON values")
			return
		}
		score := deterministicPredictionScore(canonicalInstance, canonicalParameters)
		predictions[i] = prediction{Instance: decoded, Score: score}
	}
	response := struct {
		Predictions      []prediction `json:"predictions"`
		DeployedModelID  string       `json:"deployedModelId"`
		Model            string       `json:"model"`
		ModelDisplayName string       `json:"modelDisplayName"`
		ModelVersionID   string       `json:"modelVersionId"`
		Metadata         struct {
			Simulation string `json:"simulation"`
		} `json:"metadata"`
	}{
		Predictions:      predictions,
		DeployedModelID:  "minisky-deterministic",
		Model:            "projects/" + scope.project + "/locations/" + scope.location + "/models/minisky-deterministic",
		ModelDisplayName: "MiniSky deterministic predictor",
		ModelVersionID:   "1",
	}
	response.Metadata.Simulation = "deterministic-local"
	_ = json.NewEncoder(w).Encode(response)
}

func deterministicPredictionScore(canonicalInstance, canonicalParameters []byte) float64 {
	hash := sha256.New()
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(canonicalInstance)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(canonicalInstance)
	binary.BigEndian.PutUint64(length[:], uint64(len(canonicalParameters)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(canonicalParameters)
	sum := hash.Sum(nil)
	return float64(binary.BigEndian.Uint32(sum[:4])) / float64(^uint32(0))
}

func predictScopeFromPath(path string) (predictScope, bool) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) < 5 ||
		parts[0] != "v1" ||
		parts[1] != "projects" || !validPredictPathSegment(parts[2]) ||
		parts[3] != "locations" || !validPredictPathSegment(parts[4]) {
		return predictScope{}, false
	}
	scope := predictScope{project: parts[2], location: parts[4]}
	switch {
	case len(parts) == 7 && parts[5] == "endpoints" && validPredictTarget(parts[6]):
		return scope, true
	case len(parts) == 9 &&
		parts[5] == "publishers" && validPredictPathSegment(parts[6]) &&
		parts[7] == "models" && validPredictTarget(parts[8]):
		return scope, true
	default:
		return predictScope{}, false
	}
}

func validPredictPathSegment(value string) bool {
	return value != "" && !strings.ContainsAny(value, "/:")
}

func validPredictTarget(value string) bool {
	target, ok := strings.CutSuffix(value, ":predict")
	return ok && validPredictPathSegment(target)
}

func (api *API) generateWithOllama(r *http.Request, current vertexConfig, request VertexRequest) (string, error) {
	type ollamaMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	body := struct {
		Model    string          `json:"model"`
		Messages []ollamaMessage `json:"messages"`
		Stream   bool            `json:"stream"`
	}{Model: current.Model, Stream: false}
	for _, content := range request.Contents {
		role := content.Role
		if role == "" {
			role = "user"
		}
		parts := make([]string, 0, len(content.Parts))
		for _, part := range content.Parts {
			parts = append(parts, part.Text)
		}
		body.Messages = append(body.Messages, ollamaMessage{Role: role, Content: strings.Join(parts, "\n")})
	}
	encoded, _ := json.Marshal(body)
	outbound, err := http.NewRequestWithContext(r.Context(), http.MethodPost, current.Endpoint+"/api/chat", bytes.NewReader(encoded))
	if err != nil {
		return "", err
	}
	outbound.Header.Set("Content-Type", "application/json")
	response, err := api.httpClient.Do(outbound)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Ollama returned HTTP %d", response.StatusCode)
	}
	var result struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&result); err != nil {
		return "", err
	}
	if result.Message.Content == "" {
		return "", fmt.Errorf("Ollama returned an empty response")
	}
	return result.Message.Content, nil
}

func (api *API) handleListModels(w http.ResponseWriter) {
	api.mu.RLock()
	current := api.config
	api.mu.RUnlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"models": []string{current.Model}, "provider": current.Provider})
}

func validateConfig(config vertexConfig) error {
	switch config.Provider {
	case "mock":
		return nil
	case "ollama":
		if config.Model == "" {
			return fmt.Errorf("model is required for Ollama")
		}
		parsed, err := url.Parse(config.Endpoint)
		if err != nil || parsed.Scheme != "http" || parsed.Hostname() == "" || parsed.User != nil ||
			(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("Ollama endpoint must be a loopback HTTP origin")
		}
		host := parsed.Hostname()
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("Ollama endpoint must use a literal loopback IP address")
		}
		return nil
	default:
		return fmt.Errorf("provider must be mock or ollama")
	}
}

func normalizeVertexConfig(config vertexConfig) vertexConfig {
	config.Provider = strings.ToLower(strings.TrimSpace(config.Provider))
	config.Endpoint = strings.TrimSuffix(strings.TrimSpace(config.Endpoint), "/")
	config.Model = strings.TrimSpace(config.Model)
	if config.Provider == "" {
		config.Provider = "mock"
	}
	if config.Provider == "mock" && config.Model == "" {
		config.Model = "gemini-minisky"
	}
	return config
}

func promptText(request VertexRequest) string {
	var parts []string
	for _, content := range request.Contents {
		for _, part := range content.Parts {
			if strings.TrimSpace(part.Text) != "" {
				parts = append(parts, part.Text)
			}
		}
	}
	return strings.Join(parts, "\n")
}

func tokenCount(value string) int {
	if value == "" {
		return 0
	}
	return len(strings.Fields(value))
}

func decodeJSON(r *http.Request, target any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxVertexJSONBody+1))
	if err != nil {
		return err
	}
	if len(body) > maxVertexJSONBody {
		return fmt.Errorf("JSON body exceeds %d bytes", maxVertexJSONBody)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "status": status, "message": message},
	})
}
