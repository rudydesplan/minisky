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

const vertexStateEntry = "vertexai/config"

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
	case strings.Contains(path, ":predict") && r.Method == http.MethodPost:
		api.handlePredict(w, r)
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

func (api *API) handlePredict(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Instances  []json.RawMessage `json:"instances"`
		Parameters json.RawMessage   `json:"parameters,omitempty"`
	}
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid predict JSON")
		return
	}
	if len(request.Instances) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instances must contain at least one value")
		return
	}
	predictions := make([]map[string]any, len(request.Instances))
	for i, instance := range request.Instances {
		sum := sha256.Sum256(append(append([]byte(nil), instance...), request.Parameters...))
		score := float64(binary.BigEndian.Uint32(sum[:4])) / float64(^uint32(0))
		var decoded any
		if err := json.Unmarshal(instance, &decoded); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instances must contain valid JSON values")
			return
		}
		predictions[i] = map[string]any{"instance": decoded, "score": score}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"predictions":     predictions,
		"deployedModelId": "minisky-mock",
		"model":           "publishers/minisky/models/deterministic-mock",
	})
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
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "status": status, "message": message},
	})
}
