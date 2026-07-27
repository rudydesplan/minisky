// Package documentai implements the Document AI API v1 shim.
//
// Methods:
//   - POST   /v1/{parent}/processors              — Create processor
//   - GET    /v1/{name}/processors/{id}           — Get processor
//   - GET    /v1/{parent}/processors              — List processors
//   - DELETE /v1/{name}/processors/{id}           — Delete processor (returns LRO)
//   - POST   /v1/{name}/processors/{id}:process   — Process document (synchronous)
//   - GET    /v1/{parent}/operations/{id}         — Get operation
package documentai

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("documentai.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resources
// ─────────────────────────────────────────────────────────────────────────────

// Processor represents a Document AI processor resource.
type Processor struct {
	Name                    string `json:"name"`
	Type                    string `json:"type"`
	DisplayName             string `json:"displayName,omitempty"`
	State                   string `json:"state"`
	CreateTime              string `json:"createTime"`
	DefaultProcessorVersion string `json:"defaultProcessorVersion,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API
// ─────────────────────────────────────────────────────────────────────────────

// API is the Document AI v1 shim.
type API struct {
	mu         sync.RWMutex
	mutationMu sync.Mutex
	persistMu  sync.Mutex
	stateStore documentAIStateStore
	opMgr      *orchestrator.OperationManager
	processors map[string]*Processor // key: full resource name
	operations map[string]*lro       // key: operation name
	seq        int
}

type lro struct {
	Name     string `json:"name"`
	Done     bool   `json:"done"`
	Metadata any    `json:"metadata,omitempty"`
	Response any    `json:"response,omitempty"`
}

// NewAPI creates a new Document AI API handler.
func NewAPI(managers ...*orchestrator.OperationManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := NewAPIWithStore(state.NewGuardedEntryStore(store, err))
	if len(managers) != 0 && managers[0] != nil {
		api.opMgr = managers[0]
	}
	if err != nil {
		log.Printf("[Shim: Document AI] persistence degraded: %v", err)
		return api
	}
	return api
}

func NewAPIWithStore(store documentAIStateStore) *API {
	if _, guarded := store.(*state.GuardedEntryStore); store != nil && !guarded {
		store = state.NewGuardedEntryStore(store, nil)
	}
	api := &API{
		stateStore: store,
		opMgr:      orchestrator.NewOperationManager(),
		processors: make(map[string]*Processor),
		operations: make(map[string]*lro),
	}
	if err := api.rehydrate(); err != nil {
		log.Printf("[Shim: Document AI] state rehydration failed: %v", err)
	}
	return api
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Document AI] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/v1/")

	switch {
	// GET operations
	case r.Method == http.MethodGet && strings.Contains(path, "/operations/"):
		api.getOperation(w, r, path)

	case r.Method == http.MethodPost && (strings.HasSuffix(path, ":batchProcess") ||
		strings.HasSuffix(path, ":enable") || strings.HasSuffix(path, ":disable")):
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"batch processing and processor lifecycle actions are not implemented")

	case strings.Contains(path, "/processorVersions"):
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"processor version methods are not implemented")

	// POST :process
	case r.Method == http.MethodPost && strings.HasSuffix(path, ":process"):
		api.processDocument(w, r, strings.TrimSuffix(path, ":process"))

	// Processor routes
	case strings.Contains(path, "/processors"):
		api.routeProcessors(w, r, path)

	default:
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Processor routing
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeProcessors(w http.ResponseWriter, r *http.Request, path string) {
	switch r.Method {
	case http.MethodPost:
		api.createProcessor(w, r, path)
	case http.MethodGet:
		if isCollection(path) {
			api.listProcessors(w, r, path)
		} else {
			api.getProcessor(w, r, path)
		}
	case http.MethodDelete:
		api.deleteProcessor(w, r, path)
	default:
		gcpError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Create Processor — returns the created Processor directly.
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createProcessor(w http.ResponseWriter, r *http.Request, path string) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var body struct {
		Type        string `json:"type"`
		DisplayName string `json:"displayName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid request body: "+err.Error())
		return
	}
	if body.Type == "" {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'type' is required")
		return
	}
	parent := extractParent(path)
	if !validParent(parent) || path != parent+"/processors" {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	api.mu.Lock()
	api.seq++
	processorID := fmt.Sprintf("proc-%d", api.seq)
	name := parent + "/processors/" + processorID
	now := time.Now().UTC().Format(time.RFC3339)

	processor := &Processor{
		Name:                    name,
		Type:                    body.Type,
		DisplayName:             body.DisplayName,
		State:                   "ENABLED",
		CreateTime:              now,
		DefaultProcessorVersion: name + "/processorVersions/pretrained-v1",
	}
	api.processors[name] = processor
	resp := *processor
	api.mu.Unlock()

	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.processors, name)
		api.mu.Unlock()
		gcpError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&resp)
}

// ─────────────────────────────────────────────────────────────────────────────
// Get Processor
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getProcessor(w http.ResponseWriter, _ *http.Request, path string) {
	api.mu.RLock()
	proc, ok := api.processors[path]
	if !ok {
		api.mu.RUnlock()
		gcpError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("processor %q not found", path))
		return
	}
	clone := *proc // value copy
	api.mu.RUnlock()
	json.NewEncoder(w).Encode(&clone)
}

// ─────────────────────────────────────────────────────────────────────────────
// List Processors
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) listProcessors(w http.ResponseWriter, r *http.Request, path string) {
	parent := extractParent(path)
	prefix := parent + "/processors/"

	pageSize := 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}
	pageToken := r.URL.Query().Get("pageToken")

	api.mu.RLock()
	items := make([]*Processor, 0)
	for k, v := range api.processors {
		if strings.HasPrefix(k, prefix) {
			items = append(items, v)
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(items, pageSize, pageToken, pagination.Scope{
		Service: "documentai.googleapis.com",
		Parent:  parent,
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(processor *Processor) string { return processor.Name })
	if err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"processors":    result,
		"nextPageToken": nextToken,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Delete Processor — returns LRO
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) deleteProcessor(w http.ResponseWriter, _ *http.Request, path string) {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	api.mu.Lock()
	proc, ok := api.processors[path]
	if !ok {
		api.mu.Unlock()
		gcpError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("processor %q not found", path))
		return
	}
	delete(api.processors, path)
	api.seq++
	parent := extractParent(path)
	operationName := fmt.Sprintf("%s/operations/operation-%d", parent, api.seq)
	operation := &lro{
		Name:     operationName,
		Done:     true,
		Metadata: map[string]any{"verb": "delete", "target": path},
		Response: map[string]any{},
	}
	api.operations[operationName] = operation
	api.mu.Unlock()

	if err := api.persist(); err != nil {
		api.mu.Lock()
		api.processors[path] = proc
		delete(api.operations, operationName)
		api.mu.Unlock()
		gcpError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(cloneOperation(operation))
}

// ─────────────────────────────────────────────────────────────────────────────
// Process Document — synchronous
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) processDocument(w http.ResponseWriter, r *http.Request, processorPath string) {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var req struct {
		RawDocument *struct {
			Content  string `json:"content"`
			MimeType string `json:"mimeType"`
		} `json:"rawDocument,omitempty"`
		InlineDocument json.RawMessage `json:"inlineDocument,omitempty"`
		GCSDocument    *struct {
			GCSURI   string `json:"gcsUri"`
			MimeType string `json:"mimeType"`
		} `json:"gcsDocument,omitempty"`
		SkipHumanReview bool   `json:"skipHumanReview,omitempty"`
		FieldMask       string `json:"fieldMask,omitempty"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid request body")
		return
	}

	api.mu.RLock()
	_, ok := api.processors[processorPath]
	api.mu.RUnlock()
	if !ok {
		gcpError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("processor %q not found", processorPath))
		return
	}
	modalities := 0
	if req.RawDocument != nil {
		modalities++
	}
	if len(req.InlineDocument) != 0 {
		modalities++
	}
	if req.GCSDocument != nil {
		modalities++
	}
	if modalities != 1 {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"exactly one of 'rawDocument', 'inlineDocument', or 'gcsDocument' is required")
		return
	}
	if req.InlineDocument != nil {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "field 'inlineDocument' is not implemented")
		return
	}
	if req.GCSDocument != nil {
		uri, err := url.Parse(req.GCSDocument.GCSURI)
		if err != nil || uri.Scheme != "gs" || uri.Host == "" || uri.User != nil {
			gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
				"field 'gcsDocument.gcsUri' must use a credential-free gs:// URI")
			return
		}
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "GCS document input is not implemented")
		return
	}
	if req.RawDocument.MimeType == "" {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'rawDocument.mimeType' is required")
		return
	}
	if req.RawDocument.MimeType != "application/pdf" && req.RawDocument.MimeType != "image/png" &&
		req.RawDocument.MimeType != "image/jpeg" {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'rawDocument.mimeType' is unsupported")
		return
	}
	content, err := base64.StdEncoding.DecodeString(req.RawDocument.Content)
	if err != nil || len(content) == 0 {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'rawDocument.content' must be non-empty base64")
		return
	}
	w.Header().Set("X-MiniSky-Simulated", "true")
	gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
		"document processing is not implemented; no text, entities, or confidence values were generated")
}

// ─────────────────────────────────────────────────────────────────────────────
// Get Operation
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, _ *http.Request, path string) {
	api.mu.RLock()
	local := cloneOperation(api.operations[path])
	api.mu.RUnlock()
	if local != nil {
		_ = json.NewEncoder(w).Encode(local)
		return
	}
	op, err := api.opMgr.PollScoped(path, "documentai#operation")
	if err != nil {
		gcpError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("operation %q not found", path))
		return
	}
	json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// isCollection returns true if path ends with /processors (no trailing ID).
func isCollection(path string) bool {
	return strings.HasSuffix(path, "/processors")
}

// extractParent returns the parent path before /processors.
func extractParent(path string) string {
	idx := strings.LastIndex(path, "/processors")
	if idx < 0 {
		return path
	}
	return path[:idx]
}

func validParent(parent string) bool {
	parts := strings.Split(parent, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[1] != "" &&
		parts[2] == "locations" && parts[3] != ""
}

func gcpError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}
