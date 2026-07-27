package cloudtrace

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"minisky/pkg/config"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("cloudtrace.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (GCP Cloud Trace v2 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Span represents a span in a batchWrite request.
type Span struct {
	Name         string             `json:"name"`
	SpanId       string             `json:"spanId"`
	DisplayName  *TruncatableString `json:"displayName,omitempty"`
	StartTime    string             `json:"startTime,omitempty"`
	EndTime      string             `json:"endTime,omitempty"`
	ParentSpanId string             `json:"parentSpanId,omitempty"`
	Attributes   *Attributes        `json:"attributes,omitempty"`
	Status       *Status            `json:"status,omitempty"`
}

// TruncatableString is the GCP representation of a truncatable string.
type TruncatableString struct {
	Value              string `json:"value"`
	TruncatedByteCount int    `json:"truncatedByteCount,omitempty"`
}

// Attributes holds span attributes.
type Attributes struct {
	AttributeMap map[string]*AttributeValue `json:"attributeMap,omitempty"`
}

// AttributeValue holds a single attribute value.
type AttributeValue struct {
	StringValue *TruncatableString `json:"stringValue,omitempty"`
}

// Status holds the span status.
type Status struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

// Trace represents a trace in get/list responses.
type Trace struct {
	TraceId   string `json:"traceId"`
	ProjectId string `json:"projectId"`
	Spans     []Span `json:"spans,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Cloud Trace v2 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	stateStore cloudtraceStateStore
	traces     map[string]*Trace // key: projectId:traceId
}

// NewAPI creates a new Cloud Trace API shim with persistence.
func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		stateStore: state.NewGuardedEntryStore(store, err),
		traces:     make(map[string]*Trace),
	}
	if err != nil {
		log.Printf("[Shim: Cloud Trace] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Cloud Trace] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		traces: make(map[string]*Trace),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Cloud Trace] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path

	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/traces:batchWrite"):
		api.batchWrite(w, r)
	case r.Method == http.MethodGet && strings.Contains(path, "/traces/"):
		api.getTrace(w, r)
	case r.Method == http.MethodGet && (strings.HasSuffix(path, "/traces") || strings.Contains(path, "/traces?")):
		api.listTraces(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found: "+path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

// batchWrite handles POST /v2/projects/{projectId}/traces:batchWrite
func (api *API) batchWrite(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project := extractProject(r.URL.Path)
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}

	var body struct {
		Spans []Span `json:"spans"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if len(body.Spans) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "spans is required and must not be empty")
		return
	}
	if len(body.Spans) > 1000 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "spans exceeds the maximum batch size of 1000")
		return
	}
	for _, span := range body.Spans {
		traceID := extractTraceId(span.Name)
		if traceID == "" || extractProject(span.Name) != project || span.SpanId == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "each span must have a spanId and a name scoped to the request project")
			return
		}
	}

	api.persistMu.Lock()
	api.mu.Lock()
	before := make(map[string]*Trace, len(api.traces))
	for key, trace := range api.traces {
		before[key] = deepCopyTrace(trace)
	}
	for _, s := range body.Spans {
		traceId := extractTraceId(s.Name)
		if traceId == "" {
			continue
		}
		key := project + ":" + traceId
		trace, ok := api.traces[key]
		if !ok {
			trace = &Trace{
				TraceId:   traceId,
				ProjectId: project,
			}
			api.traces[key] = trace
		}
		// Clone span before storing
		clone := s
		trace.Spans = append(trace.Spans, clone)
	}
	api.mu.Unlock()

	if api.stateStore != nil {
		snapshot := api.snapshotTraces()
		if err := api.stateStore.Save(cloudtraceStateEntry, cloudtraceMetadata{Traces: snapshot}); err != nil {
			api.mu.Lock()
			api.traces = before
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
			return
		}
	}
	api.persistMu.Unlock()

	// batchWrite returns empty {} on success
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// getTrace handles GET /v1/projects/{projectId}/traces/{traceId}
func (api *API) getTrace(w http.ResponseWriter, r *http.Request) {
	project := extractProject(r.URL.Path)
	traceId := extractTraceIdFromPath(r.URL.Path)

	if project == "" || traceId == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project and traceId are required")
		return
	}

	key := project + ":" + traceId
	api.mu.RLock()
	trace, ok := api.traces[key]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("trace '%s' not found", traceId))
		return
	}
	clone := deepCopyTrace(trace)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

// listTraces handles GET /v1/projects/{projectId}/traces
func (api *API) listTraces(w http.ResponseWriter, r *http.Request) {
	project := extractProject(r.URL.Path)
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}
	prefix := project + ":"

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
	items := make([]*Trace, 0)
	for k, v := range api.traces {
		if strings.HasPrefix(k, prefix) {
			items = append(items, deepCopyTrace(v))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(items, pageSize, pageToken, pagination.Scope{
		Service: "cloudtrace",
		Parent:  "projects/" + project,
	}, func(trace *Trace) string { return trace.TraceId })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"traces":        result,
		"nextPageToken": nextToken,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func extractProject(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "projects" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// extractTraceId extracts traceId from a span name like
// "projects/{project}/traces/{traceId}/spans/{spanId}"
func extractTraceId(spanName string) string {
	parts := strings.Split(spanName, "/")
	for i, p := range parts {
		if p == "traces" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// extractTraceIdFromPath extracts traceId from a URL path like
// "/v1/projects/{project}/traces/{traceId}"
func extractTraceIdFromPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "traces" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}
