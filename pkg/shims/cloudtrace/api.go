package cloudtrace

import (
	"encoding/hex"
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
	log.Printf("[Shim: Cloud Trace] %s %q", r.Method, r.URL.EscapedPath())
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

	project, ok := parseBatchWriteProject(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "request path must contain a canonical project")
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
		spanProject, _, nameSpanID, ok := parseSpanName(span.Name)
		if !ok || spanProject != project || nameSpanID != span.SpanId {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "each span name must match its spanId and be scoped to the request project")
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
		_, traceId, _, _ := parseSpanName(s.Name)
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
		canonical := make([]Span, 0, len(trace.Spans)+1)
		for _, stored := range trace.Spans {
			if stored.SpanId != clone.SpanId {
				canonical = append(canonical, stored)
			}
		}
		trace.Spans = append(canonical, clone)
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

func parseSpanName(name string) (project, traceID, spanID string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || !validProjectSegment(parts[1]) ||
		parts[2] != "traces" || parts[4] != "spans" ||
		!isHexID(parts[3], 32) || !isHexID(parts[5], 16) {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

func parseBatchWriteProject(r *http.Request) (string, bool) {
	if r.URL.RawPath != "" || strings.Contains(r.URL.EscapedPath(), "%") {
		return "", false
	}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 5 || parts[0] != "" || parts[1] != "v2" ||
		parts[2] != "projects" || parts[4] != "traces:batchWrite" ||
		!validProjectSegment(parts[3]) {
		return "", false
	}
	return parts[3], true
}

func validProjectSegment(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	if isDigit(value[0]) {
		for index := 1; index < len(value); index++ {
			if !isDigit(value[index]) {
				return false
			}
		}
		return true
	}
	if !isLowerLetter(value[0]) || !isLowerAlphaNumeric(value[len(value)-1]) {
		return false
	}
	for index := 1; index < len(value)-1; index++ {
		if !isLowerAlphaNumeric(value[index]) && value[index] != '-' {
			return false
		}
	}
	return true
}

func isLowerAlphaNumeric(value byte) bool {
	return isLowerLetter(value) || isDigit(value)
}

func isLowerLetter(value byte) bool {
	return value >= 'a' && value <= 'z'
}

func isDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

func isHexID(value string, length int) bool {
	if len(value) != length {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
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
