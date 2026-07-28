package errorreporting

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const maxRetainedEventsPerGroup = 100

func init() {
	registry.Register("clouderrorreporting.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (GCP Error Reporting v1beta1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// ErrorEvent represents a reported error event.
type ErrorEvent struct {
	EventTime      string          `json:"eventTime,omitempty"`
	ServiceContext *ServiceContext `json:"serviceContext,omitempty"`
	Message        string          `json:"message"`
	Context        *ErrorContext   `json:"context,omitempty"`
}

// ServiceContext identifies the service reporting the error.
type ServiceContext struct {
	Service string `json:"service"`
	Version string `json:"version,omitempty"`
}

// ErrorContext provides additional context about the error.
type ErrorContext struct {
	HTTPRequest    *HTTPRequestContext `json:"httpRequest,omitempty"`
	ReportLocation *ReportLocation     `json:"reportLocation,omitempty"`
}

// HTTPRequestContext holds HTTP request details.
type HTTPRequestContext struct {
	Method             string `json:"method,omitempty"`
	URL                string `json:"url,omitempty"`
	UserAgent          string `json:"userAgent,omitempty"`
	Referrer           string `json:"referrer,omitempty"`
	ResponseStatusCode int    `json:"responseStatusCode,omitempty"`
	RemoteIP           string `json:"remoteIp,omitempty"`
}

// ReportLocation identifies the source location of the error.
type ReportLocation struct {
	FilePath     string `json:"filePath,omitempty"`
	LineNumber   int    `json:"lineNumber,omitempty"`
	FunctionName string `json:"functionName,omitempty"`
}

// ErrorGroupStats represents aggregated error group statistics.
type ErrorGroupStats struct {
	Group              *ErrorGroupInfo `json:"group"`
	Count              string          `json:"count"`
	AffectedUsersCount string          `json:"affectedUsersCount,omitempty"`
	FirstSeenTime      string          `json:"firstSeenTime"`
	LastSeenTime       string          `json:"lastSeenTime"`
	Representative     *ErrorEvent     `json:"representative,omitempty"`
}

// ErrorGroupInfo identifies an error group.
type ErrorGroupInfo struct {
	GroupId string `json:"groupId"`
	Name    string `json:"name"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Error Reporting v1beta1 REST shim.
type API struct {
	mu             sync.RWMutex
	persistMu      sync.Mutex
	stateStore     errorreportingStateStore
	groups         map[string]*ErrorGroupStats // key: projectId:groupId
	events         map[string][]ErrorEvent     // key: projectId:groupId
	persistenceErr error
}

// NewAPI creates a new Error Reporting API shim with persistence.
func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		stateStore: state.NewGuardedEntryStore(store, err),
		groups:     make(map[string]*ErrorGroupStats),
		events:     make(map[string][]ErrorEvent),
	}
	if err != nil {
		api.persistenceErr = err
		log.Printf("[Shim: Error Reporting] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Error Reporting] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		groups: make(map[string]*ErrorGroupStats),
		events: make(map[string][]ErrorEvent),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Error Reporting] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path

	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/events:report"):
		api.reportEvent(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/groupStats"):
		api.listGroupStats(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/events"):
		api.listEvents(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found: "+path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

// reportEvent handles POST /v1beta1/projects/{projectId}/events:report
func (api *API) reportEvent(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project := extractProject(r.URL.Path)
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}

	var event ErrorEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if event.Message == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "message is required")
		return
	}

	if event.EventTime == "" {
		event.EventTime = time.Now().UTC().Format(time.RFC3339)
	}

	// Generate group ID from first line of message
	groupId := generateGroupId(event.Message)
	key := project + ":" + groupId

	api.persistMu.Lock()
	api.mu.Lock()
	if api.persistenceErr != nil {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Error Reporting persistence is unavailable")
		return
	}
	groupsBefore := make(map[string]*ErrorGroupStats, len(api.groups))
	for groupKey, group := range api.groups {
		groupsBefore[groupKey] = deepCopyGroupStats(group)
	}
	eventsBefore := make(map[string][]ErrorEvent, len(api.events))
	for eventKey, events := range api.events {
		copied := make([]ErrorEvent, len(events))
		for index, savedEvent := range events {
			copied[index] = deepCopyEvent(savedEvent)
		}
		eventsBefore[eventKey] = copied
	}
	group, ok := api.groups[key]
	if !ok {
		group = &ErrorGroupStats{
			Group: &ErrorGroupInfo{
				GroupId: groupId,
				Name:    fmt.Sprintf("projects/%s/groups/%s", project, groupId),
			},
			Count:         "0",
			FirstSeenTime: event.EventTime,
			Representative: &ErrorEvent{
				Message:        event.Message,
				ServiceContext: event.ServiceContext,
			},
		}
		api.groups[key] = group
	}
	// Increment count
	count, _ := strconv.ParseInt(group.Count, 10, 64)
	count++
	group.Count = strconv.FormatInt(count, 10)
	group.LastSeenTime = event.EventTime

	retained := append(api.events[key], event)
	if len(retained) > maxRetainedEventsPerGroup {
		retained = retained[len(retained)-maxRetainedEventsPerGroup:]
	}
	api.events[key] = retained
	api.mu.Unlock()

	if api.stateStore != nil {
		api.mu.RLock()
		groupSnapshot := make(map[string]*ErrorGroupStats, len(api.groups))
		for groupKey, savedGroup := range api.groups {
			groupSnapshot[groupKey] = deepCopyGroupStats(savedGroup)
		}
		eventSnapshot := make(map[string][]ErrorEvent, len(api.events))
		for eventKey, events := range api.events {
			copied := make([]ErrorEvent, len(events))
			for index, savedEvent := range events {
				copied[index] = deepCopyEvent(savedEvent)
			}
			eventSnapshot[eventKey] = copied
		}
		api.mu.RUnlock()
		if err := api.stateStore.Save(errorreportingStateEntry, errorreportingMetadata{Groups: groupSnapshot, Events: eventSnapshot}); err != nil {
			api.mu.Lock()
			api.groups = groupsBefore
			api.events = eventsBefore
			api.persistenceErr = fmt.Errorf("persist Error Reporting event: %w", err)
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
			return
		}
	}
	api.persistMu.Unlock()

	// events:report returns empty {} on success
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (api *API) PersistenceError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.persistenceErr
}

// listGroupStats handles GET /v1beta1/projects/{projectId}/groupStats
func (api *API) listGroupStats(w http.ResponseWriter, r *http.Request) {
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
	items := make([]*ErrorGroupStats, 0)
	for k, v := range api.groups {
		if strings.HasPrefix(k, prefix) {
			items = append(items, deepCopyGroupStats(v))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(items, pageSize, pageToken, pagination.Scope{
		Service: "errorreporting.groupStats",
		Parent:  "projects/" + project,
	}, func(group *ErrorGroupStats) string { return group.Group.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"errorGroupStats": result,
		"nextPageToken":   nextToken,
	})
}

// listEvents handles GET /v1beta1/projects/{projectId}/events
func (api *API) listEvents(w http.ResponseWriter, r *http.Request) {
	project := extractProject(r.URL.Path)
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}

	groupId := r.URL.Query().Get("groupId")

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
	items := make([]ErrorEvent, 0)
	if groupId != "" {
		// Filter by specific groupId
		key := project + ":" + groupId
		if events, ok := api.events[key]; ok {
			for _, e := range events {
				items = append(items, deepCopyEvent(e))
			}
		}
	} else {
		// All events for the project
		prefix := project + ":"
		for k, v := range api.events {
			if strings.HasPrefix(k, prefix) {
				for _, e := range v {
					items = append(items, deepCopyEvent(e))
				}
			}
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(items, pageSize, pageToken, pagination.Scope{
		Service: "errorreporting.events",
		Parent:  "projects/" + project,
		Filter:  "groupId=" + groupId,
	}, func(event ErrorEvent) string {
		return event.EventTime + "\x00" + generateGroupId(event.Message)
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"errorEvents":   result,
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

// generateGroupId produces a deterministic group ID from the first line of the message.
func generateGroupId(message string) string {
	firstLine := message
	if idx := strings.IndexByte(message, '\n'); idx >= 0 {
		firstLine = message[:idx]
	}
	h := sha256.Sum256([]byte(firstLine))
	return fmt.Sprintf("%x", h[:8])
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
