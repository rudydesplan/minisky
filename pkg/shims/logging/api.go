package logging

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const (
	loggingStateEntry = "logging/metadata"
	sinkLoopLabel     = "minisky.logging/sink-delivery"
)

func init() {
	registry.Register("logging.googleapis.com", func(ctx *registry.Context) http.Handler {
		pubsub := ctx.GetShim("pubsub.googleapis.com")
		return NewAPIWithDelivery(newLocalSinkDeliverer(config.GetRuntimeDir(), pubsub))
	})
}

type LogEntry struct {
	InsertId    string             `json:"insertId"`
	Timestamp   string             `json:"timestamp"`
	Severity    string             `json:"severity"`
	TextPayload string             `json:"textPayload,omitempty"`
	JsonPayload interface{}        `json:"jsonPayload,omitempty"`
	Resource    *MonitoredResource `json:"resource,omitempty"`
	LogName     string             `json:"logName"`
	Labels      map[string]string  `json:"labels,omitempty"`
}

type MonitoredResource struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels,omitempty"`
}

type LogSink struct {
	Name        string `json:"name"`
	Destination string `json:"destination"`
	Filter      string `json:"filter,omitempty"`
	Description string `json:"description,omitempty"`
}

type sinkDeliverer interface {
	Deliver(LogSink, LogEntry) error
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type loggingMetadata struct {
	Entries []LogEntry         `json:"entries"`
	Sinks   map[string]LogSink `json:"sinks"`
}

type API struct {
	mu        sync.RWMutex
	persistMu sync.Mutex
	entries   []LogEntry
	sinks     map[string]LogSink
	maxSize   int
	store     stateStore
	deliverer sinkDeliverer
	initErr   error
}

func NewAPI() *API {
	return NewAPIWithDelivery(newLocalSinkDeliverer(config.GetRuntimeDir(), nil))
}

func NewAPIWithDelivery(deliverer sinkDeliverer) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Logging] state disabled: %v", err)
		return newAPI(nil, deliverer)
	}
	legacy := filepath.Join(config.GetMiniskyDir(), "cloud_logs.json")
	api, err := NewAPIWithStore(store, legacy, deliverer)
	if err != nil {
		log.Printf("[Shim: Logging] state rehydration failed: %v", err)
		disabled := newAPI(nil, deliverer)
		disabled.initErr = err
		return disabled
	}
	return api
}

func NewAPIWithStore(store stateStore, legacyPath string, deliverer sinkDeliverer) (*API, error) {
	api := newAPI(store, deliverer)
	if store != nil {
		var saved loggingMetadata
		if err := store.Load(loggingStateEntry, &saved); err == nil {
			api.entries = append([]LogEntry(nil), saved.Entries...)
			if saved.Sinks != nil {
				api.sinks = saved.Sinks
			}
			return api, nil
		} else if !errors.Is(err, state.ErrNotFound) {
			return nil, fmt.Errorf("load Logging metadata: %w", err)
		}
	}

	if legacyPath == "" {
		return api, nil
	}
	legacy, err := os.Open(legacyPath)
	if errors.Is(err, os.ErrNotExist) {
		return api, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open legacy Logging state: %w", err)
	}
	defer legacy.Close()
	if err := json.NewDecoder(io.LimitReader(legacy, 16<<20)).Decode(&api.entries); err != nil {
		return nil, fmt.Errorf("decode legacy Logging state: %w", err)
	}
	if err := api.persist(); err != nil {
		return nil, fmt.Errorf("persist migrated Logging state: %w", err)
	}
	if err := os.Rename(legacyPath, legacyPath+".migrated"); err != nil {
		return nil, fmt.Errorf("mark legacy Logging state migrated: %w", err)
	}
	return api, nil
}

func newAPI(store stateStore, deliverer sinkDeliverer) *API {
	return &API{
		entries:   make([]LogEntry, 0),
		sinks:     make(map[string]LogSink),
		maxSize:   5000,
		store:     store,
		deliverer: deliverer,
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Logging] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initErr != nil {
		writeError(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION", "Logging state is unavailable")
		return
	}

	switch {
	case strings.Contains(r.URL.Path, "/alertPolicies") || strings.Contains(r.URL.Path, "/metrics"):
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Logging alerting and log-based metrics are not implemented")
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/entries:write"):
		api.handleWrite(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/entries:list"):
		api.handleList(w, r)
	case strings.Contains(r.URL.Path, "/sinks"):
		api.handleSinks(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/internal/logs"):
		api.handleInternalLogs(w)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Logging resource not found")
	}
}

func (api *API) handleWrite(w http.ResponseWriter, r *http.Request) {
	var body struct {
		LogName  string             `json:"logName"`
		Resource *MonitoredResource `json:"resource"`
		Labels   map[string]string  `json:"labels"`
		Entries  []LogEntry         `json:"entries"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid entries.write JSON")
		return
	}
	if len(body.Entries) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "entries must contain at least one log entry")
		return
	}

	now := time.Now().UTC()
	entries := make([]LogEntry, len(body.Entries))
	copy(entries, body.Entries)
	for i := range entries {
		if entries[i].Timestamp == "" {
			entries[i].Timestamp = now.Format(time.RFC3339Nano)
		}
		if entries[i].InsertId == "" {
			entries[i].InsertId = fmt.Sprintf("%d-%d", now.UnixNano(), i)
		}
		if entries[i].LogName == "" {
			entries[i].LogName = body.LogName
		}
		if entries[i].Resource == nil {
			entries[i].Resource = body.Resource
		}
		if entries[i].Labels == nil && body.Labels != nil {
			entries[i].Labels = cloneLabels(body.Labels)
		}
		if entries[i].LogName == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "logName is required")
			return
		}
	}

	sinks := make([]LogSink, 0)
	if err := api.commitMutation(func() error {
		api.entries = append(api.entries, entries...)
		if len(api.entries) > api.maxSize {
			api.entries = api.entries[len(api.entries)-api.maxSize:]
		}
		for _, sink := range api.sinks {
			sinks = append(sinks, sink)
		}
		return nil
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist log entries")
		return
	}

	if api.deliverer != nil {
		for _, entry := range entries {
			if entry.Labels[sinkLoopLabel] == "true" {
				continue
			}
			for _, sink := range sinks {
				matches, _ := matchesFilter(entry, sink.Filter)
				if matches {
					if err := api.deliverer.Deliver(sink, entry); err != nil {
						log.Printf("[Shim: Logging] sink %s delivery failed: %v", sink.Name, err)
					}
				}
			}
		}
	}
	_, _ = w.Write([]byte("{}"))
}

func (api *API) handleList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ResourceNames []string `json:"resourceNames"`
		Filter        string   `json:"filter"`
		OrderBy       string   `json:"orderBy"`
		PageSize      int      `json:"pageSize"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid entries.list JSON")
		return
	}
	if _, err := compileFilter(body.Filter); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if body.PageSize < 0 || body.PageSize > 1000 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageSize must be between 0 and 1000")
		return
	}

	api.mu.RLock()
	entries := make([]LogEntry, 0, len(api.entries))
	for _, entry := range api.entries {
		if !matchesResourceNames(entry, body.ResourceNames) {
			continue
		}
		matches, _ := matchesFilter(entry, body.Filter)
		if matches {
			entries = append(entries, entry)
		}
	}
	api.mu.RUnlock()
	sort.SliceStable(entries, func(i, j int) bool {
		if strings.EqualFold(strings.TrimSpace(body.OrderBy), "timestamp asc") {
			return entries[i].Timestamp < entries[j].Timestamp
		}
		return entries[i].Timestamp > entries[j].Timestamp
	})
	if body.PageSize > 0 && len(entries) > body.PageSize {
		entries = entries[:body.PageSize]
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
}

func (api *API) handleSinks(w http.ResponseWriter, r *http.Request) {
	project := extractProject(r.URL.Path)
	name := extractAfter(r.URL.Path, "sinks")
	key := project + ":" + name
	switch r.Method {
	case http.MethodPost:
		if name != "" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var sink LogSink
		if err := decodeJSON(r, &sink); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid sink JSON")
			return
		}
		if err := validateSink(sink); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		key = project + ":" + sink.Name
		err := api.commitMutation(func() error {
			if _, exists := api.sinks[key]; exists {
				return errLoggingAlreadyExists
			}
			api.sinks[key] = sink
			return nil
		})
		if errors.Is(err, errLoggingAlreadyExists) {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "sink already exists: "+sink.Name)
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist sink")
			return
		}
		_ = json.NewEncoder(w).Encode(sink)
	case http.MethodGet:
		if name != "" {
			api.mu.RLock()
			sink, ok := api.sinks[key]
			api.mu.RUnlock()
			if !ok {
				writeError(w, http.StatusNotFound, "NOT_FOUND", "sink not found: "+name)
				return
			}
			_ = json.NewEncoder(w).Encode(sink)
			return
		}
		api.mu.RLock()
		sinks := make([]LogSink, 0)
		for sinkKey, sink := range api.sinks {
			if strings.HasPrefix(sinkKey, project+":") {
				sinks = append(sinks, sink)
			}
		}
		api.mu.RUnlock()
		sort.Slice(sinks, func(i, j int) bool { return sinks[i].Name < sinks[j].Name })
		_ = json.NewEncoder(w).Encode(map[string]any{"sinks": sinks})
	case http.MethodDelete:
		err := api.commitMutation(func() error {
			if _, ok := api.sinks[key]; !ok {
				return errLoggingNotFound
			}
			delete(api.sinks, key)
			return nil
		})
		if errors.Is(err, errLoggingNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "sink not found: "+name)
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist sink deletion")
			return
		}
		_, _ = w.Write([]byte("{}"))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) handleInternalLogs(w http.ResponseWriter) {
	_ = json.NewEncoder(w).Encode(api.GetEntries())
}

func (api *API) PushLog(projectID, severity, resourceType, resourceName, text string) {
	if projectID == "" {
		projectID = "default-project"
	}
	entry := LogEntry{
		InsertId:    fmt.Sprintf("%d", time.Now().UnixNano()),
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
		Severity:    severity,
		TextPayload: text,
		LogName:     fmt.Sprintf("projects/%s/logs/%s", projectID, resourceType),
		Resource: &MonitoredResource{
			Type:   resourceType,
			Labels: map[string]string{"name": resourceName},
		},
	}
	if err := api.commitMutation(func() error {
		api.entries = append(api.entries, entry)
		if len(api.entries) > api.maxSize {
			api.entries = api.entries[len(api.entries)-api.maxSize:]
		}
		return nil
	}); err != nil {
		log.Printf("[Shim: Logging] internal log persistence failed: %v", err)
	}
}

func (api *API) GetEntries() []LogEntry {
	api.mu.RLock()
	defer api.mu.RUnlock()
	out := make([]LogEntry, len(api.entries))
	copy(out, api.entries)
	return out
}

func (api *API) Reset() {
	if err := api.commitMutation(func() error {
		api.entries = make([]LogEntry, 0)
		return nil
	}); err != nil {
		log.Printf("[Shim: Logging] reset persistence failed: %v", err)
	}
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if local, ok := api.deliverer.(*localSinkDeliverer); ok {
		local.pubsub = ctx.GetShim("pubsub.googleapis.com")
	}
	api.StartHarvester(ctx.SvcMgr)
}

func (api *API) StartHarvester(sm *orchestrator.ServiceManager) {
	if sm == nil || api.initErr != nil {
		return
	}
	log.Printf("[Logging] starting background log harvester")
	go func() {
		lastSeen := make(map[string]int64)
		for {
			for _, container := range sm.ListManagedContainers() {
				since := lastSeen[container.Name]
				if since == 0 {
					since = time.Now().Add(-time.Hour).Unix()
				}
				output, err := sm.GetContainerLogsSince(container.Name, since)
				lastSeen[container.Name] = time.Now().Unix()
				if err != nil || output == "" {
					continue
				}
				for _, line := range strings.Split(output, "\n") {
					line = strings.TrimSpace(line)
					if line != "" && line != "Log source not found." {
						api.PushLog("default-project", inferSeverity(line), "container",
							strings.TrimPrefix(container.Name, "minisky-"), line)
					}
				}
			}
			time.Sleep(3 * time.Second)
		}
	}()
}

func (api *API) ListProjects() []string {
	projects := make(map[string]struct{})
	for _, entry := range api.GetEntries() {
		parts := strings.Split(entry.LogName, "/")
		if len(parts) > 1 && parts[0] == "projects" {
			projects[parts[1]] = struct{}{}
		}
	}
	result := make([]string, 0, len(projects))
	for project := range projects {
		result = append(result, project)
	}
	sort.Strings(result)
	return result
}

func (api *API) persist() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	payload, err := json.Marshal(loggingMetadata{Entries: api.entries, Sinks: api.sinks})
	api.mu.RUnlock()
	if err != nil {
		return err
	}
	return api.store.Save(loggingStateEntry, json.RawMessage(payload))
}

var (
	errLoggingAlreadyExists = errors.New("logging resource already exists")
	errLoggingNotFound      = errors.New("logging resource not found")
)

func (api *API) commitMutation(mutate func() error) error {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.Lock()
	previousEntries := append([]LogEntry(nil), api.entries...)
	previousSinks := make(map[string]LogSink, len(api.sinks))
	for key, sink := range api.sinks {
		previousSinks[key] = sink
	}
	if err := mutate(); err != nil {
		api.mu.Unlock()
		return err
	}
	payload, err := json.Marshal(loggingMetadata{Entries: api.entries, Sinks: api.sinks})
	api.mu.Unlock()
	if err == nil && api.store != nil {
		err = api.store.Save(loggingStateEntry, json.RawMessage(payload))
	}
	if err != nil {
		api.mu.Lock()
		api.entries = previousEntries
		api.sinks = previousSinks
		api.mu.Unlock()
	}
	return err
}

type localSinkDeliverer struct {
	runtimeDir string
	pubsub     http.Handler
}

func newLocalSinkDeliverer(runtimeDir string, pubsub http.Handler) sinkDeliverer {
	return &localSinkDeliverer{runtimeDir: runtimeDir, pubsub: pubsub}
}

func (d *localSinkDeliverer) Deliver(sink LogSink, entry LogEntry) error {
	switch {
	case strings.HasPrefix(sink.Destination, "file://"):
		name := strings.TrimPrefix(sink.Destination, "file://")
		if !validIdentifier(name) {
			return fmt.Errorf("invalid file sink name")
		}
		dir := filepath.Join(d.runtimeDir, "logging", "sinks")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		file, err := os.OpenFile(filepath.Join(dir, name+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		defer file.Close()
		return json.NewEncoder(file).Encode(entry)
	case strings.HasPrefix(sink.Destination, "pubsub.googleapis.com/projects/"):
		if d.pubsub == nil {
			return fmt.Errorf("Pub/Sub backend is unavailable")
		}
		topic := strings.TrimPrefix(sink.Destination, "pubsub.googleapis.com/")
		entry.Labels = cloneLabels(entry.Labels)
		entry.Labels[sinkLoopLabel] = "true"
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		body, _ := json.Marshal(map[string]any{"messages": []map[string]string{
			{"data": base64.StdEncoding.EncodeToString(data)},
		}})
		request := httptestRequest(http.MethodPost, "/v1/"+topic+":publish", body)
		response := &captureResponse{header: make(http.Header)}
		d.pubsub.ServeHTTP(response, request)
		if response.status >= 300 {
			return fmt.Errorf("Pub/Sub sink returned HTTP %d", response.status)
		}
		return nil
	default:
		return fmt.Errorf("unsupported sink destination")
	}
}

type captureResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *captureResponse) Header() http.Header  { return r.header }
func (r *captureResponse) WriteHeader(code int) { r.status = code }
func (r *captureResponse) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(data)
}

func httptestRequest(method, path string, body []byte) *http.Request {
	request, _ := http.NewRequest(method, "http://logging-sink.local"+path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

type entryPredicate func(LogEntry) bool

func compileFilter(filter string) ([]entryPredicate, error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return nil, nil
	}
	clauses := strings.Split(filter, " AND ")
	predicates := make([]entryPredicate, 0, len(clauses))
	for _, raw := range clauses {
		clause := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(clause, "severity>="):
			value := strings.Trim(strings.TrimSpace(strings.TrimPrefix(clause, "severity>=")), `"`)
			minimum, ok := severityRank[strings.ToUpper(value)]
			if !ok {
				return nil, fmt.Errorf("unsupported severity filter value %q", value)
			}
			predicates = append(predicates, func(entry LogEntry) bool {
				return severityRank[strings.ToUpper(entry.Severity)] >= minimum
			})
		case strings.HasPrefix(clause, "logName="):
			value := strings.Trim(strings.TrimSpace(strings.TrimPrefix(clause, "logName=")), `"`)
			predicates = append(predicates, func(entry LogEntry) bool { return entry.LogName == value })
		case strings.HasPrefix(clause, "resource.type="):
			value := strings.Trim(strings.TrimSpace(strings.TrimPrefix(clause, "resource.type=")), `"`)
			predicates = append(predicates, func(entry LogEntry) bool {
				return entry.Resource != nil && entry.Resource.Type == value
			})
		default:
			return nil, fmt.Errorf("unsupported Logging filter clause %q", clause)
		}
	}
	return predicates, nil
}

func matchesFilter(entry LogEntry, filter string) (bool, error) {
	predicates, err := compileFilter(filter)
	if err != nil {
		return false, err
	}
	for _, predicate := range predicates {
		if !predicate(entry) {
			return false, nil
		}
	}
	return true, nil
}

var severityRank = map[string]int{
	"DEFAULT": 0, "DEBUG": 100, "INFO": 200, "NOTICE": 300,
	"WARNING": 400, "ERROR": 500, "CRITICAL": 600, "ALERT": 700, "EMERGENCY": 800,
}

func matchesResourceNames(entry LogEntry, names []string) bool {
	if len(names) == 0 {
		return true
	}
	for _, name := range names {
		if strings.HasPrefix(entry.LogName, strings.TrimSuffix(name, "/")+"/") {
			return true
		}
	}
	return false
}

func validateSink(sink LogSink) error {
	if !validIdentifier(sink.Name) {
		return fmt.Errorf("sink name is required and may contain only letters, digits, hyphens, and underscores")
	}
	if !strings.HasPrefix(sink.Destination, "file://") &&
		!strings.HasPrefix(sink.Destination, "pubsub.googleapis.com/projects/") {
		return fmt.Errorf("only file:// and pubsub.googleapis.com/projects/... destinations are supported")
	}
	if strings.HasPrefix(sink.Destination, "file://") &&
		!validIdentifier(strings.TrimPrefix(sink.Destination, "file://")) {
		return fmt.Errorf("file sink destination must be a safe relative name")
	}
	_, err := compileFilter(sink.Filter)
	return err
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 100 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

func extractProject(path string) string {
	return extractAfter(path, "projects")
}

func extractAfter(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func cloneLabels(labels map[string]string) map[string]string {
	clone := make(map[string]string, len(labels)+1)
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}

func inferSeverity(message string) string {
	upper := strings.ToUpper(message)
	if strings.Contains(upper, "ERROR") || strings.Contains(upper, "FAILED") {
		return "ERROR"
	}
	return "INFO"
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "status": status, "message": message},
	})
}
