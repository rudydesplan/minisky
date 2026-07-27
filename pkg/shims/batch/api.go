package batch

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	registry.Register("batch.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (GCP Batch v1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Job represents a google.cloud.batch.v1.Job resource.
type Job struct {
	Name             string            `json:"name"`
	UID              string            `json:"uid,omitempty"`
	CreateTime       string            `json:"createTime,omitempty"`
	UpdateTime       string            `json:"updateTime,omitempty"`
	TaskGroups       []TaskGroup       `json:"taskGroups"`
	Status           *JobStatus        `json:"status"`
	Labels           map[string]string `json:"labels,omitempty"`
	AllocationPolicy json.RawMessage   `json:"allocationPolicy,omitempty"`
	LogsPolicy       *LogsPolicy       `json:"logsPolicy,omitempty"`
	Notifications    []json.RawMessage `json:"notifications,omitempty"`
}

// JobStatus represents the status of a Batch job.
type JobStatus struct {
	State        string            `json:"state"`
	StatusEvents []json.RawMessage `json:"statusEvents,omitempty"`
}

// TaskGroup represents a group of tasks within a job.
type TaskGroup struct {
	TaskSpec    *TaskSpec `json:"taskSpec,omitempty"`
	TaskCount   string    `json:"taskCount,omitempty"`
	Parallelism string    `json:"parallelism,omitempty"`
}

// TaskSpec describes the specification for tasks in a group.
type TaskSpec struct {
	Runnables []Runnable `json:"runnables,omitempty"`
}

// Runnable describes a single runnable within a task.
type Runnable struct {
	Container *Container `json:"container,omitempty"`
}

// Container describes a container runnable.
type Container struct {
	ImageURI string `json:"imageUri,omitempty"`
}

// LogsPolicy describes the logging configuration.
type LogsPolicy struct {
	Destination string `json:"destination,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Batch v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	stateStore batchStateStore
	jobs       map[string]*Job
}

// NewAPI creates a new Batch API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:      opMgr,
		stateStore: state.NewGuardedEntryStore(store, err),
		jobs:       make(map[string]*Job),
	}
	if err != nil {
		log.Printf("[Shim: Batch] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Batch] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr: orchestrator.NewOperationManager(),
		jobs:  make(map[string]*Job),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Batch] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.Contains(r.URL.Path, "/jobs/") && strings.HasSuffix(r.URL.Path, ":cancel") && r.Method == http.MethodPost:
		api.cancelJob(w, r)
	case strings.HasSuffix(r.URL.Path, "/jobs") && r.Method == http.MethodPost:
		api.createJob(w, r)
	case strings.HasSuffix(r.URL.Path, "/jobs") && r.Method == http.MethodGet:
		api.listJobs(w, r)
	case strings.Contains(r.URL.Path, "/jobs/") && r.Method == http.MethodGet:
		api.getJob(w, r)
	case strings.Contains(r.URL.Path, "/jobs/") && r.Method == http.MethodPatch:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Batch jobs are immutable; update is not supported")
	case strings.Contains(r.URL.Path, "/jobs/") && r.Method == http.MethodPut:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Batch jobs are immutable; update is not supported")
	case strings.Contains(r.URL.Path, "/jobs/") && r.Method == http.MethodDelete:
		api.deleteJob(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Batch resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	jobID := r.URL.Query().Get("jobId")

	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if len(job.TaskGroups) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "taskGroups is required and must not be empty")
		return
	}
	for _, group := range job.TaskGroups {
		if group.TaskSpec != nil && len(group.TaskSpec.Runnables) > 0 {
			writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Batch runnable execution is not supported by the current backend")
			return
		}
	}
	if jobID == "" {
		jobID = "job-" + generateUUID()
	}

	name := fmt.Sprintf("projects/%s/locations/%s/jobs/%s", project, location, jobID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	job.Name = name
	job.UID = generateUUID()
	job.CreateTime = now
	job.UpdateTime = now
	job.Status = &JobStatus{State: "QUEUED", StatusEvents: []json.RawMessage{}}

	api.mu.Lock()
	if _, exists := api.jobs[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "job already exists: "+jobID)
		return
	}
	api.jobs[name] = &job
	clone := deepCopyJob(&job)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.jobs, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	// Create returns the Job directly (synchronous, NOT LRO)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) getJob(w http.ResponseWriter, r *http.Request) {
	name := parseJobName(r.URL.Path)

	api.mu.RLock()
	job, ok := api.jobs[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found: "+name)
		return
	}
	clone := deepCopyJob(job)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) cancelJob(w http.ResponseWriter, r *http.Request) {
	name := parseJobName(strings.TrimSuffix(r.URL.Path, ":cancel"))
	api.mu.Lock()
	job := api.jobs[name]
	if job == nil {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found: "+name)
		return
	}
	old := deepCopyJob(job)
	if job.Status == nil {
		job.Status = &JobStatus{}
	}
	job.Status.State = "CANCELLED"
	job.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	cancelled := deepCopyJob(job)
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("batch#operation", "cancel", name)
	if err != nil {
		api.mu.Lock()
		api.jobs[name] = old
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.jobs[name] = old
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	response, _ := json.Marshal(cancelled)
	_ = api.opMgr.FinalizeScopedDurable(op.Name, response, 0, "")
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

func (api *API) listJobs(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/jobs/", project, location)

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
	all := make([]*Job, 0)
	for key, job := range api.jobs {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyJob(job))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "batch.googleapis.com",
		Parent:  fmt.Sprintf("projects/%s/locations/%s", project, location),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(job *Job) string { return job.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = make([]*Job, 0)
	}

	resp := map[string]any{
		"jobs":          result,
		"nextPageToken": nextToken,
		"unreachable":   []string{},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (api *API) deleteJob(w http.ResponseWriter, r *http.Request) {
	name := parseJobName(r.URL.Path)
	api.mu.Lock()
	job, exists := api.jobs[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found: "+name)
		return
	}
	delete(api.jobs, name)
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("batch#operation", "delete", name)
	if err != nil {
		api.mu.Lock()
		api.jobs[name] = job
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.jobs[name] = job
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = api.opMgr.FinalizeScopedDurable(op.Name, json.RawMessage(`{}`), 0, "")

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "batch#operation")
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// parseParent extracts project and location from a path like
// /v1/projects/{project}/locations/{location}/...
func parseParent(path string) (project, location string, ok bool) {
	project = extractAfter(path, "projects")
	location = extractAfter(path, "locations")
	if project == "" || location == "" {
		return "", "", false
	}
	return project, location, true
}

// parseJobName reconstructs the full resource name from the URL path.
func parseJobName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	jobID := extractAfter(path, "jobs")
	return fmt.Sprintf("projects/%s/locations/%s/jobs/%s", project, location, jobID)
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

// generateUUID produces a v4-style UUID using crypto/rand.
func generateUUID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
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
