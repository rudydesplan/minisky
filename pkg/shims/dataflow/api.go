package dataflow

import (
	"context"
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

func init() {
	registry.Register("dataflow.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (GCP Dataflow v1b3)
// ─────────────────────────────────────────────────────────────────────────────

// Job represents a Dataflow job resource.
// - `id` is server-generated (numeric string).
// - `name` is a user-provided display name (NOT a resource path).
// - Legacy jobs can be read and durably cancelled or drained.
type Job struct {
	ID               string            `json:"id"`
	Name             string            `json:"name,omitempty"`
	ProjectID        string            `json:"projectId"`
	Type             string            `json:"type,omitempty"`
	CurrentState     string            `json:"currentState"`
	CurrentStateTime string            `json:"currentStateTime,omitempty"`
	CreateTime       string            `json:"createTime"`
	Environment      *Environment      `json:"environment,omitempty"`
	Location         string            `json:"location"`
	Labels           map[string]string `json:"labels,omitempty"`
	RequestedState   string            `json:"requestedState,omitempty"`
	Steps            []Step            `json:"steps,omitempty"`
}

type Step struct {
	Name       string         `json:"name"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties,omitempty"`
}

// Environment holds job environment configuration.
type Environment struct {
	TempStoragePrefix string `json:"tempStoragePrefix,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Dataflow v1b3 REST surface.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	stateStore dataflowStateStore
	jobs       map[string]*Job // keyed by job ID
	nextID     int64
	runner     jobRunner
	cancels    map[string]context.CancelFunc
}

type jobRunner interface {
	Run(context.Context, *Job) error
}

type boundedLocalRunner struct{}

// NewAPI creates a Dataflow shim with durable state.
func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		stateStore: state.NewGuardedEntryStore(store, err),
		jobs:       make(map[string]*Job),
		nextID:     1,
		runner:     boundedLocalRunner{},
		cancels:    make(map[string]context.CancelFunc),
	}
	if err != nil {
		log.Printf("[Shim: Dataflow] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Dataflow] state rehydration failed: %v", err)
	}
	// Derive nextID from existing jobs.
	for _, j := range api.jobs {
		if n, err := strconv.ParseInt(j.ID, 10, 64); err == nil && n >= api.nextID {
			api.nextID = n + 1
		}
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence, no Docker).
func newTestAPI() *API {
	return &API{
		jobs:    make(map[string]*Job),
		nextID:  1,
		runner:  boundedLocalRunner{},
		cancels: make(map[string]context.CancelFunc),
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Dataflow] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case matchJobsCollection(r.URL.Path) && r.Method == http.MethodPost:
		api.createJob(w, r)
	case matchJobsCollection(r.URL.Path) && r.Method == http.MethodGet:
		api.listJobs(w, r)
	case matchSingleJob(r.URL.Path) && r.Method == http.MethodGet:
		api.getJob(w, r)
	case matchSingleJob(r.URL.Path) && r.Method == http.MethodPut:
		api.cancelJob(w, r)
	case matchSingleJob(r.URL.Path) && r.Method == http.MethodDelete:
		api.deleteNotAllowed(w, r)
	case matchJobsCollection(r.URL.Path) && r.Method == http.MethodDelete:
		api.deleteNotAllowed(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Dataflow resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if job.Name == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "name is required")
		return
	}
	if err := validateBoundedJob(&job); err != nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", err.Error())
		return
	}
	project, location := projectLocation(r.URL.Path)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	api.mu.Lock()
	for _, existing := range api.jobs {
		if existing.ProjectID == project && existing.Location == location && existing.Name == job.Name {
			api.mu.Unlock()
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "job already exists: "+job.Name)
			return
		}
	}
	job.ID = strconv.FormatInt(api.nextID, 10)
	api.nextID++
	job.ProjectID = project
	job.Location = location
	job.CurrentState = "JOB_STATE_PENDING"
	job.CreateTime = now
	job.CurrentStateTime = now
	stored := deepCopyJob(&job)
	api.jobs[job.ID] = stored
	ctx, cancel := context.WithCancel(context.Background())
	api.cancels[job.ID] = cancel
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		cancel()
		api.mu.Lock()
		delete(api.jobs, job.ID)
		delete(api.cancels, job.ID)
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(deepCopyJob(stored))
	go api.runJob(ctx, job.ID)
}

func validateBoundedJob(job *Job) error {
	if job.Type != "JOB_TYPE_BATCH" {
		return fmt.Errorf("only bounded JOB_TYPE_BATCH jobs are supported by the local runner")
	}
	if len(job.Steps) == 0 {
		return fmt.Errorf("the local runner requires at least one executable step")
	}
	for _, step := range job.Steps {
		if step.Name == "" || (step.Kind != "Create" && step.Kind != "Count") {
			return fmt.Errorf("the local runner supports only named Create and Count steps")
		}
		if step.Kind == "Create" {
			if _, ok := step.Properties["elements"].([]any); !ok {
				return fmt.Errorf("Create step %q requires an elements array", step.Name)
			}
		}
	}
	return nil
}

func (boundedLocalRunner) Run(ctx context.Context, job *Job) error {
	var elements []any
	for _, step := range job.Steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch step.Kind {
		case "Create":
			elements = append([]any(nil), step.Properties["elements"].([]any)...)
		case "Count":
			_ = len(elements)
		}
	}
	return nil
}

func (api *API) runJob(ctx context.Context, id string) {
	api.setExecutionState(id, "JOB_STATE_RUNNING")
	err := api.runner.Run(ctx, api.jobSnapshot(id))
	api.mu.Lock()
	job := api.jobs[id]
	delete(api.cancels, id)
	if job == nil || job.CurrentState == "JOB_STATE_CANCELLED" || job.CurrentState == "JOB_STATE_DRAINED" {
		api.mu.Unlock()
		return
	}
	if err != nil {
		job.CurrentState = "JOB_STATE_FAILED"
	} else {
		job.CurrentState = "JOB_STATE_DONE"
	}
	job.CurrentStateTime = time.Now().UTC().Format(time.RFC3339Nano)
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		log.Printf("[Shim: Dataflow] persist terminal job %s: %v", id, err)
	}
}

func (api *API) setExecutionState(id, state string) {
	api.mu.Lock()
	if job := api.jobs[id]; job != nil {
		job.CurrentState = state
		job.CurrentStateTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		log.Printf("[Shim: Dataflow] persist job %s transition: %v", id, err)
	}
}

func (api *API) jobSnapshot(id string) *Job {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return deepCopyJob(api.jobs[id])
}

func (api *API) getJob(w http.ResponseWriter, r *http.Request) {
	jobID := extractJobID(r.URL.Path)

	api.mu.RLock()
	job, ok := api.jobs[jobID]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("job not found: %s", jobID))
		return
	}
	clone := deepCopyJob(job)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listJobs(w http.ResponseWriter, r *http.Request) {
	project, location := projectLocation(r.URL.Path)

	pageSize := 50
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	pageToken := r.URL.Query().Get("pageToken")

	api.mu.RLock()
	var all []*Job
	for _, job := range api.jobs {
		if job.ProjectID == project && job.Location == location {
			all = append(all, deepCopyJob(job))
		}
	}
	api.mu.RUnlock()

	page, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "dataflow.jobs",
		Parent:  fmt.Sprintf("projects/%s/locations/%s", project, location),
		Filter:  r.URL.Query().Get("filter"),
	}, func(job *Job) string {
		id, _ := strconv.ParseUint(job.ID, 10, 64)
		return fmt.Sprintf("%020d", id)
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if page == nil {
		page = []*Job{}
	}

	resp := map[string]any{"jobs": page, "nextPageToken": nextToken}
	_ = json.NewEncoder(w).Encode(resp)
}

// cancelJob handles PUT /jobs/{jobId} — used to cancel or drain a job.
func (api *API) cancelJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	jobID := extractJobID(r.URL.Path)

	var req struct {
		RequestedState string `json:"requestedState"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if req.RequestedState != "JOB_STATE_CANCELLED" && req.RequestedState != "JOB_STATE_DRAINED" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"requestedState must be JOB_STATE_CANCELLED or JOB_STATE_DRAINED")
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)

	api.mu.Lock()
	job, ok := api.jobs[jobID]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("job not found: %s", jobID))
		return
	}
	previous := deepCopyJob(job)

	// Apply the requested state transition.
	job.CurrentState = req.RequestedState
	job.CurrentStateTime = now
	job.RequestedState = req.RequestedState
	clone := deepCopyJob(job)
	cancel := api.cancels[jobID]
	delete(api.cancels, jobID)
	api.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		if api.jobs[jobID] == job {
			api.jobs[jobID] = previous
		}
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(clone)
}

// deleteNotAllowed returns 405 — Dataflow has no DELETE method.
func (api *API) deleteNotAllowed(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
		"Dataflow API does not support DELETE. Use PUT with requestedState to cancel.")
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// matchJobsCollection returns true for paths ending in /jobs (no trailing segment).
func matchJobsCollection(path string) bool {
	return strings.HasSuffix(path, "/jobs")
}

// matchSingleJob returns true for paths like .../jobs/{id}.
func matchSingleJob(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return false
	}
	return parts[len(parts)-2] == "jobs" && parts[len(parts)-1] != ""
}

func extractJobID(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-1]
}

func projectLocation(path string) (string, string) {
	return extractAfter(path, "projects"), extractAfter(path, "locations")
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
