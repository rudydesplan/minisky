package batch

import (
	"context"
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
	ImageURI   string   `json:"imageUri,omitempty"`
	Entrypoint string   `json:"entrypoint,omitempty"`
	Commands   []string `json:"commands,omitempty"`
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
	runtimes   map[string]*batchRuntimeIntent
	runs       map[string]*batchRun
	runner     containerRunner
}

type batchRun struct {
	cancel context.CancelFunc
	done   chan struct{}
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
		runtimes:   make(map[string]*batchRuntimeIntent),
		runs:       make(map[string]*batchRun),
		runner:     dockerCLIRunner{},
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
		opMgr:    orchestrator.NewOperationManager(),
		jobs:     make(map[string]*Job),
		runtimes: make(map[string]*batchRuntimeIntent),
		runs:     make(map[string]*batchRun),
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
	workload, executable, err := executableWorkload(job.TaskGroups)
	if err != nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", err.Error())
		return
	}
	if executable {
		if api.runner == nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Batch Docker backend is unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		err = api.runner.Check(ctx)
		cancel()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", err.Error())
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
	var runtime *batchRuntimeIntent
	if executable {
		workload.Ownership = newContainerOwnership(config.GetProfile(), name, job.UID)
		runtime = &batchRuntimeIntent{Ownership: workload.Ownership, Workload: workload}
	}

	api.mu.Lock()
	if _, exists := api.jobs[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "job already exists: "+jobID)
		return
	}
	api.jobs[name] = &job
	if runtime != nil {
		api.runtimes[name] = runtime
	}
	clone := deepCopyJob(&job)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.jobs, name)
		delete(api.runtimes, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}
	if runtime != nil {
		api.startContainerJob(name, workload)
	}

	// Create returns the Job directly (synchronous, NOT LRO)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(clone)
}

func executableWorkload(groups []TaskGroup) (containerWorkload, bool, error) {
	hasRunnable := false
	for _, group := range groups {
		if group.TaskSpec != nil && len(group.TaskSpec.Runnables) != 0 {
			hasRunnable = true
			break
		}
	}
	if !hasRunnable {
		return containerWorkload{}, false, nil
	}
	if len(groups) != 1 {
		return containerWorkload{}, false, fmt.Errorf("bounded Batch execution supports exactly one task group")
	}
	group := groups[0]
	if group.TaskCount != "" && group.TaskCount != "1" {
		return containerWorkload{}, false, fmt.Errorf("bounded Batch execution supports taskCount 1")
	}
	if group.Parallelism != "" && group.Parallelism != "1" {
		return containerWorkload{}, false, fmt.Errorf("bounded Batch execution supports parallelism 1")
	}
	if group.TaskSpec == nil || len(group.TaskSpec.Runnables) != 1 {
		return containerWorkload{}, false, fmt.Errorf("bounded Batch execution supports exactly one runnable")
	}
	container := group.TaskSpec.Runnables[0].Container
	if container == nil || container.ImageURI == "" {
		return containerWorkload{}, false, fmt.Errorf("bounded Batch execution requires one container imageUri")
	}
	return containerWorkload{
		ImageURI: container.ImageURI, Entrypoint: container.Entrypoint,
		Commands: append([]string(nil), container.Commands...),
	}, true, nil
}

func (api *API) startContainerJob(name string, workload containerWorkload) {
	ctx, cancel := context.WithCancel(context.Background())
	run := &batchRun{cancel: cancel, done: make(chan struct{})}
	api.mu.Lock()
	api.runs[name] = run
	api.mu.Unlock()
	go api.executeContainerJob(ctx, name, workload, run)
}

func (api *API) executeContainerJob(ctx context.Context, name string, workload containerWorkload, run *batchRun) {
	defer close(run.done)
	defer func() {
		api.mu.Lock()
		delete(api.runs, name)
		api.mu.Unlock()
	}()

	api.setJobState(name, "RUNNING", "Batch container started")
	if err := api.persistState(); err != nil {
		api.opMgr.MarkPersistenceFailure(err)
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		cleanupErr := api.runner.Cleanup(cleanupCtx, workload.Ownership)
		cancelCleanup()
		terminalErr := fmt.Errorf("persist RUNNING state: %w", err)
		if cleanupErr != nil {
			terminalErr = fmt.Errorf("%v; container cleanup: %w", terminalErr, cleanupErr)
		}
		api.finishContainerJob(name, "FAILED", -1, "", terminalErr)
		if cleanupErr == nil {
			api.mu.Lock()
			delete(api.runtimes, name)
			api.mu.Unlock()
		}
		if terminalSaveErr := api.persistState(); terminalSaveErr != nil {
			api.opMgr.MarkPersistenceFailure(terminalSaveErr)
			log.Printf("[Batch] persist terminal state for %s: %v", name, terminalSaveErr)
		}
		return
	}

	result, runErr := api.runner.Run(ctx, workload)
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
	cleanupErr := api.runner.Cleanup(cleanupCtx, workload.Ownership)
	cancelCleanup()

	api.mu.Lock()
	job := api.jobs[name]
	if job != nil && job.Status != nil && job.Status.State != "CANCELLED" {
		stateName := "SUCCEEDED"
		terminalErr := runErr
		if terminalErr == nil && result.ExitCode != 0 {
			terminalErr = fmt.Errorf("container exited with code %d", result.ExitCode)
		}
		if cleanupErr != nil {
			terminalErr = fmt.Errorf("container cleanup: %w", cleanupErr)
		}
		if terminalErr != nil {
			stateName = "FAILED"
		}
		job.Status.State = stateName
		job.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
		job.Status.StatusEvents = append(job.Status.StatusEvents,
			newStatusEvent(stateName, result.ExitCode, result.Output, terminalErr))
	}
	if cleanupErr == nil {
		delete(api.runtimes, name)
	}
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		api.opMgr.MarkPersistenceFailure(err)
		log.Printf("[Batch] persist terminal state for %s: %v", name, err)
	}
}

func (api *API) setJobState(name, stateName, description string) {
	api.mu.Lock()
	defer api.mu.Unlock()
	job := api.jobs[name]
	if job == nil {
		return
	}
	if job.Status == nil {
		job.Status = &JobStatus{}
	}
	job.Status.State = stateName
	job.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	job.Status.StatusEvents = append(job.Status.StatusEvents,
		newStatusEvent(stateName, 0, description, nil))
}

func (api *API) finishContainerJob(name, stateName string, exitCode int, output string, terminalErr error) {
	api.mu.Lock()
	defer api.mu.Unlock()
	job := api.jobs[name]
	if job == nil || job.Status == nil || job.Status.State == "CANCELLED" {
		return
	}
	job.Status.State = stateName
	job.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	job.Status.StatusEvents = append(job.Status.StatusEvents,
		newStatusEvent(stateName, exitCode, output, terminalErr))
}

func newStatusEvent(stateName string, exitCode int, _ string, eventErr error) json.RawMessage {
	description := ""
	if eventErr != nil {
		description = eventErr.Error()
	}
	if description == "" {
		description = "Job entered state " + stateName
	}
	event := map[string]any{
		"type":        "JOB_STATE_CHANGED",
		"description": description,
		"eventTime":   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if stateName == "SUCCEEDED" || stateName == "FAILED" || stateName == "CANCELLED" {
		event["taskState"] = stateName
		event["taskExecution"] = map[string]any{"exitCode": exitCode}
	}
	raw, _ := json.Marshal(event)
	return raw
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
	if job.Status.State == "SUCCEEDED" || job.Status.State == "FAILED" || job.Status.State == "CANCELLED" {
		terminal := deepCopyJob(job)
		api.mu.Unlock()
		op, err := api.opMgr.RegisterScopedTargetDurable("batch#operation", "cancel", name)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to register operation")
			return
		}
		response := typedResponse("type.googleapis.com/google.cloud.batch.v1.Job", terminal)
		_ = api.opMgr.FinalizeScopedDurable(op.Name, response, 0, "")
		_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
		return
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
	response := typedResponse("type.googleapis.com/google.cloud.batch.v1.Job", cancelled)
	_ = api.opMgr.FinalizeScopedDurable(op.Name, response, 0, "")
	api.mu.RLock()
	run := api.runs[name]
	api.mu.RUnlock()
	if run != nil {
		run.cancel()
	}
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
	api.mu.RLock()
	_, exists := api.jobs[name]
	run := api.runs[name]
	api.mu.RUnlock()
	if !exists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "job not found: "+name)
		return
	}
	if run != nil {
		run.cancel()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-run.done:
		case <-r.Context().Done():
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Batch job cleanup was cancelled")
			return
		case <-timer.C:
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Batch job cleanup timed out")
			return
		}
		api.mu.RLock()
		_, cleanupPending := api.runtimes[name]
		api.mu.RUnlock()
		if cleanupPending {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Batch job container cleanup failed")
			return
		}
	}

	api.mu.Lock()
	job, stillExists := api.jobs[name]
	if !stillExists {
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
	_ = api.opMgr.FinalizeScopedDurable(op.Name,
		json.RawMessage(`{"@type":"type.googleapis.com/google.protobuf.Empty"}`), 0, "")

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

func typedResponse(typeURL string, value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	var response map[string]any
	_ = json.Unmarshal(raw, &response)
	response["@type"] = typeURL
	raw, _ = json.Marshal(response)
	return raw
}
