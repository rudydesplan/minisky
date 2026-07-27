package workflows

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	factory := func(ctx *registry.Context) http.Handler {
		return ctx.SharedHandler("workflows", func() http.Handler {
			return NewAPI(ctx.OpMgr)
		})
	}
	registry.Register("workflows.googleapis.com", factory)
	registry.Register("workflowexecutions.googleapis.com", factory)
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (workflows.googleapis.com/v1 + workflowexecutions.googleapis.com/v1)
// ─────────────────────────────────────────────────────────────────────────────

// Workflow represents a google.cloud.workflows.v1.Workflow resource.
type Workflow struct {
	Name               string            `json:"name"`
	Description        string            `json:"description,omitempty"`
	State              string            `json:"state"`
	RevisionID         string            `json:"revisionId"`
	CreateTime         string            `json:"createTime"`
	UpdateTime         string            `json:"updateTime"`
	RevisionCreateTime string            `json:"revisionCreateTime"`
	Labels             map[string]string `json:"labels,omitempty"`
	ServiceAccount     string            `json:"serviceAccount,omitempty"`
	SourceContents     string            `json:"sourceContents,omitempty"`
}

// Execution represents a google.cloud.workflows.executions.v1.Execution resource.
type Execution struct {
	Name               string            `json:"name"`
	StartTime          string            `json:"startTime,omitempty"`
	EndTime            string            `json:"endTime,omitempty"`
	Duration           string            `json:"duration,omitempty"`
	State              string            `json:"state"`
	Argument           string            `json:"argument,omitempty"`
	Result             string            `json:"result,omitempty"`
	Error              *ExecutionError   `json:"error,omitempty"`
	WorkflowRevisionID string            `json:"workflowRevisionId,omitempty"`
	CallLogLevel       string            `json:"callLogLevel,omitempty"`
	Labels             map[string]string `json:"labels,omitempty"`
}

// ExecutionError holds error details for a failed execution.
type ExecutionError struct {
	Payload string `json:"payload,omitempty"`
	Context string `json:"context,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Workflows v1 and Workflow Executions v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	stateStore workflowsStateStore
	workflows  map[string]*Workflow
	executions map[string]*Execution
	cancels    map[string]context.CancelFunc
	revCounter int // monotonic revision counter
}

// NewAPI creates a new Workflows API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:      opMgr,
		stateStore: state.NewGuardedEntryStore(store, err),
		workflows:  make(map[string]*Workflow),
		executions: make(map[string]*Execution),
		cancels:    make(map[string]context.CancelFunc),
	}
	if err != nil {
		log.Printf("[Shim: Workflows] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Workflows] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr:      orchestrator.NewOperationManager(),
		workflows:  make(map[string]*Workflow),
		executions: make(map[string]*Execution),
		cancels:    make(map[string]context.CancelFunc),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Workflows] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.Contains(r.URL.Path, "/executions/") && strings.HasSuffix(r.URL.Path, ":cancel") && r.Method == http.MethodPost:
		api.cancelExecution(w, r)
	case strings.Contains(r.URL.Path, "/executions") && r.Method == http.MethodPost && !strings.Contains(r.URL.Path, ":cancel"):
		api.createExecution(w, r)
	case strings.Contains(r.URL.Path, "/executions/") && r.Method == http.MethodGet:
		api.getExecution(w, r)
	case strings.Contains(r.URL.Path, "/executions") && r.Method == http.MethodGet && !strings.Contains(r.URL.Path, "/executions/"):
		api.listExecutions(w, r)
	case strings.HasSuffix(r.URL.Path, "/workflows") && r.Method == http.MethodPost:
		api.createWorkflow(w, r)
	case strings.HasSuffix(r.URL.Path, "/workflows") && r.Method == http.MethodGet:
		api.listWorkflows(w, r)
	case strings.Contains(r.URL.Path, "/workflows/") && !strings.Contains(r.URL.Path, "/executions") && r.Method == http.MethodGet:
		api.getWorkflow(w, r)
	case strings.Contains(r.URL.Path, "/workflows/") && !strings.Contains(r.URL.Path, "/executions") && r.Method == http.MethodPatch:
		api.patchWorkflow(w, r)
	case strings.Contains(r.URL.Path, "/workflows/") && !strings.Contains(r.URL.Path, "/executions") && r.Method == http.MethodDelete:
		api.deleteWorkflow(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Workflows resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Workflow Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createWorkflow(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	workflowID := r.URL.Query().Get("workflowId")
	if workflowID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "workflowId query parameter is required")
		return
	}

	var wf Workflow
	if err := json.NewDecoder(r.Body).Decode(&wf); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if wf.SourceContents == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "sourceContents is required")
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/workflows/%s", project, location, workflowID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	api.mu.Lock()
	if _, exists := api.workflows[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "workflow already exists: "+workflowID)
		return
	}
	revisionID := api.nextRevisionID()
	wf.Name = name
	wf.State = "ACTIVE"
	wf.RevisionID = revisionID
	wf.CreateTime = now
	wf.UpdateTime = now
	wf.RevisionCreateTime = now
	api.workflows[name] = &wf
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("workflows#operation", "create", name)
	if err != nil {
		api.mu.Lock()
		delete(api.workflows, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.workflows, name)
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}
	api.opMgr.RunAsync(op.Name, func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": op.Name,
		"done": false,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.workflows.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getWorkflow(w http.ResponseWriter, r *http.Request) {
	name := parseWorkflowName(r.URL.Path)

	api.mu.RLock()
	wf, ok := api.workflows[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workflow not found: "+name)
		return
	}
	clone := deepCopyWorkflow(wf)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listWorkflows(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/workflows/", project, location)

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
	all := make([]*Workflow, 0)
	for key, wf := range api.workflows {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyWorkflow(wf))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "workflows.googleapis.com",
		Parent:  strings.TrimSuffix(prefix, "/workflows/"),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(workflow *Workflow) string { return workflow.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = make([]*Workflow, 0)
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"workflows":     result,
		"nextPageToken": nextToken,
		"unreachable":   []string{},
	})
}

func (api *API) patchWorkflow(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseWorkflowName(r.URL.Path)
	updateMask := r.URL.Query().Get("updateMask")

	api.mu.Lock()
	existing, ok := api.workflows[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workflow not found: "+name)
		return
	}

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	existingRaw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(existingRaw, &merged)

	if updateMask != "" {
		fields := strings.Split(updateMask, ",")
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if v, exists := patch[field]; exists {
				merged[field] = v
			}
		}
	} else {
		for k, v := range patch {
			merged[k] = v
		}
	}

	// Preserve/update output-only fields
	now := time.Now().UTC().Format(time.RFC3339Nano)
	oldRevisionCounter := api.revCounter
	revisionID := api.nextRevisionID()
	merged["name"] = existing.Name
	merged["state"] = "ACTIVE"
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = now
	merged["revisionId"] = revisionID
	merged["revisionCreateTime"] = now

	updatedRaw, _ := json.Marshal(merged)
	var updated Workflow
	_ = json.Unmarshal(updatedRaw, &updated)
	oldWf := api.workflows[name]
	api.workflows[name] = &updated
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("workflows#operation", "update", name)
	if err != nil {
		api.mu.Lock()
		api.workflows[name] = oldWf
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.workflows[name] = oldWf
		api.revCounter = oldRevisionCounter
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}
	response, _ := json.Marshal(&updated)
	_ = api.opMgr.FinalizeScopedDurable(op.Name, response, 0, "")

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

func (api *API) deleteWorkflow(w http.ResponseWriter, r *http.Request) {
	name := parseWorkflowName(r.URL.Path)
	api.mu.Lock()
	wf, exists := api.workflows[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workflow not found: "+name)
		return
	}
	delete(api.workflows, name)
	deletedExecutions := make(map[string]*Execution)
	for executionName, execution := range api.executions {
		if strings.HasPrefix(executionName, name+"/executions/") {
			deletedExecutions[executionName] = execution
			delete(api.executions, executionName)
		}
	}
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("workflows#operation", "delete", name)
	if err != nil {
		api.mu.Lock()
		api.workflows[name] = wf
		for executionName, execution := range deletedExecutions {
			api.executions[executionName] = execution
		}
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.workflows[name] = wf
		for executionName, execution := range deletedExecutions {
			api.executions[executionName] = execution
		}
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = api.opMgr.FinalizeScopedDurable(op.Name, json.RawMessage(`{}`), 0, "")

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Execution Handlers (synchronous — NOT LRO)
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createExecution(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	wfName := parseWorkflowName(r.URL.Path)

	api.mu.RLock()
	wf := api.workflows[wfName]
	api.mu.RUnlock()
	if wf == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workflow not found: "+wfName)
		return
	}

	var exec Execution
	if err := json.NewDecoder(r.Body).Decode(&exec); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	execID := fmt.Sprintf("%d", time.Now().UnixNano())
	execName := fmt.Sprintf("%s/executions/%s", wfName, execID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	exec.Name = execName
	exec.StartTime = now
	exec.State = "ACTIVE"
	exec.WorkflowRevisionID = wf.RevisionID

	api.mu.Lock()
	api.executions[execName] = &exec
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.executions, execName)
		api.mu.Unlock()
		api.compensateState(err)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	// Deep-copy for response BEFORE launching goroutine (avoids race)
	clone := deepCopyExecution(&exec)

	// Run workflow asynchronously
	ctx, cancel := context.WithCancel(context.Background())
	api.startExecution(execName, cancel)
	go api.ExecuteWorkflow(ctx, execName, wf.SourceContents, exec.Argument)

	// Return execution directly (synchronous, NOT LRO)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) getExecution(w http.ResponseWriter, r *http.Request) {
	name := parseExecutionName(r.URL.Path)

	api.mu.RLock()
	exec, ok := api.executions[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "execution not found: "+name)
		return
	}
	clone := deepCopyExecution(exec)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listExecutions(w http.ResponseWriter, r *http.Request) {
	wfName := parseWorkflowName(r.URL.Path)
	prefix := wfName + "/executions/"

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
	all := make([]*Execution, 0)
	for key, exec := range api.executions {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyExecution(exec))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "workflowexecutions.googleapis.com",
		Parent:  wfName,
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(execution *Execution) string { return execution.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = make([]*Execution, 0)
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"executions":    result,
		"nextPageToken": nextToken,
	})
}

func (api *API) cancelExecution(w http.ResponseWriter, r *http.Request) {
	// Strip :cancel suffix to get execution name
	path := strings.TrimSuffix(r.URL.Path, ":cancel")
	name := parseExecutionName(path)

	api.persistMu.Lock()
	api.mu.RLock()
	exec, ok := api.executions[name]
	if !ok {
		api.mu.RUnlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "execution not found: "+name)
		return
	}
	if exec.State != "ACTIVE" {
		clone := deepCopyExecution(exec)
		api.mu.RUnlock()
		api.persistMu.Unlock()
		_ = json.NewEncoder(w).Encode(clone)
		return
	}
	original := deepCopyExecution(exec)
	workflowsSnapshot, executionsSnapshot, revisionCounter := api.snapshotLocked()
	api.mu.RUnlock()
	candidate := deepCopyExecution(executionsSnapshot[name])
	candidate.State = "CANCELLED"
	candidate.EndTime = time.Now().UTC().Format(time.RFC3339Nano)
	executionsSnapshot[name] = candidate
	metadata := workflowsMetadata{
		Workflows: workflowsSnapshot, Executions: executionsSnapshot, RevCounter: revisionCounter,
	}
	if api.stateStore != nil {
		if err := api.stateStore.Save(workflowsStateEntry, metadata); err != nil {
			api.opMgr.MarkPersistenceFailure(err)
			// The failed Save may have committed. Write the original snapshot
			// before returning so the still-running execution remains durable.
			originalExecutions := make(map[string]*Execution, len(executionsSnapshot))
			for executionName, execution := range executionsSnapshot {
				originalExecutions[executionName] = deepCopyExecution(execution)
			}
			originalExecutions[name] = original
			if compensationErr := api.stateStore.Save(workflowsStateEntry, workflowsMetadata{
				Workflows: workflowsSnapshot, Executions: originalExecutions, RevCounter: revisionCounter,
			}); compensationErr != nil {
				api.opMgr.MarkPersistenceFailure(fmt.Errorf("cancel compensation save: %w", compensationErr))
				var durable workflowsMetadata
				if readbackErr := api.stateStore.Load(workflowsStateEntry, &durable); readbackErr != nil {
					api.opMgr.MarkPersistenceFailure(fmt.Errorf("cancel compensation readback: %w", readbackErr))
					api.persistMu.Unlock()
					writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
					return
				}
				if durable.Workflows == nil {
					durable.Workflows = make(map[string]*Workflow)
				}
				if durable.Executions == nil {
					durable.Executions = make(map[string]*Execution)
				}
				api.mu.Lock()
				api.workflows = durable.Workflows
				api.executions = durable.Executions
				api.revCounter = durable.RevCounter
				durableExecution := api.executions[name]
				var cancel context.CancelFunc
				if durableExecution != nil && durableExecution.State == "CANCELLED" {
					cancel = api.cancels[name]
				}
				api.mu.Unlock()
				api.persistMu.Unlock()
				if cancel != nil {
					cancel()
				}
				writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
				return
			}
			api.persistMu.Unlock()
			writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
			return
		}
	}

	api.mu.Lock()
	current := api.executions[name]
	var cancel context.CancelFunc
	if current != nil && current.State == "ACTIVE" {
		current.State = candidate.State
		current.EndTime = candidate.EndTime
		cancel = api.cancels[name]
	}
	clone := deepCopyExecution(current)
	completionWon := current != nil && current.State != "CANCELLED"
	if completionWon && api.stateStore != nil {
		workflowsSnapshot, executionsSnapshot, revisionCounter = api.snapshotLocked()
	}
	api.mu.Unlock()
	if completionWon && api.stateStore != nil {
		if err := api.stateStore.Save(workflowsStateEntry, workflowsMetadata{
			Workflows: workflowsSnapshot, Executions: executionsSnapshot, RevCounter: revisionCounter,
		}); err != nil {
			api.opMgr.MarkPersistenceFailure(err)
			api.persistMu.Unlock()
			writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
			return
		}
	}
	api.persistMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if clone == nil {
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) startExecution(name string, cancel context.CancelFunc) {
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.cancels == nil {
		api.cancels = make(map[string]context.CancelFunc)
	}
	api.cancels[name] = cancel
}

func (api *API) finishExecution(name string) {
	api.mu.Lock()
	defer api.mu.Unlock()
	delete(api.cancels, name)
}

// ─────────────────────────────────────────────────────────────────────────────
// Operation Handler
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "workflows#operation")
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

// parseWorkflowName reconstructs the full workflow resource name from the URL path.
func parseWorkflowName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	wfID := extractAfter(path, "workflows")
	return fmt.Sprintf("projects/%s/locations/%s/workflows/%s", project, location, wfID)
}

// parseExecutionName reconstructs the full execution resource name from the URL path.
func parseExecutionName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	wfID := extractAfter(path, "workflows")
	execID := extractAfter(path, "executions")
	return fmt.Sprintf("projects/%s/locations/%s/workflows/%s/executions/%s", project, location, wfID, execID)
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

// nextRevisionID generates a monotonically increasing revision ID.
// Caller MUST hold api.mu (write lock).
func (api *API) nextRevisionID() string {
	api.revCounter++
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	return fmt.Sprintf("%06d-%s", api.revCounter, hex.EncodeToString(suffix))
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
