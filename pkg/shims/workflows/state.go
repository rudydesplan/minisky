package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/state"
)

const workflowsStateEntry = "workflows/metadata"

func init() {
	state.MustRegisterEntryValidator(workflowsStateEntry, state.StrictEntryValidator(validateWorkflowsMetadata))
}

type workflowsStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type workflowsMetadata struct {
	Workflows  map[string]*Workflow  `json:"workflows"`
	Executions map[string]*Execution `json:"executions"`
	RevCounter int                   `json:"revCounter"`
}

func validateWorkflowsMetadata(_ state.EntryValidationContext, metadata *workflowsMetadata) error {
	if metadata.RevCounter < 0 {
		return fmt.Errorf("revCounter must not be negative")
	}
	for name, workflow := range metadata.Workflows {
		if workflow == nil {
			continue
		}
		if workflow.State != "ACTIVE" && workflow.State != "UNAVAILABLE" {
			return fmt.Errorf("workflow %q has invalid state %q", name, workflow.State)
		}
		prefix := strings.SplitN(workflow.RevisionID, "-", 2)[0]
		revision, err := strconv.Atoi(prefix)
		if err == nil && revision > metadata.RevCounter {
			return fmt.Errorf("revCounter %d collides with workflow %q revision %q", metadata.RevCounter, name, workflow.RevisionID)
		}
	}
	for name := range metadata.Executions {
		execution := metadata.Executions[name]
		if execution != nil && execution.State != "ACTIVE" && execution.State != "SUCCEEDED" &&
			execution.State != "FAILED" && execution.State != "CANCELLED" {
			return fmt.Errorf("execution %q has invalid state %q", name, execution.State)
		}
		index := strings.LastIndex(name, "/executions/")
		if index < 0 {
			return fmt.Errorf("execution %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Workflows[name[:index]]; !ok {
			return fmt.Errorf("execution %q references missing workflow", name)
		}
	}
	return nil
}

// persistState deep-copies resources and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	wfSnapshot, execSnapshot, revCounter := api.snapshot()
	return api.stateStore.Save(workflowsStateEntry, workflowsMetadata{
		Workflows:  wfSnapshot,
		Executions: execSnapshot,
		RevCounter: revCounter,
	})
}

func (api *API) compensateState(cause error) {
	api.opMgr.MarkPersistenceFailure(cause)
	if err := api.persistState(); err == nil {
		return
	} else {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("workflows compensation save: %w", err))
	}
	var durable workflowsMetadata
	if err := api.stateStore.Load(workflowsStateEntry, &durable); err != nil {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("workflows compensation readback: %w", err))
		return
	}
	api.mu.Lock()
	api.workflows = durable.Workflows
	if api.workflows == nil {
		api.workflows = make(map[string]*Workflow)
	}
	api.executions = durable.Executions
	if api.executions == nil {
		api.executions = make(map[string]*Execution)
	}
	api.revCounter = durable.RevCounter
	api.mu.Unlock()
}

func (api *API) compensateMutation(operationName string, cause error) {
	api.compensateState(cause)
	if operationName == "" {
		return
	}
	if err := api.opMgr.RollbackScopedRegistration(operationName); err != nil {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("workflows operation compensation: %w", err))
	}
}

// snapshot returns deep copies of all workflows and executions for safe serialization.
func (api *API) snapshot() (map[string]*Workflow, map[string]*Execution, int) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.snapshotLocked()
}

func (api *API) snapshotLocked() (map[string]*Workflow, map[string]*Execution, int) {
	wfs := make(map[string]*Workflow, len(api.workflows))
	for k, v := range api.workflows {
		wfs[k] = deepCopyWorkflow(v)
	}
	execs := make(map[string]*Execution, len(api.executions))
	for k, v := range api.executions {
		execs[k] = deepCopyExecution(v)
	}
	return wfs, execs, api.revCounter
}

// loadState rehydrates workflows and executions from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta workflowsMetadata
	if err := api.stateStore.Load(workflowsStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Workflows != nil {
		api.workflows = meta.Workflows
	}
	if meta.Executions != nil {
		api.executions = meta.Executions
	}
	api.revCounter = meta.RevCounter
	changed := false
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for name, execution := range api.executions {
		if execution != nil && execution.State == "ACTIVE" {
			execution.State = "FAILED"
			execution.EndTime = now
			execution.Error = &ExecutionError{
				Payload: "execution interrupted by MiniSky restart",
				Context: name,
			}
			changed = true
		}
	}
	if changed {
		return api.stateStore.Save(workflowsStateEntry, workflowsMetadata{
			Workflows: api.workflows, Executions: api.executions, RevCounter: api.revCounter,
		})
	}
	return nil
}

func deepCopyWorkflow(w *Workflow) *Workflow {
	raw, _ := json.Marshal(w)
	var clone Workflow
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func deepCopyExecution(e *Execution) *Execution {
	raw, _ := json.Marshal(e)
	var clone Execution
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
