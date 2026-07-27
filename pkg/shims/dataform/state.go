package dataform

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const dataformStateEntry = "dataform/metadata"

func init() {
	state.MustRegisterEntryValidator(dataformStateEntry, state.StrictEntryValidator(validateDataformMetadata))
}

type dataformStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type dataformMetadata struct {
	Repositories        map[string]*Repository         `json:"repositories"`
	Workspaces          map[string]*Workspace          `json:"workspaces"`
	CompilationResults  map[string]*CompilationResult  `json:"compilationResults"`
	WorkflowInvocations map[string]*WorkflowInvocation `json:"workflowInvocations"`
	NextCompilationID   uint64                         `json:"nextCompilationId"`
	NextInvocationID    uint64                         `json:"nextInvocationId"`
}

func validateDataformMetadata(_ state.EntryValidationContext, metadata *dataformMetadata) error {
	for name := range metadata.Workspaces {
		index := strings.LastIndex(name, "/workspaces/")
		if index < 0 {
			return fmt.Errorf("workspace %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Repositories[name[:index]]; !ok {
			return fmt.Errorf("workspace %q references missing repository", name)
		}
	}
	for name, result := range metadata.CompilationResults {
		index := strings.LastIndex(name, "/compilationResults/")
		if index < 0 || metadata.Repositories[name[:index]] == nil {
			return fmt.Errorf("compilation result %q has invalid parent hierarchy", name)
		}
		if result == nil || !strings.HasPrefix(result.Workspace, name[:index]+"/workspaces/") {
			return fmt.Errorf("compilation result %q references invalid workspace", name)
		}
		if metadata.Workspaces[result.Workspace] == nil {
			return fmt.Errorf("compilation result %q references missing workspace", name)
		}
	}
	for name, invocation := range metadata.WorkflowInvocations {
		index := strings.LastIndex(name, "/workflowInvocations/")
		if index < 0 || metadata.Repositories[name[:index]] == nil {
			return fmt.Errorf("workflow invocation %q has invalid parent hierarchy", name)
		}
		if invocation == nil || !strings.HasPrefix(invocation.CompilationResult, name[:index]+"/compilationResults/") {
			return fmt.Errorf("workflow invocation %q references invalid compilation result", name)
		}
		if metadata.CompilationResults[invocation.CompilationResult] == nil {
			return fmt.Errorf("workflow invocation %q references missing compilation result", name)
		}
	}
	return nil
}

func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	repoSnap := make(map[string]*Repository, len(api.repositories))
	for k, v := range api.repositories {
		repoSnap[k] = cloneRepo(v)
	}
	wsSnap := make(map[string]*Workspace, len(api.workspaces))
	for k, v := range api.workspaces {
		wsSnap[k] = cloneWorkspace(v)
	}
	compilationSnap := make(map[string]*CompilationResult, len(api.compilationResults))
	for k, v := range api.compilationResults {
		compilationSnap[k] = cloneCompilationResult(v)
	}
	invocationSnap := make(map[string]*WorkflowInvocation, len(api.workflowInvocations))
	for k, v := range api.workflowInvocations {
		invocationSnap[k] = cloneWorkflowInvocation(v)
	}
	nextCompilationID := api.nextCompilationID
	nextInvocationID := api.nextInvocationID
	api.mu.RUnlock()

	return api.stateStore.Save(dataformStateEntry, dataformMetadata{
		Repositories:        repoSnap,
		Workspaces:          wsSnap,
		CompilationResults:  compilationSnap,
		WorkflowInvocations: invocationSnap,
		NextCompilationID:   nextCompilationID,
		NextInvocationID:    nextInvocationID,
	})
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta dataformMetadata
	if err := api.stateStore.Load(dataformStateEntry, &meta); err != nil {
		if err.Error() == "state entry not found" || isNotFound(err) {
			return nil
		}
		return err
	}
	if meta.Repositories != nil {
		api.repositories = meta.Repositories
	}
	if meta.Workspaces != nil {
		api.workspaces = meta.Workspaces
	}
	if meta.CompilationResults != nil {
		api.compilationResults = meta.CompilationResults
	}
	if meta.WorkflowInvocations != nil {
		api.workflowInvocations = meta.WorkflowInvocations
	}
	if meta.NextCompilationID > 0 {
		api.nextCompilationID = meta.NextCompilationID
	}
	if meta.NextInvocationID > 0 {
		api.nextInvocationID = meta.NextInvocationID
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}

func cloneRepo(r *Repository) *Repository {
	raw, _ := json.Marshal(r)
	var c Repository
	_ = json.Unmarshal(raw, &c)
	return &c
}

func cloneWorkspace(w *Workspace) *Workspace {
	raw, _ := json.Marshal(w)
	var c Workspace
	_ = json.Unmarshal(raw, &c)
	return &c
}

func cloneCompilationResult(result *CompilationResult) *CompilationResult {
	raw, _ := json.Marshal(result)
	var clone CompilationResult
	_ = json.Unmarshal(raw, &clone)
	if clone.CompilationErrors == nil {
		clone.CompilationErrors = []CompilationError{}
	}
	return &clone
}

func cloneWorkflowInvocation(invocation *WorkflowInvocation) *WorkflowInvocation {
	raw, _ := json.Marshal(invocation)
	var clone WorkflowInvocation
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
