package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func TestCreateWorkflow(t *testing.T) {
	api := newTestAPI()
	body := `{"sourceContents":"[{\"return\":\"hello\"}]"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows?workflowId=wf1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != false {
		t.Fatal("expected LRO not done")
	}
	name, _ := resp["name"].(string)
	if name == "" {
		t.Fatal("expected operation name in response")
	}
	meta, _ := resp["metadata"].(map[string]any)
	if meta == nil {
		t.Fatal("expected metadata in response")
	}
	if meta["verb"] != "create" {
		t.Fatalf("expected verb=create, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/workflows/wf1" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}

	// Verify workflow was stored
	api.mu.RLock()
	wf := api.workflows["projects/test/locations/us-central1/workflows/wf1"]
	api.mu.RUnlock()
	if wf == nil {
		t.Fatal("workflow not stored")
	}
	if wf.State != "ACTIVE" {
		t.Fatalf("expected state=ACTIVE, got %s", wf.State)
	}
	if wf.RevisionID == "" {
		t.Fatal("expected revisionId to be generated")
	}
	if wf.CreateTime == "" {
		t.Fatal("expected createTime to be set")
	}
	if wf.SourceContents != `[{"return":"hello"}]` {
		t.Fatalf("unexpected sourceContents: %s", wf.SourceContents)
	}
}

func TestWorkflowDomainsShareOneAPIInstance(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv(registry.ExperimentalServicesEnv, "1")
	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	workflowsHandler := handlers["workflows.googleapis.com"]
	executionsHandler := handlers["workflowexecutions.googleapis.com"]
	if workflowsHandler == nil || executionsHandler == nil {
		t.Fatal("both workflow domains must be registered")
	}
	if workflowsHandler != executionsHandler {
		t.Fatal("workflow and execution domains must share one API instance")
	}
}

func TestCreateWorkflowMissingId(t *testing.T) {
	api := newTestAPI()
	body := `{"sourceContents":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkflowMissingSource(t *testing.T) {
	api := newTestAPI()
	body := `{"description":"no source"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows?workflowId=wf1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkflowDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workflows["projects/test/locations/us-central1/workflows/dup"] = &Workflow{
		Name:       "projects/test/locations/us-central1/workflows/dup",
		State:      "ACTIVE",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	body := `{"sourceContents":"test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows?workflowId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetWorkflow(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workflows["projects/test/locations/us-central1/workflows/wf1"] = &Workflow{
		Name:           "projects/test/locations/us-central1/workflows/wf1",
		State:          "ACTIVE",
		RevisionID:     "000001-abc",
		CreateTime:     "2024-01-01T00:00:00Z",
		UpdateTime:     "2024-01-01T00:00:00Z",
		SourceContents: `[{"return":"hello"}]`,
		ServiceAccount: "sa@test.iam.gserviceaccount.com",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows/wf1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var wf Workflow
	_ = json.Unmarshal(w.Body.Bytes(), &wf)
	if wf.Name != "projects/test/locations/us-central1/workflows/wf1" {
		t.Fatalf("unexpected name: %s", wf.Name)
	}
	if wf.State != "ACTIVE" {
		t.Fatalf("unexpected state: %s", wf.State)
	}
	if wf.RevisionID != "000001-abc" {
		t.Fatalf("unexpected revisionId: %s", wf.RevisionID)
	}
}

func TestGetWorkflowNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListWorkflows(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workflows["projects/test/locations/us-central1/workflows/alpha"] = &Workflow{Name: "projects/test/locations/us-central1/workflows/alpha", State: "ACTIVE", CreateTime: "2024-01-01T00:00:00Z"}
	api.workflows["projects/test/locations/us-central1/workflows/beta"] = &Workflow{Name: "projects/test/locations/us-central1/workflows/beta", State: "ACTIVE", CreateTime: "2024-01-01T00:00:00Z"}
	api.workflows["projects/test/locations/us-central1/workflows/gamma"] = &Workflow{Name: "projects/test/locations/us-central1/workflows/gamma", State: "ACTIVE", CreateTime: "2024-01-01T00:00:00Z"}
	api.mu.Unlock()

	// First page: pageSize=2
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	wfs := resp["workflows"].([]any)
	if len(wfs) != 2 {
		t.Fatalf("expected 2 workflows, got %d", len(wfs))
	}
	// Verify sorted order
	first := wfs[0].(map[string]any)["name"].(string)
	second := wfs[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	wfs = resp["workflows"].([]any)
	if len(wfs) != 1 {
		t.Fatalf("expected 1 workflow on second page, got %d", len(wfs))
	}
}

func TestDeleteWorkflow(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workflows["projects/test/locations/us-central1/workflows/wf1"] = &Workflow{
		Name:       "projects/test/locations/us-central1/workflows/wf1",
		State:      "ACTIVE",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.executions["projects/test/locations/us-central1/workflows/wf1/executions/run"] = &Execution{
		Name:  "projects/test/locations/us-central1/workflows/wf1/executions/run",
		State: "SUCCEEDED",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/workflows/wf1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for delete")
	}
	meta := resp["metadata"].(map[string]any)
	if meta["verb"] != "delete" {
		t.Fatalf("expected verb=delete, got %v", meta["verb"])
	}

	// Verify workflow was removed
	api.mu.RLock()
	_, exists := api.workflows["projects/test/locations/us-central1/workflows/wf1"]
	_, executionExists := api.executions["projects/test/locations/us-central1/workflows/wf1/executions/run"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("workflow should have been deleted")
	}
	if executionExists {
		t.Fatal("workflow deletion must cascade executions to preserve import invariants")
	}
}

func TestPatchWorkflow(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workflows["projects/test/locations/us-central1/workflows/wf1"] = &Workflow{
		Name:           "projects/test/locations/us-central1/workflows/wf1",
		State:          "ACTIVE",
		RevisionID:     "000001-abc",
		CreateTime:     "2024-01-01T00:00:00Z",
		UpdateTime:     "2024-01-01T00:00:00Z",
		SourceContents: `[{"return":"old"}]`,
		Description:    "original",
	}
	api.mu.Unlock()

	body := `{"sourceContents":"[{\"return\":\"new\"}]"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/workflows/wf1?updateMask=sourceContents", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for patch")
	}

	// Verify the workflow was updated
	api.mu.RLock()
	wf := api.workflows["projects/test/locations/us-central1/workflows/wf1"]
	api.mu.RUnlock()
	if wf.SourceContents != `[{"return":"new"}]` {
		t.Fatalf("expected updated sourceContents, got %s", wf.SourceContents)
	}
	// Description should NOT have changed (not in updateMask)
	if wf.Description != "original" {
		t.Fatalf("description should be preserved, got %s", wf.Description)
	}
	// RevisionID should have changed
	if wf.RevisionID == "000001-abc" {
		t.Fatal("revisionId should have changed on patch")
	}
	// CreateTime preserved
	if wf.CreateTime != "2024-01-01T00:00:00Z" {
		t.Fatalf("createTime should be preserved, got %s", wf.CreateTime)
	}
	// UpdateTime changed
	if wf.UpdateTime == "2024-01-01T00:00:00Z" {
		t.Fatal("updateTime should have been updated")
	}
}

func TestPatchWorkflowCompensatesPostCommitSaveAndOperation(t *testing.T) {
	store := &postCommitWorkflowStore{data: make(map[string][]byte)}
	api := newTestAPI()
	api.stateStore = store
	name := "projects/test/locations/us-central1/workflows/wf1"
	api.workflows[name] = &Workflow{
		Name: name, State: "ACTIVE", RevisionID: "000001-a",
		Description: "old", SourceContents: `[{"return":"ok"}]`,
	}
	api.revCounter = 1
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.failNext = true

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/"+name+"?updateMask=description", bytes.NewBufferString(`{"description":"new"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.workflows[name].Description; got != "old" {
		t.Fatalf("visible description = %q, want old", got)
	}
	var durable workflowsMetadata
	if err := store.Load(workflowsStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if got := durable.Workflows[name].Description; got != "old" {
		t.Fatalf("durable description = %q, want compensated old", got)
	}
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("compensated patch retained operations: %#v", operations)
	}
	if api.opMgr.PersistenceError() == nil {
		t.Fatal("ambiguous resource save did not leave sticky degradation")
	}
}

func TestCreateExecution(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workflows["projects/test/locations/us-central1/workflows/wf1"] = &Workflow{
		Name:           "projects/test/locations/us-central1/workflows/wf1",
		State:          "ACTIVE",
		RevisionID:     "000001-abc",
		SourceContents: `[{"return":"hello"}]`,
	}
	api.mu.Unlock()

	body := `{"argument":"{\"key\":\"value\"}"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows/wf1/executions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var exec Execution
	_ = json.Unmarshal(w.Body.Bytes(), &exec)
	if exec.State != "ACTIVE" {
		t.Fatalf("expected state=ACTIVE, got %s", exec.State)
	}
	if exec.Name == "" {
		t.Fatal("expected execution name")
	}
	if exec.StartTime == "" {
		t.Fatal("expected startTime")
	}
	if exec.WorkflowRevisionID != "000001-abc" {
		t.Fatalf("expected workflowRevisionId=000001-abc, got %s", exec.WorkflowRevisionID)
	}
	if exec.Argument != `{"key":"value"}` {
		t.Fatalf("unexpected argument: %s", exec.Argument)
	}

	// Verify it's NOT an LRO (no "done" field, direct Execution response)
	var raw map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &raw)
	if _, hasDone := raw["done"]; hasDone {
		t.Fatal("execution response should NOT be an LRO")
	}
}

func TestCreateExecutionWorkflowNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows/missing/executions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetExecution(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.executions["projects/test/locations/us-central1/workflows/wf1/executions/exec1"] = &Execution{
		Name:               "projects/test/locations/us-central1/workflows/wf1/executions/exec1",
		State:              "SUCCEEDED",
		StartTime:          "2024-01-01T00:00:00Z",
		EndTime:            "2024-01-01T00:00:01Z",
		Result:             `"hello"`,
		WorkflowRevisionID: "000001-abc",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows/wf1/executions/exec1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var exec Execution
	_ = json.Unmarshal(w.Body.Bytes(), &exec)
	if exec.State != "SUCCEEDED" {
		t.Fatalf("expected state=SUCCEEDED, got %s", exec.State)
	}
	if exec.Result != `"hello"` {
		t.Fatalf("unexpected result: %s", exec.Result)
	}
}

func TestGetExecutionNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows/wf1/executions/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListExecutions(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.executions["projects/test/locations/us-central1/workflows/wf1/executions/a"] = &Execution{
		Name:  "projects/test/locations/us-central1/workflows/wf1/executions/a",
		State: "SUCCEEDED",
	}
	api.executions["projects/test/locations/us-central1/workflows/wf1/executions/b"] = &Execution{
		Name:  "projects/test/locations/us-central1/workflows/wf1/executions/b",
		State: "ACTIVE",
	}
	api.executions["projects/test/locations/us-central1/workflows/wf1/executions/c"] = &Execution{
		Name:  "projects/test/locations/us-central1/workflows/wf1/executions/c",
		State: "FAILED",
	}
	// Different workflow — should not appear
	api.executions["projects/test/locations/us-central1/workflows/other/executions/x"] = &Execution{
		Name:  "projects/test/locations/us-central1/workflows/other/executions/x",
		State: "SUCCEEDED",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows/wf1/executions?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	execs := resp["executions"].([]any)
	if len(execs) != 2 {
		t.Fatalf("expected 2 executions, got %d", len(execs))
	}
	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows/wf1/executions?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	execs = resp["executions"].([]any)
	if len(execs) != 1 {
		t.Fatalf("expected 1 execution on second page, got %d", len(execs))
	}
}

func TestCancelExecution(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.executions["projects/test/locations/us-central1/workflows/wf1/executions/exec1"] = &Execution{
		Name:      "projects/test/locations/us-central1/workflows/wf1/executions/exec1",
		State:     "ACTIVE",
		StartTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows/wf1/executions/exec1:cancel", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var exec Execution
	_ = json.Unmarshal(w.Body.Bytes(), &exec)
	if exec.State != "CANCELLED" {
		t.Fatalf("expected state=CANCELLED, got %s", exec.State)
	}
	if exec.EndTime == "" {
		t.Fatal("expected endTime to be set after cancel")
	}
}

func TestCancelExecutionSaveFailureDoesNotStopOrHideExecution(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingStore{}
	name := "projects/test/locations/us-central1/workflows/wf1/executions/exec1"
	api.executions[name] = &Execution{Name: name, State: "ACTIVE", StartTime: "start"}
	cancelled := false
	api.cancels[name] = func() { cancelled = true }

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/"+name+":cancel", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if cancelled {
		t.Fatal("execution was cancelled before transition persisted")
	}
	if got := api.executions[name]; got.State != "ACTIVE" || got.EndTime != "" {
		t.Fatalf("visible execution = %#v, want original ACTIVE state", got)
	}
	if api.opMgr.PersistenceError() == nil {
		t.Fatal("uncertain cancellation did not leave sticky degradation")
	}
}

func TestCancelExecutionReconcilesAfterFailedCompensation(t *testing.T) {
	for _, test := range []struct {
		name         string
		commitCancel bool
		wantState    string
		wantCancel   bool
	}{
		{name: "durable cancellation", commitCancel: true, wantState: "CANCELLED", wantCancel: true},
		{name: "durable active", commitCancel: false, wantState: "ACTIVE", wantCancel: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			workflowName := "projects/test/locations/us-central1/workflows/wf1"
			executionName := workflowName + "/executions/exec1"
			store := &ambiguousCancelWorkflowStore{
				data:         make(map[string][]byte),
				commitCancel: test.commitCancel,
			}
			store.seed(t, workflowsMetadata{
				Workflows: map[string]*Workflow{
					workflowName: {Name: workflowName, State: "ACTIVE"},
				},
				Executions: map[string]*Execution{
					executionName: {Name: executionName, State: "ACTIVE", StartTime: "start"},
				},
			})
			api := newTestAPI()
			api.stateStore = store
			api.workflows[workflowName] = &Workflow{Name: workflowName, State: "ACTIVE"}
			api.executions[executionName] = &Execution{Name: executionName, State: "ACTIVE", StartTime: "start"}
			cancelled := false
			api.cancels[executionName] = func() { cancelled = true }

			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/"+executionName+":cancel", nil))

			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			if cancelled != test.wantCancel {
				t.Fatalf("cancel side effect = %t, want %t", cancelled, test.wantCancel)
			}
			api.mu.RLock()
			got := deepCopyExecution(api.executions[executionName])
			api.mu.RUnlock()
			if got.State != test.wantState {
				t.Fatalf("visible execution state = %q, want %q", got.State, test.wantState)
			}
			if test.wantState == "CANCELLED" && got.EndTime == "" {
				t.Fatal("reconciled cancellation lost endTime")
			}
			if api.opMgr.PersistenceError() == nil {
				t.Fatal("ambiguous cancellation did not leave sticky degradation")
			}
		})
	}
}

func TestCancelExecutionCompletionWinsWhileCancelPersistenceInFlight(t *testing.T) {
	store := &blockingWorkflowStore{
		data:    make(map[string][]byte),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	api := newTestAPI()
	api.stateStore = store
	name := "projects/test/locations/us-central1/workflows/wf1/executions/exec1"
	api.workflows["projects/test/locations/us-central1/workflows/wf1"] = &Workflow{
		Name: "projects/test/locations/us-central1/workflows/wf1", State: "ACTIVE",
	}
	api.executions[name] = &Execution{Name: name, State: "ACTIVE", StartTime: "start"}
	cancelled := false
	api.cancels[name] = func() { cancelled = true }

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/"+name+":cancel", nil))
		done <- response
	}()
	<-store.entered
	api.mu.Lock()
	api.executions[name].State = "SUCCEEDED"
	api.executions[name].Result = `"done"`
	api.executions[name].EndTime = "finished"
	api.mu.Unlock()
	close(store.release)

	response := <-done
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var execution Execution
	if err := json.Unmarshal(response.Body.Bytes(), &execution); err != nil {
		t.Fatal(err)
	}
	if execution.State != "SUCCEEDED" || execution.Result != `"done"` {
		t.Fatalf("response execution = %#v", execution)
	}
	if cancelled {
		t.Fatal("completion winner was cancelled")
	}
	var durable workflowsMetadata
	if err := store.Load(workflowsStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if got := durable.Executions[name]; got.State != "SUCCEEDED" {
		t.Fatalf("durable execution = %#v", got)
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	// Create a workflow to generate an operation
	body := `{"sourceContents":"[{\"return\":\"hello\"}]"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows?workflowId=op-test", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed: %d: %s", w.Code, w.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	opPath := createResp["name"].(string)

	// Get the operation
	req = httptest.NewRequest(http.MethodGet, "/v1/"+opPath, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var opResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &opResp)
	meta := opResp["metadata"].(map[string]any)
	if meta["verb"] != "create" {
		t.Fatalf("expected verb=create, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/workflows/op-test" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		workflows:  make(map[string]*Workflow),
		executions: make(map[string]*Execution),
	}

	// Create a workflow and execution
	api.mu.Lock()
	api.workflows["projects/p/locations/l/workflows/w1"] = &Workflow{
		Name:           "projects/p/locations/l/workflows/w1",
		State:          "ACTIVE",
		RevisionID:     "000001-abc",
		CreateTime:     "2024-06-01T00:00:00Z",
		UpdateTime:     "2024-06-01T00:00:00Z",
		SourceContents: `[{"return":"test"}]`,
	}
	api.executions["projects/p/locations/l/workflows/w1/executions/e1"] = &Execution{
		Name:               "projects/p/locations/l/workflows/w1/executions/e1",
		State:              "SUCCEEDED",
		StartTime:          "2024-06-01T00:00:00Z",
		EndTime:            "2024-06-01T00:00:01Z",
		Result:             `"test"`,
		WorkflowRevisionID: "000001-abc",
	}
	api.mu.Unlock()

	// Persist
	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Create a new API and reload
	api2 := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		workflows:  make(map[string]*Workflow),
		executions: make(map[string]*Execution),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	wf, ok := api2.workflows["projects/p/locations/l/workflows/w1"]
	if !ok {
		api2.mu.RUnlock()
		t.Fatal("workflow not found after reload")
	}
	if wf.RevisionID != "000001-abc" {
		t.Fatalf("expected revisionId=000001-abc, got %s", wf.RevisionID)
	}
	if wf.SourceContents != `[{"return":"test"}]` {
		t.Fatalf("sourceContents lost after reload")
	}

	exec, ok := api2.executions["projects/p/locations/l/workflows/w1/executions/e1"]
	if !ok {
		api2.mu.RUnlock()
		t.Fatal("execution not found after reload")
	}
	if exec.State != "SUCCEEDED" {
		t.Fatalf("expected state=SUCCEEDED, got %s", exec.State)
	}
	if exec.Result != `"test"` {
		t.Fatalf("result lost after reload")
	}
	api2.mu.RUnlock()
}

func TestRestartMarksActiveExecutionFailed(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	workflowName := "projects/p/locations/l/workflows/w"
	executionName := workflowName + "/executions/e"
	meta := workflowsMetadata{
		Workflows: map[string]*Workflow{
			workflowName: {Name: workflowName, State: "ACTIVE", RevisionID: "000001-a"},
		},
		Executions: map[string]*Execution{
			executionName: {Name: executionName, State: "ACTIVE"},
		},
		RevCounter: 1,
	}
	if err := store.Save(workflowsStateEntry, meta); err != nil {
		t.Fatal(err)
	}
	api := &API{
		opMgr: newTestAPI().opMgr, stateStore: store,
		workflows: make(map[string]*Workflow), executions: make(map[string]*Execution),
		cancels: make(map[string]context.CancelFunc),
	}
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	if got := api.executions[executionName]; got.State != "FAILED" || got.EndTime == "" {
		t.Fatalf("execution = %#v", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	// Concurrent creates
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"sourceContents":"[{\"return\":\"hello\"}]"}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/projects/test/locations/us-central1/workflows?workflowId=wf-%d", idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("unexpected status %d for create %d", w.Code, idx)
			}
		}(i)
	}

	// Concurrent lists
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/workflows", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()

	// Verify all workflows were created
	api.mu.RLock()
	count := len(api.workflows)
	api.mu.RUnlock()
	if count != n {
		t.Fatalf("expected %d workflows, got %d", n, count)
	}
}

func TestExecutionCompletesAsynchronously(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workflows["projects/test/locations/us-central1/workflows/wf1"] = &Workflow{
		Name:           "projects/test/locations/us-central1/workflows/wf1",
		State:          "ACTIVE",
		RevisionID:     "000001-abc",
		SourceContents: `[{"return":"done"}]`,
	}
	api.mu.Unlock()

	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/workflows/wf1/executions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var exec Execution
	_ = json.Unmarshal(w.Body.Bytes(), &exec)
	execName := exec.Name

	// Wait for async completion
	time.Sleep(200 * time.Millisecond)

	api.mu.RLock()
	completed := api.executions[execName]
	api.mu.RUnlock()
	if completed == nil {
		t.Fatal("execution not found")
	}
	if completed.State != "SUCCEEDED" {
		t.Fatalf("expected SUCCEEDED, got %s", completed.State)
	}
	if completed.Result != `"done"` {
		t.Fatalf("unexpected result: %s", completed.Result)
	}
}

func TestCreateExecutionFromEventFailsClosedOnSaveError(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingStore{}
	name := "projects/p/locations/l/workflows/w"
	api.workflows[name] = &Workflow{Name: name, State: "ACTIVE", RevisionID: "1", SourceContents: `[{"return":"ok"}]`}

	if err := api.CreateExecutionFromEvent(name, `{}`); err == nil {
		t.Fatal("expected persistence error")
	}
	if len(api.executions) != 0 {
		t.Fatal("failed event execution must be rolled back")
	}
}

func TestExecuteStepsHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := executeSteps(ctx, []map[string]any{{"return": "late"}}, "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestValidateCallURLEnforcesLoopbackOnlySSRFBoundary(t *testing.T) {
	for name, rawURL := range map[string]string{
		"credentials":     "http://user:secret@localhost:8080/path",
		"metadata":        "http://169.254.169.254/latest",
		"external":        "http://example.com:8080/path",
		"wrong port":      "http://localhost:8081/path",
		"non-http scheme": "https://localhost:8080/path",
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateCallURL(rawURL); err == nil || !strings.Contains(err.Error(), "SSRF protection") {
				t.Fatalf("validateCallURL(%q) error = %v", rawURL, err)
			}
		})
	}
	for _, rawURL := range []string{
		"http://localhost:8080/path",
		"http://127.0.0.1:8080/path",
	} {
		if err := validateCallURL(rawURL); err != nil {
			t.Errorf("validateCallURL(%q) = %v", rawURL, err)
		}
	}
}

func TestExecuteStepsIsBounded(t *testing.T) {
	steps := make([]map[string]any, maxWorkflowSteps+1)
	for i := range steps {
		steps[i] = map[string]any{"assign": map[string]any{"x": i}}
	}
	if _, err := executeSteps(context.Background(), steps, ""); err == nil {
		t.Fatal("expected oversized workflow to be rejected")
	}
}

func TestCancelExecutionCancelsInflightHTTPCall(t *testing.T) {
	original := noRedirectClient
	t.Cleanup(func() { noRedirectClient = original })
	started := make(chan struct{})
	stopped := make(chan struct{})
	noRedirectClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-req.Context().Done()
		close(stopped)
		return nil, req.Context().Err()
	})}

	api := newTestAPI()
	workflowName := "projects/p/locations/l/workflows/w"
	api.workflows[workflowName] = &Workflow{
		Name: workflowName, State: "ACTIVE", RevisionID: "1",
		SourceContents: `[{"call":{"call":"http.get","args":{"url":"http://localhost:8080/wait"}}}]`,
	}
	create := httptest.NewRequest(http.MethodPost, "/v1/"+workflowName+"/executions", bytes.NewBufferString(`{}`))
	createResponse := httptest.NewRecorder()
	api.ServeHTTP(createResponse, create)
	var execution Execution
	if err := json.Unmarshal(createResponse.Body.Bytes(), &execution); err != nil {
		t.Fatal(err)
	}
	<-started

	cancel := httptest.NewRequest(http.MethodPost, "/v1/"+execution.Name+":cancel", nil)
	cancelResponse := httptest.NewRecorder()
	api.ServeHTTP(cancelResponse, cancel)
	<-stopped

	api.mu.RLock()
	state := api.executions[execution.Name].State
	api.mu.RUnlock()
	if state != "CANCELLED" {
		t.Fatalf("state = %s", state)
	}
}

func TestExecuteCallDetectsOversizedResponse(t *testing.T) {
	original := noRedirectClient
	t.Cleanup(func() { noRedirectClient = original })
	noRedirectClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", maxHTTPResponseSize+1))),
		}, nil
	})}
	_, err := executeCall(context.Background(), map[string]any{
		"call": "http.get",
		"args": map[string]any{"url": "http://localhost:8080/large"},
	}, nil)
	if err == nil {
		t.Fatal("expected oversized response error")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type failingStore struct{}

func (failingStore) Load(string, any) error { return state.ErrNotFound }
func (failingStore) Save(string, any) error { return errors.New("save failed") }

type postCommitWorkflowStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	failNext bool
}

type ambiguousCancelWorkflowStore struct {
	mu           sync.Mutex
	data         map[string][]byte
	saves        int
	commitCancel bool
}

func (s *ambiguousCancelWorkflowStore) seed(t *testing.T, metadata workflowsMetadata) {
	t.Helper()
	raw, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.data[workflowsStateEntry] = raw
	s.mu.Unlock()
}

func (s *ambiguousCancelWorkflowStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *ambiguousCancelWorkflowStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.saves == 1 && s.commitCancel {
		s.data[name] = raw
	}
	return errors.New("injected ambiguous cancellation save")
}

func (s *postCommitWorkflowStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *postCommitWorkflowStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data[name] = raw
	if s.failNext {
		s.failNext = false
		return errors.New("post-commit save error")
	}
	return nil
}

type blockingWorkflowStore struct {
	mu      sync.Mutex
	data    map[string][]byte
	saves   int
	entered chan struct{}
	release chan struct{}
}

func (s *blockingWorkflowStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *blockingWorkflowStore) Save(name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.saves++
	save := s.saves
	s.mu.Unlock()
	if save == 1 {
		close(s.entered)
		<-s.release
	}
	s.mu.Lock()
	s.data[name] = raw
	s.mu.Unlock()
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// mockStore is a simple in-memory state store for testing.
type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return fmt.Errorf("not found: %w", state.ErrNotFound)
	}
	return json.Unmarshal(raw, target)
}

func (m *mockStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
