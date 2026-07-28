package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestOperationLifecycle(t *testing.T) {
	t.Parallel()

	manager := NewOperationManager()
	op := manager.Register("compute#operation", "insert", "/projects/demo/instances/vm-1", "us-central1-a", "")

	if op.Status != StatusPending || op.Done || op.Progress != 0 {
		t.Fatalf("unexpected initial operation state: %+v", op)
	}
	if manager.Get(op.Name) == nil {
		t.Fatal("registered operation could not be retrieved")
	}

	manager.Advance(op.Name, 25, StatusRunning)
	running := manager.Get(op.Name)
	if running.Status != StatusRunning || running.Progress != 25 || running.StartTime == "" {
		t.Fatalf("unexpected running operation state: %+v", running)
	}

	manager.MarkDone(op.Name)
	done := manager.Get(op.Name)
	if done.Status != StatusDone || !done.Done || done.Progress != 100 || done.EndTime == "" {
		t.Fatalf("unexpected completed operation state: %+v", done)
	}
}

func TestOperationFailure(t *testing.T) {
	t.Parallel()

	manager := NewOperationManager()
	op := manager.Register("cloudbuild#operation", "build", "", "", "us-central1")
	manager.Fail(op.Name, 500, "build failed")

	failed := manager.Get(op.Name)
	if failed.Status != StatusDone || !failed.Done || failed.Progress != 100 {
		t.Fatalf("unexpected failed operation state: %+v", failed)
	}
	if failed.Error == nil || failed.Error.Code != 500 || failed.Error.Message != "build failed" {
		t.Fatalf("unexpected operation error: %+v", failed.Error)
	}
}

func TestUnknownOperationUpdatesAreNoOps(t *testing.T) {
	t.Parallel()

	manager := NewOperationManager()
	manager.Advance("missing", 50, StatusRunning)
	manager.UpdateMetadata("missing", map[string]string{"key": "value"})
	manager.Fail("missing", 500, "failure")

	if operations := manager.List(); len(operations) != 0 {
		t.Fatalf("unknown updates created operations: %+v", operations)
	}
}

func TestOperationPollingAfterRestartReturnsStableInterruptedResult(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op := manager.Register("compute#operation", "insert", "/projects/demo/instances/vm-1", "us-central1-a", "")
	manager.Advance(op.Name, 25, StatusRunning)

	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	interrupted := restarted.Get(op.Name)
	if interrupted == nil || interrupted.Status != StatusDone || !interrupted.Done || interrupted.Progress != 100 {
		t.Fatalf("unexpected interrupted operation: %+v", interrupted)
	}
	if interrupted.Error == nil || interrupted.Error.Code != 500 ||
		!strings.Contains(interrupted.Error.Message, "interrupted by MiniSky restart") {
		t.Fatalf("unexpected interrupted error: %+v", interrupted.Error)
	}

	restartedAgain, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	stable := restartedAgain.Get(op.Name)
	if stable == nil || stable.Error == nil || stable.Error.Message != interrupted.Error.Message {
		t.Fatalf("interrupted result was not stable: first=%+v second=%+v", interrupted, stable)
	}
}

func TestRegisterDurableRollsBackWhenInitialSaveFails(t *testing.T) {
	store := &injectedOperationStore{failOnSave: map[int]error{1: errors.New("disk full")}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}

	op, err := manager.RegisterDurable("compute#operation", "insert", "/projects/demo/instances/vm-1", "", "")
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("RegisterDurable error = %v, want disk full", err)
	}
	if op != nil {
		t.Fatalf("RegisterDurable operation = %+v, want nil", op)
	}
	if operations := manager.List(); len(operations) != 0 {
		t.Fatalf("failed registration remained in memory: %+v", operations)
	}
	if manager.PersistenceError() == nil {
		t.Fatal("failed registration did not mark persistence degraded")
	}
}

func TestRegisterDurableCompensatesPostCommitFailure(t *testing.T) {
	store := &postCommitOperationStore{failOnSave: map[int]error{
		1: errors.New("post-commit registration failure"),
	}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterScopedTargetDurable(
		"filestore#operation", "update",
		"projects/demo/locations/us/instances/i",
	)
	if err == nil || op != nil {
		t.Fatalf("registration = (%+v, %v), want compensated failure", op, err)
	}
	if len(manager.List()) != 0 {
		t.Fatalf("failed registration remained visible: %#v", manager.List())
	}
	restarted, restartErr := NewOperationManagerWithStore(store)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	if len(restarted.List()) != 0 {
		t.Fatalf("failed registration remained durable: %#v", restarted.List())
	}
	if manager.PersistenceError() == nil {
		t.Fatal("ambiguous registration failure did not remain degraded")
	}
}

func TestScopedOperationPollingRejectsWrongServiceParentAndTarget(t *testing.T) {
	t.Parallel()

	manager := NewOperationManager()
	scope := OperationScope{
		ServiceKind: "workflows#operation",
		Project:     "project-a",
		Location:    "us-central1",
		Target:      "projects/project-a/locations/us-central1/workflows/flow",
	}
	op, err := manager.RegisterScopedDurable(scope, "create")
	if err != nil {
		t.Fatal(err)
	}
	if op.Name == "" || !strings.HasPrefix(op.Name, "projects/project-a/locations/us-central1/operations/") {
		t.Fatalf("scoped operation name = %q", op.Name)
	}

	for name, wrong := range map[string]OperationScope{
		"service":  {ServiceKind: "batch#operation", Project: scope.Project, Location: scope.Location},
		"project":  {ServiceKind: scope.ServiceKind, Project: "project-b", Location: scope.Location},
		"location": {ServiceKind: scope.ServiceKind, Project: scope.Project, Location: "europe-west1"},
		"target": {
			ServiceKind: scope.ServiceKind,
			Project:     scope.Project,
			Location:    scope.Location,
			Target:      "projects/project-a/locations/us-central1/workflows/other",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := manager.GetScoped(op.Name, wrong); !errors.Is(err, ErrOperationNotFound) || got != nil {
				t.Fatalf("GetScoped() = (%+v, %v), want nil NOT_FOUND", got, err)
			}
		})
	}
	if got, err := manager.GetScoped(op.Name, scope); err != nil || got == nil {
		t.Fatalf("GetScoped(correct) = (%+v, %v)", got, err)
	}
}

func TestOperationPathScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantName     string
		wantProject  string
		wantLocation string
	}{
		{
			name:         "service project location operation",
			path:         "/v1/projects/project-a/locations/us-central1/operations/op-1",
			wantName:     "projects/project-a/locations/us-central1/operations/op-1",
			wantProject:  "project-a",
			wantLocation: "us-central1",
		},
		{
			name:         "canonical name without service prefix",
			path:         "projects/project-a/locations/us-central1/operations/op-1",
			wantName:     "projects/project-a/locations/us-central1/operations/op-1",
			wantProject:  "project-a",
			wantLocation: "us-central1",
		},
		{
			name:         "regional operation",
			path:         "/v1/projects/project-a/regions/us-central1/operations/op-1",
			wantName:     "projects/project-a/regions/us-central1/operations/op-1",
			wantProject:  "project-a",
			wantLocation: "us-central1",
		},
		{
			name:         "zonal operation",
			path:         "/v1/projects/project-a/zones/us-central1-a/operations/op-1",
			wantName:     "projects/project-a/zones/us-central1-a/operations/op-1",
			wantProject:  "project-a",
			wantLocation: "us-central1-a",
		},
		{
			name:        "project global operation",
			path:        "/v1/projects/project-a/operations/op-1",
			wantName:    "projects/project-a/operations/op-1",
			wantProject: "project-a",
		},
		{
			name:         "location operation without project",
			path:         "/v1/locations/us-central1/operations/op-1",
			wantName:     "operations/op-1",
			wantLocation: "us-central1",
		},
		{
			name:     "global operation",
			path:     "/v1/operations/op-1",
			wantName: "operations/op-1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name, project, location := operationPathScope(test.path)
			if name != test.wantName || project != test.wantProject || location != test.wantLocation {
				t.Fatalf("operationPathScope(%q) = (%q, %q, %q), want (%q, %q, %q)",
					test.path, name, project, location,
					test.wantName, test.wantProject, test.wantLocation)
			}
		})
	}
}

func TestOperationPathScopeRejectsMalformedPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "slashes only", path: "///"},
		{name: "missing operation name", path: "/v1/projects/project-a/locations/us/operations"},
		{name: "empty project", path: "/v1/projects//locations/us/operations/op-1"},
		{name: "empty location", path: "/v1/projects/project-a/locations//operations/op-1"},
		{name: "empty operation name", path: "/v1/projects/project-a/locations/us/operations/"},
		{name: "extra segment before operations", path: "/v1/projects/project-a/locations/us/resources/r/operations/op-1"},
		{name: "extra segment after operation name", path: "/v1/projects/project-a/locations/us/operations/op-1/extra"},
		{name: "wrong collection", path: "/v1/projects/project-a/locations/us/tasks/op-1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name, project, location := operationPathScope(test.path)
			if name != "" || project != "" || location != "" {
				t.Fatalf("operationPathScope(%q) = (%q, %q, %q), want rejection",
					test.path, name, project, location)
			}
		})
	}
}

func TestPollScopedRejectsCrossScopeLegacyOperationPaths(t *testing.T) {
	t.Parallel()

	manager := NewOperationManager()
	op := manager.Register(
		"workflows#operation",
		"create",
		"projects/project-a/locations/us-central1/workflows/flow",
		"",
		"",
	)
	correctPath := "/v1/projects/project-a/locations/us-central1/operations/" + op.Name
	if got, err := manager.PollScoped(correctPath, "workflows#operation"); err != nil || got == nil {
		t.Fatalf("PollScoped(correct path) = (%+v, %v)", got, err)
	}

	tests := []struct {
		name string
		path string
	}{
		{name: "omitted project and location", path: "/v1/operations/" + op.Name},
		{name: "omitted location", path: "/v1/projects/project-a/operations/" + op.Name},
		{name: "wrong project", path: "/v1/projects/project-b/locations/us-central1/operations/" + op.Name},
		{name: "wrong location", path: "/v1/projects/project-a/locations/europe-west1/operations/" + op.Name},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := manager.PollScoped(test.path, "workflows#operation")
			if !errors.Is(err, ErrOperationNotFound) || got != nil {
				t.Fatalf("PollScoped(%q) = (%+v, %v), want nil NOT_FOUND", test.path, got, err)
			}
		})
	}
}

func TestScopedOperationPersistsTerminalResponseAndError(t *testing.T) {
	store := &injectedOperationStore{}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := OperationScope{
		ServiceKind: "documentai#operation",
		Project:     "demo",
		Location:    "us",
		Target:      "projects/demo/locations/us/processors/p",
	}
	success, err := manager.RegisterScopedDurable(scope, "create")
	if err != nil {
		t.Fatal(err)
	}
	response := json.RawMessage(`{"name":"projects/demo/locations/us/processors/p"}`)
	if err := manager.FinalizeScopedDurable(success.Name, response, 0, ""); err != nil {
		t.Fatal(err)
	}
	failure, err := manager.RegisterScopedDurable(scope, "delete")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.FinalizeScopedDurable(failure.Name, nil, 501, "backend unavailable"); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	succeeded, err := restarted.GetScoped(success.Name, scope)
	var persistedResponse map[string]any
	if decodeErr := json.Unmarshal(succeeded.Response, &persistedResponse); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if err != nil ||
		persistedResponse["@type"] != "type.googleapis.com/google.cloud.documentai.v1.Processor" ||
		persistedResponse["name"] != "projects/demo/locations/us/processors/p" ||
		succeeded.Error != nil {
		t.Fatalf("persisted success = (%+v, %v)", succeeded, err)
	}
	failed, err := restarted.GetScoped(failure.Name, scope)
	if err != nil || failed.Error == nil || failed.Error.Code != 12 {
		t.Fatalf("persisted failure = (%+v, %v)", failed, err)
	}
}

func TestRollbackScopedRegistrationCompensatesDurableStore(t *testing.T) {
	store := &injectedOperationStore{}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := OperationScope{
		ServiceKind: "eventarc#operation",
		Project:     "demo",
		Location:    "us",
		Target:      "projects/demo/locations/us/triggers/t",
	}
	op, err := manager.RegisterScopedDurable(scope, "create")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RollbackScopedRegistration(op.Name); err != nil {
		t.Fatal(err)
	}
	if manager.Get(op.Name) != nil {
		t.Fatal("rolled-back operation remained in memory")
	}
	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Get(op.Name) != nil {
		t.Fatal("rolled-back operation remained durable")
	}
}

func TestRollbackScopedRegistrationReconcilesPostCommitFailure(t *testing.T) {
	store := &postCommitOperationStore{failOnSave: map[int]error{
		2: errors.New("post-commit rollback failure"),
	}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterScopedTargetDurable(
		"eventarc#operation", "update",
		"projects/demo/locations/us/triggers/t",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RollbackScopedRegistration(op.Name); err == nil {
		t.Fatal("ambiguous rollback failure was not reported")
	}
	if manager.Get(op.Name) != nil {
		t.Fatalf("readback-confirmed deletion was restored in memory: %#v", manager.Get(op.Name))
	}
	restarted, restartErr := NewOperationManagerWithStore(store)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	if restarted.Get(op.Name) != nil {
		t.Fatalf("readback-confirmed deletion remained durable: %#v", restarted.Get(op.Name))
	}
	if manager.PersistenceError() == nil {
		t.Fatal("ambiguous rollback failure did not remain degraded")
	}
}

func TestRunningPersistenceFailureIsObservableAndWorkContinues(t *testing.T) {
	store := &injectedOperationStore{failOnSave: map[int]error{2: errors.New("running save failed")}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterDurable("compute#operation", "insert", "", "", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := manager.AdvanceDurable(op.Name, 25, StatusRunning); err == nil {
		t.Fatal("AdvanceDurable returned nil for injected running save failure")
	}
	running := manager.Get(op.Name)
	if running == nil || running.Status != StatusRunning || running.Done {
		t.Fatalf("unexpected running state: %+v", running)
	}
	if manager.PersistenceError() == nil ||
		!strings.Contains(manager.PersistenceError().Error(), "running save failed") {
		t.Fatalf("persistence error = %v", manager.PersistenceError())
	}

	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
		t.Fatal(err)
	}
	if done := manager.Get(op.Name); done == nil || !done.Done || done.Error != nil {
		t.Fatalf("unexpected terminal state after recovered persistence: %+v", done)
	}
}

func TestSuccessfulTerminalPersistenceFailureBecomesInProcessError(t *testing.T) {
	store := &injectedOperationStore{failOnSave: map[int]error{2: errors.New("terminal save failed")}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterDurable("compute#operation", "insert", "", "", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err == nil {
		t.Fatal("AdvanceDurable returned nil for injected terminal save failure")
	}
	done := manager.Get(op.Name)
	if done == nil || !done.Done || done.Error == nil ||
		!strings.Contains(done.Error.Message, "terminal state persistence failed") {
		t.Fatalf("terminal persistence failure is not observable: %+v", done)
	}
}

func TestFailedTerminalPersistenceFailurePreservesWorkError(t *testing.T) {
	store := &injectedOperationStore{failOnSave: map[int]error{2: errors.New("terminal save failed")}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterDurable("cloudbuild#operation", "build", "", "", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := manager.FailDurable(op.Name, 503, "backend unavailable"); err == nil {
		t.Fatal("FailDurable returned nil for injected terminal save failure")
	}
	failed := manager.Get(op.Name)
	if failed == nil || failed.Error == nil ||
		!strings.Contains(failed.Error.Message, "backend unavailable") ||
		!strings.Contains(failed.Error.Message, "terminal state persistence failed") {
		t.Fatalf("work and persistence errors were not preserved: %+v", failed)
	}
}

func TestRestartAfterTerminalPersistenceFailureReportsInterruptionWithoutReplay(t *testing.T) {
	store := &injectedOperationStore{failOnSave: map[int]error{2: errors.New("terminal save failed")}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterDurable("compute#operation", "insert", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err == nil {
		t.Fatal("AdvanceDurable returned nil for injected terminal save failure")
	}

	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	polled := restarted.Get(op.Name)
	if polled == nil || !polled.Done || polled.Error == nil ||
		!strings.Contains(polled.Error.Message, "interrupted by MiniSky restart") {
		t.Fatalf("restart polling result = %+v", polled)
	}
}

func TestRemoveDurableDeletesOperationAcrossRestart(t *testing.T) {
	store := &injectedOperationStore{}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterDurable("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveDurable(op.Name); err == nil {
		t.Fatal("RemoveDurable removed a pending operation")
	}
	if manager.Get(op.Name) == nil {
		t.Fatal("pending operation disappeared after rejected removal")
	}
	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveDurable(op.Name); err != nil {
		t.Fatal(err)
	}
	if manager.Get(op.Name) != nil {
		t.Fatalf("removed operation remained in memory: %+v", manager.Get(op.Name))
	}
	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Get(op.Name) != nil {
		t.Fatalf("removed operation returned after restart: %+v", restarted.Get(op.Name))
	}
}

func TestRemoveDurableRestoresOperationWhenSaveFails(t *testing.T) {
	store := &injectedOperationStore{failOnSave: map[int]error{2: errors.New("remove failed")}}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterDurable("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RemoveDurable(op.Name); err == nil {
		t.Fatal("RemoveDurable returned nil for injected save failure")
	}
	if manager.Get(op.Name) == nil {
		t.Fatal("failed durable removal did not restore operation")
	}
}

func TestScopedOperationResponseUsesTypedAnyAndCanonicalRPCCode(t *testing.T) {
	manager := NewOperationManager()
	op, err := manager.RegisterScopedTargetDurable(
		"workflows#operation", "update",
		"projects/demo/locations/us/workflows/flow",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.FinalizeScopedDurable(op.Name, nil, http.StatusInternalServerError, "failed"); err != nil {
		t.Fatal(err)
	}

	persisted := manager.Get(op.Name)
	if persisted.Error == nil || persisted.Error.Code != 13 {
		t.Fatalf("persisted error = %#v, want canonical INTERNAL code 13", persisted.Error)
	}
	response := ScopedOperationResponse(persisted)
	metadata := response["metadata"].(map[string]any)
	if metadata["@type"] != "type.googleapis.com/google.cloud.workflows.v1.OperationMetadata" {
		t.Fatalf("metadata @type = %#v", metadata["@type"])
	}
	operationError := response["error"].(*OperationError)
	if operationError.Code != 13 {
		t.Fatalf("serialized error code = %d, want 13", operationError.Code)
	}
}

func TestScopedSuccessPersistsTypedTerminalResponse(t *testing.T) {
	store := &injectedOperationStore{}
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op, err := manager.RegisterScopedTargetDurable(
		"apigateway#operation", "create",
		"projects/demo/locations/us/gateways/gateway",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	response := ScopedOperationResponse(restarted.Get(op.Name))
	typed, ok := response["response"].(map[string]any)
	if !ok || typed["@type"] != "type.googleapis.com/google.cloud.apigateway.v1.Gateway" {
		t.Fatalf("terminal response = %#v", response["response"])
	}
}

func TestTerminalObserverPanicDoesNotStopLaterListeners(t *testing.T) {
	manager := NewOperationManager()
	unsubscribePanic := manager.OnTerminal(func(*Operation) { panic("observer panic") })
	defer unsubscribePanic()
	observed := make(chan string, 1)
	unsubscribeObserved := manager.OnTerminal(func(operation *Operation) { observed <- operation.Name })
	defer unsubscribeObserved()
	op := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")

	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case name := <-observed:
		if name != op.Name {
			t.Fatalf("observed operation = %q, want %q", name, op.Name)
		}
	case <-time.After(time.Second):
		t.Fatal("listener after panic was not invoked")
	}
}

func TestBlockingTerminalObserverDoesNotBlockOperationCompletion(t *testing.T) {
	manager := NewOperationManager()
	entered := make(chan struct{})
	release := make(chan struct{})
	blocked := manager.ObserveTerminal(func(*Operation) {
		close(entered)
		<-release
	})
	observed := make(chan struct{})
	other := manager.ObserveTerminal(func(*Operation) { close(observed) })
	op := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
	completed := make(chan error, 1)
	go func() {
		completed <- manager.AdvanceDurable(op.Name, 100, StatusDone)
	}()

	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation completion blocked on terminal observer")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking observer was not dispatched")
	}
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("blocking observer starved another listener")
	}
	close(release)
	if err := blocked.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := other.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalObserverPreservesPerListenerOrder(t *testing.T) {
	manager := NewOperationManager()
	observed := make(chan string)
	subscription := manager.ObserveTerminal(func(operation *Operation) { observed <- operation.Name })
	defer subscription.Shutdown(context.Background())
	for range 3 {
		op := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
		if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
			t.Fatal(err)
		}
		select {
		case name := <-observed:
			if name != op.Name {
				t.Fatalf("observed operation = %q, want %q", name, op.Name)
			}
		case <-time.After(time.Second):
			t.Fatal("terminal observer did not preserve delivery order")
		}
	}
}

func TestTerminalObserverCoalescesWakeupsWhenBlocked(t *testing.T) {
	manager := NewOperationManager()
	entered := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan string, 3)
	subscription := manager.ObserveTerminal(func(operation *Operation) {
		observed <- operation.Name
		select {
		case <-entered:
		default:
			close(entered)
			<-release
		}
	})
	defer subscription.Shutdown(context.Background())
	first := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
	if err := manager.AdvanceDurable(first.Name, 100, StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking observer was not dispatched")
	}

	const notifications = 100
	var latest string
	for index := 0; index < notifications; index++ {
		op := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
		latest = op.Name
		if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	var firstObserved, latestObserved string
	select {
	case firstObserved = <-observed:
	case <-time.After(time.Second):
		t.Fatal("coalesced listener did not receive first wakeup")
	}
	select {
	case latestObserved = <-observed:
	case <-time.After(time.Second):
		t.Fatal("coalesced listener did not receive latest wakeup")
	}
	if firstObserved != first.Name || latestObserved != latest {
		t.Fatalf("coalesced notifications = [%s %s], want [%s %s]",
			firstObserved, latestObserved, first.Name, latest)
	}
	select {
	case extra := <-observed:
		t.Fatalf("coalesced observer received extra notification %q", extra)
	default:
	}
}

func TestTerminalObserverUnsubscribeIsIdempotent(t *testing.T) {
	manager := NewOperationManager()
	called := make(chan struct{}, 1)
	unsubscribe := manager.OnTerminal(func(*Operation) { called <- struct{}{} })
	unsubscribe()
	unsubscribe()
	op := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
		t.Fatal("unsubscribed terminal observer was invoked")
	case <-time.After(25 * time.Millisecond):
	}
	manager.observerMu.Lock()
	defer manager.observerMu.Unlock()
	if len(manager.terminalObservers) != 0 || len(manager.observerOrder) != 0 {
		t.Fatalf("observer cleanup left registrations: observers=%d order=%d",
			len(manager.terminalObservers), len(manager.observerOrder))
	}
}

func TestTerminalObserverShutdownIsContextBounded(t *testing.T) {
	manager := NewOperationManager()
	entered := make(chan struct{})
	release := make(chan struct{})
	subscription := manager.ObserveTerminal(func(*Operation) {
		close(entered)
		<-release
	})
	op := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
	if err := manager.AdvanceDurable(op.Name, 100, StatusDone); err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := subscription.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want context deadline exceeded", err)
	}
	close(release)
	if err := subscription.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type injectedOperationStore struct {
	mu         sync.Mutex
	data       []byte
	saveCount  int
	failOnSave map[int]error
}

type postCommitOperationStore struct {
	mu         sync.Mutex
	data       []byte
	saveCount  int
	failOnSave map[int]error
}

func (s *postCommitOperationStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *postCommitOperationStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCount++
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data = data
	return s.failOnSave[s.saveCount]
}

func (s *injectedOperationStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *injectedOperationStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCount++
	if err := s.failOnSave[s.saveCount]; err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data = data
	return nil
}
