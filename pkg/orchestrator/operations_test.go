package orchestrator

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

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

type injectedOperationStore struct {
	mu         sync.Mutex
	data       []byte
	saveCount  int
	failOnSave map[int]error
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
