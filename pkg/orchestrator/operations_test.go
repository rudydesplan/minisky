package orchestrator

import (
	"testing"
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
