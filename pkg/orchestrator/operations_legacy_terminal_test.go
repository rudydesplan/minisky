package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLegacyTerminalPathsWaitForDurablePublication(t *testing.T) {
	for _, path := range legacyTerminalPaths() {
		t.Run(path.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, operation := registerLegacyOperation(t, store)
			observed := make(chan *Operation, 1)
			unsubscribe := manager.OnTerminal(func(operation *Operation) { observed <- operation })
			defer unsubscribe()
			store.blockTerminalSave(false)
			defer store.releaseTerminalSave()
			completed := make(chan error, 1)
			go func() { completed <- path.complete(manager, operation.Name, nil) }()

			store.waitForTerminalSave(t)
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertNoTerminalObservation(t, observed)

			store.releaseTerminalSave()
			if err := waitForFinalization(t, completed); err != nil {
				t.Fatal(err)
			}
			assertLegacyTerminalSuccess(t, manager.Get(operation.Name))
			select {
			case notified := <-observed:
				assertLegacyTerminalSuccess(t, notified)
			case <-time.After(time.Second):
				t.Fatal("legacy terminal observer was not notified")
			}
		})
	}
}

func TestLegacyTerminalPreCommitFailureStaysNonterminalAcrossRestart(t *testing.T) {
	for _, path := range legacyTerminalPaths() {
		t.Run(path.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, operation := registerLegacyOperation(t, store)
			observed := make(chan *Operation, 1)
			unsubscribe := manager.OnTerminal(func(operation *Operation) { observed <- operation })
			defer unsubscribe()
			store.blockTerminalSave(true)
			completed := make(chan error, 1)
			go func() { completed <- path.complete(manager, operation.Name, nil) }()

			store.waitForTerminalSave(t)
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertNoTerminalObservation(t, observed)
			store.releaseTerminalSave()
			err := waitForFinalization(t, completed)
			if path.reportsError && !errors.Is(err, errTerminalSave) {
				t.Fatalf("terminal error = %v, want %v", err, errTerminalSave)
			}
			if !path.reportsError && err != nil {
				t.Fatal(err)
			}
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertNoTerminalObservation(t, observed)
			if manager.PersistenceError() == nil {
				t.Fatal("terminal persistence failure was not sticky")
			}

			clearTerminalStoreFailure(store)
			restarted, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			interrupted := restarted.Get(operation.Name)
			if !isInterruptedOperation(interrupted) {
				t.Fatalf("restart operation = %+v, want stable interruption", interrupted)
			}
		})
	}
}

func TestLegacyTerminalExactReadbackPublishesOnce(t *testing.T) {
	for _, path := range legacyTerminalPaths() {
		t.Run(path.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, operation := registerLegacyOperation(t, store)
			var workCalls atomic.Int32
			observed := make(chan *Operation, 2)
			unsubscribe := manager.OnTerminal(func(operation *Operation) { observed <- operation })
			defer unsubscribe()
			store.failNextSaveAfterCommit(nil)

			if err := path.complete(manager, operation.Name, &workCalls); err != nil {
				t.Fatalf("exact committed readback error = %v", err)
			}
			assertLegacyTerminalSuccess(t, manager.Get(operation.Name))
			if manager.PersistenceError() == nil {
				t.Fatal("post-commit degradation was not retained")
			}
			select {
			case <-observed:
			case <-time.After(time.Second):
				t.Fatal("exact committed readback was not notified")
			}

			if err := path.complete(manager, operation.Name, &workCalls); err != nil {
				t.Fatalf("idempotent retry error = %v", err)
			}
			assertNoRetryNotification(t, observed)
			if path.name == "RunAsync" && workCalls.Load() != 1 {
				t.Fatalf("RunAsync retry work calls = %d, want 1", workCalls.Load())
			}
		})
	}
}

func TestMarkDoneAndRunAsyncPreserveTerminalBehavior(t *testing.T) {
	t.Run("MarkDone scoped response", func(t *testing.T) {
		manager := NewOperationManager()
		operation, err := manager.RegisterScopedDurable(OperationScope{
			ServiceKind: "clouddeploy#operation",
			Project:     "project-a",
			Location:    "us-central1",
			Target:      "projects/project-a/locations/us-central1/deliveryPipelines/p/releases/r/rollouts/roll1",
		}, "create")
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.AdvanceDurable(operation.Name, 85, StatusRunning); err != nil {
			t.Fatal(err)
		}
		manager.MarkDone(operation.Name)
		completed := manager.Get(operation.Name)
		assertLegacyTerminalSuccess(t, completed)
		if !strings.Contains(string(completed.Response), "google.cloud.deploy.v1.") ||
			!strings.Contains(string(completed.Response), operation.TargetLink) {
			t.Fatalf("MarkDone response = %s", completed.Response)
		}
	})

	t.Run("RunAsync work error", func(t *testing.T) {
		manager := NewOperationManager()
		operation := manager.Register("compute#operation", "insert", "instances/vm", "", "")
		manager.runAsyncWithPauses(operation.Name, func() error {
			return legacyCodedError{code: 429, message: "capacity unavailable"}
		}, func(time.Duration) {})
		completed := manager.Get(operation.Name)
		if completed == nil || !completed.Done || completed.Progress != 100 ||
			completed.Error == nil || completed.Error.Code != 429 ||
			completed.Error.Message != "capacity unavailable" {
			t.Fatalf("RunAsync failed operation = %+v", completed)
		}
	})

	t.Run("RunAsync cancellation", func(t *testing.T) {
		manager := NewOperationManager()
		operation := manager.Register("compute#operation", "insert", "instances/vm", "", "")
		manager.runAsyncWithPauses(operation.Name, func() error { return context.Canceled }, func(time.Duration) {})
		completed := manager.Get(operation.Name)
		if completed == nil || !completed.Done || completed.Error == nil ||
			completed.Error.Code != 500 || completed.Error.Message != context.Canceled.Error() {
			t.Fatalf("RunAsync cancelled operation = %+v", completed)
		}
	})
}

type legacyTerminalPath struct {
	name         string
	reportsError bool
	complete     func(*OperationManager, string, *atomic.Int32) error
}

func legacyTerminalPaths() []legacyTerminalPath {
	return []legacyTerminalPath{
		{
			name:         "AdvanceDurable",
			reportsError: true,
			complete: func(manager *OperationManager, name string, _ *atomic.Int32) error {
				return manager.AdvanceDurable(name, 100, StatusDone)
			},
		},
		{
			name: "Advance",
			complete: func(manager *OperationManager, name string, _ *atomic.Int32) error {
				manager.Advance(name, 100, StatusDone)
				return nil
			},
		},
		{
			name: "MarkDone",
			complete: func(manager *OperationManager, name string, _ *atomic.Int32) error {
				manager.MarkDone(name)
				return nil
			},
		},
		{
			name: "RunAsync",
			complete: func(manager *OperationManager, name string, workCalls *atomic.Int32) error {
				manager.runAsyncWithPauses(name, func() error {
					if workCalls != nil {
						workCalls.Add(1)
					}
					return nil
				}, func(time.Duration) {})
				return nil
			},
		},
	}
}

func registerLegacyOperation(
	t *testing.T,
	store *terminalPublicationStore,
) (*OperationManager, *Operation) {
	t.Helper()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := manager.RegisterDurable(
		"compute#operation",
		"insert",
		"projects/project-a/zones/us-central1-a/instances/vm-a",
		"us-central1-a",
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	return manager, operation
}

func assertLegacyTerminalSuccess(t *testing.T, operation *Operation) {
	t.Helper()
	if operation == nil || !operation.Done || operation.Status != StatusDone ||
		operation.Progress != 100 || operation.EndTime == "" || operation.Error != nil {
		t.Fatalf("legacy terminal operation = %+v", operation)
	}
}

func clearTerminalStoreFailure(store *terminalPublicationStore) {
	store.mu.Lock()
	store.failFromSave = 0
	store.commitFailure = false
	store.transform = nil
	store.mu.Unlock()
}

type legacyCodedError struct {
	code    int
	message string
}

func (err legacyCodedError) Error() string      { return err.message }
func (err legacyCodedError) OperationCode() int { return err.code }
