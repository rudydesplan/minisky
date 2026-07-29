package orchestrator

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommittedTerminalReadbackDoesNotTriggerCompensation(t *testing.T) {
	tests := terminalPublicationTests()
	for _, caller := range []struct {
		name       string
		test       terminalPublicationTest
		finalize   func(*OperationManager, string) error
		assertDone func(*testing.T, *Operation)
	}{
		{
			name:     "Compute-style FinalizeDurable",
			test:     tests[0],
			finalize: func(manager *OperationManager, name string) error { return manager.FinalizeDurable(name, 0, "") },
			assertDone: func(t *testing.T, operation *Operation) {
				t.Helper()
				if operation == nil || !operation.Done || operation.Error != nil {
					t.Fatalf("terminal operation = %+v", operation)
				}
			},
		},
		{
			name:       "CloudDeploy-style FinalizeScopedDurable",
			test:       tests[1],
			finalize:   tests[1].finalize,
			assertDone: tests[1].assertTerminal,
		},
	} {
		t.Run(caller.name, func(t *testing.T) {
			test := caller.test
			store := newTerminalPublicationStore()
			manager, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			operation := test.register(t, manager)
			store.failNextSaveAfterCommit(nil)
			var compensationCalls atomic.Int32

			if err := caller.finalize(manager, operation.Name); err != nil {
				compensationCalls.Add(1)
			}
			if compensationCalls.Load() != 0 {
				t.Fatal("committed terminal readback triggered caller compensation")
			}
			caller.assertDone(t, manager.Get(operation.Name))
			if manager.PersistenceError() == nil {
				t.Fatal("committed post-save failure did not retain degradation diagnostics")
			}
		})
	}
}

func TestOperationEqualityPreservesLargeIntegers(t *testing.T) {
	left := &Operation{
		Name:     "operations/large",
		Status:   StatusDone,
		Done:     true,
		Metadata: map[string]any{"value": json.Number("9007199254740992")},
	}
	right := cloneOperation(left)
	right.Metadata = map[string]any{"value": json.Number("9007199254740993")}

	equal, err := operationsExactlyEqual(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if equal {
		t.Fatal("operations with unequal integers above 2^53 compared equal")
	}
}

func TestUpdateMetadataRejectsUnsupportedWithoutMutationOrSave(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata any
	}{
		{name: "unsupported", metadata: make(chan int)},
		{name: "cyclic", metadata: cyclicOperationMetadata()},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			operation := terminalPublicationTests()[0].register(t, manager)
			if err := manager.UpdateMetadata(operation.Name, map[string]any{"value": "previous"}); err != nil {
				t.Fatal(err)
			}
			saveCount := terminalStoreSaveCount(store)

			if err := manager.UpdateMetadata(operation.Name, test.metadata); err == nil {
				t.Fatal("unsupported metadata update returned nil")
			}
			assertOperationMetadata(t, manager.Get(operation.Name), map[string]any{"value": "previous"})
			if got := terminalStoreSaveCount(store); got != saveCount {
				t.Fatalf("unsupported metadata save count = %d, want %d", got, saveCount)
			}
		})
	}
}

func TestUpdateMetadataNormalizesTypedNilAndMutableJSON(t *testing.T) {
	store := newTerminalPublicationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operation := terminalPublicationTests()[0].register(t, manager)
	leaf := &struct {
		Value json.Number `json:"value"`
	}{Value: json.Number("9007199254740993")}
	metadata := map[string]any{
		"map":     map[string]any{"value": "original"},
		"slice":   []any{"original"},
		"pointer": leaf,
	}
	expected := map[string]any{
		"map":     map[string]any{"value": "original"},
		"slice":   []any{"original"},
		"pointer": map[string]any{"value": json.Number("9007199254740993")},
	}
	if err := manager.UpdateMetadata(operation.Name, metadata); err != nil {
		t.Fatal(err)
	}
	metadata["map"].(map[string]any)["value"] = "mutated"
	metadata["slice"].([]any)[0] = "mutated"
	leaf.Value = json.Number("1")
	assertOperationMetadata(t, manager.Get(operation.Name), expected)

	var typedNil *publicationMetadataFixture
	if err := manager.UpdateMetadata(operation.Name, typedNil); err != nil {
		t.Fatal(err)
	}
	if got := manager.Get(operation.Name).Metadata; got != nil {
		t.Fatalf("typed nil metadata = %#v, want nil JSON null", got)
	}
	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.Get(operation.Name).Metadata; got != nil {
		t.Fatalf("restarted typed nil metadata = %#v, want nil", got)
	}
}

func TestUpdateMetadataMarshalsOutsideManagerLocks(t *testing.T) {
	store := newTerminalPublicationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	first := terminalPublicationTests()[0].register(t, manager)
	second := terminalPublicationTests()[0].register(t, manager)
	entered := make(chan struct{})
	release := make(chan struct{})
	metadata := &slowReentrantOperationMetadata{
		manager: manager,
		other:   second.Name,
		entered: entered,
		release: release,
	}
	updated := make(chan error, 1)
	go func() {
		updated <- manager.UpdateMetadata(first.Name, metadata)
	}()
	<-entered

	getDone := make(chan struct{})
	go func() {
		_ = manager.Get(first.Name)
		close(getDone)
	}()
	select {
	case <-getDone:
	case <-time.After(100 * time.Millisecond):
		close(release)
		t.Fatal("slow MarshalJSON held the operation map lock")
	}
	secondUpdated := make(chan error, 1)
	go func() {
		secondUpdated <- manager.UpdateMetadata(second.Name, map[string]any{"value": "second"})
	}()
	select {
	case err := <-secondUpdated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		close(release)
		t.Fatal("slow MarshalJSON held the persistence lock")
	}
	close(release)
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reentrant metadata update did not complete")
	}
}

func TestTerminalFinalizationIsExactlyOnce(t *testing.T) {
	for _, test := range terminalRetryTests() {
		t.Run(test.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			operation := test.register(t, manager)
			observed := make(chan *Operation, 2)
			unsubscribe := manager.OnTerminal(func(operation *Operation) {
				observed <- operation
			})
			defer unsubscribe()

			if err := test.finalize(manager, operation.Name, false); err != nil {
				t.Fatal(err)
			}
			select {
			case <-observed:
			case <-time.After(time.Second):
				t.Fatal("initial terminal notification was not delivered")
			}
			manager.mu.Lock()
			manager.ops[operation.Name].EndTime = "stable-end-time"
			manager.mu.Unlock()
			saveCount := terminalStoreSaveCount(store)

			if err := test.finalize(manager, operation.Name, false); err != nil {
				t.Fatalf("same terminal retry error = %v", err)
			}
			if got := manager.Get(operation.Name); got.EndTime != "stable-end-time" {
				t.Fatalf("same retry changed EndTime to %q", got.EndTime)
			}
			if got := terminalStoreSaveCount(store); got != saveCount {
				t.Fatalf("same retry save count = %d, want %d", got, saveCount)
			}
			assertNoRetryNotification(t, observed)

			before := manager.Get(operation.Name)
			if err := test.finalize(manager, operation.Name, true); !errors.Is(err, ErrOperationTerminalConflict) {
				t.Fatalf("conflicting terminal retry error = %v", err)
			}
			after := manager.Get(operation.Name)
			equal, err := operationsExactlyEqual(before, after)
			if err != nil {
				t.Fatal(err)
			}
			if !equal {
				t.Fatalf("conflicting retry mutated operation: before=%+v after=%+v", before, after)
			}
			if got := terminalStoreSaveCount(store); got != saveCount {
				t.Fatalf("conflicting retry save count = %d, want %d", got, saveCount)
			}
			assertNoRetryNotification(t, observed)
			if test.barrierCalls != nil && test.barrierCalls.Load() != 1 {
				t.Fatalf("barrier calls = %d, want 1", test.barrierCalls.Load())
			}
		})
	}
}

func TestPostCommitReconciliationRetryNotifiesExactlyOnce(t *testing.T) {
	store := newTerminalPublicationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	test := terminalPublicationTests()[1]
	operation := test.register(t, manager)
	observed := make(chan *Operation, 2)
	unsubscribe := manager.OnTerminal(func(operation *Operation) { observed <- operation })
	defer unsubscribe()
	store.failNextSaveAfterCommit(nil)

	if err := test.finalize(manager, operation.Name); err != nil {
		t.Fatalf("exact committed readback error = %v", err)
	}
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("reconciled terminal operation was not notified")
	}
	if err := test.finalize(manager, operation.Name); err != nil {
		t.Fatalf("reconciled terminal retry error = %v", err)
	}
	assertNoRetryNotification(t, observed)
}

func TestRestartInterruptedTerminalCanBeAuthoritativelyReconciled(t *testing.T) {
	store := newTerminalPublicationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operation := terminalPublicationTests()[1].register(t, manager)
	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if interrupted := restarted.Get(operation.Name); !isInterruptedOperation(interrupted) {
		t.Fatalf("restarted operation = %+v, want interruption marker", interrupted)
	}
	response := json.RawMessage(`{"name":"rollout-1"}`)
	if err := restarted.FinalizeScopedDurable(operation.Name, response, 0, ""); err != nil {
		t.Fatal(err)
	}
	completed := restarted.Get(operation.Name)
	if isInterruptedOperation(completed) || completed.Error != nil || !completed.Done {
		t.Fatalf("reconciled operation = %+v", completed)
	}
	if err := restarted.FinalizeScopedDurable(operation.Name, response, 0, ""); err != nil {
		t.Fatalf("idempotent reconciled retry error = %v", err)
	}
}

type slowReentrantOperationMetadata struct {
	manager *OperationManager
	other   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (metadata *slowReentrantOperationMetadata) MarshalJSON() ([]byte, error) {
	metadata.once.Do(func() { close(metadata.entered) })
	<-metadata.release
	if metadata.manager.Get(metadata.other) == nil {
		return nil, errors.New("reentrant operation lookup failed")
	}
	return []byte(`{"value":"first"}`), nil
}

type terminalRetryTest struct {
	name         string
	register     func(*testing.T, *OperationManager) *Operation
	finalize     func(*OperationManager, string, bool) error
	barrierCalls *atomic.Int32
}

func terminalRetryTests() []terminalRetryTest {
	publication := terminalPublicationTests()
	var barrierCalls atomic.Int32
	return []terminalRetryTest{
		{
			name:     "FinalizeDurable",
			register: publication[0].register,
			finalize: func(manager *OperationManager, name string, conflict bool) error {
				if conflict {
					return manager.FinalizeDurable(name, 500, "different")
				}
				return manager.FinalizeDurable(name, 503, "backend unavailable")
			},
		},
		{
			name:     "FinalizeScopedDurable",
			register: publication[1].register,
			finalize: func(manager *OperationManager, name string, conflict bool) error {
				response := json.RawMessage(`{"name":"rollout-1"}`)
				if conflict {
					response = json.RawMessage(`{"name":"rollout-2"}`)
				}
				return manager.FinalizeScopedDurable(name, response, 0, "")
			},
		},
		{
			name:     "FailDurable",
			register: publication[2].register,
			finalize: func(manager *OperationManager, name string, conflict bool) error {
				if conflict {
					return manager.FailDurable(name, 500, "different")
				}
				return manager.FailDurable(name, 403, "permission denied")
			},
		},
		{
			name:         "FinalizeScopedDurableWithBarrier",
			register:     publication[1].register,
			barrierCalls: &barrierCalls,
			finalize: func(manager *OperationManager, name string, conflict bool) error {
				response := json.RawMessage(`{"name":"rollout-1"}`)
				if conflict {
					response = json.RawMessage(`{"name":"rollout-2"}`)
				}
				return manager.FinalizeScopedDurableWithBarrier(name, response, 0, "", func() error {
					barrierCalls.Add(1)
					return nil
				})
			},
		},
	}
}

func assertNoRetryNotification(t *testing.T, observed <-chan *Operation) {
	t.Helper()
	select {
	case operation := <-observed:
		t.Fatalf("terminal retry emitted duplicate notification: %+v", operation)
	case <-time.After(10 * time.Millisecond):
	}
}

func cyclicOperationMetadata() map[string]any {
	metadata := map[string]any{}
	metadata["self"] = metadata
	return metadata
}

func terminalStoreSaveCount(store *terminalPublicationStore) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saveCount
}
