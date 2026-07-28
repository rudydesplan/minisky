package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestFinalizeScopedDurableWithBarrierHidesTerminalUntilBarrier(t *testing.T) {
	store := newBarrierOperationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op := registerBarrierOperation(t, manager, "cache-a")
	store.blockSave(2)
	barrierEntered := make(chan struct{})
	releaseBarrier := make(chan struct{})
	finalized := make(chan error, 1)
	go func() {
		finalized <- manager.FinalizeScopedDurableWithBarrier(
			op.Name,
			json.RawMessage(`{"name":"cache-a"}`),
			0,
			"",
			func() error {
				close(barrierEntered)
				<-releaseBarrier
				return nil
			},
		)
	}()

	<-store.saveStarted
	assertBarrierOperationNonterminal(t, manager, op)
	select {
	case <-barrierEntered:
		t.Fatal("barrier ran before terminal snapshot persistence completed")
	default:
	}

	close(store.releaseSave)
	<-barrierEntered
	assertBarrierOperationNonterminal(t, manager, op)
	durable := store.operations(t)
	if terminal := durable[op.Name]; terminal == nil || !terminal.Done || terminal.Status != StatusDone {
		t.Fatalf("hidden durable terminal operation = %+v", terminal)
	}

	close(releaseBarrier)
	if err := <-finalized; err != nil {
		t.Fatal(err)
	}
	if visible := manager.Get(op.Name); visible == nil || !visible.Done || visible.Status != StatusDone {
		t.Fatalf("published operation = %+v", visible)
	}
}

func TestFinalizeScopedDurableWithBarrierSerializesConcurrentSave(t *testing.T) {
	store := newBarrierOperationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op := registerBarrierOperation(t, manager, "cache-a")
	barrierEntered := make(chan struct{})
	releaseBarrier := make(chan struct{})
	finalized := make(chan error, 1)
	go func() {
		finalized <- manager.FinalizeScopedDurableWithBarrier(
			op.Name, json.RawMessage(`{"name":"cache-a"}`), 0, "",
			func() error {
				close(barrierEntered)
				<-releaseBarrier
				return nil
			},
		)
	}()
	<-barrierEntered

	registered := make(chan error, 1)
	go func() {
		_, registerErr := manager.RegisterScopedDurable(OperationScope{
			ServiceKind: "memcache#operation",
			Project:     "project-a",
			Location:    "us-central1",
			Target:      "projects/project-a/locations/us-central1/instances/cache-b",
		}, "create")
		registered <- registerErr
	}()
	select {
	case err := <-registered:
		t.Fatalf("concurrent durable registration escaped barrier serialization: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseBarrier)
	if err := <-finalized; err != nil {
		t.Fatal(err)
	}
	if err := <-registered; err != nil {
		t.Fatal(err)
	}
	durable := store.operations(t)
	if terminal := durable[op.Name]; terminal == nil || !terminal.Done {
		t.Fatalf("concurrent save overwrote hidden terminal snapshot: %+v", terminal)
	}
}

func TestFinalizeScopedDurableWithBarrierReleasesCallerLockBeforeDone(t *testing.T) {
	store := newBarrierOperationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op := registerBarrierOperation(t, manager, "cache-a")
	var callerLock sync.Mutex
	callerLock.Lock()
	finalized := make(chan error, 1)
	go func() {
		finalized <- manager.FinalizeScopedDurableWithBarrier(
			op.Name, nil, 13, "backend failed",
			func() error {
				callerLock.Unlock()
				return nil
			},
		)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		visible := manager.Get(op.Name)
		if visible != nil && visible.Done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal operation was not published")
		}
		time.Sleep(time.Millisecond)
	}
	lockAcquired := make(chan struct{})
	go func() {
		callerLock.Lock()
		close(lockAcquired)
		callerLock.Unlock()
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("Done became visible before caller mutation lock was released")
	}
	if err := <-finalized; err != nil {
		t.Fatal(err)
	}
}

func TestFinalizeScopedDurableWithBarrierSaveFailureSkipsBarrierAndPublish(t *testing.T) {
	store := newBarrierOperationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op := registerBarrierOperation(t, manager, "cache-a")
	store.failSave(2, errors.New("terminal disk failure"))
	var barrierCalls atomic.Int32
	var observerCalls atomic.Int32
	unsubscribe := manager.OnTerminal(func(*Operation) { observerCalls.Add(1) })
	defer unsubscribe()

	err = manager.FinalizeScopedDurableWithBarrier(
		op.Name, json.RawMessage(`{"name":"cache-a"}`), 0, "",
		func() error {
			barrierCalls.Add(1)
			return nil
		},
	)
	if err == nil || !errors.Is(err, store.failErr) {
		t.Fatalf("finalize error = %v", err)
	}
	if barrierCalls.Load() != 0 {
		t.Fatalf("barrier calls = %d", barrierCalls.Load())
	}
	assertBarrierOperationNonterminal(t, manager, op)
	time.Sleep(20 * time.Millisecond)
	if observerCalls.Load() != 0 {
		t.Fatalf("observer calls = %d", observerCalls.Load())
	}
}

func TestFinalizeScopedDurableWithBarrierFailurePublishesDurableTruth(t *testing.T) {
	store := newBarrierOperationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op := registerBarrierOperation(t, manager, "cache-a")
	barrierErr := errors.New("association cleanup failed")
	var callerLock sync.Mutex
	callerLock.Lock()
	observed := make(chan *Operation, 1)
	unsubscribe := manager.OnTerminal(func(operation *Operation) { observed <- operation })
	defer unsubscribe()

	err = manager.FinalizeScopedDurableWithBarrier(
		op.Name, json.RawMessage(`{"name":"cache-a"}`), 0, "",
		func() error {
			defer callerLock.Unlock()
			return barrierErr
		},
	)
	if !errors.Is(err, ErrOperationTerminalBarrier) || !errors.Is(err, barrierErr) {
		t.Fatalf("barrier failure = %v", err)
	}
	if visible := manager.Get(op.Name); visible == nil || !visible.Done {
		t.Fatalf("durable terminal state was not published: %+v", visible)
	}
	restarted, restartErr := NewOperationManagerWithStore(store)
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	if durable := restarted.Get(op.Name); durable == nil || !durable.Done {
		t.Fatalf("terminal operation was not durable after barrier failure: %+v", durable)
	}
	callerLock.Lock()
	callerLock.Unlock()
	select {
	case operation := <-observed:
		if !operation.Done || manager.Get(op.Name) == nil || !manager.Get(op.Name).Done {
			t.Fatalf("observer ran before visible publication: %+v", operation)
		}
	case <-time.After(time.Second):
		t.Fatal("observer was not notified after barrier failure publication")
	}
}

func TestFinalizeScopedDurableWithBarrierObserverRunsAfterBarrierAndPublish(t *testing.T) {
	store := newBarrierOperationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	op := registerBarrierOperation(t, manager, "cache-a")
	var barrierComplete atomic.Bool
	observed := make(chan error, 1)
	subscription := manager.ObserveTerminal(func(operation *Operation) {
		if !barrierComplete.Load() {
			observed <- errors.New("observer ran before barrier")
			return
		}
		visible := manager.Get(operation.Name)
		if visible == nil || !visible.Done || !operation.Done {
			observed <- fmt.Errorf("observer saw unpublished operation: callback=%+v visible=%+v", operation, visible)
			return
		}
		observed <- nil
	})
	defer subscription.Shutdown(context.Background())

	if err := manager.FinalizeScopedDurableWithBarrier(
		op.Name, json.RawMessage(`{"name":"cache-a"}`), 0, "",
		func() error {
			barrierComplete.Store(true)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-observed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer was not notified")
	}
}

func TestFinalizeScopedDurableWithBarrierValidatesOperationState(t *testing.T) {
	manager := NewOperationManager()
	var barrierCalls atomic.Int32
	barrier := func() error {
		barrierCalls.Add(1)
		return nil
	}
	if err := manager.FinalizeScopedDurableWithBarrier("missing", nil, 0, "", barrier); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("missing operation error = %v", err)
	}
	unscoped := manager.Register("compute#operation", "insert", "instances/vm", "", "")
	if err := manager.FinalizeScopedDurableWithBarrier(unscoped.Name, nil, 0, "", barrier); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("unscoped operation error = %v", err)
	}
	scoped := registerBarrierOperation(t, manager, "cache-a")
	if err := manager.FinalizeScopedDurableWithBarrier(scoped.Name, nil, 0, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := manager.FinalizeScopedDurableWithBarrier(scoped.Name, nil, 0, "", barrier); err == nil {
		t.Fatal("already-terminal operation was finalized again")
	}
	if barrierCalls.Load() != 0 {
		t.Fatalf("barrier calls for invalid operations = %d", barrierCalls.Load())
	}
}

func registerBarrierOperation(t *testing.T, manager *OperationManager, target string) *Operation {
	t.Helper()
	op, err := manager.RegisterScopedDurable(OperationScope{
		ServiceKind: "memcache#operation",
		Project:     "project-a",
		Location:    "us-central1",
		Target:      "projects/project-a/locations/us-central1/instances/" + target,
	}, "create")
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func assertBarrierOperationNonterminal(t *testing.T, manager *OperationManager, operation *Operation) {
	t.Helper()
	visible := manager.Get(operation.Name)
	if visible == nil || visible.Done || visible.Status == StatusDone {
		t.Fatalf("operation became terminal before publication: %+v", visible)
	}
	_, operationID := splitOperationName(operation.Name)
	polled, err := manager.PollScoped(
		"/v1/projects/project-a/locations/us-central1/operations/"+operationID,
		"memcache#operation",
	)
	if err != nil {
		t.Fatal(err)
	}
	if polled.Done || polled.Status == StatusDone {
		t.Fatalf("polled operation became terminal before publication: %+v", polled)
	}
}

type barrierOperationStore struct {
	mu          sync.Mutex
	data        []byte
	saveCount   int
	blockOnSave int
	saveStarted chan struct{}
	releaseSave chan struct{}
	failOnSave  int
	failErr     error
}

func newBarrierOperationStore() *barrierOperationStore {
	return &barrierOperationStore{
		saveStarted: make(chan struct{}),
		releaseSave: make(chan struct{}),
	}
}

func (store *barrierOperationStore) blockSave(number int) {
	store.mu.Lock()
	store.blockOnSave = number
	store.mu.Unlock()
}

func (store *barrierOperationStore) failSave(number int, err error) {
	store.mu.Lock()
	store.failOnSave = number
	store.failErr = err
	store.mu.Unlock()
}

func (store *barrierOperationStore) Load(_ string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.data, target)
}

func (store *barrierOperationStore) Save(_ string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	store.mu.Lock()
	store.saveCount++
	saveNumber := store.saveCount
	block := saveNumber == store.blockOnSave
	fail := saveNumber == store.failOnSave
	failErr := store.failErr
	store.mu.Unlock()
	if block {
		close(store.saveStarted)
		<-store.releaseSave
	}
	if fail {
		return failErr
	}
	store.mu.Lock()
	store.data = append(store.data[:0], payload...)
	store.mu.Unlock()
	return nil
}

func (store *barrierOperationStore) operations(t *testing.T) map[string]*Operation {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	var operations map[string]*Operation
	if err := json.Unmarshal(store.data, &operations); err != nil {
		t.Fatal(err)
	}
	return operations
}
