package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"minisky/pkg/state"
)

const operationStateEntry = "orchestrator/operations"

// OperationStatus mirrors GCP's LRO status strings.
type OperationStatus string

const (
	StatusPending OperationStatus = "PENDING"
	StatusRunning OperationStatus = "RUNNING"
	StatusDone    OperationStatus = "DONE"
)

// Operation represents a single GCP Long-Running Operation.
type Operation struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Kind          string          `json:"kind"`
	OperationType string          `json:"operationType"`
	Status        OperationStatus `json:"status"`
	TargetLink    string          `json:"targetLink,omitempty"`
	Progress      int             `json:"progress"`
	Done          bool            `json:"done"`
	// InsertTime / StartTime / EndTime in RFC3339 format
	InsertTime string      `json:"insertTime,omitempty"`
	StartTime  string      `json:"startTime,omitempty"`
	EndTime    string      `json:"endTime,omitempty"`
	Metadata   interface{} `json:"metadata,omitempty"`
	// Error is only set when the operation fails.
	Error *OperationError `json:"error,omitempty"`
	// Zone or Region scoping (optional, service-specific)
	Zone   string `json:"zone,omitempty"`
	Region string `json:"region,omitempty"`
	// ServiceKind, Project, Location, and Response are the durable scope and
	// terminal result used by service-specific google.longrunning polling.
	ServiceKind string          `json:"serviceKind,omitempty"`
	Project     string          `json:"project,omitempty"`
	Location    string          `json:"location,omitempty"`
	Response    json.RawMessage `json:"response,omitempty"`
}

// OperationError provides GCP-shaped error details on failure.
type OperationError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type operationStore interface {
	Load(string, any) error
	Save(string, any) error
}

// ErrOperationNotFound deliberately covers unknown operations and scope
// mismatches so callers cannot discover operations owned by another service or
// parent.
var ErrOperationNotFound = errors.New("operation not found")

// OperationScope is the durable identity of one service-specific operation.
type OperationScope struct {
	ServiceKind string
	Project     string
	Location    string
	Target      string
}

// OperationManager is a thread-safe LRO registry with optional profile state.
type OperationManager struct {
	mu             sync.RWMutex
	persistMu      sync.Mutex
	ops            map[string]*Operation
	store          operationStore
	persistenceErr error

	observerMu        sync.Mutex
	terminalObservers map[uint64]*terminalObserver
	observerOrder     []uint64
	nextObserverID    uint64
}

type terminalObserver struct {
	id       uint64
	callback func(*Operation)
	pending  *Operation
	active   bool
	wakeup   chan struct{}
	done     chan struct{}
}

// NewOperationManager returns a ready-to-use OperationManager.
func NewOperationManager() *OperationManager {
	return &OperationManager{
		ops:               make(map[string]*Operation),
		terminalObservers: make(map[uint64]*terminalObserver),
	}
}

// NewOperationManagerWithStore restores operation polling metadata. Operations
// that were not terminal at shutdown become stable terminal interruption
// results; their work functions are never replayed.
func NewOperationManagerWithStore(store operationStore) (*OperationManager, error) {
	manager := &OperationManager{
		ops:               make(map[string]*Operation),
		store:             store,
		terminalObservers: make(map[uint64]*terminalObserver),
	}
	if store == nil {
		return manager, nil
	}
	if err := store.Load(operationStateEntry, &manager.ops); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return manager, nil
		}
		return nil, fmt.Errorf("load operations: %w", err)
	}
	if manager.ops == nil {
		manager.ops = make(map[string]*Operation)
	}

	interrupted := false
	for _, op := range manager.ops {
		if op == nil || op.Done || op.Status == StatusDone {
			continue
		}
		interruptOperation(op)
		interrupted = true
	}
	if interrupted {
		if err := manager.persist(); err != nil {
			return nil, fmt.Errorf("persist interrupted operations: %w", err)
		}
	}
	return manager, nil
}

// Register creates a new operation and stores it. Returns the operation for immediate serialisation.
func (om *OperationManager) Register(kind, operationType, targetLink, zone, region string) *Operation {
	op, _ := om.register(kind, operationType, targetLink, zone, region, false)
	return op
}

// RegisterDurable creates an operation only if its initial state can be saved.
// A manager without a store remains intentionally memory-only and succeeds.
func (om *OperationManager) RegisterDurable(kind, operationType, targetLink, zone, region string) (*Operation, error) {
	return om.register(kind, operationType, targetLink, zone, region, true)
}

// RegisterScopedDurable creates a durable service-scoped operation with its
// canonical polling name.
func (om *OperationManager) RegisterScopedDurable(scope OperationScope, operationType string) (*Operation, error) {
	if scope.ServiceKind == "" || scope.Target == "" {
		return nil, errors.New("operation service kind and target are required")
	}
	name := canonicalOperationName(scope.Project, scope.Location)
	op := newOperation(name, scope.ServiceKind, operationType, scope.Target, "", scope.Location)
	op.ServiceKind = scope.ServiceKind
	op.Project = scope.Project
	op.Location = scope.Location
	return om.insertDurable(op, true)
}

// RegisterScopedTargetDurable derives the canonical project and location scope
// from a resource target.
func (om *OperationManager) RegisterScopedTargetDurable(serviceKind, operationType, target string) (*Operation, error) {
	project, location := resourceParentScope(target)
	return om.RegisterScopedDurable(OperationScope{
		ServiceKind: serviceKind,
		Project:     project,
		Location:    location,
		Target:      target,
	}, operationType)
}

func (om *OperationManager) register(kind, operationType, targetLink, zone, region string, rollbackOnFailure bool) (*Operation, error) {
	name := fmt.Sprintf("operation-%d-%s", time.Now().Unix(), randomSuffix(8))
	op := newOperation(name, kind, operationType, targetLink, zone, region)
	op.ServiceKind = kind
	op.Project, op.Location = resourceParentScope(targetLink)
	if op.Location == "" {
		op.Location = region
		if op.Location == "" {
			op.Location = zone
		}
	}
	return om.insertDurable(op, rollbackOnFailure)
}

func (om *OperationManager) insertDurable(op *Operation, rollbackOnFailure bool) (*Operation, error) {
	om.persistMu.Lock()
	defer om.persistMu.Unlock()

	om.mu.Lock()
	om.ops[op.Name] = op
	om.mu.Unlock()

	if err := om.persistLocked(); err != nil {
		if rollbackOnFailure {
			om.mu.Lock()
			delete(om.ops, op.Name)
			om.mu.Unlock()
			if compensationErr := om.persistLocked(); compensationErr != nil {
				var durable map[string]*Operation
				loadErr := error(nil)
				if om.store != nil {
					loadErr = om.store.Load(operationStateEntry, &durable)
				}
				if errors.Is(loadErr, state.ErrNotFound) {
					loadErr = nil
					durable = nil
				}
				om.mu.Lock()
				if loadErr == nil {
					if durableOperation := durable[op.Name]; durableOperation != nil {
						om.ops[op.Name] = cloneOperation(durableOperation)
					} else {
						delete(om.ops, op.Name)
					}
				} else {
					om.ops[op.Name] = op
				}
				om.mu.Unlock()
				if loadErr != nil {
					err = fmt.Errorf("%w; compensate registration: %v; read back operations: %v",
						err, compensationErr, loadErr)
				} else {
					err = fmt.Errorf("%w; compensate registration: %v", err, compensationErr)
				}
			}
		}
		om.recordPersistenceFailure(op.Name, false, err)
		if rollbackOnFailure {
			return nil, err
		}
		return cloneOperation(op), nil
	}
	return cloneOperation(op), nil
}

func newOperation(name, kind, operationType, targetLink, zone, region string) *Operation {
	return &Operation{
		ID:            fmt.Sprintf("%d", rand.Int63()),
		Name:          name,
		Kind:          kind,
		OperationType: operationType,
		Status:        StatusPending,
		TargetLink:    targetLink,
		InsertTime:    time.Now().UTC().Format(time.RFC3339),
		Zone:          zone,
		Region:        region,
	}
}

// Get retrieves an operation by name. Returns nil if not found.
func (om *OperationManager) Get(name string) *Operation {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return cloneOperation(om.ops[name])
}

// GetScoped returns an operation only when every supplied scope component
// matches its durable registration.
func (om *OperationManager) GetScoped(name string, scope OperationScope) (*Operation, error) {
	op := om.Get(name)
	if op == nil ||
		scope.ServiceKind != "" && op.ServiceKind != scope.ServiceKind ||
		scope.Project != "" && op.Project != scope.Project ||
		scope.Location != "" && op.Location != scope.Location ||
		scope.Target != "" && op.TargetLink != scope.Target {
		return nil, ErrOperationNotFound
	}
	return op, nil
}

// PollScoped resolves the canonical operation name in path and validates it
// against the service and parent encoded by that path.
func (om *OperationManager) PollScoped(path, serviceKind string) (*Operation, error) {
	name, project, location := operationPathScope(path)
	if name == "" {
		return nil, ErrOperationNotFound
	}
	scope := OperationScope{
		ServiceKind: serviceKind,
		Project:     project,
		Location:    location,
	}
	op, err := om.GetScoped(name, scope)
	if err != nil {
		_, id := splitOperationName(name)
		op, err = om.GetScoped(id, scope)
	}
	if err != nil {
		return nil, err
	}
	if op.Project != project || op.Location != location {
		return nil, ErrOperationNotFound
	}
	op.Name = name
	return op, nil
}

// ScopedOperationResponse serializes the durable google.longrunning operation
// fields shared by experimental services.
func ScopedOperationResponse(op *Operation) map[string]any {
	metadata := map[string]any{
		"serviceKind": op.ServiceKind,
		"project":     op.Project,
		"location":    op.Location,
		"target":      op.TargetLink,
		"verb":        op.OperationType,
	}
	if typeURL := operationMetadataType(op.ServiceKind); typeURL != "" {
		metadata["@type"] = typeURL
	}
	response := map[string]any{
		"name":     op.Name,
		"done":     op.Done,
		"metadata": metadata,
	}
	if op.Done && op.Error != nil {
		response["error"] = op.Error
	}
	if op.Done && op.Error == nil && len(op.Response) != 0 {
		var value any
		if json.Unmarshal(op.Response, &value) == nil {
			response["response"] = value
		}
	}
	return response
}

// Advance moves the operation through the PENDING → RUNNING → DONE state machine.
// It should be called from a background goroutine.
func (om *OperationManager) Advance(name string, progress int, status OperationStatus) {
	_ = om.AdvanceDurable(name, progress, status)
}

// AdvanceDurable updates an operation and reports whether the resulting
// snapshot was saved. A terminal save failure remains visible in-process on the
// operation; after restart, the last durable non-terminal state is reported as
// interrupted and no work is replayed.
func (om *OperationManager) AdvanceDurable(name string, progress int, status OperationStatus) error {
	om.persistMu.Lock()

	om.mu.Lock()
	op, ok := om.ops[name]
	if !ok {
		om.mu.Unlock()
		om.persistMu.Unlock()
		return nil
	}

	op.Progress = progress
	op.Status = status

	if status == StatusRunning && op.StartTime == "" {
		op.StartTime = time.Now().UTC().Format(time.RFC3339)
	}

	if status == StatusDone {
		op.Done = true
		op.Progress = 100
		op.EndTime = time.Now().UTC().Format(time.RFC3339)
		ensureScopedTerminalResponse(op)
	}
	om.mu.Unlock()
	if err := om.persistLocked(); err != nil {
		om.recordPersistenceFailure(name, status == StatusDone, err)
		om.persistMu.Unlock()
		return err
	}
	terminal := status == StatusDone
	completed := om.Get(name)
	om.persistMu.Unlock()
	if terminal {
		om.notifyTerminal(completed)
	}
	return nil
}

// UpdateMetadata updates the metadata of an operation.
func (om *OperationManager) UpdateMetadata(name string, metadata interface{}) {
	om.mu.Lock()
	if op, ok := om.ops[name]; ok {
		op.Metadata = metadata
	}
	om.mu.Unlock()
	om.persistBestEffort()
}

// MarkDone marks the operation as successfully completed.
func (om *OperationManager) MarkDone(name string) {
	om.Advance(name, 100, StatusDone)
}

// List returns all operations in the registry.
func (om *OperationManager) List() []*Operation {
	om.mu.RLock()
	defer om.mu.RUnlock()
	res := make([]*Operation, 0, len(om.ops))
	for _, op := range om.ops {
		res = append(res, cloneOperation(op))
	}
	return res
}

// Fail marks the operation as DONE with an error.
func (om *OperationManager) Fail(name string, code int, message string) {
	_ = om.FailDurable(name, code, message)
}

// FailDurable marks an operation failed and reports whether the terminal state
// was saved.
func (om *OperationManager) FailDurable(name string, code int, message string) error {
	om.persistMu.Lock()

	om.mu.Lock()
	op, ok := om.ops[name]
	if !ok {
		om.mu.Unlock()
		om.persistMu.Unlock()
		return nil
	}
	op.Status = StatusDone
	op.Done = true
	op.Progress = 100
	op.EndTime = time.Now().UTC().Format(time.RFC3339)
	if isScopedOperation(op) {
		code = canonicalRPCCode(code)
	}
	op.Error = &OperationError{Code: code, Message: message}
	op.Response = nil
	om.mu.Unlock()
	if err := om.persistLocked(); err != nil {
		om.recordPersistenceFailure(name, true, err)
		om.persistMu.Unlock()
		return err
	}
	completed := om.Get(name)
	om.persistMu.Unlock()
	om.notifyTerminal(completed)
	return nil
}

// FinalizeDurable records a terminal result and reconciles an ambiguous Save
// error against the operation store before returning. The returned error is
// preserved even when readback confirms the terminal state, allowing callers
// to enter a degraded mode for uncertain filesystem durability.
func (om *OperationManager) FinalizeDurable(name string, code int, message string) error {
	return om.finalizeDurable(name, nil, code, message)
}

// FinalizeScopedDurable records the durable terminal response or error.
func (om *OperationManager) FinalizeScopedDurable(name string, response json.RawMessage, code int, message string) error {
	return om.finalizeDurable(name, response, code, message)
}

func (om *OperationManager) finalizeDurable(name string, response json.RawMessage, code int, message string) error {
	om.persistMu.Lock()

	om.mu.Lock()
	op, ok := om.ops[name]
	if !ok {
		om.mu.Unlock()
		om.persistMu.Unlock()
		return nil
	}
	op.Status = StatusDone
	op.Done = true
	op.Progress = 100
	op.EndTime = time.Now().UTC().Format(time.RFC3339)
	if code != 0 {
		if isScopedOperation(op) {
			code = canonicalRPCCode(code)
		}
		op.Error = &OperationError{Code: code, Message: message}
		op.Response = nil
	} else {
		op.Error = nil
		op.Response = append(json.RawMessage(nil), response...)
		ensureScopedTerminalResponse(op)
	}
	om.mu.Unlock()

	if err := om.persistLocked(); err != nil {
		var durable map[string]*Operation
		loadErr := error(nil)
		if om.store != nil {
			loadErr = om.store.Load(operationStateEntry, &durable)
		}
		if errors.Is(loadErr, state.ErrNotFound) {
			loadErr = nil
			durable = nil
		}
		wrapped := fmt.Errorf("terminal operation persistence degraded: %w", err)
		om.mu.Lock()
		if loadErr == nil && durable[name] != nil {
			reconciled := cloneOperation(durable[name])
			if !reconciled.Done && reconciled.Status != StatusDone {
				interruptOperation(reconciled)
			}
			om.ops[name] = reconciled
		}
		om.persistenceErr = wrapped
		om.mu.Unlock()
		if loadErr != nil {
			om.persistMu.Unlock()
			return fmt.Errorf("%w; read back operations: %v", wrapped, loadErr)
		}
		om.persistMu.Unlock()
		return wrapped
	}
	completed := om.Get(name)
	om.persistMu.Unlock()
	om.notifyTerminal(completed)
	return nil
}

// RollbackScopedRegistration removes an operation created as part of a resource
// mutation that could not be committed.
func (om *OperationManager) RollbackScopedRegistration(name string) error {
	om.persistMu.Lock()
	defer om.persistMu.Unlock()

	om.mu.Lock()
	previous := cloneOperation(om.ops[name])
	delete(om.ops, name)
	om.mu.Unlock()
	if previous == nil {
		return nil
	}
	if err := om.persistLocked(); err != nil {
		var durable map[string]*Operation
		loadErr := error(nil)
		if om.store != nil {
			loadErr = om.store.Load(operationStateEntry, &durable)
		}
		wrapped := fmt.Errorf("operation rollback persistence degraded: %w", err)
		om.mu.Lock()
		switch {
		case loadErr != nil:
			om.ops[name] = previous
		case durable[name] != nil:
			om.ops[name] = cloneOperation(durable[name])
		default:
			delete(om.ops, name)
		}
		om.persistenceErr = wrapped
		om.mu.Unlock()
		if loadErr != nil {
			return fmt.Errorf("rollback operation %q: %w; read back operations: %v", name, wrapped, loadErr)
		}
		return fmt.Errorf("rollback operation %q: %w", name, wrapped)
	}
	return nil
}

// TerminalObserverSubscription controls one isolated terminal observer.
type TerminalObserverSubscription struct {
	manager  *OperationManager
	observer *terminalObserver
	once     sync.Once
}

// OnTerminal registers a callback and returns a simple idempotent unsubscribe
// function. Lifecycle owners that must wait for an in-flight callback should
// use ObserveTerminal and TerminalObserverSubscription.Shutdown.
func (om *OperationManager) OnTerminal(observer func(*Operation)) func() {
	subscription := om.ObserveTerminal(observer)
	return subscription.Unsubscribe
}

// ObserveTerminal registers a callback for durably saved terminal states.
// Each listener owns one worker and one coalesced pending wakeup. A blocked
// listener cannot delay any other listener or operation completion.
func (om *OperationManager) ObserveTerminal(observer func(*Operation)) *TerminalObserverSubscription {
	subscription := &TerminalObserverSubscription{manager: om}
	if observer == nil {
		return subscription
	}
	om.observerMu.Lock()
	om.nextObserverID++
	id := om.nextObserverID
	registered := &terminalObserver{
		id:       id,
		callback: observer,
		active:   true,
		wakeup:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	om.terminalObservers[id] = registered
	om.observerOrder = append(om.observerOrder, id)
	om.observerMu.Unlock()
	subscription.observer = registered
	go om.runTerminalObserver(registered)
	return subscription
}

// Unsubscribe prevents future callback delivery and is safe to call repeatedly.
// It does not wait for a callback already in progress.
func (subscription *TerminalObserverSubscription) Unsubscribe() {
	if subscription == nil || subscription.observer == nil {
		return
	}
	subscription.once.Do(func() {
		om := subscription.manager
		observer := subscription.observer
		om.observerMu.Lock()
		observer.active = false
		observer.pending = nil
		delete(om.terminalObservers, observer.id)
		for index, observerID := range om.observerOrder {
			if observerID == observer.id {
				om.observerOrder = append(om.observerOrder[:index], om.observerOrder[index+1:]...)
				break
			}
		}
		om.observerMu.Unlock()
		select {
		case observer.wakeup <- struct{}{}:
		default:
		}
	})
}

// Shutdown unsubscribes and waits until an in-flight callback has returned.
func (subscription *TerminalObserverSubscription) Shutdown(ctx context.Context) error {
	if subscription == nil || subscription.observer == nil {
		return nil
	}
	subscription.Unsubscribe()
	select {
	case <-subscription.observer.done:
		return nil
	case <-ctx.Done():
		select {
		case <-subscription.observer.done:
			return nil
		default:
			return ctx.Err()
		}
	}
}

// RemoveDurable expires a terminal operation from memory and durable polling
// state. Non-terminal operations are never removed.
func (om *OperationManager) RemoveDurable(name string) error {
	om.persistMu.Lock()
	defer om.persistMu.Unlock()

	om.mu.Lock()
	operation := om.ops[name]
	if operation == nil {
		om.mu.Unlock()
		return nil
	}
	if !operation.Done {
		om.mu.Unlock()
		return fmt.Errorf("cannot remove non-terminal operation %q", name)
	}
	previous := cloneOperation(operation)
	delete(om.ops, name)
	om.mu.Unlock()

	if err := om.persistLocked(); err != nil {
		var durable map[string]*Operation
		loadErr := error(nil)
		if om.store != nil {
			loadErr = om.store.Load(operationStateEntry, &durable)
		}
		if loadErr == nil && durable[name] == nil {
			return nil
		}
		om.mu.Lock()
		om.ops[name] = previous
		om.mu.Unlock()
		if loadErr != nil {
			return fmt.Errorf("remove operation %q: %w; read back operations: %v", name, err, loadErr)
		}
		return fmt.Errorf("remove operation %q: %w", name, err)
	}
	return nil
}

// RunAsync drives a standard 3-phase LRO lifecycle in a goroutine.
// It ensures that intermediate states (PENDING, RUNNING) are visible to polling clients
// by introducing artificial delays and granular progress increments.
func (om *OperationManager) RunAsync(name string, workFn func() error) {
	go func() {
		// 1. Initial delay to ensure the caller (Terraform/UI) registers the initial PENDING state
		time.Sleep(800 * time.Millisecond)

		// 2. Transition PENDING → RUNNING (Low progress)
		om.Advance(name, 5, StatusRunning)
		time.Sleep(1200 * time.Millisecond)

		// 3. Increment progress to show life before work starts
		om.Advance(name, 25, StatusRunning)
		time.Sleep(500 * time.Millisecond)

		// 4. Execute actual work (container boot, provisioning, etc.)
		if err := workFn(); err != nil {
			code := 500
			var coded interface{ OperationCode() int }
			if errors.As(err, &coded) {
				code = coded.OperationCode()
			}
			om.Fail(name, code, err.Error())
			return
		}

		// 5. Successful work completion - show high progress before finishing
		om.Advance(name, 85, StatusRunning)
		time.Sleep(500 * time.Millisecond)

		// 6. Transition RUNNING → DONE
		om.Advance(name, 100, StatusDone)
	}()
}

func (om *OperationManager) persistBestEffort() {
	if err := om.persist(); err != nil {
		om.recordPersistenceFailure("", false, err)
	}
}

func (om *OperationManager) persist() error {
	if om.store == nil {
		return nil
	}
	om.persistMu.Lock()
	defer om.persistMu.Unlock()
	return om.persistLocked()
}

func (om *OperationManager) persistLocked() error {
	if om.store == nil {
		return nil
	}
	om.mu.RLock()
	payload, err := json.Marshal(om.ops)
	om.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("snapshot operations: %w", err)
	}
	if err := om.store.Save(operationStateEntry, json.RawMessage(payload)); err != nil {
		return fmt.Errorf("save operations: %w", err)
	}
	return nil
}

// PersistenceError returns the latest durable state failure observed by this
// manager. It remains set so health and polling surfaces can report degradation.
func (om *OperationManager) PersistenceError() error {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return om.persistenceErr
}

// MarkPersistenceFailure records a durable state failure discovered while
// compensating a resource mutation associated with this operation manager.
func (om *OperationManager) MarkPersistenceFailure(err error) {
	if err != nil {
		om.recordPersistenceFailure("", false, err)
	}
}

func (om *OperationManager) notifyTerminal(operation *Operation) {
	if operation == nil {
		return
	}
	om.observerMu.Lock()
	for _, id := range om.observerOrder {
		observer := om.terminalObservers[id]
		if observer == nil || !observer.active {
			continue
		}
		observer.pending = cloneOperation(operation)
		select {
		case observer.wakeup <- struct{}{}:
		default:
		}
	}
	om.observerMu.Unlock()
}

func (om *OperationManager) runTerminalObserver(observer *terminalObserver) {
	defer close(observer.done)
	for {
		<-observer.wakeup
		om.observerMu.Lock()
		if !observer.active {
			observer.callback = nil
			observer.pending = nil
			om.observerMu.Unlock()
			return
		}
		operation := observer.pending
		observer.pending = nil
		callback := observer.callback
		om.observerMu.Unlock()

		invokeTerminalObserver(callback, operation)
	}
}

func invokeTerminalObserver(observer func(*Operation), operation *Operation) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[OperationManager] terminal observer panic recovered: %v", recovered)
		}
	}()
	observer(cloneOperation(operation))
}

func (om *OperationManager) recordPersistenceFailure(name string, terminal bool, err error) {
	wrapped := fmt.Errorf("operation persistence degraded: %w", err)
	om.mu.Lock()
	om.persistenceErr = wrapped
	if terminal {
		if op := om.ops[name]; op != nil {
			const limitation = "terminal state persistence failed; after restart this operation may be reported as interrupted"
			if op.Error == nil {
				op.Error = &OperationError{Code: 500, Message: limitation}
			} else {
				op.Error.Message += "; " + limitation
			}
		}
	}
	om.mu.Unlock()
	log.Printf("[OperationManager] %v", wrapped)
}

func cloneOperation(op *Operation) *Operation {
	if op == nil {
		return nil
	}
	clone := *op
	if op.Error != nil {
		operationError := *op.Error
		clone.Error = &operationError
	}
	clone.Response = append(json.RawMessage(nil), op.Response...)
	return &clone
}

func canonicalOperationName(project, location string) string {
	id := fmt.Sprintf("operation-%d-%s", time.Now().Unix(), randomSuffix(8))
	switch {
	case project != "" && location != "":
		return fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, id)
	case project != "":
		return fmt.Sprintf("projects/%s/operations/%s", project, id)
	default:
		return "operations/" + id
	}
}

func splitOperationName(name string) (parent, id string) {
	index := strings.LastIndex(name, "/")
	if index < 0 {
		return "", name
	}
	return name[:index], name[index+1:]
}

func resourceParentScope(resource string) (project, location string) {
	parts := strings.Split(strings.Trim(resource, "/"), "/")
	for index, part := range parts {
		if index+1 >= len(parts) {
			break
		}
		switch part {
		case "projects":
			project = parts[index+1]
		case "locations", "regions", "zones":
			location = parts[index+1]
		}
	}
	return project, location
}

func operationPathScope(path string) (name, project, location string) {
	path = strings.TrimPrefix(path, "/")
	if path == "" || strings.HasSuffix(path, "/") {
		return "", "", ""
	}
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if part == "" {
			return "", "", ""
		}
	}

	if len(parts) > 0 && parts[0] != "projects" && parts[0] != "locations" && parts[0] != "operations" {
		parts = parts[1:]
	}
	switch {
	case len(parts) == 6 &&
		parts[0] == "projects" &&
		(parts[2] == "locations" || parts[2] == "regions" || parts[2] == "zones") &&
		parts[4] == "operations":
		return strings.Join(parts, "/"), parts[1], parts[3]
	case len(parts) == 4 && parts[0] == "projects" && parts[2] == "operations":
		return strings.Join(parts, "/"), parts[1], ""
	case len(parts) == 4 && parts[0] == "locations" && parts[2] == "operations":
		return strings.Join(parts[2:], "/"), "", parts[1]
	case len(parts) == 2 && parts[0] == "operations":
		return strings.Join(parts, "/"), "", ""
	}
	return "", "", ""
}

func interruptOperation(op *Operation) {
	op.Status = StatusDone
	op.Done = true
	op.Progress = 100
	op.EndTime = time.Now().UTC().Format(time.RFC3339)
	code := 500
	if isScopedOperation(op) {
		code = canonicalRPCCode(code)
	}
	op.Error = &OperationError{
		Code:    code,
		Message: "operation interrupted by MiniSky restart; side effects were not replayed",
	}
}

func isScopedOperation(op *Operation) bool {
	return op != nil && strings.Contains(op.Name, "/operations/") && op.ServiceKind != ""
}

func ensureScopedTerminalResponse(op *Operation) {
	if !isScopedOperation(op) || op.Error != nil {
		return
	}
	typeURL := operationResponseType(op)
	if typeURL == "" {
		return
	}
	response := make(map[string]any)
	if len(op.Response) != 0 {
		_ = json.Unmarshal(op.Response, &response)
	}
	response["@type"] = typeURL
	if typeURL != "type.googleapis.com/google.protobuf.Empty" && op.TargetLink != "" {
		if _, exists := response["name"]; !exists {
			response["name"] = op.TargetLink
		}
	}
	op.Response, _ = json.Marshal(response)
}

func operationMetadataType(serviceKind string) string {
	types := map[string]string{
		"accesscontextmanager#operation": "type.googleapis.com/google.identity.accesscontextmanager.v1.OperationMetadata",
		"alloydb#operation":              "type.googleapis.com/google.cloud.alloydb.v1.OperationMetadata",
		"apigateway#operation":           "type.googleapis.com/google.cloud.apigateway.v1.OperationMetadata",
		"batch#operation":                "type.googleapis.com/google.cloud.batch.v1.OperationMetadata",
		"clouddeploy#operation":          "type.googleapis.com/google.cloud.deploy.v1.OperationMetadata",
		"composer#operation":             "type.googleapis.com/google.cloud.orchestration.airflow.service.v1.OperationMetadata",
		"documentai#operation":           "type.googleapis.com/google.cloud.documentai.v1.CommonOperationMetadata",
		"eventarc#operation":             "type.googleapis.com/google.cloud.eventarc.v1.OperationMetadata",
		"file#operation":                 "type.googleapis.com/google.cloud.common.OperationMetadata",
		"filestore#operation":            "type.googleapis.com/google.cloud.common.OperationMetadata",
		"managedkafka#operation":         "type.googleapis.com/google.cloud.managedkafka.v1.OperationMetadata",
		"networksecurity#operation":      "type.googleapis.com/google.cloud.networksecurity.v1.OperationMetadata",
		"servicemesh#operation":          "type.googleapis.com/google.cloud.servicemesh.v1.OperationMetadata",
		"workflows#operation":            "type.googleapis.com/google.cloud.workflows.v1.OperationMetadata",
	}
	return types[serviceKind]
}

func operationResponseType(op *Operation) string {
	if strings.EqualFold(op.OperationType, "delete") {
		return "type.googleapis.com/google.protobuf.Empty"
	}
	collections := []struct {
		serviceKind string
		segment     string
		typeURL     string
	}{
		{"accesscontextmanager#operation", "/accessPolicies/", "type.googleapis.com/google.identity.accesscontextmanager.v1.AccessPolicy"},
		{"accesscontextmanager#operation", "/servicePerimeters/", "type.googleapis.com/google.identity.accesscontextmanager.v1.ServicePerimeter"},
		{"accesscontextmanager#operation", "/accessLevels/", "type.googleapis.com/google.identity.accesscontextmanager.v1.AccessLevel"},
		{"apigateway#operation", "/gateways/", "type.googleapis.com/google.cloud.apigateway.v1.Gateway"},
		{"apigateway#operation", "/configs/", "type.googleapis.com/google.cloud.apigateway.v1.ApiConfig"},
		{"apigateway#operation", "/apis/", "type.googleapis.com/google.cloud.apigateway.v1.Api"},
		{"clouddeploy#operation", "/deliveryPipelines/", "type.googleapis.com/google.cloud.deploy.v1.DeliveryPipeline"},
		{"clouddeploy#operation", "/releases/", "type.googleapis.com/google.cloud.deploy.v1.Release"},
		{"clouddeploy#operation", "/rollouts/", "type.googleapis.com/google.cloud.deploy.v1.Rollout"},
		{"workflows#operation", "/workflows/", "type.googleapis.com/google.cloud.workflows.v1.Workflow"},
		{"eventarc#operation", "/triggers/", "type.googleapis.com/google.cloud.eventarc.v1.Trigger"},
		{"eventarc#operation", "/channels/", "type.googleapis.com/google.cloud.eventarc.v1.Channel"},
		{"file#operation", "/instances/", "type.googleapis.com/google.cloud.filestore.v1.Instance"},
		{"filestore#operation", "/instances/", "type.googleapis.com/google.cloud.filestore.v1.Instance"},
		{"composer#operation", "/environments/", "type.googleapis.com/google.cloud.orchestration.airflow.service.v1.Environment"},
		{"alloydb#operation", "/instances/", "type.googleapis.com/google.cloud.alloydb.v1.Instance"},
		{"alloydb#operation", "/clusters/", "type.googleapis.com/google.cloud.alloydb.v1.Cluster"},
		{"documentai#operation", "/processors/", "type.googleapis.com/google.cloud.documentai.v1.Processor"},
		{"managedkafka#operation", "/topics/", "type.googleapis.com/google.cloud.managedkafka.v1.Topic"},
		{"managedkafka#operation", "/clusters/", "type.googleapis.com/google.cloud.managedkafka.v1.Cluster"},
		{"networksecurity#operation", "/authorizationPolicies/", "type.googleapis.com/google.cloud.networksecurity.v1.AuthorizationPolicy"},
		{"networksecurity#operation", "/serverTlsPolicies/", "type.googleapis.com/google.cloud.networksecurity.v1.ServerTlsPolicy"},
		{"servicemesh#operation", "/meshes/", "type.googleapis.com/google.cloud.servicemesh.v1.Mesh"},
		{"batch#operation", "/jobs/", "type.googleapis.com/google.cloud.batch.v1.Job"},
	}
	target := "/" + strings.Trim(op.TargetLink, "/") + "/"
	for _, collection := range collections {
		if collection.serviceKind == op.ServiceKind && strings.Contains(target, collection.segment) {
			return collection.typeURL
		}
	}
	return ""
}

func canonicalRPCCode(code int) int {
	if code >= 0 && code <= 16 {
		return code
	}
	switch code {
	case 400:
		return 3
	case 401:
		return 16
	case 403:
		return 7
	case 404:
		return 5
	case 409:
		return 6
	case 412:
		return 9
	case 429:
		return 8
	case 499:
		return 1
	case 501:
		return 12
	case 503:
		return 14
	case 504:
		return 4
	default:
		return 13
	}
}

func randomSuffix(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
