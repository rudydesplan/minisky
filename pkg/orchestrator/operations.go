package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
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

// OperationManager is a thread-safe LRO registry with optional profile state.
type OperationManager struct {
	mu             sync.RWMutex
	persistMu      sync.Mutex
	ops            map[string]*Operation
	store          operationStore
	persistenceErr error
}

// NewOperationManager returns a ready-to-use OperationManager.
func NewOperationManager() *OperationManager {
	return &OperationManager{
		ops: make(map[string]*Operation),
	}
}

// NewOperationManagerWithStore restores operation polling metadata. Operations
// that were not terminal at shutdown become stable terminal interruption
// results; their work functions are never replayed.
func NewOperationManagerWithStore(store operationStore) (*OperationManager, error) {
	manager := &OperationManager{
		ops:   make(map[string]*Operation),
		store: store,
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
		op.Status = StatusDone
		op.Done = true
		op.Progress = 100
		op.EndTime = time.Now().UTC().Format(time.RFC3339)
		op.Error = &OperationError{
			Code:    500,
			Message: "operation interrupted by MiniSky restart; side effects were not replayed",
		}
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

func (om *OperationManager) register(kind, operationType, targetLink, zone, region string, rollbackOnFailure bool) (*Operation, error) {
	id := fmt.Sprintf("%d", rand.Int63())
	name := fmt.Sprintf("operation-%d-%s", time.Now().Unix(), randomSuffix(8))

	op := &Operation{
		ID:            id,
		Name:          name,
		Kind:          kind,
		OperationType: operationType,
		Status:        StatusPending,
		TargetLink:    targetLink,
		Progress:      0,
		Done:          false,
		InsertTime:    time.Now().UTC().Format(time.RFC3339),
		Zone:          zone,
		Region:        region,
	}

	om.persistMu.Lock()
	defer om.persistMu.Unlock()

	om.mu.Lock()
	om.ops[name] = op
	om.mu.Unlock()

	if err := om.persistLocked(); err != nil {
		if rollbackOnFailure {
			om.mu.Lock()
			delete(om.ops, name)
			om.mu.Unlock()
		}
		om.recordPersistenceFailure(name, false, err)
		if rollbackOnFailure {
			return nil, err
		}
		return cloneOperation(op), nil
	}
	return cloneOperation(op), nil
}

// Get retrieves an operation by name. Returns nil if not found.
func (om *OperationManager) Get(name string) *Operation {
	om.mu.RLock()
	defer om.mu.RUnlock()
	return cloneOperation(om.ops[name])
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
	defer om.persistMu.Unlock()

	om.mu.Lock()
	op, ok := om.ops[name]
	if !ok {
		om.mu.Unlock()
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
	}
	om.mu.Unlock()
	if err := om.persistLocked(); err != nil {
		om.recordPersistenceFailure(name, status == StatusDone, err)
		return err
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
	defer om.persistMu.Unlock()

	om.mu.Lock()
	op, ok := om.ops[name]
	if !ok {
		om.mu.Unlock()
		return nil
	}
	op.Status = StatusDone
	op.Done = true
	op.Progress = 100
	op.EndTime = time.Now().UTC().Format(time.RFC3339)
	op.Error = &OperationError{Code: code, Message: message}
	om.mu.Unlock()
	if err := om.persistLocked(); err != nil {
		om.recordPersistenceFailure(name, true, err)
		return err
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
			om.Fail(name, 500, err.Error())
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
	return &clone
}

func randomSuffix(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
