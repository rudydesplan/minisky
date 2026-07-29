package orchestrator

import (
	"encoding/json"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestDurableTerminalPublicationWaitsForSave(t *testing.T) {
	for _, test := range terminalPublicationTests() {
		t.Run(test.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			operation := test.register(t, manager)
			observed := make(chan *Operation, 1)
			unsubscribe := manager.OnTerminal(func(operation *Operation) {
				observed <- operation
			})
			defer unsubscribe()

			store.blockTerminalSave(false)
			defer store.releaseTerminalSave()
			finalized := make(chan error, 1)
			go func() {
				finalized <- test.finalize(manager, operation.Name)
			}()

			store.waitForTerminalSave(t)
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertNoTerminalObservation(t, observed)

			store.releaseTerminalSave()
			if err := waitForFinalization(t, finalized); err != nil {
				t.Fatal(err)
			}
			published := manager.Get(operation.Name)
			test.assertTerminal(t, published)
			mutateOperationCopy(published)
			test.assertTerminal(t, manager.Get(operation.Name))
			select {
			case notified := <-observed:
				test.assertTerminal(t, notified)
			case <-time.After(time.Second):
				t.Fatal("terminal observer was not notified after durable save")
			}

			restarted, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			test.assertTerminal(t, restarted.Get(operation.Name))
		})
	}
}

func TestDurableTerminalSaveFailureDoesNotPublish(t *testing.T) {
	for _, test := range terminalPublicationTests() {
		t.Run(test.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			operation := test.register(t, manager)
			observed := make(chan *Operation, 1)
			unsubscribe := manager.OnTerminal(func(operation *Operation) {
				observed <- operation
			})
			defer unsubscribe()

			store.blockTerminalSave(true)
			defer store.releaseTerminalSave()
			finalized := make(chan error, 1)
			go func() {
				finalized <- test.finalize(manager, operation.Name)
			}()

			store.waitForTerminalSave(t)
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertNoTerminalObservation(t, observed)

			store.releaseTerminalSave()
			if err := waitForFinalization(t, finalized); !errors.Is(err, errTerminalSave) {
				t.Fatalf("finalize error = %v, want terminal save failure", err)
			}
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertNoTerminalObservation(t, observed)
			if manager.PersistenceError() == nil ||
				!strings.Contains(manager.PersistenceError().Error(), errTerminalSave.Error()) {
				t.Fatalf("persistence error = %v", manager.PersistenceError())
			}

			if err := test.finalize(manager, operation.Name); !errors.Is(err, errTerminalSave) {
				t.Fatalf("retry error = %v, want sticky terminal save failure", err)
			}
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertNoTerminalObservation(t, observed)
			persisted := store.operations(t)[operation.Name]
			assertOperationNonterminal(t, persisted)
		})
	}
}

func TestDurableTerminalPostCommitErrorPublishesExactReadback(t *testing.T) {
	tests := terminalPublicationTests()
	for _, test := range []terminalPublicationTest{tests[1], tests[2]} {
		t.Run(test.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			operation := test.register(t, manager)
			manager.UpdateMetadata(operation.Name, publicationMetadata("committed"))
			observed := make(chan *Operation, 1)
			unsubscribe := manager.OnTerminal(func(operation *Operation) {
				observed <- operation
			})
			defer unsubscribe()
			store.failNextSaveAfterCommit(nil)

			if err := test.finalize(manager, operation.Name); err != nil {
				t.Fatalf("exact committed readback error = %v", err)
			}
			test.assertTerminal(t, manager.Get(operation.Name))
			assertOperationMetadata(t, manager.Get(operation.Name), publicationMetadata("committed"))
			select {
			case notified := <-observed:
				test.assertTerminal(t, notified)
				assertOperationMetadata(t, notified, publicationMetadata("committed"))
			case <-time.After(time.Second):
				t.Fatal("exact post-commit terminal candidate was not notified")
			}
			if manager.PersistenceError() == nil {
				t.Fatal("post-commit save failure did not remain degraded")
			}

			restarted, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			test.assertTerminal(t, restarted.Get(operation.Name))
			assertOperationMetadata(t, restarted.Get(operation.Name), publicationMetadata("committed"))
		})
	}
}

func TestDurableTerminalPostCommitErrorRejectsUnverifiedReadback(t *testing.T) {
	tests := []struct {
		name        string
		transform   func([]byte) []byte
		readbackErr error
	}{
		{
			name:      "done mismatch",
			transform: mutateCommittedOperation(func(operation *Operation) { operation.Done = false }),
		},
		{
			name:      "status mismatch",
			transform: mutateCommittedOperation(func(operation *Operation) { operation.Status = StatusRunning }),
		},
		{
			name: "error mismatch",
			transform: mutateCommittedOperation(func(operation *Operation) {
				operation.Error = &OperationError{Code: 13, Message: "mismatched"}
			}),
		},
		{
			name: "response mismatch",
			transform: mutateCommittedOperation(func(operation *Operation) {
				operation.Response = json.RawMessage(`{"name":"mismatched"}`)
			}),
		},
		{
			name: "metadata mismatch",
			transform: mutateCommittedOperation(func(operation *Operation) {
				operation.Metadata = publicationMetadata("mismatched")
			}),
		},
		{
			name:      "scope mismatch",
			transform: mutateCommittedOperation(func(operation *Operation) { operation.Project = "other-project" }),
		},
		{
			name:      "name mismatch",
			transform: mutateCommittedOperation(func(operation *Operation) { operation.Name += "-other" }),
		},
		{
			name: "unknown field",
			transform: func(payload []byte) []byte {
				var operations map[string]map[string]any
				if err := json.Unmarshal(payload, &operations); err != nil {
					panic(err)
				}
				for _, operation := range operations {
					if operation["done"] == true {
						operation["unexpected"] = "field"
					}
				}
				payload, _ = json.Marshal(operations)
				return payload
			},
		},
		{name: "malformed", transform: func([]byte) []byte { return []byte(`{`) }},
		{name: "readback failure", readbackErr: errors.New("readback unavailable")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTerminalPublicationStore()
			manager, err := NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			operation := terminalPublicationTests()[1].register(t, manager)
			manager.UpdateMetadata(operation.Name, publicationMetadata("candidate"))
			observed := make(chan *Operation, 1)
			unsubscribe := manager.OnTerminal(func(operation *Operation) {
				observed <- operation
			})
			defer unsubscribe()
			store.failNextSaveAfterCommit(test.transform)
			if test.readbackErr != nil {
				store.failNextLoad(test.readbackErr)
			}

			err = terminalPublicationTests()[1].finalize(manager, operation.Name)
			if !errors.Is(err, errTerminalSave) {
				t.Fatalf("finalize error = %v, want post-commit save failure", err)
			}
			assertOperationNonterminal(t, manager.Get(operation.Name))
			assertOperationMetadata(t, manager.Get(operation.Name), publicationMetadata("candidate"))
			assertNoTerminalObservation(t, observed)
			if manager.PersistenceError() == nil {
				t.Fatal("unverified post-commit failure did not remain degraded")
			}
		})
	}
}

func mutateCommittedOperation(mutate func(*Operation)) func([]byte) []byte {
	return func(payload []byte) []byte {
		var operations map[string]*Operation
		if err := json.Unmarshal(payload, &operations); err != nil {
			panic(err)
		}
		for _, operation := range operations {
			if operation.Done {
				mutate(operation)
			}
		}
		payload, _ = json.Marshal(operations)
		return payload
	}
}

func TestUpdateMetadataSerializesWithTerminalPublication(t *testing.T) {
	store := newTerminalPublicationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operation := terminalPublicationTests()[0].register(t, manager)
	initial := publicationMetadata("initial")
	updated := publicationMetadata("updated")
	manager.UpdateMetadata(operation.Name, initial)

	store.blockTerminalSave(false)
	defer store.releaseTerminalSave()
	finalized := make(chan error, 1)
	go func() {
		finalized <- manager.FinalizeDurable(operation.Name, 503, "backend unavailable")
	}()
	store.waitForTerminalSave(t)

	updateStarted := make(chan struct{})
	updateDone := make(chan struct{})
	go func() {
		close(updateStarted)
		manager.UpdateMetadata(operation.Name, updated)
		close(updateDone)
	}()
	<-updateStarted
	deadline := time.Now().Add(25 * time.Millisecond)
	for time.Now().Before(deadline) {
		if operationMetadataEqual(manager.Get(operation.Name).Metadata, updated) {
			break
		}
		runtime.Gosched()
	}

	store.releaseTerminalSave()
	if err := waitForFinalization(t, finalized); err != nil {
		t.Fatal(err)
	}
	select {
	case <-updateDone:
	case <-time.After(time.Second):
		t.Fatal("metadata update did not complete after terminal save")
	}
	terminal := manager.Get(operation.Name)
	if terminal == nil || !terminal.Done {
		t.Fatalf("terminal operation = %+v", terminal)
	}
	assertOperationMetadata(t, terminal, updated)

	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	assertOperationMetadata(t, restarted.Get(operation.Name), updated)
}

func TestOperationMetadataDeepCopyIsolationAndRestart(t *testing.T) {
	store := newTerminalPublicationStore()
	manager, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operation := terminalPublicationTests()[0].register(t, manager)
	metadata := publicationMetadata("original")
	expected := publicationMetadata("original")
	manager.UpdateMetadata(operation.Name, metadata)

	mutatePublicationMetadata(metadata)
	assertOperationMetadata(t, manager.Get(operation.Name), expected)

	returned := manager.Get(operation.Name)
	mutateNormalizedPublicationMetadata(returned.Metadata.(map[string]any))
	assertOperationMetadata(t, manager.Get(operation.Name), expected)

	if err := manager.FinalizeDurable(operation.Name, 503, "backend unavailable"); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	assertOperationMetadata(t, restarted.Get(operation.Name), expected)
}

type terminalPublicationTest struct {
	name           string
	register       func(*testing.T, *OperationManager) *Operation
	finalize       func(*OperationManager, string) error
	assertTerminal func(*testing.T, *Operation)
}

func terminalPublicationTests() []terminalPublicationTest {
	registerUnscoped := func(t *testing.T, manager *OperationManager) *Operation {
		t.Helper()
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
		return operation
	}
	registerScoped := func(t *testing.T, manager *OperationManager) *Operation {
		t.Helper()
		operation, err := manager.RegisterScopedDurable(OperationScope{
			ServiceKind: "clouddeploy#operation",
			Project:     "project-a",
			Location:    "us-central1",
			Target:      "projects/project-a/locations/us-central1/deliveryPipelines/p/releases/r/rollouts/roll1",
		}, "create")
		if err != nil {
			t.Fatal(err)
		}
		return operation
	}
	assertFailure := func(code int, message string) func(*testing.T, *Operation) {
		return func(t *testing.T, operation *Operation) {
			t.Helper()
			if operation == nil || !operation.Done || operation.Status != StatusDone ||
				operation.Error == nil || operation.Error.Code != code ||
				operation.Error.Message != message {
				t.Fatalf("terminal operation = %+v", operation)
			}
		}
	}
	return []terminalPublicationTest{
		{
			name:     "FinalizeDurable",
			register: registerUnscoped,
			finalize: func(manager *OperationManager, name string) error {
				return manager.FinalizeDurable(name, 503, "backend unavailable")
			},
			assertTerminal: assertFailure(503, "backend unavailable"),
		},
		{
			name:     "FinalizeScopedDurable",
			register: registerScoped,
			finalize: func(manager *OperationManager, name string) error {
				return manager.FinalizeScopedDurable(
					name,
					json.RawMessage(`{"name":"rollout-1"}`),
					0,
					"",
				)
			},
			assertTerminal: func(t *testing.T, operation *Operation) {
				t.Helper()
				if operation == nil || !operation.Done || operation.Status != StatusDone || operation.Error != nil {
					t.Fatalf("terminal operation = %+v", operation)
				}
				var response map[string]any
				if err := json.Unmarshal(operation.Response, &response); err != nil ||
					response["name"] != "rollout-1" {
					t.Fatalf("terminal response = %s, error = %v", operation.Response, err)
				}
			},
		},
		{
			name:     "FailDurable",
			register: registerScoped,
			finalize: func(manager *OperationManager, name string) error {
				return manager.FailDurable(name, 403, "permission denied")
			},
			assertTerminal: assertFailure(7, "permission denied"),
		},
	}
}

func mutateOperationCopy(operation *Operation) {
	if operation.Error != nil {
		operation.Error.Message = "mutated"
	}
	if len(operation.Response) != 0 {
		operation.Response[0] = '!'
	}
}

type publicationMetadataFixture struct {
	Labels map[string]string `json:"labels"`
	Nested *struct {
		Values  []string `json:"values"`
		Payload any      `json:"payload"`
	} `json:"nested"`
	Items []any `json:"items"`
}

func publicationMetadata(value string) *publicationMetadataFixture {
	return &publicationMetadataFixture{
		Labels: map[string]string{"stage": value},
		Nested: &struct {
			Values  []string `json:"values"`
			Payload any      `json:"payload"`
		}{
			Values: []string{value},
			Payload: map[string]any{
				"values": []any{value, map[string]any{"nested": value}},
			},
		},
		Items: []any{map[string]any{"value": value}, &struct {
			Value string `json:"value"`
		}{Value: value}},
	}
}

func mutatePublicationMetadata(metadata *publicationMetadataFixture) {
	metadata.Labels["stage"] = "mutated"
	metadata.Nested.Values[0] = "mutated"
	metadata.Nested.Payload.(map[string]any)["values"].([]any)[1].(map[string]any)["nested"] = "mutated"
	metadata.Items[0].(map[string]any)["value"] = "mutated"
	switch item := metadata.Items[1].(type) {
	case *struct {
		Value string `json:"value"`
	}:
		item.Value = "mutated"
	case map[string]any:
		item["value"] = "mutated"
	}
}

func mutateNormalizedPublicationMetadata(metadata map[string]any) {
	metadata["labels"].(map[string]any)["stage"] = "mutated"
	metadata["nested"].(map[string]any)["values"].([]any)[0] = "mutated"
	metadata["nested"].(map[string]any)["payload"].(map[string]any)["values"].([]any)[1].(map[string]any)["nested"] = "mutated"
	metadata["items"].([]any)[0].(map[string]any)["value"] = "mutated"
	metadata["items"].([]any)[1].(map[string]any)["value"] = "mutated"
}

func assertOperationMetadata(t *testing.T, operation *Operation, expected any) {
	t.Helper()
	if operation == nil || !operationMetadataEqual(operation.Metadata, expected) {
		t.Fatalf("operation metadata = %#v, want %#v", operation, expected)
	}
}

func operationMetadataEqual(left, right any) bool {
	normalize := func(value any) any {
		payload, err := json.Marshal(value)
		if err != nil {
			return err
		}
		var normalized any
		if err := json.Unmarshal(payload, &normalized); err != nil {
			return err
		}
		return normalized
	}
	return reflect.DeepEqual(normalize(left), normalize(right))
}

func assertOperationNonterminal(t *testing.T, operation *Operation) {
	t.Helper()
	if operation == nil || operation.Done || operation.Status == StatusDone ||
		operation.Error != nil || operation.Response != nil {
		t.Fatalf("operation published terminal state before durable save: %+v", operation)
	}
}

func assertNoTerminalObservation(t *testing.T, observed <-chan *Operation) {
	t.Helper()
	select {
	case operation := <-observed:
		t.Fatalf("observer was notified before durable publication: %+v", operation)
	default:
	}
}

func waitForFinalization(t *testing.T, finalized <-chan error) error {
	t.Helper()
	select {
	case err := <-finalized:
		return err
	case <-time.After(time.Second):
		t.Fatal("terminal finalization did not return")
		return nil
	}
}

var errTerminalSave = errors.New("terminal save failed")

type terminalPublicationStore struct {
	mu            sync.Mutex
	data          []byte
	saveCount     int
	loadCount     int
	blockOnSave   int
	failFromSave  int
	commitFailure bool
	transform     func([]byte) []byte
	failOnLoad    int
	loadErr       error
	saveStarted   chan struct{}
	releaseSave   chan struct{}
	releaseSaveMu sync.Once
}

func newTerminalPublicationStore() *terminalPublicationStore {
	return &terminalPublicationStore{
		saveStarted: make(chan struct{}),
		releaseSave: make(chan struct{}),
	}
}

func (store *terminalPublicationStore) blockTerminalSave(fail bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.blockOnSave = store.saveCount + 1
	if fail {
		store.failFromSave = store.blockOnSave
	}
}

func (store *terminalPublicationStore) failNextSaveAfterCommit(transform func([]byte) []byte) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.failFromSave = store.saveCount + 1
	store.commitFailure = true
	store.transform = transform
}

func (store *terminalPublicationStore) failNextLoad(err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.failOnLoad = store.loadCount + 1
	store.loadErr = err
}

func (store *terminalPublicationStore) waitForTerminalSave(t *testing.T) {
	t.Helper()
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("terminal save did not start")
	}
}

func (store *terminalPublicationStore) releaseTerminalSave() {
	store.releaseSaveMu.Do(func() {
		close(store.releaseSave)
	})
}

func (store *terminalPublicationStore) Load(_ string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.loadCount++
	if store.loadCount == store.failOnLoad {
		return store.loadErr
	}
	if len(store.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.data, target)
}

func (store *terminalPublicationStore) Save(_ string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	store.mu.Lock()
	store.saveCount++
	saveNumber := store.saveCount
	block := saveNumber == store.blockOnSave
	fail := store.failFromSave != 0 && saveNumber >= store.failFromSave
	commitFailure := store.commitFailure
	transform := store.transform
	store.mu.Unlock()
	if block {
		close(store.saveStarted)
		<-store.releaseSave
	}
	if fail {
		if commitFailure {
			if transform != nil {
				payload = transform(payload)
			}
			store.mu.Lock()
			store.data = append(store.data[:0], payload...)
			store.mu.Unlock()
		}
		return errTerminalSave
	}
	store.mu.Lock()
	store.data = append(store.data[:0], payload...)
	store.mu.Unlock()
	return nil
}

func (store *terminalPublicationStore) operations(t *testing.T) map[string]*Operation {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	var operations map[string]*Operation
	if err := json.Unmarshal(store.data, &operations); err != nil {
		t.Fatal(err)
	}
	return operations
}
