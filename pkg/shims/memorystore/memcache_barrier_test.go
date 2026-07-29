package memorystore

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
)

func TestMemcacheTerminalPollReleasesMutationBeforeNextMutation(t *testing.T) {
	tests := []struct {
		name       string
		seed       bool
		method     string
		path       string
		body       string
		nextMethod string
		nextPath   string
		nextBody   string
	}{
		{
			name:       "create then patch",
			method:     http.MethodPost,
			path:       collectionPath("test", "us-central1") + "?instanceId=cache",
			body:       memcacheCreatePayload,
			nextMethod: http.MethodPatch,
			nextPath:   instancePath("test", "us-central1", "cache") + "?updateMask=displayName",
			nextBody:   `{"displayName":"updated"}`,
		},
		{
			name:       "update then delete",
			seed:       true,
			method:     http.MethodPatch,
			path:       instancePath("test", "us-central1", "cache") + "?updateMask=displayName",
			body:       `{"displayName":"updated"}`,
			nextMethod: http.MethodDelete,
			nextPath:   instancePath("test", "us-central1", "cache"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newMemcacheBarrierStore()
			api := mustMemcacheAPI(t, readyMemcacheBackend(1), store)
			if test.seed {
				seedMemcache(t, api, "test", "us-central1", "cache")
			}
			entered, release := store.blockNextAssociationClear(false)
			response := memcacheRequest(api, test.method, test.path, test.body)
			if response.Code != http.StatusOK {
				t.Fatalf("mutation=%d %s", response.Code, response.Body.String())
			}
			operationName := operationNameFromResponse(t, response)
			nextResponse := pollDoneThenMemcacheRequest(
				api, operationName, test.nextMethod, test.nextPath, test.nextBody,
			)
			waitSignal(t, entered)

			select {
			case early := <-nextResponse:
				close(release)
				t.Fatalf("terminal operation became visible before mutation release: next=%d %s",
					early.Code, early.Body)
			case <-time.After(150 * time.Millisecond):
			}

			close(release)
			select {
			case next := <-nextResponse:
				if next.Code != http.StatusOK {
					t.Fatalf("immediate next mutation=%d %s", next.Code, next.Body)
				}
				if next.OperationName == "" {
					t.Fatalf("immediate next mutation returned no operation: %s", next.Body)
				}
				waitForMemcacheManagerTerminal(t, api.opMgr, next.OperationName)
			case <-time.After(4 * time.Second):
				t.Fatal("next mutation did not return after terminal poll")
			}
		})
	}
}

func TestMemcacheTerminalOperationSaveFailureSkipsBarrierAndReconciles(t *testing.T) {
	store := newMemcacheBarrierStore()
	store.failNextTerminalOperationSave()
	backend := readyMemcacheBackend(1)
	api := mustMemcacheAPI(t, backend, store)

	response := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	if response.Code != http.StatusOK {
		t.Fatalf("create=%d %s", response.Code, response.Body.String())
	}
	operationName := operationNameFromResponse(t, response)
	waitForMemcachePersistenceFailure(t, api.opMgr)

	if operation := api.opMgr.Get(operationName); operation == nil || operation.Done {
		t.Fatalf("visible operation=%+v, want retained nonterminal operation", operation)
	}
	if _, associated := api.metadataSnapshot().Operations[operationName]; !associated {
		t.Fatal("terminal save failure cleared restart association")
	}
	if !api.mutationMu.TryLock() {
		t.Fatal("terminal save failure retained mutation lock")
	}
	api.mutationMu.Unlock()
	polled := memcacheRequest(api, http.MethodGet, "/v1/"+operationName, "")
	if polled.Code != http.StatusOK || !strings.Contains(polled.Body.String(), `"done":false`) {
		t.Fatalf("durable nonterminal operation was hidden by persistence degradation: %d %s",
			polled.Code, polled.Body.String())
	}
	blocked := memcacheRequest(api, http.MethodGet,
		instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")

	restarted := mustMemcacheAPI(t, backend, store)
	assertMemcacheTypedOperation(t, restarted, operationName,
		"type.googleapis.com/google.cloud.memcache.v1.Instance", false)
	if _, associated := restarted.metadataSnapshot().Operations[operationName]; associated {
		t.Fatal("restart did not clear reconciled operation association")
	}
}

func TestMemcacheAssociationBarrierFailurePublishesTruthAndReconciles(t *testing.T) {
	store := newMemcacheBarrierStore()
	entered, release := store.blockNextAssociationClear(true)
	backend := readyMemcacheBackend(1)
	api := mustMemcacheAPI(t, backend, store)

	response := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	if response.Code != http.StatusOK {
		t.Fatalf("create=%d %s", response.Code, response.Body.String())
	}
	operationName := operationNameFromResponse(t, response)
	waitSignal(t, entered)
	close(release)
	waitForMemcacheManagerTerminal(t, api.opMgr, operationName)

	operation := api.opMgr.Get(operationName)
	if operation == nil || !operation.Done || operation.Error != nil {
		t.Fatalf("terminal operation=%+v, want durable truthful success", operation)
	}
	if _, associated := api.metadataSnapshot().Operations[operationName]; !associated {
		t.Fatal("failed association clear was not retained for restart")
	}
	if !errors.Is(api.initializationError(), orchestrator.ErrOperationTerminalBarrier) {
		t.Fatalf("initialization error=%v, want terminal barrier classification", api.initializationError())
	}
	if !api.mutationMu.TryLock() {
		t.Fatal("association barrier failure retained mutation lock")
	}
	api.mutationMu.Unlock()
	blocked := memcacheRequest(api, http.MethodGet,
		instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")
	polled := memcacheRequest(api, http.MethodGet, "/v1/"+operationName, "")
	if polled.Code != http.StatusOK {
		t.Fatalf("terminal operation was hidden by barrier degradation: %d %s",
			polled.Code, polled.Body.String())
	}
	unknown := memcacheRequest(api, http.MethodGet,
		"/v1/projects/test/locations/us-central1/operations/unknown", "")
	assertRedisError(t, unknown, http.StatusServiceUnavailable, "UNAVAILABLE")

	restarted := mustMemcacheAPI(t, backend, store)
	assertMemcacheTerminalResultEqual(t, operation, restarted.opMgr.Get(operationName))
	assertMemcacheTypedOperation(t, restarted, operationName,
		"type.googleapis.com/google.cloud.memcache.v1.Instance", false)
	if _, associated := restarted.metadataSnapshot().Operations[operationName]; associated {
		t.Fatal("restart did not clear barrier-failed association")
	}
}

func TestMemcacheDurableOperationPollingSurvivesCommittedDegradation(t *testing.T) {
	for _, test := range []struct {
		name      string
		backend   *fakeMemcacheBackend
		wantError bool
	}{
		{name: "terminal success", backend: readyMemcacheBackend(1)},
		{
			name: "terminal error",
			backend: &fakeMemcacheBackend{
				provisionErr: errors.New("provision failed after admission"),
			},
			wantError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newMemcacheBarrierStore()
			store.failNextTerminalOperationSaveAfterWrite()
			api := mustMemcacheAPI(t, test.backend, store)
			foreign, err := api.opMgr.RegisterScopedDurable(orchestrator.OperationScope{
				ServiceKind: "redis#operation",
				Project:     "test",
				Location:    "us-central1",
				Target:      "projects/test/locations/us-central1/instances/foreign",
			}, "create")
			if err != nil {
				t.Fatal(err)
			}
			response := memcacheRequest(api, http.MethodPost,
				collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
			if response.Code != http.StatusOK {
				t.Fatalf("create=%d %s", response.Code, response.Body.String())
			}
			operationName := operationNameFromResponse(t, response)
			waitForMemcachePersistenceFailure(t, api.opMgr)
			waitForMemcacheManagerTerminal(t, api.opMgr, operationName)

			polled := memcacheRequest(api, http.MethodGet, "/v1/"+operationName, "")
			if polled.Code != http.StatusOK {
				t.Fatalf("committed terminal operation poll=%d %s", polled.Code, polled.Body.String())
			}
			var operation struct {
				Done     bool           `json:"done"`
				Error    any            `json:"error"`
				Response map[string]any `json:"response"`
			}
			if err := json.Unmarshal(polled.Body.Bytes(), &operation); err != nil {
				t.Fatal(err)
			}
			if !operation.Done || (operation.Error != nil) != test.wantError {
				t.Fatalf("operation=%s", polled.Body.String())
			}
			if !test.wantError &&
				operation.Response["@type"] != "type.googleapis.com/google.cloud.memcache.v1.Instance" {
				t.Fatalf("terminal response=%v", operation.Response)
			}

			unknown := memcacheRequest(api, http.MethodGet,
				"/v1/projects/test/locations/us-central1/operations/unknown", "")
			assertRedisError(t, unknown, http.StatusServiceUnavailable, "UNAVAILABLE")
			operationID := operationName[strings.LastIndex(operationName, "/")+1:]
			for _, path := range []string{
				"/v1/projects/other/locations/us-central1/operations/" + operationID,
				"/v1/projects/test/locations/europe-west1/operations/" + operationID,
				"/v1/" + foreign.Name,
			} {
				crossScope := memcacheRequest(api, http.MethodGet, path, "")
				assertRedisError(t, crossScope, http.StatusServiceUnavailable, "UNAVAILABLE")
			}

			test.backend.mu.Lock()
			beforeProvision := test.backend.provisionCalls
			test.backend.mu.Unlock()
			blockedMutation := memcacheRequest(api, http.MethodPost,
				collectionPath("test", "us-central1")+"?instanceId=other", memcacheCreatePayload)
			assertRedisError(t, blockedMutation, http.StatusServiceUnavailable, "UNAVAILABLE")
			test.backend.mu.Lock()
			afterProvision := test.backend.provisionCalls
			test.backend.mu.Unlock()
			if afterProvision != beforeProvision {
				t.Fatalf("degraded mutation reached backend: before=%d after=%d",
					beforeProvision, afterProvision)
			}
			blockedResource := memcacheRequest(api, http.MethodGet,
				instancePath("test", "us-central1", "cache"), "")
			assertRedisError(t, blockedResource, http.StatusServiceUnavailable, "UNAVAILABLE")
			for _, request := range []struct {
				method string
				path   string
			}{
				{method: http.MethodDelete, path: "/v1/" + operationName},
				{method: http.MethodPost, path: "/v1/" + operationName + ":cancel"},
				{
					method: http.MethodGet,
					path: "/v1/projects/test/locations/us-central1/operations%2F" +
						operationID,
				},
			} {
				blocked := memcacheRequest(api, request.method, request.path, "")
				assertRedisError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")
			}
		})
	}
}

func pollDoneThenMemcacheRequest(
	api *MemcacheAPI,
	operationName string,
	method string,
	path string,
	body string,
) <-chan *responseRecorder {
	result := make(chan *responseRecorder, 1)
	go func() {
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			response := memcacheRequest(api, http.MethodGet, "/v1/"+operationName, "")
			var operation struct {
				Done bool `json:"done"`
			}
			if response.Code == http.StatusOK && json.Unmarshal(response.Body.Bytes(), &operation) == nil && operation.Done {
				next := memcacheRequest(api, method, path, body)
				var nextOperation struct {
					Name string `json:"name"`
				}
				_ = json.Unmarshal(next.Body.Bytes(), &nextOperation)
				result <- &responseRecorder{
					Code: next.Code, Body: next.Body.String(), OperationName: nextOperation.Name,
				}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		result <- &responseRecorder{Code: 0, Body: "operation polling timed out"}
	}()
	return result
}

type responseRecorder struct {
	Code          int
	Body          string
	OperationName string
}

func waitForMemcacheManagerTerminal(t *testing.T, manager *orchestrator.OperationManager, name string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if operation := manager.Get(name); operation != nil && operation.Done {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("operation manager did not publish terminal operation")
}

func waitForMemcachePersistenceFailure(t *testing.T, manager *orchestrator.OperationManager) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if manager.PersistenceError() != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("operation manager did not report persistence failure")
}

type memcacheBarrierStore struct {
	delegate *entryMapStore

	mu                              sync.Mutex
	clearEntered                    chan struct{}
	clearRelease                    chan struct{}
	failClear                       bool
	failTerminalOperation           bool
	failTerminalOperationAfterWrite bool
}

func newMemcacheBarrierStore() *memcacheBarrierStore {
	return &memcacheBarrierStore{delegate: newEntryMapStore()}
}

func (store *memcacheBarrierStore) Load(key string, target any) error {
	return store.delegate.Load(key, target)
}

func (store *memcacheBarrierStore) Save(key string, value any) error {
	if key == "orchestrator/operations" {
		var operations map[string]*orchestrator.Operation
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &operations); err != nil {
			return err
		}
		terminal := false
		for _, operation := range operations {
			terminal = terminal || operation != nil && operation.Done
		}
		store.mu.Lock()
		fail := terminal && store.failTerminalOperation
		failAfterWrite := terminal && store.failTerminalOperationAfterWrite
		if fail || failAfterWrite {
			store.failTerminalOperation = false
			store.failTerminalOperationAfterWrite = false
		}
		store.mu.Unlock()
		if fail {
			return errors.New("blocked terminal operation save")
		}
		if failAfterWrite {
			if err := store.delegate.Save(key, value); err != nil {
				return err
			}
			return errors.New("committed terminal operation save failure")
		}
	}
	if key == memcacheStateEntry {
		var metadata memcacheMetadata
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return err
		}
		if len(metadata.Operations) == 0 {
			store.mu.Lock()
			entered, release, fail := store.clearEntered, store.clearRelease, store.failClear
			if entered != nil {
				store.clearEntered = nil
				store.clearRelease = nil
				store.failClear = false
			}
			store.mu.Unlock()
			if entered != nil {
				close(entered)
				<-release
				if fail {
					return errors.New("association clear failed")
				}
			}
		}
	}
	return store.delegate.Save(key, value)
}

func (store *memcacheBarrierStore) blockNextAssociationClear(fail bool) (<-chan struct{}, chan struct{}) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.clearEntered = make(chan struct{})
	store.clearRelease = make(chan struct{})
	store.failClear = fail
	return store.clearEntered, store.clearRelease
}

func (store *memcacheBarrierStore) failNextTerminalOperationSave() {
	store.mu.Lock()
	store.failTerminalOperation = true
	store.mu.Unlock()
}

func (store *memcacheBarrierStore) failNextTerminalOperationSaveAfterWrite() {
	store.mu.Lock()
	store.failTerminalOperationAfterWrite = true
	store.mu.Unlock()
}

var _ stateStore = (*memcacheBarrierStore)(nil)
