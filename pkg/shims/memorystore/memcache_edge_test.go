package memorystore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const memcacheCreatePayload = `{"nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"memcacheVersion":"MEMCACHE_1_5"}`

func TestMemcacheValidationAndUnsupportedBoundary(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		code   int
		status string
	}{
		{"malformed JSON", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=cache", `{`, 400, "INVALID_ARGUMENT"},
		{"missing node config", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=cache", `{"nodeCount":1}`, 400, "INVALID_ARGUMENT"},
		{"bad id", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=-cache", memcacheCreatePayload, 400, "INVALID_ARGUMENT"},
		{"multi zone", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=cache", `{"nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"zones":["us-central1-a","us-central1-b"]}`, 501, "UNIMPLEMENTED"},
		{"custom network", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=cache", `{"nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"authorizedNetwork":"projects/test/global/networks/custom"}`, 501, "UNIMPLEMENTED"},
		{"reserved range", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=cache", `{"nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"reservedIpRangeId":["range"]}`, 501, "UNIMPLEMENTED"},
		{"TLS field is noncanonical", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=cache", `{"nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"transitEncryptionMode":"SERVER_AUTHENTICATION"}`, 400, "INVALID_ARGUMENT"},
		{"unsupported custom method", http.MethodPost, instancePath("test", "us-central1", "cache") + ":applyParameters", `{}`, 501, "UNIMPLEMENTED"},
		{"unsupported verb", http.MethodPut, instancePath("test", "us-central1", "cache"), `{}`, 405, "METHOD_NOT_ALLOWED"},
		{"oversized body", http.MethodPost, collectionPath("test", "us-central1") + "?instanceId=cache", `{"padding":"` + strings.Repeat("x", maxMemcacheRequestBytes) + `"}`, 400, "INVALID_ARGUMENT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := readyMemcacheBackend(1)
			api, err := NewMemcacheAPIWithStore(orchestrator.NewOperationManager(), backend, nil)
			if err != nil {
				t.Fatal(err)
			}
			response := memcacheRequest(api, test.method, test.path, test.body)
			assertRedisError(t, response, test.code, test.status)
			backend.mu.Lock()
			provisionCalls := backend.provisionCalls
			backend.mu.Unlock()
			if provisionCalls != 0 {
				t.Fatalf("provision calls=%d, want 0", provisionCalls)
			}
		})
	}
}

func TestMemcacheDuplicateNotFoundAndUpdateMask(t *testing.T) {
	backend := readyMemcacheBackend(1)
	api := mustMemcacheAPI(t, backend, nil)
	createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)

	duplicate := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	assertRedisError(t, duplicate, http.StatusConflict, "ALREADY_EXISTS")

	for _, test := range []struct {
		query  string
		body   string
		code   int
		status string
	}{
		{"", `{"displayName":"new"}`, 400, "INVALID_ARGUMENT"},
		{"?updateMask=authorizedNetwork", `{"authorizedNetwork":"default"}`, 400, "INVALID_ARGUMENT"},
		{"?updateMask=displayName,displayName", `{"displayName":"new"}`, 400, "INVALID_ARGUMENT"},
		{"?updateMask=maintenancePolicy", `{"maintenancePolicy":{"description":"window"}}`, 501, "UNIMPLEMENTED"},
	} {
		response := memcacheRequest(api, http.MethodPatch,
			instancePath("test", "us-central1", "cache")+test.query, test.body)
		assertRedisError(t, response, test.code, test.status)
	}

	missingGet := memcacheRequest(api, http.MethodGet, instancePath("test", "us-central1", "missing"), "")
	assertRedisError(t, missingGet, http.StatusNotFound, "NOT_FOUND")
	missingDelete := memcacheRequest(api, http.MethodDelete, instancePath("test", "us-central1", "missing"), "")
	assertRedisError(t, missingDelete, http.StatusNotFound, "NOT_FOUND")
}

func TestMemcacheScopedOpaquePagination(t *testing.T) {
	api := mustMemcacheAPI(t, readyMemcacheBackend(1), nil)
	seedMemcache(t, api, "project-a", "us-central1", "a")
	seedMemcache(t, api, "project-a", "us-central1", "b")
	seedMemcache(t, api, "project-a", "europe-west1", "c")
	seedMemcache(t, api, "project-b", "us-central1", "d")

	first := memcacheRequest(api, http.MethodGet,
		collectionPath("project-a", "us-central1")+"?pageSize=1", "")
	var firstPage struct {
		Instances     []*MemcacheInstance `json:"instances"`
		NextPageToken string              `json:"nextPageToken"`
		Unreachable   []string            `json:"unreachable"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstPage); err != nil {
		t.Fatal(err)
	}
	if first.Code != 200 || len(firstPage.Instances) != 1 || firstPage.NextPageToken == "" ||
		firstPage.Unreachable == nil {
		t.Fatalf("first page=%d %s", first.Code, first.Body.String())
	}
	second := memcacheRequest(api, http.MethodGet,
		collectionPath("project-a", "us-central1")+"?pageSize=1&pageToken="+firstPage.NextPageToken, "")
	if second.Code != 200 || !strings.Contains(second.Body.String(), `/instances/b"`) {
		t.Fatalf("second page=%d %s", second.Code, second.Body.String())
	}
	crossScope := memcacheRequest(api, http.MethodGet,
		collectionPath("project-a", "europe-west1")+"?pageSize=1&pageToken="+firstPage.NextPageToken, "")
	assertRedisError(t, crossScope, 400, "INVALID_ARGUMENT")

	allRegions := memcacheRequest(api, http.MethodGet, collectionPath("project-a", "-"), "")
	if allRegions.Code != 200 ||
		strings.Count(allRegions.Body.String(), `"name":"projects/project-a/`) != 3 ||
		strings.Contains(allRegions.Body.String(), "project-b") {
		t.Fatalf("all regions=%d %s", allRegions.Code, allRegions.Body.String())
	}
}

func TestMemcacheExactMutationStatesAndSerialization(t *testing.T) {
	backend := readyMemcacheBackend(1)
	backend.provisionStarted = make(chan struct{}, 1)
	backend.provisionRelease = make(chan struct{})
	api := mustMemcacheAPI(t, backend, nil)

	create := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	waitSignal(t, backend.provisionStarted)
	assertMemcacheState(t, api, "test", "us-central1", "cache", "CREATING")
	concurrentDelete := memcacheRequest(api, http.MethodDelete,
		instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, concurrentDelete, 409, "ABORTED")
	close(backend.provisionRelease)
	waitForMemcacheOperation(t, api, operationNameFromResponse(t, create))
	assertMemcacheState(t, api, "test", "us-central1", "cache", "READY")

	updateStarted := make(chan struct{}, 1)
	updateRelease := make(chan struct{})
	api.operationRunner = func(_ string, work func() error) {
		go func() {
			updateStarted <- struct{}{}
			<-updateRelease
			_ = work()
		}()
	}
	update := memcacheRequest(api, http.MethodPatch,
		instancePath("test", "us-central1", "cache")+"?updateMask=displayName",
		`{"displayName":"updated"}`)
	waitSignal(t, updateStarted)
	assertMemcacheState(t, api, "test", "us-central1", "cache", "UPDATING")
	close(updateRelease)
	waitForMemcacheOperation(t, api, operationNameFromResponse(t, update))
	api.operationRunner = func(name string, work func() error) {
		go func() {
			_ = api.opMgr.AdvanceDurable(name, 5, orchestrator.StatusRunning)
			_ = work()
		}()
	}

	backend.deleteStarted = make(chan struct{}, 1)
	backend.deleteRelease = make(chan struct{})
	deleted := memcacheRequest(api, http.MethodDelete, instancePath("test", "us-central1", "cache"), "")
	waitSignal(t, backend.deleteStarted)
	assertMemcacheState(t, api, "test", "us-central1", "cache", "DELETING")
	close(backend.deleteRelease)
	waitForMemcacheOperation(t, api, operationNameFromResponse(t, deleted))
}

func TestMemcacheOperationAndMetadataSurviveRestartWithoutReplay(t *testing.T) {
	store, err := state.New(t.TempDir(), "memcache-restart")
	if err != nil {
		t.Fatal(err)
	}
	backend := readyMemcacheBackend(1)
	api, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	create := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	operationName := operationNameFromResponse(t, create)
	waitForMemcacheOperation(t, api, operationName)

	restarted, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	polled := memcacheRequest(restarted, http.MethodGet, "/v1/"+operationName, "")
	if polled.Code != 200 || !strings.Contains(polled.Body.String(), `"done":true`) {
		t.Fatalf("poll after restart=%d %s", polled.Code, polled.Body.String())
	}
	backend.mu.Lock()
	provisionCalls, reconcileCalls := backend.provisionCalls, backend.reconcileCalls
	backend.mu.Unlock()
	if provisionCalls != 1 || reconcileCalls != 1 {
		t.Fatalf("provision=%d reconcile=%d, want 1/1", provisionCalls, reconcileCalls)
	}
	wrongScope := memcacheRequest(restarted, http.MethodGet,
		strings.Replace("/v1/"+operationName, "/us-central1/", "/europe-west1/", 1), "")
	assertRedisError(t, wrongScope, 404, "NOT_FOUND")
}

func TestMemcacheSaveAmbiguityUsesReadback(t *testing.T) {
	store := &ambiguousMemcacheStore{failAfterWrite: true}
	backend := readyMemcacheBackend(1)
	api := mustMemcacheAPI(t, backend, store)
	createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)
	backend.mu.Lock()
	provisionCalls := backend.provisionCalls
	backend.mu.Unlock()
	if provisionCalls != 1 {
		t.Fatalf("provision calls=%d, want 1", provisionCalls)
	}
}

func TestMemcacheBackendFailuresAndOwnershipCompensate(t *testing.T) {
	for _, test := range []struct {
		name       string
		result     MemcacheBackendResult
		err        error
		wantDelete int
		wantState  string
	}{
		{"owned failure", MemcacheBackendResult{Owned: true, Exists: true}, errors.New("backend failed"), 1, ""},
		{"unowned result", MemcacheBackendResult{Owned: false, Exists: true}, nil, 0, "CREATING"},
		{"missing result", MemcacheBackendResult{Owned: true, Exists: false}, nil, 1, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeMemcacheBackend{result: test.result, provisionErr: test.err}
			api := mustMemcacheAPI(t, backend, nil)
			create := memcacheRequest(api, http.MethodPost,
				collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
			operationName := operationNameFromResponse(t, create)
			waitForMemcacheOperationError(t, api, operationName)
			operation := memcacheRequest(api, http.MethodGet, "/v1/"+operationName, "")
			if operation.Code != 200 || !strings.Contains(operation.Body.String(), `"error":{"code":13`) {
				t.Fatalf("terminal operation=%d %s", operation.Code, operation.Body.String())
			}
			resource := memcacheRequest(api, http.MethodGet, instancePath("test", "us-central1", "cache"), "")
			if test.wantState == "" {
				assertRedisError(t, resource, 404, "NOT_FOUND")
			} else if resource.Code != http.StatusOK ||
				!strings.Contains(resource.Body.String(), `"state":"`+test.wantState+`"`) {
				t.Fatalf("resource=%d %s", resource.Code, resource.Body.String())
			}
			backend.mu.Lock()
			deleteCalls := backend.deleteCalls
			backend.mu.Unlock()
			if deleteCalls != test.wantDelete {
				t.Fatalf("delete calls=%d, want %d", deleteCalls, test.wantDelete)
			}
		})
	}
}

func TestMemcacheCreateAdmissionFailurePreventsSideEffects(t *testing.T) {
	store := &selectiveMemcacheStore{failState: "CREATING"}
	backend := readyMemcacheBackend(1)
	api := mustMemcacheAPI(t, backend, store)
	response := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	assertRedisError(t, response, 500, "INTERNAL")
	backend.mu.Lock()
	provisionCalls := backend.provisionCalls
	backend.mu.Unlock()
	if provisionCalls != 0 {
		t.Fatalf("provision calls=%d, want 0", provisionCalls)
	}
	missing := memcacheRequest(api, http.MethodGet, instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, missing, 404, "NOT_FOUND")
}

func TestMemcacheLongRunningOperationShape(t *testing.T) {
	api := mustMemcacheAPI(t, readyMemcacheBackend(1), nil)
	create := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	name := operationNameFromResponse(t, create)
	if !strings.HasPrefix(name, "projects/test/locations/us-central1/operations/") {
		t.Fatalf("operation name=%q", name)
	}
	waitForMemcacheOperation(t, api, name)
	polled := memcacheRequest(api, http.MethodGet, "/v1/"+name, "")
	var operation struct {
		Name     string            `json:"name"`
		Done     bool              `json:"done"`
		Metadata map[string]string `json:"metadata"`
		Response *MemcacheInstance `json:"response"`
	}
	if err := json.Unmarshal(polled.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if polled.Code != 200 || !operation.Done || operation.Name != name ||
		operation.Metadata["@type"] != "type.googleapis.com/google.cloud.memcache.v1.OperationMetadata" ||
		operation.Metadata["target"] != resourceName("test", "us-central1", "cache") ||
		operation.Response == nil || operation.Response.State != "READY" {
		t.Fatalf("operation=%d %s", polled.Code, polled.Body.String())
	}
}

func TestMemcacheReconciliationNeverReplaysSideEffects(t *testing.T) {
	tests := []struct {
		name      string
		state     string
		result    MemcacheBackendResult
		wantFound bool
		wantState string
	}{
		{"unowned ready disclosed", "READY", MemcacheBackendResult{Owned: false, Exists: true}, true, "STATE_UNSPECIFIED"},
		{"missing ready disclosed", "READY", MemcacheBackendResult{Owned: true}, true, "STATE_UNSPECIFIED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &ambiguousMemcacheStore{}
			name := resourceName("test", "us-central1", "cache")
			store.seed(memcacheMetadata{Instances: map[string]memcachePersistedInstance{
				name: {
					Instance:  readyMemcacheInstance("test", "us-central1", "cache", test.state),
					BackendID: memcacheBackendID("test", "us-central1", "cache"),
				},
			}})
			backend := &fakeMemcacheBackend{result: test.result}
			api := mustMemcacheAPI(t, backend, store)
			response := memcacheRequest(api, http.MethodGet, "/v1/"+name, "")
			if !test.wantFound {
				assertRedisError(t, response, 404, "NOT_FOUND")
			} else if response.Code != 200 || !strings.Contains(response.Body.String(), `"state":"`+test.wantState+`"`) {
				t.Fatalf("response=%d %s", response.Code, response.Body.String())
			}
			backend.mu.Lock()
			provision, update, deleted := backend.provisionCalls, backend.updateCalls, backend.deleteCalls
			backend.mu.Unlock()
			if provision != 0 || update != 0 || deleted != 0 {
				t.Fatalf("restart replayed side effects: provision=%d update=%d delete=%d", provision, update, deleted)
			}
		})
	}
}

func TestMemcacheDeleteCompensation(t *testing.T) {
	t.Run("admission save failure", func(t *testing.T) {
		store := &selectiveMemcacheStore{}
		backend := readyMemcacheBackend(1)
		api := mustMemcacheAPI(t, backend, store)
		createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)
		store.failState = "DELETING"
		response := memcacheRequest(api, http.MethodDelete, instancePath("test", "us-central1", "cache"), "")
		assertRedisError(t, response, 500, "INTERNAL")
		backend.mu.Lock()
		deleteCalls := backend.deleteCalls
		backend.mu.Unlock()
		if deleteCalls != 0 {
			t.Fatalf("delete calls=%d, want 0", deleteCalls)
		}
		assertMemcacheState(t, api, "test", "us-central1", "cache", "READY")
	})

	t.Run("backend failure preserves delete provenance", func(t *testing.T) {
		store := newEntryMapStore()
		backend := readyMemcacheBackend(1)
		api := mustMemcacheAPI(t, backend, store)
		createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)
		backend.deleteErr = errors.New("delete failed")
		response := memcacheRequest(api, http.MethodDelete, instancePath("test", "us-central1", "cache"), "")
		operationName := operationNameFromResponse(t, response)
		waitForMemcacheOperationError(t, api, operationName)
		assertMemcacheState(t, api, "test", "us-central1", "cache", "DELETING")
		if _, associated := api.metadataSnapshot().Operations[operationName]; !associated {
			t.Fatal("failed delete lost operation provenance")
		}
		backend.deleteErr = nil
		restarted := mustMemcacheAPI(t, backend, store)
		assertMemcacheTypedOperation(t, restarted, operationName,
			"type.googleapis.com/google.protobuf.Empty", true)
	})

	t.Run("post-delete save failure reconciles without replay", func(t *testing.T) {
		store := &selectiveMemcacheStore{}
		backend := readyMemcacheBackend(1)
		api := mustMemcacheAPI(t, backend, store)
		createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)
		store.mu.Lock()
		store.failEmpty = true
		store.mu.Unlock()
		response := memcacheRequest(api, http.MethodDelete, instancePath("test", "us-central1", "cache"), "")
		operationName := operationNameFromResponse(t, response)
		waitForMemcacheOperationError(t, api, operationName)
		assertMemcacheState(t, api, "test", "us-central1", "cache", "DELETING")

		store.mu.Lock()
		store.failEmpty = false
		store.mu.Unlock()
		backend.mu.Lock()
		backend.result = MemcacheBackendResult{Owned: true, Exists: false}
		deletesBeforeRestart := backend.deleteCalls
		backend.mu.Unlock()
		restarted := mustMemcacheAPI(t, backend, store)
		missing := memcacheRequest(restarted, http.MethodGet, instancePath("test", "us-central1", "cache"), "")
		assertRedisError(t, missing, 404, "NOT_FOUND")
		backend.mu.Lock()
		deletesAfterRestart := backend.deleteCalls
		backend.mu.Unlock()
		if deletesAfterRestart != deletesBeforeRestart {
			t.Fatalf("restart replayed delete: before=%d after=%d", deletesBeforeRestart, deletesAfterRestart)
		}
	})
}

func TestMemcacheCorruptStateIsNotOverwritten(t *testing.T) {
	store := &ambiguousMemcacheStore{}
	store.seed("corrupt")
	api, err := NewMemcacheAPIWithStore(nil, readyMemcacheBackend(1), store)
	if err == nil || api != nil {
		t.Fatalf("api=%#v err=%v, want load failure", api, err)
	}
	var persisted string
	if err := store.Load(memcacheStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("persisted=%q err=%v", persisted, err)
	}
}

func mustMemcacheAPI(t *testing.T, backend MemcacheBackend, store stateStore) *MemcacheAPI {
	t.Helper()
	var manager *orchestrator.OperationManager
	if store == nil {
		manager = orchestrator.NewOperationManager()
	}
	api, err := NewMemcacheAPIWithStore(manager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	return api
}

func readyMemcacheBackend(nodes int) *fakeMemcacheBackend {
	return &fakeMemcacheBackend{result: readyMemcacheResult(nodes)}
}

func readyMemcacheResult(nodes int) MemcacheBackendResult {
	endpoints := make([]string, nodes)
	for index := range endpoints {
		endpoints[index] = fmt.Sprintf("127.0.0.1:%d", 41211+index)
	}
	return MemcacheBackendResult{Owned: true, Exists: true, Endpoints: endpoints}
}

func createAndWaitMemcache(t *testing.T, api *MemcacheAPI, project, location, id, body string) string {
	t.Helper()
	response := memcacheRequest(api, http.MethodPost,
		collectionPath(project, location)+"?instanceId="+id, body)
	if response.Code != 200 {
		t.Fatalf("create=%d %s", response.Code, response.Body.String())
	}
	name := operationNameFromResponse(t, response)
	waitForMemcacheOperation(t, api, name)
	return name
}

func seedMemcache(t *testing.T, api *MemcacheAPI, project, location, id string) {
	t.Helper()
	candidate := api.snapshot()
	name := resourceName(project, location, id)
	candidate[name] = memcachePersistedInstance{
		Instance:  readyMemcacheInstance(project, location, id, "READY"),
		BackendID: memcacheBackendID(project, location, id),
	}
	if err := api.commit(candidate); err != nil {
		t.Fatal(err)
	}
}

func readyMemcacheInstance(project, location, id, stateValue string) *MemcacheInstance {
	instance := &MemcacheInstance{
		Name:              resourceName(project, location, id),
		AuthorizedNetwork: fmt.Sprintf("projects/%s/global/networks/default", project),
		Zones:             []string{location + "-a"},
		NodeCount:         1,
		NodeConfig:        &MemcacheNodeConfig{CPUCount: 1, MemorySizeMB: 1024},
		MemcacheVersion:   "MEMCACHE_1_5",
		CreateTime:        time.Now().UTC().Format(time.RFC3339Nano),
		UpdateTime:        time.Now().UTC().Format(time.RFC3339Nano),
		State:             stateValue,
	}
	if stateValue == "READY" {
		instance.MemcacheFullVersion = memcacheFullVersion(instance.MemcacheVersion)
		instance.DiscoveryEndpoint = "127.0.0.1:41211"
		instance.MemcacheNodes = []MemcacheNode{{
			NodeID:              "node-1",
			Zone:                location + "-a",
			State:               "READY",
			Host:                "127.0.0.1",
			Port:                41211,
			MemcacheVersion:     instance.MemcacheVersion,
			MemcacheFullVersion: memcacheFullVersion(instance.MemcacheVersion),
		}}
	}
	return instance
}

func assertMemcacheState(t *testing.T, api *MemcacheAPI, project, location, id, want string) {
	t.Helper()
	response := memcacheRequest(api, http.MethodGet, instancePath(project, location, id), "")
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"state":"`+want+`"`) {
		t.Fatalf("state response=%d %s, want %s", response.Code, response.Body.String(), want)
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("backend call did not start")
	}
}

func collectionPath(project, location string) string {
	return fmt.Sprintf("/v1/projects/%s/locations/%s/instances", project, location)
}

func instancePath(project, location, id string) string {
	return collectionPath(project, location) + "/" + id
}

func resourceName(project, location, id string) string {
	return strings.TrimPrefix(instancePath(project, location, id), "/v1/")
}

type ambiguousMemcacheStore struct {
	mu             sync.Mutex
	entries        map[string][]byte
	failAfterWrite bool
}

func (store *ambiguousMemcacheStore) Load(key string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	raw := store.entries[key]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (store *ambiguousMemcacheStore) Save(key string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if store.entries == nil {
		store.entries = make(map[string][]byte)
	}
	store.entries[key] = data
	if key == memcacheStateEntry && store.failAfterWrite {
		return errors.New("ambiguous save")
	}
	return nil
}

func (store *ambiguousMemcacheStore) seed(value any) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.entries == nil {
		store.entries = make(map[string][]byte)
	}
	store.entries[memcacheStateEntry], _ = json.Marshal(value)
}

type selectiveMemcacheStore struct {
	mu        sync.Mutex
	entries   map[string][]byte
	failState string
	failEmpty bool
}

func (store *selectiveMemcacheStore) Load(key string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	raw := store.entries[key]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (store *selectiveMemcacheStore) Save(key string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if store.entries == nil {
		store.entries = make(map[string][]byte)
	}
	if key != memcacheStateEntry {
		store.entries[key] = data
		return nil
	}
	var metadata memcacheMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	if store.failEmpty && len(metadata.Instances) == 0 {
		return errors.New("selected empty save failure")
	}
	for _, persisted := range metadata.Instances {
		if persisted.Instance != nil && persisted.Instance.State == store.failState {
			return errors.New("selected save failure")
		}
	}
	store.entries[key] = data
	return nil
}

var _ MemcacheBackend = (*fakeMemcacheBackend)(nil)
var _ stateStore = (*ambiguousMemcacheStore)(nil)
var _ stateStore = (*selectiveMemcacheStore)(nil)
var _ = context.Background
