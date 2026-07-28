package memorystore

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func TestMemcacheRejectsUnsupportedBackendInputsBeforeAdmission(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "multiple nodes",
			body: `{"nodeCount":2,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024}}`,
		},
		{
			name: "custom parameters",
			body: `{"nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"parameters":{"params":{"-m":"64"}}}`,
		},
		{
			name: "empty custom parameters",
			body: `{"nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"parameters":{}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := orchestrator.NewOperationManager()
			store := newEntryMapStore()
			backend := readyMemcacheBackend(1)
			api, err := NewMemcacheAPIWithStore(manager, backend, nil)
			if err != nil {
				t.Fatal(err)
			}
			response := memcacheRequest(api, http.MethodPost,
				collectionPath("test", "us-central1")+"?instanceId=cache", test.body)
			assertRedisError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
			if len(manager.List()) != 0 || len(api.snapshot()) != 0 {
				t.Fatalf("operations=%d resources=%d, want no admission", len(manager.List()), len(api.snapshot()))
			}
			var saved memcacheMetadata
			if err := store.Load(memcacheStateEntry, &saved); !errors.Is(err, state.ErrNotFound) {
				t.Fatalf("state mutation err=%v state=%+v", err, saved)
			}
			backend.mu.Lock()
			provisionCalls := backend.provisionCalls
			backend.mu.Unlock()
			if provisionCalls != 0 {
				t.Fatalf("provision calls=%d, want 0", provisionCalls)
			}
		})
	}

	t.Run("node count update", func(t *testing.T) {
		backend := readyMemcacheBackend(1)
		api := mustMemcacheAPI(t, backend, nil)
		seedMemcache(t, api, "test", "us-central1", "cache")
		before := api.snapshot()
		beforeOperations := len(api.opMgr.List())
		response := memcacheRequest(api, http.MethodPatch,
			instancePath("test", "us-central1", "cache")+"?updateMask=nodeCount", `{"nodeCount":2}`)
		assertRedisError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
		if len(api.opMgr.List()) != beforeOperations || !reflect.DeepEqual(before, api.snapshot()) {
			t.Fatalf("unsupported resize mutated state")
		}
		backend.mu.Lock()
		updateCalls := backend.updateCalls
		backend.mu.Unlock()
		if updateCalls != 0 {
			t.Fatalf("update calls=%d, want 0", updateCalls)
		}
	})
}

func TestMemcachePersistentConstructorOwnsDurableOperationManager(t *testing.T) {
	store, err := state.New(t.TempDir(), "constructor")
	if err != nil {
		t.Fatal(err)
	}
	backend := readyMemcacheBackend(1)
	api, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	name := createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)

	restarted, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	polled := memcacheRequest(restarted, http.MethodGet, "/v1/"+name, "")
	if polled.Code != http.StatusOK || !strings.Contains(polled.Body.String(), `"done":true`) {
		t.Fatalf("poll after restart=%d %s", polled.Code, polled.Body.String())
	}

	if _, err := NewMemcacheAPIWithStore(orchestrator.NewOperationManager(), backend, store); err == nil {
		t.Fatal("persistent constructor accepted an unverifiable operation manager")
	}
}

func TestMemcachePersistedIdentityValidationFailsClosed(t *testing.T) {
	baseName := resourceName("project-a", "us-central1", "cache")
	base := memcacheMetadata{Instances: map[string]memcachePersistedInstance{
		baseName: {
			Instance:  readyMemcacheInstance("project-a", "us-central1", "cache", "READY"),
			BackendID: memcacheBackendID("project-a", "us-central1", "cache"),
		},
	}}
	tests := []struct {
		name   string
		mutate func(*memcacheMetadata)
	}{
		{"backend alias", func(metadata *memcacheMetadata) {
			entry := metadata.Instances[baseName]
			entry.BackendID = memcacheBackendID("project-b", "us-central1", "cache")
			metadata.Instances[baseName] = entry
		}},
		{"resource alias", func(metadata *memcacheMetadata) {
			entry := metadata.Instances[baseName]
			entry.Instance.Name = resourceName("project-b", "us-central1", "cache")
			metadata.Instances[baseName] = entry
		}},
		{"invalid location", func(metadata *memcacheMetadata) {
			entry := metadata.Instances[baseName]
			delete(metadata.Instances, baseName)
			name := resourceName("project-a", "US-CENTRAL1", "cache")
			entry.Instance.Name = name
			entry.BackendID = memcacheBackendID("project-a", "US-CENTRAL1", "cache")
			metadata.Instances[name] = entry
		}},
		{"invalid id", func(metadata *memcacheMetadata) {
			entry := metadata.Instances[baseName]
			delete(metadata.Instances, baseName)
			name := resourceName("project-a", "us-central1", "-cache")
			entry.Instance.Name = name
			entry.BackendID = memcacheBackendID("project-a", "us-central1", "-cache")
			metadata.Instances[name] = entry
		}},
		{"duplicate backend", func(metadata *memcacheMetadata) {
			otherName := resourceName("project-a", "us-central1", "other")
			metadata.Instances[otherName] = memcachePersistedInstance{
				Instance:  readyMemcacheInstance("project-a", "us-central1", "other", "READY"),
				BackendID: metadata.Instances[baseName].BackendID,
			}
		}},
		{"invalid state", func(metadata *memcacheMetadata) {
			entry := metadata.Instances[baseName]
			entry.Instance.State = "REPAIRING"
			metadata.Instances[baseName] = entry
		}},
		{"non-loopback host", func(metadata *memcacheMetadata) {
			entry := metadata.Instances[baseName]
			entry.Instance.MemcacheNodes[0].Host = "10.0.0.2"
			metadata.Instances[baseName] = entry
		}},
		{"invalid port", func(metadata *memcacheMetadata) {
			entry := metadata.Instances[baseName]
			entry.Instance.MemcacheNodes[0].Port = 70000
			metadata.Instances[baseName] = entry
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newEntryMapStore()
			metadata := cloneMemcacheMetadata(base)
			test.mutate(&metadata)
			if err := store.Save(memcacheStateEntry, metadata); err != nil {
				t.Fatal(err)
			}
			before := store.raw(memcacheStateEntry)
			api, err := NewMemcacheAPIWithStore(nil, readyMemcacheBackend(1), store)
			if err == nil || api != nil {
				t.Fatalf("api=%#v err=%v, want validation failure", api, err)
			}
			if after := store.raw(memcacheStateEntry); !reflect.DeepEqual(before, after) {
				t.Fatal("invalid persisted state was modified")
			}
		})
	}
}

func TestMemcachePersistedOperationAssociationValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*memcacheMetadata, string)
	}{
		{"cross-project resource", func(metadata *memcacheMetadata, operationName string) {
			record := metadata.Operations[operationName]
			record.ResourceName = resourceName("project-b", "us-central1", "cache")
			metadata.Operations[operationName] = record
		}},
		{"action without compensation snapshot", func(metadata *memcacheMetadata, operationName string) {
			record := metadata.Operations[operationName]
			record.Action = "update"
			metadata.Operations[operationName] = record
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, operationName := seedInterruptedMemcacheOperation(t, "create", "CREATING", nil, true)
			var metadata memcacheMetadata
			if err := store.Load(memcacheStateEntry, &metadata); err != nil {
				t.Fatal(err)
			}
			test.mutate(&metadata, operationName)
			if err := store.Save(memcacheStateEntry, metadata); err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			api, err := NewMemcacheAPIWithStore(nil, readyMemcacheBackend(1), store)
			if err == nil || api != nil {
				t.Fatalf("api=%#v err=%v, want association validation failure", api, err)
			}
			var after memcacheMetadata
			if err := store.Load(memcacheStateEntry, &after); err != nil {
				t.Fatal(err)
			}
			afterRaw, _ := json.Marshal(after)
			if !reflect.DeepEqual(before, afterRaw) {
				t.Fatal("invalid operation association was modified")
			}
		})
	}
}

func TestMemcacheCrashReconciliation(t *testing.T) {
	t.Run("create exact-owned success", func(t *testing.T) {
		store, operationName := seedInterruptedMemcacheOperation(t, "create", "CREATING", nil, true)
		backend := readyMemcacheBackend(1)
		api, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheState(t, api, "test", "us-central1", "cache", "READY")
		assertMemcacheTypedOperation(t, api, operationName,
			"type.googleapis.com/google.cloud.memcache.v1.Instance", false)
		duplicate := memcacheRequest(api, http.MethodPost,
			collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
		assertRedisError(t, duplicate, http.StatusConflict, "ALREADY_EXISTS")
		assertNoMemcacheMutationReplay(t, backend)
	})

	t.Run("create final resource before operation completion", func(t *testing.T) {
		store, operationName := seedInterruptedMemcacheOperation(t, "create", "READY", nil, true)
		backend := readyMemcacheBackend(1)
		api, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheTypedOperation(t, api, operationName,
			"type.googleapis.com/google.cloud.memcache.v1.Instance", false)
		assertNoMemcacheMutationReplay(t, backend)
	})

	t.Run("create missing compensates and retry succeeds", func(t *testing.T) {
		store, operationName := seedInterruptedMemcacheOperation(t, "create", "CREATING", nil, true)
		backend := &fakeMemcacheBackend{result: MemcacheBackendResult{Owned: true, Exists: false}}
		api, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheOperationError(t, api, operationName)
		missing := memcacheRequest(api, http.MethodGet, instancePath("test", "us-central1", "cache"), "")
		assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
		backend.mu.Lock()
		backend.result = readyMemcacheResult(1)
		backend.mu.Unlock()
		createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)
	})

	for _, test := range []struct {
		name   string
		result MemcacheBackendResult
		err    error
	}{
		{
			name:   "create inspection uncertain",
			result: MemcacheBackendResult{},
			err:    errors.New("inspect unavailable"),
		},
		{
			name:   "create foreign backend",
			result: MemcacheBackendResult{Exists: true},
			err:    errors.New("ownership conflict"),
		},
		{
			name:   "create wrong version",
			result: MemcacheBackendResult{Owned: true, Exists: true},
			err:    errors.New("version mismatch"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, operationName := seedInterruptedMemcacheOperation(t, "create", "CREATING", nil, true)
			backend := &fakeMemcacheBackend{result: test.result, reconcileErr: test.err}
			api, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheState(t, api, "test", "us-central1", "cache", "CREATING")
			assertMemcacheOperationError(t, api, operationName)
			if _, associated := api.metadataSnapshot().Operations[operationName]; !associated {
				t.Fatal("uncertain create lost its durable operation association")
			}
			retryCreate := memcacheRequest(api, http.MethodPost,
				collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
			assertRedisError(t, retryCreate, http.StatusConflict, "ALREADY_EXISTS")
			retryDelete := memcacheRequest(api, http.MethodDelete,
				instancePath("test", "us-central1", "cache"), "")
			assertRedisError(t, retryDelete, http.StatusConflict, "ABORTED")
			assertNoMemcacheMutationReplay(t, backend)
		})
	}

	t.Run("uncertain create later resolves exact-owned", func(t *testing.T) {
		store, operationName := seedInterruptedMemcacheOperation(t, "create", "CREATING", nil, true)
		backend := &fakeMemcacheBackend{reconcileErr: errors.New("inspect unavailable")}
		first, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheState(t, first, "test", "us-central1", "cache", "CREATING")
		repeated, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheState(t, repeated, "test", "us-central1", "cache", "CREATING")
		if _, associated := repeated.metadataSnapshot().Operations[operationName]; !associated {
			t.Fatal("repeated uncertain restart lost create provenance")
		}
		backend.mu.Lock()
		backend.result = readyMemcacheResult(1)
		backend.reconcileErr = nil
		backend.mu.Unlock()
		second, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheState(t, second, "test", "us-central1", "cache", "READY")
		assertMemcacheTypedOperation(t, second, operationName,
			"type.googleapis.com/google.cloud.memcache.v1.Instance", false)
		assertNoMemcacheMutationReplay(t, backend)
	})

	t.Run("uncertain create later resolves absent and retries", func(t *testing.T) {
		store, operationName := seedInterruptedMemcacheOperation(t, "create", "CREATING", nil, true)
		backend := &fakeMemcacheBackend{reconcileErr: errors.New("inspect unavailable")}
		first, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheState(t, first, "test", "us-central1", "cache", "CREATING")
		backend.mu.Lock()
		backend.result = MemcacheBackendResult{Exists: false}
		backend.reconcileErr = nil
		backend.mu.Unlock()
		second, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheOperationError(t, second, operationName)
		missing := memcacheRequest(second, http.MethodGet,
			instancePath("test", "us-central1", "cache"), "")
		assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
		backend.mu.Lock()
		backend.result = readyMemcacheResult(1)
		backend.mu.Unlock()
		createAndWaitMemcache(t, second, "test", "us-central1", "cache", memcacheCreatePayload)
	})

	t.Run("update exact-owned completes metadata only", func(t *testing.T) {
		previous := memcachePersistedInstance{
			Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
			BackendID: memcacheBackendID("test", "us-central1", "cache"),
		}
		store, operationName := seedInterruptedMemcacheOperation(t, "update", "UPDATING", &previous, true)
		var metadata memcacheMetadata
		if err := store.Load(memcacheStateEntry, &metadata); err != nil {
			t.Fatal(err)
		}
		entry := metadata.Instances[resourceName("test", "us-central1", "cache")]
		entry.Instance.DisplayName = "updated"
		metadata.Instances[entry.Instance.Name] = entry
		if err := store.Save(memcacheStateEntry, metadata); err != nil {
			t.Fatal(err)
		}
		backend := readyMemcacheBackend(1)
		api, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		get := memcacheRequest(api, http.MethodGet, instancePath("test", "us-central1", "cache"), "")
		if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"displayName":"updated"`) {
			t.Fatalf("get=%d %s", get.Code, get.Body.String())
		}
		assertMemcacheTypedOperation(t, api, operationName,
			"type.googleapis.com/google.cloud.memcache.v1.Instance", false)
		assertNoMemcacheMutationReplay(t, backend)
	})

	t.Run("delete missing completes success", func(t *testing.T) {
		previous := memcachePersistedInstance{
			Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
			BackendID: memcacheBackendID("test", "us-central1", "cache"),
		}
		store, operationName := seedInterruptedMemcacheOperation(t, "delete", "DELETING", &previous, true)
		backend := &fakeMemcacheBackend{result: MemcacheBackendResult{Owned: true, Exists: false}}
		api, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheTypedOperation(t, api, operationName,
			"type.googleapis.com/google.protobuf.Empty", false)
		retry := memcacheRequest(api, http.MethodDelete, instancePath("test", "us-central1", "cache"), "")
		assertRedisError(t, retry, http.StatusNotFound, "NOT_FOUND")
		assertNoMemcacheMutationReplay(t, backend)
	})

	t.Run("delete final resource removal before operation completion", func(t *testing.T) {
		previous := memcachePersistedInstance{
			Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
			BackendID: memcacheBackendID("test", "us-central1", "cache"),
		}
		store, operationName := seedInterruptedMemcacheOperation(t, "delete", "DELETING", &previous, false)
		backend := &fakeMemcacheBackend{result: MemcacheBackendResult{Exists: false}}
		api, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheTypedOperation(t, api, operationName,
			"type.googleapis.com/google.protobuf.Empty", true)
		assertNoMemcacheMutationReplay(t, backend)
	})

}

func TestMemcacheTerminalReconciliationPreservesExactlyOnceResult(t *testing.T) {
	previous := memcachePersistedInstance{
		Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
		BackendID: memcacheBackendID("test", "us-central1", "cache"),
	}
	for _, test := range []struct {
		name              string
		action            string
		state             string
		previous          *memcachePersistedInstance
		result            MemcacheBackendResult
		wantResource      bool
		wantResourceState string
	}{
		{
			name:              "create restart",
			action:            "create",
			state:             "CREATING",
			result:            readyMemcacheResult(1),
			wantResource:      true,
			wantResourceState: "READY",
		},
		{
			name:              "update restart",
			action:            "update",
			state:             "UPDATING",
			previous:          &previous,
			result:            readyMemcacheResult(1),
			wantResource:      true,
			wantResourceState: "READY",
		},
		{
			name:     "delete restart with authoritative absence",
			action:   "delete",
			state:    "DELETING",
			previous: &previous,
			result:   MemcacheBackendResult{Owned: true, Exists: false},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, operationName := seedInterruptedMemcacheOperation(
				t, test.action, test.state, test.previous, true,
			)
			manager, err := orchestrator.NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.FinalizeScopedDurable(
				operationName, nil, 13, "original terminal failure",
			); err != nil {
				t.Fatal(err)
			}
			before := manager.Get(operationName)

			backend := &fakeMemcacheBackend{result: test.result}
			api, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheTerminalResultEqual(t, before, api.opMgr.Get(operationName))
			if _, associated := api.metadataSnapshot().Operations[operationName]; associated {
				t.Fatal("authoritatively reconciled terminal operation retained its association")
			}
			if test.wantResource {
				assertMemcacheState(t, api, "test", "us-central1", "cache", test.wantResourceState)
			} else {
				missing := memcacheRequest(api, http.MethodGet,
					instancePath("test", "us-central1", "cache"), "")
				assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
			}
			assertNoMemcacheMutationReplay(t, backend)
		})
	}
}

func TestMemcacheTerminalReconciliationSameResultRetry(t *testing.T) {
	previous := memcachePersistedInstance{
		Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
		BackendID: memcacheBackendID("test", "us-central1", "cache"),
	}
	store, operationName := seedInterruptedMemcacheOperation(
		t, "delete", "DELETING", &previous, false,
	)
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.FinalizeScopedDurable(
		operationName,
		json.RawMessage(`{"@type":"type.googleapis.com/google.protobuf.Empty"}`),
		0,
		"",
	); err != nil {
		t.Fatal(err)
	}
	before := manager.Get(operationName)

	backend := &fakeMemcacheBackend{result: MemcacheBackendResult{Owned: true, Exists: false}}
	api, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	assertMemcacheTerminalResultEqual(t, before, api.opMgr.Get(operationName))
	if _, associated := api.metadataSnapshot().Operations[operationName]; associated {
		t.Fatal("idempotent terminal retry did not clear operation association")
	}
	assertNoMemcacheMutationReplay(t, backend)
}

func TestMemcacheTerminalSuccessSurvivesAuthoritativeBackendAbsence(t *testing.T) {
	store, operationName := seedInterruptedMemcacheOperation(
		t, "create", "READY", nil, true,
	)
	var metadata memcacheMetadata
	if err := store.Load(memcacheStateEntry, &metadata); err != nil {
		t.Fatal(err)
	}
	response, err := typedMemcacheInstance(
		metadata.Instances[resourceName("test", "us-central1", "cache")].Instance,
	)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.FinalizeScopedDurable(operationName, response, 0, ""); err != nil {
		t.Fatal(err)
	}
	before := manager.Get(operationName)

	backend := &fakeMemcacheBackend{result: MemcacheBackendResult{Owned: true, Exists: false}}
	api, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	assertMemcacheTerminalResultEqual(t, before, api.opMgr.Get(operationName))
	missing := memcacheRequest(api, http.MethodGet,
		instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
	if _, associated := api.metadataSnapshot().Operations[operationName]; associated {
		t.Fatal("authoritative absence retained terminal create association")
	}
	assertNoMemcacheMutationReplay(t, backend)
}

func TestMemcacheReconcilePassesPersistedBackendSpec(t *testing.T) {
	store, _ := seedInterruptedMemcacheOperation(t, "create", "CREATING", nil, true)
	backend := &fakeMemcacheBackend{result: MemcacheBackendResult{Exists: false}}
	if _, err := NewMemcacheAPIWithStore(nil, backend, store); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.reconcileSpecs) != 1 {
		t.Fatalf("reconcile specs=%d, want 1", len(backend.reconcileSpecs))
	}
	spec := backend.reconcileSpecs[0]
	if spec.BackendID != memcacheBackendID("test", "us-central1", "cache") ||
		spec.NodeCount != 1 || spec.CPUCount != 1 || spec.MemoryMB != 1024 ||
		spec.Version != "MEMCACHE_1_5" || len(spec.Params) != 0 {
		t.Fatalf("reconcile spec=%+v", spec)
	}
}

func TestMemcacheInterruptedDeletePreservesProvenanceUntilAuthoritativeAbsence(t *testing.T) {
	t.Run("exact-owned resumes same delete operation", func(t *testing.T) {
		previous := memcachePersistedInstance{
			Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
			BackendID: memcacheBackendID("test", "us-central1", "cache"),
		}
		store, operationName := seedInterruptedMemcacheOperation(
			t, "delete", "DELETING", &previous, true,
		)
		backend := readyMemcacheBackend(1)
		api, err := NewMemcacheAPIWithStore(nil, backend, store)
		if err != nil {
			t.Fatal(err)
		}
		assertMemcacheTypedOperation(t, api, operationName,
			"type.googleapis.com/google.protobuf.Empty", true)
		missing := memcacheRequest(api, http.MethodGet,
			instancePath("test", "us-central1", "cache"), "")
		assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
		backend.mu.Lock()
		deleteCalls := backend.deleteCalls
		backend.mu.Unlock()
		if deleteCalls != 1 {
			t.Fatalf("delete calls=%d, want one exact-owned completion", deleteCalls)
		}
	})

	for _, test := range []struct {
		name   string
		result MemcacheBackendResult
		err    error
	}{
		{"inspection error remains deleting", MemcacheBackendResult{}, errors.New("inspect unavailable")},
		{"foreign remains deleting", MemcacheBackendResult{Exists: true}, errors.New("ownership conflict")},
		{"wrong version remains deleting", MemcacheBackendResult{Owned: true, Exists: true}, errors.New("version mismatch")},
	} {
		t.Run(test.name, func(t *testing.T) {
			previous := memcachePersistedInstance{
				Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
				BackendID: memcacheBackendID("test", "us-central1", "cache"),
			}
			store, operationName := seedInterruptedMemcacheOperation(
				t, "delete", "DELETING", &previous, true,
			)
			backend := &fakeMemcacheBackend{result: test.result, reconcileErr: test.err}
			first, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheState(t, first, "test", "us-central1", "cache", "DELETING")
			assertMemcacheOperationError(t, first, operationName)
			if _, associated := first.metadataSnapshot().Operations[operationName]; !associated {
				t.Fatal("uncertain delete lost operation provenance")
			}

			second, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheState(t, second, "test", "us-central1", "cache", "DELETING")
			if _, associated := second.metadataSnapshot().Operations[operationName]; !associated {
				t.Fatal("repeated restart lost delete operation provenance")
			}

			backend.mu.Lock()
			backend.result = MemcacheBackendResult{Exists: false}
			backend.reconcileErr = nil
			backend.mu.Unlock()
			resolved, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheTypedOperation(t, resolved, operationName,
				"type.googleapis.com/google.protobuf.Empty", true)
			missing := memcacheRequest(resolved, http.MethodGet,
				instancePath("test", "us-central1", "cache"), "")
			assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
			assertNoMemcacheMutationReplay(t, backend)
		})
	}
}

func TestMemcacheInterruptedUpdateNeverReportsFalseReady(t *testing.T) {
	for _, test := range []struct {
		name   string
		result MemcacheBackendResult
		err    error
	}{
		{"inspection error", MemcacheBackendResult{}, errors.New("inspect unavailable")},
		{"foreign backend", MemcacheBackendResult{Exists: true}, errors.New("ownership conflict")},
		{"wrong version", MemcacheBackendResult{Owned: true, Exists: true}, errors.New("version mismatch")},
	} {
		t.Run(test.name, func(t *testing.T) {
			previous := memcachePersistedInstance{
				Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
				BackendID: memcacheBackendID("test", "us-central1", "cache"),
			}
			store, operationName := seedInterruptedMemcacheOperation(
				t, "update", "UPDATING", &previous, true,
			)
			var metadata memcacheMetadata
			if err := store.Load(memcacheStateEntry, &metadata); err != nil {
				t.Fatal(err)
			}
			entry := metadata.Instances[resourceName("test", "us-central1", "cache")]
			entry.Instance.DisplayName = "desired"
			metadata.Instances[entry.Instance.Name] = entry
			if err := store.Save(memcacheStateEntry, metadata); err != nil {
				t.Fatal(err)
			}
			backend := &fakeMemcacheBackend{result: test.result, reconcileErr: test.err}
			api, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheState(t, api, "test", "us-central1", "cache", "UPDATING")
			assertMemcacheOperationError(t, api, operationName)
			if _, associated := api.metadataSnapshot().Operations[operationName]; !associated {
				t.Fatal("uncertain update lost operation provenance")
			}
			retryUpdate := memcacheRequest(api, http.MethodPatch,
				instancePath("test", "us-central1", "cache")+"?updateMask=displayName",
				`{"displayName":"retry"}`)
			assertRedisError(t, retryUpdate, http.StatusConflict, "ABORTED")
			retryDelete := memcacheRequest(api, http.MethodDelete,
				instancePath("test", "us-central1", "cache"), "")
			assertRedisError(t, retryDelete, http.StatusConflict, "ABORTED")

			repeated, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheState(t, repeated, "test", "us-central1", "cache", "UPDATING")
			if _, associated := repeated.metadataSnapshot().Operations[operationName]; !associated {
				t.Fatal("repeated uncertain restart lost update provenance")
			}

			backend.mu.Lock()
			backend.result = readyMemcacheResult(1)
			backend.reconcileErr = nil
			backend.mu.Unlock()
			resolved, err := NewMemcacheAPIWithStore(nil, backend, store)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheState(t, resolved, "test", "us-central1", "cache", "READY")
			assertMemcacheTypedOperation(t, resolved, operationName,
				"type.googleapis.com/google.cloud.memcache.v1.Instance", false)
		})
	}
}

func TestMemcacheGuardedStickyAdmissionFailsClosed(t *testing.T) {
	delegate := newGuardedMemcacheFailureStore()
	store := state.NewGuardedEntryStore(delegate, nil)
	backend := readyMemcacheBackend(1)
	api, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	response := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	if response.Code == http.StatusOK {
		t.Fatalf("sticky ambiguous admission was accepted: %s", response.Body.String())
	}
	backend.mu.Lock()
	provisionCalls := backend.provisionCalls
	backend.mu.Unlock()
	if provisionCalls != 0 {
		t.Fatalf("provision calls=%d, want 0", provisionCalls)
	}
	if store.Degraded() == nil || api.initializationError() == nil {
		t.Fatalf("store degraded=%v api degraded=%v", store.Degraded(), api.initializationError())
	}
	blocked := memcacheRequest(api, http.MethodGet,
		instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")
}

func TestMemcacheUpdateParametersCanonicalRouting(t *testing.T) {
	api := mustMemcacheAPI(t, readyMemcacheBackend(1), nil)
	before := api.metadataSnapshot()
	for _, test := range []struct {
		name   string
		method string
		path   string
		code   int
		status string
	}{
		{
			"canonical patch",
			http.MethodPatch,
			instancePath("test", "us-central1", "cache") + ":updateParameters",
			http.StatusNotImplemented,
			"UNIMPLEMENTED",
		},
		{
			"wrong post",
			http.MethodPost,
			instancePath("test", "us-central1", "cache") + ":updateParameters",
			http.StatusMethodNotAllowed,
			"METHOD_NOT_ALLOWED",
		},
		{
			"encoded alias",
			http.MethodPatch,
			instancePath("test", "us-central1", "cache") + "%3AupdateParameters",
			http.StatusNotFound,
			"NOT_FOUND",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := memcacheRequest(api, test.method, test.path, `{}`)
			assertRedisError(t, response, test.code, test.status)
			if !reflect.DeepEqual(before, api.metadataSnapshot()) || len(api.opMgr.List()) != 0 {
				t.Fatal("unsupported custom method mutated state")
			}
		})
	}
}

func TestMemcacheProductionServiceManagerContractActivates(t *testing.T) {
	if backend := memcacheBackendFromManager(&orchestrator.ServiceManager{}); backend == nil {
		t.Fatal("production ServiceManager does not activate the Memcached backend contract")
	}
}

func TestMemcacheTypedNilBackendFailsMutationsUnavailable(t *testing.T) {
	if backend := memcacheBackendFromManager(nil); backend != nil {
		t.Fatalf("typed nil manager activated backend %#v", backend)
	}
	api := mustMemcacheAPI(t, nil, nil)
	create := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	assertRedisError(t, create, http.StatusServiceUnavailable, "UNAVAILABLE")

	seedMemcache(t, api, "test", "us-central1", "cache")
	update := memcacheRequest(api, http.MethodPatch,
		instancePath("test", "us-central1", "cache")+"?updateMask=displayName",
		`{"displayName":"updated"}`)
	if update.Code != http.StatusOK {
		t.Fatalf("metadata update=%d %s", update.Code, update.Body.String())
	}
	waitForMemcacheOperation(t, api, operationNameFromResponse(t, update))
	deleted := memcacheRequest(api, http.MethodDelete,
		instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, deleted, http.StatusServiceUnavailable, "UNAVAILABLE")
	if len(api.opMgr.List()) != 1 {
		t.Fatalf("nil backend operations=%+v, want metadata-only update", api.opMgr.List())
	}
}

func TestMemcacheRegistryStartupWithNilServiceManager(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "typed-nil")
	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	handler := handlers["memcache.googleapis.com"]
	if handler == nil {
		t.Fatal("Memcached registry factory did not boot")
	}
	response := memcacheRequest(handler, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	assertRedisError(t, response, http.StatusServiceUnavailable, "UNAVAILABLE")
}

func TestMemcacheProductionStoreIsGuarded(t *testing.T) {
	raw, err := state.New(t.TempDir(), "guarded-production")
	if err != nil {
		t.Fatal(err)
	}
	store := newMemcacheProductionStore(raw, nil)
	if _, ok := store.(*state.GuardedEntryStore); !ok {
		t.Fatalf("production store type=%T, want *state.GuardedEntryStore", store)
	}
}

func TestMemcacheRealStoreAdmissionFailureRecovery(t *testing.T) {
	for _, test := range []struct {
		name string
		mode realMemcacheFaultMode
	}{
		{"definitive pre-write failure", realMemcacheFailBeforeWrite},
		{"ambiguous post-write failure", realMemcacheFailAfterWrite},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := state.New(t.TempDir(), "real-fault")
			if err != nil {
				t.Fatal(err)
			}
			faults := &realFaultingMemcacheStore{delegate: raw, mode: test.mode}
			guarded := state.NewGuardedEntryStore(faults, nil)
			backend := readyMemcacheBackend(1)
			api, err := NewMemcacheAPIWithStore(nil, backend, guarded)
			if err != nil {
				t.Fatal(err)
			}
			response := memcacheRequest(api, http.MethodPost,
				collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
			if response.Code == http.StatusOK {
				t.Fatalf("unguaranteed admission accepted: %s", response.Body.String())
			}
			backend.mu.Lock()
			provisionCalls := backend.provisionCalls
			backend.mu.Unlock()
			if provisionCalls != 0 {
				t.Fatalf("provision calls=%d, want 0", provisionCalls)
			}
			if api.opMgr.PersistenceError() == nil {
				t.Fatal("metadata degradation was not propagated to operation persistence")
			}
			operations := api.opMgr.List()
			if len(operations) != 1 {
				t.Fatalf("operations=%+v, want one durable admission record", operations)
			}
			operationName := operations[0].Name

			faults.mu.Lock()
			faults.mode = realMemcacheFaultNone
			faults.mu.Unlock()
			restarted, err := NewMemcacheAPIWithStore(
				nil,
				&fakeMemcacheBackend{result: MemcacheBackendResult{Exists: false}},
				state.NewGuardedEntryStore(faults, nil),
			)
			if err != nil {
				t.Fatal(err)
			}
			assertMemcacheOperationError(t, restarted, operationName)
			missing := memcacheRequest(restarted, http.MethodGet,
				instancePath("test", "us-central1", "cache"), "")
			assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")

			retryBackend := readyMemcacheBackend(1)
			restarted.backend = retryBackend
			createAndWaitMemcache(t, restarted, "test", "us-central1", "cache", memcacheCreatePayload)
		})
	}
}

func TestMemcacheAuthoritativeUpdateAbsenceResolvesForRecreate(t *testing.T) {
	previous := memcachePersistedInstance{
		Instance:  readyMemcacheInstance("test", "us-central1", "cache", "READY"),
		BackendID: memcacheBackendID("test", "us-central1", "cache"),
	}
	store, operationName := seedInterruptedMemcacheOperation(
		t, "update", "UPDATING", &previous, true,
	)
	backend := &fakeMemcacheBackend{result: MemcacheBackendResult{Exists: false}}
	api, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	assertMemcacheOperationError(t, api, operationName)
	missing := memcacheRequest(api, http.MethodGet,
		instancePath("test", "us-central1", "cache"), "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
	if backend.provisionCalls != 0 {
		t.Fatalf("reconciliation recreated backend %d times", backend.provisionCalls)
	}
	backend.mu.Lock()
	backend.result = readyMemcacheResult(1)
	backend.mu.Unlock()
	createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)
}

func TestMemcacheCustomRoutesRejectInvalidResourceIDs(t *testing.T) {
	api := mustMemcacheAPI(t, readyMemcacheBackend(1), nil)
	before := api.metadataSnapshot()
	for _, path := range []string{
		collectionPath("test", "us-central1") + "/:updateParameters",
		collectionPath("test", "us-central1") + "/-bad:updateParameters",
		collectionPath("test", "us-central1") + "/bad%2Falias:updateParameters",
		collectionPath("test", "us-central1") + "/bad%00:updateParameters",
		collectionPath("test", "us-central1") + "/bad%5Calias:updateParameters",
	} {
		response := memcacheRequest(api, http.MethodPatch, path, `{}`)
		assertRedisError(t, response, http.StatusNotFound, "NOT_FOUND")
	}
	valid := memcacheRequest(api, http.MethodPatch,
		instancePath("test", "us-central1", "cache")+":updateParameters", `{}`)
	assertRedisError(t, valid, http.StatusNotImplemented, "UNIMPLEMENTED")
	if !reflect.DeepEqual(before, api.metadataSnapshot()) || len(api.opMgr.List()) != 0 {
		t.Fatal("custom route validation mutated state")
	}
}

func TestMemcacheAnyResponseTypes(t *testing.T) {
	api := mustMemcacheAPI(t, readyMemcacheBackend(1), nil)
	createName := createAndWaitMemcache(t, api, "test", "us-central1", "cache", memcacheCreatePayload)
	assertMemcacheTypedOperation(t, api, createName,
		"type.googleapis.com/google.cloud.memcache.v1.Instance", false)

	update := memcacheRequest(api, http.MethodPatch,
		instancePath("test", "us-central1", "cache")+"?updateMask=displayName", `{"displayName":"updated"}`)
	updateName := operationNameFromResponse(t, update)
	waitForMemcacheOperation(t, api, updateName)
	assertMemcacheTypedOperation(t, api, updateName,
		"type.googleapis.com/google.cloud.memcache.v1.Instance", false)

	deleted := memcacheRequest(api, http.MethodDelete, instancePath("test", "us-central1", "cache"), "")
	deleteName := operationNameFromResponse(t, deleted)
	waitForMemcacheOperation(t, api, deleteName)
	assertMemcacheTypedOperation(t, api, deleteName,
		"type.googleapis.com/google.protobuf.Empty", true)
}

func TestMemcacheStickySaveAmbiguityTerminatesWithError(t *testing.T) {
	store := newEntryMapStore()
	store.memcacheAmbiguousThenSticky = true
	backend := readyMemcacheBackend(1)
	api, err := NewMemcacheAPIWithStore(nil, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	create := memcacheRequest(api, http.MethodPost,
		collectionPath("test", "us-central1")+"?instanceId=cache", memcacheCreatePayload)
	if create.Code != http.StatusOK {
		t.Fatalf("create=%d %s", create.Code, create.Body.String())
	}
	operationName := operationNameFromResponse(t, create)
	waitForMemcacheOperationError(t, api, operationName)
	polled := memcacheRequest(api, http.MethodGet, "/v1/"+operationName, "")
	if strings.Contains(polled.Body.String(), `"response"`) {
		t.Fatalf("sticky persistence failure reported success: %s", polled.Body.String())
	}
}

func TestMemcacheExactRoutesAndUnsupportedOperations(t *testing.T) {
	api := mustMemcacheAPI(t, readyMemcacheBackend(1), nil)
	for _, test := range []struct {
		name   string
		method string
		path   string
		code   int
		status string
	}{
		{"missing v1", http.MethodGet, "/projects/test/locations/us-central1/instances", 404, "NOT_FOUND"},
		{"wrong version", http.MethodGet, "/v1beta2/projects/test/locations/us-central1/instances", 404, "NOT_FOUND"},
		{"encoded alias", http.MethodGet, "/v1/projects/test/locations/us-central1/instances%2Fcache", 404, "NOT_FOUND"},
		{"operations list", http.MethodGet, "/v1/projects/test/locations/us-central1/operations", 501, "UNIMPLEMENTED"},
		{"operations list wrong verb", http.MethodPost, "/v1/projects/test/locations/us-central1/operations", 405, "METHOD_NOT_ALLOWED"},
		{"operations delete", http.MethodDelete, "/v1/projects/test/locations/us-central1/operations/op", 501, "UNIMPLEMENTED"},
		{"operations cancel", http.MethodPost, "/v1/projects/test/locations/us-central1/operations/op:cancel", 501, "UNIMPLEMENTED"},
		{"operations cancel wrong verb", http.MethodGet, "/v1/projects/test/locations/us-central1/operations/op:cancel", 405, "METHOD_NOT_ALLOWED"},
		{"unknown operation action", http.MethodPost, "/v1/projects/test/locations/us-central1/operations/op:unknown", 404, "NOT_FOUND"},
		{"unknown route", http.MethodGet, "/v1/projects/test/locations/us-central1/widgets", 404, "NOT_FOUND"},
		{"known resource wrong verb", http.MethodPut, instancePath("test", "us-central1", "cache"), 405, "METHOD_NOT_ALLOWED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := memcacheRequest(api, test.method, test.path, "")
			assertRedisError(t, response, test.code, test.status)
		})
	}
}

func seedInterruptedMemcacheOperation(
	t *testing.T,
	action string,
	stateValue string,
	previous *memcachePersistedInstance,
	includeResource bool,
) (*state.Store, string) {
	t.Helper()
	store, err := state.New(t.TempDir(), "crash")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	name := resourceName("test", "us-central1", "cache")
	op, err := manager.RegisterScopedDurable(orchestrator.OperationScope{
		ServiceKind: memcacheOperationKind,
		Project:     "test",
		Location:    "us-central1",
		Target:      name,
	}, action)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AdvanceDurable(op.Name, 5, orchestrator.StatusRunning); err != nil {
		t.Fatal(err)
	}
	metadata := memcacheMetadata{
		Instances: make(map[string]memcachePersistedInstance),
		Operations: map[string]memcacheOperationRecord{
			op.Name: {
				Action:       action,
				ResourceName: name,
				Previous:     previous,
			},
		},
	}
	if includeResource {
		instance := readyMemcacheInstance("test", "us-central1", "cache", stateValue)
		if stateValue != "READY" {
			instance.MemcacheNodes = nil
			instance.DiscoveryEndpoint = ""
		}
		metadata.Instances[name] = memcachePersistedInstance{
			Instance:  instance,
			BackendID: memcacheBackendID("test", "us-central1", "cache"),
		}
	}
	if err := store.Save(memcacheStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	return store, op.Name
}

func assertMemcacheTypedOperation(t *testing.T, api *MemcacheAPI, name, typeURL string, empty bool) {
	t.Helper()
	response := memcacheRequest(api, http.MethodGet, "/v1/"+name, "")
	if response.Code != http.StatusOK {
		t.Fatalf("operation=%d %s", response.Code, response.Body.String())
	}
	var operation struct {
		Done     bool           `json:"done"`
		Error    any            `json:"error"`
		Response map[string]any `json:"response"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if !operation.Done || operation.Error != nil || operation.Response["@type"] != typeURL {
		t.Fatalf("operation=%s", response.Body.String())
	}
	if empty && len(operation.Response) != 1 {
		t.Fatalf("empty response=%v", operation.Response)
	}
}

func assertMemcacheOperationError(t *testing.T, api *MemcacheAPI, name string) {
	t.Helper()
	response := memcacheRequest(api, http.MethodGet, "/v1/"+name, "")
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"done":true`) ||
		!strings.Contains(response.Body.String(), `"error"`) {
		t.Fatalf("operation error=%d %s", response.Code, response.Body.String())
	}
}

func assertMemcacheTerminalResultEqual(t *testing.T, before, after *orchestrator.Operation) {
	t.Helper()
	if before == nil || after == nil {
		t.Fatalf("terminal operation before=%+v after=%+v", before, after)
	}
	var beforeResponse any
	var afterResponse any
	if len(before.Response) != 0 {
		if err := json.Unmarshal(before.Response, &beforeResponse); err != nil {
			t.Fatal(err)
		}
	}
	if len(after.Response) != 0 {
		if err := json.Unmarshal(after.Response, &afterResponse); err != nil {
			t.Fatal(err)
		}
	}
	beforeWithoutResponse := *before
	beforeWithoutResponse.Response = nil
	afterWithoutResponse := *after
	afterWithoutResponse.Response = nil
	if !reflect.DeepEqual(beforeWithoutResponse, afterWithoutResponse) ||
		!reflect.DeepEqual(beforeResponse, afterResponse) {
		t.Fatalf("terminal result changed: before=%+v after=%+v", before, after)
	}
}

func assertNoMemcacheMutationReplay(t *testing.T, backend *fakeMemcacheBackend) {
	t.Helper()
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.provisionCalls != 0 || backend.updateCalls != 0 || backend.deleteCalls != 0 {
		t.Fatalf("replayed mutation: provision=%d update=%d delete=%d",
			backend.provisionCalls, backend.updateCalls, backend.deleteCalls)
	}
}

func cloneMemcacheMetadata(input memcacheMetadata) memcacheMetadata {
	raw, _ := json.Marshal(input)
	var cloned memcacheMetadata
	_ = json.Unmarshal(raw, &cloned)
	return cloned
}

type entryMapStore struct {
	mu                          sync.Mutex
	entries                     map[string][]byte
	memcacheSaves               int
	memcacheAmbiguousThenSticky bool
}

func newEntryMapStore() *entryMapStore {
	return &entryMapStore{entries: make(map[string][]byte)}
}

func (store *entryMapStore) Load(key string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	raw := store.entries[key]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (store *entryMapStore) Save(key string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if key == memcacheStateEntry && store.memcacheAmbiguousThenSticky {
		store.memcacheSaves++
		if store.memcacheSaves == 1 {
			store.entries[key] = raw
			return errors.New("ambiguous admission save")
		}
		return errors.New("sticky Memcached save failure")
	}
	store.entries[key] = raw
	return nil
}

func (store *entryMapStore) raw(key string) []byte {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]byte(nil), store.entries[key]...)
}

var _ stateStore = (*entryMapStore)(nil)

type guardedMemcacheFailureStore struct {
	mu       sync.Mutex
	entries  map[string][]byte
	degraded bool
}

func newGuardedMemcacheFailureStore() *guardedMemcacheFailureStore {
	return &guardedMemcacheFailureStore{entries: make(map[string][]byte)}
}

func (store *guardedMemcacheFailureStore) Load(key string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.degraded {
		return errors.New("sticky storage failure")
	}
	raw := store.entries[key]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (store *guardedMemcacheFailureStore) Save(key string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.degraded {
		return errors.New("sticky storage failure")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	store.entries[key] = raw
	if key == memcacheStateEntry {
		store.degraded = true
		return errors.New("ambiguous metadata save")
	}
	return nil
}

var _ stateStore = (*guardedMemcacheFailureStore)(nil)

type realMemcacheFaultMode int

const (
	realMemcacheFaultNone realMemcacheFaultMode = iota
	realMemcacheFailBeforeWrite
	realMemcacheFailAfterWrite
)

type realFaultingMemcacheStore struct {
	mu       sync.Mutex
	delegate *state.Store
	mode     realMemcacheFaultMode
}

func (store *realFaultingMemcacheStore) Load(key string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.delegate.Load(key, target)
}

func (store *realFaultingMemcacheStore) Save(key string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if key != memcacheStateEntry || store.mode == realMemcacheFaultNone {
		return store.delegate.Save(key, value)
	}
	if store.mode == realMemcacheFailBeforeWrite {
		return errors.New("definitive metadata pre-write failure")
	}
	if err := store.delegate.Save(key, value); err != nil {
		return err
	}
	return errors.New("ambiguous metadata post-write failure")
}

var _ stateStore = (*realFaultingMemcacheStore)(nil)
