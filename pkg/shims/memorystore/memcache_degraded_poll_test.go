package memorystore

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestMemcacheBackendAbsentRestartKeepsInterruptedOperationPollable(t *testing.T) {
	store, operationName := seedInterruptedMemcacheOperation(
		t, "create", "CREATING", nil, true,
	)
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newMemcacheAPIWithPersistentStore(manager, nil, store, true); err == nil {
		t.Fatal("backend-absent restart unexpectedly reconciled")
	} else {
		api := newMemcacheAPI(manager, nil, nil)
		api.setInitializationError(err)

		polled := memcacheRequest(api, http.MethodGet, "/v1/"+operationName, "")
		if polled.Code != http.StatusOK ||
			!strings.Contains(polled.Body.String(), `"done":true`) ||
			!strings.Contains(polled.Body.String(), `"operation interrupted by MiniSky restart`) {
			t.Fatalf("interrupted operation poll=%d %s", polled.Code, polled.Body.String())
		}
		blocked := memcacheRequest(api, http.MethodGet,
			instancePath("test", "us-central1", "cache"), "")
		assertRedisError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")
	}
}

func TestMemcacheUnreadableOperationStateFailsUnknownPollClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		store stateStore
	}{
		{
			name: "corrupt",
			store: func() stateStore {
				store := newEntryMapStore()
				if err := store.Save("orchestrator/operations", "corrupt"); err != nil {
					t.Fatal(err)
				}
				return store
			}(),
		},
		{name: "unreadable", store: unreadableMemcacheOperationStore{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, loadErr := orchestrator.NewOperationManagerWithStore(test.store)
			if loadErr == nil {
				t.Fatal("invalid operation state unexpectedly loaded")
			}
			api := newMemcacheAPI(orchestrator.NewOperationManager(), readyMemcacheBackend(1), nil)
			api.setInitializationError(loadErr)

			unknown := memcacheRequest(api, http.MethodGet,
				"/v1/projects/test/locations/us-central1/operations/unknown", "")
			assertRedisError(t, unknown, http.StatusServiceUnavailable, "UNAVAILABLE")
		})
	}
}

func TestMemcacheHealthyOperationPollingPreservesExactRouting(t *testing.T) {
	manager := orchestrator.NewOperationManager()
	operation, err := manager.RegisterScopedDurable(orchestrator.OperationScope{
		ServiceKind: memcacheOperationKind,
		Project:     "test",
		Location:    "us-central1",
		Target:      resourceName("test", "us-central1", "cache"),
	}, "create")
	if err != nil {
		t.Fatal(err)
	}
	response := json.RawMessage(
		`{"@type":"type.googleapis.com/google.cloud.memcache.v1.Instance","name":"projects/test/locations/us-central1/instances/cache"}`,
	)
	if err := manager.FinalizeScopedDurable(operation.Name, response, 0, ""); err != nil {
		t.Fatal(err)
	}
	api := newMemcacheAPI(manager, readyMemcacheBackend(1), nil)

	polled := memcacheRequest(api, http.MethodGet, "/v1/"+operation.Name, "")
	if polled.Code != http.StatusOK ||
		!strings.Contains(polled.Body.String(), `"done":true`) ||
		!strings.Contains(polled.Body.String(), `"response"`) {
		t.Fatalf("healthy operation poll=%d %s", polled.Code, polled.Body.String())
	}
	operationID := operation.Name[strings.LastIndex(operation.Name, "/")+1:]
	for _, path := range []string{
		"/v1/projects/other/locations/us-central1/operations/" + operationID,
		"/v1/projects/test/locations/europe-west1/operations/" + operationID,
		"/v1/projects/test/locations/us-central1/operations/unknown",
	} {
		notFound := memcacheRequest(api, http.MethodGet, path, "")
		assertRedisError(t, notFound, http.StatusNotFound, "NOT_FOUND")
	}
	cancel := memcacheRequest(api, http.MethodPost, "/v1/"+operation.Name+":cancel", "")
	assertRedisError(t, cancel, http.StatusNotImplemented, "UNIMPLEMENTED")
	encoded := memcacheRequest(api, http.MethodGet,
		"/v1/projects/test/locations/us-central1/operations%2F"+operationID, "")
	assertRedisError(t, encoded, http.StatusNotFound, "NOT_FOUND")
}

type unreadableMemcacheOperationStore struct{}

func (unreadableMemcacheOperationStore) Load(key string, _ any) error {
	if key == "orchestrator/operations" {
		return errors.New("operation state is unreadable")
	}
	return state.ErrNotFound
}

func (unreadableMemcacheOperationStore) Save(string, any) error {
	return nil
}

var _ stateStore = unreadableMemcacheOperationStore{}
