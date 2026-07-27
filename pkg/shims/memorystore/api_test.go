package memorystore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestRedisLifecycleValidationPersistenceAndOwnedReconciliation(t *testing.T) {
	store, err := state.New(t.TempDir(), "redis")
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}

	invalid := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache", `{"tier":"BASIC"}`)
	assertRedisError(t, invalid, http.StatusBadRequest, "INVALID_ARGUMENT")

	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1,"redisVersion":"REDIS_7_2"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	operationName := operationNameFromResponse(t, create)
	waitForRedisOperation(t, api, operationName)
	get := redisRequest(api, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"host":"127.0.0.1"`) ||
		!strings.Contains(get.Body.String(), `"port":46379`) {
		t.Fatalf("get status = %d, body = %s", get.Code, get.Body.String())
	}
	duplicate := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	assertRedisError(t, duplicate, http.StatusConflict, "ALREADY_EXISTS")

	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if backend.provisionCalls != 1 || backend.reconcileCalls != 1 {
		t.Fatalf("provision calls = %d, reconcile calls = %d", backend.provisionCalls, backend.reconcileCalls)
	}
	get = redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"state":"READY"`) {
		t.Fatalf("restarted get status = %d, body = %s", get.Code, get.Body.String())
	}

	deleted := redisRequest(restarted, http.MethodDelete,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleted.Code, deleted.Body.String())
	}
	waitForRedisOperation(t, restarted, operationNameFromResponse(t, deleted))
	missing := redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
	if backend.deleteCalls != 1 {
		t.Fatalf("delete calls = %d", backend.deleteCalls)
	}
}

func TestValkeyCreateIsCanonicalUnsupportedBeforeMutation(t *testing.T) {
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		bytes.NewBufferString(`{"engineVersion":"VALKEY_8_1","nodeType":"SHARED_CORE_NANO"}`))
	request.Host = "memorystore.googleapis.com"
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	assertRedisError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
	if backend.provisionCalls != 0 {
		t.Fatalf("provision calls = %d, want 0", backend.provisionCalls)
	}
	api.mu.RLock()
	count := len(api.instances)
	api.mu.RUnlock()
	if count != 0 {
		t.Fatalf("instances = %d, want 0", count)
	}
}

func TestRedisReconciliationRejectsUnownedBackend(t *testing.T) {
	store, err := state.New(t.TempDir(), "redis")
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	waitForRedisOperation(t, api, operationNameFromResponse(t, create))

	backend.owned = false
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	get := redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"state":"REPAIRING"`) ||
		strings.Contains(get.Body.String(), `"port":46379`) {
		t.Fatalf("unowned reconciliation response = %s", get.Body.String())
	}
}

func TestRedisBackendAndPersistenceFailuresAreTerminal(t *testing.T) {
	backend := &fakeRedisBackend{err: errors.New("docker unavailable")}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	op := waitForRedisOperation(t, api, operationNameFromResponse(t, create))
	if op.Error == nil || !strings.Contains(op.Error.Message, "docker unavailable") {
		t.Fatalf("operation = %+v", op)
	}
}

func TestRedisOperationMappingSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "redis-operations")
	if err != nil {
		t.Fatal(err)
	}
	manager := orchestrator.NewOperationManager()
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	operationName := operationNameFromResponse(t, create)
	waitForRedisOperation(t, api, operationName)

	restarted, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	polled := redisRequest(restarted, http.MethodGet, "/v1/"+operationName, "")
	if polled.Code != http.StatusOK || !strings.Contains(polled.Body.String(), `"done":true`) {
		t.Fatalf("poll after restart = %d %s", polled.Code, polled.Body.String())
	}
}

func TestRedisCreateAtomicPersistenceFailureLeavesNoCreatingOrphan(t *testing.T) {
	store := &failCombinedRedisStore{}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}

	failed := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	assertRedisError(t, failed, http.StatusInternalServerError, "INTERNAL")
	if backend.provisionCalls != 0 {
		t.Fatalf("provision calls = %d, want 0", backend.provisionCalls)
	}
	missing := redisRequest(api, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")

	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	missing = redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
}

func TestRedisDeletePersistsDeletingAndOperationAtomicallyAndRollsBack(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/cache"
	store := &failCombinedRedisStore{}
	if err := store.Save(memorystoreStateEntry, redisMetadata{
		Instances: map[string]persistedInstance{
			name: {
				Instance: &Instance{
					Name:         name,
					Tier:         "BASIC",
					MemorySizeGb: 1,
					State:        "READY",
					LocationId:   "us-central1",
				},
				BackendID: "redis-backend",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}

	failed := redisRequest(api, http.MethodDelete, "/v1/"+name, "")
	assertRedisError(t, failed, http.StatusInternalServerError, "INTERNAL")
	if backend.deleteCalls != 0 {
		t.Fatalf("delete calls = %d, want 0", backend.deleteCalls)
	}
	api.mu.RLock()
	instance := cloneInstance(api.instances[name])
	operationCount := len(api.operations)
	api.mu.RUnlock()
	if instance == nil || instance.State != "READY" {
		t.Fatalf("instance after failed save = %+v, want previous READY state", instance)
	}
	if operationCount != 0 {
		t.Fatalf("operation mappings after failed save = %d, want 0", operationCount)
	}
	var persisted redisMetadata
	if err := store.Load(memorystoreStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if saved := persisted.Instances[name].Instance; saved == nil || saved.State != "READY" {
		t.Fatalf("persisted instance after failed save = %+v, want previous READY snapshot", saved)
	}
	if len(persisted.Operations) != 0 {
		t.Fatalf("persisted operation mappings after failed save = %d, want 0", len(persisted.Operations))
	}

	store.resetFailure()
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	get := redisRequest(restarted, http.MethodGet, "/v1/"+name, "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"state":"READY"`) {
		t.Fatalf("restart after failed deletion = %d %s", get.Code, get.Body.String())
	}
}

func TestBackendIDUsesUnambiguousCanonicalComponents(t *testing.T) {
	left := backendID("a-b", "c", "d")
	right := backendID("a", "b-c", "d")
	if left == right {
		t.Fatalf("ambiguous names produced the same backend id %q", left)
	}
	if left != backendID("a-b", "c", "d") {
		t.Fatal("backend id is not deterministic")
	}
}

func TestCorruptStateDisablesMemorystoreRuntime(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-redis")
	store, err := state.New(root, "corrupt-redis")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(memorystoreStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(orchestrator.NewOperationManager(), nil, nil)
	response := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=blocked",
		`{"tier":"BASIC","memorySizeGb":1}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(memorystoreStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

type fakeRedisBackend struct {
	mu             sync.Mutex
	endpoint       string
	owned          bool
	err            error
	provisionCalls int
	reconcileCalls int
	deleteCalls    int
}

type failCombinedRedisStore struct {
	mu      sync.Mutex
	data    []byte
	failing bool
}

func (s *failCombinedRedisStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *failCombinedRedisStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failing {
		return errors.New("injected save failure")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var metadata redisMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	if len(metadata.Operations) > 0 {
		s.failing = true
		return errors.New("injected save failure")
	}
	s.data = data
	return nil
}

func (s *failCombinedRedisStore) resetFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failing = false
}

func (b *fakeRedisBackend) ProvisionRedis(context.Context, string, string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.provisionCalls++
	if b.err != nil {
		return "", b.err
	}
	return b.endpoint, nil
}

func (b *fakeRedisBackend) ReconcileRedis(context.Context, string) (string, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reconcileCalls++
	return b.endpoint, b.owned, b.err
}

func (b *fakeRedisBackend) DeleteRedis(context.Context, string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleteCalls++
	return b.err
}

func redisRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Host = "redis.googleapis.com"
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func operationNameFromResponse(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var operation struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatalf("decode operation: %v; body = %s", err, response.Body.String())
	}
	if operation.Name == "" {
		t.Fatalf("missing operation name: %s", response.Body.String())
	}
	return operation.Name
}

func waitForRedisOperation(t *testing.T, api *API, name string) *orchestrator.Operation {
	t.Helper()
	path := "/v1/" + name
	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		response := redisRequest(api, http.MethodGet, path, "")
		if response.Code != http.StatusOK {
			t.Fatalf("poll status = %d, body = %s", response.Code, response.Body.String())
		}
		var envelope struct {
			Done  bool                         `json:"done"`
			Error *orchestrator.OperationError `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Done {
			return &orchestrator.Operation{Done: envelope.Done, Error: envelope.Error}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("operation did not finish")
	return nil
}

func assertRedisError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Status != status {
		t.Fatalf("status = %q, want %q", envelope.Error.Status, status)
	}
}
