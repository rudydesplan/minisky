package memorystore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
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
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(manager, backend, nil, store)
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

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithStore(restartedManager, backend, nil, store)
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

func TestRedisAPIConstructionDoesNotRepeatHookRegistration(t *testing.T) {
	before := redisHookRegistrationAttempts.Load()
	if before != 1 {
		t.Fatalf("package hook registration attempts=%d, want one", before)
	}
	for index := 0; index < 3; index++ {
		if _, err := NewAPIWithStore(orchestrator.NewOperationManager(),
			&fakeRedisBackend{}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if after := redisHookRegistrationAttempts.Load(); after != before {
		t.Fatalf("API construction changed hook registration attempts from %d to %d",
			before, after)
	}
}

func TestRedisAcceptsTerraformProviderCreatePayload(t *testing.T) {
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	create := redisRequest(api, http.MethodPost,
		"/v1/projects/local-dev-project/locations/us-central1/instances?instanceId=minisky-terraform",
		`{"connectMode":"DIRECT_PEERING","labels":{"goog-terraform-provisioned":"true"},"memorySizeGb":1,"name":"projects/local-dev-project/locations/us-central1/instances/minisky-terraform","redisVersion":"REDIS_7_2","tier":"BASIC","transitEncryptionMode":"DISABLED"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	waitForRedisOperation(t, api, operationNameFromResponse(t, create))

	get := redisRequest(api, http.MethodGet,
		"/v1/projects/local-dev-project/locations/us-central1/instances/minisky-terraform", "")
	if get.Code != http.StatusOK ||
		!strings.Contains(get.Body.String(), `"connectMode":"DIRECT_PEERING"`) ||
		!strings.Contains(get.Body.String(), `"transitEncryptionMode":"DISABLED"`) {
		t.Fatalf("get status = %d, body = %s", get.Code, get.Body.String())
	}
}

func TestRedisRejectsUnsupportedTransportModesBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{
			name: "encrypted transit",
			body: `{"tier":"BASIC","memorySizeGb":1,"transitEncryptionMode":"SERVER_AUTHENTICATION"}`,
		},
		{
			name: "unknown transit mode",
			body: `{"tier":"BASIC","memorySizeGb":1,"transitEncryptionMode":"UNKNOWN_MODE"}`,
		},
		{
			name: "private service access",
			body: `{"tier":"BASIC","memorySizeGb":1,"connectMode":"PRIVATE_SERVICE_ACCESS"}`,
		},
		{
			name: "unknown connect mode",
			body: `{"tier":"BASIC","memorySizeGb":1,"connectMode":"UNKNOWN_MODE"}`,
		},
		{
			name: "cross-project authorized network",
			body: `{"tier":"BASIC","memorySizeGb":1,"authorizedNetwork":"projects/other/global/networks/default"}`,
		},
		{
			name: "unsupported persistence mode",
			body: `{"tier":"BASIC","memorySizeGb":1,"persistenceConfig":{"persistenceMode":"AOF"}}`,
		},
		{
			name: "snapshot period without RDB",
			body: `{"tier":"BASIC","memorySizeGb":1,"persistenceConfig":{"persistenceMode":"DISABLED","rdbSnapshotPeriod":"ONE_HOUR"}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
			api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil)
			if err != nil {
				t.Fatal(err)
			}

			response := redisRequest(api, http.MethodPost,
				"/v1/projects/test/locations/us-central1/instances?instanceId=cache", test.body)
			assertRedisError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
			if backend.provisionCalls != 0 {
				t.Fatalf("provision calls=%d, want 0", backend.provisionCalls)
			}
			api.mu.RLock()
			instanceCount := len(api.instances)
			operationCount := len(api.operations)
			api.mu.RUnlock()
			if instanceCount != 0 || operationCount != 0 {
				t.Fatalf("instances=%d operations=%d, want no mutation", instanceCount, operationCount)
			}
		})
	}
}

func TestRedisDomainUsesValkeyBackendImage(t *testing.T) {
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1,"redisVersion":"REDIS_7_2"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	waitForRedisOperation(t, api, operationNameFromResponse(t, create))
	backend.mu.Lock()
	image := backend.lastImage
	backend.mu.Unlock()
	if !strings.Contains(image, "valkey") {
		t.Fatalf("backend image = %q, want Valkey", image)
	}
}

func TestRedisBackendSpecIsImmutableAndRejectsUnsupportedVersions(t *testing.T) {
	spec, err := redisBackendSpec("REDIS_7_2", "redis-backend")
	if err != nil {
		t.Fatal(err)
	}
	if spec.ResourceID != "redis-backend" ||
		spec.Image != "valkey/valkey:7.2.12-alpine@sha256:28ca383369c5497fb4d63092e852a1c9e23c5a0b5553bb8f0f54a0b7fa0ddd4b" ||
		spec.ImageID != "" ||
		spec.Platform != "linux/amd64" {
		t.Fatalf("Redis backend spec = %#v", spec)
	}
	if _, err := redisBackendSpec("REDIS_7_4", "redis-backend"); err == nil {
		t.Fatal("unsupported Redis version resolved through a mutable fallback")
	}
}

func TestRedisPersistsResolvedBackendIdentity(t *testing.T) {
	store, err := state.New(t.TempDir(), "redis-identity")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1,"redisVersion":"REDIS_7_2"}`)
	waitForRedisOperation(t, api, operationNameFromResponse(t, create))

	var metadata redisMetadata
	if err := store.Load(memorystoreStateEntry, &metadata); err != nil {
		t.Fatal(err)
	}
	persisted := metadata.Instances["projects/test/locations/us-central1/instances/cache"]
	if persisted.Backend.ImageID == "" || persisted.Backend.RepoDigest == "" ||
		persisted.Backend.VolumeIdentity == "" || persisted.Backend.VolumeProvenance == "" ||
		persisted.Backend.ContainerID == "" ||
		persisted.Backend.ContainerIdentity == "" || persisted.Backend.Generation != 1 {
		t.Fatalf("persisted Redis backend identity = %#v", persisted.Backend)
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
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	waitForRedisOperation(t, api, operationNameFromResponse(t, create))

	backend.owned = false
	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithStore(restartedManager, backend, nil, store)
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

func TestRedisPostProvisionSaveFailureCleansOwnedBackendAndMetadata(t *testing.T) {
	store := &failSecondRedisStore{}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	operationName := operationNameFromResponse(t, create)
	operation := waitForRedisOperation(t, api, operationName)
	if operation.Error == nil || !strings.Contains(operation.Error.Message, "injected post-provision save failure") {
		t.Fatalf("operation = %+v", operation)
	}
	if backend.discardCalls != 1 || backend.publishCalls != 0 {
		t.Fatalf("discard calls=%d publish calls=%d, want 1 and 0",
			backend.discardCalls, backend.publishCalls)
	}
	api.mu.RLock()
	instance := api.instances["projects/test/locations/us-central1/instances/cache"]
	api.mu.RUnlock()
	if instance != nil {
		t.Fatalf("failed create retained metadata: %+v", instance)
	}

	restarted, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	missing := redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/cache", "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
}

func TestRedisPublishesRuntimeOnlyAfterDurableSave(t *testing.T) {
	store := &blockingSecondRedisStore{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	operationName := operationNameFromResponse(t, create)
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("post-provision persistence did not block")
	}
	backend.mu.Lock()
	publishCalls := backend.publishCalls
	backend.mu.Unlock()
	if publishCalls != 0 {
		t.Fatalf("runtime published before durable save: %d calls", publishCalls)
	}
	close(store.release)
	operation := waitForRedisOperation(t, api, operationName)
	if operation.Error != nil {
		t.Fatalf("operation = %+v", operation)
	}
	backend.mu.Lock()
	publishCalls = backend.publishCalls
	backend.mu.Unlock()
	if publishCalls != 1 {
		t.Fatalf("runtime publish calls=%d, want 1 after save", publishCalls)
	}
}

func TestRedisPublishFailureCompensatesPersistedProvisional(t *testing.T) {
	store, err := state.New(t.TempDir(), "redis-publish-failure")
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{
		endpoint:   "127.0.0.1:46379",
		owned:      true,
		publishErr: errors.New("injected runtime publication failure"),
	}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=cache",
		`{"tier":"BASIC","memorySizeGb":1}`)
	operation := waitForRedisOperation(t, api, operationNameFromResponse(t, create))
	if operation.Error == nil || !strings.Contains(operation.Error.Message, "publication failure") {
		t.Fatalf("operation = %+v", operation)
	}
	if backend.publishCalls != 1 || backend.discardCalls != 1 {
		t.Fatalf("publish=%d discard=%d, want 1 and 1", backend.publishCalls, backend.discardCalls)
	}
	var metadata redisMetadata
	if err := store.Load(memorystoreStateEntry, &metadata); err != nil {
		t.Fatal(err)
	}
	if len(metadata.Instances) != 0 {
		t.Fatalf("publication failure retained durable metadata: %#v", metadata.Instances)
	}
}

func TestRedisRehydrationPreservesPriorSpecWhenReplacementSaveFailsAndRetries(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/cache"
	prior := persistedRedisBackendForTest(name)
	candidate := prior
	candidate.ContainerID = "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	candidate.ContainerIdentity = "sha256:3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	candidate.Generation++
	store := &toggleRedisStore{}
	if err := store.Save(memorystoreStateEntry, redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{name: {
			Instance: &Instance{
				Name: name, Tier: "BASIC", MemorySizeGb: 1, State: "READY",
				LocationId: "us-central1", Host: "127.0.0.1", Port: 46379, RedisVersion: "REDIS_7_2",
			},
			BackendID: prior.ResourceID,
			Backend:   prior,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	store.failing = true
	backend := &fakeRedisBackend{
		endpoint:        "127.0.0.1:46380",
		owned:           true,
		reconcileResult: &candidate,
	}
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store); err == nil {
		t.Fatal("failed replacement persistence was accepted")
	}
	if backend.discardCalls != 1 || backend.publishCalls != 0 {
		t.Fatalf("failed replacement discard=%d publish=%d, want 1 and 0",
			backend.discardCalls, backend.publishCalls)
	}
	var persisted redisMetadata
	if err := store.Load(memorystoreStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Instances[name].Backend; got != prior {
		t.Fatalf("failed replacement changed durable spec: %#v", got)
	}

	store.failing = false
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.backends[name] != candidate {
		t.Fatalf("retry backend=%#v, want %#v", restarted.backends[name], candidate)
	}
	if backend.publishCalls != 1 {
		t.Fatalf("retry publish calls=%d, want 1", backend.publishCalls)
	}
}

func TestRedisMultiInstanceRehydrationRetainsPublishedUnchangedRuntime(t *testing.T) {
	const (
		firstName  = "projects/test/locations/us-central1/instances/cache-a"
		secondName = "projects/test/locations/us-central1/instances/cache-b"
	)
	firstBackend := persistedRedisBackendForTest(firstName)
	secondBackend := persistedRedisBackendForTest(secondName)
	secondBackend.ContainerID = "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	store := &toggleRedisStore{}
	if err := store.Save(memorystoreStateEntry, redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{
			firstName: {
				Instance: &Instance{Name: firstName, Tier: "BASIC", MemorySizeGb: 1,
					State: "READY", LocationId: "us-central1", Host: "127.0.0.1", Port: 46379,
					RedisVersion: "REDIS_7_2"},
				BackendID: firstBackend.ResourceID,
				Backend:   firstBackend,
			},
			secondName: {
				Instance: &Instance{Name: secondName, Tier: "BASIC", MemorySizeGb: 1,
					State: "READY", LocationId: "us-central1", Host: "127.0.0.1", Port: 46379,
					RedisVersion: "REDIS_7_2"},
				BackendID: secondBackend.ResourceID,
				Backend:   secondBackend,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{
		endpoint:       "127.0.0.1:46379",
		owned:          true,
		publishFailAt:  2,
		publishBlockAt: 2,
		publishEntered: make(chan struct{}),
		publishRelease: make(chan struct{}),
	}
	result := make(chan error, 1)
	go func() {
		_, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
		result <- err
	}()
	select {
	case <-backend.publishEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("second publication did not block")
	}
	backend.mu.Lock()
	_, firstVisible := backend.published[firstBackend.ResourceID]
	_, secondVisible := backend.published[secondBackend.ResourceID]
	backend.mu.Unlock()
	if !firstVisible || secondVisible {
		t.Fatalf("concurrent publication visibility first=%t second=%t", firstVisible, secondVisible)
	}
	close(backend.publishRelease)
	if err := <-result; err == nil {
		t.Fatal("second publication failure was accepted")
	}
	backend.mu.Lock()
	firstVisible = backend.published[firstBackend.ResourceID].ContainerID == firstBackend.ContainerID
	secondVisible = backend.published[secondBackend.ResourceID].ContainerID != ""
	unpublishCalls := backend.unpublishCalls
	discardCalls := backend.discardCalls
	backend.mu.Unlock()
	if !firstVisible || secondVisible || unpublishCalls != 0 || discardCalls != 0 {
		t.Fatalf("rollback lost unchanged provenance first=%t second=%t unpublish=%d discard=%d",
			firstVisible, secondVisible, unpublishCalls, discardCalls)
	}
	var failedState redisMetadata
	if err := store.Load(memorystoreStateEntry, &failedState); err != nil {
		t.Fatal(err)
	}
	if got := failedState.Instances[firstName].Instance.State; got != "READY" {
		t.Fatalf("successfully published unchanged runtime state=%q, want READY", got)
	}
	if got := failedState.Instances[secondName].Instance.State; got != "REPAIRING" {
		t.Fatalf("failed publication state=%q, want REPAIRING", got)
	}

	backend.publishFailAt = 0
	backend.publishBlockAt = 0
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted == nil {
		t.Fatal("retry returned nil API")
	}
	backend.mu.Lock()
	publishedCount := len(backend.published)
	backend.mu.Unlock()
	if publishedCount != 2 {
		t.Fatalf("retry published runtimes=%d, want 2", publishedCount)
	}
}

func TestRedisRestartResumesExactOwnedDeletingBackend(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/cache"
	store := &failCombinedRedisStore{}
	if err := store.Save(memorystoreStateEntry, redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{name: {
			Instance: &Instance{
				Name: name, Tier: "BASIC", MemorySizeGb: 1, State: "DELETING",
				LocationId: "us-central1", Host: "127.0.0.1", Port: 46379, RedisVersion: "REDIS_7_2",
			},
			BackendID: backendID("test", "us-central1", "cache"),
			Backend:   persistedRedisBackendForTest(name),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if backend.deleteCalls != 1 {
		t.Fatalf("delete calls = %d, want one resumed exact-owned deletion", backend.deleteCalls)
	}
	missing := redisRequest(restarted, http.MethodGet, "/v1/"+name, "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")

	var persisted redisMetadata
	if err := store.Load(memorystoreStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Instances[name].Instance != nil {
		t.Fatalf("deleting metadata survived restart: %+v", persisted.Instances[name])
	}
}

func TestRedisRestartCleansOrphanedVolumeWhenContainerIsMissing(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/cache"
	store := &failCombinedRedisStore{}
	if err := store.Save(memorystoreStateEntry, redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{name: {
			Instance: &Instance{
				Name: name, Tier: "BASIC", MemorySizeGb: 1, State: "DELETING",
				LocationId: "us-central1", Host: "127.0.0.1", Port: 46379, RedisVersion: "REDIS_7_2",
			},
			BackendID: backendID("test", "us-central1", "cache"),
			Backend:   persistedRedisBackendForTest(name),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{owned: false}
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if backend.deleteCalls != 1 {
		t.Fatalf("delete calls = %d, want orphaned volume cleanup despite missing container", backend.deleteCalls)
	}
	missing := redisRequest(restarted, http.MethodGet, "/v1/"+name, "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
}

func TestRedisRestartPreservesDeletingWhenResumedCleanupFails(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/cache"
	store := &failCombinedRedisStore{}
	if err := store.Save(memorystoreStateEntry, redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{name: {
			Instance: &Instance{
				Name: name, Tier: "BASIC", MemorySizeGb: 1, State: "DELETING",
				LocationId: "us-central1", Host: "127.0.0.1", Port: 46379, RedisVersion: "REDIS_7_2",
			},
			BackendID: backendID("test", "us-central1", "cache"),
			Backend:   persistedRedisBackendForTest(name),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{
		owned:     false,
		deleteErr: errors.New("injected cleanup failure"),
	}
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err == nil {
		t.Fatal("restart accepted failed resumed deletion")
	}
	if restarted != nil {
		t.Fatalf("restart API = %#v, want nil on failed resumed deletion", restarted)
	}
	var persisted redisMetadata
	if err := store.Load(memorystoreStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Instances[name].Instance; got == nil || got.State != "DELETING" {
		t.Fatalf("failed resumed deletion changed durable state: %#v", got)
	}
}

func TestRedisOperationMappingSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "redis-operations")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
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

func TestRedisMutationTransactionHidesRejectedCreateFromConcurrentSaveAndRestart(t *testing.T) {
	store := &blockingFailFirstRedisStore{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	firstResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstResult <- redisRequest(api, http.MethodPost,
			"/v1/projects/test/locations/us-central1/instances?instanceId=rejected",
			`{"tier":"BASIC","memorySizeGb":1}`)
	}()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("rejected create did not reach blocked save")
	}
	secondResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		secondResult <- redisRequest(api, http.MethodPost,
			"/v1/projects/test/locations/us-central1/instances?instanceId=accepted",
			`{"tier":"BASIC","memorySizeGb":1}`)
	}()
	readResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		readResult <- redisRequest(api, http.MethodGet,
			"/v1/projects/test/locations/us-central1/instances/rejected", "")
	}()
	select {
	case <-secondResult:
		t.Fatal("concurrent create observed transient rejected create transaction")
	case <-readResult:
		t.Fatal("concurrent read observed transient rejected create transaction")
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	assertRedisError(t, <-firstResult, http.StatusInternalServerError, "INTERNAL")
	second := <-secondResult
	if second.Code != http.StatusOK {
		t.Fatalf("accepted create status=%d body=%s", second.Code, second.Body.String())
	}
	assertRedisError(t, <-readResult, http.StatusNotFound, "NOT_FOUND")
	waitForRedisOperation(t, api, operationNameFromResponse(t, second))

	restarted, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	assertRedisError(t, redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/rejected", ""),
		http.StatusNotFound, "NOT_FOUND")
	if got := redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/accepted", ""); got.Code != http.StatusOK {
		t.Fatalf("accepted create missing after restart: %d %s", got.Code, got.Body.String())
	}
}

func TestRedisMutationTransactionRollsBackRejectedDeleteBeforeConcurrentSave(t *testing.T) {
	store := &blockingFailNthRedisStore{
		failAt:  3,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=retained",
		`{"tier":"BASIC","memorySizeGb":1}`)
	waitForRedisOperation(t, api, operationNameFromResponse(t, create))

	deleteResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		deleteResult <- redisRequest(api, http.MethodDelete,
			"/v1/projects/test/locations/us-central1/instances/retained", "")
	}()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("rejected delete did not reach blocked save")
	}
	createResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		createResult <- redisRequest(api, http.MethodPost,
			"/v1/projects/test/locations/us-central1/instances?instanceId=concurrent",
			`{"tier":"BASIC","memorySizeGb":1}`)
	}()
	select {
	case <-createResult:
		t.Fatal("concurrent save observed transient DELETING state")
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	assertRedisError(t, <-deleteResult, http.StatusInternalServerError, "INTERNAL")
	concurrent := <-createResult
	if concurrent.Code != http.StatusOK {
		t.Fatalf("concurrent create=%d %s", concurrent.Code, concurrent.Body.String())
	}
	waitForRedisOperation(t, api, operationNameFromResponse(t, concurrent))

	restarted, err := NewAPIWithStore(manager, backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"retained", "concurrent"} {
		got := redisRequest(restarted, http.MethodGet,
			"/v1/projects/test/locations/us-central1/instances/"+id, "")
		if got.Code != http.StatusOK {
			t.Fatalf("instance %s missing after restart: %d %s", id, got.Code, got.Body.String())
		}
	}
}

func TestRedisPortableExportImportRedactsRuntimeProvenance(t *testing.T) {
	root := t.TempDir()
	source, err := state.New(root, "source")
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	sourceOperations, err := orchestrator.NewOperationManagerWithStore(source)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(sourceOperations, backend, nil, source)
	if err != nil {
		t.Fatal(err)
	}
	create := redisRequest(api, http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=portable",
		`{"tier":"BASIC","memorySizeGb":1}`)
	waitForRedisOperation(t, api, operationNameFromResponse(t, create))

	var exported bytes.Buffer
	if err := source.Export(&exported); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		`"host":"127.0.0.1"`, `"port":46379`, `"hostPort":"46379"`,
		`"containerId"`, `"containerIdentity"`, `"imageId"`,
		`"volumeIdentity"`, `"volumeProvenance"`, `/var/lib/docker/volumes/`,
		`appendonly.aof`,
	} {
		if bytes.Contains(exported.Bytes(), []byte(forbidden)) {
			t.Fatalf("portable Redis export contains profile-local value %q: %s",
				forbidden, exported.String())
		}
	}
	target, err := state.New(root, "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Import(bytes.NewReader(exported.Bytes())); err != nil {
		t.Fatal(err)
	}
	var imported redisMetadata
	if err := target.Load(memorystoreStateEntry, &imported); err != nil {
		t.Fatal(err)
	}
	persisted := imported.Instances["projects/test/locations/us-central1/instances/portable"]
	if persisted.Instance.State != "REPAIRING" || persisted.Instance.Host != "" ||
		persisted.Instance.Port != 0 || persisted.Backend.ContainerID != "" ||
		persisted.Backend.HostPort != "" || persisted.Backend.ImageID != "" {
		t.Fatalf("portable Redis import retained local runtime: %#v", persisted)
	}
	targetOperations, err := orchestrator.NewOperationManagerWithStore(target)
	if err != nil {
		t.Fatal(err)
	}
	targetBackend := &fakeRedisBackend{owned: false}
	restarted, err := NewAPIWithStore(targetOperations, targetBackend, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if targetBackend.reconcileCalls != 0 || targetBackend.publishCalls != 0 {
		t.Fatalf("portable import reconcile=%d publish=%d, want no backend calls for metadata-only state",
			targetBackend.reconcileCalls, targetBackend.publishCalls)
	}
	got := redisRequest(restarted, http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/portable", "")
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"state":"REPAIRING"`) {
		t.Fatalf("portable imported instance=%d %s", got.Code, got.Body.String())
	}
}

func TestRedisSnapshotRequiresExplicitOperationSiblingEvenWhenEmpty(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/metadata-only"
	root := t.TempDir()
	source, err := state.New(root, "explicit-source")
	if err != nil {
		t.Fatal(err)
	}
	resourceID := backendID("test", "us-central1", "metadata-only")
	metadata := redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{name: {
			Instance: &Instance{Name: name, Tier: "BASIC", MemorySizeGb: 1,
				State: "REPAIRING", LocationId: "us-central1", RedisVersion: "REDIS_7_2"},
			BackendID: resourceID, Backend: orchestrator.Redis72BackendSpec(resourceID),
		}},
	}
	if err := source.Save(memorystoreStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	var rejected bytes.Buffer
	if err := source.Export(&rejected); err == nil {
		t.Fatal("Redis export accepted an absent durable operation sibling")
	}
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(),
		&fakeRedisBackend{}, nil, source); err == nil {
		t.Fatal("Redis local load accepted an absent durable operation sibling")
	}
	if err := source.Save("orchestrator/operations",
		json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	var exported bytes.Buffer
	if err := source.Export(&exported); err != nil {
		t.Fatalf("explicit empty operation sibling rejected: %v", err)
	}
	target, err := state.New(root, "explicit-target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Import(bytes.NewReader(exported.Bytes())); err != nil {
		t.Fatalf("explicit empty sibling import: %v", err)
	}
	if err := target.Save("test/marker", map[string]string{"value": "preserved"}); err != nil {
		t.Fatal(err)
	}
	metadataPayload, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	missingSibling, err := json.Marshal(state.Snapshot{
		Format: state.SnapshotFormat, Version: state.Version,
		Entries: map[string]json.RawMessage{memorystoreStateEntry: metadataPayload},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Import(bytes.NewReader(missingSibling)); err == nil {
		t.Fatal("Redis import accepted an absent durable operation sibling")
	}
	var marker map[string]string
	if err := target.Load("test/marker", &marker); err != nil ||
		marker["value"] != "preserved" {
		t.Fatalf("failed Redis sibling import changed target: marker=%v err=%v", marker, err)
	}
	backend := &fakeRedisBackend{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if backend.reconcileCalls != 0 || backend.publishCalls != 0 {
		t.Fatalf("metadata-only explicit-empty load touched backend: %#v", backend)
	}
	if got := redisRequest(api, http.MethodGet, "/v1/"+name, ""); got.Code != http.StatusOK {
		t.Fatalf("metadata-only instance unavailable: %d %s", got.Code, got.Body.String())
	}
}

func TestRedisPendingOperationPortableRoundTripBecomesInterrupted(t *testing.T) {
	const (
		name          = "projects/test/locations/us-central1/instances/pending"
		deleteManager = "operation-1234567889-deletexx"
		managerName   = "operation-1234567890-pendingx"
	)
	operationName := "projects/test/locations/us-central1/operations/" + managerName
	deleteOperationName := "projects/test/locations/us-central1/operations/" + deleteManager
	root := t.TempDir()
	resourceID := backendID("test", "us-central1", "pending")
	metadata := redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{name: {
			Instance: &Instance{Name: name, Tier: "BASIC", MemorySizeGb: 1,
				State: "CREATING", LocationId: "us-central1", RedisVersion: "REDIS_7_2"},
			BackendID: resourceID, Backend: orchestrator.Redis72BackendSpec(resourceID),
		}},
		Operations: map[string]operationTarget{
			deleteOperationName: {
				ManagerName: deleteManager, ResourceKey: name, Delete: true,
			},
			operationName: {
				ManagerName: managerName, ResourceKey: name,
			},
		},
	}
	operations := map[string]*orchestrator.Operation{
		deleteManager: {
			ID: "122", Name: deleteManager, Kind: "redis#operation",
			OperationType: "DELETE", Status: orchestrator.StatusDone,
			Progress: 100, Done: true, TargetLink: name,
			InsertTime: "2026-07-01T09:59:00Z", StartTime: "2026-07-01T09:59:00Z",
			EndTime: "2026-07-01T09:59:01Z", Region: "us-central1",
			ServiceKind: "redis#operation", Project: "test", Location: "us-central1",
		},
		managerName: {
			ID: "123", Name: managerName, Kind: "redis#operation",
			OperationType: "CREATE", Status: orchestrator.StatusPending,
			TargetLink: name, InsertTime: "2026-07-01T10:00:00Z",
			Region: "us-central1", ServiceKind: "redis#operation",
			Project: "test", Location: "us-central1",
		},
	}
	metadataPayload, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	operationPayload, err := json.Marshal(operations)
	if err != nil {
		t.Fatal(err)
	}
	portablePayload, err := json.Marshal(state.Snapshot{
		Format: state.SnapshotFormat, Version: state.Version,
		Entries: map[string]json.RawMessage{
			memorystoreStateEntry:     metadataPayload,
			"orchestrator/operations": operationPayload,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := state.New(root, "pending-target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Import(bytes.NewReader(portablePayload)); err != nil {
		t.Fatal(err)
	}
	var importedMetadata redisMetadata
	if err := target.Load(memorystoreStateEntry, &importedMetadata); err != nil {
		t.Fatal(err)
	}
	if importedMetadata.Instances[name].Instance.State != "REPAIRING" {
		t.Fatalf("pending import state=%q, want REPAIRING",
			importedMetadata.Instances[name].Instance.State)
	}
	var importedOperations map[string]*orchestrator.Operation
	if err := target.Load("orchestrator/operations", &importedOperations); err != nil {
		t.Fatal(err)
	}
	operation := importedOperations[managerName]
	if operation == nil || operation.Status != orchestrator.StatusDone || !operation.Done ||
		operation.Progress != 100 || operation.EndTime == "" || operation.Error == nil ||
		!strings.Contains(operation.Error.Message, "side effects were not replayed") {
		t.Fatalf("portable pending operation was not safely interrupted: %#v", operation)
	}
}

func TestRedisLegacyMetadataMigratesWithoutBackendAdoption(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/legacy"
	store, err := state.New(t.TempDir(), "legacy-migration")
	if err != nil {
		t.Fatal(err)
	}
	spec := persistedRedisBackendForTest(name)
	// This fixture is the exact pre-schema wire shape introduced by 59d8e9e:
	// each map value contained only instance and backendId.
	legacyPayload := []byte(fmt.Sprintf(`{
		"instances": {
			%q: {
				"instance": {
					"name": %q,
					"displayName": "Legacy cache",
					"labels": {"team": "platform"},
					"tier": "BASIC",
					"memorySizeGb": 2,
					"host": "127.0.0.1",
					"port": 46379,
					"state": "READY",
					"createTime": "2026-07-01T10:00:00Z",
					"locationId": "us-central1",
					"alternativeLocationId": "us-east1",
					"authorizedNetwork": "projects/test/global/networks/default",
					"connectMode": "DIRECT_PEERING",
					"persistenceConfig": {"persistenceMode": "RDB", "rdbSnapshotPeriod": "SIX_HOURS"},
					"redisVersion": "REDIS_7_2",
					"transitEncryptionMode": "DISABLED"
				},
				"backendId": %q
			}
		}
	}`, name, name, spec.ResourceID))
	if err := store.Save(memorystoreStateEntry, json.RawMessage(legacyPayload)); err != nil {
		t.Fatal(err)
	}
	backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if backend.provisionCalls != 0 || backend.reconcileCalls != 0 ||
		backend.deleteCalls != 0 || backend.publishCalls != 0 {
		t.Fatalf("legacy migration touched backend: provision=%d reconcile=%d delete=%d publish=%d",
			backend.provisionCalls, backend.reconcileCalls, backend.deleteCalls, backend.publishCalls)
	}
	response := redisRequest(api, http.MethodGet, "/v1/"+name, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"REPAIRING"`) ||
		!strings.Contains(response.Body.String(), `"displayName":"Legacy cache"`) ||
		!strings.Contains(response.Body.String(), `"team":"platform"`) {
		t.Fatalf("migrated legacy response=%d %s", response.Code, response.Body.String())
	}
	var migrated redisMetadata
	if err := store.Load(memorystoreStateEntry, &migrated); err != nil {
		t.Fatal(err)
	}
	persisted := migrated.Instances[name]
	if migrated.Schema != redisMetadataSchema || migrated.Version != redisMetadataVersion ||
		persisted.Instance.Host != "" || persisted.Instance.Port != 0 ||
		persisted.Backend.ContainerID != "" || persisted.Backend.ImageID != "" ||
		persisted.Backend.HostPort != "" || persisted.Backend.Generation != 0 {
		t.Fatalf("legacy migration retained local provenance: %#v", migrated)
	}
	if persisted.Instance.DisplayName != "Legacy cache" ||
		persisted.Instance.PersistenceConfig == nil ||
		persisted.Instance.PersistenceConfig.RdbSnapshotPeriod != "SIX_HOURS" {
		t.Fatalf("legacy migration lost canonical user metadata: %#v", persisted.Instance)
	}
	var operationSibling map[string]*orchestrator.Operation
	if err := store.Load("orchestrator/operations", &operationSibling); err != nil ||
		operationSibling == nil || len(operationSibling) != 0 {
		t.Fatalf("legacy migration operation sibling=%v err=%v, want explicit empty set",
			operationSibling, err)
	}
}

func TestRedisLegacyMigrationBatchFailureLeavesOldSnapshotWhole(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/legacy-atomic"
	resourceID := backendID("test", "us-central1", "legacy-atomic")
	legacyPayload := json.RawMessage(fmt.Sprintf(`{
		"instances": {%q: {
			"instance": {
				"name": %q, "tier": "BASIC", "memorySizeGb": 1,
				"state": "READY", "locationId": "us-central1",
				"redisVersion": "REDIS_7_2", "host": "127.0.0.1", "port": 46379
			},
			"backendId": %q
		}}
	}`, name, name, resourceID))
	for _, failureStage := range []int{1, 2} {
		t.Run(fmt.Sprintf("entry-%d", failureStage), func(t *testing.T) {
			store := &atomicMigrationRedisStore{
				entries: map[string]json.RawMessage{
					memorystoreStateEntry: append(json.RawMessage(nil), legacyPayload...),
					"unrelated/entry":     json.RawMessage(`{"preserved":true}`),
				},
				failAt: failureStage,
			}
			backend := &fakeRedisBackend{endpoint: "127.0.0.1:46379", owned: true}
			if api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store); err == nil || api != nil {
				t.Fatalf("migration stage %d returned api=%#v err=%v", failureStage, api, err)
			}
			if !bytes.Equal(store.entries[memorystoreStateEntry], legacyPayload) {
				t.Fatalf("failed migration changed legacy metadata: %s",
					store.entries[memorystoreStateEntry])
			}
			if _, exists := store.entries["orchestrator/operations"]; exists {
				t.Fatal("failed migration partially persisted operation sibling")
			}
			if string(store.entries["unrelated/entry"]) != `{"preserved":true}` {
				t.Fatal("failed migration changed unrelated entry")
			}
			if backend.reconcileCalls != 0 || backend.publishCalls != 0 {
				t.Fatalf("failed migration touched backend: %#v", backend)
			}

			store.failAt = 0
			api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
			if err != nil {
				t.Fatalf("retry migration: %v", err)
			}
			if api.instances[name].State != "REPAIRING" {
				t.Fatalf("retry migration state=%q", api.instances[name].State)
			}
			var operations map[string]*orchestrator.Operation
			if err := store.Load("orchestrator/operations", &operations); err != nil ||
				operations == nil || len(operations) != 0 {
				t.Fatalf("committed operation sibling=%v err=%v", operations, err)
			}
			restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store)
			if err != nil || restarted.instances[name].State != "REPAIRING" {
				t.Fatalf("restart after migration api=%#v err=%v", restarted, err)
			}
			if backend.reconcileCalls != 0 || backend.publishCalls != 0 {
				t.Fatalf("migration retry/restart touched backend: %#v", backend)
			}
		})
	}
}

func TestRedisLegacyMigrationPreservesConcurrentOperationCommit(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/legacy-interleaving"
	root := t.TempDir()
	first, err := state.New(root, "legacy-interleaving")
	if err != nil {
		t.Fatal(err)
	}
	second, err := state.New(root, "legacy-interleaving")
	if err != nil {
		t.Fatal(err)
	}
	resourceID := backendID("test", "us-central1", "legacy-interleaving")
	legacyPayload := json.RawMessage(fmt.Sprintf(`{
		"instances": {%q: {
			"instance": {
				"name": %q, "tier": "BASIC", "memorySizeGb": 1,
				"state": "READY", "locationId": "us-central1",
				"redisVersion": "REDIS_7_2"
			},
			"backendId": %q
		}}
	}`, name, name, resourceID))
	if err := first.Save(memorystoreStateEntry, legacyPayload); err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(second)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &interleavingTransformRedisStore{
		Store: first, entered: make(chan struct{}), release: make(chan struct{}),
	}
	backend := &fakeRedisBackend{}
	type result struct {
		api *API
		err error
	}
	started := make(chan result, 1)
	go func() {
		api, startErr := NewAPIWithStore(manager, backend, nil, wrapped)
		started <- result{api: api, err: startErr}
	}()
	select {
	case <-wrapped.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy migration did not pause before transactional reload")
	}
	operation, err := manager.RegisterDurable(
		"test#operation", "CREATE", "projects/test/resources/concurrent", "", "us-central1")
	if err != nil {
		t.Fatal(err)
	}
	close(wrapped.release)
	outcome := <-started
	if outcome.err != nil || outcome.api == nil {
		t.Fatalf("migration outcome api=%#v err=%v", outcome.api, outcome.err)
	}
	if backend.reconcileCalls != 0 || backend.publishCalls != 0 {
		t.Fatalf("migration interleaving touched backend: %#v", backend)
	}
	var durable map[string]*orchestrator.Operation
	if err := first.Load("orchestrator/operations", &durable); err != nil {
		t.Fatal(err)
	}
	if durable[operation.Name] == nil || manager.Get(operation.Name) == nil {
		t.Fatalf("concurrent operation lost: disk=%v memory=%v",
			durable[operation.Name], manager.Get(operation.Name))
	}
	restartedManager, err := orchestrator.NewOperationManagerWithStore(first)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithStore(restartedManager, backend, nil, first)
	if err != nil || restarted == nil || restartedManager.Get(operation.Name) == nil {
		t.Fatalf("restart lost concurrent operation api=%#v operation=%#v err=%v",
			restarted, restartedManager.Get(operation.Name), err)
	}
}

func TestRedisLegacyMigrationRejectsAmbiguousStateWithoutReplacement(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/legacy"
	validSpec := persistedRedisBackendForTest(name)
	valid := legacyRedisMetadata{Instances: map[string]legacyPersistedInstance{name: {
		Instance: &Instance{Name: name, Tier: "BASIC", MemorySizeGb: 1, State: "READY",
			LocationId: "us-central1", Host: "127.0.0.1", Port: 46379, RedisVersion: "REDIS_7_2"},
		BackendID: validSpec.ResourceID,
	}}}
	tests := map[string]func(map[string]json.RawMessage){
		"partial-envelope": func(envelope map[string]json.RawMessage) {
			envelope["schema"] = json.RawMessage(`"minisky-memorystore-redis"`)
		},
		"unknown-field": func(envelope map[string]json.RawMessage) {
			envelope["unexpected"] = json.RawMessage(`true`)
		},
		"mismatched-key": func(envelope map[string]json.RawMessage) {
			var instances map[string]legacyPersistedInstance
			if err := json.Unmarshal(envelope["instances"], &instances); err != nil {
				panic(err)
			}
			item := instances[name]
			item.Instance.Name += "-other"
			instances[name] = item
			envelope["instances"], _ = json.Marshal(instances)
		},
		"hybrid-backend-object": func(envelope map[string]json.RawMessage) {
			var instances map[string]map[string]json.RawMessage
			if err := json.Unmarshal(envelope["instances"], &instances); err != nil {
				panic(err)
			}
			item := instances[name]
			item["backend"] = json.RawMessage(`{"containerId":"copied-labels-are-not-identity"}`)
			instances[name] = item
			envelope["instances"], _ = json.Marshal(instances)
		},
	}
	for testName, mutate := range tests {
		t.Run(testName, func(t *testing.T) {
			store := &toggleRedisStore{}
			raw, err := json.Marshal(valid)
			if err != nil {
				t.Fatal(err)
			}
			var envelope map[string]json.RawMessage
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatal(err)
			}
			mutate(envelope)
			raw, err = json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			store.data = append([]byte(nil), raw...)
			backend := &fakeRedisBackend{owned: true}
			if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, store); err == nil {
				t.Fatal("ambiguous legacy state was accepted")
			}
			if !bytes.Equal(store.data, raw) {
				t.Fatal("failed legacy migration replaced prior state")
			}
			if backend.provisionCalls != 0 || backend.reconcileCalls != 0 ||
				backend.deleteCalls != 0 || backend.publishCalls != 0 {
				t.Fatal("failed legacy migration touched a backend")
			}
		})
	}
}

func TestRedisImportSemanticValidationPreservesPriorProfile(t *testing.T) {
	store, err := state.New(t.TempDir(), "validation-target")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("unrelated/marker", map[string]string{"value": "preserved"}); err != nil {
		t.Fatal(err)
	}
	validName := "projects/test/locations/us-central1/instances/cache"
	base := redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{validName: {
			Instance: &Instance{Name: validName, Tier: "BASIC", MemorySizeGb: 1,
				State: "REPAIRING", LocationId: "us-central1", RedisVersion: "REDIS_7_2"},
			BackendID: backendID("test", "us-central1", "cache"),
			Backend:   orchestrator.Redis72BackendSpec(backendID("test", "us-central1", "cache")),
		}},
	}
	tests := map[string]func(*redisMetadata){
		"legacy-schema":       func(value *redisMetadata) { value.Schema = ""; value.Version = 0 },
		"unsupported-version": func(value *redisMetadata) { value.Version++ },
		"duplicate-backend": func(value *redisMetadata) {
			duplicate := value.Instances[validName]
			duplicate.Instance = cloneInstance(duplicate.Instance)
			duplicate.Instance.Name = "projects/test/locations/us-central1/instances/cache-2"
			value.Instances[duplicate.Instance.Name] = duplicate
		},
		"malformed-operation": func(value *redisMetadata) {
			value.Operations = map[string]operationTarget{"bad": {Delete: true, ResourceKey: "missing"}}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := base
			value.Instances = map[string]persistedInstance{validName: base.Instances[validName]}
			mutate(&value)
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := json.Marshal(state.Snapshot{
				Format: state.SnapshotFormat, Version: state.Version,
				Entries: map[string]json.RawMessage{memorystoreStateEntry: raw},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Import(bytes.NewReader(snapshot)); err == nil {
				t.Fatal("invalid Redis snapshot replaced the target profile")
			}
			var marker map[string]string
			if err := store.Load("unrelated/marker", &marker); err != nil ||
				marker["value"] != "preserved" {
				t.Fatalf("failed import changed prior profile: marker=%v err=%v", marker, err)
			}
		})
	}
}

func TestRedisImportRejectsNoncanonicalSemanticShapes(t *testing.T) {
	const (
		instanceName = "projects/test/locations/us-central1/instances/cache"
		managerName  = "operation-1234567890-abcdefgh"
	)
	operationName := "projects/test/locations/us-central1/operations/" + managerName
	spec := persistedRedisBackendForTest(instanceName)
	base := redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{instanceName: {
			Instance: &Instance{Name: instanceName, Tier: "BASIC", MemorySizeGb: 1,
				Host: "127.0.0.1", Port: 46379, State: "READY",
				LocationId: "us-central1", RedisVersion: "REDIS_7_2",
				ConnectMode: "DIRECT_PEERING", TransitEncryptionMode: "DISABLED"},
			BackendID: spec.ResourceID, Backend: spec,
		}},
		Operations: map[string]operationTarget{operationName: {
			ManagerName: managerName, ResourceKey: instanceName,
		}},
	}
	baseOperations := map[string]*orchestrator.Operation{managerName: {
		ID: "123", Name: managerName, Kind: "redis#operation", OperationType: "CREATE",
		Status: orchestrator.StatusDone, Progress: 100, Done: true, TargetLink: instanceName,
		InsertTime: "2026-07-01T10:00:00Z", StartTime: "2026-07-01T10:00:01Z",
		EndTime: "2026-07-01T10:00:02Z",
		Region:  "us-central1", ServiceKind: "redis#operation",
		Project: "test", Location: "us-central1",
	}}
	type mutation func(*redisMetadata, map[string]*orchestrator.Operation)
	tests := map[string]mutation{
		"noncanonical-resource-path": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			item := value.Instances[instanceName]
			delete(value.Instances, instanceName)
			item.Instance.Name += "/extra"
			value.Instances[item.Instance.Name] = item
		},
		"map-key-name-mismatch": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			item := value.Instances[instanceName]
			item.Instance.Name += "-other"
			value.Instances[instanceName] = item
		},
		"location-mismatch": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.LocationId = "us-east1"
		},
		"derived-backend-mismatch": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			item := value.Instances[instanceName]
			item.BackendID = "redis-foreign"
			item.Backend.ResourceID = "redis-foreign"
			value.Instances[instanceName] = item
		},
		"unsupported-tier": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.Tier = "PREMIUM"
		},
		"unsupported-version": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.RedisVersion = "REDIS_7_4"
		},
		"unsupported-connect-mode": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.ConnectMode = "PRIVATE_SERVICE_ACCESS"
		},
		"unsupported-encryption": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.TransitEncryptionMode = "SERVER_AUTHENTICATION"
		},
		"unsupported-state": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.State = "AVAILABLE"
		},
		"malformed-create-time": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.CreateTime = "yesterday"
		},
		"cross-project-network": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.AuthorizedNetwork =
				"projects/other/global/networks/default"
		},
		"unsupported-persistence": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.PersistenceConfig =
				&PersistenceConfig{PersistenceMode: "AOF", RdbSnapshotPeriod: "ONE_HOUR"}
		},
		"partial-runtime-provenance": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			item := value.Instances[instanceName]
			item.Backend.ContainerIdentity = ""
			value.Instances[instanceName] = item
		},
		"runtime-host-mismatch": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Instances[instanceName].Instance.Host = "localhost"
		},
		"ready-without-runtime": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			item := value.Instances[instanceName]
			item.Backend = orchestrator.Redis72BackendSpec(item.BackendID)
			item.Instance.Host = ""
			item.Instance.Port = 0
			value.Instances[instanceName] = item
		},
		"noncanonical-operation-name": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			target := value.Operations[operationName]
			delete(value.Operations, operationName)
			value.Operations["operations/"+managerName] = target
		},
		"operation-target-mismatch": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			target := value.Operations[operationName]
			target.ResourceKey = "projects/test/locations/us-central1/instances/other"
			value.Operations[operationName] = target
		},
		"operation-verb-mismatch": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].OperationType = "DELETE"
		},
		"operation-scope-mismatch": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Project = "other"
		},
		"unsupported-operation-status": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Status = "SUCCEEDED"
		},
		"missing-durable-operation": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			delete(operations, managerName)
		},
		"operation-status-result-mismatch": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Status = orchestrator.StatusRunning
			operations[managerName].Done = true
		},
		"operation-error-response-conflict": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Error = &orchestrator.OperationError{Code: 500, Message: "failed"}
			operations[managerName].Response = json.RawMessage(`{"name":"unexpected"}`)
		},
		"operation-arbitrary-response": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Response = json.RawMessage(`{"name":"unexpected"}`)
		},
		"operation-malformed-error": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Error = &orchestrator.OperationError{Code: 200}
		},
		"operation-out-of-range-error": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Error = &orchestrator.OperationError{Code: 600, Message: "invalid"}
		},
		"operation-invalid-insert-time": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].InsertTime = "not-a-time"
		},
		"operation-end-before-start": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].EndTime = "2026-07-01T09:59:59Z"
		},
		"terminal-progress-not-complete": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations[managerName].Progress = 99
		},
		"orphan-operation": func(_ *redisMetadata, operations map[string]*orchestrator.Operation) {
			operations["operation-orphan"] = &orchestrator.Operation{
				ID: "orphan", Name: "operation-orphan", Kind: "redis#operation", OperationType: "CREATE",
				Status: orchestrator.StatusDone, Progress: 100, Done: true,
				InsertTime: "2026-07-01T10:00:00Z", EndTime: "2026-07-01T10:00:02Z",
				TargetLink: instanceName, Region: "us-central1", ServiceKind: "redis#operation",
				Project: "test", Location: "us-central1",
			}
		},
		"completed-delete-retains-resource": func(value *redisMetadata, operations map[string]*orchestrator.Operation) {
			target := value.Operations[operationName]
			target.Delete = true
			value.Operations[operationName] = target
			operations[managerName].OperationType = "DELETE"
		},
		"completed-create-missing-resource": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			delete(value.Instances, instanceName)
		},
		"duplicate-operation-manager": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			value.Operations["projects/test/locations/us-central1/operations/duplicate"] =
				value.Operations[operationName]
		},
		"duplicate-operation-id": func(value *redisMetadata, operations map[string]*orchestrator.Operation) {
			const otherManager = "operation-1234567890-ijklmnop"
			otherName := "projects/test/locations/us-central1/operations/" + otherManager
			value.Operations[otherName] = operationTarget{
				ManagerName: otherManager, ResourceKey: instanceName,
			}
			clone := *operations[managerName]
			clone.Name = otherManager
			operations[otherManager] = &clone
		},
		"conflicting-active-operations": func(value *redisMetadata, operations map[string]*orchestrator.Operation) {
			const otherManager = "operation-1234567890-qrstuvwx"
			otherName := "projects/test/locations/us-central1/operations/" + otherManager
			value.Instances[instanceName].Instance.State = "CREATING"
			first := operations[managerName]
			first.Status = orchestrator.StatusRunning
			first.Progress = 50
			first.Done = false
			first.EndTime = ""
			first.Response = nil
			value.Operations[otherName] = operationTarget{
				ManagerName: otherManager, ResourceKey: instanceName,
			}
			clone := *first
			clone.ID = "456"
			clone.Name = otherManager
			operations[otherManager] = &clone
		},
		"duplicate-backend-resource": func(value *redisMetadata, _ map[string]*orchestrator.Operation) {
			item := value.Instances[instanceName]
			item.Instance = cloneInstance(item.Instance)
			item.Instance.Name = "projects/test/locations/us-central1/instances/cache-2"
			value.Instances[item.Instance.Name] = item
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			var metadata redisMetadata
			raw, err := json.Marshal(base)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &metadata); err != nil {
				t.Fatal(err)
			}
			var operations map[string]*orchestrator.Operation
			raw, err = json.Marshal(baseOperations)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &operations); err != nil {
				t.Fatal(err)
			}
			mutate(&metadata, operations)
			metadataRaw, _ := json.Marshal(metadata)
			operationsRaw, _ := json.Marshal(operations)
			snapshot, _ := json.Marshal(state.Snapshot{
				Format: state.SnapshotFormat, Version: state.Version,
				Entries: map[string]json.RawMessage{
					memorystoreStateEntry:     metadataRaw,
					"orchestrator/operations": operationsRaw,
				},
			})
			store, err := state.New(t.TempDir(), "semantic-target")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Save("unrelated/marker", map[string]string{"value": "preserved"}); err != nil {
				t.Fatal(err)
			}
			if err := store.Import(bytes.NewReader(snapshot)); err == nil {
				t.Fatal("noncanonical Redis snapshot was accepted")
			}
			var marker map[string]string
			if err := store.Load("unrelated/marker", &marker); err != nil ||
				marker["value"] != "preserved" {
				t.Fatalf("failed import changed prior profile: marker=%v err=%v", marker, err)
			}
		})
	}
}

func TestRedisValidationAcceptsHistoricalCreateAfterCompletedDelete(t *testing.T) {
	const instanceName = "projects/test/locations/us-central1/instances/cache"
	createManager := "operation-1234567890-abcdefgh"
	deleteManager := "operation-1234567891-ijklmnop"
	metadata := redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{},
		Operations: map[string]operationTarget{
			"projects/test/locations/us-central1/operations/" + createManager: {
				ManagerName: createManager, ResourceKey: instanceName,
			},
			"projects/test/locations/us-central1/operations/" + deleteManager: {
				ManagerName: deleteManager, ResourceKey: instanceName, Delete: true,
			},
		},
	}
	operation := func(id, name, verb, start, end string) *orchestrator.Operation {
		return &orchestrator.Operation{
			ID: id, Name: name, Kind: "redis#operation", OperationType: verb,
			Status: orchestrator.StatusDone, Progress: 100, Done: true,
			TargetLink: instanceName, InsertTime: start, StartTime: start, EndTime: end,
			Region: "us-central1", ServiceKind: "redis#operation",
			Project: "test", Location: "us-central1",
		}
	}
	operations := map[string]*orchestrator.Operation{
		createManager: operation("123", createManager, "CREATE",
			"2026-07-01T10:00:00Z", "2026-07-01T10:00:01Z"),
		deleteManager: operation("456", deleteManager, "DELETE",
			"2026-07-01T10:01:00Z", "2026-07-01T10:01:01Z"),
	}
	if err := validateRedisMetadataWithOperations(&metadata, operations); err != nil {
		t.Fatalf("completed create/delete history rejected: %v", err)
	}

	recreateManager := "operation-1234567892-qrstuvwx"
	recreateName := "projects/test/locations/us-central1/operations/" + recreateManager
	metadata.Operations[recreateName] = operationTarget{
		ManagerName: recreateManager, ResourceKey: instanceName,
	}
	resourceID := backendID("test", "us-central1", "cache")
	metadata.Instances[instanceName] = persistedInstance{
		Instance: &Instance{
			Name: instanceName, Tier: "BASIC", MemorySizeGb: 1, State: "CREATING",
			LocationId: "us-central1", RedisVersion: "REDIS_7_2",
		},
		BackendID: resourceID, Backend: orchestrator.Redis72BackendSpec(resourceID),
	}
	operations[recreateManager] = &orchestrator.Operation{
		ID: "789", Name: recreateManager, Kind: "redis#operation", OperationType: "CREATE",
		Status: orchestrator.StatusRunning, Progress: 50, TargetLink: instanceName,
		InsertTime: "2026-07-01T10:02:00Z", StartTime: "2026-07-01T10:02:00Z",
		Region: "us-central1", ServiceKind: "redis#operation",
		Project: "test", Location: "us-central1",
	}
	if err := validateRedisMetadataWithOperations(&metadata, operations); err != nil {
		t.Fatalf("active recreation after completed deletion rejected: %v", err)
	}
}

func TestRedisValidationUsesLatestTerminalOutcomeIncludingFailures(t *testing.T) {
	const instanceName = "projects/test/locations/us-central1/instances/cache"
	terminal := func(id, manager, verb, end string, failed bool) *orchestrator.Operation {
		operation := &orchestrator.Operation{
			ID: id, Name: manager, Kind: "redis#operation", OperationType: verb,
			Status: orchestrator.StatusDone, Progress: 100, Done: true,
			TargetLink: instanceName, InsertTime: end, StartTime: end, EndTime: end,
			Region: "us-central1", ServiceKind: "redis#operation",
			Project: "test", Location: "us-central1",
		}
		if failed {
			operation.Error = &orchestrator.OperationError{Code: 500, Message: "backend cleanup incomplete"}
		}
		return operation
	}
	type event struct {
		manager string
		delete  bool
		op      *orchestrator.Operation
	}
	validate := func(t *testing.T, instance *persistedInstance, events ...event) error {
		t.Helper()
		metadata := redisMetadata{
			Schema: redisMetadataSchema, Version: redisMetadataVersion,
			Instances:  map[string]persistedInstance{},
			Operations: map[string]operationTarget{},
		}
		if instance != nil {
			metadata.Instances[instanceName] = *instance
		}
		operations := make(map[string]*orchestrator.Operation, len(events))
		for _, item := range events {
			name := "projects/test/locations/us-central1/operations/" + item.manager
			metadata.Operations[name] = operationTarget{
				ManagerName: item.manager, ResourceKey: instanceName, Delete: item.delete,
			}
			operations[item.manager] = item.op
		}
		return validateRedisMetadataWithOperations(&metadata, operations)
	}
	resourceID := backendID("test", "us-central1", "cache")
	repairing := persistedInstance{
		Instance: &Instance{
			Name: instanceName, Tier: "BASIC", MemorySizeGb: 1, State: "REPAIRING",
			LocationId: "us-central1", RedisVersion: "REDIS_7_2",
		},
		BackendID: resourceID, Backend: orchestrator.Redis72BackendSpec(resourceID),
	}
	readySpec := persistedRedisBackendForTest(instanceName)
	ready := persistedInstance{
		Instance: &Instance{
			Name: instanceName, Tier: "BASIC", MemorySizeGb: 1, State: "READY",
			Host: "127.0.0.1", Port: 46379, LocationId: "us-central1",
			RedisVersion: "REDIS_7_2",
		},
		BackendID: resourceID, Backend: readySpec,
	}
	deleteManager := "operation-1234567890-deleteaa"
	failedCreateManager := "operation-1234567891-createbb"
	deleteSuccess := event{manager: deleteManager, delete: true,
		op: terminal("100", deleteManager, "DELETE", "2026-07-01T10:00:00Z", false)}
	failedCreate := event{manager: failedCreateManager,
		op: terminal("101", failedCreateManager, "CREATE", "2026-07-01T10:01:00Z", true)}

	if err := validate(t, &repairing, deleteSuccess, failedCreate); err != nil {
		t.Fatalf("failed create retained canonical REPAIRING metadata: %v", err)
	}
	if err := validate(t, nil, deleteSuccess, failedCreate); err != nil {
		t.Fatalf("failed create cleanup to absence: %v", err)
	}
	if err := validate(t, &ready, deleteSuccess, failedCreate); err == nil {
		t.Fatal("failed create accepted impossible READY runtime")
	}

	createManager := "operation-1234567892-createcc"
	failedDeleteManager := "operation-1234567893-deletedd"
	createSuccess := event{manager: createManager,
		op: terminal("102", createManager, "CREATE", "2026-07-01T10:02:00Z", false)}
	failedDelete := event{manager: failedDeleteManager, delete: true,
		op: terminal("103", failedDeleteManager, "DELETE", "2026-07-01T10:03:00Z", true)}
	if err := validate(t, &ready, createSuccess, failedDelete); err != nil {
		t.Fatalf("failed delete retained prior runtime: %v", err)
	}
	if err := validate(t, nil, createSuccess, failedDelete); err != nil {
		t.Fatalf("failed delete after completed cleanup: %v", err)
	}
	if err := validate(t, &repairing, createSuccess, failedDelete); err != nil {
		t.Fatalf("failed delete retained metadata-only repair state: %v", err)
	}
	impossibleDeleting := ready
	impossibleDeleting.Instance = cloneInstance(ready.Instance)
	impossibleDeleting.Instance.State = "DELETING"
	if err := validate(t, &impossibleDeleting, createSuccess, failedDelete); err == nil {
		t.Fatal("failed delete accepted impossible retained DELETING state")
	}

	laterCreateManager := "operation-1234567894-createee"
	laterCreate := event{manager: laterCreateManager,
		op: terminal("104", laterCreateManager, "CREATE", "2026-07-01T10:04:00Z", false)}
	if err := validate(t, &ready, deleteSuccess, failedCreate, laterCreate); err != nil {
		t.Fatalf("later successful create did not override failure: %v", err)
	}
	tiedFailedCreate := failedCreate
	tiedFailedCreate.op = terminal("105", failedCreateManager, "CREATE",
		"2026-07-01T10:00:00Z", true)
	if err := validate(t, nil, deleteSuccess, tiedFailedCreate); err == nil {
		t.Fatal("equal terminal timestamps were accepted")
	}
}

func TestRedisTerminalHistoryIsIsolatedPerResource(t *testing.T) {
	const (
		deletedName = "projects/test/locations/us-central1/instances/deleted"
		readyName   = "projects/test/locations/us-central1/instances/ready"
		deleteOp    = "operation-1234567890-deletezz"
		createOp    = "operation-1234567891-createzz"
	)
	readySpec := persistedRedisBackendForTest(readyName)
	metadata := redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{readyName: {
			Instance: &Instance{
				Name: readyName, Tier: "BASIC", MemorySizeGb: 1, State: "READY",
				Host: "127.0.0.1", Port: 46379, LocationId: "us-central1",
				RedisVersion: "REDIS_7_2",
			},
			BackendID: readySpec.ResourceID, Backend: readySpec,
		}},
		Operations: map[string]operationTarget{
			"projects/test/locations/us-central1/operations/" + deleteOp: {
				ManagerName: deleteOp, ResourceKey: deletedName, Delete: true,
			},
			"projects/test/locations/us-central1/operations/" + createOp: {
				ManagerName: createOp, ResourceKey: readyName,
			},
		},
	}
	terminal := func(id, manager, verb, target, end string) *orchestrator.Operation {
		return &orchestrator.Operation{
			ID: id, Name: manager, Kind: "redis#operation", OperationType: verb,
			Status: orchestrator.StatusDone, Progress: 100, Done: true, TargetLink: target,
			InsertTime: end, StartTime: end, EndTime: end, Region: "us-central1",
			ServiceKind: "redis#operation", Project: "test", Location: "us-central1",
		}
	}
	operations := map[string]*orchestrator.Operation{
		deleteOp: terminal("201", deleteOp, "DELETE", deletedName, "2026-07-01T10:00:00Z"),
		createOp: terminal("202", createOp, "CREATE", readyName, "2026-07-01T10:00:00Z"),
	}
	if err := validateRedisMetadataWithOperations(&metadata, operations); err != nil {
		t.Fatalf("independent terminal histories interfered: %v", err)
	}
}

func TestRedisDeletePersistsDeletingAndOperationAtomicallyAndRollsBack(t *testing.T) {
	const name = "projects/test/locations/us-central1/instances/cache"
	store := &failCombinedRedisStore{}
	if err := store.Save(memorystoreStateEntry, redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: map[string]persistedInstance{
			name: {
				Instance: &Instance{
					Name:         name,
					Tier:         "BASIC",
					MemorySizeGb: 1,
					State:        "READY",
					LocationId:   "us-central1",
					Host:         "127.0.0.1",
					Port:         46379,
					RedisVersion: "REDIS_7_2",
				},
				BackendID: backendID("test", "us-central1", "cache"),
				Backend:   persistedRedisBackendForTest(name),
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
	mu              sync.Mutex
	endpoint        string
	owned           bool
	err             error
	deleteErr       error
	lastImage       string
	provisionCalls  int
	reconcileCalls  int
	deleteCalls     int
	publishCalls    int
	unpublishCalls  int
	discardCalls    int
	reconcileResult *orchestrator.RedisBackendSpec
	publishErr      error
	publishFailAt   int
	publishBlockAt  int
	publishEntered  chan struct{}
	publishRelease  chan struct{}
	published       map[string]orchestrator.RedisBackendSpec
}

func persistedRedisBackendForTest(name string) orchestrator.RedisBackendSpec {
	project, location, id, ok := canonicalRedisInstanceName(name)
	if !ok {
		panic("invalid Redis test instance name")
	}
	spec := orchestrator.Redis72BackendSpec(backendID(project, location, id))
	spec.ImageID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	spec.VolumeIdentity = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	spec.VolumeProvenance = "sha256:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	spec.ContainerIdentity = "sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	spec.ContainerID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	spec.Generation = 1
	spec.HostPort = "46379"
	return spec
}

type failCombinedRedisStore struct {
	mu      sync.Mutex
	data    []byte
	failing bool
}

type failSecondRedisStore struct {
	mu    sync.Mutex
	data  []byte
	saves int
}

type blockingSecondRedisStore struct {
	mu      sync.Mutex
	data    []byte
	saves   int
	entered chan struct{}
	release chan struct{}
}

type blockingFailFirstRedisStore struct {
	mu      sync.Mutex
	data    []byte
	saves   int
	entered chan struct{}
	release chan struct{}
}

type blockingFailNthRedisStore struct {
	mu      sync.Mutex
	data    []byte
	saves   int
	failAt  int
	entered chan struct{}
	release chan struct{}
}

type toggleRedisStore struct {
	mu      sync.Mutex
	data    []byte
	failing bool
}

type atomicMigrationRedisStore struct {
	mu      sync.Mutex
	entries map[string]json.RawMessage
	failAt  int
}

type interleavingTransformRedisStore struct {
	*state.Store
	entered chan struct{}
	release chan struct{}
}

func (s *interleavingTransformRedisStore) TransformEntries(
	expectedVersion string,
	transform state.EntryTransform,
) (state.TransformResult, error) {
	close(s.entered)
	<-s.release
	return s.Store.TransformEntries(expectedVersion, transform)
}

func (s *atomicMigrationRedisStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, exists := s.entries[name]
	if !exists {
		return state.ErrNotFound
	}
	return json.Unmarshal(payload, target)
}

func (s *atomicMigrationRedisStore) Save(name string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[name] = payload
	return nil
}

func (s *atomicMigrationRedisStore) SaveEntries(entries map[string]json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := make(map[string]json.RawMessage, len(s.entries)+len(entries))
	for name, payload := range s.entries {
		candidate[name] = append(json.RawMessage(nil), payload...)
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for index, name := range names {
		if s.failAt == index+1 {
			return fmt.Errorf("injected atomic migration entry %d failure", index+1)
		}
		candidate[name] = append(json.RawMessage(nil), entries[name]...)
	}
	s.entries = candidate
	return nil
}

func (s *atomicMigrationRedisStore) TransformEntries(
	_ string,
	transform state.EntryTransform,
) (state.TransformResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := make(map[string]json.RawMessage, len(s.entries)+1)
	for name, payload := range s.entries {
		candidate[name] = append(json.RawMessage(nil), payload...)
	}
	if err := transform(candidate); err != nil {
		return state.TransformResult{}, err
	}
	names := []string{memorystoreStateEntry, "orchestrator/operations"}
	for index, name := range names {
		if s.failAt == index+1 {
			return state.TransformResult{},
				fmt.Errorf("injected atomic migration entry %d failure", index+1)
		}
		if payload, exists := candidate[name]; exists {
			candidate[name] = append(json.RawMessage(nil), payload...)
		}
	}
	s.entries = candidate
	return state.TransformResult{Version: "test-version", Entries: candidate}, nil
}

func (s *toggleRedisStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *blockingFailFirstRedisStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *blockingFailFirstRedisStore) Save(_ string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.saves++
	save := s.saves
	s.mu.Unlock()
	if save == 1 {
		close(s.entered)
		<-s.release
		return errors.New("injected first Redis save failure")
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

func (s *blockingFailNthRedisStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *blockingFailNthRedisStore) Save(_ string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.saves++
	save := s.saves
	s.mu.Unlock()
	if save == s.failAt {
		close(s.entered)
		<-s.release
		return errors.New("injected Redis transaction save failure")
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

func (s *toggleRedisStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failing {
		return errors.New("injected reconciliation save failure")
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	return err
}

func (s *blockingSecondRedisStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *blockingSecondRedisStore) Save(_ string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.saves++
	saveNumber := s.saves
	s.mu.Unlock()
	if saveNumber == 2 {
		close(s.entered)
		<-s.release
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

func (s *failSecondRedisStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *failSecondRedisStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saves == 2 {
		return errors.New("injected post-provision save failure")
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	return err
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

func (b *fakeRedisBackend) ProvisionRedis(
	_ context.Context,
	spec orchestrator.RedisBackendSpec,
) (string, orchestrator.RedisBackendSpec, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.provisionCalls++
	b.lastImage = spec.Image
	if spec.ImageID == "" {
		spec.ImageID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	}
	if spec.VolumeIdentity == "" {
		spec.VolumeIdentity = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		spec.VolumeProvenance = "sha256:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	}
	if spec.ContainerID == "" {
		spec.ContainerID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		spec.ContainerIdentity = "sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		spec.Generation = 1
	}
	if _, port, err := net.SplitHostPort(b.endpoint); err == nil {
		spec.HostPort = port
	}
	if b.err != nil {
		return "", spec, b.err
	}
	return b.endpoint, spec, nil
}

func (b *fakeRedisBackend) ReconcileRedis(
	_ context.Context,
	spec orchestrator.RedisBackendSpec,
) (string, orchestrator.RedisBackendSpec, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reconcileCalls++
	if b.reconcileResult != nil {
		spec = *b.reconcileResult
	}
	if _, port, err := net.SplitHostPort(b.endpoint); err == nil && spec.HostPort == "" {
		spec.HostPort = port
	}
	return b.endpoint, spec, b.owned, b.err
}

func (b *fakeRedisBackend) DeleteRedis(context.Context, orchestrator.RedisBackendSpec) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleteCalls++
	if b.deleteErr != nil {
		return b.deleteErr
	}
	return b.err
}

func (b *fakeRedisBackend) PublishRedis(_ context.Context, spec orchestrator.RedisBackendSpec) error {
	b.mu.Lock()
	b.publishCalls++
	call := b.publishCalls
	block := b.publishBlockAt == call
	entered := b.publishEntered
	release := b.publishRelease
	b.mu.Unlock()
	if block {
		close(entered)
		<-release
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.publishErr != nil || b.publishFailAt == call {
		if b.publishErr != nil {
			return b.publishErr
		}
		return errors.New("injected indexed publish failure")
	}
	if b.published == nil {
		b.published = make(map[string]orchestrator.RedisBackendSpec)
	}
	b.published[spec.ResourceID] = spec
	return nil
}

func (b *fakeRedisBackend) UnpublishRedis(spec orchestrator.RedisBackendSpec) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.unpublishCalls++
	delete(b.published, spec.ResourceID)
}

func (b *fakeRedisBackend) DiscardRedis(
	context.Context,
	orchestrator.RedisBackendSpec,
	bool,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.discardCalls++
	return nil
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
