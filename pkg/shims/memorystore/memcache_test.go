package memorystore

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
)

func TestMemcacheProviderLifecycle(t *testing.T) {
	backend := &fakeMemcacheBackend{
		result: MemcacheBackendResult{
			Owned:  true,
			Exists: true,
			Endpoints: []string{
				"127.0.0.1:41211",
			},
		},
	}
	api, err := NewMemcacheAPIWithStore(orchestrator.NewOperationManager(), backend, nil)
	if err != nil {
		t.Fatal(err)
	}

	create := memcacheRequest(api, http.MethodPost,
		"/v1/projects/local-dev-project/locations/us-central1/instances?instanceId=cache",
		`{"authorizedNetwork":"projects/local-dev-project/global/networks/default","displayName":"cache","labels":{"goog-terraform-provisioned":"true"},"memcacheVersion":"MEMCACHE_1_5","nodeConfig":{"cpuCount":1,"memorySizeMb":1024},"nodeCount":1}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	waitForMemcacheOperation(t, api, operationNameFromResponse(t, create))

	get := memcacheRequest(api, http.MethodGet,
		"/v1/projects/local-dev-project/locations/us-central1/instances/cache", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	var instance MemcacheInstance
	if err := json.Unmarshal(get.Body.Bytes(), &instance); err != nil {
		t.Fatal(err)
	}
	if instance.State != "READY" || len(instance.MemcacheNodes) != 1 ||
		instance.MemcacheNodes[0].Host != "127.0.0.1" ||
		instance.MemcacheNodes[0].Port != 41211 {
		t.Fatalf("instance=%+v", instance)
	}

	update := memcacheRequest(api, http.MethodPatch,
		"/v1/projects/local-dev-project/locations/us-central1/instances/cache?updateMask=displayName,labels",
		`{"displayName":"updated","labels":{"env":"test"}}`)
	if update.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", update.Code, update.Body.String())
	}
	waitForMemcacheOperation(t, api, operationNameFromResponse(t, update))

	deleted := memcacheRequest(api, http.MethodDelete,
		"/v1/projects/local-dev-project/locations/us-central1/instances/cache", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleted.Code, deleted.Body.String())
	}
	waitForMemcacheOperation(t, api, operationNameFromResponse(t, deleted))
	missing := memcacheRequest(api, http.MethodGet,
		"/v1/projects/local-dev-project/locations/us-central1/instances/cache", "")
	assertRedisError(t, missing, http.StatusNotFound, "NOT_FOUND")
}

type fakeMemcacheBackend struct {
	mu sync.Mutex

	result           MemcacheBackendResult
	provisionErr     error
	updateErr        error
	reconcileErr     error
	deleteErr        error
	provisionStarted chan struct{}
	provisionRelease chan struct{}
	updateStarted    chan struct{}
	updateRelease    chan struct{}
	deleteStarted    chan struct{}
	deleteRelease    chan struct{}

	provisionCalls int
	updateCalls    int
	deleteCalls    int
	reconcileCalls int
	reconcileSpecs []MemcacheBackendSpec
}

func (b *fakeMemcacheBackend) ProvisionMemcache(
	_ context.Context,
	_ string,
	_ int,
	_ int,
	_ int,
	_ string,
	_ map[string]string,
) ([]string, bool, bool, error) {
	b.mu.Lock()
	b.provisionCalls++
	result, err := b.result, b.provisionErr
	started, release := b.provisionStarted, b.provisionRelease
	b.mu.Unlock()
	signalMemcacheCall(started)
	waitMemcacheCall(release)
	return result.Endpoints, result.Owned, result.Exists, err
}

func (b *fakeMemcacheBackend) UpdateMemcache(
	_ context.Context,
	_ string,
	_ int,
	_ int,
	_ int,
	_ string,
	_ map[string]string,
) ([]string, bool, bool, error) {
	b.mu.Lock()
	b.updateCalls++
	result, err := b.result, b.updateErr
	started, release := b.updateStarted, b.updateRelease
	b.mu.Unlock()
	signalMemcacheCall(started)
	waitMemcacheCall(release)
	return result.Endpoints, result.Owned, result.Exists, err
}

func (b *fakeMemcacheBackend) ReconcileMemcache(
	_ context.Context,
	backendID string,
	nodeCount int,
	cpuCount int,
	memoryMB int,
	version string,
	params map[string]string,
) ([]string, bool, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reconcileCalls++
	b.reconcileSpecs = append(b.reconcileSpecs, MemcacheBackendSpec{
		BackendID: backendID,
		NodeCount: nodeCount,
		CPUCount:  cpuCount,
		MemoryMB:  memoryMB,
		Version:   version,
		Params:    cloneStringMap(params),
	})
	return b.result.Endpoints, b.result.Owned, b.result.Exists, b.reconcileErr
}

func (b *fakeMemcacheBackend) DeleteMemcache(context.Context, string) error {
	b.mu.Lock()
	b.deleteCalls++
	err := b.deleteErr
	started, release := b.deleteStarted, b.deleteRelease
	b.mu.Unlock()
	signalMemcacheCall(started)
	waitMemcacheCall(release)
	return err
}

func signalMemcacheCall(channel chan struct{}) {
	if channel == nil {
		return
	}
	select {
	case channel <- struct{}{}:
	default:
	}
}

func waitMemcacheCall(channel chan struct{}) {
	if channel != nil {
		<-channel
	}
}

func memcacheRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Host = "memcache.googleapis.com"
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func waitForMemcacheOperation(t *testing.T, api *MemcacheAPI, name string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		response := memcacheRequest(api, http.MethodGet, "/v1/"+name, "")
		if response.Code != http.StatusOK {
			t.Fatalf("poll status=%d body=%s", response.Code, response.Body.String())
		}
		var operation struct {
			Done  bool                         `json:"done"`
			Error *orchestrator.OperationError `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
			t.Fatal(err)
		}
		if operation.Done {
			if operation.Error != nil {
				t.Fatalf("Memcached operation failed: %+v", operation.Error)
			}
			api.mutationMu.Lock()
			api.mutationMu.Unlock()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("Memcached operation did not complete")
}

func waitForMemcacheOperationError(t *testing.T, api *MemcacheAPI, name string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		response := memcacheRequest(api, http.MethodGet, "/v1/"+name, "")
		if response.Code != http.StatusOK {
			t.Fatalf("poll status=%d body=%s", response.Code, response.Body.String())
		}
		var operation struct {
			Done  bool                         `json:"done"`
			Error *orchestrator.OperationError `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
			t.Fatal(err)
		}
		if operation.Done {
			if operation.Error == nil {
				t.Fatalf("Memcached operation unexpectedly succeeded: %s", response.Body.String())
			}
			api.mutationMu.Lock()
			api.mutationMu.Unlock()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("Memcached operation did not finish with an error")
}
