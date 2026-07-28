package registry

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type sharedTestHandler struct{}

func (*sharedTestHandler) ServeHTTP(http.ResponseWriter, *http.Request) {}

func TestSharedHandlerCreatesOneOwnerConcurrently(t *testing.T) {
	ctx := &Context{shared: make(map[string]http.Handler)}
	var creates atomic.Int32
	factory := func() http.Handler {
		creates.Add(1)
		return &sharedTestHandler{}
	}
	handlers := make(chan http.Handler, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			handlers <- ctx.SharedHandler("bigtable", factory)
		}()
	}
	group.Wait()
	close(handlers)
	var first http.Handler
	for handler := range handlers {
		if first == nil {
			first = handler
		} else if first != handler {
			t.Fatal("shared domains received different owners")
		}
	}
	if creates.Load() != 1 {
		t.Fatalf("owner creations = %d, want 1", creates.Load())
	}
}

func TestRequireDockerRejectsHybridRegistration(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("hybrid Compute registration was accepted as pure Docker passthrough")
		}
	}()
	RequireDocker("compute.googleapis.com")
}

func TestRuntimeHandlerDockerOperationPreflight(t *testing.T) {
	const (
		runnableBatch = `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox:1.36"}}]},"taskCount":"1","parallelism":"1"}]}`
		metadataBatch = `{"taskGroups":[{}]}`
		primary       = `{"instanceType":"PRIMARY"}`
		readPool      = `{"instanceType":"READ_POOL"}`
	)
	tests := []struct {
		name       string
		domain     string
		method     string
		path       string
		body       string
		nextStatus int
		wantStatus int
		wantCalled bool
	}{
		{"batch runnable create", "batch.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/jobs", runnableBatch, http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"composer environment create", "composer.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/environments", `{}`, http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"composer environment delete", "composer.googleapis.com", http.MethodDelete, "/v1/projects/p/locations/l/environments/e", "", http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"managed kafka cluster create", "managedkafka.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/clusters", `{}`, http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"managed kafka cluster delete", "managedkafka.googleapis.com", http.MethodDelete, "/v1/projects/p/locations/l/clusters/c", "", http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"alloydb primary create", "alloydb.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/clusters/c/instances", primary, http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"alloydb instance delete", "alloydb.googleapis.com", http.MethodDelete, "/v1/projects/p/locations/l/clusters/c/instances/i", "", http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"redis create", "redis.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/instances", `{}`, http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"redis delete", "redis.googleapis.com", http.MethodDelete, "/v1/projects/p/locations/l/instances/i", "", http.StatusTeapot, http.StatusServiceUnavailable, false},
		{"memcache node count update reaches semantic handler", "memcache.googleapis.com", http.MethodPatch, "/v1/projects/p/locations/l/instances/i?updateMask=nodeCount", `{}`, http.StatusTeapot, http.StatusTeapot, true},
		{"memcache comma separated node count reaches semantic handler", "memcache.googleapis.com", http.MethodPatch, "/v1/projects/p/locations/l/instances/i?updateMask=displayName%2C%20nodeCount%20", `{}`, http.StatusTeapot, http.StatusTeapot, true},

		{"batch metadata create", "batch.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/jobs", metadataBatch, http.StatusCreated, http.StatusCreated, true},
		{"batch unsupported runnable shape", "batch.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/jobs", `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox"}}]},"taskCount":"2"}]}`, http.StatusNotImplemented, http.StatusNotImplemented, true},
		{"batch read", "batch.googleapis.com", http.MethodGet, "/v1/projects/p/locations/l/jobs/j", "", http.StatusOK, http.StatusOK, true},
		{"composer metadata patch", "composer.googleapis.com", http.MethodPatch, "/v1/projects/p/locations/l/environments/e", `{}`, http.StatusOK, http.StatusOK, true},
		{"managed kafka topic create is outside cluster contract", "managedkafka.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/clusters/c/topics", `{}`, http.StatusCreated, http.StatusCreated, true},
		{"managed kafka metadata patch", "managedkafka.googleapis.com", http.MethodPatch, "/v1/projects/p/locations/l/clusters/c", `{}`, http.StatusOK, http.StatusOK, true},
		{"alloydb metadata cluster create", "alloydb.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/clusters", `{}`, http.StatusCreated, http.StatusCreated, true},
		{"alloydb unsupported read pool", "alloydb.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/clusters/c/instances", readPool, http.StatusNotImplemented, http.StatusNotImplemented, true},
		{"valkey remains unsupported", "memorystore.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/instances", `{}`, http.StatusNotImplemented, http.StatusNotImplemented, true},
		{"redis unsupported export", "redis.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/instances/i:export", `{}`, http.StatusNotImplemented, http.StatusNotImplemented, true},
		{"memcache metadata update", "memcache.googleapis.com", http.MethodPatch, "/v1/projects/p/locations/l/instances/i?updateMask=displayName%2Clabels", `{}`, http.StatusOK, http.StatusOK, true},
		{"memcache noncanonical mask case remains metadata", "memcache.googleapis.com", http.MethodPatch, "/v1/projects/p/locations/l/instances/i?updateMask=NodeCount", `{}`, http.StatusOK, http.StatusOK, true},
		{"unknown domain defaults off", "example.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/instances", `{}`, http.StatusCreated, http.StatusCreated, true},
		{"available Docker bypasses preflight", "composer.googleapis.com", http.MethodPost, "/v1/projects/p/locations/l/environments", `{}`, http.StatusCreated, http.StatusCreated, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var called atomic.Bool
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called.Store(true)
				w.WriteHeader(test.nextStatus)
			})
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			response := httptest.NewRecorder()

			dockerAvailable := test.name == "available Docker bypasses preflight"
			RuntimeHandler(test.domain, next, dockerAvailable).ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if called.Load() != test.wantCalled {
				t.Fatalf("next called = %t, want %t", called.Load(), test.wantCalled)
			}
			if test.wantStatus == http.StatusServiceUnavailable {
				var envelope struct {
					Error struct {
						Code    int    `json:"code"`
						Message string `json:"message"`
						Status  string `json:"status"`
					} `json:"error"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
					t.Fatalf("response is not JSON: %v", err)
				}
				if envelope.Error.Code != http.StatusServiceUnavailable ||
					envelope.Error.Status != "UNAVAILABLE" ||
					envelope.Error.Message != "MiniSky: Docker backend unavailable" {
					t.Fatalf("unexpected unavailable envelope: %+v", envelope.Error)
				}
			}
		})
	}
}

func TestRuntimeHandlerPreservesRequestBodyAndOrdering(t *testing.T) {
	body := []byte(`{"taskGroups":[{}]}`)
	var received []byte
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusAccepted)
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/projects/p/locations/l/jobs", bytes.NewReader(body))
	response := httptest.NewRecorder()

	RuntimeHandler("batch.googleapis.com", next, false).ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
	if !bytes.Equal(received, body) {
		t.Fatalf("body after preflight = %s, want %s", received, body)
	}

	response = httptest.NewRecorder()
	RuntimeHandler("batch.googleapis.com", next, false).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, UnsupportedContractPath, strings.NewReader(`{}`)),
	)
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported contract status = %d, want %d", response.Code, http.StatusNotImplemented)
	}

	t.Setenv(ExperimentalServicesEnv, "")
	response = httptest.NewRecorder()
	RuntimeHandler(
		"batch.googleapis.com",
		experimentalDisabledHandler("batch.googleapis.com"),
		false,
	).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodPost, "/v1/projects/p/locations/l/jobs", strings.NewReader(
			`{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox"}}]}}]}`,
		)),
	)
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("default-off status = %d, want %d; body: %s",
			response.Code, http.StatusNotImplemented, response.Body.String())
	}
}
