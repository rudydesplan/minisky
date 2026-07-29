package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGeneratedRedisClientUsesCanonicalLifecyclePaths(t *testing.T) {
	const (
		parent       = "/_minisky/redis.googleapis.com/v1/projects/demo/locations/us-central1"
		instancePath = parent + "/instances/cache"
	)
	responses := map[string]string{
		"POST " + parent + "/instances":        `{"name":"projects/demo/locations/us-central1/operations/create"}`,
		"GET " + parent + "/operations/create": `{"name":"projects/demo/locations/us-central1/operations/create","done":true}`,
		"GET " + instancePath:                  `{"name":"projects/demo/locations/us-central1/instances/cache","tier":"BASIC","memorySizeGb":1,"redisVersion":"REDIS_7_2","state":"READY","host":"127.0.0.1","port":46379}`,
		"GET " + parent + "/instances":         `{"instances":[{"name":"projects/demo/locations/us-central1/instances/cache","state":"READY"}]}`,
		"DELETE " + instancePath:               `{"name":"projects/demo/locations/us-central1/operations/delete"}`,
		"GET " + parent + "/operations/delete": `{"name":"projects/demo/locations/us-central1/operations/delete","done":true}`,
	}
	seen := make(map[string]bool)
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		key := request.Method + " " + request.URL.Path
		if deleted && key == "GET "+instancePath {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":404,"status":"NOT_FOUND","message":"instance not found"}}`)
			return
		}
		body, ok := responses[key]
		if !ok {
			http.Error(w, "unexpected request "+key, http.StatusNotFound)
			return
		}
		if request.Method == http.MethodPost && request.URL.Query().Get("instanceId") != "cache" {
			http.Error(w, "missing instanceId", http.StatusBadRequest)
			return
		}
		if request.Method == http.MethodDelete {
			deleted = true
		}
		seen[key] = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)

	service, err := newRedisService(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{endpoint: server.URL, project: "demo", location: "us-central1", instanceID: "cache"}
	if err := createInstance(context.Background(), service, cfg); err != nil {
		t.Fatal(err)
	}
	if err := verifyInstance(context.Background(), service, cfg); err != nil {
		t.Fatal(err)
	}
	if err := deleteInstance(context.Background(), service, cfg); err != nil {
		t.Fatal(err)
	}
	for key := range responses {
		if !seen[key] {
			t.Errorf("generated Redis client did not request %s", key)
		}
	}
}
