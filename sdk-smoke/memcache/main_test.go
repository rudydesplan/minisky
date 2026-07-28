package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConfigRequiresLoopbackGateway(t *testing.T) {
	t.Setenv("MINISKY_ENDPOINT", "https://example.com")
	if _, err := configFromEnv(); err == nil {
		t.Fatal("non-loopback gateway was accepted")
	}
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:8080")
	t.Setenv("MINISKY_MEMCACHE_MODE", "verify")
	if _, err := configFromEnv(); err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedClientUsesCanonicalFullDomainLifecyclePaths(t *testing.T) {
	responses := map[string]string{
		"POST /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/instances":        `{"name":"projects/demo/locations/us-central1/operations/create","done":true}`,
		"GET /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/operations/create": `{"name":"projects/demo/locations/us-central1/operations/create","done":true}`,
		"PATCH /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/instances/cache": `{"name":"projects/demo/locations/us-central1/operations/update","done":true}`,
		"GET /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/operations/update": `{"name":"projects/demo/locations/us-central1/operations/update","done":true}`,
		"GET /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/instances/cache":   `{"name":"projects/demo/locations/us-central1/instances/cache","state":"READY","displayName":"MiniSky Memcached updated","nodeCount":1,"nodeConfig":{"cpuCount":1,"memorySizeMb":1024}}`,
		"GET /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/instances":         `{"instances":[{"name":"projects/demo/locations/us-central1/instances/cache","state":"READY"}]}`,
	}
	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		body, ok := responses[key]
		if !ok {
			http.Error(w, fmt.Sprintf("unexpected request %s", key), http.StatusNotFound)
			return
		}
		if r.Method == http.MethodPost && r.URL.Query().Get("instanceId") != "cache" {
			http.Error(w, "missing instanceId", http.StatusBadRequest)
			return
		}
		if r.Method == http.MethodPatch && r.URL.Query().Get("updateMask") != "displayName" {
			http.Error(w, "missing updateMask", http.StatusBadRequest)
			return
		}
		seen[key] = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)

	service, err := newMemcacheService(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{endpoint: server.URL, project: "demo", location: "us-central1", instanceID: "cache"}
	if err := createInstance(context.Background(), service, cfg); err != nil {
		t.Fatal(err)
	}
	for key := range responses {
		if key[:4] == "GET " && !seen[key] {
			t.Errorf("generated client did not request %s", key)
		}
	}
}

func TestGeneratedClientUsesCanonicalFullDomainDeletePath(t *testing.T) {
	const instancePath = "/_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/instances/cache"
	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		seen[key] = true
		w.Header().Set("Content-Type", "application/json")
		switch key {
		case "DELETE " + instancePath:
			fmt.Fprint(w, `{"name":"projects/demo/locations/us-central1/operations/delete"}`)
		case "GET /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/operations/delete":
			fmt.Fprint(w, `{"name":"projects/demo/locations/us-central1/operations/delete","done":true}`)
		case "GET " + instancePath:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"error":{"code":404,"status":"NOT_FOUND","message":"instance not found"}}`)
		default:
			http.Error(w, fmt.Sprintf("unexpected request %s", key), http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	service, err := newMemcacheService(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{endpoint: server.URL, project: "demo", location: "us-central1", instanceID: "cache"}
	if err := deleteInstance(context.Background(), service, cfg); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		"DELETE " + instancePath,
		"GET /_minisky/memcache.googleapis.com/v1/projects/demo/locations/us-central1/operations/delete",
		"GET " + instancePath,
	} {
		if !seen[key] {
			t.Errorf("generated client did not request %s", key)
		}
	}
}
