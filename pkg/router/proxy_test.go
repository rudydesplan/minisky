package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServeHTTPRoutesLocalComputeRequest(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/compute/v1/projects/demo/zones/us-central1-a/instances", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestServeHTTPValidatesPathMappedRequest(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("handler was called for an invalid request")
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(
		http.MethodPost,
		"http://localhost:8080/compute/v1/projects/demo/zones/us-central1-a/instances",
		strings.NewReader(`{}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestServeHTTPFlattensFirebaseSubdomain(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("firebaseio.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "https://demo.firebaseio.com/users.json", nil)
	req.Host = "demo.firebaseio.com"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestServeHTTPReturnsGCPErrorForUnknownDomain(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	req := httptest.NewRequest(http.MethodGet, "https://unknown.googleapis.com/v1/resources", nil)
	req.Host = "unknown.googleapis.com"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
	if contentType := rec.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
}
