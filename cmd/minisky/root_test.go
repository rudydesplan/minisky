package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"resource-1"}`))
	}))
	t.Cleanup(server.Close)

	var response struct {
		Name string `json:"name"`
	}
	if err := getJSON(server.URL, &response); err != nil {
		t.Fatalf("getJSON returned an error: %v", err)
	}
	if response.Name != "resource-1" {
		t.Fatalf("name = %q, want resource-1", response.Name)
	}
}

func TestGetJSONRejectsErrorStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	err := getJSON(server.URL, &struct{}{})
	if err == nil || !strings.Contains(err.Error(), "503 Service Unavailable") {
		t.Fatalf("error = %v, want status error", err)
	}
}

func TestGetJSONRejectsInvalidResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`not-json`))
	}))
	t.Cleanup(server.Close)

	if err := getJSON(server.URL, &struct{}{}); err == nil {
		t.Fatal("getJSON accepted an invalid JSON response")
	}
}
