package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestMiniSkyAPIURLUsesCanonicalRoute(t *testing.T) {
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:9090/base/")
	miniskyEndpoint = ""

	got, err := miniskyAPIURL("cloudbuild", "/v1/projects/local-dev-project/builds")
	if err != nil {
		t.Fatalf("miniskyAPIURL returned an error: %v", err)
	}
	want := "http://127.0.0.1:9090/base/_minisky/cloudbuild/v1/projects/local-dev-project/builds"
	if got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func TestMiniSkyAPIURLFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:9090")
	miniskyEndpoint = "http://127.0.0.1:9191/"
	t.Cleanup(func() { miniskyEndpoint = "" })

	got, err := miniskyAPIURL("secretmanager", "v1/projects/test/secrets")
	if err != nil {
		t.Fatalf("miniskyAPIURL returned an error: %v", err)
	}
	if want := "http://127.0.0.1:9191/_minisky/secretmanager/v1/projects/test/secrets"; got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func TestMiniSkyAPIURLRejectsInvalidEndpoint(t *testing.T) {
	t.Setenv("MINISKY_ENDPOINT", "localhost:8080")
	miniskyEndpoint = ""

	if _, err := miniskyAPIURL("storage", "/storage/v1/b"); err == nil {
		t.Fatal("miniskyAPIURL accepted an endpoint without a scheme")
	}
}

func TestCanonicalURLAndStatusHandlingWithHTTPTestServer(t *testing.T) {
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		http.Error(w, "gateway unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)
	t.Setenv("MINISKY_ENDPOINT", server.URL)
	miniskyEndpoint = ""

	endpoint, err := miniskyAPIURL("artifactregistry", "/v1/projects/p/locations/l/repositories")
	if err != nil {
		t.Fatalf("miniskyAPIURL returned an error: %v", err)
	}
	err = getJSON(endpoint, &struct{}{})
	if err == nil || !strings.Contains(err.Error(), "502 Bad Gateway") {
		t.Fatalf("error = %v, want status error", err)
	}
	if want := "/_minisky/artifactregistry/v1/projects/p/locations/l/repositories"; requestedPath != want {
		t.Fatalf("request path = %q, want %q", requestedPath, want)
	}
}

func TestPostJSONUsesPOSTAndDecodesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", contentType)
		}
		w.Write([]byte(`{"entries":[{"insertId":"1"}]}`))
	}))
	t.Cleanup(server.Close)

	var response struct {
		Entries []struct {
			InsertID string `json:"insertId"`
		} `json:"entries"`
	}
	if err := postJSON(server.URL, map[string]interface{}{}, &response); err != nil {
		t.Fatalf("postJSON returned an error: %v", err)
	}
	if len(response.Entries) != 1 || response.Entries[0].InsertID != "1" {
		t.Fatalf("response = %#v, want one log entry", response)
	}
}

func TestUnsupportedHeadlessCommandMessage(t *testing.T) {
	var stderr bytes.Buffer
	command := &cobra.Command{}
	command.SetErr(&stderr)

	unsupportedHeadlessCommand(command, "Spanner")
	if got := stderr.String(); !strings.Contains(got, "does not expose a public MiniSky shim route") {
		t.Fatalf("message = %q, want unsupported shim explanation", got)
	}
}

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
