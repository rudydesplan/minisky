package artifactregistry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestListPackagesUsesRegistryCatalog(t *testing.T) {
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/_catalog" {
			t.Fatalf("unexpected registry path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"repositories": []string{"team/api", "worker"},
		})
	}))
	defer registry.Close()

	api := newTestAPI(registry.URL)
	response := serve(t, api, http.MethodGet, "/v1/projects/test/locations/us/repositories/apps/packages", "")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}

	var body struct {
		Packages []Package `json:"packages"`
	}
	decodeResponse(t, response, &body)
	if len(body.Packages) != 2 {
		t.Fatalf("packages = %#v", body.Packages)
	}
	if got, want := body.Packages[0].Name, "projects/test/locations/us/repositories/apps/packages/team/api"; got != want {
		t.Fatalf("package name = %q, want %q", got, want)
	}
	if got, want := body.Packages[1].DisplayName, "worker"; got != want {
		t.Fatalf("display name = %q, want %q", got, want)
	}
}

func TestListVersionsUsesRegistryTags(t *testing.T) {
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/team/api/tags/list" {
			t.Fatalf("unexpected registry path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": "team/api",
			"tags": []string{"latest", "v1.2.3"},
		})
	}))
	defer registry.Close()

	api := newTestAPI(registry.URL)
	response := serve(t, api, http.MethodGet, "/v1/projects/test/locations/us/repositories/apps/packages/team%2Fapi/versions", "")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}

	var body struct {
		Versions []Version `json:"versions"`
	}
	decodeResponse(t, response, &body)
	if len(body.Versions) != 2 {
		t.Fatalf("versions = %#v", body.Versions)
	}
	base := "projects/test/locations/us/repositories/apps/packages/team/api"
	if got, want := body.Versions[1].Name, base+"/versions/v1.2.3"; got != want {
		t.Fatalf("version name = %q, want %q", got, want)
	}
	if got, want := body.Versions[0].RelatedTags, []string{base + "/tags/latest"}; len(got) != 1 || got[0] != want[0] {
		t.Fatalf("related tags = %#v, want %#v", got, want)
	}
}

func TestEmptyRegistryReturnsEmptyCollections(t *testing.T) {
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/_catalog":
			_, _ = w.Write([]byte(`{"repositories":[]}`))
		case "/v2/empty/tags/list":
			_, _ = w.Write([]byte(`{"name":"empty","tags":null}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer registry.Close()

	api := newTestAPI(registry.URL)
	for _, path := range []string{
		"/v1/projects/test/locations/us/repositories/apps/packages",
		"/v1/projects/test/locations/us/repositories/apps/packages/empty/versions",
	} {
		response := serve(t, api, http.MethodGet, path, "")
		if response.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, body = %s", path, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "null") {
			t.Fatalf("%s returned a null collection: %s", path, response.Body.String())
		}
	}
}

func TestDeleteRepository(t *testing.T) {
	api := newTestAPI("http://registry.invalid")
	created := serve(t, api, http.MethodPost, "/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", created.Code, created.Body.String())
	}

	deleted := serve(t, api, http.MethodDelete, "/v1/projects/test/locations/us/repositories/apps", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleted.Code, deleted.Body.String())
	}

	listed := serve(t, api, http.MethodGet, "/v1/projects/test/locations/us/repositories", "")
	var body struct {
		Repositories []Repository `json:"repositories"`
	}
	decodeResponse(t, listed, &body)
	if len(body.Repositories) != 0 {
		t.Fatalf("repositories after delete = %#v", body.Repositories)
	}

	missing := serve(t, api, http.MethodDelete, "/v1/projects/test/locations/us/repositories/apps", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("second delete status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestRegistryFailureIsExplicit(t *testing.T) {
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "registry unavailable", http.StatusServiceUnavailable)
	}))
	defer registry.Close()

	api := newTestAPI(registry.URL)
	response := serve(t, api, http.MethodGet, "/v1/projects/test/locations/us/repositories/apps/packages", "")

	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if body := response.Body.String(); !strings.Contains(body, "503") || !strings.Contains(body, "registry unavailable") {
		t.Fatalf("failure did not preserve upstream details: %s", body)
	}
}

func TestPackageMutationIsExplicitlyUnsupported(t *testing.T) {
	api := newTestAPI("http://registry.invalid")
	response := serve(t, api, http.MethodDelete, "/v1/projects/test/locations/us/repositories/apps/packages/api", "")
	if response.Code != http.StatusNotImplemented || !strings.Contains(response.Body.String(), "digest") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func newTestAPI(registryURL string) *API {
	index := NewDockerRegistryIndex(http.DefaultClient, registryURL)
	return NewAPIWithRegistryIndex(orchestrator.NewOperationManager(), nil, index)
}

func serve(t *testing.T, api *API, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, response.Body.String())
	}
}
