package artifactregistry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
)

func TestRepositoryCreateOperationReadDeleteLifecycle(t *testing.T) {
	api := newTestAPI("http://registry.invalid")
	const repositoryName = "projects/test/locations/us/repositories/apps"

	created := serve(t, api, http.MethodPost, "/v1/projects/test/locations/us/repositories?repository_id=apps", `{
		"format":"DOCKER",
		"description":"application images",
		"labels":{"environment":"test"}
	}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", created.Code, created.Body.String())
	}
	var createOperation struct {
		Name string `json:"name"`
		Done bool   `json:"done"`
	}
	decodeResponse(t, created, &createOperation)
	if !strings.HasPrefix(createOperation.Name, "projects/test/locations/us/operations/") {
		t.Fatalf("create operation name = %q", createOperation.Name)
	}
	if createOperation.Done {
		t.Fatal("create operation was immediately done")
	}

	createResult := pollOperation(t, api, createOperation.Name)
	var createdRepository Repository
	response, ok := createResult["response"]
	if !ok {
		t.Fatalf("terminal create operation = %#v", createResult)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal create response: %v", err)
	}
	if err := json.Unmarshal(encoded, &createdRepository); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if createdRepository.Name != repositoryName || createdRepository.Format != "DOCKER" {
		t.Fatalf("created repository = %#v", createdRepository)
	}

	read := serve(t, api, http.MethodGet, "/v1/"+repositoryName, "")
	if read.Code != http.StatusOK {
		t.Fatalf("read status = %d, body = %s", read.Code, read.Body.String())
	}
	var readRepository Repository
	decodeResponse(t, read, &readRepository)
	if readRepository.Name != repositoryName ||
		readRepository.Description != "application images" ||
		readRepository.Labels["environment"] != "test" ||
		readRepository.CreateTime == "" ||
		readRepository.UpdateTime != readRepository.CreateTime {
		t.Fatalf("read repository = %#v", readRepository)
	}

	deleted := serve(t, api, http.MethodDelete, "/v1/"+repositoryName, "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleted.Code, deleted.Body.String())
	}
	var deleteOperation struct {
		Name string `json:"name"`
		Done bool   `json:"done"`
	}
	decodeResponse(t, deleted, &deleteOperation)
	if !strings.HasPrefix(deleteOperation.Name, "projects/test/locations/us/operations/") {
		t.Fatalf("delete operation name = %q", deleteOperation.Name)
	}
	deleteResult := pollOperation(t, api, deleteOperation.Name)
	if _, ok := deleteResult["response"]; !ok {
		t.Fatalf("terminal delete operation = %#v", deleteResult)
	}

	missing := serve(t, api, http.MethodGet, "/v1/"+repositoryName, "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("read after delete status = %d, body = %s", missing.Code, missing.Body.String())
	}
	assertErrorStatus(t, missing, "NOT_FOUND")
}

func TestUnknownRepositoryOperationReturnsNotFound(t *testing.T) {
	api := newTestAPI("http://registry.invalid")
	response := serve(t, api, http.MethodGet, "/v1/projects/test/locations/us/operations/missing", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	assertErrorStatus(t, response, "NOT_FOUND")
}

func TestListPackagesUsesRepositoryScopedRegistryCatalog(t *testing.T) {
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/_catalog" {
			t.Fatalf("unexpected registry path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"repositories": []string{"apps/team/api", "apps/worker", "other/unrelated"},
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
		if r.URL.Path != "/v2/apps/team/api/tags/list" {
			t.Fatalf("unexpected registry path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": "apps/team/api",
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
		case "/v2/apps/empty/tags/list":
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

func pollOperation(t *testing.T, api *API, name string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response := serve(t, api, http.MethodGet, "/v1/"+name, "")
		if response.Code != http.StatusOK {
			t.Fatalf("poll %q status = %d, body = %s", name, response.Code, response.Body.String())
		}
		var operation map[string]any
		decodeResponse(t, response, &operation)
		if done, _ := operation["done"].(bool); done {
			if operation["error"] != nil {
				t.Fatalf("operation failed: %#v", operation)
			}
			return operation
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("operation %q did not complete", name)
	return nil
}

func assertErrorStatus(t *testing.T, response *httptest.ResponseRecorder, want string) {
	t.Helper()
	var body struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	decodeResponse(t, response, &body)
	if body.Error.Status != want {
		t.Fatalf("error status = %q, want %q", body.Error.Status, want)
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
