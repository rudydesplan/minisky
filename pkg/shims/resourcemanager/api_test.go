package resourcemanager

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"minisky/pkg/state"
)

func TestProjectCRUDPersistsAndKeepsProjectsIsolated(t *testing.T) {
	store, err := state.New(t.TempDir(), "projects")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"project-a", "project-b"} {
		response := request(t, api, http.MethodPost, "/v3/projects", `{"projectId":"`+id+`","displayName":"`+id+`"}`)
		if response.Code != http.StatusOK {
			t.Fatalf("create %s: status=%d body=%s", id, response.Code, response.Body.String())
		}
	}
	if !api.Exists("project-a") || !api.Exists("project-b") {
		t.Fatal("created projects were not indexed")
	}

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	response := request(t, restarted, http.MethodGet, "/v3/projects", "")
	var list struct {
		Projects []Project `json:"projects"`
	}
	decode(t, response, &list)
	if len(list.Projects) != 3 {
		t.Fatalf("projects = %#v, want seed plus two", list.Projects)
	}

	response = request(t, restarted, http.MethodDelete, "/v3/projects/project-a", "")
	if response.Code != http.StatusOK || restarted.Exists("project-a") {
		t.Fatalf("delete response=%d body=%s", response.Code, response.Body.String())
	}
	if !restarted.Exists("project-b") {
		t.Fatal("deleting project-a affected project-b")
	}
}

func TestProjectValidationAndSeedDeletion(t *testing.T) {
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{}`, `{"projectId":"INVALID"}`, `{"projectId":"ab"}`} {
		response := request(t, api, http.MethodPost, "/v3/projects", body)
		assertError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
	}
	response := request(t, api, http.MethodDelete, "/v3/projects/local-dev-project", "")
	assertError(t, response, http.StatusBadRequest, "FAILED_PRECONDITION")
}

func TestProjectHierarchyAncestors(t *testing.T) {
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.PutOrganization(Organization{Name: "organizations/100", DisplayName: "Local"}); err != nil {
		t.Fatal(err)
	}
	if err := api.PutFolder(Folder{Name: "folders/200", Parent: "organizations/100", DisplayName: "Team"}); err != nil {
		t.Fatal(err)
	}
	response := request(t, api, http.MethodPost, "/v3/projects", `{"projectId":"nested-project","parent":"folders/200"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create nested project: %s", response.Body.String())
	}
	ancestors := api.Ancestors("projects/nested-project")
	if len(ancestors) != 3 || ancestors[0] != "projects/nested-project" ||
		ancestors[1] != "folders/200" || ancestors[2] != "organizations/100" {
		t.Fatalf("ancestors = %v", ancestors)
	}
}

func TestProjectRegistryExportImport(t *testing.T) {
	source, err := state.New(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(source)
	if err != nil {
		t.Fatal(err)
	}
	if response := request(t, api, http.MethodPost, "/v3/projects", `{"projectId":"export-project"}`); response.Code != http.StatusOK {
		t.Fatalf("create: %s", response.Body.String())
	}
	var snapshot bytes.Buffer
	if err := source.Export(&snapshot); err != nil {
		t.Fatal(err)
	}
	target, err := state.New(t.TempDir(), "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Import(bytes.NewReader(snapshot.Bytes())); err != nil {
		t.Fatal(err)
	}
	imported, err := NewAPIWithStore(target)
	if err != nil {
		t.Fatal(err)
	}
	if !imported.Exists("export-project") {
		t.Fatal("project did not survive export/import")
	}
}

func request(t *testing.T, api *API, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, response *httptest.ResponseRecorder, destination any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), destination); err != nil {
		t.Fatalf("decode %q: %v", response.Body.String(), err)
	}
}

func assertError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	var body struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	decode(t, response, &body)
	if response.Code != code || body.Error.Code != code || body.Error.Status != status {
		t.Fatalf("response=%d error=%+v body=%s", response.Code, body.Error, response.Body.String())
	}
}
