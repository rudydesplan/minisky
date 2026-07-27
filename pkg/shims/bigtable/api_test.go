package bigtable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/state"
)

type fakeBigtableBackend struct {
	endpoint string
	err      error
	ensure   func()
}

func (b fakeBigtableBackend) Ensure(context.Context) (string, error) {
	if b.ensure != nil {
		b.ensure()
	}
	return b.endpoint, b.err
}

func TestAdminLifecyclePersistsAcrossRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, fakeBigtableBackend{endpoint: "127.0.0.1:9000"}, store)
	if err != nil {
		t.Fatal(err)
	}

	assertBigtableRequest(t, api, http.MethodPost, "/v2/projects/demo/instances",
		`{"instanceId":"primary","instance":{"displayName":"Primary","type":"DEVELOPMENT"}}`, http.StatusOK)
	assertBigtableRequest(t, api, http.MethodPost, "/v2/projects/demo/instances/primary/tables",
		`{"tableId":"events","table":{"columnFamilies":{"cf":{}}}}`, http.StatusOK)

	restarted, err := NewAPIWithStore(nil, fakeBigtableBackend{endpoint: "127.0.0.1:9000"}, store)
	if err != nil {
		t.Fatal(err)
	}
	instance := assertBigtableRequest(t, restarted, http.MethodGet,
		"/v2/projects/demo/instances/primary", "", http.StatusOK)
	if got := instance["state"]; got != metadataOnlyInstanceState {
		t.Fatalf("rehydrated instance state = %v, want %s", got, metadataOnlyInstanceState)
	}
	table := assertBigtableRequest(t, restarted, http.MethodGet,
		"/v2/projects/demo/instances/primary/tables/events", "", http.StatusOK)
	if table["name"] != "projects/demo/instances/primary/tables/events" {
		t.Fatalf("rehydrated table = %#v", table)
	}

	assertBigtableRequest(t, restarted, http.MethodDelete,
		"/v2/projects/demo/instances/primary/tables/events", "", http.StatusOK)
	assertBigtableRequest(t, restarted, http.MethodDelete,
		"/v2/projects/demo/instances/primary", "", http.StatusOK)
	empty, err := NewAPIWithStore(nil, fakeBigtableBackend{endpoint: "127.0.0.1:9000"}, store)
	if err != nil {
		t.Fatal(err)
	}
	assertBigtableError(t, empty, http.MethodGet, "/v2/projects/demo/instances/primary", "",
		http.StatusNotFound, "NOT_FOUND")
}

func TestCanonicalErrorsAndUnavailableEmulator(t *testing.T) {
	api, err := NewAPIWithStore(nil, fakeBigtableBackend{err: errors.New("docker unavailable")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertBigtableError(t, api, http.MethodPost, "/v2/projects/demo/instances", "{",
		http.StatusBadRequest, "INVALID_ARGUMENT")
	assertBigtableError(t, api, http.MethodPost, "/v2/projects/demo/instances",
		`{"instanceId":"primary","instance":{}}`, http.StatusServiceUnavailable, "UNAVAILABLE")
	api.instances["projects/demo/instances/primary"] = &Instance{Name: "projects/demo/instances/primary"}
	api.tables["projects/demo/instances/primary/tables/events"] = &Table{
		Name: "projects/demo/instances/primary/tables/events",
	}
	assertBigtableError(t, api, http.MethodGet,
		"/v2/projects/demo/instances/primary/tables/events:readRows", "",
		http.StatusServiceUnavailable, "UNAVAILABLE")
}

func TestTableCreateRechecksParentAfterBackendStartup(t *testing.T) {
	var api *API
	backend := fakeBigtableBackend{endpoint: "127.0.0.1:9000", ensure: func() {
		api.mu.Lock()
		delete(api.instances, "projects/demo/instances/primary")
		api.mu.Unlock()
	}}
	var err error
	api, err = NewAPIWithStore(nil, backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.instances["projects/demo/instances/primary"] = &Instance{Name: "projects/demo/instances/primary"}

	assertBigtableError(t, api, http.MethodPost,
		"/v2/projects/demo/instances/primary/tables",
		`{"tableId":"events","table":{}}`, http.StatusNotFound, "NOT_FOUND")
	if len(api.tables) != 0 {
		t.Fatalf("orphan tables = %#v", api.tables)
	}
}

func TestUnsupportedAndMethodErrorsUseCanonicalEnvelopes(t *testing.T) {
	api, err := NewAPIWithStore(nil, fakeBigtableBackend{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertBigtableError(t, api, http.MethodPatch,
		"/v2/projects/demo/instances/primary/clusters/primary-c1", `{}`,
		http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	assertBigtableError(t, api, http.MethodPatch,
		"/v2/projects/demo/instances/primary", `{}`,
		http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
}

func TestClusterAdminLifecyclePersistsAcrossRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "clusters")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, fakeBigtableBackend{endpoint: "127.0.0.1:9000"}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.instances["projects/demo/instances/primary"] = &Instance{
		Name: "projects/demo/instances/primary", State: "READY",
	}
	response := assertBigtableRequest(t, api, http.MethodPost,
		"/v2/projects/demo/instances/primary/clusters?clusterId=primary-c1",
		`{"location":"projects/demo/locations/us-central1-b","serveNodes":1}`,
		http.StatusOK)
	if response["done"] != true {
		t.Fatalf("create operation = %#v", response)
	}
	operationName, _ := response["name"].(string)
	metadata := response["metadata"].(map[string]interface{})
	if metadata["@type"] != "type.googleapis.com/google.bigtable.admin.v2.CreateClusterMetadata" {
		t.Fatalf("operation metadata = %#v", metadata)
	}
	originalRequest := metadata["originalRequest"].(map[string]interface{})
	if originalRequest["parent"] != "projects/demo/instances/primary" ||
		originalRequest["clusterId"] != "primary-c1" {
		t.Fatalf("operation target metadata = %#v", originalRequest)
	}
	polled := assertBigtableRequest(t, api, http.MethodGet, "/v2/"+operationName, "", http.StatusOK)
	cluster := polled["response"].(map[string]interface{})
	if cluster["name"] != "projects/demo/instances/primary/clusters/primary-c1" ||
		cluster["state"] != metadataOnlyInstanceState ||
		cluster["@type"] != "type.googleapis.com/google.bigtable.admin.v2.Cluster" {
		t.Fatalf("created cluster response = %#v", cluster)
	}
	foreignName := strings.Replace(operationName, "projects/demo/", "projects/foreign/", 1)
	api.operations[foreignName] = cloneBigtableOperation(api.operations[operationName])
	api.operations[foreignName].Name = foreignName
	assertBigtableError(t, api, http.MethodGet, "/v2/"+foreignName, "",
		http.StatusNotFound, "NOT_FOUND")

	restarted, err := NewAPIWithStore(nil, fakeBigtableBackend{endpoint: "127.0.0.1:9000"}, store)
	if err != nil {
		t.Fatal(err)
	}
	assertBigtableRequest(t, restarted, http.MethodGet,
		"/v2/projects/demo/instances/primary/clusters/primary-c1", "", http.StatusOK)
	assertBigtableRequest(t, restarted, http.MethodGet, "/v2/"+operationName, "", http.StatusOK)
	deleted := assertBigtableRequest(t, restarted, http.MethodDelete,
		"/v2/projects/demo/instances/primary/clusters/primary-c1", "", http.StatusOK)
	if deleted["done"] != true {
		t.Fatalf("delete operation = %#v", deleted)
	}
	deleteMetadata := deleted["metadata"].(map[string]interface{})
	if deleteMetadata["@type"] != "type.googleapis.com/google.bigtable.admin.v2.DeleteClusterMetadata" {
		t.Fatalf("delete metadata = %#v", deleteMetadata)
	}
	deleteRequest := deleteMetadata["originalRequest"].(map[string]interface{})
	if deleteRequest["name"] != "projects/demo/instances/primary/clusters/primary-c1" {
		t.Fatalf("delete target metadata = %#v", deleteRequest)
	}
	assertBigtableError(t, restarted, http.MethodGet,
		"/v2/projects/demo/instances/primary/clusters/primary-c1", "",
		http.StatusNotFound, "NOT_FOUND")
}

func assertBigtableRequest(
	t *testing.T, handler http.Handler, method, path, body string, status int,
) map[string]interface{} {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
	if rec.Code != status {
		t.Fatalf("%s %s = %d, body %s", method, path, rec.Code, rec.Body.String())
	}
	var decoded map[string]interface{}
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode %s %s: %v", method, path, err)
		}
	}
	return decoded
}

func assertBigtableError(
	t *testing.T, handler http.Handler, method, path, body string, code int, status string,
) {
	t.Helper()
	decoded := assertBigtableRequest(t, handler, method, path, body, code)
	errBody, ok := decoded["error"].(map[string]interface{})
	if !ok || int(errBody["code"].(float64)) != code || errBody["status"] != status {
		t.Fatalf("error envelope = %#v", decoded)
	}
	if _, ok := errBody["details"].([]interface{}); !ok {
		t.Fatalf("error details missing: %#v", decoded)
	}
}
