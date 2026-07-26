package bigtable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
	assertBigtableError(t, api, http.MethodGet,
		"/v2/projects/demo/instances/primary/clusters", "",
		http.StatusNotImplemented, "UNIMPLEMENTED")
	assertBigtableError(t, api, http.MethodPatch,
		"/v2/projects/demo/instances/primary", `{}`,
		http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
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
