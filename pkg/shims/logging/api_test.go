package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestLoggingMigratesLegacyEntriesToProfileState(t *testing.T) {
	root := t.TempDir()
	legacy := filepath.Join(root, "cloud_logs.json")
	if err := os.WriteFile(legacy, []byte(`[{"insertId":"legacy","severity":"INFO","textPayload":"old","logName":"projects/p/logs/app"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.New(root, "profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store, legacy, nil)
	if err != nil {
		t.Fatal(err)
	}
	if entries := api.GetEntries(); len(entries) != 1 || entries[0].InsertId != "legacy" {
		t.Fatalf("migrated entries = %#v", entries)
	}
	if _, err := os.Stat(legacy + ".migrated"); err != nil {
		t.Fatalf("legacy file was not marked migrated: %v", err)
	}
	restarted, err := NewAPIWithStore(store, legacy, nil)
	if err != nil {
		t.Fatal(err)
	}
	if entries := restarted.GetEntries(); len(entries) != 1 {
		t.Fatalf("restarted entries = %#v", entries)
	}
}

func TestLoggingWriteListFiltersAndUnsupportedSurfaces(t *testing.T) {
	api, err := NewAPIWithStore(nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	write := loggingRequest(api, http.MethodPost, "/v2/entries:write", `{"entries":[
		{"severity":"INFO","textPayload":"ok","logName":"projects/p/logs/app","resource":{"type":"global"}},
		{"severity":"ERROR","textPayload":"boom","logName":"projects/p/logs/app","resource":{"type":"cloud_run_revision"}}
	]}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write status = %d, body = %s", write.Code, write.Body.String())
	}
	list := loggingRequest(api, http.MethodPost, "/v2/entries:list",
		`{"resourceNames":["projects/p"],"filter":"severity>=ERROR AND resource.type=\"cloud_run_revision\""}`)
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	if !strings.Contains(list.Body.String(), "boom") || strings.Contains(list.Body.String(), `"ok"`) {
		t.Fatalf("filtered entries = %s", list.Body.String())
	}
	unsupportedFilter := loggingRequest(api, http.MethodPost, "/v2/entries:list", `{"filter":"timestamp > \"yesterday\""}`)
	assertLoggingError(t, unsupportedFilter, http.StatusBadRequest, "INVALID_ARGUMENT")
	for _, path := range []string{"/v2/projects/p/metrics", "/v3/projects/p/alertPolicies"} {
		response := loggingRequest(api, http.MethodPost, path, `{}`)
		assertLoggingError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
	}
}

func TestLoggingSaveFailureRollsBackAndRetryPublishesOnce(t *testing.T) {
	store := &toggleLoggingStore{fail: true}
	api, err := NewAPIWithStore(store, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"entries":[{"severity":"INFO","textPayload":"retry","logName":"projects/p/logs/app"}]}`
	failed := loggingRequest(api, http.MethodPost, "/v2/entries:write", body)
	if failed.Code != http.StatusInternalServerError || len(api.GetEntries()) != 0 {
		t.Fatalf("failed write status=%d entries=%#v", failed.Code, api.GetEntries())
	}
	store.fail = false
	retried := loggingRequest(api, http.MethodPost, "/v2/entries:write", body)
	if retried.Code != http.StatusOK || len(api.GetEntries()) != 1 {
		t.Fatalf("retried write status=%d entries=%#v", retried.Code, api.GetEntries())
	}
}

func TestCorruptStateDisablesLoggingRuntime(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-logging")
	store, err := state.New(root, "corrupt-logging")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(loggingStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI()
	response := loggingRequest(api, http.MethodPost, "/v2/entries:write",
		`{"entries":[{"logName":"projects/p/logs/app","textPayload":"blocked"}]}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(loggingStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

type toggleLoggingStore struct{ fail bool }

func (*toggleLoggingStore) Load(string, any) error { return state.ErrNotFound }
func (store *toggleLoggingStore) Save(string, any) error {
	if store.fail {
		return errors.New("disk full")
	}
	return nil
}

func TestLoggingFileAndPubSubSinksAvoidDeliveryLoops(t *testing.T) {
	deliverer := &recordingSinkDeliverer{}
	api, err := NewAPIWithStore(nil, "", deliverer)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"name":"errors-file","destination":"file://errors","filter":"severity>=ERROR"}`,
		`{"name":"errors-topic","destination":"pubsub.googleapis.com/projects/p/topics/errors","filter":"severity>=ERROR"}`,
	} {
		response := loggingRequest(api, http.MethodPost, "/v2/projects/p/sinks", body)
		if response.Code != http.StatusOK {
			t.Fatalf("create sink status = %d, body = %s", response.Code, response.Body.String())
		}
	}
	write := loggingRequest(api, http.MethodPost, "/v2/entries:write",
		`{"entries":[{"severity":"ERROR","textPayload":"deliver me","logName":"projects/p/logs/app","resource":{"type":"global"}}]}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write status = %d, body = %s", write.Code, write.Body.String())
	}
	if len(deliverer.deliveries) != 2 {
		t.Fatalf("deliveries = %#v", deliverer.deliveries)
	}

	looped := loggingRequest(api, http.MethodPost, "/v2/entries:write",
		`{"entries":[{"severity":"ERROR","textPayload":"do not redeliver","logName":"projects/p/logs/app","resource":{"type":"global"},"labels":{"minisky.logging/sink-delivery":"true"}}]}`)
	if looped.Code != http.StatusOK {
		t.Fatalf("loop write status = %d, body = %s", looped.Code, looped.Body.String())
	}
	if len(deliverer.deliveries) != 2 {
		t.Fatalf("sink delivery loop was not prevented: %#v", deliverer.deliveries)
	}
}

type recordingSinkDeliverer struct {
	deliveries []string
}

func (d *recordingSinkDeliverer) Deliver(sink LogSink, entry LogEntry) error {
	d.deliveries = append(d.deliveries, sink.Destination+":"+entry.TextPayload)
	return nil
}

func loggingRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertLoggingError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Status != status {
		t.Fatalf("status = %q, want %q", envelope.Error.Status, status)
	}
}
