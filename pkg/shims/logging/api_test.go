package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
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

func TestLoggingEntriesAndSinksSurviveRestartWithProjectIsolation(t *testing.T) {
	root := t.TempDir()
	store, err := state.New(root, "restart")
	if err != nil {
		t.Fatal(err)
	}
	deliverer := &recordingSinkDeliverer{}
	api, err := NewAPIWithStore(store, "", deliverer)
	if err != nil {
		t.Fatal(err)
	}
	create := loggingRequest(api, http.MethodPost, "/v2/projects/project-a/sinks",
		`{"name":"errors","destination":"file://errors","filter":"severity>=ERROR"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create sink status=%d body=%s", create.Code, create.Body.String())
	}
	write := loggingRequest(api, http.MethodPost, "/v2/entries:write", `{
		"logName":"projects/project-a/logs/app",
		"resource":{"type":"global","labels":{"project_id":"project-a"}},
		"labels":{"phase":"16","shared":"default"},
		"entries":[
			{"insertId":"a-info","timestamp":"2026-07-26T08:00:00Z","severity":"INFO","textPayload":"info","labels":{"shared":"entry"}},
			{"insertId":"a-error","timestamp":"2026-07-26T08:01:00Z","severity":"ERROR","textPayload":"error"},
			{"insertId":"b-error","timestamp":"2026-07-26T08:02:00Z","severity":"ERROR","textPayload":"other","logName":"projects/project-b/logs/app"}
		]}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write status=%d body=%s", write.Code, write.Body.String())
	}
	if len(deliverer.deliveries) != 1 {
		t.Fatalf("deliveries=%#v", deliverer.deliveries)
	}

	restartedDelivery := &recordingSinkDeliverer{}
	restarted, err := NewAPIWithStore(store, "", restartedDelivery)
	if err != nil {
		t.Fatal(err)
	}
	if len(restartedDelivery.deliveries) != 0 {
		t.Fatalf("restart replayed sink delivery: %#v", restartedDelivery.deliveries)
	}
	list := loggingRequest(restarted, http.MethodPost, "/v2/entries:list", `{
		"resourceNames":["projects/project-a"],
		"filter":"severity>=ERROR AND logName=\"projects/project-a/logs/app\" AND resource.type=\"global\"",
		"orderBy":"timestamp asc",
		"pageSize":10
	}`)
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	var listed struct {
		Entries []LogEntry `json:"entries"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Entries) != 1 || listed.Entries[0].InsertId != "a-error" {
		t.Fatalf("entries=%#v", listed.Entries)
	}
	if listed.Entries[0].Labels["phase"] != "16" || listed.Entries[0].Resource == nil ||
		listed.Entries[0].Resource.Type != "global" {
		t.Fatalf("inherited fields=%#v", listed.Entries[0])
	}
	get := loggingRequest(restarted, http.MethodGet, "/v2/projects/project-a/sinks/errors", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get sink status=%d body=%s", get.Code, get.Body.String())
	}
	listSinks := loggingRequest(restarted, http.MethodGet, "/v2/projects/project-a/sinks", "")
	if listSinks.Code != http.StatusOK || !strings.Contains(listSinks.Body.String(), `"name":"errors"`) {
		t.Fatalf("list sinks status=%d body=%s", listSinks.Code, listSinks.Body.String())
	}
	deleteSink := loggingRequest(restarted, http.MethodDelete, "/v2/projects/project-a/sinks/errors", "")
	if deleteSink.Code != http.StatusOK {
		t.Fatalf("delete sink status=%d body=%s", deleteSink.Code, deleteSink.Body.String())
	}
	afterDelete, err := NewAPIWithStore(store, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	missing := loggingRequest(afterDelete, http.MethodGet, "/v2/projects/project-a/sinks/errors", "")
	assertLoggingError(t, missing, http.StatusNotFound, "NOT_FOUND")
}

func TestFailedSinkDeliveryReplaysOnceAfterRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "sink-replay")
	if err != nil {
		t.Fatal(err)
	}
	failing := &recordingSinkDeliverer{err: errors.New("temporary delivery failure")}
	api, err := NewAPIWithStore(store, "", failing)
	if err != nil {
		t.Fatal(err)
	}
	create := loggingRequest(api, http.MethodPost, "/v2/projects/p/sinks",
		`{"name":"errors","destination":"file://errors","filter":"severity>=ERROR"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create sink status=%d body=%s", create.Code, create.Body.String())
	}
	write := loggingRequest(api, http.MethodPost, "/v2/entries:write",
		`{"entries":[{"insertId":"stable","severity":"ERROR","textPayload":"retry me","logName":"projects/p/logs/app"}]}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write status=%d body=%s", write.Code, write.Body.String())
	}

	replayed := &recordingSinkDeliverer{}
	restarted, err := NewAPIWithStore(store, "", replayed)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReplayPendingDeliveries(); err != nil {
		t.Fatal(err)
	}
	if len(replayed.deliveries) != 1 || replayed.deliveries[0] != "file://errors:retry me" {
		t.Fatalf("replayed deliveries=%#v", replayed.deliveries)
	}

	again := &recordingSinkDeliverer{}
	secondRestart, err := NewAPIWithStore(store, "", again)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondRestart.ReplayPendingDeliveries(); err != nil {
		t.Fatal(err)
	}
	if len(again.deliveries) != 0 {
		t.Fatalf("acknowledged delivery replayed again: %#v", again.deliveries)
	}
}

func TestSinkDeletionDurablyCancelsPendingDelivery(t *testing.T) {
	store, err := state.New(t.TempDir(), "sink-delete")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store, "", &recordingSinkDeliverer{err: errors.New("offline")})
	if err != nil {
		t.Fatal(err)
	}
	create := loggingRequest(api, http.MethodPost, "/v2/projects/p/sinks",
		`{"name":"errors","destination":"file://errors"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create=%d %s", create.Code, create.Body.String())
	}
	write := loggingRequest(api, http.MethodPost, "/v2/entries:write",
		`{"entries":[{"insertId":"stable","severity":"ERROR","textPayload":"never replay","logName":"projects/p/logs/app"}]}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write=%d %s", write.Code, write.Body.String())
	}
	api.mu.RLock()
	if len(api.pending) != 1 || api.pending[0].SinkKey != "p:errors" {
		t.Fatalf("pending=%#v", api.pending)
	}
	api.mu.RUnlock()
	deleted := loggingRequest(api, http.MethodDelete, "/v2/projects/p/sinks/errors", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete=%d %s", deleted.Code, deleted.Body.String())
	}

	replayed := &recordingSinkDeliverer{}
	restarted, err := NewAPIWithStore(store, "", replayed)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReplayPendingDeliveries(); err != nil {
		t.Fatal(err)
	}
	if len(replayed.deliveries) != 0 || len(restarted.pending) != 0 {
		t.Fatalf("deleted sink replayed: deliveries=%#v pending=%#v", replayed.deliveries, restarted.pending)
	}
}

func TestLoggingListOrderingAndBounds(t *testing.T) {
	api, err := NewAPIWithStore(nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	write := loggingRequest(api, http.MethodPost, "/v2/entries:write", `{"entries":[
		{"insertId":"b","timestamp":"2026-07-26T08:00:00Z","logName":"projects/p/logs/app"},
		{"insertId":"a","timestamp":"2026-07-26T08:00:00Z","logName":"projects/p/logs/app"},
		{"insertId":"c","timestamp":"2026-07-26T08:01:00Z","logName":"projects/p/logs/app"}
	]}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write status=%d body=%s", write.Code, write.Body.String())
	}
	for _, test := range []struct {
		name    string
		orderBy string
		want    []string
	}{
		{name: "default ascending", want: []string{"a", "b", "c"}},
		{name: "ascending", orderBy: "timestamp asc", want: []string{"a", "b", "c"}},
		{name: "descending", orderBy: "timestamp desc", want: []string{"c", "b", "a"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := loggingRequest(api, http.MethodPost, "/v2/entries:list",
				fmt.Sprintf(`{"resourceNames":["projects/p"],"orderBy":%q,"pageSize":10}`, test.orderBy))
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var body struct {
				Entries []LogEntry `json:"entries"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(body.Entries))
			for i := range body.Entries {
				got[i] = body.Entries[i].InsertId
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("order=%v want=%v", got, test.want)
			}
		})
	}
	for _, body := range []string{
		`{"orderBy":"severity desc"}`,
		`{"pageToken":"unsupported-token"}`,
		`{"resourceNames":[` + repeatedJSONStrings("projects/p", 101) + `]}`,
	} {
		response := loggingRequest(api, http.MethodPost, "/v2/entries:list", body)
		assertLoggingError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
	}
}

func TestLoggingNormalizesTimestampsBeforeOrdering(t *testing.T) {
	api, err := NewAPIWithStore(nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	write := loggingRequest(api, http.MethodPost, "/v2/entries:write", `{"entries":[
		{"insertId":"offset","timestamp":"2026-07-26T10:00:00+02:00","logName":"projects/p/logs/app"},
		{"insertId":"zulu","timestamp":"2026-07-26T08:30:00Z","logName":"projects/p/logs/app"}
	]}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write status=%d body=%s", write.Code, write.Body.String())
	}
	list := loggingRequest(api, http.MethodPost, "/v2/entries:list",
		`{"resourceNames":["projects/p"],"orderBy":"timestamp asc","pageSize":10}`)
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	var body struct {
		Entries []LogEntry `json:"entries"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Entries) != 2 || body.Entries[0].InsertId != "offset" ||
		body.Entries[0].Timestamp != "2026-07-26T08:00:00Z" ||
		body.Entries[1].InsertId != "zulu" {
		t.Fatalf("entries=%#v", body.Entries)
	}
}

func TestLoggingRejectsInvalidTimestampsForRealAndDryRunWrites(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprintf("dryRun=%t", dryRun), func(t *testing.T) {
			api, err := NewAPIWithStore(nil, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			response := loggingRequest(api, http.MethodPost, "/v2/entries:write",
				fmt.Sprintf(`{"dryRun":%t,"entries":[{"insertId":"bad","timestamp":"not-a-timestamp","logName":"projects/p/logs/app"}]}`, dryRun))
			assertLoggingError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
			if len(api.GetEntries()) != 0 {
				t.Fatalf("invalid timestamp persisted entries=%#v", api.GetEntries())
			}
		})
	}
}

func TestLoggingWriteOptionsAndBounds(t *testing.T) {
	deliverer := &recordingSinkDeliverer{}
	api, err := NewAPIWithStore(nil, "", deliverer)
	if err != nil {
		t.Fatal(err)
	}
	create := loggingRequest(api, http.MethodPost, "/v2/projects/p/sinks",
		`{"name":"all","destination":"file://all"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create sink status=%d body=%s", create.Code, create.Body.String())
	}
	dryRun := loggingRequest(api, http.MethodPost, "/v2/entries:write", `{
		"dryRun":true,
		"logName":"projects/p/logs/app",
		"resource":{"type":"global"},
		"labels":{"default":"yes","same":"default"},
		"entries":[{"insertId":"dry","timestamp":"2026-07-26T08:00:00Z","labels":{"same":"entry"}}]
	}`)
	if dryRun.Code != http.StatusOK || len(api.GetEntries()) != 0 || len(deliverer.deliveries) != 0 {
		t.Fatalf("dry run status=%d entries=%#v deliveries=%#v", dryRun.Code, api.GetEntries(), deliverer.deliveries)
	}
	partial := loggingRequest(api, http.MethodPost, "/v2/entries:write",
		`{"partialSuccess":true,"entries":[{"logName":"projects/p/logs/app"}]}`)
	assertLoggingError(t, partial, http.StatusNotImplemented, "UNIMPLEMENTED")

	entries := make([]string, 1001)
	for i := range entries {
		entries[i] = `{"logName":"projects/p/logs/app"}`
	}
	oversizedBatch := loggingRequest(api, http.MethodPost, "/v2/entries:write",
		`{"entries":[`+strings.Join(entries, ",")+`]}`)
	assertLoggingError(t, oversizedBatch, http.StatusBadRequest, "INVALID_ARGUMENT")
	if len(api.GetEntries()) != 0 {
		t.Fatalf("oversized batch mutated entries=%#v", api.GetEntries())
	}
}

func TestLoggingStrictJSONBodyLimit(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}
	const prefix = `{"value":"`
	const suffix = `"}`
	exact := prefix + strings.Repeat("x", (1<<20)-len(prefix)-len(suffix)) + suffix
	for _, test := range []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "exactly one MiB", body: exact},
		{name: "over one MiB", body: exact + " ", wantErr: true},
		{name: "unknown field", body: `{"value":"ok","extra":true}`, wantErr: true},
		{name: "trailing JSON", body: `{"value":"ok"} {}`, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			var decoded payload
			err := decodeJSON(request, &decoded)
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestLoggingSinkRoutesAndDestinationsAreCanonical(t *testing.T) {
	api, err := NewAPIWithStore(nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/v2/projects/p/sinks/extra/path",
		"/v2/projects//sinks/name",
		"/v2/projects/p/sinks/",
	} {
		response := loggingRequest(api, http.MethodGet, path, "")
		assertLoggingError(t, response, http.StatusNotFound, "NOT_FOUND")
	}
	collectionDelete := loggingRequest(api, http.MethodDelete, "/v2/projects/p/sinks", "")
	assertLoggingError(t, collectionDelete, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	for _, destination := range []string{
		"pubsub.googleapis.com/projects/p/topics/",
		"pubsub.googleapis.com/projects//topics/t",
		"pubsub.googleapis.com/projects/p/topics/t/extra",
		"pubsub.googleapis.com/projects/p/not-topics/t",
	} {
		response := loggingRequest(api, http.MethodPost, "/v2/projects/p/sinks",
			fmt.Sprintf(`{"name":"bad","destination":%q}`, destination))
		assertLoggingError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
	}
	for _, method := range []string{http.MethodPatch, http.MethodPut} {
		response := loggingRequest(api, method, "/v2/projects/p/sinks/name", `{}`)
		assertLoggingError(t, response, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
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

func TestSinkDeleteSaveFailureRetainsSinkAndPendingDelivery(t *testing.T) {
	store := &toggleLoggingStore{fail: true}
	api := newAPI(store, &recordingSinkDeliverer{})
	sink := LogSink{Name: "errors", Destination: "file://errors"}
	api.sinks["p:errors"] = sink
	api.pending = []sinkDelivery{{
		ID: "stable", SinkKey: "p:errors", Sink: sink,
		Entry: LogEntry{LogName: "projects/p/logs/app", TextPayload: "retry"},
	}}

	response := loggingRequest(api, http.MethodDelete, "/v2/projects/p/sinks/errors", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete=%d %s", response.Code, response.Body.String())
	}
	if api.sinks["p:errors"].Name != "errors" || len(api.pending) != 1 ||
		api.pending[0].ID != "stable" {
		t.Fatalf("failed delete did not roll back: sinks=%#v pending=%#v", api.sinks, api.pending)
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
	err        error
}

func (d *recordingSinkDeliverer) Deliver(sink LogSink, entry LogEntry) error {
	d.deliveries = append(d.deliveries, sink.Destination+":"+entry.TextPayload)
	return d.err
}

func loggingRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func repeatedJSONStrings(value string, count int) string {
	encoded, _ := json.Marshal(value)
	values := make([]string, count)
	for i := range values {
		values[i] = string(encoded)
	}
	return strings.Join(values, ",")
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
