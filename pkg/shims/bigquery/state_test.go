package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type failingBigQueryStore struct {
	mu       sync.Mutex
	metadata bigQueryMetadata
	failSave bool
	saveCall int
	failFrom int
}

func (s *failingBigQueryStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metadata.Datasets == nil && s.metadata.Tables == nil && s.metadata.Jobs == nil {
		return state.ErrNotFound
	}
	payload, err := json.Marshal(s.metadata)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, target)
}

func (s *failingBigQueryStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCall++
	if s.failSave || s.failFrom > 0 && s.saveCall >= s.failFrom {
		return errors.New("injected save failure")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, &s.metadata)
}

func (s *failingBigQueryStore) saves() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCall
}

func bigQueryRequest(api *API, method, path, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
	return response
}

func TestMetadataRehydratesAfterRestart(t *testing.T) {
	t.Parallel()

	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}

	datasetBody := `{"datasetReference":{"datasetId":"analytics"},"description":"restart test"}`
	request := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/demo/datasets", strings.NewReader(datasetBody))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create dataset status = %d, body = %s", response.Code, response.Body.String())
	}

	tableBody := `{"tableReference":{"tableId":"events"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"}]}}`
	request = httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/demo/datasets/analytics/tables", strings.NewReader(tableBody))
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create table status = %d, body = %s", response.Code, response.Body.String())
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/demo/datasets/analytics/tables/events", nil)
	response = httptest.NewRecorder()
	restarted.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("rehydrated table status = %d, body = %s", response.Code, response.Body.String())
	}
	var table Table
	if err := json.NewDecoder(response.Body).Decode(&table); err != nil {
		t.Fatal(err)
	}
	if table.TableReference.TableId != "events" || table.Schema == nil || len(table.Schema.Fields) != 1 {
		t.Fatalf("rehydrated table = %#v", table)
	}
}

func TestDatasetPatchAndPutPersistOmissionSemantics(t *testing.T) {
	t.Parallel()

	store, err := state.New(t.TempDir(), "updates")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	path := "/bigquery/v2/projects/demo/datasets/analytics"
	create := bigQueryRequest(api, http.MethodPost, "/bigquery/v2/projects/demo/datasets",
		`{"datasetReference":{"datasetId":"analytics"},"description":"original","labels":{"env":"test","team":"data"}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	patch := bigQueryRequest(api, http.MethodPatch, path, `{"labels":{"env":"prod"}}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status=%d body=%s", patch.Code, patch.Body.String())
	}
	var patched Dataset
	if err := json.Unmarshal(patch.Body.Bytes(), &patched); err != nil {
		t.Fatal(err)
	}
	if patched.Description != "original" || !reflect.DeepEqual(patched.Labels, map[string]string{"env": "prod"}) {
		t.Fatalf("patched dataset=%#v", patched)
	}

	put := bigQueryRequest(api, http.MethodPut, path, `{"description":"replacement"}`)
	if put.Code != http.StatusOK {
		t.Fatalf("put status=%d body=%s", put.Code, put.Body.String())
	}
	var replaced Dataset
	if err := json.Unmarshal(put.Body.Bytes(), &replaced); err != nil {
		t.Fatal(err)
	}
	if replaced.Description != "replacement" || replaced.Labels != nil {
		t.Fatalf("replaced dataset=%#v", replaced)
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	got := bigQueryRequest(restarted, http.MethodGet, path, "")
	if got.Code != http.StatusOK {
		t.Fatalf("restart get status=%d body=%s", got.Code, got.Body.String())
	}
	var persisted Dataset
	if err := json.Unmarshal(got.Body.Bytes(), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Description != "replacement" || persisted.Labels != nil ||
		persisted.CreationTime != replaced.CreationTime || persisted.Location != "US" {
		t.Fatalf("persisted dataset=%#v", persisted)
	}

	unsupported := bigQueryRequest(restarted, http.MethodPatch, path, `{"friendlyName":"unsupported"}`)
	assertBigQueryError(t, unsupported, http.StatusNotImplemented, "UNIMPLEMENTED")
	immutable := bigQueryRequest(restarted, http.MethodPatch, path, `{"location":"EU"}`)
	assertBigQueryError(t, immutable, http.StatusBadRequest, "INVALID_ARGUMENT")
}

func TestDuplicateDatasetAndTableReturnAlreadyExistsWithoutOverwrite(t *testing.T) {
	t.Parallel()

	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	datasetCollection := "/bigquery/v2/projects/demo/datasets"
	firstDataset := bigQueryRequest(api, http.MethodPost, datasetCollection,
		`{"datasetReference":{"datasetId":"analytics"},"description":"first"}`)
	if firstDataset.Code != http.StatusOK {
		t.Fatalf("first dataset status=%d body=%s", firstDataset.Code, firstDataset.Body.String())
	}
	duplicateDataset := bigQueryRequest(api, http.MethodPost, datasetCollection,
		`{"datasetReference":{"datasetId":"analytics"},"description":"replacement"}`)
	assertBigQueryError(t, duplicateDataset, http.StatusConflict, "ALREADY_EXISTS")

	tableCollection := datasetCollection + "/analytics/tables"
	firstTable := bigQueryRequest(api, http.MethodPost, tableCollection,
		`{"tableReference":{"tableId":"events"},"description":"first"}`)
	if firstTable.Code != http.StatusOK {
		t.Fatalf("first table status=%d body=%s", firstTable.Code, firstTable.Body.String())
	}
	duplicateTable := bigQueryRequest(api, http.MethodPost, tableCollection,
		`{"tableReference":{"tableId":"events"},"description":"replacement"}`)
	assertBigQueryError(t, duplicateTable, http.StatusConflict, "ALREADY_EXISTS")

	gotDataset := bigQueryRequest(api, http.MethodGet, datasetCollection+"/analytics", "")
	var dataset Dataset
	if err := json.Unmarshal(gotDataset.Body.Bytes(), &dataset); err != nil {
		t.Fatal(err)
	}
	if dataset.Description != "first" {
		t.Fatalf("duplicate overwrote dataset: %#v", dataset)
	}
	gotTable := bigQueryRequest(api, http.MethodGet, tableCollection+"/events", "")
	var table Table
	if err := json.Unmarshal(gotTable.Body.Bytes(), &table); err != nil {
		t.Fatal(err)
	}
	if table.Description != "first" {
		t.Fatalf("duplicate overwrote table: %#v", table)
	}
}

func TestMutationSaveFailuresRollbackMetadata(t *testing.T) {
	store := &failingBigQueryStore{}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	datasetCollection := "/bigquery/v2/projects/demo/datasets"
	if response := bigQueryRequest(api, http.MethodPost, datasetCollection,
		`{"datasetReference":{"datasetId":"analytics"},"description":"original"}`); response.Code != http.StatusOK {
		t.Fatalf("seed dataset status=%d body=%s", response.Code, response.Body.String())
	}
	tableCollection := datasetCollection + "/analytics/tables"
	if response := bigQueryRequest(api, http.MethodPost, tableCollection,
		`{"tableReference":{"tableId":"events"},"description":"original"}`); response.Code != http.StatusOK {
		t.Fatalf("seed table status=%d body=%s", response.Code, response.Body.String())
	}

	store.mu.Lock()
	store.failSave = true
	store.mu.Unlock()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"create dataset", http.MethodPost, datasetCollection, `{"datasetReference":{"datasetId":"failed"}}`},
		{"patch dataset", http.MethodPatch, datasetCollection + "/analytics", `{"description":"changed"}`},
		{"delete dataset", http.MethodDelete, datasetCollection + "/analytics", ""},
		{"create table", http.MethodPost, tableCollection, `{"tableReference":{"tableId":"failed"}}`},
		{"delete table", http.MethodDelete, tableCollection + "/events", ""},
		{"create job", http.MethodPost, "/bigquery/v2/projects/demo/jobs",
			`{"jobReference":{"jobId":"failed"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT 1"}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := bigQueryRequest(api, test.method, test.path, test.body)
			assertBigQueryError(t, response, http.StatusInternalServerError, "INTERNAL")
		})
	}

	if response := bigQueryRequest(api, http.MethodGet, datasetCollection+"/analytics", ""); response.Code != http.StatusOK {
		t.Fatalf("dataset was not rolled back: %d %s", response.Code, response.Body.String())
	} else {
		var dataset Dataset
		if err := json.Unmarshal(response.Body.Bytes(), &dataset); err != nil {
			t.Fatal(err)
		}
		if dataset.Description != "original" {
			t.Fatalf("dataset after rollback=%#v", dataset)
		}
	}
	if response := bigQueryRequest(api, http.MethodGet, datasetCollection+"/failed", ""); response.Code != http.StatusNotFound {
		t.Fatalf("failed dataset remained in memory: %d %s", response.Code, response.Body.String())
	}
	if response := bigQueryRequest(api, http.MethodGet, tableCollection+"/events", ""); response.Code != http.StatusOK {
		t.Fatalf("table was not rolled back: %d %s", response.Code, response.Body.String())
	}
	if response := bigQueryRequest(api, http.MethodGet, tableCollection+"/failed", ""); response.Code != http.StatusNotFound {
		t.Fatalf("failed table remained in memory: %d %s", response.Code, response.Body.String())
	}
	if response := bigQueryRequest(api, http.MethodGet, "/bigquery/v2/projects/demo/jobs/failed", ""); response.Code != http.StatusNotFound {
		t.Fatalf("failed job remained in memory: %d %s", response.Code, response.Body.String())
	}
}

func TestJobMetadataSurvivesRestartAndPolling(t *testing.T) {
	t.Parallel()

	store, err := state.New(t.TempDir(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"query-job","location":"EU"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT 1"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		poll := bigQueryRequest(api, http.MethodGet, path+"/query-job", "")
		var job Job
		if err := json.Unmarshal(poll.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status.State == "DONE" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete: %s", poll.Body.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	poll := bigQueryRequest(restarted, http.MethodGet, path+"/query-job", "")
	if poll.Code != http.StatusOK {
		t.Fatalf("restart poll status=%d body=%s", poll.Code, poll.Body.String())
	}
	var persisted Job
	if err := json.Unmarshal(poll.Body.Bytes(), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.State != "DONE" || persisted.JobReference.Location != "EU" ||
		persisted.Configuration.Query == nil || persisted.Configuration.Query.Query != "SELECT 1" {
		t.Fatalf("persisted job=%#v", persisted)
	}

	api.mutationMu.Lock()
	api.mu.Lock()
	persisted.Status.ErrorResult = &ErrorProto{Reason: "backendError", Message: "terminal failure"}
	api.jobs["demo:query-job"] = cloneJob(&persisted)
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		api.mutationMu.Unlock()
		t.Fatal(err)
	}
	api.mutationMu.Unlock()

	restarted, err = NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	poll = bigQueryRequest(restarted, http.MethodGet, path+"/query-job", "")
	if err := json.Unmarshal(poll.Body.Bytes(), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.ErrorResult == nil || persisted.Status.ErrorResult.Message != "terminal failure" {
		t.Fatalf("terminal error was not persisted: %#v", persisted.Status)
	}
}

func TestRunningJobIsReconciledAsInterruptedOnRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "interrupted-job")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	releaseExecution := make(chan struct{})
	api.executeJob = func(JobConfig) ([]map[string]interface{}, error) {
		<-releaseExecution
		return nil, nil
	}
	t.Cleanup(func() {
		api.store = nil
		close(releaseExecution)
	})

	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"interrupted"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT 1"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	poll := bigQueryRequest(restarted, http.MethodGet, path+"/interrupted", "")
	if poll.Code != http.StatusOK {
		t.Fatalf("poll status=%d body=%s", poll.Code, poll.Body.String())
	}
	var job Job
	if err := json.Unmarshal(poll.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.Status.State != "DONE" || job.Status.ErrorResult == nil ||
		job.Status.ErrorResult.Reason != interruptedJobReason ||
		!strings.Contains(job.Status.ErrorResult.Message, "restart") {
		t.Fatalf("reconciled job=%#v", job)
	}

	restartedAgain, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	poll = bigQueryRequest(restartedAgain, http.MethodGet, path+"/interrupted", "")
	if err := json.Unmarshal(poll.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.Status.State != "DONE" || job.Status.ErrorResult == nil ||
		job.Status.ErrorResult.Reason != interruptedJobReason {
		t.Fatalf("durable reconciliation=%#v", job)
	}
}

func TestTerminalJobSaveFailureRetriesThenFailsClosed(t *testing.T) {
	store := &failingBigQueryStore{failFrom: 2}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.executeJob = func(JobConfig) ([]map[string]interface{}, error) {
		return []map[string]interface{}{{"value": "complete"}}, nil
	}

	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"save-failure"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT 1"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	deadline := time.Now().Add(3 * time.Second)
	for api.persistenceFailure() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if degraded := api.persistenceFailure(); degraded == nil ||
		!strings.Contains(degraded.Error(), "injected save failure") {
		t.Fatalf("persistence diagnostic=%v", degraded)
	}
	if got := store.saves(); got != 1+terminalJobPersistenceAttempts {
		t.Fatalf("save calls=%d want=%d", got, 1+terminalJobPersistenceAttempts)
	}

	api.mu.RLock()
	completed := cloneJob(api.jobs["demo:save-failure"])
	api.mu.RUnlock()
	if completed == nil || completed.Status.State != "DONE" {
		t.Fatalf("completion rolled back to nonterminal state: %#v", completed)
	}
	poll := bigQueryRequest(api, http.MethodGet, path+"/save-failure", "")
	assertBigQueryError(t, poll, http.StatusServiceUnavailable, "UNAVAILABLE")

	store.mu.Lock()
	store.failFrom = 0
	store.mu.Unlock()
	restarted, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	poll = bigQueryRequest(restarted, http.MethodGet, path+"/save-failure", "")
	var interrupted Job
	if err := json.Unmarshal(poll.Body.Bytes(), &interrupted); err != nil {
		t.Fatal(err)
	}
	if interrupted.Status.State != "DONE" || interrupted.Status.ErrorResult == nil ||
		interrupted.Status.ErrorResult.Reason != interruptedJobReason {
		t.Fatalf("restart did not reconcile durable RUNNING job: %#v", interrupted)
	}
}

func TestDegradedPersistenceReturnsRetryableUnavailable(t *testing.T) {
	api := newAPI(nil, nil)
	api.persistenceErr = errors.New("injected degraded persistence")
	response := bigQueryRequest(api, http.MethodGet, "/bigquery/v2/projects/demo/jobs/missing", "")
	assertBigQueryError(t, response, http.StatusServiceUnavailable, "UNAVAILABLE")

	server := httptest.NewServer(api)
	defer server.Close()
	client, err := bigqueryv2.NewService(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL+"/"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Jobs.Get("demo", "missing").Do()
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("generated Google client error=%T %v, want *googleapi.Error", err, err)
	}
	if apiErr.Code != http.StatusServiceUnavailable ||
		!strings.Contains(apiErr.Body, `"status":"UNAVAILABLE"`) {
		t.Fatalf("generated Google client error=%#v", apiErr)
	}
	if apiErr.Code < 500 {
		t.Fatalf("Google client error code %d is not retryable", apiErr.Code)
	}
}

func TestQueryResultPrecisionSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "precise-results")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.executeJob = func(JobConfig) ([]map[string]interface{}, error) {
		return []map[string]interface{}{{
			"max_int":       int64(9223372036854775807),
			"large_int":     int64(9007199254740993),
			"decimal_value": json.Number("12345678901234567890.123456789"),
		}}, nil
	}
	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"precise"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT values"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	waitForBigQueryJob(t, api, path+"/precise")
	before := bigQueryRequest(api, http.MethodGet, path+"/precise/results", "")

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	after := bigQueryRequest(restarted, http.MethodGet, path+"/precise/results", "")
	if before.Body.String() != after.Body.String() {
		t.Fatalf("wire results changed across restart:\nbefore=%s\nafter=%s",
			before.Body.String(), after.Body.String())
	}
	for _, value := range []string{
		`"v":"9223372036854775807"`,
		`"v":"9007199254740993"`,
		`"v":"12345678901234567890.123456789"`,
	} {
		if !strings.Contains(after.Body.String(), value) {
			t.Fatalf("missing precise cell %s in %s", value, after.Body.String())
		}
	}
}

func TestLegacyMapBackedQueryResultsMigrateToPositionalRows(t *testing.T) {
	store, err := state.New(t.TempDir(), "legacy-query-results")
	if err != nil {
		t.Fatal(err)
	}
	legacy := json.RawMessage(`{
		"datasets": {},
		"tables": {},
		"jobs": {
			"demo:legacy": {
				"kind": "bigquery#job",
				"id": "demo:legacy",
				"jobReference": {"projectId": "demo", "jobId": "legacy"},
				"status": {"state": "DONE"},
				"statistics": {"creationTime": "1", "totalBytesProcessed": "0"},
				"configuration": {"jobType": "QUERY", "query": {"query": "SELECT 1"}}
			}
		},
		"jobResults": {
			"demo:legacy": {
				"rows": [{"value": {"value": "9007199254740993"}}],
				"schema": {"fields": [{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]}
			}
		}
	}`)
	if err := store.Save(bigQueryStateEntry, legacy); err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	response := bigQueryRequest(api, http.MethodGet, "/bigquery/v2/projects/demo/queries/legacy", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"v":"9007199254740993"`) {
		t.Fatalf("legacy query result status=%d body=%s", response.Code, response.Body.String())
	}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	var migrated json.RawMessage
	if err := store.Load(bigQueryStateEntry, &migrated); err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, migrated); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(compact.Bytes(), []byte(`"rows":[[`)) {
		t.Fatalf("legacy result was not migrated to positional rows: %s", migrated)
	}
}

func TestGeneratedClientReadsTypedNestedResultsAfterRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "typed-results")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.executeJobWithSchema = func(JobConfig) ([]QueryValues, *TableSchema, error) {
		return []QueryValues{{
				true,
				int64(9223372036854775807),
				1.25,
				json.Number("12345678901234567890.123456789"),
				json.Number("123456789012345678.12345678901234567890"),
				[]byte{0, 1, 2, 255},
				time.Date(2026, time.July, 26, 0, 0, 0, 0, time.UTC),
				time.Date(1, time.January, 1, 12, 34, 56, 123456000, time.UTC),
				time.Date(2026, time.July, 26, 12, 34, 56, 123456000, time.UTC),
				time.Unix(-1, 500_000_000).UTC(),
				nil,
				[]interface{}{int64(7), int64(8)},
				map[string]interface{}{
					"name":   "Ada",
					"active": true,
				},
			}}, &TableSchema{Fields: []FieldSchema{
				{Name: "bool_value", Type: "BOOLEAN", Mode: "NULLABLE"},
				{Name: "integer_value", Type: "INTEGER", Mode: "REQUIRED"},
				{Name: "float_value", Type: "FLOAT", Mode: "NULLABLE"},
				{Name: "numeric_value", Type: "NUMERIC", Mode: "NULLABLE"},
				{Name: "bignumeric_value", Type: "BIGNUMERIC", Mode: "NULLABLE"},
				{Name: "bytes_value", Type: "BYTES", Mode: "NULLABLE"},
				{Name: "date_value", Type: "DATE", Mode: "NULLABLE"},
				{Name: "time_value", Type: "TIME", Mode: "NULLABLE"},
				{Name: "datetime_value", Type: "DATETIME", Mode: "NULLABLE"},
				{Name: "timestamp_value", Type: "TIMESTAMP", Mode: "NULLABLE"},
				{Name: "null_value", Type: "STRING", Mode: "NULLABLE"},
				{Name: "repeated_value", Type: "INTEGER", Mode: "REPEATED"},
				{Name: "record_value", Type: "RECORD", Mode: "NULLABLE", Fields: []FieldSchema{
					{Name: "name", Type: "STRING", Mode: "NULLABLE"},
					{Name: "active", Type: "BOOLEAN", Mode: "NULLABLE"},
				}},
			}}, nil
	}
	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"typed"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT typed"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	waitForBigQueryJob(t, api, path+"/typed")

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(restarted)
	defer server.Close()
	client, err := bigqueryv2.NewService(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL+"/"),
	)
	if err != nil {
		t.Fatal(err)
	}
	results, err := client.Jobs.GetQueryResults("demo", "typed").Do()
	if err != nil {
		t.Fatal(err)
	}
	if results.Schema == nil || len(results.Schema.Fields) != 13 || len(results.Rows) != 1 {
		t.Fatalf("generated result=%#v", results)
	}
	fields := map[string]*bigqueryv2.TableFieldSchema{}
	values := map[string]interface{}{}
	for index, field := range results.Schema.Fields {
		fields[field.Name] = field
		values[field.Name] = results.Rows[0].F[index].V
	}
	if fields["integer_value"].Type != "INTEGER" || fields["integer_value"].Mode != "REQUIRED" ||
		fields["repeated_value"].Mode != "REPEATED" ||
		fields["record_value"].Type != "RECORD" || len(fields["record_value"].Fields) != 2 {
		t.Fatalf("generated schema=%#v", results.Schema.Fields)
	}
	if values["bool_value"] != "true" ||
		values["integer_value"] != "9223372036854775807" ||
		values["float_value"] != "1.25" ||
		values["numeric_value"] != "12345678901234567890.123456789" ||
		values["bignumeric_value"] != "123456789012345678.12345678901234567890" ||
		values["bytes_value"] != "AAEC/w==" ||
		values["date_value"] != "2026-07-26" ||
		values["time_value"] != "12:34:56.123456" ||
		values["datetime_value"] != "2026-07-26T12:34:56.123456" ||
		values["timestamp_value"] != "-0.500000" ||
		values["null_value"] != nil {
		t.Fatalf("generated scalar values=%#v", values)
	}
	repeated, ok := values["repeated_value"].([]interface{})
	if !ok || len(repeated) != 2 {
		t.Fatalf("generated repeated value=%#v", values["repeated_value"])
	}
	firstRepeated, ok := repeated[0].(map[string]interface{})
	if !ok || firstRepeated["v"] != "7" {
		t.Fatalf("generated repeated cell=%#v", repeated[0])
	}
	record, ok := values["record_value"].(map[string]interface{})
	if !ok {
		t.Fatalf("generated record value=%#v", values["record_value"])
	}
	recordFields, ok := record["f"].([]interface{})
	if !ok || len(recordFields) != 2 {
		t.Fatalf("generated record fields=%#v", record)
	}
	firstRecord, ok := recordFields[0].(map[string]interface{})
	secondRecord, secondOK := recordFields[1].(map[string]interface{})
	if !ok || !secondOK || firstRecord["v"] != "Ada" || secondRecord["v"] != "true" {
		t.Fatalf("generated nested cells=%#v", recordFields)
	}
}

func TestOversizedQueryResultsFailWithinPersistenceBounds(t *testing.T) {
	rowSchema := &TableSchema{Fields: []FieldSchema{{Name: "value", Type: "STRING", Mode: "NULLABLE"}}}
	atLimit := make([]QueryValues, maxPersistedQueryResultRows)
	for index := range atLimit {
		atLimit[index] = QueryValues{nil}
	}
	if _, _, resultErr := boundedQueryResults(atLimit, rowSchema); resultErr != nil {
		t.Fatalf("row limit should be inclusive: %#v", resultErr)
	}
	overLimit := append(atLimit, QueryValues{nil})
	if _, _, resultErr := boundedQueryResults(overLimit, rowSchema); resultErr == nil ||
		resultErr.Reason != queryResultsTooLargeReason {
		t.Fatalf("row limit error=%#v", resultErr)
	}

	longName := strings.Repeat("n", maxPersistedQueryResultBytes/2+1)
	nestedSchema := &TableSchema{Fields: []FieldSchema{{
		Name: "record", Type: "RECORD", Mode: "NULLABLE",
		Fields: []FieldSchema{{Name: longName, Type: "STRING", Mode: "NULLABLE"}},
	}}}
	nestedRows := []QueryValues{{map[string]interface{}{longName: "value"}}}
	if _, _, resultErr := boundedQueryResults(nestedRows, nestedSchema); resultErr == nil ||
		resultErr.Reason != queryResultsTooLargeReason {
		t.Fatalf("long nested field persistence limit error=%#v", resultErr)
	}

	store, err := state.New(t.TempDir(), "bounded-results")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.executeJob = func(JobConfig) ([]map[string]interface{}, error) {
		return []map[string]interface{}{{"value": strings.Repeat("x", maxPersistedQueryResultBytes)}}, nil
	}
	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"oversized"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT value"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	job := waitForBigQueryJob(t, api, path+"/oversized")
	if job.Status.ErrorResult == nil || job.Status.ErrorResult.Reason != queryResultsTooLargeReason ||
		job.RawRows != nil || job.Schema != nil {
		t.Fatalf("oversized result job=%#v", job)
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	job = waitForBigQueryJob(t, restarted, path+"/oversized")
	if job.Status.ErrorResult == nil || job.Status.ErrorResult.Reason != queryResultsTooLargeReason {
		t.Fatalf("persisted oversized result job=%#v", job)
	}
}

func TestConcurrentReadsReturnStableSnapshots(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	collection := "/bigquery/v2/projects/demo/datasets"
	if response := bigQueryRequest(api, http.MethodPost, collection,
		`{"datasetReference":{"datasetId":"analytics"},"labels":{"version":"0"}}`); response.Code != http.StatusOK {
		t.Fatalf("seed status=%d body=%s", response.Code, response.Body.String())
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(index int) {
			defer wg.Done()
			for iteration := 0; iteration < 50; iteration++ {
				response := bigQueryRequest(api, http.MethodPatch, collection+"/analytics",
					`{"labels":{"version":"`+fmt.Sprint(index, "-", iteration)+`"}}`)
				if response.Code != http.StatusOK {
					t.Errorf("patch status=%d body=%s", response.Code, response.Body.String())
					return
				}
			}
		}(i)
		go func() {
			defer wg.Done()
			for iteration := 0; iteration < 50; iteration++ {
				response := bigQueryRequest(api, http.MethodGet, collection+"/analytics", "")
				if response.Code != http.StatusOK {
					t.Errorf("get status=%d body=%s", response.Code, response.Body.String())
					return
				}
				var dataset Dataset
				if err := json.Unmarshal(response.Body.Bytes(), &dataset); err != nil {
					t.Errorf("decode snapshot: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func assertBigQueryError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status=%d want=%d body=%s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != code || envelope.Error.Status != status {
		t.Fatalf("error=%#v body=%s", envelope.Error, response.Body.String())
	}
}

func waitForBigQueryJob(t *testing.T, api *API, path string) Job {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		response := bigQueryRequest(api, http.MethodGet, path, "")
		if response.Code != http.StatusOK {
			t.Fatalf("poll status=%d body=%s", response.Code, response.Body.String())
		}
		var job Job
		if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status.State == "DONE" {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not complete: %s", response.Body.String())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSameNamedDatasetsAreIsolatedByProject(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"datasetReference":{"datasetId":"shared-name"}}`
	for _, project := range []string{"project-a", "project-b"} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
			"/bigquery/v2/projects/"+project+"/datasets", strings.NewReader(body)))
		if response.Code != http.StatusOK {
			t.Fatalf("create %s: %d %s", project, response.Code, response.Body.String())
		}
	}
	deleteResponse := httptest.NewRecorder()
	api.ServeHTTP(deleteResponse, httptest.NewRequest(http.MethodDelete,
		"/bigquery/v2/projects/project-a/datasets/shared-name", nil))
	if deleteResponse.Code != http.StatusNoContent {
		t.Fatalf("delete project-a: %d", deleteResponse.Code)
	}
	getResponse := httptest.NewRecorder()
	api.ServeHTTP(getResponse, httptest.NewRequest(http.MethodGet,
		"/bigquery/v2/projects/project-b/datasets/shared-name", nil))
	if getResponse.Code != http.StatusOK {
		t.Fatalf("project-b resource collided: %d %s", getResponse.Code, getResponse.Body.String())
	}
}
