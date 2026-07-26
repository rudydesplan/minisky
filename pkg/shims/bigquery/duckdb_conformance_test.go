//go:build cgo

package bigquery

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"minisky/pkg/state"
)

func newConformanceBackend(t *testing.T, dbPath string) *DuckDBBackend {
	t.Helper()

	backend := &DuckDBBackend{enabled: true, dbPath: dbPath}
	if err := backend.init(); err != nil {
		t.Fatalf("initialize DuckDB backend: %v", err)
	}
	t.Cleanup(func() {
		if backend.db != nil {
			_ = backend.db.Close()
		}
	})
	return backend
}

func TestDuckDBConformanceSelectOne(t *testing.T) {
	backend := newConformanceBackend(t, filepath.Join(t.TempDir(), "select.duckdb"))

	rows, err := backend.ExecuteQuery("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("execute SELECT 1: %v", err)
	}
	if len(rows) != 1 || rows[0]["one"] != int32(1) {
		t.Fatalf("rows = %#v, want one row containing one=1", rows)
	}
}

func TestDuckDBConformanceDDLInsertAndQuery(t *testing.T) {
	backend := newConformanceBackend(t, filepath.Join(t.TempDir(), "ddl.duckdb"))
	schema := &TableSchema{Fields: []FieldSchema{
		{Name: "id", Type: "INTEGER", Mode: "REQUIRED"},
		{Name: "name", Type: "STRING", Mode: "NULLABLE"},
	}}

	if err := backend.CreateTable("project", "dataset", "users", schema); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := backend.ExecuteQuery("INSERT INTO dataset.users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	rows, err := backend.ExecuteQuery("SELECT name FROM `project.dataset.users` WHERE id = 1")
	if err != nil {
		t.Fatalf("query inserted row: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "alice" {
		t.Fatalf("rows = %#v, want alice", rows)
	}
}

func TestDuckDBConformanceNestedRecordDDL(t *testing.T) {
	backend := newConformanceBackend(t, filepath.Join(t.TempDir(), "nested.duckdb"))
	schema := &TableSchema{Fields: []FieldSchema{{
		Name: "address",
		Type: "RECORD",
		Fields: []FieldSchema{
			{Name: "city", Type: "STRING"},
			{Name: "postcode", Type: "INTEGER"},
		},
	}}}

	if err := backend.CreateTable("project", "dataset", "nested", schema); err != nil {
		t.Fatalf("create nested record table: %v", err)
	}
	rows, err := backend.ExecuteQuery(`DESCRIBE dataset__nested`)
	if err != nil {
		t.Fatalf("inspect nested column: %v", err)
	}
	if len(rows) != 1 || !strings.Contains(rows[0]["column_type"].(string), "STRUCT") {
		t.Fatalf("nested column type = %#v, want STRUCT", rows)
	}
}

func TestDuckDBConformanceStreamingInsert(t *testing.T) {
	backend := newConformanceBackend(t, filepath.Join(t.TempDir(), "stream.duckdb"))
	schema := &TableSchema{Fields: []FieldSchema{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "STRING"},
	}}
	if err := backend.CreateTable("project", "dataset", "events", schema); err != nil {
		t.Fatalf("create stream target: %v", err)
	}

	api := &API{
		backend: backend,
		tables: map[string]*Table{
			tableKey("project", "dataset", "events"): {
				TableReference: TableRef{ProjectId: "project", DatasetId: "dataset", TableId: "events"},
				Schema:         schema,
			},
		},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/bigquery/v2/projects/project/datasets/dataset/tables/events/insertAll",
		strings.NewReader(`{"rows":[{"insertId":"1","json":{"id":7,"name":"streamed"}}]}`),
	)
	response := httptest.NewRecorder()
	api.insertAll(response, request, request.URL.Path)
	if response.Code != http.StatusOK {
		t.Fatalf("insertAll status = %d, body = %s", response.Code, response.Body.String())
	}

	rows, err := backend.ExecuteQuery("SELECT id, name FROM dataset.events")
	if err != nil {
		t.Fatalf("query streamed row: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "streamed" {
		t.Fatalf("rows = %#v, want streamed row", rows)
	}
}

func TestDuckDBConformanceCSVLoad(t *testing.T) {
	backend := newConformanceBackend(t, filepath.Join(t.TempDir(), "load.duckdb"))
	csvPath := filepath.Join(t.TempDir(), "rows.csv")
	if err := os.WriteFile(csvPath, []byte("id,name\n1,loaded\n"), 0644); err != nil {
		t.Fatalf("write CSV fixture: %v", err)
	}

	if err := backend.LoadData("project", "dataset", "loaded", csvPath, "CSV"); err != nil {
		t.Fatalf("load CSV: %v", err)
	}
	rows, err := backend.ExecuteQuery("SELECT id, name FROM dataset.loaded")
	if err != nil {
		t.Fatalf("query loaded data: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"] != "loaded" {
		t.Fatalf("rows = %#v, want loaded row", rows)
	}
}

func TestDuckDBConformancePersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state", "persistent.duckdb")
	first := newConformanceBackend(t, dbPath)
	if _, err := first.ExecuteQuery("CREATE TABLE persisted (value VARCHAR)"); err != nil {
		t.Fatalf("create persistent table: %v", err)
	}
	if _, err := first.ExecuteQuery("INSERT INTO persisted VALUES ('survives')"); err != nil {
		t.Fatalf("insert persistent row: %v", err)
	}
	if err := first.db.Close(); err != nil {
		t.Fatalf("close first backend: %v", err)
	}
	first.db = nil

	second := newConformanceBackend(t, dbPath)
	rows, err := second.ExecuteQuery("SELECT value FROM persisted")
	if err != nil {
		t.Fatalf("query reopened database: %v", err)
	}
	if len(rows) != 1 || rows[0]["value"] != "survives" {
		t.Fatalf("rows = %#v, want persisted row", rows)
	}
}

func TestDuckDBConformanceUnsupportedLoadFormat(t *testing.T) {
	backend := newConformanceBackend(t, filepath.Join(t.TempDir(), "format.duckdb"))

	err := backend.LoadData("project", "dataset", "table", "data.avro", "AVRO")
	if err == nil || !strings.Contains(err.Error(), "unsupported format") {
		t.Fatalf("error = %v, want unsupported format", err)
	}
}

func TestDuckDBConformanceQueryResultsAreJSONSerializable(t *testing.T) {
	backend := newConformanceBackend(t, filepath.Join(t.TempDir(), "json.duckdb"))
	rows, err := backend.ExecuteQuery("SELECT 1 AS value, 'text' AS label")
	if err != nil {
		t.Fatalf("execute query: %v", err)
	}
	if _, err := json.Marshal(rows); err != nil {
		t.Fatalf("marshal query rows: %v", err)
	}
}

func TestDuckDBTerminalJobErrorSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "terminal-job")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.backend = newConformanceBackend(t, filepath.Join(t.TempDir(), "jobs.duckdb"))

	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"invalid-query"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT FROM"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		poll := bigQueryRequest(api, http.MethodGet, path+"/invalid-query", "")
		var job Job
		if err := json.Unmarshal(poll.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status.State == "DONE" {
			if job.Status.ErrorResult == nil {
				t.Fatalf("terminal job has no error: %s", poll.Body.String())
			}
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
	poll := bigQueryRequest(restarted, http.MethodGet, path+"/invalid-query", "")
	var persisted Job
	if err := json.Unmarshal(poll.Body.Bytes(), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.State != "DONE" || persisted.Status.ErrorResult == nil ||
		persisted.Status.ErrorResult.Reason != "backendError" {
		t.Fatalf("persisted terminal job=%#v", persisted)
	}
}

func TestDuckDBQueryResultsSurviveRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "query-results")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.backend = newConformanceBackend(t, filepath.Join(t.TempDir(), "results.duckdb"))

	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"persisted-results"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT 9223372036854775807::BIGINT AS max_int, 9007199254740993::BIGINT AS large_int, CAST('12345678901234567890' || '.' || '123456789' AS DECIMAL(38,9)) AS decimal_value, CAST('123456789012345678' || '.' || '12345678901234567890' AS DECIMAL(38,20)) AS high_scale_decimal"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	job := waitForBigQueryJob(t, api, path+"/persisted-results")
	if job.Status.ErrorResult != nil {
		t.Fatalf("query failed: %#v", job.Status.ErrorResult)
	}
	beforeRestart := bigQueryRequest(api, http.MethodGet, path+"/persisted-results/results", "")
	if beforeRestart.Code != http.StatusOK {
		t.Fatalf("pre-restart results status=%d body=%s", beforeRestart.Code, beforeRestart.Body.String())
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	results := bigQueryRequest(restarted, http.MethodGet, path+"/persisted-results/results", "")
	if results.Code != http.StatusOK {
		t.Fatalf("results status=%d body=%s", results.Code, results.Body.String())
	}
	var response struct {
		JobComplete bool `json:"jobComplete"`
		TotalRows   string
		Schema      struct {
			Fields []FieldSchema `json:"fields"`
		} `json:"schema"`
		Rows []QueryResultRow `json:"rows"`
	}
	if err := json.Unmarshal(results.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.JobComplete || response.TotalRows != "1" || len(response.Schema.Fields) != 4 ||
		len(response.Rows) != 1 || len(response.Rows[0].F) != 4 {
		t.Fatalf("restarted results=%s", results.Body.String())
	}
	types := map[string]string{}
	for _, field := range response.Schema.Fields {
		types[field.Name] = field.Type
	}
	if types["max_int"] != "INTEGER" || types["large_int"] != "INTEGER" ||
		types["decimal_value"] != "NUMERIC" || types["high_scale_decimal"] != "BIGNUMERIC" {
		t.Fatalf("restarted schema types=%#v body=%s", types, results.Body.String())
	}
	values := map[string]string{}
	for index, field := range response.Schema.Fields {
		values[field.Name] = fmt.Sprint(response.Rows[0].F[index].V)
	}
	if values["max_int"] != "9223372036854775807" ||
		values["large_int"] != "9007199254740993" ||
		values["decimal_value"] != "12345678901234567890.123456789" ||
		values["high_scale_decimal"] != "123456789012345678.12345678901234567890" {
		t.Fatalf("restarted values=%#v body=%s", values, results.Body.String())
	}
	if beforeRestart.Body.String() != results.Body.String() {
		t.Fatalf("query result wire bytes changed across restart:\nbefore=%s\nafter=%s",
			beforeRestart.Body.String(), results.Body.String())
	}
}

func TestDuckDBTypedNestedResultsSurviveRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "typed-nested-results")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.backend = newConformanceBackend(t, filepath.Join(t.TempDir(), "typed-nested.duckdb"))

	query := "SELECT true AS bool_value, 42::BIGINT AS integer_value, " +
		"(5::DOUBLE / 4) AS float_value, " +
		"CAST('12345678901234567890' || '.' || '123456789' AS DECIMAL(38,9)) AS numeric_value, " +
		"from_hex('000102ff') AS bytes_value, DATE '2026-07-26' AS date_value, " +
		"TIME '12:34:56' AS time_value, TIMESTAMP '2026-07-26 12:34:56' AS datetime_value, " +
		"[7::BIGINT, 8::BIGINT] AS repeated_value, " +
		"struct_pack(name := 'Ada', active := true) AS record_value, NULL::VARCHAR AS null_value"
	path := "/bigquery/v2/projects/demo/jobs"
	body, err := json.Marshal(map[string]interface{}{
		"jobReference": map[string]interface{}{"jobId": "typed-nested"},
		"configuration": map[string]interface{}{
			"jobType": "QUERY",
			"query":   map[string]interface{}{"query": query},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	create := bigQueryRequest(api, http.MethodPost, path, string(body))
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	job := waitForBigQueryJob(t, api, path+"/typed-nested")
	if job.Status.ErrorResult != nil {
		t.Fatalf("typed nested query failed: %#v", job.Status.ErrorResult)
	}
	before := bigQueryRequest(api, http.MethodGet, path+"/typed-nested/results", "")
	if before.Code != http.StatusOK {
		t.Fatalf("pre-restart status=%d body=%s", before.Code, before.Body.String())
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	after := bigQueryRequest(restarted, http.MethodGet, path+"/typed-nested/results", "")
	if after.Code != http.StatusOK {
		t.Fatalf("post-restart status=%d body=%s", after.Code, after.Body.String())
	}
	if before.Body.String() != after.Body.String() {
		t.Fatalf("typed nested wire changed across restart:\nbefore=%s\nafter=%s",
			before.Body.String(), after.Body.String())
	}
	var response struct {
		Schema TableSchema      `json:"schema"`
		Rows   []QueryResultRow `json:"rows"`
	}
	if err := json.Unmarshal(after.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	types := map[string]FieldSchema{}
	for _, field := range response.Schema.Fields {
		types[field.Name] = field
	}
	if types["bool_value"].Type != "BOOLEAN" ||
		types["integer_value"].Type != "INTEGER" ||
		types["float_value"].Type != "FLOAT" ||
		types["numeric_value"].Type != "NUMERIC" ||
		types["bytes_value"].Type != "BYTES" ||
		types["date_value"].Type != "DATE" ||
		types["time_value"].Type != "TIME" ||
		types["datetime_value"].Type != "DATETIME" ||
		types["repeated_value"].Mode != "REPEATED" ||
		types["record_value"].Type != "RECORD" || len(types["record_value"].Fields) != 2 {
		t.Fatalf("typed nested schema=%#v body=%s", types, after.Body.String())
	}
}

func TestDuckDBRejectsUnsupportedNestedArrays(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.backend = newConformanceBackend(t, filepath.Join(t.TempDir(), "nested-arrays.duckdb"))
	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"nested-arrays"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT [[1, 2], [3, 4]] AS values"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	job := waitForBigQueryJob(t, api, path+"/nested-arrays")
	if job.Status.ErrorResult == nil ||
		!strings.Contains(job.Status.ErrorResult.Message, "nested arrays are not supported") {
		t.Fatalf("nested arrays were not explicitly rejected: %#v", job)
	}
}

func TestDuckDBDuplicateAliasesSurviveRestartPositionally(t *testing.T) {
	store, err := state.New(t.TempDir(), "duplicate-aliases")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.backend = newConformanceBackend(t, filepath.Join(t.TempDir(), "duplicate-aliases.duckdb"))
	path := "/bigquery/v2/projects/demo/jobs"
	create := bigQueryRequest(api, http.MethodPost, path,
		`{"jobReference":{"jobId":"duplicates"},"configuration":{"jobType":"QUERY","query":{"query":"SELECT 1::BIGINT AS duplicate, 2::BIGINT AS duplicate"}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	job := waitForBigQueryJob(t, api, path+"/duplicates")
	if job.Status.ErrorResult != nil {
		t.Fatalf("duplicate alias query failed: %#v", job.Status.ErrorResult)
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	response := bigQueryRequest(restarted, http.MethodGet, path+"/duplicates/results", "")
	var result struct {
		Schema TableSchema      `json:"schema"`
		Rows   []QueryResultRow `json:"rows"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Schema.Fields) != 2 || result.Schema.Fields[0].Name != "duplicate" ||
		result.Schema.Fields[1].Name != "duplicate" || len(result.Rows) != 1 ||
		len(result.Rows[0].F) != 2 || result.Rows[0].F[0].V != "1" || result.Rows[0].F[1].V != "2" {
		t.Fatalf("duplicate aliases were not preserved: %s", response.Body.String())
	}
}

func TestDuckDBDecimalTypeClassification(t *testing.T) {
	tests := []struct {
		typeName string
		want     string
		wantErr  bool
	}{
		{typeName: "DECIMAL(38,9)", want: "NUMERIC"},
		{typeName: "DECIMAL(38,20)", want: "BIGNUMERIC"},
		{typeName: "DECIMAL(76,38)", want: "BIGNUMERIC"},
		{typeName: "DECIMAL(77,38)", wantErr: true},
		{typeName: "DECIMAL(39,0)", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.typeName, func(t *testing.T) {
			got, err := bigQueryDecimalType(test.typeName)
			if test.wantErr {
				if err == nil {
					t.Fatalf("bigQueryDecimalType(%q)=%q, want error", test.typeName, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("bigQueryDecimalType(%q)=(%q, %v), want %q", test.typeName, got, err, test.want)
			}
		})
	}
}
