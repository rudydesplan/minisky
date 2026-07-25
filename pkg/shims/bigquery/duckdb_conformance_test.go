//go:build cgo

package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
