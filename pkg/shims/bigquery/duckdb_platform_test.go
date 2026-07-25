//go:build !cgo

package bigquery

import (
	"strings"
	"testing"
)

func TestDuckDBUnavailableWithoutCGO(t *testing.T) {
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_BQ_BACKEND", "")

	backend := NewDuckDBBackend()
	if backend.Enabled() {
		t.Fatal("DuckDB backend is enabled without CGO")
	}
	if state := backend.Status(); state.Backend != "simulation" || state.Diagnostic == "" {
		t.Fatalf("backend state = %#v, want diagnosed simulation fallback", state)
	}

	if err := backend.SetEnabled(true); err == nil || !strings.Contains(err.Error(), "requires CGO") {
		t.Fatalf("SetEnabled error = %v, want CGO requirement", err)
	}
	if _, err := backend.ExecuteQuery("SELECT 1"); err == nil || !strings.Contains(err.Error(), "CGO_ENABLED=1") {
		t.Fatalf("ExecuteQuery error = %v, want CGO requirement", err)
	}
	if err := backend.LoadData("project", "dataset", "table", "rows.csv", "CSV"); err == nil || !strings.Contains(err.Error(), "CGO_ENABLED=1") {
		t.Fatalf("LoadData error = %v, want CGO requirement", err)
	}
}

func TestDuckDBCreateTableRejectsMissingCGO(t *testing.T) {
	backend := NewDuckDBBackend()
	err := backend.CreateTable("project", "dataset", "table", &TableSchema{})
	if err == nil || !strings.Contains(err.Error(), "CGO_ENABLED=1") {
		t.Fatalf("CreateTable error = %v, want CGO requirement", err)
	}
}
