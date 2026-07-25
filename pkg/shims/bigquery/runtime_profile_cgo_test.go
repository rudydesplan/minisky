//go:build cgo

package bigquery

import "testing"

func TestDuckDBBackendUsesFullProfileOnCGOBuild(t *testing.T) {
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_BQ_BACKEND", "")
	t.Setenv("MINISKY_DUCKDB_PATH", t.TempDir()+"/profile.duckdb")

	backend := NewDuckDBBackend()
	defer backend.Close()
	if state := backend.Status(); !state.Enabled || state.Backend != "duckdb" || state.Source != "profile" {
		t.Fatalf("backend state = %#v, want profile-selected DuckDB", state)
	}
}

func TestDuckDBBackendSimulationProfileIsBackwardCompatible(t *testing.T) {
	t.Setenv("MINISKY_RUNTIME_PROFILE", "")
	t.Setenv("MINISKY_BQ_BACKEND", "")
	t.Setenv("MINISKY_DUCKDB_PATH", t.TempDir()+"/profile.duckdb")

	backend := NewDuckDBBackend()
	defer backend.Close()
	if state := backend.Status(); state.Enabled || state.Backend != "simulation" {
		t.Fatalf("backend state = %#v, want simulation default", state)
	}
}
