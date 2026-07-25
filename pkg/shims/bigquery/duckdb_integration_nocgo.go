//go:build !cgo

package bigquery

import (
	"fmt"
	"log"

	"minisky/pkg/config"
)

// DuckDBBackend is a mock version for platforms without CGO (like Windows native build).
type DuckDBBackend struct {
	enabled bool
	status  config.BackendState
}

func NewDuckDBBackend() *DuckDBBackend {
	selection := config.ResolveBackend("MINISKY_BQ_BACKEND", "duckdb")
	diagnostic := ""
	if selection.Requested {
		diagnostic = "DuckDB requires a CGO-enabled build; using simulation"
	}
	status := selection.Effective(false, diagnostic)
	if status.Diagnostic != "" {
		log.Printf("[DuckDBBackend] WARNING: %s", status.Diagnostic)
	}
	return &DuckDBBackend{enabled: false, status: status}
}

func (d *DuckDBBackend) Enabled() bool { return false }

func (d *DuckDBBackend) Status() config.BackendState { return d.status }

func (d *DuckDBBackend) SetEnabled(enabled bool) error {
	if enabled {
		d.status.Backend = config.RuntimeProfileSimulation
		d.status.Enabled = false
		d.status.Source = "dashboard"
		d.status.Diagnostic = "DuckDB requires a CGO-enabled build; using simulation"
		return fmt.Errorf("DuckDB backend requires CGO and is not available on this platform/build")
	}
	d.status.Backend = config.RuntimeProfileSimulation
	d.status.Enabled = false
	d.status.Source = "dashboard"
	d.status.Diagnostic = ""
	return nil
}

func (d *DuckDBBackend) Close() error { return nil }

func (d *DuckDBBackend) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("duckdb backend requires CGO_ENABLED=1")
}

func (d *DuckDBBackend) LoadData(project, dataset, table, sourceURI, format string) error {
	return fmt.Errorf("duckdb backend requires CGO_ENABLED=1")
}

func (d *DuckDBBackend) CreateTable(project, dataset, table string, schema *TableSchema) error {
	return fmt.Errorf("duckdb backend requires CGO_ENABLED=1")
}

func (d *DuckDBBackend) InsertRows(dataset, table string, schema *TableSchema, rows []map[string]interface{}) error {
	return fmt.Errorf("duckdb backend requires CGO_ENABLED=1")
}
