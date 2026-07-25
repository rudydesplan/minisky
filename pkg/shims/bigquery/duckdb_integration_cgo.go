//go:build cgo

package bigquery

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5b — DuckDB Integration
//
// This file wires the BigQuery shim to a real local DuckDB instance.
// When enabled via MINISKY_BQ_BACKEND=duckdb, jobs.insert calls execute the
// SQL query against an embedded DuckDB database instead of returning empty results.
//
// Prerequisites:
//   - Add dependency: go get github.com/marcboeker/go-duckdb
//   - CGO must be enabled (DuckDB requires it): CGO_ENABLED=1
//
// Enable with: export MINISKY_BQ_BACKEND=duckdb
//
// Table DDL Mapping:
//   When a BigQuery table is created with a schema, MiniSky automatically
//   creates a matching DuckDB table using the mapped types below.
//
// BigQuery → DuckDB Type Mapping:
//   STRING    → VARCHAR
//   INTEGER   → BIGINT
//   FLOAT     → DOUBLE
//   BOOLEAN   → BOOLEAN
//   TIMESTAMP → TIMESTAMP WITH TIME ZONE
//   DATE      → DATE
//   RECORD    → STRUCT (nested)
//   BYTES     → BLOB
// ─────────────────────────────────────────────────────────────────────────────

import (
	"database/sql"
	"fmt"
	"log"
	"minisky/pkg/config"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

// DuckDBBackend manages an embedded DuckDB database for BigQuery query execution.
type DuckDBBackend struct {
	enabled bool
	dbPath  string
	db      *sql.DB
	status  config.BackendState
}

// NewDuckDBBackend returns a DuckDBBackend selected by the runtime profile or
// an explicit MINISKY_BQ_BACKEND override.
func NewDuckDBBackend() *DuckDBBackend {
	selection := config.ResolveBackend("MINISKY_BQ_BACKEND", "duckdb")
	dbPath := os.Getenv("MINISKY_DUCKDB_PATH")
	if dbPath == "" {
		dbPath = filepath.Join(config.GetMiniskyDir(), "data", "bigquery.duckdb")
	}

	b := &DuckDBBackend{
		enabled: selection.Requested,
		dbPath:  dbPath,
		status:  selection.Effective(selection.Requested, ""),
	}

	if selection.Requested {
		log.Printf("[DuckDBBackend] ✅ DuckDB integration ENABLED — queries will execute against %s", dbPath)
		if err := b.init(); err != nil {
			diagnostic := fmt.Sprintf("DuckDB initialization failed: %v; using simulation", err)
			log.Printf("[DuckDBBackend] WARNING: %s", diagnostic)
			b.enabled = false
			b.status = selection.Effective(false, diagnostic)
		}
	} else if b.status.Diagnostic != "" {
		log.Printf("[DuckDBBackend] WARNING: %s", b.status.Diagnostic)
	}
	return b
}

// Enabled reports whether DuckDB backend is active.
func (d *DuckDBBackend) Enabled() bool { return d.enabled }

// Status reports the effective backend selected after initialization.
func (d *DuckDBBackend) Status() config.BackendState { return d.status }

// SetEnabled toggles the DuckDB backend dynamically.
func (d *DuckDBBackend) SetEnabled(enabled bool) error {
	if enabled {
		log.Printf("[DuckDBBackend] dynamically ENABLED via UI")
		if err := d.init(); err != nil {
			d.enabled = false
			d.status.Backend = config.RuntimeProfileSimulation
			d.status.Enabled = false
			d.status.Source = "dashboard"
			d.status.Diagnostic = fmt.Sprintf("DuckDB initialization failed: %v; using simulation", err)
			return err
		}
		d.enabled = true
		d.status.Backend = "duckdb"
		d.status.Enabled = true
		d.status.Source = "dashboard"
		d.status.Diagnostic = ""
		return nil
	}
	log.Printf("[DuckDBBackend] dynamically DISABLED via UI")
	d.enabled = false
	d.status.Backend = config.RuntimeProfileSimulation
	d.status.Enabled = false
	d.status.Source = "dashboard"
	d.status.Diagnostic = ""
	return d.Close()
}

// init opens or creates the DuckDB database file.
func (d *DuckDBBackend) init() error {
	dir := filepath.Dir(d.dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("cannot create data directory: %w", err)
	}

	if err := d.Close(); err != nil {
		return fmt.Errorf("close existing duckdb connection: %w", err)
	}
	db, err := sql.Open("duckdb", d.dbPath)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	d.db = db
	if err := db.Ping(); err != nil {
		_ = db.Close()
		d.db = nil
		return err
	}
	return nil
}

// Close releases the DuckDB connection when the backend is disabled or replaced.
func (d *DuckDBBackend) Close() error {
	if d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	return err
}

// ExecuteQuery runs a BigQuery StandardSQL query and returns rows as a slice of maps.
// The query is first translated from BigQuery SQL dialect to DuckDB SQL.
func (d *DuckDBBackend) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	if !d.enabled {
		return nil, fmt.Errorf("duckdb backend not enabled")
	}
	translated := translateBQtoDuck(query)
	log.Printf("[DuckDBBackend] Executing: %s", translated)

	rows, err := d.db.Query(translated)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var results []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		rowMap := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			// Convert bytes arrays into strings if possible
			v := *val
			if b, ok := v.([]byte); ok {
				rowMap[colName] = string(b)
			} else {
				rowMap[colName] = v
			}
		}
		results = append(results, rowMap)
	}
	return results, nil
}

// LoadData ingests a file or URL into a DuckDB table.
func (d *DuckDBBackend) LoadData(project, dataset, table, sourceURI, format string) error {
	if !d.enabled {
		return fmt.Errorf("duckdb backend not enabled")
	}
	tableName := fmt.Sprintf("%s__%s", dataset, table)

	var query string
	format = strings.ToUpper(format)

	// Convert Windows path separators to forward slashes to prevent SQL escape sequence errors
	safeURI := strings.ReplaceAll(filepath.ToSlash(sourceURI), "'", "''")

	switch format {
	case "CSV":
		query = fmt.Sprintf("CREATE OR REPLACE TABLE \"%s\" AS SELECT * FROM read_csv_auto('%s')", tableName, safeURI)
	case "JSON", "NEWLINE_DELIMITED_JSON":
		query = fmt.Sprintf("CREATE OR REPLACE TABLE \"%s\" AS SELECT * FROM read_json_auto('%s')", tableName, safeURI)
	case "PARQUET":
		query = fmt.Sprintf("CREATE OR REPLACE TABLE \"%s\" AS SELECT * FROM read_parquet('%s')", tableName, safeURI)
	default:
		return fmt.Errorf("unsupported format for DuckDB load: %s", format)
	}

	log.Printf("[DuckDBBackend] Loading data: %s", query)
	_, err := d.db.Exec(query)
	return err
}

// CreateTable creates a DuckDB table from a BigQuery TableSchema.
func (d *DuckDBBackend) CreateTable(project, dataset, table string, schema *TableSchema) error {
	if !d.enabled || schema == nil {
		return nil
	}
	ddl := buildDDL(project, dataset, table, schema)
	log.Printf("[DuckDBBackend] Creating table: %s", ddl)

	_, err := d.db.Exec(ddl)
	if err != nil {
		log.Printf("[DuckDBBackend] Error creating table: %v", err)
	}
	return err
}

// InsertRows writes BigQuery streaming rows into their DuckDB table.
func (d *DuckDBBackend) InsertRows(dataset, table string, schema *TableSchema, rows []map[string]interface{}) error {
	if !d.enabled {
		return fmt.Errorf("duckdb backend not enabled")
	}
	if d.db == nil {
		return fmt.Errorf("duckdb backend is not initialized")
	}
	if schema == nil || len(schema.Fields) == 0 || len(rows) == 0 {
		return nil
	}

	columns := make([]string, len(schema.Fields))
	placeholders := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		columns[i] = quoteIdentifier(field.Name)
		placeholders[i] = "?"
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(dataset+"__"+table),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin streaming insert: %w", err)
	}
	defer tx.Rollback()

	statement, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("prepare streaming insert: %w", err)
	}
	defer statement.Close()

	for _, row := range rows {
		values := make([]interface{}, len(schema.Fields))
		for i, field := range schema.Fields {
			values[i] = row[field.Name]
		}
		if _, err := statement.Exec(values...); err != nil {
			return fmt.Errorf("execute streaming insert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit streaming insert: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL Translation helpers
// ─────────────────────────────────────────────────────────────────────────────

// bqToDuckTypeMap maps BigQuery field types to DuckDB equivalents.
var bqToDuckTypeMap = map[string]string{
	"STRING":     "VARCHAR",
	"BYTES":      "BLOB",
	"INTEGER":    "BIGINT",
	"INT64":      "BIGINT",
	"FLOAT":      "DOUBLE",
	"FLOAT64":    "DOUBLE",
	"NUMERIC":    "DECIMAL(38,9)",
	"BIGNUMERIC": "DECIMAL(76,38)",
	"BOOLEAN":    "BOOLEAN",
	"BOOL":       "BOOLEAN",
	"TIMESTAMP":  "TIMESTAMPTZ",
	"DATE":       "DATE",
	"TIME":       "TIME",
	"DATETIME":   "TIMESTAMP",
	"GEOGRAPHY":  "VARCHAR", // approximate — DuckDB lacks native GEOGRAPHY
	"JSON":       "JSON",
	"RECORD":     "STRUCT", // nested — requires recursive handling
	"STRUCT":     "STRUCT",
}

// translateBQtoDuck does lightweight BigQuery → DuckDB SQL dialect conversion.
// Handles the most common divergences between the two dialects.
func translateBQtoDuck(bqSQL string) string {
	s := bqSQL

	// Backtick → double-quote identifiers  (`project.dataset.table` → "project.dataset.table")
	s = strings.ReplaceAll(s, "`", "\"")

	// CURRENT_TIMESTAMP() → CURRENT_TIMESTAMP
	s = strings.ReplaceAll(s, "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")

	// TIMESTAMP_TRUNC(x, DAY) → DATE_TRUNC('day', x)  (basic form)
	// Note: Full translation requires a proper SQL parser; this handles the common case.
	if strings.Contains(s, "TIMESTAMP_TRUNC") {
		log.Printf("[DuckDBBackend] WARN: TIMESTAMP_TRUNC requires manual translation — result may vary")
	}

	// SAFE_DIVIDE(a, b) → CASE WHEN b = 0 THEN NULL ELSE a / b END
	if strings.Contains(s, "SAFE_DIVIDE") {
		log.Printf("[DuckDBBackend] WARN: SAFE_DIVIDE not auto-translated — consider rewriting query")
	}

	// dataset.table → dataset__table (DuckDB internal mapping)
	// Supports project.dataset.table (3 segments) and dataset.table (2 segments)
	// 1. project.dataset.table -> dataset.table
	projectRe := regexp.MustCompile(`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)`)
	s = projectRe.ReplaceAllString(s, "$2.$3")

	// 2. dataset.table -> dataset__table
	datasetRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)`)
	s = datasetRe.ReplaceAllString(s, "${1}__$2")

	return s
}

// buildDDL generates a CREATE TABLE IF NOT EXISTS statement for DuckDB.
func buildDDL(project, dataset, table string, schema *TableSchema) string {
	_ = project
	// DuckDB table name: dataset__table (project is ignored in local context)
	tableName := fmt.Sprintf("%s__%s", dataset, table)
	cols := make([]string, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		duckType := duckDBFieldType(f)
		nullable := ""
		if strings.ToUpper(f.Mode) == "REQUIRED" {
			nullable = " NOT NULL"
		}
		cols = append(cols, fmt.Sprintf("  %s %s%s", quoteIdentifier(f.Name), duckType, nullable))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n);",
		quoteIdentifier(tableName), strings.Join(cols, ",\n"))
}

func duckDBFieldType(field FieldSchema) string {
	fieldType := strings.ToUpper(field.Type)
	var duckType string
	if fieldType == "RECORD" || fieldType == "STRUCT" {
		children := make([]string, 0, len(field.Fields))
		for _, child := range field.Fields {
			children = append(children, fmt.Sprintf("%s %s", quoteIdentifier(child.Name), duckDBFieldType(child)))
		}
		if len(children) == 0 {
			duckType = "STRUCT"
		} else {
			duckType = fmt.Sprintf("STRUCT(%s)", strings.Join(children, ", "))
		}
	} else {
		duckType = bqToDuckTypeMap[fieldType]
		if duckType == "" {
			duckType = "VARCHAR"
		}
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		duckType += "[]"
	}
	return duckType
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
