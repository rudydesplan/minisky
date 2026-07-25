//go:build cgo

package main

import (
	"path/filepath"
	"testing"
)

func TestRunBigQueryDoctorWithDuckDB(t *testing.T) {
	if err := runBigQueryDoctor(filepath.Join(t.TempDir(), "doctor.duckdb")); err != nil {
		t.Fatalf("BigQuery doctor failed: %v", err)
	}
}
