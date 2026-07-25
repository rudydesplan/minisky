//go:build !cgo

package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRunBigQueryDoctorReportsMissingCGO(t *testing.T) {
	err := runBigQueryDoctor(filepath.Join(t.TempDir(), "doctor.duckdb"))
	if err == nil || !strings.Contains(err.Error(), "CGO") {
		t.Fatalf("error = %v, want CGO requirement", err)
	}
}
