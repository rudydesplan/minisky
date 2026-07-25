package main

import (
	"fmt"
	"os"
	"path/filepath"

	"minisky/pkg/shims/bigquery"

	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run platform capability checks",
}

var doctorBigQueryCmd = &cobra.Command{
	Use:   "bigquery",
	Short: "Verify embedded DuckDB query execution",
	RunE: func(cmd *cobra.Command, args []string) error {
		tempDir, err := os.MkdirTemp("", "minisky-bigquery-doctor-*")
		if err != nil {
			return fmt.Errorf("create temporary directory: %w", err)
		}
		defer os.RemoveAll(tempDir)

		if err := runBigQueryDoctor(filepath.Join(tempDir, "doctor.duckdb")); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "BigQuery DuckDB check passed")
		return nil
	},
}

func runBigQueryDoctor(dbPath string) error {
	previousBackend, hadBackend := os.LookupEnv("MINISKY_BQ_BACKEND")
	previousPath, hadPath := os.LookupEnv("MINISKY_DUCKDB_PATH")
	defer restoreEnvironment("MINISKY_BQ_BACKEND", previousBackend, hadBackend)
	defer restoreEnvironment("MINISKY_DUCKDB_PATH", previousPath, hadPath)

	if err := os.Setenv("MINISKY_BQ_BACKEND", "duckdb"); err != nil {
		return err
	}
	if err := os.Setenv("MINISKY_DUCKDB_PATH", dbPath); err != nil {
		return err
	}

	backend := bigquery.NewDuckDBBackend()
	defer backend.Close()
	if !backend.Enabled() {
		if err := backend.SetEnabled(true); err != nil {
			return fmt.Errorf("BigQuery DuckDB check requires CGO support: %w", err)
		}
	}

	rows, err := backend.ExecuteQuery("SELECT 1 AS result")
	if err != nil {
		return fmt.Errorf("execute BigQuery DuckDB check: %w", err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["result"]) != "1" {
		return fmt.Errorf("unexpected BigQuery DuckDB result: %#v", rows)
	}
	return nil
}

func restoreEnvironment(key, value string, wasSet bool) {
	if wasSet {
		_ = os.Setenv(key, value)
		return
	}
	_ = os.Unsetenv(key)
}

func init() {
	doctorCmd.AddCommand(doctorBigQueryCmd)
	rootCmd.AddCommand(doctorCmd)
}
