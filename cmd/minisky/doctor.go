package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/shims/bigquery"
	"minisky/pkg/shims/gke"
	"minisky/pkg/shims/serverless"

	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run platform capability checks",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDoctor(cmd.Context(), cmd.OutOrStdout(), defaultDoctorDependencies(), doctorFix)
	},
}

var doctorFix bool

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

type doctorDependencies struct {
	checkDocker        func() error
	checkDuckDB        func() error
	findTool           func(string) error
	installTool        func(context.Context, string) error
	checkPortAvailable func(string) error
	checkDataDir       func() error
	backendStates      func() []namedBackendState
	apiPort            string
	uiPort             string
}

type namedBackendState struct {
	name  string
	state config.BackendState
}

type doctorCheck struct {
	name     string
	required bool
	run      func() error
	fix      func(context.Context) error
}

func defaultDoctorDependencies() doctorDependencies {
	return doctorDependencies{
		checkDocker:        checkDockerConnectivity,
		checkDuckDB:        checkDuckDBCapability,
		findTool:           findDoctorTool,
		installTool:        orchestrator.InstallToolDependency,
		checkPortAvailable: checkDoctorPort,
		checkDataDir:       checkDoctorDataDir,
		backendStates:      effectiveBackendStates,
		apiPort:            environmentOrDefault("MINISKY_PORT", "8080"),
		uiPort:             environmentOrDefault("MINISKY_UI_PORT", "8081"),
	}
}

func runDoctor(ctx context.Context, output io.Writer, dependencies doctorDependencies, fix bool) error {
	if dependencies.backendStates != nil {
		profile := config.GetRuntimeProfile()
		fmt.Fprintf(output, "PROFILE %s\n", profile.Name)
		if profile.Diagnostic != "" {
			fmt.Fprintf(output, "WARN runtime profile: %s\n", profile.Diagnostic)
		}
		for _, backend := range dependencies.backendStates() {
			fmt.Fprintf(output, "BACKEND %s %s (%s)\n", backend.name, backend.state.Backend, backend.state.Source)
			if backend.state.Diagnostic != "" {
				fmt.Fprintf(output, "WARN %s: %s\n", backend.name, backend.state.Diagnostic)
			}
		}
	}

	checks := []doctorCheck{
		{name: "Docker connectivity", required: true, run: dependencies.checkDocker},
		{name: "DuckDB", required: true, run: dependencies.checkDuckDB},
		{
			name: "kind",
			run:  func() error { return dependencies.findTool("kind") },
			fix:  func(ctx context.Context) error { return dependencies.installTool(ctx, "kind") },
		},
		{
			name: "pack",
			run:  func() error { return dependencies.findTool("pack") },
			fix:  func(ctx context.Context) error { return dependencies.installTool(ctx, "pack") },
		},
		{name: "API port " + dependencies.apiPort, required: true, run: func() error {
			return dependencies.checkPortAvailable(dependencies.apiPort)
		}},
		{name: "UI port " + dependencies.uiPort, required: true, run: func() error {
			return dependencies.checkPortAvailable(dependencies.uiPort)
		}},
		{name: "data directory writable", required: true, run: dependencies.checkDataDir},
	}

	requiredFailures := 0
	for _, check := range checks {
		if err := check.run(); err != nil {
			if fix && !check.required && check.fix != nil {
				if fixErr := check.fix(ctx); fixErr != nil {
					fmt.Fprintf(output, "FAIL %s (optional): fix failed: %v\n", check.name, fixErr)
					continue
				}
				if verifyErr := check.run(); verifyErr != nil {
					fmt.Fprintf(output, "FAIL %s (optional): fix did not make tool available: %v\n", check.name, verifyErr)
					continue
				}
				fmt.Fprintf(output, "FIXED %s\n", check.name)
				fmt.Fprintf(output, "PASS %s\n", check.name)
				continue
			}
			if check.required {
				requiredFailures++
				fmt.Fprintf(output, "FAIL %s: %v\n", check.name, err)
			} else {
				fmt.Fprintf(output, "FAIL %s (optional): %v\n", check.name, err)
			}
			continue
		}
		fmt.Fprintf(output, "PASS %s\n", check.name)
	}

	if requiredFailures > 0 {
		return fmt.Errorf("doctor found %d required failure(s)", requiredFailures)
	}
	return nil
}

func effectiveBackendStates() []namedBackendState {
	bqBackend := bigquery.NewDuckDBBackend()
	defer bqBackend.Close()
	gkeBackend := gke.NewKindBackend()
	serverlessBackend := serverless.NewBuildpacksBackend()
	return []namedBackendState{
		{name: "BigQuery", state: bqBackend.Status()},
		{name: "GKE", state: gkeBackend.Status()},
		{name: "Serverless", state: serverlessBackend.Status()},
	}
}

func checkDockerConnectivity() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	output, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput()
	if ctx.Err() != nil {
		return fmt.Errorf("timed out")
	}
	if err != nil {
		detail := strings.TrimSpace(string(output))
		if detail != "" {
			return fmt.Errorf("%s", strings.Split(detail, "\n")[0])
		}
		return err
	}
	return nil
}

func checkDuckDBCapability() error {
	tempDir, err := os.MkdirTemp("", "minisky-duckdb-doctor-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer os.RemoveAll(tempDir)
	return runBigQueryDoctor(filepath.Join(tempDir, "doctor.duckdb"))
}

func findDoctorTool(name string) error {
	binaryName := name
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	localPath := filepath.Join(config.GetMiniskyDir(), "bin", binaryName)
	if info, err := os.Stat(localPath); err == nil && !info.IsDir() {
		return nil
	}
	if _, err := exec.LookPath(binaryName); err != nil {
		return fmt.Errorf("not installed")
	}
	return nil
}

func checkDoctorPort(port string) error {
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		return fmt.Errorf("unavailable")
	}
	return listener.Close()
}

func checkDoctorDataDir() error {
	dataDir := config.GetMiniskyDir()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("%s: %w", dataDir, err)
	}
	probe, err := os.CreateTemp(dataDir, ".doctor-write-*")
	if err != nil {
		return fmt.Errorf("%s: %w", dataDir, err)
	}
	probePath := probe.Name()
	if _, err := probe.WriteString("ok"); err != nil {
		_ = probe.Close()
		_ = os.Remove(probePath)
		return fmt.Errorf("%s: %w", dataDir, err)
	}
	if err := probe.Close(); err != nil {
		_ = os.Remove(probePath)
		return fmt.Errorf("%s: %w", dataDir, err)
	}
	if err := os.Remove(probePath); err != nil {
		return fmt.Errorf("%s: %w", dataDir, err)
	}
	return nil
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
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
	doctorCmd.Flags().BoolVar(&doctorFix, "fix", false, "Install missing optional kind and pack tools")
	doctorCmd.AddCommand(doctorBigQueryCmd)
	rootCmd.AddCommand(doctorCmd)
}
