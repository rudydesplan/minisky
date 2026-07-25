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
	"strconv"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/router"
	localsecurity "minisky/pkg/security"
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
	checkDiskSpace     func() error
	checkTLS           func() error
	checkQuotas        func() error
	checkAudit         func() error
	requiredImages     []string
	checkImage         func(string) error
	pullImage          func(context.Context, string) error
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
		checkDiskSpace:     checkDoctorDiskSpace,
		checkTLS:           checkDoctorTLS,
		checkQuotas:        checkDoctorQuotas,
		checkAudit:         checkDoctorAudit,
		requiredImages:     requiredDoctorImages(),
		checkImage:         checkDoctorImage,
		pullImage:          pullDoctorImage,
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
	if dependencies.checkDiskSpace != nil {
		checks = append(checks, doctorCheck{name: "disk space", required: true, run: dependencies.checkDiskSpace})
	}
	if dependencies.checkTLS != nil {
		checks = append(checks, doctorCheck{name: "TLS configuration", required: true, run: dependencies.checkTLS})
	}
	if dependencies.checkQuotas != nil {
		checks = append(checks, doctorCheck{name: "quota configuration", required: true, run: dependencies.checkQuotas})
	}
	if dependencies.checkAudit != nil {
		checks = append(checks, doctorCheck{name: "audit hash chain", required: true, run: dependencies.checkAudit})
	}
	if dependencies.checkImage != nil {
		for _, image := range dependencies.requiredImages {
			image := image
			checks = append(checks, doctorCheck{
				name: "required image " + image,
				run:  func() error { return dependencies.checkImage(image) },
				fix: func(ctx context.Context) error {
					if dependencies.pullImage == nil {
						return fmt.Errorf("image pull is unavailable")
					}
					return dependencies.pullImage(ctx, image)
				},
			})
		}
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

func checkDoctorTLS() error {
	_, _, err := localsecurity.PrepareTLS(localsecurity.TLSOptions{
		Mode:       localsecurity.TLSMode(os.Getenv("MINISKY_TLS_MODE")),
		ProfileDir: config.GetProfileDir(),
		CertFile:   os.Getenv("MINISKY_TLS_CERT"),
		KeyFile:    os.Getenv("MINISKY_TLS_KEY"),
		ClientCA:   os.Getenv("MINISKY_TLS_CLIENT_CA"),
		ServerName: "localhost",
	})
	return err
}

func checkDoctorQuotas() error {
	_, err := router.ParseQuotaConfigJSON(os.Getenv("MINISKY_QUOTAS_JSON"), time.Now)
	return err
}

func checkDoctorAudit() error {
	enabled, _ := strconv.ParseBool(os.Getenv("MINISKY_AUDIT_ENABLED"))
	strict, _ := strconv.ParseBool(os.Getenv("MINISKY_AUDIT_STRICT"))
	if !enabled && !strict {
		return nil
	}
	audit, err := localsecurity.OpenAuditLog(config.GetProfileDir(), config.GetProfile(), strict)
	if err != nil {
		return err
	}
	defer audit.Close()
	return audit.Verify()
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

func checkDoctorDiskSpace() error {
	if runtime.GOOS == "windows" {
		output, err := exec.Command("powershell", "-NoProfile", "-Command",
			"(Get-PSDrive -Name ([IO.Path]::GetPathRoot($env:USERPROFILE).TrimEnd(':'))).Free").Output()
		if err != nil {
			return fmt.Errorf("query free space: %w", err)
		}
		free, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
		if err != nil {
			return fmt.Errorf("parse free space: %w", err)
		}
		if free < 512<<20 {
			return fmt.Errorf("less than 512 MiB available")
		}
		return nil
	}
	output, err := exec.Command("df", "-Pk", config.GetMiniskyDir()).Output()
	if err != nil {
		// The directory may not exist until the writable-data check runs.
		output, err = exec.Command("df", "-Pk", filepath.Dir(config.GetMiniskyDir())).Output()
	}
	if err != nil {
		return fmt.Errorf("query free space: %w", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 4 {
		return fmt.Errorf("unexpected disk-space output")
	}
	availableKB, err := strconv.ParseUint(fields[len(fields)-3], 10, 64)
	if err != nil {
		return fmt.Errorf("parse available disk space: %w", err)
	}
	if availableKB < 512*1024 {
		return fmt.Errorf("less than 512 MiB available")
	}
	return nil
}

func requiredDoctorImages() []string {
	registry := config.GetImageRegistry()
	seen := make(map[string]struct{})
	images := make([]string, 0, len(registry.Emulators)+1)
	for _, emulator := range registry.Emulators {
		if emulator.Image == "" {
			continue
		}
		if _, ok := seen[emulator.Image]; ok {
			continue
		}
		seen[emulator.Image] = struct{}{}
		images = append(images, emulator.Image)
	}
	if image := registry.ArtifactRegistry.Image; image != "" {
		if _, ok := seen[image]; !ok {
			seen[image] = struct{}{}
			images = append(images, image)
		}
	}
	if image := registry.Memorystore.Redis.DefaultImage; image != "" {
		if _, ok := seen[image]; !ok {
			images = append(images, image)
		}
	}
	return images
}

func checkDoctorImage(image string) error {
	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		return fmt.Errorf("not present locally")
	}
	return nil
}

func pullDoctorImage(ctx context.Context, image string) error {
	if !containsImage(requiredDoctorImages(), image) {
		return fmt.Errorf("refusing to pull undeclared image %q", image)
	}
	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(commandCtx, "docker", "pull", image).CombinedOutput(); err != nil {
		return fmt.Errorf("pull %s: %s", image, strings.TrimSpace(string(output)))
	}
	return nil
}

func containsImage(images []string, target string) bool {
	for _, image := range images {
		if image == target {
			return true
		}
	}
	return false
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
