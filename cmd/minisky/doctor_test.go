package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"minisky/pkg/config"
)

func TestRunDoctorReportsAllPassingChecks(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	err := runDoctor(context.Background(), &output, doctorDependencies{
		checkDocker:        func() error { return nil },
		checkDuckDB:        func() error { return nil },
		findTool:           func(string) error { return nil },
		checkPortAvailable: func(string) error { return nil },
		checkDataDir:       func() error { return nil },
		apiPort:            "18080",
		uiPort:             "18081",
	}, false)
	if err != nil {
		t.Fatalf("runDoctor returned an error: %v", err)
	}

	for _, expected := range []string{
		"PASS Docker connectivity",
		"PASS DuckDB",
		"PASS kind",
		"PASS pack",
		"PASS API port 18080",
		"PASS UI port 18081",
		"PASS data directory writable",
	} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("output %q does not contain %q", output.String(), expected)
		}
	}
}

func TestRunDoctorReportsEffectiveBackendStates(t *testing.T) {
	t.Setenv(config.RuntimeProfileEnv, "full")

	dependencies := passingDoctorDependencies()
	dependencies.backendStates = func() []namedBackendState {
		return []namedBackendState{
			{name: "BigQuery", state: config.BackendState{Profile: "full", Backend: "duckdb", Enabled: true, Source: "profile"}},
			{name: "GKE", state: config.BackendState{Profile: "full", Backend: "simulation", Source: "profile", Diagnostic: "Kind dependency missing (kind); using simulation"}},
		}
	}

	var output bytes.Buffer
	if err := runDoctor(context.Background(), &output, dependencies, false); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"PROFILE full",
		"BACKEND BigQuery duckdb (profile)",
		"BACKEND GKE simulation (profile)",
		"WARN GKE: Kind dependency missing",
	} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("output %q does not contain %q", output.String(), expected)
		}
	}
}

func TestRunDoctorReturnsErrorForRequiredFailureAndContinues(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	err := runDoctor(context.Background(), &output, doctorDependencies{
		checkDocker:        func() error { return errors.New("cannot reach socket") },
		checkDuckDB:        func() error { return nil },
		findTool:           func(string) error { return nil },
		checkPortAvailable: func(string) error { return nil },
		checkDataDir:       func() error { return nil },
		apiPort:            "8080",
		uiPort:             "8081",
	}, false)
	if err == nil {
		t.Fatal("runDoctor returned nil for a required failure")
	}
	if !strings.Contains(output.String(), "FAIL Docker connectivity: cannot reach socket") {
		t.Fatalf("output = %q, want Docker failure", output.String())
	}
	if !strings.Contains(output.String(), "PASS data directory writable") {
		t.Fatalf("output = %q, want checks to continue after failure", output.String())
	}
}

func TestRunDoctorDoesNotFailForOptionalTools(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	err := runDoctor(context.Background(), &output, doctorDependencies{
		checkDocker: func() error { return nil },
		checkDuckDB: func() error { return nil },
		findTool: func(name string) error {
			return errors.New(name + " not found")
		},
		checkPortAvailable: func(string) error { return nil },
		checkDataDir:       func() error { return nil },
		apiPort:            "8080",
		uiPort:             "8081",
	}, false)
	if err != nil {
		t.Fatalf("runDoctor returned an error for optional failures: %v", err)
	}
	if !strings.Contains(output.String(), "FAIL kind (optional): kind not found") {
		t.Fatalf("output = %q, want optional kind failure", output.String())
	}
	if !strings.Contains(output.String(), "FAIL pack (optional): pack not found") {
		t.Fatalf("output = %q, want optional pack failure", output.String())
	}
}

func TestRunDoctorTreatsRuntimeChecksAsRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fail func(*doctorDependencies)
	}{
		{
			name: "DuckDB",
			fail: func(dependencies *doctorDependencies) {
				dependencies.checkDuckDB = func() error { return errors.New("failed") }
			},
		},
		{
			name: "API port",
			fail: func(dependencies *doctorDependencies) {
				dependencies.checkPortAvailable = func(port string) error {
					if port == "8080" {
						return errors.New("failed")
					}
					return nil
				}
			},
		},
		{
			name: "UI port",
			fail: func(dependencies *doctorDependencies) {
				dependencies.checkPortAvailable = func(port string) error {
					if port == "8081" {
						return errors.New("failed")
					}
					return nil
				}
			},
		},
		{
			name: "data directory",
			fail: func(dependencies *doctorDependencies) {
				dependencies.checkDataDir = func() error { return errors.New("failed") }
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dependencies := doctorDependencies{
				checkDocker:        func() error { return nil },
				checkDuckDB:        func() error { return nil },
				findTool:           func(string) error { return nil },
				checkPortAvailable: func(string) error { return nil },
				checkDataDir:       func() error { return nil },
				apiPort:            "8080",
				uiPort:             "8081",
			}
			test.fail(&dependencies)
			if err := runDoctor(context.Background(), &bytes.Buffer{}, dependencies, false); err == nil {
				t.Fatalf("runDoctor returned nil when %s failed", test.name)
			}
		})
	}
}

func TestRunDoctorChecksConfiguredPorts(t *testing.T) {
	t.Parallel()

	var checked []string
	err := runDoctor(context.Background(), &bytes.Buffer{}, doctorDependencies{
		checkDocker: func() error { return nil },
		checkDuckDB: func() error { return nil },
		findTool:    func(string) error { return nil },
		checkPortAvailable: func(port string) error {
			checked = append(checked, port)
			return nil
		},
		checkDataDir: func() error { return nil },
		apiPort:      "19080",
		uiPort:       "19081",
	}, false)
	if err != nil {
		t.Fatalf("runDoctor returned an error: %v", err)
	}
	if strings.Join(checked, ",") != "19080,19081" {
		t.Fatalf("checked ports = %v, want [19080 19081]", checked)
	}
}

func TestRunDoctorFixesOnlyMissingOptionalTools(t *testing.T) {
	t.Parallel()

	installed := map[string]bool{}
	var installCalls []string
	var dockerChecks, duckDBChecks, portChecks, dataDirChecks int
	dependencies := doctorDependencies{
		checkDocker: func() error {
			dockerChecks++
			return errors.New("docker remains unavailable")
		},
		checkDuckDB: func() error {
			duckDBChecks++
			return nil
		},
		findTool: func(name string) error {
			if !installed[name] {
				return errors.New("not installed")
			}
			return nil
		},
		installTool: func(_ context.Context, name string) error {
			installCalls = append(installCalls, name)
			installed[name] = true
			return nil
		},
		checkPortAvailable: func(string) error {
			portChecks++
			return nil
		},
		checkDataDir: func() error {
			dataDirChecks++
			return nil
		},
		apiPort: "8080",
		uiPort:  "8081",
	}

	var output bytes.Buffer
	err := runDoctor(context.Background(), &output, dependencies, true)
	if err == nil {
		t.Fatal("runDoctor returned nil for the unchanged required Docker failure")
	}
	if strings.Join(installCalls, ",") != "kind,pack" {
		t.Fatalf("installed %v, want only [kind pack]", installCalls)
	}
	if dockerChecks != 1 || duckDBChecks != 1 || portChecks != 2 || dataDirChecks != 1 {
		t.Fatalf("required checks mutated or rerun: docker=%d duckdb=%d ports=%d data=%d",
			dockerChecks, duckDBChecks, portChecks, dataDirChecks)
	}
	for _, expected := range []string{"FIXED kind", "FIXED pack", "FAIL Docker connectivity"} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("output %q does not contain %q", output.String(), expected)
		}
	}
}

func TestRunDoctorFixDoesNotInstallPresentTools(t *testing.T) {
	t.Parallel()

	dependencies := passingDoctorDependencies()
	dependencies.installTool = func(context.Context, string) error {
		t.Fatal("installer called for a present tool")
		return nil
	}
	if err := runDoctor(context.Background(), &bytes.Buffer{}, dependencies, true); err != nil {
		t.Fatalf("runDoctor returned an error: %v", err)
	}
}

func TestRunDoctorReportsOptionalFixFailure(t *testing.T) {
	t.Parallel()

	dependencies := passingDoctorDependencies()
	dependencies.findTool = func(string) error { return errors.New("not installed") }
	dependencies.installTool = func(_ context.Context, name string) error {
		return errors.New("verification failed for " + name)
	}

	var output bytes.Buffer
	if err := runDoctor(context.Background(), &output, dependencies, true); err != nil {
		t.Fatalf("optional fix failure must not fail doctor: %v", err)
	}
	if !strings.Contains(output.String(), "FAIL kind (optional): fix failed: verification failed for kind") {
		t.Fatalf("output = %q, want fix failure", output.String())
	}
}

func passingDoctorDependencies() doctorDependencies {
	return doctorDependencies{
		checkDocker:        func() error { return nil },
		checkDuckDB:        func() error { return nil },
		findTool:           func(string) error { return nil },
		installTool:        func(context.Context, string) error { return nil },
		checkPortAvailable: func(string) error { return nil },
		checkDataDir:       func() error { return nil },
		apiPort:            "8080",
		uiPort:             "8081",
	}
}
