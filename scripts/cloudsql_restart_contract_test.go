package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	cloudSQLRestartTarget = "test-cloudsql-restart-integration"
	cloudSQLRestartJob    = "cloudsql-restart-integration"
	cloudSQLRestartTest   = "TestCloudSQLRestartReconciliationLiveDocker"
)

func TestCloudSQLTerraformFixtureUsesBoundedSupportedVersion(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "terraform", "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(source)
	start := strings.Index(contents, `resource "google_sql_database_instance" "fidelity"`)
	if start < 0 {
		t.Fatal("Cloud SQL fidelity Terraform resource is missing")
	}
	end := strings.Index(contents[start:], "\n}\n")
	if end < 0 {
		t.Fatal("Cloud SQL fidelity Terraform resource is unterminated")
	}
	resource := contents[start : start+end]
	if !strings.Contains(resource, `database_version = "POSTGRES_16"`) {
		t.Fatalf("Cloud SQL fidelity fixture must request exact supported POSTGRES_16:\n%s", resource)
	}
	if strings.Contains(resource, "POSTGRES_15") {
		t.Fatalf("Cloud SQL fidelity fixture regressed to unsupported POSTGRES_15:\n%s", resource)
	}
}

func TestCloudSQLRestartMakeTargetContract(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(source)
	if !strings.Contains(strings.SplitN(contents, "\n", 2)[0], cloudSQLRestartTarget) {
		t.Fatalf(".PHONY does not own %s", cloudSQLRestartTarget)
	}

	const wantRecipe = cloudSQLRestartTarget + ":\n" +
		"\tMINISKY_DOCKER_CLOUDSQL_INTEGRATION=1 go test -race -count=1 -timeout=10m " +
		"./pkg/shims/cloudsql -run '^" + cloudSQLRestartTest + "$$'"
	if got := makeTargetBlock(t, contents, cloudSQLRestartTarget); got != wantRecipe {
		t.Fatalf("Cloud SQL restart target wiring differs\n got: %q\nwant: %q", got, wantRecipe)
	}
}

func TestCriticalCloudSQLRestartWorkflowContract(t *testing.T) {
	workflow := readWorkflow(t, "critical-integration.yml")
	triggers := mustMap(workflow["on"])
	for _, event := range []string{"pull_request", "push"} {
		paths := stringSlice(mustMap(triggers[event])["paths"])
		for _, required := range []string{
			".github/workflows/critical-integration.yml",
			"Makefile",
			"pkg/shims/**",
			"scripts/cloudsql_restart_contract_test.go",
		} {
			if !contains(paths, required) {
				t.Errorf("%s paths omit %q", event, required)
			}
		}
	}

	job := workflowJob(t, workflow, cloudSQLRestartJob)
	if got := scalarString(job["name"]); got != cloudSQLRestartJob {
		t.Errorf("Cloud SQL restart job name = %q, want %q", got, cloudSQLRestartJob)
	}
	if got := scalarString(job["runs-on"]); got != "ubuntu-latest" {
		t.Errorf("Cloud SQL restart runner = %q, want ubuntu-latest", got)
	}
	if timeout, ok := integer(job["timeout-minutes"]); !ok || timeout != 15 {
		t.Errorf("Cloud SQL restart timeout = %v, want 15 minutes", job["timeout-minutes"])
	}
	for _, forbidden := range []string{"if", "needs", "continue-on-error", "concurrency", "strategy", "env"} {
		if _, found := job[forbidden]; found {
			t.Errorf("required Cloud SQL restart job contains forbidden %q", forbidden)
		}
	}

	permissions := mustMap(job["permissions"])
	if len(permissions) != 1 || scalarString(permissions["contents"]) != "read" {
		t.Errorf("Cloud SQL restart permissions = %#v, want contents: read only", permissions)
	}

	for _, action := range []string{checkoutAction, setupGoAction} {
		if firstStepUsing(job, action) == nil {
			t.Errorf("pinned action %q is absent", action)
		}
	}
	setupGo := firstStepUsing(job, setupGoAction)
	setupWith := mustMap(setupGo["with"])
	for name, want := range map[string]string{
		"go-version-file":       "go.mod",
		"cache":                 "true",
		"cache-dependency-path": "go.sum",
	} {
		if got := scalarString(setupWith[name]); got != want {
			t.Errorf("setup-go %s = %q, want %q", name, got, want)
		}
	}

	if errors := validateRequiredShellStep(job,
		"Run guarded Cloud SQL restart integration",
		"make "+cloudSQLRestartTarget,
	); len(errors) != 0 {
		t.Errorf("%s", strings.Join(errors, "; "))
	}
	if errors := validateRequiredShellStep(job,
		"Verify Docker availability",
		"docker info >/dev/null",
	); len(errors) != 0 {
		t.Errorf("%s", strings.Join(errors, "; "))
	}

	for _, step := range stepMaps(job) {
		if _, found := step["continue-on-error"]; found {
			t.Errorf("step %q contains continue-on-error", scalarString(step["name"]))
		}
		if _, found := step["env"]; found {
			t.Errorf("step %q bypasses Make-owned opt-in through env", scalarString(step["name"]))
		}
		run := scalarString(step["run"])
		for _, forbidden := range []string{
			"docker system prune",
			"docker container prune",
			"docker network prune",
			"docker volume prune",
			"docker rm",
			"docker network rm",
			"docker volume rm",
		} {
			if strings.Contains(run, forbidden) {
				t.Errorf("step %q contains forbidden Docker cleanup %q", scalarString(step["name"]), forbidden)
			}
		}
	}
}

func TestCloudSQLRestartLiveDockerEvidenceContract(t *testing.T) {
	source, err := os.ReadFile(filepath.Join(
		"..",
		"pkg",
		"shims",
		"cloudsql",
		"restart_integration_test.go",
	))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(source)
	for _, required := range []string{
		"TestCloudSQLRestartReconciliationLiveDocker",
		"MINISKY_DOCKER_CLOUDSQL_INTEGRATION",
		`databaseVersion":"POSTGRES_16"`,
		"context.WithTimeout",
		"reconcileRestored",
		"assertCloudSQLDockerInventory",
		"removeExactCloudSQLContainerRetainingVolume",
		"assertPortableCloudSQLSnapshotIsMetadataOnly",
		"createCloudSQLSentinelVolume",
		"github.com/jackc/pgx/v5",
		"exact local provenance only",
		"no legacy recovery",
		"no portable credentials or runtime tokens",
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("live Cloud SQL restart evidence is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"os/exec",
		"exec.Command",
		"docker system prune",
		"containers/prune",
		"volumes/prune",
		"time.Sleep(",
	} {
		if strings.Contains(contents, forbidden) {
			t.Errorf("live Cloud SQL restart evidence contains forbidden %q", forbidden)
		}
	}
}

func makeTargetBlock(t *testing.T, source, target string) string {
	t.Helper()
	marker := target + ":\n"
	start := strings.Index(source, marker)
	if start < 0 {
		t.Fatalf("Make target %s is absent", target)
	}
	end := strings.Index(source[start:], "\n\n")
	if end < 0 {
		end = len(source) - start
	}
	return strings.TrimSuffix(source[start:start+end], "\n")
}
