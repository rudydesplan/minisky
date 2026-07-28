package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMemcacheIntegrationUsesStableSharedLockBeforeDockerChecks(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	lock := strings.Index(source, `shared_lock="/tmp/minisky-net-integration.lock"`)
	acquire := strings.Index(source, `mkdir "${shared_lock}"`)
	if lock < 0 || acquire < 0 {
		t.Fatal("stable shared MiniSky integration lock is not declared and acquired")
	}
	for _, later := range []string{
		"docker info",
		"docker network inspect minisky-net",
		"docker ps -a",
		`work_dir="$(mktemp`,
		`profile="memcache-integration-`,
	} {
		index := strings.Index(source, later)
		if index < 0 {
			t.Fatalf("expected script operation %q is absent", later)
		}
		if acquire > index {
			t.Fatalf("shared lock acquisition occurs after %q", later)
		}
	}
	if strings.Contains(source[lock:acquire], "TMPDIR") {
		t.Fatal("caller TMPDIR influences the shared integration lock")
	}
}

func TestMemcacheIntegrationAlternateTMPDIRCannotBypassSharedLock(t *testing.T) {
	bin := t.TempDir()
	logPath := filepath.Join(bin, "mkdir.log")
	fakeMkdir := `#!/usr/bin/env bash
printf '%s\n' "$*" >>"${FAKE_MKDIR_LOG}"
exit 1
`
	if err := os.WriteFile(filepath.Join(bin, "mkdir"), []byte(fakeMkdir), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("bash", "memcache-integration.sh")
	command.Env = append(os.Environ(),
		"MINISKY_MEMCACHE_INTEGRATION=1",
		"TMPDIR="+filepath.Join(t.TempDir(), "alternate"),
		"FAKE_MKDIR_LOG="+logPath,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("occupied shared lock was ignored:\n%s", output)
	}
	if !strings.Contains(string(output), "Another MiniSky Docker integration is active") {
		t.Fatalf("unexpected lock refusal:\n%s", output)
	}
	log, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got := strings.TrimSpace(string(log)); got != "/tmp/minisky-net-integration.lock" {
		t.Fatalf("mkdir target=%q, want stable shared lock", got)
	}
}

func TestMemcacheIntegrationAlternateTMPDIRRunsCannotRace(t *testing.T) {
	bin := t.TempDir()
	lockRoot := filepath.Join(t.TempDir(), "locks")
	marker := filepath.Join(t.TempDir(), "docker-info")
	release := filepath.Join(t.TempDir(), "release")
	if err := os.MkdirAll(lockRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	fakeMkdir := `#!/usr/bin/env bash
set -eu
case "$1" in
  /tmp/minisky-*-integration.lock)
    exec /bin/mkdir "${FAKE_LOCK_ROOT}/$(basename "$1")"
    ;;
esac
exec /bin/mkdir "$@"
`
	fakeRmdir := `#!/usr/bin/env bash
set -eu
case "$1" in
  /tmp/minisky-*-integration.lock)
    exec /bin/rmdir "${FAKE_LOCK_ROOT}/$(basename "$1")"
    ;;
esac
exec /bin/rmdir "$@"
`
	fakeDocker := `#!/usr/bin/env bash
set -eu
if [[ "${1:-}" == "info" ]]; then
  : >"${FAKE_DOCKER_MARKER}"
  while [[ ! -e "${FAKE_DOCKER_RELEASE}" ]]; do sleep 0.02; done
  exit 73
fi
exit 74
`
	for nameAndSource := range map[string]string{
		"mkdir":  fakeMkdir,
		"rmdir":  fakeRmdir,
		"docker": fakeDocker,
	} {
		if err := os.WriteFile(filepath.Join(bin, nameAndSource), []byte(map[string]string{
			"mkdir":  fakeMkdir,
			"rmdir":  fakeRmdir,
			"docker": fakeDocker,
		}[nameAndSource]), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	baseEnv := append(os.Environ(),
		"MINISKY_MEMCACHE_INTEGRATION=1",
		"FAKE_LOCK_ROOT="+lockRoot,
		"FAKE_DOCKER_MARKER="+marker,
		"FAKE_DOCKER_RELEASE="+release,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	first := exec.Command("bash", "memcache-integration.sh")
	first.Env = append(baseEnv, "TMPDIR="+filepath.Join(t.TempDir(), "first"))
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(release, nil, 0o600)
		_ = first.Process.Kill()
		_, _ = first.Process.Wait()
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first integration run did not acquire locks before Docker checks")
		}
		time.Sleep(20 * time.Millisecond)
	}

	second := exec.Command("bash", "memcache-integration.sh")
	second.Env = append(baseEnv, "TMPDIR="+filepath.Join(t.TempDir(), "second"))
	output, err := second.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Another MiniSky Docker integration is active") {
		t.Fatalf("second run bypassed canonical shared lock: err=%v\n%s", err, output)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err == nil {
		t.Fatal("first harness unexpectedly succeeded after forced Docker failure")
	}
}

func TestMemcacheDestroyAssertionIsExactAndFailClosed(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	assertion := shellFunction(t, source, "assert_no_memcache_container")
	for _, filter := range []string{
		`--filter "label=managed-by=minisky"`,
		`--filter "label=minisky.profile=${profile}"`,
		`--filter "label=minisky.service=memorystore-memcached"`,
		`--filter "label=minisky.resource=${tf_import}"`,
	} {
		if !strings.Contains(assertion, filter) {
			t.Errorf("destroy assertion lacks exact filter %q", filter)
		}
	}
	destroy := strings.Index(source, `terraform_bounded -chdir="${terraform_dir}" destroy`)
	call := -1
	if destroy >= 0 {
		if relative := strings.Index(source[destroy:], "\nassert_no_memcache_container\n"); relative >= 0 {
			call = destroy + relative
		}
	}
	if destroy < 0 || call < destroy {
		t.Fatal("exact container absence is not asserted after Terraform destroy")
	}

	bin := t.TempDir()
	fakeDocker := `#!/usr/bin/env bash
echo "inventory unavailable" >&2
exit 73
`
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fakeDocker), 0o700); err != nil {
		t.Fatal(err)
	}
	harness := strings.Join([]string{
		"set -Eeuo pipefail",
		`profile="profile-under-test"`,
		`tf_import="projects/demo/locations/us/instances/cache"`,
		assertion,
		"assert_no_memcache_container",
	}, "\n")
	command := exec.Command("bash", "-c", harness)
	command.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("Docker inventory error was accepted:\n%s", output)
	}
	if !strings.Contains(string(output), "Memcached Docker inventory failed") {
		t.Fatalf("inventory failure lost its diagnostic:\n%s", output)
	}
}

func TestMemcacheTerraformLifecycleUsesDedicatedUntargetedFixture(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	if strings.Contains(source, "-target=") {
		t.Fatal("Memcached lifecycle still filters Terraform drift with -target")
	}
	if !strings.Contains(source, `terraform_dir="${repository_root}/terraform/memcache"`) {
		t.Fatal("Memcached lifecycle does not use its dedicated one-resource fixture")
	}
	if count := strings.Count(source, "\nassert_no_drift\n"); count != 3 {
		t.Fatalf("no-drift assertion count=%d, want immediate, post-restart, and post-import checks", count)
	}
	normalization := shellFunction(t, source, "normalize_imported_provider_state")
	for _, required := range []string{
		`terraform_bounded -chdir="${terraform_dir}" plan`,
		`-out="${import_normalization_plan}"`,
		`terraform_bounded -chdir="${terraform_dir}" show -json "${import_normalization_plan}"`,
		`validate_import_normalization_plan "${import_normalization_json}"`,
		`terraform_bounded -chdir="${terraform_dir}" apply -input=false "${import_normalization_plan}"`,
		`assert_api_snapshot_unchanged`,
	} {
		if !strings.Contains(normalization, required) {
			t.Errorf("import normalization lacks guarded step %q", required)
		}
	}
	for _, forbidden := range []string{"state edit", "state push", "ignore_changes"} {
		if strings.Contains(normalization, forbidden) {
			t.Fatalf("import normalization filters or fabricates state with %q", forbidden)
		}
	}
	restart := strings.Index(source, "stop_minisky\nstart_minisky\nassert_no_drift")
	remove := strings.Index(source, `terraform_bounded -chdir="${terraform_dir}" state rm`)
	normalize := strings.Index(source, "\nnormalize_imported_provider_state\n")
	postImportPlan := strings.LastIndex(source, "\nassert_no_drift\n")
	destroy := strings.Index(source, `terraform_bounded -chdir="${terraform_dir}" destroy`)
	if restart < 0 || remove < restart || normalize < remove ||
		postImportPlan < normalize || destroy < postImportPlan {
		t.Fatalf("lifecycle order is not apply/no-drift/restart/no-drift/import/normalize/no-drift/destroy")
	}
}

func TestMemcacheImportNormalizationPlanRejectsAPIBackedChanges(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	validator := shellFunction(t, source, "validate_import_normalization_plan")
	validPlan := `{
	  "resource_changes": [{
	    "address": "google_memcache_instance.compatibility",
	    "change": {
	      "actions": ["update"],
	      "before": {"deletion_protection": null, "terraform_labels": {}, "timeouts": null},
	      "after": {
	        "deletion_protection": false,
	        "terraform_labels": {"goog-terraform-provisioned": "true"},
	        "timeouts": {"create": "3m", "update": "3m", "delete": "3m"}
	      }
	    }
	  }]
	}`
	invalidPlan := strings.Replace(validPlan,
		`"timeouts": {"create": "3m", "update": "3m", "delete": "3m"}`,
		`"timeouts": {"create": "3m", "update": "3m", "delete": "3m"}, "display_name": "mutated"`, 1)

	for _, test := range []struct {
		name    string
		plan    string
		wantOK  bool
		message string
	}{
		{name: "exact provider-only changes", plan: validPlan, wantOK: true},
		{name: "API-backed field", plan: invalidPlan, message: "display_name"},
	} {
		t.Run(test.name, func(t *testing.T) {
			planPath := filepath.Join(t.TempDir(), "plan.json")
			if err := os.WriteFile(planPath, []byte(test.plan), 0o600); err != nil {
				t.Fatal(err)
			}
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				`tf_address="google_memcache_instance.compatibility"`,
				validator,
				`validate_import_normalization_plan "$1"`,
			}, "\n")
			command := exec.Command("bash", "-c", harness, "plan-validator", planPath)
			output, err := command.CombinedOutput()
			if test.wantOK && err != nil {
				t.Fatalf("exact provider-only plan rejected: %v\n%s", err, output)
			}
			if !test.wantOK && (err == nil || !strings.Contains(string(output), test.message)) {
				t.Fatalf("API-backed plan accepted or diagnostic missing: err=%v\n%s", err, output)
			}
		})
	}
}

func TestMemcacheSignalsCannotExitSuccessfully(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	signalExit := shellFunction(t, source, "signal_exit")
	for _, test := range []struct {
		name   string
		signal string
		code   string
	}{
		{name: "INT", signal: "INT", code: "130"},
		{name: "TERM", signal: "TERM", code: "143"},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				signalExit,
				`trap 'signal_exit ` + test.code + `' ` + test.signal,
				"kill -" + test.signal + " $$",
				"exit 0",
			}, "\n")
			command := exec.Command("bash", "-c", harness)
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != mustAtoi(t, test.code) {
				t.Fatalf("signal exited successfully or with wrong status: err=%v\n%s", err, output)
			}
		})
	}
}

func TestMemcacheCleanupAndWatchdogAreBoundedAndDurable(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	cleanup := shellFunction(t, source, "cleanup")
	for _, required := range []string{"run_bounded", "wait_for_pid_exit"} {
		if !strings.Contains(cleanup, required) {
			t.Errorf("cleanup lacks bounded operation %q", required)
		}
	}
	if !strings.Contains(source, "start_watchdog") {
		t.Fatal("overall lifecycle watchdog is absent")
	}
	if !strings.Contains(shellFunction(t, source, "run_bounded"), "overall_deadline_epoch") {
		t.Fatal("bounded commands do not honor the overall lifecycle deadline")
	}
	for _, lifecycleCommand := range []string{
		"init", "validate", "apply", "plan", "show", "state", "import", "destroy",
	} {
		if !strings.Contains(source, `terraform_bounded -chdir="${terraform_dir}" `+lifecycleCommand) {
			t.Errorf("Terraform %s is outside the bounded command wrapper", lifecycleCommand)
		}
	}
	destroy := strings.Index(source, `terraform_bounded -chdir="${terraform_dir}" destroy`)
	restart := strings.Index(source[destroy:], "stop_minisky\nstart_minisky")
	notFound := strings.Index(source[destroy:], `deleted_status=`)
	containerAbsent := strings.Index(source[destroy:], "assert_no_memcache_container")
	if destroy < 0 || restart < 0 || notFound < restart || containerAbsent < notFound {
		t.Fatal("post-destroy order is not restart, durable 404, then exact container absence")
	}
}

func TestMemcacheNetworkInspectionDistinguishesAbsenceFromErrors(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	inspect := shellFunction(t, source, "inspect_minisky_network")
	if !strings.Contains(inspect, "No such network") {
		t.Fatal("network inspection does not distinguish true absence")
	}
	if !strings.Contains(shellFunction(t, source, "cleanup"), "inspect_minisky_network") {
		t.Fatal("cleanup bypasses fail-closed network inspection")
	}
}

func mustAtoi(t *testing.T, value string) int {
	t.Helper()
	var result int
	if _, err := fmt.Sscanf(value, "%d", &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestMemcacheTerraformFixtureModelsProviderOnlyFieldsWithoutAPIFabrication(t *testing.T) {
	root := filepath.Join("..", "terraform", "memcache")
	main, err := os.ReadFile(filepath.Join(root, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(main)
	for _, required := range []string{
		`resource "google_memcache_instance" "compatibility"`,
		"deletion_protection = false",
		"timeouts {",
		`create = "3m"`,
		`update = "3m"`,
		`delete = "3m"`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("dedicated provider fixture is missing %q", required)
		}
	}
	for _, computed := range []string{"effective_labels", "terraform_labels"} {
		if strings.Contains(source, computed) {
			t.Errorf("provider-computed field %q was fabricated in fixture configuration", computed)
		}
	}
	versions, err := os.ReadFile(filepath.Join(root, "versions.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(versions), `version = "7.41.0"`) {
		t.Fatal("dedicated fixture does not pin Google provider 7.41.0")
	}
}
