package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMemcacheBackendIdentityMatchesShimContract(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	backendID := shellFunction(t, source, "memcache_backend_id")
	harness := strings.Join([]string{
		"set -Eeuo pipefail",
		`run_bounded() { local seconds="$1"; shift; test "${seconds}" -gt 0; "$@"; }`,
		backendID,
		`memcache_backend_id "local-dev-project" "us-central1" "minisky-sdk-memcached"`,
	}, "\n")
	output, err := exec.Command("bash", "-c", harness).CombinedOutput()
	if err != nil {
		t.Fatalf("derive backend identity: %v\n%s", err, output)
	}
	hasher := sha256.New()
	for _, part := range []string{"local-dev-project", "us-central1", "minisky-sdk-memcached"} {
		if err := binary.Write(hasher, binary.BigEndian, uint32(len(part))); err != nil {
			t.Fatal(err)
		}
		_, _ = hasher.Write([]byte(part))
	}
	want := fmt.Sprintf("memcache-%x", hasher.Sum(nil)[:16])
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("backend identity=%q, want %q", got, want)
	}
}

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
	bin := presenceOnlyRequiredCommands(t, "curl", "docker", "go", "python3", "terraform")
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
	bin := presenceOnlyRequiredCommands(t, "curl", "docker", "go", "python3", "terraform")
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
	fakePython := `#!/bin/bash
if [[ "$#" -eq 4 && "$1" == "-" && "$2" == "15" && "$3" == "docker" && "$4" == "info" ]]; then
  exec docker info
fi
printf 'Unexpected fake python3 invocation:' >&2
printf ' %q' "$@" >&2
printf '\n' >&2
exit 75
`
	for nameAndSource := range map[string]string{
		"mkdir":   fakeMkdir,
		"rmdir":   fakeRmdir,
		"docker":  fakeDocker,
		"python3": fakePython,
	} {
		if err := os.WriteFile(filepath.Join(bin, nameAndSource), []byte(map[string]string{
			"mkdir":   fakeMkdir,
			"rmdir":   fakeRmdir,
			"docker":  fakeDocker,
			"python3": fakePython,
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
	firstExit := make(chan error, 1)
	go func() {
		firstExit <- first.Wait()
	}()
	t.Cleanup(func() {
		_ = os.WriteFile(release, nil, 0o600)
		_ = first.Process.Kill()
		select {
		case <-firstExit:
		case <-time.After(2 * time.Second):
		}
	})
	if err := waitForFileOrProcessExit(marker, firstExit, 20*time.Second); err != nil {
		t.Fatalf("first integration run did not acquire locks before Docker checks: %v", err)
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
	if err := <-firstExit; err == nil {
		t.Fatal("first harness unexpectedly succeeded after forced Docker failure")
	}
}

func TestWaitForFileOrProcessExitReportsEarlyExit(t *testing.T) {
	command := exec.Command("bash", "-c", "exit 17")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() {
		exited <- command.Wait()
	}()
	started := time.Now()
	err := waitForFileOrProcessExit(filepath.Join(t.TempDir(), "never-created"), exited, 20*time.Second)
	if err == nil || !strings.Contains(err.Error(), "process exited before readiness") {
		t.Fatalf("early exit error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("early process exit took %s to report", elapsed)
	}
}

func TestWaitForFileOrProcessExitAllowsSlowHealthyStartup(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ready")
	command := exec.Command("bash", "-c", `sleep 0.3; : >"$1"; sleep 0.1`, "slow-start", marker)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() {
		exited <- command.Wait()
	}()
	if err := waitForFileOrProcessExit(marker, exited, 2*time.Second); err != nil {
		t.Fatalf("slow healthy startup was rejected: %v", err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		_ = command.Process.Kill()
		t.Fatal("slow-start test process did not exit")
	}
}

func TestMemcacheIntegrationExecutesProtocolEvidenceBeforeSDKRestartAndDelete(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	discovery := shellFunction(t, source, "discover_sdk_memcache_endpoint")
	for _, required := range []string{
		`"${gateway}/_minisky/memcache.googleapis.com/v1/${sdk_import}"`,
		`document.get("discoveryEndpoint")`,
		`ipaddress.ip_address(host).is_loopback`,
		`document.get("name") != expected_name`,
		`document.get("discoveryEndpoint") != expected_endpoint`,
	} {
		if !strings.Contains(discovery, required) {
			t.Errorf("SDK endpoint discovery lacks %q", required)
		}
	}

	protocol := shellFunction(t, source, "assert_memcache_protocol")
	for _, required := range []string{
		"run_bounded",
		"socket.create_connection",
		`b"set "`,
		`b"get "`,
		`b"STORED\r\n"`,
		`b"VALUE "`,
		`b"END\r\n"`,
	} {
		if !strings.Contains(protocol, required) {
			t.Errorf("active Memcached protocol evidence lacks %q", required)
		}
	}
	for _, bound := range []string{
		"len(key_bytes) > 250",
		"len(value_bytes) > 1024",
		"timeout=2",
		"connection.settimeout(2)",
	} {
		if !strings.Contains(protocol, bound) {
			t.Errorf("Memcached protocol evidence lacks bound %q", bound)
		}
	}

	create := strings.Index(source, "\nrun_sdk create\n")
	activeEvidence := strings.Index(source, strings.Join([]string{
		`sdk_binding="$(discover_exact_memcache_container "${sdk_backend_id}")"`,
		`IFS=$'\t' read -r sdk_container_id sdk_endpoint <<<"${sdk_binding}"`,
		`discover_sdk_memcache_endpoint "${sdk_endpoint}" >/dev/null`,
		`assert_exact_memcache_container_binding "${sdk_container_id}" "${sdk_backend_id}" "${sdk_endpoint}"`,
		`assert_memcache_protocol "${sdk_endpoint}" "minisky-protocol-evidence" "memcached-data-plane-ok"`,
		`assert_exact_memcache_container_binding "${sdk_container_id}" "${sdk_backend_id}" "${sdk_endpoint}"`,
	}, "\n"))
	restart := strings.Index(source, "\nstop_minisky\n")
	deleteSDK := strings.Index(source, "\nrun_sdk delete\n")
	cleanupSDK := strings.Index(source, "\nassert_no_memcache_container \"${sdk_backend_id}\"\n")
	terraformInit := strings.Index(source, "\nterraform_bounded -chdir=\"${terraform_dir}\" init")
	if create < 0 || activeEvidence < create || restart < activeEvidence || deleteSDK < restart ||
		cleanupSDK < deleteSDK || terraformInit < cleanupSDK {
		t.Fatal("active protocol set/get evidence is not unconditionally executed after SDK create and before restart/delete")
	}
	betweenCreateAndRestart := source[create:restart]
	if strings.Contains(betweenCreateAndRestart, "MINISKY_DOCKER_MEMCACHED_INTEGRATION") {
		t.Fatal("authoritative protocol evidence is hidden behind an additional opt-in")
	}
}

func TestMemcacheContainerBindingRejectsOwnershipAndIdentityMutations(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	discover := shellFunction(t, source, "discover_exact_memcache_container")
	inspect := shellFunction(t, source, "inspect_exact_memcache_container")
	assertBinding := shellFunction(t, source, "assert_exact_memcache_container_binding")
	for _, filter := range []string{
		"docker ps -aq --no-trunc",
		`--filter "label=managed-by=minisky"`,
		`--filter "label=minisky.profile=${profile}"`,
		`--filter "label=minisky.service=memorystore-memcached"`,
		`--filter "label=minisky.resource=${resource_id}"`,
	} {
		if !strings.Contains(discover, filter) {
			t.Errorf("container discovery lacks exact filter %q", filter)
		}
	}
	if strings.Contains(source, "docker rm") && strings.Contains(source, "memcachedDockerName") {
		t.Fatal("Memcached integration deletes a backend by mutable name")
	}

	for _, test := range []struct {
		name       string
		scenario   string
		wantOK     bool
		diagnostic string
	}{
		{name: "exact labels and stable immutable binding", scenario: "exact", wantOK: true},
		{name: "zero matching containers", scenario: "zero", diagnostic: "found 0"},
		{name: "multiple matching containers", scenario: "multiple", diagnostic: "found 2"},
		{name: "Docker inventory failure", scenario: "inventory-failure", diagnostic: "inventory unavailable"},
		{name: "foreign labels", scenario: "foreign-labels", diagnostic: "labels do not exactly match"},
		{name: "stopped container", scenario: "stopped", diagnostic: "is not running"},
		{name: "Docker inspect failure", scenario: "inspect-failure", diagnostic: "inspect unavailable"},
		{name: "immutable ID replacement", scenario: "replacement", diagnostic: "immutable ID changed"},
		{name: "published port change", scenario: "port-change", diagnostic: "published endpoint changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			bin := t.TempDir()
			counter := filepath.Join(t.TempDir(), "inspect-count")
			writeExecutable(t, filepath.Join(bin, "docker"), fakeMemcacheDocker)
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				`profile="profile-under-test"`,
				`run_bounded() { local seconds="$1"; shift; test "${seconds}" -gt 0; "$@"; }`,
				inspect,
				discover,
				assertBinding,
				`resource_id="projects/demo/locations/us/instances/cache"`,
				`binding="$(discover_exact_memcache_container "${resource_id}")"`,
				`IFS=$'\t' read -r container_id endpoint <<<"${binding}"`,
				`assert_exact_memcache_container_binding "${container_id}" "${resource_id}" "${endpoint}"`,
			}, "\n")
			command := exec.Command("bash", "-c", harness)
			command.Env = append(os.Environ(),
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"FAKE_DOCKER_SCENARIO="+test.scenario,
				"FAKE_DOCKER_COUNTER="+counter,
			)
			output, err := command.CombinedOutput()
			if test.wantOK && err != nil {
				t.Fatalf("exact-owned binding rejected: %v\n%s", err, output)
			}
			if !test.wantOK && (err == nil || !strings.Contains(string(output), test.diagnostic)) {
				t.Fatalf("mutation was accepted or diagnostic missing: err=%v\n%s", err, output)
			}
		})
	}
}

func TestMemcacheAPIEndpointRejectsUnrelatedLoopbackService(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	discovery := shellFunction(t, source, "discover_sdk_memcache_endpoint")
	for _, test := range []struct {
		name        string
		apiEndpoint string
		wantOK      bool
	}{
		{name: "exact published endpoint", apiEndpoint: "127.0.0.1:11222", wantOK: true},
		{name: "unrelated loopback service", apiEndpoint: "127.0.0.1:11223"},
	} {
		t.Run(test.name, func(t *testing.T) {
			bin := t.TempDir()
			writeExecutable(t, filepath.Join(bin, "curl"), fakeMemcacheAPICurl)
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				`run_bounded() { local seconds="$1"; shift; test "${seconds}" -gt 0; "$@"; }`,
				`work_dir="$1"`,
				`gateway="http://127.0.0.1:19000"`,
				`sdk_import="projects/demo/locations/us/instances/cache"`,
				discovery,
				`discover_sdk_memcache_endpoint "127.0.0.1:11222"`,
			}, "\n")
			command := exec.Command("bash", "-c", harness, "api-endpoint-test", t.TempDir())
			command.Env = append(os.Environ(),
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"FAKE_API_ENDPOINT="+test.apiEndpoint,
			)
			output, err := command.CombinedOutput()
			if test.wantOK && err != nil {
				t.Fatalf("exact API endpoint rejected: %v\n%s", err, output)
			}
			if !test.wantOK && (err == nil ||
				!strings.Contains(string(output), "does not match exact-owned Docker endpoint")) {
				t.Fatalf("unrelated loopback endpoint accepted: err=%v\n%s", err, output)
			}
		})
	}
}

func TestMemcacheDestroyAssertionIsExactAndFailClosed(t *testing.T) {
	source := readShellScript(t, "memcache-integration.sh")
	assertion := shellFunction(t, source, "assert_no_memcache_container")
	if !strings.Contains(assertion, "run_bounded 10 docker ps -aq") {
		t.Fatal("exact cleanup inventory is not deadline-bounded")
	}
	for _, filter := range []string{
		`--filter "label=managed-by=minisky"`,
		`--filter "label=minisky.profile=${profile}"`,
		`--filter "label=minisky.service=memorystore-memcached"`,
		`--filter "label=minisky.resource=${resource_id}"`,
	} {
		if !strings.Contains(assertion, filter) {
			t.Errorf("destroy assertion lacks exact filter %q", filter)
		}
	}
	destroy := strings.Index(source, `terraform_bounded -chdir="${terraform_dir}" destroy`)
	call := -1
	if destroy >= 0 {
		if relative := strings.Index(source[destroy:], "\nassert_no_memcache_container \"${tf_backend_id}\"\n"); relative >= 0 {
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
		`run_bounded() { local seconds="$1"; shift; test "${seconds}" -gt 0; "$@"; }`,
		assertion,
		`assert_no_memcache_container "projects/demo/locations/us/instances/cache"`,
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

func waitForFileOrProcessExit(path string, processExit <-chan error, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("readiness timeout must be positive")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect readiness marker: %w", err)
		}
		select {
		case err := <-processExit:
			return fmt.Errorf("process exited before readiness: %v", err)
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("readiness marker %q was not created within %s", path, timeout)
		}
	}
}

const fakeMemcacheDocker = `#!/usr/bin/env bash
set -Eeuo pipefail
exact_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
replacement_id="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
scenario="${FAKE_DOCKER_SCENARIO}"
case "${1:-}" in
  ps)
    case "${scenario}" in
      zero) exit 0 ;;
      multiple) printf '%s\n%s\n' "${exact_id}" "${replacement_id}"; exit 0 ;;
      inventory-failure) echo "inventory unavailable" >&2; exit 72 ;;
      *) printf '%s\n' "${exact_id}"; exit 0 ;;
    esac
    ;;
  inspect)
    if [[ "${scenario}" == "inspect-failure" ]]; then
      echo "inspect unavailable" >&2
      exit 73
    fi
    count=0
    if [[ -f "${FAKE_DOCKER_COUNTER}" ]]; then
      read -r count <"${FAKE_DOCKER_COUNTER}"
    fi
    count=$((count + 1))
    printf '%s\n' "${count}" >"${FAKE_DOCKER_COUNTER}"
    output_id="${exact_id}"
    port="11222"
    managed_by="minisky"
    running="true"
    status="running"
    if [[ "${scenario}" == "replacement" && "${count}" -gt 1 ]]; then
      output_id="${replacement_id}"
    fi
    if [[ "${scenario}" == "port-change" && "${count}" -gt 1 ]]; then
      port="11223"
    fi
    if [[ "${scenario}" == "foreign-labels" ]]; then
      managed_by="foreign"
    fi
    if [[ "${scenario}" == "stopped" ]]; then
      running="false"
      status="exited"
    fi
    printf '"%s"\t%s\t"%s"\t{"managed-by":"%s","minisky.profile":"profile-under-test","minisky.service":"memorystore-memcached","minisky.resource":"projects/demo/locations/us/instances/cache"}\t[{"HostIp":"127.0.0.1","HostPort":"%s"}]\n' \
      "${output_id}" "${running}" "${status}" "${managed_by}" "${port}"
    ;;
  *)
    echo "unexpected docker command: $*" >&2
    exit 74
    ;;
esac
`

const fakeMemcacheAPICurl = `#!/usr/bin/env bash
set -Eeuo pipefail
output=""
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == "--output" ]]; then
    output="$2"
    shift 2
  else
    shift
  fi
done
if [[ -z "${output}" ]]; then
  echo "missing --output" >&2
  exit 75
fi
printf '{"name":"projects/demo/locations/us/instances/cache","state":"READY","discoveryEndpoint":"%s","memcacheNodes":[{"host":"127.0.0.1","port":%s}]}\n' \
  "${FAKE_API_ENDPOINT}" "${FAKE_API_ENDPOINT##*:}" >"${output}"
`

func writeExecutable(t *testing.T, path, source string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(source), 0o700); err != nil {
		t.Fatal(err)
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
