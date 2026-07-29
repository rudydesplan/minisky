package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRedisIntegrationLifecycleContract(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	for _, required := range []string{
		`MINISKY_REDIS_INTEGRATION`,
		`shared_lock="/tmp/minisky-net-integration.lock"`,
		`phase_lock="/tmp/minisky-redis-integration.lock"`,
		`run_sdk create`,
		`run_sdk verify`,
		`assert_redis_value`,
		`remove_exact_redis_container`,
		`terraform_dir="${repository_root}/terraform/redis"`,
		`terraform_bounded -chdir="${terraform_dir}" apply`,
		`terraform_bounded -chdir="${terraform_dir}" destroy`,
		`assert_no_drift`,
		`state --profile "${profile}" export`,
		`assert_export_boundary`,
		`assert_exact_cleanup`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("Redis integration lifecycle lacks %q", required)
		}
	}
	if count := strings.Count(source, "\nassert_no_drift\n"); count != 2 {
		t.Errorf("Redis Terraform no-drift assertion count=%d, want immediate and post-restart", count)
	}
	for _, forbidden := range []string{"-target=", "redis-cli", "docker system prune", "docker volume prune"} {
		if strings.Contains(source, forbidden) {
			t.Errorf("Redis integration contains forbidden broad or dependency behavior %q", forbidden)
		}
	}
}

func TestRedisRestartAndReplacementFollowGracefulShutdownContract(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	restartSection := source[strings.Index(source, `restarted_container_id`):]
	if !strings.Contains(restartSection, `if [[ "${restarted_container_id}" == "${sdk_container_id}"`) {
		t.Fatal("process restart does not require a freshly reconciled Redis container")
	}
	removeIndex := strings.Index(restartSection, `remove_exact_redis_container`)
	stopIndex := strings.Index(restartSection, `stop_minisky`)
	if removeIndex < 0 || stopIndex < 0 || removeIndex > stopIndex {
		t.Fatal("exact container replacement is not initiated before graceful shutdown")
	}
}

func TestRedisRunBoundedPreservesCommandStandardInput(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	runBounded := shellFunction(t, source, "run_bounded")
	script := "in_cleanup=1\n" + runBounded + `
run_bounded 5 python3 - <<'PY'
print("bounded-stdin-reached-child")
PY
`
	output, err := exec.Command("bash", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("run_bounded stdin probe failed: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "bounded-stdin-reached-child" {
		t.Fatalf("run_bounded discarded command stdin: got %q", got)
	}

	t.Run("signal exit maps to shell status", func(t *testing.T) {
		output, err := exec.Command("bash", "-c", "in_cleanup=1\n"+runBounded+`
set +e
run_bounded 5 bash -c 'kill -TERM $$'
status=$?
set -e
printf '%s\n' "${status}"
`).CombinedOutput()
		if err != nil {
			t.Fatalf("signal mapping probe failed: %v\n%s", err, output)
		}
		if got := strings.TrimSpace(string(output)); got != "143" {
			t.Fatalf("SIGTERM status=%q, want 143", got)
		}
	})

	t.Run("timeout kills descendants", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("POSIX process groups are not available on Windows")
		}
		pidFile := filepath.Join(t.TempDir(), "descendant.pid")
		command := exec.Command("bash", "-c", "in_cleanup=1\n"+runBounded+`
set +e
run_bounded 0.2 bash -c 'trap "exit 0" TERM; bash -c '"'"'trap "" TERM; sleep 30'"'"' & echo "$!" >"$1"; wait' _ "$1"
status=$?
set -e
printf '%s\n' "${status}"
`, "probe", pidFile)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("timeout probe failed: %v\n%s", err, output)
		}
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		if got := lines[len(lines)-1]; got != "124" {
			t.Fatalf("timeout status=%q, want 124", got)
		}
		rawPID, err := os.ReadFile(pidFile)
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			err = exec.Command("kill", "-0", strconv.Itoa(pid)).Run()
			if err != nil {
				break
			}
			if time.Now().After(deadline) {
				_ = exec.Command("kill", "-KILL", strconv.Itoa(pid)).Run()
				t.Fatalf("timed-out descendant %d survived process-group cleanup", pid)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	for _, signalTest := range []struct {
		name       string
		signal     string
		wantStatus string
	}{
		{name: "external SIGTERM", signal: "TERM", wantStatus: "143"},
		{name: "external SIGINT", signal: "INT", wantStatus: "130"},
	} {
		t.Run(signalTest.name+" kills descendants", func(t *testing.T) {
			if runtime.GOOS == "windows" {
				t.Skip("POSIX process groups are not available on Windows")
			}
			root := t.TempDir()
			wrapperPIDPath := filepath.Join(root, "wrapper.pid")
			descendantPIDPath := filepath.Join(root, "descendant.pid")
			readyPath := filepath.Join(root, "ready")
			statusPath := filepath.Join(root, "status")
			child := filepath.Join(root, "child")
			writeExecutable(t, child, `#!/usr/bin/env bash
set -eu
trap '' INT TERM
bash -c 'trap "" INT TERM; while :; do sleep 1; done' &
printf '%s\n' "$!" >"$2"
printf '%s\n' "${PPID}" >"$1"
touch "$3"
wait
`)
			script := "in_cleanup=1\n" + runBounded + `
set +e
run_bounded 30 "$1" "$2" "$3" "$4"
status=$?
set -e
printf '%s\n' "${status}" >"$5"
`
			command := exec.Command("bash", "-c", script, "probe",
				child, wrapperPIDPath, descendantPIDPath, readyPath, statusPath)
			outputFile, err := os.Create(filepath.Join(root, "output"))
			if err != nil {
				t.Fatal(err)
			}
			defer outputFile.Close()
			command.Stdout = outputFile
			command.Stderr = outputFile
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if command.Process != nil {
					_ = command.Process.Kill()
				}
			})
			waitForTestPath(t, readyPath, 2*time.Second)
			wrapperPID := readTestPID(t, wrapperPIDPath)
			descendantPID := readTestPID(t, descendantPIDPath)
			if err := exec.Command("kill", "-"+signalTest.signal, strconv.Itoa(wrapperPID)).Run(); err != nil {
				t.Fatalf("signal wrapper %d: %v", wrapperPID, err)
			}
			waitDone := make(chan error, 1)
			go func() { waitDone <- command.Wait() }()
			select {
			case err := <-waitDone:
				if err != nil {
					t.Fatalf("wrapper probe shell failed: %v", err)
				}
			case <-time.After(3 * time.Second):
				_ = exec.Command("kill", "-KILL", strconv.Itoa(descendantPID)).Run()
				t.Fatal("externally interrupted wrapper did not return promptly")
			}
			if got := strings.TrimSpace(string(mustReadFile(t, statusPath))); got != signalTest.wantStatus {
				t.Fatalf("external %s status=%q, want %s", signalTest.signal, got, signalTest.wantStatus)
			}
			assertTestProcessGone(t, descendantPID, 2*time.Second)
		})
	}
}

func TestRedisInitialNetworkInspectionIsBoundedAndExact(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Redis integration requires POSIX process groups")
	}
	source := readShellScript(t, "redis-integration.sh")
	runBounded := shellFunction(t, source, "run_bounded")
	assertAbsent := shellFunction(t, source, "assert_initial_network_absent")
	bin := t.TempDir()
	writeExecutable(t, filepath.Join(bin, "docker"), `#!/usr/bin/env bash
set -eu
[[ "$*" == "network inspect minisky-net" ]]
case "${FAKE_NETWORK_MODE}" in
  absent)
    printf '%s\n' '[]'
    printf '%s\n' 'Error response from daemon: network minisky-net not found' >&2
    exit 1
    ;;
  daemon-error)
    printf '%s\n' 'permission denied opening Docker socket' >&2
    exit 1
    ;;
  ambiguous-error)
    printf '%s\n' 'permission denied: network minisky-net not found' >&2
    exit 1
    ;;
  hang)
    sleep 30
    ;;
esac
`)
	base := "in_cleanup=0\noverall_timeout_seconds=1\noverall_deadline_epoch=$(($(date +%s)+1))\n" +
		runBounded + "\n" + assertAbsent + "\nassert_initial_network_absent\n"
	for _, test := range []struct {
		name   string
		mode   string
		wantOK bool
	}{
		{name: "explicit not found", mode: "absent", wantOK: true},
		{name: "daemon error", mode: "daemon-error"},
		{name: "not-found text in another error", mode: "ambiguous-error"},
		{name: "bounded hang", mode: "hang"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command("bash", "-c", base)
			command.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"FAKE_NETWORK_MODE="+test.mode,
			)
			start := time.Now()
			output, err := command.CombinedOutput()
			elapsed := time.Since(start)
			if test.wantOK && err != nil {
				t.Fatalf("explicit absence rejected: %v\n%s", err, output)
			}
			if !test.wantOK && err == nil {
				t.Fatalf("%s was accepted:\n%s", test.name, output)
			}
			if test.mode == "daemon-error" && !strings.Contains(string(output), "permission denied") {
				t.Fatalf("daemon diagnostic was lost:\n%s", output)
			}
			if elapsed > 2500*time.Millisecond {
				t.Fatalf("%s inspection was not bounded: %s", test.name, elapsed)
			}
		})
	}
}

func TestRedisOwnershipConflictDispositionIsExact(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	classifier := shellFunction(t, source, "require_ownership_conflict")
	tests := []struct {
		name     string
		status   string
		message  string
		expected string
		wantOK   bool
	}{
		{name: "exact network conflict", status: "1", message: `Docker resource ownership conflict: network "minisky-net" exists but is not owned`, expected: `Docker resource ownership conflict: network "minisky-net" exists but is not owned`, wantOK: true},
		{name: "exact volume conflict", status: "1", message: `refusing to adopt pre-existing Redis volume "expected"`, expected: `refusing to adopt pre-existing Redis volume "expected"`, wantOK: true},
		{name: "exact container conflict", status: "1", message: `Redis container "expected" already exists`, expected: `Redis container "expected" already exists`, wantOK: true},
		{name: "timeout", status: "124", message: `refusing to adopt pre-existing Redis volume "expected"`, expected: `refusing to adopt pre-existing Redis volume "expected"`},
		{name: "readiness success", status: "0", expected: `ownership conflict`},
		{name: "unrelated startup error", status: "1", message: "listen tcp: address already in use", expected: `ownership conflict`},
		{name: "signal", status: "143", message: `Redis container "expected" already exists`, expected: `Redis container "expected" already exists`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "probe.log")
			if err := os.WriteFile(logPath, []byte(test.message), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("bash", "-c", classifier+`
require_ownership_conflict probe "$1" "$2" "$3"
			`, "probe", test.status, test.expected, logPath)
			output, err := command.CombinedOutput()
			if test.wantOK && err != nil {
				t.Fatalf("exact ownership conflict rejected: %v\n%s", err, output)
			}
			if !test.wantOK && err == nil {
				t.Fatalf("non-conflict disposition accepted: %s", output)
			}
		})
	}
}

func TestRedisDockerEndpointSelectionIsCanonicalAndContextFree(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Redis integration requires a POSIX Unix-socket Docker endpoint")
	}
	source := readShellScript(t, "redis-integration.sh")
	configure := shellFunction(t, source, "configure_docker_endpoint")
	runBounded := shellFunction(t, source, "run_bounded")
	bin := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "docker.log")
	writeExecutable(t, filepath.Join(bin, "docker"), `#!/usr/bin/env bash
set -eu
printf 'context=%s host=%s args=%s\n' "${DOCKER_CONTEXT:-}" "${DOCKER_HOST:-}" "$*" >>"${DOCKER_LOG}"
case "$*" in
  "--context selected context inspect selected --format "*)
    printf '%s\n' "unix:///tmp/../tmp/minisky-docker.sock"
    ;;
  "info")
    ;;
  *)
    exit 91
    ;;
esac
`)
	script := "set -e\nin_cleanup=1\n" + runBounded + "\n" + configure + `
configure_docker_endpoint
docker info
printf 'resolved=%s context=%s\n' "${DOCKER_HOST}" "${DOCKER_CONTEXT:-}"
`
	command := exec.Command("bash", "-c", script)
	command.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"DOCKER_LOG="+logPath,
		"DOCKER_CONTEXT=selected",
		"DOCKER_HOST=",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("context-only endpoint resolution failed: %v\n%s", err, output)
	}
	canonicalTmp, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		t.Fatal(err)
	}
	expectedHost := "unix://" + filepath.Join(canonicalTmp, "minisky-docker.sock")
	if !strings.Contains(string(output), "resolved="+expectedHost+" context=") {
		t.Fatalf("endpoint was not canonicalized with context cleared:\n%s", output)
	}
	logged, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logged), "context= host="+expectedHost+" args=info") {
		t.Fatalf("post-resolution Docker command did not use one explicit endpoint:\n%s", logged)
	}

	command = exec.Command("bash", "-c", script)
	command.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"DOCKER_LOG="+logPath,
		"DOCKER_CONTEXT=selected",
		"DOCKER_HOST=unix:///other.sock",
	)
	if output, err = command.CombinedOutput(); err == nil {
		t.Fatalf("ambiguous DOCKER_CONTEXT/DOCKER_HOST was accepted:\n%s", output)
	}
}

func TestRedisInvalidTimeoutDoesNotLeakLocks(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	root := t.TempDir()
	shared := filepath.Join(root, "shared.lock")
	phase := filepath.Join(root, "phase.lock")
	source = strings.ReplaceAll(source, "/tmp/minisky-net-integration.lock", shared)
	source = strings.ReplaceAll(source, "/tmp/minisky-redis-integration.lock", phase)
	script := filepath.Join(root, "redis-integration.sh")
	if err := os.WriteFile(script, []byte(source), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/bash", script)
	command.Env = append(os.Environ(),
		"MINISKY_REDIS_INTEGRATION=1",
		"MINISKY_REDIS_TIMEOUT_SECONDS=invalid",
	)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "must be a positive integer") {
		t.Fatalf("invalid timeout result=%v, want validation failure:\n%s", err, output)
	}
	for _, lock := range []string{shared, phase} {
		if _, statErr := os.Stat(lock); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("invalid timeout leaked lock %s: %v", lock, statErr)
		}
	}

	t.Run("phase collision releases shared lock", func(t *testing.T) {
		if err := os.Mkdir(phase, 0o700); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Remove(phase) })
		bin := t.TempDir()
		writeExecutable(t, filepath.Join(bin, "docker"), `#!/usr/bin/env bash
set -eu
[[ "$*" == "info" ]]
`)
		command := exec.Command("/bin/bash", script)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"DOCKER_HOST=unix:///fake/docker.sock",
			"DOCKER_CONTEXT=",
			"MINISKY_REDIS_INTEGRATION=1",
			"MINISKY_REDIS_TIMEOUT_SECONDS=30",
		)
		output, err := command.CombinedOutput()
		if err == nil || !strings.Contains(string(output), "Another MiniSky Redis integration is active") {
			t.Fatalf("phase collision result=%v, want lock refusal:\n%s", err, output)
		}
		if _, statErr := os.Stat(shared); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("phase collision leaked shared lock: %v", statErr)
		}
		if _, statErr := os.Stat(phase); statErr != nil {
			t.Fatalf("pre-existing phase lock was removed: %v", statErr)
		}
	})
}

func TestRedisMiniSkyStartsFromIsolatedDirectory(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	runBounded := shellFunction(t, source, "run_bounded")
	runMiniSky := shellFunction(t, source, "run_minisky_bounded")
	repository := t.TempDir()
	sentinel := filepath.Join(repository, ".minisky", "sentinel")
	if err := os.MkdirAll(filepath.Dir(sentinel), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentinel, []byte("repository-state"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeDir := t.TempDir()
	binary := filepath.Join(t.TempDir(), "minisky")
	pwdLog := filepath.Join(t.TempDir(), "pwd")
	writeExecutable(t, binary, `#!/usr/bin/env bash
set -eu
printf '%s|%s|%s\n' "${PWD}" "${DOCKER_HOST:-}" "${DOCKER_CONTEXT:-}" >"${PWD_LOG}"
if [[ -f .minisky/sentinel ]]; then
  rm .minisky/sentinel
fi
`)
	script := "in_cleanup=1\n" + runBounded + "\n" + runMiniSky + `
run_minisky_bounded 5 start
`
	command := exec.Command("bash", "-c", script)
	command.Dir = repository
	command.Env = append(os.Environ(),
		"minisky_runtime_dir="+runtimeDir,
		"minisky_binary="+binary,
		"PWD_LOG="+pwdLog,
		"DOCKER_HOST=unix:///canonical.sock",
		"DOCKER_CONTEXT=",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated MiniSky probe failed: %v\n%s", err, output)
	}
	pwd, err := os.ReadFile(pwdLog)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.TrimSpace(string(pwd)), runtimeDir+"|unix:///canonical.sock|"; got != want {
		t.Fatalf("MiniSky environment=%q, want %q", got, want)
	}
	if got, err := os.ReadFile(sentinel); err != nil || string(got) != "repository-state" {
		t.Fatalf("repository sentinel changed: data=%q err=%v", got, err)
	}
}

func TestRedisCleanupUsesOnlyTrackedImmutableResources(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	runBounded := shellFunction(t, source, "run_bounded")
	cleanupTracked := shellFunction(t, source, "cleanup_tracked_resources")
	bin := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "docker.log")
	writeExecutable(t, filepath.Join(bin, "docker"), `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${DOCKER_LOG}"
case "$*" in
  "inspect --type container container-id")
    printf '%s\n' '[{"Id":"container-id","Config":{"Labels":{"managed-by":"minisky","minisky.profile":"profile","minisky.service":"memorystore-redis"}}}]'
    ;;
  "volume inspect volume-name")
    if [[ "${FAKE_DOCKER_MODE:-}" == "volume-mismatch" ]]; then
      printf '%s\n' '[{"Name":"volume-name","CreatedAt":"changed","Mountpoint":"/changed","Labels":{"managed-by":"minisky","minisky.profile":"profile","minisky.service":"memorystore-redis"}}]'
    else
      printf '%s\n' '[{"Name":"volume-name","CreatedAt":"created","Mountpoint":"/mount","Labels":{"managed-by":"minisky","minisky.profile":"profile","minisky.service":"memorystore-redis"}}]'
    fi
    ;;
  "network inspect network-id")
    if [[ "${FAKE_DOCKER_MODE:-}" == "network-inspect-failure" ]]; then
      printf '%s\n' "daemon inventory unavailable" >&2
      exit 5
    fi
    if [[ "${FAKE_DOCKER_MODE:-}" == "network-ambiguous-not-found" ]]; then
      printf '%s\n' "permission denied: network network-id not found" >&2
      exit 1
    fi
    printf '%s\n' '[{"Id":"network-id","Labels":{"managed-by":"minisky","minisky.profile":"profile"}}]'
    ;;
  "rm -f container-id"|"volume rm volume-name"|"network rm network-id")
    ;;
  *)
    printf 'unexpected broad or untracked Docker command: %s\n' "$*" >&2
    exit 99
    ;;
esac
`)
	base := "set -u\nin_cleanup=1\nprofile=profile\n" + runBounded + "\n" + cleanupTracked
	t.Run("empty arrays are nounset safe", func(t *testing.T) {
		command := exec.Command("/bin/bash", "-c", base+`
tracked_container_ids=()
tracked_volumes=()
tracked_network_ids=()
cleanup_tracked_resources
`)
		command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "DOCKER_LOG="+logPath)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("empty tracked arrays failed under nounset: %v\n%s", err, output)
		}
	})

	t.Run("exact tracked resources only", func(t *testing.T) {
		command := exec.Command("/bin/bash", "-c", base+`
tracked_container_ids=(container-id)
tracked_volumes=('volume-name|created|/mount')
tracked_network_ids=(network-id)
cleanup_tracked_resources
`)
		command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "DOCKER_LOG="+logPath)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("tracked cleanup failed: %v\n%s", err, output)
		}
		logged, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, exact := range []string{"rm -f container-id", "volume rm volume-name", "network rm network-id"} {
			if !strings.Contains(string(logged), exact) {
				t.Errorf("tracked cleanup omitted %q:\n%s", exact, logged)
			}
		}
		for _, broad := range []string{"ps ", "volume ls", "network ls", "prune"} {
			if strings.Contains(string(logged), broad) {
				t.Errorf("tracked cleanup used broad inventory %q:\n%s", broad, logged)
			}
		}
	})

	t.Run("volume identity drift refuses deletion", func(t *testing.T) {
		localLog := filepath.Join(t.TempDir(), "docker.log")
		command := exec.Command("/bin/bash", "-c", base+`
tracked_container_ids=()
tracked_volumes=('volume-name|created|/mount')
tracked_network_ids=()
cleanup_tracked_resources
`)
		command.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"DOCKER_LOG="+localLog,
			"FAKE_DOCKER_MODE=volume-mismatch",
		)
		output, err := command.CombinedOutput()
		if err == nil {
			t.Fatalf("drifted volume was accepted:\n%s", output)
		}
		logged, readErr := os.ReadFile(localLog)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(string(logged), "volume rm") {
			t.Fatalf("drifted volume reached deletion:\n%s", logged)
		}
	})

	for _, mode := range []string{"network-inspect-failure", "network-ambiguous-not-found"} {
		t.Run(mode+" is diagnostic", func(t *testing.T) {
			localLog := filepath.Join(t.TempDir(), "docker.log")
			command := exec.Command("/bin/bash", "-c", base+`
tracked_container_ids=()
tracked_volumes=()
tracked_network_ids=(network-id)
cleanup_tracked_resources
`)
			command.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"DOCKER_LOG="+localLog,
				"FAKE_DOCKER_MODE="+mode,
			)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("unknown network state was accepted:\n%s", output)
			}
			if !strings.Contains(string(output), "Unable to inspect tracked Redis network") {
				t.Fatalf("network inspection diagnostic was lost:\n%s", output)
			}
			logged, readErr := os.ReadFile(localLog)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Contains(string(logged), "network rm") {
				t.Fatalf("unknown network state reached deletion:\n%s", logged)
			}
		})
	}
}

func TestRedisForeignNetworkUnknownStateFailsCleanup(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	runBounded := shellFunction(t, source, "run_bounded")
	removeForeign := shellFunction(t, source, "remove_foreign_network")
	bin := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "docker.log")
	writeExecutable(t, filepath.Join(bin, "docker"), `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${DOCKER_LOG}"
if [[ "$*" == "network inspect foreign-network-id" ]]; then
  printf '%s\n' "malformed or unavailable network inventory" >&2
  exit 5
fi
exit 99
`)
	command := exec.Command("bash", "-c", "set -e\nin_cleanup=1\nforeign_network_id=foreign-network-id\ngate_id=gate\n"+runBounded+"\n"+removeForeign+`
remove_foreign_network
`)
	command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "DOCKER_LOG="+logPath)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("unknown foreign-network state was accepted:\n%s", output)
	}
	if !strings.Contains(string(output), "preserving unknown state") ||
		!strings.Contains(string(output), "malformed or unavailable network inventory") {
		t.Fatalf("unknown-state diagnostics were lost:\n%s", output)
	}
	logged, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if strings.Contains(string(logged), "network rm") {
		t.Fatalf("unknown foreign network reached deletion:\n%s", logged)
	}
}

func TestRedisStartupTracksNetworkBeforeReadiness(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Redis integration requires POSIX process control")
	}
	source := readShellScript(t, "redis-integration.sh")
	runBounded := shellFunction(t, source, "run_bounded")
	waitForPID := shellFunction(t, source, "wait_for_pid_exit")
	stopMiniSky := shellFunction(t, source, "stop_minisky")
	trackPending := shellFunction(t, source, "track_current_network_if_present")
	trackCurrent := shellFunction(t, source, "track_current_network")
	cleanupTracked := shellFunction(t, source, "cleanup_tracked_resources")
	startMiniSky := shellFunction(t, source, "start_minisky")
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	networkState := filepath.Join(root, "network-present")
	dockerLog := filepath.Join(root, "docker.log")
	writeExecutable(t, filepath.Join(bin, "docker"), `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${DOCKER_LOG}"
case "$*" in
  "network inspect minisky-net"|"network inspect network-created-before-ready")
    if [[ ! -f "${NETWORK_STATE}" ]]; then
      printf '%s\n' 'Error response from daemon: network minisky-net not found' >&2
      exit 1
    fi
    if [[ "${FAKE_NETWORK_OWNER}" == "foreign" ]]; then
      printf '%s\n' '[{"Id":"network-created-before-ready","Labels":{"managed-by":"someone-else","minisky.profile":"other"}}]'
    else
      printf '%s\n' '[{"Id":"network-created-before-ready","Labels":{"managed-by":"minisky","minisky.profile":"profile"}}]'
    fi
    ;;
  "network rm network-created-before-ready")
    rm -f "${NETWORK_STATE}"
    ;;
  *)
    printf 'unexpected docker command: %s\n' "$*" >&2
    exit 99
    ;;
esac
`)
	writeExecutable(t, filepath.Join(bin, "curl"), `#!/usr/bin/env bash
if [[ "${FAKE_READY:-}" == "1" ]]; then
  exit 0
fi
exit 1
`)
	minisky := filepath.Join(bin, "minisky")
	writeExecutable(t, minisky, `#!/usr/bin/env bash
set -eu
touch "${NETWORK_STATE}"
if [[ "${FAKE_READY:-}" == "1" ]]; then
  trap 'exit 0' TERM
  while :; do sleep 1; done
fi
sleep 0.6
exit 1
`)
	base := `set -eu
in_cleanup=1
profile=profile
initial_network_absence_proven=1
tracked_network_ids=()
tracked_container_ids=()
tracked_volumes=()
minisky_pid=""
minisky_runtime_dir="$1"
minisky_binary="$2"
work_dir="$3"
gateway=http://127.0.0.1:1
api_port=1
ui_port=2
` + runBounded + "\n" + waitForPID + "\n" + stopMiniSky + "\n" +
		trackPending + "\n" + trackCurrent + "\n" + cleanupTracked + "\n" + startMiniSky + `
if start_minisky; then
  start_status=0
else
  start_status=$?
fi
stop_minisky || true
if cleanup_tracked_resources; then
  cleanup_status=0
else
  cleanup_status=$?
fi
printf 'start=%s cleanup=%s tracked=%s\n' "${start_status}" "${cleanup_status}" "${tracked_network_ids[*]:-}"
`
	for _, test := range []struct {
		name          string
		owner         string
		ready         bool
		wantStart     string
		wantRemoved   bool
		wantTrackedID bool
	}{
		{name: "owned network before readiness is tracked and cleaned", owner: "owned", wantStart: "1", wantRemoved: true, wantTrackedID: true},
		{name: "owned network after readiness is tracked and cleaned", owner: "owned", ready: true, wantStart: "0", wantRemoved: true, wantTrackedID: true},
		{name: "foreign network is preserved", owner: "foreign", wantStart: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_ = os.Remove(networkState)
			if err := os.WriteFile(dockerLog, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			runtimeDir := t.TempDir()
			workDir := t.TempDir()
			ready := "0"
			if test.ready {
				ready = "1"
			}
			command := exec.Command("/bin/bash", "-c", base, "probe", runtimeDir, minisky, workDir)
			command.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"DOCKER_LOG="+dockerLog,
				"NETWORK_STATE="+networkState,
				"FAKE_NETWORK_OWNER="+test.owner,
				"FAKE_READY="+ready,
			)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("startup cleanup probe failed: %v\n%s\nlog:\n%s", err, output, mustReadFile(t, filepath.Join(workDir, "minisky.log")))
			}
			if !strings.Contains(string(output), "start="+test.wantStart+" ") {
				t.Fatalf("startup status mismatch, want %s:\n%s\nlog:\n%s", test.wantStart, output, mustReadFile(t, filepath.Join(workDir, "minisky.log")))
			}
			_, statErr := os.Stat(networkState)
			removed := errors.Is(statErr, os.ErrNotExist)
			if removed != test.wantRemoved {
				t.Fatalf("network removed=%v, want %v:\n%s", removed, test.wantRemoved, output)
			}
			if test.wantTrackedID && !strings.Contains(string(output), "tracked=network-created-before-ready") {
				t.Fatalf("owned pre-readiness network ID was not tracked:\n%s\nlog:\n%s", output, mustReadFile(t, filepath.Join(workDir, "minisky.log")))
			}
			logged := mustReadFile(t, dockerLog)
			if !test.wantRemoved && strings.Contains(string(logged), "network rm") {
				t.Fatalf("foreign network reached cleanup:\n%s", logged)
			}
		})
	}
}

func TestRedisPreflightRejectsExistingSameProfileResources(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	runBounded := shellFunction(t, source, "run_bounded")
	preflight := shellFunction(t, source, "assert_profile_empty")
	bin := t.TempDir()
	writeExecutable(t, filepath.Join(bin, "docker"), `#!/usr/bin/env bash
set -eu
case "$*" in
  "ps -aq --no-trunc --filter label=managed-by=minisky --filter label=minisky.profile=profile --filter label=minisky.service=memorystore-redis")
    printf '%s\n' "preexisting-container"
    ;;
  "volume ls -q --filter label=managed-by=minisky --filter label=minisky.profile=profile --filter label=minisky.service=memorystore-redis")
    printf '%s\n' "preexisting-volume"
    ;;
  *)
    exit 99
    ;;
esac
`)
	command := exec.Command("bash", "-c", "in_cleanup=1\nprofile=profile\n"+runBounded+"\n"+preflight+`
assert_profile_empty
`)
	command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("same-profile preflight resources were accepted:\n%s", output)
	}
	if !strings.Contains(string(output), "profile is not empty") {
		t.Fatalf("same-profile refusal lacks diagnostic:\n%s", output)
	}
}

func TestRedisProtocolAndReplacementAreExact(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	protocol := shellFunction(t, source, "assert_redis_value")
	for _, required := range []string{
		"socket.create_connection",
		`b"SET"`,
		`b"GET"`,
		"connection.settimeout",
		"address.is_loopback",
	} {
		if !strings.Contains(protocol, required) {
			t.Errorf("hand-rolled bounded RESP evidence lacks %q", required)
		}
	}
	replacement := shellFunction(t, source, "remove_exact_redis_container")
	inspection := shellFunction(t, source, "inspect_exact_redis_binding")
	for _, required := range []string{
		"expected_container_id",
		"expected_volume",
		"inspect_exact_redis_binding",
		"docker rm -f",
	} {
		if !strings.Contains(replacement, required) {
			t.Errorf("exact Redis replacement lacks %q", required)
		}
	}
	for _, required := range []string{
		"docker inspect",
		`"managed-by"`,
		`"minisky.profile"`,
		`"minisky.service"`,
		`"minisky.resource"`,
		`"--appendonly", "yes"`,
		`get("NetworkMode") != "minisky-net"`,
	} {
		if !strings.Contains(inspection, required) {
			t.Errorf("exact Redis replacement inspection lacks %q", required)
		}
	}
	for _, required := range []string{
		`"minisky.generation"`,
		`"minisky.container-identity"`,
		`"minisky.container-name"`,
		`"minisky.image-id"`,
		`"minisky.volume-name"`,
		`"minisky.volume-identity"`,
		`"minisky.volume-provenance"`,
		`"sha256:" + hashlib.sha256`,
		`volume_name + "\0" + volume.get("CreatedAt") + "\0" + volume.get("Mountpoint")`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("exact Redis full provenance inspection lacks %q", required)
		}
	}
	if strings.Contains(replacement, "docker volume rm") {
		t.Fatal("container replacement removes the AOF volume")
	}
	if !strings.Contains(source, `sdk_binding="$(inspect_exact_redis_binding`) ||
		!strings.Contains(source, `restarted_binding="$(inspect_exact_redis_binding`) ||
		!strings.Contains(source, `replacement_binding="$(inspect_exact_redis_binding`) {
		t.Fatal("Redis binding inspection failures can be masked by read/here-string status")
	}
}

func TestRedisForeignResourcesAreRefusedAndPreserved(t *testing.T) {
	source := readShellScript(t, "redis-integration.sh")
	for _, required := range []string{
		"create_foreign_network",
		"assert_foreign_network_preserved",
		"create_foreign_volume",
		"assert_foreign_volume_preserved",
		"create_foreign_container",
		"assert_foreign_container_preserved",
		"require_ownership_conflict",
	} {
		if !strings.Contains(source, required) {
			t.Errorf("foreign-resource evidence lacks %q", required)
		}
	}
	for _, required := range []string{
		`read -r foreign_volume_resource foreign_volume_container foreign_volume_name`,
		`read -r foreign_container_resource foreign_container_name foreign_container_volume`,
		`Redis deterministic foreign-volume names are incomplete.`,
		`Redis deterministic foreign-container names are incomplete.`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("foreign-resource name parsing lacks %q", required)
		}
	}
	if strings.Contains(source, `read -r _`) {
		t.Fatal("foreign-resource name parsing discards fields through repeated underscore variables")
	}
	volumeAssertion := shellFunction(t, source, "assert_foreign_volume_preserved")
	for _, required := range []string{
		`items[0].get("Name") != expected_name`,
		`labels.get("managed-by") != "redis-gate-foreign"`,
		`labels.get("minisky.redis-gate-run") != gate`,
	} {
		if !strings.Contains(volumeAssertion, required) {
			t.Errorf("foreign-volume identity assertion lacks %q", required)
		}
	}
}

func TestRedisDedicatedTerraformFixturePinsProvider(t *testing.T) {
	root := filepath.Join("..", "terraform", "redis")
	main, err := os.ReadFile(filepath.Join(root, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(main)
	for _, required := range []string{
		`version = "7.41.0"`,
		`resource "google_redis_instance" "durability_gate"`,
		`redis_version           = "REDIS_7_2"`,
		`transit_encryption_mode = "DISABLED"`,
		`deletion_protection     = false`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("Redis Terraform fixture lacks %q", required)
		}
	}
	if _, err := os.Stat(filepath.Join(root, ".terraform.lock.hcl")); err != nil {
		t.Fatalf("Redis Terraform provider lock is missing: %v", err)
	}
}

func TestRedisGateIsOwnedByMakeAndCriticalWorkflow(t *testing.T) {
	makefile, err := os.ReadFile(filepath.Join("..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(makefile),
		"test-redis-integration:\n\tMINISKY_REDIS_INTEGRATION=1 ./scripts/redis-integration.sh") {
		t.Fatal("Makefile does not own the guarded Redis integration target")
	}
	workflow, err := os.ReadFile(filepath.Join("..", ".github", "workflows", "critical-integration.yml"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(workflow)
	start := strings.Index(source, "\n  redis-integration:\n")
	if start < 0 {
		t.Fatal("critical workflow lacks required redis-integration job")
	}
	job := source[start:]
	if next := strings.Index(job, "\n  enterprise-controls:"); next >= 0 {
		job = job[:next]
	}
	for _, required := range []string{
		"make test-redis-integration",
		"terraform -chdir=terraform/redis init",
		"docker info",
	} {
		if !strings.Contains(job, required) {
			t.Errorf("Redis critical job lacks %q", required)
		}
	}
	if strings.Contains(job, "continue-on-error") {
		t.Fatal("Redis critical job is permissive")
	}
}

func waitForTestPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readTestPID(t *testing.T, path string) int {
	t.Helper()
	pid, err := strconv.Atoi(strings.TrimSpace(string(mustReadFile(t, path))))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func assertTestProcessGone(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if err := exec.Command("kill", "-0", strconv.Itoa(pid)).Run(); err != nil {
			return
		}
		if time.Now().After(deadline) {
			_ = exec.Command("kill", "-KILL", strconv.Itoa(pid)).Run()
			t.Fatalf("process %d survived cleanup", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
