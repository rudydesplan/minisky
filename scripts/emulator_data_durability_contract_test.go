package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const (
	emulatorBoundaryScript = "storage-persistence-pubsub-session-integration.sh"
	emulatorBoundaryTarget = "test-storage-persistence-pubsub-session"
	emulatorBoundaryJob    = "storage-persistence-pubsub-session"
)

func TestStoragePersistencePubSubSessionScriptContract(t *testing.T) {
	source := readShellScript(t, emulatorBoundaryScript)
	for _, required := range []string{
		`MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION`,
		`MINISKY_DOCKER_EMULATOR_BOUNDARY_INTEGRATION=1`,
		`MINISKY_STORAGE_TEST_IMAGE`,
		`MINISKY_PUBSUB_TEST_IMAGE`,
		`@sha256:`,
		`docker info >/dev/null`,
		`TestStoragePersistenceAndPubSubSessionBoundaries`,
		`TestEnsureDurableEmulatorAllowsVendorLabelsButRejectsOwnershipAndMountMismatch`,
		`TestRemoveDurableEmulatorRequiresExactOwnershipBeforeCleanup`,
		`TestDurableEmulatorRuntimeDataIsExcludedFromMetadataExport`,
		`TestCleanupProfileSweepsOnlyExactOwnedDockerResources`,
		`--filter "label=managed-by=minisky"`,
		`--filter "label=minisky.profile=${target_profile}"`,
		`docker network ls -q`,
		`docker volume ls -q`,
		`docker network rm`,
		`docker volume rm`,
		`status=$?`,
		`exit "${status}"`,
	} {
		if !strings.Contains(source, required) {
			t.Errorf("script omits required durability contract %q", required)
		}
	}

	imagePattern := regexp.MustCompile(`[A-Za-z0-9./:_-]+@sha256:[a-f0-9]{64}`)
	if matches := imagePattern.FindAllString(source, -1); len(matches) < 2 {
		t.Fatalf("script has %d immutable emulator image references, want at least 2", len(matches))
	}
}

func TestStoragePersistencePubSubSessionScriptRefusesWithoutOptIn(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(bash, emulatorBoundaryScript)
	command.Env = withoutEnvironment(os.Environ(),
		"MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION",
	)
	output, err := command.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 2 {
		t.Fatalf("unguarded script exit=%v, want 2\n%s", err, output)
	}
	if !strings.Contains(string(output),
		"Refusing to start Storage persistence and Pub/Sub session-boundary integration") {
		t.Fatalf("unguarded refusal is not actionable:\n%s", output)
	}
}

func TestStoragePersistencePubSubSessionScriptRejectsMutableImagesBeforeMutation(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	bin := presenceOnlyRequiredCommands(t, "go", "python3")
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/bash\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(bash, emulatorBoundaryScript)
	command.Env = append(
		withoutEnvironment(os.Environ(),
			"PATH",
			"MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION",
			"MINISKY_STORAGE_TEST_IMAGE",
		),
		"PATH="+bin,
		"MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION=1",
		"MINISKY_STORAGE_TEST_IMAGE=fsouza/fake-gcs-server:latest",
	)
	output, err := command.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 2 {
		t.Fatalf("mutable image exit=%v, want 2\n%s", err, output)
	}
	if !strings.Contains(string(output), "requires immutable image@sha256 references") {
		t.Fatalf("mutable image rejection is not actionable:\n%s", output)
	}
}

func TestStoragePersistencePubSubSessionPublicPullBypassesStaleCredentialHelper(t *testing.T) {
	const fakeDocker = `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${FAKE_DOCKER_LOG}"

if [[ "${1:-}" == "--config" ]]; then
  config_dir="${2:?missing isolated config}"
  shift 2
  [[ "${1:-}" == "--host" ]]
  [[ "${2:-}" == "unix:///fake/docker.sock" ]]
  shift 2
  [[ "${1:-}" == "pull" ]]
  [[ "$*" == *"gcr.io/google.com/cloudsdktool/cloud-sdk:emulators@sha256:"* ]]
  [[ "$(<"${config_dir}/config.json")" == '{"auths":{}}' ]]
  if [[ "${FAKE_ANONYMOUS_PULL_FAIL:-0}" == "1" ]]; then
    printf '%s\n' "fake anonymous registry unavailable" >&2
    exit 71
  fi
  printf '%s\n' "anonymous pinned pull succeeded"
  exit 0
fi

case "${1:-} ${2:-}" in
  "context inspect")
    printf '%s\n' "unix:///fake/docker.sock"
    ;;
  "info --format")
    printf '%s\n' "linux/x86_64"
    ;;
  "info "*|"version "*|"rm "*)
    ;;
  "pull "*)
    if [[ "$*" == *"gcr.io/google.com/cloudsdktool/cloud-sdk:emulators@sha256:"* ]]; then
      printf '%s\n' "ERROR: stale credential helper requires non-interactive reauthentication" >&2
      exit 17
    fi
    if [[ "$*" == *"private.example/minisky/pubsub@sha256:"* ]]; then
      printf '%s\n' "private pull used host credential policy"
    fi
    ;;
  "image inspect")
    printf '%s\n' "linux/amd64"
    ;;
  "run --rm")
    if [[ "$*" == *"gcloud beta emulators pubsub start --help"* ]]; then
      printf '%s\n' "  --data-dir=DATA_DIR"
    fi
    ;;
  "ps -aq"|"ps -a"|"network ls"|"volume ls")
    ;;
  *)
    printf 'unexpected fake docker invocation:' >&2
    printf ' %q' "$@" >&2
    printf '\n' >&2
    exit 99
    ;;
esac
`

	for _, test := range []struct {
		name              string
		anonymousPullFail string
		imageOverride     string
		wantAnonymous     bool
		wantExit          int
		wantDiagnostic    string
	}{
		{
			name:           "stale host helper is bypassed",
			wantAnonymous:  true,
			wantExit:       88,
			wantDiagnostic: "isolated anonymous Docker config",
		},
		{
			name:              "anonymous registry failure stays fatal",
			anonymousPullFail: "1",
			wantAnonymous:     true,
			wantExit:          1,
			wantDiagnostic:    "registry, network, or digest acquisition failure",
		},
		{
			name:           "private override retains host credential policy",
			imageOverride:  "private.example/minisky/pubsub@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantExit:       88,
			wantDiagnostic: "active Docker credential policy",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logPath := filepath.Join(t.TempDir(), "docker.log")
			command := boundaryScriptCommand(t, fakeDocker, boundaryFakeGo, map[string]string{
				"FAKE_ANONYMOUS_PULL_FAIL":    test.anonymousPullFail,
				"FAKE_DOCKER_ENGINE_PLATFORM": "linux/x86_64",
				"FAKE_DOCKER_LOG":             logPath,
				"FAKE_GO_BUILD_EXIT":          "88",
				"MINISKY_PUBSUB_TEST_IMAGE":   test.imageOverride,
			})
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != test.wantExit {
				t.Fatalf("script exit=%v, want %d\n%s", err, test.wantExit, output)
			}
			if !strings.Contains(string(output), test.wantDiagnostic) {
				t.Fatalf("pull diagnostic omits %q:\n%s", test.wantDiagnostic, output)
			}
			if strings.Contains(string(output), "stale credential helper") {
				t.Fatalf("host credential helper was invoked:\n%s", output)
			}
			if strings.Contains(string(output), "does not advertise --data-dir") {
				t.Fatalf("image acquisition failure was misreported as capability failure:\n%s", output)
			}

			log, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			usedAnonymousConfig := strings.Contains(string(log), "--config ")
			if usedAnonymousConfig != test.wantAnonymous {
				t.Fatalf("isolated config use=%v, want %v:\n%s", usedAnonymousConfig, test.wantAnonymous, log)
			}
			if test.wantAnonymous &&
				!strings.Contains(string(log), "--host unix:///fake/docker.sock pull --platform linux/amd64") {
				t.Fatalf("public pull did not preserve daemon endpoint with isolated config:\n%s", log)
			}
		})
	}
}

func TestStoragePersistencePubSubSessionCleanupCoversExactPrimaryAndIsolationProfiles(t *testing.T) {
	source := readShellScript(t, emulatorBoundaryScript)
	for _, name := range []string{"owned_containers", "owned_networks", "owned_volumes"} {
		inventory := shellFunction(t, source, name)
		if !strings.Contains(inventory, `local target_profile="${1:?profile is required}"`) {
			t.Errorf("%s does not require an explicit exact profile", name)
		}
		if !strings.Contains(inventory, `label=minisky.profile=${target_profile}`) {
			t.Errorf("%s does not filter the requested exact profile", name)
		}
		if strings.Contains(inventory, "|| true") {
			t.Errorf("%s swallows Docker inventory failures", name)
		}
	}

	cleanup := shellFunction(t, source, "cleanup")
	if !strings.Contains(cleanup, `for cleanup_profile in "${profile}" "${profile}-isolated"; do`) {
		t.Error("EXIT cleanup does not sweep both exact boundary profiles")
	}

	workflow := readWorkflow(t, "critical-integration.yml")
	job := workflowJob(t, workflow, emulatorBoundaryJob)
	workflowCleanup := scalarString(stepByName(job,
		"Clean exact-owned Storage and PubSub boundary resources")["run"])
	if !strings.Contains(workflowCleanup,
		`for cleanup_profile in "${MINISKY_EMULATOR_BOUNDARY_PROFILE}" "${MINISKY_EMULATOR_BOUNDARY_PROFILE}-isolated"; do`) {
		t.Error("workflow fallback cleanup does not sweep both exact boundary profiles")
	}
}

func TestStoragePersistencePubSubSessionCleanupInventoryFailureControlsExitStatus(t *testing.T) {
	for _, test := range []struct {
		name       string
		goTestExit int
		wantExit   int
	}{
		{name: "successful test becomes cleanup failure", goTestExit: 0, wantExit: 1},
		{name: "test failure status is preserved", goTestExit: 42, wantExit: 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := boundaryScriptCommand(t, boundaryFakeDocker, boundaryFakeGo, map[string]string{
				"FAKE_DOCKER_ENGINE_PLATFORM": "linux/x86_64",
				"FAKE_DOCKER_INVENTORY_FAIL":  "1",
				"FAKE_GO_TEST_EXIT":           strconv.Itoa(test.goTestExit),
			})
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != test.wantExit {
				t.Fatalf("script exit=%v, want %d\n%s", err, test.wantExit, output)
			}
			if !strings.Contains(string(output),
				"Unable to inventory exact-owned containers for profile emulator-boundary-contract") {
				t.Fatalf("cleanup inventory failure is not retained in diagnostics:\n%s", output)
			}
		})
	}
}

func TestStoragePersistencePubSubSessionVolumeReplacementFailsClosed(t *testing.T) {
	source := readShellScript(t, emulatorBoundaryScript)
	harness := strings.Join([]string{
		"set -Eeuo pipefail",
		shellFunction(t, source, "owned_containers"),
		shellFunction(t, source, "owned_networks"),
		shellFunction(t, source, "owned_volumes"),
		shellFunction(t, source, "cleanup_exact_profile"),
		"set +e",
		`cleanup_exact_profile "emulator-boundary-contract"`,
		"status=$?",
		"set -e",
		`printf 'status=%s\n' "${status}"`,
	}, "\n")

	const fakeDocker = `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${FAKE_DOCKER_LOG}"
case "${1:-} ${2:-}" in
  "ps -aq"|"network ls")
    ;;
  "volume ls")
    count=0
    if [[ -f "${FAKE_VOLUME_LIST_COUNT}" ]]; then
      read -r count <"${FAKE_VOLUME_LIST_COUNT}"
    fi
    printf '%s\n' "$((count + 1))" >"${FAKE_VOLUME_LIST_COUNT}"
    if (( count == 0 )); then
      printf '%s\n' "boundary-volume"
    fi
    ;;
  "volume inspect")
    case "${FAKE_VOLUME_INSPECT_MODE}" in
      matching)
        printf '%s\n' "minisky|emulator-boundary-contract"
        ;;
      mismatch)
        printf '%s\n' "someone-else|unrelated-profile"
        ;;
      fail)
        printf '%s\n' "fake volume inspect unavailable" >&2
        exit 73
        ;;
    esac
    ;;
  "volume rm")
    ;;
  *)
    printf 'unexpected fake docker invocation:' >&2
    printf ' %q' "$@" >&2
    printf '\n' >&2
    exit 99
    ;;
esac
`

	for _, test := range []struct {
		name        string
		inspectMode string
		wantStatus  string
		wantRemove  bool
	}{
		{name: "replacement labels mismatch", inspectMode: "mismatch", wantStatus: "status=1"},
		{name: "replacement inspect fails", inspectMode: "fail", wantStatus: "status=1"},
		{name: "exact labels still match", inspectMode: "matching", wantStatus: "status=0", wantRemove: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			bin := t.TempDir()
			logPath := filepath.Join(bin, "docker.log")
			if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fakeDocker), 0o700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("bash", "-c", harness)
			command.Env = append(
				os.Environ(),
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
				"FAKE_DOCKER_LOG="+logPath,
				"FAKE_VOLUME_LIST_COUNT="+filepath.Join(bin, "volume-list-count"),
				"FAKE_VOLUME_INSPECT_MODE="+test.inspectMode,
			)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("cleanup harness failed: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), test.wantStatus) {
				t.Fatalf("cleanup status differs, want %q:\n%s", test.wantStatus, output)
			}

			log, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			removed := strings.Contains(string(log), "volume rm boundary-volume")
			if removed != test.wantRemove {
				t.Fatalf("volume removal=%t, want %t:\n%s", removed, test.wantRemove, log)
			}
			if !strings.Contains(string(output), "no conditional immutable-ID delete") {
				t.Fatalf("cleanup omits the residual name-based deletion boundary:\n%s", output)
			}
		})
	}
}

func TestStoragePersistencePubSubSessionReportsPubSubPlatformPrerequisite(t *testing.T) {
	for _, test := range []struct {
		name             string
		enginePlatform   string
		emulationFails   bool
		wantExit         int
		wantPrerequisite bool
	}{
		{
			name:             "ARM Linux without amd64 emulation",
			enginePlatform:   "linux/aarch64",
			emulationFails:   true,
			wantExit:         1,
			wantPrerequisite: true,
		},
		{
			name:           "ARM Linux with amd64 emulation",
			enginePlatform: "linux/aarch64",
			wantExit:       88,
		},
		{
			name:           "x86 Linux",
			enginePlatform: "linux/x86_64",
			wantExit:       88,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			emulationFails := "0"
			if test.emulationFails {
				emulationFails = "1"
			}
			command := boundaryScriptCommand(t, boundaryFakeDocker, boundaryFakeGo, map[string]string{
				"FAKE_DOCKER_ENGINE_PLATFORM": test.enginePlatform,
				"FAKE_DOCKER_EMULATION_FAIL":  emulationFails,
				"FAKE_GO_BUILD_EXIT":          "88",
			})
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != test.wantExit {
				t.Fatalf("script exit=%v, want %d\n%s", err, test.wantExit, output)
			}
			hasPrerequisite := strings.Contains(string(output),
				"requires linux/amd64 execution")
			if hasPrerequisite != test.wantPrerequisite {
				t.Fatalf("platform prerequisite diagnostic=%t, want %t:\n%s",
					hasPrerequisite, test.wantPrerequisite, output)
			}
			if strings.Contains(string(output), "does not advertise --data-dir") {
				t.Fatalf("platform failure was misreported as missing --data-dir support:\n%s", output)
			}
		})
	}
}

func TestStoragePersistencePubSubSessionMakeTargetContract(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	required := emulatorBoundaryTarget + ":\n\t" +
		"MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION=1 " +
		"./scripts/" + emulatorBoundaryScript
	if !strings.Contains(string(source), required) {
		t.Fatalf("Makefile omits exact guarded target:\n%s", required)
	}
}

func TestCriticalStoragePersistencePubSubSessionWorkflowContract(t *testing.T) {
	workflow := readWorkflow(t, "critical-integration.yml")
	triggers := mustMap(workflow["on"])
	for _, event := range []string{"pull_request", "push"} {
		paths := stringSlice(mustMap(triggers[event])["paths"])
		for _, required := range []string{
			".github/workflows/critical-integration.yml",
			"Makefile",
			"pkg/config/images.json",
			"pkg/orchestrator/**",
			"scripts/" + emulatorBoundaryScript,
			"scripts/emulator_data_durability_contract_test.go",
		} {
			if !contains(paths, required) {
				t.Errorf("%s paths omit %q", event, required)
			}
		}
	}

	job := workflowJob(t, workflow, emulatorBoundaryJob)
	for _, forbidden := range []string{"needs", "if", "continue-on-error", "concurrency"} {
		if _, found := job[forbidden]; found {
			t.Errorf("required durability job contains forbidden %q", forbidden)
		}
	}
	if scalarString(job["runs-on"]) != "ubuntu-latest" {
		t.Errorf("durability job runner = %q, want ubuntu-latest", scalarString(job["runs-on"]))
	}
	timeout, ok := integer(job["timeout-minutes"])
	if !ok || timeout < 1 || timeout > 30 {
		t.Errorf("durability job timeout = %v, want 1..30 minutes", job["timeout-minutes"])
	}

	jobEnv := mustMap(job["env"])
	profile := scalarString(jobEnv["MINISKY_EMULATOR_BOUNDARY_PROFILE"])
	if !strings.Contains(profile, "github.run_id") ||
		!strings.Contains(profile, "github.run_attempt") {
		t.Errorf("durability profile is not collision-safe: %q", profile)
	}
	if value := scalarString(jobEnv["MINISKY_EMULATOR_BOUNDARY_DIAGNOSTICS_DIR"]); value != "" {
		t.Errorf("job-level diagnostics directory uses runner-dependent value %q", value)
	}
	for _, name := range []string{"MINISKY_STORAGE_TEST_IMAGE", "MINISKY_PUBSUB_TEST_IMAGE"} {
		if !regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`).MatchString(scalarString(jobEnv[name])) {
			t.Errorf("%s is not an immutable image reference", name)
		}
	}

	for _, action := range []string{
		checkoutAction,
		setupNodeAction,
		setupGoAction,
		uploadAction,
	} {
		if firstStepUsing(job, action) == nil {
			t.Errorf("pinned action %q is absent", action)
		}
	}
	for _, step := range stepMaps(job) {
		if uses := scalarString(step["uses"]); uses != "" &&
			!regexp.MustCompile(`^(?:\./|[^@]+@[a-f0-9]{40}$)`).MatchString(uses) {
			t.Errorf("workflow action is not immutable: %q", uses)
		}
		if _, found := step["continue-on-error"]; found {
			t.Errorf("step %q contains continue-on-error", scalarString(step["name"]))
		}
	}

	for _, required := range []struct {
		step    string
		command string
	}{
		{step: "Build UI from lockfile", command: "npm ci"},
		{step: "Build UI from lockfile", command: "npm run build"},
		{step: "Verify Docker availability", command: "docker info >/dev/null"},
		{
			step:    "Run guarded Storage persistence and PubSub session gate",
			command: "make test-storage-persistence-pubsub-session 2>&1 | tee critical-storage-persistence-pubsub-session.log",
		},
	} {
		if errors := validateRequiredShellStep(job, required.step, required.command); len(errors) != 0 {
			t.Errorf("%s", strings.Join(errors, "; "))
		}
	}
	run := stepByName(job, "Run guarded Storage persistence and PubSub session gate")
	lines := activeCommandLines(scalarString(run["run"]))
	if !contains(lines, "set -o pipefail") {
		t.Error("durability lifecycle pipeline does not enable pipefail")
	}
	const diagnosticsDir = "${{ runner.temp }}/storage-persistence-pubsub-session-diagnostics"
	for _, stepName := range []string{
		"Run guarded Storage persistence and PubSub session gate",
		"Capture Storage and PubSub boundary diagnostics",
	} {
		step := stepByName(job, stepName)
		if got := scalarString(mustMap(step["env"])["MINISKY_EMULATOR_BOUNDARY_DIAGNOSTICS_DIR"]); got != diagnosticsDir {
			t.Errorf("%s diagnostics directory = %q, want %q", stepName, got, diagnosticsDir)
		}
	}

	cleanup := stepByName(job, "Clean exact-owned Storage and PubSub boundary resources")
	if cleanup == nil || !conditionIsAlways(cleanup["if"]) {
		t.Error("durability cleanup must run with exactly always()")
	} else {
		timeout, ok := integer(cleanup["timeout-minutes"])
		if !ok || timeout < 1 || timeout > 2 {
			t.Errorf("durability cleanup timeout = %v, want 1..2 minutes", cleanup["timeout-minutes"])
		}
		cleanupRun := scalarString(cleanup["run"])
		for _, required := range []string{
			"set -Eeuo pipefail",
			"cleanup_exact_profile",
			`--filter "label=managed-by=minisky"`,
			`--filter "label=minisky.profile=${cleanup_profile}"`,
			"Unable to verify exact-owned container cleanup",
			`test "${cleanup_failed}" -eq 0`,
		} {
			if !strings.Contains(cleanupRun, required) {
				t.Errorf("durability cleanup omits exact ownership filter %q", required)
			}
		}
		for _, forbidden := range []string{"|| true", "< <("} {
			if strings.Contains(cleanupRun, forbidden) {
				t.Errorf("durability cleanup can swallow Docker failures through %q", forbidden)
			}
		}
	}

	for _, name := range []string{
		"Capture Storage and PubSub boundary diagnostics",
		"Retain Storage and PubSub boundary diagnostics",
	} {
		step := stepByName(job, name)
		if step == nil || !conditionCoversFailureAndCancellation(step["if"]) {
			t.Errorf("%s must cover failure() || cancelled()", name)
		}
	}
	diagnosticsRun := scalarString(stepByName(job,
		"Capture Storage and PubSub boundary diagnostics")["run"])
	if !strings.Contains(diagnosticsRun,
		`for diagnostic_profile in "${MINISKY_EMULATOR_BOUNDARY_PROFILE}" "${MINISKY_EMULATOR_BOUNDARY_PROFILE}-isolated"; do`) {
		t.Error("failure diagnostics do not cover both exact boundary profiles")
	}
}

func boundaryScriptCommand(t *testing.T, dockerSource, goSource string, environment map[string]string) *exec.Cmd {
	t.Helper()

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	for name, source := range map[string]string{
		"docker": dockerSource,
		"go":     goSource,
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	marker := filepath.Join(bin, "go-invoked")
	command := exec.Command(bash, emulatorBoundaryScript)
	command.Env = append(
		withoutEnvironment(os.Environ(),
			"PATH",
			"DOCKER_CONFIG",
			"DOCKER_CONTEXT",
			"DOCKER_HOST",
			"MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION",
			"MINISKY_EMULATOR_BOUNDARY_PROFILE",
			"FAKE_DOCKER_MARKER",
		),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION=1",
		"MINISKY_EMULATOR_BOUNDARY_PROFILE=emulator-boundary-contract",
		"FAKE_DOCKER_MARKER="+marker,
	)
	for name, value := range environment {
		command.Env = append(command.Env, fmt.Sprintf("%s=%s", name, value))
	}
	return command
}

const boundaryFakeDocker = `#!/usr/bin/env bash
set -eu
if [[ "${1:-}" == "--config" ]]; then
  shift 2
  [[ "${1:-}" == "--host" ]]
  shift 2
  [[ "${1:-}" == "pull" ]]
  exit 0
fi
command="${1:-}"
subcommand="${2:-}"
case "${command} ${subcommand}" in
  "context inspect")
    printf '%s\n' "unix:///fake/docker.sock"
    ;;
  "info --format")
    printf '%s\n' "${FAKE_DOCKER_ENGINE_PLATFORM:-linux/x86_64}"
    ;;
  "info "*|"version "*|"pull "*|"rm "*)
    ;;
  "image inspect")
    printf '%s\n' "linux/amd64"
    ;;
  "run --rm")
    if [[ "$*" == *"sh -c exit 0"* && "${FAKE_DOCKER_EMULATION_FAIL:-0}" == "1" ]]; then
      printf '%s\n' "exec format error" >&2
      exit 125
    fi
    if [[ "$*" == *"gcloud beta emulators pubsub start --help"* ]]; then
      printf '%s\n' "  --data-dir=DATA_DIR"
    fi
    ;;
  "ps -aq")
    if [[ -f "${FAKE_DOCKER_MARKER}" && "${FAKE_DOCKER_INVENTORY_FAIL:-0}" == "1" ]]; then
      printf '%s\n' "fake container inventory unavailable" >&2
      exit 73
    fi
    ;;
  "ps -a")
    ;;
  "network ls"|"volume ls")
    ;;
  *)
    printf 'unexpected fake docker invocation:' >&2
    printf ' %q' "$@" >&2
    printf '\n' >&2
    exit 99
    ;;
esac
`

const boundaryFakeGo = `#!/usr/bin/env bash
set -eu
case "${1:-}" in
  build)
    exit "${FAKE_GO_BUILD_EXIT:-0}"
    ;;
  test)
    : >"${FAKE_DOCKER_MARKER}"
    exit "${FAKE_GO_TEST_EXIT:-0}"
    ;;
  *)
    exit 98
    ;;
esac
`
