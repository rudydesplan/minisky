package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"minisky/pkg/router"
	"minisky/pkg/shims/binaryauthorization"
	"minisky/pkg/shims/iam"
)

func TestPhase25BinaryAuthorizationTerraformContract(t *testing.T) {
	root := filepath.Clean("..")
	read := func(path string) string {
		t.Helper()
		content, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(content)
	}
	requireAll := func(path string, values ...string) {
		t.Helper()
		content := read(path)
		for _, value := range values {
			if !strings.Contains(content, value) {
				t.Errorf("%s is missing %q", path, value)
			}
		}
	}

	requireAll("terraform/providers.tf",
		"binary_authorization_custom_endpoint",
		"/_minisky/binaryauthorization/v1/",
	)
	requireAll("terraform/main.tf",
		`resource "google_binary_authorization_policy" "phase25"`,
		"enable_phase25_binary_authorization_policy",
		`name_pattern = "gcr.io/minisky-phase25/allowed/*"`,
		`evaluation_mode  = "ALWAYS_DENY"`,
		`enforcement_mode = "ENFORCED_BLOCK_AND_AUDIT_LOG"`,
	)
	requireAll("terraform/variables.tf",
		`variable "enable_phase25_binary_authorization_policy"`,
	)
	requireAll("terraform/outputs.tf",
		`output "phase25_binary_authorization_policy_name"`,
	)
	requireAll("Makefile",
		"test-phase25-binary-authorization-terraform:",
		"MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION=1 ./scripts/phase25-binary-authorization-terraform-integration.sh",
	)
	requireAll("scripts/phase25-binary-authorization-terraform-integration.sh",
		`MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION`,
		`binary_authorization_custom_endpoint`,
		`google_binary_authorization_policy.phase25[0]`,
		`init -backend-config="path=${tf_state}" -input=false -lockfile=readonly`,
		`state rm -backup="${work}/state-before-import.backup"`,
		`"projects/${project}"`,
		`/policy:evaluate`,
		`"ALWAYS_ALLOW"`,
		`"ALWAYS_DENY"`,
		`"gcr.io/google_containers/*"`,
		`unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT`,
		`unset GOOGLE_ACCESS_TOKEN GOOGLE_OAUTH_ACCESS_TOKEN CLOUDSDK_AUTH_ACCESS_TOKEN`,
		`trap cleanup EXIT`,
		`trap 'cleanup 130' INT`,
		`trap 'cleanup 143' TERM`,
		`rm -rf "${work}"`,
		`local advisory evaluation can block MiniSky Cloud Deploy rollouts for enforced DENY`,
		`permits audit-only decisions`,
		`not production or GKE admission security`,
	)
}

func TestPhase25BinaryAuthorizationTerraformGuard(t *testing.T) {
	script := "./phase25-binary-authorization-terraform-integration.sh"
	command := exec.Command(script)
	command.Env = withoutEnvironment(os.Environ(), "MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION")
	output, err := command.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 2 {
		t.Fatalf("unguarded exit = %v, want 2\n%s", err, output)
	}
	if !strings.Contains(string(output), "explicit opt-in") {
		t.Fatalf("unguarded output is not actionable:\n%s", output)
	}
}

func TestPhase25BinaryAuthorizationTerraformCollisionRefusal(t *testing.T) {
	temp := t.TempDir()
	lock := filepath.Join(temp, "minisky-phase25-binary-authorization-terraform-integration.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("./phase25-binary-authorization-terraform-integration.sh")
	command.Env = append(
		withoutEnvironment(os.Environ(), "MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION", "TMPDIR"),
		"MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION=1",
		"TMPDIR="+temp,
	)
	output, err := command.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 1 {
		t.Fatalf("collision exit = %v, want 1\n%s", err, output)
	}
	if !strings.Contains(string(output), "gate is active") {
		t.Fatalf("collision output is not actionable:\n%s", output)
	}
}

func TestPhase25BinaryAuthorizationEarlyCleanupTrapOrdering(t *testing.T) {
	source := readShellScript(t, "phase25-binary-authorization-terraform-integration.sh")
	lockAcquired := strings.Index(source, `trap cleanup EXIT`)
	rootSetup := strings.Index(source, `root="$(cd `)
	workSetup := strings.Index(source, `work="$(mktemp -d)"`)
	if lockAcquired < 0 || rootSetup < 0 || workSetup < 0 {
		t.Fatal("early cleanup trap or setup markers are missing")
	}
	if lockAcquired > rootSetup || lockAcquired > workSetup {
		t.Fatal("cleanup trap is not installed immediately after lock acquisition")
	}
	if strings.Count(source, `trap cleanup EXIT`) != 1 {
		t.Fatal("cleanup EXIT trap is replaced after setup")
	}
}

func TestPhase25BinaryAuthorizationPreDaemonSetupFailuresReleaseLock(t *testing.T) {
	for _, test := range []struct {
		name       string
		command    string
		wantStatus int
	}{
		{
			name:       "mktemp failure",
			command:    "#!/usr/bin/env bash\nexit 47\n",
			wantStatus: 47,
		},
		{
			name: "setup mkdir failure",
			command: `#!/usr/bin/env bash
if [[ "${1:-}" == "-p" ]]; then exit 48; fi
exec /bin/mkdir "$@"
`,
			wantStatus: 48,
		},
		{
			name: "nonempty state path check",
			command: `#!/usr/bin/env bash
/bin/mkdir "$@"
if [[ "${1:-}" == "-p" ]]; then printf 'injected\n' >"${3}/injected"; fi
`,
			wantStatus: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			temp := t.TempDir()
			bin := t.TempDir()
			helper := "mkdir"
			if test.name == "mktemp failure" {
				helper = "mktemp"
			}
			if err := os.WriteFile(filepath.Join(bin, helper), []byte(test.command), 0o700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("./phase25-binary-authorization-terraform-integration.sh")
			command.Env = append(
				withoutEnvironment(os.Environ(), "MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION", "TMPDIR", "PATH"),
				"MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION=1",
				"TMPDIR="+temp,
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			)
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != test.wantStatus {
				t.Fatalf("setup failure exit=%v, want %d\n%s", err, test.wantStatus, output)
			}
			entries, err := os.ReadDir(temp)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("setup failure leaked temporary paths: %v", entries)
			}
		})
	}
}

func TestPhase25BinaryAuthorizationEarlyCleanupSignals(t *testing.T) {
	source := readShellScript(t, "phase25-binary-authorization-terraform-integration.sh")
	for _, test := range []struct {
		signal     string
		wantStatus int
	}{
		{signal: "INT", wantStatus: 130},
		{signal: "TERM", wantStatus: 143},
	} {
		t.Run(test.signal, func(t *testing.T) {
			temp := t.TempDir()
			work := filepath.Join(temp, "work")
			lock := filepath.Join(temp, "lock")
			if err := os.Mkdir(work, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(lock, 0o700); err != nil {
				t.Fatal(err)
			}
			pidFile := filepath.Join(temp, "pid")
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				`lock="${TEST_LOCK}"`,
				`work="${TEST_WORK}"`,
				`pid=""`,
				`watchdog_pid=""`,
				shellFunction(t, source, "cleanup"),
				"trap cleanup EXIT",
				"trap 'cleanup 130' INT",
				"trap 'cleanup 143' TERM",
				`sleep 10 & pid=$!`,
				`printf '%s\n' "${pid}" >"${TEST_PID_FILE}"`,
				`kill -${TEST_SIGNAL} "$$"`,
			}, "\n")
			command := exec.Command("bash", "-c", harness)
			command.Env = append(os.Environ(),
				"TEST_LOCK="+lock,
				"TEST_WORK="+work,
				"TEST_PID_FILE="+pidFile,
				"TEST_SIGNAL="+test.signal,
			)
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != test.wantStatus {
				t.Fatalf("%s cleanup exit=%v, want %d\n%s", test.signal, err, test.wantStatus, output)
			}
			for _, path := range []string{work, lock} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("%s cleanup left %s: %v", test.signal, path, err)
				}
			}
			rawPID, err := os.ReadFile(pidFile)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
			if err != nil {
				t.Fatal(err)
			}
			if err := syscall.Kill(pid, 0); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
				t.Fatalf("%s cleanup left child process %d running", test.signal, pid)
			}
		})
	}
}

func TestPhase25BinaryAuthorizationPlanExitBehavior(t *testing.T) {
	source := readShellScript(t, "phase25-binary-authorization-terraform-integration.sh")
	harness := strings.Join([]string{
		"set -Eeuo pipefail",
		`tf_vars=(-var=x=y)`,
		`tf() { return "${FAKE_TF_EXIT}"; }`,
		shellFunction(t, source, "assert_plan_exit"),
		`FAKE_TF_EXIT=0 assert_plan_exit 0 matching`,
		`FAKE_TF_EXIT=2 assert_plan_exit 2 stale`,
		`if FAKE_TF_EXIT=1 assert_plan_exit 0 unexpected >/dev/null 2>&1; then exit 91; fi`,
		`if FAKE_TF_EXIT=0 assert_plan_exit 2 unexpected >/dev/null 2>&1; then exit 92; fi`,
	}, "\n")
	command := exec.Command("bash", "-c", harness)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("plan exit contract failed: %v\n%s", err, output)
	}
}

func TestPhase25BinaryAuthorizationImportOrderingBehavior(t *testing.T) {
	source := readShellScript(t, "phase25-binary-authorization-terraform-integration.sh")
	log := filepath.Join(t.TempDir(), "calls.log")
	harness := strings.Join([]string{
		"set -Eeuo pipefail",
		`work="/isolated"`,
		`project="demo"`,
		`tf_vars=(-var=profile=local)`,
		`tf() { printf 'TF %s\n' "$*" >>"${CALL_LOG}"; }`,
		`put_stale_policy() { printf 'STALE\n' >>"${CALL_LOG}"; }`,
		`assert_plan_exit() { printf 'PLAN %s %s\n' "$1" "$2" >>"${CALL_LOG}"; }`,
		shellFunction(t, source, "verify_matching_import"),
		shellFunction(t, source, "verify_stale_import_and_reconcile"),
		"verify_matching_import",
		"verify_stale_import_and_reconcile",
	}, "\n")
	command := exec.Command("bash", "-c", harness)
	command.Env = append(os.Environ(), "CALL_LOG="+log)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("import sequence harness failed: %v\n%s", err, output)
	}
	content, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	want := []string{
		"TF state rm -backup=/isolated/state-before-import.backup google_binary_authorization_policy.phase25[0]",
		"TF import -input=false -var=profile=local google_binary_authorization_policy.phase25[0] projects/demo",
		"PLAN 0 matching import refresh",
		"STALE",
		"TF state rm -backup=/isolated/state-before-stale-import.backup google_binary_authorization_policy.phase25[0]",
		"TF import -input=false -var=profile=local google_binary_authorization_policy.phase25[0] projects/demo",
		"PLAN 2 stale import refresh",
		"TF apply -auto-approve -input=false -target=google_binary_authorization_policy.phase25[0] -var=profile=local",
		"PLAN 0 stale import reconcile",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("import lifecycle order:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(want, "\n"))
	}
}

func TestPhase25BinaryAuthorizationCleanupBehavior(t *testing.T) {
	source := readShellScript(t, "phase25-binary-authorization-terraform-integration.sh")
	for _, test := range []struct {
		name          string
		setup         func(t *testing.T, work, diagnostics, bin string)
		escape        bool
		helperFailure bool
		wantRedaction bool
	}{
		{
			name: "unwritable diagnostics destination",
			setup: func(t *testing.T, _, diagnostics, _ string) {
				t.Helper()
				if err := os.WriteFile(diagnostics, []byte("not a directory"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:          "failing diagnostics helper",
			helperFailure: true,
		},
		{
			name:   "escaped diagnostics destination",
			escape: true,
		},
		{
			name:          "sanitized temporary diagnostics",
			wantRedaction: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			temp := t.TempDir()
			work := filepath.Join(temp, "work")
			lock := filepath.Join(temp, "lock")
			diagnostics := filepath.Join(work, "diagnostics")
			outside := filepath.Join(temp, "outside-diagnostics")
			bin := filepath.Join(temp, "bin")
			for _, path := range []string{work, lock, bin} {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			log := "Authorization: Bearer super-secret\naccess_token=super-secret\n"
			if err := os.WriteFile(filepath.Join(work, "terraform.log"), []byte(log), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(work, "terraform.tfstate"), []byte("state-secret"), 0o600); err != nil {
				t.Fatal(err)
			}
			if test.escape {
				diagnostics = outside
			}
			if test.helperFailure {
				fakePython := "#!/usr/bin/env bash\nexit 71\n"
				if err := os.WriteFile(filepath.Join(bin, "python3"), []byte(fakePython), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if test.setup != nil {
				test.setup(t, work, diagnostics, bin)
			}
			pidFile := filepath.Join(temp, "pid")
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				`work="${TEST_WORK}"`,
				`lock="${TEST_LOCK}"`,
				`diagnostics="${TEST_DIAGNOSTICS}"`,
				`terraform_log="${work}/terraform.log"`,
				`watchdog_pid=""`,
				shellFunction(t, source, "emit_diagnostics"),
				shellFunction(t, source, "cleanup"),
				"trap cleanup EXIT INT TERM",
				`sleep 60 & pid=$!`,
				`printf '%s\n' "${pid}" >"${TEST_PID_FILE}"`,
				"exit 37",
			}, "\n")
			command := exec.Command("bash", "-c", harness)
			environment := append(os.Environ(),
				"TEST_WORK="+work,
				"TEST_LOCK="+lock,
				"TEST_DIAGNOSTICS="+diagnostics,
				"TEST_PID_FILE="+pidFile,
			)
			if test.helperFailure {
				environment = append(environment, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			}
			command.Env = environment
			output, err := command.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 37 {
				t.Fatalf("cleanup exit=%v, want original 37\n%s", err, output)
			}
			for _, path := range []string{work, lock} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("cleanup left %s: %v", path, err)
				}
			}
			if _, err := os.Stat(outside); !os.IsNotExist(err) {
				t.Fatalf("diagnostics escaped temporary work directory: %v", err)
			}
			rawPID, err := os.ReadFile(pidFile)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
			if err != nil {
				t.Fatal(err)
			}
			if err := syscall.Kill(pid, 0); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
				t.Fatalf("cleanup left child process %d running", pid)
			}
			if strings.Contains(string(output), "super-secret") || strings.Contains(string(output), "state-secret") {
				t.Fatalf("cleanup exposed credentials or state:\n%s", output)
			}
			if test.wantRedaction && !strings.Contains(string(output), "<redacted>") {
				t.Fatalf("cleanup did not emit sanitized diagnostics:\n%s", output)
			}
		})
	}
}

func TestPhase25BinaryAuthorizationStrictIAMLifecycle(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	t.Setenv("MINISKY_ENABLE_EXPERIMENTAL_SERVICES", "1")

	iamAPI, err := iam.NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	const policy = `{"policy":{"version":1,"bindings":[` +
		`{"role":"permission:binaryauthorization.policy.get","members":["user:reader@example.com","user:admin@example.com"]},` +
		`{"role":"permission:binaryauthorization.policy.update","members":["user:updater@example.com","user:admin@example.com"]},` +
		`{"role":"permission:binaryauthorization.policy.evaluate","members":["user:wrong@example.com"]}` +
		`]}}`
	setupRequest := httptest.NewRequest(http.MethodPost, "http://iam/v1/projects/demo:setIamPolicy", strings.NewReader(policy))
	setupResponse := httptest.NewRecorder()
	iamAPI.ServeHTTP(setupResponse, setupRequest)
	if setupResponse.Code != http.StatusOK {
		t.Fatalf("IAM policy setup=%d body=%s", setupResponse.Code, setupResponse.Body.String())
	}

	binaryAPI, err := binaryauthorization.NewAPIWithStore(nil, binaryauthorization.AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	gateway := router.NewProxyRouterWithManager(nil)
	gateway.RegisterShim("iam.googleapis.com", iamAPI)
	gateway.RegisterShim("binaryauthorization.googleapis.com", binaryAPI)
	gateway.ConfigureSecurity(iamAPI, nil, false, "phase25-audience")

	token := func(principal string) string {
		t.Helper()
		value, _, err := iamAPI.IssueLocalToken(principal, "phase25-audience",
			[]string{"https://www.googleapis.com/auth/cloud-platform"}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	const (
		url  = "http://localhost/_minisky/binaryauthorization/v1/projects/demo/policy"
		body = `{"name":"projects/demo/policy","admissionWhitelistPatterns":[{"namePattern":"gcr.io/minisky-phase25/allowed/*"}],"defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`
	)
	request := func(method, bearer, suppliedPrincipal string) *httptest.ResponseRecorder {
		t.Helper()
		var payload *bytes.Reader
		if method == http.MethodPut {
			payload = bytes.NewReader([]byte(body))
		} else {
			payload = bytes.NewReader(nil)
		}
		req := httptest.NewRequest(method, url, payload)
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		if suppliedPrincipal != "" {
			req.Header.Set("X-MiniSky-Principal", suppliedPrincipal)
		}
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, req)
		return response
	}
	assertStatus := func(label string, response *httptest.ResponseRecorder, want int) {
		t.Helper()
		if response.Code != want {
			t.Fatalf("%s=%d, want %d body=%s", label, response.Code, want, response.Body.String())
		}
	}

	assertStatus("unauthenticated PUT", request(http.MethodPut, "", ""), http.StatusUnauthorized)
	assertStatus("principal-header bypass", request(http.MethodPut, "", "user:admin@example.com"), http.StatusUnauthorized)
	assertStatus("wrong permission PUT", request(http.MethodPut, token("user:wrong@example.com"), ""), http.StatusForbidden)
	assertStatus("no grant PUT", request(http.MethodPut, token("user:none@example.com"), ""), http.StatusForbidden)
	assertStatus("get-only PUT", request(http.MethodPut, token("user:reader@example.com"), ""), http.StatusForbidden)
	assertStatus("update-only PUT", request(http.MethodPut, token("user:updater@example.com"), ""), http.StatusOK)
	assertStatus("update-only GET", request(http.MethodGet, token("user:updater@example.com"), ""), http.StatusForbidden)
	assertStatus("exact grants PUT", request(http.MethodPut, token("user:admin@example.com"), ""), http.StatusOK)
	assertStatus("exact grants GET", request(http.MethodGet, token("user:admin@example.com"), ""), http.StatusOK)
}

func withoutEnvironment(environment []string, names ...string) []string {
	blocked := make(map[string]struct{}, len(names))
	for _, name := range names {
		blocked[name] = struct{}{}
	}
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if _, found := blocked[name]; !found {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}
