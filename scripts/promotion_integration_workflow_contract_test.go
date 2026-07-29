package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	promotionWorkflowName   = "promotion-integration.yml"
	promotionDownloadAction = "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
)

var (
	promotionTargets = []string{
		"test-phase18-workflows-terraform",
		"test-phase18-eventarc-terraform",
		"test-phase19-composer-terraform",
		"test-phase19-managed-kafka-terraform",
		"test-phase20-alloydb-terraform",
		"test-phase20-filestore-terraform",
		"test-phase20-identity-platform-terraform",
		"test-phase20-storage-transfer-terraform",
		"test-phase21-service-directory-terraform",
		"test-phase23-document-ai-terraform",
		"test-phase24-org-policy-terraform",
		"test-phase25-binary-authorization-terraform",
		"test-phase18-25-sdk",
		"test-phase19-sdk",
		"test-phase19-heavy-backend",
		"test-phase20-sdk",
		"test-phase21-22-sdk",
		"test-phase23-sdk",
		"test-phase24-25-sdk",
	}
	promotionIntegrationJobs = []string{
		"phase18-25-terraform-integration",
		"phase18-25-sdk-integration",
		"phase19-sdk-integration",
		"phase19-heavy-backend-integration",
		"phase20-sdk-integration",
		"phase21-22-sdk-integration",
		"phase23-sdk-integration",
		"phase24-25-sdk-integration",
	}
	removedCIShadowJobs = map[string]string{
		"terraform-integration":                    "terraform-provider",
		"state-durability-integration":             "state-durability",
		"event-delivery-integration":               "event-delivery",
		"phase10-artifact-integration":             "artifact-registry",
		"phase13-wif-integration":                  "workload-identity",
		"phase15-emulator-integration":             "data-emulators",
		"phase16-monitoring-integration":           "phase16-service-lifecycles",
		"phase16-logging-integration":              "phase16-service-lifecycles",
		"phase16-dns-integration":                  "phase16-service-lifecycles",
		"phase16-vertex-integration":               "phase16-service-lifecycles",
		"phase16-subnetwork-integration":           "subnetwork-sdk",
		"phase16-subnetwork-terraform-integration": "subnetwork-terraform",
		"phase17-enterprise-integration":           "enterprise-controls",
		"phase18-25-sdk-integration":               "",
		"phase19-sdk-integration":                  "",
		"phase19-heavy-backend-integration":        "",
		"phase20-sdk-integration":                  "",
		"phase21-22-sdk-integration":               "",
		"phase23-sdk-integration":                  "",
		"phase24-25-sdk-integration":               "",
	}
	removedCIDispatchInputs = []string{
		"run_terraform_integration",
		"run_state_durability_integration",
		"run_event_delivery_integration",
		"run_phase10_artifact_integration",
		"run_phase13_wif_integration",
		"run_phase15_emulator_integration",
		"run_phase16_monitoring_integration",
		"run_phase16_logging_integration",
		"run_phase16_dns_integration",
		"run_phase16_vertex_integration",
		"run_phase16_subnetwork_integration",
		"run_phase16_subnetwork_terraform_integration",
		"run_phase17_enterprise_integration",
		"run_phase18_25_sdk_integration",
		"run_phase19_sdk_integration",
		"run_phase19_heavy_backend_integration",
		"run_phase20_sdk_integration",
		"run_phase21_22_sdk_integration",
		"run_phase23_sdk_integration",
		"run_phase24_25_sdk_integration",
	}
	requiredPromotionPaths = []string{
		".claude/skills/**",
		".github/actions/**",
		".github/workflows/**",
		".gitlab/ci/minisky.yml",
		".goreleaser.yaml",
		"README.md",
		"cmd/**",
		"deployments/docker-compose.yml",
		"docs/**",
		"pkg/**",
		"sdk-smoke/**",
		"scripts/**",
		"terraform/**",
		"Makefile",
		"go.mod",
		"go.sum",
		"ui/index.html",
		"ui/eslint.config.*",
		"ui/package.json",
		"ui/package-lock.json",
		"ui/public/**",
		"ui/src/**",
		"ui/tsconfig*.json",
		"ui/vite.config.*",
		"ui/*.go",
	}
	promotionJobTargets = map[string]string{
		"phase18-25-sdk-integration":        "test-phase18-25-sdk",
		"phase19-sdk-integration":           "test-phase19-sdk",
		"phase19-heavy-backend-integration": "test-phase19-heavy-backend",
		"phase20-sdk-integration":           "test-phase20-sdk",
		"phase21-22-sdk-integration":        "test-phase21-22-sdk",
		"phase23-sdk-integration":           "test-phase23-sdk",
		"phase24-25-sdk-integration":        "test-phase24-25-sdk",
	}
	terraformTargetsByID = map[string]string{
		"workflows":            "test-phase18-workflows-terraform",
		"eventarc":             "test-phase18-eventarc-terraform",
		"composer":             "test-phase19-composer-terraform",
		"managed-kafka":        "test-phase19-managed-kafka-terraform",
		"alloydb":              "test-phase20-alloydb-terraform",
		"filestore":            "test-phase20-filestore-terraform",
		"identity-platform":    "test-phase20-identity-platform-terraform",
		"storage-transfer":     "test-phase20-storage-transfer-terraform",
		"service-directory":    "test-phase21-service-directory-terraform",
		"document-ai":          "test-phase23-document-ai-terraform",
		"org-policy":           "test-phase24-org-policy-terraform",
		"binary-authorization": "test-phase25-binary-authorization-terraform",
	}
	terraformLocksByID = map[string]string{
		"workflows":            "minisky-phase18-workflows-terraform-integration.lock",
		"eventarc":             "minisky-phase18-eventarc-terraform-integration.lock",
		"composer":             "minisky-phase19-composer-terraform-integration.lock",
		"managed-kafka":        "minisky-phase19-managed-kafka-terraform-integration.lock",
		"alloydb":              "minisky-phase20-alloydb-terraform-integration.lock",
		"filestore":            "minisky-phase20-filestore-terraform-integration.lock",
		"identity-platform":    "minisky-phase20-identity-platform-terraform-integration.lock",
		"storage-transfer":     "minisky-phase20-storage-transfer-terraform.lock",
		"service-directory":    "minisky-phase21-service-directory-terraform-integration.lock",
		"document-ai":          "minisky-phase23-document-ai-terraform-integration.lock",
		"org-policy":           "minisky-phase24-org-policy-terraform-integration.lock",
		"binary-authorization": "minisky-phase25-binary-authorization-terraform-integration.lock",
	}
	pinnedActionPattern = regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`)
)

type promotionWorkflowSet struct {
	promotion map[string]any
	ci        map[string]any
	critical  map[string]any
}

func TestPromotionIntegrationWorkflowContract(t *testing.T) {
	if problems := validatePromotionArchitecture(readPromotionWorkflowSet(t)); len(problems) != 0 {
		t.Fatalf("promotion workflow contract failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestPromotionIntegrationWorkflowRejectsEveryRequiredPathClassMutation(t *testing.T) {
	for _, event := range []string{"pull_request", "push"} {
		for _, required := range requiredPromotionPaths {
			t.Run(event+"/"+required, func(t *testing.T) {
				workflows := clonePromotionWorkflowSet(t, readPromotionWorkflowSet(t))
				trigger := mustMap(mustMap(workflows.promotion["on"])[event])
				trigger["paths"] = withoutString(stringSlice(trigger["paths"]), required)
				if problems := validatePromotionArchitecture(workflows); len(problems) == 0 {
					t.Fatalf("%s unexpectedly passed without required path class %q", event, required)
				}
			})
		}
	}
}

func TestPromotionIntegrationWorkflowRejectsMutations(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(promotionWorkflowSet)
	}{
		{name: "missing pull request trigger", mutate: func(workflows promotionWorkflowSet) {
			delete(mustMap(workflows.promotion["on"]), "pull_request")
		}},
		{name: "missing schedule trigger", mutate: func(workflows promotionWorkflowSet) {
			delete(mustMap(workflows.promotion["on"]), "schedule")
		}},
		{name: "dispatch inputs restored", mutate: func(workflows promotionWorkflowSet) {
			mustMap(workflows.promotion["on"])["workflow_dispatch"] = map[string]any{
				"inputs": map[string]any{"run_phase19_sdk": map[string]any{"default": false}},
			}
		}},
		{name: "dispatch input job gate restored", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.promotion, "phase19-sdk-integration")["if"] =
				"github.event_name == 'workflow_dispatch' && inputs.run_phase19_sdk"
		}},
		{name: "duplicate owner restored in ci", mutate: func(workflows promotionWorkflowSet) {
			mustMap(workflows.ci["jobs"])["duplicate-promotion-owner"] = map[string]any{
				"runs-on":         "ubuntu-latest",
				"timeout-minutes": 5,
				"steps": []any{map[string]any{
					"name": "Duplicate owner",
					"run":  "make test-phase19-sdk",
				}},
			}
		}},
		{name: "critical shadow restored in ci", mutate: func(workflows promotionWorkflowSet) {
			mustMap(workflows.ci["jobs"])["phase16-monitoring-integration"] = map[string]any{
				"runs-on":         "ubuntu-latest",
				"timeout-minutes": 5,
				"steps":           []any{},
			}
		}},
		{name: "required path omitted", mutate: func(workflows promotionWorkflowSet) {
			trigger := mustMap(mustMap(workflows.promotion["on"])["pull_request"])
			trigger["paths"] = withoutString(stringSlice(trigger["paths"]), "pkg/**")
		}},
		{name: "pr group loses stable identity", mutate: func(workflows promotionWorkflowSet) {
			mustMap(workflows.promotion["concurrency"])["group"] =
				"promotion-integration-${{ github.event_name }}-${{ github.run_id }}"
		}},
		{name: "non pr pending runs share a group", mutate: func(workflows promotionWorkflowSet) {
			mustMap(workflows.promotion["concurrency"])["group"] =
				"promotion-integration-${{ github.event_name == 'pull_request' && format('pr-{0}', github.event.pull_request.number) || github.ref }}"
		}},
		{name: "job continue on error", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.promotion, "phase20-sdk-integration")["continue-on-error"] = true
		}},
		{name: "step continue on error", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.promotion, "phase23-sdk-integration"),
				"Run Phase 23 generated-client integration")["continue-on-error"] = true
		}},
		{name: "cleanup disabled", mutate: func(workflows promotionWorkflowSet) {
			promotionCleanupStep(promotionJob(workflows.promotion, "phase24-25-sdk-integration"))["if"] = false
		}},
		{name: "cleanup is no op", mutate: func(workflows promotionWorkflowSet) {
			promotionCleanupStep(promotionJob(workflows.promotion, "phase23-sdk-integration"))["run"] = ":"
		}},
		{name: "cleanup removal command missing", mutate: func(workflows promotionWorkflowSet) {
			promotionCleanupStep(promotionJob(workflows.promotion,
				"phase21-22-sdk-integration"))["run"] =
				`test ! -d "${TMPDIR:-/tmp}/minisky-phase21-22-sdk-integration.lock"`
		}},
		{name: "cleanup removes irrelevant path", mutate: func(workflows promotionWorkflowSet) {
			step := promotionCleanupStep(promotionJob(workflows.promotion, "phase20-sdk-integration"))
			step["run"] = scalarString(step["run"]) + "\nrm -rf /tmp/unrelated\n"
		}},
		{name: "diagnostics removed", mutate: func(workflows promotionWorkflowSet) {
			removePromotionStep(promotionJob(workflows.promotion, "phase19-sdk-integration"),
				"Capture bounded failure diagnostics")
		}},
		{name: "diagnostics mask shell failure", mutate: func(workflows promotionWorkflowSet) {
			step := stepByName(promotionJob(workflows.promotion, "phase23-sdk-integration"),
				"Capture bounded failure diagnostics")
			step["run"] = scalarString(step["run"]) + "\ndocker ps -a || true\n"
		}},
		{name: "lifecycle disabled", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.promotion, "phase20-sdk-integration"),
				"Run Phase 20 Docker-backed generated-client integration")["if"] = false
		}},
		{name: "lifecycle has always false expression", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.promotion, "phase18-25-sdk-integration"),
				"Run Phase 18-25 generated-client integration")["if"] = "${{ 1 == 0 }}"
		}},
		{name: "lifecycle has dispatch only gate", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.promotion, "phase19-sdk-integration"),
				"Run Phase 19 generated-client integration")["if"] =
				"github.event_name == 'workflow_dispatch'"
		}},
		{name: "lifecycle make target swapped", mutate: func(workflows promotionWorkflowSet) {
			step := stepByName(promotionJob(workflows.promotion, "phase19-sdk-integration"),
				"Run Phase 19 generated-client integration")
			step["run"] = strings.Replace(scalarString(step["run"]),
				"test-phase19-sdk", "test-phase20-sdk", 1)
		}},
		{name: "matrix make target swapped", mutate: func(workflows promotionWorkflowSet) {
			matrix := mustMap(mustMap(
				promotionJob(workflows.promotion, "phase18-25-terraform-integration")["strategy"])["matrix"])
			mustMap(matrix["include"].([]any)[0])["make_target"] = "test-phase18-eventarc-terraform"
		}},
		{name: "matrix cleanup lock swapped", mutate: func(workflows promotionWorkflowSet) {
			matrix := mustMap(mustMap(
				promotionJob(workflows.promotion, "phase18-25-terraform-integration")["strategy"])["matrix"])
			mustMap(matrix["include"].([]any)[0])["lock_name"] =
				"minisky-phase18-eventarc-terraform-integration.lock"
		}},
		{name: "lifecycle masks shell failure", mutate: func(workflows promotionWorkflowSet) {
			step := stepByName(promotionJob(workflows.promotion, "phase24-25-sdk-integration"),
				"Run Phase 24-25 generated-client integration")
			step["run"] = scalarString(step["run"]) + "\ntrue\n"
		}},
		{name: "integration prerequisite removed", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.promotion, "phase19-heavy-backend-integration")["needs"] =
				[]any{"promotion-assets", "phase18-25-evidence"}
		}},
		{name: "unsafe integration added to prerequisite", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.promotion, "sdk-smoke-validate")["steps"] = append(
				promotionJob(workflows.promotion, "sdk-smoke-validate")["steps"].([]any),
				map[string]any{"name": "Unsafe", "run": "make test-phase19-sdk"},
			)
		}},
		{name: "prerequisite has expression false gate", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.promotion, "promotion-assets"),
				"Build promotion UI")["if"] = "${{ 1 != 1 }}"
		}},
		{name: "prerequisite has dispatch only gate", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.promotion, "terraform-validate"),
				"Validate integration script syntax")["if"] =
				"github.event_name == 'workflow_dispatch'"
		}},
		{name: "safe validation parity drifts", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.promotion, "terraform-validate"),
				"Verify air-gapped bundle workflow")["run"] = "true"
		}},
		{name: "preparation artifact removed", mutate: func(workflows promotionWorkflowSet) {
			removePromotionStep(promotionJob(workflows.promotion, "promotion-assets"),
				"Share promotion UI assets")
		}},
		{name: "duplicate promotion Go quality restored", mutate: func(workflows promotionWorkflowSet) {
			job := promotionJob(workflows.promotion, "promotion-assets")
			job["steps"] = append(job["steps"].([]any), map[string]any{
				"name": "Duplicate Go quality",
				"run":  "go test -race ./cmd/... ./pkg/... ./ui",
			})
		}},
		{name: "job timeout removed", mutate: func(workflows promotionWorkflowSet) {
			delete(promotionJob(workflows.promotion, "phase18-25-sdk-integration"), "timeout-minutes")
		}},
		{name: "permissions removed", mutate: func(workflows promotionWorkflowSet) {
			delete(workflows.promotion, "permissions")
		}},
		{name: "job permissions broadened", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.promotion, "phase20-sdk-integration")["permissions"] =
				map[string]any{"contents": "write"}
		}},
		{name: "main runs cancel", mutate: func(workflows promotionWorkflowSet) {
			mustMap(workflows.promotion["concurrency"])["cancel-in-progress"] = true
		}},
		{name: "matrix fail fast enabled", mutate: func(workflows promotionWorkflowSet) {
			mustMap(promotionJob(workflows.promotion, "phase18-25-terraform-integration")["strategy"])["fail-fast"] = true
		}},
		{name: "matrix parallelism changed", mutate: func(workflows promotionWorkflowSet) {
			mustMap(promotionJob(workflows.promotion, "phase18-25-terraform-integration")["strategy"])["max-parallel"] = 4
		}},
		{name: "matrix owner omitted", mutate: func(workflows promotionWorkflowSet) {
			matrix := mustMap(mustMap(
				promotionJob(workflows.promotion, "phase18-25-terraform-integration")["strategy"])["matrix"])
			matrix["include"] = matrix["include"].([]any)[1:]
		}},
		{name: "critical owner disabled", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.critical, "artifact-registry")["if"] = false
		}},
		{name: "critical owner has expression false gate", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.critical, "subnetwork-terraform")["if"] = "${{ 2 < 1 }}"
		}},
		{name: "critical owner command changed", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.critical, "subnetwork-sdk"),
				"Run guarded subnetwork SDK lifecycle")["run"] = "make test-phase16-dns"
		}},
		{name: "critical lifecycle has dispatch only gate", mutate: func(workflows promotionWorkflowSet) {
			stepByName(promotionJob(workflows.critical, "enterprise-controls"),
				"Run guarded enterprise controls lifecycle")["if"] =
				"github.event_name == 'workflow_dispatch'"
		}},
		{name: "critical owner env removed", mutate: func(workflows promotionWorkflowSet) {
			step := stepByName(promotionJob(workflows.critical, "data-emulators"),
				"Run guarded emulator lifecycle")
			delete(mustMap(step["env"]), "MINISKY_PHASE15_INTEGRATION")
		}},
		{name: "critical owner prerequisite removed", mutate: func(workflows promotionWorkflowSet) {
			delete(promotionJob(workflows.critical, "enterprise-controls"), "needs")
		}},
		{name: "critical owner diagnostics removed", mutate: func(workflows promotionWorkflowSet) {
			removePromotionStep(promotionJob(workflows.critical, "workload-identity"),
				"Retain failure log")
		}},
		{name: "critical owner continue on error", mutate: func(workflows promotionWorkflowSet) {
			promotionJob(workflows.critical, "event-delivery")["continue-on-error"] = true
		}},
		{name: "critical automatic trigger removed", mutate: func(workflows promotionWorkflowSet) {
			delete(mustMap(workflows.critical["on"]), "pull_request")
		}},
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			workflows := clonePromotionWorkflowSet(t, readPromotionWorkflowSet(t))
			mutation.mutate(workflows)
			if problems := validatePromotionArchitecture(workflows); len(problems) == 0 {
				t.Fatal("mutated workflows unexpectedly satisfied the structural contract")
			}
		})
	}
}

func validatePromotionArchitecture(workflows promotionWorkflowSet) []string {
	var problems []string
	promotion := workflows.promotion
	triggers := mustMap(promotion["on"])
	for _, event := range []string{"pull_request", "push"} {
		trigger := mustMap(triggers[event])
		if trigger == nil {
			problems = append(problems, fmt.Sprintf("promotion workflow is missing %s trigger", event))
			continue
		}
		paths := stringSlice(trigger["paths"])
		for _, required := range requiredPromotionPaths {
			if !contains(paths, required) {
				problems = append(problems, fmt.Sprintf("%s paths omit %q", event, required))
			}
		}
		if event == "push" && !contains(stringSlice(trigger["branches"]), "main") {
			problems = append(problems, "push trigger does not target main")
		}
	}
	if schedule, ok := triggers["schedule"].([]any); !ok || len(schedule) != 1 ||
		scalarString(mustMap(schedule[0])["cron"]) != "41 6 * * 2" {
		problems = append(problems, "weekly promotion schedule must be Tuesday at 06:41 UTC")
	}
	dispatch, found := triggers["workflow_dispatch"]
	if !found {
		problems = append(problems, "promotion workflow is missing workflow_dispatch")
	} else if value := mustMap(dispatch); value != nil && len(value) != 0 {
		problems = append(problems, "workflow_dispatch must run the full suite without inputs")
	}

	permissions := mustMap(promotion["permissions"])
	if len(permissions) != 1 || scalarString(permissions["contents"]) != "read" {
		problems = append(problems, fmt.Sprintf("workflow permissions are not least privilege: %v", permissions))
	}
	concurrency := mustMap(promotion["concurrency"])
	wantGroup := "promotion-integration-${{github.event_name=='pull_request'&&" +
		"format('pr-{0}',github.event.pull_request.number)||" +
		"format('{0}-{1}-{2}',github.event_name,github.run_id,github.run_attempt)}}"
	if compactWorkflowExpression(scalarString(concurrency["group"])) != wantGroup {
		problems = append(problems,
			"promotion concurrency must use a stable PR identity and unique non-PR event/run identity")
	}
	if normalizeCondition(scalarString(concurrency["cancel-in-progress"])) !=
		"github.event_name=='pull_request'" {
		problems = append(problems, "concurrency cancellation must be pull-request-only")
	}

	jobs := mustMap(promotion["jobs"])
	requiredJobs := append([]string{
		"promotion-assets",
		"sdk-smoke-validate",
		"phase18-25-evidence",
		"terraform-validate",
	}, promotionIntegrationJobs...)
	for _, name := range requiredJobs {
		job := mustMap(jobs[name])
		if job == nil {
			problems = append(problems, fmt.Sprintf("promotion job %q is absent", name))
			continue
		}
		if _, found := job["if"]; found {
			problems = append(problems, fmt.Sprintf("promotion job %q has a dispatch/event gate", name))
		}
		if _, found := job["continue-on-error"]; found {
			problems = append(problems, fmt.Sprintf("promotion job %q contains continue-on-error", name))
		}
		timeout, ok := integer(job["timeout-minutes"])
		matrixTimeout := name == "phase18-25-terraform-integration" &&
			scalarString(job["timeout-minutes"]) == "${{ matrix.timeout_minutes }}"
		if (!ok || timeout <= 0 || timeout > 60) && !matrixTimeout {
			problems = append(problems, fmt.Sprintf("promotion job %q has missing or unbounded timeout", name))
		}
		for _, step := range stepMaps(job) {
			if _, found := step["continue-on-error"]; found {
				problems = append(problems, fmt.Sprintf("promotion job %q step %q contains continue-on-error",
					name, scalarString(step["name"])))
			}
			if action := scalarString(step["uses"]); action != "" &&
				!strings.HasPrefix(action, "./") && !pinnedActionPattern.MatchString(action) {
				problems = append(problems, fmt.Sprintf("promotion action is not commit-pinned: %q", action))
			}
		}
		if jobPermissions, found := job["permissions"]; found {
			permissions := mustMap(jobPermissions)
			if len(permissions) != 1 || scalarString(permissions["contents"]) != "read" {
				problems = append(problems,
					fmt.Sprintf("promotion job %q overrides least-privilege permissions: %v", name, permissions))
			}
		}
	}

	if mustMap(jobs["quality"]) != nil {
		problems = append(problems, "promotion workflow retains the duplicate full quality job")
	}
	problems = append(problems, validatePromotionPrerequisites(promotion)...)
	problems = append(problems, validateAuthoritativePrerequisiteParity(promotion, workflows.ci)...)
	problems = append(problems, validatePromotionArtifactFlow(promotion)...)

	for _, name := range promotionIntegrationJobs {
		job := promotionJob(promotion, name)
		cleanup := promotionCleanupStep(job)
		if cleanup == nil || !conditionIsAlways(cleanup["if"]) {
			problems = append(problems, fmt.Sprintf("%s lacks always-on cleanup", name))
		} else if timeout, ok := integer(cleanup["timeout-minutes"]); !ok || timeout <= 0 || timeout > 2 {
			problems = append(problems, fmt.Sprintf("%s cleanup timeout is not bounded", name))
		} else {
			problems = append(problems, validateExactPromotionCleanup(name, cleanup)...)
		}
		diagnostics := stepByName(job, "Capture bounded failure diagnostics")
		if diagnostics == nil || !conditionCoversFailureAndCancellation(diagnostics["if"]) {
			problems = append(problems, fmt.Sprintf("%s lacks failure/cancellation diagnostics", name))
		} else {
			for _, line := range activeCommandLines(scalarString(diagnostics["run"])) {
				if shellLineMasksFailure(line) {
					problems = append(problems,
						fmt.Sprintf("%s diagnostics mask shell failure: %q", name, line))
				}
			}
		}
		artifact := promotionFailureArtifact(job)
		if artifact == nil || !conditionCoversFailureAndCancellation(artifact["if"]) ||
			scalarString(artifact["uses"]) != uploadAction {
			problems = append(problems, fmt.Sprintf("%s lacks pinned failure artifact retention", name))
		}
	}

	matrix := mustMap(promotionJob(promotion, "phase18-25-terraform-integration")["strategy"])
	if value, ok := matrix["fail-fast"].(bool); !ok || value {
		problems = append(problems, "Terraform matrix fail-fast must be false")
	}
	if value, ok := integer(matrix["max-parallel"]); !ok || value != 12 {
		problems = append(problems, "Terraform matrix max-parallel must be 12")
	}
	include := mustMap(matrix["matrix"])["include"]
	entries, _ := include.([]any)
	if len(entries) != 12 {
		problems = append(problems, fmt.Sprintf("Terraform matrix has %d entries, want 12", len(entries)))
	}
	seenMatrixIDs := make(map[string]bool, len(entries))
	for _, rawEntry := range entries {
		entry := mustMap(rawEntry)
		id := scalarString(entry["id"])
		seenMatrixIDs[id] = true
		if want, found := terraformTargetsByID[id]; !found || scalarString(entry["make_target"]) != want {
			problems = append(problems, fmt.Sprintf("Terraform matrix entry %q has wrong make target %q",
				id, scalarString(entry["make_target"])))
		}
		if want, found := terraformLocksByID[id]; !found || scalarString(entry["lock_name"]) != want {
			problems = append(problems, fmt.Sprintf("Terraform matrix entry %q has wrong cleanup lock %q",
				id, scalarString(entry["lock_name"])))
		}
	}
	for id := range terraformTargetsByID {
		if !seenMatrixIDs[id] {
			problems = append(problems, fmt.Sprintf("Terraform matrix omits required id %q", id))
		}
	}
	problems = append(problems, validatePromotionLifecycleCommands(promotion)...)

	owners := make(map[string][]string, len(promotionTargets))
	for workflowName, workflow := range map[string]map[string]any{
		promotionWorkflowName:      workflows.promotion,
		"ci.yml":                   workflows.ci,
		"critical-integration.yml": workflows.critical,
	} {
		for _, target := range ownedPromotionTargets(workflow) {
			owners[target] = append(owners[target], workflowName)
		}
	}
	for _, target := range promotionTargets {
		if len(owners[target]) != 1 || owners[target][0] != promotionWorkflowName {
			problems = append(problems, fmt.Sprintf("%s owners = %v, want promotion workflow exactly once",
				target, owners[target]))
		}
	}
	ciJobs := mustMap(workflows.ci["jobs"])
	criticalJobs := mustMap(workflows.critical["jobs"])
	for obsolete, criticalOwner := range removedCIShadowJobs {
		if mustMap(ciJobs[obsolete]) != nil {
			problems = append(problems, fmt.Sprintf("ci.yml retains obsolete integration shadow %q", obsolete))
		}
		if criticalOwner != "" && mustMap(criticalJobs[criticalOwner]) == nil {
			problems = append(problems, fmt.Sprintf("critical owner %q for removed shadow %q is absent",
				criticalOwner, obsolete))
		}
	}
	problems = append(problems, validateCriticalShadowOwners(workflows.critical)...)
	ciDispatch := mustMap(mustMap(workflows.ci["on"])["workflow_dispatch"])
	ciInputs := mustMap(ciDispatch["inputs"])
	for _, obsolete := range removedCIDispatchInputs {
		if _, found := ciInputs[obsolete]; found {
			problems = append(problems, fmt.Sprintf("ci.yml retains obsolete dispatch input %q", obsolete))
		}
	}
	return problems
}

func validatePromotionPrerequisites(workflow map[string]any) []string {
	var problems []string
	for _, name := range promotionIntegrationJobs {
		want := []string{"promotion-assets", "sdk-smoke-validate", "phase18-25-evidence"}
		if name == "phase18-25-terraform-integration" {
			want = []string{"promotion-assets", "phase18-25-evidence", "terraform-validate"}
		}
		if got := stringSlice(promotionJob(workflow, name)["needs"]); !sameStringSet(got, want) {
			problems = append(problems, fmt.Sprintf("%s needs = %v, want %v", name, got, want))
		}
	}

	requiredCommands := map[string][]string{
		"promotion-assets": {
			"npm ci",
			"npm run build",
		},
		"sdk-smoke-validate": {
			"python -m pip install --requirement sdk-smoke/python/requirements.txt",
			"go test ./sdk-smoke/go ./sdk-smoke/phase15 ./sdk-smoke/phase16 ./sdk-smoke/phase16-logging ./sdk-smoke/phase16-dns ./sdk-smoke/phase16-subnetwork ./sdk-smoke/phase16-vertex ./sdk-smoke/phase18-25 ./sdk-smoke/phase19 ./sdk-smoke/phase20 ./sdk-smoke/phase21-22 ./sdk-smoke/phase23 ./sdk-smoke/phase24-25",
			"python -m compileall -q sdk-smoke/python",
			"make test-java-sdk-compile",
		},
		"phase18-25-evidence": {"make test-phase18-25-evidence"},
		"terraform-validate": {
			"terraform fmt -check -recursive",
			"terraform init -backend=false -input=false -lockfile=readonly",
			"terraform validate",
		},
	}
	assets := promotionJob(workflow, "promotion-assets")
	if timeout, ok := integer(assets["timeout-minutes"]); !ok || timeout != 10 {
		problems = append(problems, "promotion asset preparation timeout must be 10 minutes")
	}
	if firstStepUsing(assets, setupGoAction) != nil {
		problems = append(problems, "promotion asset preparation must not set up Go")
	}
	for _, command := range []string{
		"make check-docs-truth",
		`test -z "$(gofmt -l cmd pkg ui)"`,
		"go vet ./cmd/... ./pkg/... ./ui",
		"go test -race ./cmd/... ./pkg/... ./ui",
		"go test -count=1 ./scripts",
		"go build -trimpath ./cmd/minisky",
	} {
		if jobHasActiveCommand(assets, command) {
			problems = append(problems,
				fmt.Sprintf("promotion asset preparation duplicates CI command %q", command))
		}
	}
	for jobName, commands := range requiredCommands {
		job := promotionJob(workflow, jobName)
		for _, command := range commands {
			if !jobHasActiveCommand(job, command) {
				problems = append(problems,
					fmt.Sprintf("prerequisite %s lacks exact active command %q", jobName, command))
			}
		}
		for _, step := range stepMaps(job) {
			if _, found := step["if"]; found {
				problems = append(problems,
					fmt.Sprintf("prerequisite %s step %q has an unexpected condition",
						jobName, scalarString(step["name"])))
			}
			run := scalarString(step["run"])
			for _, target := range promotionTargets {
				if strings.Contains(run, "make "+target) {
					problems = append(problems,
						fmt.Sprintf("prerequisite %s accidentally runs integration target %s", jobName, target))
				}
			}
			if strings.Contains(run, "docker info") ||
				strings.Contains(run, "provision-required-images.sh") ||
				regexp.MustCompile(`MINISKY_[A-Z0-9_]+_INTEGRATION=1`).MatchString(run) {
				problems = append(problems,
					fmt.Sprintf("prerequisite %s contains unsafe integration activation", jobName))
			}
		}
	}
	return problems
}

func validateAuthoritativePrerequisiteParity(promotion, ci map[string]any) []string {
	var problems []string
	for _, jobName := range []string{"terraform-validate"} {
		authoritative := commandStepsByName(promotionJob(ci, jobName))
		candidate := commandStepsByName(promotionJob(promotion, jobName))
		if len(candidate) != len(authoritative) {
			problems = append(problems, fmt.Sprintf(
				"promotion %s has %d command steps, authoritative CI has %d",
				jobName, len(candidate), len(authoritative)))
		}
		for name, source := range authoritative {
			target := candidate[name]
			if target == nil {
				problems = append(problems,
					fmt.Sprintf("promotion %s omits authoritative CI step %q", jobName, name))
				continue
			}
			if scalarString(target["run"]) != scalarString(source["run"]) ||
				scalarString(target["working-directory"]) != scalarString(source["working-directory"]) ||
				!sameScalarMap(mustMap(target["env"]), mustMap(source["env"])) {
				problems = append(problems,
					fmt.Sprintf("promotion %s step %q drifts from authoritative CI command semantics",
						jobName, name))
			}
		}
		for name := range candidate {
			if authoritative[name] == nil {
				problems = append(problems,
					fmt.Sprintf("promotion %s adds non-authoritative command step %q", jobName, name))
			}
		}
	}
	return problems
}

func commandStepsByName(job map[string]any) map[string]map[string]any {
	result := make(map[string]map[string]any)
	for _, step := range stepMaps(job) {
		if scalarString(step["run"]) != "" {
			result[scalarString(step["name"])] = step
		}
	}
	return result
}

func sameScalarMap(got, want map[string]any) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if scalarString(got[key]) != scalarString(value) {
			return false
		}
	}
	return true
}

func validatePromotionArtifactFlow(workflow map[string]any) []string {
	var problems []string
	assets := promotionJob(workflow, "promotion-assets")
	upload := firstStepUsing(assets, uploadAction)
	if upload == nil {
		problems = append(problems, "promotion asset prerequisite does not produce the UI artifact")
	} else {
		with := mustMap(upload["with"])
		if scalarString(with["name"]) != "promotion-ui-dist" ||
			scalarString(with["path"]) != "ui/dist" ||
			scalarString(with["if-no-files-found"]) != "error" {
			problems = append(problems, "promotion UI artifact contract is incorrect")
		}
	}
	for _, name := range promotionIntegrationJobs {
		download := firstStepUsing(promotionJob(workflow, name), promotionDownloadAction)
		if download == nil {
			problems = append(problems, fmt.Sprintf("%s does not consume the promotion UI artifact", name))
			continue
		}
		with := mustMap(download["with"])
		if scalarString(with["name"]) != "promotion-ui-dist" ||
			scalarString(with["path"]) != "ui/dist" {
			problems = append(problems, fmt.Sprintf("%s consumes the wrong promotion artifact", name))
		}
	}
	return problems
}

func validateExactPromotionCleanup(jobName string, cleanup map[string]any) []string {
	lockPaths := map[string]string{
		"phase18-25-terraform-integration":  `${TMPDIR:-/tmp}/${{ matrix.lock_name }}`,
		"phase18-25-sdk-integration":        `${TMPDIR:-/tmp}/minisky-phase18-25-sdk-integration.lock`,
		"phase19-sdk-integration":           `${TMPDIR:-/tmp}/minisky-phase19-sdk-integration.lock`,
		"phase19-heavy-backend-integration": `${TMPDIR:-/tmp}/minisky-phase19-sdk-integration.lock`,
		"phase20-sdk-integration":           `${TMPDIR:-/tmp}/minisky-phase20-sdk-integration.lock`,
		"phase21-22-sdk-integration":        `${TMPDIR:-/tmp}/minisky-phase21-22-sdk-integration.lock`,
		"phase23-sdk-integration":           `${TMPDIR:-/tmp}/minisky-phase23-sdk-integration.lock`,
		"phase24-25-sdk-integration":        `${TMPDIR:-/tmp}/minisky-phase24-25-sdk-integration.lock`,
	}
	lockPath := lockPaths[jobName]
	expectedRemove := `rm -rf "` + lockPath + `"`
	expectedVerify := `test ! -d "` + lockPath + `"`
	lines := activeCommandLines(scalarString(cleanup["run"]))
	var problems []string
	if !contains(lines, expectedRemove) || !contains(lines, expectedVerify) {
		problems = append(problems,
			fmt.Sprintf("%s cleanup does not remove and verify its exact lock %q", jobName, lockPath))
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "rm -rf ") && line != expectedRemove {
			problems = append(problems,
				fmt.Sprintf("%s cleanup removes unrelated path: %q", jobName, line))
		}
		if shellLineMasksFailure(line) {
			problems = append(problems,
				fmt.Sprintf("%s cleanup masks shell failure: %q", jobName, line))
		}
	}
	if jobName == "phase19-heavy-backend-integration" {
		run := scalarString(cleanup["run"])
		if !strings.Contains(run, `label=minisky.profile=${MINISKY_PHASE19_PROFILE}`) ||
			!strings.Contains(run, `docker rm -f -v "${container}"`) ||
			scalarString(mustMap(cleanup["env"])["MINISKY_PHASE19_PROFILE"]) !=
				"${{ env.PHASE19_HEAVY_PROFILE }}" {
			problems = append(problems,
				"phase19-heavy-backend-integration cleanup lacks exact profile-owned Docker removal")
		}
	}
	return problems
}

func validatePromotionLifecycleCommands(workflow map[string]any) []string {
	var problems []string
	exactCommands := map[string]string{
		"phase18-25-terraform-integration":  `make "${{ matrix.make_target }}" 2>&1 | tee "terraform-${{ matrix.id }}.log"`,
		"phase18-25-sdk-integration":        "make test-phase18-25-sdk 2>&1 | tee phase18-25-sdk-integration.log",
		"phase19-sdk-integration":           "make test-phase19-sdk 2>&1 | tee phase19-sdk-integration.log",
		"phase19-heavy-backend-integration": "make test-phase19-heavy-backend 2>&1 | tee phase19-heavy-backend-integration.log",
		"phase20-sdk-integration":           "make test-phase20-sdk 2>&1 | tee phase20-sdk-integration.log",
		"phase21-22-sdk-integration":        "make test-phase21-22-sdk 2>&1 | tee phase21-22-sdk-integration.log",
		"phase23-sdk-integration":           "make test-phase23-sdk 2>&1 | tee phase23-sdk-integration.log",
		"phase24-25-sdk-integration":        "make test-phase24-25-sdk 2>&1 | tee phase24-25-sdk-integration.log",
	}
	for jobName, command := range exactCommands {
		job := promotionJob(workflow, jobName)
		var lifecycle map[string]any
		for _, step := range stepMaps(job) {
			if contains(activeCommandLines(scalarString(step["run"])), command) {
				lifecycle = step
				break
			}
		}
		if lifecycle == nil {
			problems = append(problems,
				fmt.Sprintf("%s lacks its enabled exact lifecycle command", jobName))
			continue
		}
		if _, found := lifecycle["if"]; found {
			problems = append(problems,
				fmt.Sprintf("%s lifecycle has an unexpected condition", jobName))
		}
		lines := activeCommandLines(scalarString(lifecycle["run"]))
		commandIndex, pipefailIndex := -1, -1
		for index, line := range lines {
			if line == "set -o pipefail" || strings.Contains(line, "set -Eeuo pipefail") {
				pipefailIndex = index
			}
			if line == command {
				commandIndex = index
			}
			if shellLineMasksFailure(line) {
				problems = append(problems,
					fmt.Sprintf("%s lifecycle masks shell failure: %q", jobName, line))
			}
		}
		if commandIndex < 0 || pipefailIndex < 0 || pipefailIndex > commandIndex {
			problems = append(problems,
				fmt.Sprintf("%s lifecycle does not enable pipefail before its exact command", jobName))
		}
	}
	for jobName, target := range promotionJobTargets {
		if !strings.Contains(exactCommands[jobName], "make "+target+" ") {
			problems = append(problems, fmt.Sprintf("%s does not own exact target %s", jobName, target))
		}
	}
	heavy := promotionJob(workflow, "phase19-heavy-backend-integration")
	if scalarString(mustMap(heavy["env"])["PHASE19_HEAVY_PROFILE"]) !=
		"phase19-heavy-${{ github.run_id }}-${{ github.run_attempt }}" {
		problems = append(problems, "Phase 19 heavy lifecycle lacks its exact run-owned profile")
	}
	heavyLifecycle := stepByName(heavy, "Run Phase 19 heavy backend integration")
	if scalarString(mustMap(heavyLifecycle["env"])["MINISKY_PHASE19_PROFILE"]) !=
		"${{ env.PHASE19_HEAVY_PROFILE }}" {
		problems = append(problems, "Phase 19 heavy lifecycle does not use its exact run-owned profile")
	}
	return problems
}

type criticalShadowContract struct {
	owner        string
	command      string
	matrixTarget string
	env          map[string]string
	artifact     bool
}

func validateCriticalShadowOwners(workflow map[string]any) []string {
	var problems []string
	triggers := mustMap(workflow["on"])
	if mustMap(triggers["pull_request"]) == nil {
		problems = append(problems, "critical workflow no longer runs automatically on pull requests")
	}
	push := mustMap(triggers["push"])
	if push == nil || !contains(stringSlice(push["branches"]), "main") {
		problems = append(problems, "critical workflow no longer runs automatically on main pushes")
	}
	permissions := mustMap(workflow["permissions"])
	if len(permissions) != 1 || scalarString(permissions["contents"]) != "read" {
		problems = append(problems, "critical workflow no longer has least-privilege contents access")
	}
	prepare := promotionJob(workflow, "prepare")
	prepareUpload := firstStepUsing(prepare, uploadAction)
	if prepare == nil || stepIsAlwaysDisabled(prepare) || prepareUpload == nil ||
		scalarString(mustMap(prepareUpload["with"])["name"]) != "critical-integration-ui-dist" {
		problems = append(problems, "critical workflow lacks enabled bounded preparation artifact production")
	}

	contracts := map[string]criticalShadowContract{
		"terraform-integration": {
			owner: "terraform-provider", command: "./scripts/terraform-integration.sh",
			env: map[string]string{
				"MINISKY_TERRAFORM_INTEGRATION": "1", "MINISKY_TERRAFORM_CLOUDSQL": "1",
				"MINISKY_TERRAFORM_GKE": "1", "MINISKY_TERRAFORM_JAVA_SMOKE": "1",
				"MINISKY_TERRAFORM_PHASE15": "1",
			},
		},
		"state-durability-integration": {
			owner: "state-durability", command: "./scripts/state-durability-integration.sh",
			env: map[string]string{"MINISKY_STATE_DURABILITY_INTEGRATION": "1"},
		},
		"event-delivery-integration": {
			owner:   "event-delivery",
			command: "make test-event-delivery 2>&1 | tee critical-phase9-event-delivery.log", artifact: true,
		},
		"phase10-artifact-integration": {
			owner:   "artifact-registry",
			command: "make test-phase10-artifact 2>&1 | tee critical-phase10-artifact.log", artifact: true,
		},
		"phase13-wif-integration": {
			owner:   "workload-identity",
			command: "make test-phase13-wif 2>&1 | tee critical-phase13-wif.log", artifact: true,
		},
		"phase15-emulator-integration": {
			owner:   "data-emulators",
			command: "./scripts/phase15-emulator-integration.sh 2>&1 | tee critical-phase15-emulators.log",
			env:     map[string]string{"MINISKY_PHASE15_INTEGRATION": "1"}, artifact: true,
		},
		"phase16-monitoring-integration": {
			owner: "phase16-service-lifecycles", matrixTarget: "test-phase16-monitoring", artifact: true,
		},
		"phase16-logging-integration": {
			owner: "phase16-service-lifecycles", matrixTarget: "test-phase16-logging", artifact: true,
		},
		"phase16-dns-integration": {
			owner: "phase16-service-lifecycles", matrixTarget: "test-phase16-dns", artifact: true,
		},
		"phase16-vertex-integration": {
			owner: "phase16-service-lifecycles", matrixTarget: "test-phase16-vertex", artifact: true,
		},
		"phase16-subnetwork-integration": {
			owner:   "subnetwork-sdk",
			command: "make test-phase16-subnetwork 2>&1 | tee critical-phase16-subnetwork-sdk.log", artifact: true,
		},
		"phase16-subnetwork-terraform-integration": {
			owner:    "subnetwork-terraform",
			command:  "make test-phase16-subnetwork-terraform 2>&1 | tee critical-phase16-subnetwork-terraform.log",
			artifact: true,
		},
		"phase17-enterprise-integration": {
			owner:   "enterprise-controls",
			command: "make test-phase17-enterprise 2>&1 | tee critical-phase17-enterprise.log", artifact: true,
		},
	}
	for shadow, contract := range contracts {
		job := promotionJob(workflow, contract.owner)
		if job == nil {
			problems = append(problems, fmt.Sprintf("critical owner %q for %q is absent", contract.owner, shadow))
			continue
		}
		if _, found := job["if"]; found {
			problems = append(problems, fmt.Sprintf("critical owner %q has an unexpected condition", contract.owner))
		}
		if !sameStringSet(stringSlice(job["needs"]), []string{"prepare"}) {
			problems = append(problems,
				fmt.Sprintf("critical owner %q no longer depends on bounded preparation", contract.owner))
		}
		if _, found := job["continue-on-error"]; found {
			problems = append(problems, fmt.Sprintf("critical owner %q contains continue-on-error", contract.owner))
		}
		if timeout, ok := integer(job["timeout-minutes"]); !ok || timeout <= 0 || timeout > 60 {
			problems = append(problems, fmt.Sprintf("critical owner %q has no bounded timeout", contract.owner))
		}
		if jobPermissions, found := job["permissions"]; found {
			permissions := mustMap(jobPermissions)
			if len(permissions) != 1 || scalarString(permissions["contents"]) != "read" {
				problems = append(problems,
					fmt.Sprintf("critical owner %q broadens workflow permissions", contract.owner))
			}
		}
		download := firstStepUsing(job, promotionDownloadAction)
		if download == nil ||
			scalarString(mustMap(download["with"])["name"]) != "critical-integration-ui-dist" {
			problems = append(problems,
				fmt.Sprintf("critical owner %q does not consume bounded preparation", contract.owner))
		}
		for _, step := range stepMaps(job) {
			if _, found := step["continue-on-error"]; found {
				problems = append(problems,
					fmt.Sprintf("critical owner %q step contains continue-on-error", contract.owner))
			}
		}
		var lifecycle map[string]any
		if contract.matrixTarget != "" {
			if !criticalMatrixOwnsTarget(job, contract.matrixTarget) ||
				!jobHasActiveCommand(job, `make "${MAKE_TARGET}" 2>&1 | tee "${LOG_FILE}"`) {
				problems = append(problems,
					fmt.Sprintf("critical owner %q lacks matrix target %s", contract.owner, contract.matrixTarget))
			}
			lifecycle = stepByName(job, "Run guarded service lifecycle")
		} else {
			for _, step := range stepMaps(job) {
				if contains(activeCommandLines(scalarString(step["run"])), contract.command) {
					lifecycle = step
					break
				}
			}
			if lifecycle == nil {
				problems = append(problems,
					fmt.Sprintf("critical owner %q lacks exact command %q", contract.owner, contract.command))
			}
		}
		if lifecycle != nil {
			if _, found := lifecycle["if"]; found {
				problems = append(problems,
					fmt.Sprintf("critical owner %q lifecycle has an unexpected condition", contract.owner))
			}
			env := mustMap(lifecycle["env"])
			for key, want := range contract.env {
				if scalarString(env[key]) != want {
					problems = append(problems,
						fmt.Sprintf("critical owner %q env %s is not %q", contract.owner, key, want))
				}
			}
		}
		if contract.artifact {
			artifact := firstStepUsing(job, uploadAction)
			if artifact == nil || normalizeCondition(scalarString(artifact["if"])) != "failure()" {
				problems = append(problems,
					fmt.Sprintf("critical owner %q lacks bounded failure diagnostics", contract.owner))
			} else {
				with := mustMap(artifact["with"])
				retention, retained := integer(with["retention-days"])
				if scalarString(with["if-no-files-found"]) != "error" ||
					!retained || retention != 7 {
					problems = append(problems,
						fmt.Sprintf("critical owner %q failure diagnostics are not bounded", contract.owner))
				}
			}
		}
	}
	return problems
}

func criticalMatrixOwnsTarget(job map[string]any, target string) bool {
	matrix := mustMap(mustMap(job["strategy"])["matrix"])
	entries, _ := matrix["include"].([]any)
	for _, rawEntry := range entries {
		if scalarString(mustMap(rawEntry)["target"]) == target {
			return true
		}
	}
	return false
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	values := make(map[string]int, len(got))
	for _, value := range got {
		values[value]++
	}
	for _, value := range want {
		values[value]--
	}
	for _, count := range values {
		if count != 0 {
			return false
		}
	}
	return true
}

func shellLineMasksFailure(line string) bool {
	compact := strings.Join(strings.Fields(line), " ")
	return strings.Contains(compact, "set +e") ||
		strings.Contains(compact, "set +o pipefail") ||
		strings.Contains(compact, "|| true") ||
		strings.Contains(compact, "|| :") ||
		compact == "true" || compact == ":" ||
		exitZeroPattern.MatchString(strings.NewReplacer(`"`, "", `'`, "").Replace(compact))
}

func stepIsAlwaysDisabled(step map[string]any) bool {
	condition, found := step["if"]
	if !found || condition == nil {
		return false
	}
	if conditionIsFalse(condition) {
		return true
	}
	normalized := normalizeCondition(scalarString(condition))
	if normalized == "!true" || strings.HasPrefix(normalized, "false&&") {
		return true
	}
	equality := regexp.MustCompile(`^([0-9]+)==([0-9]+)$`).FindStringSubmatch(normalized)
	return len(equality) == 3 && equality[1] != equality[2]
}

func compactWorkflowExpression(value string) string {
	return regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(value), "")
}

func readPromotionWorkflowSet(t *testing.T) promotionWorkflowSet {
	t.Helper()
	return promotionWorkflowSet{
		promotion: readWorkflow(t, promotionWorkflowName),
		ci:        readWorkflow(t, "ci.yml"),
		critical:  readWorkflow(t, "critical-integration.yml"),
	}
}

func clonePromotionWorkflowSet(t *testing.T, workflows promotionWorkflowSet) promotionWorkflowSet {
	t.Helper()
	clone := func(source map[string]any) map[string]any {
		t.Helper()
		data, err := yaml.Marshal(source)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := yaml.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	return promotionWorkflowSet{
		promotion: clone(workflows.promotion),
		ci:        clone(workflows.ci),
		critical:  clone(workflows.critical),
	}
}

func promotionJob(workflow map[string]any, name string) map[string]any {
	return mustMap(mustMap(workflow["jobs"])[name])
}

func promotionCleanupStep(job map[string]any) map[string]any {
	for _, step := range stepMaps(job) {
		name := scalarString(step["name"])
		if strings.Contains(name, "cleanup") || strings.HasPrefix(name, "Remove exact-owned") {
			return step
		}
	}
	return nil
}

func promotionFailureArtifact(job map[string]any) map[string]any {
	for _, step := range stepMaps(job) {
		if scalarString(step["uses"]) == uploadAction {
			return step
		}
	}
	return nil
}

func removePromotionStep(job map[string]any, name string) {
	steps, _ := job["steps"].([]any)
	filtered := make([]any, 0, len(steps))
	for _, raw := range steps {
		if scalarString(mustMap(raw)["name"]) != name {
			filtered = append(filtered, raw)
		}
	}
	job["steps"] = filtered
}

func ownedPromotionTargets(workflow map[string]any) []string {
	targetSet := make(map[string]bool, len(promotionTargets))
	for _, target := range promotionTargets {
		targetSet[target] = true
	}
	var owned []string
	for _, rawJob := range mustMap(workflow["jobs"]) {
		job := mustMap(rawJob)
		strategy := mustMap(job["strategy"])
		matrix := mustMap(strategy["matrix"])
		entries, _ := matrix["include"].([]any)
		for _, rawEntry := range entries {
			target := scalarString(mustMap(rawEntry)["make_target"])
			if targetSet[target] {
				owned = append(owned, target)
			}
		}
		for _, step := range stepMaps(job) {
			for _, line := range activeCommandLines(scalarString(step["run"])) {
				fields := strings.Fields(line)
				for index, field := range fields {
					if field == "make" && index+1 < len(fields) {
						target := strings.Trim(fields[index+1], `"'`)
						if targetSet[target] {
							owned = append(owned, target)
						}
					}
				}
			}
		}
	}
	return owned
}

func withoutString(values []string, omitted string) []any {
	result := make([]any, 0, len(values))
	for _, value := range values {
		if value != omitted {
			result = append(result, value)
		}
	}
	return result
}
