package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	checkoutAction   = "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
	setupNodeAction  = "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020"
	setupGoAction    = "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16"
	setupTFAction    = "hashicorp/setup-terraform@dfe3c3f87815947d99a8997f908cb6525fc44e9e"
	uploadAction     = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
	exactMakeCommand = "make test-memcache-integration 2>&1 | tee critical-memcache-integration.log"
)

func TestCriticalMemcacheIntegrationWorkflowContract(t *testing.T) {
	workflow := readWorkflow(t, "critical-integration.yml")
	if errors := validateCriticalMemcacheWorkflow(workflow); len(errors) != 0 {
		t.Fatalf("critical Memcached workflow contract failed:\n- %s", strings.Join(errors, "\n- "))
	}
}

func TestCriticalMemcacheJobCannotBeSkippedByOrderingDependencies(t *testing.T) {
	job := workflowJob(t, readWorkflow(t, "critical-integration.yml"), "memcache-integration")
	if _, found := job["needs"]; found {
		t.Fatal("self-contained Memcached required check has ordering dependencies and can be skipped when they fail")
	}
	if condition, found := job["if"]; found {
		t.Fatalf("Memcached required check has forbidden job condition %q", scalarString(condition))
	}
}

func TestMatrixAndMemcacheJobsRejectSharedConcurrency(t *testing.T) {
	criticalJob := workflowJob(t, readWorkflow(t, "critical-integration.yml"), "memcache-integration")
	if _, found := criticalJob["concurrency"]; found {
		t.Error("Memcached job must not use cross-runner shared concurrency")
	}

	promotion := readWorkflow(t, "promotion-integration.yml")
	matrixJob := workflowJob(t, promotion, "phase18-25-terraform-integration")
	if _, found := matrixJob["concurrency"]; found {
		t.Error("12-leg Terraform matrix must not use job concurrency that cancels pending legs")
	}
	for name, value := range mustMap(promotion["jobs"]) {
		job := mustMap(value)
		if concurrency, found := job["concurrency"]; found &&
			strings.Contains(scalarString(mustMap(concurrency)["group"]), "minisky-net-integration") {
			t.Errorf("%s retains dangerous repository-wide minisky-net concurrency", name)
		}
	}
}

func TestCriticalMemcacheWorkflowRejectsStaticMutations(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "job concurrency", mutate: func(workflow map[string]any) {
			workflowJobMap(workflow)["concurrency"] = map[string]any{"group": "minisky-net-integration"}
		}},
		{name: "ordering dependency", mutate: func(workflow map[string]any) {
			workflowJobMap(workflow)["needs"] = []any{"prepare"}
		}},
		{name: "job disabled", mutate: func(workflow map[string]any) {
			workflowJobMap(workflow)["if"] = false
		}},
		{name: "job semantically never", mutate: func(workflow map[string]any) {
			workflowJobMap(workflow)["if"] = "${{ 1 == 2 }}"
		}},
		{name: "job continue on error", mutate: func(workflow map[string]any) {
			workflowJobMap(workflow)["continue-on-error"] = true
		}},
		{name: "commented exact target", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")["run"] =
				"set -o pipefail\n# " + exactMakeCommand
		}},
		{name: "disabled exact target", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")["if"] = false
		}},
		{name: "conditional exact target", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")["if"] = "${{ always() }}"
		}},
		{name: "semantically disabled prerequisite", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Verify Docker availability")["if"] = "${{ !cancelled() && false }}"
		}},
		{name: "target fake pass", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = scalarString(step["run"]) + " || true"
		}},
		{name: "target colon fake pass", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = scalarString(step["run"]) + " || :"
		}},
		{name: "errexit disabled", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = "set +e\n" + scalarString(step["run"])
		}},
		{name: "trailing success exit", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = scalarString(step["run"]) + "\nexit 0"
		}},
		{name: "embedded success exit", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Initialize pinned Memcached provider")
			step["run"] = scalarString(step["run"]) + "; exit 0"
		}},
		{name: "quoted success exit", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Verify Docker availability")
			step["run"] = scalarString(step["run"]) + "\nexit \"0\""
		}},
		{name: "command substitution wrapper", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = "result=\"$(\nset -o pipefail\n" + exactMakeCommand + "\n)\"\nprintf '%s\\n' \"${result}\""
		}},
		{name: "background subshell", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = "(\nset -o pipefail\n" + exactMakeCommand + "\n) &"
		}},
		{name: "function wrapper discards status", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = "run_lifecycle() {\nset -o pipefail\n" + exactMakeCommand + "\n}\nrun_lifecycle\ntrue"
		}},
		{name: "pipeline without pipefail", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")["run"] = exactMakeCommand
		}},
		{name: "pipefail enabled too late", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")["run"] =
				exactMakeCommand + "\nset -o pipefail"
		}},
		{name: "pipefail disabled", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = "set -o pipefail\nset +o pipefail\n" + exactMakeCommand
		}},
		{name: "result status overwritten", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")
			step["run"] = scalarString(step["run"]) + "\nresult_status=$?\nresult_status=0\nexit \"${result_status}\""
		}},
		{name: "step continue on error", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Run hardened Memcached integration lifecycle")["continue-on-error"] = true
		}},
		{name: "commented docker prerequisite", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Verify Docker availability")["run"] = "# docker info >/dev/null"
		}},
		{name: "docker prerequisite masked", mutate: func(workflow map[string]any) {
			step := stepByName(workflowJobMap(workflow), "Verify Docker availability")
			step["run"] = scalarString(step["run"]) + " || :"
		}},
		{name: "commented validation fixture", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Initialize pinned Memcached provider")["run"] =
				"# terraform -chdir=terraform/memcache init -backend=false -input=false -lockfile=readonly"
		}},
		{name: "unpinned action", mutate: func(workflow map[string]any) {
			firstStepUsing(workflowJobMap(workflow), checkoutAction)["uses"] = "actions/checkout@main"
		}},
		{name: "cleanup not always", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Clean exact-owned Memcached resources")["if"] = "failure()"
		}},
		{name: "cleanup condition widened", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Clean exact-owned Memcached resources")["if"] = "${{ always() && true }}"
		}},
		{name: "cleanup unbounded", mutate: func(workflow map[string]any) {
			delete(stepByName(workflowJobMap(workflow), "Clean exact-owned Memcached resources"), "timeout-minutes")
		}},
		{name: "diagnostics miss cancellation", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Capture Memcached failure diagnostics")["if"] = "failure()"
		}},
		{name: "diagnostics condition reordered", mutate: func(workflow map[string]any) {
			stepByName(workflowJobMap(workflow), "Capture Memcached failure diagnostics")["if"] =
				"${{ cancelled() || failure() }}"
		}},
		{name: "artifact misses cancellation", mutate: func(workflow map[string]any) {
			firstStepUsing(workflowJobMap(workflow), uploadAction)["if"] = "failure()"
		}},
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			workflow := readWorkflow(t, "critical-integration.yml")
			mutation.mutate(workflow)
			if errors := validateCriticalMemcacheWorkflow(workflow); len(errors) == 0 {
				t.Fatal("mutated workflow unexpectedly satisfied the structural contract")
			}
		})
	}
}

func TestCIValidatesMemcacheIntegrationScript(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	if errors := validateRequiredCIJobs(workflow); len(errors) != 0 {
		t.Fatalf("required CI validation contract failed:\n- %s", strings.Join(errors, "\n- "))
	}
}

func TestRequiredCIJobsRejectStaticBypasses(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "scripts job condition", mutate: func(workflow map[string]any) {
			mustMap(mustMap(workflow["jobs"])["quality-scripts"])["if"] = "${{ github.ref == 'never' }}"
		}},
		{name: "terraform job false condition", mutate: func(workflow map[string]any) {
			mustMap(mustMap(workflow["jobs"])["terraform-validate"])["if"] = false
		}},
		{name: "scripts test step condition", mutate: func(workflow map[string]any) {
			stepByName(mustMap(mustMap(workflow["jobs"])["quality-scripts"]), "Test integration script contracts")["if"] =
				"${{ success() && false }}"
		}},
		{name: "shellcheck step condition", mutate: func(workflow map[string]any) {
			stepByName(mustMap(mustMap(workflow["jobs"])["terraform-validate"]), "Validate Memcached integration script")["if"] =
				"${{ always() }}"
		}},
		{name: "scripts test set plus e", mutate: func(workflow map[string]any) {
			step := stepByName(mustMap(mustMap(workflow["jobs"])["quality-scripts"]), "Test integration script contracts")
			step["run"] = "set +e\n" + scalarString(step["run"])
		}},
		{name: "shellcheck masked with true", mutate: func(workflow map[string]any) {
			step := stepByName(mustMap(mustMap(workflow["jobs"])["terraform-validate"]), "Validate Memcached integration script")
			step["run"] = scalarString(step["run"]) + " || true"
		}},
		{name: "scripts test backgrounded", mutate: func(workflow map[string]any) {
			step := stepByName(mustMap(mustMap(workflow["jobs"])["quality-scripts"]), "Test integration script contracts")
			step["run"] = "(\n" + scalarString(step["run"]) + "\n) &"
		}},
		{name: "shellcheck status overwritten", mutate: func(workflow map[string]any) {
			step := stepByName(mustMap(mustMap(workflow["jobs"])["terraform-validate"]), "Validate Memcached integration script")
			step["run"] = scalarString(step["run"]) + "\ncheck_status=$?\ncheck_status=0\nexit \"${check_status}\""
		}},
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			workflow := readWorkflow(t, "ci.yml")
			mutation.mutate(workflow)
			if errors := validateRequiredCIJobs(workflow); len(errors) == 0 {
				t.Fatal("mutated required validation job unexpectedly satisfied the structural contract")
			}
		})
	}
}

func validateCriticalMemcacheWorkflow(workflow map[string]any) []string {
	var errors []string
	triggers := mustMap(workflow["on"])
	for _, event := range []string{"pull_request", "push"} {
		trigger := mustMap(triggers[event])
		paths := stringSlice(trigger["paths"])
		for _, required := range []string{
			".github/workflows/critical-integration.yml",
			".github/workflows/ci.yml",
			"scripts/memcache-integration.sh",
			"scripts/memcache_ci_workflow_contract_test.go",
			"terraform/memcache/**",
		} {
			if !contains(paths, required) {
				errors = append(errors, fmt.Sprintf("%s paths omit %q", event, required))
			}
		}
	}

	job := workflowJobMap(workflow)
	if _, found := job["needs"]; found {
		errors = append(errors, "self-contained required job must not have needs")
	}
	if _, found := job["concurrency"]; found {
		errors = append(errors, "Memcached job must not have shared concurrency")
	}
	if _, found := job["if"]; found {
		errors = append(errors, "Memcached job must not have an if condition")
	}
	if _, found := job["continue-on-error"]; found {
		errors = append(errors, "Memcached job contains continue-on-error")
	}
	timeout, ok := integer(job["timeout-minutes"])
	if !ok || timeout <= 0 || timeout > 30 {
		errors = append(errors, "Memcached job timeout must be between 1 and 30 minutes")
	}

	steps := stepMaps(job)
	for _, step := range steps {
		if _, found := step["continue-on-error"]; found {
			errors = append(errors, fmt.Sprintf("step %q contains continue-on-error", scalarString(step["name"])))
		}
		name := scalarString(step["name"])
		switch name {
		case "Clean exact-owned Memcached resources":
			if !conditionIsAlways(step["if"]) {
				errors = append(errors, "cleanup must use exactly always()")
			}
		case "Capture Memcached failure diagnostics", "Retain Memcached failure diagnostics":
			if !conditionCoversFailureAndCancellation(step["if"]) {
				errors = append(errors, fmt.Sprintf("%s must use failure() || cancelled()", name))
			}
		default:
			if _, found := step["if"]; found {
				errors = append(errors, fmt.Sprintf("required step %q must not have an if condition", name))
			}
		}
	}

	for _, action := range []string{checkoutAction, setupNodeAction, setupGoAction, setupTFAction, uploadAction} {
		if firstStepUsing(job, action) == nil {
			errors = append(errors, fmt.Sprintf("pinned action %q is absent", action))
		}
	}
	terraform := firstStepUsing(job, setupTFAction)
	if terraform == nil || scalarString(mustMap(terraform["with"])["terraform_version"]) != "1.15.8" {
		errors = append(errors, "Terraform setup is not pinned to 1.15.8")
	}
	for _, command := range []string{
		"npm ci",
		"npm run build",
		"docker info >/dev/null",
		"terraform -chdir=terraform/memcache init -backend=false -input=false -lockfile=readonly",
		"set -o pipefail",
		exactMakeCommand,
	} {
		if !jobHasActiveCommand(job, command) {
			errors = append(errors, fmt.Sprintf("active command %q is absent", command))
		}
	}
	for _, required := range []struct {
		step    string
		command string
	}{
		{step: "Build UI from lockfile", command: "npm ci"},
		{step: "Build UI from lockfile", command: "npm run build"},
		{step: "Verify Docker availability", command: "docker info >/dev/null"},
		{step: "Initialize pinned Memcached provider", command: "terraform -chdir=terraform/memcache init -backend=false -input=false -lockfile=readonly"},
		{step: "Run hardened Memcached integration lifecycle", command: exactMakeCommand},
	} {
		errors = append(errors, validateRequiredShellStep(job, required.step, required.command)...)
	}
	errors = append(errors, validateLifecyclePipefail(job)...)

	cleanup := stepByName(job, "Clean exact-owned Memcached resources")
	if cleanup == nil || !conditionIsAlways(cleanup["if"]) {
		errors = append(errors, "cleanup must run with always()")
	}
	if cleanup == nil {
		errors = append(errors, "cleanup step is absent")
	} else if timeout, ok := integer(cleanup["timeout-minutes"]); !ok || timeout <= 0 || timeout > 2 {
		errors = append(errors, "cleanup timeout must be bounded to at most two minutes")
	}
	for _, name := range []string{
		"Capture Memcached failure diagnostics",
		"Retain Memcached failure diagnostics",
	} {
		step := stepByName(job, name)
		if step == nil || !conditionCoversFailureAndCancellation(step["if"]) {
			errors = append(errors, fmt.Sprintf("%s must cover failure() || cancelled()", name))
		}
	}
	return errors
}

func validateRequiredCIJobs(workflow map[string]any) []string {
	var errors []string
	jobs := mustMap(workflow["jobs"])
	required := []struct {
		job     string
		step    string
		command string
	}{
		{job: "quality-scripts", step: "Test integration script contracts", command: "go test -count=1 ./scripts"},
		{job: "terraform-validate", step: "Validate Memcached integration script", command: "bash -n scripts/memcache-integration.sh"},
		{job: "terraform-validate", step: "Validate Memcached integration script", command: "shellcheck scripts/memcache-integration.sh"},
	}
	checkedJobs := map[string]bool{}
	for _, contract := range required {
		job := mustMap(jobs[contract.job])
		if job == nil {
			errors = append(errors, fmt.Sprintf("required job %q is absent", contract.job))
			continue
		}
		if !checkedJobs[contract.job] {
			if _, found := job["if"]; found {
				errors = append(errors, fmt.Sprintf("required job %q must not have an if condition", contract.job))
			}
			if _, found := job["continue-on-error"]; found {
				errors = append(errors, fmt.Sprintf("required job %q contains continue-on-error", contract.job))
			}
			for _, step := range stepMaps(job) {
				if _, found := step["if"]; found {
					errors = append(errors, fmt.Sprintf("required job %q step %q must not have an if condition",
						contract.job, scalarString(step["name"])))
				}
				if _, found := step["continue-on-error"]; found {
					errors = append(errors, fmt.Sprintf("required job %q step %q contains continue-on-error",
						contract.job, scalarString(step["name"])))
				}
			}
			checkedJobs[contract.job] = true
		}
		errors = append(errors, validateRequiredShellStep(job, contract.step, contract.command)...)
	}
	return errors
}

var (
	exitZeroPattern      = regexp.MustCompile(`(^|[;&|[:space:]])exit[[:space:]]+0($|[;&|[:space:]])`)
	statusResetPattern   = regexp.MustCompile(`(?i)(^|[;[:space:]])(status|result|exit_status|exit_code|rc|code|check_status|result_status)[A-Za-z0-9_]*[[:space:]]*=`)
	controlWrapperPrefix = regexp.MustCompile(`^(if|then|else|elif|fi|for|while|until|case|esac|function)([[:space:]]|$)`)
)

func validateRequiredShellStep(job map[string]any, stepName, command string) []string {
	step := stepByName(job, stepName)
	if step == nil {
		return []string{fmt.Sprintf("required step %q is absent", stepName)}
	}
	lines := activeCommandLines(scalarString(step["run"]))
	if !contains(lines, command) {
		return []string{fmt.Sprintf("required step %q lacks active exact command %q", stepName, command)}
	}
	var errors []string
	run := scalarString(step["run"])
	if strings.Contains(run, "$(") || strings.Contains(run, "`") {
		errors = append(errors, fmt.Sprintf("required step %q wraps commands in command substitution", stepName))
	}
	for _, line := range lines {
		compact := strings.Join(strings.Fields(line), " ")
		unquoted := strings.NewReplacer(`"`, "", `'`, "").Replace(compact)
		switch {
		case strings.Contains(compact, "set +e"):
			errors = append(errors, fmt.Sprintf("required step %q disables errexit", stepName))
		case strings.Contains(compact, "set +o pipefail"):
			errors = append(errors, fmt.Sprintf("required step %q disables pipefail", stepName))
		case strings.Contains(compact, "|| true"), strings.Contains(compact, "|| :"),
			strings.Contains(compact, "|| /bin/true"):
			errors = append(errors, fmt.Sprintf("required step %q masks command failure", stepName))
		case exitZeroPattern.MatchString(unquoted), strings.Contains(unquoted, "exit $((0))"):
			errors = append(errors, fmt.Sprintf("required step %q forces a successful exit", stepName))
		case statusResetPattern.MatchString(compact):
			errors = append(errors, fmt.Sprintf("required step %q overwrites result status", stepName))
		case compact == "true" || compact == ":":
			errors = append(errors, fmt.Sprintf("required step %q appends a success sentinel", stepName))
		case compact == "(" || compact == ")" || compact == "{" || compact == "}" ||
			strings.Contains(compact, "() {") || strings.HasSuffix(compact, "{"):
			errors = append(errors, fmt.Sprintf("required step %q wraps commands in a subshell or block", stepName))
		case strings.HasSuffix(compact, " &"):
			errors = append(errors, fmt.Sprintf("required step %q backgrounds commands without a wait", stepName))
		case controlWrapperPrefix.MatchString(compact):
			errors = append(errors, fmt.Sprintf("required step %q wraps commands in shell control flow", stepName))
		}
	}
	return errors
}

func validateLifecyclePipefail(job map[string]any) []string {
	step := stepByName(job, "Run hardened Memcached integration lifecycle")
	if step == nil {
		return []string{"Memcached lifecycle step is absent"}
	}
	lines := activeCommandLines(scalarString(step["run"]))
	pipefail := -1
	lifecycle := -1
	for index, line := range lines {
		if line == "set -o pipefail" || strings.Contains(line, "set -Eeuo pipefail") {
			pipefail = index
		}
		if line == exactMakeCommand {
			lifecycle = index
			break
		}
	}
	if lifecycle < 0 {
		return []string{"active exact Memcached lifecycle command is absent"}
	}
	if pipefail < 0 || pipefail > lifecycle {
		return []string{"Memcached lifecycle pipeline executes before pipefail is enabled"}
	}
	return nil
}

func readWorkflow(t *testing.T, name string) map[string]any {
	t.Helper()
	path := filepath.Join("..", ".github", "workflows", name)
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var workflow map[string]any
	if err := yaml.Unmarshal(source, &workflow); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return workflow
}

func workflowJob(t *testing.T, workflow map[string]any, name string) map[string]any {
	t.Helper()
	job := mustMap(mustMap(workflow["jobs"])[name])
	if job == nil {
		t.Fatalf("workflow job %q is absent", name)
	}
	return job
}

func workflowJobMap(workflow map[string]any) map[string]any {
	return mustMap(mustMap(workflow["jobs"])["memcache-integration"])
}

func stepMaps(job map[string]any) []map[string]any {
	raw, _ := job["steps"].([]any)
	steps := make([]map[string]any, 0, len(raw))
	for _, value := range raw {
		steps = append(steps, mustMap(value))
	}
	return steps
}

func stepByName(job map[string]any, name string) map[string]any {
	for _, step := range stepMaps(job) {
		if scalarString(step["name"]) == name {
			return step
		}
	}
	return nil
}

func firstStepUsing(job map[string]any, action string) map[string]any {
	for _, step := range stepMaps(job) {
		if scalarString(step["uses"]) == action {
			return step
		}
	}
	return nil
}

func activeCommandLines(run string) []string {
	var lines []string
	for _, raw := range strings.Split(run, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func jobHasActiveCommand(job map[string]any, command string) bool {
	for _, step := range stepMaps(job) {
		if stepDisabled(step) {
			continue
		}
		for _, line := range activeCommandLines(scalarString(step["run"])) {
			if line == command {
				return true
			}
		}
	}
	return false
}

func stepDisabled(step map[string]any) bool {
	condition, found := step["if"]
	return found && conditionIsFalse(condition)
}

func conditionIsFalse(value any) bool {
	if boolean, ok := value.(bool); ok {
		return !boolean
	}
	normalized := normalizeCondition(scalarString(value))
	return normalized == "false"
}

func conditionIsAlways(value any) bool {
	return normalizeCondition(scalarString(value)) == "always()"
}

func conditionCoversFailureAndCancellation(value any) bool {
	normalized := normalizeCondition(scalarString(value))
	return normalized == "failure()||cancelled()"
}

func normalizeCondition(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "${{")
	value = strings.TrimSuffix(value, "}}")
	return strings.ReplaceAll(strings.TrimSpace(value), " ", "")
}

func mustMap(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func stringSlice(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			values = append(values, scalarString(item))
		}
		return values
	default:
		return nil
	}
}

func scalarString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	default:
		return ""
	}
}

func integer(value any) (int, bool) {
	result, ok := value.(int)
	return result, ok
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
