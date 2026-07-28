package evidence

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"minisky/pkg/registry"
	_ "minisky/pkg/shims"
)

func TestPhase18To25InventoryMatchesRegistryTruth(t *testing.T) {
	inventory, err := Phase18To25()
	if err != nil {
		t.Fatal(err)
	}
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}

	experimental := make(map[string]registry.Service)
	for _, service := range services {
		if service.Support == registry.SupportExperimental {
			experimental[service.Domain] = service
		}
	}
	if len(inventory) != len(experimental) {
		t.Fatalf("inventory entries = %d, experimental domains = %d", len(inventory), len(experimental))
	}

	seen := make(map[string]bool, len(inventory))
	terraformClaims := 0
	for _, entry := range inventory {
		if seen[entry.Domain] {
			t.Errorf("duplicate evidence entry for %s", entry.Domain)
		}
		seen[entry.Domain] = true
		service, ok := experimental[entry.Domain]
		if !ok {
			t.Errorf("%s is not a registry experimental domain", entry.Domain)
			continue
		}
		if entry.Selector != entry.Domain {
			t.Errorf("%s selector = %q, want unambiguous canonical domain", entry.Domain, entry.Selector)
		}
		if entry.Persistence != string(service.Persistence) {
			t.Errorf("%s persistence = %q, registry = %q", entry.Domain, entry.Persistence, service.Persistence)
		}
		if entry.Package == "" || len(entry.Tests) == 0 || entry.MethodNote == "" {
			t.Errorf("%s has incomplete evidence metadata: %+v", entry.Domain, entry)
		}
		if entry.IAMPath == "" {
			t.Errorf("%s has no offline strict-IAM gateway evidence route", entry.Domain)
		}
		if entry.TerraformClaim {
			terraformClaims++
			if entry.Domain != "workflows.googleapis.com" && entry.Domain != "eventarc.googleapis.com" &&
				entry.Domain != "composer.googleapis.com" && entry.Domain != "managedkafka.googleapis.com" &&
				entry.Domain != "file.googleapis.com" && entry.Domain != "identityplatform.googleapis.com" &&
				entry.Domain != "alloydb.googleapis.com" && entry.Domain != "servicedirectory.googleapis.com" &&
				entry.Domain != "documentai.googleapis.com" && entry.Domain != "orgpolicy.googleapis.com" &&
				entry.Domain != "binaryauthorization.googleapis.com" {
				if entry.Domain == "storagetransfer.googleapis.com" {
					continue
				}
				t.Errorf("%s has an unexpected Terraform compatibility claim", entry.Domain)
			}
		}
		if service.Persistence == registry.PersistenceFile ||
			service.Persistence == registry.PersistenceHybrid {
			hasRestartEvidence := false
			for _, test := range entry.Tests {
				if strings.Contains(test, "Persist") || strings.Contains(test, "Restart") ||
					strings.Contains(test, "Reload") || strings.Contains(test, "Survive") {
					hasRestartEvidence = true
				}
			}
			if !hasRestartEvidence || strings.Contains(entry.MethodNote, "no restart claim") {
				t.Errorf("%s claims durable persistence without named restart evidence", entry.Domain)
			}
		}
	}
	if terraformClaims != 12 {
		t.Errorf("Terraform claims = %d, want twelve passed bounded provider lifecycles", terraformClaims)
	}
}

func TestPhase18To25InventoryReferencesExecutablePackageTests(t *testing.T) {
	root := repositoryRoot(t)
	inventory, err := Phase18To25()
	if err != nil {
		t.Fatal(err)
	}
	cache := make(map[string]string)
	for _, entry := range inventory {
		assertTestReferences(t, root, cache, entry.Domain, entry.Package, entry.Tests)
	}
	gates, err := AggregateGates()
	if err != nil {
		t.Fatal(err)
	}
	for _, gate := range gates {
		if gate.Name == "" || gate.MethodNote == "" || len(gate.Tests) == 0 {
			t.Errorf("incomplete aggregate gate: %+v", gate)
		}
		assertTestReferences(t, root, cache, gate.Name, gate.Package, gate.Tests)
	}
}

func TestPhase12PlatformGatesAreCompleteAndReferenceable(t *testing.T) {
	root := repositoryRoot(t)
	gates, err := PlatformGates()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]EvidenceStatus{
		"phase12-package-race-unit":   EvidenceLocalPassed,
		"phase12-guarded-integration": EvidenceLocalPassed,
		"phase12-guard-self-test":     EvidenceLocalPassed,
		"phase12-ci":                  EvidenceCIPassed,
	}
	if len(gates) != len(want) {
		t.Fatalf("platform gates = %d, want %d", len(gates), len(want))
	}
	cache := make(map[string]string)
	for _, gate := range gates {
		status, ok := want[gate.ID]
		if !ok {
			t.Errorf("unexpected platform gate %q", gate.ID)
			continue
		}
		delete(want, gate.ID)
		if gate.Phase != 12 || gate.Name == "" {
			t.Errorf("%s has incomplete platform identity: %+v", gate.ID, gate)
		}
		if gate.Check.Status != status {
			t.Errorf("%s status = %q, want %q", gate.ID, gate.Check.Status, status)
		}
		if err := validateEvidenceCheck(root, gate.ID, gate.Check, cache); err != nil {
			t.Errorf("%s: %v", gate.ID, err)
		}
	}
	for id := range want {
		t.Errorf("missing platform gate %q", id)
	}
}

func TestEvidenceCheckRejectsFabricatedOrMisplacedCIEvidence(t *testing.T) {
	validRun := "https://github.com/rudydesplan/minisky/actions/runs/123456"
	validCommit := strings.Repeat("a", 40)
	tests := []struct {
		name  string
		run   string
		sha   string
		valid bool
	}{
		{
			name:  "repository run and full commit",
			run:   validRun,
			sha:   validCommit,
			valid: true,
		},
		{
			name: "missing run",
			sha:  validCommit,
		},
		{
			name: "wrong owner",
			run:  "https://github.com/other/minisky/actions/runs/123456",
			sha:  validCommit,
		},
		{
			name: "wrong repository",
			run:  "https://github.com/rudydesplan/other/actions/runs/123456",
			sha:  validCommit,
		},
		{
			name: "wrong host",
			run:  "https://example.com/rudydesplan/minisky/actions/runs/123456",
			sha:  validCommit,
		},
		{
			name: "host suffix attack",
			run:  "https://github.com.evil.test/rudydesplan/minisky/actions/runs/123456",
			sha:  validCommit,
		},
		{
			name: "wrong scheme",
			run:  "http://github.com/rudydesplan/minisky/actions/runs/123456",
			sha:  validCommit,
		},
		{
			name: "malformed run path",
			run:  "https://github.com/rudydesplan/minisky/actions/runs/latest",
			sha:  validCommit,
		},
		{
			name: "extra path",
			run:  "https://github.com/rudydesplan/minisky/actions/runs/123456/jobs/7",
			sha:  validCommit,
		},
		{
			name: "query",
			run:  validRun + "?attempt=1",
			sha:  validCommit,
		},
		{
			name: "fragment",
			run:  validRun + "#summary",
			sha:  validCommit,
		},
		{
			name: "short commit",
			run:  validRun,
			sha:  "deadbeef",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateEvidenceCheck(EvidenceCheck{
				Status: EvidenceCIPassed,
				RunURL: test.run,
				Commit: test.sha,
				Note:   "recorded",
			})
			if test.valid && err != nil {
				t.Fatalf("valid evidence rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}

	for _, status := range []EvidenceStatus{EvidenceLocalPassed, EvidenceConfiguredUnverified} {
		t.Run(string(status)+" carrying CI fields", func(t *testing.T) {
			err := ValidateEvidenceCheck(EvidenceCheck{
				Status: status,
				RunURL: validRun,
				Commit: validCommit,
				Note:   "conflated",
			})
			if err == nil {
				t.Fatal("non-CI status accepted immutable CI fields")
			}
		})
	}
}

func TestBatchGateEvidenceIsCompleteAndReferenceable(t *testing.T) {
	root := repositoryRoot(t)
	inventory, err := Phase18To25()
	if err != nil {
		t.Fatal(err)
	}
	gates, err := BatchGates()
	if err != nil {
		t.Fatal(err)
	}
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	registered := make(map[string]bool, len(services))
	for _, service := range services {
		registered[service.Domain] = true
	}

	wantIDs := []string{"phase18-25", "phase19", "phase20", "phase21-22", "phase23", "phase24-25"}
	if len(gates) != len(wantIDs) {
		t.Fatalf("batch gates = %d, want %d", len(gates), len(wantIDs))
	}
	byID := make(map[string]BatchGate, len(gates))
	domainGate := make(map[string]string)
	terraformCheckByDomain := make(map[string]string)
	cache := make(map[string]string)
	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for _, gate := range gates {
		if _, duplicate := byID[gate.ID]; duplicate {
			t.Errorf("duplicate batch gate %q", gate.ID)
		}
		byID[gate.ID] = gate
		for _, domain := range gate.Domains {
			if prior := domainGate[domain]; prior != "" {
				t.Errorf("%s belongs to both %s and %s", domain, prior, gate.ID)
			}
			domainGate[domain] = gate.ID
		}
		for _, domain := range gate.RelatedDomains {
			if !registered[domain] {
				t.Errorf("%s references unregistered related domain %s", gate.ID, domain)
			}
		}
		for _, domain := range gate.UnsupportedGeneratedDomains {
			if domainGate[domain] != gate.ID {
				t.Errorf("%s unsupported generated-client domain %s is not owned by the batch",
					gate.ID, domain)
			}
		}
		checks := map[string]EvidenceCheck{
			"packageUnit":              gate.PackageUnit,
			"generatedClientLifecycle": gate.GeneratedClientLifecycle,
			"daemonRestart":            gate.DaemonRestart,
			"realBackendDocker":        gate.RealBackendDocker,
			"strictIAM":                gate.StrictIAM,
			"cleanup":                  gate.Cleanup,
			"ci":                       gate.CI,
		}
		if gate.BackendCI.Status != "" {
			checks["backendCI"] = gate.BackendCI
		}
		for name, check := range checks {
			if err := validateEvidenceCheck(root, gate.ID+" "+name, check, cache); err != nil {
				t.Error(err)
			}
		}
		generatedStatus := EvidenceConfiguredUnverified
		restartStatus := EvidenceConfiguredUnverified
		backendStatus := gate.RealBackendDocker.Status
		cleanupStatus := EvidenceConfiguredUnverified
		if gate.ID == "phase18-25" || gate.ID == "phase19" ||
			gate.ID == "phase20" || gate.ID == "phase21-22" ||
			gate.ID == "phase23" || gate.ID == "phase24-25" {
			generatedStatus = EvidenceLocalPassed
			restartStatus = EvidenceLocalPassed
			cleanupStatus = EvidenceLocalPassed
		}
		if gate.ID == "phase18-25" || gate.ID == "phase19" ||
			gate.ID == "phase20" || gate.ID == "phase24-25" {
			backendStatus = EvidenceLocalPassed
		}
		if gate.PackageUnit.Status != EvidenceLocalPassed ||
			gate.GeneratedClientLifecycle.Status != generatedStatus ||
			gate.DaemonRestart.Status != restartStatus ||
			gate.RealBackendDocker.Status != backendStatus ||
			gate.StrictIAM.Status != EvidenceLocalPassed ||
			gate.Cleanup.Status != cleanupStatus ||
			gate.CI.Status != EvidenceCIPassed {
			t.Errorf("%s overstates or conflates batch evidence: %+v", gate.ID, gate)
		}
		if gate.CI.RunURL != "https://github.com/rudydesplan/minisky/actions/runs/30285572232" ||
			gate.CI.Commit != "62d6fa245774f3ff3bdd9b82e19d1c617650d448" {
			t.Errorf("%s CI evidence does not identify the passing run and commit: %+v", gate.ID, gate.CI)
		}
		for _, check := range gate.TerraformChecks {
			if prior := terraformCheckByDomain[check.Domain]; prior != "" {
				t.Errorf("%s has Terraform checks in both %s and %s", check.Domain, prior, gate.ID)
			}
			terraformCheckByDomain[check.Domain] = gate.ID
			crossPhaseBinaryAuthorization := gate.ID == "phase24-25" &&
				check.Domain == "binaryauthorization.googleapis.com"
			if domainGate[check.Domain] != gate.ID && !crossPhaseBinaryAuthorization {
				t.Errorf("%s Terraform check is not owned by batch %s", check.Domain, gate.ID)
			}
			if check.Status != EvidenceLocalPassed || check.Note == "" ||
				len(check.References) == 0 || check.Script == "" || check.MakeTarget == "" {
				t.Errorf("%s has incomplete per-domain Terraform evidence: %+v", check.Domain, check)
			}
			for _, reference := range check.References {
				assertTestReferences(t, root, cache, check.Domain+" terraform", reference.Package, reference.Tests)
			}
			if _, err := os.Stat(filepath.Join(root, check.Script)); err != nil {
				t.Errorf("%s Terraform script %q: %v", check.Domain, check.Script, err)
			}
			if !strings.Contains(string(makefile), "\n"+check.MakeTarget+":") {
				t.Errorf("%s references missing Terraform Make target %q", check.Domain, check.MakeTarget)
			}
		}
		generatedSource := ""
		for _, reference := range gate.GeneratedClientLifecycle.References {
			if strings.HasPrefix(reference.Package, "sdk-smoke/") {
				generatedSource += cache[reference.Package]
			}
		}
		for _, domain := range append(append([]string(nil), gate.Domains...), gate.RelatedDomains...) {
			if !strings.Contains(generatedSource, domain) {
				t.Errorf("%s generated-client tests do not name batch domain %s", gate.ID, domain)
			}
		}
	}
	for _, id := range wantIDs {
		if _, ok := byID[id]; !ok {
			t.Errorf("missing batch gate %q", id)
		}
	}
	for _, entry := range inventory {
		if entry.BatchGate == "" || domainGate[entry.Domain] != entry.BatchGate {
			t.Errorf("%s batch gate = %q, inventory mapping = %q",
				entry.Domain, entry.BatchGate, domainGate[entry.Domain])
		}
		switch entry.GeneratedClientBoundary {
		case GeneratedClientCovered, GeneratedClientExplicitUnsupported:
		default:
			t.Errorf("%s has unknown generated-client boundary %q",
				entry.Domain, entry.GeneratedClientBoundary)
		}
		checkGate, hasCheck := terraformCheckByDomain[entry.Domain]
		expectedCheckGate := entry.BatchGate
		if entry.Domain == "binaryauthorization.googleapis.com" {
			expectedCheckGate = "phase24-25"
		}
		if entry.TerraformClaim && (!hasCheck || checkGate != expectedCheckGate) {
			t.Errorf("%s claims Terraform without its own batch-scoped check", entry.Domain)
		}
		if !entry.TerraformClaim && hasCheck {
			t.Errorf("%s has Terraform check without a domain claim", entry.Domain)
		}
	}
	if len(terraformCheckByDomain) != 12 {
		t.Errorf("per-domain Terraform checks = %d, want 12", len(terraformCheckByDomain))
	}
}

func TestPhase18AggregateEvidenceNamesAreStableAndReferenceable(t *testing.T) {
	gates, err := AggregateGates()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]AggregateGate, len(gates))
	for _, gate := range gates {
		byName[gate.Name] = gate
	}

	required := map[string]struct {
		packagePath string
		tests       []string
	}{
		"phase18-generated-clients": {
			packagePath: "sdk-smoke/phase18-25",
			tests: []string{
				"TestGeneratedClientsUseFullDomainCanonicalPaths",
				"TestGeneratedBatchRunnableAndTerminalExitCode",
			},
		},
		"phase18-batch-restart": {
			packagePath: "pkg/shims/batch",
			tests:       []string{"TestRestartCleansOwnedContainerAndFailsInterruptedExecutableJob"},
		},
		"phase18-batch-execution": {
			packagePath: "pkg/shims/batch",
			tests: []string{
				"TestCreateExecutableJobRunsAndCapturesTerminalState",
				"TestDockerExecutableJobIntegration",
			},
		},
	}
	for name, want := range required {
		gate, ok := byName[name]
		if !ok {
			t.Errorf("missing required aggregate evidence gate %q", name)
			continue
		}
		if gate.Package != want.packagePath {
			t.Errorf("%s package = %q, want %q", name, gate.Package, want.packagePath)
		}
		if strings.Join(gate.Tests, "\x00") != strings.Join(want.tests, "\x00") {
			t.Errorf("%s tests = %v, want %v", name, gate.Tests, want.tests)
		}
	}
}

func TestPhase19IntegrationUsesCanonicalDockerOwnershipLabels(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repositoryRoot(t), "scripts", "phase19-sdk-integration.sh"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	for _, label := range []string{"minisky.profile", "minisky.resource"} {
		if !strings.Contains(source, `label=`+label+`=`) {
			t.Errorf("Phase 19 integration does not query canonical Docker label %q", label)
		}
	}
	if strings.Contains(source, "com.minisky.") {
		t.Fatal("Phase 19 integration queries obsolete com.minisky Docker labels")
	}
}

func TestPhase19HeavyBackendCIIsExplicitAndIsolated(t *testing.T) {
	root := repositoryRoot(t)
	gates, err := BatchGates()
	if err != nil {
		t.Fatal(err)
	}
	var phase19 BatchGate
	for _, gate := range gates {
		if gate.ID == "phase19" {
			phase19 = gate
			break
		}
	}
	if phase19.BackendCI.Status != EvidenceCIPassed ||
		phase19.BackendCI.Workflow != ".github/workflows/ci.yml" ||
		phase19.BackendCI.Job != "phase19-heavy-backend-integration" ||
		phase19.BackendCI.MakeTarget != "test-phase19-heavy-backend" {
		t.Fatalf("Phase 19 heavy backend CI evidence is not passed: %+v", phase19.BackendCI)
	}
	if phase19.BackendCI.RunURL != "https://github.com/rudydesplan/minisky/actions/runs/30287887431" ||
		phase19.BackendCI.Commit != "d657e4b0b77a34ddb615124db2d82da810238502" {
		t.Fatalf("Phase 19 heavy backend CI does not identify the passing run: %+v", phase19.BackendCI)
	}

	workflow, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(workflow)
	inputStart := strings.Index(source, "      run_phase19_heavy_backend_integration:")
	inputEnd := strings.Index(source[inputStart+1:], "\n      run_phase20_sdk_integration:")
	if inputStart < 0 || inputEnd < 0 {
		t.Fatal("Phase 19 heavy backend workflow_dispatch input is missing or misplaced")
	}
	input := source[inputStart : inputStart+1+inputEnd]
	for _, want := range []string{"type: boolean", "default: false"} {
		if !strings.Contains(input, want) {
			t.Errorf("Phase 19 heavy backend input is missing %q", want)
		}
	}

	jobStart := strings.Index(source, "\n  phase19-heavy-backend-integration:")
	jobEnd := strings.Index(source[jobStart+1:], "\n  phase20-sdk-integration:")
	if jobStart < 0 || jobEnd < 0 {
		t.Fatal("Phase 19 heavy backend job is missing or misplaced")
	}
	job := source[jobStart : jobStart+1+jobEnd]
	for _, want := range []string{
		"github.event_name == 'workflow_dispatch' && inputs.run_phase19_heavy_backend_integration",
		"permissions:\n      contents: read",
		"timeout-minutes: 30",
		"docker info >/dev/null",
		"MINISKY_PHASE19_PROFILE:",
		"make test-phase19-heavy-backend",
		"if: failure()",
		"phase19-heavy-backend-integration.log",
		"if: always()",
		`docker rm -f -v`,
	} {
		if !strings.Contains(job, want) {
			t.Errorf("Phase 19 heavy backend job is missing %q", want)
		}
	}

	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"\ntest-phase19-heavy-backend:",
		"MINISKY_PHASE19_SDK_INTEGRATION=1 MINISKY_PHASE19_DOCKER_INTEGRATION=1",
	} {
		if !strings.Contains(string(makefile), want) {
			t.Errorf("Phase 19 heavy backend Make contract is missing %q", want)
		}
	}

	script, err := os.ReadFile(filepath.Join(root, "scripts", "phase19-sdk-integration.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`profile="${MINISKY_PHASE19_PROFILE:-phase19-sdk-$$}"`,
		`diagnostics_dir="${MINISKY_PHASE19_DIAGNOSTICS_DIR:-}"`,
		`docker ps -aq --filter "label=minisky.profile=${profile}"`,
		`docker volume rm "${volume}"`,
		"assert_no_owned_resources",
	} {
		if !strings.Contains(string(script), want) {
			t.Errorf("Phase 19 heavy backend script is missing %q", want)
		}
	}
}

func TestPhase20IntegrationSeedsStorageBeforeTransferBoundary(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repositoryRoot(t), "scripts", "phase20-sdk-integration.sh"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(script)
	create := strings.Index(source, "MINISKY_PHASE20_MODE=create go run ./sdk-smoke/phase20")
	boundary := strings.Index(source, "MINISKY_PHASE20_MODE=boundary go run ./sdk-smoke/phase20")
	if create < 0 || boundary < 0 {
		t.Fatal("Phase 20 integration is missing create or boundary mode")
	}
	if create > boundary {
		t.Fatal("Phase 20 integration runs Storage Transfer boundary before seeding its source and sink buckets")
	}
}

func TestPhase18WorkflowsTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	providers := read("terraform/providers.tf")
	main := read("terraform/main.tf")
	variables := read("terraform/variables.tf")
	makefile := read("Makefile")
	script := read("scripts/phase18-workflows-terraform-integration.sh")

	for path, contract := range map[string]struct {
		source string
		wants  []string
	}{
		"terraform/providers.tf": {providers, []string{"workflows_custom_endpoint", "/_minisky/workflows/v1/"}},
		"terraform/main.tf":      {main, []string{"google_workflows_workflow", "enable_phase18_workflows_resource", "deletion_protection = false"}},
		"terraform/variables.tf": {variables, []string{"enable_phase18_workflows_resource"}},
		"Makefile":               {makefile, []string{"test-phase18-workflows-terraform", "MINISKY_PHASE18_WORKFLOWS_TERRAFORM_INTEGRATION=1"}},
		"scripts/phase18-workflows-terraform-integration.sh": {script, []string{
			"MINISKY_PHASE18_WORKFLOWS_TERRAFORM_INTEGRATION",
			"MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1",
			"unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT",
			"terraform.tfstate",
			"plan -detailed-exitcode",
			"does not support import",
			"destroy",
			"Expected destroyed workflow",
		}},
	} {
		for _, want := range contract.wants {
			if !strings.Contains(contract.source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase18EventarcTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	providers := read("terraform/providers.tf")
	main := read("terraform/main.tf")
	variables := read("terraform/variables.tf")
	makefile := read("Makefile")
	script := read("scripts/phase18-eventarc-terraform-integration.sh")

	for path, contract := range map[string]struct {
		source string
		wants  []string
	}{
		"terraform/providers.tf": {providers, []string{"eventarc_custom_endpoint", "/_minisky/eventarc/v1/"}},
		"terraform/main.tf": {main, []string{
			"google_eventarc_trigger", "enable_phase18_eventarc_resource",
			"matching_criteria", "destination", "workflow", "transport", "pubsub",
		}},
		"terraform/variables.tf": {variables, []string{
			"enable_phase18_eventarc_resource", "phase18_eventarc_trigger_name",
			"phase18_eventarc_transport_topic",
		}},
		"Makefile": {makefile, []string{
			"test-phase18-eventarc-terraform",
			"MINISKY_PHASE18_EVENTARC_TERRAFORM_INTEGRATION=1",
		}},
		"scripts/phase18-eventarc-terraform-integration.sh": {script, []string{
			"MINISKY_PHASE18_EVENTARC_TERRAFORM_INTEGRATION",
			"MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1",
			"unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT",
			"terraform.tfstate",
			"plan -detailed-exitcode",
			"state rm",
			"terraform -chdir=\"${terraform_dir}\" import",
			"destroy",
			"Eventarc trigger ${trigger_canonical}",
			"does not exercise event delivery",
		}},
	} {
		for _, want := range contract.wants {
			if !strings.Contains(contract.source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase18EventDeliveryGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	makefile := read("Makefile")
	script := read("scripts/phase18-event-delivery-integration.sh")
	batchEvidence := read("pkg/evidence/batch_gates.json")

	for path, contract := range map[string]struct {
		source string
		wants  []string
	}{
		"Makefile": {makefile, []string{
			"test-phase18-event-delivery",
			"MINISKY_PHASE18_EVENT_DELIVERY_INTEGRATION=1",
		}},
		"scripts/phase18-event-delivery-integration.sh": {script, []string{
			"MINISKY_PHASE18_EVENT_DELIVERY_INTEGRATION",
			"MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1",
			"/_minisky/pubsub.googleapis.com/v1/",
			"/_minisky/eventarc.googleapis.com/v1/",
			"/_minisky/workflows.googleapis.com/v1/",
			"/_minisky/workflowexecutions.googleapis.com/v1/",
			"MINISKY_STATE_DIR",
			"MINISKY_PROFILE",
			"127.0.0.1",
			"assert_no_owned_resources",
			"terminal Workflow result",
			"trigger-delete-operation",
			"assert_no_executions_for",
			"foreign_topic_nonce",
			"foreign_project_nonce",
			"assert_no_executions_for_nonces",
			"deterministic Eventarc intent replay remains unproven",
		}},
		"pkg/evidence/batch_gates.json": {batchEvidence, []string{
			`"script":"scripts/phase18-event-delivery-integration.sh"`,
			`"makeTarget":"test-phase18-event-delivery"`,
			"public-gateway Pub/Sub",
			"foreign-topic and foreign-project",
			"terminal execution persistence",
			"deterministic Eventarc intent replay",
		}},
	} {
		for _, want := range contract.wants {
			if !strings.Contains(contract.source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase19ComposerTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"composer_custom_endpoint", "/_minisky/composer/v1/"},
		"terraform/main.tf":      {"google_composer_environment", "enable_phase19_composer_resource"},
		"terraform/variables.tf": {"enable_phase19_composer_resource", "phase19_composer_environment_name"},
		"Makefile":               {"test-phase19-composer-terraform", "MINISKY_PHASE19_COMPOSER_TERRAFORM_INTEGRATION=1"},
		"scripts/phase19-composer-terraform-integration.sh": {
			"MINISKY_PHASE19_COMPOSER_TERRAFORM_INTEGRATION",
			"MINISKY_PHASE19_DOCKER_INTEGRATION",
			"MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1",
			"plan -detailed-exitcode", "state rm", " import ", "destroy",
			"airflow dags trigger",
			"apache/airflow:2.10.5-python3.12@sha256:6499a680a93463846d3a6be980e85d601dc97b0d81e82eed9ef5e5cb9da31b79",
			"does not claim Cloud Composer parity",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase19ManagedKafkaTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"managed_kafka_custom_endpoint", "/_minisky/managedkafka/v1/"},
		"terraform/main.tf":      {"google_managed_kafka_cluster", "enable_phase19_managed_kafka_resource"},
		"terraform/variables.tf": {"enable_phase19_managed_kafka_resource", "phase19_managed_kafka_cluster_id"},
		"Makefile":               {"test-phase19-managed-kafka-terraform", "MINISKY_PHASE19_MANAGED_KAFKA_TERRAFORM_INTEGRATION=1"},
		"scripts/phase19-managed-kafka-terraform-integration.sh": {
			"MINISKY_PHASE19_MANAGED_KAFKA_TERRAFORM_INTEGRATION",
			"MINISKY_PHASE19_DOCKER_INTEGRATION",
			"MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1",
			"plan -detailed-exitcode", "state rm", " import ", "destroy",
			"kafka-console-producer.sh", "kafka-console-consumer.sh",
			"apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase20FilestoreTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"filestore_custom_endpoint", "/_minisky/file/v1/"},
		"terraform/main.tf":      {"google_filestore_instance", "enable_phase20_filestore_resource"},
		"terraform/variables.tf": {"enable_phase20_filestore_resource", "phase20_filestore_instance_name"},
		"Makefile":               {"test-phase20-filestore-terraform", "MINISKY_PHASE20_FILESTORE_TERRAFORM_INTEGRATION=1"},
		"scripts/phase20-filestore-terraform-integration.sh": {
			"MINISKY_PHASE20_FILESTORE_TERRAFORM_INTEGRATION",
			"MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1",
			"plan -detailed-exitcode", "state rm", " import ", "destroy",
			"filestore-data", "minisky-metadata-only", "durable 404",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase20IdentityPlatformTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"identity_platform_custom_endpoint", "/_minisky/identityplatform/v2/"},
		"terraform/main.tf":      {"google_identity_platform_config", "enable_phase20_identity_platform_config"},
		"terraform/variables.tf": {"enable_phase20_identity_platform_config", "phase20_identity_platform_authorized_domains"},
		"Makefile":               {"test-phase20-identity-platform-terraform", "MINISKY_PHASE20_IDENTITY_PLATFORM_TERRAFORM_INTEGRATION=1"},
		"scripts/phase20-identity-platform-terraform-integration.sh": {
			"MINISKY_PHASE20_IDENTITY_PLATFORM_TERRAFORM_INTEGRATION",
			"plan -detailed-exitcode", "state rm", " import ", "destroy",
			"reset", "authorizedDomains", "MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase20StorageTransferTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"storage_transfer_custom_endpoint", "/_minisky/storagetransfer/v1/"},
		"terraform/main.tf":      {"google_storage_transfer_job", "enable_phase20_storage_transfer_job"},
		"terraform/variables.tf": {"enable_phase20_storage_transfer_job", "phase20_storage_transfer_source_bucket"},
		"Makefile":               {"test-phase20-storage-transfer-terraform", "MINISKY_PHASE20_STORAGE_TRANSFER_TERRAFORM_INTEGRATION=1"},
		"scripts/phase20-storage-transfer-terraform-integration.sh": {
			"MINISKY_PHASE20_STORAGE_TRANSFER_TERRAFORM_INTEGRATION",
			"transferJobs", ":run", "plan -detailed-exitcode", "state rm", " import ", "destroy",
			"minisky-net-integration.lock",
			"Another MiniSky Docker integration is active",
			"baseline-containers",
			"baseline-volumes",
			"baseline-networks",
			"baseline_ready",
			"trap cleanup EXIT INT TERM",
			"Failed to capture baseline Docker inventory",
			`--filter "label=managed-by=minisky"`,
			`--filter "label=minisky.profile=${profile}"`,
			"preflight_owned_resources",
			"Refusing live Phase 20 Storage Transfer run",
			"docker rm -f",
			"docker volume rm",
			"docker network rm",
			"Storage Transfer cleanup incomplete",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase20StorageTransferCollisionRefusalPreservesPreexistingResources(t *testing.T) {
	root := repositoryRoot(t)
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tmp, "docker.log")
	writeFakeStorageTransferCommands(t, bin)
	command := exec.Command("bash", filepath.Join(root, "scripts", "phase20-storage-transfer-terraform-integration.sh"))
	command.Dir = root
	command.Env = append(os.Environ(),
		"MINISKY_PHASE20_STORAGE_TRANSFER_TERRAFORM_INTEGRATION=1",
		"FAKE_DOCKER_COLLISION=1",
		"FAKE_DOCKER_LOG="+logPath,
		"TMPDIR="+tmp,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	outputPath := filepath.Join(tmp, "command.log")
	outputFile, createErr := os.Create(outputPath)
	if createErr != nil {
		t.Fatal(createErr)
	}
	command.Stdout = outputFile
	command.Stderr = outputFile
	err := command.Run()
	if closeErr := outputFile.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	output, readOutputErr := os.ReadFile(outputPath)
	if readOutputErr != nil {
		t.Fatal(readOutputErr)
	}
	if err == nil {
		t.Fatal("collision run unexpectedly succeeded")
	}
	if !strings.Contains(string(output), "Refusing live Phase 20 Storage Transfer run") {
		t.Fatalf("collision refusal missing from output: %s", output)
	}
	log, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, destructive := range []string{"rm -f", "volume rm", "network rm"} {
		if strings.Contains(string(log), destructive) {
			t.Fatalf("collision cleanup mutated pre-existing resource with %q:\n%s", destructive, log)
		}
	}
}

func TestPhase20StorageTransferHonorsSharedNetworkLock(t *testing.T) {
	root := repositoryRoot(t)
	tmp := t.TempDir()
	if err := os.Mkdir(filepath.Join(tmp, "minisky-net-integration.lock"), 0o700); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(tmp, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tmp, "docker.log")
	writeFakeStorageTransferCommands(t, bin)
	command := exec.Command("bash", filepath.Join(root, "scripts", "phase20-storage-transfer-terraform-integration.sh"))
	command.Dir = root
	command.Env = append(os.Environ(),
		"MINISKY_PHASE20_STORAGE_TRANSFER_TERRAFORM_INTEGRATION=1",
		"FAKE_DOCKER_LOG="+logPath,
		"TMPDIR="+tmp,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	outputPath := filepath.Join(tmp, "command.log")
	outputFile, createErr := os.Create(outputPath)
	if createErr != nil {
		t.Fatal(createErr)
	}
	command.Stdout = outputFile
	command.Stderr = outputFile
	err := command.Run()
	if closeErr := outputFile.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	output, readOutputErr := os.ReadFile(outputPath)
	if readOutputErr != nil {
		t.Fatal(readOutputErr)
	}
	if err == nil {
		t.Fatal("shared-lock collision unexpectedly succeeded")
	}
	if !strings.Contains(string(output), "Another MiniSky Docker integration is active") {
		t.Fatalf("shared-lock refusal missing from output: %s", output)
	}
	if log, readErr := os.ReadFile(logPath); readErr == nil && len(log) != 0 {
		t.Fatalf("Docker was inspected before shared-lock refusal:\n%s", log)
	}
}

func TestPhase20StorageTransferEarlySetupFailuresReleaseLocks(t *testing.T) {
	for _, test := range []struct {
		name    string
		command string
		source  string
	}{
		{
			name:    "repository root resolution",
			command: "dirname",
			source:  "#!/usr/bin/env bash\nprintf '%s\\n' \"${TMPDIR}/missing/child\"\n",
		},
		{
			name:    "temporary workdir",
			command: "mktemp",
			source:  "#!/usr/bin/env bash\nexit 91\n",
		},
		{
			name:    "workdir children",
			command: "mkdir",
			source: `#!/usr/bin/env bash
set -eu
count=0
[[ -f "${FAKE_MKDIR_COUNT}" ]] && count="$(<"${FAKE_MKDIR_COUNT}")"
count=$((count + 1))
printf '%s' "${count}" >"${FAKE_MKDIR_COUNT}"
if [[ "${count}" -ge 3 ]]; then exit 92; fi
exec /bin/mkdir "$@"
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := repositoryRoot(t)
			tmp := t.TempDir()
			bin := filepath.Join(tmp, "bin")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			writeFakeStorageTransferCommands(t, bin)
			if err := os.WriteFile(filepath.Join(bin, test.command), []byte(test.source), 0o700); err != nil {
				t.Fatal(err)
			}
			result := runStorageTransferScriptTest(t, root, tmp, bin, nil)
			if result.err == nil {
				t.Fatal("injected early setup failure unexpectedly succeeded")
			}
			assertStorageTransferLocksReleased(t, tmp)
			assertNoStorageTransferDockerDeletion(t, result.dockerLog)
		})
	}
}

func TestPhase20StorageTransferCleanupFailureStillReleasesLocks(t *testing.T) {
	root := repositoryRoot(t)
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFakeStorageTransferCommands(t, bin)
	if err := os.WriteFile(filepath.Join(bin, "rm"), []byte("#!/usr/bin/env bash\nexit 93\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	result := runStorageTransferScriptTest(t, root, tmp, bin, []string{"FAKE_DOCKER_COLLISION=1"})
	if result.err == nil {
		t.Fatal("collision with injected cleanup failure unexpectedly succeeded")
	}
	assertStorageTransferLocksReleased(t, tmp)
	assertNoStorageTransferDockerDeletion(t, result.dockerLog)
}

func TestPhase20StorageTransferInventoryFailureFailsClosed(t *testing.T) {
	root := repositoryRoot(t)
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFakeStorageTransferCommands(t, bin)
	result := runStorageTransferScriptTest(t, root, tmp, bin, []string{
		"FAKE_DOCKER_INVENTORY_FAIL_ONCE=1",
		"FAKE_DOCKER_STATE=" + filepath.Join(tmp, "docker.state"),
	})
	if result.err == nil {
		t.Fatal("Docker inventory failure unexpectedly succeeded")
	}
	if !strings.Contains(result.output, "Failed to capture baseline Docker inventory") {
		t.Fatalf("inventory failure did not fail closed: %s", result.output)
	}
	followup := exec.Command(filepath.Join(bin, "docker"), "ps", "-aq")
	followup.Env = append(os.Environ(),
		"FAKE_DOCKER_INVENTORY_FAIL_ONCE=1",
		"FAKE_DOCKER_LOG="+filepath.Join(tmp, "docker.log"),
		"FAKE_DOCKER_STATE="+filepath.Join(tmp, "docker.state"),
	)
	output, err := followup.Output()
	if err != nil {
		t.Fatalf("follow-up Docker inventory did not recover: %v", err)
	}
	if !strings.Contains(string(output), "preexisting-container") {
		t.Fatalf("follow-up inventory did not expose pre-existing resource: %s", output)
	}
	assertStorageTransferLocksReleased(t, tmp)
	log, err := os.ReadFile(filepath.Join(tmp, "docker.log"))
	if err != nil {
		t.Fatal(err)
	}
	assertNoStorageTransferDockerDeletion(t, string(log))
}

type storageTransferScriptResult struct {
	output    string
	dockerLog string
	err       error
}

func runStorageTransferScriptTest(
	t *testing.T,
	root, tmp, bin string,
	extraEnv []string,
) storageTransferScriptResult {
	t.Helper()
	logPath := filepath.Join(tmp, "docker.log")
	command := exec.Command("bash", filepath.Join(root, "scripts", "phase20-storage-transfer-terraform-integration.sh"))
	command.Dir = root
	command.Env = append(os.Environ(),
		"MINISKY_PHASE20_STORAGE_TRANSFER_TERRAFORM_INTEGRATION=1",
		"FAKE_DOCKER_LOG="+logPath,
		"FAKE_MKDIR_COUNT="+filepath.Join(tmp, "mkdir.count"),
		"TMPDIR="+tmp,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	command.Env = append(command.Env, extraEnv...)
	outputPath := filepath.Join(tmp, "command.log")
	outputFile, err := os.Create(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout = outputFile
	command.Stderr = outputFile
	runErr := command.Run()
	if err := outputFile.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	dockerLog, err := os.ReadFile(logPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return storageTransferScriptResult{output: string(output), dockerLog: string(dockerLog), err: runErr}
}

func assertStorageTransferLocksReleased(t *testing.T, tmp string) {
	t.Helper()
	for _, lock := range []string{
		"minisky-net-integration.lock",
		"minisky-phase20-storage-transfer-terraform.lock",
	} {
		if _, err := os.Stat(filepath.Join(tmp, lock)); !os.IsNotExist(err) {
			t.Errorf("lock %s was not released: %v", lock, err)
		}
	}
}

func assertNoStorageTransferDockerDeletion(t *testing.T, log string) {
	t.Helper()
	for _, destructive := range []string{"rm -f", "volume rm", "network rm"} {
		if strings.Contains(log, destructive) {
			t.Errorf("cleanup mutated a pre-existing resource with %q:\n%s", destructive, log)
		}
	}
}

func writeFakeStorageTransferCommands(t *testing.T, bin string) {
	t.Helper()
	docker := `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${FAKE_DOCKER_LOG}"
if [[ "${FAKE_DOCKER_INVENTORY_FAIL_ONCE:-}" == "1" && "$1 $2" == "ps -aq" ]]; then
  count=0
  [[ -f "${FAKE_DOCKER_STATE}" ]] && count="$(<"${FAKE_DOCKER_STATE}")"
  count=$((count + 1))
  printf '%s' "${count}" >"${FAKE_DOCKER_STATE}"
  if [[ "${count}" == "1" ]]; then exit 94; fi
  echo preexisting-container
fi
if [[ "${FAKE_DOCKER_COLLISION:-}" == "1" ]]; then
  case "$1 $2" in
    "ps -aq") echo preexisting-container ;;
    "volume ls") echo preexisting-volume ;;
    "network ls") echo preexisting-network ;;
    "network inspect") exit 1 ;;
  esac
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(docker), 0o700); err != nil {
		t.Fatal(err)
	}
	goCommand := "#!/usr/bin/env bash\necho 'go build must not run during preflight tests' >&2\nexit 97\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(goCommand), 0o700); err != nil {
		t.Fatal(err)
	}
	terraformCommand := "#!/usr/bin/env bash\necho 'terraform must not run during preflight tests' >&2\nexit 98\n"
	if err := os.WriteFile(filepath.Join(bin, "terraform"), []byte(terraformCommand), 0o700); err != nil {
		t.Fatal(err)
	}
}

func TestPhase20AlloyDBTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"alloydb_custom_endpoint", "/_minisky/alloydb/v1/"},
		"terraform/main.tf":      {"google_alloydb_cluster", "google_alloydb_instance", "PRIMARY", "minisky-metadata-only"},
		"Makefile":               {"test-phase20-alloydb-terraform", "MINISKY_PHASE20_ALLOYDB_TERRAFORM_INTEGRATION=1"},
		"scripts/phase20-alloydb-terraform-integration.sh": {
			"MINISKY_PHASE20_ALLOYDB_DOCKER_INTEGRATION", "postgres:15.8-bookworm",
			"psql", "plan -detailed-exitcode", "state rm", " import ", "destroy",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase21ServiceDirectoryTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"service_directory_custom_endpoint", "/_minisky/servicedirectory/v1/"},
		"terraform/main.tf": {
			"google_service_directory_namespace", "google_service_directory_service",
			"google_service_directory_endpoint", "enable_phase21_service_directory_resources",
		},
		"Makefile": {"test-phase21-service-directory-terraform", "MINISKY_PHASE21_SERVICE_DIRECTORY_TERRAFORM_INTEGRATION=1"},
		"scripts/phase21-service-directory-terraform-integration.sh": {
			"MINISKY_PHASE21_SERVICE_DIRECTORY_TERRAFORM_INTEGRATION", "plan -detailed-exitcode",
			"state rm", " import ", "destroy",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase23DocumentAITerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"document_ai_custom_endpoint", "/_minisky/documentai/v1/"},
		"terraform/main.tf":      {"google_document_ai_processor", "enable_phase23_document_ai_processor", "OCR_PROCESSOR"},
		"Makefile":               {"test-phase23-document-ai-terraform", "MINISKY_PHASE23_DOCUMENT_AI_TERRAFORM_INTEGRATION=1"},
		"scripts/phase23-document-ai-terraform-integration.sh": {
			"MINISKY_PHASE23_DOCUMENT_AI_TERRAFORM_INTEGRATION", "plan -detailed-exitcode",
			"state rm", " import ", "destroy",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase24OrgPolicyTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {"org_policy_custom_endpoint", "/_minisky/orgpolicy/v2/"},
		"terraform/main.tf":      {"google_org_policy_policy", "enable_phase24_org_policy", "compute.disableSerialPortAccess"},
		"Makefile":               {"test-phase24-org-policy-terraform", "MINISKY_PHASE24_ORG_POLICY_TERRAFORM_INTEGRATION=1"},
		"scripts/phase24-org-policy-terraform-integration.sh": {
			"MINISKY_PHASE24_ORG_POLICY_TERRAFORM_INTEGRATION", ":evaluate",
			"plan -detailed-exitcode", "state rm", " import ", "destroy",
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase25BinaryAuthorizationTerraformGateStaticContract(t *testing.T) {
	root := repositoryRoot(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	contracts := map[string][]string{
		"terraform/providers.tf": {
			"binary_authorization_custom_endpoint",
			"/_minisky/binaryauthorization/v1/",
		},
		"terraform/main.tf": {
			`resource "google_binary_authorization_policy" "phase25"`,
			"enable_phase25_binary_authorization_policy",
			`name_pattern = "gcr.io/minisky-phase25/allowed/*"`,
			`evaluation_mode  = "ALWAYS_DENY"`,
			`enforcement_mode = "ENFORCED_BLOCK_AND_AUDIT_LOG"`,
		},
		"terraform/variables.tf": {
			"variable \"enable_phase25_binary_authorization_policy\" {\n" +
				"  description = \"Manage the optional local Phase-25 Binary Authorization project policy\"\n" +
				"  type        = bool\n" +
				"  default     = false\n" +
				"}",
		},
		"terraform/outputs.tf": {
			"output \"phase25_binary_authorization_policy_name\" {\n" +
				"  description = \"Canonical name of the optional local Binary Authorization policy, or null when disabled\"\n" +
				"  value       = local.use_minisky && var.enable_phase25_binary_authorization_policy ? \"projects/${var.project_id}/policy\" : null\n" +
				"}",
		},
		"Makefile": {
			"test-phase25-binary-authorization-terraform:",
			"MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION=1 ./scripts/phase25-binary-authorization-terraform-integration.sh",
		},
		"scripts/phase25-binary-authorization-terraform-integration.sh": {
			"MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION",
			"binary_authorization_custom_endpoint",
			"google_binary_authorization_policy.phase25[0]",
			`assert_plan_exit 0 "matching import refresh"`,
			`assert_plan_exit 2 "stale import refresh"`,
			`assert_plan_exit 0 "stale import reconcile"`,
			`state rm -backup="${work}/state-before-import.backup"`,
			`state rm -backup="${work}/state-before-stale-import.backup"`,
			`"projects/${project}"`,
			`"gcr.io/google_containers/*"`,
			`"ALWAYS_ALLOW"`,
			`for stale in ("description", "globalPolicyEvaluationMode", "clusterAdmissionRules")`,
			"trap cleanup EXIT",
			"trap 'cleanup 130' INT",
			"trap 'cleanup 143' TERM",
			`rm -rf "${work}"`,
			`rmdir "${lock}"`,
		},
	}
	for path, wants := range contracts {
		source := read(path)
		for _, want := range wants {
			if !strings.Contains(source, want) {
				t.Errorf("%s does not contain %q", path, want)
			}
		}
	}
}

func TestPhase25BinaryAuthorizationTerraformEvidenceIsLocalOnly(t *testing.T) {
	gates, err := BatchGates()
	if err != nil {
		t.Fatal(err)
	}
	for _, gate := range gates {
		if gate.ID != "phase24-25" {
			continue
		}
		for _, check := range gate.TerraformChecks {
			if check.Domain != "binaryauthorization.googleapis.com" {
				continue
			}
			if check.Status != EvidenceLocalPassed {
				t.Fatalf("Binary Authorization Terraform evidence status = %q, want local-passed", check.Status)
			}
			if check.Script != "scripts/phase25-binary-authorization-terraform-integration.sh" ||
				check.MakeTarget != "test-phase25-binary-authorization-terraform" {
				t.Fatalf("Binary Authorization Terraform evidence references = %+v", check)
			}
			if check.RunURL != "" || check.Commit != "" || check.Workflow != "" || check.Job != "" {
				t.Fatalf("local Binary Authorization Terraform evidence contains CI provenance: %+v", check)
			}
			return
		}
		t.Fatal("phase24-25 gate has no Binary Authorization Terraform check")
	}
	t.Fatal("phase24-25 gate is missing")
}

func TestPhase25BinaryAuthorizationClaimsStayTruthful(t *testing.T) {
	inventory, err := Phase18To25()
	if err != nil {
		t.Fatal(err)
	}
	var methodNote string
	for _, entry := range inventory {
		if entry.Domain == "binaryauthorization.googleapis.com" {
			methodNote = entry.MethodNote
			break
		}
	}
	if methodNote == "" {
		t.Fatal("Binary Authorization inventory entry is missing")
	}
	for _, want := range []string{
		"pre-restart allow/deny observation",
		"restart persistence/no-drift",
		"Cloud Deploy deny",
		"dry-run AUDIT permit",
		"without durable audit logging",
		"GKE/production admission security",
	} {
		if !strings.Contains(methodNote, want) {
			t.Errorf("Binary Authorization method note does not contain %q: %s", want, methodNote)
		}
	}

	gates, err := BatchGates()
	if err != nil {
		t.Fatal(err)
	}
	var checkNote string
	for _, gate := range gates {
		if gate.ID != "phase24-25" {
			continue
		}
		for _, check := range gate.TerraformChecks {
			if check.Domain == "binaryauthorization.googleapis.com" {
				checkNote = check.Note
			}
		}
	}
	if checkNote == "" {
		t.Fatal("Binary Authorization Terraform evidence note is missing")
	}
	for _, want := range []string{
		"allow/deny observations before restart",
		"after restart, policy persistence and zero drift",
		"enforced DENY blocks MiniSky Cloud Deploy rollouts",
		"DRYRUN_AUDIT_LOG_ONLY permits rollout and returns AUDIT",
		"without creating a durable audit record or log",
		"attestation/global/cluster evaluation returns explicit UNSUPPORTED",
		"not GKE or production admission security",
		"no CI pass is recorded",
	} {
		if !strings.Contains(checkNote, want) {
			t.Errorf("Binary Authorization Terraform note does not contain %q: %s", want, checkNote)
		}
	}
	for _, stale := range []string{
		"records audit",
		"recording only",
		"allow/deny observations survive restart",
	} {
		if strings.Contains(methodNote, stale) || strings.Contains(checkNote, stale) {
			t.Errorf("Binary Authorization evidence retains misleading wording %q", stale)
		}
	}
}

func TestPromotedTerraformStateRemovalBackupsStayInTemporaryWorkdirs(t *testing.T) {
	root := repositoryRoot(t)
	scripts := []string{
		"scripts/phase18-eventarc-terraform-integration.sh",
		"scripts/phase19-composer-terraform-integration.sh",
		"scripts/phase19-managed-kafka-terraform-integration.sh",
		"scripts/phase20-filestore-terraform-integration.sh",
		"scripts/phase20-identity-platform-terraform-integration.sh",
		"scripts/phase20-storage-transfer-terraform-integration.sh",
		"scripts/phase20-alloydb-terraform-integration.sh",
		"scripts/phase21-service-directory-terraform-integration.sh",
		"scripts/phase23-document-ai-terraform-integration.sh",
		"scripts/phase24-org-policy-terraform-integration.sh",
	}
	for _, script := range scripts {
		data, err := os.ReadFile(filepath.Join(root, script))
		if err != nil {
			t.Fatal(err)
		}
		source := string(data)
		if !strings.Contains(source, "state rm") ||
			!strings.Contains(source, `-backup="${work}/state-before-import.backup"`) {
			t.Errorf("%s does not keep state-rm backup in its temporary workdir", script)
		}
	}
}

func TestRegistryCountDocsAndTerraformClaimsStayAligned(t *testing.T) {
	root := repositoryRoot(t)
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	wantCount := fmt.Sprintf("**🚀 %d Registry-Verified Domains**", len(services))
	if !strings.Contains(string(readme), wantCount) {
		t.Errorf("README does not contain registry-derived count %q", wantCount)
	}

	compatibility, err := os.ReadFile(filepath.Join(root, "docs", "service-compatibility.md"))
	if err != nil {
		t.Fatal(err)
	}
	if rows := strings.Count(string(compatibility), "| `"); rows != len(services) {
		t.Errorf("compatibility rows = %d, registry domains = %d", rows, len(services))
	}

	terraformDocs, err := os.ReadFile(filepath.Join(root, "docs", "terraform-compatibility.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(terraformDocs), "Resources not listed above are not claimed as Terraform-compatible") {
		t.Fatal("Terraform documentation lost its explicit bounded-claim statement")
	}
	tfFiles, err := filepath.Glob(filepath.Join(root, "terraform", "*.tf"))
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := Phase18To25()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range tfFiles {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range inventory {
			if strings.Contains(string(data), entry.Domain) && !entry.TerraformClaim {
				t.Errorf("%s contains unproved experimental endpoint %s", path, entry.Domain)
			}
		}
	}
}

func TestRoadmapCanvasReportsPerDomainTerraformTruth(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "docs", "minisky-roadmap-completion-plan.canvas.tsx"))
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Join(strings.Fields(string(data)), " ")
	for _, want := range []string{
		"twelve independently recorded, domain-scoped Terraform",
		"Twelve bounded provider lifecycles now pass",
		"google_binary_authorization_policy",
		"matching import returns 0",
		"stale import returns 2",
		"exact default policy",
		"apply observes allow/deny before restart",
		"after restart, the gate proves policy persistence and no drift",
		"without repeating those decisions",
		"enforced DENY locally blocks MiniSky Cloud Deploy rollouts",
		"DRYRUN_AUDIT_LOG_ONLY permits rollout and returns AUDIT",
		"no durable audit record or log is created",
		"returns explicit UNSUPPORTED",
		"not GKE or production admission security",
		"without batch-wide production promotion",
	} {
		if !strings.Contains(source, want) {
			t.Errorf("roadmap canvas does not contain %q", want)
		}
	}
	for _, stale := range []string{
		"Zero Phase 18–25 provider claims",
		"Phase 18–25 Terraform evidence remain unverified or absent",
		"first Phase 18 Terraform slice for one default-off",
		"one Workflows provider slice passes locally",
		"records only the local advisory outcome",
		"allow/deny observations survive restart",
	} {
		if strings.Contains(source, stale) {
			t.Errorf("roadmap canvas retains stale Terraform claim %q", stale)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve evidence test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func validateEvidenceCheck(
	root string,
	evidenceName string,
	check EvidenceCheck,
	cache map[string]string,
) error {
	if err := ValidateEvidenceCheck(check); err != nil {
		return fmt.Errorf("%s: %w", evidenceName, err)
	}
	var problems []string
	for _, reference := range check.References {
		matches, err := filepath.Glob(filepath.Join(root, reference.Package, "*_test.go"))
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if len(matches) == 0 {
			problems = append(problems, fmt.Sprintf("reference package %q has no tests", reference.Package))
			continue
		}
		source := cache[reference.Package]
		if source == "" {
			var joined strings.Builder
			for _, match := range matches {
				data, err := os.ReadFile(match)
				if err != nil {
					problems = append(problems, err.Error())
					continue
				}
				joined.Write(data)
			}
			source = joined.String()
			cache[reference.Package] = source
		}
		for _, testName := range reference.Tests {
			if !strings.Contains(source, "func "+testName+"(") {
				problems = append(problems,
					fmt.Sprintf("references missing %s.%s", reference.Package, testName))
			}
		}
	}
	if check.Script != "" {
		if _, err := os.Stat(filepath.Join(root, check.Script)); err != nil {
			problems = append(problems, fmt.Sprintf("script %q: %v", check.Script, err))
		}
	}
	if check.MakeTarget != "" {
		makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
		if err != nil {
			problems = append(problems, err.Error())
		} else if !strings.Contains(string(makefile), "\n"+check.MakeTarget+":") {
			problems = append(problems, fmt.Sprintf("references missing Make target %q", check.MakeTarget))
		}
	}
	if check.Workflow != "" {
		workflow, err := os.ReadFile(filepath.Join(root, check.Workflow))
		if err != nil {
			problems = append(problems, fmt.Sprintf("workflow %q: %v", check.Workflow, err))
		} else if check.Job != "" && !strings.Contains(string(workflow), "\n  "+check.Job+":") {
			problems = append(problems, fmt.Sprintf("references missing CI job %q", check.Job))
		}
	} else if check.Job != "" {
		problems = append(problems, "CI job is present without a workflow")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s: %s", evidenceName, strings.Join(problems, "; "))
	}
	return nil
}

func assertTestReferences(
	t *testing.T,
	root string,
	cache map[string]string,
	evidenceName string,
	packagePath string,
	tests []string,
) {
	t.Helper()
	source, ok := cache[packagePath]
	if !ok {
		matches, err := filepath.Glob(filepath.Join(root, packagePath, "*_test.go"))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) == 0 {
			t.Errorf("%s has no package tests", packagePath)
			return
		}
		var joined strings.Builder
		for _, match := range matches {
			data, err := os.ReadFile(match)
			if err != nil {
				t.Fatal(err)
			}
			joined.Write(data)
		}
		source = joined.String()
		cache[packagePath] = source
	}
	for _, name := range tests {
		if !strings.Contains(source, "func "+name+"(") {
			t.Errorf("%s references missing %s.%s", evidenceName, packagePath, name)
		}
	}
}
