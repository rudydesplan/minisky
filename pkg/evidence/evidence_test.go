package evidence

import (
	"fmt"
	"os"
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
			t.Errorf("%s must not claim Terraform compatibility", entry.Domain)
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
	cache := make(map[string]string)
	makefile, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
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
			"terraform":                gate.Terraform,
			"cleanup":                  gate.Cleanup,
			"ci":                       gate.CI,
		}
		for name, check := range checks {
			if check.Status == "" || check.Note == "" {
				t.Errorf("%s %s has incomplete status metadata: %+v", gate.ID, name, check)
			}
			for _, reference := range check.References {
				assertTestReferences(t, root, cache, gate.ID+" "+name, reference.Package, reference.Tests)
			}
			if check.Script != "" {
				if _, err := os.Stat(filepath.Join(root, check.Script)); err != nil {
					t.Errorf("%s %s script %q: %v", gate.ID, name, check.Script, err)
				}
			}
			if check.MakeTarget != "" &&
				!strings.Contains(string(makefile), "\n"+check.MakeTarget+":") {
				t.Errorf("%s %s references missing Make target %q", gate.ID, name, check.MakeTarget)
			}
			if check.Workflow != "" {
				if _, err := os.Stat(filepath.Join(root, check.Workflow)); err != nil {
					t.Errorf("%s %s workflow %q: %v", gate.ID, name, check.Workflow, err)
				}
			}
			if check.Job != "" && !strings.Contains(string(workflow), "\n  "+check.Job+":") {
				t.Errorf("%s %s references missing CI job %q", gate.ID, name, check.Job)
			}
		}
		wantPackageStatus := EvidenceLocalPassed
		if gate.ID == "phase18-25" {
			wantPackageStatus = EvidenceConfiguredUnverified
		}
		if gate.PackageUnit.Status != wantPackageStatus ||
			gate.GeneratedClientLifecycle.Status != EvidenceConfiguredUnverified ||
			gate.DaemonRestart.Status != EvidenceConfiguredUnverified ||
			gate.StrictIAM.Status != EvidenceLocalPassed ||
			gate.Terraform.Status != EvidenceAbsent ||
			gate.Cleanup.Status != EvidenceConfiguredUnverified ||
			gate.CI.Status != EvidenceConfiguredUnverified {
			t.Errorf("%s overstates or conflates batch evidence: %+v", gate.ID, gate)
		}
		if len(gate.Terraform.References) != 0 || gate.Terraform.Script != "" ||
			gate.Terraform.MakeTarget != "" {
			t.Errorf("%s creates a Terraform claim without a provider gate", gate.ID)
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
			if strings.Contains(string(data), entry.Domain) {
				t.Errorf("%s contains unproved experimental endpoint %s", path, entry.Domain)
			}
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
