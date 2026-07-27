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
		if entry.TerraformClaim {
			t.Errorf("%s must not claim Terraform compatibility", entry.Domain)
		}
		if service.Persistence == registry.PersistenceFile {
			hasRestartEvidence := false
			for _, test := range entry.Tests {
				if strings.Contains(test, "Persist") || strings.Contains(test, "Restart") ||
					strings.Contains(test, "Reload") || strings.Contains(test, "Survive") {
					hasRestartEvidence = true
				}
			}
			if !hasRestartEvidence || strings.Contains(entry.MethodNote, "no restart claim") {
				t.Errorf("%s claims file persistence without named restart evidence", entry.Domain)
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
