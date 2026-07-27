package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"minisky/pkg/evidence"
	"minisky/pkg/registry"
)

func TestServiceCatalogGolden(t *testing.T) {
	services, inventory, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	got, err := renderServiceCatalog(services, inventory)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "service-catalog.golden.md"))
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("service catalog differs from golden file; run go run ./cmd/docs-truth to regenerate documentation and update the golden deliberately")
	}
}

func TestRenderingIsDeterministic(t *testing.T) {
	services, inventory, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	reverseServices(services)
	reverseInventory(inventory)

	first, err := renderServiceCatalog(services, inventory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := renderServiceCatalog(services, inventory)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("rendering the same truth twice produced different output")
	}
}

func TestReplaceGeneratedSectionIsIdempotent(t *testing.T) {
	const original = "before\n" + serviceCatalogStart + "\nstale\n" + serviceCatalogEnd + "\nafter\n"
	once, err := replaceGeneratedSection(original, serviceCatalogStart, serviceCatalogEnd, "fresh\n")
	if err != nil {
		t.Fatal(err)
	}
	twice, err := replaceGeneratedSection(once, serviceCatalogStart, serviceCatalogEnd, "fresh\n")
	if err != nil {
		t.Fatal(err)
	}
	if once != twice {
		t.Fatalf("second replacement changed output:\n%s", twice)
	}
	if !strings.Contains(once, "before\n"+serviceCatalogStart+"\nfresh\n"+serviceCatalogEnd+"\nafter") {
		t.Fatalf("hand-written boundaries were not preserved:\n%s", once)
	}
}

func TestRenderPhaseSummaryDoesNotPromotePackageTests(t *testing.T) {
	services := []registry.Service{{
		Domain:      "example.googleapis.com",
		Support:     registry.SupportExperimental,
		Persistence: registry.PersistenceFile,
	}}
	inventory := []evidence.PhaseService{{
		Domain:         "example.googleapis.com",
		Selector:       "example.googleapis.com",
		Persistence:    "file",
		Package:        "pkg/shims/example",
		Tests:          []string{"TestPersistAndReload"},
		MethodNote:     "metadata-only lifecycle",
		TerraformClaim: false,
	}}
	got, err := renderPhaseSummary(services, inventory)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"1 experimental",
		"1 default-off",
		"0 Terraform claims",
		"Generated-client evidence: not recorded",
		"CI evidence: not recorded",
		"Package tests are not promotion evidence",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary does not contain %q:\n%s", want, got)
		}
	}
}

func TestWriteOrCheckDetectsDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc.md")
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeOrCheck(path, []byte("fresh"), true); err == nil {
		t.Fatal("check mode accepted documentation drift")
	}
	if err := writeOrCheck(path, []byte("fresh"), false); err != nil {
		t.Fatal(err)
	}
	if err := writeOrCheck(path, []byte("fresh"), true); err != nil {
		t.Fatalf("check mode rejected current documentation: %v", err)
	}
}

func reverseServices(values []registry.Service) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseInventory(values []evidence.PhaseService) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
