// Command docs-truth renders compatibility documentation from registry and
// Phase 18-25 evidence metadata.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"minisky/pkg/evidence"
	"minisky/pkg/registry"
	_ "minisky/pkg/shims"
)

const (
	serviceCatalogStart = "<!-- BEGIN GENERATED SERVICE CATALOG -->"
	serviceCatalogEnd   = "<!-- END GENERATED SERVICE CATALOG -->"
	phaseSummaryStart   = "<!-- BEGIN GENERATED PHASE 18-25 SUMMARY -->"
	phaseSummaryEnd     = "<!-- END GENERATED PHASE 18-25 SUMMARY -->"
	readmeCountStart    = "<!-- BEGIN GENERATED REGISTRY COUNT -->"
	readmeCountEnd      = "<!-- END GENERATED REGISTRY COUNT -->"
)

func main() {
	check := flag.Bool("check", false, "fail if generated documentation is stale")
	root := flag.String("root", ".", "repository root")
	flag.Parse()

	if err := generate(filepath.Clean(*root), *check); err != nil {
		fmt.Fprintln(os.Stderr, "docs-truth:", err)
		os.Exit(1)
	}
}

func truth() ([]registry.Service, []evidence.PhaseService, error) {
	services, err := registry.Services()
	if err != nil {
		return nil, nil, err
	}
	inventory, err := evidence.Phase18To25()
	if err != nil {
		return nil, nil, err
	}
	return services, inventory, nil
}

func generate(root string, check bool) error {
	services, inventory, err := truth()
	if err != nil {
		return err
	}
	catalog, err := renderServiceCatalog(services, inventory)
	if err != nil {
		return err
	}
	summary, err := renderPhaseSummary(services, inventory)
	if err != nil {
		return err
	}

	docsPath := filepath.Join(root, "docs", "service-compatibility.md")
	docs, err := os.ReadFile(docsPath)
	if err != nil {
		return err
	}
	updatedDocs, err := replaceGeneratedSection(
		string(docs), serviceCatalogStart, serviceCatalogEnd, catalog,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}
	updatedDocs, err = replaceGeneratedSection(
		updatedDocs, phaseSummaryStart, phaseSummaryEnd, summary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", docsPath, err)
	}

	readmePath := filepath.Join(root, "README.md")
	readme, err := os.ReadFile(readmePath)
	if err != nil {
		return err
	}
	count := fmt.Sprintf(
		"- **🚀 %d Registry-Verified Domains**: The exact catalog count and generated\n"+
			"  compatibility rows come from `registry.Services()`. Phase 18–25\n"+
			"  inventory entries remain experimental and default-off.\n"+
			"  See [Service Compatibility](docs/service-compatibility.md).\n",
		len(services),
	)
	updatedReadme, err := replaceGeneratedSection(
		string(readme), readmeCountStart, readmeCountEnd, count,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}
	updatedReadme, err = replaceGeneratedSection(
		updatedReadme, phaseSummaryStart, phaseSummaryEnd, summary,
	)
	if err != nil {
		return fmt.Errorf("%s: %w", readmePath, err)
	}

	targets := []struct {
		path string
		data []byte
	}{
		{docsPath, []byte(updatedDocs)},
		{readmePath, []byte(updatedReadme)},
		{filepath.Join(root, "cmd", "docs-truth", "testdata", "service-catalog.golden.md"), []byte(catalog)},
	}
	var drift []string
	for _, target := range targets {
		if err := writeOrCheck(target.path, target.data, check); err != nil {
			if check && errors.Is(err, errDrift) {
				drift = append(drift, target.path)
				continue
			}
			return err
		}
	}
	if len(drift) > 0 {
		return fmt.Errorf("generated documentation drift: %s; run go run ./cmd/docs-truth", strings.Join(drift, ", "))
	}
	return nil
}

func renderServiceCatalog(
	services []registry.Service,
	inventory []evidence.PhaseService,
) (string, error) {
	services = append([]registry.Service(nil), services...)
	inventory = append([]evidence.PhaseService(nil), inventory...)
	sort.Slice(services, func(i, j int) bool { return services[i].Domain < services[j].Domain })
	sort.Slice(inventory, func(i, j int) bool { return inventory[i].Domain < inventory[j].Domain })

	evidenceByDomain := make(map[string]evidence.PhaseService, len(inventory))
	for _, entry := range inventory {
		if _, duplicate := evidenceByDomain[entry.Domain]; duplicate {
			return "", fmt.Errorf("duplicate Phase 18-25 evidence for %s", entry.Domain)
		}
		evidenceByDomain[entry.Domain] = entry
	}

	var output strings.Builder
	output.WriteString("| Domain | Registry classification | Persistence | Support | Fidelity | Default-off | Method note / evidence tests | Terraform claim |\n")
	output.WriteString("|--------|-------------------------|-------------|---------|----------|-------------|------------------------------|-----------------|\n")
	for _, service := range services {
		entry, hasEvidence := evidenceByDomain[service.Domain]
		if service.Support == registry.SupportExperimental && !hasEvidence {
			return "", fmt.Errorf("experimental service %s has no Phase 18-25 evidence entry", service.Domain)
		}
		if hasEvidence {
			if service.Support != registry.SupportExperimental {
				return "", fmt.Errorf("%s has Phase 18-25 evidence but registry support is %s", service.Domain, service.Support)
			}
			if entry.Persistence != string(service.Persistence) {
				return "", fmt.Errorf(
					"%s evidence persistence %s differs from registry %s",
					service.Domain, entry.Persistence, service.Persistence,
				)
			}
			delete(evidenceByDomain, service.Domain)
		}

		fidelity := string(service.Fidelity)
		if fidelity == "" {
			fidelity = "— (not promoted)"
		}
		classification := string(service.Fidelity)
		if classification == "" {
			classification = string(service.Support)
		}
		defaultOff := "No"
		note := registryNote(service)
		terraformClaim := "Not inventoried"
		if hasEvidence {
			defaultOff = "Yes"
			note = fmt.Sprintf(
				"%s<br>Package tests only: %s",
				entry.MethodNote,
				formatTests(entry.Package, entry.Tests),
			)
			terraformClaim = "No"
			if entry.TerraformClaim {
				terraformClaim = "Yes"
			}
		}
		fmt.Fprintf(
			&output,
			"| `%s` | %s | %s | %s | %s | %s | %s | %s |\n",
			service.Domain,
			classification,
			service.Persistence,
			service.Support,
			fidelity,
			defaultOff,
			escapeCell(note),
			terraformClaim,
		)
	}
	if len(evidenceByDomain) > 0 {
		var unknown []string
		for domain := range evidenceByDomain {
			unknown = append(unknown, domain)
		}
		sort.Strings(unknown)
		return "", fmt.Errorf("evidence entries missing from registry: %s", strings.Join(unknown, ", "))
	}
	return output.String(), nil
}

func renderPhaseSummary(
	services []registry.Service,
	inventory []evidence.PhaseService,
) (string, error) {
	gates, err := evidence.BatchGates()
	if err != nil {
		return "", err
	}
	experimental := 0
	for _, service := range services {
		if service.Support == registry.SupportExperimental {
			experimental++
		}
	}
	if experimental != len(inventory) {
		return "", fmt.Errorf(
			"experimental registry count %d differs from Phase 18-25 inventory count %d",
			experimental, len(inventory),
		)
	}
	terraformClaims := 0
	persistence := make(map[string]int)
	for _, entry := range inventory {
		persistence[entry.Persistence]++
		if entry.TerraformClaim {
			terraformClaims++
		}
	}
	var categories []string
	for category, count := range persistence {
		categories = append(categories, fmt.Sprintf("%s=%d", category, count))
	}
	sort.Strings(categories)

	packageLocal := 0
	strictIAMLocal := 0
	generatedLocal := 0
	generatedConfigured := 0
	restartLocal := 0
	cleanupLocal := 0
	ciConfigured := 0
	for _, gate := range gates {
		if gate.PackageUnit.Status == evidence.EvidenceLocalPassed {
			packageLocal++
		}
		if gate.StrictIAM.Status == evidence.EvidenceLocalPassed {
			strictIAMLocal++
		}
		if gate.GeneratedClientLifecycle.Status == evidence.EvidenceLocalPassed {
			generatedLocal++
		}
		if gate.GeneratedClientLifecycle.Status == evidence.EvidenceConfiguredUnverified {
			generatedConfigured++
		}
		if gate.DaemonRestart.Status == evidence.EvidenceLocalPassed {
			restartLocal++
		}
		if gate.Cleanup.Status == evidence.EvidenceLocalPassed {
			cleanupLocal++
		}
		if gate.CI.Status == evidence.EvidenceConfiguredUnverified {
			ciConfigured++
		}
	}

	return fmt.Sprintf(
		"**Generated truth:** %d experimental; %d default-off; %d Terraform claims. "+
			"Persistence inventory: %s.\n\n"+
			"Machine-readable promotion matrix: %d batch gates. Package-unit gates passed locally: %d/%d; "+
			"strict-IAM gates passed locally: %d/%d. Generated-client lifecycle gates passed locally: %d/%d; "+
			"configured but unverified: %d/%d. Restart gates passed locally: %d/%d; cleanup gates passed locally: %d/%d; "+
			"CI gates configured but unverified: %d/%d. Package and IAM passes do not promote compatibility; "+
			"every inventoried service remains experimental until its required integration gates pass.\n",
		experimental,
		experimental,
		terraformClaims,
		strings.Join(categories, ", "),
		len(gates),
		packageLocal,
		len(gates),
		strictIAMLocal,
		len(gates),
		generatedLocal,
		len(gates),
		generatedConfigured,
		len(gates),
		restartLocal,
		len(gates),
		cleanupLocal,
		len(gates),
		ciConfigured,
		len(gates),
	), nil
}

func registryNote(service registry.Service) string {
	if service.SupportReason != "" {
		return service.SupportReason
	}
	if service.BackendContract != "" {
		return service.BackendContract
	}
	return "Registry metadata only; see the hand-written compatibility boundaries above"
}

func formatTests(packagePath string, tests []string) string {
	formatted := make([]string, len(tests))
	for index, test := range tests {
		formatted[index] = "`" + packagePath + "." + test + "`"
	}
	return strings.Join(formatted, "<br>")
}

func escapeCell(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}

func replaceGeneratedSection(document, start, end, generated string) (string, error) {
	if strings.Count(document, start) != 1 || strings.Count(document, end) != 1 {
		return "", fmt.Errorf("expected exactly one %q and %q marker", start, end)
	}
	startIndex := strings.Index(document, start) + len(start)
	endIndex := strings.Index(document, end)
	if endIndex < startIndex {
		return "", fmt.Errorf("end marker %q precedes start marker", end)
	}
	generated = strings.TrimSuffix(generated, "\n")
	return document[:startIndex] + "\n" + generated + "\n" + document[endIndex:], nil
}

var errDrift = errors.New("generated file is stale")

func writeOrCheck(path string, want []byte, check bool) error {
	got, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if bytes.Equal(got, want) {
		return nil
	}
	if check {
		return fmt.Errorf("%w: %s", errDrift, path)
	}
	mode := os.FileMode(0o644)
	if info, statErr := os.Stat(path); statErr == nil {
		mode = info.Mode().Perm()
	}
	return os.WriteFile(path, want, mode)
}
