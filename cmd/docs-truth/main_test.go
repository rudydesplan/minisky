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
		TerraformClaim: true,
	}}
	got, err := renderPhaseSummary(services, inventory)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"1 experimental",
		"1 default-off",
		"1 Terraform claim",
		"6 batch gates",
		"11 per-domain Terraform checks",
		"Package-unit gates passed locally: 6/6",
		"strict-IAM gates passed locally: 6/6",
		"Generated-client lifecycle gates passed locally: 6/6",
		"configured but unverified: 0/6",
		"Restart gates passed locally: 6/6",
		"cleanup gates passed locally: 6/6",
		"CI gates passed: 6/6; configured but unverified: 0/6",
		"Heavy backend CI gates passed: 1/1; configured but unverified: 0/1",
		"Package and IAM passes do not promote compatibility",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary does not contain %q:\n%s", want, got)
		}
	}
}

func TestRenderPhase12PlatformSummaryBranchesOnCIStatus(t *testing.T) {
	validRun := "https://github.com/rudydesplan/minisky/actions/runs/123"
	validCommit := strings.Repeat("a", 40)
	tests := []struct {
		name      string
		check     evidence.EvidenceCheck
		want      string
		forbidden string
	}{
		{
			name:  "configured unverified",
			check: evidence.EvidenceCheck{Status: evidence.EvidenceConfiguredUnverified, Note: "configured"},
			want:  "Required pull-request/main CI and optional manual execution are configured, but no external Phase 12 pass is recorded.",
		},
		{
			name: "ci passed",
			check: evidence.EvidenceCheck{
				Status: evidence.EvidenceCIPassed,
				RunURL: validRun,
				Commit: validCommit,
				Note:   "recorded",
			},
			want:      "Required Phase 12 CI passed in [GitHub Actions run 123](" + validRun + ") on commit `" + validCommit + "`.",
			forbidden: "no external Phase 12 pass is recorded",
		},
		{
			name:  "optional unverified",
			check: evidence.EvidenceCheck{Status: evidence.EvidenceOptionalUnverified, Note: "optional"},
			want:  "Phase 12 CI is optional and externally unverified.",
		},
		{
			name:  "local passed",
			check: evidence.EvidenceCheck{Status: evidence.EvidenceLocalPassed, Note: "local only"},
			want:  "The Phase 12 CI check is recorded only as local-passed; no external CI pass is recorded.",
		},
		{
			name:  "not applicable",
			check: evidence.EvidenceCheck{Status: evidence.EvidenceNotApplicable, Note: "not applicable"},
			want:  "Phase 12 CI is marked not-applicable by machine evidence.",
		},
		{
			name:  "absent",
			check: evidence.EvidenceCheck{Status: evidence.EvidenceAbsent, Note: "absent"},
			want:  "Phase 12 CI evidence is absent.",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gates := []evidence.PlatformGate{
				{
					ID:    "phase12-package-race-unit",
					Phase: 12,
					Name:  "Local gate",
					Check: evidence.EvidenceCheck{Status: evidence.EvidenceLocalPassed, Note: "passed here"},
				},
				{ID: "phase12-ci", Phase: 12, Name: "CI gate", Check: test.check},
			}
			got, err := renderPhase12PlatformSummary(gates)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{
				test.want,
				"bounded W3C propagation",
				"project-keyed lookup scoping, not cross-project authorization",
				"persistent trace backend",
				"RBAC replay isolation",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("Phase 12 summary does not contain %q:\n%s", want, got)
				}
			}
			if test.forbidden != "" && strings.Contains(got, test.forbidden) {
				t.Errorf("Phase 12 summary contains contradictory %q:\n%s", test.forbidden, got)
			}
		})
	}
}

func TestPhase12ClaimsMatchMachineCIEvidence(t *testing.T) {
	validRun := "https://github.com/rudydesplan/minisky/actions/runs/123"
	validCommit := strings.Repeat("a", 40)
	tests := []struct {
		name     string
		check    evidence.EvidenceCheck
		document string
		valid    bool
	}{
		{
			name:     "configured rejects premature pass",
			check:    evidence.EvidenceCheck{Status: evidence.EvidenceConfiguredUnverified, Note: "configured"},
			document: "Phase 12 is CI-verified.",
		},
		{
			name:     "configured accepts truthful wording",
			check:    evidence.EvidenceCheck{Status: evidence.EvidenceConfiguredUnverified, Note: "configured"},
			document: "Phase 12 required CI is configured; no external pass is recorded.",
			valid:    true,
		},
		{
			name:     "optional rejects premature pass",
			check:    evidence.EvidenceCheck{Status: evidence.EvidenceOptionalUnverified, Note: "optional"},
			document: "Phase 12 CI passed.",
		},
		{
			name:     "local rejects premature pass",
			check:    evidence.EvidenceCheck{Status: evidence.EvidenceLocalPassed, Note: "local"},
			document: "Phase 12 CI has passed.",
		},
		{
			name:     "not applicable rejects premature pass",
			check:    evidence.EvidenceCheck{Status: evidence.EvidenceNotApplicable, Note: "not applicable"},
			document: "Phase 12 is CI-verified.",
		},
		{
			name:     "absent rejects premature pass",
			check:    evidence.EvidenceCheck{Status: evidence.EvidenceAbsent, Note: "absent"},
			document: "Phase 12 CI passed.",
		},
		{
			name: "passed accepts pass wording",
			check: evidence.EvidenceCheck{
				Status: evidence.EvidenceCIPassed,
				RunURL: validRun,
				Commit: validCommit,
				Note:   "recorded",
			},
			document: "Phase 12 CI passed in GitHub Actions.",
			valid:    true,
		},
		{
			name: "passed rejects stale unverified wording",
			check: evidence.EvidenceCheck{
				Status: evidence.EvidenceCIPassed,
				RunURL: validRun,
				Commit: validCommit,
				Note:   "recorded",
			},
			document: "Phase 12 has no external pass recorded.",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gates := []evidence.PlatformGate{{
				ID: "phase12-ci", Phase: 12, Name: "CI", Check: test.check,
			}}
			err := validatePhase12Claims(test.document, gates)
			if test.valid && err != nil {
				t.Fatalf("truthful claim rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("claim contradicted machine CI evidence")
			}
		})
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

func TestValidateHandwrittenClaimsRejectsStaleTerraformAbsence(t *testing.T) {
	stale := "Terraform provider evidence remains absent, so every experimental domain remains default-off."
	if err := validateHandwrittenClaims(stale); err == nil {
		t.Fatal("accepted stale batch-wide Terraform absence claim")
	}
	if err := validateHandwrittenClaims(
		"Per-domain Terraform claims and boundaries are listed in the generated catalog."); err != nil {
		t.Fatalf("rejected current per-domain wording: %v", err)
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
