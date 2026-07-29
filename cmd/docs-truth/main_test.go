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

func TestGeneratedServiceDocumentListsEachDomainExactlyOnce(t *testing.T) {
	services, inventory, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := renderServiceCatalog(services, inventory)
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile(filepath.Join("testdata", "service-catalog.golden.md"))
	if err != nil {
		t.Fatal(err)
	}
	document, err := os.ReadFile(filepath.Join("..", "..", "docs", "service-compatibility.md"))
	if err != nil {
		t.Fatal(err)
	}
	regenerated, err := replaceGeneratedSection(
		string(document), serviceCatalogStart, serviceCatalogEnd, catalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range services {
		literal := "`" + service.Domain + "`"
		if count := strings.Count(catalog, literal); count != 1 {
			t.Errorf("generated catalog lists %q %d times, want exactly once", service.Domain, count)
		}
		if count := strings.Count(string(golden), literal); count != 1 {
			t.Errorf("golden catalog lists %q %d times, want exactly once", service.Domain, count)
		}
		if count := strings.Count(regenerated, literal); count != 1 {
			t.Errorf("regenerated service document lists %q %d times, want exactly once", service.Domain, count)
		}
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
		"12 per-domain Terraform checks",
		"Package-unit gates passed locally: 6/6",
		"strict-IAM gates passed locally: 6/6",
		"Generated-client lifecycle gates passed locally: 6/6",
		"configured but unverified: 0/6",
		"Restart gates passed locally: 6/6",
		"cleanup gates passed locally: 6/6",
		"CI gates passed: 6/6; configured but unverified: 0/6",
		"Heavy backend CI gates passed: 1/1; configured but unverified: 0/1",
		"Terraform CI gates passed: 12/12; configured but unverified: 0/12",
		"Admission replay gates passed locally: 1/1",
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

func TestRenderRegistryCountIncludesSupportInventory(t *testing.T) {
	got, err := renderRegistryCount([]registry.Service{
		{Domain: "implemented.example", Support: registry.SupportImplemented},
		{Domain: "experimental.example", Support: registry.SupportExperimental},
		{Domain: "deferred.example", Support: registry.SupportDeferred},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"3 Registry-Verified Domains",
		"1 implemented, 1 experimental, and 1 deferred",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("registry count missing %q:\n%s", want, got)
		}
	}
}

func validMemcacheServiceGate() evidence.ServiceGate {
	return evidence.ServiceGate{
		ID:              "phase15-memcached",
		Phase:           15,
		Name:            "Bounded Memcached lifecycle",
		Domain:          "memcache.googleapis.com",
		ProviderVersion: "7.41.0",
		Dimensions:      []string{"sdk-create", "terraform-apply", "exact-docker-cleanup"},
		EvidenceCheck: evidence.EvidenceCheck{
			Status:     evidence.EvidenceLocalPassedUncommitted,
			Script:     "scripts/memcache-integration.sh",
			MakeTarget: "test-memcache-integration",
			Note:       "bounded working-tree local pass",
		},
		CI: evidence.EvidenceCheck{
			Status:   evidence.EvidenceConfiguredUnverified,
			Workflow: ".github/workflows/critical-integration.yml",
			Job:      "memcache-integration",
			Note:     "configured without external provenance",
		},
	}
}

func futureImmutableMemcacheServiceGate() evidence.ServiceGate {
	gate := validMemcacheServiceGate()
	gate.Status = evidence.EvidenceLocalPassed
	gate.SourceCommit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	gate.Note = "future immutable local pass"
	return gate
}

func futureMemcacheServiceGate() evidence.ServiceGate {
	gate := futureImmutableMemcacheServiceGate()
	gate.CI = evidence.EvidenceCheck{
		Status:   evidence.EvidenceCIPassed,
		Workflow: ".github/workflows/critical-integration.yml",
		Job:      "memcache-integration",
		RunURL:   "https://github.com/rudydesplan/minisky/actions/runs/123456",
		JobURL:   "https://github.com/rudydesplan/minisky/actions/runs/123456/job/654321",
		Commit:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Note:     "future immutable pass",
	}
	return gate
}

func TestSelectMemcacheServiceGateValidatesInventory(t *testing.T) {
	services := []registry.Service{{
		Domain:  "memcache.googleapis.com",
		Support: registry.SupportImplemented,
	}}
	valid := validMemcacheServiceGate()
	for _, test := range []struct {
		name      string
		services  []registry.Service
		gates     []evidence.ServiceGate
		wantError string
	}{
		{name: "valid current uncommitted pass", services: services, gates: []evidence.ServiceGate{valid}},
		{name: "valid future immutable local pass", services: services, gates: []evidence.ServiceGate{futureImmutableMemcacheServiceGate()}},
		{name: "valid future CI pass", services: services, gates: []evidence.ServiceGate{futureMemcacheServiceGate()}},
		{name: "missing", services: services, wantError: "exactly one"},
		{
			name:     "duplicate",
			services: services,
			gates: []evidence.ServiceGate{
				valid,
				func() evidence.ServiceGate {
					duplicate := valid
					duplicate.ID = "duplicate-memcached"
					return duplicate
				}(),
			},
			wantError: "exactly one",
		},
		{
			name:      "unregistered domain",
			services:  nil,
			gates:     []evidence.ServiceGate{valid},
			wantError: "unregistered",
		},
		{
			name:     "malformed lifecycle metadata",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := valid
				malformed.MakeTarget = ""
				return malformed
			}()},
			wantError: "missing required lifecycle metadata",
		},
		{
			name:     "uncommitted local pass with mixed revision provenance",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := valid
				malformed.SourceCommit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				return malformed
			}()},
			wantError: "must not include a local source commit",
		},
		{
			name:     "immutable local pass without revision provenance",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := futureImmutableMemcacheServiceGate()
				malformed.SourceCommit = ""
				return malformed
			}()},
			wantError: "requires a full source commit",
		},
		{
			name:     "unknown local status",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := valid
				malformed.Status = evidence.EvidenceConfiguredUnverified
				return malformed
			}()},
			wantError: "want local-passed-uncommitted or local-passed",
		},
		{
			name:     "unknown CI status",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := valid
				malformed.CI.Status = evidence.EvidenceAbsent
				return malformed
			}()},
			wantError: "want configured-unverified or ci-passed",
		},
		{
			name:     "configured CI with mixed pass provenance",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := valid
				malformed.CI.RunURL = "https://github.com/rudydesplan/minisky/actions/runs/123456"
				malformed.CI.Commit = "0123456789abcdef0123456789abcdef01234567"
				return malformed
			}()},
			wantError: "must not include CI run, job, or commit fields",
		},
		{
			name:     "CI pass without immutable provenance",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := futureMemcacheServiceGate()
				malformed.CI.RunURL = ""
				malformed.CI.Commit = ""
				return malformed
			}()},
			wantError: "requires a rudydesplan/minisky GitHub Actions run URL",
		},
		{
			name:     "CI pass with abbreviated commit",
			services: services,
			gates: []evidence.ServiceGate{func() evidence.ServiceGate {
				malformed := futureMemcacheServiceGate()
				malformed.CI.Commit = "0123456"
				return malformed
			}()},
			wantError: "requires a lowercase 40-hex commit",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := selectMemcacheServiceGate(test.services, test.gates)
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error=%v want substring %q", err, test.wantError)
			}
		})
	}
}

func TestCurrentMemcacheServiceGateHasRequiredEvidenceState(t *testing.T) {
	services, _, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	gates, err := evidence.ServiceGates()
	if err != nil {
		t.Fatal(err)
	}
	gate, err := selectMemcacheServiceGate(services, gates)
	if err != nil {
		t.Fatal(err)
	}
	if gate.Status != evidence.EvidenceLocalPassed {
		t.Fatalf("local status=%q want local-passed", gate.Status)
	}
	if gate.SourceCommit != "8e16d147b0127bd3120eae106aa0da1fb59a52c9" {
		t.Fatalf("local evidence source commit=%q", gate.SourceCommit)
	}
	if gate.CI.Status != evidence.EvidenceCIPassed {
		t.Fatalf("CI status=%q want ci-passed", gate.CI.Status)
	}
	if gate.CI.RunURL == "" || gate.CI.JobURL == "" || gate.CI.Commit != gate.SourceCommit {
		t.Fatalf("CI lacks exact pass provenance: %+v", gate.CI)
	}
}

func TestCurrentRedisServiceGateRendersLocalUncommittedBoundary(t *testing.T) {
	services, _, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	gates, err := evidence.ServiceGates()
	if err != nil {
		t.Fatal(err)
	}
	gate, err := selectRedisServiceGate(services, gates)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderRedisServiceGate(gate)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"`local-passed-uncommitted`",
		"`phase15-redis`",
		"source SHA-256",
		"diff SHA-256",
		"Google provider `7.41.0`",
		"`make test-redis-integration`",
		"CI is `configured-unverified`",
		"no portable AOF export",
		"cooperative Docker daemon",
		"before resolved provenance is persisted",
		"restart neither adopts nor automatically removes",
	} {
		if !strings.Contains(rendered, required) {
			t.Errorf("Redis rendering lacks %q:\n%s", required, rendered)
		}
	}
	if strings.Contains(rendered, "immutable source commit") ||
		strings.Contains(rendered, "CI is `ci-passed`") {
		t.Fatal("Redis local uncommitted evidence was rendered as committed or CI-passed")
	}
}

func TestRenderMemcacheServiceGateUsesMachineEvidence(t *testing.T) {
	gate := validMemcacheServiceGate()
	rendered, err := renderMemcacheServiceGate(gate)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"`local-passed-uncommitted`",
		"in the current working tree",
		"locally passing working-tree gate is non-promotable",
		"has no immutable source revision evidence",
		"Google provider `7.41.0`",
		"`make test-memcache-integration`",
		"`scripts/memcache-integration.sh`",
		"Lifecycle dimensions (3): `sdk-create`, `terraform-apply`, `exact-docker-cleanup`",
		"CI is `configured-unverified`",
		"no external run URL or commit is recorded",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("rendered service gate missing %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "CI is `ci-passed`") {
		t.Fatal("configured Memcached evidence was rendered as a CI pass")
	}
	if strings.Contains(rendered, "at immutable source commit") {
		t.Fatal("uncommitted Memcached evidence was rendered with immutable revision provenance")
	}
}

func TestRenderMemcacheServiceGateFutureCIPassUsesProvenance(t *testing.T) {
	gate := futureMemcacheServiceGate()
	rendered, err := renderMemcacheServiceGate(gate)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"CI is `ci-passed`",
		"[GitHub Actions run 123456](" + gate.CI.RunURL + ")",
		"([job](" + gate.CI.JobURL + "))",
		"`" + gate.CI.Commit + "`",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("future CI rendering missing %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "`configured-unverified`") {
		t.Fatal("future CI pass retained configured-unverified wording")
	}
}

func TestMemcacheEvidenceTransitionsRenderAcrossDocumentsWithoutChangingPhaseAggregate(t *testing.T) {
	services := []registry.Service{{
		Domain:  "memcache.googleapis.com",
		Support: registry.SupportImplemented,
	}}
	currentGates, err := evidence.ServiceGates()
	if err != nil {
		t.Fatal(err)
	}
	currentGate, err := selectMemcacheServiceGate(services, currentGates)
	if err != nil {
		t.Fatal(err)
	}
	const (
		markers        = memcacheSummaryStart + "\nstale\n" + memcacheSummaryEnd
		phaseAggregate = "Terraform CI gates passed: 12/12; configured but unverified: 0/12."
	)
	for _, transition := range []struct {
		name   string
		gate   evidence.ServiceGate
		wants  []string
		forbid []string
	}{
		{
			name: "current immutable CI pass",
			gate: currentGate,
			wants: []string{
				"`local-passed`",
				"at immutable source commit `8e16d147b0127bd3120eae106aa0da1fb59a52c9`",
				"CI is `ci-passed`",
			},
			forbid: []string{"`local-passed-uncommitted`", "non-promotable", "`configured-unverified`"},
		},
		{
			name: "future immutable local pass",
			gate: futureImmutableMemcacheServiceGate(),
			wants: []string{
				"`local-passed`",
				"at immutable source commit `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`",
				"CI is `configured-unverified`",
			},
			forbid: []string{"`local-passed-uncommitted`", "non-promotable", "CI is `ci-passed`"},
		},
		{
			name: "future immutable CI pass",
			gate: futureMemcacheServiceGate(),
			wants: []string{
				"`local-passed`",
				"at immutable source commit `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`",
				"CI is `ci-passed`",
				"[GitHub Actions run 123456](https://github.com/rudydesplan/minisky/actions/runs/123456)",
				"`bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb`",
			},
			forbid: []string{"`local-passed-uncommitted`", "non-promotable", "`configured-unverified`"},
		},
	} {
		t.Run(transition.name, func(t *testing.T) {
			gate, err := selectMemcacheServiceGate(
				services,
				[]evidence.ServiceGate{transition.gate},
			)
			if err != nil {
				t.Fatal(err)
			}
			documents := map[string]string{
				"README":                  markers + "\n" + phaseAggregate,
				"service compatibility":   markers,
				"Terraform compatibility": markers,
			}
			for name, document := range documents {
				t.Run(name, func(t *testing.T) {
					rendered, err := replaceMemcacheServiceGate(document, gate)
					if err != nil {
						t.Fatal(err)
					}
					start := strings.Index(rendered, memcacheSummaryStart)
					end := strings.Index(rendered, memcacheSummaryEnd)
					if start < 0 || end <= start {
						t.Fatal("generated Memcached markers are missing or out of order")
					}
					generated := rendered[start:end]
					for _, want := range transition.wants {
						if !strings.Contains(generated, want) {
							t.Errorf("generated section missing %q:\n%s", want, generated)
						}
					}
					for _, forbidden := range transition.forbid {
						if strings.Contains(generated, forbidden) {
							t.Errorf("generated section contains forbidden %q:\n%s", forbidden, generated)
						}
					}
					if name == "README" && !strings.Contains(rendered, phaseAggregate) {
						t.Fatal("Memcached rendering changed the independent Phase 18-25 aggregate")
					}
				})
			}
		})
	}
}

func TestRenderMemcacheServiceGateFutureImmutableLocalPassUsesRevision(t *testing.T) {
	gate := futureImmutableMemcacheServiceGate()
	rendered, err := renderMemcacheServiceGate(gate)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"`local-passed`",
		"at immutable source commit `" + gate.SourceCommit + "`",
		"CI is `configured-unverified`",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("future immutable local rendering missing %q:\n%s", want, rendered)
		}
	}
	for _, forbidden := range []string{"`local-passed-uncommitted`", "non-promotable"} {
		if strings.Contains(rendered, forbidden) {
			t.Errorf("future immutable local rendering contains %q:\n%s", forbidden, rendered)
		}
	}
}

func TestMemcacheDocumentationClaimsStayConsistent(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	read := func(name string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	if err := validateMemcacheClaims(
		read("docs/service-compatibility.md"),
		read("docs/state-model.md"),
		read("docs/terraform-compatibility.md"),
	); err != nil {
		t.Fatal(err)
	}
	const (
		validService   = serviceCatalogStart + "\n| `memcache.googleapis.com` | standard | hybrid | implemented | standard | No |\n" + serviceCatalogEnd + "\n" + memcacheSummaryStart + "\n" + memcacheSummaryEnd
		validState     = "Memcached metadata is profile-persisted in owned Memcached containers"
		validTerraform = memcacheSummaryStart + "\n" + memcacheSummaryEnd + "\n`memcache_custom_endpoint` `google_memcache_instance`\n`effective_labels` is computed by the provider from API `labels`"
	)
	for _, test := range []struct {
		name      string
		service   string
		state     string
		terraform string
		wantError string
	}{
		{
			name:      "deferred service claim",
			service:   validService + "\nMemcached returns 501 UNIMPLEMENTED for all operations",
			state:     validState,
			terraform: validTerraform,
			wantError: "contradictory",
		},
		{
			name:      "duplicate generated row",
			service:   serviceCatalogStart + "\n| `memcache.googleapis.com` |\n| `memcache.googleapis.com` |\n" + serviceCatalogEnd,
			state:     validState,
			terraform: validTerraform,
			wantError: "exactly once",
		},
		{
			name:      "stale configured status",
			service:   validService,
			state:     validState,
			terraform: validTerraform + "\nMemcached remains configured but unverified",
			wantError: "stale Memcached status",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateMemcacheClaims(test.service, test.state, test.terraform)
			if err == nil {
				t.Fatal("contradictory Memcached documentation was accepted")
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error=%q want substring %q", err, test.wantError)
			}
		})
	}
}

func TestStoragePubSubBoundaryRenderingAndClaimsStayBounded(t *testing.T) {
	gates, err := evidence.EmulatorBoundaryGates()
	if err != nil {
		t.Fatal(err)
	}
	gate, err := selectStoragePubSubBoundaryGate(gates)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderStoragePubSubBoundary(gate)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"`local-passed-uncommitted`",
		"`make test-storage-persistence-pubsub-session`",
		"`scripts/storage-persistence-pubsub-session-integration.sh`",
		"`TestStoragePersistenceAndPubSubSessionBoundaries`",
		"source SHA-256: `" + gate.SourceSHA256 + "`",
		"diff SHA-256: `" + gate.DiffSHA256 + "`",
		"isolated anonymous Docker configuration",
		"immutable digest syntax",
		"`linux/amd64` platform execution",
		"advertised `--data-dir` capability",
		"Storage uses a profile-scoped runtime bind mount",
		"survive exact-owned Storage emulator-container replacement",
		"same exact-owned Pub/Sub container remains alive",
		"Replacing the Pub/Sub backend/container loses topics, subscriptions, and queued messages",
		"Graceful MiniSky shutdown tears down managed Docker resources",
		"outside metadata export/import",
		"does not claim exactly-once delivery, portable data export, IAM, HA, security, or full GCP parity",
		"assumes cooperative, exclusive use of the managed resource names",
		"Docker volume deletion accepts only a mutable name",
		"revalidates exact ownership and identity immediately before deletion and fails closed",
		"final inspect-to-delete interval cannot be excluded atomically",
		"not a hostile-daemon security boundary",
		"global unowned image cache may retain an authorized pull",
		"amd64/emulation/session-only",
		"Five unrelated local volumes and a pre-existing lock",
		"CI is `ci-passed`",
		"[GitHub Actions run 30431422780](" + gate.CI.RunURL + ")",
		"([job](" + gate.CI.JobURL + "))",
		"`" + gate.CI.Commit + "`",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("Storage/PubSub rendering is missing %q:\n%s", want, rendered)
		}
	}
	for _, forbidden := range []string{
		"Pub/Sub survives backend replacement",
		"Pub/Sub survives container replacement",
		"Storage is unmounted",
		"`configured-unverified`",
	} {
		if strings.Contains(rendered, forbidden) {
			t.Errorf("Storage/PubSub rendering contains false claim %q", forbidden)
		}
	}

	root := filepath.Clean(filepath.Join("..", ".."))
	read := func(name string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	service := read("docs/service-compatibility.md")
	state := read("docs/state-model.md")
	if err := validateStoragePubSubClaims(service, state); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		service string
		state   string
	}{
		{
			name:    "rejects PubSub container replacement survival",
			service: service + "\nPub/Sub survives container replacement.",
			state:   state,
		},
		{
			name:    "rejects unmounted Storage",
			service: service,
			state:   state + "\nStorage is unmounted.",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateStoragePubSubClaims(test.service, test.state); err == nil {
				t.Fatal("false emulator persistence claim was accepted")
			}
		})
	}
}

func TestPR22PromotionRenderingUsesExactRunAndJobProvenance(t *testing.T) {
	revision, err := evidence.CurrentPromotionRevision()
	if err != nil {
		t.Fatal(err)
	}
	gates, err := evidence.BatchGates()
	if err != nil {
		t.Fatal(err)
	}
	services, _, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	serviceGates, err := evidence.ServiceGates()
	if err != nil {
		t.Fatal(err)
	}
	memcache, err := selectMemcacheServiceGate(services, serviceGates)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderPromotionSummary(revision, gates, memcache)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		revision.Commit,
		"[general CI run 30416460163](" + revision.GeneralCI.RunURL + ")",
		"([job](" + revision.GeneralCI.JobURL + "))",
		"[critical reliability run 30416460134](" + revision.CriticalReliability.RunURL + ")",
		"([job](" + memcache.CI.JobURL + "))",
		"[the bounded promotion run 30416460053]",
		"All 12 Terraform jobs passed",
		"[binary-authorization job]",
		"All seven SDK/backend jobs passed",
		"[phase19 backend job]",
		"current working-tree promotion workflow does not retain a duplicate full-quality job",
		"authoritative quality checks remain in the separate general CI workflow",
		"`promotion-assets` builds and shares `ui/dist`",
		"PR #22's URLs do not verify those current workflow changes",
		"uncommitted Storage/Pub/Sub boundary gate is not attributed",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("promotion rendering is missing %q:\n%s", want, rendered)
		}
	}
}

func TestCloudSQLDocumentationClaimsStayCurrentAndBounded(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	readme := read("README.md")
	service := read("docs/service-compatibility.md")
	state := read("docs/state-model.md")
	terraform := read("docs/terraform-compatibility.md")
	services, _, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	gates, err := evidence.ServiceGates()
	if err != nil {
		t.Fatal(err)
	}
	gate, err := selectCloudSQLServiceGate(services, gates)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateCloudSQLClaims(readme, service, state, terraform, gate); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		readme    string
		service   string
		state     string
		terraform string
	}{
		{
			name:      "rejects container-only state",
			readme:    readme,
			service:   service,
			state:     state + "\nCloud SQL database files live only in containers.",
			terraform: terraform,
		},
		{
			name:      "rejects future named-volume wording",
			readme:    readme,
			service:   service,
			state:     state + "\nAdd profile-scoped named volumes for database data.",
			terraform: terraform,
		},
		{
			name:      "rejects pending status after stable pass",
			readme:    readme + "\nCloud SQL restart recovery is pending.",
			service:   service,
			state:     state,
			terraform: terraform,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateCloudSQLClaims(
				test.readme,
				test.service,
				test.state,
				test.terraform,
				gate,
			); err == nil {
				t.Fatal("stale Cloud SQL documentation was accepted")
			}
		})
	}
}

func TestStableCertificationRenderingPreventsStatusConflation(t *testing.T) {
	services, _, err := truth()
	if err != nil {
		t.Fatal(err)
	}
	serviceGates, err := evidence.ServiceGates()
	if err != nil {
		t.Fatal(err)
	}
	cloudSQL, err := selectCloudSQLServiceGate(services, serviceGates)
	if err != nil {
		t.Fatal(err)
	}
	emulatorGates, err := evidence.EmulatorBoundaryGates()
	if err != nil {
		t.Fatal(err)
	}
	storagePubSub, err := selectStoragePubSubBoundaryGate(emulatorGates)
	if err != nil {
		t.Fatal(err)
	}
	qualityGates, err := evidence.QualityGates()
	if err != nil {
		t.Fatal(err)
	}
	windows, err := selectWindowsStateMarkerGate(qualityGates)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := renderStableSnapshotCertification(cloudSQL, storagePubSub, windows)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"source SHA-256 `" + cloudSQL.SourceSHA256 + "`",
		"diff SHA-256 `" + cloudSQL.DiffSHA256 + "`",
		"Cloud SQL restart recovery is `local-passed-uncommitted`",
		"Storage/Pub/Sub is `local-passed-uncommitted`",
		"PR #23 exact-head commit `" + cloudSQL.CI.Commit + "`",
		"[critical run 30431422780](" + cloudSQL.CI.RunURL + ")",
		"([Cloud SQL job](" + cloudSQL.CI.JobURL + "))",
		"([Storage/Pub/Sub job](" + storagePubSub.CI.JobURL + "))",
		"Native `windows-state-markers` is `ci-passed`",
		"[general CI run 30431422742](" + windows.NativeCI.RunURL + ")",
		"([native job](" + windows.NativeCI.JobURL + "))",
		"authoritative `quality` aggregate is also `ci-passed`",
		"([quality job](" + windows.AuthoritativeQuality.JobURL + "))",
		"PR #22 URLs apply only to their exact historical commit",
	} {
		if !strings.Contains(rendered, required) {
			t.Errorf("stable certification rendering is missing %q:\n%s", required, rendered)
		}
	}
	for _, forbidden := range []string{
		"`configured-unverified`",
		"windows-state-markers` is `local-passed",
		"Cloud SQL restart recovery is pending",
		"PR #22 URLs verify PR #23",
	} {
		if strings.Contains(rendered, forbidden) {
			t.Errorf("stable certification rendering conflates status with %q", forbidden)
		}
	}
	if err := validateStableCertificationClaims(
		[]string{rendered},
		cloudSQL,
		storagePubSub,
		windows,
	); err != nil {
		t.Fatal(err)
	}
	if err := validateStableCertificationClaims(
		[]string{rendered + "\nPR #22 URLs verify PR #23."},
		cloudSQL,
		storagePubSub,
		windows,
	); err == nil {
		t.Fatal("PR #22/PR #23 provenance conflation was accepted")
	}
}

func TestPromotionWorkflowDocsSeparateHistoricalAndCurrentTruth(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	documents := []string{
		read("README.md"),
		read("docs/service-compatibility.md"),
		read("docs/terraform-compatibility.md"),
	}
	if err := validatePromotionWorkflowClaims(documents...); err != nil {
		t.Fatal(err)
	}
	documents[0] += "\nThe promotion workflow owns every gate while retaining the quality contracts."
	if err := validatePromotionWorkflowClaims(documents...); err == nil {
		t.Fatal("historical/current promotion provenance conflation was accepted")
	}
}

func TestBinaryAuthorizationTerraformDocsStayBounded(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	readme := strings.Join(strings.Fields(read("README.md")), " ")
	for _, want := range []string{
		"`google_binary_authorization_policy`",
		"observes whitelist allow and enforced deny before restart",
		"After restart, it proves policy persistence and no drift",
		"does not repeat those decisions",
		"matching import returns `0`",
		"stale import returns `2`",
		"Destroy resets the singleton to the exact default policy",
		"enforced `DENY` locally blocks MiniSky Cloud Deploy rollouts",
		"`DRYRUN_AUDIT_LOG_ONLY` permits rollout and returns `AUDIT`",
		"no durable audit record or log is created",
		"returns explicit `UNSUPPORTED`",
	} {
		if !strings.Contains(readme, want) {
			t.Errorf("README does not contain %q", want)
		}
	}

	terraformDocs := strings.Join(strings.Fields(read("docs/terraform-compatibility.md")), " ")
	for _, want := range []string{
		"`google_binary_authorization_policy` (optional Phase 25)",
		"Provider `7.41.0`",
		"matching import",
		"exit `0`",
		"stale import",
		"exit `2`",
		"exact default policy",
		"observes a whitelist allow plus an enforced default-rule deny before restart",
		"After restart, the gate proves policy persistence and zero drift",
		"does not repeat the allow/deny observations",
		"enforced `DENY` locally blocks MiniSky Cloud Deploy rollouts",
		"`DRYRUN_AUDIT_LOG_ONLY` permits rollout and returns `AUDIT`",
		"no durable audit record or log is created",
		"returns explicit `UNSUPPORTED`",
		"not service fidelity promotion, GKE admission, or production admission security",
		"Binary Authorization Terraform leg is `ci-passed`",
		"[binary-authorization job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962058)",
		"exact source revision `8e16d147b0127bd3120eae106aa0da1fb59a52c9`",
	} {
		if !strings.Contains(terraformDocs, want) {
			t.Errorf("Terraform documentation does not contain %q", want)
		}
	}

	serviceDocs := strings.Join(strings.Fields(read("docs/service-compatibility.md")), " ")
	for _, want := range []string{
		"Binary Authorization Terraform claim remains an independent Phase 24–25 bounded check",
		"binary-authorization job",
		"recorded promotion pass does not promote the service",
		"experimental/default-off support",
		"pre-restart allow/deny observation",
		"restart persistence/no-drift",
		"Cloud Deploy deny",
		"dry-run AUDIT permit",
		"without durable audit logging",
	} {
		if !strings.Contains(serviceDocs, want) {
			t.Errorf("Service compatibility documentation does not contain %q", want)
		}
	}

	golden := strings.Join(strings.Fields(read("cmd/docs-truth/testdata/service-catalog.golden.md")), " ")
	for _, want := range []string{
		"pre-restart allow/deny observation",
		"restart persistence/no-drift",
		"Cloud Deploy deny",
		"dry-run AUDIT permit",
		"explicit unsupported outcomes",
		"without durable audit logging",
	} {
		if !strings.Contains(golden, want) {
			t.Errorf("Service catalog golden does not contain %q", want)
		}
	}

	for name, document := range map[string]string{
		"README":                readme,
		"Terraform docs":        terraformDocs,
		"service compatibility": serviceDocs,
		"service golden":        golden,
	} {
		for _, stale := range []string{
			"records only the local advisory outcome",
			"allow/deny observations survive restart",
			"dry-run audit permits deployment and records",
			"The gate is recorded as `local-passed` only",
			"there is no claimed CI run URL or commit",
		} {
			if strings.Contains(document, stale) {
				t.Errorf("%s retains misleading wording %q", name, stale)
			}
		}
	}
}

func TestTerraformPromotionClaimsFollowMachineEvidence(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	data, err := os.ReadFile(filepath.Join(root, "docs", "terraform-compatibility.md"))
	if err != nil {
		t.Fatal(err)
	}
	gates, err := evidence.BatchGates()
	if err != nil {
		t.Fatal(err)
	}
	document := string(data)
	if err := validateTerraformPromotionClaims(document, gates); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		mutation func(string) string
	}{
		{
			name: "rejects local-only contradiction",
			mutation: func(value string) string {
				return strings.Replace(
					value,
					"The Binary Authorization Terraform leg is\n`ci-passed`",
					"The gate is recorded as `local-passed` only",
					1,
				)
			},
		},
		{
			name: "rejects mismatched job provenance",
			mutation: func(value string) string {
				return strings.Replace(
					value,
					"/job/90464962058",
					"/job/1",
					1,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateTerraformPromotionClaims(test.mutation(document), gates); err == nil {
				t.Fatal("contradictory Terraform promotion documentation was accepted")
			}
		})
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
