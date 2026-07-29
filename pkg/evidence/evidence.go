// Package evidence exposes machine-readable platform and service evidence.
package evidence

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

type GeneratedClientBoundary string

const (
	GeneratedClientCovered             GeneratedClientBoundary = "covered"
	GeneratedClientExplicitUnsupported GeneratedClientBoundary = "explicit-unsupported"
)

// PhaseService records how one default-off service is exercised without
// promoting it to an implemented compatibility claim.
type PhaseService struct {
	Domain                  string                  `json:"domain"`
	Selector                string                  `json:"selector"`
	Persistence             string                  `json:"persistence"`
	Package                 string                  `json:"package"`
	Tests                   []string                `json:"tests"`
	MethodNote              string                  `json:"methodNote"`
	BatchGate               string                  `json:"batchGate"`
	GeneratedClientBoundary GeneratedClientBoundary `json:"generatedClientBoundary"`
	TerraformClaim          bool                    `json:"terraformClaim"`
	IAMPath                 string                  `json:"iamPath,omitempty"`
	IAMMethod               string                  `json:"iamMethod,omitempty"`
	IAMBody                 string                  `json:"iamBody,omitempty"`
	IAMProject              string                  `json:"iamProject,omitempty"`
}

//go:embed phase18_25.json
var phase18To25JSON []byte

// AggregateGate records one cross-package, offline evidence gate.
type AggregateGate struct {
	Name       string   `json:"name"`
	Package    string   `json:"package"`
	Tests      []string `json:"tests"`
	MethodNote string   `json:"methodNote"`
}

//go:embed aggregate_gates.json
var aggregateGatesJSON []byte

type EvidenceStatus string

const (
	GitHubRepository                              = "rudydesplan/minisky"
	EvidenceLocalPassed            EvidenceStatus = "local-passed"
	EvidenceLocalPassedUncommitted EvidenceStatus = "local-passed-uncommitted"
	EvidenceCIPassed               EvidenceStatus = "ci-passed"
	EvidenceConfiguredUnverified   EvidenceStatus = "configured-unverified"
	EvidenceOptionalUnverified     EvidenceStatus = "optional-unverified"
	EvidenceNotApplicable          EvidenceStatus = "not-applicable"
	EvidenceAbsent                 EvidenceStatus = "absent"
)

type TestReference struct {
	Package string   `json:"package"`
	Tests   []string `json:"tests"`
}

// EvidenceCheck records one independently qualified batch-gate dimension.
// Configured checks are not pass claims until an actual local or GitHub run is
// recorded. CI passes identify the immutable source commit and durable run URL.
type EvidenceCheck struct {
	Status       EvidenceStatus  `json:"status"`
	References   []TestReference `json:"references,omitempty"`
	Script       string          `json:"script,omitempty"`
	MakeTarget   string          `json:"makeTarget,omitempty"`
	Workflow     string          `json:"workflow,omitempty"`
	Job          string          `json:"job,omitempty"`
	RunURL       string          `json:"runUrl,omitempty"`
	JobURL       string          `json:"jobUrl,omitempty"`
	Commit       string          `json:"commit,omitempty"`
	SourceCommit string          `json:"sourceCommit,omitempty"`
	SourceSHA256 string          `json:"sourceSha256,omitempty"`
	DiffSHA256   string          `json:"diffSha256,omitempty"`
	Note         string          `json:"note"`
}

// PlatformGate records one platform-level verification boundary. It remains
// distinct from BatchGate because platform diagnostics are not experimental
// service domains and do not participate in Phase 18-25 promotion.
type PlatformGate struct {
	ID    string        `json:"id"`
	Phase int           `json:"phase"`
	Name  string        `json:"name"`
	Check EvidenceCheck `json:"check"`
}

//go:embed platform_gates.json
var platformGatesJSON []byte

// QualityGate separates locally verified prerequisites from a native CI gate.
// In particular, cross-compilation and workflow contracts are not evidence that
// native-platform tests executed.
type QualityGate struct {
	ID                   string        `json:"id"`
	Name                 string        `json:"name"`
	RequiredBy           []string      `json:"requiredBy"`
	LocalPrerequisites   EvidenceCheck `json:"localPrerequisites"`
	NativeCI             EvidenceCheck `json:"nativeCI"`
	AuthoritativeQuality EvidenceCheck `json:"authoritativeQuality"`
}

//go:embed quality_gates.json
var qualityGatesJSON []byte

// ServiceGate records a bounded service lifecycle without adding the service
// to the default-off Phase 18-25 promotion inventory.
type GateAssertion struct {
	Path     string   `json:"path"`
	Contains []string `json:"contains"`
}

type ServiceGate struct {
	ID              string                     `json:"id"`
	Phase           int                        `json:"phase"`
	Name            string                     `json:"name"`
	Domain          string                     `json:"domain"`
	ProviderVersion string                     `json:"providerVersion"`
	Dimensions      []string                   `json:"dimensions"`
	Assertions      map[string][]GateAssertion `json:"assertions"`
	CI              EvidenceCheck              `json:"ci"`
	EvidenceCheck
}

//go:embed service_gates.json
var serviceGatesJSON []byte

// EmulatorBoundaryGate records a bounded lifecycle shared by passthrough
// emulators without implying that either service has a metadata adapter.
type EmulatorBoundaryGate struct {
	ID         string                     `json:"id"`
	Name       string                     `json:"name"`
	Domains    []string                   `json:"domains"`
	Dimensions []string                   `json:"dimensions"`
	Assertions map[string][]GateAssertion `json:"assertions"`
	CI         EvidenceCheck              `json:"ci"`
	EvidenceCheck
}

//go:embed emulator_boundaries.json
var emulatorBoundariesJSON []byte

// PromotionRevision records the non-promotion checks that must agree with the
// per-job promotion evidence before documentation can call one revision green.
type PromotionRevision struct {
	ID                  string        `json:"id"`
	PullRequestURL      string        `json:"pullRequestUrl"`
	Commit              string        `json:"commit"`
	GeneralCI           EvidenceCheck `json:"generalCI"`
	CriticalReliability EvidenceCheck `json:"criticalReliability"`
}

//go:embed promotion_revision.json
var promotionRevisionJSON []byte

// TerraformCheck records one provider lifecycle for exactly one service
// domain. Terraform evidence is intentionally not represented as a batch-wide
// check because each claimed domain has its own fixture and executable gate.
type TerraformCheck struct {
	Domain   string        `json:"domain"`
	MatrixID string        `json:"matrixId"`
	CI       EvidenceCheck `json:"ci"`
	EvidenceCheck
}

// BatchGate consolidates one generated-client integration batch without
// promoting its experimental domains or implying Terraform compatibility.
type BatchGate struct {
	ID                          string           `json:"id"`
	Domains                     []string         `json:"domains"`
	RelatedDomains              []string         `json:"relatedDomains,omitempty"`
	UnsupportedGeneratedDomains []string         `json:"unsupportedGeneratedDomains,omitempty"`
	PackageUnit                 EvidenceCheck    `json:"packageUnit"`
	GeneratedClientLifecycle    EvidenceCheck    `json:"generatedClientLifecycle"`
	DaemonRestart               EvidenceCheck    `json:"daemonRestart"`
	RealBackendDocker           EvidenceCheck    `json:"realBackendDocker"`
	StrictIAM                   EvidenceCheck    `json:"strictIAM"`
	AdmissionReplay             EvidenceCheck    `json:"admissionReplay,omitempty"`
	TerraformChecks             []TerraformCheck `json:"terraformChecks,omitempty"`
	Cleanup                     EvidenceCheck    `json:"cleanup"`
	CI                          EvidenceCheck    `json:"ci"`
	BackendCI                   EvidenceCheck    `json:"backendCI,omitempty"`
}

//go:embed batch_gates.json
var batchGatesJSON []byte

// Phase18To25 returns a copy of the checked-in evidence inventory.
func Phase18To25() ([]PhaseService, error) {
	var services []PhaseService
	if err := json.Unmarshal(phase18To25JSON, &services); err != nil {
		return nil, fmt.Errorf("decode Phase 18-25 evidence: %w", err)
	}
	gates, err := BatchGates()
	if err != nil {
		return nil, err
	}
	batchByDomain := make(map[string]string)
	explicitUnsupported := make(map[string]bool)
	for _, gate := range gates {
		for _, domain := range gate.Domains {
			batchByDomain[domain] = gate.ID
		}
		for _, domain := range gate.UnsupportedGeneratedDomains {
			explicitUnsupported[domain] = true
		}
	}
	for index := range services {
		services[index].BatchGate = batchByDomain[services[index].Domain]
		services[index].GeneratedClientBoundary = GeneratedClientCovered
		if explicitUnsupported[services[index].Domain] {
			services[index].GeneratedClientBoundary = GeneratedClientExplicitUnsupported
		}
	}
	return services, nil
}

// AggregateGates returns a copy of the checked-in cross-package gate inventory.
func AggregateGates() ([]AggregateGate, error) {
	var gates []AggregateGate
	if err := json.Unmarshal(aggregateGatesJSON, &gates); err != nil {
		return nil, fmt.Errorf("decode aggregate evidence gates: %w", err)
	}
	return gates, nil
}

// BatchGates returns the checked-in Phase 18-25 batch-gate matrix.
func BatchGates() ([]BatchGate, error) {
	var gates []BatchGate
	if err := json.Unmarshal(batchGatesJSON, &gates); err != nil {
		return nil, fmt.Errorf("decode Phase 18-25 batch gates: %w", err)
	}
	for _, gate := range gates {
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
		if gate.AdmissionReplay.Status != "" {
			checks["admissionReplay"] = gate.AdmissionReplay
		}
		for name, check := range checks {
			if err := ValidateEvidenceCheck(check); err != nil {
				return nil, fmt.Errorf("%s %s: %w", gate.ID, name, err)
			}
		}
		for _, check := range gate.TerraformChecks {
			if err := ValidateEvidenceCheck(check.EvidenceCheck); err != nil {
				return nil, fmt.Errorf("%s Terraform check for %s: %w", gate.ID, check.Domain, err)
			}
			if check.MatrixID == "" {
				return nil, fmt.Errorf("%s Terraform check for %s: matrix ID is required", gate.ID, check.Domain)
			}
			if err := ValidateEvidenceCheck(check.CI); err != nil {
				return nil, fmt.Errorf("%s Terraform CI check for %s: %w", gate.ID, check.Domain, err)
			}
		}
	}
	return gates, nil
}

// PlatformGates returns the checked-in platform verification inventory.
func PlatformGates() ([]PlatformGate, error) {
	var gates []PlatformGate
	if err := json.Unmarshal(platformGatesJSON, &gates); err != nil {
		return nil, fmt.Errorf("decode platform gates: %w", err)
	}
	for _, gate := range gates {
		if gate.ID == "" || gate.Phase == 0 || gate.Name == "" {
			return nil, fmt.Errorf("platform gate has incomplete identity: %+v", gate)
		}
		if err := ValidateEvidenceCheck(gate.Check); err != nil {
			return nil, fmt.Errorf("%s: %w", gate.ID, err)
		}
	}
	return gates, nil
}

// QualityGates returns native quality requirements without converting local
// cross-compilation or workflow-contract checks into native execution claims.
func QualityGates() ([]QualityGate, error) {
	var gates []QualityGate
	if err := json.Unmarshal(qualityGatesJSON, &gates); err != nil {
		return nil, fmt.Errorf("decode quality gates: %w", err)
	}
	seen := make(map[string]bool, len(gates))
	for _, gate := range gates {
		if gate.ID == "" || gate.Name == "" || len(gate.RequiredBy) == 0 {
			return nil, fmt.Errorf("quality gate has incomplete identity: %+v", gate)
		}
		if seen[gate.ID] {
			return nil, fmt.Errorf("duplicate quality gate %q", gate.ID)
		}
		seen[gate.ID] = true
		if err := ValidateEvidenceCheck(gate.LocalPrerequisites); err != nil {
			return nil, fmt.Errorf("%s local prerequisites: %w", gate.ID, err)
		}
		if gate.LocalPrerequisites.Status != EvidenceLocalPassedUncommitted {
			return nil, fmt.Errorf("%s local prerequisites must be local-passed-uncommitted", gate.ID)
		}
		if err := ValidateEvidenceCheck(gate.NativeCI); err != nil {
			return nil, fmt.Errorf("%s native CI: %w", gate.ID, err)
		}
		if gate.NativeCI.Status != EvidenceCIPassed ||
			gate.NativeCI.Workflow == "" || gate.NativeCI.Job == "" ||
			gate.NativeCI.JobURL == "" {
			return nil, fmt.Errorf("%s native CI requires an immutable job-linked pass", gate.ID)
		}
		if err := ValidateEvidenceCheck(gate.AuthoritativeQuality); err != nil {
			return nil, fmt.Errorf("%s authoritative quality: %w", gate.ID, err)
		}
		if gate.AuthoritativeQuality.Status != EvidenceCIPassed ||
			gate.AuthoritativeQuality.Workflow == "" ||
			gate.AuthoritativeQuality.Job == "" ||
			gate.AuthoritativeQuality.JobURL == "" {
			return nil, fmt.Errorf("%s authoritative quality requires an immutable job-linked pass", gate.ID)
		}
		if gate.AuthoritativeQuality.RunURL != gate.NativeCI.RunURL ||
			gate.AuthoritativeQuality.Commit != gate.NativeCI.Commit {
			return nil, fmt.Errorf("%s native and authoritative quality passes must share a run and commit", gate.ID)
		}
	}
	return gates, nil
}

// ServiceGates returns bounded service evidence that is independent of the
// experimental Phase 18-25 promotion matrix.
func ServiceGates() ([]ServiceGate, error) {
	return loadServiceGates(serviceGatesJSON)
}

// EmulatorBoundaryGates returns bounded passthrough-emulator lifecycle truth.
func EmulatorBoundaryGates() ([]EmulatorBoundaryGate, error) {
	var gates []EmulatorBoundaryGate
	if err := json.Unmarshal(emulatorBoundariesJSON, &gates); err != nil {
		return nil, fmt.Errorf("decode emulator boundary gates: %w", err)
	}
	seen := make(map[string]bool, len(gates))
	for _, gate := range gates {
		if gate.ID == "" || gate.Name == "" || len(gate.Domains) < 2 {
			return nil, fmt.Errorf("emulator boundary gate has incomplete identity: %+v", gate)
		}
		if seen[gate.ID] {
			return nil, fmt.Errorf("duplicate emulator boundary gate %q", gate.ID)
		}
		seen[gate.ID] = true
		domains := make(map[string]bool, len(gate.Domains))
		for _, domain := range gate.Domains {
			if domain == "" || domains[domain] {
				return nil, fmt.Errorf("%s has an empty or duplicate domain %q", gate.ID, domain)
			}
			domains[domain] = true
		}
		dimensions := make(map[string]bool, len(gate.Dimensions))
		for _, dimension := range gate.Dimensions {
			if dimension == "" || dimensions[dimension] {
				return nil, fmt.Errorf("%s has an empty or duplicate lifecycle dimension %q", gate.ID, dimension)
			}
			dimensions[dimension] = true
		}
		if len(dimensions) == 0 || len(gate.Assertions) != len(dimensions) {
			return nil, fmt.Errorf("%s assertion dimensions do not exactly match lifecycle dimensions", gate.ID)
		}
		for dimension, assertions := range gate.Assertions {
			if !dimensions[dimension] || len(assertions) == 0 {
				return nil, fmt.Errorf("%s has invalid assertions for dimension %q", gate.ID, dimension)
			}
			for _, assertion := range assertions {
				if assertion.Path == "" || len(assertion.Contains) == 0 {
					return nil, fmt.Errorf("%s dimension %q has an incomplete gate assertion", gate.ID, dimension)
				}
				for _, fragment := range assertion.Contains {
					if fragment == "" {
						return nil, fmt.Errorf("%s dimension %q has an empty assertion fragment", gate.ID, dimension)
					}
				}
			}
		}
		if err := ValidateEvidenceCheck(gate.EvidenceCheck); err != nil {
			return nil, fmt.Errorf("%s local evidence: %w", gate.ID, err)
		}
		if gate.Status != EvidenceLocalPassedUncommitted || gate.SourceCommit != "" ||
			gate.SourceSHA256 == "" || gate.DiffSHA256 == "" {
			return nil, fmt.Errorf("%s must remain local-passed-uncommitted with stable source/diff fingerprints and no commit", gate.ID)
		}
		if gate.Script == "" || gate.MakeTarget == "" || len(gate.References) == 0 {
			return nil, fmt.Errorf("%s local evidence requires script, Make target, and references", gate.ID)
		}
		if err := ValidateEvidenceCheck(gate.CI); err != nil {
			return nil, fmt.Errorf("%s CI evidence: %w", gate.ID, err)
		}
		if gate.CI.Status != EvidenceCIPassed ||
			gate.CI.Workflow == "" || gate.CI.Job == "" ||
			gate.CI.JobURL == "" {
			return nil, fmt.Errorf("%s CI requires an immutable job-linked pass", gate.ID)
		}
	}
	return gates, nil
}

// CurrentPromotionRevision returns the immutable companion checks for the
// externally verified promotion revision.
func CurrentPromotionRevision() (PromotionRevision, error) {
	var revision PromotionRevision
	if err := json.Unmarshal(promotionRevisionJSON, &revision); err != nil {
		return PromotionRevision{}, fmt.Errorf("decode promotion revision: %w", err)
	}
	if revision.ID == "" || !fullCommitPattern.MatchString(revision.Commit) {
		return PromotionRevision{}, fmt.Errorf("promotion revision has incomplete identity")
	}
	prURL, err := url.Parse(revision.PullRequestURL)
	if err != nil || prURL.Scheme != "https" || prURL.Host != "github.com" ||
		prURL.Path != "/"+GitHubRepository+"/pull/22" || prURL.RawQuery != "" ||
		prURL.Fragment != "" || prURL.User != nil {
		return PromotionRevision{}, fmt.Errorf("promotion revision requires the immutable PR #22 URL")
	}
	for name, check := range map[string]EvidenceCheck{
		"general CI":           revision.GeneralCI,
		"critical reliability": revision.CriticalReliability,
	} {
		if err := ValidateEvidenceCheck(check); err != nil {
			return PromotionRevision{}, fmt.Errorf("promotion revision %s: %w", name, err)
		}
		if check.Status != EvidenceCIPassed || check.Commit != revision.Commit ||
			check.JobURL == "" {
			return PromotionRevision{}, fmt.Errorf(
				"promotion revision %s must be a job-linked CI pass on %s",
				name,
				revision.Commit,
			)
		}
	}
	if revision.GeneralCI.Workflow != ".github/workflows/ci.yml" ||
		revision.GeneralCI.Job != "quality" {
		return PromotionRevision{}, fmt.Errorf(
			"promotion revision general CI must identify workflow job quality",
		)
	}
	if revision.CriticalReliability.Workflow != ".github/workflows/critical-integration.yml" ||
		revision.CriticalReliability.Job != "state-durability" {
		return PromotionRevision{}, fmt.Errorf(
			"promotion revision critical reliability must identify workflow job state-durability",
		)
	}
	return revision, nil
}

func loadServiceGates(data []byte) ([]ServiceGate, error) {
	var gates []ServiceGate
	if err := json.Unmarshal(data, &gates); err != nil {
		return nil, fmt.Errorf("decode service gates: %w", err)
	}
	seen := make(map[string]bool, len(gates))
	for _, gate := range gates {
		if gate.ID == "" || gate.Phase == 0 || gate.Name == "" ||
			gate.Domain == "" || gate.ProviderVersion == "" {
			return nil, fmt.Errorf("service gate has incomplete identity: %+v", gate)
		}
		if seen[gate.ID] {
			return nil, fmt.Errorf("duplicate service gate %q", gate.ID)
		}
		seen[gate.ID] = true
		dimensions := make(map[string]bool, len(gate.Dimensions))
		for _, dimension := range gate.Dimensions {
			if dimension == "" {
				return nil, fmt.Errorf("%s has an empty lifecycle dimension", gate.ID)
			}
			if dimensions[dimension] {
				return nil, fmt.Errorf("%s duplicates lifecycle dimension %q", gate.ID, dimension)
			}
			dimensions[dimension] = true
		}
		if len(dimensions) == 0 {
			return nil, fmt.Errorf("%s has no lifecycle dimensions", gate.ID)
		}
		if len(gate.Assertions) != len(dimensions) {
			return nil, fmt.Errorf("%s assertion dimensions do not exactly match lifecycle dimensions", gate.ID)
		}
		for dimension, assertions := range gate.Assertions {
			if !dimensions[dimension] {
				return nil, fmt.Errorf("%s assertion names unknown dimension %q", gate.ID, dimension)
			}
			if len(assertions) == 0 {
				return nil, fmt.Errorf("%s dimension %q has no gate assertions", gate.ID, dimension)
			}
			for _, assertion := range assertions {
				if assertion.Path == "" || len(assertion.Contains) == 0 {
					return nil, fmt.Errorf("%s dimension %q has an incomplete gate assertion", gate.ID, dimension)
				}
				for _, fragment := range assertion.Contains {
					if fragment == "" {
						return nil, fmt.Errorf("%s dimension %q has an empty assertion fragment", gate.ID, dimension)
					}
				}
			}
		}
		if err := ValidateEvidenceCheck(gate.EvidenceCheck); err != nil {
			return nil, fmt.Errorf("%s local evidence: %w", gate.ID, err)
		}
		switch gate.Status {
		case EvidenceLocalPassed:
			if gate.SourceCommit == "" || !fullCommitPattern.MatchString(gate.SourceCommit) {
				return nil, fmt.Errorf("%s immutable local evidence requires a full source commit", gate.ID)
			}
		case EvidenceLocalPassedUncommitted:
			if gate.SourceCommit != "" || gate.SourceSHA256 == "" || gate.DiffSHA256 == "" {
				return nil, fmt.Errorf("%s uncommitted local evidence requires stable source/diff fingerprints and no source commit", gate.ID)
			}
		default:
			return nil, fmt.Errorf(
				"%s local evidence status must be %q or %q",
				gate.ID,
				EvidenceLocalPassed,
				EvidenceLocalPassedUncommitted,
			)
		}
		if gate.Script == "" || gate.MakeTarget == "" || len(gate.References) == 0 {
			return nil, fmt.Errorf("%s local evidence requires script, Make target, and references", gate.ID)
		}
		for _, reference := range gate.References {
			if reference.Package == "" || len(reference.Tests) == 0 {
				return nil, fmt.Errorf("%s local evidence has an incomplete test reference", gate.ID)
			}
			for _, test := range reference.Tests {
				if test == "" {
					return nil, fmt.Errorf("%s local evidence has an empty test reference", gate.ID)
				}
			}
		}
		if !providerVersionPattern.MatchString(gate.ProviderVersion) {
			return nil, fmt.Errorf("%s provider version %q is not exact", gate.ID, gate.ProviderVersion)
		}
		if err := ValidateEvidenceCheck(gate.CI); err != nil {
			return nil, fmt.Errorf("%s CI evidence: %w", gate.ID, err)
		}
		if gate.CI.Status != EvidenceConfiguredUnverified && gate.CI.Status != EvidenceCIPassed {
			return nil, fmt.Errorf("%s CI evidence status must be configured-unverified or ci-passed", gate.ID)
		}
		if gate.CI.Workflow == "" || gate.CI.Job == "" {
			return nil, fmt.Errorf("%s CI evidence requires workflow and job", gate.ID)
		}
		if len(gate.CI.References) != 0 || gate.CI.Script != "" || gate.CI.MakeTarget != "" {
			return nil, fmt.Errorf("%s CI evidence must not duplicate local execution references", gate.ID)
		}
	}
	return gates, nil
}

var (
	fullCommitPattern      = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256Pattern          = regexp.MustCompile(`^[0-9a-f]{64}$`)
	providerVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	actionsRunPattern      = regexp.MustCompile(
		`^/` + regexp.QuoteMeta(GitHubRepository) + `/actions/runs/[0-9]+$`,
	)
	actionsJobPattern = regexp.MustCompile(
		`^/` + regexp.QuoteMeta(GitHubRepository) + `/actions/runs/[0-9]+/job/[0-9]+$`,
	)
)

// ValidateEvidenceCheck prevents configured or local evidence from being
// presented as an immutable CI pass and rejects incomplete CI pass claims.
func ValidateEvidenceCheck(check EvidenceCheck) error {
	if check.Status == "" || check.Note == "" {
		return fmt.Errorf("status and note are required")
	}
	if check.SourceCommit != "" && !fullCommitPattern.MatchString(check.SourceCommit) {
		return fmt.Errorf("source commit requires a lowercase 40-hex commit")
	}
	if (check.SourceSHA256 == "") != (check.DiffSHA256 == "") {
		return fmt.Errorf("stable local source and diff SHA-256 fingerprints must be recorded together")
	}
	if check.SourceSHA256 != "" &&
		(!sha256Pattern.MatchString(check.SourceSHA256) || !sha256Pattern.MatchString(check.DiffSHA256)) {
		return fmt.Errorf("stable local source and diff fingerprints require lowercase 64-hex SHA-256 values")
	}
	if check.SourceSHA256 != "" && check.Status != EvidenceLocalPassedUncommitted {
		return fmt.Errorf("%s must not include stable uncommitted source fingerprints", check.Status)
	}
	switch check.Status {
	case EvidenceCIPassed:
		if check.SourceCommit != "" {
			return fmt.Errorf("ci-passed uses commit, not sourceCommit")
		}
		runURL, err := url.Parse(check.RunURL)
		if err != nil || runURL.Scheme != "https" || runURL.Host != "github.com" ||
			!actionsRunPattern.MatchString(runURL.Path) || runURL.RawQuery != "" ||
			runURL.Fragment != "" || runURL.User != nil {
			return fmt.Errorf("ci-passed requires a %s GitHub Actions run URL", GitHubRepository)
		}
		if !fullCommitPattern.MatchString(check.Commit) {
			return fmt.Errorf("ci-passed requires a lowercase 40-hex commit")
		}
		if check.JobURL != "" {
			jobURL, err := url.Parse(check.JobURL)
			if err != nil || jobURL.Scheme != "https" || jobURL.Host != "github.com" ||
				!actionsJobPattern.MatchString(jobURL.Path) || jobURL.RawQuery != "" ||
				jobURL.Fragment != "" || jobURL.User != nil {
				return fmt.Errorf("ci-passed job URL must identify a %s GitHub Actions job", GitHubRepository)
			}
			if !strings.HasPrefix(jobURL.Path, runURL.Path+"/job/") {
				return fmt.Errorf("ci-passed job URL must belong to its recorded run")
			}
		}
	case EvidenceLocalPassed, EvidenceLocalPassedUncommitted, EvidenceConfiguredUnverified, EvidenceOptionalUnverified,
		EvidenceNotApplicable, EvidenceAbsent:
		if check.RunURL != "" || check.JobURL != "" || check.Commit != "" {
			return fmt.Errorf("%s must not include CI run, job, or commit fields", check.Status)
		}
		if check.SourceCommit != "" && check.Status != EvidenceLocalPassed {
			return fmt.Errorf("%s must not include a local source commit", check.Status)
		}
	default:
		return fmt.Errorf("unknown evidence status %q", check.Status)
	}
	return nil
}
