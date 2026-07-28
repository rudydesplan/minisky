// Package evidence exposes machine-readable platform and service evidence.
package evidence

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
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
	Commit       string          `json:"commit,omitempty"`
	SourceCommit string          `json:"sourceCommit,omitempty"`
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

// ServiceGates returns bounded service evidence that is independent of the
// experimental Phase 18-25 promotion matrix.
func ServiceGates() ([]ServiceGate, error) {
	return loadServiceGates(serviceGatesJSON)
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
			if gate.SourceCommit != "" {
				return nil, fmt.Errorf("%s uncommitted local evidence must not include a source commit", gate.ID)
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
	providerVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	actionsRunPattern      = regexp.MustCompile(
		`^/` + regexp.QuoteMeta(GitHubRepository) + `/actions/runs/[0-9]+$`,
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
	case EvidenceLocalPassed, EvidenceLocalPassedUncommitted, EvidenceConfiguredUnverified, EvidenceOptionalUnverified,
		EvidenceNotApplicable, EvidenceAbsent:
		if check.RunURL != "" || check.Commit != "" {
			return fmt.Errorf("%s must not include CI run or commit fields", check.Status)
		}
		if check.SourceCommit != "" && check.Status != EvidenceLocalPassed {
			return fmt.Errorf("%s must not include a local source commit", check.Status)
		}
	default:
		return fmt.Errorf("unknown evidence status %q", check.Status)
	}
	return nil
}
