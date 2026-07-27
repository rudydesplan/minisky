// Package evidence exposes the executable Phase 18-25 evidence inventory.
package evidence

import (
	_ "embed"
	"encoding/json"
	"fmt"
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
	EvidenceLocalPassed          EvidenceStatus = "local-passed"
	EvidenceCIPassed             EvidenceStatus = "ci-passed"
	EvidenceConfiguredUnverified EvidenceStatus = "configured-unverified"
	EvidenceOptionalUnverified   EvidenceStatus = "optional-unverified"
	EvidenceNotApplicable        EvidenceStatus = "not-applicable"
	EvidenceAbsent               EvidenceStatus = "absent"
)

type TestReference struct {
	Package string   `json:"package"`
	Tests   []string `json:"tests"`
}

// EvidenceCheck records one independently qualified batch-gate dimension.
// Configured checks are not pass claims until an actual local or GitHub run is
// recorded. CI passes identify the immutable source commit and durable run URL.
type EvidenceCheck struct {
	Status     EvidenceStatus  `json:"status"`
	References []TestReference `json:"references,omitempty"`
	Script     string          `json:"script,omitempty"`
	MakeTarget string          `json:"makeTarget,omitempty"`
	Workflow   string          `json:"workflow,omitempty"`
	Job        string          `json:"job,omitempty"`
	RunURL     string          `json:"runUrl,omitempty"`
	Commit     string          `json:"commit,omitempty"`
	Note       string          `json:"note"`
}

// BatchGate consolidates one generated-client integration batch without
// promoting its experimental domains or implying Terraform compatibility.
type BatchGate struct {
	ID                          string        `json:"id"`
	Domains                     []string      `json:"domains"`
	RelatedDomains              []string      `json:"relatedDomains,omitempty"`
	UnsupportedGeneratedDomains []string      `json:"unsupportedGeneratedDomains,omitempty"`
	PackageUnit                 EvidenceCheck `json:"packageUnit"`
	GeneratedClientLifecycle    EvidenceCheck `json:"generatedClientLifecycle"`
	DaemonRestart               EvidenceCheck `json:"daemonRestart"`
	RealBackendDocker           EvidenceCheck `json:"realBackendDocker"`
	StrictIAM                   EvidenceCheck `json:"strictIAM"`
	Terraform                   EvidenceCheck `json:"terraform"`
	Cleanup                     EvidenceCheck `json:"cleanup"`
	CI                          EvidenceCheck `json:"ci"`
	BackendCI                   EvidenceCheck `json:"backendCI,omitempty"`
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
	return gates, nil
}
