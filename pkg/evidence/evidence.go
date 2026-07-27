// Package evidence exposes the executable Phase 18-25 evidence inventory.
package evidence

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

// PhaseService records how one default-off service is exercised without
// promoting it to an implemented compatibility claim.
type PhaseService struct {
	Domain         string   `json:"domain"`
	Selector       string   `json:"selector"`
	Persistence    string   `json:"persistence"`
	Package        string   `json:"package"`
	Tests          []string `json:"tests"`
	MethodNote     string   `json:"methodNote"`
	TerraformClaim bool     `json:"terraformClaim"`
	IAMPath        string   `json:"iamPath,omitempty"`
	IAMMethod      string   `json:"iamMethod,omitempty"`
	IAMBody        string   `json:"iamBody,omitempty"`
	IAMProject     string   `json:"iamProject,omitempty"`
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

// Phase18To25 returns a copy of the checked-in evidence inventory.
func Phase18To25() ([]PhaseService, error) {
	var services []PhaseService
	if err := json.Unmarshal(phase18To25JSON, &services); err != nil {
		return nil, fmt.Errorf("decode Phase 18-25 evidence: %w", err)
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
