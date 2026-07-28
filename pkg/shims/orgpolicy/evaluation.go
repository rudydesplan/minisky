package orgpolicy

import (
	"fmt"
	"strings"
)

// Evaluation is a bounded boolean-constraint decision. Source identifies the
// nearest policy used, or constraintDefault when no ancestor policy exists.
type Evaluation struct {
	Enforced bool   `json:"enforced"`
	Source   string `json:"source"`
}

// Evaluate resolves a boolean constraint through an explicitly supplied,
// root-to-leaf ancestor chain. MiniSky does not infer organization hierarchy.
func (api *API) Evaluate(resource, constraint string, ancestors []string) (Evaluation, error) {
	if !validHierarchy(resource, ancestors) {
		return Evaluation{}, fmt.Errorf("invalid root-to-leaf resource hierarchy")
	}
	if !strings.HasPrefix(constraint, "constraints/") {
		return Evaluation{}, fmt.Errorf("invalid constraint name")
	}

	api.mu.RLock()
	defer api.mu.RUnlock()
	definition, known := api.constraints[constraint]
	if !known {
		return Evaluation{}, fmt.Errorf("constraint not found")
	}
	policyID := strings.TrimPrefix(constraint, "constraints/")
	name := resource + "/policies/" + policyID
	if policy := api.policies[name]; policy != nil && policy.Spec != nil && len(policy.Spec.Rules) > 0 {
		return Evaluation{Enforced: policy.Spec.Rules[0].Enforce, Source: name}, nil
	}
	for i := len(ancestors) - 1; i >= 0; i-- {
		name = ancestors[i] + "/policies/" + policyID
		if policy := api.policies[name]; policy != nil && policy.Spec != nil && len(policy.Spec.Rules) > 0 {
			return Evaluation{Enforced: policy.Spec.Rules[0].Enforce, Source: name}, nil
		}
	}
	return Evaluation{
		Enforced: definition.ConstraintDefault == "DENY",
		Source:   "constraintDefault",
	}, nil
}

func validHierarchy(resource string, ancestors []string) bool {
	if !strings.HasPrefix(resource, "projects/") || strings.Count(resource, "/") != 1 {
		return false
	}
	stage := 0
	for _, ancestor := range ancestors {
		switch {
		case strings.HasPrefix(ancestor, "organizations/") && strings.Count(ancestor, "/") == 1:
			if stage != 0 {
				return false
			}
			stage = 1
		case strings.HasPrefix(ancestor, "folders/") && strings.Count(ancestor, "/") == 1:
			if stage == 0 {
				return false
			}
			stage = 2
		default:
			return false
		}
	}
	return true
}
