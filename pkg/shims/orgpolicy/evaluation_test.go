package orgpolicy

import "testing"

func TestEvaluatePolicyUsesNearestAncestor(t *testing.T) {
	api := newTestAPI()
	const constraint = "constraints/compute.requireOsLogin"
	api.policies["organizations/123/policies/compute.requireOsLogin"] = &Policy{
		Name: "organizations/123/policies/compute.requireOsLogin",
		Spec: &PolicySpec{Rules: []PolicyRule{{Enforce: true}}},
	}
	api.policies["folders/456/policies/compute.requireOsLogin"] = &Policy{
		Name: "folders/456/policies/compute.requireOsLogin",
		Spec: &PolicySpec{Rules: []PolicyRule{{Enforce: false}}},
	}

	decision, err := api.Evaluate("projects/demo", constraint, []string{"organizations/123", "folders/456"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Enforced || decision.Source != "folders/456/policies/compute.requireOsLogin" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestEvaluatePolicyFallsBackToConstraintDefault(t *testing.T) {
	api := newTestAPI()
	decision, err := api.Evaluate("projects/demo", "constraints/compute.requireOsLogin", []string{"organizations/123"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Enforced || decision.Source != "constraintDefault" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestEvaluatePolicyRejectsUnorderedHierarchy(t *testing.T) {
	api := newTestAPI()
	if _, err := api.Evaluate("projects/demo", "constraints/compute.requireOsLogin", []string{"folders/456", "organizations/123"}); err == nil {
		t.Fatal("expected hierarchy validation error")
	}
}
