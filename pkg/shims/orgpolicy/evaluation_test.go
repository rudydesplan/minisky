package orgpolicy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestEvaluatePolicyUsesResourceBeforeAncestors(t *testing.T) {
	api := newTestAPI()
	const constraint = "constraints/compute.requireOsLogin"
	api.policies["organizations/123/policies/compute.requireOsLogin"] = &Policy{
		Name: "organizations/123/policies/compute.requireOsLogin",
		Spec: &PolicySpec{Rules: []PolicyRule{{Enforce: true}}},
	}
	api.policies["projects/demo/policies/compute.requireOsLogin"] = &Policy{
		Name: "projects/demo/policies/compute.requireOsLogin",
		Spec: &PolicySpec{Rules: []PolicyRule{{Enforce: false}}},
	}

	decision, err := api.Evaluate("projects/demo", constraint, []string{"organizations/123"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Enforced || decision.Source != "projects/demo/policies/compute.requireOsLogin" {
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

func TestEvaluatePolicyRequestUsesHierarchy(t *testing.T) {
	api := newTestAPI()
	api.policies["folders/456/policies/compute.requireOsLogin"] = &Policy{
		Name: "folders/456/policies/compute.requireOsLogin",
		Spec: &PolicySpec{Rules: []PolicyRule{{Enforce: true}}},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v2/policies:evaluate",
		strings.NewReader(`{
			"resource":"projects/demo",
			"constraint":"constraints/compute.requireOsLogin",
			"ancestors":["organizations/123","folders/456"]
		}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var decision Evaluation
	if err := json.Unmarshal(response.Body.Bytes(), &decision); err != nil {
		t.Fatal(err)
	}
	if !decision.Enforced || decision.Source != "folders/456/policies/compute.requireOsLogin" {
		t.Fatalf("decision = %#v", decision)
	}
}
