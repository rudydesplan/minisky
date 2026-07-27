package networksecurity

import "testing"

func TestAuthorizationPolicyEvaluationIsExplicitlyMetadataOnly(t *testing.T) {
	api := newTestAPI()
	api.policies["projects/p/locations/global/authorizationPolicies/deny-admin"] = &AuthorizationPolicy{
		Name:   "projects/p/locations/global/authorizationPolicies/deny-admin",
		Action: "DENY",
		Rules: []Rule{{
			Sources:      []Source{{Principals: []string{"user:alice@example.com"}}},
			Destinations: []Destination{{Hosts: []string{"api.local"}, Methods: []string{"GET"}, Paths: []string{"/admin"}}},
		}},
	}

	decision := api.Evaluate(EvaluationRequest{
		Project:   "p",
		Location:  "global",
		Principal: "user:alice@example.com",
		Host:      "api.local",
		Method:    "GET",
		Path:      "/admin",
	})
	if decision.Allowed || decision.Enforcement != "METADATA_ONLY" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestAuthorizationPolicyEvaluationDefaultsAllow(t *testing.T) {
	decision := newTestAPI().Evaluate(EvaluationRequest{Project: "p", Location: "global"})
	if !decision.Allowed || decision.Enforcement != "METADATA_ONLY" {
		t.Fatalf("decision = %#v", decision)
	}
}
