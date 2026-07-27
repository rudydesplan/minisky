package networksecurity

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

func TestAuthorizationPolicyEvaluationFailsClosedForInvalidScope(t *testing.T) {
	for _, request := range []EvaluationRequest{
		{Project: "", Location: "global"},
		{Project: "../p", Location: "global"},
		{Project: "p", Location: ""},
		{Project: "p", Location: "../global"},
	} {
		decision := newTestAPI().Evaluate(request)
		if decision.Allowed || decision.Reason != "invalid evaluation scope" {
			t.Fatalf("request = %#v, decision = %#v", request, decision)
		}
	}
}

func TestAuthorizationPolicyRequestUsesStoredDenyRule(t *testing.T) {
	api := newTestAPI()
	api.policies["projects/p/locations/global/authorizationPolicies/deny-admin"] = &AuthorizationPolicy{
		Name:   "projects/p/locations/global/authorizationPolicies/deny-admin",
		Action: "DENY",
		Rules: []Rule{{
			Destinations: []Destination{{Hosts: []string{"api.local"}, Paths: []string{"/admin"}}},
		}},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/p/locations/global/authorizationPolicies:evaluate",
		strings.NewReader(`{"project":"p","location":"global","host":"api.local","path":"/admin"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var decision EvaluationDecision
	if err := json.Unmarshal(response.Body.Bytes(), &decision); err != nil {
		t.Fatal(err)
	}
	if decision.Allowed || decision.Policy == "" {
		t.Fatalf("decision = %#v", decision)
	}
}
