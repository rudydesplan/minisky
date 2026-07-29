package binaryauthorization

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	generatedbinauthz "google.golang.org/api/binaryauthorization/v1"
	"google.golang.org/api/option"

	"minisky/pkg/state"
)

const enforcedMode = "ENFORCED_BLOCK_AND_AUDIT_LOG"

type memoryStore struct {
	mu      sync.Mutex
	payload []byte
	saveErr error
}

type blockingStore struct {
	mu           sync.Mutex
	payload      []byte
	saveCount    int
	firstStarted chan struct{}
	releaseFirst chan struct{}
}

func (s *blockingStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.payload, target)
}

func (s *blockingStore) Save(_ string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.saveCount++
	count := s.saveCount
	s.mu.Unlock()
	if count == 1 {
		close(s.firstStarted)
		<-s.releaseFirst
	}
	s.mu.Lock()
	s.payload = payload
	s.mu.Unlock()
	return nil
}

type lockCheckingStore struct {
	api *API
}

func (*lockCheckingStore) Load(string, any) error { return state.ErrNotFound }

func (s *lockCheckingStore) Save(string, any) error {
	if !s.api.mu.TryLock() {
		return errors.New("API state lock held during Save")
	}
	s.api.mu.Unlock()
	return nil
}

type stickyFailureStore struct {
	loadErr error
	saves   int
}

func (s *stickyFailureStore) Load(string, any) error { return s.loadErr }
func (s *stickyFailureStore) Save(string, any) error {
	s.saves++
	return nil
}

func (s *memoryStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.payload, target)
}

func (s *memoryStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	payload, err := json.Marshal(value)
	if err == nil {
		s.payload = payload
	}
	return err
}

func TestEvaluatePolicyAllowsOnlyConfiguredImagePatterns(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	err = api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		AdmissionWhitelistPatterns: []AdmissionWhitelistPattern{{
			NamePattern: "us-docker.pkg.dev/demo/releases/*",
		}},
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_DENY",
			EnforcementMode: enforcedMode,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if decision := api.Evaluate("projects/demo", "us-docker.pkg.dev/demo/releases/app@sha256:abc"); !decision.Allowed {
		t.Fatalf("allowed decision = %#v", decision)
	}
	if decision := api.Evaluate("projects/demo", "docker.io/library/alpine:latest"); decision.Allowed {
		t.Fatalf("denied decision = %#v", decision)
	}
}

func TestProviderMinimalPutInfersCanonicalName(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut,
		"/v1/projects/demo/policy",
		strings.NewReader(`{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var policy Policy
	if err := json.Unmarshal(response.Body.Bytes(), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Name != "projects/demo/policy" {
		t.Fatalf("name=%q", policy.Name)
	}
	if got := api.Evaluate("projects/demo", "example/image"); !got.Allowed {
		t.Fatalf("decision=%#v", got)
	}
}

func TestProviderSuppliedMismatchedNameIsRejected(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut,
		"/v1/projects/demo/policy",
		strings.NewReader(`{"name":"projects/other/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got := api.Evaluate("projects/demo", "example/image"); got.Reason != "policy not found" {
		t.Fatalf("mismatched name mutated policy: %#v", got)
	}
}

func TestProjectIdentifiersRejectColonControlAndEncodingBeforeMutation(t *testing.T) {
	var authorizations int
	api, err := NewAPIWithStore(nil, AuthorizerFunc(func(string, string) error {
		authorizations++
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	validBody := `{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`
	for _, path := range []string{
		"/v1/projects/acme:prod/policy",
		"/v1/projects/acme%3Aprod/policy",
	} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPut, path, strings.NewReader(validBody)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("path=%q status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
	if err := api.SetPolicy("projects/acme\nprod", Policy{
		Name: "projects/acme\nprod/policy",
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_ALLOW",
			EnforcementMode: enforcedMode,
		},
	}); err == nil {
		t.Fatal("control-bearing project was accepted")
	}
	if authorizations != 0 {
		t.Fatalf("invalid projects reached authorizer %d times", authorizations)
	}
	if len(api.policies) != 0 {
		t.Fatalf("invalid projects mutated state: %#v", api.policies)
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut,
		"/v1/projects/acme-prod/policy", strings.NewReader(validBody)))
	if response.Code != http.StatusOK {
		t.Fatalf("valid hyphen project status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestProviderFieldsRoundTripWithoutAliasing(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	body := `{
		"description":"deny untrusted images",
		"globalPolicyEvaluationMode":"DISABLE",
		"admissionWhitelistPatterns":[{"namePattern":"us-docker.pkg.dev/demo/releases/*"}],
		"defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"},
		"clusterAdmissionRules":{
			"us-central1-a.prod":{"evaluationMode":"REQUIRE_ATTESTATION","enforcementMode":"DRYRUN_AUDIT_LOG_ONLY","requireAttestationsBy":["projects/security/attestors/provenance"]}
		}
	}`
	put := httptest.NewRecorder()
	api.ServeHTTP(put, httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy", strings.NewReader(body)))
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", put.Code, put.Body.String())
	}

	var returned Policy
	if err := json.Unmarshal(put.Body.Bytes(), &returned); err != nil {
		t.Fatal(err)
	}
	if returned.Name != "projects/demo/policy" ||
		returned.Description != "deny untrusted images" ||
		returned.GlobalPolicyEvaluationMode != "DISABLE" ||
		returned.DefaultAdmissionRule.EvaluationMode != "ALWAYS_DENY" ||
		returned.DefaultAdmissionRule.EnforcementMode != enforcedMode {
		t.Fatalf("PUT policy=%#v", returned)
	}
	cluster := returned.ClusterAdmissionRules["us-central1-a.prod"]
	if cluster.EvaluationMode != "REQUIRE_ATTESTATION" ||
		len(cluster.RequireAttestationsBy) != 1 {
		t.Fatalf("cluster rule=%#v", cluster)
	}

	returned.AdmissionWhitelistPatterns[0].NamePattern = "mutated"
	cluster.RequireAttestationsBy[0] = "mutated"
	returned.ClusterAdmissionRules["us-central1-a.prod"] = cluster

	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	get := httptest.NewRecorder()
	restarted.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET status=%d body=%s", get.Code, get.Body.String())
	}
	var persisted Policy
	if err := json.Unmarshal(get.Body.Bytes(), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.AdmissionWhitelistPatterns[0].NamePattern == "mutated" ||
		persisted.ClusterAdmissionRules["us-central1-a.prod"].RequireAttestationsBy[0] == "mutated" {
		t.Fatalf("response aliased active policy: %#v", persisted)
	}
}

func TestRawPolicyEnumsNormalizeBeforeValidation(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	body := `{
		"globalPolicyEvaluationMode":"GLOBAL_POLICY_EVALUATION_MODE_UNSPECIFIED",
		"admissionWhitelistPatterns":[{"namePattern":"gcr.io/google_containers/*"}],
		"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}
	}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut,
		"/v1/projects/raw-enums/policy", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var policy Policy
	if err := json.Unmarshal(response.Body.Bytes(), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.GlobalPolicyEvaluationMode != "DISABLE" ||
		len(policy.AdmissionWhitelistPatterns) != 1 ||
		policy.AdmissionWhitelistPatterns[0].NamePattern != "gcr.io/google_containers/*" ||
		policy.DefaultAdmissionRule.EvaluationMode != "ALWAYS_ALLOW" ||
		policy.DefaultAdmissionRule.EnforcementMode != enforcedMode {
		t.Fatalf("canonical policy=%#v", policy)
	}
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	persisted, ok := restarted.getPolicy("projects/raw-enums")
	if !ok || persisted.GlobalPolicyEvaluationMode != "DISABLE" ||
		len(persisted.AdmissionWhitelistPatterns) != 1 ||
		persisted.DefaultAdmissionRule.EnforcementMode != enforcedMode {
		t.Fatalf("restarted policy=%#v found=%t", persisted, ok)
	}
}

func TestGeneratedClientPolicyShapesUseCanonicalValidation(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	var requestBodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, readErr := io.ReadAll(request.Body)
		if readErr != nil {
			t.Errorf("read generated request: %v", readErr)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "read generated request")
			return
		}
		requestBodies = append(requestBodies, string(body))
		request.Body = io.NopCloser(bytes.NewReader(body))
		api.ServeHTTP(w, request)
	}))
	defer server.Close()
	client, err := generatedbinauthz.NewService(context.Background(),
		option.WithoutAuthentication(), option.WithEndpoint(server.URL+"/"))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		project     string
		policy      *generatedbinauthz.Policy
		wantBody    string
		wantEval    string
		wantGlobal  string
		wantPattern string
	}{
		{
			name:    "phase 21-22 omitted enforcement mode",
			project: "projects/generated-allow",
			policy: &generatedbinauthz.Policy{
				Name: "projects/generated-allow/policy",
				DefaultAdmissionRule: &generatedbinauthz.AdmissionRule{
					EvaluationMode: "ALWAYS_ALLOW",
				},
			},
			wantBody: `{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW"},"name":"projects/generated-allow/policy"}` + "\n",
			wantEval: "ALWAYS_ALLOW",
		},
		{
			name:    "phase 24-25 legacy deny alias",
			project: "projects/generated-deny",
			policy: &generatedbinauthz.Policy{
				Name: "projects/generated-deny/policy",
				DefaultAdmissionRule: &generatedbinauthz.AdmissionRule{
					EvaluationMode:  "DISALLOWED",
					EnforcementMode: enforcedMode,
				},
			},
			wantBody: `{"defaultAdmissionRule":{"enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG","evaluationMode":"DISALLOWED"},"name":"projects/generated-deny/policy"}` + "\n",
			wantEval: "ALWAYS_DENY",
		},
		{
			name:    "documented global mode and allowlist",
			project: "projects/generated-documented",
			policy: &generatedbinauthz.Policy{
				Name:                       "projects/generated-documented/policy",
				GlobalPolicyEvaluationMode: "GLOBAL_POLICY_EVALUATION_MODE_UNSPECIFIED",
				AdmissionWhitelistPatterns: []*generatedbinauthz.AdmissionWhitelistPattern{{
					NamePattern: "gcr.io/google_containers/*",
				}},
				DefaultAdmissionRule: &generatedbinauthz.AdmissionRule{
					EvaluationMode:  "ALWAYS_ALLOW",
					EnforcementMode: enforcedMode,
				},
			},
			wantBody:    `{"admissionWhitelistPatterns":[{"namePattern":"gcr.io/google_containers/*"}],"defaultAdmissionRule":{"enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG","evaluationMode":"ALWAYS_ALLOW"},"globalPolicyEvaluationMode":"GLOBAL_POLICY_EVALUATION_MODE_UNSPECIFIED","name":"projects/generated-documented/policy"}` + "\n",
			wantEval:    "ALWAYS_ALLOW",
			wantGlobal:  "DISABLE",
			wantPattern: "gcr.io/google_containers/*",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestBodies = nil
			got, err := client.Projects.UpdatePolicy(test.project+"/policy", test.policy).
				Context(context.Background()).Do()
			if err != nil {
				t.Fatal(err)
			}
			if len(requestBodies) != 1 || requestBodies[0] != test.wantBody {
				t.Fatalf("generated request bodies=%q want=%q", requestBodies, test.wantBody)
			}
			if got.DefaultAdmissionRule == nil ||
				got.DefaultAdmissionRule.EvaluationMode != test.wantEval ||
				got.DefaultAdmissionRule.EnforcementMode != enforcedMode ||
				got.GlobalPolicyEvaluationMode != test.wantGlobal {
				t.Fatalf("canonical generated response=%#v", got)
			}
			if test.wantPattern != "" &&
				(len(got.AdmissionWhitelistPatterns) != 1 ||
					got.AdmissionWhitelistPatterns[0].NamePattern != test.wantPattern) {
				t.Fatalf("generated allowlist response=%#v", got.AdmissionWhitelistPatterns)
			}
		})
	}

	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		policy, ok := restarted.getPolicy(test.project)
		if !ok || policy.DefaultAdmissionRule.EvaluationMode != test.wantEval ||
			policy.DefaultAdmissionRule.EnforcementMode != enforcedMode ||
			policy.GlobalPolicyEvaluationMode != test.wantGlobal {
			t.Fatalf("restarted project=%q policy=%#v found=%t", test.project, policy, ok)
		}
	}
}

func TestProviderResetPersistsCanonicalAllowAllAcrossRestart(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	stale := `{
		"description":"stale",
		"globalPolicyEvaluationMode":"DISABLE",
		"clusterAdmissionRules":{"us-central1-a.prod":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}},
		"defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}
	}`
	staleResponse := httptest.NewRecorder()
	api.ServeHTTP(staleResponse, httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy", strings.NewReader(stale)))
	if staleResponse.Code != http.StatusOK {
		t.Fatalf("stale PUT status=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}
	reset := `{
		"name":"projects/demo/policy",
		"admissionWhitelistPatterns":[{"namePattern":"gcr.io/google_containers/*"}],
		"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}
	}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy", strings.NewReader(reset)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var resetPolicy Policy
	if err := json.Unmarshal(response.Body.Bytes(), &resetPolicy); err != nil {
		t.Fatal(err)
	}
	assertCanonicalDefaultPolicy(t, resetPolicy)
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	decision := restarted.Evaluate("projects/demo", "docker.io/library/alpine:latest")
	if !decision.Allowed || decision.Reason != "default admission rule allows image" {
		t.Fatalf("decision=%#v", decision)
	}
	get := httptest.NewRecorder()
	restarted.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil))
	var persisted Policy
	if err := json.Unmarshal(get.Body.Bytes(), &persisted); err != nil {
		t.Fatal(err)
	}
	assertCanonicalDefaultPolicy(t, persisted)
}

func assertCanonicalDefaultPolicy(t *testing.T, policy Policy) {
	t.Helper()
	if policy.Name != "projects/demo/policy" ||
		policy.Description != "" ||
		policy.GlobalPolicyEvaluationMode != "" ||
		len(policy.ClusterAdmissionRules) != 0 ||
		len(policy.AdmissionWhitelistPatterns) != 1 ||
		policy.AdmissionWhitelistPatterns[0].NamePattern != "gcr.io/google_containers/*" ||
		policy.DefaultAdmissionRule.EvaluationMode != "ALWAYS_ALLOW" ||
		policy.DefaultAdmissionRule.EnforcementMode != enforcedMode {
		t.Fatalf("policy is not canonical provider default: %#v", policy)
	}
}

func TestUnsupportedEvaluationModesRemainExplicit(t *testing.T) {
	tests := []struct {
		name   string
		policy Policy
		body   string
		reason string
	}{
		{
			name: "attestation",
			policy: Policy{
				Name: "projects/demo/policy",
				DefaultAdmissionRule: AdmissionRule{
					EvaluationMode:        "REQUIRE_ATTESTATION",
					EnforcementMode:       enforcedMode,
					RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
				},
			},
			body:   `{"image":"example/image"}`,
			reason: "attestation evaluation is not implemented",
		},
		{
			name: "global platform policy",
			policy: Policy{
				Name:                       "projects/demo/policy",
				GlobalPolicyEvaluationMode: "ENABLE",
				DefaultAdmissionRule: AdmissionRule{
					EvaluationMode:  "ALWAYS_ALLOW",
					EnforcementMode: enforcedMode,
				},
			},
			body:   `{"image":"example/image"}`,
			reason: "global platform policy evaluation is not implemented",
		},
		{
			name: "cluster context",
			policy: Policy{
				Name: "projects/demo/policy",
				ClusterAdmissionRules: map[string]AdmissionRule{
					"us-central1-a.prod": {
						EvaluationMode:  "ALWAYS_DENY",
						EnforcementMode: enforcedMode,
					},
				},
				DefaultAdmissionRule: AdmissionRule{
					EvaluationMode:  "ALWAYS_ALLOW",
					EnforcementMode: enforcedMode,
				},
			},
			body:   `{"image":"example/image","cluster":"us-central1-a.prod"}`,
			reason: "cluster admission rule evaluation is not implemented",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			if err := api.SetPolicy("projects/demo", test.policy); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
				"/v1/projects/demo/policy:evaluate", strings.NewReader(test.body)))
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var decision Decision
			if err := json.Unmarshal(response.Body.Bytes(), &decision); err != nil {
				t.Fatal(err)
			}
			if decision.Allowed || decision.Reason != test.reason {
				t.Fatalf("decision=%#v", decision)
			}
			if decision.Outcome != "UNSUPPORTED" || decision.Enforced || decision.AuditOnly {
				t.Fatalf("unsupported decision metadata=%#v", decision)
			}
			if test.name != "cluster context" {
				if err := api.EvaluateImage("projects/demo", "example/image"); !errors.Is(err, ErrEvaluationUnsupported) {
					t.Fatalf("deployment evaluation error=%v", err)
				}
			}
		})
	}
}

func TestDryRunRuleReportsDenialWithoutBlockingDeployment(t *testing.T) {
	for _, test := range []struct {
		name string
		rule AdmissionRule
	}{
		{
			name: "always deny",
			rule: AdmissionRule{
				EvaluationMode:  "ALWAYS_DENY",
				EnforcementMode: "DRYRUN_AUDIT_LOG_ONLY",
			},
		},
		{
			name: "require attestation",
			rule: AdmissionRule{
				EvaluationMode:        "REQUIRE_ATTESTATION",
				EnforcementMode:       "DRYRUN_AUDIT_LOG_ONLY",
				RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			if err := api.SetPolicy("projects/demo", Policy{
				Name:                 "projects/demo/policy",
				DefaultAdmissionRule: test.rule,
			}); err != nil {
				t.Fatal(err)
			}
			decision := api.Evaluate("projects/demo", "example/image")
			if !decision.Allowed || decision.Outcome != "AUDIT" ||
				decision.Enforced || !decision.AuditOnly {
				t.Fatalf("dry-run decision metadata=%#v", decision)
			}
			if err := api.EvaluateImage("projects/demo", "example/image"); err != nil {
				t.Fatalf("dry-run policy blocked deployment: %v", err)
			}

			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
				"/v1/projects/demo/policy:evaluate", strings.NewReader(`{"image":"example/image"}`)))
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var HTTPDecision Decision
			if err := json.Unmarshal(response.Body.Bytes(), &HTTPDecision); err != nil {
				t.Fatal(err)
			}
			if !HTTPDecision.Allowed || HTTPDecision.Outcome != "AUDIT" || !HTTPDecision.AuditOnly {
				t.Fatalf("HTTP decision=%#v", HTTPDecision)
			}
		})
	}
}

func TestDryRunClusterRuleReturnsEffectiveAuditPermit(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		ClusterAdmissionRules: map[string]AdmissionRule{
			"us-central1-a.prod": {
				EvaluationMode:        "REQUIRE_ATTESTATION",
				EnforcementMode:       "DRYRUN_AUDIT_LOG_ONLY",
				RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
			},
		},
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_DENY",
			EnforcementMode: enforcedMode,
		},
	}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/policy:evaluate",
		strings.NewReader(`{"image":"example/image","cluster":"us-central1-a.prod"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var decision Decision
	if err := json.Unmarshal(response.Body.Bytes(), &decision); err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed || decision.Outcome != "AUDIT" || !decision.AuditOnly || decision.Enforced {
		t.Fatalf("decision=%#v", decision)
	}
}

func TestAdmissionWhitelistPatternValidationAndMatching(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []string{"", " ", "gcr.io*", "gcr.io/ex*ample", "gcr.io/ex*ample/*", "gcr.io/example/**"}
	for _, pattern := range invalid {
		err := api.SetPolicy("projects/demo", Policy{
			Name: "projects/demo/policy",
			AdmissionWhitelistPatterns: []AdmissionWhitelistPattern{{
				NamePattern: pattern,
			}},
			DefaultAdmissionRule: AdmissionRule{
				EvaluationMode:  "ALWAYS_DENY",
				EnforcementMode: enforcedMode,
			},
		})
		if err == nil {
			t.Fatalf("pattern %q was accepted", pattern)
		}
	}

	if err := api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		AdmissionWhitelistPatterns: []AdmissionWhitelistPattern{
			{NamePattern: "gcr.io/example/*"},
			{NamePattern: "gcr.io/google_containers/*"},
			{NamePattern: "gcr.io/google_containers/pause:3.9"},
		},
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_DENY",
			EnforcementMode: enforcedMode,
		},
	}); err != nil {
		t.Fatal(err)
	}
	for _, image := range []string{
		"gcr.io/example/app:latest",
		"gcr.io/google_containers/owned-by-default-pattern:latest",
		"gcr.io/google_containers/pause:3.9",
	} {
		if decision := api.Evaluate("projects/demo", image); !decision.Allowed {
			t.Fatalf("image %q decision=%#v", image, decision)
		}
	}
	if decision := api.Evaluate("projects/demo", "gcr.io/example-evil/app:latest"); decision.Allowed {
		t.Fatalf("unsafe prefix match decision=%#v", decision)
	}
}

func TestInvalidProviderPolicyDoesNotMutate(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	original := Policy{
		Name: "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_ALLOW",
			EnforcementMode: enforcedMode,
		},
	}
	if err := api.SetPolicy("projects/demo", original); err != nil {
		t.Fatal(err)
	}
	tests := []string{
		`{"defaultAdmissionRule":{"evaluationMode":"BOGUS","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`,
		`{"defaultAdmissionRule":{"evaluationMode":"EVALUATION_MODE_UNSPECIFIED","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`,
		`{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"BOGUS"}}`,
		`{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCEMENT_MODE_UNSPECIFIED"}}`,
		`{"globalPolicyEvaluationMode":"BOGUS","defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`,
		`{"defaultAdmissionRule":{"evaluationMode":"REQUIRE_ATTESTATION","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`,
		`{"clusterAdmissionRules":{"bad cluster":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}},"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`,
	}
	for _, body := range tests {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPut,
			"/v1/projects/demo/policy", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d response=%s", body, response.Code, response.Body.String())
		}
		var envelope struct {
			Error struct {
				Code    int           `json:"code"`
				Status  string        `json:"status"`
				Details []interface{} `json:"details"`
			} `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("body=%s decode error envelope: %v", body, err)
		}
		if envelope.Error.Code != http.StatusBadRequest ||
			envelope.Error.Status != "INVALID_ARGUMENT" ||
			envelope.Error.Details == nil {
			t.Fatalf("body=%s error envelope=%#v", body, envelope)
		}
		if got := api.Evaluate("projects/demo", "example/image"); !got.Allowed {
			t.Fatalf("body=%s mutated policy: %#v", body, got)
		}
	}
}

func TestLegacyDisallowedCanonicalizesToAlwaysDeny(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetPolicy("projects/demo", Policy{
		Name:                 "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{EvaluationMode: "DISALLOWED"},
	}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil))
	var policy Policy
	if err := json.Unmarshal(response.Body.Bytes(), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.DefaultAdmissionRule.EvaluationMode != "ALWAYS_DENY" ||
		policy.DefaultAdmissionRule.EnforcementMode != enforcedMode {
		t.Fatalf("legacy policy=%#v", policy)
	}
}

func TestPolicyBodyBoundsAndTrailingJSON(t *testing.T) {
	oversized := `{"description":"` + strings.Repeat("x", 1<<20) + `"}`
	for _, test := range []struct {
		name    string
		body    string
		chunked bool
		status  int
	}{
		{name: "fixed oversized", body: oversized, status: http.StatusRequestEntityTooLarge},
		{name: "chunked oversized", body: oversized, chunked: true, status: http.StatusRequestEntityTooLarge},
		{
			name:   "trailing JSON",
			body:   `{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}} {}`,
			status: http.StatusBadRequest,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPut,
				"/v1/projects/demo/policy", strings.NewReader(test.body))
			if test.chunked {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.status, response.Body.String())
			}
			if got := api.Evaluate("projects/demo", "example/image"); got.Reason != "policy not found" {
				t.Fatalf("request mutated policy: %#v", got)
			}
		})
	}
}

func TestPolicyStateExportImportPreservesEvaluation(t *testing.T) {
	source, err := state.New(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(source, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_DENY",
			EnforcementMode: enforcedMode,
		},
	}); err != nil {
		t.Fatal(err)
	}
	var exported bytes.Buffer
	if err := source.Export(&exported); err != nil {
		t.Fatal(err)
	}
	target, err := state.New(t.TempDir(), "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Import(&exported); err != nil {
		t.Fatal(err)
	}
	imported, err := NewAPIWithStore(target, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := imported.Evaluate("projects/demo", "example/image"); got.Allowed || got.Reason != "default admission rule denies image" {
		t.Fatalf("decision=%#v", got)
	}
}

func TestLegacyStateImportCanonicalizesPolicy(t *testing.T) {
	legacy := metadata{Policies: map[string]Policy{
		"projects/demo": {
			Name:                 "projects/demo/policy",
			DefaultAdmissionRule: AdmissionRule{EvaluationMode: "DISALLOWED"},
		},
	}}
	payload, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(map[string]any{
		"format":  "minisky-state",
		"version": 1,
		"entries": map[string]json.RawMessage{stateEntry: payload},
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.New(t.TempDir(), "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Import(bytes.NewReader(snapshot)); err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := api.Evaluate("projects/demo", "example/image"); got.Allowed || got.Reason != "default admission rule denies image" {
		t.Fatalf("decision=%#v", got)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil))
	var canonical Policy
	if err := json.Unmarshal(response.Body.Bytes(), &canonical); err != nil {
		t.Fatal(err)
	}
	if canonical.DefaultAdmissionRule.EvaluationMode != "ALWAYS_DENY" ||
		canonical.DefaultAdmissionRule.EnforcementMode != enforcedMode {
		t.Fatalf("legacy persisted policy was not canonicalized: %#v", canonical)
	}
}

func TestPersistedPolicyAliasCollisionsAreRejectedDeterministically(t *testing.T) {
	allow := `{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`
	deny := `{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`
	legacyWithoutName := `{"defaultAdmissionRule":{"evaluationMode":"DISALLOWED"}}`
	for _, test := range []struct {
		name    string
		payload string
	}{
		{
			name: "conflicting legacy first",
			payload: `{"policies":{"projects/demo":` + allow +
				`,"projects/demo/policy":` + deny + `}}`,
		},
		{
			name: "conflicting canonical first",
			payload: `{"policies":{"projects/demo/policy":` + deny +
				`,"projects/demo":` + allow + `}}`,
		},
		{
			name: "identical aliases",
			payload: `{"policies":{"projects/demo":` + allow +
				`,"projects/demo/policy":` + allow + `}}`,
		},
		{
			name: "legacy migration and omitted name",
			payload: `{"policies":{"projects/demo":` + legacyWithoutName +
				`,"projects/demo/policy":` + deny + `}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &memoryStore{payload: []byte(test.payload)}
			original := append([]byte(nil), store.payload...)
			if _, err := NewAPIWithStore(store, AllowAllAuthorizer{}); err == nil ||
				!strings.Contains(err.Error(), "ambiguous persisted policy") {
				t.Fatalf("load error=%v", err)
			}
			if !bytes.Equal(store.payload, original) {
				t.Fatal("ambiguous persisted state was overwritten")
			}
		})
	}
}

func TestAliasCollisionLeavesFallbackStickyUnavailable(t *testing.T) {
	payload := []byte(`{"policies":{
		"projects/demo":{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}},
		"projects/demo/policy":{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}
	}}`)
	store := &memoryStore{payload: append([]byte(nil), payload...)}
	_, loadErr := NewAPIWithStore(store, AllowAllAuthorizer{})
	if loadErr == nil {
		t.Fatal("expected ambiguous persisted state to fail")
	}
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	api.store = store
	api.initializationErr = loadErr

	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil),
		httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy",
			strings.NewReader(`{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`)),
	} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable ||
			!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
			t.Fatalf("%s status=%d body=%s", request.Method, response.Code, response.Body.String())
		}
	}
	if !bytes.Equal(store.payload, payload) {
		t.Fatal("sticky unavailable fallback overwrote ambiguous state")
	}
}

func TestImportPolicyAliasCollisionsPreserveActiveState(t *testing.T) {
	allow := `{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`
	deny := `{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`
	legacyWithoutName := `{"defaultAdmissionRule":{"evaluationMode":"DISALLOWED"}}`
	for _, test := range []struct {
		name    string
		payload string
	}{
		{
			name: "conflicting legacy first",
			payload: `{"policies":{"projects/demo":` + allow +
				`,"projects/demo/policy":` + deny + `}}`,
		},
		{
			name: "conflicting canonical first",
			payload: `{"policies":{"projects/demo/policy":` + deny +
				`,"projects/demo":` + allow + `}}`,
		},
		{
			name: "identical aliases",
			payload: `{"policies":{"projects/demo":` + allow +
				`,"projects/demo/policy":` + allow + `}}`,
		},
		{
			name: "legacy migration and omitted name",
			payload: `{"policies":{"projects/demo":` + legacyWithoutName +
				`,"projects/demo/policy":` + deny + `}}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := state.New(t.TempDir(), "alias-import")
			if err != nil {
				t.Fatal(err)
			}
			api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			if err := api.SetPolicy("projects/demo", Policy{
				Name: "projects/demo/policy",
				DefaultAdmissionRule: AdmissionRule{
					EvaluationMode:  "ALWAYS_ALLOW",
					EnforcementMode: enforcedMode,
				},
			}); err != nil {
				t.Fatal(err)
			}
			var before bytes.Buffer
			if err := store.Export(&before); err != nil {
				t.Fatal(err)
			}
			snapshot, err := json.Marshal(map[string]any{
				"format":  "minisky-state",
				"version": 1,
				"entries": map[string]json.RawMessage{
					stateEntry: json.RawMessage(test.payload),
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Import(bytes.NewReader(snapshot)); err == nil ||
				!strings.Contains(err.Error(), "ambiguous persisted policy") {
				t.Fatalf("import error=%v", err)
			}
			var after bytes.Buffer
			if err := store.Export(&after); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before.Bytes(), after.Bytes()) {
				t.Fatal("rejected import changed active snapshot")
			}
			restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			if decision := restarted.Evaluate("projects/demo", "example/image"); !decision.Allowed {
				t.Fatalf("rejected import changed active policy: %#v", decision)
			}
		})
	}
}

func TestInvalidPolicyImportIsRejectedBeforeStateReplacement(t *testing.T) {
	store, err := state.New(t.TempDir(), "import")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_ALLOW",
			EnforcementMode: enforcedMode,
		},
	}); err != nil {
		t.Fatal(err)
	}

	invalid := metadata{Policies: map[string]Policy{
		"projects/demo/policy": {
			Name: "projects/demo/policy",
			DefaultAdmissionRule: AdmissionRule{
				EvaluationMode:  "BOGUS",
				EnforcementMode: enforcedMode,
			},
		},
	}}
	payload, err := json.Marshal(invalid)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(map[string]any{
		"format":  "minisky-state",
		"version": 1,
		"entries": map[string]json.RawMessage{stateEntry: payload},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Import(bytes.NewReader(snapshot)); err == nil {
		t.Fatal("expected invalid policy import to fail")
	}
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.Evaluate("projects/demo", "example/image"); !got.Allowed {
		t.Fatalf("failed import replaced active state: %#v", got)
	}
}

func TestHTTPPersistenceFailureRollsBackWithInternalError(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_ALLOW",
			EnforcementMode: enforcedMode,
		},
	}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.saveErr = errors.New("disk full")
	store.mu.Unlock()

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy",
		strings.NewReader(`{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_DENY","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`)))
	if response.Code != http.StatusInternalServerError ||
		!strings.Contains(response.Body.String(), `"status":"INTERNAL"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got := api.Evaluate("projects/demo", "example/image"); !got.Allowed {
		t.Fatalf("failed save mutated policy: %#v", got)
	}
	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil))
	var current Policy
	if err := json.Unmarshal(get.Body.Bytes(), &current); err != nil {
		t.Fatal(err)
	}
	if current.DefaultAdmissionRule.EvaluationMode != "ALWAYS_ALLOW" {
		t.Fatalf("GET observed failed update: %#v", current)
	}
	store.mu.Lock()
	store.saveErr = nil
	store.mu.Unlock()
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.Evaluate("projects/demo", "example/image"); !got.Allowed {
		t.Fatalf("restart observed failed update: %#v", got)
	}
}

func TestConcurrentBlockedSavesPreserveRequestOrder(t *testing.T) {
	store := &blockingStore{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	put := func(mode string) int {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy",
			strings.NewReader(`{"defaultAdmissionRule":{"evaluationMode":"`+mode+`","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`)))
		return response.Code
	}
	firstDone := make(chan int, 1)
	go func() {
		firstDone <- put("ALWAYS_ALLOW")
	}()
	<-store.firstStarted

	secondDone := make(chan int, 1)
	go func() {
		secondDone <- put("ALWAYS_DENY")
	}()
	select {
	case status := <-secondDone:
		t.Fatalf("second PUT completed before first save: status=%d", status)
	case <-time.After(25 * time.Millisecond):
	}
	close(store.releaseFirst)
	if status := <-firstDone; status != http.StatusOK {
		t.Fatalf("first PUT status=%d", status)
	}
	if status := <-secondDone; status != http.StatusOK {
		t.Fatalf("second PUT status=%d", status)
	}
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.Evaluate("projects/demo", "example/image"); got.Outcome != "DENY" {
		t.Fatalf("durable policy ordering=%#v", got)
	}
}

func TestStateLockIsNotHeldDuringSave(t *testing.T) {
	store := &lockCheckingStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	store.api = api
	if err := api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_ALLOW",
			EnforcementMode: enforcedMode,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentPolicyUpdatesAndReads(t *testing.T) {
	api, err := NewAPIWithStore(&memoryStore{}, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for i := 0; i < 24; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			mode := "ALWAYS_ALLOW"
			if index%2 == 0 {
				mode = "ALWAYS_DENY"
			}
			if err := api.SetPolicy("projects/demo", Policy{
				Name: "projects/demo/policy",
				DefaultAdmissionRule: AdmissionRule{
					EvaluationMode:  mode,
					EnforcementMode: enforcedMode,
				},
			}); err != nil {
				t.Errorf("SetPolicy: %v", err)
				return
			}
			_ = api.Evaluate("projects/demo", "example/image")
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil))
			if response.Code != http.StatusOK {
				t.Errorf("GET status=%d body=%s", response.Code, response.Body.String())
			}
		}(i)
	}
	wait.Wait()
}

func TestSetAndGetPolicyUseDeepCopies(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	input := Policy{
		Name: "projects/demo/policy",
		AdmissionWhitelistPatterns: []AdmissionWhitelistPattern{{
			NamePattern: "gcr.io/example/*",
		}},
		ClusterAdmissionRules: map[string]AdmissionRule{
			"us-central1-a.prod": {
				EvaluationMode:        "REQUIRE_ATTESTATION",
				EnforcementMode:       enforcedMode,
				RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
			},
		},
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:  "ALWAYS_DENY",
			EnforcementMode: enforcedMode,
		},
	}
	if err := api.SetPolicy("projects/demo", input); err != nil {
		t.Fatal(err)
	}
	input.AdmissionWhitelistPatterns[0].NamePattern = "mutated"
	rule := input.ClusterAdmissionRules["us-central1-a.prod"]
	rule.RequireAttestationsBy[0] = "mutated"
	input.ClusterAdmissionRules["us-central1-a.prod"] = rule

	first, ok := api.getPolicy("projects/demo")
	if !ok {
		t.Fatal("policy not found")
	}
	first.AdmissionWhitelistPatterns[0].NamePattern = "mutated again"
	rule = first.ClusterAdmissionRules["us-central1-a.prod"]
	rule.RequireAttestationsBy[0] = "mutated again"
	first.ClusterAdmissionRules["us-central1-a.prod"] = rule

	second, ok := api.getPolicy("projects/demo")
	if !ok {
		t.Fatal("policy not found")
	}
	if second.AdmissionWhitelistPatterns[0].NamePattern != "gcr.io/example/*" ||
		second.ClusterAdmissionRules["us-central1-a.prod"].RequireAttestationsBy[0] !=
			"projects/security/attestors/provenance" {
		t.Fatalf("stored policy was aliased: %#v", second)
	}
}

func TestPolicyRestartSaveFailureAndAuthorization(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	policy := Policy{Name: "projects/demo/policy", DefaultAdmissionRule: AdmissionRule{EvaluationMode: "ALWAYS_ALLOW"}}
	if err := api.SetPolicy("projects/demo", policy); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.Evaluate("projects/demo", "example/image").Allowed {
		t.Fatal("rehydrated policy was not evaluated")
	}

	store.saveErr = errors.New("disk full")
	if err := api.SetPolicy("projects/demo", Policy{Name: "projects/demo/policy", DefaultAdmissionRule: AdmissionRule{EvaluationMode: "DISALLOWED"}}); err == nil {
		t.Fatal("expected save failure")
	}
	if !api.Evaluate("projects/demo", "example/image").Allowed {
		t.Fatal("failed save changed active policy")
	}

	denied, err := NewAPIWithStore(nil, AuthorizerFunc(func(string, string) error { return errors.New("denied") }))
	if err != nil {
		t.Fatal(err)
	}
	if err := denied.SetPolicy("projects/demo", policy); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("authorization error = %v", err)
	}
}

func TestPolicyUpdateUsesExactAuthorizerContract(t *testing.T) {
	var action, resource string
	api, err := NewAPIWithStore(nil, AuthorizerFunc(func(gotAction, gotResource string) error {
		action, resource = gotAction, gotResource
		return errors.New("denied")
	}))
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy",
		strings.NewReader(`{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`)))
	if response.Code != http.StatusForbidden ||
		!strings.Contains(response.Body.String(), `"status":"PERMISSION_DENIED"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if action != "binaryauthorization.policy.update" || resource != "projects/demo/policy" {
		t.Fatalf("Authorize(%q, %q)", action, resource)
	}
	if got := api.Evaluate("projects/demo", "example/image"); got.Reason != "policy not found" {
		t.Fatalf("denied update mutated policy: %#v", got)
	}
}

func TestMissingPolicyFailsClosed(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	decision := api.Evaluate("projects/demo", "example/image")
	if decision.Allowed || decision.Reason != "policy not found" {
		t.Fatalf("decision = %#v", decision)
	}
	if err := api.EvaluateImage("projects/demo", "example/image"); !errors.Is(err, ErrAdmissionDenied) {
		t.Fatalf("deployment evaluation error = %v", err)
	}
}

func TestEvaluateRequestUsesActivePolicy(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetPolicy("projects/demo", Policy{
		Name:                 "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{EvaluationMode: "DISALLOWED"},
		AdmissionWhitelistPatterns: []AdmissionWhitelistPattern{{
			NamePattern: "us-docker.pkg.dev/demo/releases/*",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/policy:evaluate",
		strings.NewReader(`{"image":"us-docker.pkg.dev/demo/releases/app@sha256:abc"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var decision Decision
	if err := json.Unmarshal(response.Body.Bytes(), &decision); err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed || decision.Policy != "projects/demo/policy" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestAttestationRoutesRemainUnimplemented(t *testing.T) {
	api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/attestors", strings.NewReader(`{}`)))
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d, want 501: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"status":"UNIMPLEMENTED"`) {
		t.Fatalf("response = %s", response.Body.String())
	}
}

func TestInvalidPersistedPolicyIsRejected(t *testing.T) {
	store := &memoryStore{}
	store.payload, _ = json.Marshal(metadata{Policies: map[string]Policy{
		"projects/demo": {
			Name:                 "projects/demo/policy",
			DefaultAdmissionRule: AdmissionRule{EvaluationMode: "REQUIRE_ATTESTATION"},
		},
	}})
	original := append([]byte(nil), store.payload...)
	if _, err := NewAPIWithStore(store, AllowAllAuthorizer{}); err == nil {
		t.Fatal("expected unsupported persisted evaluation mode to be rejected")
	}
	if !bytes.Equal(store.payload, original) {
		t.Fatal("invalid persisted policy was overwritten")
	}
}

func TestInitializationFailureIsStickyAndProviderRefreshFailsUnavailable(t *testing.T) {
	store := &stickyFailureStore{loadErr: errors.New("corrupt state")}
	if _, err := NewAPIWithStore(store, AllowAllAuthorizer{}); err == nil {
		t.Fatal("expected initial load failure")
	} else {
		api, fallbackErr := NewAPIWithStore(nil, AllowAllAuthorizer{})
		if fallbackErr != nil {
			t.Fatal(fallbackErr)
		}
		api.store = store
		api.initializationErr = err

		for _, request := range []*http.Request{
			httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil),
			httptest.NewRequest(http.MethodPost, "/v1/projects/demo/policy:evaluate",
				strings.NewReader(`{"image":"example/image"}`)),
			httptest.NewRequest(http.MethodPut, "/v1/projects/demo/policy",
				strings.NewReader(`{"defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW","enforcementMode":"ENFORCED_BLOCK_AND_AUDIT_LOG"}}`)),
		} {
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != http.StatusServiceUnavailable ||
				!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
				t.Fatalf("%s status=%d body=%s", request.Method, response.Code, response.Body.String())
			}
		}
		if decision := api.Evaluate("projects/demo", "example/image"); decision.Outcome != "UNAVAILABLE" || decision.Allowed {
			t.Fatalf("direct decision=%#v", decision)
		}
		if err := api.EvaluateImage("projects/demo", "example/image"); !errors.Is(err, ErrPersistence) {
			t.Fatalf("direct evaluation error=%v", err)
		} else {
			var unavailable interface {
				PolicyEvaluationUnavailable() bool
			}
			if !errors.As(err, &unavailable) || !unavailable.PolicyEvaluationUnavailable() {
				t.Fatalf("persistence error lacks unavailable classification: %v", err)
			}
		}
		if store.saves != 0 {
			t.Fatalf("initialization failure triggered %d saves", store.saves)
		}

		store.loadErr = nil
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/policy", nil))
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("initialization failure recovered implicitly: status=%d body=%s",
				response.Code, response.Body.String())
		}
	}
}

func TestEvaluateImageClassifications(t *testing.T) {
	type classification interface {
		PolicyEvaluationUnsupported() bool
	}
	for _, test := range []struct {
		name        string
		rule        AdmissionRule
		wantError   error
		unsupported bool
	}{
		{
			name: "allow",
			rule: AdmissionRule{
				EvaluationMode:  "ALWAYS_ALLOW",
				EnforcementMode: enforcedMode,
			},
		},
		{
			name: "audit permit",
			rule: AdmissionRule{
				EvaluationMode:  "ALWAYS_DENY",
				EnforcementMode: "DRYRUN_AUDIT_LOG_ONLY",
			},
		},
		{
			name: "enforced deny",
			rule: AdmissionRule{
				EvaluationMode:  "ALWAYS_DENY",
				EnforcementMode: enforcedMode,
			},
			wantError: ErrAdmissionDenied,
		},
		{
			name: "unsupported",
			rule: AdmissionRule{
				EvaluationMode:        "REQUIRE_ATTESTATION",
				EnforcementMode:       enforcedMode,
				RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
			},
			wantError:   ErrEvaluationUnsupported,
			unsupported: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			if err := api.SetPolicy("projects/demo", Policy{
				Name:                 "projects/demo/policy",
				DefaultAdmissionRule: test.rule,
			}); err != nil {
				t.Fatal(err)
			}
			evaluationErr := api.EvaluateImage("projects/demo", "example/image")
			if test.wantError == nil {
				if evaluationErr != nil {
					t.Fatalf("evaluation error=%v", evaluationErr)
				}
				return
			}
			if !errors.Is(evaluationErr, test.wantError) {
				t.Fatalf("evaluation error=%v want=%v", evaluationErr, test.wantError)
			}
			var unsupported classification
			if errors.As(evaluationErr, &unsupported) != test.unsupported {
				t.Fatalf("unsupported classification=%t want=%t", unsupported != nil, test.unsupported)
			}
		})
	}
	t.Run("unavailable", func(t *testing.T) {
		api, err := NewAPIWithStore(nil, AllowAllAuthorizer{})
		if err != nil {
			t.Fatal(err)
		}
		api.initializationErr = errors.New("corrupt state")
		evaluationErr := api.EvaluateImage("projects/demo", "example/image")
		if !errors.Is(evaluationErr, ErrPersistence) {
			t.Fatalf("evaluation error=%v", evaluationErr)
		}
		var unavailable interface {
			PolicyEvaluationUnavailable() bool
		}
		if !errors.As(evaluationErr, &unavailable) || !unavailable.PolicyEvaluationUnavailable() {
			t.Fatalf("unavailable classification missing: %v", evaluationErr)
		}
	})
}

func TestPersistedRequireAttestationRemainsExplicitlyUnsupported(t *testing.T) {
	store := &memoryStore{}
	api, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.SetPolicy("projects/demo", Policy{
		Name: "projects/demo/policy",
		DefaultAdmissionRule: AdmissionRule{
			EvaluationMode:        "REQUIRE_ATTESTATION",
			EnforcementMode:       enforcedMode,
			RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithStore(store, AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	decision := restarted.Evaluate("projects/demo", "example/image")
	if decision.Outcome != "UNSUPPORTED" || decision.Enforced || decision.Allowed {
		t.Fatalf("decision=%#v", decision)
	}
	if err := restarted.EvaluateImage("projects/demo", "example/image"); !errors.Is(err, ErrEvaluationUnsupported) {
		t.Fatalf("evaluation error=%v", err)
	}
}
