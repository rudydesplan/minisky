package binaryauthorization

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

type memoryStore struct {
	mu      sync.Mutex
	payload []byte
	saveErr error
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
		DefaultAdmissionRule: AdmissionRule{EvaluationMode: "DISALLOWED"},
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
	if _, err := NewAPIWithStore(store, AllowAllAuthorizer{}); err == nil {
		t.Fatal("expected unsupported persisted evaluation mode to be rejected")
	}
}
