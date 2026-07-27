// Package binaryauthorization implements bounded package-local image policy evaluation.
package binaryauthorization

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"minisky/pkg/config"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const stateEntry = "binaryauthorization/metadata"

var (
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrPermissionDenied = errors.New("permission denied")
	ErrAdmissionDenied  = errors.New("admission denied")
)

func init() {
	state.MustRegisterEntryValidator(stateEntry, state.StrictEntryValidator[metadata](nil))
	registry.Register("binaryauthorization.googleapis.com", func(*registry.Context) http.Handler {
		return NewAPI()
	})
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type Authorizer interface {
	Authorize(action, resource string) error
}

type AuthorizerFunc func(action, resource string) error

func (authorize AuthorizerFunc) Authorize(action, resource string) error {
	return authorize(action, resource)
}

type AllowAllAuthorizer struct{}

func (AllowAllAuthorizer) Authorize(string, string) error { return nil }

type AdmissionWhitelistPattern struct {
	NamePattern string `json:"namePattern"`
}

type AdmissionRule struct {
	EvaluationMode  string `json:"evaluationMode"`
	EnforcementMode string `json:"enforcementMode,omitempty"`
}

type Policy struct {
	Name                       string                      `json:"name"`
	AdmissionWhitelistPatterns []AdmissionWhitelistPattern `json:"admissionWhitelistPatterns,omitempty"`
	DefaultAdmissionRule       AdmissionRule               `json:"defaultAdmissionRule"`
}

type Decision struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
	Policy  string `json:"policy,omitempty"`
}

type metadata struct {
	Policies map[string]Policy `json:"policies"`
}

// Evaluator is the injection point supported deployment packages can consume.
type Evaluator interface {
	EvaluateImage(project, image string) error
}

type API struct {
	mu                sync.RWMutex
	persistMu         sync.Mutex
	store             stateStore
	authorizer        Authorizer
	initializationErr error
	policies          map[string]Policy
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	guarded := state.NewGuardedEntryStore(store, err)
	api, loadErr := NewAPIWithStore(guarded, AllowAllAuthorizer{})
	if loadErr == nil {
		return api
	}
	api, _ = NewAPIWithStore(nil, AllowAllAuthorizer{})
	api.store = guarded
	api.initializationErr = loadErr
	return api
}

func NewAPIWithStore(store stateStore, authorizer Authorizer) (*API, error) {
	if authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer is required", ErrInvalidArgument)
	}
	api := &API{store: store, authorizer: authorizer, policies: make(map[string]Policy)}
	if store == nil {
		return api, nil
	}
	var saved metadata
	if err := store.Load(stateEntry, &saved); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Binary Authorization policy: %w", err)
	}
	for project, policy := range saved.Policies {
		if err := validatePolicy(project, policy); err != nil {
			return nil, fmt.Errorf("invalid persisted policy: %w", err)
		}
		api.policies[project] = clonePolicy(policy)
	}
	return api, nil
}

func (api *API) SetPolicy(project string, policy Policy) error {
	if api.initializationErr != nil {
		return fmt.Errorf("Binary Authorization persistence unavailable: %w", api.initializationErr)
	}
	if err := validatePolicy(project, policy); err != nil {
		return err
	}
	if err := api.authorizer.Authorize("binaryauthorization.policy.update", project+"/policy"); err != nil {
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	snapshot := make(map[string]Policy, len(api.policies)+1)
	for key, existing := range api.policies {
		snapshot[key] = clonePolicy(existing)
	}
	api.mu.RUnlock()
	snapshot[project] = clonePolicy(policy)
	if api.store != nil {
		if err := api.store.Save(stateEntry, metadata{Policies: snapshot}); err != nil {
			return fmt.Errorf("persist Binary Authorization policy: %w", err)
		}
	}
	api.mu.Lock()
	api.policies = snapshot
	api.mu.Unlock()
	return nil
}

func (api *API) Evaluate(project, image string) Decision {
	if !validProject(project) || strings.TrimSpace(image) == "" {
		return Decision{Reason: "invalid evaluation request"}
	}
	api.mu.RLock()
	policy, exists := api.policies[project]
	api.mu.RUnlock()
	if !exists {
		return Decision{Reason: "policy not found"}
	}
	for _, pattern := range policy.AdmissionWhitelistPatterns {
		if imageMatches(pattern.NamePattern, image) {
			return Decision{Allowed: true, Reason: "image matched admission whitelist", Policy: policy.Name}
		}
	}
	switch policy.DefaultAdmissionRule.EvaluationMode {
	case "ALWAYS_ALLOW":
		return Decision{Allowed: true, Reason: "default admission rule allows image", Policy: policy.Name}
	case "DISALLOWED":
		return Decision{Reason: "default admission rule disallows image", Policy: policy.Name}
	default:
		return Decision{Reason: "unsupported admission evaluation mode", Policy: policy.Name}
	}
}

// EvaluateImage exposes the policy decision through a package-neutral error
// contract so deployment shims can inject an evaluator without importing this
// package or sharing global state.
func (api *API) EvaluateImage(project, image string) error {
	decision := api.Evaluate(project, image)
	if decision.Allowed {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrAdmissionDenied, decision.Reason)
}

func (api *API) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	project := projectFromPath(request.URL.Path)
	switch {
	case strings.HasSuffix(request.URL.Path, "/policy") && request.Method == http.MethodPut:
		request.Body = http.MaxBytesReader(w, request.Body, 1<<20)
		var policy Policy
		if err := json.NewDecoder(request.Body).Decode(&policy); err != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "invalid policy")
			return
		}
		if err := api.SetPolicy(project, policy); err != nil {
			if errors.Is(err, ErrPermissionDenied) {
				writeError(w, 403, "PERMISSION_DENIED", "permission denied")
			} else {
				writeError(w, 400, "INVALID_ARGUMENT", err.Error())
			}
			return
		}
		_ = json.NewEncoder(w).Encode(policy)
	case strings.HasSuffix(request.URL.Path, "/policy") && request.Method == http.MethodGet:
		api.mu.RLock()
		policy, ok := api.policies[project]
		api.mu.RUnlock()
		if !ok {
			writeError(w, 404, "NOT_FOUND", "policy not found")
			return
		}
		_ = json.NewEncoder(w).Encode(policy)
	case strings.HasSuffix(request.URL.Path, "/policy:evaluate") && request.Method == http.MethodPost:
		request.Body = http.MaxBytesReader(w, request.Body, 1<<20)
		var body struct {
			Image string `json:"image"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "invalid evaluation request")
			return
		}
		_ = json.NewEncoder(w).Encode(api.Evaluate(project, body.Image))
	default:
		writeError(w, 501, "UNIMPLEMENTED",
			"Binary Authorization attestation and deployment interception are not implemented")
	}
}

func validatePolicy(project string, policy Policy) error {
	if !validProject(project) || policy.Name != project+"/policy" {
		return fmt.Errorf("%w: policy name must match project", ErrInvalidArgument)
	}
	switch policy.DefaultAdmissionRule.EvaluationMode {
	case "ALWAYS_ALLOW", "DISALLOWED":
	default:
		return fmt.Errorf("%w: unsupported evaluation mode", ErrInvalidArgument)
	}
	for _, pattern := range policy.AdmissionWhitelistPatterns {
		if pattern.NamePattern == "" || strings.Count(pattern.NamePattern, "*") > 1 ||
			(strings.Contains(pattern.NamePattern, "*") && !strings.HasSuffix(pattern.NamePattern, "*")) {
			return fmt.Errorf("%w: invalid image pattern", ErrInvalidArgument)
		}
	}
	return nil
}

func validProject(project string) bool {
	return strings.HasPrefix(project, "projects/") && strings.Count(project, "/") == 1 &&
		strings.TrimPrefix(project, "projects/") != ""
}

func imageMatches(pattern, image string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(image, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == image
}

func projectFromPath(path string) string {
	trimmed := strings.TrimPrefix(path, "/v1/")
	parts := strings.Split(trimmed, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return "projects/" + parts[1]
	}
	return ""
}

func clonePolicy(policy Policy) Policy {
	clone := policy
	clone.AdmissionWhitelistPatterns = append([]AdmissionWhitelistPattern(nil), policy.AdmissionWhitelistPatterns...)
	return clone
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": message, "status": status, "details": []any{}},
	})
}
