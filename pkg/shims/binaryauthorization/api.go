// Package binaryauthorization implements bounded package-local image policy evaluation.
package binaryauthorization

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"minisky/pkg/config"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const stateEntry = "binaryauthorization/metadata"

type persistenceFailure struct{}

func (persistenceFailure) Error() string { return "persistence failure" }

func (persistenceFailure) PolicyEvaluationUnavailable() bool { return true }

var (
	ErrInvalidArgument             = errors.New("invalid argument")
	ErrPermissionDenied            = errors.New("permission denied")
	ErrAdmissionDenied             = errors.New("admission denied")
	ErrEvaluationUnsupported       = errors.New("evaluation unsupported")
	ErrPersistence           error = persistenceFailure{}
)

const defaultEnforcementMode = "ENFORCED_BLOCK_AND_AUDIT_LOG"

var (
	projectSegmentPattern = regexp.MustCompile(`^(?:[0-9]+|[a-z][a-z0-9-]*[a-z0-9]|[a-z])$`)
	clusterPattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*\.[a-z0-9][a-z0-9-]*$`)
	attestorPattern       = regexp.MustCompile(`^projects/[^/\s]+/attestors/[^/\s]+$`)
)

func init() {
	state.MustRegisterEntryValidator(stateEntry, validateStateEntry)
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
	EvaluationMode        string   `json:"evaluationMode"`
	EnforcementMode       string   `json:"enforcementMode,omitempty"`
	RequireAttestationsBy []string `json:"requireAttestationsBy,omitempty"`
}

type Policy struct {
	Name                       string                      `json:"name"`
	Description                string                      `json:"description,omitempty"`
	GlobalPolicyEvaluationMode string                      `json:"globalPolicyEvaluationMode,omitempty"`
	AdmissionWhitelistPatterns []AdmissionWhitelistPattern `json:"admissionWhitelistPatterns,omitempty"`
	ClusterAdmissionRules      map[string]AdmissionRule    `json:"clusterAdmissionRules,omitempty"`
	DefaultAdmissionRule       AdmissionRule               `json:"defaultAdmissionRule"`
}

type Decision struct {
	Allowed   bool   `json:"allowed"`
	Outcome   string `json:"outcome"`
	Enforced  bool   `json:"enforced,omitempty"`
	AuditOnly bool   `json:"auditOnly,omitempty"`
	Reason    string `json:"reason"`
	Policy    string `json:"policy,omitempty"`
}

type evaluationUnsupportedError struct {
	reason string
}

func (err evaluationUnsupportedError) Error() string {
	return ErrEvaluationUnsupported.Error() + ": " + err.reason
}

func (evaluationUnsupportedError) Unwrap() error { return ErrEvaluationUnsupported }

func (evaluationUnsupportedError) PolicyEvaluationUnsupported() bool { return true }

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
	restored, err := restorePersistedPolicies(saved.Policies)
	if err != nil {
		return nil, fmt.Errorf("invalid persisted policy: %w", err)
	}
	api.policies = restored
	return api, nil
}

func (api *API) SetPolicy(project string, policy Policy) error {
	if api.initializationErr != nil {
		return fmt.Errorf("%w: Binary Authorization persistence unavailable: %v", ErrPersistence, api.initializationErr)
	}
	policy = normalizePolicy(project, policy)
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
		if err := api.store.Save(stateEntry, persistedMetadata(snapshot)); err != nil {
			return fmt.Errorf("%w: persist Binary Authorization policy: %v", ErrPersistence, err)
		}
	}
	api.mu.Lock()
	api.policies = snapshot
	api.mu.Unlock()
	return nil
}

func (api *API) Evaluate(project, image string) Decision {
	return api.evaluate(project, image, "")
}

func (api *API) evaluate(project, image, cluster string) Decision {
	if api.initializationErr != nil {
		return Decision{Outcome: "UNAVAILABLE", Reason: "policy persistence is unavailable"}
	}
	if !validProject(project) || strings.TrimSpace(image) == "" {
		return Decision{Outcome: "INVALID", Reason: "invalid evaluation request"}
	}
	api.mu.RLock()
	policy, exists := api.policies[project]
	api.mu.RUnlock()
	if !exists {
		return Decision{Outcome: "DENY", Enforced: true, Reason: "policy not found"}
	}
	policy = clonePolicy(policy)
	for _, pattern := range policy.AdmissionWhitelistPatterns {
		if imageMatches(pattern.NamePattern, image) {
			return Decision{Allowed: true, Outcome: "ALLOW", Reason: "image matched admission whitelist", Policy: policy.Name}
		}
	}
	if policy.GlobalPolicyEvaluationMode == "ENABLE" {
		return Decision{Outcome: "UNSUPPORTED", Reason: "global platform policy evaluation is not implemented", Policy: policy.Name}
	}
	if cluster != "" {
		if rule, configured := policy.ClusterAdmissionRules[cluster]; configured {
			if rule.EnforcementMode == "DRYRUN_AUDIT_LOG_ONLY" {
				return Decision{
					Allowed:   true,
					Outcome:   "AUDIT",
					AuditOnly: true,
					Reason:    "cluster admission evaluation is unsupported and not enforced in dry-run mode",
					Policy:    policy.Name,
				}
			}
			return Decision{Outcome: "UNSUPPORTED", Reason: "cluster admission rule evaluation is not implemented", Policy: policy.Name}
		}
	}
	switch policy.DefaultAdmissionRule.EvaluationMode {
	case "ALWAYS_ALLOW":
		return Decision{Allowed: true, Outcome: "ALLOW", Reason: "default admission rule allows image", Policy: policy.Name}
	case "ALWAYS_DENY":
		if policy.DefaultAdmissionRule.EnforcementMode == "DRYRUN_AUDIT_LOG_ONLY" {
			return Decision{
				Allowed:   true,
				Outcome:   "AUDIT",
				AuditOnly: true,
				Reason:    "default admission rule is not enforced in dry-run mode",
				Policy:    policy.Name,
			}
		}
		return Decision{
			Outcome:  "DENY",
			Enforced: true,
			Reason:   "default admission rule denies image",
			Policy:   policy.Name,
		}
	case "REQUIRE_ATTESTATION":
		if policy.DefaultAdmissionRule.EnforcementMode == "DRYRUN_AUDIT_LOG_ONLY" {
			return Decision{
				Allowed:   true,
				Outcome:   "AUDIT",
				AuditOnly: true,
				Reason:    "attestation evaluation is unsupported and not enforced in dry-run mode",
				Policy:    policy.Name,
			}
		}
		return Decision{Outcome: "UNSUPPORTED", Reason: "attestation evaluation is not implemented", Policy: policy.Name}
	default:
		return Decision{Outcome: "UNSUPPORTED", Reason: "unsupported admission evaluation mode", Policy: policy.Name}
	}
}

// EvaluateImage exposes the policy decision through a package-neutral error
// contract so deployment shims can inject an evaluator without importing this
// package or sharing global state.
func (api *API) EvaluateImage(project, image string) error {
	decision := api.Evaluate(project, image)
	switch decision.Outcome {
	case "ALLOW", "AUDIT":
		return nil
	case "UNSUPPORTED":
		return evaluationUnsupportedError{reason: decision.Reason}
	case "UNAVAILABLE":
		return fmt.Errorf("%w: %s", ErrPersistence, decision.Reason)
	default:
		return fmt.Errorf("%w: %s", ErrAdmissionDenied, decision.Reason)
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	project := projectFromPath(request.URL.Path)
	policyPath := "/v1/" + project + "/policy"
	if api.initializationErr != nil &&
		(request.URL.Path == policyPath || request.URL.Path == policyPath+":evaluate") {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Binary Authorization persistence is unavailable")
		return
	}
	switch {
	case request.URL.Path == policyPath && request.Method == http.MethodPut:
		request.Body = http.MaxBytesReader(w, request.Body, 1<<20)
		var policy Policy
		if err := decodeJSON(request, &policy); err != nil {
			if isRequestTooLarge(err) {
				writeError(w, http.StatusRequestEntityTooLarge, "INVALID_ARGUMENT", "policy exceeds 1048576 bytes")
			} else {
				writeError(w, 400, "INVALID_ARGUMENT", "invalid policy")
			}
			return
		}
		if policy.Name == "" {
			policy.Name = project + "/policy"
		}
		if err := validatePolicy(project, policy); err != nil {
			writeError(w, 400, "INVALID_ARGUMENT", err.Error())
			return
		}
		if err := api.SetPolicy(project, policy); err != nil {
			if errors.Is(err, ErrPermissionDenied) {
				writeError(w, 403, "PERMISSION_DENIED", "permission denied")
			} else if errors.Is(err, ErrPersistence) {
				writeError(w, 500, "INTERNAL", "failed to persist Binary Authorization policy")
			} else {
				writeError(w, 400, "INVALID_ARGUMENT", err.Error())
			}
			return
		}
		stored, _ := api.getPolicy(project)
		_ = json.NewEncoder(w).Encode(stored)
	case request.URL.Path == policyPath && request.Method == http.MethodGet:
		policy, ok := api.getPolicy(project)
		if !ok {
			writeError(w, 404, "NOT_FOUND", "policy not found")
			return
		}
		_ = json.NewEncoder(w).Encode(policy)
	case request.URL.Path == policyPath+":evaluate" && request.Method == http.MethodPost:
		request.Body = http.MaxBytesReader(w, request.Body, 1<<20)
		var body struct {
			Image   string `json:"image"`
			Cluster string `json:"cluster,omitempty"`
		}
		if err := decodeJSON(request, &body); err != nil {
			if isRequestTooLarge(err) {
				writeError(w, http.StatusRequestEntityTooLarge, "INVALID_ARGUMENT", "evaluation request exceeds 1048576 bytes")
			} else {
				writeError(w, 400, "INVALID_ARGUMENT", "invalid evaluation request")
			}
			return
		}
		if strings.TrimSpace(body.Image) == "" ||
			(body.Cluster != "" && !clusterPattern.MatchString(body.Cluster)) {
			writeError(w, 400, "INVALID_ARGUMENT", "invalid evaluation request")
			return
		}
		_ = json.NewEncoder(w).Encode(api.evaluate(project, body.Image, body.Cluster))
	default:
		writeError(w, 501, "UNIMPLEMENTED",
			"Binary Authorization attestation and deployment interception are not implemented")
	}
}

func validatePolicy(project string, policy Policy) error {
	if !validProject(project) || policy.Name != project+"/policy" {
		return fmt.Errorf("%w: policy name must match project", ErrInvalidArgument)
	}
	if err := validateAdmissionRule("defaultAdmissionRule", policy.DefaultAdmissionRule); err != nil {
		return err
	}
	switch policy.GlobalPolicyEvaluationMode {
	case "", "ENABLE", "DISABLE":
	default:
		return fmt.Errorf("%w: unsupported global policy evaluation mode", ErrInvalidArgument)
	}
	for _, pattern := range policy.AdmissionWhitelistPatterns {
		if strings.TrimSpace(pattern.NamePattern) != pattern.NamePattern ||
			pattern.NamePattern == "" || strings.Count(pattern.NamePattern, "*") > 1 ||
			(strings.Contains(pattern.NamePattern, "*") && !strings.HasSuffix(pattern.NamePattern, "*")) {
			return fmt.Errorf("%w: invalid image pattern", ErrInvalidArgument)
		}
		if wildcard := strings.IndexByte(pattern.NamePattern, '*'); wildcard >= 0 &&
			!strings.Contains(pattern.NamePattern[:wildcard], "/") {
			return fmt.Errorf("%w: invalid image pattern", ErrInvalidArgument)
		}
	}
	for cluster, rule := range policy.ClusterAdmissionRules {
		if !clusterPattern.MatchString(cluster) {
			return fmt.Errorf("%w: invalid cluster admission rule identifier", ErrInvalidArgument)
		}
		if err := validateAdmissionRule("clusterAdmissionRules", rule); err != nil {
			return err
		}
	}
	return nil
}

func validProject(project string) bool {
	if !strings.HasPrefix(project, "projects/") || strings.Count(project, "/") != 1 {
		return false
	}
	segment := strings.TrimPrefix(project, "projects/")
	return segment != "." && segment != ".." && projectSegmentPattern.MatchString(segment)
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
	if policy.ClusterAdmissionRules != nil {
		clone.ClusterAdmissionRules = make(map[string]AdmissionRule, len(policy.ClusterAdmissionRules))
		for cluster, rule := range policy.ClusterAdmissionRules {
			rule.RequireAttestationsBy = append([]string(nil), rule.RequireAttestationsBy...)
			clone.ClusterAdmissionRules[cluster] = rule
		}
	}
	clone.DefaultAdmissionRule.RequireAttestationsBy =
		append([]string(nil), policy.DefaultAdmissionRule.RequireAttestationsBy...)
	return clone
}

func normalizePolicy(project string, policy Policy) Policy {
	policy = clonePolicy(policy)
	if policy.Name == "" {
		policy.Name = project + "/policy"
	}
	policy.DefaultAdmissionRule = normalizeAdmissionRule(policy.DefaultAdmissionRule)
	for cluster, rule := range policy.ClusterAdmissionRules {
		policy.ClusterAdmissionRules[cluster] = normalizeAdmissionRule(rule)
	}
	return policy
}

func validateStateEntry(_ state.EntryValidationContext, payload json.RawMessage) error {
	var saved metadata
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&saved); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON")
		}
		return err
	}
	_, err := restorePersistedPolicies(saved.Policies)
	return err
}

func validatePersistedPolicy(key string, policy Policy) (string, Policy, error) {
	project := key
	if strings.HasSuffix(project, "/policy") {
		project = strings.TrimSuffix(project, "/policy")
	}
	if !validProject(project) {
		return "", Policy{}, errors.New("invalid persisted policy key")
	}
	policy = normalizePolicy(project, policy)
	if err := validatePolicy(project, policy); err != nil {
		return "", Policy{}, err
	}
	return project, policy, nil
}

func restorePersistedPolicies(policies map[string]Policy) (map[string]Policy, error) {
	restored := make(map[string]Policy, len(policies))
	canonicalKeys := make(map[string]string, len(policies))
	for key, policy := range policies {
		project, policy, err := validatePersistedPolicy(key, policy)
		if err != nil {
			return nil, err
		}
		if _, exists := canonicalKeys[policy.Name]; exists {
			return nil, fmt.Errorf("ambiguous persisted policy aliases canonicalize to %q", policy.Name)
		}
		canonicalKeys[policy.Name] = key
		restored[project] = clonePolicy(policy)
	}
	return restored, nil
}

func persistedMetadata(policies map[string]Policy) metadata {
	saved := metadata{Policies: make(map[string]Policy, len(policies))}
	for _, policy := range policies {
		policy = clonePolicy(policy)
		saved.Policies[policy.Name] = policy
	}
	return saved
}

func normalizeAdmissionRule(rule AdmissionRule) AdmissionRule {
	if rule.EvaluationMode == "DISALLOWED" {
		rule.EvaluationMode = "ALWAYS_DENY"
	}
	if rule.EnforcementMode == "" {
		rule.EnforcementMode = defaultEnforcementMode
	}
	return rule
}

func validateAdmissionRule(field string, rule AdmissionRule) error {
	switch rule.EvaluationMode {
	case "ALWAYS_ALLOW", "ALWAYS_DENY":
		if len(rule.RequireAttestationsBy) != 0 {
			return fmt.Errorf("%w: %s requireAttestationsBy must be empty", ErrInvalidArgument, field)
		}
	case "REQUIRE_ATTESTATION":
		if len(rule.RequireAttestationsBy) == 0 {
			return fmt.Errorf("%w: %s requireAttestationsBy is required", ErrInvalidArgument, field)
		}
	default:
		return fmt.Errorf("%w: unsupported evaluation mode", ErrInvalidArgument)
	}
	switch rule.EnforcementMode {
	case "ENFORCED_BLOCK_AND_AUDIT_LOG", "DRYRUN_AUDIT_LOG_ONLY":
	default:
		return fmt.Errorf("%w: unsupported enforcement mode", ErrInvalidArgument)
	}
	for _, attestor := range rule.RequireAttestationsBy {
		if !attestorPattern.MatchString(attestor) {
			return fmt.Errorf("%w: invalid attestor resource name", ErrInvalidArgument)
		}
	}
	return nil
}

func (api *API) getPolicy(project string) (Policy, bool) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	policy, ok := api.policies[project]
	return clonePolicy(policy), ok
}

func decodeJSON(request *http.Request, target any) error {
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return errors.New("unexpected trailing JSON")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func isRequestTooLarge(err error) bool {
	var maxBytesError *http.MaxBytesError
	return errors.As(err, &maxBytesError)
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": message, "status": status, "details": []any{}},
	})
}
