package orgpolicy

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("orgpolicy.googleapis.com", func(_ *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resources (Organization Policy v2 contract)
// ─────────────────────────────────────────────────────────────────────────────

// PolicySpec represents the spec of an organization policy.
type PolicySpec struct {
	Rules      []PolicyRule `json:"rules,omitempty"`
	UpdateTime string       `json:"updateTime,omitempty"`
}

// PolicyRule represents a single rule in a policy spec.
type PolicyRule struct {
	Enforce bool `json:"enforce,omitempty"`
}

// Policy represents a google.cloud.orgpolicy.v2.Policy resource.
type Policy struct {
	Name       string      `json:"name"`
	Spec       *PolicySpec `json:"spec,omitempty"`
	Alternate  any         `json:"alternate,omitempty"`
	DryRunSpec any         `json:"dryRunSpec,omitempty"`
}

// Constraint represents a google.cloud.orgpolicy.v2.Constraint resource (read-only).
type Constraint struct {
	Name              string `json:"name"`
	DisplayName       string `json:"displayName,omitempty"`
	Description       string `json:"description,omitempty"`
	ConstraintDefault string `json:"constraintDefault,omitempty"`
	BooleanConstraint any    `json:"booleanConstraint,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Organization Policy v2 REST shim.
type API struct {
	mu          sync.RWMutex
	persistMu   sync.Mutex
	stateStore  orgPolicyStateStore
	policies    map[string]*Policy
	constraints map[string]*Constraint
}

// NewAPI creates a new Org Policy shim with persistence.
func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		stateStore:  state.NewGuardedEntryStore(store, err),
		policies:    make(map[string]*Policy),
		constraints: make(map[string]*Constraint),
	}
	api.seedConstraints()
	if err != nil {
		log.Printf("[Shim: Org Policy] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Org Policy] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	api := &API{
		policies:    make(map[string]*Policy),
		constraints: make(map[string]*Constraint),
	}
	api.seedConstraints()
	return api
}

func (api *API) seedConstraints() {
	defaults := []Constraint{
		{Name: "constraints/compute.disableSerialPortAccess", DisplayName: "Disable VM serial port access", Description: "Disable VM serial port access", ConstraintDefault: "ALLOW", BooleanConstraint: map[string]any{}},
		{Name: "constraints/compute.requireOsLogin", DisplayName: "Require OS Login", Description: "Require OS Login", ConstraintDefault: "ALLOW", BooleanConstraint: map[string]any{}},
		{Name: "constraints/iam.disableServiceAccountKeyCreation", DisplayName: "Disable SA key creation", Description: "Disable service account key creation", ConstraintDefault: "ALLOW", BooleanConstraint: map[string]any{}},
		{Name: "constraints/storage.uniformBucketLevelAccess", DisplayName: "Uniform bucket access", Description: "Enforce uniform bucket-level access", ConstraintDefault: "ALLOW", BooleanConstraint: map[string]any{}},
		{Name: "constraints/compute.restrictVpcPeering", DisplayName: "Restrict VPC peering", Description: "Restrict VPC peering", ConstraintDefault: "ALLOW", BooleanConstraint: map[string]any{}},
	}
	for i := range defaults {
		api.constraints[defaults[i].Name] = &defaults[i]
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !canonicalOrgPolicyRequestPath(r) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "non-canonical Org Policy request path")
		return
	}
	log.Printf("[Shim: Org Policy] %s %s", r.Method, r.URL.Path)

	path := r.URL.Path

	switch {
	case strings.HasSuffix(path, ":evaluate") && r.Method == http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var request struct {
			Resource   string   `json:"resource"`
			Constraint string   `json:"constraint"`
			Ancestors  []string `json:"ancestors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid evaluation request")
			return
		}
		decision, err := api.Evaluate(request.Resource, request.Constraint, request.Ancestors)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		_ = json.NewEncoder(w).Encode(decision)
	case strings.Contains(path, "/constraints"):
		api.routeConstraints(w, r)
	case strings.Contains(path, "/policies"):
		api.routePolicies(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Org Policy resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Constraints (read-only, pre-seeded)
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeConstraints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	// List constraints: GET /v2/{parent}/constraints
	api.listConstraints(w, r)
}

func (api *API) listConstraints(w http.ResponseWriter, r *http.Request) {
	pageSize := 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}

	api.mu.RLock()
	all := make([]*Constraint, 0, len(api.constraints))
	for _, c := range api.constraints {
		all = append(all, c)
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, r.URL.Query().Get("pageToken"), pagination.Scope{
		Service: "orgpolicy.constraints",
		Parent:  strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v2/"), "/constraints"),
	}, func(constraint *Constraint) string { return constraint.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"constraints":   result,
		"nextPageToken": nextToken,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Policies
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routePolicies(w http.ResponseWriter, r *http.Request) {
	collection, valid := canonicalPolicyRoute(r.URL.Path)
	if !valid ||
		(r.Method == http.MethodPost && !collection) ||
		((r.Method == http.MethodPatch || r.Method == http.MethodDelete) && collection) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid canonical policy route")
		return
	}
	switch r.Method {
	case http.MethodPost:
		api.createPolicy(w, r)
	case http.MethodGet:
		if isCollectionPath(r.URL.Path) {
			api.listPolicies(w, r)
		} else {
			api.getPolicy(w, r)
		}
	case http.MethodPatch:
		api.patchPolicy(w, r)
	case http.MethodDelete:
		api.deletePolicy(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func canonicalOrgPolicyRequestPath(request *http.Request) bool {
	if request.URL.RawPath != "" || strings.Contains(request.URL.EscapedPath(), "%") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/"), "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.Contains(part, `\`) {
			return false
		}
		for _, character := range part {
			if character < 0x20 || character == 0x7f {
				return false
			}
		}
	}
	return true
}

func canonicalPolicyRoute(path string) (collection, valid bool) {
	if !strings.HasPrefix(path, "/v2/") {
		return false, false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/v2/"), "/")
	if len(parts) != 3 && len(parts) != 4 {
		return false, false
	}
	if parts[0] != "projects" || !validPolicyScopeSegment(parts[1]) || parts[2] != "policies" {
		return false, false
	}
	if len(parts) == 3 {
		return true, true
	}
	return false, validPolicyID(parts[3])
}

func (api *API) createPolicy(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var resource Policy
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if resource.Name == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "policy name is required in body")
		return
	}

	_, ok := policyConstraintForRequest(r.URL.Path, resource.Name)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"policy name must exactly match the request project")
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if resource.Spec != nil {
		resource.Spec.UpdateTime = now
	}

	api.mu.Lock()
	if _, exists := api.policies[resource.Name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "policy already exists: "+resource.Name)
		return
	}
	api.policies[resource.Name] = &resource
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.policies, resource.Name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(deepCopyPolicy(&resource))
}

func (api *API) getPolicy(w http.ResponseWriter, r *http.Request) {
	name := parsePolicyName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.policies[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "policy not found: "+name)
		return
	}
	clone := deepCopyPolicy(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listPolicies(w http.ResponseWriter, r *http.Request) {
	parent := parsePolicyParent(r.URL.Path)
	prefix := parent + "/policies/"

	pageSize := 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}

	api.mu.RLock()
	all := make([]*Policy, 0)
	for name, p := range api.policies {
		if strings.HasPrefix(name, prefix) {
			all = append(all, deepCopyPolicy(p))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, r.URL.Query().Get("pageToken"), pagination.Scope{
		Service: "orgpolicy.policies",
		Parent:  parent,
	}, func(policy *Policy) string { return policy.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = make([]*Policy, 0)
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"policies":      result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchPolicy(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parsePolicyName(r.URL.Path)
	if _, ok := policyConstraintForRequest(r.URL.Path, name); !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid canonical policy name")
		return
	}

	api.mu.Lock()
	existing, ok := api.policies[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "policy not found: "+name)
		return
	}

	var patch Policy
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if patch.Spec != nil {
		existing.Spec = patch.Spec
		existing.Spec.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if patch.Alternate != nil {
		existing.Alternate = patch.Alternate
	}
	if patch.DryRunSpec != nil {
		existing.DryRunSpec = patch.DryRunSpec
	}
	clone := deepCopyPolicy(existing)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) deletePolicy(w http.ResponseWriter, r *http.Request) {
	name := parsePolicyName(r.URL.Path)
	if _, ok := policyConstraintForRequest(r.URL.Path, name); !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid canonical policy name")
		return
	}

	api.mu.Lock()
	resource, exists := api.policies[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "policy not found: "+name)
		return
	}
	delete(api.policies, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.policies[name] = resource
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// parsePolicyName extracts the full policy name from a v2 path.
// Path: /v2/projects/{p}/policies/{constraintName}
func parsePolicyName(path string) string {
	// Strip /v2/ prefix
	trimmed := strings.TrimPrefix(path, "/v2/")
	trimmed = strings.TrimPrefix(trimmed, "v2/")
	return trimmed
}

// parsePolicyParent extracts the parent from a v2 policies list path.
// Path: /v2/projects/{p}/policies → parent = projects/{p}
func parsePolicyParent(path string) string {
	trimmed := strings.TrimPrefix(path, "/v2/")
	trimmed = strings.TrimPrefix(trimmed, "v2/")
	idx := strings.LastIndex(trimmed, "/policies")
	if idx < 0 {
		return trimmed
	}
	return trimmed[:idx]
}

// isCollectionPath returns true if the path ends with /policies (list endpoint).
func isCollectionPath(path string) bool {
	trimmed := strings.TrimPrefix(path, "/v2/")
	trimmed = strings.TrimPrefix(trimmed, "v2/")
	// If path is like projects/{p}/policies (no trailing segment after policies)
	parts := strings.Split(trimmed, "/")
	return len(parts) >= 2 && parts[len(parts)-1] == "policies"
}

func policyConstraintForRequest(path, policyName string) (string, bool) {
	parent := parsePolicyParent(path)
	parentParts := strings.Split(parent, "/")
	if len(parentParts) != 2 || parentParts[0] != "projects" ||
		!validPolicyScopeSegment(parentParts[1]) {
		return "", false
	}
	prefix := parent + "/policies/"
	if !strings.HasPrefix(policyName, prefix) {
		return "", false
	}
	policyID := strings.TrimPrefix(policyName, prefix)
	if !validPolicyID(policyID) {
		return "", false
	}
	return "constraints/" + policyID, true
}

func validPolicyID(policyID string) bool {
	if policyID == "" || !policyIDAlphaNumeric(rune(policyID[0])) ||
		!policyIDAlphaNumeric(rune(policyID[len(policyID)-1])) {
		return false
	}
	previousSeparator := false
	for _, character := range policyID {
		if policyIDAlphaNumeric(character) {
			previousSeparator = false
			continue
		}
		if character != '.' && character != '_' && character != '-' {
			return false
		}
		if previousSeparator {
			return false
		}
		previousSeparator = true
	}
	return true
}

func policyIDAlphaNumeric(character rune) bool {
	return (character >= 'a' && character <= 'z') ||
		(character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9')
}

func validPolicyScopeSegment(segment string) bool {
	if segment == "" || segment == "." || segment == ".." {
		return false
	}
	for index, character := range segment {
		alphaNumeric := (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9')
		if !alphaNumeric && character != '-' {
			return false
		}
		if (index == 0 || index == len(segment)-1) && !alphaNumeric {
			return false
		}
	}
	return true
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}
