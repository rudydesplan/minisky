package networksecurity

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("networksecurity.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Network Security v1)
// ─────────────────────────────────────────────────────────────────────────────

// AuthorizationPolicy represents a Network Security authorization policy.
type AuthorizationPolicy struct {
	Name        string            `json:"name"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
	Description string            `json:"description,omitempty"`
	Action      string            `json:"action"`
	Rules       []Rule            `json:"rules,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// Rule represents an authorization rule.
type Rule struct {
	Sources      []Source      `json:"sources,omitempty"`
	Destinations []Destination `json:"destinations,omitempty"`
}

// Source represents a traffic source.
type Source struct {
	Principals []string `json:"principals,omitempty"`
	IpBlocks   []string `json:"ipBlocks,omitempty"`
}

// Destination represents a traffic destination.
type Destination struct {
	Hosts   []string `json:"hosts,omitempty"`
	Ports   []int    `json:"ports,omitempty"`
	Methods []string `json:"methods,omitempty"`
	Paths   []string `json:"paths,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Network Security v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	stateStore networksecurityStateStore
	policies   map[string]*AuthorizationPolicy
}

// NewAPI creates a new Network Security API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newAPI(opMgr, state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: NetworkSecurity] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: NetworkSecurity] state rehydration failed: %v", err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store networksecurityStateStore) *API {
	return &API{
		opMgr:      opMgr,
		stateStore: store,
		policies:   make(map[string]*AuthorizationPolicy),
	}
}

func newTestAPI() *API {
	return newAPI(orchestrator.NewOperationManager(), nil)
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: NetworkSecurity] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Enforcement", "metadata-only")

	switch {
	case strings.HasSuffix(r.URL.Path, ":evaluate") && r.Method == http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var request EvaluationRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid evaluation request")
			return
		}
		_ = json.NewEncoder(w).Encode(api.Evaluate(request))
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.HasSuffix(r.URL.Path, "/authorizationPolicies") && r.Method == http.MethodPost:
		api.createPolicy(w, r)
	case strings.HasSuffix(r.URL.Path, "/authorizationPolicies") && r.Method == http.MethodGet:
		api.listPolicies(w, r)
	case strings.Contains(r.URL.Path, "/authorizationPolicies/") && r.Method == http.MethodGet:
		api.getPolicy(w, r)
	case strings.Contains(r.URL.Path, "/authorizationPolicies/") && r.Method == http.MethodPatch:
		api.patchPolicy(w, r)
	case strings.Contains(r.URL.Path, "/authorizationPolicies/") && r.Method == http.MethodDelete:
		api.deletePolicy(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Network Security resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

var validActions = map[string]bool{
	"ALLOW": true,
	"DENY":  true,
}

func (api *API) createPolicy(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	policyID := r.URL.Query().Get("authorizationPolicyId")
	if policyID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "authorizationPolicyId query parameter is required")
		return
	}

	var policy AuthorizationPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if policy.Action == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'action' is required")
		return
	}
	if !validActions[policy.Action] {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid action: "+policy.Action+", must be ALLOW or DENY")
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/authorizationPolicies/%s", project, location, policyID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	policy.Name = name
	policy.CreateTime = now
	policy.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.policies[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "authorization policy already exists: "+policyID)
		return
	}
	api.policies[name] = &policy
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &policy, true, func() {
		api.mu.Lock()
		delete(api.policies, name)
		api.mu.Unlock()
	})
}

func (api *API) getPolicy(w http.ResponseWriter, r *http.Request) {
	name := buildPolicyName(r.URL.Path)

	api.mu.RLock()
	policy, ok := api.policies[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "authorization policy not found: "+name)
		return
	}
	clone := clonePolicy(policy)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listPolicies(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/authorizationPolicies/", project, location)

	pageSize := 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}
	pageToken := r.URL.Query().Get("pageToken")

	api.mu.RLock()
	var all []*AuthorizationPolicy
	for key, p := range api.policies {
		if strings.HasPrefix(key, prefix) {
			all = append(all, clonePolicy(p))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "networksecurity.authorizationPolicies",
		Parent:  strings.TrimSuffix(prefix, "/authorizationPolicies/"),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(policy *AuthorizationPolicy) string { return policy.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*AuthorizationPolicy{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"authorizationPolicies": result,
		"nextPageToken":         nextToken,
	})
}

func (api *API) patchPolicy(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildPolicyName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.policies[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "authorization policy not found: "+name)
		return
	}
	before := clonePolicy(existing)

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	raw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(raw, &merged)

	updateMask := r.URL.Query().Get("updateMask")
	if updateMask != "" {
		fields := strings.Split(updateMask, ",")
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if v, exists := patch[field]; exists {
				merged[field] = v
			}
		}
	} else {
		for k, v := range patch {
			merged[k] = v
		}
	}

	merged["name"] = existing.Name
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated AuthorizationPolicy
	_ = json.Unmarshal(updatedRaw, &updated)
	api.policies[name] = &updated
	api.mu.Unlock()

	api.finishMutation(w, "update", name, &updated, false, func() {
		api.mu.Lock()
		api.policies[name] = before
		api.mu.Unlock()
	})
}

func (api *API) deletePolicy(w http.ResponseWriter, r *http.Request) {
	name := buildPolicyName(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.policies[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "authorization policy not found: "+name)
		return
	}
	delete(api.policies, name)
	api.mu.Unlock()

	api.finishMutation(w, "delete", name, map[string]any{}, false, func() {
		api.mu.Lock()
		api.policies[name] = resource
		api.mu.Unlock()
	})
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "networksecurity#operation")
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
}

func (api *API) finishMutation(w http.ResponseWriter, verb, target string, response any, async bool, rollback func()) {
	op, err := api.opMgr.RegisterScopedTargetDurable("networksecurity#operation", verb, target)
	if err != nil {
		rollback()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "failed to register operation")
		return
	}
	compensate := func() {
		rollback()
		stateErr := api.persistState()
		operationErr := api.opMgr.RollbackScopedRegistration(op.Name)
		if stateErr != nil {
			api.opMgr.MarkPersistenceFailure(stateErr)
		}
		if stateErr != nil || operationErr != nil {
			log.Printf("[Shim: NetworkSecurity] mutation compensation degraded: state=%v operation=%v", stateErr, operationErr)
		}
	}
	if err := api.persistState(); err != nil {
		compensate()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		compensate()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to encode operation response")
		return
	}
	if async {
		go func() {
			time.Sleep(50 * time.Millisecond)
			if err := api.opMgr.FinalizeScopedDurable(op.Name, encoded, 0, ""); err != nil {
				log.Printf("[Shim: NetworkSecurity] terminal operation persistence degraded: %v", err)
			}
		}()
		_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
		return
	}
	if err := api.opMgr.FinalizeScopedDurable(op.Name, encoded, 0, ""); err != nil {
		compensate()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "operation persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func parseParent(path string) (project, location string, ok bool) {
	project = extractAfter(path, "projects")
	location = extractAfter(path, "locations")
	if project == "" || location == "" {
		return "", "", false
	}
	return project, location, true
}

func buildPolicyName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	policyID := extractAfter(path, "authorizationPolicies")
	return fmt.Sprintf("projects/%s/locations/%s/authorizationPolicies/%s", project, location, policyID)
}

func extractAfter(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
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
