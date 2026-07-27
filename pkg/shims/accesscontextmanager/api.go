package accesscontextmanager

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
	registry.Register("accesscontextmanager.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Access Context Manager v1)
// ─────────────────────────────────────────────────────────────────────────────

// AccessPolicy represents an access policy.
type AccessPolicy struct {
	Name       string `json:"name"`
	Title      string `json:"title,omitempty"`
	Parent     string `json:"parent"`
	CreateTime string `json:"createTime,omitempty"`
	UpdateTime string `json:"updateTime,omitempty"`
}

// ServicePerimeter represents a service perimeter.
type ServicePerimeter struct {
	Name       string           `json:"name"`
	Title      string           `json:"title,omitempty"`
	Status     *PerimeterStatus `json:"status,omitempty"`
	CreateTime string           `json:"createTime,omitempty"`
	UpdateTime string           `json:"updateTime,omitempty"`
}

// PerimeterStatus holds the perimeter configuration.
type PerimeterStatus struct {
	Resources          []string `json:"resources,omitempty"`
	RestrictedServices []string `json:"restrictedServices,omitempty"`
	AccessLevels       []string `json:"accessLevels,omitempty"`
}

// AccessLevel represents an access level.
type AccessLevel struct {
	Name       string      `json:"name"`
	Title      string      `json:"title,omitempty"`
	Basic      *BasicLevel `json:"basic,omitempty"`
	CreateTime string      `json:"createTime,omitempty"`
	UpdateTime string      `json:"updateTime,omitempty"`
}

// BasicLevel holds basic access level conditions.
type BasicLevel struct {
	Conditions []Condition `json:"conditions,omitempty"`
}

// Condition represents an access level condition.
type Condition struct {
	IpSubnetworks []string `json:"ipSubnetworks,omitempty"`
	Regions       []string `json:"regions,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Access Context Manager v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	stateStore acmStateStore
	policies   map[string]*AccessPolicy
	perimeters map[string]*ServicePerimeter
	levels     map[string]*AccessLevel
	seqNum     int
}

// NewAPI creates a new Access Context Manager API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newAPI(opMgr, state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: AccessContextManager] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: AccessContextManager] state rehydration failed: %v", err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store acmStateStore) *API {
	return &API{
		opMgr:      opMgr,
		stateStore: store,
		policies:   make(map[string]*AccessPolicy),
		perimeters: make(map[string]*ServicePerimeter),
		levels:     make(map[string]*AccessLevel),
	}
}

func newTestAPI() *API {
	return newAPI(orchestrator.NewOperationManager(), nil)
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: AccessContextManager] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.HasSuffix(r.URL.Path, ":checkAccess") && r.Method == http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var request AccessRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid access decision request")
			return
		}
		_ = json.NewEncoder(w).Encode(api.CheckAccess(request))
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	// Access Levels
	case strings.HasSuffix(r.URL.Path, "/accessLevels") && r.Method == http.MethodPost:
		api.createAccessLevel(w, r)
	case strings.HasSuffix(r.URL.Path, "/accessLevels") && r.Method == http.MethodGet:
		api.listAccessLevels(w, r)
	case strings.Contains(r.URL.Path, "/accessLevels/") && r.Method == http.MethodGet:
		api.getAccessLevel(w, r)
	case strings.Contains(r.URL.Path, "/accessLevels/") && r.Method == http.MethodPatch:
		api.patchAccessLevel(w, r)
	case strings.Contains(r.URL.Path, "/accessLevels/") && r.Method == http.MethodDelete:
		api.deleteAccessLevel(w, r)
	// Service Perimeters
	case strings.HasSuffix(r.URL.Path, "/servicePerimeters") && r.Method == http.MethodPost:
		api.createServicePerimeter(w, r)
	case strings.HasSuffix(r.URL.Path, "/servicePerimeters") && r.Method == http.MethodGet:
		api.listServicePerimeters(w, r)
	case strings.Contains(r.URL.Path, "/servicePerimeters/") && r.Method == http.MethodGet:
		api.getServicePerimeter(w, r)
	case strings.Contains(r.URL.Path, "/servicePerimeters/") && r.Method == http.MethodPatch:
		api.patchServicePerimeter(w, r)
	case strings.Contains(r.URL.Path, "/servicePerimeters/") && r.Method == http.MethodDelete:
		api.deleteServicePerimeter(w, r)
	// Access Policies
	case r.URL.Path == "/v1/accessPolicies" && r.Method == http.MethodPost:
		api.createAccessPolicy(w, r)
	case r.URL.Path == "/v1/accessPolicies" && r.Method == http.MethodGet:
		api.listAccessPolicies(w, r)
	case isAccessPolicyResource(r.URL.Path) && r.Method == http.MethodGet:
		api.getAccessPolicy(w, r)
	case isAccessPolicyResource(r.URL.Path) && r.Method == http.MethodPatch:
		api.patchAccessPolicy(w, r)
	case isAccessPolicyResource(r.URL.Path) && r.Method == http.MethodDelete:
		api.deleteAccessPolicy(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Access Context Manager resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Access Policy handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createAccessPolicy(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var policy AccessPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if policy.Parent == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'parent' is required")
		return
	}
	if !strings.HasPrefix(policy.Parent, "organizations/") {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "parent must be in format organizations/{orgId}")
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	api.mu.Lock()
	api.seqNum++
	name := fmt.Sprintf("accessPolicies/%d", api.seqNum)
	policy.Name = name
	policy.CreateTime = now
	policy.UpdateTime = now
	api.policies[name] = &policy
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &policy, func() {
		api.mu.Lock()
		delete(api.policies, name)
		api.mu.Unlock()
	})
}

func (api *API) getAccessPolicy(w http.ResponseWriter, r *http.Request) {
	name := extractPolicyName(r.URL.Path)

	api.mu.RLock()
	policy, ok := api.policies[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "access policy not found: "+name)
		return
	}
	clone := clonePolicy(policy)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listAccessPolicies(w http.ResponseWriter, r *http.Request) {
	pageSize := 50
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
	var all []*AccessPolicy
	for _, p := range api.policies {
		all = append(all, clonePolicy(p))
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "accesscontextmanager.accessPolicies",
		Parent:  r.URL.Query().Get("parent"),
	}, func(policy *AccessPolicy) string { return policy.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*AccessPolicy{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"accessPolicies": result,
		"nextPageToken":  nextToken,
	})
}

func (api *API) patchAccessPolicy(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := extractPolicyName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.policies[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "access policy not found: "+name)
		return
	}

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
	merged["parent"] = existing.Parent
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated AccessPolicy
	_ = json.Unmarshal(updatedRaw, &updated)
	before := clonePolicy(existing)
	api.policies[name] = &updated
	api.mu.Unlock()

	api.finishMutation(w, "update", name, &updated, func() {
		api.mu.Lock()
		api.policies[name] = before
		api.mu.Unlock()
	})
}

func (api *API) deleteAccessPolicy(w http.ResponseWriter, r *http.Request) {
	name := extractPolicyName(r.URL.Path)

	api.mu.Lock()
	policy, exists := api.policies[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "access policy not found: "+name)
		return
	}
	delete(api.policies, name)
	// Cascade delete perimeters and levels under this policy
	deletedPerimeters := make(map[string]*ServicePerimeter)
	for key, v := range api.perimeters {
		if strings.HasPrefix(key, name+"/") {
			deletedPerimeters[key] = v
			delete(api.perimeters, key)
		}
	}
	deletedLevels := make(map[string]*AccessLevel)
	for key, v := range api.levels {
		if strings.HasPrefix(key, name+"/") {
			deletedLevels[key] = v
			delete(api.levels, key)
		}
	}
	api.mu.Unlock()

	api.finishMutation(w, "delete", name, map[string]any{}, func() {
		api.mu.Lock()
		api.policies[name] = policy
		for k, v := range deletedPerimeters {
			api.perimeters[k] = v
		}
		for k, v := range deletedLevels {
			api.levels[k] = v
		}
		api.mu.Unlock()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Service Perimeter handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createServicePerimeter(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	policyName := extractPolicyName(r.URL.Path)

	api.mu.RLock()
	_, policyExists := api.policies[policyName]
	api.mu.RUnlock()
	if !policyExists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent access policy not found: "+policyName)
		return
	}

	var perimeter ServicePerimeter
	if err := json.NewDecoder(r.Body).Decode(&perimeter); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	perimeterID := r.URL.Query().Get("servicePerimeterId")
	if perimeterID == "" {
		// Use title as ID if no explicit ID
		if perimeter.Title != "" {
			perimeterID = perimeter.Title
		} else {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "servicePerimeterId or title is required")
			return
		}
	}

	name := fmt.Sprintf("%s/servicePerimeters/%s", policyName, perimeterID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	perimeter.Name = name
	perimeter.CreateTime = now
	perimeter.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.perimeters[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "service perimeter already exists: "+perimeterID)
		return
	}
	api.perimeters[name] = &perimeter
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &perimeter, func() {
		api.mu.Lock()
		delete(api.perimeters, name)
		api.mu.Unlock()
	})
}

func (api *API) getServicePerimeter(w http.ResponseWriter, r *http.Request) {
	name := buildPerimeterName(r.URL.Path)

	api.mu.RLock()
	perimeter, ok := api.perimeters[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "service perimeter not found: "+name)
		return
	}
	clone := clonePerimeter(perimeter)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listServicePerimeters(w http.ResponseWriter, r *http.Request) {
	policyName := extractPolicyName(r.URL.Path)
	prefix := policyName + "/servicePerimeters/"

	pageSize := 50
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
	var all []*ServicePerimeter
	for key, p := range api.perimeters {
		if strings.HasPrefix(key, prefix) {
			all = append(all, clonePerimeter(p))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "accesscontextmanager.servicePerimeters",
		Parent:  policyName,
	}, func(perimeter *ServicePerimeter) string { return perimeter.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*ServicePerimeter{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"servicePerimeters": result,
		"nextPageToken":     nextToken,
	})
}

func (api *API) patchServicePerimeter(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildPerimeterName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.perimeters[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "service perimeter not found: "+name)
		return
	}

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	raw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(raw, &merged)

	for k, v := range patch {
		merged[k] = v
	}
	merged["name"] = existing.Name
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated ServicePerimeter
	_ = json.Unmarshal(updatedRaw, &updated)
	before := clonePerimeter(existing)
	api.perimeters[name] = &updated
	api.mu.Unlock()

	api.finishMutation(w, "update", name, &updated, func() {
		api.mu.Lock()
		api.perimeters[name] = before
		api.mu.Unlock()
	})
}

func (api *API) deleteServicePerimeter(w http.ResponseWriter, r *http.Request) {
	name := buildPerimeterName(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.perimeters[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "service perimeter not found: "+name)
		return
	}
	delete(api.perimeters, name)
	api.mu.Unlock()

	api.finishMutation(w, "delete", name, map[string]any{}, func() {
		api.mu.Lock()
		api.perimeters[name] = resource
		api.mu.Unlock()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Access Level handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createAccessLevel(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	policyName := extractPolicyName(r.URL.Path)

	api.mu.RLock()
	_, policyExists := api.policies[policyName]
	api.mu.RUnlock()
	if !policyExists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent access policy not found: "+policyName)
		return
	}

	var level AccessLevel
	if err := json.NewDecoder(r.Body).Decode(&level); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	levelID := r.URL.Query().Get("accessLevelId")
	if levelID == "" {
		if level.Title != "" {
			levelID = level.Title
		} else {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "accessLevelId or title is required")
			return
		}
	}

	name := fmt.Sprintf("%s/accessLevels/%s", policyName, levelID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	level.Name = name
	level.CreateTime = now
	level.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.levels[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "access level already exists: "+levelID)
		return
	}
	api.levels[name] = &level
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &level, func() {
		api.mu.Lock()
		delete(api.levels, name)
		api.mu.Unlock()
	})
}

func (api *API) getAccessLevel(w http.ResponseWriter, r *http.Request) {
	name := buildLevelName(r.URL.Path)

	api.mu.RLock()
	level, ok := api.levels[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "access level not found: "+name)
		return
	}
	clone := cloneLevel(level)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listAccessLevels(w http.ResponseWriter, r *http.Request) {
	policyName := extractPolicyName(r.URL.Path)
	prefix := policyName + "/accessLevels/"

	pageSize := 50
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
	var all []*AccessLevel
	for key, l := range api.levels {
		if strings.HasPrefix(key, prefix) {
			all = append(all, cloneLevel(l))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "accesscontextmanager.accessLevels",
		Parent:  policyName,
	}, func(level *AccessLevel) string { return level.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*AccessLevel{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"accessLevels":  result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchAccessLevel(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildLevelName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.levels[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "access level not found: "+name)
		return
	}

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	raw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(raw, &merged)

	for k, v := range patch {
		merged[k] = v
	}
	merged["name"] = existing.Name
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated AccessLevel
	_ = json.Unmarshal(updatedRaw, &updated)
	before := cloneLevel(existing)
	api.levels[name] = &updated
	api.mu.Unlock()

	api.finishMutation(w, "update", name, &updated, func() {
		api.mu.Lock()
		api.levels[name] = before
		api.mu.Unlock()
	})
}

func (api *API) deleteAccessLevel(w http.ResponseWriter, r *http.Request) {
	name := buildLevelName(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.levels[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "access level not found: "+name)
		return
	}
	delete(api.levels, name)
	api.mu.Unlock()

	api.finishMutation(w, "delete", name, map[string]any{}, func() {
		api.mu.Lock()
		api.levels[name] = resource
		api.mu.Unlock()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Operation handler
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "accesscontextmanager#operation")
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
}

func (api *API) finishMutation(
	w http.ResponseWriter,
	verb, target string,
	result any,
	rollback func(),
) {
	op, err := api.opMgr.RegisterScopedTargetDurable(
		"accesscontextmanager#operation", verb, target,
	)
	if err != nil {
		rollback()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to register operation")
		return
	}
	compensate := func() {
		rollback()
		rollbackStateErr := api.persistState()
		rollbackOperationErr := api.opMgr.RollbackScopedRegistration(op.Name)
		if rollbackStateErr != nil || rollbackOperationErr != nil {
			log.Printf("[Shim: AccessContextManager] mutation compensation degraded: state=%v operation=%v",
				rollbackStateErr, rollbackOperationErr)
		}
	}
	if err := api.persistState(); err != nil {
		compensate()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	response, err := json.Marshal(result)
	if err != nil {
		compensate()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to encode operation response")
		return
	}
	if err := api.opMgr.FinalizeScopedDurable(op.Name, response, 0, ""); err != nil {
		compensate()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Operation persistence failed")
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func extractPolicyName(path string) string {
	// Paths: /v1/accessPolicies/{id} or /v1/accessPolicies/{id}/...
	trimmed := strings.TrimPrefix(path, "/v1/")
	parts := strings.Split(trimmed, "/")
	if len(parts) >= 2 && parts[0] == "accessPolicies" {
		return "accessPolicies/" + parts[1]
	}
	return ""
}

func buildPerimeterName(path string) string {
	policyName := extractPolicyName(path)
	perimeterID := extractAfter(path, "servicePerimeters")
	return fmt.Sprintf("%s/servicePerimeters/%s", policyName, perimeterID)
}

func buildLevelName(path string) string {
	policyName := extractPolicyName(path)
	levelID := extractAfter(path, "accessLevels")
	return fmt.Sprintf("%s/accessLevels/%s", policyName, levelID)
}

func isAccessPolicyResource(path string) bool {
	trimmed := strings.TrimPrefix(path, "/v1/")
	parts := strings.Split(trimmed, "/")
	// accessPolicies/{id} with no further sub-resources
	return len(parts) == 2 && parts[0] == "accessPolicies"
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
