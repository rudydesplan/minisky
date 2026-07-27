package servicemesh

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
	registry.Register("networkservices.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Network Services / Service Mesh v1)
// ─────────────────────────────────────────────────────────────────────────────

// Mesh represents a service mesh resource.
type Mesh struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// HttpRoute represents an HTTP route resource.
type HttpRoute struct {
	Name       string            `json:"name"`
	Hostnames  []string          `json:"hostnames,omitempty"`
	Meshes     []string          `json:"meshes,omitempty"`
	Rules      []RouteRule       `json:"rules,omitempty"`
	CreateTime string            `json:"createTime,omitempty"`
	UpdateTime string            `json:"updateTime,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// RouteRule represents a routing rule.
type RouteRule struct {
	Matches []RouteMatch `json:"matches,omitempty"`
	Action  *RouteAction `json:"action,omitempty"`
}

// RouteMatch represents a match condition.
type RouteMatch struct {
	FullPathMatch string `json:"fullPathMatch,omitempty"`
	PrefixMatch   string `json:"prefixMatch,omitempty"`
	RegexMatch    string `json:"regexMatch,omitempty"`
}

// RouteAction represents the action to take for matched traffic.
type RouteAction struct {
	Destinations []RouteDestination `json:"destinations,omitempty"`
}

// RouteDestination represents a traffic destination.
type RouteDestination struct {
	ServiceName string `json:"serviceName,omitempty"`
	Weight      int    `json:"weight,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Network Services (Service Mesh) v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	stateStore servicemeshStateStore
	meshes     map[string]*Mesh
	httpRoutes map[string]*HttpRoute
}

// NewAPI creates a new Service Mesh API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newAPI(opMgr, state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: ServiceMesh] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: ServiceMesh] state rehydration failed: %v", err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store servicemeshStateStore) *API {
	return &API{
		opMgr:      opMgr,
		stateStore: store,
		meshes:     make(map[string]*Mesh),
		httpRoutes: make(map[string]*HttpRoute),
	}
}

func newTestAPI() *API {
	return newAPI(orchestrator.NewOperationManager(), nil)
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: ServiceMesh] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Enforcement", "metadata-only")

	switch {
	case strings.HasSuffix(r.URL.Path, ":resolve") && r.Method == http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var request struct {
			Project  string `json:"project"`
			Location string `json:"location"`
			Host     string `json:"host"`
			Path     string `json:"path"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid route decision request")
			return
		}
		_ = json.NewEncoder(w).Encode(api.ResolveRoute(request.Project, request.Location, request.Host, request.Path))
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	// HttpRoutes
	case strings.HasSuffix(r.URL.Path, "/httpRoutes") && r.Method == http.MethodPost:
		api.createHttpRoute(w, r)
	case strings.HasSuffix(r.URL.Path, "/httpRoutes") && r.Method == http.MethodGet:
		api.listHttpRoutes(w, r)
	case strings.Contains(r.URL.Path, "/httpRoutes/") && r.Method == http.MethodGet:
		api.getHttpRoute(w, r)
	case strings.Contains(r.URL.Path, "/httpRoutes/") && r.Method == http.MethodPatch:
		api.patchHttpRoute(w, r)
	case strings.Contains(r.URL.Path, "/httpRoutes/") && r.Method == http.MethodDelete:
		api.deleteHttpRoute(w, r)
	// Meshes
	case strings.HasSuffix(r.URL.Path, "/meshes") && r.Method == http.MethodPost:
		api.createMesh(w, r)
	case strings.HasSuffix(r.URL.Path, "/meshes") && r.Method == http.MethodGet:
		api.listMeshes(w, r)
	case strings.Contains(r.URL.Path, "/meshes/") && r.Method == http.MethodGet:
		api.getMesh(w, r)
	case strings.Contains(r.URL.Path, "/meshes/") && r.Method == http.MethodPatch:
		api.patchMesh(w, r)
	case strings.Contains(r.URL.Path, "/meshes/") && r.Method == http.MethodDelete:
		api.deleteMesh(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Network Services resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Mesh handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createMesh(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	meshID := r.URL.Query().Get("meshId")
	if meshID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "meshId query parameter is required")
		return
	}

	var mesh Mesh
	if err := json.NewDecoder(r.Body).Decode(&mesh); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/meshes/%s", project, location, meshID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	mesh.Name = name
	mesh.CreateTime = now
	mesh.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.meshes[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "mesh already exists: "+meshID)
		return
	}
	api.meshes[name] = &mesh
	api.mu.Unlock()

	// Register LRO first (if fails → rollback map, return 503)
	op, err := api.opMgr.RegisterDurable("networkservices#operation", "create", name, "", location)
	if err != nil {
		api.mu.Lock()
		delete(api.meshes, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	// Then persist (if fails → rollback map, return 503)
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.meshes, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": false,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.networkservices.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getMesh(w http.ResponseWriter, r *http.Request) {
	name := buildMeshName(r.URL.Path)

	api.mu.RLock()
	mesh, ok := api.meshes[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "mesh not found: "+name)
		return
	}
	clone := cloneMesh(mesh)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listMeshes(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/meshes/", project, location)

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
	var all []*Mesh
	for key, m := range api.meshes {
		if strings.HasPrefix(key, prefix) {
			all = append(all, cloneMesh(m))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "networkservices.meshes",
		Parent:  strings.TrimSuffix(prefix, "/meshes/"),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(mesh *Mesh) string { return mesh.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*Mesh{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"meshes":        result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchMesh(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildMeshName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.meshes[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "mesh not found: "+name)
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
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated Mesh
	_ = json.Unmarshal(updatedRaw, &updated)
	api.meshes[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	project, location, _ := parseParent(r.URL.Path)
	op, err := api.opMgr.RegisterDurable("networkservices#operation", "update", name, "", location)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	api.opMgr.MarkDone(op.Name)

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":  "type.googleapis.com/google.cloud.networkservices.v1.OperationMetadata",
			"target": name,
			"verb":   "update",
		},
	})
}

func (api *API) deleteMesh(w http.ResponseWriter, r *http.Request) {
	name := buildMeshName(r.URL.Path)
	project, location, _ := parseParent(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.meshes[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "mesh not found: "+name)
		return
	}
	delete(api.meshes, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.meshes[name] = resource
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	op, err := api.opMgr.RegisterDurable("networkservices#operation", "delete", name, "", location)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	api.opMgr.MarkDone(op.Name)

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":  "type.googleapis.com/google.cloud.networkservices.v1.OperationMetadata",
			"target": name,
			"verb":   "delete",
		},
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// HttpRoute handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createHttpRoute(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	routeID := r.URL.Query().Get("httpRouteId")
	if routeID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "httpRouteId query parameter is required")
		return
	}

	var route HttpRoute
	if err := json.NewDecoder(r.Body).Decode(&route); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if len(route.Hostnames) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'hostnames' is required and must not be empty")
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/httpRoutes/%s", project, location, routeID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	route.Name = name
	route.CreateTime = now
	route.UpdateTime = now
	if err := api.ValidateReferences(&route); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	api.mu.Lock()
	if _, exists := api.httpRoutes[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "httpRoute already exists: "+routeID)
		return
	}
	api.httpRoutes[name] = &route
	api.mu.Unlock()

	// Register LRO first (if fails → rollback map, return 503)
	op, err := api.opMgr.RegisterDurable("networkservices#operation", "create", name, "", location)
	if err != nil {
		api.mu.Lock()
		delete(api.httpRoutes, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	// Then persist (if fails → rollback map, return 503)
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.httpRoutes, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": false,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.networkservices.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getHttpRoute(w http.ResponseWriter, r *http.Request) {
	name := buildHttpRouteName(r.URL.Path)

	api.mu.RLock()
	route, ok := api.httpRoutes[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "httpRoute not found: "+name)
		return
	}
	clone := cloneHttpRoute(route)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listHttpRoutes(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/httpRoutes/", project, location)

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
	var all []*HttpRoute
	for key, route := range api.httpRoutes {
		if strings.HasPrefix(key, prefix) {
			all = append(all, cloneHttpRoute(route))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "networkservices.httpRoutes",
		Parent:  strings.TrimSuffix(prefix, "/httpRoutes/"),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(route *HttpRoute) string { return route.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*HttpRoute{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"httpRoutes":    result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchHttpRoute(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildHttpRouteName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.httpRoutes[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "httpRoute not found: "+name)
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
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated HttpRoute
	_ = json.Unmarshal(updatedRaw, &updated)
	api.httpRoutes[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	project, location, _ := parseParent(r.URL.Path)
	op, err := api.opMgr.RegisterDurable("networkservices#operation", "update", name, "", location)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	api.opMgr.MarkDone(op.Name)

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":  "type.googleapis.com/google.cloud.networkservices.v1.OperationMetadata",
			"target": name,
			"verb":   "update",
		},
	})
}

func (api *API) deleteHttpRoute(w http.ResponseWriter, r *http.Request) {
	name := buildHttpRouteName(r.URL.Path)
	project, location, _ := parseParent(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.httpRoutes[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "httpRoute not found: "+name)
		return
	}
	delete(api.httpRoutes, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.httpRoutes[name] = resource
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	op, err := api.opMgr.RegisterDurable("networkservices#operation", "delete", name, "", location)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	api.opMgr.MarkDone(op.Name)

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":  "type.googleapis.com/google.cloud.networkservices.v1.OperationMetadata",
			"target": name,
			"verb":   "delete",
		},
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Operation handler
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "networkservices#operation")
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
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

func buildMeshName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	meshID := extractAfter(path, "meshes")
	return fmt.Sprintf("projects/%s/locations/%s/meshes/%s", project, location, meshID)
}

func buildHttpRouteName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	routeID := extractAfter(path, "httpRoutes")
	return fmt.Sprintf("projects/%s/locations/%s/httpRoutes/%s", project, location, routeID)
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
