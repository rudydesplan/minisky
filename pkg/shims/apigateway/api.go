package apigateway

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
	registry.Register("apigateway.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resources (API Gateway v1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Api represents a google.cloud.apigateway.v1.Api resource.
type Api struct {
	Name           string            `json:"name"`
	DisplayName    string            `json:"displayName,omitempty"`
	ManagedService string            `json:"managedService,omitempty"`
	CreateTime     string            `json:"createTime,omitempty"`
	UpdateTime     string            `json:"updateTime,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	State          string            `json:"state,omitempty"`
}

// Gateway represents a google.cloud.apigateway.v1.Gateway resource.
type Gateway struct {
	Name            string            `json:"name"`
	DisplayName     string            `json:"displayName,omitempty"`
	ApiConfig       string            `json:"apiConfig,omitempty"`
	State           string            `json:"state,omitempty"`
	DefaultHostname string            `json:"defaultHostname,omitempty"`
	CreateTime      string            `json:"createTime,omitempty"`
	UpdateTime      string            `json:"updateTime,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the API Gateway v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	stateStore apigatewayStateStore
	apis       map[string]*Api
	configs    map[string]*ApiConfig
	gateways   map[string]*Gateway
}

// NewAPI creates a new API Gateway shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:      opMgr,
		stateStore: state.NewGuardedEntryStore(store, err),
		apis:       make(map[string]*Api),
		configs:    make(map[string]*ApiConfig),
		gateways:   make(map[string]*Gateway),
	}
	if err != nil {
		log.Printf("[Shim: API Gateway] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: API Gateway] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr:    orchestrator.NewOperationManager(),
		apis:     make(map[string]*Api),
		configs:  make(map[string]*ApiConfig),
		gateways: make(map[string]*Gateway),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: API Gateway] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.Contains(r.URL.Path, "/configs"):
		api.routeConfigs(w, r)
	case strings.Contains(r.URL.Path, "/gateways"):
		api.routeGateways(w, r)
	case strings.Contains(r.URL.Path, "/apis"):
		api.routeApis(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "API Gateway resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// APIs
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeApis(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createApi(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "apis") {
			api.listApis(w, r)
		} else {
			api.getApi(w, r)
		}
	case http.MethodPatch:
		api.patchApi(w, r)
	case http.MethodDelete:
		api.deleteApi(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createApi(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, ok := parseGlobalParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	apiID := r.URL.Query().Get("apiId")
	if apiID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "apiId query parameter is required")
		return
	}

	var resource Api
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := fmt.Sprintf("projects/%s/locations/global/apis/%s", project, apiID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.CreateTime = now
	resource.UpdateTime = now
	resource.State = "ACTIVE"

	api.mu.Lock()
	if _, exists := api.apis[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "API already exists: "+apiID)
		return
	}
	api.apis[name] = &resource
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &resource, true, func() {
		api.mu.Lock()
		delete(api.apis, name)
		api.mu.Unlock()
	})
}

func (api *API) getApi(w http.ResponseWriter, r *http.Request) {
	name := parseApiName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.apis[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "API not found: "+name)
		return
	}
	clone := deepCopyApi(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listApis(w http.ResponseWriter, r *http.Request) {
	project, ok := parseGlobalParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/global/apis/", project)

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
	all := make([]*Api, 0)
	for key, a := range api.apis {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyApi(a))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "apigateway.apis",
		Parent:  fmt.Sprintf("projects/%s/locations/global", project),
	}, func(api *Api) string { return api.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"apis":          result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchApi(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseApiName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.apis[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "API not found: "+name)
		return
	}
	before := deepCopyApi(existing)

	var patch Api
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if patch.DisplayName != "" {
		existing.DisplayName = patch.DisplayName
	}
	if patch.Labels != nil {
		existing.Labels = patch.Labels
	}
	if patch.ManagedService != "" {
		existing.ManagedService = patch.ManagedService
	}
	existing.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	updated := deepCopyApi(existing)
	api.mu.Unlock()

	api.finishMutation(w, "update", name, updated, false, func() {
		api.mu.Lock()
		api.apis[name] = before
		api.mu.Unlock()
	})
}

func (api *API) deleteApi(w http.ResponseWriter, r *http.Request) {
	name := parseApiName(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.apis[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "API not found: "+name)
		return
	}
	configPrefix := name + "/configs/"
	for configName := range api.configs {
		if strings.HasPrefix(configName, configPrefix) {
			api.mu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "API configs must be deleted before the API")
			return
		}
	}
	delete(api.apis, name)
	api.mu.Unlock()

	api.finishMutation(w, "delete", name, map[string]any{}, false, func() {
		api.mu.Lock()
		api.apis[name] = resource
		api.mu.Unlock()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Gateways
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeGateways(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createGateway(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "gateways") {
			api.listGateways(w, r)
		} else {
			api.getGateway(w, r)
		}
	case http.MethodPatch:
		api.patchGateway(w, r)
	case http.MethodDelete:
		api.deleteGateway(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createGateway(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseLocationParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	gatewayID := r.URL.Query().Get("gatewayId")
	if gatewayID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "gatewayId query parameter is required")
		return
	}

	var resource Gateway
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/gateways/%s", project, location, gatewayID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.CreateTime = now
	resource.UpdateTime = now
	resource.State = "ACTIVE"
	resource.DefaultHostname = fmt.Sprintf("%s-%s.apigateway.example.com", gatewayID, project)

	api.mu.Lock()
	if _, exists := api.gateways[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "gateway already exists: "+gatewayID)
		return
	}
	if resource.ApiConfig == "" {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "apiConfig is required")
		return
	}
	if _, exists := api.configs[resource.ApiConfig]; !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "API config not found: "+resource.ApiConfig)
		return
	}
	api.gateways[name] = &resource
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &resource, true, func() {
		api.mu.Lock()
		delete(api.gateways, name)
		api.mu.Unlock()
	})
}

func (api *API) getGateway(w http.ResponseWriter, r *http.Request) {
	name := parseGatewayName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.gateways[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "gateway not found: "+name)
		return
	}
	clone := deepCopyGateway(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listGateways(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseLocationParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/gateways/", project, location)

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
	all := make([]*Gateway, 0)
	for key, gw := range api.gateways {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyGateway(gw))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "apigateway.gateways",
		Parent:  fmt.Sprintf("projects/%s/locations/%s", project, location),
	}, func(gateway *Gateway) string { return gateway.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"gateways":      result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchGateway(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseGatewayName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.gateways[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "gateway not found: "+name)
		return
	}
	before := deepCopyGateway(existing)

	var patch Gateway
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if patch.DisplayName != "" {
		existing.DisplayName = patch.DisplayName
	}
	if patch.ApiConfig != "" {
		existing.ApiConfig = patch.ApiConfig
	}
	if patch.Labels != nil {
		existing.Labels = patch.Labels
	}
	existing.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	updated := deepCopyGateway(existing)
	api.mu.Unlock()

	api.finishMutation(w, "update", name, updated, false, func() {
		api.mu.Lock()
		api.gateways[name] = before
		api.mu.Unlock()
	})
}

func (api *API) deleteGateway(w http.ResponseWriter, r *http.Request) {
	name := parseGatewayName(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.gateways[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "gateway not found: "+name)
		return
	}
	delete(api.gateways, name)
	api.mu.Unlock()

	api.finishMutation(w, "delete", name, map[string]any{}, false, func() {
		api.mu.Lock()
		api.gateways[name] = resource
		api.mu.Unlock()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Operations
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "apigateway#operation")
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
}

func (api *API) finishMutation(w http.ResponseWriter, verb, target string, response any, async bool, rollback func()) {
	op, err := api.opMgr.RegisterScopedTargetDurable("apigateway#operation", verb, target)
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
			log.Printf("[Shim: APIGateway] mutation compensation degraded: state=%v operation=%v", stateErr, operationErr)
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
				log.Printf("[Shim: APIGateway] terminal operation persistence degraded: %v", err)
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

// parseGlobalParent extracts project from /v1/projects/{p}/locations/global/apis
func parseGlobalParent(path string) (project string, ok bool) {
	project = extractAfter(path, "projects")
	if project == "" {
		return "", false
	}
	return project, true
}

// parseLocationParent extracts project and location from path.
func parseLocationParent(path string) (project, location string, ok bool) {
	project = extractAfter(path, "projects")
	location = extractAfter(path, "locations")
	if project == "" || location == "" {
		return "", "", false
	}
	return project, location, true
}

// parseApiName reconstructs the full resource name from the URL path.
func parseApiName(path string) string {
	project := extractAfter(path, "projects")
	apiID := extractAfter(path, "apis")
	return fmt.Sprintf("projects/%s/locations/global/apis/%s", project, apiID)
}

// parseGatewayName reconstructs the full resource name from the URL path.
func parseGatewayName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	gatewayID := extractAfter(path, "gateways")
	return fmt.Sprintf("projects/%s/locations/%s/gateways/%s", project, location, gatewayID)
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

func isCollection(path, resource string) bool {
	return strings.HasSuffix(path, "/"+resource)
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
