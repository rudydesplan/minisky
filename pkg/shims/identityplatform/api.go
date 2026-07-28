package identityplatform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	registry.Register("identityplatform.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Identity Platform v2)
// ─────────────────────────────────────────────────────────────────────────────

// Tenant represents an Identity Platform tenant.
type Tenant struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	CreateTime  string `json:"createTime,omitempty"`
	UpdateTime  string `json:"updateTime,omitempty"`
}

// OAuthIdpConfig represents an OAuth IdP configuration.
type OAuthIdpConfig struct {
	Name         string `json:"name"`
	DisplayName  string `json:"displayName,omitempty"`
	ClientID     string `json:"clientId,omitempty"`
	Issuer       string `json:"issuer,omitempty"`
	ClientSecret string `json:"clientSecret,omitempty"`
	Enabled      bool   `json:"enabled,omitempty"`
	CreateTime   string `json:"createTime,omitempty"`
	UpdateTime   string `json:"updateTime,omitempty"`
}

type ProjectConfig struct {
	Name              string   `json:"name"`
	AuthorizedDomains []string `json:"authorizedDomains,omitempty"`
	MultiTenant       bool     `json:"multiTenant,omitempty"`
}

type TenantConfig struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	DisableAuth bool   `json:"disableAuth,omitempty"`
}

type authConfigBackend interface {
	ApplyProjectConfig(context.Context, string, *ProjectConfig) error
	ApplyTenantConfig(context.Context, string, string, *TenantConfig) error
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Identity Platform v2 REST shim.
type API struct {
	mu              sync.RWMutex
	persistMu       sync.Mutex
	projectConfigMu sync.Mutex
	tenantConfigMu  sync.Mutex
	opMgr           *orchestrator.OperationManager
	stateStore      identityPlatformStateStore
	tenants         map[string]*Tenant
	oauthConfigs    map[string]*OAuthIdpConfig
	projectConfigs  map[string]*ProjectConfig
	tenantConfigs   map[string]*TenantConfig
	authBackend     authConfigBackend
	authHandler     http.Handler
	tenantSeq       int
	initErr         error
}

// NewAPI creates a new Identity Platform API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newAPI(opMgr, state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: IdentityPlatform] persistence degraded: %v", err)
		api.markUnavailable(fmt.Errorf("open Identity Platform state: %w", err))
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: IdentityPlatform] state rehydration failed: %v", err)
		api.markUnavailable(fmt.Errorf("load Identity Platform state: %w", err))
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store identityPlatformStateStore) *API {
	return &API{
		opMgr:          opMgr,
		stateStore:     store,
		tenants:        make(map[string]*Tenant),
		oauthConfigs:   make(map[string]*OAuthIdpConfig),
		projectConfigs: make(map[string]*ProjectConfig),
		tenantConfigs:  make(map[string]*TenantConfig),
	}
}

func newTestAPI() *API {
	return newAPI(orchestrator.NewOperationManager(), nil)
}

func (api *API) initializationError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.initErr
}

func (api *API) markUnavailable(err error) {
	if err == nil {
		return
	}
	api.mu.Lock()
	api.initErr = errors.Join(api.initErr, err)
	api.mu.Unlock()
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if handler := ctx.GetShim("identitytoolkit.googleapis.com"); handler != nil {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.authHandler = handler
		api.authBackend = firebaseAuthConfigBackend{handler: handler}
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: IdentityPlatform] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initializationError() != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Identity Platform state is unavailable")
		return
	}

	switch {
	case isIdentityToolkitUserMethod(r.URL.Path) && r.Method == http.MethodPost:
		api.forwardIdentityToolkitUserMethod(w, r)
	case strings.HasSuffix(r.URL.Path, ":initializeAuth") && r.Method == http.MethodPost:
		api.initializeProjectConfig(w, r)
	case isTenantConfig(r.URL.Path) && r.Method == http.MethodGet:
		api.getTenantConfig(w, r)
	case isTenantConfig(r.URL.Path) && r.Method == http.MethodPatch:
		api.patchTenantConfig(w, r)
	case isProjectConfig(r.URL.Path) && r.Method == http.MethodGet:
		api.getProjectConfig(w, r)
	case isProjectConfig(r.URL.Path) && r.Method == http.MethodPatch:
		api.patchProjectConfig(w, r)
	// OAuthIdpConfigs (must match before tenants since path contains /tenants/)
	case isOAuthConfigCollection(r.URL.Path) && r.Method == http.MethodPost:
		api.createOAuthIdpConfig(w, r)
	case isOAuthConfigCollection(r.URL.Path) && r.Method == http.MethodGet:
		api.listOAuthIdpConfigs(w, r)
	case isOAuthConfigResource(r.URL.Path) && r.Method == http.MethodGet:
		api.getOAuthIdpConfig(w, r)
	case isOAuthConfigResource(r.URL.Path) && r.Method == http.MethodPatch:
		api.updateOAuthIdpConfig(w, r)
	case isOAuthConfigResource(r.URL.Path) && r.Method == http.MethodDelete:
		api.deleteOAuthIdpConfig(w, r)
	// Tenants
	case isTenantCollection(r.URL.Path) && r.Method == http.MethodPost:
		api.createTenant(w, r)
	case isTenantCollection(r.URL.Path) && r.Method == http.MethodGet:
		api.listTenants(w, r)
	case isTenantResource(r.URL.Path) && r.Method == http.MethodGet:
		api.getTenant(w, r)
	case isTenantResource(r.URL.Path) && r.Method == http.MethodPatch:
		api.updateTenant(w, r)
	case isTenantResource(r.URL.Path) && r.Method == http.MethodDelete:
		api.deleteTenant(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Identity Platform resource not found")
	}
}

func (api *API) initializeProjectConfig(w http.ResponseWriter, r *http.Request) {
	project := strings.TrimSuffix(extractAfter(r.URL.Path, "projects"), ":initializeAuth")
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}
	api.projectConfigMu.Lock()
	defer api.projectConfigMu.Unlock()

	name := "projects/" + project + "/config"
	api.mu.Lock()
	prior := cloneProjectConfig(api.projectConfigs[name])
	config := cloneProjectConfig(prior)
	if config == nil {
		config = &ProjectConfig{Name: name}
		api.projectConfigs[name] = config
	}
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		durable, committed, reconcileErr := api.reconcileProjectConfig(name, config)
		if reconcileErr != nil {
			api.markUnavailable(errors.Join(err, reconcileErr))
			api.mu.Lock()
			if prior == nil {
				delete(api.projectConfigs, name)
			} else {
				api.projectConfigs[name] = prior
			}
			api.mu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE",
				"State persistence failed and durable state could not be reconciled")
			return
		}
		if committed {
			_ = json.NewEncoder(w).Encode(durable)
			return
		}
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(config)
}

func (api *API) forwardIdentityToolkitUserMethod(w http.ResponseWriter, r *http.Request) {
	api.mu.RLock()
	handler := api.authHandler
	api.mu.RUnlock()
	if handler == nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Firebase Auth backend is unavailable")
		return
	}
	handler.ServeHTTP(w, r)
}

func (api *API) getProjectConfig(w http.ResponseWriter, r *http.Request) {
	api.projectConfigMu.Lock()
	defer api.projectConfigMu.Unlock()

	project := extractAfter(r.URL.Path, "projects")
	name := "projects/" + project + "/config"
	api.mu.RLock()
	config := cloneProjectConfig(api.projectConfigs[name])
	api.mu.RUnlock()
	if config == nil {
		config = &ProjectConfig{Name: name}
	}
	_ = json.NewEncoder(w).Encode(config)
}

func (api *API) patchProjectConfig(w http.ResponseWriter, r *http.Request) {
	project := extractAfter(r.URL.Path, "projects")
	if project == "" || r.URL.Query().Get("updateMask") == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project and updateMask are required")
		return
	}
	var patch ProjectConfig
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid project config JSON")
		return
	}
	api.projectConfigMu.Lock()
	defer api.projectConfigMu.Unlock()

	name := "projects/" + project + "/config"
	api.mu.RLock()
	prior := cloneProjectConfig(api.projectConfigs[name])
	updated := cloneProjectConfig(prior)
	backend := api.authBackend
	api.mu.RUnlock()
	if updated == nil {
		updated = &ProjectConfig{Name: name}
	}
	for _, field := range strings.Split(r.URL.Query().Get("updateMask"), ",") {
		switch strings.TrimSpace(field) {
		case "authorizedDomains":
			updated.AuthorizedDomains = append([]string(nil), patch.AuthorizedDomains...)
		case "multiTenant":
			updated.MultiTenant = patch.MultiTenant
		default:
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unsupported update mask")
			return
		}
	}
	if backend == nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Firebase Auth backend is unavailable")
		return
	}
	if err := backend.ApplyProjectConfig(r.Context(), project, updated); err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Firebase Auth backend rejected project config")
		return
	}
	api.mu.Lock()
	api.projectConfigs[name] = updated
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		durable, committed, reconcileErr := api.reconcileProjectConfig(name, updated)
		if reconcileErr != nil {
			api.markUnavailable(errors.Join(err, reconcileErr))
			rollback := cloneProjectConfig(prior)
			if rollback == nil {
				rollback = &ProjectConfig{Name: name}
			}
			rollbackErr := backend.ApplyProjectConfig(r.Context(), project, rollback)
			api.markUnavailable(rollbackErr)
			api.mu.Lock()
			if prior == nil {
				delete(api.projectConfigs, name)
			} else {
				api.projectConfigs[name] = prior
			}
			api.mu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE",
				"State persistence failed and durable state could not be reconciled")
			return
		}
		if committed {
			_ = json.NewEncoder(w).Encode(durable)
			return
		}
		rollback := cloneProjectConfig(durable)
		if rollback == nil {
			rollback = &ProjectConfig{Name: name}
		}
		if rollbackErr := backend.ApplyProjectConfig(r.Context(), project, rollback); rollbackErr != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE",
				"State persistence and Firebase Auth backend rollback failed")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(updated)
}

func (api *API) getTenantConfig(w http.ResponseWriter, r *http.Request) {
	api.tenantConfigMu.Lock()
	defer api.tenantConfigMu.Unlock()

	name := buildTenantName(r.URL.Path) + "/config"
	api.mu.RLock()
	config := cloneTenantConfig(api.tenantConfigs[name])
	api.mu.RUnlock()
	if config == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "tenant config not found")
		return
	}
	_ = json.NewEncoder(w).Encode(config)
}

func (api *API) patchTenantConfig(w http.ResponseWriter, r *http.Request) {
	tenantName := buildTenantName(r.URL.Path)
	project := extractAfter(r.URL.Path, "projects")
	tenantID := extractAfter(r.URL.Path, "tenants")
	if r.URL.Query().Get("updateMask") == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "updateMask is required")
		return
	}
	var patch TenantConfig
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid tenant config JSON")
		return
	}
	api.tenantConfigMu.Lock()
	defer api.tenantConfigMu.Unlock()

	api.mu.RLock()
	_, exists := api.tenants[tenantName]
	current := cloneTenantConfig(api.tenantConfigs[tenantName+"/config"])
	previous := cloneTenantConfig(current)
	backend := api.authBackend
	api.mu.RUnlock()
	if !exists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "tenant not found")
		return
	}
	if current == nil {
		current = &TenantConfig{Name: tenantName + "/config"}
	}
	for _, field := range strings.Split(r.URL.Query().Get("updateMask"), ",") {
		switch strings.TrimSpace(field) {
		case "displayName":
			current.DisplayName = patch.DisplayName
		case "disableAuth":
			current.DisableAuth = patch.DisableAuth
		default:
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unsupported update mask")
			return
		}
	}
	if backend == nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Firebase Auth backend is unavailable")
		return
	}
	if err := backend.ApplyTenantConfig(r.Context(), project, tenantID, current); err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Firebase Auth backend rejected tenant config")
		return
	}
	api.mu.Lock()
	api.tenantConfigs[current.Name] = current
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		durable, committed, reconcileErr := api.reconcileTenantConfig(current.Name, current)
		if reconcileErr != nil {
			api.markUnavailable(errors.Join(err, reconcileErr))
			rollback := cloneTenantConfig(previous)
			if rollback == nil {
				rollback = &TenantConfig{Name: current.Name}
			}
			rollbackErr := backend.ApplyTenantConfig(r.Context(), project, tenantID, rollback)
			api.markUnavailable(rollbackErr)
			api.mu.Lock()
			if previous == nil {
				delete(api.tenantConfigs, current.Name)
			} else {
				api.tenantConfigs[previous.Name] = previous
			}
			api.mu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE",
				"State persistence failed and durable state could not be reconciled")
			return
		}
		if committed {
			_ = json.NewEncoder(w).Encode(durable)
			return
		}
		rollback := cloneTenantConfig(durable)
		if rollback == nil {
			rollback = &TenantConfig{Name: current.Name}
		}
		rollbackErr := backend.ApplyTenantConfig(r.Context(), project, tenantID, rollback)
		if rollbackErr != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE",
				"State persistence and Firebase Auth backend rollback failed")
			return
		}
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(current)
}

type firebaseAuthConfigBackend struct {
	handler http.Handler
}

func (b firebaseAuthConfigBackend) ApplyProjectConfig(ctx context.Context, project string, config *ProjectConfig) error {
	return b.apply(ctx, "/emulator/v1/projects/"+project+"/config", config)
}

func (b firebaseAuthConfigBackend) ApplyTenantConfig(ctx context.Context, project, tenant string, config *TenantConfig) error {
	return b.apply(ctx, "/emulator/v1/projects/"+project+"/tenants/"+tenant+"/config", config)
}

func (b firebaseAuthConfigBackend) apply(ctx context.Context, path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPatch, path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response := &backendResponse{header: make(http.Header), status: http.StatusOK}
	b.handler.ServeHTTP(response, request)
	if response.status < 200 || response.status >= 300 {
		return fmt.Errorf("Firebase Auth config status %d", response.status)
	}
	return nil
}

type backendResponse struct {
	header http.Header
	status int
}

func (r *backendResponse) Header() http.Header            { return r.header }
func (r *backendResponse) Write(data []byte) (int, error) { return len(data), nil }
func (r *backendResponse) WriteHeader(status int)         { r.status = status }

// ─────────────────────────────────────────────────────────────────────────────
// Tenant handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createTenant(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project := extractAfter(r.URL.Path, "projects")
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	var tenant Tenant
	if err := json.NewDecoder(r.Body).Decode(&tenant); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if tenant.DisplayName == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'displayName' is required")
		return
	}

	api.mu.Lock()
	api.tenantSeq++
	tenantID := fmt.Sprintf("tenant-%d", api.tenantSeq)
	name := fmt.Sprintf("projects/%s/tenants/%s", project, tenantID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tenant.Name = name
	tenant.CreateTime = now
	tenant.UpdateTime = now
	api.tenants[name] = &tenant
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		if api.tenants[name] == &tenant {
			delete(api.tenants, name)
		}
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(&tenant)
}

func (api *API) getTenant(w http.ResponseWriter, r *http.Request) {
	name := buildTenantName(r.URL.Path)

	api.mu.RLock()
	tenant, ok := api.tenants[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "tenant not found: "+name)
		return
	}
	clone := cloneTenant(tenant)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listTenants(w http.ResponseWriter, r *http.Request) {
	project := extractAfter(r.URL.Path, "projects")
	prefix := fmt.Sprintf("projects/%s/tenants/", project)

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
	var all []*Tenant
	for key, t := range api.tenants {
		if strings.HasPrefix(key, prefix) {
			all = append(all, cloneTenant(t))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "identityplatform.tenants",
		Parent:  "projects/" + project,
	}, func(tenant *Tenant) string { return tenant.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*Tenant{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"tenants":       result,
		"nextPageToken": nextToken,
	})
}

func (api *API) updateTenant(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildTenantName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.tenants[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "tenant not found: "+name)
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
	var updated Tenant
	_ = json.Unmarshal(updatedRaw, &updated)
	api.tenants[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		if api.tenants[name] == &updated {
			api.tenants[name] = existing
		}
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(&updated)
}

func (api *API) deleteTenant(w http.ResponseWriter, r *http.Request) {
	name := buildTenantName(r.URL.Path)

	api.mu.Lock()
	tenant, exists := api.tenants[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "tenant not found: "+name)
		return
	}
	delete(api.tenants, name)
	// Cascade delete OAuth configs under this tenant
	configPrefix := name + "/oauthIdpConfigs/"
	deletedConfigs := make(map[string]*OAuthIdpConfig)
	for key := range api.oauthConfigs {
		if strings.HasPrefix(key, configPrefix) {
			deletedConfigs[key] = api.oauthConfigs[key]
			delete(api.oauthConfigs, key)
		}
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.tenants[name] = tenant
		for key, config := range deletedConfigs {
			api.oauthConfigs[key] = config
		}
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// OAuthIdpConfig handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createOAuthIdpConfig(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	tenantName := buildTenantName(r.URL.Path)
	configID := r.URL.Query().Get("oauthIdpConfigId")
	if configID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "oauthIdpConfigId query parameter is required")
		return
	}

	// Verify parent tenant exists
	api.mu.RLock()
	_, tenantExists := api.tenants[tenantName]
	api.mu.RUnlock()
	if !tenantExists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent tenant not found: "+tenantName)
		return
	}

	var cfg OAuthIdpConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := fmt.Sprintf("%s/oauthIdpConfigs/%s", tenantName, configID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	cfg.Name = name
	cfg.CreateTime = now
	cfg.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.oauthConfigs[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "oauthIdpConfig already exists: "+configID)
		return
	}
	api.oauthConfigs[name] = &cfg
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		if api.oauthConfigs[name] == &cfg {
			delete(api.oauthConfigs, name)
		}
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(publicOAuthConfig(&cfg))
}

func (api *API) getOAuthIdpConfig(w http.ResponseWriter, r *http.Request) {
	name := buildOAuthConfigName(r.URL.Path)

	api.mu.RLock()
	cfg, ok := api.oauthConfigs[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "oauthIdpConfig not found: "+name)
		return
	}
	clone := cloneOAuthConfig(cfg)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(publicOAuthConfig(clone))
}

func (api *API) listOAuthIdpConfigs(w http.ResponseWriter, r *http.Request) {
	tenantName := buildTenantName(r.URL.Path)
	prefix := tenantName + "/oauthIdpConfigs/"

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
	var all []*OAuthIdpConfig
	for key, cfg := range api.oauthConfigs {
		if strings.HasPrefix(key, prefix) {
			all = append(all, publicOAuthConfig(cfg))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "identityplatform.oauthIdpConfigs",
		Parent:  tenantName,
	}, func(config *OAuthIdpConfig) string { return config.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*OAuthIdpConfig{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"oauthIdpConfigs": result,
		"nextPageToken":   nextToken,
	})
}

func (api *API) updateOAuthIdpConfig(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildOAuthConfigName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.oauthConfigs[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "oauthIdpConfig not found: "+name)
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
	var updated OAuthIdpConfig
	_ = json.Unmarshal(updatedRaw, &updated)
	api.oauthConfigs[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		if api.oauthConfigs[name] == &updated {
			api.oauthConfigs[name] = existing
		}
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(publicOAuthConfig(&updated))
}

func (api *API) deleteOAuthIdpConfig(w http.ResponseWriter, r *http.Request) {
	name := buildOAuthConfigName(r.URL.Path)

	api.mu.Lock()
	config, exists := api.oauthConfigs[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "oauthIdpConfig not found: "+name)
		return
	}
	delete(api.oauthConfigs, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.oauthConfigs[name] = config
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func buildTenantName(path string) string {
	project := extractAfter(path, "projects")
	tenantID := extractAfter(path, "tenants")
	if idx := strings.Index(tenantID, "/"); idx >= 0 {
		tenantID = tenantID[:idx]
	}
	return fmt.Sprintf("projects/%s/tenants/%s", project, tenantID)
}

func buildOAuthConfigName(path string) string {
	tenantName := buildTenantName(path)
	configID := extractAfter(path, "oauthIdpConfigs")
	return fmt.Sprintf("%s/oauthIdpConfigs/%s", tenantName, configID)
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

func isTenantCollection(path string) bool {
	return strings.HasSuffix(path, "/tenants") && !strings.Contains(path, "/oauthIdpConfigs")
}

func isTenantResource(path string) bool {
	if !strings.Contains(path, "/tenants/") {
		return false
	}
	if strings.Contains(path, "/oauthIdpConfigs") {
		return false
	}
	return true
}

func isProjectConfig(path string) bool {
	return strings.HasSuffix(path, "/config") && strings.Contains(path, "/projects/") &&
		!strings.Contains(path, "/tenants/")
}

func isTenantConfig(path string) bool {
	return strings.HasSuffix(path, "/config") && strings.Contains(path, "/tenants/")
}

func isOAuthConfigCollection(path string) bool {
	return strings.Contains(path, "/tenants/") && strings.HasSuffix(path, "/oauthIdpConfigs")
}

func isOAuthConfigResource(path string) bool {
	return strings.Contains(path, "/oauthIdpConfigs/")
}

func isIdentityToolkitUserMethod(path string) bool {
	switch path {
	case "/v1/accounts:signUp", "/v1/accounts:signInWithPassword", "/v1/accounts:lookup",
		"/v1/accounts:update", "/v1/accounts:delete":
		return true
	default:
		return false
	}
}

func publicOAuthConfig(config *OAuthIdpConfig) *OAuthIdpConfig {
	clone := cloneOAuthConfig(config)
	if clone != nil {
		clone.ClientSecret = ""
	}
	return clone
}

func cloneProjectConfig(config *ProjectConfig) *ProjectConfig {
	if config == nil {
		return nil
	}
	clone := *config
	clone.AuthorizedDomains = append([]string(nil), config.AuthorizedDomains...)
	return &clone
}

func cloneTenantConfig(config *TenantConfig) *TenantConfig {
	if config == nil {
		return nil
	}
	clone := *config
	return &clone
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
