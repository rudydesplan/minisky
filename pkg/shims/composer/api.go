package composer

import (
	"context"
	"crypto/rand"
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
	registry.Register("composer.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Cloud Composer v1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Environment represents a google.cloud.orchestration.airflow.service.v1.Environment resource.
type Environment struct {
	Name       string             `json:"name"`
	UUID       string             `json:"uuid,omitempty"`
	CreateTime string             `json:"createTime,omitempty"`
	UpdateTime string             `json:"updateTime,omitempty"`
	State      string             `json:"state,omitempty"`
	Config     *EnvironmentConfig `json:"config,omitempty"`
	Labels     map[string]string  `json:"labels,omitempty"`
}

// EnvironmentConfig holds the environment configuration.
type EnvironmentConfig struct {
	NodeCount      int             `json:"nodeCount,omitempty"`
	SoftwareConfig *SoftwareConfig `json:"softwareConfig,omitempty"`
	AirflowURI     string          `json:"airflowUri,omitempty"`
	DagGcsPrefix   string          `json:"dagGcsPrefix,omitempty"`
}

// SoftwareConfig holds software-related configuration.
type SoftwareConfig struct {
	ImageVersion  string `json:"imageVersion,omitempty"`
	PythonVersion string `json:"pythonVersion,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Cloud Composer v1 REST shim.
type API struct {
	mu               sync.RWMutex
	mutationMu       sync.Mutex
	persistMu        sync.Mutex
	opMgr            *orchestrator.OperationManager
	stateStore       composerStateStore
	environments     map[string]*Environment
	backend          airflowBackend
	reconcileTimeout time.Duration
}

type airflowBackend interface {
	Provision(context.Context, string) (string, error)
	Reconcile(context.Context, string) (string, bool, error)
	Delete(context.Context, string) error
}

// NewAPI creates a new Composer API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:        opMgr,
		stateStore:   state.NewGuardedEntryStore(store, err),
		environments: make(map[string]*Environment),
		backend:      newDockerAirflowBackend(),
	}
	if err != nil {
		log.Printf("[Shim: Composer] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Composer] state rehydration failed: %v", err)
		api.stateStore = state.NewGuardedEntryStore(store, err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr:        orchestrator.NewOperationManager(),
		environments: make(map[string]*Environment),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Composer] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.HasSuffix(r.URL.Path, "/environments") && r.Method == http.MethodPost:
		api.createEnvironment(w, r)
	case strings.HasSuffix(r.URL.Path, "/environments") && r.Method == http.MethodGet:
		api.listEnvironments(w, r)
	case strings.Contains(r.URL.Path, "/environments/") && r.Method == http.MethodGet:
		api.getEnvironment(w, r)
	case strings.Contains(r.URL.Path, "/environments/") && r.Method == http.MethodPatch:
		api.patchEnvironment(w, r)
	case strings.Contains(r.URL.Path, "/environments/") && r.Method == http.MethodDelete:
		api.deleteEnvironment(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Composer resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createEnvironment(w http.ResponseWriter, r *http.Request) {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	var env Environment
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	// Environment name is in the body — extract environmentId from it
	if env.Name == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "name is required in the request body")
		return
	}

	// Validate name format: must match parent
	expectedPrefix := fmt.Sprintf("projects/%s/locations/%s/environments/", project, location)
	if !strings.HasPrefix(env.Name, expectedPrefix) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "name must match parent: "+expectedPrefix+"<environmentId>")
		return
	}
	name := env.Name
	now := time.Now().UTC().Format(time.RFC3339Nano)

	env.UUID = generateUUID()
	env.CreateTime = now
	env.UpdateTime = now
	env.State = "CREATING"

	api.mu.Lock()
	if _, exists := api.environments[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "environment already exists: "+name)
		return
	}
	if api.backend == nil {
		api.mu.Unlock()
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Cloud Composer environment provisioning requires the pinned Airflow backend")
		return
	}
	api.environments[name] = &env
	api.mu.Unlock()

	// Register LRO first (if fails → rollback map, return 503)
	op, err := api.opMgr.RegisterDurable("composer#operation", "create", name, "", location)
	if err != nil {
		api.mu.Lock()
		delete(api.environments, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	// Then persist (if fails → rollback map, return 503)
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.environments, name)
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		api.mutationMu.Lock()
		defer api.mutationMu.Unlock()
		endpoint, err := api.backend.Provision(context.Background(), name)
		api.mu.Lock()
		current := api.environments[name]
		if current != nil {
			current.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
			if err != nil {
				current.State = "ERROR"
			} else {
				current.State = "RUNNING"
				if current.Config == nil {
					current.Config = &EnvironmentConfig{}
				}
				if current.Config.SoftwareConfig == nil {
					current.Config.SoftwareConfig = &SoftwareConfig{ImageVersion: "composer-3-airflow-2.10.5"}
				}
				current.Config.AirflowURI = endpoint
				current.Config.DagGcsPrefix = "minisky://" + name + "/dags"
			}
		}
		api.mu.Unlock()
		if persistErr := api.persistState(); persistErr != nil && err == nil {
			api.mu.Lock()
			if current := api.environments[name]; current != nil {
				current.State = "ERROR"
				if current.Config != nil {
					current.Config.AirflowURI = ""
					current.Config.DagGcsPrefix = ""
				}
			}
			api.mu.Unlock()
			return persistErr
		}
		return err
	})

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": false,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.orchestration.airflow.service.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getEnvironment(w http.ResponseWriter, r *http.Request) {
	name := parseEnvironmentName(r.URL.Path)

	api.mu.RLock()
	env, ok := api.environments[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "environment not found: "+name)
		return
	}
	clone := deepCopyEnvironment(env)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listEnvironments(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/environments/", project, location)

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
	all := make([]*Environment, 0)
	for key, env := range api.environments {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyEnvironment(env))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "composer.googleapis.com",
		Parent:  fmt.Sprintf("projects/%s/locations/%s", project, location),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(environment *Environment) string { return environment.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = make([]*Environment, 0)
	}

	resp := map[string]any{
		"environments":  result,
		"nextPageToken": nextToken,
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (api *API) patchEnvironment(w http.ResponseWriter, r *http.Request) {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseEnvironmentName(r.URL.Path)
	updateMask := r.URL.Query().Get("updateMask")

	api.mu.Lock()
	existing, ok := api.environments[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "environment not found: "+name)
		return
	}
	if api.backend == nil {
		api.mu.Unlock()
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Cloud Composer environment updates require the pinned Airflow backend")
		return
	}
	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	existingRaw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(existingRaw, &merged)

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

	// Preserve output-only fields
	merged["name"] = existing.Name
	merged["uuid"] = existing.UUID
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)
	merged["state"] = existing.State

	updatedRaw, _ := json.Marshal(merged)
	var updated Environment
	_ = json.Unmarshal(updatedRaw, &updated)
	oldEnv := api.environments[name]
	api.mu.Unlock()

	project, location, _ := parseParent(r.URL.Path)
	op, err := api.opMgr.RegisterDurable("composer#operation", "update", name, "", location)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	api.mu.Lock()
	if api.environments[name] != oldEnv {
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, 503, "UNAVAILABLE", "Environment changed while registering operation")
		return
	}
	api.environments[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		api.environments[name] = oldEnv
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	if err := api.opMgr.FinalizeScopedDurable(op.Name, nil, 0, ""); err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to persist operation result")
		return
	}

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.orchestration.airflow.service.v1.OperationMetadata",
			"target":     name,
			"verb":       "update",
			"apiVersion": "v1",
		},
		"response": map[string]any{
			"@type": "type.googleapis.com/google.cloud.orchestration.airflow.service.v1.Environment",
		},
	})
}

func (api *API) deleteEnvironment(w http.ResponseWriter, r *http.Request) {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()

	name := parseEnvironmentName(r.URL.Path)
	project, location, _ := parseParent(r.URL.Path)

	api.mu.Lock()
	env, exists := api.environments[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "environment not found: "+name)
		return
	}
	if api.backend == nil {
		api.mu.Unlock()
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Cloud Composer environment deletion requires the pinned Airflow backend")
		return
	}
	api.mu.Unlock()

	op, err := api.opMgr.RegisterDurable("composer#operation", "delete", name, "", location)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.backend.Delete(r.Context(), name); err != nil {
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Airflow backend deletion failed: "+err.Error())
		return
	}
	api.mu.Lock()
	env, exists = api.environments[name]
	if !exists {
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "environment not found: "+name)
		return
	}
	delete(api.environments, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		restored := deepCopyEnvironment(env)
		restored.State = "ERROR"
		if restored.Config != nil {
			restored.Config.AirflowURI = ""
			restored.Config.DagGcsPrefix = ""
		}
		api.environments[name] = restored
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	if err := api.opMgr.FinalizeScopedDurable(op.Name, nil, 0, ""); err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to persist operation result")
		return
	}

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.orchestration.airflow.service.v1.OperationMetadata",
			"target":     name,
			"verb":       "delete",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "composer#operation")
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

func parseEnvironmentName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	envID := extractAfter(path, "environments")
	return fmt.Sprintf("projects/%s/locations/%s/environments/%s", project, location, envID)
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

func generateUUID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
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
