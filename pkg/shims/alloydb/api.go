package alloydb

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
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
	registry.Register("alloydb.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr, ctx.SvcMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (AlloyDB v1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Cluster represents a google.cloud.alloydb.v1.Cluster resource.
type Cluster struct {
	Name            string            `json:"name"`
	UID             string            `json:"uid,omitempty"`
	CreateTime      string            `json:"createTime,omitempty"`
	UpdateTime      string            `json:"updateTime,omitempty"`
	State           string            `json:"state,omitempty"`
	DatabaseVersion string            `json:"databaseVersion,omitempty"`
	Network         string            `json:"network,omitempty"`
	DisplayName     string            `json:"displayName,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// Instance represents the bounded fields supported from
// google.cloud.alloydb.v1.Instance.
type Instance struct {
	Name            string            `json:"name"`
	DisplayName     string            `json:"displayName,omitempty"`
	UID             string            `json:"uid,omitempty"`
	CreateTime      string            `json:"createTime,omitempty"`
	UpdateTime      string            `json:"updateTime,omitempty"`
	State           string            `json:"state,omitempty"`
	InstanceType    string            `json:"instanceType,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	IPAddress       string            `json:"ipAddress,omitempty"`
	PublicIPAddress string            `json:"publicIpAddress,omitempty"`
	backendEndpoint string
}

type alloydbBackend interface {
	Provision(context.Context, orchestrator.AlloyDBIdentity) (string, bool, error)
	Reconcile(context.Context, orchestrator.AlloyDBIdentity) (string, bool, error)
	Delete(context.Context, orchestrator.AlloyDBIdentity) error
}

type serviceManagerBackend struct{ manager *orchestrator.ServiceManager }

func (backend serviceManagerBackend) Provision(ctx context.Context, identity orchestrator.AlloyDBIdentity) (string, bool, error) {
	if backend.manager == nil {
		return "", false, fmt.Errorf("AlloyDB backend is unavailable")
	}
	return backend.manager.ProvisionAlloyDB(ctx, identity)
}

func (backend serviceManagerBackend) Reconcile(ctx context.Context, identity orchestrator.AlloyDBIdentity) (string, bool, error) {
	if backend.manager == nil {
		return "", false, fmt.Errorf("AlloyDB backend is unavailable")
	}
	return backend.manager.ReconcileAlloyDB(ctx, identity)
}

func (backend serviceManagerBackend) Delete(ctx context.Context, identity orchestrator.AlloyDBIdentity) error {
	if backend.manager == nil {
		return fmt.Errorf("AlloyDB backend is unavailable")
	}
	return backend.manager.DeleteAlloyDB(ctx, identity)
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the AlloyDB v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	backend    alloydbBackend
	stateStore alloydbStateStore
	clusters   map[string]*Cluster
	instances  map[string]*Instance
}

// NewAPI creates a new AlloyDB API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:      opMgr,
		stateStore: state.NewGuardedEntryStore(store, err),
		clusters:   make(map[string]*Cluster),
		instances:  make(map[string]*Instance),
	}
	if svcMgr != nil {
		api.backend = serviceManagerBackend{manager: svcMgr}
	}
	if err != nil {
		log.Printf("[Shim: AlloyDB] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: AlloyDB] state rehydration failed: %v", err)
		return api
	}
	api.reconcileBackends()
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr:     orchestrator.NewOperationManager(),
		clusters:  make(map[string]*Cluster),
		instances: make(map[string]*Instance),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: AlloyDB] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.HasSuffix(r.URL.Path, "/instances") && r.Method == http.MethodPost:
		api.createInstance(w, r)
	case strings.HasSuffix(r.URL.Path, "/instances") && r.Method == http.MethodGet:
		api.listInstances(w, r)
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodGet:
		api.getInstance(w, r)
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodDelete:
		api.deleteInstance(w, r)
	case strings.HasSuffix(r.URL.Path, "/clusters") && r.Method == http.MethodPost:
		api.createCluster(w, r)
	case strings.HasSuffix(r.URL.Path, "/clusters") && r.Method == http.MethodGet:
		api.listClusters(w, r)
	case strings.Contains(r.URL.Path, "/clusters/") && r.Method == http.MethodGet:
		api.getCluster(w, r)
	case strings.Contains(r.URL.Path, "/clusters/") && r.Method == http.MethodPatch:
		api.patchCluster(w, r)
	case strings.Contains(r.URL.Path, "/clusters/") && r.Method == http.MethodDelete:
		api.deleteCluster(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "AlloyDB resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createCluster(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	clusterID := r.URL.Query().Get("clusterId")
	if clusterID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "clusterId query parameter is required")
		return
	}

	var cluster Cluster
	if err := json.NewDecoder(r.Body).Decode(&cluster); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if cluster.Network == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "network is required")
		return
	}
	if api.backend == nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"AlloyDB cluster provisioning requires a backend that MiniSky does not implement")
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", project, location, clusterID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	cluster.Name = name
	cluster.UID = generateUUID()
	cluster.CreateTime = now
	cluster.UpdateTime = now
	cluster.State = "READY"

	api.mu.Lock()
	if _, exists := api.clusters[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "cluster already exists: "+clusterID)
		return
	}
	api.clusters[name] = &cluster
	api.mu.Unlock()

	// Register LRO first (if fails → rollback map, return 503)
	op, err := api.opMgr.RegisterDurable("alloydb#operation", "create", name, "", location)
	if err != nil {
		api.mu.Lock()
		delete(api.clusters, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	// Then persist (if fails → rollback map, return 503)
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.clusters, name)
		api.mu.Unlock()
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "state persistence failed")
		_ = api.opMgr.RemoveDurable(op.Name)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	api.opMgr.MarkDone(op.Name)

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.alloydb.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
		"response": deepCopyCluster(&cluster),
	})
}

func (api *API) createInstance(w http.ResponseWriter, r *http.Request) {
	if api.backend == nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"AlloyDB instance provisioning requires an exact-owned Docker backend")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	identity, ok := parseInstanceIdentity(r.URL.Path, r.URL.Query().Get("instanceId"))
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instanceId and cluster parent are required")
		return
	}
	var instance Instance
	if err := json.NewDecoder(r.Body).Decode(&instance); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if instance.InstanceType != "PRIMARY" {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"only PRIMARY AlloyDB instances have a bounded local backend")
		return
	}
	clusterName := fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
		identity.Project, identity.Location, identity.Cluster)
	name := identityName(identity)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	api.mu.Lock()
	cluster := api.clusters[clusterName]
	if cluster == nil || cluster.State != "READY" {
		api.mu.Unlock()
		writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "parent cluster is not ready")
		return
	}
	if _, exists := api.instances[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "instance already exists: "+identity.Instance)
		return
	}
	instance.Name = name
	instance.UID = generateUUID()
	instance.CreateTime = now
	instance.UpdateTime = now
	instance.State = "CREATING"
	api.instances[name] = &instance
	api.mu.Unlock()

	op, err := api.opMgr.RegisterDurable("alloydb#operation", "create", name, "", identity.Location)
	if err != nil {
		api.removeInstance(name)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.removeInstance(name)
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "state persistence failed")
		_ = api.opMgr.RemoveDurable(op.Name)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}

	endpoint, created, provisionErr := api.backend.Provision(r.Context(), identity)
	if provisionErr != nil {
		var cleanupErr error
		if created {
			cleanupErr = api.cleanupBackend(identity)
			provisionErr = errors.Join(provisionErr, cleanupErr)
		}
		if cleanupErr != nil {
			api.setInstanceState(name, "ERROR")
		} else {
			api.removeInstance(name)
		}
		if persistErr := api.persistState(); persistErr != nil {
			api.opMgr.MarkPersistenceFailure(persistErr)
			provisionErr = fmt.Errorf("%w; persist rollback: %v", provisionErr, persistErr)
		}
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, provisionErr.Error())
		writeOperation(w, identity.Project, identity.Location, op.Name, name, "create", true,
			map[string]any{"code": http.StatusInternalServerError, "message": provisionErr.Error()})
		return
	}

	host, _, err := net.SplitHostPort(endpoint)
	if err != nil || host != "127.0.0.1" {
		cleanupErr := api.cleanupBackend(identity)
		if cleanupErr != nil {
			api.setInstanceState(name, "ERROR")
		} else {
			api.removeInstance(name)
		}
		_ = api.persistState()
		message := fmt.Sprintf("backend returned invalid loopback endpoint %q", endpoint)
		if cleanupErr != nil {
			message += ": cleanup failed: " + cleanupErr.Error()
		}
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, message)
		writeOperation(w, identity.Project, identity.Location, op.Name, name, "create", true,
			map[string]any{"code": http.StatusInternalServerError, "message": message})
		return
	}
	api.mu.Lock()
	instance.State = "READY"
	instance.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	instance.IPAddress = host
	instance.backendEndpoint = endpoint
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		cleanupErr := api.cleanupBackend(identity)
		api.removeInstance(name)
		api.opMgr.MarkPersistenceFailure(err)
		message := "state persistence failed after backend creation"
		if cleanupErr != nil {
			message += ": cleanup failed: " + cleanupErr.Error()
		}
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, message)
		writeOperation(w, identity.Project, identity.Location, op.Name, name, "create", true,
			map[string]any{"code": http.StatusInternalServerError, "message": message})
		return
	}
	api.opMgr.MarkDone(op.Name)
	writeOperation(w, identity.Project, identity.Location, op.Name, name, "create", true, nil)
}

func (api *API) getInstance(w http.ResponseWriter, r *http.Request) {
	name := parseInstanceName(r.URL.Path)
	api.mu.RLock()
	instance := deepCopyInstance(api.instances[name])
	api.mu.RUnlock()
	if instance == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "instance not found: "+name)
		return
	}
	_ = json.NewEncoder(w).Encode(instance)
}

func (api *API) listInstances(w http.ResponseWriter, r *http.Request) {
	parent := parseClusterName(r.URL.Path)
	prefix := parent + "/instances/"
	api.mu.RLock()
	instances := make([]*Instance, 0)
	for name, instance := range api.instances {
		if strings.HasPrefix(name, prefix) {
			instances = append(instances, deepCopyInstance(instance))
		}
	}
	api.mu.RUnlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"instances": instances})
}

func (api *API) deleteInstance(w http.ResponseWriter, r *http.Request) {
	name := parseInstanceName(r.URL.Path)
	identity, ok := parseIdentityFromName(name)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid instance name")
		return
	}
	api.mu.Lock()
	instance := api.instances[name]
	if instance == nil {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "instance not found: "+name)
		return
	}
	previousState := instance.State
	instance.State = "DELETING"
	api.mu.Unlock()
	op, err := api.opMgr.RegisterDurable("alloydb#operation", "delete", name, "", identity.Location)
	if err != nil {
		api.setInstanceState(name, previousState)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.setInstanceState(name, previousState)
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "state persistence failed")
		_ = api.opMgr.RemoveDurable(op.Name)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}
	if err := api.backend.Delete(r.Context(), identity); err != nil {
		api.setInstanceState(name, "ERROR")
		_ = api.persistState()
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, err.Error())
		writeOperation(w, identity.Project, identity.Location, op.Name, name, "delete", true,
			map[string]any{"code": http.StatusInternalServerError, "message": err.Error()})
		return
	}
	api.removeInstance(name)
	if err := api.persistState(); err != nil {
		api.opMgr.MarkPersistenceFailure(err)
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "state persistence failed after backend deletion")
		writeOperation(w, identity.Project, identity.Location, op.Name, name, "delete", true,
			map[string]any{"code": http.StatusInternalServerError, "message": "state persistence failed after backend deletion"})
		return
	}
	api.opMgr.MarkDone(op.Name)
	writeOperation(w, identity.Project, identity.Location, op.Name, name, "delete", true, nil)
}

func (api *API) getCluster(w http.ResponseWriter, r *http.Request) {
	name := parseClusterName(r.URL.Path)

	api.mu.RLock()
	cluster, ok := api.clusters[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "cluster not found: "+name)
		return
	}
	clone := deepCopyCluster(cluster)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listClusters(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/clusters/", project, location)

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
	all := make([]*Cluster, 0)
	for key, cluster := range api.clusters {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyCluster(cluster))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "alloydb.googleapis.com",
		Parent:  fmt.Sprintf("projects/%s/locations/%s", project, location),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(cluster *Cluster) string { return cluster.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = make([]*Cluster, 0)
	}

	resp := map[string]any{
		"clusters":      result,
		"nextPageToken": nextToken,
		"unreachable":   []string{},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (api *API) patchCluster(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseClusterName(r.URL.Path)
	updateMask := r.URL.Query().Get("updateMask")

	api.mu.Lock()
	existing, ok := api.clusters[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "cluster not found: "+name)
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
	merged["uid"] = existing.UID
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)
	merged["state"] = existing.State

	updatedRaw, _ := json.Marshal(merged)
	var updated Cluster
	_ = json.Unmarshal(updatedRaw, &updated)
	oldCluster := api.clusters[name]
	api.clusters[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		api.clusters[name] = oldCluster
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	project, location, _ := parseParent(r.URL.Path)
	op, err := api.opMgr.RegisterDurable("alloydb#operation", "update", name, "", location)
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
			"@type":      "type.googleapis.com/google.cloud.alloydb.v1.OperationMetadata",
			"target":     name,
			"verb":       "update",
			"apiVersion": "v1",
		},
		"response": map[string]any{
			"@type": "type.googleapis.com/google.cloud.alloydb.v1.Cluster",
		},
	})
}

func (api *API) deleteCluster(w http.ResponseWriter, r *http.Request) {
	name := parseClusterName(r.URL.Path)
	project, location, _ := parseParent(r.URL.Path)

	api.mu.Lock()
	cluster, exists := api.clusters[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "cluster not found: "+name)
		return
	}
	for instanceName := range api.instances {
		if strings.HasPrefix(instanceName, name+"/instances/") {
			api.mu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION",
				"cluster still has instances")
			return
		}
	}
	delete(api.clusters, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.clusters[name] = cluster
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	op, err := api.opMgr.RegisterDurable("alloydb#operation", "delete", name, "", location)
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
			"@type":      "type.googleapis.com/google.cloud.alloydb.v1.OperationMetadata",
			"target":     name,
			"verb":       "delete",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "alloydb#operation")
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

func parseClusterName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	clusterID := extractAfter(path, "clusters")
	return fmt.Sprintf("projects/%s/locations/%s/clusters/%s", project, location, clusterID)
}

func parseInstanceIdentity(path, instanceID string) (orchestrator.AlloyDBIdentity, bool) {
	identity := orchestrator.AlloyDBIdentity{
		Project:  extractAfter(path, "projects"),
		Location: extractAfter(path, "locations"),
		Cluster:  extractAfter(path, "clusters"),
		Instance: instanceID,
	}
	return identity, identity.Project != "" && identity.Location != "" &&
		identity.Cluster != "" && identity.Instance != ""
}

func parseIdentityFromName(name string) (orchestrator.AlloyDBIdentity, bool) {
	return parseInstanceIdentity(name, extractAfter(name, "instances"))
}

func identityName(identity orchestrator.AlloyDBIdentity) string {
	return fmt.Sprintf("projects/%s/locations/%s/clusters/%s/instances/%s",
		identity.Project, identity.Location, identity.Cluster, identity.Instance)
}

func parseInstanceName(path string) string {
	identity, _ := parseInstanceIdentity(path, extractAfter(path, "instances"))
	return identityName(identity)
}

func (api *API) removeInstance(name string) {
	api.mu.Lock()
	delete(api.instances, name)
	api.mu.Unlock()
}

func (api *API) setInstanceState(name, state string) {
	api.mu.Lock()
	if instance := api.instances[name]; instance != nil {
		instance.State = state
		instance.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	api.mu.Unlock()
}

func (api *API) cleanupBackend(identity orchestrator.AlloyDBIdentity) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return api.backend.Delete(ctx, identity)
}

func writeOperation(
	w http.ResponseWriter,
	project, location, operationName, target, verb string,
	done bool,
	operationError map[string]any,
) {
	response := map[string]any{
		"name": fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, operationName),
		"done": done,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.alloydb.v1.OperationMetadata",
			"target":     target,
			"verb":       verb,
			"apiVersion": "v1",
		},
	}
	if operationError != nil {
		operationError["details"] = []any{}
		response["error"] = operationError
	} else if verb != "delete" {
		response["response"] = map[string]any{
			"@type": "type.googleapis.com/google.cloud.alloydb.v1.Instance",
		}
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
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

func deepCopyInstance(instance *Instance) *Instance {
	if instance == nil {
		return nil
	}
	raw, _ := json.Marshal(instance)
	var clone Instance
	_ = json.Unmarshal(raw, &clone)
	clone.backendEndpoint = instance.backendEndpoint
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
