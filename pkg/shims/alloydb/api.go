package alloydb

import (
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
	registry.Register("alloydb.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
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

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the AlloyDB v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	stateStore alloydbStateStore
	clusters   map[string]*Cluster
}

var provisioningBackendAvailable bool

// NewAPI creates a new AlloyDB API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:      opMgr,
		stateStore: state.NewGuardedEntryStore(store, err),
		clusters:   make(map[string]*Cluster),
	}
	if err != nil {
		log.Printf("[Shim: AlloyDB] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: AlloyDB] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr:    orchestrator.NewOperationManager(),
		clusters: make(map[string]*Cluster),
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
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"AlloyDB instance provisioning is unavailable because MiniSky has no exact-owned AlloyDB backend")
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
	if !provisioningBackendAvailable {
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
	cluster.State = "CREATING"

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
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		// Metadata-only: resource stays in CREATING state.
		// Real provisioning requires Docker (not available in this experimental shim).
		return nil
	})

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": false,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.alloydb.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
	})
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
