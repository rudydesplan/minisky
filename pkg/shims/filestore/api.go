package filestore

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	registry.Register("file.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Filestore v1)
// ─────────────────────────────────────────────────────────────────────────────

// Instance represents a Cloud Filestore instance.
type Instance struct {
	Name        string            `json:"name"`
	Tier        string            `json:"tier"`
	FileShares  []FileShare       `json:"fileShares,omitempty"`
	Networks    []Network         `json:"networks,omitempty"`
	State       string            `json:"state,omitempty"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Description string            `json:"description,omitempty"`
}

// FileShare represents a file share configuration.
type FileShare struct {
	Name       string `json:"name"`
	CapacityGb int64  `json:"capacityGb,omitempty"`
}

func (share *FileShare) UnmarshalJSON(data []byte) error {
	var wire struct {
		Name       string          `json:"name"`
		CapacityGb json.RawMessage `json:"capacityGb"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	share.Name = wire.Name
	if len(wire.CapacityGb) == 0 {
		return nil
	}
	if err := json.Unmarshal(wire.CapacityGb, &share.CapacityGb); err == nil {
		return nil
	}
	var value string
	if err := json.Unmarshal(wire.CapacityGb, &value); err != nil {
		return err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return err
	}
	share.CapacityGb = parsed
	return nil
}

// Network represents a network configuration.
type Network struct {
	Network string   `json:"network"`
	Modes   []string `json:"modes,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Filestore v1 REST shim.
type API struct {
	mu                 sync.RWMutex
	persistMu          sync.Mutex
	backendMu          sync.Mutex
	opMgr              *orchestrator.OperationManager
	stateStore         filestoreStateStore
	instances          map[string]*Instance
	dataRoot           string
	removeInstanceData func(string) error
	initErr            error
}

// NewAPI creates a new Filestore API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newAPI(opMgr, state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: Filestore] persistence degraded: %v", err)
		api.initErr = fmt.Errorf("open Filestore state: %w", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Filestore] state rehydration failed: %v", err)
		api.initErr = fmt.Errorf("load Filestore state: %w", err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store filestoreStateStore) *API {
	api := &API{
		opMgr:      opMgr,
		stateStore: store,
		instances:  make(map[string]*Instance),
		dataRoot:   filepath.Join(config.GetStateDir(), "profiles", config.GetProfile(), "filestore-data"),
	}
	api.removeInstanceData = func(name string) error {
		return secureRemoveTree(api.dataRoot, name)
	}
	return api
}

func newTestAPI() *API {
	return newAPI(orchestrator.NewOperationManager(), nil)
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Filestore] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initErr != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Filestore state is unavailable")
		return
	}

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.HasSuffix(r.URL.Path, "/instances") && r.Method == http.MethodPost:
		api.createInstance(w, r)
	case strings.HasSuffix(r.URL.Path, "/instances") && r.Method == http.MethodGet:
		api.listInstances(w, r)
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodGet:
		api.getInstance(w, r)
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodPatch:
		api.patchInstance(w, r)
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodDelete:
		api.deleteInstance(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Filestore resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

var validTiers = map[string]bool{
	"BASIC_HDD":      true,
	"BASIC_SSD":      true,
	"HIGH_SCALE_SSD": true,
	"ENTERPRISE":     true,
}

const (
	maxLocalFileBytes     = 16 << 20
	maxLocalInstanceBytes = 64 << 20
	maxLocalInstanceFiles = 1024
)

func (api *API) createInstance(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	instanceID := r.URL.Query().Get("instanceId")
	if !validLocalComponent(instanceID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instanceId query parameter is required")
		return
	}

	var inst Instance
	if err := json.NewDecoder(r.Body).Decode(&inst); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if inst.Tier == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'tier' is required")
		return
	}
	if !validTiers[inst.Tier] {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid tier: "+inst.Tier)
		return
	}
	if len(inst.FileShares) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'fileShares' is required")
		return
	}
	seenShares := make(map[string]struct{}, len(inst.FileShares))
	for _, share := range inst.FileShares {
		if !validLocalComponent(share.Name) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "file share name is invalid")
			return
		}
		if _, duplicate := seenShares[share.Name]; duplicate {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "file share names must be unique")
			return
		}
		seenShares[share.Name] = struct{}{}
	}

	name := fmt.Sprintf("projects/%s/locations/%s/instances/%s", project, location, instanceID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	inst.Name = name
	inst.State = "READY"
	inst.CreateTime = now
	inst.UpdateTime = now

	api.backendMu.Lock()
	defer api.backendMu.Unlock()
	api.mu.Lock()
	if _, exists := api.instances[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "instance already exists: "+instanceID)
		return
	}
	api.mu.Unlock()
	if err := api.createShareDirectories(name, inst.FileShares); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to create local file shares")
		return
	}
	api.mu.Lock()
	api.instances[name] = &inst
	api.mu.Unlock()

	// Register LRO first (if fails → rollback map, return 503)
	op, err := api.opMgr.RegisterDurable("filestore#operation", "create", name, "", location)
	if err != nil {
		api.mu.Lock()
		delete(api.instances, name)
		api.mu.Unlock()
		_ = api.removeInstanceData(api.instanceDataKey(name))
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	// Then persist (if fails → rollback map, return 503)
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.instances, name)
		api.mu.Unlock()
		if api.compensateMutation(op.Name, err) {
			api.mu.RLock()
			_, committed := api.instances[name]
			api.mu.RUnlock()
			if !committed {
				_ = api.removeInstanceData(api.instanceDataKey(name))
			}
		}
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
			"@type":      "type.googleapis.com/google.cloud.filestore.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getInstance(w http.ResponseWriter, r *http.Request) {
	name := buildInstanceName(r.URL.Path)

	api.mu.RLock()
	inst, ok := api.instances[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "instance not found: "+name)
		return
	}
	clone := cloneInstance(inst)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listInstances(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/instances/", project, location)

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
	var all []*Instance
	for key, inst := range api.instances {
		if strings.HasPrefix(key, prefix) {
			all = append(all, cloneInstance(inst))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "file.instances",
		Parent:  strings.TrimSuffix(prefix, "/instances/"),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(instance *Instance) string { return instance.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*Instance{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"instances":     result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchInstance(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildInstanceName(r.URL.Path)

	api.mu.RLock()
	existing, ok := api.instances[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "instance not found: "+name)
		return
	}
	old := cloneInstance(existing)
	api.mu.RUnlock()

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	raw, _ := json.Marshal(old)
	var merged map[string]any
	_ = json.Unmarshal(raw, &merged)

	updateMask := r.URL.Query().Get("updateMask")
	fields := make([]string, 0, len(patch))
	if updateMask == "" {
		for field := range patch {
			fields = append(fields, field)
		}
	} else {
		fields = strings.Split(updateMask, ",")
	}
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "labels" && field != "description" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
				"unsupported or backing-data-sensitive update field: "+field)
			return
		}
		if value, exists := patch[field]; exists {
			merged[field] = value
		}
	}

	merged["name"] = old.Name
	merged["createTime"] = old.CreateTime
	merged["state"] = old.State
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated Instance
	_ = json.Unmarshal(updatedRaw, &updated)
	if err := validateInstance(name, &updated); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	op, err := api.opMgr.RegisterScopedTargetDurable("filestore#operation", "update", name)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	api.mu.Lock()
	current := api.instances[name]
	if current == nil || current.UpdateTime != old.UpdateTime {
		api.mu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, http.StatusConflict, "ABORTED", "instance changed during update")
		return
	}
	api.instances[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.instances[name] = old
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	response, _ := json.Marshal(&updated)
	if err := api.opMgr.FinalizeScopedDurable(op.Name, response, 0, ""); err != nil {
		api.mu.Lock()
		api.instances[name] = old
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "Failed to persist operation result")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

// WriteShareFile writes bounded test/workload data to the local Filestore substitute.
// The backing host path is intentionally not part of the HTTP resource model.
func (api *API) WriteShareFile(instanceName, shareName, relativePath string, data []byte) error {
	api.backendMu.Lock()
	defer api.backendMu.Unlock()
	relative, err := api.shareFilePath(instanceName, shareName, relativePath)
	if err != nil {
		return err
	}
	if len(data) > maxLocalFileBytes {
		return fmt.Errorf("file exceeds 16 MiB local substitute limit")
	}
	files, total, err := secureTreeUsage(api.dataRoot, api.instanceDataKey(instanceName),
		maxLocalInstanceFiles, maxLocalInstanceBytes)
	if err != nil {
		return err
	}
	oldSize, exists, err := secureFileSize(api.dataRoot, relative)
	if err != nil {
		return err
	}
	if (!exists && files >= maxLocalInstanceFiles) ||
		total-oldSize+int64(len(data)) > maxLocalInstanceBytes {
		return fmt.Errorf("instance exceeds local substitute quota")
	}
	return secureWriteFile(api.dataRoot, relative, data)
}

// ReadShareFile reads bounded data from the local Filestore substitute.
func (api *API) ReadShareFile(instanceName, shareName, relativePath string) ([]byte, error) {
	api.backendMu.Lock()
	defer api.backendMu.Unlock()
	relative, err := api.shareFilePath(instanceName, shareName, relativePath)
	if err != nil {
		return nil, err
	}
	data, err := secureReadFile(api.dataRoot, relative, maxLocalFileBytes)
	if err != nil {
		return nil, err
	}
	if len(data) > maxLocalFileBytes {
		return nil, fmt.Errorf("file exceeds 16 MiB local substitute limit")
	}
	return data, nil
}

func (api *API) createShareDirectories(instanceName string, shares []FileShare) error {
	for _, share := range shares {
		if !validLocalComponent(share.Name) {
			return fmt.Errorf("invalid share name")
		}
		if err := secureMkdirAll(api.dataRoot, filepath.Join(api.instanceDataKey(instanceName), share.Name)); err != nil {
			_ = api.removeInstanceData(api.instanceDataKey(instanceName))
			return err
		}
	}
	return nil
}

func (api *API) shareFilePath(instanceName, shareName, relativePath string) (string, error) {
	api.mu.RLock()
	instance := api.instances[instanceName]
	found := false
	if instance != nil {
		if instance.State == "READY" {
			for _, share := range instance.FileShares {
				if share.Name == shareName {
					found = true
					break
				}
			}
		}
	}
	api.mu.RUnlock()
	if !found {
		return "", fmt.Errorf("instance or share not found")
	}
	if strings.Contains(relativePath, `\`) {
		return "", fmt.Errorf("invalid relative file path")
	}
	clean := filepath.Clean(relativePath)
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid relative file path")
	}
	for _, component := range strings.Split(clean, "/") {
		if !validLocalComponent(component) {
			return "", fmt.Errorf("invalid relative file path")
		}
	}
	return filepath.Join(api.instanceDataKey(instanceName), shareName, clean), nil
}

func (api *API) instanceDataPath(instanceName string) string {
	return filepath.Join(api.dataRoot, api.instanceDataKey(instanceName))
}

func (api *API) instanceDataKey(instanceName string) string {
	sum := sha256.Sum256([]byte(instanceName))
	return fmt.Sprintf("%x", sum[:16])
}

func (api *API) localShareTreeReady(instanceName string, shares []FileShare) bool {
	if len(shares) == 0 {
		return true
	}
	for _, share := range shares {
		exists, err := secureDirectoryExists(
			api.dataRoot,
			filepath.Join(api.instanceDataKey(instanceName), share.Name),
		)
		if err != nil || !exists {
			return false
		}
	}
	return true
}

func validLocalComponent(value string) bool {
	return value != "" && value != "." && value != ".." &&
		!strings.ContainsAny(value, `/\`)
}

func (api *API) deleteInstance(w http.ResponseWriter, r *http.Request) {
	name := buildInstanceName(r.URL.Path)
	project, location, _ := parseParent(r.URL.Path)

	api.backendMu.Lock()
	defer api.backendMu.Unlock()
	api.mu.Lock()
	resource, exists := api.instances[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "instance not found: "+name)
		return
	}
	api.mu.Unlock()

	op, err := api.opMgr.RegisterDurable("filestore#operation", "delete", name, "", location)
	if err != nil {
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	dataKey := api.instanceDataKey(name)
	tombstone := dataKey + fmt.Sprintf(".deleting-%d", time.Now().UnixNano())
	renamed := false
	if err := secureRename(api.dataRoot, dataKey, tombstone); err == nil {
		renamed = true
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = api.opMgr.RollbackScopedRegistration(op.Name)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to remove local file shares")
		return
	}
	api.mu.Lock()
	delete(api.instances, name)
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.instances[name] = resource
		api.mu.Unlock()
		if renamed {
			_ = secureRename(api.dataRoot, tombstone, dataKey)
		}
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}
	if renamed {
		if err := api.removeInstanceData(tombstone); err != nil {
			_ = secureRename(api.dataRoot, tombstone, dataKey)
			api.mu.Lock()
			api.instances[name] = resource
			api.mu.Unlock()
			if persistErr := api.persistState(); persistErr != nil {
				api.compensateMutation(op.Name, persistErr)
				writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "cleanup and metadata rollback failed")
				return
			}
			_ = api.opMgr.RollbackScopedRegistration(op.Name)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to remove local file shares")
			return
		}
	}
	api.opMgr.MarkDone(op.Name)

	opName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": opName,
		"done": true,
		"metadata": map[string]any{
			"@type":  "type.googleapis.com/google.cloud.filestore.v1.OperationMetadata",
			"target": name,
			"verb":   "delete",
		},
	})
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "filestore#operation")
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

func buildInstanceName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	instanceID := extractAfter(path, "instances")
	return fmt.Sprintf("projects/%s/locations/%s/instances/%s", project, location, instanceID)
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
