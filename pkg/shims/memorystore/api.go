package memorystore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

const memorystoreStateEntry = "memorystore/redis"

const redisBackendTimeout = 35 * time.Second

func init() {
	registry.Register("redis.googleapis.com", func(ctx *registry.Context) http.Handler {
		var logAPI *logging.API
		if handler, ok := ctx.GetShim("logging.googleapis.com").(*logging.API); ok {
			logAPI = handler
		}
		return NewAPI(ctx.OpMgr, ctx.SvcMgr, logAPI)
	})
	registry.Register("memcache.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewMemcacheAPI(ctx.OpMgr, memcacheBackendFromManager(ctx.SvcMgr))
	})
}

type Instance struct {
	Name                  string             `json:"name"`
	DisplayName           string             `json:"displayName,omitempty"`
	Labels                map[string]string  `json:"labels,omitempty"`
	Tier                  string             `json:"tier"`
	MemorySizeGb          int                `json:"memorySizeGb"`
	Host                  string             `json:"host,omitempty"`
	Port                  int                `json:"port,omitempty"`
	State                 string             `json:"state"`
	CreateTime            string             `json:"createTime"`
	LocationId            string             `json:"locationId"`
	AlternativeLocationId string             `json:"alternativeLocationId,omitempty"`
	AuthorizedNetwork     string             `json:"authorizedNetwork,omitempty"`
	ConnectMode           string             `json:"connectMode,omitempty"`
	PersistenceConfig     *PersistenceConfig `json:"persistenceConfig,omitempty"`
	RedisVersion          string             `json:"redisVersion,omitempty"`
	TransitEncryptionMode string             `json:"transitEncryptionMode,omitempty"`
	BackendID             string             `json:"-"`
}

type PersistenceConfig struct {
	PersistenceMode   string `json:"persistenceMode"`
	RdbSnapshotPeriod string `json:"rdbSnapshotPeriod,omitempty"`
}

type redisBackend interface {
	ProvisionRedis(context.Context, string, string) (string, error)
	ReconcileRedis(context.Context, string) (string, bool, error)
	DeleteRedis(context.Context, string) error
}

type serviceManagerBackend struct {
	manager *orchestrator.ServiceManager
}

func (b serviceManagerBackend) ProvisionRedis(ctx context.Context, id, image string) (string, error) {
	if b.manager == nil {
		return "", fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.ProvisionRedis(ctx, id, image)
}

func (b serviceManagerBackend) ReconcileRedis(ctx context.Context, id string) (string, bool, error) {
	if b.manager == nil {
		return "", false, fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.ReconcileRedis(ctx, id)
}

func (b serviceManagerBackend) DeleteRedis(ctx context.Context, id string) error {
	if b.manager == nil {
		return fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.DeleteRedis(ctx, id)
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type redisMetadata struct {
	Instances  map[string]persistedInstance `json:"instances"`
	Operations map[string]operationTarget   `json:"operations,omitempty"`
}

type persistedInstance struct {
	Instance  *Instance `json:"instance"`
	BackendID string    `json:"backendId"`
}

type operationTarget struct {
	ManagerName string `json:"managerName"`
	ResourceKey string `json:"resourceKey"`
	Delete      bool   `json:"delete,omitempty"`
}

type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	backend    redisBackend
	logAPI     *logging.API
	store      stateStore
	instances  map[string]*Instance
	operations map[string]operationTarget
	initErr    error
}

func NewAPI(opMgr *orchestrator.OperationManager, manager *orchestrator.ServiceManager, logAPI *logging.API) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Memorystore] state disabled: %v", err)
		return newAPI(opMgr, serviceManagerBackend{manager: manager}, logAPI, nil)
	}
	api, err := NewAPIWithStore(opMgr, serviceManagerBackend{manager: manager}, logAPI, store)
	if err != nil {
		log.Printf("[Shim: Memorystore] state rehydration failed: %v", err)
		disabled := newAPI(opMgr, serviceManagerBackend{manager: manager}, logAPI, nil)
		disabled.initErr = err
		return disabled
	}
	return api
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, backend redisBackend, logAPI *logging.API, store stateStore) (*API, error) {
	api := newAPI(opMgr, backend, logAPI, store)
	if store == nil {
		return api, nil
	}
	var saved redisMetadata
	if err := store.Load(memorystoreStateEntry, &saved); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Memorystore metadata: %w", err)
	}
	if saved.Instances != nil {
		for key, persisted := range saved.Instances {
			if persisted.Instance != nil {
				persisted.Instance.BackendID = persisted.BackendID
				api.instances[key] = persisted.Instance
			}
		}
	}
	if saved.Operations != nil {
		api.operations = saved.Operations
	}
	changed := false
	for key, instance := range api.instances {
		if instance == nil {
			delete(api.instances, key)
			changed = true
			continue
		}
		if instance.State == "DELETING" {
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), redisBackendTimeout)
			deleteErr := backend.DeleteRedis(deleteCtx, instance.BackendID)
			deleteCancel()
			if deleteErr != nil {
				return nil, fmt.Errorf("resume deleting Redis backend %q: %w", instance.BackendID, deleteErr)
			}
			delete(api.instances, key)
			for _, target := range api.operations {
				if target.Delete && target.ResourceKey == key {
					api.opMgr.MarkDone(target.ManagerName)
				}
			}
			changed = true
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		endpoint, owned, err := backend.ReconcileRedis(ctx, instance.BackendID)
		cancel()
		if err == nil && !owned && instance.State == "CREATING" {
			delete(api.instances, key)
			changed = true
			continue
		}
		if err != nil || !owned {
			instance.State = "REPAIRING"
			instance.Host = ""
			instance.Port = 0
			changed = true
			continue
		}
		host, port, err := parseLoopbackEndpoint(endpoint)
		if err != nil {
			instance.State = "REPAIRING"
			instance.Host = ""
			instance.Port = 0
		} else {
			instance.State = "READY"
			instance.Host = host
			instance.Port = port
		}
		changed = true
	}
	if changed {
		if err := api.persist(); err != nil {
			return nil, fmt.Errorf("persist Memorystore reconciliation: %w", err)
		}
	}
	return api, nil
}

func newAPI(opMgr *orchestrator.OperationManager, backend redisBackend, logAPI *logging.API, store stateStore) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	return &API{
		opMgr:      opMgr,
		backend:    backend,
		logAPI:     logAPI,
		store:      store,
		instances:  make(map[string]*Instance),
		operations: make(map[string]operationTarget),
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Memorystore] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initErr != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Memorystore state is unavailable")
		return
	}
	if strings.EqualFold(strings.Split(r.Host, ":")[0], "memorystore.googleapis.com") {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Memorystore for Valkey requires a dedicated owned Valkey backend; the Redis backend is not reused")
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
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodDelete:
		api.deleteInstance(w, r)
	case strings.Contains(r.URL.Path, ":export") || strings.Contains(r.URL.Path, ":import") ||
		strings.Contains(r.URL.Path, ":failover") || strings.Contains(r.URL.Path, ":upgrade"):
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Memorystore import, export, failover, and upgrade are not implemented")
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memorystore resource not found")
	}
}

func (api *API) createInstance(w http.ResponseWriter, r *http.Request) {
	project, location := projectLocation(r.URL.Path)
	id := r.URL.Query().Get("instanceId")
	if project == "" || location == "" || !validID(id) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project, location, and a valid instanceId are required")
		return
	}
	var instance Instance
	if err := decodeJSON(r, &instance); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid Redis instance JSON")
		return
	}
	if instance.MemorySizeGb <= 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "memorySizeGb must be greater than zero")
		return
	}
	if instance.Tier == "" {
		instance.Tier = "BASIC"
	}
	if instance.Tier != "BASIC" && instance.Tier != "STANDARD_HA" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "tier must be BASIC or STANDARD_HA")
		return
	}
	if instance.TransitEncryptionMode != "" && instance.TransitEncryptionMode != "DISABLED" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"transitEncryptionMode must be DISABLED because the local Valkey backend does not support TLS")
		return
	}
	if instance.ConnectMode != "" && instance.ConnectMode != "DIRECT_PEERING" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"connectMode must be DIRECT_PEERING because the local Valkey backend does not implement private service access")
		return
	}
	if instance.RedisVersion == "" {
		instance.RedisVersion = "REDIS_7_2"
	}
	name := fmt.Sprintf("projects/%s/locations/%s/instances/%s", project, location, id)
	key := name
	instance.Name = name
	instance.LocationId = location
	instance.State = "CREATING"
	instance.CreateTime = time.Now().UTC().Format(time.RFC3339)
	instance.BackendID = backendID(project, location, id)

	api.mu.Lock()
	if _, exists := api.instances[key]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Redis instance already exists: "+id)
		return
	}
	managerOp, err := api.opMgr.RegisterDurable("redis#operation", "CREATE", name, "", location)
	if err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis operation")
		return
	}
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, managerOp.Name)
	api.instances[key] = cloneInstance(&instance)
	api.operations[serviceName] = operationTarget{ManagerName: managerOp.Name, ResourceKey: key}
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.operations, serviceName)
		delete(api.instances, key)
		api.mu.Unlock()
		api.opMgr.Fail(managerOp.Name, http.StatusInternalServerError, "failed to persist Redis operation")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis instance and operation")
		return
	}
	image := redisImage(instance.RedisVersion)
	api.opMgr.RunAsync(managerOp.Name, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		endpoint, err := api.backend.ProvisionRedis(ctx, instance.BackendID, image)
		cancel()
		api.mu.Lock()
		current := api.instances[key]
		if current != nil {
			if err != nil {
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			} else {
				host, port, parseErr := parseLoopbackEndpoint(endpoint)
				if parseErr != nil {
					err = parseErr
					current.State = "REPAIRING"
					current.Host = ""
					current.Port = 0
				} else {
					current.State = "READY"
					current.Host = host
					current.Port = port
				}
			}
		}
		api.mu.Unlock()
		if err != nil {
			cleanupErr := api.cleanupRedisBackend(instance.BackendID)
			api.mu.Lock()
			if cleanupErr == nil {
				delete(api.instances, key)
			} else if current := api.instances[key]; current != nil {
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			}
			api.mu.Unlock()
			persistErr := api.persist()
			return errors.Join(err, cleanupErr, persistErr)
		}
		if persistErr := api.persist(); persistErr != nil {
			cleanupErr := api.cleanupRedisBackend(instance.BackendID)
			api.mu.Lock()
			if cleanupErr == nil {
				delete(api.instances, key)
			} else if current := api.instances[key]; current != nil {
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			}
			api.mu.Unlock()
			compensationErr := api.persist()
			return errors.Join(persistErr, cleanupErr, compensationErr)
		}
		api.pushLog(project, "INFO", id, "Redis instance is READY")
		return nil
	})
	api.writeOperation(w, serviceName, managerOp)
}

func (api *API) listInstances(w http.ResponseWriter, r *http.Request) {
	project, location := projectLocation(r.URL.Path)
	prefix := fmt.Sprintf("projects/%s/locations/%s/instances/", project, location)
	api.mu.RLock()
	instances := make([]*Instance, 0)
	for key, instance := range api.instances {
		if strings.HasPrefix(key, prefix) {
			instances = append(instances, cloneInstance(instance))
		}
	}
	api.mu.RUnlock()
	sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })
	_ = json.NewEncoder(w).Encode(map[string]any{"instances": instances})
}

func (api *API) getInstance(w http.ResponseWriter, r *http.Request) {
	name := instanceName(r.URL.Path)
	api.mu.RLock()
	instance := cloneInstance(api.instances[name])
	api.mu.RUnlock()
	if instance == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis instance not found")
		return
	}
	_ = json.NewEncoder(w).Encode(instance)
}

func (api *API) deleteInstance(w http.ResponseWriter, r *http.Request) {
	name := instanceName(r.URL.Path)
	api.mu.Lock()
	instance := api.instances[name]
	if instance == nil {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis instance not found")
		return
	}
	project, location, id := instanceParts(name)
	backendResource := instance.BackendID
	previous := cloneInstance(instance)
	managerOp, err := api.opMgr.RegisterDurable("redis#operation", "DELETE", name, "", location)
	if err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis operation")
		return
	}
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, managerOp.Name)
	instance.State = "DELETING"
	api.operations[serviceName] = operationTarget{ManagerName: managerOp.Name, ResourceKey: name, Delete: true}
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.operations, serviceName)
		api.instances[name] = previous
		api.mu.Unlock()
		_ = api.opMgr.FailDurable(managerOp.Name, http.StatusInternalServerError, "failed to persist Redis deletion")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis deletion")
		return
	}
	api.opMgr.RunAsync(managerOp.Name, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		err := api.backend.DeleteRedis(ctx, backendResource)
		cancel()
		if err != nil {
			api.mu.Lock()
			if current := api.instances[name]; current != nil {
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			}
			api.mu.Unlock()
			if persistErr := api.persist(); persistErr != nil {
				return errors.Join(err, persistErr)
			}
			return err
		}
		api.mu.Lock()
		delete(api.instances, name)
		api.mu.Unlock()
		if err := api.persist(); err != nil {
			return err
		}
		api.pushLog(project, "INFO", id, "Redis instance deleted")
		return nil
	})
	api.writeOperation(w, serviceName, managerOp)
}

func (api *API) cleanupRedisBackend(backendID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
	defer cancel()
	return api.backend.DeleteRedis(ctx, backendID)
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/")
	api.mu.RLock()
	target, ok := api.operations[name]
	api.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis operation not found")
		return
	}
	operation := api.opMgr.Get(target.ManagerName)
	if operation == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis operation not found")
		return
	}
	api.writeOperation(w, name, operation)
}

func (api *API) writeOperation(w http.ResponseWriter, name string, operation *orchestrator.Operation) {
	response := map[string]any{
		"name": name,
		"done": operation.Done,
		"metadata": map[string]any{
			"target": operation.TargetLink,
			"verb":   operation.OperationType,
		},
	}
	if operation.Error != nil {
		response["error"] = operation.Error
	} else if operation.Done {
		api.mu.RLock()
		target := api.operations[name]
		instance := cloneInstance(api.instances[target.ResourceKey])
		api.mu.RUnlock()
		if target.Delete {
			response["response"] = map[string]any{}
		} else if instance != nil {
			response["response"] = instance
		}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (api *API) persist() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	instances := make(map[string]persistedInstance, len(api.instances))
	for key, instance := range api.instances {
		instances[key] = persistedInstance{Instance: instance, BackendID: instance.BackendID}
	}
	operations := make(map[string]operationTarget, len(api.operations))
	for name, target := range api.operations {
		operations[name] = target
	}
	payload, err := json.Marshal(redisMetadata{Instances: instances, Operations: operations})
	api.mu.RUnlock()
	if err != nil {
		return err
	}
	return api.store.Save(memorystoreStateEntry, json.RawMessage(payload))
}

func (api *API) pushLog(project, severity, id, message string) {
	if api.logAPI != nil {
		api.logAPI.PushLog(project, severity, "memorystore_instance", id, message)
	}
}

func redisImage(version string) string {
	registry := config.GetImageRegistry()
	target := strings.TrimPrefix(version, "REDIS_")
	target = strings.ReplaceAll(target, "_", ".")
	for _, version := range registry.Memorystore.Valkey.Versions {
		if strings.HasPrefix(version.Version, target) {
			return version.Image
		}
	}
	return registry.Memorystore.Valkey.DefaultImage
}

func parseLoopbackEndpoint(endpoint string) (string, int, error) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", 0, fmt.Errorf("invalid Redis endpoint: %w", err)
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", 0, fmt.Errorf("Redis endpoint is not loopback")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid Redis endpoint port")
	}
	return host, port, nil
}

func projectLocation(path string) (string, string) {
	return extractAfter(path, "projects"), extractAfter(path, "locations")
}

func instanceName(path string) string {
	project, location := projectLocation(path)
	return fmt.Sprintf("projects/%s/locations/%s/instances/%s", project, location, extractAfter(path, "instances"))
}

func instanceParts(name string) (string, string, string) {
	return extractAfter(name, "projects"), extractAfter(name, "locations"), extractAfter(name, "instances")
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

func validID(id string) bool {
	if len(id) < 1 || len(id) > 40 {
		return false
	}
	for i, char := range id {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (char == '-' && i > 0) {
			continue
		}
		return false
	}
	return id[len(id)-1] != '-'
}

func backendID(project, location, id string) string {
	hasher := sha256.New()
	for _, part := range []string{project, location, id} {
		_ = binary.Write(hasher, binary.BigEndian, uint32(len(part)))
		_, _ = hasher.Write([]byte(part))
	}
	return fmt.Sprintf("redis-%x", hasher.Sum(nil)[:16])
}

func cloneInstance(instance *Instance) *Instance {
	if instance == nil {
		return nil
	}
	clone := *instance
	if instance.Labels != nil {
		clone.Labels = make(map[string]string, len(instance.Labels))
		for key, value := range instance.Labels {
			clone.Labels[key] = value
		}
	}
	if instance.PersistenceConfig != nil {
		config := *instance.PersistenceConfig
		clone.PersistenceConfig = &config
	}
	return &clone
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "status": status, "message": message, "details": []any{}},
	})
}
