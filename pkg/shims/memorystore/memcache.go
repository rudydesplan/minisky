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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/pagination"
	"minisky/pkg/state"
)

const (
	memcacheStateEntry      = "memorystore/memcache"
	memcacheOperationKind   = "memcache#operation"
	memcacheBackendTimeout  = 35 * time.Second
	maxMemcacheRequestBytes = 1 << 20
)

// MemcacheBackend is the narrow data-plane contract supplied by orchestration.
// Implementations must only report Owned for resources created for MiniSky.
type MemcacheBackend interface {
	ProvisionMemcache(context.Context, string, int, int, int, string, map[string]string) ([]string, bool, bool, error)
	UpdateMemcache(context.Context, string, int, int, int, string, map[string]string) ([]string, bool, bool, error)
	ReconcileMemcache(context.Context, string, int, int, int, string, map[string]string) ([]string, bool, bool, error)
	DeleteMemcache(context.Context, string) error
}

var _ MemcacheBackend = (*orchestrator.ServiceManager)(nil)

// MemcacheBackendSpec contains no GCP credentials or host-specific details.
type MemcacheBackendSpec struct {
	BackendID string
	NodeCount int
	CPUCount  int
	MemoryMB  int
	Version   string
	Params    map[string]string
}

// MemcacheBackendResult describes exact-owned local nodes.
type MemcacheBackendResult struct {
	Owned     bool
	Exists    bool
	Endpoints []string
}

type MemcacheNodeConfig struct {
	CPUCount     int `json:"cpuCount"`
	MemorySizeMB int `json:"memorySizeMb"`
}

type MemcacheParameters struct {
	ID     string            `json:"id,omitempty"`
	Params map[string]string `json:"params,omitempty"`
}

type MemcacheNode struct {
	NodeID              string              `json:"nodeId"`
	Zone                string              `json:"zone"`
	State               string              `json:"state"`
	Host                string              `json:"host"`
	Port                int                 `json:"port"`
	Parameters          *MemcacheParameters `json:"parameters,omitempty"`
	MemcacheVersion     string              `json:"memcacheVersion"`
	MemcacheFullVersion string              `json:"memcacheFullVersion"`
}

type MemcacheMaintenancePolicy struct {
	Description             string                      `json:"description,omitempty"`
	WeeklyMaintenanceWindow []MemcacheMaintenanceWindow `json:"weeklyMaintenanceWindow,omitempty"`
	CreateTime              string                      `json:"createTime,omitempty"`
	UpdateTime              string                      `json:"updateTime,omitempty"`
}

type MemcacheMaintenanceWindow struct {
	Day       string         `json:"day"`
	StartTime map[string]int `json:"startTime"`
	Duration  string         `json:"duration"`
}

// MemcacheInstance is the bounded v1 provider-facing resource.
type MemcacheInstance struct {
	Name                string                     `json:"name"`
	DisplayName         string                     `json:"displayName,omitempty"`
	Labels              map[string]string          `json:"labels,omitempty"`
	AuthorizedNetwork   string                     `json:"authorizedNetwork,omitempty"`
	Zones               []string                   `json:"zones,omitempty"`
	NodeCount           int                        `json:"nodeCount"`
	NodeConfig          *MemcacheNodeConfig        `json:"nodeConfig"`
	MemcacheVersion     string                     `json:"memcacheVersion"`
	Parameters          *MemcacheParameters        `json:"parameters,omitempty"`
	MemcacheNodes       []MemcacheNode             `json:"memcacheNodes,omitempty"`
	CreateTime          string                     `json:"createTime"`
	UpdateTime          string                     `json:"updateTime"`
	State               string                     `json:"state"`
	MemcacheFullVersion string                     `json:"memcacheFullVersion,omitempty"`
	DiscoveryEndpoint   string                     `json:"discoveryEndpoint,omitempty"`
	MaintenancePolicy   *MemcacheMaintenancePolicy `json:"maintenancePolicy,omitempty"`
	ReservedIPRangeID   []string                   `json:"reservedIpRangeId,omitempty"`
}

type memcachePersistedInstance struct {
	Instance  *MemcacheInstance `json:"instance"`
	BackendID string            `json:"backendId"`
}

type memcacheMetadata struct {
	Instances  map[string]memcachePersistedInstance `json:"instances"`
	Operations map[string]memcacheOperationRecord   `json:"operations,omitempty"`
}

type memcacheOperationRecord struct {
	Action       string                     `json:"action"`
	ResourceName string                     `json:"resourceName"`
	Previous     *memcachePersistedInstance `json:"previous,omitempty"`
}

type MemcacheAPI struct {
	mu              sync.RWMutex
	mutationMu      sync.Mutex
	opMgr           *orchestrator.OperationManager
	backend         MemcacheBackend
	store           stateStore
	instances       map[string]memcachePersistedInstance
	operations      map[string]memcacheOperationRecord
	initErr         error
	operationRunner func(string, func() error)
}

type degradedStateStore interface {
	Degraded() error
}

func NewMemcacheAPI(opMgr *orchestrator.OperationManager, backend MemcacheBackend) *MemcacheAPI {
	rawStore, err := state.New(config.GetStateDir(), config.GetProfile())
	store := newMemcacheProductionStore(rawStore, err)
	if err != nil {
		log.Printf("[Shim: Memcached] persistence unavailable: %v", err)
		api := newMemcacheAPI(opMgr, backend, store)
		api.setInitializationError(fmt.Errorf("open Memcached state: %w", err))
		return api
	}
	api, err := newMemcacheAPIWithPersistentStore(opMgr, backend, store, true)
	if err != nil {
		log.Printf("[Shim: Memcached] state rehydration failed: %v", err)
		disabled := newMemcacheAPI(opMgr, backend, nil)
		disabled.setInitializationError(err)
		return disabled
	}
	return api
}

func newMemcacheProductionStore(delegate stateStore, initializationErr error) stateStore {
	return state.NewGuardedEntryStore(delegate, initializationErr)
}

func NewMemcacheAPIWithStore(opMgr *orchestrator.OperationManager, backend MemcacheBackend, store stateStore) (*MemcacheAPI, error) {
	return newMemcacheAPIWithPersistentStore(opMgr, backend, store, false)
}

func newMemcacheAPIWithPersistentStore(
	opMgr *orchestrator.OperationManager,
	backend MemcacheBackend,
	store stateStore,
	trustProcessManager bool,
) (*MemcacheAPI, error) {
	if store != nil {
		// NewMemcacheAPI receives the process manager created by startup from
		// this same profile store. Test/custom constructors cannot prove that
		// invariant, so they must let this API own a durable manager instead.
		if opMgr != nil && !trustProcessManager {
			return nil, errors.New("persistent Memcached API requires ownership of its operation manager")
		}
		if opMgr == nil {
			var err error
			opMgr, err = orchestrator.NewOperationManagerWithStore(store)
			if err != nil {
				return nil, fmt.Errorf("load Memcached operations: %w", err)
			}
		}
	}
	api := newMemcacheAPI(opMgr, backend, store)
	if store == nil {
		return api, nil
	}
	var saved memcacheMetadata
	if err := store.Load(memcacheStateEntry, &saved); err != nil {
		if !errors.Is(err, state.ErrNotFound) {
			return nil, fmt.Errorf("load Memcached metadata: %w", err)
		}
	}
	restored, err := restoreMemcacheInstances(saved.Instances)
	if err != nil {
		return nil, fmt.Errorf("validate Memcached metadata: %w", err)
	}
	api.instances = restored
	operations, err := restoreMemcacheOperations(saved.Operations, restored, api.opMgr)
	if err != nil {
		return nil, fmt.Errorf("validate Memcached operation associations: %w", err)
	}
	api.operations = operations
	if err := api.reconcileOrphanMemcacheOperations(); err != nil {
		return nil, fmt.Errorf("reconcile orphan Memcached operations: %w", err)
	}
	if err := api.reconcile(); err != nil {
		return nil, err
	}
	return api, nil
}

func newMemcacheAPI(opMgr *orchestrator.OperationManager, backend MemcacheBackend, store stateStore) *MemcacheAPI {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	api := &MemcacheAPI{
		opMgr:      opMgr,
		backend:    backend,
		store:      store,
		instances:  make(map[string]memcachePersistedInstance),
		operations: make(map[string]memcacheOperationRecord),
	}
	api.operationRunner = func(name string, work func() error) {
		go func() {
			if err := opMgr.AdvanceDurable(name, 5, orchestrator.StatusRunning); err != nil {
				api.setInitializationError(err)
				return
			}
			_ = work()
		}()
	}
	return api
}

func memcacheBackendFromManager(manager *orchestrator.ServiceManager) MemcacheBackend {
	if manager == nil {
		return nil
	}
	if contract, ok := any(manager).(MemcacheBackend); ok {
		return contract
	}
	return nil
}

func (api *MemcacheAPI) setInitializationError(err error) {
	if err == nil {
		return
	}
	api.mu.Lock()
	api.initErr = err
	api.mu.Unlock()
}

func (api *MemcacheAPI) initializationError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.initErr
}

func (api *MemcacheAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if api.initializationError() != nil || api.opMgr.PersistenceError() != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Memcached state is unavailable")
		return
	}

	if r.URL.RawPath != "" || r.URL.EscapedPath() != r.URL.Path {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memcached resource not found")
		return
	}
	route, ok := parseMemcacheRoute(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memcached resource not found")
		return
	}
	switch route.kind {
	case memcacheRouteOperations:
		switch r.Method {
		case http.MethodGet:
			api.getMemcacheOperation(w, r)
		case http.MethodDelete:
			writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
				"deleting Memcached operations is not implemented")
		default:
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	case memcacheRouteCollection:
		switch r.Method {
		case http.MethodPost:
			api.createMemcacheInstance(w, r, route)
		case http.MethodGet:
			api.listMemcacheInstances(w, r, route)
		default:
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	case memcacheRouteInstance:
		switch r.Method {
		case http.MethodGet:
			api.getMemcacheInstance(w, route)
		case http.MethodPatch:
			api.updateMemcacheInstance(w, r, route)
		case http.MethodDelete:
			api.deleteMemcacheInstance(w, r, route)
		default:
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	case memcacheRouteUnsupported:
		if r.Method != route.method {
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Memcached parameters, maintenance, and version operations are not implemented")
	}
}

type memcacheRouteKind int

const (
	memcacheRouteCollection memcacheRouteKind = iota
	memcacheRouteInstance
	memcacheRouteOperations
	memcacheRouteUnsupported
)

type memcacheRoute struct {
	kind     memcacheRouteKind
	project  string
	location string
	id       string
	name     string
	method   string
}

func parseMemcacheRoute(path string) (memcacheRoute, bool) {
	if !strings.HasPrefix(path, "/v1/") || strings.Contains(path, "//") {
		return memcacheRoute{}, false
	}
	trimmed := strings.TrimPrefix(path, "/v1/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 6 && parts[0] == "projects" && parts[1] != "" &&
		parts[2] == "locations" && parts[3] != "" && parts[4] == "instances" && parts[5] != "" {
		id := parts[5]
		if index := strings.IndexByte(id, ':'); index >= 0 {
			if !validID(id[:index]) {
				return memcacheRoute{}, false
			}
			action := id[index+1:]
			switch action {
			case "updateParameters":
				return memcacheRoute{kind: memcacheRouteUnsupported, method: http.MethodPatch}, true
			case "applyParameters", "rescheduleMaintenance", "upgrade":
				return memcacheRoute{kind: memcacheRouteUnsupported, method: http.MethodPost}, true
			default:
				return memcacheRoute{}, false
			}
		}
		return memcacheRoute{
			kind: memcacheRouteInstance, project: parts[1], location: parts[3], id: id,
			name: strings.Join(parts[:6], "/"),
		}, true
	}
	if len(parts) == 5 && parts[0] == "projects" && parts[1] != "" &&
		parts[2] == "locations" && parts[3] != "" && parts[4] == "instances" {
		return memcacheRoute{
			kind: memcacheRouteCollection, project: parts[1], location: parts[3],
			name: strings.Join(parts[:4], "/"),
		}, true
	}
	if len(parts) == 6 && parts[0] == "projects" && parts[1] != "" &&
		parts[2] == "locations" && parts[3] != "" && parts[4] == "operations" && parts[5] != "" {
		if index := strings.IndexByte(parts[5], ':'); index >= 0 {
			if parts[5][index+1:] == "cancel" {
				return memcacheRoute{kind: memcacheRouteUnsupported, method: http.MethodPost}, true
			}
			return memcacheRoute{}, false
		}
		return memcacheRoute{kind: memcacheRouteOperations, project: parts[1], location: parts[3]}, true
	}
	if len(parts) == 5 && parts[0] == "projects" && parts[1] != "" &&
		parts[2] == "locations" && parts[3] != "" && parts[4] == "operations" {
		return memcacheRoute{kind: memcacheRouteUnsupported, method: http.MethodGet}, true
	}
	return memcacheRoute{}, false
}

func (api *MemcacheAPI) createMemcacheInstance(w http.ResponseWriter, r *http.Request, route memcacheRoute) {
	id := r.URL.Query().Get("instanceId")
	if !validID(id) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instanceId is invalid")
		return
	}
	var requested MemcacheInstance
	if err := decodeMemcacheJSON(w, r, &requested); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid Memcached instance JSON")
		return
	}
	name := route.name + "/instances/" + id
	if requested.Name != "" && requested.Name != name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instance name does not match request path")
		return
	}
	if status, code, message := validateMemcacheCreate(route, &requested); code != 0 {
		writeError(w, code, status, message)
		return
	}
	if api.backend == nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Memcached backend is unavailable")
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	requested.Name = name
	requested.State = "CREATING"
	requested.CreateTime = now
	requested.UpdateTime = now
	requested.AuthorizedNetwork = canonicalMemcacheNetwork(route.project, requested.AuthorizedNetwork)
	if len(requested.Zones) == 0 {
		requested.Zones = []string{route.location + "-a"}
	}
	if requested.MemcacheVersion == "" {
		requested.MemcacheVersion = "MEMCACHE_1_5"
	}
	backendID := memcacheBackendID(route.project, route.location, id)

	if !api.mutationMu.TryLock() {
		writeError(w, http.StatusConflict, "ABORTED", "another Memcached mutation is in progress")
		return
	}
	if api.getPersisted(name).Instance != nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Memcached instance already exists")
		return
	}
	op, err := api.opMgr.RegisterScopedDurable(orchestrator.OperationScope{
		ServiceKind: memcacheOperationKind,
		Project:     route.project,
		Location:    route.location,
		Target:      name,
	}, "create")
	if err != nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to register Memcached operation")
		return
	}
	candidate := api.metadataSnapshot()
	candidate.Instances[name] = memcachePersistedInstance{Instance: cloneMemcacheInstance(&requested), BackendID: backendID}
	candidate.Operations[op.Name] = memcacheOperationRecord{Action: "create", ResourceName: name}
	if err := api.commitMetadata(candidate); err != nil {
		rollbackErr := api.opMgr.RollbackScopedRegistration(op.Name)
		api.mutationMu.Unlock()
		if rollbackErr != nil {
			api.setInitializationError(errors.Join(err, rollbackErr))
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Memcached creation")
		return
	}
	api.mutationMu.Unlock()

	spec := memcacheSpec(backendID, &requested)
	api.operationRunner(op.Name, func() error {
		return api.completeMemcacheCreate(op.Name, name, spec)
	})
	api.writeMemcacheOperation(w, op)
}

func validateMemcacheCreate(route memcacheRoute, instance *MemcacheInstance) (string, int, string) {
	if route.location == "-" {
		return "INVALID_ARGUMENT", http.StatusBadRequest, "instances cannot be created in location '-'"
	}
	if instance.NodeCount < 1 || instance.NodeConfig == nil ||
		instance.NodeConfig.CPUCount < 1 || instance.NodeConfig.MemorySizeMB < 1 {
		return "INVALID_ARGUMENT", http.StatusBadRequest, "nodeCount and positive nodeConfig values are required"
	}
	if instance.NodeCount != 1 {
		return "UNIMPLEMENTED", http.StatusNotImplemented, "the local Memcached backend supports exactly one node"
	}
	if instance.Parameters != nil {
		return "UNIMPLEMENTED", http.StatusNotImplemented, "custom Memcached parameters are not implemented"
	}
	if len(instance.DisplayName) > 80 {
		return "INVALID_ARGUMENT", http.StatusBadRequest, "displayName must not exceed 80 characters"
	}
	if instance.MemcacheVersion != "" && instance.MemcacheVersion != "MEMCACHE_1_5" &&
		instance.MemcacheVersion != "MEMCACHE_1_6_15" {
		return "INVALID_ARGUMENT", http.StatusBadRequest, "unsupported memcacheVersion"
	}
	if len(instance.Zones) > 1 {
		return "UNIMPLEMENTED", http.StatusNotImplemented, "multi-zone Memcached placement is not implemented"
	}
	if len(instance.Zones) == 1 && !strings.HasPrefix(instance.Zones[0], route.location+"-") {
		return "INVALID_ARGUMENT", http.StatusBadRequest, "zone must belong to the requested region"
	}
	if !supportedMemcacheNetwork(route.project, instance.AuthorizedNetwork) {
		return "UNIMPLEMENTED", http.StatusNotImplemented, "custom Memcached networks are not implemented"
	}
	if len(instance.ReservedIPRangeID) != 0 {
		return "UNIMPLEMENTED", http.StatusNotImplemented, "reserved IP ranges are not implemented"
	}
	if instance.MaintenancePolicy != nil {
		return "UNIMPLEMENTED", http.StatusNotImplemented, "maintenance policies are not implemented"
	}
	if instance.State != "" || instance.CreateTime != "" || instance.UpdateTime != "" ||
		len(instance.MemcacheNodes) != 0 || instance.DiscoveryEndpoint != "" ||
		instance.MemcacheFullVersion != "" {
		return "INVALID_ARGUMENT", http.StatusBadRequest, "output-only Memcached fields are not accepted"
	}
	return "", 0, ""
}

func (api *MemcacheAPI) completeMemcacheCreate(operationName, name string, spec MemcacheBackendSpec) error {
	api.mutationMu.Lock()
	mutationOwned := true
	releaseMutation := func() {
		if mutationOwned {
			mutationOwned = false
			api.mutationMu.Unlock()
		}
	}
	defer releaseMutation()

	ctx, cancel := context.WithTimeout(context.Background(), memcacheBackendTimeout)
	result, provisionErr := provisionMemcacheBackend(ctx, api.backend, spec)
	cancel()
	if provisionErr != nil || !result.Owned || !result.Exists {
		var cleanupErr error
		compensated := false
		if result.Owned {
			cleanupErr = api.deleteMemcacheBackend(spec.BackendID)
			compensated = cleanupErr == nil
		}
		authoritativelyAbsent := provisionErr == nil && !result.Exists
		var persistErr error
		if compensated || authoritativelyAbsent {
			candidate := api.snapshot()
			delete(candidate, name)
			persistErr = api.commit(candidate)
		}
		message := "Memcached backend provisioning failed"
		if provisionErr == nil {
			message = "Memcached backend ownership could not be verified"
		}
		finalizeErr := api.finalizeMemcacheFailureWithBarrier(
			operationName,
			message,
			(compensated || authoritativelyAbsent) && persistErr == nil,
			releaseMutation,
		)
		return errors.Join(errors.New(message), cleanupErr, persistErr, finalizeErr)
	}
	nodes, err := memcacheNodes(result, api.getPersisted(name).Instance)
	if err != nil {
		cleanupErr := api.deleteMemcacheBackend(spec.BackendID)
		candidate := api.snapshot()
		if cleanupErr == nil {
			delete(candidate, name)
		}
		var persistErr error
		if cleanupErr == nil {
			persistErr = api.commit(candidate)
		}
		finalizeErr := api.finalizeMemcacheFailureWithBarrier(operationName,
			"Memcached backend returned invalid endpoints", cleanupErr == nil && persistErr == nil,
			releaseMutation)
		return errors.Join(err, cleanupErr, persistErr, finalizeErr)
	}
	candidate := api.snapshot()
	persisted := candidate[name]
	if persisted.Instance == nil {
		return errors.New("Memcached creation metadata disappeared")
	}
	persisted.Instance.State = "READY"
	persisted.Instance.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	persisted.Instance.MemcacheNodes = nodes
	persisted.Instance.MemcacheFullVersion = memcacheFullVersion(persisted.Instance.MemcacheVersion)
	persisted.Instance.DiscoveryEndpoint = result.Endpoints[0]
	candidate[name] = persisted
	if err := api.commit(candidate); err != nil {
		cleanupErr := api.deleteMemcacheBackend(spec.BackendID)
		compensation := api.snapshot()
		if cleanupErr == nil {
			delete(compensation, name)
		}
		var compensationErr error
		if cleanupErr == nil {
			compensationErr = api.commit(compensation)
		}
		finalizeErr := api.finalizeMemcacheFailureWithBarrier(operationName,
			"Memcached state persistence failed after backend creation",
			cleanupErr == nil && compensationErr == nil,
			releaseMutation,
		)
		return errors.Join(err, cleanupErr, compensationErr, finalizeErr)
	}
	if err := api.finalizeMemcacheInstanceSuccessWithBarrier(
		operationName, persisted.Instance, releaseMutation,
	); err != nil {
		api.setInitializationError(err)
		return err
	}
	return nil
}

func (api *MemcacheAPI) getMemcacheInstance(w http.ResponseWriter, route memcacheRoute) {
	persisted := api.getPersisted(route.name)
	if persisted.Instance == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memcached instance not found")
		return
	}
	_ = json.NewEncoder(w).Encode(persisted.Instance)
}

func (api *MemcacheAPI) listMemcacheInstances(w http.ResponseWriter, r *http.Request, route memcacheRoute) {
	if r.URL.Query().Get("filter") != "" || r.URL.Query().Get("orderBy") != "" {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Memcached list filtering and ordering are not implemented")
		return
	}
	pageSize := 1000
	if raw := r.URL.Query().Get("pageSize"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageSize must be a non-negative integer")
			return
		}
		if parsed > 0 {
			pageSize = parsed
		}
	}
	if pageSize > 1000 {
		pageSize = 1000
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/instances/", route.project, route.location)
	allLocationsPrefix := fmt.Sprintf("projects/%s/locations/", route.project)
	api.mu.RLock()
	instances := make([]*MemcacheInstance, 0)
	for name, persisted := range api.instances {
		if persisted.Instance == nil {
			continue
		}
		if strings.HasPrefix(name, prefix) ||
			route.location == "-" && strings.HasPrefix(name, allLocationsPrefix) {
			instances = append(instances, cloneMemcacheInstance(persisted.Instance))
		}
	}
	api.mu.RUnlock()
	page, token, err := pagination.Page(instances, pageSize, r.URL.Query().Get("pageToken"),
		pagination.Scope{
			Service: "memcache.googleapis.com",
			Parent:  route.name,
		}, func(instance *MemcacheInstance) string { return instance.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if page == nil {
		page = make([]*MemcacheInstance, 0)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"instances":     page,
		"nextPageToken": token,
		"unreachable":   []string{},
	})
}

func (api *MemcacheAPI) updateMemcacheInstance(w http.ResponseWriter, r *http.Request, route memcacheRoute) {
	var requested MemcacheInstance
	if err := decodeMemcacheJSON(w, r, &requested); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid Memcached instance JSON")
		return
	}
	if requested.Name != "" && requested.Name != route.name {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "instance name does not match request path")
		return
	}
	mask, status, code, message := parseMemcacheUpdateMask(r.URL.Query().Get("updateMask"))
	if code != 0 {
		writeError(w, code, status, message)
		return
	}
	if _, ok := mask["maintenancePolicy"]; ok {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "maintenance policies are not implemented")
		return
	}
	if _, ok := mask["nodeCount"]; ok {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Memcached node-count updates are not implemented")
		return
	}
	if len(requested.DisplayName) > 80 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "displayName must not exceed 80 characters")
		return
	}
	if !api.mutationMu.TryLock() {
		writeError(w, http.StatusConflict, "ABORTED", "another Memcached mutation is in progress")
		return
	}
	previous := api.getPersisted(route.name)
	if previous.Instance == nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memcached instance not found")
		return
	}
	if previous.Instance.State != "READY" {
		api.mutationMu.Unlock()
		writeError(w, http.StatusConflict, "ABORTED", "Memcached instance has a mutation in progress")
		return
	}
	updated := cloneMemcacheInstance(previous.Instance)
	if _, ok := mask["displayName"]; ok {
		updated.DisplayName = requested.DisplayName
	}
	if _, ok := mask["labels"]; ok {
		updated.Labels = cloneStringMap(requested.Labels)
	}
	updated.State = "UPDATING"
	updated.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)

	op, err := api.opMgr.RegisterScopedDurable(orchestrator.OperationScope{
		ServiceKind: memcacheOperationKind,
		Project:     route.project,
		Location:    route.location,
		Target:      route.name,
	}, "update")
	if err != nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to register Memcached operation")
		return
	}
	candidate := api.metadataSnapshot()
	entry := candidate.Instances[route.name]
	entry.Instance = updated
	candidate.Instances[route.name] = entry
	previousCopy := cloneMemcachePersisted(previous)
	candidate.Operations[op.Name] = memcacheOperationRecord{
		Action: "update", ResourceName: route.name, Previous: &previousCopy,
	}
	if err := api.commitMetadata(candidate); err != nil {
		rollbackErr := api.opMgr.RollbackScopedRegistration(op.Name)
		api.mutationMu.Unlock()
		if rollbackErr != nil {
			api.setInitializationError(errors.Join(err, rollbackErr))
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Memcached update")
		return
	}
	api.mutationMu.Unlock()

	api.operationRunner(op.Name, func() error {
		return api.completeMemcacheUpdate(op.Name, route.name, previous, mask)
	})
	api.writeMemcacheOperation(w, op)
}

func parseMemcacheUpdateMask(raw string) (map[string]struct{}, string, int, string) {
	if strings.TrimSpace(raw) == "" {
		return nil, "INVALID_ARGUMENT", http.StatusBadRequest, "updateMask is required"
	}
	allowed := map[string]struct{}{
		"displayName": {}, "labels": {}, "nodeCount": {}, "maintenancePolicy": {},
	}
	mask := make(map[string]struct{})
	for _, field := range strings.Split(raw, ",") {
		field = strings.TrimSpace(field)
		if _, ok := allowed[field]; !ok || field == "" {
			return nil, "INVALID_ARGUMENT", http.StatusBadRequest, "updateMask contains an unsupported field"
		}
		if _, duplicate := mask[field]; duplicate {
			return nil, "INVALID_ARGUMENT", http.StatusBadRequest, "updateMask contains a duplicate field"
		}
		mask[field] = struct{}{}
	}
	return mask, "", 0, ""
}

func (api *MemcacheAPI) completeMemcacheUpdate(operationName, name string, previous memcachePersistedInstance, mask map[string]struct{}) error {
	api.mutationMu.Lock()
	mutationOwned := true
	releaseMutation := func() {
		if mutationOwned {
			mutationOwned = false
			api.mutationMu.Unlock()
		}
	}
	defer releaseMutation()

	current := api.getPersisted(name)
	if current.Instance == nil {
		return api.finalizeMemcacheFailureWithBarrier(
			operationName, "Memcached update metadata disappeared", false, releaseMutation,
		)
	}
	var result MemcacheBackendResult
	var updateErr error
	if _, changesNodes := mask["nodeCount"]; changesNodes {
		ctx, cancel := context.WithTimeout(context.Background(), memcacheBackendTimeout)
		result, updateErr = updateMemcacheBackend(ctx, api.backend, memcacheSpec(current.BackendID, current.Instance))
		cancel()
		if updateErr == nil && (!result.Owned || !result.Exists) {
			updateErr = errors.New("backend ownership could not be verified")
		}
	}
	if updateErr != nil {
		candidate := api.snapshot()
		candidate[name] = cloneMemcachePersisted(previous)
		persistErr := api.commit(candidate)
		finalizeErr := api.finalizeMemcacheFailureWithBarrier(
			operationName, "Memcached backend update failed", persistErr == nil, releaseMutation,
		)
		return errors.Join(updateErr, persistErr, finalizeErr)
	}
	candidate := api.snapshot()
	ready := candidate[name]
	ready.Instance.State = "READY"
	ready.Instance.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	if _, changesNodes := mask["nodeCount"]; changesNodes {
		nodes, err := memcacheNodes(result, ready.Instance)
		if err != nil {
			candidate[name] = cloneMemcachePersisted(previous)
			persistErr := api.commit(candidate)
			finalizeErr := api.finalizeMemcacheFailureWithBarrier(operationName,
				"Memcached backend returned invalid endpoints", persistErr == nil, releaseMutation)
			return errors.Join(err, persistErr, finalizeErr)
		}
		ready.Instance.MemcacheNodes = nodes
		ready.Instance.DiscoveryEndpoint = result.Endpoints[0]
	}
	candidate[name] = ready
	if err := api.commit(candidate); err != nil {
		finalizeErr := api.finalizeMemcacheFailureWithBarrier(operationName,
			"Memcached state persistence failed after backend update", false, releaseMutation)
		return errors.Join(err, finalizeErr)
	}
	if err := api.finalizeMemcacheInstanceSuccessWithBarrier(
		operationName, ready.Instance, releaseMutation,
	); err != nil {
		api.setInitializationError(err)
		return err
	}
	return nil
}

func (api *MemcacheAPI) deleteMemcacheInstance(w http.ResponseWriter, _ *http.Request, route memcacheRoute) {
	if !api.mutationMu.TryLock() {
		writeError(w, http.StatusConflict, "ABORTED", "another Memcached mutation is in progress")
		return
	}
	previous := api.getPersisted(route.name)
	if previous.Instance == nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memcached instance not found")
		return
	}
	if previous.Instance.State != "READY" {
		api.mutationMu.Unlock()
		writeError(w, http.StatusConflict, "ABORTED", "Memcached instance has a mutation in progress")
		return
	}
	if api.backend == nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Memcached backend is unavailable")
		return
	}
	op, err := api.opMgr.RegisterScopedDurable(orchestrator.OperationScope{
		ServiceKind: memcacheOperationKind,
		Project:     route.project,
		Location:    route.location,
		Target:      route.name,
	}, "delete")
	if err != nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to register Memcached operation")
		return
	}
	candidate := api.metadataSnapshot()
	deleting := candidate.Instances[route.name]
	deleting.Instance.State = "DELETING"
	deleting.Instance.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	candidate.Instances[route.name] = deleting
	previousCopy := cloneMemcachePersisted(previous)
	candidate.Operations[op.Name] = memcacheOperationRecord{
		Action: "delete", ResourceName: route.name, Previous: &previousCopy,
	}
	if err := api.commitMetadata(candidate); err != nil {
		rollbackErr := api.opMgr.RollbackScopedRegistration(op.Name)
		api.mutationMu.Unlock()
		if rollbackErr != nil {
			api.setInitializationError(errors.Join(err, rollbackErr))
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Memcached deletion")
		return
	}
	api.mutationMu.Unlock()

	api.operationRunner(op.Name, func() error {
		return api.completeMemcacheDelete(op.Name, route.name, previous)
	})
	api.writeMemcacheOperation(w, op)
}

func (api *MemcacheAPI) completeMemcacheDelete(operationName, name string, previous memcachePersistedInstance) error {
	api.mutationMu.Lock()
	mutationOwned := true
	releaseMutation := func() {
		if mutationOwned {
			mutationOwned = false
			api.mutationMu.Unlock()
		}
	}
	defer releaseMutation()

	if err := api.deleteMemcacheBackend(previous.BackendID); err != nil {
		finalizeErr := api.finalizeMemcacheFailureWithBarrier(
			operationName, "Memcached backend deletion failed", false, releaseMutation,
		)
		return errors.Join(err, finalizeErr)
	}
	candidate := api.snapshot()
	delete(candidate, name)
	if err := api.commit(candidate); err != nil {
		finalizeErr := api.finalizeMemcacheFailureWithBarrier(operationName,
			"Memcached state persistence failed after backend deletion", false, releaseMutation)
		return errors.Join(err, finalizeErr)
	}
	if err := api.finalizeMemcacheEmptySuccessWithBarrier(operationName, releaseMutation); err != nil {
		api.setInitializationError(err)
		return err
	}
	return nil
}

func (api *MemcacheAPI) finalizeMemcacheInstanceSuccessWithBarrier(
	operationName string,
	instance *MemcacheInstance,
	releaseMutation func(),
) error {
	response, err := typedMemcacheInstance(instance)
	if err != nil {
		return api.finalizeMemcacheFailureWithBarrier(
			operationName, "encode Memcached operation response", false, releaseMutation,
		)
	}
	return api.finalizeMemcacheWithBarrier(operationName, response, 0, "", true, releaseMutation)
}

func (api *MemcacheAPI) finalizeMemcacheEmptySuccessWithBarrier(
	operationName string,
	releaseMutation func(),
) error {
	response := json.RawMessage(`{"@type":"type.googleapis.com/google.protobuf.Empty"}`)
	return api.finalizeMemcacheWithBarrier(operationName, response, 0, "", true, releaseMutation)
}

func (api *MemcacheAPI) finalizeMemcacheFailureWithBarrier(
	operationName string,
	message string,
	stateCommitted bool,
	releaseMutation func(),
) error {
	return api.finalizeMemcacheWithBarrier(
		operationName, nil, 13, message, stateCommitted, releaseMutation,
	)
}

func (api *MemcacheAPI) finalizeMemcacheWithBarrier(
	operationName string,
	response json.RawMessage,
	code int,
	message string,
	clearAssociation bool,
	releaseMutation func(),
) error {
	err := api.opMgr.FinalizeScopedDurableWithBarrier(
		operationName,
		response,
		code,
		message,
		func() error {
			defer releaseMutation()
			if !clearAssociation {
				return nil
			}
			return api.clearMemcacheOperation(operationName)
		},
	)
	if errors.Is(err, orchestrator.ErrOperationTerminalBarrier) {
		api.setInitializationError(fmt.Errorf(
			"finalize Memcached operation %q: %w", operationName, err,
		))
	}
	return err
}

func (api *MemcacheAPI) finalizeMemcacheInstanceSuccess(operationName string, instance *MemcacheInstance) error {
	response, err := typedMemcacheInstance(instance)
	if err != nil {
		return api.finalizeMemcacheFailure(operationName, "encode Memcached operation response", false)
	}
	if err := api.opMgr.FinalizeScopedDurable(operationName, response, 0, ""); err != nil {
		return err
	}
	return api.clearMemcacheOperation(operationName)
}

func (api *MemcacheAPI) finalizeMemcacheEmptySuccess(operationName string) error {
	response := json.RawMessage(`{"@type":"type.googleapis.com/google.protobuf.Empty"}`)
	if err := api.opMgr.FinalizeScopedDurable(operationName, response, 0, ""); err != nil {
		return err
	}
	return api.clearMemcacheOperation(operationName)
}

func (api *MemcacheAPI) finalizeMemcacheFailure(operationName, message string, stateCommitted bool) error {
	if err := api.opMgr.FinalizeScopedDurable(operationName, nil, 13, message); err != nil {
		return err
	}
	if !stateCommitted {
		return nil
	}
	return api.clearMemcacheOperation(operationName)
}

func (api *MemcacheAPI) clearMemcacheOperation(operationName string) error {
	candidate := api.metadataSnapshot()
	delete(candidate.Operations, operationName)
	return api.commitMetadata(candidate)
}

func typedMemcacheInstance(instance *MemcacheInstance) (json.RawMessage, error) {
	raw, err := json.Marshal(instance)
	if err != nil {
		return nil, err
	}
	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	response["@type"] = "type.googleapis.com/google.cloud.memcache.v1.Instance"
	return json.Marshal(response)
}

func (api *MemcacheAPI) getMemcacheOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, memcacheOperationKind)
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memcached operation not found")
		return
	}
	api.writeMemcacheOperation(w, op)
}

func (api *MemcacheAPI) reconcileOrphanMemcacheOperations() error {
	for _, operation := range api.opMgr.List() {
		if operation.ServiceKind != memcacheOperationKind {
			continue
		}
		if _, associated := api.operations[operation.Name]; associated {
			continue
		}
		interrupted := operation.Error != nil &&
			strings.Contains(operation.Error.Message, "operation interrupted by MiniSky restart")
		if operation.Done && !interrupted {
			continue
		}
		if err := api.opMgr.FinalizeScopedDurable(
			operation.Name,
			nil,
			13,
			"Memcached admission metadata was not durably committed",
		); err != nil {
			return err
		}
	}
	return nil
}

func (api *MemcacheAPI) writeMemcacheOperation(w http.ResponseWriter, operation *orchestrator.Operation) {
	response := map[string]any{
		"name": operation.Name,
		"done": operation.Done,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.memcache.v1.OperationMetadata",
			"apiVersion": "v1",
			"createTime": operation.InsertTime,
			"target":     operation.TargetLink,
			"verb":       operation.OperationType,
		},
	}
	if operation.Done && operation.Error != nil {
		response["error"] = operation.Error
	}
	if operation.Done && operation.Error == nil && len(operation.Response) != 0 {
		var value any
		if json.Unmarshal(operation.Response, &value) == nil {
			response["response"] = value
		}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (api *MemcacheAPI) reconcile() error {
	if len(api.instances) == 0 && len(api.operations) == 0 {
		return nil
	}
	if len(api.operations) != 0 && api.backend == nil {
		return errors.New("reconcile admitted Memcached operations: backend is unavailable")
	}
	operationNames := make([]string, 0, len(api.operations))
	for name := range api.operations {
		operationNames = append(operationNames, name)
	}
	sort.Strings(operationNames)
	reconciledResources := make(map[string]struct{}, len(operationNames))
	for _, operationName := range operationNames {
		record := api.metadataSnapshot().Operations[operationName]
		if err := api.reconcileMemcacheOperation(operationName, record); err != nil {
			return fmt.Errorf("reconcile Memcached operation %q: %w", operationName, err)
		}
		reconciledResources[record.ResourceName] = struct{}{}
	}

	candidate := api.snapshot()
	for name, persisted := range candidate {
		if _, reconciled := reconciledResources[name]; reconciled {
			continue
		}
		if persisted.Instance == nil {
			delete(candidate, name)
			continue
		}
		if api.backend == nil {
			if persisted.Instance.State != "DELETING" {
				persisted.Instance.State = "STATE_UNSPECIFIED"
				persisted.Instance.MemcacheNodes = nil
				persisted.Instance.DiscoveryEndpoint = ""
				candidate[name] = persisted
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), memcacheBackendTimeout)
		result, err := reconcileMemcacheBackend(ctx, api.backend, memcacheSpec(persisted.BackendID, persisted.Instance))
		cancel()
		if err != nil || !result.Owned {
			persisted.Instance.State = "STATE_UNSPECIFIED"
			persisted.Instance.MemcacheNodes = nil
			persisted.Instance.DiscoveryEndpoint = ""
			candidate[name] = persisted
			continue
		}
		if !result.Exists {
			if persisted.Instance.State == "CREATING" || persisted.Instance.State == "DELETING" {
				delete(candidate, name)
			} else {
				persisted.Instance.State = "STATE_UNSPECIFIED"
				persisted.Instance.MemcacheNodes = nil
				persisted.Instance.DiscoveryEndpoint = ""
				candidate[name] = persisted
			}
			continue
		}
		nodes, nodeErr := memcacheNodes(result, persisted.Instance)
		if nodeErr != nil {
			persisted.Instance.State = "STATE_UNSPECIFIED"
			persisted.Instance.MemcacheNodes = nil
			persisted.Instance.DiscoveryEndpoint = ""
			candidate[name] = persisted
			continue
		}
		persisted.Instance.State = "READY"
		persisted.Instance.MemcacheNodes = nodes
		persisted.Instance.DiscoveryEndpoint = result.Endpoints[0]
		candidate[name] = persisted
	}
	if reflect.DeepEqual(candidate, api.snapshot()) {
		return nil
	}
	if err := api.commit(candidate); err != nil {
		return fmt.Errorf("persist Memcached reconciliation: %w", err)
	}
	return nil
}

func (api *MemcacheAPI) reconcileMemcacheOperation(operationName string, record memcacheOperationRecord) error {
	current := api.getPersisted(record.ResourceName)
	expected := current
	if expected.Instance == nil && record.Previous != nil {
		expected = cloneMemcachePersisted(*record.Previous)
	}
	if expected.Instance == nil {
		return errors.New("admitted Memcached operation has no persisted backend specification")
	}
	ctx, cancel := context.WithTimeout(context.Background(), memcacheBackendTimeout)
	result, reconcileErr := reconcileMemcacheBackend(
		ctx, api.backend, memcacheSpec(expected.BackendID, expected.Instance),
	)
	cancel()

	switch record.Action {
	case "create":
		if reconcileErr == nil && result.Exists && result.Owned {
			nodes, err := memcacheNodes(result, current.Instance)
			if err != nil {
				reconcileErr = err
			} else {
				candidate := api.snapshot()
				ready := candidate[record.ResourceName]
				ready.Instance.State = "READY"
				ready.Instance.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
				ready.Instance.MemcacheNodes = nodes
				ready.Instance.DiscoveryEndpoint = result.Endpoints[0]
				ready.Instance.MemcacheFullVersion = memcacheFullVersion(ready.Instance.MemcacheVersion)
				candidate[record.ResourceName] = ready
				if err := api.commit(candidate); err != nil {
					return err
				}
				return api.finalizeMemcacheInstanceSuccess(operationName, ready.Instance)
			}
		}
		if reconcileErr == nil && !result.Exists {
			candidate := api.snapshot()
			delete(candidate, record.ResourceName)
			if err := api.commit(candidate); err != nil {
				return err
			}
			return api.finalizeMemcacheFailure(operationName,
				"Memcached creation was interrupted before backend provisioning completed", true)
		}
		// Inspection, ownership, and version errors are not proof of absence.
		// Preserve both the admitted resource and operation association so a
		// later restart can resolve the same operation without replaying create.
		return nil

	case "update":
		if reconcileErr == nil && result.Exists && result.Owned {
			nodes, err := memcacheNodes(result, current.Instance)
			if err != nil {
				reconcileErr = err
			} else {
				candidate := api.snapshot()
				ready := candidate[record.ResourceName]
				ready.Instance.State = "READY"
				ready.Instance.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
				ready.Instance.MemcacheNodes = nodes
				ready.Instance.DiscoveryEndpoint = result.Endpoints[0]
				candidate[record.ResourceName] = ready
				if err := api.commit(candidate); err != nil {
					return err
				}
				return api.finalizeMemcacheInstanceSuccess(operationName, ready.Instance)
			}
		}
		if reconcileErr == nil && !result.Exists {
			candidate := api.snapshot()
			delete(candidate, record.ResourceName)
			if err := api.commit(candidate); err != nil {
				return err
			}
			return api.finalizeMemcacheFailure(operationName,
				"Memcached backend disappeared while update was interrupted", true)
		}
		// Absence, foreign ownership, version mismatch, and inspection failure
		// other than confirmed absence cannot prove the requested update was
		// safely applied or rolled back. Retain UPDATING plus its association.
		return nil

	case "delete":
		if reconcileErr == nil && !result.Exists {
			candidate := api.snapshot()
			delete(candidate, record.ResourceName)
			if err := api.commit(candidate); err != nil {
				return err
			}
			return api.finalizeMemcacheEmptySuccess(operationName)
		}
		if reconcileErr == nil && result.Exists && result.Owned {
			if err := api.deleteMemcacheBackend(expected.BackendID); err != nil {
				// Exact ownership made retry safe, but failed deletion remains
				// associated for another restart or authoritative observation.
				return nil
			}
			candidate := api.snapshot()
			delete(candidate, record.ResourceName)
			if err := api.commit(candidate); err != nil {
				return err
			}
			return api.finalizeMemcacheEmptySuccess(operationName)
		}
		// Foreign, wrong-version, and inspection-error results are not proof
		// that deletion completed. Preserve DELETING and the same operation.
		return nil
	}
	return fmt.Errorf("unsupported admitted action %q", record.Action)
}

func (api *MemcacheAPI) deleteMemcacheBackend(backendID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), memcacheBackendTimeout)
	defer cancel()
	return api.backend.DeleteMemcache(ctx, backendID)
}

func (api *MemcacheAPI) getPersisted(name string) memcachePersistedInstance {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return cloneMemcachePersisted(api.instances[name])
}

func (api *MemcacheAPI) snapshot() map[string]memcachePersistedInstance {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return cloneMemcacheInstances(api.instances)
}

func (api *MemcacheAPI) metadataSnapshot() memcacheMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return memcacheMetadata{
		Instances:  cloneMemcacheInstances(api.instances),
		Operations: cloneMemcacheOperations(api.operations),
	}
}

func (api *MemcacheAPI) commit(candidate map[string]memcachePersistedInstance) error {
	metadata := api.metadataSnapshot()
	metadata.Instances = candidate
	return api.commitMetadata(metadata)
}

func (api *MemcacheAPI) commitMetadata(candidate memcacheMetadata) error {
	candidate = normalizeMemcacheMetadata(candidate)
	if api.store != nil {
		if err := api.store.Save(memcacheStateEntry, candidate); err != nil {
			if guarded, ok := api.store.(degradedStateStore); ok && guarded.Degraded() != nil {
				wrapped := fmt.Errorf("save Memcached metadata: %w", err)
				api.opMgr.MarkPersistenceFailure(wrapped)
				api.setInitializationError(wrapped)
				return wrapped
			}
			var readback memcacheMetadata
			loadErr := api.store.Load(memcacheStateEntry, &readback)
			if loadErr != nil || !reflect.DeepEqual(normalizeMemcacheMetadata(readback), candidate) {
				if loadErr != nil {
					return fmt.Errorf("save Memcached metadata: %w; read back: %v", err, loadErr)
				}
				return fmt.Errorf("save Memcached metadata: %w", err)
			}
		}
	}
	api.mu.Lock()
	api.instances = candidate.Instances
	api.operations = candidate.Operations
	api.mu.Unlock()
	return nil
}

func decodeMemcacheJSON(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxMemcacheRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func memcacheSpec(backendID string, instance *MemcacheInstance) MemcacheBackendSpec {
	spec := MemcacheBackendSpec{
		BackendID: backendID,
		NodeCount: instance.NodeCount,
		CPUCount:  instance.NodeConfig.CPUCount,
		MemoryMB:  instance.NodeConfig.MemorySizeMB,
		Version:   instance.MemcacheVersion,
	}
	if instance.Parameters != nil {
		spec.Params = cloneStringMap(instance.Parameters.Params)
	}
	return spec
}

func provisionMemcacheBackend(
	ctx context.Context,
	backend MemcacheBackend,
	spec MemcacheBackendSpec,
) (MemcacheBackendResult, error) {
	endpoints, owned, exists, err := backend.ProvisionMemcache(
		ctx, spec.BackendID, spec.NodeCount, spec.CPUCount, spec.MemoryMB, spec.Version, spec.Params,
	)
	return MemcacheBackendResult{Endpoints: endpoints, Owned: owned, Exists: exists}, err
}

func updateMemcacheBackend(
	ctx context.Context,
	backend MemcacheBackend,
	spec MemcacheBackendSpec,
) (MemcacheBackendResult, error) {
	endpoints, owned, exists, err := backend.UpdateMemcache(
		ctx, spec.BackendID, spec.NodeCount, spec.CPUCount, spec.MemoryMB, spec.Version, spec.Params,
	)
	return MemcacheBackendResult{Endpoints: endpoints, Owned: owned, Exists: exists}, err
}

func reconcileMemcacheBackend(
	ctx context.Context,
	backend MemcacheBackend,
	spec MemcacheBackendSpec,
) (MemcacheBackendResult, error) {
	endpoints, owned, exists, err := backend.ReconcileMemcache(
		ctx, spec.BackendID, spec.NodeCount, spec.CPUCount, spec.MemoryMB, spec.Version, spec.Params,
	)
	return MemcacheBackendResult{Endpoints: endpoints, Owned: owned, Exists: exists}, err
}

func memcacheNodes(result MemcacheBackendResult, instance *MemcacheInstance) ([]MemcacheNode, error) {
	if instance == nil || len(result.Endpoints) != instance.NodeCount {
		return nil, errors.New("Memcached backend endpoint count does not match nodeCount")
	}
	nodes := make([]MemcacheNode, len(result.Endpoints))
	for index, endpoint := range result.Endpoints {
		host, port, err := parseMemcacheEndpoint(endpoint)
		if err != nil {
			return nil, err
		}
		zone := instance.Zones[index%len(instance.Zones)]
		nodes[index] = MemcacheNode{
			NodeID:              fmt.Sprintf("node-%d", index+1),
			Zone:                zone,
			State:               "READY",
			Host:                host,
			Port:                port,
			Parameters:          cloneMemcacheParameters(instance.Parameters),
			MemcacheVersion:     instance.MemcacheVersion,
			MemcacheFullVersion: memcacheFullVersion(instance.MemcacheVersion),
		}
	}
	return nodes, nil
}

func parseMemcacheEndpoint(endpoint string) (string, int, error) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", 0, fmt.Errorf("invalid Memcached endpoint: %w", err)
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", 0, errors.New("Memcached endpoint is not loopback")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, errors.New("invalid Memcached endpoint port")
	}
	return host, port, nil
}

func memcacheFullVersion(version string) string {
	if version == "MEMCACHE_1_6_15" {
		return "memcached-1.6.15"
	}
	return "memcached-1.5.16"
}

func supportedMemcacheNetwork(project, network string) bool {
	return network == "" || network == "default" ||
		network == fmt.Sprintf("projects/%s/global/networks/default", project)
}

func canonicalMemcacheNetwork(project, network string) string {
	if network == "" || network == "default" {
		return fmt.Sprintf("projects/%s/global/networks/default", project)
	}
	return network
}

func memcacheBackendID(project, location, id string) string {
	hasher := sha256.New()
	for _, part := range []string{project, location, id} {
		_ = binary.Write(hasher, binary.BigEndian, uint32(len(part)))
		_, _ = hasher.Write([]byte(part))
	}
	return fmt.Sprintf("memcache-%x", hasher.Sum(nil)[:16])
}

func normalizeMemcacheInstances(input map[string]memcachePersistedInstance) map[string]memcachePersistedInstance {
	if input == nil {
		return make(map[string]memcachePersistedInstance)
	}
	return cloneMemcacheInstances(input)
}

func normalizeMemcacheMetadata(input memcacheMetadata) memcacheMetadata {
	return memcacheMetadata{
		Instances:  normalizeMemcacheInstances(input.Instances),
		Operations: cloneMemcacheOperations(input.Operations),
	}
}

func cloneMemcacheOperations(input map[string]memcacheOperationRecord) map[string]memcacheOperationRecord {
	cloned := make(map[string]memcacheOperationRecord, len(input))
	for name, operation := range input {
		record := operation
		if operation.Previous != nil {
			previous := cloneMemcachePersisted(*operation.Previous)
			record.Previous = &previous
		}
		cloned[name] = record
	}
	return cloned
}

func restoreMemcacheInstances(input map[string]memcachePersistedInstance) (map[string]memcachePersistedInstance, error) {
	restored := make(map[string]memcachePersistedInstance, len(input))
	backendOwners := make(map[string]string, len(input))
	for name, persisted := range input {
		if persisted.Instance == nil {
			continue
		}
		validated, err := validateMemcachePersisted(name, persisted)
		if err != nil {
			return nil, err
		}
		if owner := backendOwners[validated.BackendID]; owner != "" && owner != name {
			return nil, fmt.Errorf("persisted instances %q and %q alias backend %q", owner, name, validated.BackendID)
		}
		backendOwners[validated.BackendID] = name
		restored[name] = validated
	}
	return restored, nil
}

func validateMemcachePersisted(name string, persisted memcachePersistedInstance) (memcachePersistedInstance, error) {
	route, ok := parseMemcacheRoute("/v1/" + name)
	if !ok || route.kind != memcacheRouteInstance || persisted.Instance == nil ||
		persisted.Instance.Name != name || !validMemcacheProject(route.project) ||
		!validMemcacheLocation(route.location) || !validID(route.id) {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted instance %q", name)
	}
	expectedBackendID := memcacheBackendID(route.project, route.location, route.id)
	if persisted.BackendID != expectedBackendID {
		return memcachePersistedInstance{}, fmt.Errorf("persisted instance %q has non-canonical backend identity", name)
	}
	instance := cloneMemcacheInstance(persisted.Instance)
	if instance.NodeCount != 1 || instance.NodeConfig == nil ||
		instance.NodeConfig.CPUCount < 1 || instance.NodeConfig.MemorySizeMB < 1 ||
		instance.Parameters != nil {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted node configuration for %q", name)
	}
	if instance.MemcacheVersion == "" {
		instance.MemcacheVersion = "MEMCACHE_1_5"
	}
	if instance.MemcacheVersion != "MEMCACHE_1_5" && instance.MemcacheVersion != "MEMCACHE_1_6_15" {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted version for %q", name)
	}
	if len(instance.Zones) != 1 || !strings.HasPrefix(instance.Zones[0], route.location+"-") {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted zones for %q", name)
	}
	if instance.AuthorizedNetwork != canonicalMemcacheNetwork(route.project, "") {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted network for %q", name)
	}
	switch instance.State {
	case "CREATING", "READY", "UPDATING", "DELETING", "STATE_UNSPECIFIED":
	default:
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted state for %q", name)
	}
	if instance.CreateTime == "" || instance.UpdateTime == "" {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted timestamps for %q", name)
	}
	if _, err := time.Parse(time.RFC3339Nano, instance.CreateTime); err != nil {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted createTime for %q", name)
	}
	if _, err := time.Parse(time.RFC3339Nano, instance.UpdateTime); err != nil {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted updateTime for %q", name)
	}
	if len(instance.MemcacheNodes) > 1 {
		return memcachePersistedInstance{}, fmt.Errorf("invalid persisted nodes for %q", name)
	}
	if len(instance.MemcacheNodes) == 1 {
		node := instance.MemcacheNodes[0]
		endpoint := net.JoinHostPort(node.Host, strconv.Itoa(node.Port))
		if _, _, err := parseMemcacheEndpoint(endpoint); err != nil ||
			node.NodeID != "node-1" || node.Zone != instance.Zones[0] || node.State != "READY" ||
			node.MemcacheVersion != instance.MemcacheVersion ||
			node.MemcacheFullVersion != memcacheFullVersion(instance.MemcacheVersion) {
			return memcachePersistedInstance{}, fmt.Errorf("invalid persisted node endpoint for %q", name)
		}
		if instance.DiscoveryEndpoint != endpoint {
			return memcachePersistedInstance{}, fmt.Errorf("invalid persisted discovery endpoint for %q", name)
		}
	} else if instance.DiscoveryEndpoint != "" {
		return memcachePersistedInstance{}, fmt.Errorf("persisted discovery endpoint has no node for %q", name)
	}
	if instance.State == "READY" && len(instance.MemcacheNodes) != 1 {
		return memcachePersistedInstance{}, fmt.Errorf("ready persisted instance %q has no node", name)
	}
	return memcachePersistedInstance{Instance: instance, BackendID: persisted.BackendID}, nil
}

func restoreMemcacheOperations(
	input map[string]memcacheOperationRecord,
	instances map[string]memcachePersistedInstance,
	manager *orchestrator.OperationManager,
) (map[string]memcacheOperationRecord, error) {
	restored := cloneMemcacheOperations(input)
	resourceOperations := make(map[string]string, len(restored))
	for operationName, record := range restored {
		operationRoute, ok := parseMemcacheRoute("/v1/" + operationName)
		if !ok || operationRoute.kind != memcacheRouteOperations {
			return nil, fmt.Errorf("invalid persisted operation name %q", operationName)
		}
		resourceRoute, ok := parseMemcacheRoute("/v1/" + record.ResourceName)
		if !ok || resourceRoute.kind != memcacheRouteInstance ||
			resourceRoute.project != operationRoute.project ||
			resourceRoute.location != operationRoute.location {
			return nil, fmt.Errorf("operation %q has invalid resource association", operationName)
		}
		if prior := resourceOperations[record.ResourceName]; prior != "" {
			return nil, fmt.Errorf("resource %q has duplicate operation associations", record.ResourceName)
		}
		resourceOperations[record.ResourceName] = operationName
		switch record.Action {
		case "create":
			if record.Previous != nil || instances[record.ResourceName].Instance == nil {
				return nil, fmt.Errorf("create operation %q has invalid association", operationName)
			}
			stateValue := instances[record.ResourceName].Instance.State
			if stateValue != "CREATING" && stateValue != "READY" {
				return nil, fmt.Errorf("create operation %q has invalid resource state %q", operationName, stateValue)
			}
		case "update", "delete":
			if record.Previous == nil {
				return nil, fmt.Errorf("%s operation %q has no previous resource", record.Action, operationName)
			}
			if _, err := validateMemcachePersisted(record.ResourceName, *record.Previous); err != nil {
				return nil, fmt.Errorf("%s operation %q previous resource: %w", record.Action, operationName, err)
			}
			if record.Action == "update" && instances[record.ResourceName].Instance == nil {
				return nil, fmt.Errorf("update operation %q has no current resource", operationName)
			}
			if record.Action == "update" {
				stateValue := instances[record.ResourceName].Instance.State
				if stateValue != "UPDATING" && stateValue != "READY" {
					return nil, fmt.Errorf("update operation %q has invalid resource state %q", operationName, stateValue)
				}
			}
			if record.Action == "delete" && instances[record.ResourceName].Instance != nil {
				stateValue := instances[record.ResourceName].Instance.State
				if stateValue != "DELETING" {
					return nil, fmt.Errorf("delete operation %q has invalid resource state %q", operationName, stateValue)
				}
			}
		default:
			return nil, fmt.Errorf("operation %q has invalid action %q", operationName, record.Action)
		}
		if _, err := manager.GetScoped(operationName, orchestrator.OperationScope{
			ServiceKind: memcacheOperationKind,
			Project:     operationRoute.project,
			Location:    operationRoute.location,
			Target:      record.ResourceName,
		}); err != nil {
			return nil, fmt.Errorf("operation %q is not durably registered", operationName)
		}
	}
	for resourceName, persisted := range instances {
		switch persisted.Instance.State {
		case "CREATING", "UPDATING", "DELETING":
			if resourceOperations[resourceName] == "" {
				return nil, fmt.Errorf("transitional resource %q has no operation association", resourceName)
			}
		}
	}
	return restored, nil
}

func validMemcacheProject(project string) bool {
	if project == "" || project == "." || project == ".." {
		return false
	}
	for index, character := range project {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			character == '-' || character == '.' || character == ':' && index > 0 {
			continue
		}
		return false
	}
	return true
}

func validMemcacheLocation(location string) bool {
	if location == "" || location[0] < 'a' || location[0] > 'z' {
		return false
	}
	for _, character := range location {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func cloneMemcacheInstances(input map[string]memcachePersistedInstance) map[string]memcachePersistedInstance {
	cloned := make(map[string]memcachePersistedInstance, len(input))
	for name, persisted := range input {
		cloned[name] = cloneMemcachePersisted(persisted)
	}
	return cloned
}

func cloneMemcachePersisted(input memcachePersistedInstance) memcachePersistedInstance {
	return memcachePersistedInstance{
		Instance:  cloneMemcacheInstance(input.Instance),
		BackendID: input.BackendID,
	}
}

func cloneMemcacheInstance(input *MemcacheInstance) *MemcacheInstance {
	if input == nil {
		return nil
	}
	raw, _ := json.Marshal(input)
	var cloned MemcacheInstance
	_ = json.Unmarshal(raw, &cloned)
	return &cloned
}

func cloneMemcacheParameters(input *MemcacheParameters) *MemcacheParameters {
	if input == nil {
		return nil
	}
	return &MemcacheParameters{ID: input.ID, Params: cloneStringMap(input.Params)}
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
