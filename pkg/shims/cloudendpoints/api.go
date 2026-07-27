// Package cloudendpoints implements a bounded local Service Management and
// Service Control plane for Cloud Endpoints.
package cloudendpoints

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
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const maxControlRequestBytes = 1 << 20

func init() {
	factory := func(ctx *registry.Context) http.Handler {
		return ctx.SharedHandler("cloudendpoints", func() http.Handler { return NewAPI() })
	}
	registry.Register("servicemanagement.googleapis.com", factory)
	registry.Register("servicecontrol.googleapis.com", factory)
}

type ServiceConfig struct {
	Name  string `json:"name"`
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
}

type TrafficPercentStrategy struct {
	Percentages map[string]float64 `json:"percentages"`
}

type Rollout struct {
	RolloutID              string                  `json:"rolloutId"`
	ServiceName            string                  `json:"serviceName"`
	Status                 string                  `json:"status,omitempty"`
	CreateTime             string                  `json:"createTime,omitempty"`
	TrafficPercentStrategy *TrafficPercentStrategy `json:"trafficPercentStrategy,omitempty"`
}

type Operation struct {
	Name     string         `json:"name"`
	Done     bool           `json:"done"`
	Response *ServiceConfig `json:"response,omitempty"`
	Rollout  *Rollout       `json:"rollout,omitempty"`
}

type ControlOperation struct {
	OperationID   string `json:"operationId"`
	OperationName string `json:"operationName,omitempty"`
	ConsumerID    string `json:"consumerId,omitempty"`
	CheckedAt     string `json:"checkedAt,omitempty"`
	ReportedAt    string `json:"reportedAt,omitempty"`
}

type CheckResponse struct {
	CheckErrors      []any  `json:"checkErrors"`
	ServiceConfigID  string `json:"serviceConfigId"`
	ServiceRolloutID string `json:"serviceRolloutId"`
}

type ReportResponse struct {
	ReportErrors     []any  `json:"reportErrors"`
	ServiceConfigID  string `json:"serviceConfigId"`
	ServiceRolloutID string `json:"serviceRolloutId"`
}

type endpointsStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	stateStore endpointsStateStore
	configs    map[string]*ServiceConfig
	rollouts   map[string]*Rollout
	operations map[string]*ControlOperation
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newTestAPI()
	api.stateStore = state.NewGuardedEntryStore(store, err)
	if err != nil {
		log.Printf("[Shim: Cloud Endpoints] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Cloud Endpoints] state rehydration failed: %v", err)
	}
	return api
}

func newTestAPI() *API {
	return &API{
		configs:    make(map[string]*ServiceConfig),
		rollouts:   make(map[string]*Rollout),
		operations: make(map[string]*ControlOperation),
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.Body = http.MaxBytesReader(w, r.Body, maxControlRequestBytes)
	path := r.URL.Path

	switch {
	case strings.HasSuffix(path, ":allocateQuota"):
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "quota allocation is not supported")
	case strings.HasSuffix(path, ":check") && r.Method == http.MethodPost:
		api.check(w, r)
	case strings.HasSuffix(path, ":report") && r.Method == http.MethodPost:
		api.report(w, r)
	case strings.Contains(path, "/configs"):
		api.routeConfigs(w, r)
	case strings.Contains(path, "/rollouts"):
		api.routeRollouts(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Cloud Endpoints resource not found")
	}
}

func (api *API) routeConfigs(w http.ResponseWriter, r *http.Request) {
	service := serviceName(r.URL.Path)
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/configs"):
		var resource ServiceConfig
		if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid service config JSON")
			return
		}
		if resource.Name != service || resource.ID == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "config name must match the service and id is required")
			return
		}
		key := service + ":" + resource.ID
		api.persistMu.Lock()
		api.mu.Lock()
		if _, exists := api.configs[key]; exists {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "service config already exists")
			return
		}
		api.configs[key] = &resource
		api.mu.Unlock()
		if err := api.saveSnapshot(); err != nil {
			api.mu.Lock()
			delete(api.configs, key)
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
			return
		}
		api.persistMu.Unlock()
		_ = json.NewEncoder(w).Encode(Operation{
			Name: "operations/config-" + resource.ID, Done: true, Response: copyConfig(&resource),
		})
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/configs"):
		api.listConfigs(w, r, service)
	case r.Method == http.MethodGet:
		id := segmentAfter(r.URL.Path, "configs")
		api.mu.RLock()
		resource := copyConfig(api.configs[service+":"+id])
		api.mu.RUnlock()
		if resource == nil {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "service config not found")
			return
		}
		_ = json.NewEncoder(w).Encode(resource)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) listConfigs(w http.ResponseWriter, r *http.Request, service string) {
	pageSize, ok := parsePageSize(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageSize must be a positive integer")
		return
	}
	api.mu.RLock()
	items := make([]*ServiceConfig, 0)
	for key, resource := range api.configs {
		if strings.HasPrefix(key, service+":") {
			items = append(items, copyConfig(resource))
		}
	}
	api.mu.RUnlock()
	page, token, err := pagination.Page(items, pageSize, r.URL.Query().Get("pageToken"), pagination.Scope{
		Service: "cloudendpoints.configs", Parent: "services/" + service,
	}, func(resource *ServiceConfig) string { return resource.ID })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"serviceConfigs": page, "nextPageToken": token})
}

func (api *API) routeRollouts(w http.ResponseWriter, r *http.Request) {
	service := serviceName(r.URL.Path)
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/rollouts"):
		var rollout Rollout
		if err := json.NewDecoder(r.Body).Decode(&rollout); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid rollout JSON")
			return
		}
		configID, ok := promotedConfig(rollout.TrafficPercentStrategy)
		if rollout.RolloutID == "" || rollout.ServiceName != service || !ok {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "rollout requires one service config at 100 percent")
			return
		}
		key := service + ":" + rollout.RolloutID
		api.persistMu.Lock()
		api.mu.Lock()
		if api.configs[service+":"+configID] == nil {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "rollout references an unknown service config")
			return
		}
		if api.rollouts[key] != nil {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "rollout already exists")
			return
		}
		rollout.Status = "SUCCESS"
		rollout.CreateTime = time.Now().UTC().Format(time.RFC3339Nano)
		api.rollouts[key] = &rollout
		api.mu.Unlock()
		if err := api.saveSnapshot(); err != nil {
			api.mu.Lock()
			delete(api.rollouts, key)
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
			return
		}
		api.persistMu.Unlock()
		_ = json.NewEncoder(w).Encode(Operation{
			Name: "operations/rollout-" + rollout.RolloutID, Done: true, Rollout: copyRollout(&rollout),
		})
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/rollouts"):
		api.listRollouts(w, r, service)
	case r.Method == http.MethodGet:
		id := segmentAfter(r.URL.Path, "rollouts")
		api.mu.RLock()
		resource := copyRollout(api.rollouts[service+":"+id])
		api.mu.RUnlock()
		if resource == nil {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "rollout not found")
			return
		}
		_ = json.NewEncoder(w).Encode(resource)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) listRollouts(w http.ResponseWriter, r *http.Request, service string) {
	pageSize, ok := parsePageSize(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageSize must be a positive integer")
		return
	}
	api.mu.RLock()
	items := make([]*Rollout, 0)
	for key, resource := range api.rollouts {
		if strings.HasPrefix(key, service+":") {
			items = append(items, copyRollout(resource))
		}
	}
	api.mu.RUnlock()
	page, token, err := pagination.Page(items, pageSize, r.URL.Query().Get("pageToken"), pagination.Scope{
		Service: "cloudendpoints.rollouts", Parent: "services/" + service,
	}, func(resource *Rollout) string { return resource.RolloutID })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"rollouts": page, "nextPageToken": token})
}

func (api *API) check(w http.ResponseWriter, r *http.Request) {
	service := strings.TrimSuffix(serviceName(r.URL.Path), ":check")
	var request struct {
		ServiceName string            `json:"serviceName"`
		Operation   *ControlOperation `json:"operation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Operation == nil ||
		request.ServiceName != service || request.Operation.OperationID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "serviceName and operation.operationId are required")
		return
	}
	rollout, configID := api.activeRollout(service)
	if rollout == nil {
		writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "service has no active rollout")
		return
	}
	api.persistMu.Lock()
	api.mu.Lock()
	key := service + ":" + request.Operation.OperationID
	before := api.operations[key]
	if before != nil &&
		(before.OperationName != request.Operation.OperationName || before.ConsumerID != request.Operation.ConsumerID) {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "operationId is already correlated to a different operation")
		return
	}
	request.Operation.CheckedAt = time.Now().UTC().Format(time.RFC3339Nano)
	api.operations[key] = request.Operation
	api.mu.Unlock()
	if err := api.saveSnapshot(); err != nil {
		api.mu.Lock()
		if before == nil {
			delete(api.operations, key)
		} else {
			api.operations[key] = before
		}
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}
	api.persistMu.Unlock()
	_ = json.NewEncoder(w).Encode(CheckResponse{CheckErrors: []any{}, ServiceConfigID: configID, ServiceRolloutID: rollout.RolloutID})
}

func (api *API) report(w http.ResponseWriter, r *http.Request) {
	service := strings.TrimSuffix(serviceName(r.URL.Path), ":report")
	var request struct {
		ServiceName string             `json:"serviceName"`
		Operations  []ControlOperation `json:"operations"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.ServiceName != service || len(request.Operations) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "serviceName and operations are required")
		return
	}
	rollout, configID := api.activeRollout(service)
	if rollout == nil {
		writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "service has no active rollout")
		return
	}
	api.persistMu.Lock()
	api.mu.Lock()
	for _, operation := range request.Operations {
		saved := api.operations[service+":"+operation.OperationID]
		if operation.OperationID == "" || saved == nil ||
			saved.OperationName != operation.OperationName || saved.ConsumerID != operation.ConsumerID {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "reported operation was not checked")
			return
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	reportedBefore := make(map[string]string, len(request.Operations))
	for _, operation := range request.Operations {
		key := service + ":" + operation.OperationID
		reportedBefore[key] = api.operations[key].ReportedAt
		api.operations[key].ReportedAt = now
	}
	api.mu.Unlock()
	if err := api.saveSnapshot(); err != nil {
		api.mu.Lock()
		for key, reportedAt := range reportedBefore {
			api.operations[key].ReportedAt = reportedAt
		}
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}
	api.persistMu.Unlock()
	_ = json.NewEncoder(w).Encode(ReportResponse{ReportErrors: []any{}, ServiceConfigID: configID, ServiceRolloutID: rollout.RolloutID})
}

func (api *API) activeRollout(service string) (*Rollout, string) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	var selected *Rollout
	for key, rollout := range api.rollouts {
		if strings.HasPrefix(key, service+":") && rollout.Status == "SUCCESS" &&
			(selected == nil || rollout.CreateTime > selected.CreateTime) {
			selected = rollout
		}
	}
	if selected == nil {
		return nil, ""
	}
	configID, _ := promotedConfig(selected.TrafficPercentStrategy)
	return copyRollout(selected), configID
}

func promotedConfig(strategy *TrafficPercentStrategy) (string, bool) {
	if strategy == nil || len(strategy.Percentages) != 1 {
		return "", false
	}
	for id, percent := range strategy.Percentages {
		return id, id != "" && percent == 100
	}
	return "", false
}

func serviceName(path string) string {
	return segmentAfter(path, "services")
}

func segmentAfter(path, segment string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for index, part := range parts {
		if part == segment && index+1 < len(parts) {
			return parts[index+1]
		}
	}
	return ""
}

func parsePageSize(r *http.Request) (int, bool) {
	size := 100
	if raw := r.URL.Query().Get("pageSize"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return 0, false
		}
		size = value
	}
	if size > 500 {
		size = 500
	}
	return size, true
}

func copyConfig(resource *ServiceConfig) *ServiceConfig {
	if resource == nil {
		return nil
	}
	clone := *resource
	return &clone
}

func copyRollout(resource *Rollout) *Rollout {
	if resource == nil {
		return nil
	}
	raw, _ := json.Marshal(resource)
	var clone Rollout
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "status": status, "details": []any{},
	}}); err != nil {
		log.Printf("[Shim: Cloud Endpoints] write error response: %v", fmt.Errorf("%w", err))
	}
}
