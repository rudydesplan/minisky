package servicedirectory

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("servicedirectory.googleapis.com", func(_ *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resources (Service Directory v1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Namespace represents a google.cloud.servicedirectory.v1.Namespace resource.
type Namespace struct {
	Name        string            `json:"name"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	UID         string            `json:"uid,omitempty"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
}

// Service represents a google.cloud.servicedirectory.v1.Service resource.
type Service struct {
	Name        string            `json:"name"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	UID         string            `json:"uid,omitempty"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
	Endpoints   []*Endpoint       `json:"endpoints,omitempty"`
}

// Endpoint represents a google.cloud.servicedirectory.v1.Endpoint resource.
type Endpoint struct {
	Name        string            `json:"name"`
	Address     string            `json:"address,omitempty"`
	Port        int               `json:"port,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	UID         string            `json:"uid,omitempty"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
	Network     string            `json:"network,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Service Directory v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	stateStore sdStateStore
	namespaces map[string]*Namespace
	services   map[string]*Service
	endpoints  map[string]*Endpoint
}

// NewAPI creates a new Service Directory shim with persistence.
func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		stateStore: state.NewGuardedEntryStore(store, err),
		namespaces: make(map[string]*Namespace),
		services:   make(map[string]*Service),
		endpoints:  make(map[string]*Endpoint),
	}
	if err != nil {
		log.Printf("[Shim: Service Directory] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Service Directory] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		namespaces: make(map[string]*Namespace),
		services:   make(map[string]*Service),
		endpoints:  make(map[string]*Endpoint),
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Service Directory] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":resolve"):
		api.resolveService(w, r)
	case strings.Contains(r.URL.Path, "/endpoints"):
		api.routeEndpoints(w, r)
	case strings.Contains(r.URL.Path, "/services"):
		api.routeServices(w, r)
	case strings.Contains(r.URL.Path, "/namespaces"):
		api.routeNamespaces(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Service Directory resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Namespaces
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeNamespaces(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createNamespace(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "namespaces") {
			api.listNamespaces(w, r)
		} else {
			api.getNamespace(w, r)
		}
	case http.MethodPatch:
		api.patchNamespace(w, r)
	case http.MethodDelete:
		api.deleteNamespace(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createNamespace(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	nsID := r.URL.Query().Get("namespaceId")
	if !validResourceID(nsID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "namespaceId must be a canonical path component")
		return
	}

	var resource Namespace
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/namespaces/%s", project, location, nsID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.UID = generateUID()
	resource.CreateTime = now
	resource.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.namespaces[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "namespace already exists: "+nsID)
		return
	}
	api.namespaces[name] = &resource
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.namespaces, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(deepCopyNamespace(&resource))
}

func (api *API) getNamespace(w http.ResponseWriter, r *http.Request) {
	name := parseNamespaceName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.namespaces[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "namespace not found: "+name)
		return
	}
	clone := deepCopyNamespace(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listNamespaces(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/namespaces/", project, location)

	pageSize, pageToken := parsePagination(r)

	api.mu.RLock()
	all := make([]*Namespace, 0)
	for key, ns := range api.namespaces {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyNamespace(ns))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "servicedirectory.namespaces",
		Parent:  fmt.Sprintf("projects/%s/locations/%s", project, location),
	}, func(namespace *Namespace) string { return namespace.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"namespaces":    result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchNamespace(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseNamespaceName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.namespaces[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "namespace not found: "+name)
		return
	}

	var patch Namespace
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	before := deepCopyNamespace(existing)
	if patch.Annotations != nil {
		existing.Annotations = patch.Annotations
	}
	if patch.Labels != nil {
		existing.Labels = patch.Labels
	}
	existing.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	clone := deepCopyNamespace(existing)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.namespaces[name] = before
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) deleteNamespace(w http.ResponseWriter, r *http.Request) {
	name := parseNamespaceName(r.URL.Path)

	api.mu.Lock()
	_, exists := api.namespaces[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "namespace not found: "+name)
		return
	}
	prefix := name + "/services/"
	for k := range api.services {
		if strings.HasPrefix(k, prefix) {
			api.mu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "namespace must be empty before deletion")
			return
		}
	}
	ns := api.namespaces[name]
	delete(api.namespaces, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resources since persist failed
		api.mu.Lock()
		api.namespaces[name] = ns
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Services
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeServices(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createService(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "services") {
			api.listServices(w, r)
		} else {
			api.getService(w, r)
		}
	case http.MethodPatch:
		api.patchService(w, r)
	case http.MethodDelete:
		api.deleteService(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createService(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	svcID := r.URL.Query().Get("serviceId")
	if !validResourceID(svcID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "serviceId must be a canonical path component")
		return
	}

	// Parse parent namespace from path
	nsName := parseServiceParent(r.URL.Path)

	var resource Service
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := nsName + "/services/" + svcID
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.UID = generateUID()
	resource.CreateTime = now
	resource.UpdateTime = now
	resource.Endpoints = nil // endpoints are separate resources

	api.mu.Lock()
	if _, exists := api.namespaces[nsName]; !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent namespace not found: "+nsName)
		return
	}
	if _, exists := api.services[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "service already exists: "+svcID)
		return
	}
	api.services[name] = &resource
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.services, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(deepCopyService(&resource))
}

func (api *API) getService(w http.ResponseWriter, r *http.Request) {
	name := parseServiceName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.services[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "service not found: "+name)
		return
	}
	clone := deepCopyService(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listServices(w http.ResponseWriter, r *http.Request) {
	nsName := parseServiceParent(r.URL.Path)
	prefix := nsName + "/services/"

	pageSize, pageToken := parsePagination(r)

	api.mu.RLock()
	all := make([]*Service, 0)
	for key, svc := range api.services {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyService(svc))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "servicedirectory.services",
		Parent:  nsName,
	}, func(service *Service) string { return service.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"services":      result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchService(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseServiceName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.services[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "service not found: "+name)
		return
	}

	var patch Service
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	before := deepCopyService(existing)
	if patch.Annotations != nil {
		existing.Annotations = patch.Annotations
	}
	if patch.Metadata != nil {
		existing.Metadata = patch.Metadata
	}
	existing.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	clone := deepCopyService(existing)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.services[name] = before
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) deleteService(w http.ResponseWriter, r *http.Request) {
	name := parseServiceName(r.URL.Path)

	api.mu.Lock()
	svc, exists := api.services[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "service not found: "+name)
		return
	}
	epPrefix := name + "/endpoints/"
	for k := range api.endpoints {
		if strings.HasPrefix(k, epPrefix) {
			api.mu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "service must have no endpoints before deletion")
			return
		}
	}
	delete(api.services, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resources since persist failed
		api.mu.Lock()
		api.services[name] = svc
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Endpoints
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeEndpoints(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createEndpoint(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "endpoints") {
			api.listEndpoints(w, r)
		} else {
			api.getEndpoint(w, r)
		}
	case http.MethodPatch:
		api.patchEndpoint(w, r)
	case http.MethodDelete:
		api.deleteEndpoint(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createEndpoint(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	epID := r.URL.Query().Get("endpointId")
	if !validResourceID(epID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "endpointId must be a canonical path component")
		return
	}

	// Parse parent service from path
	svcName := parseEndpointParent(r.URL.Path)

	var resource Endpoint
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if resource.Port < 0 || resource.Port > 65535 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "endpoint port must be between 0 and 65535")
		return
	}

	name := svcName + "/endpoints/" + epID
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.UID = generateUID()
	resource.CreateTime = now
	resource.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.services[svcName]; !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent service not found: "+svcName)
		return
	}
	if _, exists := api.endpoints[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "endpoint already exists: "+epID)
		return
	}
	api.endpoints[name] = &resource
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.endpoints, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(deepCopyEndpoint(&resource))
}

func (api *API) getEndpoint(w http.ResponseWriter, r *http.Request) {
	name := parseEndpointName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.endpoints[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "endpoint not found: "+name)
		return
	}
	clone := deepCopyEndpoint(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listEndpoints(w http.ResponseWriter, r *http.Request) {
	svcName := parseEndpointParent(r.URL.Path)
	prefix := svcName + "/endpoints/"

	pageSize, pageToken := parsePagination(r)

	api.mu.RLock()
	all := make([]*Endpoint, 0)
	for key, ep := range api.endpoints {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyEndpoint(ep))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "servicedirectory.endpoints",
		Parent:  svcName,
	}, func(endpoint *Endpoint) string { return endpoint.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"endpoints":     result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchEndpoint(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseEndpointName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.endpoints[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "endpoint not found: "+name)
		return
	}

	var patch Endpoint
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	before := deepCopyEndpoint(existing)
	if patch.Address != "" {
		existing.Address = patch.Address
	}
	if patch.Port != 0 {
		existing.Port = patch.Port
	}
	if patch.Metadata != nil {
		existing.Metadata = patch.Metadata
	}
	if patch.Annotations != nil {
		existing.Annotations = patch.Annotations
	}
	if patch.Network != "" {
		existing.Network = patch.Network
	}
	existing.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	clone := deepCopyEndpoint(existing)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.endpoints[name] = before
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) deleteEndpoint(w http.ResponseWriter, r *http.Request) {
	name := parseEndpointName(r.URL.Path)

	api.mu.Lock()
	ep, exists := api.endpoints[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "endpoint not found: "+name)
		return
	}
	delete(api.endpoints, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.endpoints[name] = ep
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (api *API) resolveService(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	name := parseServiceName(strings.TrimSuffix(r.URL.Path, ":resolve"))

	api.mu.RLock()
	service, ok := api.services[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "service not found: "+name)
		return
	}
	resolved := deepCopyService(service)
	resolved.Endpoints = make([]*Endpoint, 0)
	prefix := name + "/endpoints/"
	for endpointName, endpoint := range api.endpoints {
		if strings.HasPrefix(endpointName, prefix) {
			resolved.Endpoints = append(resolved.Endpoints, deepCopyEndpoint(endpoint))
		}
	}
	api.mu.RUnlock()
	sort.Slice(resolved.Endpoints, func(i, j int) bool {
		return resolved.Endpoints[i].Name < resolved.Endpoints[j].Name
	})

	_ = json.NewEncoder(w).Encode(map[string]any{"service": resolved})
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

func parseNamespaceName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	nsID := extractAfter(path, "namespaces")
	return fmt.Sprintf("projects/%s/locations/%s/namespaces/%s", project, location, nsID)
}

func parseServiceParent(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	nsID := extractAfter(path, "namespaces")
	return fmt.Sprintf("projects/%s/locations/%s/namespaces/%s", project, location, nsID)
}

func parseServiceName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	nsID := extractAfter(path, "namespaces")
	svcID := extractAfter(path, "services")
	return fmt.Sprintf("projects/%s/locations/%s/namespaces/%s/services/%s", project, location, nsID, svcID)
}

func parseEndpointParent(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	nsID := extractAfter(path, "namespaces")
	svcID := extractAfter(path, "services")
	return fmt.Sprintf("projects/%s/locations/%s/namespaces/%s/services/%s", project, location, nsID, svcID)
}

func parseEndpointName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	nsID := extractAfter(path, "namespaces")
	svcID := extractAfter(path, "services")
	epID := extractAfter(path, "endpoints")
	return fmt.Sprintf("projects/%s/locations/%s/namespaces/%s/services/%s/endpoints/%s", project, location, nsID, svcID, epID)
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

func validResourceID(value string) bool {
	return value != "" && value != "." && value != ".." &&
		!strings.ContainsAny(value, `/\`)
}

func isCollection(path, resource string) bool {
	return strings.HasSuffix(path, "/"+resource)
}

func parsePagination(r *http.Request) (pageSize int, pageToken string) {
	pageSize = 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}
	pageToken = r.URL.Query().Get("pageToken")
	return
}

func generateUID() string {
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
