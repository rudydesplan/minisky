package apigateway

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/pagination"
)

const maxAPIConfigDocumentBytes = 1 << 20

type ApiConfig struct {
	Name             string            `json:"name"`
	DisplayName      string            `json:"displayName,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	State            string            `json:"state,omitempty"`
	CreateTime       string            `json:"createTime,omitempty"`
	UpdateTime       string            `json:"updateTime,omitempty"`
	OpenAPIDocuments []OpenAPIDocument `json:"openapiDocuments,omitempty"`
	GRPCServices     []json.RawMessage `json:"grpcServices,omitempty"`
	BackendURL       string            `json:"-"`
}

type OpenAPIDocument struct {
	Document APIConfigFile `json:"document"`
}

type APIConfigFile struct {
	Path     string `json:"path,omitempty"`
	Contents string `json:"contents"`
}

func (api *API) routeConfigs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createConfig(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "configs") {
			api.listConfigs(w, r)
		} else {
			api.getConfig(w, r)
		}
	case http.MethodDelete:
		api.deleteConfig(w, r)
	case http.MethodPatch:
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "API config updates are not supported")
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createConfig(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	apiID := extractAfter(r.URL.Path, "apis")
	project := extractAfter(r.URL.Path, "projects")
	if project == "" || apiID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid API parent")
		return
	}
	parent := fmt.Sprintf("projects/%s/locations/global/apis/%s", project, apiID)
	configID := r.URL.Query().Get("apiConfigId")
	if configID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "apiConfigId query parameter is required")
		return
	}
	var config ApiConfig
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid API config JSON")
		return
	}
	if len(config.GRPCServices) != 0 {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "gRPC API configs are not supported")
		return
	}
	backend, err := backendFromDocuments(config.OpenAPIDocuments)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	config.BackendURL = backend.String()
	config.Name = parent + "/configs/" + configID
	config.State = "ACTIVE"
	config.CreateTime = time.Now().UTC().Format(time.RFC3339Nano)
	config.UpdateTime = config.CreateTime

	api.persistMu.Lock()
	api.mu.Lock()
	if _, ok := api.apis[parent]; !ok {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent API not found: "+parent)
		return
	}
	if _, ok := api.configs[config.Name]; ok {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "API config already exists: "+configID)
		return
	}
	api.configs[config.Name] = &config
	api.mu.Unlock()
	if api.stateStore != nil {
		if err := api.stateStore.Save(stateEntry, api.snapshot()); err != nil {
			api.mu.Lock()
			delete(api.configs, config.Name)
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
			return
		}
	}
	api.persistMu.Unlock()
	_ = json.NewEncoder(w).Encode(deepCopyConfig(&config))
}

func (api *API) getConfig(w http.ResponseWriter, r *http.Request) {
	name := parseConfigName(r.URL.Path)
	api.mu.RLock()
	config, ok := api.configs[name]
	if ok {
		config = deepCopyConfig(config)
	}
	api.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "API config not found: "+name)
		return
	}
	_ = json.NewEncoder(w).Encode(config)
}

func (api *API) listConfigs(w http.ResponseWriter, r *http.Request) {
	parent := parseApiName(r.URL.Path)
	prefix := parent + "/configs/"
	pageSize := 100
	if raw := r.URL.Query().Get("pageSize"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "pageSize must be a positive integer")
			return
		}
		pageSize = value
	}
	if pageSize > 500 {
		pageSize = 500
	}
	api.mu.RLock()
	configs := make([]*ApiConfig, 0)
	for name, config := range api.configs {
		if strings.HasPrefix(name, prefix) {
			configs = append(configs, deepCopyConfig(config))
		}
	}
	api.mu.RUnlock()
	page, nextToken, err := pagination.Page(configs, pageSize, r.URL.Query().Get("pageToken"), pagination.Scope{
		Service: "apigateway.configs",
		Parent:  parent,
	}, func(config *ApiConfig) string { return config.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"apiConfigs": page, "nextPageToken": nextToken})
}

func (api *API) deleteConfig(w http.ResponseWriter, r *http.Request) {
	name := parseConfigName(r.URL.Path)
	api.persistMu.Lock()
	api.mu.Lock()
	config, ok := api.configs[name]
	if !ok {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "API config not found: "+name)
		return
	}
	for _, gateway := range api.gateways {
		if gateway.ApiConfig == name {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "API config is in use by a gateway")
			return
		}
	}
	delete(api.configs, name)
	api.mu.Unlock()
	if api.stateStore != nil {
		if err := api.stateStore.Save(stateEntry, api.snapshot()); err != nil {
			api.mu.Lock()
			api.configs[name] = config
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
			return
		}
	}
	api.persistMu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (api *API) GatewayProxy(resourceName string) (http.Handler, error) {
	if _, err := api.proxyForResource(resourceName); err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy, err := api.proxyForResource(resourceName)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "gateway deployment is no longer available")
			return
		}
		proxy.ServeHTTP(w, r)
	}), nil
}

func (api *API) proxyForResource(resourceName string) (http.Handler, error) {
	api.mu.RLock()
	if gateway, ok := api.gateways[resourceName]; ok {
		resourceName = gateway.ApiConfig
	}
	config, ok := api.configs[resourceName]
	if ok {
		config = deepCopyConfig(config)
	}
	api.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("API config for %q not found", resourceName)
	}
	target, err := backendFromDocuments(config.OpenAPIDocuments)
	if err != nil && config.BackendURL != "" {
		target, err = validateLoopbackTarget(config.BackendURL)
	}
	if err != nil {
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	direct := proxy.Director
	proxy.Director = func(request *http.Request) {
		direct(request)
		request.Host = target.Host
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
		writeError(w, http.StatusBadGateway, "UNAVAILABLE", "local API backend unavailable")
	}
	return proxy, nil
}

func backendFromDocuments(documents []OpenAPIDocument) (*url.URL, error) {
	if len(documents) != 1 {
		return nil, errors.New("exactly one OpenAPI document is required")
	}
	contents, err := base64.StdEncoding.DecodeString(documents[0].Document.Contents)
	if err != nil {
		return nil, errors.New("OpenAPI document contents must be valid base64")
	}
	if len(contents) > maxAPIConfigDocumentBytes {
		return nil, errors.New("OpenAPI document exceeds 1 MiB")
	}
	var document any
	if err := json.Unmarshal(contents, &document); err != nil {
		return nil, errors.New("OpenAPI document must be valid JSON")
	}
	address := findBackendAddress(document)
	if address == "" {
		return nil, errors.New("OpenAPI document requires an x-google-backend.address")
	}
	return validateLoopbackTarget(address)
}

func findBackendAddress(value any) string {
	switch typed := value.(type) {
	case map[string]any:
		if backend, ok := typed["x-google-backend"].(map[string]any); ok {
			if address, ok := backend["address"].(string); ok {
				return address
			}
		}
		for _, nested := range typed {
			if address := findBackendAddress(nested); address != "" {
				return address
			}
		}
	case []any:
		for _, nested := range typed {
			if address := findBackendAddress(nested); address != "" {
				return address
			}
		}
	}
	return ""
}

func validateLoopbackTarget(raw string) (*url.URL, error) {
	target, err := url.Parse(raw)
	if err != nil || target.Scheme != "http" || target.Host == "" || target.User != nil {
		return nil, errors.New("backend address must be an HTTP loopback origin")
	}
	host := target.Hostname()
	if host == "localhost" {
		port := target.Port()
		if port == "" {
			port = "80"
		}
		target.Host = net.JoinHostPort("127.0.0.1", port)
	} else {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, errors.New("backend address must use localhost or a literal loopback IP")
		}
	}
	target.RawQuery = ""
	target.Fragment = ""
	return target, nil
}

func parseConfigName(path string) string {
	return parseApiName(path) + "/configs/" + extractAfter(path, "configs")
}

func deepCopyConfig(config *ApiConfig) *ApiConfig {
	raw, _ := json.Marshal(config)
	var clone ApiConfig
	_ = json.Unmarshal(raw, &clone)
	clone.BackendURL = config.BackendURL
	return &clone
}
