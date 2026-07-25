package artifactregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
)

const defaultRegistryURL = "http://127.0.0.1:5000"

func init() {
	registry.Register("artifactregistry.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr, ctx.SvcMgr)
	})
}

type Repository struct {
	Name        string            `json:"name"`
	Format      string            `json:"format"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
	Mode        string            `json:"mode,omitempty"`
}

type Package struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	CreateTime  string `json:"createTime,omitempty"`
	UpdateTime  string `json:"updateTime,omitempty"`
}

type Version struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	CreateTime  string   `json:"createTime,omitempty"`
	RelatedTags []string `json:"relatedTags,omitempty"`
}

// RegistryIndex exposes the image metadata needed by the Artifact Registry API.
type RegistryIndex interface {
	Repositories(context.Context) ([]string, error)
	Tags(context.Context, string) ([]string, error)
}

type dockerRegistryIndex struct {
	client  *http.Client
	baseURL string
}

// NewDockerRegistryIndex creates an index backed by Docker Registry HTTP API v2.
func NewDockerRegistryIndex(client *http.Client, baseURL string) RegistryIndex {
	if client == nil {
		client = http.DefaultClient
	}
	return &dockerRegistryIndex{
		client:  client,
		baseURL: strings.TrimRight(baseURL, "/"),
	}
}

func (index *dockerRegistryIndex) Repositories(ctx context.Context) ([]string, error) {
	var response struct {
		Repositories []string `json:"repositories"`
	}
	if err := index.get(ctx, "/v2/_catalog", &response); err != nil {
		return nil, err
	}
	if response.Repositories == nil {
		response.Repositories = []string{}
	}
	sort.Strings(response.Repositories)
	return response.Repositories, nil
}

func (index *dockerRegistryIndex) Tags(ctx context.Context, repository string) ([]string, error) {
	segments := strings.Split(repository, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}

	var response struct {
		Tags []string `json:"tags"`
	}
	if err := index.get(ctx, "/v2/"+strings.Join(segments, "/")+"/tags/list", &response); err != nil {
		return nil, err
	}
	if response.Tags == nil {
		response.Tags = []string{}
	}
	sort.Strings(response.Tags)
	return response.Tags, nil
}

func (index *dockerRegistryIndex) get(ctx context.Context, path string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, index.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("build registry request: %w", err)
	}
	response, err := index.client.Do(request)
	if err != nil {
		return fmt.Errorf("registry request %s: %w", path, err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if readErr != nil {
			return fmt.Errorf("registry request %s returned %s (read response: %v)", path, response.Status, readErr)
		}
		return fmt.Errorf("registry request %s returned %s: %s", path, response.Status, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return fmt.Errorf("decode registry response %s: %w", path, err)
	}
	return nil
}

type API struct {
	mu     sync.RWMutex
	svcMgr *orchestrator.ServiceManager
	opMgr  *orchestrator.OperationManager
	repos  map[string]*Repository
	index  RegistryIndex
}

func NewAPI(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager) *API {
	registryURL := os.Getenv("MINISKY_ARTIFACT_REGISTRY_URL")
	var index RegistryIndex
	if registryURL != "" {
		client := &http.Client{Timeout: 5 * time.Second}
		index = NewDockerRegistryIndex(client, registryURL)
	} else if sm == nil {
		index = NewDockerRegistryIndex(&http.Client{Timeout: 5 * time.Second}, defaultRegistryURL)
	}
	return NewAPIWithRegistryIndex(opMgr, sm, index)
}

// NewAPIWithRegistryIndex allows callers to inject a registry index.
func NewAPIWithRegistryIndex(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, index RegistryIndex) *API {
	return &API{
		opMgr:  opMgr,
		svcMgr: sm,
		repos:  make(map[string]*Repository),
		index:  index,
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.Contains(path, "/operations/") {
		if r.Method == http.MethodGet {
			api.handleGetOperation(w, r)
			return
		}
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Artifact Registry operation not found")
		return
	}
	// v1/projects/{project}/locations/{location}/repositories
	if strings.Contains(path, "/repositories") {
		if strings.Contains(path, "/packages") {
			if r.Method != http.MethodGet {
				writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "package and version mutation is not supported; use the Docker Registry v2 API with a manifest digest")
				return
			}
			if strings.HasSuffix(strings.TrimRight(path, "/"), "/versions") {
				api.handleListVersions(w, r)
				return
			}
			api.handleListPackages(w, r)
			return
		}
		isCollection := strings.HasSuffix(strings.TrimRight(path, "/"), "/repositories")
		switch r.Method {
		case http.MethodGet:
			if isCollection {
				api.handleListRepositories(w, r)
			} else {
				api.handleGetRepository(w, r)
			}
			return
		case http.MethodPost:
			if isCollection {
				api.handleCreateRepository(w, r)
				return
			}
		case http.MethodDelete:
			if !isCollection {
				api.handleDeleteRepository(w, r)
				return
			}
		}
	}

	writeError(w, http.StatusNotFound, "NOT_FOUND", "Artifact Registry resource not found")
}

func (api *API) handleGetRepository(w http.ResponseWriter, r *http.Request) {
	name := resourceName(r.URL.Path)
	api.mu.RLock()
	repository := cloneRepository(api.repos[name])
	api.mu.RUnlock()
	if repository == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "repository not found: "+name)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(repository)
}

func (api *API) handleGetOperation(w http.ResponseWriter, r *http.Request) {
	serviceName := resourceName(r.URL.Path)
	managerName := serviceName[strings.LastIndex(serviceName, "/")+1:]
	if api.opMgr == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Artifact Registry operation not found: "+serviceName)
		return
	}
	operation := api.opMgr.Get(managerName)
	if operation == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Artifact Registry operation not found: "+serviceName)
		return
	}
	api.writeOperation(w, serviceName, operation)
}

func (api *API) writeOperation(w http.ResponseWriter, serviceName string, operation *orchestrator.Operation) {
	response := map[string]any{
		"name": serviceName,
		"done": operation.Done,
		"metadata": map[string]any{
			"target": operation.TargetLink,
			"verb":   operation.OperationType,
		},
	}
	if operation.Error != nil {
		response["error"] = operation.Error
	} else if operation.Done {
		if operation.OperationType == "DELETE" {
			response["response"] = map[string]any{}
		} else {
			api.mu.RLock()
			repository := cloneRepository(api.repos[operation.TargetLink])
			api.mu.RUnlock()
			if repository != nil {
				response["response"] = repository
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func resourceName(path string) string {
	return strings.Trim(strings.TrimPrefix(path, "/v1/"), "/")
}

func cloneRepository(repository *Repository) *Repository {
	if repository == nil {
		return nil
	}
	clone := *repository
	if repository.Labels != nil {
		clone.Labels = make(map[string]string, len(repository.Labels))
		for key, value := range repository.Labels {
			clone.Labels[key] = value
		}
	}
	return &clone
}

func (api *API) handleListRepositories(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	var project string
	for i, p := range parts {
		if p == "projects" && i+1 < len(parts) {
			project = parts[i+1]
			break
		}
	}

	list := make([]Repository, 0)
	api.mu.RLock()
	for _, repo := range api.repos {
		if strings.Contains(repo.Name, fmt.Sprintf("projects/%s", project)) {
			list = append(list, *repo)
		}
	}
	api.mu.RUnlock()
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"repositories": list,
	})
}

func (api *API) handleCreateRepository(w http.ResponseWriter, r *http.Request) {
	var repo Repository
	if err := json.NewDecoder(r.Body).Decode(&repo); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid repository JSON")
		return
	}

	// Path: v1/projects/{project}/locations/{location}/repositories?repositoryId=...
	parts := strings.Split(r.URL.Path, "/")
	project := "default"
	location := "us-central1"

	for i, p := range parts {
		if p == "projects" && i+1 < len(parts) {
			project = parts[i+1]
		}
		if p == "locations" && i+1 < len(parts) {
			location = parts[i+1]
		}
	}

	repoId := r.URL.Query().Get("repositoryId")
	if repoId == "" {
		repoId = r.URL.Query().Get("repository_id")
	}
	if repoId == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "repositoryId is required")
		return
	}
	repo.Name = fmt.Sprintf("projects/%s/locations/%s/repositories/%s", project, location, repoId)
	repo.CreateTime = time.Now().UTC().Format(time.RFC3339Nano)
	repo.UpdateTime = repo.CreateTime
	if repo.Mode == "" {
		repo.Mode = "STANDARD_REPOSITORY"
	}

	api.mu.Lock()
	if _, exists := api.repos[repo.Name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "repository already exists: "+repo.Name)
		return
	}
	api.repos[repo.Name] = &repo
	api.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if api.opMgr == nil {
		_ = json.NewEncoder(w).Encode(repo)
		return
	}
	op, err := api.opMgr.RegisterDurable("artifactregistry#operation", "CREATE", repo.Name, "", location)
	if err != nil {
		api.mu.Lock()
		delete(api.repos, repo.Name)
		api.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository operation")
		return
	}
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)
	api.writeOperation(w, serviceName, op)
	api.opMgr.RunAsync(op.Name, func() error { return nil })
}

func (api *API) handleListPackages(w http.ResponseWriter, r *http.Request) {
	parent, ok := resourceBefore(r.URL.Path, "/packages")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	index, err := api.registryIndex(r.Context())
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	repositories, err := index.Repositories(r.Context())
	if err != nil {
		writeUpstreamError(w, err)
		return
	}

	repositoryID := repositoryIDFromParent(parent)
	prefix := repositoryID + "/"
	packages := make([]Package, 0, len(repositories))
	for _, repository := range repositories {
		if !strings.HasPrefix(repository, prefix) {
			continue
		}
		packageID := strings.TrimPrefix(repository, prefix)
		if packageID == "" {
			continue
		}
		packages = append(packages, Package{
			Name:        parent + "/packages/" + packageID,
			DisplayName: packageID,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"packages": packages,
	})
}

func (api *API) handleListVersions(w http.ResponseWriter, r *http.Request) {
	repositoryParent, ok := resourceBefore(r.URL.Path, "/packages")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	packagePosition := strings.Index(r.URL.Path, "/packages/")
	if packagePosition < 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	packageSuffix := r.URL.Path[packagePosition+len("/packages/"):]
	packageID := strings.TrimSuffix(strings.TrimRight(packageSuffix, "/"), "/versions")
	packageID, err := url.PathUnescape(packageID)
	if err != nil || packageID == "" {
		http.Error(w, "invalid package name", http.StatusBadRequest)
		return
	}

	index, err := api.registryIndex(r.Context())
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	registryRepository := repositoryIDFromParent(repositoryParent) + "/" + packageID
	tags, err := index.Tags(r.Context(), registryRepository)
	if err != nil {
		writeUpstreamError(w, err)
		return
	}
	packageName := repositoryParent + "/packages/" + packageID
	versions := make([]Version, 0, len(tags))
	for _, tag := range tags {
		versions = append(versions, Version{
			Name:        packageName + "/versions/" + tag,
			RelatedTags: []string{packageName + "/tags/" + tag},
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"versions": versions,
	})
}

func (api *API) registryIndex(ctx context.Context) (RegistryIndex, error) {
	api.mu.RLock()
	index := api.index
	api.mu.RUnlock()
	if index != nil {
		return index, nil
	}
	if api.svcMgr == nil {
		return nil, fmt.Errorf("Docker Registry backend is not configured")
	}
	baseURL, err := api.svcMgr.EnsureServiceRunning(
		ctx,
		"artifactregistry.googleapis.com",
		"REGISTRY_STORAGE_DELETE_ENABLED=true",
	)
	if err != nil {
		return nil, fmt.Errorf("start owned registry backend: %w", err)
	}
	index = NewDockerRegistryIndex(&http.Client{Timeout: 5 * time.Second}, baseURL)
	api.mu.Lock()
	if api.index == nil {
		api.index = index
	} else {
		index = api.index
	}
	api.mu.Unlock()
	return index, nil
}

func (api *API) handleDeleteRepository(w http.ResponseWriter, r *http.Request) {
	name := resourceName(r.URL.Path)
	if name == "" || strings.HasSuffix(name, "/repositories") {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	api.mu.Lock()
	repo, exists := api.repos[name]
	if exists {
		delete(api.repos, name)
	}
	api.mu.Unlock()
	if !exists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "repository not found: "+name)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if api.opMgr == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{})
		return
	}
	location := locationFromResource(repo.Name)
	op, err := api.opMgr.RegisterDurable("artifactregistry#operation", "DELETE", repo.Name, "", location)
	if err != nil {
		api.mu.Lock()
		api.repos[name] = repo
		api.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository operation")
		return
	}
	project := projectFromResource(repo.Name)
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)
	api.writeOperation(w, serviceName, op)
	api.opMgr.RunAsync(op.Name, func() error { return nil })
}

func repositoryIDFromParent(parent string) string {
	const marker = "/repositories/"
	position := strings.LastIndex(parent, marker)
	if position < 0 {
		return ""
	}
	return strings.Trim(parent[position+len(marker):], "/")
}

func projectFromResource(name string) string {
	parts := strings.Split(name, "/")
	for i, part := range parts {
		if part == "projects" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func resourceBefore(path, marker string) (string, bool) {
	position := strings.Index(path, marker)
	if position < 0 {
		return "", false
	}
	return strings.Trim(strings.TrimPrefix(path[:position], "/v1/"), "/"), true
}

func locationFromResource(name string) string {
	parts := strings.Split(name, "/")
	for i, part := range parts {
		if part == "locations" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func writeUpstreamError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusBadGateway, "BAD_GATEWAY", "artifact registry upstream: "+err.Error())
}

func writeError(w http.ResponseWriter, status int, symbolicStatus, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": message,
			"status":  symbolicStatus,
			"details": []any{},
		},
	})
}

func (api *API) Proxy() *httputil.ReverseProxy {
	return nil
}
