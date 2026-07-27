package dataform

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
	"minisky/pkg/orchestrator"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("dataform.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Dataform v1beta1)
// ─────────────────────────────────────────────────────────────────────────────

// Repository represents a Dataform repository resource.
type Repository struct {
	Name              string             `json:"name"`
	CreateTime        string             `json:"createTime,omitempty"`
	UpdateTime        string             `json:"updateTime,omitempty"`
	GitRemoteSettings *GitRemoteSettings `json:"gitRemoteSettings,omitempty"`
	DisplayName       string             `json:"displayName,omitempty"`
}

// GitRemoteSettings holds git remote configuration.
type GitRemoteSettings struct {
	URL                string `json:"url,omitempty"`
	DefaultBranch      string `json:"defaultBranch,omitempty"`
	TokenSecretVersion string `json:"authenticationTokenSecretVersion,omitempty"`
}

// Workspace represents a Dataform workspace resource.
type Workspace struct {
	Name       string `json:"name"`
	CreateTime string `json:"createTime,omitempty"`
	UpdateTime string `json:"updateTime,omitempty"`
}

// CompilationResult is a bounded local compilation of an empty workspace.
type CompilationResult struct {
	Name              string             `json:"name"`
	Workspace         string             `json:"workspace,omitempty"`
	CompilationErrors []CompilationError `json:"compilationErrors"`
}

type CompilationError struct {
	Message string `json:"message"`
}

// WorkflowInvocation records the terminal outcome of invoking an empty compilation.
type WorkflowInvocation struct {
	Name              string    `json:"name"`
	CompilationResult string    `json:"compilationResult"`
	State             string    `json:"state"`
	InvocationTiming  *Interval `json:"invocationTiming,omitempty"`
}

type Interval struct {
	StartTime string `json:"startTime,omitempty"`
	EndTime   string `json:"endTime,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Dataform v1beta1 REST shim.
type API struct {
	mu                  sync.RWMutex
	persistMu           sync.Mutex
	opMgr               *orchestrator.OperationManager
	stateStore          dataformStateStore
	repositories        map[string]*Repository
	workspaces          map[string]*Workspace
	compilationResults  map[string]*CompilationResult
	workflowInvocations map[string]*WorkflowInvocation
	nextCompilationID   uint64
	nextInvocationID    uint64
}

// NewAPI creates a new Dataform API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newAPI(opMgr, state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: Dataform] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Dataform] state rehydration failed: %v", err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store dataformStateStore) *API {
	return &API{
		opMgr:               opMgr,
		stateStore:          store,
		repositories:        make(map[string]*Repository),
		workspaces:          make(map[string]*Workspace),
		compilationResults:  make(map[string]*CompilationResult),
		workflowInvocations: make(map[string]*WorkflowInvocation),
		nextCompilationID:   1,
		nextInvocationID:    1,
	}
}

func newTestAPI() *API {
	return newAPI(orchestrator.NewOperationManager(), nil)
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Dataform] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case isCompilationCollection(r.URL.Path) && r.Method == http.MethodPost:
		api.createCompilationResult(w, r)
	case isCompilationResource(r.URL.Path) && r.Method == http.MethodGet:
		api.getCompilationResult(w, r)
	case isInvocationCollection(r.URL.Path) && r.Method == http.MethodPost:
		api.createWorkflowInvocation(w, r)
	case isInvocationResource(r.URL.Path) && r.Method == http.MethodGet:
		api.getWorkflowInvocation(w, r)
	// Workspaces (must match before repositories since path contains /repositories/)
	case isWorkspaceCollection(r.URL.Path) && r.Method == http.MethodPost:
		api.createWorkspace(w, r)
	case isWorkspaceCollection(r.URL.Path) && r.Method == http.MethodGet:
		api.listWorkspaces(w, r)
	case isWorkspaceResource(r.URL.Path) && r.Method == http.MethodGet:
		api.getWorkspace(w, r)
	case isWorkspaceResource(r.URL.Path) && r.Method == http.MethodPatch:
		api.updateWorkspace(w, r)
	case isWorkspaceResource(r.URL.Path) && r.Method == http.MethodDelete:
		api.deleteWorkspace(w, r)
	// Repositories
	case isRepoCollection(r.URL.Path) && r.Method == http.MethodPost:
		api.createRepository(w, r)
	case isRepoCollection(r.URL.Path) && r.Method == http.MethodGet:
		api.listRepositories(w, r)
	case isRepoResource(r.URL.Path) && r.Method == http.MethodGet:
		api.getRepository(w, r)
	case isRepoResource(r.URL.Path) && r.Method == http.MethodPatch:
		api.updateRepository(w, r)
	case isRepoResource(r.URL.Path) && r.Method == http.MethodDelete:
		api.deleteRepository(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Dataform resource not found")
	}
}

func (api *API) createCompilationResult(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	repoName := buildRepoName(r.URL.Path)
	var request struct {
		Workspace    string `json:"workspace"`
		GitCommitish string `json:"gitCommitish"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if request.GitCommitish != "" {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Dataform Git compilation is not implemented; only an existing empty workspace can be compiled locally")
		return
	}
	if request.Workspace == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "workspace is required")
		return
	}
	if !strings.HasPrefix(request.Workspace, repoName+"/workspaces/") {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "workspace must belong to the parent repository")
		return
	}

	api.mu.Lock()
	if _, ok := api.repositories[repoName]; !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent repository not found: "+repoName)
		return
	}
	if _, ok := api.workspaces[request.Workspace]; !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workspace not found: "+request.Workspace)
		return
	}
	name := fmt.Sprintf("%s/compilationResults/cr-%d", repoName, api.nextCompilationID)
	api.nextCompilationID++
	result := &CompilationResult{
		Name:              name,
		Workspace:         request.Workspace,
		CompilationErrors: []CompilationError{},
	}
	api.compilationResults[name] = result
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.compilationResults, name)
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(cloneCompilationResult(result))
}

func (api *API) getCompilationResult(w http.ResponseWriter, r *http.Request) {
	name := buildCompilationName(r.URL.Path)
	api.mu.RLock()
	result, ok := api.compilationResults[name]
	if ok {
		result = cloneCompilationResult(result)
	}
	api.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "compilation result not found: "+name)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}

func (api *API) createWorkflowInvocation(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	repoName := buildRepoName(r.URL.Path)
	var request struct {
		CompilationResult string `json:"compilationResult"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if request.CompilationResult == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "compilationResult is required")
		return
	}
	if !strings.HasPrefix(request.CompilationResult, repoName+"/compilationResults/") {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "compilationResult must belong to the parent repository")
		return
	}

	api.mu.Lock()
	if _, ok := api.repositories[repoName]; !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent repository not found: "+repoName)
		return
	}
	if _, ok := api.compilationResults[request.CompilationResult]; !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "compilation result not found: "+request.CompilationResult)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	name := fmt.Sprintf("%s/workflowInvocations/wi-%d", repoName, api.nextInvocationID)
	api.nextInvocationID++
	invocation := &WorkflowInvocation{
		Name:              name,
		CompilationResult: request.CompilationResult,
		State:             "SUCCEEDED",
		InvocationTiming:  &Interval{StartTime: now, EndTime: now},
	}
	api.workflowInvocations[name] = invocation
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.workflowInvocations, name)
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(cloneWorkflowInvocation(invocation))
}

func (api *API) getWorkflowInvocation(w http.ResponseWriter, r *http.Request) {
	name := buildInvocationName(r.URL.Path)
	api.mu.RLock()
	invocation, ok := api.workflowInvocations[name]
	if ok {
		invocation = cloneWorkflowInvocation(invocation)
	}
	api.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workflow invocation not found: "+name)
		return
	}
	_ = json.NewEncoder(w).Encode(invocation)
}

// ─────────────────────────────────────────────────────────────────────────────
// Repository handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createRepository(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	repoID := r.URL.Query().Get("repositoryId")
	if repoID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "repositoryId query parameter is required")
		return
	}

	var repo Repository
	if err := json.NewDecoder(r.Body).Decode(&repo); err != nil {
		if err.Error() != "EOF" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
			return
		}
	}

	name := fmt.Sprintf("projects/%s/locations/%s/repositories/%s", project, location, repoID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	repo.Name = name
	repo.CreateTime = now
	repo.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.repositories[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "repository already exists: "+repoID)
		return
	}
	api.repositories[name] = &repo
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.repositories, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(&repo)
}

func (api *API) getRepository(w http.ResponseWriter, r *http.Request) {
	name := buildRepoName(r.URL.Path)

	api.mu.RLock()
	repo, ok := api.repositories[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "repository not found: "+name)
		return
	}
	clone := cloneRepo(repo)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listRepositories(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/repositories/", project, location)

	pageSize := 50
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
	var all []*Repository
	for key, repo := range api.repositories {
		if strings.HasPrefix(key, prefix) {
			all = append(all, cloneRepo(repo))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "dataform.repositories",
		Parent:  strings.TrimSuffix(prefix, "/repositories/"),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(repository *Repository) string { return repository.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*Repository{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"repositories":  result,
		"nextPageToken": nextToken,
	})
}

func (api *API) updateRepository(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildRepoName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.repositories[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "repository not found: "+name)
		return
	}

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	raw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(raw, &merged)

	for k, v := range patch {
		merged[k] = v
	}
	merged["name"] = existing.Name
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated Repository
	_ = json.Unmarshal(updatedRaw, &updated)
	api.repositories[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(&updated)
}

func (api *API) deleteRepository(w http.ResponseWriter, r *http.Request) {
	name := buildRepoName(r.URL.Path)

	api.mu.Lock()
	repo, exists := api.repositories[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "repository not found: "+name)
		return
	}
	delete(api.repositories, name)
	// Cascade delete workspaces under this repository
	wsPrefix := name + "/workspaces/"
	deletedWorkspaces := make(map[string]*Workspace)
	for key, v := range api.workspaces {
		if strings.HasPrefix(key, wsPrefix) {
			deletedWorkspaces[key] = v
			delete(api.workspaces, key)
		}
	}
	compilationPrefix := name + "/compilationResults/"
	deletedCompilations := make(map[string]*CompilationResult)
	for key, value := range api.compilationResults {
		if strings.HasPrefix(key, compilationPrefix) {
			deletedCompilations[key] = value
			delete(api.compilationResults, key)
		}
	}
	invocationPrefix := name + "/workflowInvocations/"
	deletedInvocations := make(map[string]*WorkflowInvocation)
	for key, value := range api.workflowInvocations {
		if strings.HasPrefix(key, invocationPrefix) {
			deletedInvocations[key] = value
			delete(api.workflowInvocations, key)
		}
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resources since persist failed
		api.mu.Lock()
		api.repositories[name] = repo
		for k, v := range deletedWorkspaces {
			api.workspaces[k] = v
		}
		for k, v := range deletedCompilations {
			api.compilationResults[k] = v
		}
		for k, v := range deletedInvocations {
			api.workflowInvocations[k] = v
		}
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Workspace handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createWorkspace(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	repoName := buildRepoName(r.URL.Path)
	wsID := r.URL.Query().Get("workspaceId")
	if wsID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "workspaceId query parameter is required")
		return
	}

	var ws Workspace
	if err := json.NewDecoder(r.Body).Decode(&ws); err != nil && err.Error() != "EOF" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := fmt.Sprintf("%s/workspaces/%s", repoName, wsID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	ws.Name = name
	ws.CreateTime = now
	ws.UpdateTime = now

	api.mu.Lock()
	if _, repoExists := api.repositories[repoName]; !repoExists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent repository not found: "+repoName)
		return
	}
	if _, exists := api.workspaces[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "workspace already exists: "+wsID)
		return
	}
	api.workspaces[name] = &ws
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.workspaces, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(&ws)
}

func (api *API) getWorkspace(w http.ResponseWriter, r *http.Request) {
	name := buildWorkspaceName(r.URL.Path)

	api.mu.RLock()
	ws, ok := api.workspaces[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workspace not found: "+name)
		return
	}
	clone := cloneWorkspace(ws)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listWorkspaces(w http.ResponseWriter, r *http.Request) {
	repoName := buildRepoName(r.URL.Path)
	prefix := repoName + "/workspaces/"

	pageSize := 50
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
	var all []*Workspace
	for key, ws := range api.workspaces {
		if strings.HasPrefix(key, prefix) {
			all = append(all, cloneWorkspace(ws))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "dataform.workspaces",
		Parent:  repoName,
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(workspace *Workspace) string { return workspace.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*Workspace{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"workspaces":    result,
		"nextPageToken": nextToken,
	})
}

func (api *API) updateWorkspace(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := buildWorkspaceName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.workspaces[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workspace not found: "+name)
		return
	}

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	raw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(raw, &merged)

	for k, v := range patch {
		merged[k] = v
	}
	merged["name"] = existing.Name
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated Workspace
	_ = json.Unmarshal(updatedRaw, &updated)
	api.workspaces[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(&updated)
}

func (api *API) deleteWorkspace(w http.ResponseWriter, r *http.Request) {
	name := buildWorkspaceName(r.URL.Path)

	api.mu.Lock()
	ws, exists := api.workspaces[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "workspace not found: "+name)
		return
	}
	delete(api.workspaces, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.workspaces[name] = ws
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
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

func buildRepoName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	repoID := extractAfter(path, "repositories")
	if idx := strings.Index(repoID, "/"); idx >= 0 {
		repoID = repoID[:idx]
	}
	return fmt.Sprintf("projects/%s/locations/%s/repositories/%s", project, location, repoID)
}

func buildWorkspaceName(path string) string {
	repoName := buildRepoName(path)
	wsID := extractAfter(path, "workspaces")
	return fmt.Sprintf("%s/workspaces/%s", repoName, wsID)
}

func buildCompilationName(path string) string {
	return fmt.Sprintf("%s/compilationResults/%s", buildRepoName(path), extractAfter(path, "compilationResults"))
}

func buildInvocationName(path string) string {
	return fmt.Sprintf("%s/workflowInvocations/%s", buildRepoName(path), extractAfter(path, "workflowInvocations"))
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

// isWorkspaceCollection matches paths ending in /workspaces (no trailing ID).
func isWorkspaceCollection(path string) bool {
	return strings.Contains(path, "/repositories/") && strings.HasSuffix(path, "/workspaces")
}

// isWorkspaceResource matches paths with /workspaces/{id}.
func isWorkspaceResource(path string) bool {
	if !strings.Contains(path, "/workspaces/") {
		return false
	}
	wsID := extractAfter(path, "workspaces")
	return wsID != ""
}

func isCompilationCollection(path string) bool {
	return strings.Contains(path, "/repositories/") && strings.HasSuffix(path, "/compilationResults")
}

func isCompilationResource(path string) bool {
	return strings.Contains(path, "/compilationResults/") && extractAfter(path, "compilationResults") != ""
}

func isInvocationCollection(path string) bool {
	return strings.Contains(path, "/repositories/") && strings.HasSuffix(path, "/workflowInvocations")
}

func isInvocationResource(path string) bool {
	return strings.Contains(path, "/workflowInvocations/") && extractAfter(path, "workflowInvocations") != ""
}

// isRepoCollection matches paths ending in /repositories (no trailing ID).
func isRepoCollection(path string) bool {
	return strings.HasSuffix(path, "/repositories") && !strings.Contains(path, "/workspaces")
}

// isRepoResource matches paths with /repositories/{id} but not /workspaces.
func isRepoResource(path string) bool {
	if !strings.Contains(path, "/repositories/") {
		return false
	}
	if strings.Contains(path, "/workspaces") {
		return false
	}
	return true
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
