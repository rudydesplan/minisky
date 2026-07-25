package resourcemanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const (
	stateEntry       = "resourcemanager/metadata"
	defaultProjectID = "local-dev-project"
	defaultOrgName   = "organizations/100000000000"
)

var projectIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)

func init() {
	registry.Register("cloudresourcemanager.googleapis.com", func(*registry.Context) http.Handler {
		return NewAPI()
	})
}

type Project struct {
	Name          string    `json:"name"`
	Parent        string    `json:"parent,omitempty"`
	ProjectID     string    `json:"projectId"`
	State         string    `json:"state"`
	DisplayName   string    `json:"displayName,omitempty"`
	CreateTime    time.Time `json:"createTime"`
	DeleteTime    time.Time `json:"deleteTime,omitempty"`
	ETag          string    `json:"etag"`
	SeededDefault bool      `json:"-"`
}

type Organization struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
}

type Folder struct {
	Name        string `json:"name"`
	Parent      string `json:"parent"`
	DisplayName string `json:"displayName,omitempty"`
}

type metadata struct {
	Projects      map[string]*Project      `json:"projects"`
	Organizations map[string]*Organization `json:"organizations,omitempty"`
	Folders       map[string]*Folder       `json:"folders,omitempty"`
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type API struct {
	mu        sync.RWMutex
	persistMu sync.Mutex
	store     stateStore
	projects  map[string]*Project
	orgs      map[string]*Organization
	folders   map[string]*Folder
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Resource Manager] state disabled: %v", err)
		api, _ := NewAPIWithStore(nil)
		return api
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		log.Printf("[Shim: Resource Manager] state rehydration failed: %v", err)
		api, _ = NewAPIWithStore(nil)
	}
	return api
}

func NewAPIWithStore(store stateStore) (*API, error) {
	api := &API{
		store:    store,
		projects: make(map[string]*Project),
		orgs:     make(map[string]*Organization),
		folders:  make(map[string]*Folder),
	}
	if store != nil {
		var saved metadata
		if err := store.Load(stateEntry, &saved); err != nil && !errors.Is(err, state.ErrNotFound) {
			return nil, fmt.Errorf("load Resource Manager metadata: %w", err)
		} else if err == nil {
			api.projects = normalizeProjects(saved.Projects)
			api.orgs = normalizeOrganizations(saved.Organizations)
			api.folders = normalizeFolders(saved.Folders)
		}
	}
	if _, ok := api.projects[defaultProjectID]; !ok {
		now := time.Now().UTC()
		api.projects[defaultProjectID] = &Project{
			Name: defaultProjectName(), ProjectID: defaultProjectID, DisplayName: "MiniSky local development",
			State: "ACTIVE", CreateTime: now, ETag: etag(now), SeededDefault: true,
		}
		if err := api.persist(); err != nil {
			return nil, fmt.Errorf("seed default project: %w", err)
		}
	}
	if _, ok := api.orgs[defaultOrgName]; !ok {
		api.orgs[defaultOrgName] = &Organization{Name: defaultOrgName, DisplayName: "MiniSky local organization"}
		if err := api.persist(); err != nil {
			return nil, fmt.Errorf("seed local organization: %w", err)
		}
	}
	return api, nil
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case path == "/v3/projects":
		switch r.Method {
		case http.MethodPost:
			api.createProject(w, r)
		case http.MethodGet:
			api.listProjects(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	case strings.HasPrefix(path, "/v3/projects/"):
		id := strings.TrimPrefix(path, "/v3/projects/")
		switch r.Method {
		case http.MethodGet:
			api.getProject(w, id)
		case http.MethodDelete:
			api.deleteProject(w, id)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	case path == "/v3/folders" && r.Method == http.MethodPost:
		api.createFolder(w, r)
	case strings.HasPrefix(path, "/v3/folders/") && r.Method == http.MethodGet:
		api.getFolder(w, strings.TrimPrefix(path, "/v3/folders/"))
	case strings.HasPrefix(path, "/v3/organizations/") && r.Method == http.MethodGet:
		api.getOrganization(w, strings.TrimPrefix(path, "/v3/organizations/"))
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Resource Manager resource not found")
	}
}

func (api *API) createFolder(w http.ResponseWriter, r *http.Request) {
	var input Folder
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.Parent == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Folder parent is required")
		return
	}
	if input.Name == "" {
		input.Name = fmt.Sprintf("folders/%d", time.Now().UnixNano())
	}
	if err := api.PutFolder(input); err != nil {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "Folder name or parent is invalid")
		return
	}
	_ = json.NewEncoder(w).Encode(doneOperation("create-"+strings.TrimPrefix(input.Name, "folders/"), input))
}

func (api *API) getFolder(w http.ResponseWriter, id string) {
	api.mu.RLock()
	folder := api.folders["folders/"+id]
	api.mu.RUnlock()
	if folder == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Folder not found")
		return
	}
	_ = json.NewEncoder(w).Encode(folder)
}

func (api *API) getOrganization(w http.ResponseWriter, id string) {
	api.mu.RLock()
	org := api.orgs["organizations/"+id]
	api.mu.RUnlock()
	if org == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Organization not found")
		return
	}
	_ = json.NewEncoder(w).Encode(org)
}

func (api *API) createProject(w http.ResponseWriter, r *http.Request) {
	var input Project
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid project request")
		return
	}
	if !projectIDPattern.MatchString(input.ProjectID) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "projectId must be 6-30 lowercase letters, digits, or hyphens")
		return
	}
	if input.Parent != "" && !api.parentExists(input.Parent) {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "Project parent does not exist")
		return
	}
	now := time.Now().UTC()
	project := &Project{
		Name: "projects/" + input.ProjectID, Parent: input.Parent, ProjectID: input.ProjectID,
		DisplayName: input.DisplayName, State: "ACTIVE", CreateTime: now, ETag: etag(now),
	}
	api.mu.Lock()
	if _, exists := api.projects[input.ProjectID]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Project already exists")
		return
	}
	api.projects[input.ProjectID] = project
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.projects, input.ProjectID)
		api.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist project")
		return
	}
	// Resource Manager creates are LROs in GCP. MiniSky performs the local
	// metadata mutation synchronously and returns a completed operation.
	_ = json.NewEncoder(w).Encode(doneOperation("create-"+input.ProjectID, project))
}

func (api *API) getProject(w http.ResponseWriter, id string) {
	api.mu.RLock()
	project := cloneProject(api.projects[id])
	api.mu.RUnlock()
	if project == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Project not found")
		return
	}
	_ = json.NewEncoder(w).Encode(project)
}

func (api *API) listProjects(w http.ResponseWriter, r *http.Request) {
	parent := r.URL.Query().Get("parent")
	api.mu.RLock()
	projects := make([]Project, 0, len(api.projects))
	for _, project := range api.projects {
		if parent == "" || project.Parent == parent {
			projects = append(projects, *cloneProject(project))
		}
	}
	api.mu.RUnlock()
	sort.Slice(projects, func(i, j int) bool { return projects[i].ProjectID < projects[j].ProjectID })
	_ = json.NewEncoder(w).Encode(map[string]any{"projects": projects})
}

func (api *API) deleteProject(w http.ResponseWriter, id string) {
	if id == defaultProjectID {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "The seeded local development project cannot be deleted")
		return
	}
	api.mu.Lock()
	project := api.projects[id]
	if project != nil {
		delete(api.projects, id)
	}
	api.mu.Unlock()
	if project == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Project not found")
		return
	}
	if err := api.persist(); err != nil {
		api.mu.Lock()
		api.projects[id] = project
		api.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist project deletion")
		return
	}
	project.State = "DELETE_REQUESTED"
	project.DeleteTime = time.Now().UTC()
	_ = json.NewEncoder(w).Encode(doneOperation("delete-"+id, project))
}

func (api *API) Exists(projectID string) bool {
	api.mu.RLock()
	defer api.mu.RUnlock()
	project := api.projects[projectID]
	return project != nil && project.State == "ACTIVE"
}

func (api *API) ProjectIDs() []string {
	api.mu.RLock()
	defer api.mu.RUnlock()
	result := make([]string, 0, len(api.projects))
	for id, project := range api.projects {
		if project.State == "ACTIVE" {
			result = append(result, id)
		}
	}
	sort.Strings(result)
	return result
}

func (api *API) PutOrganization(org Organization) error {
	if !strings.HasPrefix(org.Name, "organizations/") || strings.TrimPrefix(org.Name, "organizations/") == "" {
		return errors.New("organization name must use organizations/{id}")
	}
	api.mu.Lock()
	copy := org
	api.orgs[org.Name] = &copy
	api.mu.Unlock()
	return api.persist()
}

func (api *API) PutFolder(folder Folder) error {
	if !strings.HasPrefix(folder.Name, "folders/") || !api.parentExists(folder.Parent) {
		return errors.New("folder name or parent is invalid")
	}
	api.mu.Lock()
	copy := folder
	api.folders[folder.Name] = &copy
	api.mu.Unlock()
	return api.persist()
}

// Ancestors returns resource then each configured parent, stopping safely on
// missing or cyclic hierarchy data. IAM uses this for inherited policy lookup.
func (api *API) Ancestors(resource string) []string {
	api.mu.RLock()
	defer api.mu.RUnlock()
	result := []string{strings.TrimPrefix(resource, "/v1/")}
	seen := map[string]struct{}{result[0]: {}}
	current := result[0]
	for {
		parent := ""
		switch {
		case strings.HasPrefix(current, "projects/"):
			if project := api.projects[strings.TrimPrefix(current, "projects/")]; project != nil {
				parent = project.Parent
			}
		case strings.HasPrefix(current, "folders/"):
			if folder := api.folders[current]; folder != nil {
				parent = folder.Parent
			}
		}
		if parent == "" {
			break
		}
		if _, duplicate := seen[parent]; duplicate {
			break
		}
		seen[parent] = struct{}{}
		result = append(result, parent)
		current = parent
	}
	return result
}

func (api *API) parentExists(parent string) bool {
	if parent == "" {
		return true
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if strings.HasPrefix(parent, "organizations/") {
		return api.orgs[parent] != nil
	}
	if strings.HasPrefix(parent, "folders/") {
		return api.folders[parent] != nil
	}
	return false
}

func (api *API) persist() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	snapshot := metadata{
		Projects:      cloneProjects(api.projects),
		Organizations: cloneOrganizations(api.orgs),
		Folders:       cloneFolders(api.folders),
	}
	api.mu.RUnlock()
	return api.store.Save(stateEntry, snapshot)
}

func doneOperation(name string, response any) map[string]any {
	return map[string]any{"name": "operations/" + name, "done": true, "response": response}
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "status": status, "message": message,
	}})
}

func defaultProjectName() string { return "projects/" + defaultProjectID }
func etag(now time.Time) string  { return fmt.Sprintf("%x", now.UnixNano()) }

func normalizeProjects(values map[string]*Project) map[string]*Project {
	if values == nil {
		return make(map[string]*Project)
	}
	return values
}
func normalizeOrganizations(values map[string]*Organization) map[string]*Organization {
	if values == nil {
		return make(map[string]*Organization)
	}
	return values
}
func normalizeFolders(values map[string]*Folder) map[string]*Folder {
	if values == nil {
		return make(map[string]*Folder)
	}
	return values
}
func cloneProject(value *Project) *Project {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func cloneProjects(values map[string]*Project) map[string]*Project {
	result := make(map[string]*Project, len(values))
	for key, value := range values {
		result[key] = cloneProject(value)
	}
	return result
}
func cloneOrganizations(values map[string]*Organization) map[string]*Organization {
	result := make(map[string]*Organization, len(values))
	for key, value := range values {
		copy := *value
		result[key] = &copy
	}
	return result
}
func cloneFolders(values map[string]*Folder) map[string]*Folder {
	result := make(map[string]*Folder, len(values))
	for key, value := range values {
		copy := *value
		result[key] = &copy
	}
	return result
}
