package artifactregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/observability"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const (
	defaultRegistryURL         = "http://127.0.0.1:5000"
	artifactRegistryStateEntry = "artifactregistry/metadata"
	// Keep a bounded terminal outcome history for provider polling. Outcomes
	// for operations that are still pending are always retained, even if an
	// unusually large pending set temporarily exceeds this limit.
	maxPersistedOperationOutcomes = 128
)

func init() {
	state.MustRegisterEntryValidator(artifactRegistryStateEntry, state.StrictEntryValidator[artifactRegistryMetadata](nil))
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
	response, err := observability.Do(index.client, request)
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
	mu                   sync.RWMutex
	mutationMu           sync.Mutex
	lifecycleGate        chan struct{}
	svcMgr               *orchestrator.ServiceManager
	opMgr                *orchestrator.OperationManager
	store                stateStore
	repos                map[string]*Repository
	outcomes             map[string]artifactOperationOutcome
	nextOutcomeSequence  uint64
	index                RegistryIndex
	initErr              error
	observerSubscription *orchestrator.TerminalObserverSubscription
	closed               bool

	afterOperationRegistration func(*orchestrator.Operation) error
	operationRunner            func(string)
	compactionHook             func()
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type artifactRegistryMetadata struct {
	Repositories        map[string]*Repository              `json:"repositories"`
	Outcomes            map[string]artifactOperationOutcome `json:"operationOutcomes,omitempty"`
	NextOutcomeSequence uint64                              `json:"nextOutcomeSequence,omitempty"`
}

type artifactOperationOutcome struct {
	OperationType string      `json:"operationType"`
	Target        string      `json:"target"`
	Repository    *Repository `json:"repository,omitempty"`
	Sequence      uint64      `json:"sequence"`
}

func (api *API) PersistenceError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.initErr
}

func configuredRegistryIndex(sm *orchestrator.ServiceManager) RegistryIndex {
	registryURL := os.Getenv("MINISKY_ARTIFACT_REGISTRY_URL")
	if registryURL != "" {
		client := &http.Client{Timeout: 5 * time.Second}
		return NewDockerRegistryIndex(client, registryURL)
	} else if sm == nil {
		return NewDockerRegistryIndex(&http.Client{Timeout: 5 * time.Second}, defaultRegistryURL)
	}
	return nil
}

func NewAPI(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager) *API {
	index := configuredRegistryIndex(sm)
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Artifact Registry] state disabled: %v", err)
		api := newAPI(opMgr, sm, index, nil)
		if observeErr := api.observeTerminalOperations(context.Background()); observeErr != nil {
			api.initErr = observeErr
		}
		return api
	}
	api, err := NewAPIWithRegistryIndexAndStore(opMgr, sm, index, store)
	if err != nil {
		log.Printf("[Shim: Artifact Registry] state rehydration failed: %v", err)
		api = newAPI(opMgr, sm, index, store)
		api.initErr = err
	}
	return api
}

// NewAPIWithRegistryIndex allows callers to inject a registry index.
func NewAPIWithRegistryIndex(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, index RegistryIndex) *API {
	api := newAPI(opMgr, sm, index, nil)
	if err := api.observeTerminalOperations(context.Background()); err != nil {
		api.initErr = err
	}
	return api
}

// NewAPIWithRegistryIndexAndStore constructs an Artifact Registry shim backed
// by the supplied profile metadata store. Registry v2 package and version data
// remains exclusively in the injected index and is never rehydrated from state;
// manifest and blob content is not part of this metadata snapshot.
func NewAPIWithRegistryIndexAndStore(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, index RegistryIndex, store stateStore) (*API, error) {
	api := newAPI(opMgr, sm, index, store)
	if store == nil {
		if err := api.observeTerminalOperations(context.Background()); err != nil {
			return nil, err
		}
		return api, nil
	}
	var persisted artifactRegistryMetadata
	if err := store.Load(artifactRegistryStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			if observeErr := api.observeTerminalOperations(context.Background()); observeErr != nil {
				return nil, observeErr
			}
			return api, nil
		}
		return nil, fmt.Errorf("load Artifact Registry metadata: %w", err)
	}
	normalizeArtifactRegistryMetadata(&persisted)
	for name, repository := range persisted.Repositories {
		if repository == nil {
			return nil, fmt.Errorf("load Artifact Registry metadata: repository %q is null", name)
		}
		api.repos[name] = cloneRepository(repository)
	}
	for name, outcome := range persisted.Outcomes {
		outcome.Repository = cloneRepository(outcome.Repository)
		api.outcomes[name] = outcome
	}
	api.nextOutcomeSequence = persisted.NextOutcomeSequence
	if err := api.compactOperationOutcomes(); err != nil {
		return nil, fmt.Errorf("compact Artifact Registry operation outcomes: %w", err)
	}
	if err := api.observeTerminalOperations(context.Background()); err != nil {
		return nil, err
	}
	return api, nil
}

func newAPI(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, index RegistryIndex, store stateStore) *API {
	api := &API{
		opMgr:         opMgr,
		svcMgr:        sm,
		store:         store,
		repos:         make(map[string]*Repository),
		outcomes:      make(map[string]artifactOperationOutcome),
		index:         index,
		lifecycleGate: make(chan struct{}, 1),
	}
	api.lifecycleGate <- struct{}{}
	if opMgr != nil {
		api.operationRunner = func(name string) {
			opMgr.RunAsync(name, func() error { return nil })
		}
	}
	return api
}

func (api *API) observeTerminalOperations(ctx context.Context) error {
	if err := api.acquireLifecycle(ctx); err != nil {
		return err
	}
	defer api.releaseLifecycle()
	if api.opMgr == nil {
		return nil
	}
	api.mu.Lock()
	previous := api.observerSubscription
	closed := api.closed
	api.mu.Unlock()
	if previous != nil {
		if err := previous.Shutdown(ctx); err != nil {
			return err
		}
		api.mu.Lock()
		if api.observerSubscription == previous {
			api.observerSubscription = nil
		}
		closed = api.closed
		api.mu.Unlock()
	}
	if closed {
		return nil
	}
	subscription := api.opMgr.ObserveTerminal(func(*orchestrator.Operation) {
		if err := api.compactOperationOutcomes(); err != nil {
			log.Printf("[Shim: Artifact Registry] outcome compaction failed: %v", err)
		}
	})
	api.mu.Lock()
	if api.closed {
		api.mu.Unlock()
		return subscription.Shutdown(ctx)
	}
	api.observerSubscription = subscription
	api.mu.Unlock()
	return nil
}

// Shutdown releases the terminal observer registered with the shared operation
// manager. The bootstrap plugin shutdown path calls this method.
func (api *API) Shutdown(ctx context.Context) error {
	if err := api.acquireLifecycle(ctx); err != nil {
		return err
	}
	defer api.releaseLifecycle()
	api.mu.Lock()
	api.closed = true
	subscription := api.observerSubscription
	api.mu.Unlock()
	if subscription == nil {
		return nil
	}
	if err := subscription.Shutdown(ctx); err != nil {
		return err
	}
	api.mu.Lock()
	if api.observerSubscription == subscription {
		api.observerSubscription = nil
	}
	api.mu.Unlock()
	return nil
}

func (api *API) acquireLifecycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-api.lifecycleGate:
		if err := ctx.Err(); err != nil {
			api.releaseLifecycle()
			return err
		}
		return nil
	}
}

func (api *API) releaseLifecycle() {
	api.lifecycleGate <- struct{}{}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if api.initializationError() != nil {
		writeError(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION", "Artifact Registry state is unavailable")
		return
	}
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
	api.mu.RLock()
	outcome, hasOutcome := api.outcomes[operation.Name]
	outcome.Repository = cloneRepository(outcome.Repository)
	api.mu.RUnlock()
	response := map[string]any{
		"name": serviceName,
		"done": operation.Done,
		"metadata": map[string]any{
			"target": operation.TargetLink,
			"verb":   operation.OperationType,
		},
	}
	if operation.Error != nil && !(operation.Done && hasOutcome) {
		response["error"] = operation.Error
	} else if operation.Done {
		if hasOutcome && outcome.OperationType == "DELETE" {
			response["response"] = map[string]any{}
		} else if hasOutcome && outcome.Repository != nil {
			response["response"] = outcome.Repository
		} else if operation.OperationType == "DELETE" {
			response["response"] = map[string]any{}
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

	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()

	api.mu.RLock()
	if _, exists := api.repos[repo.Name]; exists {
		api.mu.RUnlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "repository already exists: "+repo.Name)
		return
	}
	previous := api.snapshotLocked()
	api.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if api.opMgr == nil {
		committed := cloneMetadata(previous)
		committed.Repositories[repo.Name] = cloneRepository(&repo)
		if err := api.persistSnapshot(committed); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository metadata")
			return
		}
		api.replaceMetadata(committed)
		_ = json.NewEncoder(w).Encode(repo)
		return
	}
	op, err := api.opMgr.RegisterDurable("artifactregistry#operation", "CREATE", repo.Name, "", location)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository operation")
		return
	}
	if api.afterOperationRegistration != nil {
		if err := api.afterOperationRegistration(op); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "repository operation interrupted before metadata commit")
			return
		}
	}
	committed := cloneMetadata(previous)
	committed.Repositories[repo.Name] = cloneRepository(&repo)
	api.addOperationOutcome(&committed, op.Name, artifactOperationOutcome{
		OperationType: "CREATE",
		Target:        repo.Name,
		Repository:    cloneRepository(&repo),
	})
	if err := api.persistRegisteredMutation(committed, previous, op.Name); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository operation outcome")
		return
	}
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)
	api.writeOperation(w, serviceName, op)
	if api.operationRunner != nil {
		api.operationRunner(op.Name)
	}
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

	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()

	api.mu.RLock()
	repo := cloneRepository(api.repos[name])
	if repo == nil {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "repository not found: "+name)
		return
	}
	previous := api.snapshotLocked()
	api.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if api.opMgr == nil {
		committed := cloneMetadata(previous)
		delete(committed.Repositories, name)
		if err := api.persistSnapshot(committed); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository deletion")
			return
		}
		api.replaceMetadata(committed)
		_ = json.NewEncoder(w).Encode(map[string]any{})
		return
	}
	location := locationFromResource(repo.Name)
	op, err := api.opMgr.RegisterDurable("artifactregistry#operation", "DELETE", repo.Name, "", location)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository operation")
		return
	}
	if api.afterOperationRegistration != nil {
		if err := api.afterOperationRegistration(op); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "repository operation interrupted before metadata commit")
			return
		}
	}
	committed := cloneMetadata(previous)
	delete(committed.Repositories, name)
	api.addOperationOutcome(&committed, op.Name, artifactOperationOutcome{
		OperationType: "DELETE",
		Target:        repo.Name,
	})
	if err := api.persistRegisteredMutation(committed, previous, op.Name); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist repository operation outcome")
		return
	}
	project := projectFromResource(repo.Name)
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name)
	api.writeOperation(w, serviceName, op)
	if api.operationRunner != nil {
		api.operationRunner(op.Name)
	}
}

func (api *API) snapshotLocked() artifactRegistryMetadata {
	snapshot := artifactRegistryMetadata{
		Repositories:        make(map[string]*Repository, len(api.repos)),
		Outcomes:            make(map[string]artifactOperationOutcome, len(api.outcomes)),
		NextOutcomeSequence: api.nextOutcomeSequence,
	}
	for name, repository := range api.repos {
		snapshot.Repositories[name] = cloneRepository(repository)
	}
	for name, outcome := range api.outcomes {
		outcome.Repository = cloneRepository(outcome.Repository)
		snapshot.Outcomes[name] = outcome
	}
	return snapshot
}

func cloneMetadata(metadata artifactRegistryMetadata) artifactRegistryMetadata {
	clone := artifactRegistryMetadata{
		Repositories:        make(map[string]*Repository, len(metadata.Repositories)),
		Outcomes:            make(map[string]artifactOperationOutcome, len(metadata.Outcomes)),
		NextOutcomeSequence: metadata.NextOutcomeSequence,
	}
	for name, repository := range metadata.Repositories {
		clone.Repositories[name] = cloneRepository(repository)
	}
	for name, outcome := range metadata.Outcomes {
		outcome.Repository = cloneRepository(outcome.Repository)
		clone.Outcomes[name] = outcome
	}
	return clone
}

func (api *API) persistSnapshot(snapshot artifactRegistryMetadata) error {
	if api.store == nil {
		return nil
	}
	if err := api.store.Save(artifactRegistryStateEntry, snapshot); err != nil {
		return fmt.Errorf("save Artifact Registry metadata: %w", err)
	}
	return nil
}

func (api *API) replaceMetadata(snapshot artifactRegistryMetadata) {
	api.mu.Lock()
	clone := cloneMetadata(snapshot)
	api.repos = clone.Repositories
	api.outcomes = clone.Outcomes
	api.nextOutcomeSequence = clone.NextOutcomeSequence
	api.mu.Unlock()
}

func (api *API) addOperationOutcome(metadata *artifactRegistryMetadata, operationName string, outcome artifactOperationOutcome) {
	metadata.NextOutcomeSequence++
	outcome.Sequence = metadata.NextOutcomeSequence
	metadata.Outcomes[operationName] = outcome
}

func (api *API) operationOutcomeEvictions(metadata artifactRegistryMetadata) []string {
	if api.opMgr == nil {
		return nil
	}
	operations := make(map[string]*orchestrator.Operation)
	for _, operation := range api.opMgr.List() {
		if operation != nil && operation.Kind == "artifactregistry#operation" {
			operations[operation.Name] = operation
		}
	}
	evictions := make([]string, 0)
	pending := make(map[string]bool)
	type candidate struct {
		name     string
		sequence uint64
	}
	terminal := make([]candidate, 0, len(metadata.Outcomes))
	for name, outcome := range metadata.Outcomes {
		operation := operations[name]
		if operation == nil {
			evictions = append(evictions, name)
			continue
		}
		if !operation.Done {
			pending[name] = true
			continue
		}
		terminal = append(terminal, candidate{name: name, sequence: outcome.Sequence})
	}
	sort.Slice(terminal, func(i, j int) bool {
		if terminal[i].sequence == terminal[j].sequence {
			return terminal[i].name > terminal[j].name
		}
		return terminal[i].sequence > terminal[j].sequence
	})
	keepTerminal := maxPersistedOperationOutcomes - len(pending)
	if keepTerminal < 0 {
		keepTerminal = 0
	}
	for index := keepTerminal; index < len(terminal); index++ {
		evictions = append(evictions, terminal[index].name)
	}
	return evictions
}

func (api *API) compactOperationOutcomes() error {
	if api.compactionHook != nil {
		api.compactionHook()
	}
	if api.opMgr == nil {
		return nil
	}
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()

	api.mu.RLock()
	snapshot := api.snapshotLocked()
	api.mu.RUnlock()
	evictions := api.operationOutcomeEvictions(snapshot)
	if len(evictions) == 0 {
		return nil
	}
	for _, name := range evictions {
		if err := api.opMgr.RemoveDurable(name); err != nil {
			return fmt.Errorf("expire operation %q: %w", name, err)
		}
		delete(snapshot.Outcomes, name)
	}
	if api.store == nil {
		api.replaceMetadata(snapshot)
		return nil
	}
	if err := api.persistSnapshot(snapshot); err != nil {
		var observed artifactRegistryMetadata
		if loadErr := api.store.Load(artifactRegistryStateEntry, &observed); loadErr == nil {
			normalizeArtifactRegistryMetadata(&observed)
			if artifactRegistryMetadataEqual(observed, snapshot) {
				api.replaceMetadata(snapshot)
				return nil
			}
		}
		api.degrade(err)
		return err
	}
	api.replaceMetadata(snapshot)
	return nil
}

func (api *API) persistRegisteredMutation(committed, previous artifactRegistryMetadata, operationName string) error {
	saveErr := api.persistSnapshot(committed)
	if saveErr == nil {
		api.replaceMetadata(committed)
		return nil
	}

	var observed artifactRegistryMetadata
	loadErr := api.store.Load(artifactRegistryStateEntry, &observed)
	if loadErr == nil {
		normalizeArtifactRegistryMetadata(&observed)
		switch {
		case artifactRegistryMetadataEqual(observed, committed):
			api.replaceMetadata(committed)
			return nil
		case artifactRegistryMetadataEqual(observed, previous):
			return api.failRegisteredOperation(operationName, saveErr)
		default:
			api.replaceMetadata(observed)
			ambiguous := fmt.Errorf("Artifact Registry metadata save returned an error and readback differed from both snapshots: %w", saveErr)
			api.degrade(ambiguous)
			return ambiguous
		}
	}
	if errors.Is(loadErr, state.ErrNotFound) && artifactRegistryMetadataEmpty(previous) {
		return api.failRegisteredOperation(operationName, saveErr)
	}
	ambiguous := errors.Join(saveErr, fmt.Errorf("read back Artifact Registry metadata: %w", loadErr))
	api.degrade(ambiguous)
	return ambiguous
}

func (api *API) failRegisteredOperation(operationName string, saveErr error) error {
	if api.opMgr == nil {
		return saveErr
	}
	if failErr := api.opMgr.FailDurable(operationName, http.StatusInternalServerError,
		"repository metadata and operation outcome were not persisted"); failErr != nil {
		combined := errors.Join(saveErr, failErr)
		api.degrade(combined)
		return combined
	}
	return saveErr
}

func normalizeArtifactRegistryMetadata(metadata *artifactRegistryMetadata) {
	if metadata.Repositories == nil {
		metadata.Repositories = make(map[string]*Repository)
	}
	if metadata.Outcomes == nil {
		metadata.Outcomes = make(map[string]artifactOperationOutcome)
	}
	for _, outcome := range metadata.Outcomes {
		if outcome.Sequence > metadata.NextOutcomeSequence {
			metadata.NextOutcomeSequence = outcome.Sequence
		}
	}
}

func artifactRegistryMetadataEqual(left, right artifactRegistryMetadata) bool {
	leftPayload, leftErr := json.Marshal(left)
	rightPayload, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftPayload, rightPayload)
}

func artifactRegistryMetadataEmpty(metadata artifactRegistryMetadata) bool {
	return len(metadata.Repositories) == 0 &&
		len(metadata.Outcomes) == 0 &&
		metadata.NextOutcomeSequence == 0
}

func (api *API) initializationError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.initErr
}

func (api *API) degrade(err error) {
	api.mu.Lock()
	api.initErr = fmt.Errorf("Artifact Registry persistence is degraded: %w", err)
	api.mu.Unlock()
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
