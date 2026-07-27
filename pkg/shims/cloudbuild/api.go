package cloudbuild

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const cloudBuildExecutionTimeout = 15 * time.Minute

func init() {
	registry.Register("cloudbuild.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.SvcMgr, ctx.OpMgr)
	})
}

type cloudBuildBackend interface {
	ReconcileBuildResources(context.Context) error
	EnsureBuildWorkspace(context.Context, string, string) error
	RemoveBuildWorkspace(context.Context, string, string) error
	ProvisionBuildStep(context.Context, string, string, string, []string, []string, []string) error
	WaitBuildContainer(context.Context, string, string) (orchestrator.BuildContainerResult, error)
	StopAndRemoveBuildContainer(context.Context, string, string) error
}

type API struct {
	mu          sync.RWMutex
	mutationMu  sync.Mutex
	svcMgr      cloudBuildBackend
	opMgr       *orchestrator.OperationManager
	stateStore  cloudBuildStateStore
	degradedErr error
	builds      map[string]*Build
	triggers    map[string]*BuildTrigger
	buildIDs    map[string]struct{}
	randomID    func([]byte) (int, error)
	runAsync    func(string, func() error)
}

// NewAPI creates a Cloud Build shim with profile-scoped durable metadata.
func NewAPI(svcMgr *orchestrator.ServiceManager, opMgr *orchestrator.OperationManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	guarded := state.NewGuardedEntryStore(store, err)
	api, loadErr := newAPIWithStore(cloudBuildBackendFrom(svcMgr), opMgr, guarded, nil)
	if err != nil {
		log.Printf("[Shim: Cloud Build] persistence degraded: %v", err)
	}
	if loadErr != nil {
		log.Printf("[Shim: Cloud Build] state rehydration failed: %v", loadErr)
	}
	return api
}

func newAPI(svcMgr *orchestrator.ServiceManager, opMgr *orchestrator.OperationManager) *API {
	api, _ := newAPIWithStore(cloudBuildBackendFrom(svcMgr), opMgr, nil, nil)
	return api
}

func cloudBuildBackendFrom(svcMgr *orchestrator.ServiceManager) cloudBuildBackend {
	if svcMgr == nil {
		return nil
	}
	return svcMgr
}

func newAPIWithStore(
	svcMgr cloudBuildBackend,
	opMgr *orchestrator.OperationManager,
	store cloudBuildStateStore,
	reconcile func() error,
) (*API, error) {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	api := &API{
		svcMgr:     svcMgr,
		opMgr:      opMgr,
		stateStore: store,
		builds:     make(map[string]*Build),
		triggers:   make(map[string]*BuildTrigger),
		buildIDs:   make(map[string]struct{}),
		randomID:   rand.Read,
	}
	api.runAsync = opMgr.RunAsync
	if reconcile == nil && svcMgr != nil {
		reconcile = func() error {
			return svcMgr.ReconcileBuildResources(context.Background())
		}
	}
	if err := api.loadState(); err != nil {
		api.degradedErr = err
		return api, err
	}
	if reconcile != nil {
		if err := reconcile(); err != nil {
			log.Printf("[Shim: Cloud Build] reconcile owned build resources: %v", err)
		}
	}
	return api, nil
}

func (api *API) allocateBuildID(prefix string) (string, error) {
	const attempts = 16
	for range attempts {
		random := make([]byte, 16)
		n, err := api.randomID(random)
		if err != nil {
			return "", fmt.Errorf("generate build ID: %w", err)
		}
		if n != len(random) {
			return "", fmt.Errorf("generate build ID: short random read")
		}
		candidate := prefix + hex.EncodeToString(random)
		api.mu.Lock()
		if _, exists := api.buildIDs[candidate]; !exists {
			api.buildIDs[candidate] = struct{}{}
			api.mu.Unlock()
			return candidate, nil
		}
		api.mu.Unlock()
	}
	return "", fmt.Errorf("generate unique build ID after %d attempts", attempts)
}

type Build struct {
	Id           string  `json:"id,omitempty"`
	ProjectId    string  `json:"projectId,omitempty"`
	Status       string  `json:"status,omitempty"`
	StatusDetail string  `json:"statusDetail,omitempty"`
	Steps        []Step  `json:"steps,omitempty"`
	CreateTime   string  `json:"createTime,omitempty"`
	StartTime    string  `json:"startTime,omitempty"`
	FinishTime   string  `json:"finishTime,omitempty"`
	Source       *Source `json:"source,omitempty"`
}

type Source struct {
	RepoSource *RepoSource `json:"repoSource,omitempty"`
}

type RepoSource struct {
	RepoName   string `json:"repoName"` // e.g. "github.com/user/repo"
	BranchName string `json:"branchName,omitempty"`
}

type BuildTrigger struct {
	Id          string        `json:"id,omitempty"`
	Description string        `json:"description,omitempty"`
	Github      *GithubConfig `json:"github,omitempty"`
	Build       *Build        `json:"build,omitempty"`
}

type GithubConfig struct {
	Owner string      `json:"owner"`
	Name  string      `json:"name"`
	Push  *PushFilter `json:"push,omitempty"`
}

type PushFilter struct {
	Branch string `json:"branch,omitempty"`
}

type Step struct {
	Name string   `json:"name"`
	Args []string `json:"args,omitempty"`
	Env  []string `json:"env,omitempty"`
	Dir  string   `json:"dir,omitempty"`
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	log.Printf("[Shim: Cloud Build] %s %s", r.Method, path)

	if r.Method == "POST" && strings.HasSuffix(path, "/builds") {
		parts := strings.Split(path, "/")
		var project string
		for i, p := range parts {
			if p == "projects" && i+1 < len(parts) {
				project = parts[i+1]
				break
			}
		}
		if project == "" {
			project = "local-dev-project"
		}
		api.handleCreateBuild(w, r, project)
		return
	}

	if r.Method == "GET" && strings.HasSuffix(path, "/builds") {
		parts := strings.Split(path, "/")
		var project string
		for i, p := range parts {
			if p == "projects" && i+1 < len(parts) {
				project = parts[i+1]
				break
			}
		}
		if project == "" {
			project = "local-dev-project"
		}
		api.handleListBuilds(w, r, project)
		return
	}

	if r.Method == "GET" && strings.Contains(path, "/operations/") {
		api.handleGetOperation(w, path)
		return
	}

	if r.Method == "POST" && strings.HasSuffix(path, "/triggers") {
		writeUnimplemented(w, "Cloud Build triggers are not implemented")
		return
	}

	if r.Method == "POST" && strings.Contains(path, "/triggers/") && strings.HasSuffix(path, ":run") {
		writeUnimplemented(w, "Cloud Build trigger runs are not implemented")
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func writeUnimplemented(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    http.StatusNotImplemented,
			"message": message,
			"status":  "UNIMPLEMENTED",
		},
	})
}

func (api *API) handleCreateBuild(w http.ResponseWriter, r *http.Request, project string) {
	if err := api.persistenceError(); err != nil {
		writeUnavailable(w, "Cloud Build metadata persistence is unavailable")
		return
	}
	var build Build
	if err := json.NewDecoder(r.Body).Decode(&build); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	buildID, err := api.allocateBuildID("build-")
	if err != nil {
		http.Error(w, "failed to allocate build identity", http.StatusInternalServerError)
		return
	}
	build.Id = buildID
	build.ProjectId = project
	build.Status = "QUEUED"
	build.CreateTime = time.Now().UTC().Format(time.RFC3339)
	resourceID := fmt.Sprintf("projects/%s/builds/%s", project, build.Id)

	if err := api.commitBuild(resourceID, &build); err != nil {
		api.releaseBuildID(buildID)
		writeUnavailable(w, "Cloud Build metadata persistence is unavailable")
		return
	}

	op, err := api.opMgr.RegisterDurable("cloudbuild#operation", "CREATE", "/v1/"+resourceID, "", "")
	if err != nil {
		if rollbackErr := api.removeBuild(resourceID); rollbackErr != nil {
			log.Printf("[Shim: Cloud Build] rollback build after operation failure: %v", rollbackErr)
		} else {
			api.releaseBuildID(buildID)
		}
		writeUnavailable(w, "Cloud Build operation persistence is unavailable")
		return
	}
	api.opMgr.UpdateMetadata(op.Name, build)
	api.pushLog(project, "INFO", build.Id, fmt.Sprintf("Build %s queued with %d steps", build.Id, len(build.Steps)))

	api.runAsync(op.Name, func() error { return api.executeBuild(project, build, op.Name) })

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(api.opMgr.Get(op.Name))
}

func (api *API) handleListBuilds(w http.ResponseWriter, r *http.Request, project string) {
	api.mu.RLock()
	var builds []Build
	for _, build := range api.builds {
		if build != nil && build.ProjectId == project {
			builds = append(builds, *cloneBuild(build))
		}
	}
	api.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"builds": builds})
}

func (api *API) executeBuild(project string, build Build, opName string) error {
	build.Status = "WORKING"
	build.StartTime = time.Now().UTC().Format(time.RFC3339)
	resourceID := fmt.Sprintf("projects/%s/builds/%s", project, build.Id)
	if err := api.commitBuild(resourceID, &build); err != nil {
		return fmt.Errorf("persist working build: %w", err)
	}
	api.opMgr.UpdateMetadata(opName, build)

	// Workspace volume for sharing code between steps
	identity := cloudBuildDockerIdentity(resourceID)
	workspaceVol := identity + "-workspace"
	ctx, cancel := context.WithTimeout(context.Background(), cloudBuildExecutionTimeout)
	defer cancel()
	if api.svcMgr == nil {
		build.Status = "FAILURE"
		build.StatusDetail = "Cloud Build execution backend is unavailable"
		build.FinishTime = time.Now().UTC().Format(time.RFC3339)
		if err := api.commitBuild(resourceID, &build); err != nil {
			return fmt.Errorf("persist unavailable build backend: %w", err)
		}
		api.opMgr.UpdateMetadata(opName, build)
		return fmt.Errorf("Cloud Build execution backend is unavailable")
	}
	if err := api.svcMgr.EnsureBuildWorkspace(ctx, workspaceVol, resourceID); err != nil {
		build.Status = "FAILURE"
		build.StatusDetail = err.Error()
		build.FinishTime = time.Now().UTC().Format(time.RFC3339)
		if persistErr := api.commitBuild(resourceID, &build); persistErr != nil {
			return fmt.Errorf("prepare build workspace: %v; persist failure: %w", err, persistErr)
		}
		api.opMgr.UpdateMetadata(opName, build)
		return fmt.Errorf("prepare build workspace: %w", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := api.svcMgr.RemoveBuildWorkspace(cleanupCtx, workspaceVol, resourceID); err != nil {
			log.Printf("[Shim: Cloud Build] cleanup workspace %s: %v", workspaceVol, err)
		}
	}()

	failed := false
	failureDetail := ""
	executed := 0

	// Implicit step: Clone source if provided
	if build.Source != nil && build.Source.RepoSource != nil {
		repo := build.Source.RepoSource.RepoName
		if !strings.HasPrefix(repo, "http") {
			repo = "https://" + repo
		}
		branch := build.Source.RepoSource.BranchName
		if branch == "" {
			branch = "main"
		}

		api.pushLog(project, "INFO", build.Id, fmt.Sprintf("Cloning %s (branch: %s)...", repo, branch))

		cloneContainer := identity + "-clone"
		// We use a helper container to clone into a volume
		err := api.svcMgr.ProvisionBuildStep(ctx, cloneContainer, resourceID, "alpine/git:latest", []string{workspaceVol + ":/workspace"}, []string{}, []string{"clone", "-b", branch, repo, "/workspace"})
		if err != nil {
			api.pushLog(project, "ERROR", build.Id, fmt.Sprintf("Source clone failed: %v", err))
			failed = true
			failureDetail = fmt.Sprintf("source clone start failed: %v", err)
		} else {
			defer api.cleanupBuildContainer(cloneContainer, resourceID)
			executed++
			result, waitErr := api.svcMgr.WaitBuildContainer(ctx, cloneContainer, resourceID)
			if waitErr != nil {
				api.pushLog(project, "ERROR", build.Id, fmt.Sprintf("Source clone wait failed: %v", waitErr))
				failed = true
				failureDetail = fmt.Sprintf("source clone did not complete: %v", waitErr)
			} else if result.ExitCode != 0 {
				failed = true
				failureDetail = buildExitDetail("source clone", result)
				api.pushLog(project, "ERROR", build.Id, failureDetail)
			} else if strings.TrimSpace(result.Logs) != "" {
				api.pushLog(project, "INFO", build.Id, result.Logs)
			}
			if err := api.removeBuildContainer(cloneContainer, resourceID); err != nil {
				api.pushLog(project, "ERROR", build.Id, fmt.Sprintf("Source clone cleanup failed: %v", err))
				failed = true
				if failureDetail == "" {
					failureDetail = fmt.Sprintf("source clone cleanup failed: %v", err)
				}
			}
		}
	}

	if !failed {
		for i, step := range build.Steps {
			api.pushLog(project, "INFO", build.Id, fmt.Sprintf("Step #%d: %s %s", i, step.Name, strings.Join(step.Args, " ")))

			img := step.Name
			if !strings.Contains(img, "/") && !strings.Contains(img, ":") {
				img = img + ":latest"
			}

			if strings.HasPrefix(img, "gcr.io/cloud-builders/") {
				tool := strings.TrimPrefix(img, "gcr.io/cloud-builders/")
				if tool == "docker" {
					img = "docker:latest"
				}
			}

			containerName := fmt.Sprintf("%s-step-%d", identity, i)
			// Mount the workspace volume to all steps
			err := api.svcMgr.ProvisionBuildStep(ctx, containerName, resourceID, img, []string{workspaceVol + ":/workspace"}, step.Env, step.Args)
			if err != nil {
				api.pushLog(project, "ERROR", build.Id, fmt.Sprintf("Step #%d failed: %v", i, err))
				failed = true
				failureDetail = fmt.Sprintf("step #%d start failed: %v", i, err)
				break
			}
			defer api.cleanupBuildContainer(containerName, resourceID)
			executed++

			result, waitErr := api.svcMgr.WaitBuildContainer(ctx, containerName, resourceID)
			if waitErr != nil {
				api.pushLog(project, "ERROR", build.Id, fmt.Sprintf("Step #%d wait failed: %v", i, waitErr))
				failed = true
				failureDetail = fmt.Sprintf("step #%d did not complete: %v", i, waitErr)
			} else if result.ExitCode != 0 {
				failed = true
				failureDetail = buildExitDetail(fmt.Sprintf("step #%d", i), result)
				api.pushLog(project, "ERROR", build.Id, failureDetail)
			} else {
				if strings.TrimSpace(result.Logs) != "" {
					api.pushLog(project, "INFO", build.Id, result.Logs)
				}
				api.pushLog(project, "INFO", build.Id, fmt.Sprintf("Step #%d finished successfully", i))
			}
			if err := api.removeBuildContainer(containerName, resourceID); err != nil {
				api.pushLog(project, "ERROR", build.Id, fmt.Sprintf("Step #%d cleanup failed: %v", i, err))
				failed = true
				if failureDetail == "" {
					failureDetail = fmt.Sprintf("step #%d cleanup failed: %v", i, err)
				}
			}
			if failed {
				break
			}
		}
	}

	build.FinishTime = time.Now().UTC().Format(time.RFC3339)
	if !failed && executed == 0 {
		failed = true
		failureDetail = "build contained no executable steps"
	}
	if failed {
		build.Status = "FAILURE"
		build.StatusDetail = failureDetail
		if build.StatusDetail == "" {
			build.StatusDetail = "one or more build steps failed"
		}
		if err := api.commitBuild(resourceID, &build); err != nil {
			return fmt.Errorf("persist failed build: %w", err)
		}
		api.opMgr.UpdateMetadata(opName, build)
		return fmt.Errorf("build failed")
	}
	build.Status = "SUCCESS"
	build.StatusDetail = ""
	api.pushLog(project, "INFO", build.Id, "Build SUCCESS")
	if err := api.commitBuild(resourceID, &build); err != nil {
		return fmt.Errorf("persist successful build: %w", err)
	}
	api.opMgr.UpdateMetadata(opName, build)
	return nil
}

func cloudBuildDockerIdentity(resourceID string) string {
	sum := sha256.Sum256([]byte(config.GetProfile() + "\x00" + resourceID))
	return fmt.Sprintf("minisky-build-%x", sum[:10])
}

func (api *API) cleanupBuildContainer(name, resourceID string) {
	if err := api.removeBuildContainer(name, resourceID); err != nil {
		log.Printf("[Shim: Cloud Build] cleanup container %s: %v", name, err)
	}
}

func (api *API) removeBuildContainer(name, resourceID string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return api.svcMgr.StopAndRemoveBuildContainer(cleanupCtx, name, resourceID)
}

func buildExitDetail(action string, result orchestrator.BuildContainerResult) string {
	detail := fmt.Sprintf("%s failed with exit code %d", action, result.ExitCode)
	if logs := strings.TrimSpace(result.Logs); logs != "" {
		detail += ": " + logs
	}
	if result.LogsTruncated {
		detail += " [logs truncated]"
	}
	return detail
}

func (api *API) handleGetOperation(w http.ResponseWriter, path string) {
	name := path[strings.LastIndex(path, "/")+1:]
	operation := api.opMgr.Get(name)
	if operation == nil || operation.Kind != "cloudbuild#operation" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(operation)
}

func writeUnavailable(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    http.StatusServiceUnavailable,
			"message": message,
			"status":  "UNAVAILABLE",
		},
	})
}

func (api *API) releaseBuildID(id string) {
	api.mu.Lock()
	delete(api.buildIDs, id)
	api.mu.Unlock()
}

func (api *API) handleCreateTrigger(w http.ResponseWriter, r *http.Request, project string) {
	var trigger BuildTrigger
	json.NewDecoder(r.Body).Decode(&trigger)
	trigger.Id = fmt.Sprintf("trigger-%d", time.Now().Unix())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(trigger)
}

func (api *API) handleRunTrigger(w http.ResponseWriter, r *http.Request, project, triggerId string) {
	// In a real implementation, we'd look up the trigger by ID.
	// For the emulator, we just simulate starting a build from "GitHub"
	buildID, err := api.allocateBuildID("build-trigger-")
	if err != nil {
		http.Error(w, "failed to allocate build identity", http.StatusInternalServerError)
		return
	}
	build := Build{
		Id:         buildID,
		ProjectId:  project,
		Status:     "QUEUED",
		CreateTime: time.Now().UTC().Format(time.RFC3339),
		Source: &Source{
			RepoSource: &RepoSource{
				RepoName:   "github.com/GoogleCloudPlatform/cloud-builders",
				BranchName: "master",
			},
		},
		Steps: []Step{
			{Name: "ubuntu", Args: []string{"echo", "Triggered from GitHub!"}},
		},
	}

	op, err := api.opMgr.RegisterDurable("cloudbuild#operation", "RUN_TRIGGER", fmt.Sprintf("/v1/projects/%s/builds/%s", project, build.Id), "", "")
	if err != nil {
		http.Error(w, "failed to persist build operation", http.StatusInternalServerError)
		return
	}
	api.opMgr.UpdateMetadata(op.Name, build)
	api.runAsync(op.Name, func() error { return api.executeBuild(project, build, op.Name) })

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(op)
}

func (api *API) Proxy() *httputil.ReverseProxy {
	return nil // Not used in this implementation style
}

func (api *API) pushLog(project, severity, id, msg string) {
	log.Printf("[%s] BUILD %s: %s", severity, id, msg)
}
