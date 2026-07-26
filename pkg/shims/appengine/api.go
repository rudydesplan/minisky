package appengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/logging"
	"minisky/pkg/shims/serverless"
	"minisky/pkg/state"
)

const appEngineStateEntry = "appengine/metadata"

var (
	singletonAPI *API
	once         sync.Once
)

func init() {
	state.MustRegisterEntryValidator(appEngineStateEntry, state.StrictEntryValidator[appEngineMetadata](nil))
	f := func(ctx *registry.Context) http.Handler {
		once.Do(func() {
			// App Engine needs access to the Serverless backend for Buildpacks
			var serverlessAPI *serverless.API
			if s, ok := ctx.GetShim("cloudfunctions.googleapis.com").(*serverless.API); ok {
				serverlessAPI = s
			}
			// App Engine emits structured logs into Cloud Logging
			var logAPI *logging.API
			if l, ok := ctx.GetShim("logging.googleapis.com").(*logging.API); ok {
				logAPI = l
			}
			singletonAPI = NewAPI(ctx.OpMgr, ctx.SvcMgr, serverlessAPI, logAPI)
		})
		return singletonAPI
	}
	registry.Register("appengine.googleapis.com", f)
}

// AppEngine Resources
type App struct {
	Id              string `json:"id"`
	LocationId      string `json:"locationId"`
	DefaultHostname string `json:"defaultHostname"`
}

type Service struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type Version struct {
	Id           string            `json:"id"`
	Name         string            `json:"name"`
	Runtime      string            `json:"runtime"`
	State        string            `json:"servingStatus"` // SERVING, STOPPED
	Deployment   *Deployment       `json:"deployment,omitempty"`
	Entrypoint   *Entrypoint       `json:"entrypoint,omitempty"`
	EnvVariables map[string]string `json:"envVariables,omitempty"`
	CreateTime   string            `json:"createTime"`
}

type Deployment struct {
	Files map[string]File `json:"files,omitempty"`
}

type File struct {
	SourceUrl string `json:"sourceUrl"`
}

type Entrypoint struct {
	Shell string `json:"shell"`
}

type API struct {
	mu         sync.RWMutex
	mutationMu sync.Mutex
	opMgr      *orchestrator.OperationManager
	svcMgr     appEngineServiceManager
	serverless *serverless.API
	logAPI     *logging.API
	store      appEngineStateStore
	initErr    error
	apps       map[string]*App
	services   map[string]map[string]*Service            // appId -> serviceId -> Service
	versions   map[string]map[string]map[string]*Version // appId -> serviceId -> versionId -> Version
	deletions  map[string]appEngineDeletion

	operationRunner func(string, func() error)
	afterAdmission  func()
}

type appEngineStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type appEngineServiceManager interface {
	ProvisionServerlessVM(orchestrator.ServerlessIdentity, string, []string) (string, error)
	DeleteServerlessVM(orchestrator.ServerlessIdentity) error
}

type appEngineMetadata struct {
	Apps      map[string]*App                           `json:"apps"`
	Services  map[string]map[string]*Service            `json:"services"`
	Versions  map[string]map[string]map[string]*Version `json:"versions"`
	Deletions map[string]appEngineDeletion              `json:"deletions,omitempty"`
}

type appEngineDeletion struct {
	AppID     string `json:"appId"`
	ServiceID string `json:"serviceId"`
	VersionID string `json:"versionId"`
}

func NewAPI(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, serverless *serverless.API, logAPI *logging.API) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: AppEngine] persistence degraded: %v", err)
		api := newAPI(opMgr, sm, serverless, logAPI, nil)
		api.initErr = fmt.Errorf("open App Engine state: %w", err)
		return api
	}
	api, err := NewAPIWithStore(opMgr, sm, serverless, logAPI, store)
	if err != nil {
		log.Printf("[Shim: AppEngine] state rehydration failed: %v", err)
		api = newAPI(opMgr, sm, serverless, logAPI, store)
		api.initErr = err
	}
	return api
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, sm appEngineServiceManager, serverlessAPI *serverless.API, logAPI *logging.API, store appEngineStateStore) (*API, error) {
	api := newAPI(opMgr, sm, serverlessAPI, logAPI, store)
	if store == nil {
		return api, nil
	}
	var persisted appEngineMetadata
	if err := store.Load(appEngineStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load App Engine metadata: %w", err)
	}
	previous := cloneAppEngineMetadata(persisted)
	if err := normalizeAppEngineMetadata(&persisted); err != nil {
		return nil, fmt.Errorf("load App Engine metadata: %w", err)
	}
	if !appEngineMetadataEqual(previous, persisted) {
		if err := api.commitMetadata(previous, persisted); err != nil {
			return nil, fmt.Errorf("persist App Engine restart normalization: %w", err)
		}
	} else {
		api.replaceMetadata(persisted)
	}
	return api, nil
}

func newAPI(opMgr *orchestrator.OperationManager, sm appEngineServiceManager, serverlessAPI *serverless.API, logAPI *logging.API, store appEngineStateStore) *API {
	api := &API{
		opMgr:      opMgr,
		svcMgr:     sm,
		serverless: serverlessAPI,
		logAPI:     logAPI,
		store:      store,
		apps:       make(map[string]*App),
		services:   make(map[string]map[string]*Service),
		versions:   make(map[string]map[string]map[string]*Version),
		deletions:  make(map[string]appEngineDeletion),
	}
	if opMgr != nil {
		api.operationRunner = opMgr.RunAsync
	}
	return api
}

// pushLog emits a structured log entry to Cloud Logging (no-op if logAPI is nil)
func (api *API) pushLog(projectId, severity, service, text string) {
	if api.logAPI == nil {
		return
	}
	api.logAPI.PushLog(projectId, severity, "gae_app", service, text)
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: AppEngine] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initializationError() != nil {
		writeAppEngineError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "App Engine persistence is unavailable")
		return
	}
	if api.afterAdmission != nil && r.Method != http.MethodGet && r.Method != http.MethodHead {
		api.afterAdmission()
	}

	path := r.URL.Path
	// Mock App Engine v1 API
	// projects/{projectId}/apps
	// projects/{projectId}/apps/{appId}/services
	// projects/{projectId}/apps/{appId}/services/{serviceId}/versions

	switch {
	case strings.HasSuffix(path, "/apps"):
		api.handleApps(w, r)
	case strings.Contains(path, "/services"):
		if strings.Contains(path, "/versions") {
			api.handleVersions(w, r)
		} else {
			api.handleServices(w, r)
		}
	case strings.Contains(path, "/operations/"):
		api.handleOperations(w, r)
	case strings.Contains(path, "/deploy"): // MiniSky Direct Deploy extension
		api.handleDirectDeploy(w, r)
	default:
		writeAppEngineError(w, http.StatusNotFound, "NOT_FOUND", "Resource not found")
	}
}

func (api *API) handleApps(w http.ResponseWriter, r *http.Request) {
	project := extractSegmentAfter(r.URL.Path, "projects")
	if r.Method == http.MethodGet {
		api.mu.RLock()
		app := cloneApp(api.apps[project])
		api.mu.RUnlock()
		if app == nil {
			writeAppEngineError(w, http.StatusNotFound, "NOT_FOUND", "App not found")
			return
		}
		_ = json.NewEncoder(w).Encode(app)
		return
	}
	writeAppEngineError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
}

func (api *API) handleServices(w http.ResponseWriter, r *http.Request) {
	appId := extractSegmentAfter(r.URL.Path, "apps")
	if r.Method == http.MethodGet {
		api.mu.RLock()
		svcs := api.services[appId]
		items := []*Service{}
		for _, s := range svcs {
			items = append(items, cloneService(s))
		}
		api.mu.RUnlock()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"services": items})
		return
	}
	writeAppEngineError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
}

func (api *API) handleVersions(w http.ResponseWriter, r *http.Request) {
	appId := extractSegmentAfter(r.URL.Path, "apps")
	serviceId := extractSegmentAfter(r.URL.Path, "services")
	versionId := extractSegmentAfter(r.URL.Path, "versions")

	switch r.Method {
	case http.MethodGet:
		if versionId != "" {
			api.mu.RLock()
			v := cloneVersion(nestedVersion(api.versions, appId, serviceId, versionId))
			api.mu.RUnlock()
			if v == nil {
				writeAppEngineError(w, http.StatusNotFound, "NOT_FOUND", "Version not found")
				return
			}
			_ = json.NewEncoder(w).Encode(v)
		} else {
			api.mu.RLock()
			items := []*Version{}
			if services := api.versions[appId]; services != nil {
				for _, v := range services[serviceId] {
					items = append(items, cloneVersion(v))
				}
			}
			api.mu.RUnlock()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"versions": items})
		}
	case http.MethodDelete:
		api.mutationMu.Lock()
		defer api.mutationMu.Unlock()
		if api.rejectDegradedMutation(w) {
			return
		}
		api.mu.RLock()
		version := cloneVersion(nestedVersion(api.versions, appId, serviceId, versionId))
		previous := api.snapshotLocked()
		api.mu.RUnlock()
		if version == nil {
			writeAppEngineError(w, http.StatusNotFound, "NOT_FOUND", "Version not found")
			return
		}
		key := appEngineVersionKey(appId, serviceId, versionId)
		intent := cloneAppEngineMetadata(previous)
		intent.Deletions[key] = appEngineDeletion{AppID: appId, ServiceID: serviceId, VersionID: versionId}
		if api.rejectDegradedMutation(w) {
			return
		}
		if err := api.commitMetadata(previous, intent); err != nil {
			writeAppEngineError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist App Engine deletion intent")
			return
		}

		identity := appEngineIdentity(appId, serviceId, versionId)
		if api.rejectDegradedMutation(w) {
			return
		}
		if api.svcMgr != nil {
			if err := api.svcMgr.DeleteServerlessVM(identity); err != nil {
				writeAppEngineError(w, http.StatusBadGateway, "BAD_GATEWAY", "failed to delete App Engine backend")
				return
			}
		}
		finalized := cloneAppEngineMetadata(intent)
		delete(finalized.Versions[appId][serviceId], versionId)
		delete(finalized.Deletions, key)
		if api.rejectDegradedMutation(w) {
			return
		}
		if err := api.commitMetadata(intent, finalized); err != nil {
			writeAppEngineError(w, http.StatusInternalServerError, "INTERNAL", "failed to finalize App Engine version deletion")
			return
		}
		api.pushLog(appId, "INFO", serviceId, fmt.Sprintf("Deleted version %s", versionId))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"done": true})
	default:
		writeAppEngineError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

// handleDirectDeploy is a MiniSky-specific extension for the Dashboard
func (api *API) handleDirectDeploy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Project    string `json:"project"`
		Service    string `json:"service"`
		Version    string `json:"version"`
		Runtime    string `json:"runtime"`
		Code       string `json:"code"`
		Entrypoint string `json:"entrypoint"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAppEngineError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid deployment JSON")
		return
	}
	if req.Project == "" {
		writeAppEngineError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}

	if req.Service == "" {
		req.Service = "default"
	}
	if req.Version == "" {
		req.Version = fmt.Sprintf("v-%d", time.Now().Unix())
	}

	fullName := fmt.Sprintf("apps/%s/services/%s/versions/%s", req.Project, req.Service, req.Version)
	api.mutationMu.Lock()
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		return
	}
	op, err := api.opMgr.RegisterDurable("appengine#operation", "CREATE", fullName, "", "us-central1")
	if err != nil {
		api.mutationMu.Unlock()
		writeAppEngineError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist App Engine operation")
		return
	}

	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	snapshot := cloneAppEngineMetadata(previous)
	ensureAppEngineMaps(&snapshot, req.Project, req.Service)
	snapshot.Apps[req.Project] = &App{Id: req.Project, LocationId: "us-central1", DefaultHostname: req.Project + ".appspot.com"}
	snapshot.Services[req.Project][req.Service] = &Service{Id: req.Service, Name: "apps/" + req.Project + "/services/" + req.Service}
	snapshot.Versions[req.Project][req.Service][req.Version] = &Version{
		Id:         req.Version,
		Name:       fullName,
		Runtime:    req.Runtime,
		State:      "STOPPED",
		CreateTime: time.Now().Format(time.RFC3339),
	}
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "App Engine persistence became unavailable")
		return
	}
	if err := api.commitMetadata(previous, snapshot); err != nil {
		api.mutationMu.Unlock()
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "App Engine metadata was not persisted")
		writeAppEngineError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist App Engine version metadata")
		return
	}
	api.mutationMu.Unlock()
	api.pushLog(req.Project, "INFO", req.Service, fmt.Sprintf("Starting deployment of version %s (runtime: %s)", req.Version, req.Runtime))

	work := func() error {
		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("App Engine persistence unavailable before deployment: %w", err)
		}
		// Leverage Serverless Backend
		if api.serverless == nil || api.svcMgr == nil {
			return fmt.Errorf("serverless backend not initialized")
		}
		backend := api.serverless.GetBackend()

		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("App Engine persistence unavailable before source staging: %w", err)
		}
		tmpDir, err := os.MkdirTemp("", "minisky-gae-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)

		fileName := "main.py"
		if strings.HasPrefix(req.Runtime, "node") {
			fileName = "index.js"
		}
		if strings.HasPrefix(req.Runtime, "go") {
			fileName = "main.go"
		}

		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("App Engine persistence unavailable before source write: %w", err)
		}
		if err := os.WriteFile(tmpDir+"/"+fileName, []byte(req.Code), 0o600); err != nil {
			return fmt.Errorf("write App Engine source: %w", err)
		}

		identity := appEngineIdentity(req.Project, req.Service, req.Version)
		if backend == nil {
			return fmt.Errorf("serverless build backend not initialized")
		}
		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("App Engine persistence unavailable before build: %w", err)
		}
		image, err := backend.BuildFunction(identity, tmpDir, req.Entrypoint)
		if err != nil {
			return err
		}

		// Provision as a Serverless VM
		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("App Engine persistence unavailable before provision: %w", err)
		}
		_, err = api.svcMgr.ProvisionServerlessVM(identity, image, []string{"PORT=8080", "GAE_SERVICE=" + req.Service, "GAE_VERSION=" + req.Version})
		if err != nil {
			api.pushLog(req.Project, "ERROR", req.Service, fmt.Sprintf("Deployment failed for version %s: %v", req.Version, err))
			return err
		}
		if err := api.setVersionState(req.Project, req.Service, req.Version, "SERVING"); err != nil {
			return err
		}
		api.pushLog(req.Project, "INFO", req.Service, fmt.Sprintf("Version %s deployed successfully", req.Version))
		return nil
	}
	if api.operationRunner != nil {
		api.operationRunner(op.Name, work)
	}

	_ = json.NewEncoder(w).Encode(op)
}

func (api *API) handleOperations(w http.ResponseWriter, r *http.Request) {
	project := extractSegmentAfter(r.URL.Path, "projects")
	opName := extractSegmentAfter(r.URL.Path, "operations")
	op := api.opMgr.Get(opName)
	if op == nil || op.Kind != "appengine#operation" ||
		!strings.HasPrefix(op.TargetLink, "apps/"+project+"/") {
		writeAppEngineError(w, http.StatusNotFound, "NOT_FOUND", "Operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(op)
}

func (api *API) setVersionState(appID, serviceID, versionID, servingStatus string) error {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("App Engine persistence unavailable: %w", err)
	}
	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	snapshot := cloneAppEngineMetadata(previous)
	version := nestedVersion(snapshot.Versions, appID, serviceID, versionID)
	if version == nil {
		return fmt.Errorf("App Engine version disappeared during deployment")
	}
	version.State = servingStatus
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("App Engine persistence unavailable before outcome save: %w", err)
	}
	saveFailed, err := api.commitMetadataOutcome(previous, snapshot)
	if saveFailed {
		degradation := err
		if degradation == nil {
			degradation = errors.New("App Engine deployment outcome save required readback reconciliation")
		}
		api.degrade(degradation)
	}
	if err != nil {
		return fmt.Errorf("persist App Engine deployment outcome: %w", err)
	}
	if saveFailed {
		return errors.New("persist App Engine deployment outcome: save returned an error")
	}
	return nil
}

func (api *API) snapshotLocked() appEngineMetadata {
	payload, _ := json.Marshal(appEngineMetadata{
		Apps: api.apps, Services: api.services, Versions: api.versions,
		Deletions: api.deletions,
	})
	var snapshot appEngineMetadata
	_ = json.Unmarshal(payload, &snapshot)
	if snapshot.Apps == nil {
		snapshot.Apps = make(map[string]*App)
	}
	if snapshot.Services == nil {
		snapshot.Services = make(map[string]map[string]*Service)
	}
	if snapshot.Versions == nil {
		snapshot.Versions = make(map[string]map[string]map[string]*Version)
	}
	if snapshot.Deletions == nil {
		snapshot.Deletions = make(map[string]appEngineDeletion)
	}
	return snapshot
}

func (api *API) commitMetadata(previous, candidate appEngineMetadata) error {
	_, err := api.commitMetadataOutcome(previous, candidate)
	return err
}

func (api *API) commitMetadataOutcome(previous, candidate appEngineMetadata) (bool, error) {
	if api.store == nil {
		api.replaceMetadata(candidate)
		return false, nil
	}
	saveErr := api.store.Save(appEngineStateEntry, candidate)
	if saveErr == nil {
		api.replaceMetadata(candidate)
		return false, nil
	}
	var observed appEngineMetadata
	loadErr := api.store.Load(appEngineStateEntry, &observed)
	if loadErr == nil {
		if err := normalizeAppEngineMetadataShape(&observed); err != nil {
			loadErr = err
		} else {
			switch {
			case appEngineMetadataEqual(observed, candidate):
				api.replaceMetadata(candidate)
				return true, nil
			case appEngineMetadataEqual(observed, previous):
				return true, saveErr
			}
		}
	} else if errors.Is(loadErr, state.ErrNotFound) && appEngineMetadataEmpty(previous) {
		return true, saveErr
	}
	readbackErr := loadErr
	if readbackErr == nil {
		readbackErr = errors.New("readback differed from previous and candidate snapshots")
	}
	ambiguous := errors.Join(saveErr, fmt.Errorf("read back App Engine metadata: %w", readbackErr))
	api.degrade(ambiguous)
	return true, ambiguous
}

func (api *API) replaceMetadata(metadata appEngineMetadata) {
	api.mu.Lock()
	api.apps = metadata.Apps
	api.services = metadata.Services
	api.versions = metadata.Versions
	api.deletions = metadata.Deletions
	api.mu.Unlock()
}

func normalizeAppEngineMetadata(metadata *appEngineMetadata) error {
	if err := normalizeAppEngineMetadataShape(metadata); err != nil {
		return err
	}
	for _, services := range metadata.Versions {
		for _, versions := range services {
			for _, version := range versions {
				version.State = "STOPPED"
			}
		}
	}
	return nil
}

func normalizeAppEngineMetadataShape(metadata *appEngineMetadata) error {
	if metadata.Apps == nil {
		metadata.Apps = make(map[string]*App)
	}
	if metadata.Services == nil {
		metadata.Services = make(map[string]map[string]*Service)
	}
	if metadata.Versions == nil {
		metadata.Versions = make(map[string]map[string]map[string]*Version)
	}
	if metadata.Deletions == nil {
		metadata.Deletions = make(map[string]appEngineDeletion)
	}
	for id, app := range metadata.Apps {
		if app == nil {
			return fmt.Errorf("app %q is null", id)
		}
	}
	for appID, services := range metadata.Services {
		if services == nil {
			metadata.Services[appID] = make(map[string]*Service)
			continue
		}
		for id, service := range services {
			if service == nil {
				return fmt.Errorf("service %q/%q is null", appID, id)
			}
		}
	}
	for appID, services := range metadata.Versions {
		if services == nil {
			metadata.Versions[appID] = make(map[string]map[string]*Version)
			continue
		}
		for serviceID, versions := range services {
			if versions == nil {
				services[serviceID] = make(map[string]*Version)
				continue
			}
			for id, version := range versions {
				if version == nil {
					return fmt.Errorf("version %q/%q/%q is null", appID, serviceID, id)
				}
			}
		}
	}
	return nil
}

func cloneAppEngineMetadata(metadata appEngineMetadata) appEngineMetadata {
	payload, _ := json.Marshal(metadata)
	var clone appEngineMetadata
	_ = json.Unmarshal(payload, &clone)
	_ = normalizeAppEngineMetadataShape(&clone)
	return clone
}

func appEngineMetadataEqual(left, right appEngineMetadata) bool {
	leftPayload, leftErr := json.Marshal(left)
	rightPayload, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftPayload, rightPayload)
}

func appEngineMetadataEmpty(metadata appEngineMetadata) bool {
	return len(metadata.Apps) == 0 && len(metadata.Services) == 0 &&
		len(metadata.Versions) == 0 && len(metadata.Deletions) == 0
}

func ensureAppEngineMaps(metadata *appEngineMetadata, appID, serviceID string) {
	if metadata.Services[appID] == nil {
		metadata.Services[appID] = make(map[string]*Service)
	}
	if metadata.Versions[appID] == nil {
		metadata.Versions[appID] = make(map[string]map[string]*Version)
	}
	if metadata.Versions[appID][serviceID] == nil {
		metadata.Versions[appID][serviceID] = make(map[string]*Version)
	}
}

func nestedVersion(versions map[string]map[string]map[string]*Version, appID, serviceID, versionID string) *Version {
	if versions[appID] == nil || versions[appID][serviceID] == nil {
		return nil
	}
	return versions[appID][serviceID][versionID]
}

func cloneApp(app *App) *App {
	if app == nil {
		return nil
	}
	clone := *app
	return &clone
}

func cloneService(service *Service) *Service {
	if service == nil {
		return nil
	}
	clone := *service
	return &clone
}

func cloneVersion(version *Version) *Version {
	if version == nil {
		return nil
	}
	payload, _ := json.Marshal(version)
	var clone Version
	_ = json.Unmarshal(payload, &clone)
	return &clone
}

func appEngineIdentity(project, service, version string) orchestrator.ServerlessIdentity {
	return orchestrator.ServerlessIdentity{
		ResourceType: orchestrator.ServerlessAppEngineVersion,
		Project:      project,
		Location:     "global",
		Name:         service + "/versions/" + version,
	}
}

func appEngineVersionKey(appID, serviceID, versionID string) string {
	return appID + "/" + serviceID + "/" + versionID
}

func (api *API) PersistenceError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.initErr
}

func (api *API) initializationError() error { return api.PersistenceError() }

func (api *API) rejectDegradedMutation(w http.ResponseWriter) bool {
	if api.PersistenceError() == nil {
		return false
	}
	writeAppEngineError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "App Engine persistence is unavailable")
	return true
}

func (api *API) degrade(err error) {
	api.mu.Lock()
	if api.initErr == nil {
		api.initErr = fmt.Errorf("App Engine persistence is degraded: %w", err)
	} else {
		api.initErr = errors.Join(api.initErr, err)
	}
	api.mu.Unlock()
}

func writeAppEngineError(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": message, "status": status},
	})
}

func extractSegmentAfter(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}
