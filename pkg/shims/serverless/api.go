package serverless

import (
	"encoding/json"
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
	"minisky/pkg/state"
)

var (
	singletonAPI *API
	once         sync.Once
)

func init() {
	f := func(ctx *registry.Context) http.Handler {
		once.Do(func() {
			singletonAPI = NewAPI(ctx.OpMgr, ctx.SvcMgr, nil)
		})
		return singletonAPI
	}
	registry.Register("cloudfunctions.googleapis.com", f)
	registry.Register("run.googleapis.com", f)
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types — Cloud Functions v2 & Cloud Run v2
// ─────────────────────────────────────────────────────────────────────────────

// Function mirrors the Cloud Functions v2 Function resource.
type Function struct {
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	BuildConfig   *BuildConfig      `json:"buildConfig,omitempty"`
	ServiceConfig *ServiceConfig    `json:"serviceConfig,omitempty"`
	EventTrigger  *EventTrigger     `json:"eventTrigger,omitempty"`
	State         string            `json:"state"` // DEPLOYING, ACTIVE, FAILED, DELETING
	StateMessages []StateMessage    `json:"stateMessages,omitempty"`
	UpdateTime    string            `json:"updateTime"`
	Labels        map[string]string `json:"labels,omitempty"`
	Url           string            `json:"url"`
	SourceCode    string            `json:"sourceCode,omitempty"`
	Runtime       string            `json:"runtime,omitempty"`
	EntryPoint    string            `json:"entryPoint,omitempty"`
}

type BuildConfig struct {
	Runtime      string        `json:"runtime"` // nodejs20, python312, go122, etc.
	EntryPoint   string        `json:"entryPoint"`
	Source       *Source       `json:"source,omitempty"`
	EventTrigger *EventTrigger `json:"eventTrigger,omitempty"`
}

type Source struct {
	StorageSource *StorageSource `json:"storageSource,omitempty"`
	RepoSource    *RepoSource    `json:"repoSource,omitempty"`
}

type StorageSource struct {
	Bucket string `json:"bucket"`
	Object string `json:"object"`
}

type RepoSource struct {
	ProjectId  string `json:"projectId"`
	RepoName   string `json:"repoName"`
	BranchName string `json:"branchName"`
}

type ServiceConfig struct {
	MaxInstanceCount     int               `json:"maxInstanceCount"`
	MinInstanceCount     int               `json:"minInstanceCount"`
	AvailableMemory      string            `json:"availableMemory"`
	TimeoutSeconds       int               `json:"timeoutSeconds"`
	EnvironmentVariables map[string]string `json:"environmentVariables,omitempty"`
	IngressSettings      string            `json:"ingressSettings"` // ALLOW_ALL, ALLOW_INTERNAL_ONLY
	Uri                  string            `json:"uri"`
}

type StateMessage struct {
	Severity string `json:"severity"`
	Type     string `json:"type"`
	Message  string `json:"message"`
}

type EventTrigger struct {
	Trigger     string `json:"trigger,omitempty"`
	EventType   string `json:"eventType,omitempty"`
	PubsubTopic string `json:"pubsubTopic,omitempty"`
	Resource    string `json:"resource,omitempty"`
}

// Service mirrors the Cloud Run v2 Service resource.
type Service struct {
	Name               string            `json:"name"`
	Description        string            `json:"description,omitempty"`
	Uid                string            `json:"uid"`
	Generation         string            `json:"generation"`
	Labels             map[string]string `json:"labels,omitempty"`
	Annotations        map[string]string `json:"annotations,omitempty"`
	CreateTime         string            `json:"createTime"`
	UpdateTime         string            `json:"updateTime"`
	Creator            string            `json:"creator"`
	LastModifier       string            `json:"lastModifier"`
	Ingress            string            `json:"ingress"`     // INGRESS_TRAFFIC_ALL, INGRESS_TRAFFIC_INTERNAL_ONLY
	LaunchStage        string            `json:"launchStage"` // GA, BETA
	Template           *RevisionTemplate `json:"template,omitempty"`
	TrafficStatuses    []TrafficStatus   `json:"trafficStatuses,omitempty"`
	Uri                string            `json:"uri"`
	Reconciling        bool              `json:"reconciling"`
	Conditions         []Condition       `json:"conditions,omitempty"`
	ObservedGeneration string            `json:"observedGeneration"`
	SourceCode         string            `json:"sourceCode,omitempty"`
	Runtime            string            `json:"runtime,omitempty"`
	EventTrigger       *EventTrigger     `json:"eventTrigger,omitempty"`
}

type RevisionTemplate struct {
	Containers                    []Container    `json:"containers,omitempty"`
	Scaling                       *ScalingConfig `json:"scaling,omitempty"`
	MaxInstanceRequestConcurrency int            `json:"maxInstanceRequestConcurrency,omitempty"`
}

type Container struct {
	Image     string                `json:"image"`
	Env       []EnvVar              `json:"env,omitempty"`
	Resources *ResourceRequirements `json:"resources,omitempty"`
	Ports     []ContainerPort       `json:"ports,omitempty"`
}

type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

type ResourceRequirements struct {
	Limits map[string]string `json:"limits,omitempty"`
}

type ContainerPort struct {
	Name          string `json:"name,omitempty"`
	ContainerPort int    `json:"containerPort"`
}

type ScalingConfig struct {
	MinInstanceCount int `json:"minInstanceCount"`
	MaxInstanceCount int `json:"maxInstanceCount"`
}

type TrafficStatus struct {
	Type     string `json:"type"`
	Revision string `json:"revision,omitempty"`
	Percent  int    `json:"percent"`
}

type Condition struct {
	Type               string `json:"type"`
	State              string `json:"state"` // CONDITION_SUCCEEDED, CONDITION_FAILED, CONDITION_RECONCILING
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime"`
	Severity           string `json:"severity,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API handles both cloudfunctions.googleapis.com and run.googleapis.com traffic.
type buildBackend interface {
	Enabled() bool
	Requested() bool
	Status() config.BackendState
	GetLogs(string) string
	DownloadSourceFromGCS(string, string) (string, error)
	BuildFunction(orchestrator.ServerlessIdentity, string, string) (string, error)
	BuildService(orchestrator.ServerlessIdentity, string) (string, error)
}

type serverlessManager interface {
	ProvisionServerlessVM(orchestrator.ServerlessIdentity, string, []string) (string, error)
	DeleteServerlessVM(orchestrator.ServerlessIdentity) error
}

type API struct {
	mu              sync.RWMutex
	persistMu       sync.Mutex
	lifecycleMu     sync.Mutex
	opMgr           *orchestrator.OperationManager
	svcMgr          serverlessManager
	logger          *logging.API
	backend         buildBackend
	client          *http.Client
	stateStore      *state.Store
	functions       map[string]*Function // key: project:location:name
	services        map[string]*Service  // key: project:location:name
	activeLifecycle map[orchestrator.ServerlessIdentity]struct{}
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if logShim, ok := ctx.GetShim("logging.googleapis.com").(*logging.API); ok {
		api.logger = logShim
	}
}

func NewAPI(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, logger *logging.API) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Serverless] state disabled: %v", err)
		return newAPI(opMgr, sm, logger, nil)
	}
	api, err := NewAPIWithStore(opMgr, sm, logger, store)
	if err != nil {
		log.Printf("[Shim: Serverless] state rehydration failed: %v", err)
		return newAPI(opMgr, sm, logger, nil)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, logger *logging.API, store *state.Store) *API {
	return &API{
		opMgr:           opMgr,
		svcMgr:          sm,
		logger:          logger,
		backend:         NewBuildpacksBackend(),
		client:          http.DefaultClient,
		stateStore:      store,
		functions:       make(map[string]*Function),
		services:        make(map[string]*Service),
		activeLifecycle: make(map[orchestrator.ServerlessIdentity]struct{}),
	}
}

// GetBackend exposes the backend for dynamic dashboard configuration.
func (api *API) GetBackend() *BuildpacksBackend {
	backend, _ := api.backend.(*BuildpacksBackend)
	return backend
}

// SetHTTPClient replaces the client used for event delivery.
func (api *API) SetHTTPClient(client *http.Client) {
	if client == nil {
		client = http.DefaultClient
	}
	api.mu.Lock()
	api.client = client
	api.mu.Unlock()
}

// HandleEvent is called by other shims (Shim-to-Shim) to trigger functions.
func (api *API) HandleEvent(eventType, resource, payload string) {
	api.mu.RLock()
	targets := make([]eventTarget, 0)
	for _, fn := range api.functions {
		if fn.State != "ACTIVE" || fn.EventTrigger == nil || fn.Url == "" {
			continue
		}
		if triggerMatches(fn.EventTrigger, eventType, resource) {
			targets = append(targets, eventTarget{name: fn.Name, url: fn.Url})
		}
	}
	for _, service := range api.services {
		if service.Reconciling || service.EventTrigger == nil || service.Uri == "" {
			continue
		}
		if triggerMatches(service.EventTrigger, eventType, resource) {
			targets = append(targets, eventTarget{name: service.Name, url: service.Uri})
		}
	}
	client := api.client
	api.mu.RUnlock()

	for _, target := range targets {
		log.Printf("[Serverless] ⚡ Triggering %s due to %s event on %s", target.name, eventType, resource)
		go deliverEvent(client, target.url, payload)
	}
}

type eventTarget struct {
	name string
	url  string
}

func deliverEvent(client *http.Client, targetURL, payload string) {
	resp, err := client.Post(targetURL, "application/json", strings.NewReader(payload))
	if err != nil {
		log.Printf("[Serverless] ❌ Trigger failed for %s: %v", targetURL, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("[Serverless] ✅ Trigger success for %s: %s", targetURL, resp.Status)
}

func triggerMatches(trigger *EventTrigger, eventType, resource string) bool {
	if trigger == nil || (trigger.EventType != "" && trigger.EventType != eventType) {
		return false
	}

	switch eventType {
	case "google.cloud.pubsub.topic.v1.messagePublished":
		source := trigger.PubsubTopic
		if source == "" {
			source = trigger.Resource
		}
		return resourceMatches(source, resource, "topics")
	case "google.storage.object.finalize", "google.storage.object.delete",
		"google.cloud.storage.object.v1.finalized", "google.cloud.storage.object.v1.deleted":
		return resourceMatches(trigger.Resource, resource, "buckets")
	default:
		return trigger.Resource != "" && trigger.Resource == resource
	}
}

func resourceMatches(configured, actual, kind string) bool {
	if configured == "" || actual == "" {
		return false
	}
	if configured == actual {
		return true
	}
	configuredBare := !strings.Contains(strings.Trim(configured, "/"), "/")
	actualBare := !strings.Contains(strings.Trim(actual, "/"), "/")
	if configuredBare == actualBare {
		return false
	}
	return resourceLeaf(configured, kind) == resourceLeaf(actual, kind)
}

func resourceLeaf(resource, kind string) string {
	parts := strings.Split(strings.Trim(resource, "/"), "/")
	for i, part := range parts {
		if part == kind && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return ""
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Serverless] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path

	switch {
	case strings.Contains(path, "/functions"):
		api.routeFunctions(w, r, path)
	case strings.Contains(path, "/services"):
		api.routeServices(w, r, path)
	case strings.Contains(path, "/operations/"):
		api.getOperation(w, r, path)
	case strings.Contains(path, "/deploy"):
		api.deployResource(w, r)
	case strings.Contains(path, "/delete"):
		api.handleDeleteAction(w, r)
	case strings.Contains(path, "/logs/"):
		api.handleLogs(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Serverless resource not found: "+path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cloud Functions
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeFunctions(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	location := firstOf(extractSegmentAfter(path, "locations"), "us-central1")
	fnName := extractSegmentAfter(path, "functions")

	switch r.Method {
	case http.MethodPost:
		api.createFunction(w, r, project, location)
	case http.MethodGet:
		if fnName != "" {
			api.getFunction(w, project, location, fnName)
		} else {
			api.listFunctions(w, project, location)
		}
	case http.MethodDelete:
		api.deleteFunction(w, r, project, location, fnName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) createFunction(w http.ResponseWriter, r *http.Request, project, location string) {
	functionId := r.URL.Query().Get("functionId")
	var body Function
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}

	if functionId == "" {
		// Extract name from resource name if provided
		parts := strings.Split(body.Name, "/")
		if len(parts) > 0 {
			functionId = parts[len(parts)-1]
		}
	}
	if functionId == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "functionId query param or name is required")
		return
	}

	fullName := fmt.Sprintf("projects/%s/locations/%s/functions/%s", project, location, functionId)
	serviceUri := fmt.Sprintf("http://localhost:8080/%s", functionId)
	identity := serverlessIdentity(orchestrator.ServerlessFunction, project, location, functionId)

	fn := &Function{
		Name:          fullName,
		Description:   body.Description,
		BuildConfig:   body.BuildConfig,
		ServiceConfig: body.ServiceConfig,
		State:         "DEPLOYING",
		UpdateTime:    time.Now().UTC().Format(time.RFC3339),
		Labels:        body.Labels,
		Url:           serviceUri,
		EventTrigger:  body.EventTrigger,
	}
	if fn.EventTrigger == nil && fn.BuildConfig != nil {
		fn.EventTrigger = fn.BuildConfig.EventTrigger
	}
	if fn.ServiceConfig == nil {
		fn.ServiceConfig = &ServiceConfig{
			MaxInstanceCount: 100,
			AvailableMemory:  "256Mi",
			TimeoutSeconds:   60,
			IngressSettings:  "ALLOW_ALL",
			Uri:              serviceUri,
		}
	}

	key := serverlessKey(project, location, functionId)
	releaseLifecycle, ok := api.tryLifecycle(identity)
	if !ok {
		writeLifecycleConflict(w, identity)
		return
	}
	asyncOwnsLifecycle := false
	defer func() {
		if !asyncOwnsLifecycle {
			releaseLifecycle()
		}
	}()
	api.mu.Lock()
	api.functions[key] = fn
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist function metadata: "+err.Error())
		return
	}

	op, err := api.opMgr.RegisterDurable("cloudfunctions#operation", "CREATE", fullName, "", location)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	asyncOwnsLifecycle = true
	api.opMgr.RunAsync(op.Name, func() error {
		defer releaseLifecycle()
		if !api.backend.Enabled() {
			if api.backend.Requested() {
				err := fmt.Errorf("Buildpacks execution was requested but is unavailable: %s", api.backend.Status().Diagnostic)
				api.mu.Lock()
				fn.State = "FAILED"
				fn.StateMessages = []StateMessage{{Severity: "ERROR", Type: "BUILD", Message: err.Error()}}
				api.mu.Unlock()
				_ = api.persistMetadata()
				return err
			}
			api.mu.Lock()
			fn.State = "ACTIVE"
			fn.Url = ""
			fn.ServiceConfig.Uri = ""
			fn.StateMessages = []StateMessage{{
				Severity: "WARNING",
				Type:     "SIMULATION",
				Message:  "Simulation mode stores function metadata; user code was not built or started.",
			}}
			api.mu.Unlock()
			return api.persistMetadata()
		}

		// 1. Build the requested user source. Executable mode never substitutes
		// an unrelated utility image when source is absent.
		image := ""
		var err error

		sourcePath := ""
		if fn.BuildConfig != nil && fn.BuildConfig.Source != nil && fn.BuildConfig.Source.StorageSource != nil {
			src := fn.BuildConfig.Source.StorageSource
			sourcePath, err = api.backend.DownloadSourceFromGCS(src.Bucket, src.Object)
			if err != nil {
				log.Printf("[Serverless] GCS Download failed: %v", err)
				api.mu.Lock()
				fn.State = "FAILED"
				api.mu.Unlock()
				if persistErr := api.persistMetadata(); persistErr != nil {
					log.Printf("[Serverless] persist failed function: %v", persistErr)
				}
				return err
			}
		}

		if sourcePath == "" {
			err = fmt.Errorf("Buildpacks execution requires buildConfig.source.storageSource")
		} else {
			entryPoint := "handler"
			if fn.BuildConfig != nil && fn.BuildConfig.EntryPoint != "" {
				entryPoint = fn.BuildConfig.EntryPoint
			}
			image, err = api.backend.BuildFunction(identity, sourcePath, entryPoint)
		}
		if err != nil {
			log.Printf("[Serverless] Build failed: %v", err)
			api.mu.Lock()
			fn.State = "FAILED"
			api.mu.Unlock()
			if persistErr := api.persistMetadata(); persistErr != nil {
				log.Printf("[Serverless] persist failed function: %v", persistErr)
			}
			return err
		}

		// 2. Provision Container
		env := []string{}
		if fn.ServiceConfig != nil {
			for k, v := range fn.ServiceConfig.EnvironmentVariables {
				env = append(env, k+"="+v)
			}
		}

		url, err := api.svcMgr.ProvisionServerlessVM(identity, image, env)
		if err != nil {
			log.Printf("[Serverless] Provisioning failed: %v", err)
			api.mu.Lock()
			fn.State = "FAILED"
			api.mu.Unlock()
			if persistErr := api.persistMetadata(); persistErr != nil {
				log.Printf("[Serverless] persist failed function: %v", persistErr)
			}
			return err
		}

		api.mu.Lock()
		fn.State = "ACTIVE"
		fn.Url = url
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			return api.failProvisionPersistence(identity, "persist active function", err)
		}
		return nil
	})

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(toCloudFunctionsLRO(op, project, location))
}

func (api *API) getFunction(w http.ResponseWriter, project, location, name string) {
	key := serverlessKey(project, location, name)
	api.mu.RLock()
	fn, ok := api.functions[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Function "+name+" not found")
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(fn)
}

func (api *API) listFunctions(w http.ResponseWriter, project, location string) {
	prefix := serverlessKey(project, location, "")
	api.mu.RLock()
	items := []*Function{}
	for k, v := range api.functions {
		// In the emulator, if project/location are not specified in the URL, return all
		if project == "" || strings.HasPrefix(k, prefix) {
			items = append(items, v)
		}
	}
	api.mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"functions": items,
	})
}

func (api *API) deleteFunction(w http.ResponseWriter, r *http.Request, project, location, name string) {
	key := serverlessKey(project, location, name)
	api.mu.RLock()
	_, ok := api.functions[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Function "+name+" not found")
		return
	}
	fullName := fmt.Sprintf("projects/%s/locations/%s/functions/%s", project, location, name)
	identity := serverlessIdentity(orchestrator.ServerlessFunction, project, location, name)
	releaseLifecycle, ok := api.tryLifecycle(identity)
	if !ok {
		writeLifecycleConflict(w, identity)
		return
	}
	asyncOwnsLifecycle := false
	defer func() {
		if !asyncOwnsLifecycle {
			releaseLifecycle()
		}
	}()
	op, err := api.opMgr.RegisterDurable("cloudfunctions#operation", "DELETE", fullName, "", location)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	asyncOwnsLifecycle = true
	api.opMgr.RunAsync(op.Name, func() error {
		defer releaseLifecycle()
		if err := api.svcMgr.DeleteServerlessVM(identity); err != nil {
			return err
		}
		return api.deleteMetadata(orchestrator.ServerlessFunction, key)
	})
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(toCloudFunctionsLRO(op, project, location))
}

// ─────────────────────────────────────────────────────────────────────────────
// Cloud Run Services
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeServices(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	location := firstOf(extractSegmentAfter(path, "locations"), "us-central1")
	svcName := extractSegmentAfter(path, "services")

	switch r.Method {
	case http.MethodPost:
		api.createService(w, r, project, location)
	case http.MethodGet:
		if svcName != "" {
			api.getService(w, project, location, svcName)
		} else {
			api.listServices(w, project, location)
		}
	case http.MethodDelete:
		api.deleteService(w, r, project, location, svcName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) createService(w http.ResponseWriter, r *http.Request, project, location string) {
	serviceId := r.URL.Query().Get("serviceId")
	var body Service
	json.NewDecoder(r.Body).Decode(&body)
	if serviceId == "" {
		parts := strings.Split(body.Name, "/")
		if len(parts) > 0 {
			serviceId = parts[len(parts)-1]
		}
	}
	if serviceId == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "serviceId is required")
		return
	}

	fullName := fmt.Sprintf("projects/%s/locations/%s/services/%s", project, location, serviceId)
	svcUri := fmt.Sprintf("https://%s-%s.run.app", serviceId, project)
	identity := serverlessIdentity(orchestrator.ServerlessService, project, location, serviceId)

	svc := &Service{
		Name:               fullName,
		Description:        body.Description,
		Uid:                fmt.Sprintf("%x", time.Now().UnixNano()),
		Generation:         "1",
		Labels:             body.Labels,
		Annotations:        body.Annotations,
		CreateTime:         time.Now().UTC().Format(time.RFC3339),
		UpdateTime:         time.Now().UTC().Format(time.RFC3339),
		Creator:            "minisky-local@local-dev.iam.gserviceaccount.com",
		LastModifier:       "minisky-local@local-dev.iam.gserviceaccount.com",
		Ingress:            firstOf(body.Ingress, "INGRESS_TRAFFIC_ALL"),
		LaunchStage:        "GA",
		Template:           body.Template,
		EventTrigger:       body.EventTrigger,
		Reconciling:        true,
		Uri:                svcUri,
		ObservedGeneration: "0",
	}

	key := serverlessKey(project, location, serviceId)
	releaseLifecycle, ok := api.tryLifecycle(identity)
	if !ok {
		writeLifecycleConflict(w, identity)
		return
	}
	asyncOwnsLifecycle := false
	defer func() {
		if !asyncOwnsLifecycle {
			releaseLifecycle()
		}
	}()
	api.mu.Lock()
	api.services[key] = svc
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist service metadata: "+err.Error())
		return
	}

	op, err := api.opMgr.RegisterDurable("run#operation", "CREATE", fullName, "", location)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	asyncOwnsLifecycle = true
	api.opMgr.RunAsync(op.Name, func() error {
		defer releaseLifecycle()
		if !api.backend.Enabled() {
			if api.backend.Requested() {
				err := fmt.Errorf("serverless execution was requested but Buildpacks is unavailable: %s", api.backend.Status().Diagnostic)
				api.mu.Lock()
				svc.Reconciling = false
				svc.Conditions = []Condition{{Type: "Ready", State: "CONDITION_FAILED", Message: err.Error(), LastTransitionTime: time.Now().UTC().Format(time.RFC3339)}}
				api.mu.Unlock()
				_ = api.persistMetadata()
				return err
			}
			api.mu.Lock()
			svc.Reconciling = false
			svc.Uri = ""
			svc.Conditions = []Condition{{
				Type: "Ready", State: "CONDITION_SUCCEEDED",
				Message:            "Simulation mode stores service metadata; user code was not started.",
				LastTransitionTime: time.Now().UTC().Format(time.RFC3339),
			}}
			api.mu.Unlock()
			return api.persistMetadata()
		}

		// 1. Use the caller's container image. Executable mode has no utility
		// image fallback because that would report success without running code.
		image := ""
		if body.Template != nil && len(body.Template.Containers) > 0 {
			image = body.Template.Containers[0].Image
		}
		if image == "" {
			err := fmt.Errorf("Cloud Run execution requires template.containers[0].image")
			api.mu.Lock()
			svc.Reconciling = false
			svc.Conditions = []Condition{{Type: "Ready", State: "CONDITION_FAILED", Message: err.Error(), LastTransitionTime: time.Now().UTC().Format(time.RFC3339)}}
			api.mu.Unlock()
			_ = api.persistMetadata()
			return err
		}

		// 2. Provision Container
		env := []string{}
		if body.Template != nil && len(body.Template.Containers) > 0 {
			for _, e := range body.Template.Containers[0].Env {
				env = append(env, e.Name+"="+e.Value)
			}
		}

		url, err := api.svcMgr.ProvisionServerlessVM(identity, image, env)
		if err != nil {
			log.Printf("[Serverless] Run Provisioning failed: %v", err)
			api.mu.Lock()
			svc.Reconciling = false
			svc.Conditions = []Condition{{
				Type:               "Ready",
				State:              "CONDITION_FAILED",
				Message:            err.Error(),
				LastTransitionTime: time.Now().UTC().Format(time.RFC3339),
			}}
			api.mu.Unlock()
			if persistErr := api.persistMetadata(); persistErr != nil {
				log.Printf("[Serverless] persist failed service: %v", persistErr)
			}
			return err
		}

		api.mu.Lock()
		svc.Reconciling = false
		svc.ObservedGeneration = "1"
		svc.Uri = url
		svc.Conditions = []Condition{{
			Type:               "Ready",
			State:              "CONDITION_SUCCEEDED",
			LastTransitionTime: time.Now().UTC().Format(time.RFC3339),
		}}
		svc.TrafficStatuses = []TrafficStatus{
			{Type: "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST", Percent: 100},
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			return api.failProvisionPersistence(identity, "persist ready service", err)
		}
		return nil
	})

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(toRunLRO(op, project, location))
}

func (api *API) getService(w http.ResponseWriter, project, location, name string) {
	key := serverlessKey(project, location, name)
	api.mu.RLock()
	svc, ok := api.services[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Service "+name+" not found")
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(svc)
}

func (api *API) listServices(w http.ResponseWriter, project, location string) {
	prefix := serverlessKey(project, location, "")
	api.mu.RLock()
	items := []*Service{}
	for k, v := range api.services {
		if project == "" || strings.HasPrefix(k, prefix) {
			items = append(items, v)
		}
	}
	api.mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"services": items})
}

func (api *API) deleteService(w http.ResponseWriter, r *http.Request, project, location, name string) {
	key := serverlessKey(project, location, name)
	api.mu.RLock()
	_, ok := api.services[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Service "+name+" not found")
		return
	}
	fullName := fmt.Sprintf("projects/%s/locations/%s/services/%s", project, location, name)
	identity := serverlessIdentity(orchestrator.ServerlessService, project, location, name)
	releaseLifecycle, ok := api.tryLifecycle(identity)
	if !ok {
		writeLifecycleConflict(w, identity)
		return
	}
	asyncOwnsLifecycle := false
	defer func() {
		if !asyncOwnsLifecycle {
			releaseLifecycle()
		}
	}()
	op, err := api.opMgr.RegisterDurable("run#operation", "DELETE", fullName, "", location)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	asyncOwnsLifecycle = true
	api.opMgr.RunAsync(op.Name, func() error {
		defer releaseLifecycle()
		if err := api.svcMgr.DeleteServerlessVM(identity); err != nil {
			return err
		}
		return api.deleteMetadata(orchestrator.ServerlessService, key)
	})
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(toRunLRO(op, project, location))
}

type DeployRequest struct {
	Type         string        `json:"type"` // function, service
	Name         string        `json:"name"`
	Runtime      string        `json:"runtime"`
	Code         string        `json:"code"`
	Project      string        `json:"project"`
	Location     string        `json:"location"`
	EntryPoint   string        `json:"entryPoint"`
	EventTrigger *EventTrigger `json:"eventTrigger,omitempty"`
}

func (api *API) deployResource(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Serverless] deployResource called")
	var req DeployRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Project == "" {
		req.Project = "default-project"
	}
	if req.Location == "" {
		req.Location = "us-central1"
	}
	if req.Type != "function" && req.Type != "service" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "type must be function or service")
		return
	}

	log.Printf("[Serverless] Direct deployment requested: %s (%s)", req.Name, req.Type)

	fullName := fmt.Sprintf("projects/%s/locations/%s/%s/%s", req.Project, req.Location, req.Type+"s", req.Name)
	opType := "cloudfunctions#operation"
	resourceType := orchestrator.ServerlessFunction
	if req.Type == "service" {
		opType = "run#operation"
		resourceType = orchestrator.ServerlessService
	}
	identity := serverlessIdentity(resourceType, req.Project, req.Location, req.Name)
	releaseLifecycle, ok := api.tryLifecycle(identity)
	if !ok {
		writeLifecycleConflict(w, identity)
		return
	}
	asyncOwnsLifecycle := false
	defer func() {
		if !asyncOwnsLifecycle {
			releaseLifecycle()
		}
	}()
	op, err := api.opMgr.RegisterDurable(opType, "CREATE", fullName, "", req.Location)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}

	// Register initial state
	api.mu.Lock()
	if req.Type == "function" {
		api.functions[serverlessKey(req.Project, req.Location, req.Name)] = &Function{
			Name: fullName, State: "DEPLOYING", UpdateTime: time.Now().Format(time.RFC3339),
			SourceCode: req.Code, Runtime: req.Runtime, EntryPoint: req.EntryPoint,
			EventTrigger: req.EventTrigger,
		}
	} else {
		api.services[serverlessKey(req.Project, req.Location, req.Name)] = &Service{
			Name: fullName, Reconciling: true, UpdateTime: time.Now().Format(time.RFC3339),
			SourceCode: req.Code, Runtime: req.Runtime, EventTrigger: req.EventTrigger,
		}
	}
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		http.Error(w, "persist deployment metadata: "+err.Error(), http.StatusInternalServerError)
		return
	}

	asyncOwnsLifecycle = true
	api.opMgr.RunAsync(op.Name, func() error {
		defer releaseLifecycle()
		// 1. Create source directory
		tmpDir, err := os.MkdirTemp("", "minisky-deploy-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpDir)

		// 2. Write code and the minimal service metadata required by the
		// Python buildpack to detect and launch a stdlib HTTP service.
		fileName := "index.js"
		if strings.HasPrefix(req.Runtime, "python") {
			fileName = "main.py"
		} else if strings.HasPrefix(req.Runtime, "go") {
			fileName = "function.go"
		}

		if err := os.WriteFile(tmpDir+"/"+fileName, []byte(req.Code), 0644); err != nil {
			return err
		}
		if req.Type == "service" && strings.HasPrefix(req.Runtime, "python") {
			if err := os.WriteFile(tmpDir+"/requirements.txt", nil, 0644); err != nil {
				return err
			}
			if err := os.WriteFile(tmpDir+"/Procfile", []byte("web: python main.py\n"), 0644); err != nil {
				return err
			}
		}

		// 3. Build
		entryPoint := req.EntryPoint
		if entryPoint == "" {
			entryPoint = "handler"
		}

		var image string
		if req.Type == "service" {
			image, err = api.backend.BuildService(identity, tmpDir)
		} else {
			image, err = api.backend.BuildFunction(identity, tmpDir, entryPoint)
		}
		if err != nil {
			api.mu.Lock()
			key := serverlessKey(req.Project, req.Location, req.Name)
			if f, ok := api.functions[key]; ok {
				f.State = "FAILED"
			}
			if s, ok := api.services[key]; ok {
				setServiceFailed(s, "BuildFailed", err)
			}
			api.mu.Unlock()
			if persistErr := api.persistMetadata(); persistErr != nil {
				log.Printf("[Serverless] persist failed deployment: %v", persistErr)
			}
			return err
		}

		// 4. Provision — inject Google Cloud Function env vars so buildpacks
		// correctly identify the entry point for Python/Node/Go runtimes.
		sigType := "http"
		if req.EventTrigger != nil {
			sigType = "event"
		}

		envVars := []string{
			"PORT=8080",
			"FUNCTION_TARGET=" + entryPoint,
			"GOOGLE_FUNCTION_TARGET=" + entryPoint,
			"FUNCTION_SIGNATURE_TYPE=" + sigType,
			"GOOGLE_FUNCTION_SIGNATURE_TYPE=" + sigType,
		}
		// Python specific: Some framework versions look for this if FUNCTION_TARGET fails
		if strings.HasPrefix(req.Runtime, "python") && sigType == "http" {
			envVars = append(envVars, "FLASK_APP=main:"+entryPoint)
		}

		if req.Type == "service" {
			envVars = []string{"PORT=8080"} // Cloud Run services don't use FUNCTION_TARGET
		}

		url, err := api.svcMgr.ProvisionServerlessVM(identity, image, envVars)
		if err != nil {
			api.mu.Lock()
			key := serverlessKey(req.Project, req.Location, req.Name)
			if f, ok := api.functions[key]; ok {
				f.State = "FAILED"
			}
			if s, ok := api.services[key]; ok {
				setServiceFailed(s, "ProvisionFailed", err)
			}
			api.mu.Unlock()
			if persistErr := api.persistMetadata(); persistErr != nil {
				log.Printf("[Serverless] persist failed deployment: %v", persistErr)
			}
			return err
		}

		// 5. Update state
		api.mu.Lock()
		if req.Type == "function" {
			if f, ok := api.functions[serverlessKey(req.Project, req.Location, req.Name)]; ok {
				f.State = "ACTIVE"
				f.Url = url
			}
		} else {
			if s, ok := api.services[serverlessKey(req.Project, req.Location, req.Name)]; ok {
				s.Reconciling = false
				s.Uri = url
			}
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			return api.failProvisionPersistence(identity, "persist deployed resource", err)
		}
		return nil
	})

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(op)
}

func (api *API) handleDeleteAction(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")    // Full or short name
	resType := r.URL.Query().Get("type") // function or service

	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}

	// Extract short name if full path was provided
	shortName := name
	if strings.Contains(name, "/") {
		parts := strings.Split(name, "/")
		shortName = parts[len(parts)-1]
	}

	log.Printf("[Serverless] Deleting %s: %s", resType, shortName)

	project := extractSegmentAfter(r.URL.Path, "projects")
	if project == "" {
		project = r.URL.Query().Get("project")
	}
	if project == "" {
		project = "default-project"
	}
	location := firstOf(extractSegmentAfter(r.URL.Path, "locations"), "us-central1")
	key := serverlessKey(project, location, shortName)

	var resourceType orchestrator.ServerlessResourceType
	api.mu.RLock()
	exists := false
	if resType == "function" {
		resourceType = orchestrator.ServerlessFunction
		_, exists = api.functions[key]
	} else if resType == "service" {
		resourceType = orchestrator.ServerlessService
		_, exists = api.services[key]
	}
	api.mu.RUnlock()
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Serverless resource not found")
		return
	}

	identity := serverlessIdentity(resourceType, project, location, shortName)
	releaseLifecycle, ok := api.tryLifecycle(identity)
	if !ok {
		writeLifecycleConflict(w, identity)
		return
	}
	defer releaseLifecycle()
	if err := api.svcMgr.DeleteServerlessVM(identity); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "delete serverless container: "+err.Error())
		return
	}
	if err := api.deleteMetadata(resourceType, key); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "persist deletion: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"DELETED"}`)
}

func setServiceFailed(service *Service, reason string, err error) {
	service.Reconciling = false
	service.Uri = ""
	service.Conditions = []Condition{{
		Type:               "Ready",
		State:              "CONDITION_FAILED",
		Reason:             reason,
		Message:            err.Error(),
		LastTransitionTime: time.Now().UTC().Format(time.RFC3339),
	}}
}

func (api *API) failProvisionPersistence(identity orchestrator.ServerlessIdentity, action string, persistErr error) error {
	failure := fmt.Errorf("%s: %w", action, persistErr)
	if cleanupErr := api.svcMgr.DeleteServerlessVM(identity); cleanupErr != nil {
		message := cleanupErr.Error()
		if len(message) > 256 {
			message = message[:256]
		}
		failure = fmt.Errorf("%w; cleanup owned backend failed: %s", failure, message)
	}

	key := serverlessKey(identity.Project, identity.Location, identity.Name)
	api.mu.Lock()
	switch identity.ResourceType {
	case orchestrator.ServerlessFunction:
		if function := api.functions[key]; function != nil {
			function.State = "FAILED"
			function.Url = ""
			if function.ServiceConfig != nil {
				function.ServiceConfig.Uri = ""
			}
			function.StateMessages = []StateMessage{{
				Severity: "ERROR",
				Type:     "PERSISTENCE",
				Message:  failure.Error(),
			}}
		}
	case orchestrator.ServerlessService:
		if service := api.services[key]; service != nil {
			setServiceFailed(service, "PersistenceFailed", failure)
		}
	}
	api.mu.Unlock()
	if retryErr := api.persistMetadata(); retryErr != nil {
		log.Printf("[Serverless] persist cleanup state failed: %v", retryErr)
	}
	return failure
}

func (api *API) handleLogs(w http.ResponseWriter, r *http.Request) {
	name := extractSegmentAfter(r.URL.Path, "logs")
	if name == "" {
		http.Error(w, "missing resource name", http.StatusBadRequest)
		return
	}

	logs := api.backend.GetLogs(name)
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(logs))
}

// ─────────────────────────────────────────────────────────────────────────────
// Operations
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request, path string) {
	opName := extractSegmentAfter(path, "operations")
	op := api.opMgr.Get(opName)
	if op == nil {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Operation not found: "+opName)
		return
	}
	project := extractSegmentAfter(path, "projects")
	if project == "" {
		project = "default-project"
	}
	res := map[string]interface{}{
		"name":     fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, "us-central1", op.Name), // simplified for dashboard
		"done":     op.Done,
		"metadata": map[string]string{"@type": "type.googleapis.com/google.cloud.functions.v2.OperationMetadata"},
	}
	if op.Error != nil {
		res["error"] = op.Error
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func toCloudFunctionsLRO(op *orchestrator.Operation, project, location string) map[string]interface{} {
	return map[string]interface{}{
		"name": fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name),
		"metadata": map[string]string{
			"@type": "type.googleapis.com/google.cloud.functions.v2.OperationMetadata",
		},
		"done": op.Done,
	}
}

func toRunLRO(op *orchestrator.Operation, project, location string) map[string]interface{} {
	return map[string]interface{}{
		"name": fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, op.Name),
		"metadata": map[string]string{
			"@type": "type.googleapis.com/google.cloud.run.v2.Service",
		},
		"done": op.Done,
	}
}

func (api *API) OnStorageEvent(bucket, object, eventType string) {
	payload, _ := json.Marshal(map[string]string{"bucket": bucket, "name": object})
	api.HandleEvent(eventType, bucket, string(payload))
}

func serverlessKey(project, location, name string) string {
	return project + ":" + location + ":" + name
}

func serverlessIdentity(resourceType orchestrator.ServerlessResourceType, project, location, name string) orchestrator.ServerlessIdentity {
	return orchestrator.ServerlessIdentity{
		ResourceType: resourceType,
		Project:      project,
		Location:     location,
		Name:         name,
	}
}

func (api *API) tryLifecycle(identity orchestrator.ServerlessIdentity) (func(), bool) {
	api.lifecycleMu.Lock()
	if api.activeLifecycle == nil {
		api.activeLifecycle = make(map[orchestrator.ServerlessIdentity]struct{})
	}
	if _, active := api.activeLifecycle[identity]; active {
		api.lifecycleMu.Unlock()
		return nil, false
	}
	api.activeLifecycle[identity] = struct{}{}
	api.lifecycleMu.Unlock()

	return func() {
		api.lifecycleMu.Lock()
		delete(api.activeLifecycle, identity)
		api.lifecycleMu.Unlock()
	}, true
}

func writeLifecycleConflict(w http.ResponseWriter, identity orchestrator.ServerlessIdentity) {
	w.WriteHeader(http.StatusConflict)
	writeError(w, http.StatusConflict, "ABORTED", "Serverless lifecycle already in progress for "+identity.CanonicalResource())
}

func extractSegmentAfter(path, keyword string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == keyword && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func firstOf(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{"code": code, "status": status, "message": message},
	})
}
