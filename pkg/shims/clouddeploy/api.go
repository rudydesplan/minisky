package clouddeploy

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
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
	registry.Register("clouddeploy.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

type imagePolicyEvaluator interface {
	EvaluateImage(project, image string) error
}

type unsupportedImagePolicyEvaluation interface {
	PolicyEvaluationUnsupported() bool
}

type unavailableImagePolicyEvaluation interface {
	PolicyEvaluationUnavailable() bool
}

type allowImagePolicyEvaluator struct{}

func (allowImagePolicyEvaluator) EvaluateImage(string, string) error { return nil }

// ─────────────────────────────────────────────────────────────────────────────
// Resources (Cloud Deploy v1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Stage represents a stage in a serial pipeline.
type Stage struct {
	TargetID string   `json:"targetId,omitempty"`
	Profiles []string `json:"profiles,omitempty"`
}

// SerialPipeline represents the serial pipeline configuration.
type SerialPipeline struct {
	Stages []Stage `json:"stages,omitempty"`
}

// DeliveryPipeline represents a google.cloud.deploy.v1.DeliveryPipeline resource.
type DeliveryPipeline struct {
	Name           string            `json:"name"`
	UID            string            `json:"uid,omitempty"`
	CreateTime     string            `json:"createTime,omitempty"`
	UpdateTime     string            `json:"updateTime,omitempty"`
	SerialPipeline *SerialPipeline   `json:"serialPipeline,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

// Release represents a google.cloud.deploy.v1.Release resource.
type Release struct {
	Name       string            `json:"name"`
	UID        string            `json:"uid,omitempty"`
	CreateTime string            `json:"createTime,omitempty"`
	UpdateTime string            `json:"updateTime,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// Rollout represents a google.cloud.deploy.v1.Rollout resource.
type Rollout struct {
	Name        string            `json:"name"`
	UID         string            `json:"uid,omitempty"`
	CreateTime  string            `json:"createTime,omitempty"`
	UpdateTime  string            `json:"updateTime,omitempty"`
	TargetID    string            `json:"targetId,omitempty"`
	Image       string            `json:"image,omitempty"`
	State       string            `json:"state,omitempty"`
	LocalTarget string            `json:"localTarget,omitempty"`
	Strategy    json.RawMessage   `json:"strategy,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Cloud Deploy v1 REST shim.
type API struct {
	mu              sync.RWMutex
	persistMu       sync.Mutex
	opMgr           *orchestrator.OperationManager
	stateStore      clouddeployStateStore
	policyEvaluator imagePolicyEvaluator
	pipelines       map[string]*DeliveryPipeline
	releases        map[string]*Release
	rollouts        map[string]*Rollout
}

// NewAPI creates a new Cloud Deploy shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:      opMgr,
		stateStore: state.NewGuardedEntryStore(store, err),
		pipelines:  make(map[string]*DeliveryPipeline),
		releases:   make(map[string]*Release),
		rollouts:   make(map[string]*Rollout),
	}
	if err != nil {
		log.Printf("[Shim: Cloud Deploy] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Cloud Deploy] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr:           orchestrator.NewOperationManager(),
		policyEvaluator: allowImagePolicyEvaluator{},
		pipelines:       make(map[string]*DeliveryPipeline),
		releases:        make(map[string]*Release),
		rollouts:        make(map[string]*Rollout),
	}
}

// OnPostBoot injects the Binary Authorization evaluator after all experimental
// shims have been instantiated. The structural interface avoids package cycles.
func (api *API) OnPostBoot(ctx *registry.Context) {
	if evaluator, ok := ctx.GetShim("binaryauthorization.googleapis.com").(imagePolicyEvaluator); ok {
		api.policyEvaluator = evaluator
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Cloud Deploy] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.Contains(r.URL.Path, "/rollouts"):
		api.routeRollouts(w, r)
	case strings.Contains(r.URL.Path, "/releases"):
		api.routeReleases(w, r)
	case strings.Contains(r.URL.Path, "/deliveryPipelines"):
		api.routePipelines(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Cloud Deploy resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Delivery Pipelines
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routePipelines(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createPipeline(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "deliveryPipelines") {
			api.listPipelines(w, r)
		} else {
			api.getPipeline(w, r)
		}
	case http.MethodPatch:
		api.patchPipeline(w, r)
	case http.MethodDelete:
		api.deletePipeline(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createPipeline(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	pipelineID := r.URL.Query().Get("deliveryPipelineId")
	if pipelineID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "deliveryPipelineId query parameter is required")
		return
	}

	var resource DeliveryPipeline
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/deliveryPipelines/%s", project, location, pipelineID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.UID = generateUID()
	resource.CreateTime = now
	resource.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.pipelines[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "delivery pipeline already exists: "+pipelineID)
		return
	}
	api.pipelines[name] = &resource
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &resource, true, func() {
		api.mu.Lock()
		delete(api.pipelines, name)
		api.mu.Unlock()
	})
}

func (api *API) getPipeline(w http.ResponseWriter, r *http.Request) {
	name := parsePipelineName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.pipelines[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "delivery pipeline not found: "+name)
		return
	}
	clone := deepCopyPipeline(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listPipelines(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/deliveryPipelines/", project, location)

	pageSize, pageToken := parsePagination(r)

	api.mu.RLock()
	all := make([]*DeliveryPipeline, 0)
	for key, p := range api.pipelines {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyPipeline(p))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "clouddeploy.deliveryPipelines",
		Parent:  fmt.Sprintf("projects/%s/locations/%s", project, location),
	}, func(pipeline *DeliveryPipeline) string { return pipeline.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"deliveryPipelines": result,
		"nextPageToken":     nextToken,
	})
}

func (api *API) patchPipeline(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parsePipelineName(r.URL.Path)

	api.mu.Lock()
	existing, ok := api.pipelines[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "delivery pipeline not found: "+name)
		return
	}
	before := deepCopyPipeline(existing)

	var patch DeliveryPipeline
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if patch.SerialPipeline != nil {
		existing.SerialPipeline = patch.SerialPipeline
	}
	if patch.Labels != nil {
		existing.Labels = patch.Labels
	}
	existing.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	updated := deepCopyPipeline(existing)
	api.mu.Unlock()

	api.finishMutation(w, "update", name, updated, false, func() {
		api.mu.Lock()
		api.pipelines[name] = before
		api.mu.Unlock()
	})
}

func (api *API) deletePipeline(w http.ResponseWriter, r *http.Request) {
	name := parsePipelineName(r.URL.Path)

	api.mu.Lock()
	resource, exists := api.pipelines[name]
	if !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "delivery pipeline not found: "+name)
		return
	}
	releasePrefix := name + "/releases/"
	for releaseName := range api.releases {
		if strings.HasPrefix(releaseName, releasePrefix) {
			api.mu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "releases must be deleted before the delivery pipeline")
			return
		}
	}
	delete(api.pipelines, name)
	api.mu.Unlock()

	api.finishMutation(w, "delete", name, map[string]any{}, false, func() {
		api.mu.Lock()
		api.pipelines[name] = resource
		api.mu.Unlock()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Releases
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeReleases(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createRelease(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "releases") {
			api.listReleases(w, r)
		} else {
			api.getRelease(w, r)
		}
	case http.MethodDelete:
		api.deleteRelease(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createRelease(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	releaseID := r.URL.Query().Get("releaseId")
	if releaseID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "releaseId query parameter is required")
		return
	}

	// Parse parent pipeline
	pipelineName := parseReleaseParent(r.URL.Path)

	var resource Release
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	name := pipelineName + "/releases/" + releaseID
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.UID = generateUID()
	resource.CreateTime = now
	resource.UpdateTime = now

	api.mu.Lock()
	if _, exists := api.pipelines[pipelineName]; !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent delivery pipeline not found: "+pipelineName)
		return
	}
	if _, exists := api.releases[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "release already exists: "+releaseID)
		return
	}
	api.releases[name] = &resource
	api.mu.Unlock()

	api.finishMutation(w, "create", name, &resource, true, func() {
		api.mu.Lock()
		delete(api.releases, name)
		api.mu.Unlock()
	})
}

func (api *API) getRelease(w http.ResponseWriter, r *http.Request) {
	name := parseReleaseName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.releases[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "release not found: "+name)
		return
	}
	clone := deepCopyRelease(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listReleases(w http.ResponseWriter, r *http.Request) {
	pipelineName := parseReleaseParent(r.URL.Path)
	prefix := pipelineName + "/releases/"

	pageSize, pageToken := parsePagination(r)

	api.mu.RLock()
	all := make([]*Release, 0)
	for key, rel := range api.releases {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyRelease(rel))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "clouddeploy.releases",
		Parent:  pipelineName,
	}, func(release *Release) string { return release.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"releases":      result,
		"nextPageToken": nextToken,
	})
}

func (api *API) deleteRelease(w http.ResponseWriter, r *http.Request) {
	name := parseReleaseName(r.URL.Path)
	api.persistMu.Lock()
	api.mu.Lock()
	resource, ok := api.releases[name]
	if !ok {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "release not found: "+name)
		return
	}
	prefix := name + "/rollouts/"
	for rolloutName := range api.rollouts {
		if strings.HasPrefix(rolloutName, prefix) {
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "rollouts must be deleted before the release")
			return
		}
	}
	delete(api.releases, name)
	api.mu.Unlock()
	if api.stateStore != nil {
		if err := api.stateStore.Save(stateEntry, api.snapshot()); err != nil {
			api.mu.Lock()
			api.releases[name] = resource
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
			return
		}
	}
	api.persistMu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Rollouts
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeRollouts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		api.createRollout(w, r)
	case http.MethodGet:
		if isCollection(r.URL.Path, "rollouts") {
			api.listRollouts(w, r)
		} else {
			api.getRollout(w, r)
		}
	case http.MethodDelete:
		api.deleteRollout(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createRollout(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	rolloutID := r.URL.Query().Get("rolloutId")
	if rolloutID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "rolloutId query parameter is required")
		return
	}

	// Parse parent release
	releaseName := parseRolloutParent(r.URL.Path)

	var resource Rollout
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	api.mu.RLock()
	_, parentExists := api.releases[releaseName]
	api.mu.RUnlock()
	if !parentExists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent release not found: "+releaseName)
		return
	}
	if len(resource.Strategy) != 0 {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "rollout strategies are not supported")
		return
	}
	if resource.LocalTarget == "" {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "only executable loopback localTarget rollouts are supported")
		return
	}
	if strings.TrimSpace(resource.Image) == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "image is required for executable localTarget rollouts")
		return
	}
	var localTarget *url.URL
	var err error
	localTarget, err = validateLocalTarget(resource.LocalTarget)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	resource.LocalTarget = localTarget.String()

	name := releaseName + "/rollouts/" + rolloutID
	now := time.Now().UTC().Format(time.RFC3339Nano)

	resource.Name = name
	resource.UID = generateUID()
	resource.CreateTime = now
	resource.UpdateTime = now
	resource.State = "IN_PROGRESS"

	api.mu.Lock()
	if _, exists := api.releases[releaseName]; !exists {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "parent release not found: "+releaseName)
		return
	}
	if _, exists := api.rollouts[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "rollout already exists: "+rolloutID)
		return
	}
	api.rollouts[name] = &resource
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("clouddeploy#operation", "create", name)
	if err != nil {
		api.mu.Lock()
		delete(api.rollouts, name)
		api.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.rollouts, name)
		api.mu.Unlock()
		stateErr := api.persistState()
		operationErr := api.opMgr.RollbackScopedRegistration(op.Name)
		if stateErr != nil {
			api.opMgr.MarkPersistenceFailure(stateErr)
		}
		if stateErr != nil || operationErr != nil {
			log.Printf("[Shim: CloudDeploy] rollout compensation degraded: state=%v operation=%v", stateErr, operationErr)
		}
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}
	go api.executeRollout(op.Name, name, releaseName, resource.Image, localTarget)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

func (api *API) executeRollout(operationName, rolloutName, releaseName, image string, localTarget *url.URL) {
	if err := api.opMgr.AdvanceDurable(operationName, 0, orchestrator.StatusRunning); err != nil {
		api.finishRolloutExecution(operationName, rolloutName,
			fmt.Errorf("operation persistence failed"), http.StatusServiceUnavailable)
		return
	}
	project := "projects/" + extractAfter(releaseName, "projects")
	if api.policyEvaluator == nil {
		api.finishRolloutExecution(operationName, rolloutName,
			fmt.Errorf("Binary Authorization evaluator unavailable"), http.StatusServiceUnavailable)
		return
	}
	if err := api.policyEvaluator.EvaluateImage(project, image); err != nil {
		var unavailable unavailableImagePolicyEvaluation
		if errors.As(err, &unavailable) && unavailable.PolicyEvaluationUnavailable() {
			api.finishRolloutExecution(operationName, rolloutName,
				fmt.Errorf("Binary Authorization evaluator unavailable: %w", err), http.StatusServiceUnavailable)
			return
		}
		var unsupported unsupportedImagePolicyEvaluation
		if errors.As(err, &unsupported) && unsupported.PolicyEvaluationUnsupported() {
			api.finishRolloutExecution(operationName, rolloutName,
				fmt.Errorf("Binary Authorization evaluation unsupported: %w", err), http.StatusNotImplemented)
			return
		}
		api.finishRolloutExecution(operationName, rolloutName,
			fmt.Errorf("Binary Authorization denied image: %w", err), http.StatusForbidden)
		return
	}

	payload, err := json.Marshal(map[string]string{"release": releaseName, "image": image})
	if err == nil {
		var request *http.Request
		request, err = http.NewRequest(http.MethodPost, localTarget.String(), strings.NewReader(string(payload)))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
			client := &http.Client{
				Timeout: 2 * time.Second,
				CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
					return fmt.Errorf("redirects are not followed")
				},
			}
			var response *http.Response
			response, err = client.Do(request)
			if response != nil {
				_ = response.Body.Close()
				if err == nil && (response.StatusCode < 200 || response.StatusCode >= 300) {
					err = fmt.Errorf("local target returned HTTP %d", response.StatusCode)
				}
			}
		}
	}
	api.finishRolloutExecution(operationName, rolloutName, err, http.StatusInternalServerError)
}

func (api *API) finishRolloutExecution(operationName, rolloutName string, executionErr error, errorCode int) {
	api.mu.Lock()
	saved := api.rollouts[rolloutName]
	if saved != nil {
		if executionErr == nil {
			saved.State = "SUCCEEDED"
		} else {
			saved.State = "FAILED"
		}
		saved.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	rollout := deepCopyRollout(saved)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		if finalizeErr := api.opMgr.FinalizeScopedDurable(operationName, nil,
			http.StatusServiceUnavailable, "rollout state persistence failed"); finalizeErr != nil {
			log.Printf("[Shim: CloudDeploy] rollout persistence failure could not be finalized: %v", finalizeErr)
		}
		return
	}
	if executionErr != nil {
		if err := api.opMgr.FinalizeScopedDurable(operationName, nil, errorCode, executionErr.Error()); err != nil {
			log.Printf("[Shim: CloudDeploy] failed rollout operation persistence degraded: %v", err)
		}
		return
	}
	response, err := json.Marshal(rollout)
	if err != nil {
		_ = api.opMgr.FinalizeScopedDurable(operationName, nil,
			http.StatusInternalServerError, "failed to encode rollout response")
		return
	}
	if err := api.opMgr.FinalizeScopedDurable(operationName, response, 0, ""); err != nil {
		log.Printf("[Shim: CloudDeploy] successful rollout operation persistence degraded: %v", err)
	}
}

func (api *API) getRollout(w http.ResponseWriter, r *http.Request) {
	name := parseRolloutName(r.URL.Path)

	api.mu.RLock()
	resource, ok := api.rollouts[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "rollout not found: "+name)
		return
	}
	clone := deepCopyRollout(resource)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listRollouts(w http.ResponseWriter, r *http.Request) {
	releaseName := parseRolloutParent(r.URL.Path)
	prefix := releaseName + "/rollouts/"

	pageSize, pageToken := parsePagination(r)

	api.mu.RLock()
	all := make([]*Rollout, 0)
	for key, ro := range api.rollouts {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyRollout(ro))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "clouddeploy.rollouts",
		Parent:  releaseName,
	}, func(rollout *Rollout) string { return rollout.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"rollouts":      result,
		"nextPageToken": nextToken,
	})
}

func (api *API) deleteRollout(w http.ResponseWriter, r *http.Request) {
	name := parseRolloutName(r.URL.Path)
	api.persistMu.Lock()
	api.mu.Lock()
	resource, ok := api.rollouts[name]
	if !ok {
		api.mu.Unlock()
		api.persistMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "rollout not found: "+name)
		return
	}
	delete(api.rollouts, name)
	api.mu.Unlock()
	if api.stateStore != nil {
		if err := api.stateStore.Save(stateEntry, api.snapshot()); err != nil {
			api.mu.Lock()
			api.rollouts[name] = resource
			api.mu.Unlock()
			api.persistMu.Unlock()
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
			return
		}
	}
	api.persistMu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Operations
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "clouddeploy#operation")
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
}

func (api *API) finishMutation(w http.ResponseWriter, verb, target string, response any, async bool, rollback func()) {
	op, err := api.opMgr.RegisterScopedTargetDurable("clouddeploy#operation", verb, target)
	if err != nil {
		rollback()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "failed to register operation")
		return
	}
	compensate := func() {
		rollback()
		stateErr := api.persistState()
		operationErr := api.opMgr.RollbackScopedRegistration(op.Name)
		if stateErr != nil {
			api.opMgr.MarkPersistenceFailure(stateErr)
		}
		if stateErr != nil || operationErr != nil {
			log.Printf("[Shim: CloudDeploy] mutation compensation degraded: state=%v operation=%v", stateErr, operationErr)
		}
	}
	if err := api.persistState(); err != nil {
		compensate()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "state persistence failed")
		return
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		compensate()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to encode operation response")
		return
	}
	if async {
		go func() {
			time.Sleep(50 * time.Millisecond)
			if err := api.opMgr.FinalizeScopedDurable(op.Name, encoded, 0, ""); err != nil {
				log.Printf("[Shim: CloudDeploy] terminal operation persistence degraded: %v", err)
			}
		}()
		_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
		return
	}
	if err := api.opMgr.FinalizeScopedDurable(op.Name, encoded, 0, ""); err != nil {
		compensate()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "operation persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func validateLocalTarget(raw string) (*url.URL, error) {
	target, err := url.Parse(raw)
	if err != nil || target.Scheme != "http" || target.Host == "" || target.User != nil {
		return nil, fmt.Errorf("localTarget must be an HTTP loopback origin")
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
			return nil, fmt.Errorf("localTarget must use localhost or a literal loopback IP")
		}
	}
	target.RawQuery = ""
	target.Fragment = ""
	return target, nil
}

func parseParent(path string) (project, location string, ok bool) {
	project = extractAfter(path, "projects")
	location = extractAfter(path, "locations")
	if project == "" || location == "" {
		return "", "", false
	}
	return project, location, true
}

func parsePipelineName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	pipelineID := extractAfter(path, "deliveryPipelines")
	return fmt.Sprintf("projects/%s/locations/%s/deliveryPipelines/%s", project, location, pipelineID)
}

func parseReleaseParent(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	pipelineID := extractAfter(path, "deliveryPipelines")
	return fmt.Sprintf("projects/%s/locations/%s/deliveryPipelines/%s", project, location, pipelineID)
}

func parseReleaseName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	pipelineID := extractAfter(path, "deliveryPipelines")
	releaseID := extractAfter(path, "releases")
	return fmt.Sprintf("projects/%s/locations/%s/deliveryPipelines/%s/releases/%s", project, location, pipelineID, releaseID)
}

func parseRolloutParent(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	pipelineID := extractAfter(path, "deliveryPipelines")
	releaseID := extractAfter(path, "releases")
	return fmt.Sprintf("projects/%s/locations/%s/deliveryPipelines/%s/releases/%s", project, location, pipelineID, releaseID)
}

func parseRolloutName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	pipelineID := extractAfter(path, "deliveryPipelines")
	releaseID := extractAfter(path, "releases")
	rolloutID := extractAfter(path, "rollouts")
	return fmt.Sprintf("projects/%s/locations/%s/deliveryPipelines/%s/releases/%s/rollouts/%s", project, location, pipelineID, releaseID, rolloutID)
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
