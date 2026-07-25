package scheduler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	configpkg "minisky/pkg/config"
	"minisky/pkg/registry"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

func init() {
	registry.Register("cloudscheduler.googleapis.com", func(ctx *registry.Context) http.Handler {
		var logAPI *logging.API
		if l, ok := ctx.GetShim("logging.googleapis.com").(*logging.API); ok {
			logAPI = l
		}
		return NewAPI(logAPI)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

type Job struct {
	Name              string           `json:"name"`
	Description       string           `json:"description,omitempty"`
	Target            *Target          `json:"target,omitempty"` // One of httpTarget, pubsubTarget, appEngineHttpTarget
	HttpTarget        *HttpTarget      `json:"httpTarget,omitempty"`
	PubsubTarget      *PubsubTarget    `json:"pubsubTarget,omitempty"`
	AppEngineTarget   *AppEngineTarget `json:"appEngineHttpTarget,omitempty"`
	Schedule          string           `json:"schedule"`
	TimeZone          string           `json:"timeZone,omitempty"`
	State             string           `json:"state"` // ENABLED, PAUSED, DISABLED
	Status            *Status          `json:"status,omitempty"`
	LastAttemptTime   string           `json:"lastAttemptTime,omitempty"`
	LastAttemptStatus int              `json:"lastAttemptStatus,omitempty"`
	LastAttemptError  string           `json:"lastAttemptError,omitempty"`
	NextRunTime       string           `json:"nextRunTime,omitempty"`
}

type Target struct{}

type HttpTarget struct {
	Uri        string            `json:"uri"`
	HttpMethod string            `json:"httpMethod"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       string            `json:"body,omitempty"`
}

type PubsubTarget struct {
	TopicName  string            `json:"topicName"`
	Data       string            `json:"data"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type AppEngineTarget struct {
	RelativeUri string            `json:"relativeUri"`
	HttpMethod  string            `json:"httpMethod"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        string            `json:"body,omitempty"`
}

type Status struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

type API struct {
	mu             sync.RWMutex
	store          stateStore
	jobs           map[string]*Job // key: projects/{p}/locations/{l}/jobs/{j}
	cron           *cron.Cron
	cronIDs        map[string]cron.EntryID
	logAPI         *logging.API
	client         *http.Client
	gatewayBaseURL string
	now            func() time.Time
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

const schedulerStateEntry = "scheduler/metadata"

type schedulerMetadata struct {
	Jobs map[string]*Job `json:"jobs"`
}

type Config struct {
	GatewayBaseURL string
	HTTPClient     *http.Client
	Now            func() time.Time
}

func NewAPI(logAPI *logging.API) *API {
	config := Config{
		GatewayBaseURL: os.Getenv("MINISKY_GATEWAY_URL"),
	}
	store, err := state.New(configpkg.GetStateDir(), configpkg.GetProfile())
	if err != nil {
		log.Printf("[Shim: Cloud Scheduler] state disabled: %v", err)
		return startScheduler(newAPI(logAPI, config, nil))
	}
	api, err := NewAPIWithConfigAndStore(logAPI, config, store)
	if err != nil {
		log.Printf("[Shim: Cloud Scheduler] state rehydration failed: %v", err)
		return startScheduler(newAPI(logAPI, config, store))
	}
	return api
}

func NewAPIWithConfig(logAPI *logging.API, config Config) *API {
	api, _ := NewAPIWithConfigAndStore(logAPI, config, nil)
	return api
}

// NewAPIWithConfigAndStore constructs a Scheduler shim backed by the supplied
// metadata store. It reports unreadable state instead of silently replacing it.
func NewAPIWithConfigAndStore(logAPI *logging.API, config Config, store stateStore) (*API, error) {
	api := newAPI(logAPI, config, store)
	if store != nil {
		var persisted schedulerMetadata
		if err := store.Load(schedulerStateEntry, &persisted); err != nil {
			if !errors.Is(err, state.ErrNotFound) {
				return nil, fmt.Errorf("load Scheduler metadata: %w", err)
			}
		} else if persisted.Jobs != nil {
			api.jobs = persisted.Jobs
		}
	}
	for _, job := range api.jobs {
		api.scheduleJobLocked(job)
	}
	return startScheduler(api), nil
}

func newAPI(logAPI *logging.API, config Config, store stateStore) *API {
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	api := &API{
		store:          store,
		jobs:           make(map[string]*Job),
		cron:           cron.New(),
		cronIDs:        make(map[string]cron.EntryID),
		logAPI:         logAPI,
		client:         &clientCopy,
		gatewayBaseURL: strings.TrimRight(config.GatewayBaseURL, "/"),
		now:            now,
	}
	return api
}

func startScheduler(api *API) *API {
	api.cron.Start()
	return api
}

func (api *API) Close() {
	<-api.cron.Stop().Done()
}

func (api *API) persistMetadata() error {
	if api.store == nil {
		return nil
	}
	api.mu.RLock()
	jobs := make(map[string]*Job, len(api.jobs))
	for name, job := range api.jobs {
		jobs[name] = cloneJob(job)
	}
	api.mu.RUnlock()
	return api.store.Save(schedulerStateEntry, schedulerMetadata{Jobs: jobs})
}

func (api *API) persistOrError(w http.ResponseWriter) bool {
	if err := api.persistMetadata(); err != nil {
		log.Printf("[Shim: Cloud Scheduler] persist metadata: %v", err)
		http.Error(w, "Failed to persist Scheduler metadata", http.StatusInternalServerError)
		return false
	}
	return true
}

func (api *API) pushLog(projectId, severity, jobId, text string) {
	if api.logAPI == nil {
		return
	}
	api.logAPI.PushLog(projectId, severity, "cloud_scheduler_job", jobId, text)
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Cloud Scheduler] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path

	// Job verbs (run, pause, resume)
	switch {
	case strings.HasSuffix(path, ":run"):
		api.runJob(w, r, strings.TrimSuffix(path, ":run"))
		return
	case strings.HasSuffix(path, ":pause"):
		api.pauseJob(w, r, strings.TrimSuffix(path, ":pause"))
		return
	case strings.HasSuffix(path, ":resume"):
		api.resumeJob(w, r, strings.TrimSuffix(path, ":resume"))
		return
	}

	if strings.Contains(path, "/jobs") {
		api.routeJobs(w, r, path)
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func (api *API) routeJobs(w http.ResponseWriter, r *http.Request, path string) {
	jobName := extractJobName(path)

	switch r.Method {
	case http.MethodPost:
		api.createJob(w, r, path)
	case http.MethodGet:
		if jobName != "" {
			api.getJob(w, jobName)
		} else {
			api.listJobs(w, r, path)
		}
	case http.MethodDelete:
		api.deleteJob(w, jobName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) createJob(w http.ResponseWriter, r *http.Request, path string) {
	var job Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// In GCP, Name is usually provided in the body or generated
	// If it's a relative path, we prefix it
	if !strings.HasPrefix(job.Name, "projects/") {
		job.Name = strings.TrimSuffix(path, "/") + "/" + job.Name
	}

	job.State = "ENABLED"
	job.Status = &Status{Code: 0, Message: "Job created"}

	api.mu.Lock()
	api.jobs[job.Name] = &job
	api.scheduleJobLocked(&job)
	result := job
	api.mu.Unlock()

	if !api.persistOrError(w) {
		return
	}
	project := extractProject(job.Name)
	api.pushLog(project, "INFO", job.Name, "Job created: "+job.Schedule)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

func (api *API) getJob(w http.ResponseWriter, name string) {
	api.mu.RLock()
	job, ok := api.jobs[name]
	var result Job
	if ok {
		result = *job
	}
	api.mu.RUnlock()

	if !ok {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(result)
}

func (api *API) listJobs(w http.ResponseWriter, r *http.Request, path string) {
	prefix := strings.TrimSuffix(path, "/jobs") + "/jobs/"
	api.mu.RLock()
	var items []Job
	for k, v := range api.jobs {
		if strings.HasPrefix(k, prefix) {
			items = append(items, *v)
		}
	}
	api.mu.RUnlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"jobs": items,
	})
}

func (api *API) deleteJob(w http.ResponseWriter, name string) {
	api.mu.Lock()
	if id, ok := api.cronIDs[name]; ok {
		api.cron.Remove(id)
		delete(api.cronIDs, name)
	}
	delete(api.jobs, name)
	api.mu.Unlock()

	if !api.persistOrError(w) {
		return
	}
	project := extractProject(name)
	api.pushLog(project, "INFO", name, "Job deleted")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{})
}

func (api *API) runJob(w http.ResponseWriter, r *http.Request, name string) {
	api.mu.RLock()
	job, ok := api.jobs[name]
	var result Job
	if ok {
		result = *job
	}
	api.mu.RUnlock()

	if !ok {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	go api.executeJob(job)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

func (api *API) pauseJob(w http.ResponseWriter, r *http.Request, name string) {
	api.mu.Lock()
	if job, ok := api.jobs[name]; ok {
		job.State = "PAUSED"
		if id, ok := api.cronIDs[name]; ok {
			api.cron.Remove(id)
			delete(api.cronIDs, name)
		}
	}
	api.mu.Unlock()
	if !api.persistOrError(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (api *API) resumeJob(w http.ResponseWriter, r *http.Request, name string) {
	api.mu.Lock()
	if job, ok := api.jobs[name]; ok {
		job.State = "ENABLED"
		api.scheduleJobLocked(job)
	}
	api.mu.Unlock()
	if !api.persistOrError(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ─────────────────────────────────────────────────────────────────────────────
// Engine
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) scheduleJobLocked(job *Job) {
	if job.State != "ENABLED" {
		return
	}

	// Remove old if exists
	if id, ok := api.cronIDs[job.Name]; ok {
		api.cron.Remove(id)
	}

	id, err := api.cron.AddFunc(job.Schedule, func() {
		api.executeJob(job)
	})

	if err != nil {
		log.Printf("[Scheduler] Error scheduling job %s: %v", job.Name, err)
		return
	}
	api.cronIDs[job.Name] = id
}

func (api *API) executeJob(job *Job) {
	project := extractProject(job.Name)
	api.pushLog(project, "INFO", job.Name, "Job started")
	startTime := api.now()

	statusCode := 0
	var err error
	if job.HttpTarget != nil {
		statusCode, err = api.executeHttp(job.HttpTarget)
	} else if job.PubsubTarget != nil {
		statusCode, err = api.executePubsub(job.PubsubTarget)
	} else if job.AppEngineTarget != nil {
		statusCode, err = api.executeAppEngine(job.AppEngineTarget)
	} else {
		err = fmt.Errorf("job has no delivery target")
	}

	api.mu.Lock()
	job.LastAttemptTime = startTime.Format(time.RFC3339Nano)
	job.LastAttemptStatus = statusCode
	if err != nil {
		job.Status = &Status{Code: 13, Message: err.Error()}
		job.LastAttemptError = err.Error()
	} else {
		job.Status = &Status{Code: 0, Message: "Success"}
		job.LastAttemptError = ""
	}
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		log.Printf("[Shim: Cloud Scheduler] persist execution metadata: %v", err)
	}

	if err != nil {
		api.pushLog(project, "ERROR", job.Name, "Job failed: "+err.Error())
	} else {
		api.pushLog(project, "INFO", job.Name, "Job finished successfully")
	}
}

func (api *API) executeHttp(target *HttpTarget) (int, error) {
	req, err := newTargetRequest(target.HttpMethod, target.Uri, target.Headers, target.Body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "MiniSky-Cloud-Scheduler")

	return api.deliver(req, "HTTP")
}

func (api *API) executePubsub(target *PubsubTarget) (int, error) {
	if api.gatewayBaseURL == "" {
		return 0, fmt.Errorf("PubSub delivery requires MINISKY_GATEWAY_URL or Config.GatewayBaseURL")
	}
	payload := map[string]interface{}{
		"messages": []map[string]interface{}{
			{
				"data":       target.Data,
				"attributes": target.Attributes,
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("encode PubSub message: %w", err)
	}
	uri := api.gatewayBaseURL + "/v1/" + strings.TrimPrefix(target.TopicName, "/") + ":publish"
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	return api.deliver(req, "PubSub")
}

func (api *API) executeAppEngine(target *AppEngineTarget) (int, error) {
	if api.gatewayBaseURL == "" {
		return 0, fmt.Errorf("App Engine delivery requires MINISKY_GATEWAY_URL or Config.GatewayBaseURL")
	}
	uri := api.gatewayBaseURL + "/" + strings.TrimPrefix(target.RelativeUri, "/")
	req, err := newTargetRequest(target.HttpMethod, uri, target.Headers, target.Body)
	if err != nil {
		return 0, err
	}
	return api.deliver(req, "App Engine")
}

func newTargetRequest(method, uri string, headers map[string]string, body string) (*http.Request, error) {
	if method == "" {
		method = http.MethodPost
	}
	req, err := http.NewRequest(method, uri, bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	return req, nil
}

func (api *API) deliver(req *http.Request, targetName string) (int, error) {
	resp, err := api.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return resp.StatusCode, fmt.Errorf("%s delivery failed: %s", targetName, resp.Status)
	}
	return resp.StatusCode, nil
}

func extractJobName(path string) string {
	parts := strings.Split(path, "/jobs/")
	if len(parts) > 1 {
		return parts[0] + "/jobs/" + parts[1]
	}
	return ""
}

func extractProject(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "projects" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return "default-project"
}

func cloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	clone := *job
	if job.Status != nil {
		status := *job.Status
		clone.Status = &status
	}
	if job.HttpTarget != nil {
		target := *job.HttpTarget
		target.Headers = cloneStringMap(job.HttpTarget.Headers)
		clone.HttpTarget = &target
	}
	if job.PubsubTarget != nil {
		target := *job.PubsubTarget
		target.Attributes = cloneStringMap(job.PubsubTarget.Attributes)
		clone.PubsubTarget = &target
	}
	if job.AppEngineTarget != nil {
		target := *job.AppEngineTarget
		target.Headers = cloneStringMap(job.AppEngineTarget.Headers)
		clone.AppEngineTarget = &target
	}
	return &clone
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
