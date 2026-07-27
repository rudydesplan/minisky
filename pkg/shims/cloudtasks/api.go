package cloudtasks

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/observability"
	"minisky/pkg/registry"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

func init() {
	state.MustRegisterEntryValidator(cloudTasksStateEntry, state.StrictEntryValidator[cloudTasksMetadata](nil))
	registry.Register("cloudtasks.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI()
	})
}

type Task struct {
	Name             string       `json:"name"`
	HTTPRequest      *HTTPRequest `json:"httpRequest,omitempty"`
	CreateTime       string       `json:"createTime"`
	ScheduleTime     string       `json:"scheduleTime,omitempty"`
	Status           string       `json:"status"` // Internal use
	AttemptCount     int          `json:"attemptCount,omitempty"`
	LastStatusCode   int          `json:"lastStatusCode,omitempty"`
	LastError        string       `json:"lastError,omitempty"`
	FirstAttemptTime string       `json:"firstAttemptTime,omitempty"`
	LastAttemptTime  string       `json:"lastAttemptTime,omitempty"`
}

type HTTPRequest struct {
	URL        string            `json:"url"`
	HTTPMethod string            `json:"httpMethod"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       string            `json:"body,omitempty"` // Base64
}

type Queue struct {
	Name        string      `json:"name"`
	State       string      `json:"state"`
	RetryConfig RetryConfig `json:"retryConfig,omitempty"`
}

type RetryConfig struct {
	MaxAttempts      int    `json:"maxAttempts,omitempty"`
	MaxRetryDuration string `json:"maxRetryDuration,omitempty"`
	MinBackoff       string `json:"minBackoff,omitempty"`
	MaxBackoff       string `json:"maxBackoff,omitempty"`
	MaxDoublings     int    `json:"maxDoublings,omitempty"`
}

type deliveryJob struct {
	cancel context.CancelFunc
}

type API struct {
	mu                sync.RWMutex
	persistMu         sync.Mutex
	store             stateStore
	queues            map[string]*Queue
	tasks             map[string][]*Task
	jobs              map[string]*deliveryJob
	logAPI            *logging.API
	client            *http.Client
	lookupIP          func(context.Context, string) ([]net.IP, error)
	dial              func(context.Context, string, string) (net.Conn, error)
	allowLocalTargets bool
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	closeOnce         sync.Once
	closed            bool
	initErr           error
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

const cloudTasksStateEntry = "cloudtasks/metadata"

type cloudTasksMetadata struct {
	Queues map[string]*Queue  `json:"queues"`
	Tasks  map[string][]*Task `json:"tasks"`
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Cloud Tasks] state disabled: %v", err)
		return newAPI(nil)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		log.Printf("[Shim: Cloud Tasks] state rehydration failed: %v", err)
		disabled := newAPI(nil)
		disabled.initErr = err
		return disabled
	}
	return api
}

func NewAPIWithStore(store stateStore) (*API, error) {
	api := newAPI(store)
	if store == nil {
		return api, nil
	}
	var persisted cloudTasksMetadata
	if err := store.Load(cloudTasksStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Cloud Tasks metadata: %w", err)
	}
	if persisted.Queues != nil {
		api.queues = persisted.Queues
	}
	if persisted.Tasks != nil {
		api.tasks = persisted.Tasks
	}
	api.mu.Lock()
	interrupted := api.markInterruptedTasksLocked()
	api.mu.Unlock()
	if interrupted {
		if err := api.persistMetadata(); err != nil {
			return nil, fmt.Errorf("persist interrupted Cloud Tasks metadata: %w", err)
		}
	}
	return api, nil
}

func newAPI(store stateStore) *API {
	ctx, cancel := context.WithCancel(context.Background())
	api := &API{
		store:             store,
		queues:            make(map[string]*Queue),
		tasks:             make(map[string][]*Task),
		jobs:              make(map[string]*deliveryJob),
		allowLocalTargets: os.Getenv("MINISKY_CLOUDTASKS_ALLOW_LOCAL_TARGETS") == "1",
		ctx:               ctx,
		cancel:            cancel,
	}
	api.lookupIP = func(ctx context.Context, host string) ([]net.IP, error) {
		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		result := make([]net.IP, 0, len(addresses))
		for _, address := range addresses {
			result = append(result, address.IP)
		}
		return result, err
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	api.dial = dialer.DialContext
	api.client = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext:       api.dialTaskTarget,
			DisableKeepAlives: true,
		},
	}
	return api
}

func (api *API) persistMetadata() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	metadata := cloneMetadata(api.queues, api.tasks)
	api.mu.RUnlock()
	return api.store.Save(cloudTasksStateEntry, metadata)
}

func (api *API) persistOrError(w http.ResponseWriter) bool {
	if err := api.persistMetadata(); err != nil {
		log.Printf("[Shim: Cloud Tasks] persist metadata: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":500,"status":"INTERNAL","message":"Failed to persist Cloud Tasks metadata"}}`))
		return false
	}
	return true
}

func (api *API) Close() {
	_ = api.Shutdown(context.Background())
}

// Shutdown stops pending delivery workers, waits within the caller's deadline,
// and persists the final task state.
func (api *API) Shutdown(ctx context.Context) error {
	api.closeOnce.Do(func() {
		api.mu.Lock()
		api.closed = true
		api.cancel()
		api.mu.Unlock()
	})
	done := make(chan struct{})
	go func() {
		api.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		api.mu.Lock()
		api.markInterruptedTasksLocked()
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			return fmt.Errorf("persist Cloud Tasks shutdown metadata: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (api *API) markInterruptedTasksLocked() bool {
	changed := false
	for _, tasks := range api.tasks {
		for _, task := range tasks {
			if task == nil || task.Status != "PENDING" && task.Status != "RETRYING" {
				continue
			}
			task.Status = "FAILED"
			if task.AttemptCount > 0 {
				task.LastError = "delivery interrupted by emulator restart or shutdown; target outcome is unknown; task was not replayed to avoid duplicate delivery"
			} else {
				task.LastError = "delivery interrupted by emulator restart or shutdown; task was not replayed"
			}
			changed = true
		}
	}
	return changed
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if logShim, ok := ctx.GetShim("logging.googleapis.com").(*logging.API); ok {
		api.logAPI = logShim
	}
	api.ResumePendingDeliveries()
}

func (api *API) pushLog(projectId, severity, resourceName, text string) {
	if api.logAPI == nil {
		return
	}
	api.logAPI.PushLog(projectId, severity, "cloud_tasks_queue", resourceName, text)
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if api.initErr != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":503,"status":"FAILED_PRECONDITION","message":"Cloud Tasks state is unavailable"}}`))
		return
	}

	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	log.Printf("[Shim: Cloud Tasks DEBUG] %s %s (Parts: %d)", r.Method, r.URL.Path, len(parts))

	if len(parts) < 3 || parts[1] != "projects" {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "discovery") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	project := parts[2]

	// Handle REST API
	// v2/projects/{project}/locations/{location}/queues
	if len(parts) >= 6 && parts[0] == "v2" && parts[3] == "locations" && parts[5] == "queues" {
		queueId := ""
		if len(parts) >= 7 {
			queueId = parts[6]
		}

		switch {
		case len(parts) == 6:
			if r.Method == http.MethodGet {
				api.listQueues(w, r, project)
				return
			}
			if r.Method == http.MethodPost {
				api.createQueue(w, r, project)
				return
			}
		case len(parts) == 7:
			if r.Method == http.MethodDelete {
				api.deleteQueue(w, r, project, queueId)
				return
			}
		case len(parts) >= 8 && parts[7] == "tasks":
			if len(parts) == 8 {
				if r.Method == http.MethodGet {
					api.listTasks(w, r, project, queueId)
					return
				}
				if r.Method == http.MethodPost {
					api.createTask(w, r, project, queueId)
					return
				}
			} else if len(parts) == 9 {
				if r.Method == http.MethodGet {
					api.getTask(w, r, project, queueId, parts[8])
					return
				}
				if r.Method == http.MethodDelete {
					api.deleteTask(w, r, project, queueId, parts[8])
					return
				}
			}
		}
	}

	log.Printf("[Shim ERROR: Cloud Tasks] Unhandled %s %s", r.Method, r.URL.Path)
	w.WriteHeader(http.StatusNotFound)
}

func (api *API) listQueues(w http.ResponseWriter, r *http.Request, project string) {
	api.mu.RLock()
	defer api.mu.RUnlock()

	var result []*Queue
	prefix := fmt.Sprintf("projects/%s/locations/us-central1/queues/", project)
	for name, q := range api.queues {
		if strings.HasPrefix(name, prefix) {
			result = append(result, q)
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"queues": result})
}

func (api *API) createQueue(w http.ResponseWriter, r *http.Request, project string) {
	var q Queue
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	api.mu.Lock()
	if _, exists := api.queues[q.Name]; exists {
		api.mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"error":{"code":409,"message":"Queue already exists"}}`))
		return
	}
	q.State = "RUNNING"
	api.queues[q.Name] = &q
	api.mu.Unlock()

	if !api.persistOrError(w) {
		api.mu.Lock()
		delete(api.queues, q.Name)
		api.mu.Unlock()
		return
	}
	api.pushLog(project, "INFO", q.Name, "Created queue")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(q)
}

func (api *API) deleteQueue(w http.ResponseWriter, r *http.Request, project, queueId string) {
	name := fmt.Sprintf("projects/%s/locations/us-central1/queues/%s", project, queueId)
	log.Printf("[Shim: Cloud Tasks] Attempting to delete queue: %s", name)

	api.persistMu.Lock()
	api.mu.Lock()
	queue, exists := api.queues[name]
	removedTasks := api.tasks[name]
	delete(api.queues, name)
	delete(api.tasks, name)
	metadata := cloneMetadata(api.queues, api.tasks)
	api.mu.Unlock()

	var err error
	if api.store != nil {
		err = api.store.Save(cloudTasksStateEntry, metadata)
	}
	if err != nil {
		api.mu.Lock()
		if exists {
			api.queues[name] = queue
		}
		if removedTasks != nil {
			api.tasks[name] = removedTasks
		}
		api.mu.Unlock()
		api.persistMu.Unlock()
		log.Printf("[Shim: Cloud Tasks] persist queue deletion: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":500,"status":"INTERNAL","message":"Failed to persist Cloud Tasks metadata"}}`))
		return
	}
	api.mu.Lock()
	for _, task := range removedTasks {
		if job := api.jobs[task.Name]; job != nil {
			job.cancel()
			delete(api.jobs, task.Name)
		}
	}
	api.mu.Unlock()
	api.persistMu.Unlock()
	if !exists {
		log.Printf("[Shim WARNING: Cloud Tasks] Queue not found for deletion: %s", name)
	} else {
		api.pushLog(project, "INFO", name, "Deleted queue")
	}
	w.WriteHeader(http.StatusOK)
}

func (api *API) listTasks(w http.ResponseWriter, r *http.Request, project, queueId string) {
	name := fmt.Sprintf("projects/%s/locations/us-central1/queues/%s", project, queueId)

	api.mu.RLock()
	storedTasks := api.tasks[name]
	tasks := make([]*Task, 0, len(storedTasks))
	for _, task := range storedTasks {
		taskCopy := *task
		tasks = append(tasks, &taskCopy)
	}
	api.mu.RUnlock()

	json.NewEncoder(w).Encode(map[string]interface{}{"tasks": tasks})
}

func (api *API) getTask(w http.ResponseWriter, r *http.Request, project, queueId, taskId string) {
	queueName := fmt.Sprintf("projects/%s/locations/us-central1/queues/%s", project, queueId)
	taskName := fmt.Sprintf("%s/tasks/%s", queueName, taskId)

	api.mu.RLock()
	for _, task := range api.tasks[queueName] {
		if task.Name == taskName {
			taskCopy := *task
			api.mu.RUnlock()
			json.NewEncoder(w).Encode(taskCopy)
			return
		}
	}
	api.mu.RUnlock()
	w.WriteHeader(http.StatusNotFound)
}

func (api *API) createTask(w http.ResponseWriter, r *http.Request, project, queueId string) {
	var body struct {
		Task *Task `json:"task"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	queueName := fmt.Sprintf("projects/%s/locations/us-central1/queues/%s", project, queueId)

	task := body.Task
	if task == nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if task.Name == "" {
		task.Name = fmt.Sprintf("%s/tasks/%d", queueName, time.Now().UnixNano())
	}
	task.CreateTime = time.Now().Format(time.RFC3339)
	task.Status = "PENDING"

	api.mu.Lock()
	api.tasks[queueName] = append(api.tasks[queueName], task)
	createdTask := *task
	queue := api.queues[queueName]
	shouldExecute := task.HTTPRequest != nil && !api.closed
	api.mu.Unlock()

	if !api.persistOrError(w) {
		api.mu.Lock()
		removeTaskLocked(api.tasks, queueName, task.Name)
		api.mu.Unlock()
		return
	}

	api.mu.Lock()
	if shouldExecute && !api.closed {
		retryConfig := RetryConfig{}
		if queue != nil {
			retryConfig = queue.RetryConfig
		}
		api.scheduleDeliveryLocked(project, queueName, task, retryConfig)
	}
	api.mu.Unlock()

	api.pushLog(project, "INFO", queueName, "Task created: "+task.Name)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(createdTask)
}

// ResumePendingDeliveries schedules persisted nonterminal work exactly once per
// API lifetime. Terminal tasks and tasks whose queue was deleted are ignored.
func (api *API) ResumePendingDeliveries() {
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.closed {
		return
	}
	for queueName, tasks := range api.tasks {
		queue := api.queues[queueName]
		if queue == nil || queue.State != "RUNNING" {
			continue
		}
		project := projectFromQueueName(queueName)
		for _, task := range tasks {
			if task == nil || task.HTTPRequest == nil ||
				task.Status != "PENDING" && task.Status != "RETRYING" {
				continue
			}
			api.scheduleDeliveryLocked(project, queueName, task, queue.RetryConfig)
		}
	}
}

func (api *API) scheduleDeliveryLocked(
	project, queueName string,
	task *Task,
	config RetryConfig,
) {
	if api.closed || api.jobs[task.Name] != nil {
		return
	}
	request := *task.HTTPRequest
	request.Headers = make(map[string]string, len(task.HTTPRequest.Headers))
	for name, value := range task.HTTPRequest.Headers {
		request.Headers[name] = value
	}
	ctx, cancel := context.WithCancel(api.ctx)
	job := &deliveryJob{cancel: cancel}
	api.jobs[task.Name] = job
	api.wg.Add(1)
	go api.executeTask(ctx, project, queueName, task.Name, task.ScheduleTime, request, config, job)
}

func (api *API) executeTask(
	ctx context.Context,
	project, queueName, taskName, scheduleTime string,
	request HTTPRequest,
	config RetryConfig,
	job *deliveryJob,
) {
	defer api.wg.Done()
	defer func() {
		api.mu.Lock()
		if api.jobs[taskName] == job {
			delete(api.jobs, taskName)
		}
		api.mu.Unlock()
	}()

	if scheduled, err := time.Parse(time.RFC3339Nano, scheduleTime); err == nil {
		delay := time.Until(scheduled)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
	}

	maxAttempts, minBackoff, maxBackoff, maxRetryDuration := normalizedRetryConfig(config)
	for {
		if ctx.Err() != nil {
			return
		}

		attempt, firstAttempt, exhausted, err := api.recordAttemptStart(
			queueName, taskName, maxAttempts, maxRetryDuration,
		)
		if err != nil {
			log.Printf("[Shim: Cloud Tasks] persist attempt start: %v", err)
			return
		}
		if exhausted {
			return
		}

		body, err := decodeTaskBody(request.Body)
		if err != nil {
			if persistErr := api.recordAttemptOutcome(queueName, taskName, 0, err, true); persistErr != nil {
				log.Printf("[Shim: Cloud Tasks] persist invalid task outcome: %v", persistErr)
			}
			return
		}
		if err = validateTaskHTTPRequest(request); err != nil {
			if persistErr := api.recordAttemptOutcome(queueName, taskName, 0, err, true); persistErr != nil {
				log.Printf("[Shim: Cloud Tasks] persist invalid target outcome: %v", persistErr)
			}
			return
		}
		pinnedContext, err := api.pinTaskTarget(ctx, request.URL)
		if err != nil {
			if persistErr := api.recordAttemptOutcome(queueName, taskName, 0, err, true); persistErr != nil {
				log.Printf("[Shim: Cloud Tasks] persist target resolution outcome: %v", persistErr)
			}
			return
		}

		method := request.HTTPMethod
		if method == "" {
			method = http.MethodPost
		}
		req, err := http.NewRequestWithContext(pinnedContext, method, request.URL, bytes.NewReader(body))
		if err == nil {
			for name, value := range request.Headers {
				req.Header.Set(name, value)
			}
		}

		statusCode := 0
		if err == nil {
			log.Printf("[Shim: Cloud Tasks] Executing task %s -> %s %s (attempt %d)", taskName, method, request.URL, attempt)
			var response *http.Response
			response, err = observability.Do(api.client, req)
			if response != nil {
				statusCode = response.StatusCode
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
				_ = response.Body.Close()
				if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
					err = fmt.Errorf("HTTP status %d", statusCode)
				}
			}
		}

		if ctx.Err() != nil {
			return
		}
		if err == nil {
			if persistErr := api.recordAttemptOutcome(queueName, taskName, statusCode, nil, true); persistErr != nil {
				log.Printf("[Shim: Cloud Tasks] persist successful task outcome: %v", persistErr)
				return
			}
			api.pushLog(project, "INFO", queueName, fmt.Sprintf("Task executed successfully: %s (Target: %s)", taskName, request.URL))
			return
		}

		delay := retryDelay(minBackoff, maxBackoff, config.MaxDoublings, attempt)
		retryDurationExpired := maxRetryDuration > 0 && time.Since(firstAttempt)+delay >= maxRetryDuration
		terminal := attempt >= maxAttempts || retryDurationExpired
		if persistErr := api.recordAttemptOutcome(queueName, taskName, statusCode, err, terminal); persistErr != nil {
			log.Printf("[Shim: Cloud Tasks] persist failed task outcome: %v", persistErr)
			return
		}
		if terminal {
			api.pushLog(project, "ERROR", queueName, fmt.Sprintf("Task failed after %d attempts: %s (%v)", attempt, taskName, err))
			return
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func normalizedRetryConfig(config RetryConfig) (int, time.Duration, time.Duration, time.Duration) {
	maxAttempts := config.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if maxAttempts > 100 {
		maxAttempts = 100
	}

	minBackoff := parseBoundedDuration(config.MinBackoff, 100*time.Millisecond, 5*time.Second)
	maxBackoff := parseBoundedDuration(config.MaxBackoff, time.Second, 5*time.Second)
	if maxBackoff < minBackoff {
		maxBackoff = minBackoff
	}
	maxRetryDuration := parseBoundedDuration(config.MaxRetryDuration, 0, 30*time.Second)
	return maxAttempts, minBackoff, maxBackoff, maxRetryDuration
}

func parseBoundedDuration(value string, fallback, maximum time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return fallback
	}
	if duration > maximum {
		return maximum
	}
	return duration
}

func retryDelay(minBackoff, maxBackoff time.Duration, maxDoublings, attempt int) time.Duration {
	if maxDoublings < 0 {
		maxDoublings = 0
	}
	delay := minBackoff
	doublings := attempt - 1
	if doublings > maxDoublings {
		doublings = maxDoublings
	}
	for i := 0; i < doublings && delay < maxBackoff; i++ {
		if delay > maxBackoff/2 {
			return maxBackoff
		}
		delay *= 2
	}
	if attempt-1 > maxDoublings {
		multiplier := attempt - maxDoublings
		if multiplier > 1 {
			if delay > maxBackoff/time.Duration(multiplier) {
				return maxBackoff
			}
			delay *= time.Duration(multiplier)
		}
	}
	if delay > maxBackoff {
		return maxBackoff
	}
	return delay
}

func (api *API) recordAttemptStart(
	queueName, taskName string,
	maxAttempts int,
	maxRetryDuration time.Duration,
) (int, time.Time, bool, error) {
	now := time.Now().UTC()
	api.mu.Lock()
	var task *Task
	for _, candidate := range api.tasks[queueName] {
		if candidate.Name == taskName {
			task = candidate
			break
		}
	}
	if task == nil || task.Status != "PENDING" && task.Status != "RETRYING" {
		api.mu.Unlock()
		return 0, time.Time{}, true, nil
	}
	previous := *task
	firstAttempt := parseAttemptTime(task.FirstAttemptTime)
	exhausted := task.AttemptCount >= maxAttempts ||
		maxRetryDuration > 0 && !firstAttempt.IsZero() && now.Sub(firstAttempt) >= maxRetryDuration
	if exhausted {
		task.Status = "FAILED"
		task.LastError = "delivery retry budget exhausted before replay"
	} else {
		if firstAttempt.IsZero() {
			firstAttempt = now
			task.FirstAttemptTime = now.Format(time.RFC3339Nano)
		}
		task.AttemptCount++
		task.LastAttemptTime = now.Format(time.RFC3339Nano)
		task.Status = "RETRYING"
	}
	attempt := task.AttemptCount
	api.mu.Unlock()

	if err := api.persistMetadata(); err != nil {
		api.mu.Lock()
		for _, current := range api.tasks[queueName] {
			if current.Name == taskName && current.AttemptCount == attempt {
				*current = previous
				break
			}
		}
		api.mu.Unlock()
		return 0, time.Time{}, true, err
	}
	return attempt, firstAttempt, exhausted, nil
}

func (api *API) recordAttemptOutcome(
	queueName, taskName string,
	statusCode int,
	err error,
	terminal bool,
) error {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.Lock()
	var previous *Task
	for _, task := range api.tasks[queueName] {
		if task.Name != taskName {
			continue
		}
		snapshot := *task
		previous = &snapshot
		task.LastStatusCode = statusCode
		if err == nil {
			task.Status = "COMPLETED"
			task.LastError = ""
		} else {
			task.LastError = err.Error()
			if terminal {
				task.Status = "FAILED"
			} else {
				task.Status = "RETRYING"
			}
		}
		break
	}
	metadata := cloneMetadata(api.queues, api.tasks)
	api.mu.Unlock()
	if previous == nil || api.store == nil {
		return nil
	}
	if persistErr := api.store.Save(cloudTasksStateEntry, metadata); persistErr != nil {
		api.mu.Lock()
		for _, task := range api.tasks[queueName] {
			if task.Name == taskName {
				*task = *previous
				break
			}
		}
		api.mu.Unlock()
		return persistErr
	}
	return nil
}

func parseAttemptTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func decodeTaskBody(encoded string) ([]byte, error) {
	const maxTaskBodyBytes = 1 << 20
	if base64.StdEncoding.DecodedLen(len(encoded)) > maxTaskBodyBytes {
		return nil, fmt.Errorf("decode HTTP body: body exceeds %d bytes", maxTaskBodyBytes)
	}
	body, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode HTTP body: %w", err)
	}
	if len(body) > maxTaskBodyBytes {
		return nil, fmt.Errorf("decode HTTP body: body exceeds %d bytes", maxTaskBodyBytes)
	}
	return body, nil
}

type pinnedTaskTarget struct {
	host      string
	port      string
	addresses []net.IP
}

type pinnedTaskTargetContextKey struct{}

func (api *API) pinTaskTarget(ctx context.Context, rawURL string) (context.Context, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return ctx, errors.New("unsafe Cloud Tasks HTTP target")
	}
	host := strings.TrimSuffix(strings.ToLower(target.Hostname()), ".")
	port := target.Port()
	if port == "" {
		if target.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	addresses := []net.IP(nil)
	if literal := net.ParseIP(host); literal != nil {
		addresses = append(addresses, literal)
	} else {
		addresses, err = api.lookupIP(ctx, host)
		if err != nil {
			return ctx, fmt.Errorf("resolve Cloud Tasks HTTP target: %w", err)
		}
	}
	if len(addresses) == 0 {
		return ctx, errors.New("resolve Cloud Tasks HTTP target: no addresses")
	}
	for _, address := range addresses {
		if !safeTaskTargetAddress(address, target.Scheme, api.allowLocalTargets) {
			return ctx, errors.New("unsafe Cloud Tasks HTTP target address")
		}
	}
	pinned := &pinnedTaskTarget{
		host: host, port: port, addresses: append([]net.IP(nil), addresses...),
	}
	return context.WithValue(ctx, pinnedTaskTargetContextKey{}, pinned), nil
}

func safeTaskTargetAddress(address net.IP, scheme string, allowLocal bool) bool {
	if address == nil || address.IsUnspecified() || address.IsMulticast() ||
		address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() ||
		address.IsPrivate() {
		return false
	}
	if address.IsLoopback() {
		return allowLocal && scheme == "http"
	}
	return address.IsGlobalUnicast()
}

func (api *API) dialTaskTarget(
	ctx context.Context,
	network, address string,
) (net.Conn, error) {
	pinned, _ := ctx.Value(pinnedTaskTargetContextKey{}).(*pinnedTaskTarget)
	host, port, err := net.SplitHostPort(address)
	if err != nil || pinned == nil ||
		strings.TrimSuffix(strings.ToLower(host), ".") != pinned.host || port != pinned.port {
		return nil, errors.New("Cloud Tasks target was not resolved and pinned")
	}
	var dialErrors []error
	for _, ip := range pinned.addresses {
		connection, dialErr := api.dial(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		dialErrors = append(dialErrors, dialErr)
	}
	return nil, errors.Join(dialErrors...)
}

func validateTaskHTTPRequest(request HTTPRequest) error {
	target, err := url.Parse(request.URL)
	if err != nil || target.Scheme != "http" && target.Scheme != "https" ||
		target.Host == "" || target.User != nil {
		return errors.New("unsafe Cloud Tasks HTTP target")
	}
	host := strings.TrimSuffix(strings.ToLower(target.Hostname()), ".")
	if host == "metadata.google.internal" || host == "metadata.google" {
		return errors.New("unsafe Cloud Tasks HTTP target")
	}
	if len(request.Headers) > 100 {
		return errors.New("Cloud Tasks HTTP headers exceed limit")
	}
	totalHeaderBytes := 0
	for name, value := range request.Headers {
		totalHeaderBytes += len(name) + len(value)
		if totalHeaderBytes > 64<<10 || strings.ContainsAny(name+value, "\r\n") {
			return errors.New("Cloud Tasks HTTP headers exceed limit")
		}
	}
	return nil
}

func projectFromQueueName(queueName string) string {
	parts := strings.Split(queueName, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}

func (api *API) deleteTask(w http.ResponseWriter, r *http.Request, project, queueId, taskId string) {
	queueName := fmt.Sprintf("projects/%s/locations/us-central1/queues/%s", project, queueId)
	taskName := fmt.Sprintf("%s/tasks/%s", queueName, taskId)
	log.Printf("[Shim: Cloud Tasks] Attempting to delete task: %s", taskName)

	api.persistMu.Lock()
	api.mu.Lock()
	tasks := api.tasks[queueName]
	for i, t := range tasks {
		if t.Name == taskName {
			remaining := append([]*Task(nil), tasks[:i]...)
			remaining = append(remaining, tasks[i+1:]...)
			api.tasks[queueName] = remaining
			metadata := cloneMetadata(api.queues, api.tasks)
			api.mu.Unlock()
			var err error
			if api.store != nil {
				err = api.store.Save(cloudTasksStateEntry, metadata)
			}
			if err != nil {
				api.mu.Lock()
				api.tasks[queueName] = tasks
				api.mu.Unlock()
				api.persistMu.Unlock()
				log.Printf("[Shim: Cloud Tasks] persist task deletion: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"code":500,"status":"INTERNAL","message":"Failed to persist Cloud Tasks metadata"}}`))
				return
			}
			api.mu.Lock()
			if job := api.jobs[taskName]; job != nil {
				job.cancel()
				delete(api.jobs, taskName)
			}
			api.mu.Unlock()
			api.persistMu.Unlock()
			log.Printf("[Shim: Cloud Tasks] Successfully deleted task: %s", taskName)
			api.pushLog(project, "INFO", queueName, "Task deleted: "+taskName)
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	api.mu.Unlock()
	api.persistMu.Unlock()

	log.Printf("[Shim WARNING: Cloud Tasks] Task not found for deletion: %s", taskName)
	w.WriteHeader(http.StatusNotFound)
}

func cloneMetadata(queues map[string]*Queue, tasks map[string][]*Task) cloudTasksMetadata {
	result := cloudTasksMetadata{
		Queues: make(map[string]*Queue, len(queues)),
		Tasks:  make(map[string][]*Task, len(tasks)),
	}
	for name, queue := range queues {
		clone := *queue
		result.Queues[name] = &clone
	}
	for queueName, queueTasks := range tasks {
		clones := make([]*Task, len(queueTasks))
		for i, task := range queueTasks {
			clone := *task
			if task.HTTPRequest != nil {
				request := *task.HTTPRequest
				request.Headers = make(map[string]string, len(task.HTTPRequest.Headers))
				for name, value := range task.HTTPRequest.Headers {
					request.Headers[name] = value
				}
				clone.HTTPRequest = &request
			}
			clones[i] = &clone
		}
		result.Tasks[queueName] = clones
	}
	return result
}

func removeTaskLocked(tasks map[string][]*Task, queueName, taskName string) {
	queueTasks := tasks[queueName]
	for i, task := range queueTasks {
		if task.Name == taskName {
			tasks[queueName] = append(queueTasks[:i], queueTasks[i+1:]...)
			return
		}
	}
}
