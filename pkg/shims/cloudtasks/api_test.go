package cloudtasks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestHTTPTaskDeliverySuccess(t *testing.T) {
	requests := make(chan struct {
		method string
		body   string
		header string
	}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		requests <- struct {
			method string
			body   string
			header string
		}{r.Method, string(body), r.Header.Get("X-Task-Test")}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	api := newMemoryAPI(t)
	defer api.Close()
	queueName := createTestQueue(t, api, RetryConfig{MaxAttempts: 1})
	taskName := createTestTask(t, api, queueName, &HTTPRequest{
		URL:        target.URL,
		HTTPMethod: http.MethodPut,
		Headers:    map[string]string{"X-Task-Test": "delivered"},
		Body:       base64.StdEncoding.EncodeToString([]byte("payload")),
	})

	select {
	case got := <-requests:
		if got.method != http.MethodPut || got.body != "payload" || got.header != "delivered" {
			t.Fatalf("unexpected delivered request: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for HTTP delivery")
	}

	task := waitForTaskStatus(t, api, queueName, taskName, "COMPLETED")
	if task.AttemptCount != 1 || task.LastStatusCode != http.StatusNoContent || task.LastError != "" {
		t.Fatalf("unexpected completed task outcome: %+v", task)
	}
}

func TestHTTPTaskRetriesTransientFailureThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	api := newMemoryAPI(t)
	defer api.Close()
	queueName := createTestQueue(t, api, RetryConfig{
		MaxAttempts: 3,
		MinBackoff:  "5ms",
		MaxBackoff:  "10ms",
	})
	taskName := createTestTask(t, api, queueName, &HTTPRequest{
		URL:        target.URL,
		HTTPMethod: http.MethodPost,
	})

	task := waitForTaskStatus(t, api, queueName, taskName, "COMPLETED")
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 HTTP attempts, got %d", got)
	}
	if task.AttemptCount != 2 || task.LastStatusCode != http.StatusOK || task.LastError != "" {
		t.Fatalf("unexpected retried task outcome: %+v", task)
	}
}

func TestHTTPTaskTerminalFailure(t *testing.T) {
	var attempts atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(w, "still broken", http.StatusInternalServerError)
	}))
	defer target.Close()

	api := newMemoryAPI(t)
	defer api.Close()
	queueName := createTestQueue(t, api, RetryConfig{
		MaxAttempts: 3,
		MinBackoff:  "2ms",
		MaxBackoff:  "4ms",
	})
	taskName := createTestTask(t, api, queueName, &HTTPRequest{
		URL:        target.URL,
		HTTPMethod: http.MethodGet,
	})

	task := waitForTaskStatus(t, api, queueName, taskName, "FAILED")
	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 HTTP attempts, got %d", got)
	}
	if task.AttemptCount != 3 || task.LastStatusCode != http.StatusInternalServerError {
		t.Fatalf("unexpected terminal task outcome: %+v", task)
	}
	if !strings.Contains(task.LastError, "500") {
		t.Fatalf("expected terminal error to contain status code, got %q", task.LastError)
	}
}

func TestGetTaskReturnsExactNamedTask(t *testing.T) {
	const queueName = "projects/test/locations/us-central1/queues/q"
	const taskName = queueName + "/tasks/wanted"
	api := newMemoryAPI(t)
	defer api.Close()
	api.tasks[queueName] = []*Task{
		{Name: queueName + "/tasks/other", Status: "COMPLETED"},
		{Name: taskName, Status: "FAILED", AttemptCount: 2, LastStatusCode: http.StatusInternalServerError},
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v2/"+taskName, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("get task status = %d, body = %s", response.Code, response.Body.String())
	}
	var task Task
	if err := json.NewDecoder(response.Body).Decode(&task); err != nil {
		t.Fatal(err)
	}
	if task.Name != taskName || task.Status != "FAILED" || task.AttemptCount != 2 ||
		task.LastStatusCode != http.StatusInternalServerError {
		t.Fatalf("get task = %#v", task)
	}

	missing := httptest.NewRecorder()
	api.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/v2/"+queueName+"/tasks/missing", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing task status = %d, body = %s", missing.Code, missing.Body.String())
	}
}

func TestTaskStateSurvivesRestartWithoutReplay(t *testing.T) {
	store := &memoryStateStore{}
	var deliveries atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deliveries.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatalf("create API: %v", err)
	}
	api.allowLocalTargets = true
	queueName := createTestQueue(t, api, RetryConfig{MaxAttempts: 1})
	taskName := createTestTask(t, api, queueName, &HTTPRequest{URL: target.URL})
	waitForTaskStatus(t, api, queueName, taskName, "COMPLETED")
	api.Close()

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatalf("restart API: %v", err)
	}
	restarted.allowLocalTargets = true
	defer restarted.Close()
	task := waitForTaskStatus(t, restarted, queueName, taskName, "COMPLETED")
	if task.AttemptCount != 1 {
		t.Fatalf("attempt count after restart = %d, want 1", task.AttemptCount)
	}
	time.Sleep(20 * time.Millisecond)
	if got := deliveries.Load(); got != 1 {
		t.Fatalf("terminal task replayed after restart: deliveries = %d", got)
	}
}

func TestRestartTerminatesUnfinishedTasksWithoutReplay(t *testing.T) {
	store := &memoryStateStore{}
	var deliveries atomic.Int32
	queueName := "projects/test/locations/us-central1/queues/q"
	if err := store.Save(cloudTasksStateEntry, cloudTasksMetadata{
		Queues: map[string]*Queue{
			queueName: {
				Name: queueName, State: "RUNNING",
				RetryConfig: RetryConfig{MaxAttempts: 3, MinBackoff: "1ms"},
			},
		},
		Tasks: map[string][]*Task{
			queueName: {
				{Name: queueName + "/tasks/pending", Status: "PENDING", HTTPRequest: &HTTPRequest{URL: "https://example.invalid"}},
				{Name: queueName + "/tasks/retrying", Status: "RETRYING", AttemptCount: 1, HTTPRequest: &HTTPRequest{URL: "https://example.invalid"}},
				{Name: queueName + "/tasks/done", Status: "COMPLETED", AttemptCount: 1, HTTPRequest: &HTTPRequest{URL: "https://example.invalid"}},
				{Name: queueName + "/tasks/failed", Status: "FAILED", AttemptCount: 3, HTTPRequest: &HTTPRequest{URL: "https://example.invalid"}},
			},
		},
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatalf("restart API: %v", err)
	}
	defer api.Close()
	api.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		deliveries.Add(1)
		return nil, errors.New("restart must not perform outbound delivery")
	})}
	api.ResumePendingDeliveries()
	pending := waitForTaskStatus(t, api, queueName, queueName+"/tasks/pending", "FAILED")
	retrying := waitForTaskStatus(t, api, queueName, queueName+"/tasks/retrying", "FAILED")
	if pending.AttemptCount != 0 || retrying.AttemptCount != 1 {
		t.Fatalf("restart consumed attempts: pending=%+v retrying=%+v", pending, retrying)
	}
	if !strings.Contains(pending.LastError, "interrupted") ||
		!strings.Contains(retrying.LastError, "target outcome is unknown") ||
		!strings.Contains(retrying.LastError, "duplicate") {
		t.Fatalf("restart errors do not disclose interruption risk: pending=%+v retrying=%+v",
			pending, retrying)
	}
	if got := deliveries.Load(); got != 0 {
		t.Fatalf("restart performed %d outbound deliveries", got)
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if api.tasks[queueName][2].Status != "COMPLETED" || api.tasks[queueName][3].Status != "FAILED" {
		t.Fatalf("terminal tasks changed: %+v", api.tasks[queueName])
	}
}

func TestRestartInterruptionSaveFailureLeavesDurableStateUntouched(t *testing.T) {
	const queueName = "projects/test/locations/us-central1/queues/q"
	store := &failNextStateStore{}
	if err := store.Save(cloudTasksStateEntry, cloudTasksMetadata{
		Queues: map[string]*Queue{queueName: {Name: queueName, State: "RUNNING"}},
		Tasks: map[string][]*Task{queueName: {
			{Name: queueName + "/tasks/retrying", Status: "RETRYING", AttemptCount: 1},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	store.failNext.Store(true)

	api, err := NewAPIWithStore(store)
	if err == nil {
		api.Close()
		t.Fatal("restart succeeded after interruption state failed to persist")
	}
	var persisted cloudTasksMetadata
	if loadErr := store.Load(cloudTasksStateEntry, &persisted); loadErr != nil {
		t.Fatal(loadErr)
	}
	task := persisted.Tasks[queueName][0]
	if task.Status != "RETRYING" || task.AttemptCount != 1 {
		t.Fatalf("failed restart changed durable task: %+v", task)
	}
}

func TestAttemptIsPersistedBeforeOutboundDelivery(t *testing.T) {
	store := &memoryStateStore{}
	persistedBeforeDelivery := make(chan bool, 1)
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api.allowLocalTargets = true
	defer api.Close()
	queueName := createTestQueue(t, api, RetryConfig{MaxAttempts: 1})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var metadata cloudTasksMetadata
		err := store.Load(cloudTasksStateEntry, &metadata)
		task := metadata.Tasks[queueName][0]
		persistedBeforeDelivery <- err == nil && task.Status == "RETRYING" && task.AttemptCount == 1
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	taskName := createTestTask(t, api, queueName, &HTTPRequest{URL: target.URL})
	if persisted := <-persistedBeforeDelivery; !persisted {
		t.Fatal("attempt was not durably reserved before outbound delivery")
	}
	waitForTaskStatus(t, api, queueName, taskName, "COMPLETED")
}

func TestShutdownPersistsInterruptedBackoffAsTerminal(t *testing.T) {
	store := &memoryStateStore{}
	var attempts atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api.allowLocalTargets = true
	queueName := createTestQueue(t, api, RetryConfig{
		MaxAttempts: 3, MinBackoff: "500ms", MaxBackoff: "500ms",
	})
	taskName := createTestTask(t, api, queueName, &HTTPRequest{URL: target.URL})
	waitForTaskRetryOutcome(t, api, queueName, taskName)
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.allowLocalTargets = true
	defer restarted.Close()
	restarted.ResumePendingDeliveries()
	task := waitForTaskStatus(t, restarted, queueName, taskName, "FAILED")
	if task.AttemptCount != 1 || attempts.Load() != 1 ||
		!strings.Contains(task.LastError, "interrupted") {
		t.Fatalf("interrupted task=%+v outbound attempts=%d", task, attempts.Load())
	}
}

func TestAcceptedTaskHonorsSchedule(t *testing.T) {
	store := &memoryStateStore{}
	delivered := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api.allowLocalTargets = true
	defer api.Close()
	queueName := createTestQueue(t, api, RetryConfig{MaxAttempts: 3})
	scheduleTime := time.Now().Add(50 * time.Millisecond).Format(time.RFC3339Nano)
	body := fmt.Sprintf(`{"task":{"scheduleTime":%q,"httpRequest":{"url":%q}}}`, scheduleTime, target.URL)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "/v2/"+queueName+"/tasks", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("create scheduled task: %d: %s", response.Code, response.Body.String())
	}
	var created Task
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
		t.Fatal("future task delivered before its schedule time")
	case <-time.After(10 * time.Millisecond):
	}
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("scheduled replay was not delivered")
	}
	waitForTaskStatus(t, api, queueName, created.Name, "COMPLETED")
}

func TestAcceptedTaskRejectsUnsafeOrOversizedRequestsWithoutOutboundIO(t *testing.T) {
	tooManyHeaders := make(map[string]string, 101)
	for index := range 101 {
		tooManyHeaders[fmt.Sprintf("X-Test-%d", index)] = "value"
	}
	tests := []struct {
		name    string
		request *HTTPRequest
	}{
		{name: "metadata", request: &HTTPRequest{URL: "http://169.254.169.254/computeMetadata/v1/"}},
		{name: "private", request: &HTTPRequest{URL: "http://10.0.0.1/task"}},
		{name: "credentials", request: &HTTPRequest{URL: "http://user:pass@example.com/task"}},
		{name: "headers", request: &HTTPRequest{URL: "https://example.com/task", Headers: tooManyHeaders}},
		{name: "body", request: &HTTPRequest{
			URL:  "https://example.com/task",
			Body: base64.StdEncoding.EncodeToString(make([]byte, (1<<20)+1)),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &memoryStateStore{}
			api, err := NewAPIWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			defer api.Close()
			var calls atomic.Int32
			api.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return nil, errors.New("outbound I/O must not occur")
			})}
			queueName := createTestQueue(t, api, RetryConfig{MaxAttempts: 1})
			taskName := createTestTask(t, api, queueName, test.request)
			task := waitForTaskStatus(t, api, queueName, taskName, "FAILED")
			if calls.Load() != 0 || task.AttemptCount != 1 {
				t.Fatalf("task=%+v outbound calls=%d", task, calls.Load())
			}
		})
	}
}

func TestTaskTargetResolutionPinsValidatedAddressAgainstRebinding(t *testing.T) {
	api := newAPI(nil)
	defer api.Close()
	var resolutions atomic.Int32
	api.lookupIP = func(context.Context, string) ([]net.IP, error) {
		if resolutions.Add(1) == 1 {
			return []net.IP{net.ParseIP("203.0.113.10")}, nil
		}
		return []net.IP{net.ParseIP("169.254.169.254")}, nil
	}
	var dialed string
	dialFailure := errors.New("deterministic dial stop")
	api.dial = func(_ context.Context, _, address string) (net.Conn, error) {
		dialed = address
		return nil, dialFailure
	}

	ctx, err := api.pinTaskTarget(context.Background(), "https://task.example.test/deliver")
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.dialTaskTarget(ctx, "tcp", "task.example.test:443")
	if !errors.Is(err, dialFailure) {
		t.Fatalf("dial error = %v", err)
	}
	if resolutions.Load() != 1 {
		t.Fatalf("resolver calls = %d, want one pinned resolution", resolutions.Load())
	}
	if dialed != "203.0.113.10:443" {
		t.Fatalf("dialed address = %q", dialed)
	}
}

func TestTaskTargetResolutionRejectsUnsafeAnswersAndScopesLocalException(t *testing.T) {
	tests := []struct {
		name       string
		address    string
		scheme     string
		allowLocal bool
		wantError  bool
	}{
		{name: "private", address: "10.0.0.1", scheme: "https", wantError: true},
		{name: "link-local-metadata", address: "169.254.169.254", scheme: "http", wantError: true},
		{name: "loopback-default", address: "127.0.0.1", scheme: "http", wantError: true},
		{name: "loopback-explicit-http", address: "127.0.0.1", scheme: "http", allowLocal: true},
		{name: "loopback-explicit-https", address: "127.0.0.1", scheme: "https", allowLocal: true, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newAPI(nil)
			defer api.Close()
			api.allowLocalTargets = test.allowLocal
			api.lookupIP = func(context.Context, string) ([]net.IP, error) {
				return []net.IP{net.ParseIP(test.address)}, nil
			}
			_, err := api.pinTaskTarget(
				context.Background(), test.scheme+"://target.example.test/deliver",
			)
			if (err != nil) != test.wantError {
				t.Fatalf("pin error = %v, wantError=%v", err, test.wantError)
			}
		})
	}
}

func TestShutdownHonorsContextAndCompletesAfterWorkersExit(t *testing.T) {
	api := newAPI(nil)
	api.wg.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := api.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want deadline exceeded", err)
	}
	api.wg.Done()
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown after worker exit: %v", err)
	}
	api.mu.RLock()
	closed := api.closed
	api.mu.RUnlock()
	if !closed {
		t.Fatal("shutdown did not close task delivery")
	}
}

func TestCorruptStateDisablesCloudTasksInsteadOfOverwriting(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-tasks")
	store, err := state.New(root, "corrupt-tasks")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(cloudTasksStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI()
	defer api.Close()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/v2/projects/test/locations/us-central1/queues", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(cloudTasksStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

func TestQueueAndTaskDeletionRemainAtomicWithPersistence(t *testing.T) {
	const queueName = "projects/test/locations/us-central1/queues/q"
	const taskName = queueName + "/tasks/t"
	for _, test := range []struct {
		name string
		path string
	}{
		{name: "queue", path: "/v2/" + queueName},
		{name: "task", path: "/v2/" + taskName},
	} {
		t.Run(test.name+" rollback", func(t *testing.T) {
			store := &deleteOrderStore{fail: true}
			api := newAPI(store)
			defer api.Close()
			var canceled atomic.Bool
			api.queues[queueName] = &Queue{Name: queueName}
			api.tasks[queueName] = []*Task{{Name: taskName, Status: "PENDING"}}
			api.jobs[taskName] = &deliveryJob{cancel: func() { canceled.Store(true) }}

			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, test.path, nil))

			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if canceled.Load() {
				t.Fatal("worker was canceled before deletion persisted")
			}
			if api.queues[queueName] == nil || len(api.tasks[queueName]) != 1 || api.jobs[taskName] == nil {
				t.Fatal("memory deletion was not rolled back")
			}
		})

		t.Run(test.name+" persists before cancel", func(t *testing.T) {
			store := &deleteOrderStore{}
			api := newAPI(store)
			defer api.Close()
			var canceledBeforeSave atomic.Bool
			api.queues[queueName] = &Queue{Name: queueName}
			api.tasks[queueName] = []*Task{{Name: taskName, Status: "PENDING"}}
			api.jobs[taskName] = &deliveryJob{cancel: func() {
				if !store.saved.Load() {
					canceledBeforeSave.Store(true)
				}
			}}

			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, test.path, nil))

			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if canceledBeforeSave.Load() {
				t.Fatal("worker was canceled before deletion persisted")
			}
		})
	}
}

func TestTerminalOutcomeRollsBackWhenSaveFails(t *testing.T) {
	const queueName = "projects/test/locations/us-central1/queues/q"
	const taskName = queueName + "/tasks/t"
	store := &terminalFailStateStore{}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	api.queues[queueName] = &Queue{Name: queueName, State: "RUNNING"}
	api.tasks[queueName] = []*Task{{
		Name: taskName, Status: "RETRYING", AttemptCount: 1,
	}}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	store.failTerminal.Store(true)

	err = api.recordAttemptOutcome(queueName, taskName, http.StatusNoContent, nil, true)
	if err == nil {
		t.Fatal("terminal outcome unexpectedly persisted")
	}
	api.mu.RLock()
	live := *api.tasks[queueName][0]
	api.mu.RUnlock()
	if live.Status != "RETRYING" || live.LastStatusCode != 0 {
		t.Fatalf("failed terminal save changed live task: %+v", live)
	}
	var durable cloudTasksMetadata
	if err := store.Load(cloudTasksStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if task := durable.Tasks[queueName][0]; task.Status != "RETRYING" || task.LastStatusCode != 0 {
		t.Fatalf("failed terminal save changed durable task: %+v", task)
	}
}

func TestConcurrentTerminalOutcomesPersistCompleteSnapshot(t *testing.T) {
	const queueName = "projects/test/locations/us-central1/queues/q"
	store := &memoryStateStore{}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	api.queues[queueName] = &Queue{Name: queueName, State: "RUNNING"}
	api.tasks[queueName] = []*Task{
		{Name: queueName + "/tasks/a", Status: "RETRYING", AttemptCount: 1},
		{Name: queueName + "/tasks/b", Status: "RETRYING", AttemptCount: 1},
	}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	var workers sync.WaitGroup
	for _, task := range api.tasks[queueName] {
		workers.Add(1)
		go func(name string) {
			defer workers.Done()
			if err := api.recordAttemptOutcome(queueName, name, http.StatusNoContent, nil, true); err != nil {
				t.Errorf("record %s: %v", name, err)
			}
		}(task.Name)
	}
	workers.Wait()

	var durable cloudTasksMetadata
	if err := store.Load(cloudTasksStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	for _, task := range durable.Tasks[queueName] {
		if task.Status != "COMPLETED" || task.LastStatusCode != http.StatusNoContent {
			t.Fatalf("incomplete concurrent snapshot: %+v", durable.Tasks[queueName])
		}
	}
}

type memoryStateStore struct {
	mu   sync.Mutex
	data []byte
}

type failNextStateStore struct {
	memoryStateStore
	failNext atomic.Bool
}

type terminalFailStateStore struct {
	memoryStateStore
	failTerminal atomic.Bool
}

type deleteOrderStore struct {
	fail  bool
	saved atomic.Bool
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func (s *deleteOrderStore) Load(string, any) error { return state.ErrNotFound }

func (s *deleteOrderStore) Save(string, any) error {
	if s.fail {
		return errors.New("injected save failure")
	}
	s.saved.Store(true)
	return nil
}

func (s *memoryStateStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *memoryStateStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data = data
	return nil
}

func (s *failNextStateStore) Save(key string, value any) error {
	if s.failNext.CompareAndSwap(true, false) {
		return errors.New("injected save failure")
	}
	return s.memoryStateStore.Save(key, value)
}

func (s *terminalFailStateStore) Save(key string, value any) error {
	if s.failTerminal.Load() {
		metadata, ok := value.(cloudTasksMetadata)
		if ok {
			for _, tasks := range metadata.Tasks {
				for _, task := range tasks {
					if task.Status == "COMPLETED" {
						return errors.New("injected terminal save failure")
					}
				}
			}
		}
	}
	return s.memoryStateStore.Save(key, value)
}

var _ stateStore = (*memoryStateStore)(nil)

func createTestQueue(t *testing.T, api *API, retryConfig RetryConfig) string {
	t.Helper()
	const queueName = "projects/test/locations/us-central1/queues/test-queue"
	body := fmt.Sprintf(`{"name":%q,"retryConfig":%s}`, queueName, mustJSON(t, retryConfig))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/projects/test/locations/us-central1/queues", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create queue returned %d: %s", rec.Code, rec.Body.String())
	}
	return queueName
}

func newMemoryAPI(t *testing.T) *API {
	t.Helper()
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatalf("create in-memory API: %v", err)
	}
	api.allowLocalTargets = true
	return api
}

func createTestTask(t *testing.T, api *API, queueName string, request *HTTPRequest) string {
	t.Helper()
	body := fmt.Sprintf(`{"task":{"httpRequest":%s}}`, mustJSON(t, request))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/"+queueName+"/tasks", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create task returned %d: %s", rec.Code, rec.Body.String())
	}
	var task Task
	if err := json.NewDecoder(rec.Body).Decode(&task); err != nil {
		t.Fatalf("decode created task: %v", err)
	}
	return task.Name
}

func waitForTaskStatus(t *testing.T, api *API, queueName, taskName, status string) Task {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		api.mu.RLock()
		for _, task := range api.tasks[queueName] {
			if task.Name == taskName && task.Status == status {
				result := *task
				api.mu.RUnlock()
				return result
			}
		}
		api.mu.RUnlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for task %s to reach %s", taskName, status)
	return Task{}
}

func waitForTaskRetryOutcome(t *testing.T, api *API, queueName, taskName string) Task {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		api.mu.RLock()
		for _, task := range api.tasks[queueName] {
			if task.Name == taskName && task.Status == "RETRYING" && task.LastError != "" {
				result := *task
				api.mu.RUnlock()
				return result
			}
		}
		api.mu.RUnlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for task %s retry outcome", taskName)
	return Task{}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test value: %v", err)
	}
	return string(data)
}
