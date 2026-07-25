package cloudtasks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	queueName := createTestQueue(t, api, RetryConfig{MaxAttempts: 1})
	taskName := createTestTask(t, api, queueName, &HTTPRequest{URL: target.URL})
	waitForTaskStatus(t, api, queueName, taskName, "COMPLETED")
	api.Close()

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatalf("restart API: %v", err)
	}
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

func TestRestartMarksInterruptedTasksTerminal(t *testing.T) {
	store := &memoryStateStore{}
	if err := store.Save(cloudTasksStateEntry, cloudTasksMetadata{
		Queues: map[string]*Queue{
			"projects/test/locations/us-central1/queues/q": {
				Name:  "projects/test/locations/us-central1/queues/q",
				State: "RUNNING",
			},
		},
		Tasks: map[string][]*Task{
			"projects/test/locations/us-central1/queues/q": {
				{Name: "pending", Status: "PENDING"},
				{Name: "retrying", Status: "RETRYING", AttemptCount: 2},
				{Name: "done", Status: "FAILED", AttemptCount: 3},
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

	api.mu.RLock()
	defer api.mu.RUnlock()
	tasks := api.tasks["projects/test/locations/us-central1/queues/q"]
	if tasks[0].Status != "FAILED" || !strings.Contains(tasks[0].LastError, "interrupted") {
		t.Fatalf("pending task restart state = %+v", tasks[0])
	}
	if tasks[1].Status != "FAILED" || tasks[1].AttemptCount != 2 {
		t.Fatalf("retrying task restart state = %+v", tasks[1])
	}
	if tasks[2].Status != "FAILED" || tasks[2].LastError != "" {
		t.Fatalf("terminal task changed on restart = %+v", tasks[2])
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

type memoryStateStore struct {
	mu   sync.Mutex
	data []byte
}

type deleteOrderStore struct {
	fail  bool
	saved atomic.Bool
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

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal test value: %v", err)
	}
	return string(data)
}
