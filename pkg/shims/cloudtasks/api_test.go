package cloudtasks

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

	api := NewAPI()
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

	api := NewAPI()
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

	api := NewAPI()
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
