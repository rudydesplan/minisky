package scheduler

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type deliveredRequest struct {
	path        string
	method      string
	body        string
	header      string
	contentType string
}

func TestHTTPDeliveryRecordsSuccessAndHonorsRequest(t *testing.T) {
	request := make(chan struct {
		method string
		body   string
		header string
	}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		request <- struct {
			method string
			body   string
			header string
		}{r.Method, string(body), r.Header.Get("X-Scheduler-Test")}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	attemptTime := time.Date(2026, time.July, 25, 8, 0, 0, 0, time.UTC)
	api := NewAPIWithConfig(nil, Config{
		HTTPClient: target.Client(),
		Now:        func() time.Time { return attemptTime },
	})
	defer api.Close()

	job := &Job{
		Name:  "projects/test/locations/us-central1/jobs/http-success",
		State: "ENABLED",
		HttpTarget: &HttpTarget{
			Uri:        target.URL,
			HttpMethod: http.MethodPatch,
			Headers:    map[string]string{"X-Scheduler-Test": "present"},
			Body:       "payload",
		},
	}
	api.executeJob(job)

	got := <-request
	if got.method != http.MethodPatch || got.body != "payload" || got.header != "present" {
		t.Fatalf("unexpected delivered request: %+v", got)
	}
	assertAttempt(t, job, attemptTime, http.StatusNoContent, "", 0)
	if job.State != "ENABLED" {
		t.Fatalf("delivery changed lifecycle state to %q", job.State)
	}
}

func TestHTTPDeliveryRecordsNon2xxFailure(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "try again", http.StatusTemporaryRedirect)
	}))
	defer target.Close()

	api := NewAPIWithConfig(nil, Config{HTTPClient: target.Client()})
	defer api.Close()
	job := &Job{
		Name:       "projects/test/locations/us-central1/jobs/http-failure",
		State:      "ENABLED",
		HttpTarget: &HttpTarget{Uri: target.URL, HttpMethod: http.MethodGet},
	}

	api.executeJob(job)

	if job.LastAttemptStatus != http.StatusTemporaryRedirect {
		t.Fatalf("last attempt status = %d, want %d", job.LastAttemptStatus, http.StatusTemporaryRedirect)
	}
	if !strings.Contains(job.LastAttemptError, "307") || job.Status == nil || job.Status.Code == 0 {
		t.Fatalf("failure outcome not recorded: %+v", job)
	}
}

func TestPubSubDeliveryRecordsSuccess(t *testing.T) {
	request := make(chan deliveredRequest, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Messages []struct {
				Data       string            `json:"data"`
				Attributes map[string]string `json:"attributes"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode publish body: %v", err)
		}
		if len(payload.Messages) != 1 {
			t.Errorf("message count = %d, want 1", len(payload.Messages))
		} else if payload.Messages[0].Data != "cGF5bG9hZA==" || payload.Messages[0].Attributes["source"] != "scheduler" {
			t.Errorf("unexpected publish message: %+v", payload.Messages[0])
		}
		request <- deliveredRequest{
			path:        r.URL.Path,
			contentType: r.Header.Get("Content-Type"),
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer gateway.Close()

	api := NewAPIWithConfig(nil, Config{
		GatewayBaseURL: gateway.URL,
		HTTPClient:     gateway.Client(),
	})
	defer api.Close()
	job := &Job{
		Name:  "projects/test/locations/us-central1/jobs/pubsub-success",
		State: "ENABLED",
		PubsubTarget: &PubsubTarget{
			TopicName:  "projects/test/topics/events",
			Data:       "cGF5bG9hZA==",
			Attributes: map[string]string{"source": "scheduler"},
		},
	}

	api.executeJob(job)

	got := <-request
	if got.path != "/v1/projects/test/topics/events:publish" {
		t.Fatalf("publish path = %q", got.path)
	}
	if got.contentType != "application/json" {
		t.Fatalf("content type = %q", got.contentType)
	}
	assertAttempt(t, job, time.Time{}, http.StatusOK, "", 0)
}

func TestPubSubDeliveryRecordsFailure(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer gateway.Close()

	api := NewAPIWithConfig(nil, Config{
		GatewayBaseURL: gateway.URL,
		HTTPClient:     gateway.Client(),
	})
	defer api.Close()
	job := &Job{
		Name:         "projects/test/locations/us-central1/jobs/pubsub-failure",
		State:        "ENABLED",
		PubsubTarget: &PubsubTarget{TopicName: "projects/test/topics/events"},
	}

	api.executeJob(job)

	if job.LastAttemptStatus != http.StatusServiceUnavailable {
		t.Fatalf("last attempt status = %d, want %d", job.LastAttemptStatus, http.StatusServiceUnavailable)
	}
	if !strings.Contains(job.LastAttemptError, "503") || job.Status == nil || job.Status.Code == 0 {
		t.Fatalf("failure outcome not recorded: %+v", job)
	}
}

func TestAppEngineDeliveryHonorsRequestAndOutcome(t *testing.T) {
	for _, test := range []struct {
		name       string
		response   int
		statusCode int
		wantError  bool
	}{
		{name: "success", response: http.StatusAccepted, statusCode: 0},
		{name: "failure", response: http.StatusBadGateway, statusCode: 13, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := make(chan deliveredRequest, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				request <- deliveredRequest{
					path:   r.URL.Path,
					method: r.Method,
					body:   string(body),
					header: r.Header.Get("X-AppEngine-Test"),
				}
				w.WriteHeader(test.response)
			}))
			defer gateway.Close()

			api := NewAPIWithConfig(nil, Config{
				GatewayBaseURL: gateway.URL,
				HTTPClient:     gateway.Client(),
			})
			defer api.Close()
			job := &Job{
				Name:  "projects/test/locations/us-central1/jobs/appengine",
				State: "ENABLED",
				AppEngineTarget: &AppEngineTarget{
					RelativeUri: "/tasks/run",
					HttpMethod:  http.MethodPut,
					Headers:     map[string]string{"X-AppEngine-Test": "present"},
					Body:        "payload",
				},
			}

			api.executeJob(job)

			got := <-request
			if got.path != "/tasks/run" || got.method != http.MethodPut || got.body != "payload" || got.header != "present" {
				t.Fatalf("unexpected delivered request: %+v", got)
			}
			if job.LastAttemptStatus != test.response || job.Status == nil || job.Status.Code != test.statusCode {
				t.Fatalf("unexpected delivery outcome: %+v", job)
			}
			if (job.LastAttemptError != "") != test.wantError {
				t.Fatalf("last attempt error = %q, want error %t", job.LastAttemptError, test.wantError)
			}
		})
	}
}

func assertAttempt(t *testing.T, job *Job, attemptTime time.Time, status int, lastError string, statusCode int) {
	t.Helper()
	if attemptTime.IsZero() {
		if job.LastAttemptTime == "" {
			t.Fatal("last attempt time was not recorded")
		}
	} else if job.LastAttemptTime != attemptTime.Format(time.RFC3339Nano) {
		t.Fatalf("last attempt time = %q, want %q", job.LastAttemptTime, attemptTime.Format(time.RFC3339Nano))
	}
	if job.LastAttemptStatus != status || job.LastAttemptError != lastError {
		t.Fatalf("unexpected attempt outcome: %+v", job)
	}
	if job.Status == nil || job.Status.Code != statusCode {
		t.Fatalf("unexpected job status: %+v", job.Status)
	}
}
