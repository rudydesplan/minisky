package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type deliveredRequest struct {
	path        string
	method      string
	body        string
	header      string
	contentType string
}

var _ interface {
	Shutdown(context.Context) error
} = (*API)(nil)

func TestVersionedRunPathFindsCreatedJob(t *testing.T) {
	delivered := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	api := NewAPIWithConfig(nil, Config{HTTPClient: target.Client()})
	defer api.Close()

	body := `{"name":"projects/test/locations/us-central1/jobs/http","schedule":"0 0 1 1 *","httpTarget":{"uri":"` + target.URL + `","httpMethod":"POST"}}`
	create := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs", strings.NewReader(body))
	create.Header.Set("Content-Type", "application/json")
	createResponse := httptest.NewRecorder()
	api.ServeHTTP(createResponse, create)
	if createResponse.Code != http.StatusOK {
		t.Fatalf("create job returned %d: %s", createResponse.Code, createResponse.Body.String())
	}

	run := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs/http:run", nil)
	runResponse := httptest.NewRecorder()
	api.ServeHTTP(runResponse, run)
	if runResponse.Code != http.StatusOK {
		t.Fatalf("run job returned %d: %s", runResponse.Code, runResponse.Body.String())
	}

	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("scheduled HTTP target was not invoked")
	}
}

func TestManualRunOutlivesTriggerRequestContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(started)
		<-release
		if err := request.Context().Err(); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}
	api := NewAPIWithConfig(nil, Config{HTTPClient: client})
	defer api.Close()
	const name = "projects/test/locations/us-central1/jobs/manual"
	api.jobs[name] = &Job{
		Name: name, State: "ENABLED",
		HttpTarget: &HttpTarget{Uri: "http://127.0.0.1:18080/deliver", HttpMethod: http.MethodPost},
	}

	triggerContext, cancelTrigger := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/v1/"+name+":run", nil).WithContext(triggerContext)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("run status = %d, body = %s", response.Code, response.Body.String())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manual execution did not start")
	}
	cancelTrigger()
	close(release)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		api.mu.RLock()
		job := cloneJob(api.jobs[name])
		api.mu.RUnlock()
		if job.Status != nil {
			if job.Status.Code != 0 || job.LastAttemptStatus != http.StatusNoContent ||
				job.LastAttemptError != "" {
				t.Fatalf("manual execution outcome = %+v", job)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("manual execution outcome was not recorded")
}

func TestShutdownCancelsAndWaitsForManualExecution(t *testing.T) {
	started := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(started)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	api := NewAPIWithConfig(nil, Config{HTTPClient: client})
	const name = "projects/test/locations/us-central1/jobs/shutdown"
	api.jobs[name] = &Job{
		Name: name, State: "ENABLED",
		HttpTarget: &HttpTarget{Uri: "http://127.0.0.1:18080/deliver", HttpMethod: http.MethodPost},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/"+name+":run", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("run status = %d, body = %s", response.Code, response.Body.String())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manual execution did not start")
	}

	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	api.mu.RLock()
	job := cloneJob(api.jobs[name])
	api.mu.RUnlock()
	if job.Status == nil || job.Status.Code == 0 ||
		!strings.Contains(job.LastAttemptError, context.Canceled.Error()) {
		t.Fatalf("shutdown outcome = %+v", job)
	}
}

func TestShutdownHonorsCallerDeadlineAndCanFinishLater(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		close(started)
		<-release
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}
	api := NewAPIWithConfig(nil, Config{HTTPClient: client})
	const name = "projects/test/locations/us-central1/jobs/blocked"
	api.jobs[name] = &Job{
		Name: name, State: "ENABLED",
		HttpTarget: &HttpTarget{Uri: "http://127.0.0.1:18080/deliver", HttpMethod: http.MethodPost},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/"+name+":run", nil))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manual execution did not start")
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelShutdown()
	if err := api.Shutdown(shutdownContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want deadline exceeded", err)
	}
	close(release)
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown after worker release: %v", err)
	}
}

func TestCustomJobVerbsRequirePostWithoutSideEffects(t *testing.T) {
	for _, verb := range []string{"run", "pause", "resume"} {
		for _, method := range []string{
			http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete,
			http.MethodHead, http.MethodOptions,
		} {
			t.Run(verb+"/"+method, func(t *testing.T) {
				var deliveries atomic.Int32
				api := NewAPIWithConfig(nil, Config{HTTPClient: &http.Client{
					Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
						deliveries.Add(1)
						return &http.Response{
							StatusCode: http.StatusNoContent,
							Body:       io.NopCloser(strings.NewReader("")),
							Header:     make(http.Header),
						}, nil
					}),
				}})
				defer api.Close()
				const name = "projects/test/locations/us-central1/jobs/method"
				api.jobs[name] = &Job{
					Name: name, State: "ENABLED", Schedule: "0 0 1 1 *",
					HttpTarget: &HttpTarget{
						Uri: "http://127.0.0.1:18080/deliver", HttpMethod: http.MethodPost,
					},
				}
				api.persistenceErr = errors.New("degraded persistence must not mask method rejection")
				before := api.snapshotJobs()
				response := httptest.NewRecorder()
				api.ServeHTTP(response, httptest.NewRequest(method, "/v1/"+name+":"+verb, nil))
				if response.Code != http.StatusMethodNotAllowed {
					t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
				}
				var body struct {
					Error struct {
						Code   int    `json:"code"`
						Status string `json:"status"`
					} `json:"error"`
				}
				if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body.Error.Code != http.StatusMethodNotAllowed ||
					body.Error.Status != "METHOD_NOT_ALLOWED" {
					t.Fatalf("error = %+v", body.Error)
				}
				if !jobsEqual(before, api.snapshotJobs()) {
					t.Fatalf("%s %s mutated job state", method, verb)
				}
				if deliveries.Load() != 0 {
					t.Fatalf("%s %s triggered %d deliveries", method, verb, deliveries.Load())
				}
			})
		}
	}
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
	api.executeJob(context.Background(), job)

	got := <-request
	if got.method != http.MethodPatch || got.body != "payload" || got.header != "present" {
		t.Fatalf("unexpected delivered request: %+v", got)
	}
	assertAttempt(t, job, attemptTime, http.StatusNoContent, "", 0)
	if job.State != "ENABLED" {
		t.Fatalf("delivery changed lifecycle state to %q", job.State)
	}
}

func TestHTTPDeliveryPropagatesTraceContextWithInjectedClient(t *testing.T) {
	traceparents := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparents <- r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(previous)

	client := target.Client()
	client.Timeout = 123 * time.Millisecond
	api := NewAPIWithConfig(nil, Config{HTTPClient: client})
	defer api.Close()
	ctx, span := provider.Tracer("scheduler-test").Start(context.Background(), "manual-run")
	api.executeJob(ctx, &Job{
		Name:  "projects/test/locations/us-central1/jobs/traced",
		State: "ENABLED",
		HttpTarget: &HttpTarget{
			Uri:        target.URL,
			HttpMethod: http.MethodPost,
		},
	})
	span.End()

	if traceparent := <-traceparents; traceparent == "" {
		t.Fatal("scheduler delivery did not propagate traceparent")
	}
	if client.Timeout != 123*time.Millisecond {
		t.Fatalf("injected client timeout changed to %s", client.Timeout)
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

	api.executeJob(context.Background(), job)

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

	api.executeJob(context.Background(), job)

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

	api.executeJob(context.Background(), job)

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

			api.executeJob(context.Background(), job)

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

func TestSetGatewayBaseURLUsesDaemonAddress(t *testing.T) {
	api := NewAPIWithConfig(nil, Config{GatewayBaseURL: "http://127.0.0.1:8080"})
	defer api.Close()

	api.SetGatewayBaseURL("http://127.0.0.1:39123/")

	api.mu.RLock()
	got := api.gatewayBaseURL
	api.mu.RUnlock()
	if got != "http://127.0.0.1:39123" {
		t.Fatalf("gateway base URL = %q, want daemon address", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	if function == nil {
		return nil, errors.New("nil round trip function")
	}
	return function(request)
}
