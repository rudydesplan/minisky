package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func newTestTracerProvider(exporter sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	return sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)), nil
}

func TestGatewayRequestIDStatusLoggingAndPropagation(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp, err := newTestTracerProvider(exporter)
	if err != nil {
		t.Fatal(err)
	}
	defer tp.Shutdown(context.Background())

	var logs bytes.Buffer
	manager := New(Config{Capacity: 10, LogWriter: &logs, TracerProvider: tp})
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Context().Value(requestIDKey{}); got == nil {
			t.Fatal("request ID missing from context")
		}
		w.WriteHeader(http.StatusCreated)
	}), func(*http.Request) RequestLabels {
		return RequestLabels{Service: "compute.googleapis.com", Route: "/v1/projects/{id}/instances/{id}"}
	})

	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/projects/demo/instances/vm-1", nil)
	req.Header.Set(RequestIDHeader, " invalid id ")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	id := rec.Header().Get(RequestIDHeader)
	if id == "" || id == " invalid id " || len(id) > MaxRequestIDLength {
		t.Fatalf("unexpected request ID %q", id)
	}
	records := manager.Store().Query(Query{})
	if len(records) != 1 || records[0].Status != http.StatusCreated || records[0].TraceID == "" {
		t.Fatalf("records = %#v", records)
	}
	var encoded map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &encoded); err != nil {
		t.Fatalf("structured log: %v", err)
	}
	for _, forbidden := range []string{"authorization", "cookie", "body"} {
		if _, ok := encoded[forbidden]; ok {
			t.Fatalf("log contains forbidden field %q", forbidden)
		}
	}
}

func TestDiagnosticsRecordsAndReplayAreProjectScoped(t *testing.T) {
	manager := New(Config{Capacity: 10, ReplayEnabled: true, LogWriter: io.Discard})
	var replayedProjects []string
	var wrapped http.Handler
	wrapped = manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(replayHeader) != "" {
			replayedProjects = append(replayedProjects, r.Header.Get("X-MiniSky-Project"))
		}
		w.WriteHeader(http.StatusNoContent)
	}), func(r *http.Request) RequestLabels {
		return RequestLabels{
			Service: "compute.googleapis.com",
			Route:   NormalizeRoute(r.URL.Path),
			Project: r.Header.Get("X-MiniSky-Project"),
		}
	})
	manager.SetReplayTarget(wrapped)
	for _, project := range []string{"project-alpha", "project-bravo"} {
		request := httptest.NewRequest(http.MethodGet, "http://localhost/v1/projects/"+project+"/instances", nil)
		request.Header.Set("X-MiniSky-Project", project)
		wrapped.ServeHTTP(httptest.NewRecorder(), request)
	}
	alpha := manager.Store().Query(Query{Project: "project-alpha"})
	if len(alpha) != 1 || alpha[0].Project != "project-alpha" {
		t.Fatalf("project-scoped records = %#v", alpha)
	}
	listRequest := httptest.NewRequest(http.MethodGet, "http://localhost/api/diagnostics/requests?project=project-alpha", nil)
	listRequest.RemoteAddr = "127.0.0.1:1234"
	listRequest.Header.Set("X-MiniSky-Project", "project-alpha")
	listResponse := httptest.NewRecorder()
	manager.DiagnosticsHandler().ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK || strings.Contains(listResponse.Body.String(), "project-bravo") {
		t.Fatalf("project-scoped diagnostics list status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	replayRequest := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/api/diagnostics/requests/"+alpha[0].RequestID+"/replay?project=project-bravo",
		nil,
	)
	replayRequest.RemoteAddr = "127.0.0.1:1234"
	replayRequest.Header.Set("X-MiniSky-Project", "project-bravo")
	replayResponse := httptest.NewRecorder()
	manager.DiagnosticsHandler().ServeHTTP(replayResponse, replayRequest)
	if replayResponse.Code != http.StatusNotFound {
		t.Fatalf("cross-project diagnostics replay status=%d body=%s", replayResponse.Code, replayResponse.Body.String())
	}
	if _, err := manager.Replay(context.Background(), "project-bravo", alpha[0].RequestID); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("cross-project replay error = %v, want %v", err, ErrRecordNotFound)
	}
	if _, err := manager.Replay(context.Background(), "project-alpha", alpha[0].RequestID); err != nil {
		t.Fatal(err)
	}
	if len(replayedProjects) != 1 || replayedProjects[0] != "project-alpha" {
		t.Fatalf("replayed projects = %#v", replayedProjects)
	}
}

func TestBoundedStoreEvictsOldestConcurrently(t *testing.T) {
	store := NewStore(3)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store.Add(Record{RequestID: string(rune(i + 1))})
			_ = store.Query(Query{})
		}(i)
	}
	wg.Wait()
	if got := len(store.Query(Query{})); got != 3 {
		t.Fatalf("store length = %d, want 3", got)
	}
}

func TestNormalizeRouteBoundsResourceIdentifiers(t *testing.T) {
	a := NormalizeRoute("/v1/projects/alpha/zones/us-central1-a/instances/one")
	b := NormalizeRoute("/v1/projects/beta/zones/europe-west1-b/instances/two")
	if a != b || strings.Contains(a, "alpha") || strings.HasSuffix(a, "/one") {
		t.Fatalf("routes are not low-cardinality: %q %q", a, b)
	}
}

func TestReplaySafetyAndBodyPolicy(t *testing.T) {
	var seenReplay string
	var seenAuthorization string
	manager := New(Config{Capacity: 10, ReplayEnabled: true, ReplayMaxBodyBytes: 128})
	var wrapped http.Handler
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenReplay = r.Header.Get(replayHeader)
		if seenReplay != "" {
			seenAuthorization = r.Header.Get("Authorization")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	wrapped = manager.Wrap(next, func(r *http.Request) RequestLabels {
		return RequestLabels{Service: "compute.googleapis.com", Route: NormalizeRoute(r.URL.Path)}
	})
	manager.SetReplayTarget(wrapped)

	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/projects/demo/instances?view=full", strings.NewReader(`{"name":"vm","password":"secret"}`))
	req.Host = "compute.googleapis.com"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	records := manager.Store().Query(Query{})
	if len(records) != 1 {
		t.Fatalf("records = %d", len(records))
	}
	if _, err := manager.Replay(context.Background(), "", records[0].RequestID); err != ErrPayloadRedacted {
		t.Fatalf("replay error = %v, want %v", err, ErrPayloadRedacted)
	}

	ok := httptest.NewRequest(http.MethodGet, "http://localhost/v1/projects/demo/instances?view=full", nil)
	ok.Host = "compute.googleapis.com"
	ok.Header.Set("Authorization", "Bearer must-not-be-captured")
	okRec := httptest.NewRecorder()
	wrapped.ServeHTTP(okRec, ok)
	records = manager.Store().Query(Query{Method: http.MethodGet})
	result, err := manager.Replay(context.Background(), "", records[0].RequestID)
	if err != nil || result.Status != http.StatusNoContent || seenReplay != "1" || seenAuthorization != "" {
		t.Fatalf("replay = %#v, %v, marker %q, authorization %q", result, err, seenReplay, seenAuthorization)
	}
	if got := len(manager.Store().Query(Query{Method: http.MethodGet})); got != 2 {
		t.Fatalf("recursive capture occurred, records = %d", got)
	}
}

func TestReplayRejectsStandardAPIKeyQuery(t *testing.T) {
	manager := New(Config{Capacity: 2, ReplayEnabled: true, LogWriter: io.Discard})
	var wrapped http.Handler
	wrapped = manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	manager.SetReplayTarget(wrapped)

	request := httptest.NewRequest(http.MethodGet, "http://localhost/v1/projects/demo?key=secret", nil)
	wrapped.ServeHTTP(httptest.NewRecorder(), request)
	record := manager.Store().Query(Query{})[0]
	if record.Replayable {
		t.Fatal("request containing the standard key query parameter is replayable")
	}
	if _, err := manager.Replay(context.Background(), "", record.RequestID); !errors.Is(err, ErrPayloadRedacted) {
		t.Fatalf("replay error = %v, want %v", err, ErrPayloadRedacted)
	}
}

func TestReplayRejectsOversizedBody(t *testing.T) {
	manager := New(Config{Capacity: 2, ReplayEnabled: true, ReplayMaxBodyBytes: 4})
	var dispatched string
	handler := manager.Wrap(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		dispatched = string(body)
	}), nil)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "http://localhost/x", strings.NewReader("123456789")))
	if dispatched != "123456789" {
		t.Fatalf("dispatched body = %q, want complete original body", dispatched)
	}
	record := manager.Store().Query(Query{})[0]
	if _, err := manager.Replay(context.Background(), "", record.RequestID); err != ErrPayloadOmitted {
		t.Fatalf("error = %v, want %v", err, ErrPayloadOmitted)
	}
}

func TestReplayPreservesLoopbackCanonicalRouting(t *testing.T) {
	manager := New(Config{Capacity: 2, ReplayEnabled: true, LogWriter: io.Discard})
	var paths []string
	var wrapped http.Handler
	wrapped = manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Host+r.URL.RequestURI())
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	manager.SetReplayTarget(wrapped)
	request := httptest.NewRequest(http.MethodGet, "http://localhost:8080/_minisky/compute/v1/projects/demo/instances", nil)
	wrapped.ServeHTTP(httptest.NewRecorder(), request)
	record := manager.Store().Query(Query{})[0]
	if _, err := manager.Replay(context.Background(), "", record.RequestID); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[1] != "127.0.0.1:8080/_minisky/compute/v1/projects/demo/instances" {
		t.Fatalf("replay paths = %#v", paths)
	}
}

func TestMetricsHandlerIsPrometheusCompatibleAndLocalOnly(t *testing.T) {
	manager := New(Config{Capacity: 2})
	manager.metrics.Observe(RequestLabels{Service: "compute.googleapis.com", Route: "/v1/projects/{id}"}, http.MethodGet, 204, 0.01)
	req := httptest.NewRequest(http.MethodGet, "http://localhost/api/diagnostics/metrics", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rec := httptest.NewRecorder()
	manager.MetricsHandler().ServeHTTP(rec, req)
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(body, "minisky_gateway_requests_total") || strings.Contains(body, "demo") {
		t.Fatalf("status=%d body=%s", rec.Code, body)
	}

	req = httptest.NewRequest(http.MethodGet, "http://localhost/api/diagnostics/metrics", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	rec = httptest.NewRecorder()
	manager.MetricsHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-loopback status = %d", rec.Code)
	}
}

func TestQuotaMetricUsesBoundedServiceRouteAndScopeLabels(t *testing.T) {
	manager := New(Config{Capacity: 2})
	manager.ObserveQuotaRejection(
		RequestLabels{Service: "compute.googleapis.com", Route: "/v1/projects/customer-project/instances/vm-1"},
		"project",
	)
	request := httptest.NewRequest(http.MethodGet, "http://localhost/api/diagnostics/metrics", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	response := httptest.NewRecorder()
	manager.MetricsHandler().ServeHTTP(response, request)
	body := response.Body.String()
	if !strings.Contains(body, `minisky_gateway_quota_rejections_total{service="compute.googleapis.com",route="/v1/projects/{id}/instances/{id}",scope="project"} 1`) {
		t.Fatalf("quota metric missing bounded labels:\n%s", body)
	}
	if strings.Contains(body, "customer-project") || strings.Contains(body, "vm-1") {
		t.Fatalf("quota metric leaked high-cardinality labels:\n%s", body)
	}
}

func TestResourceCountMetricIsBoundedAndRefreshesAtScrapeTime(t *testing.T) {
	manager := New(Config{Capacity: 2})
	count := 2
	manager.RegisterResourceCounter("logging.googleapis.com", "log_entry", func() int {
		return count
	})

	scrape := func() string {
		request := httptest.NewRequest(http.MethodGet, "http://localhost/api/diagnostics/metrics", nil)
		request.RemoteAddr = "127.0.0.1:1234"
		response := httptest.NewRecorder()
		manager.MetricsHandler().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("metrics status = %d", response.Code)
		}
		return response.Body.String()
	}

	body := scrape()
	if !strings.Contains(body, `minisky_resources{service="logging.googleapis.com",resource_kind="log_entry"} 2`) {
		t.Fatalf("resource count missing:\n%s", body)
	}
	count = 0
	body = scrape()
	if !strings.Contains(body, `minisky_resources{service="logging.googleapis.com",resource_kind="log_entry"} 0`) {
		t.Fatalf("resource deletion was not reflected:\n%s", body)
	}
	for _, forbidden := range []string{"project=", "resource_name=", "demo"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("resource metric contains high-cardinality label %q:\n%s", forbidden, body)
		}
	}
}

func TestDiagnosticsMetricsRouteBypassesGateway(t *testing.T) {
	manager := New(Config{Capacity: 1, LogWriter: io.Discard})
	request := httptest.NewRequest(http.MethodGet, "http://localhost/api/diagnostics/metrics", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	response := httptest.NewRecorder()
	manager.DiagnosticsHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://localhost/metrics", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	response = httptest.NewRecorder()
	manager.DiagnosticsHandler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("undocumented metrics alias status = %d", response.Code)
	}
	if got := len(manager.Store().Query(Query{})); got != 0 {
		t.Fatalf("diagnostics request entered gateway store: %d records", got)
	}
}

func TestInstrumentedTransportInjectsTraceparent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp, err := newTestTracerProvider(exporter)
	if err != nil {
		t.Fatal(err)
	}
	defer tp.Shutdown(context.Background())
	old := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(old)

	var traceparent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := (&http.Client{Transport: InstrumentTransport(http.DefaultTransport)}).Do(req)
	span.End()
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if traceparent == "" {
		t.Fatal("traceparent was not injected")
	}
}

func TestDoPreservesInjectedClientTimeoutRedirectAndTransport(t *testing.T) {
	transportCalled := false
	redirectCalled := false
	client := &http.Client{
		Timeout: 37 * time.Millisecond,
		Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			transportCalled = true
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    request,
			}, nil
		}),
		CheckRedirect: func(*http.Request, []*http.Request) error {
			redirectCalled = true
			return http.ErrUseLastResponse
		},
	}
	request, err := http.NewRequest(http.MethodGet, "http://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := Do(client, request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if !transportCalled {
		t.Fatal("injected transport was not called")
	}
	if client.Timeout != 37*time.Millisecond || client.CheckRedirect == nil {
		t.Fatal("injected client settings were modified")
	}
	if redirectCalled {
		t.Fatal("redirect callback unexpectedly ran")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestWrapPreservesOptionalResponseWriterInterfaces(t *testing.T) {
	manager := New(Config{Capacity: 1, LogWriter: io.Discard})
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, ok := w.(http.Flusher); !ok {
			t.Error("Flusher was not preserved")
		}
		if _, ok := w.(http.Pusher); ok {
			t.Error("Pusher was exposed although the underlying writer lacks it")
		}
	}), nil)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://localhost/test", nil))
}

func TestTelemetryDisabledInstallsLocalTracingAndPropagation(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	shutdown, err := SetupTelemetry(context.Background(), TelemetryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if otel.GetTracerProvider() == previousProvider {
		t.Fatal("disabled export did not install a local tracer provider")
	}
	carrier := propagation.MapCarrier{}
	ctx, span := otel.Tracer("disabled-export-test").Start(context.Background(), "local")
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	span.End()
	if carrier.Get("traceparent") == "" {
		t.Fatal("disabled export did not establish W3C propagation")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidExporterDegradesToLocalTracing(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	shutdown, err := SetupTelemetry(context.Background(), TelemetryConfig{
		Enabled:  true,
		Endpoint: "not-a-url",
	})
	if err != nil {
		t.Fatalf("invalid exporter prevented local telemetry: %v", err)
	}
	carrier := propagation.MapCarrier{}
	ctx, span := otel.Tracer("invalid-exporter-test").Start(context.Background(), "local")
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	span.End()
	if carrier.Get("traceparent") == "" {
		t.Fatal("local trace context was unavailable after exporter setup failed")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := shutdown(shutdownCtx); err != nil {
		t.Fatalf("local telemetry shutdown: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown took %s, want bounded by context", elapsed)
	}
}

func TestDoDoesNotDoubleInstrumentExistingTransport(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	}()

	client := &http.Client{Transport: InstrumentTransport(roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	}))}
	ctx, span := provider.Tracer("test").Start(context.Background(), "parent")
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.test", nil)
	response, err := Do(client, request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	span.End()
	if got := len(exporter.GetSpans().Snapshots()); got != 2 {
		t.Fatalf("span count = %d, want parent plus one client span", got)
	}
}

func TestExporterFailureDoesNotChangeGatewayResponse(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer collector.Close()
	shutdown, err := SetupTelemetry(context.Background(), TelemetryConfig{
		Enabled:        true,
		Endpoint:       collector.URL,
		ServiceVersion: "test",
		ExportTimeout:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	carrier := propagation.MapCarrier{}
	traceCtx, traceSpan := otel.Tracer("unavailable-exporter-test").Start(context.Background(), "local")
	otel.GetTextMapPropagator().Inject(traceCtx, carrier)
	traceSpan.End()
	if carrier.Get("traceparent") == "" {
		t.Fatal("unavailable exporter disabled local trace context")
	}
	manager := New(Config{Capacity: 1, LogWriter: io.Discard})
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://localhost/test", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("response status = %d", response.Code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	_ = shutdown(ctx)
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("shutdown exceeded its bounded deadline: %s", elapsed)
	}
	_ = shutdown(ctx)
}
