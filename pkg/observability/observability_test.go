package observability

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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

	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/projects/demo/instances/vm-1?access_token=super-secret", nil)
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
	if records[0].TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace ID = %q, want propagated parent trace ID", records[0].TraceID)
	}
	for _, span := range exporter.GetSpans().Snapshots() {
		for _, attribute := range span.Attributes() {
			if strings.Contains(strings.ToLower(string(attribute.Key)), "query") &&
				strings.Contains(attribute.Value.AsString(), "super-secret") {
				t.Fatalf("span attribute %q leaked a sensitive query value", attribute.Key)
			}
		}
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
	for _, path := range []string{"/v1:secret", "/v999999:token-secret"} {
		route := NormalizeRoute(path)
		if strings.Contains(route, "secret") || strings.Contains(route, "999999") {
			t.Fatalf("route %q preserved attacker-controlled version/action data as %q", path, route)
		}
	}
}

func TestNormalizeRouteStripsUnboundedActionsFromEverySegmentClass(t *testing.T) {
	tests := map[string]string{
		"version":                 "/v1:token-secret",
		"unknown version":         "/v999999:token-secret",
		"collection":              "/v1/projects:token-secret/demo",
		"static":                  "/v1/storage:token-secret/b/demo",
		"resource":                "/v1/projects/demo:token-secret",
		"custom method position":  "/v1/custom:token-secret",
		"known resource action":   "/v1/projects/demo:publish",
		"known collection action": "/v1/projects:publish",
	}
	for name, path := range tests {
		t.Run(name, func(t *testing.T) {
			route := NormalizeRoute(path)
			if strings.Contains(route, "token-secret") || strings.Contains(route, "999999") {
				t.Fatalf("NormalizeRoute(%q) = %q, leaked attacker suffix", path, route)
			}
			if strings.HasPrefix(name, "known ") && !strings.HasSuffix(route, ":publish") {
				t.Fatalf("NormalizeRoute(%q) = %q, lost bounded known action", path, route)
			}
		})
	}
}

func TestMetricSeriesRemainBoundedUnderHostAndRouteCardinalityStress(t *testing.T) {
	metrics := NewMetrics("compute.googleapis.com")
	metrics.Observe(
		RequestLabels{Service: "compute.googleapis.com", Route: "/v1/projects/demo/instances/vm"},
		http.MethodGet,
		http.StatusOK,
		0,
	)
	for i := 0; i < 1000; i++ {
		metrics.Observe(RequestLabels{
			Service: "tenant-" + strconv.Itoa(i) + ".example.test",
			Route:   "/v" + strconv.Itoa(i) + "/projects/project-" + strconv.Itoa(i) + "/instances/vm-" + strconv.Itoa(i) + ":action-" + strconv.Itoa(i),
		}, http.MethodGet, http.StatusOK, 0)
	}
	const maximumSeries = 256
	if got := len(metrics.values); got > maximumSeries {
		t.Fatalf("metric series = %d, want at most %d", got, maximumSeries)
	}
	foundKnownService := false
	for key := range metrics.values {
		if key.service == "compute.googleapis.com" {
			foundKnownService = true
		}
	}
	if !foundKnownService {
		t.Fatal("bounded fallback discarded a known registered service label")
	}
}

func TestUnknownMetricServicesCollapseBeforeSeriesCreation(t *testing.T) {
	manager := New(Config{
		Capacity:      4,
		LogWriter:     io.Discard,
		KnownServices: []string{"compute.googleapis.com", "storage.googleapis.com"},
	})
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), func(request *http.Request) RequestLabels {
		return RequestLabels{Service: request.Host, Route: request.URL.Path}
	})
	for i := 0; i < 300; i++ {
		request := httptest.NewRequest(
			http.MethodGet,
			"http://attacker-"+strconv.Itoa(i)+".unknown-host-canary.example/v1/projects/demo/instances/vm:action-"+strconv.Itoa(i),
			nil,
		)
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}
	for _, service := range []string{"compute.googleapis.com", "storage.googleapis.com"} {
		known := httptest.NewRequest(
			http.MethodGet,
			"http://"+service+"/v1/projects/demo/instances/vm",
			nil,
		)
		handler.ServeHTTP(httptest.NewRecorder(), known)
	}

	if got := len(manager.metrics.values); got != 3 {
		t.Fatalf("metric series = %d, want two known plus one unknown fallback", got)
	}
	request := httptest.NewRequest(http.MethodGet, "http://localhost/api/diagnostics/metrics", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	response := httptest.NewRecorder()
	manager.MetricsHandler().ServeHTTP(response, request)
	body := response.Body.String()
	if strings.Contains(body, "unknown-host-canary") || strings.Contains(body, "attacker-") {
		t.Fatalf("Prometheus output leaked unknown host canary:\n%s", body)
	}
	for _, label := range []string{
		`service="other"`,
		`service="compute.googleapis.com"`,
		`service="storage.googleapis.com"`,
	} {
		if !strings.Contains(body, label) {
			t.Fatalf("Prometheus output missing %s:\n%s", label, body)
		}
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

func TestReplayCapsResponseBytesAndPreservesFlushing(t *testing.T) {
	manager := New(Config{
		Capacity:               2,
		ReplayEnabled:          true,
		ReplayMaxResponseBytes: 4,
		LogWriter:              io.Discard,
	})
	var replayFlushed bool
	var wrapped http.Handler
	wrapped = manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(replayHeader) == "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("replay response writer lost http.Flusher")
		}
		_, _ = w.Write([]byte("1234"))
		flusher.Flush()
		replayFlushed = true
		_, _ = w.Write([]byte("5678"))
	}), nil)
	manager.SetReplayTarget(wrapped)

	wrapped.ServeHTTP(
		httptest.NewRecorder(),
		httptest.NewRequest(http.MethodGet, "http://localhost/v1/projects/demo/instances", nil),
	)
	record := manager.Store().Query(Query{})[0]
	if _, err := manager.Replay(context.Background(), "", record.RequestID); !errors.Is(err, ErrReplayResponseTooLarge) {
		t.Fatalf("replay error = %v, want %v", err, ErrReplayResponseTooLarge)
	}
	if !replayFlushed {
		t.Fatal("replay handler could not flush before reaching the response cap")
	}
}

func TestMetricsHandlerIsPrometheusCompatibleAndLocalOnly(t *testing.T) {
	manager := New(Config{Capacity: 2, KnownServices: []string{"compute.googleapis.com"}})
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
	manager := New(Config{Capacity: 2, KnownServices: []string{"compute.googleapis.com"}})
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
	manager := New(Config{Capacity: 2, KnownServices: []string{"logging.googleapis.com"}})
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

func TestOutboundTelemetrySanitizesHeadersAndPreservesDownstreamSemantics(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previousProvider)

	traceState, err := trace.ParseTraceState("vendor=context-tracestate-secret")
	if err != nil {
		t.Fatal(err)
	}
	remoteParent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{1},
		TraceFlags: trace.FlagsSampled,
		TraceState: traceState,
		Remote:     true,
	})
	baggageMember, err := baggage.NewMember("context-key", "context-baggage-secret")
	if err != nil {
		t.Fatal(err)
	}
	contextBaggage, err := baggage.New(baggageMember)
	if err != nil {
		t.Fatal(err)
	}
	parentContext := baggage.ContextWithBaggage(
		trace.ContextWithRemoteSpanContext(context.Background(), remoteParent),
		contextBaggage,
	)
	ctx, parent := provider.Tracer("outbound-header-test").Start(
		parentContext,
		"parent",
	)
	originalHeaders := http.Header{
		"User-Agent":      {"user-agent-secret"},
		"Forwarded":       {"for=forwarded-secret"},
		"X-Forwarded-For": {"203.0.113.99"},
		"Authorization":   {"Bearer authorization-secret"},
		"Cookie":          {"session=cookie-secret"},
		"X-Arbitrary":     {"arbitrary-header-secret"},
		"Tracestate":      {"vendor=header-tracestate-secret"},
		"Baggage":         {"token=baggage-secret"},
		"Traceparent":     {"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
	}
	var downstreamHeaders http.Header
	base := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		downstreamHeaders = request.Header.Clone()
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://backend.example/v1/projects/demo", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header = originalHeaders.Clone()
	callerHeaders := request.Header.Clone()
	response, err := (&http.Client{
		Transport: InstrumentTransport(TrustTransport(base)),
	}).Do(request)
	parent.End()
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	if !reflect.DeepEqual(request.Header, callerHeaders) {
		t.Fatalf("caller request headers mutated:\ngot  %v\nwant %v", request.Header, callerHeaders)
	}
	for _, name := range []string{
		"User-Agent", "Forwarded", "X-Forwarded-For",
		"Authorization", "Cookie", "X-Arbitrary",
	} {
		if got, want := downstreamHeaders.Values(name), originalHeaders.Values(name); !reflect.DeepEqual(got, want) {
			t.Errorf("downstream %s = %q, want %q", name, got, want)
		}
	}
	if downstreamHeaders.Get("tracestate") != "" || downstreamHeaders.Get("baggage") != "" {
		t.Fatalf("downstream propagated untrusted state: tracestate=%q baggage=%q",
			downstreamHeaders.Get("tracestate"), downstreamHeaders.Get("baggage"))
	}
	propagated := propagation.TraceContext{}.Extract(
		context.Background(),
		propagation.HeaderCarrier(downstreamHeaders),
	)
	propagatedContext := trace.SpanContextFromContext(propagated)
	if !propagatedContext.IsValid() || propagatedContext.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("downstream traceparent = %q, want current trace %s",
			downstreamHeaders.Get("traceparent"), parent.SpanContext().TraceID())
	}
	if downstreamHeaders.Get("traceparent") == originalHeaders.Get("traceparent") {
		t.Fatal("downstream retained attacker-supplied traceparent")
	}
	expectedDownstream := originalHeaders.Clone()
	expectedDownstream.Del("traceparent")
	expectedDownstream.Del("tracestate")
	expectedDownstream.Del("baggage")
	expectedDownstream.Set("traceparent", downstreamHeaders.Get("traceparent"))
	if !reflect.DeepEqual(downstreamHeaders, expectedDownstream) {
		t.Fatalf("downstream headers:\ngot  %v\nwant %v", downstreamHeaders, expectedDownstream)
	}

	var clientSpans []sdktrace.ReadOnlySpan
	var exportedKinds []string
	for _, span := range exporter.GetSpans().Snapshots() {
		exportedKinds = append(exportedKinds, span.Name())
		if span.SpanKind() == trace.SpanKindClient {
			clientSpans = append(clientSpans, span)
		}
	}
	if len(clientSpans) != 1 {
		t.Fatalf("client span count = %d, want 1; exported=%v", len(clientSpans), exportedKinds)
	}
	assertSpanDataExcludes(t, clientSpans,
		"user-agent-secret", "forwarded-secret", "203.0.113.99",
		"authorization-secret", "cookie-secret", "arbitrary-header-secret",
		"context-tracestate-secret", "header-tracestate-secret",
		"context-baggage-secret", "baggage-secret",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb")
}

func TestInstrumentedTransportConcurrentReuseDoesNotMutateCallerHeaders(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	ctx, parent := provider.Tracer("concurrent-outbound-test").Start(context.Background(), "parent")
	defer parent.End()
	spanContext := parent.SpanContext()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://backend.example/v1/projects/demo",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header = http.Header{
		"User-Agent":    {"shared-user-agent"},
		"Authorization": {"Bearer shared-token"},
		"Traceparent":   {"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
		"Tracestate":    {"vendor=shared-state"},
		"Baggage":       {"shared=secret"},
	}
	originalHeaders := request.Header.Clone()
	const requests = 32
	var (
		mu       sync.Mutex
		captured []http.Header
	)
	base := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		mu.Lock()
		captured = append(captured, request.Header.Clone())
		mu.Unlock()
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	})
	transport := InstrumentTransport(TrustTransport(base))
	errs := make(chan error, requests)
	var group sync.WaitGroup
	for range requests {
		group.Add(1)
		go func() {
			defer group.Done()
			response, err := transport.RoundTrip(request)
			if err == nil {
				err = response.Body.Close()
			}
			errs <- err
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(request.Header, originalHeaders) {
		t.Fatalf("caller request headers mutated:\ngot  %v\nwant %v", request.Header, originalHeaders)
	}
	if len(captured) != requests {
		t.Fatalf("captured requests = %d, want %d", len(captured), requests)
	}
	for _, headers := range captured {
		if headers.Get("User-Agent") != "shared-user-agent" ||
			headers.Get("Authorization") != "Bearer shared-token" {
			t.Errorf("required downstream headers changed: %v", headers)
		}
		if headers.Get("tracestate") != "" || headers.Get("baggage") != "" {
			t.Errorf("untrusted propagation reached downstream: %v", headers)
		}
		propagated := propagation.TraceContext{}.Extract(
			context.Background(),
			propagation.HeaderCarrier(headers),
		)
		if got := trace.SpanContextFromContext(propagated).TraceID(); got != spanContext.TraceID() {
			t.Errorf("propagated trace ID = %s, want %s", got, spanContext.TraceID())
		}
	}
}

func TestHTTPSpansExcludeRawResourceAndSensitiveURLData(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider, err := newTestTracerProvider(exporter)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Shutdown(context.Background())
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	}()

	manager := New(Config{Capacity: 2, LogWriter: io.Discard, TracerProvider: provider})
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), func(*http.Request) RequestLabels {
		return RequestLabels{
			Service: "compute.googleapis.com",
			Route:   "/v1/projects/project-secret/instances/vm-secret",
			Project: "project-secret",
		}
	})
	inbound := httptest.NewRequest(
		http.MethodGet,
		"http://compute.googleapis.com/v1/projects/project-secret/instances/vm-secret?access_token=token-secret",
		nil,
	)
	handler.ServeHTTP(httptest.NewRecorder(), inbound)

	var outboundURL string
	base := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		outboundURL = request.URL.String()
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	})
	ctx, parent := provider.Tracer("privacy-test").Start(context.Background(), "bounded-parent")
	outbound, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://project-secret.backend.example/v1/projects/project-secret/instances/vm-secret?access_token=token-secret",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: InstrumentTransport(TrustTransport(base))}).Do(outbound)
	parent.End()
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if outboundURL != outbound.URL.String() {
		t.Fatalf("outbound request URL changed: got %q want %q", outboundURL, outbound.URL.String())
	}

	failingBase := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("token-secret transport failure")
	})
	failingRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://project-secret.backend.example/v1/projects/project-secret?access_token=token-secret",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&http.Client{Transport: InstrumentTransport(TrustTransport(failingBase))}).Do(failingRequest); err == nil ||
		!strings.Contains(err.Error(), "token-secret") {
		t.Fatalf("outbound transport error changed: %v", err)
	}

	assertSpanDataExcludes(t, exporter.GetSpans().Snapshots(),
		"project-secret", "vm-secret", "token-secret", "access_token")
}

func TestInboundSpanUsesFixedServerAddressForArbitraryValidHost(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider, err := newTestTracerProvider(exporter)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Shutdown(context.Background())
	manager := New(Config{Capacity: 1, LogWriter: io.Discard, TracerProvider: provider})
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	request := httptest.NewRequest(http.MethodGet, "http://attacker-valid.example/v1:secret", nil)
	request.Host = "attacker-valid.example"
	handler.ServeHTTP(httptest.NewRecorder(), request)

	spans := exporter.GetSpans().Snapshots()
	assertSpanDataExcludes(t, spans, "attacker-valid.example", "secret")
	foundFixedAddress := false
	for _, span := range spans {
		for _, attribute := range span.Attributes() {
			if string(attribute.Key) == "server.address" && attribute.Value.AsString() == "minisky.local" {
				foundFixedAddress = true
			}
		}
	}
	if !foundFixedAddress {
		t.Fatal("inbound span did not use the fixed MiniSky server address")
	}
}

func TestInboundSpansExposeBoundedServiceIdentity(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider, err := newTestTracerProvider(exporter)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Shutdown(context.Background())
	manager := New(Config{
		Capacity:       3,
		LogWriter:      io.Discard,
		TracerProvider: provider,
		KnownServices:  []string{"compute.googleapis.com", "storage.googleapis.com"},
	})
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), func(request *http.Request) RequestLabels {
		return RequestLabels{
			Service: request.Header.Get("X-Test-Service"),
			Route:   "/v1/projects/project-secret/resources/resource-secret",
		}
	})
	for _, service := range []string{
		"compute.googleapis.com",
		"storage.googleapis.com",
		"attacker-valid.example",
	} {
		request := httptest.NewRequest(
			http.MethodGet,
			"http://attacker-host.example/v1/projects/project-secret/resources/resource-secret?access_token=token-secret",
			nil,
		)
		request.Host = "attacker-host.example"
		request.Header.Set("X-Test-Service", service)
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}

	spans := exporter.GetSpans().Snapshots()
	if len(spans) != 3 {
		t.Fatalf("span count = %d, want 3", len(spans))
	}
	services := make(map[string]int)
	spanNames := make(map[string]struct{})
	for _, span := range spans {
		spanNames[span.Name()] = struct{}{}
		var serverAddress, service string
		for _, attribute := range span.Attributes() {
			switch string(attribute.Key) {
			case "server.address":
				serverAddress = attribute.Value.AsString()
			case "minisky.service":
				service = attribute.Value.AsString()
			}
		}
		if serverAddress != "minisky.local" {
			t.Errorf("server.address = %q, want minisky.local", serverAddress)
		}
		services[service]++
	}
	for _, service := range []string{"compute.googleapis.com", "storage.googleapis.com", "other"} {
		if services[service] != 1 {
			t.Errorf("minisky.service %q count = %d, want 1; all=%v", service, services[service], services)
		}
	}
	if len(spanNames) != 1 {
		t.Errorf("identical normalized routes produced different span names: %v", spanNames)
	}
	assertSpanDataExcludes(t, spans,
		"attacker-host.example", "attacker-valid.example",
		"project-secret", "resource-secret", "token-secret", "access_token")
}

func TestInboundTelemetryStripsHeaderCanariesAndPreservesHandlerHeaders(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider, err := newTestTracerProvider(exporter)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Shutdown(context.Background())
	manager := New(Config{
		Capacity:       1,
		LogWriter:      io.Discard,
		TracerProvider: provider,
		KnownServices:  []string{"compute.googleapis.com"},
	})
	canaryHeaders := http.Header{
		"User-Agent":        {"user-agent-secret"},
		"Forwarded":         {"for=forwarded-secret;host=forwarded-host-secret"},
		"X-Forwarded-For":   {"203.0.113.77"},
		"X-Forwarded-Host":  {"forwarded-host-secret"},
		"X-Forwarded-Proto": {"proto-secret"},
		"True-Client-Ip":    {"198.51.100.88"},
		"Tracestate":        {"vendor=tracestate-secret"},
		"Baggage":           {"baggage-key=baggage-secret"},
	}
	var handlerHeaders http.Header
	var handlerTraceID string
	handler := manager.Wrap(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		handlerHeaders = request.Header.Clone()
		handlerTraceID = trace.SpanContextFromContext(request.Context()).TraceID().String()
		w.WriteHeader(http.StatusNoContent)
	}), func(*http.Request) RequestLabels {
		return RequestLabels{Service: "compute.googleapis.com", Route: "/v1/projects/{id}"}
	})
	request := httptest.NewRequest(http.MethodGet, "http://attacker.example/v1/projects/project-secret", nil)
	for name, values := range canaryHeaders {
		request.Header[name] = append([]string(nil), values...)
	}
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	handler.ServeHTTP(httptest.NewRecorder(), request)

	for name, values := range canaryHeaders {
		if got := handlerHeaders.Values(name); !reflect.DeepEqual(got, values) {
			t.Errorf("handler header %s = %q, want %q", name, got, values)
		}
	}
	if handlerTraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("handler trace ID = %q, want propagated parent", handlerTraceID)
	}
	assertSpanDataExcludes(t, exporter.GetSpans().Snapshots(),
		"user-agent-secret", "forwarded-secret", "forwarded-host-secret",
		"203.0.113.77", "proto-secret", "198.51.100.88",
		"tracestate-secret", "baggage-secret", "project-secret")
}

func assertSpanDataExcludes(t *testing.T, spans []sdktrace.ReadOnlySpan, forbidden ...string) {
	t.Helper()
	for _, span := range spans {
		values := []string{
			span.Name(),
			span.Status().Description,
			span.SpanContext().TraceState().String(),
			span.Parent().TraceState().String(),
		}
		for _, attribute := range span.Attributes() {
			values = append(values, string(attribute.Key), attribute.Value.Emit())
		}
		for _, attribute := range span.Resource().Attributes() {
			values = append(values, string(attribute.Key), attribute.Value.Emit())
		}
		for _, event := range span.Events() {
			values = append(values, event.Name)
			for _, attribute := range event.Attributes {
				values = append(values, string(attribute.Key), attribute.Value.Emit())
			}
		}
		for _, link := range span.Links() {
			values = append(values,
				link.SpanContext.TraceID().String(),
				link.SpanContext.SpanID().String(),
				link.SpanContext.TraceState().String(),
			)
			for _, attribute := range link.Attributes {
				values = append(values, string(attribute.Key), attribute.Value.Emit())
			}
		}
		joined := strings.ToLower(strings.Join(values, "\n"))
		for _, secret := range forbidden {
			if strings.Contains(joined, strings.ToLower(secret)) {
				t.Errorf("span %q leaked %q in exported data:\n%s", span.Name(), secret, joined)
			}
		}
	}
}

func TestDoPreservesInjectedClientTimeoutRedirectAndTransport(t *testing.T) {
	transportCalled := false
	redirectCalled := false
	client := &http.Client{
		Timeout: 37 * time.Millisecond,
		Transport: TrustTransport(roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			transportCalled = true
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    request,
			}, nil
		})),
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

func TestTelemetryDisabledDoesNotStartExporter(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	exports := make(chan struct{}, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		exports <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer collector.Close()
	shutdown, err := SetupTelemetry(context.Background(), TelemetryConfig{
		Endpoint: collector.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("disabled-exporter-test").Start(context.Background(), "local-only")
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exports:
		t.Fatal("disabled telemetry contacted the configured exporter")
	default:
	}
}

func TestBoundedParentSamplerDoesNotTrustRemoteSampledFlag(t *testing.T) {
	traceID := trace.TraceID{1}
	spanID := trace.SpanID{1}
	remoteSampled := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	remoteContext := trace.ContextWithRemoteSpanContext(context.Background(), remoteSampled)
	decision := boundedParentSampler(0).ShouldSample(sdktrace.SamplingParameters{
		ParentContext: remoteContext,
		TraceID:       traceID,
		Name:          "remote",
	})
	if decision.Decision != sdktrace.Drop {
		t.Fatalf("remote sampled decision = %v, want Drop at zero ratio", decision.Decision)
	}
	decision = boundedParentSampler(1).ShouldSample(sdktrace.SamplingParameters{
		ParentContext: remoteContext,
		TraceID:       traceID,
		Name:          "remote",
	})
	if decision.Decision != sdktrace.RecordAndSample {
		t.Fatalf("remote sampled decision = %v, want RecordAndSample at full ratio", decision.Decision)
	}
}

func TestBoundedParentSamplerIgnoresRemoteSampledFlagForSameTraceID(t *testing.T) {
	sampler := boundedParentSampler(0.1)
	var inside, outside trace.TraceID
	binary.BigEndian.PutUint64(inside[8:], 2)
	binary.BigEndian.PutUint64(outside[8:], ^uint64(0)-1)
	for name, traceID := range map[string]trace.TraceID{"inside": inside, "outside": outside} {
		var decisions []sdktrace.SamplingDecision
		for _, flags := range []trace.TraceFlags{0, trace.FlagsSampled} {
			parent := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     trace.SpanID{1},
				TraceFlags: flags,
				Remote:     true,
			})
			decisions = append(decisions, sampler.ShouldSample(sdktrace.SamplingParameters{
				ParentContext: trace.ContextWithRemoteSpanContext(context.Background(), parent),
				TraceID:       traceID,
				Name:          name,
			}).Decision)
		}
		if decisions[0] != decisions[1] {
			t.Fatalf("%s trace decisions differ by sampled flag: %v", name, decisions)
		}
		want := sdktrace.RecordAndSample
		if name == "outside" {
			want = sdktrace.Drop
		}
		if decisions[0] != want {
			t.Fatalf("%s trace decision = %v, want %v", name, decisions[0], want)
		}
	}
}

func TestMalformedTraceparentUsesRootSamplingPolicy(t *testing.T) {
	traceID := trace.TraceID{1}
	decision := boundedParentSampler(0).ShouldSample(sdktrace.SamplingParameters{
		ParentContext: propagation.TraceContext{}.Extract(
			context.Background(),
			propagation.MapCarrier{"traceparent": "malformed-sampled-parent"},
		),
		TraceID: traceID,
		Name:    "malformed",
	})
	if decision.Decision != sdktrace.Drop {
		t.Fatalf("malformed traceparent decision = %v, want root-policy Drop", decision.Decision)
	}
}

func TestInvalidExporterReturnsDiagnosticWithoutChangingGlobals(t *testing.T) {
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
	if err == nil {
		t.Fatal("invalid exporter endpoint was silently accepted")
	}
	if otel.GetTracerProvider() != previousProvider || otel.GetTextMapPropagator() != previousPropagator {
		t.Fatal("invalid exporter setup replaced process telemetry globals")
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

type trackingSpanExporter struct{ shutdown bool }

func (*trackingSpanExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }
func (exporter *trackingSpanExporter) Shutdown(context.Context) error {
	exporter.shutdown = true
	return nil
}

func TestExporterIsClosedWhenResourceInitializationFails(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	previousExporterFactory := newTelemetryExporter
	previousResourceFactory := newTelemetryResource
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		newTelemetryExporter = previousExporterFactory
		newTelemetryResource = previousResourceFactory
	})

	exporter := &trackingSpanExporter{}
	newTelemetryExporter = func(context.Context, string, time.Duration) (sdktrace.SpanExporter, error) {
		return exporter, nil
	}
	newTelemetryResource = func(context.Context, string) (*resource.Resource, error) {
		return nil, errors.New("resource failed")
	}

	shutdown, err := SetupTelemetry(context.Background(), TelemetryConfig{
		Enabled:  true,
		Endpoint: "http://collector.example",
	})
	if err == nil || !strings.Contains(err.Error(), "resource failed") {
		t.Fatalf("setup error = %v, want resource diagnostic", err)
	}
	if !exporter.shutdown {
		t.Fatal("partially initialized exporter was not closed")
	}
	if otel.GetTracerProvider() != previousProvider || otel.GetTextMapPropagator() != previousPropagator {
		t.Fatal("partial setup changed process telemetry globals")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("failed setup returned unsafe shutdown: %v", err)
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

	client := &http.Client{Transport: InstrumentTransport(TrustTransport(roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	})))}
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

func TestInstrumentTransportDoesNotTrustForeignOTelTransport(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previousProvider)

	dispatched := false
	base := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		dispatched = true
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	})
	foreign := otelhttp.NewTransport(base)
	instrumented := InstrumentTransport(foreign)
	if instrumented == foreign {
		t.Fatal("foreign otelhttp transport bypassed MiniSky privacy instrumentation")
	}
	ctx, span := provider.Tracer("foreign-transport-test").Start(context.Background(), "parent")
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://project-secret.example/v1/projects/project-secret?access_token=token-secret",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, requestErr := (&http.Client{Transport: instrumented}).Do(request)
	span.End()
	if response != nil {
		response.Body.Close()
	}
	if requestErr == nil && !dispatched {
		t.Fatal("foreign transport neither dispatched nor returned an explicit rejection")
	}
	assertSpanDataExcludes(t, exporter.GetSpans().Snapshots(),
		"project-secret", "token-secret", "access_token")
}

type decoratingRoundTripper struct{ base http.RoundTripper }

func (transport decoratingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport.base.RoundTrip(request)
}

func TestInstrumentTransportRejectsDecoratedForeignOTelTransport(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previousProvider)

	dispatched := false
	rawExporter := otelhttp.NewTransport(roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		dispatched = true
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	}))
	instrumented := InstrumentTransport(decoratingRoundTripper{base: rawExporter})
	ctx, span := provider.Tracer("decorated-transport-test").Start(context.Background(), "parent")
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://project-secret.example/v1/projects/project-secret?access_token=token-secret",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, requestErr := (&http.Client{Transport: instrumented}).Do(request)
	span.End()
	if response != nil {
		response.Body.Close()
	}
	if requestErr == nil {
		t.Fatal("unknown decorated transport was not rejected")
	}
	if dispatched {
		t.Fatal("decorated foreign OTel transport observed the raw request")
	}
	assertSpanDataExcludes(t, exporter.GetSpans().Snapshots(),
		"project-secret", "token-secret", "access_token")
}

func TestInstrumentTransportSanitizesEveryNonessentialURLField(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previousProvider)

	originalURL := &url.URL{
		Scheme:      "http",
		Opaque:      "//opaque-secret.example/raw-secret",
		User:        url.UserPassword("user-secret", "password-secret"),
		Host:        "host-secret.example",
		Path:        "/v1:action-secret/projects/project-secret",
		RawPath:     "/raw-secret",
		ForceQuery:  true,
		RawQuery:    "access_token=token-secret",
		Fragment:    "fragment-secret",
		RawFragment: "raw-fragment-secret",
	}
	var downstreamURL url.URL
	base := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		downstreamURL = *request.URL
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    request,
		}, nil
	})
	ctx, span := provider.Tracer("url-fields-test").Start(context.Background(), "parent")
	request := (&http.Request{
		Method: http.MethodGet,
		URL:    originalURL,
		Header: make(http.Header),
		Host:   originalURL.Host,
	}).WithContext(ctx)
	response, err := (&http.Client{Transport: InstrumentTransport(TrustTransport(base))}).Do(request)
	span.End()
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if !reflect.DeepEqual(downstreamURL, *originalURL) {
		t.Fatalf("downstream URL changed:\ngot  %#v\nwant %#v", downstreamURL, *originalURL)
	}
	assertSpanDataExcludes(t, exporter.GetSpans().Snapshots(),
		"opaque-secret", "raw-secret", "user-secret", "password-secret",
		"host-secret", "action-secret", "project-secret", "token-secret",
		"fragment-secret", "raw-fragment-secret", "access_token")
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
	sampleAll := 1.0
	shutdown, err := SetupTelemetry(context.Background(), TelemetryConfig{
		Enabled:        true,
		Endpoint:       collector.URL,
		ServiceVersion: "test",
		ExportTimeout:  100 * time.Millisecond,
		SamplingRatio:  &sampleAll,
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

func TestConcurrentTelemetryShutdownRespectsEachCallerContext(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	exportStarted := make(chan struct{})
	releaseExport := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseExport) }) }
	t.Cleanup(release)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(exportStarted)
		<-releaseExport
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)

	sampleAll := 1.0
	shutdown, err := SetupTelemetry(context.Background(), TelemetryConfig{
		Enabled:        true,
		Endpoint:       collector.URL,
		ServiceVersion: "test",
		ExportTimeout:  time.Second,
		SamplingRatio:  &sampleAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("concurrent-shutdown-test").Start(context.Background(), "pending-export")
	span.End()

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- shutdown(context.Background())
	}()
	<-exportStarted

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- shutdown(cancelled)
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("second shutdown error = %v, want context canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		release()
		<-firstDone
		t.Fatal("second shutdown ignored its canceled context while another shutdown was running")
	}

	release()
	if err := <-firstDone; err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
}
