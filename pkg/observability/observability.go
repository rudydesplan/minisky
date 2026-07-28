package observability

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/felixge/httpsnoop"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	RequestIDHeader            = "X-MiniSky-Request-ID"
	MaxRequestIDLength         = 64
	replayHeader               = "X-MiniSky-Internal-Replay"
	defaultCapacity            = 1000
	defaultReplayLimit         = 64 << 10
	defaultReplayResponseLimit = 64 << 10
	maxMetricSeries            = 256
	serviceAttributeKey        = "minisky.service"
)

var (
	ErrRecordNotFound         = errors.New("request record not found")
	ErrReplayDisabled         = errors.New("request replay is disabled")
	ErrPayloadOmitted         = errors.New("request payload was omitted because it exceeded the capture limit")
	ErrPayloadRedacted        = errors.New("request payload contained sensitive data and cannot be replayed")
	ErrReplayResponseTooLarge = errors.New("replay response exceeded the capture limit")
	errOutboundRequest        = errors.New("outbound request failed")
	errUntrustedTransport     = errors.New("custom transport is not trusted for privacy-safe instrumentation")
)

type requestIDKey struct{}

type RequestLabels struct {
	Service string
	Route   string
	Project string
}

type Record struct {
	Timestamp  time.Time `json:"timestamp"`
	RequestID  string    `json:"requestId"`
	TraceID    string    `json:"traceId,omitempty"`
	SpanID     string    `json:"spanId,omitempty"`
	Method     string    `json:"method"`
	Route      string    `json:"route"`
	Service    string    `json:"service"`
	Project    string    `json:"project,omitempty"`
	Status     int       `json:"status"`
	LatencyMS  float64   `json:"latencyMs"`
	Replayable bool      `json:"replayable"`

	replay *replayPayload
}

type Query struct {
	Service string
	Method  string
	Status  int
	TraceID string
	Project string
}

type Store struct {
	mu       sync.RWMutex
	capacity int
	records  []Record
}

func NewStore(capacity int) *Store {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	return &Store{capacity: capacity, records: make([]Record, 0, capacity)}
}

func (s *Store) Add(record Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.records) == s.capacity {
		copy(s.records, s.records[1:])
		s.records[len(s.records)-1] = record
		return
	}
	s.records = append(s.records, record)
}

func (s *Store) Query(query Query) []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Record, 0, len(s.records))
	for i := len(s.records) - 1; i >= 0; i-- {
		record := s.records[i]
		if query.Project != "" && record.Project != query.Project {
			continue
		}
		if query.Service != "" && record.Service != query.Service {
			continue
		}
		if query.Method != "" && record.Method != query.Method {
			continue
		}
		if query.Status != 0 && record.Status != query.Status {
			continue
		}
		if query.TraceID != "" && record.TraceID != query.TraceID {
			continue
		}
		record.replay = nil
		result = append(result, record)
	}
	return result
}

func (s *Store) find(project, requestID string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.records) - 1; i >= 0; i-- {
		if s.records[i].Project == project && s.records[i].RequestID == requestID {
			return s.records[i], true
		}
	}
	return Record{}, false
}

type Config struct {
	Capacity               int
	LogWriter              io.Writer
	ReplayEnabled          bool
	ReplayMaxBodyBytes     int64
	ReplayMaxResponseBytes int64
	TracerProvider         trace.TracerProvider
	KnownServices          []string
}

type Manager struct {
	store                  *Store
	metrics                *Metrics
	logWriter              io.Writer
	logMu                  sync.Mutex
	replayEnabled          bool
	replayMaxBytes         int64
	replayMaxResponseBytes int64
	replayTarget           http.Handler
	replayMu               sync.RWMutex
	tracerProvider         trace.TracerProvider
	knownServices          map[string]struct{}
}

func New(config Config) *Manager {
	writer := config.LogWriter
	if writer == nil {
		writer = os.Stdout
	}
	limit := config.ReplayMaxBodyBytes
	if limit <= 0 {
		limit = defaultReplayLimit
	}
	responseLimit := config.ReplayMaxResponseBytes
	if responseLimit <= 0 {
		responseLimit = defaultReplayResponseLimit
	}
	provider := config.TracerProvider
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	knownServices := make(map[string]struct{}, len(config.KnownServices)+1)
	addKnownServices(knownServices, config.KnownServices...)
	addKnownServices(knownServices, "minisky.local")
	return &Manager{
		store:                  NewStore(config.Capacity),
		metrics:                NewMetrics(config.KnownServices...),
		logWriter:              writer,
		replayEnabled:          config.ReplayEnabled,
		replayMaxBytes:         limit,
		replayMaxResponseBytes: responseLimit,
		tracerProvider:         provider,
		knownServices:          knownServices,
	}
}

func (m *Manager) Store() *Store { return m.store }

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func (m *Manager) Wrap(next http.Handler, resolve func(*http.Request) RequestLabels) http.Handler {
	core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := sanitizeRequestID(r.Header.Get(RequestIDHeader))
		if requestID == "" {
			requestID = generateRequestID()
		}
		w.Header().Set(RequestIDHeader, requestID)
		r = r.WithContext(context.WithValue(r.Context(), requestIDKey{}, requestID))

		labels := RequestLabels{Service: normalizeService(r.Host), Route: NormalizeRoute(r.URL.Path)}
		if resolve != nil {
			labels = resolve(r)
		}
		labels.Service = normalizeService(labels.Service)
		labels.Route = NormalizeRoute(labels.Route)
		trace.SpanFromContext(r.Context()).SetAttributes(attribute.String(
			serviceAttributeKey,
			m.telemetryService(labels.Service),
		))

		var payload *replayPayload
		var payloadErr error
		if m.replayEnabled && r.Header.Get(replayHeader) == "" {
			payload, payloadErr = m.captureReplay(r, labels)
		}

		metrics := httpsnoop.CaptureMetrics(next, w, r)
		status := metrics.Code
		if status == 0 {
			status = http.StatusOK
		}
		latency := time.Since(started)
		spanContext := trace.SpanContextFromContext(r.Context())
		record := Record{
			Timestamp:  started.UTC(),
			RequestID:  requestID,
			Method:     r.Method,
			Route:      labels.Route,
			Service:    labels.Service,
			Project:    strings.TrimSpace(labels.Project),
			Status:     status,
			LatencyMS:  float64(latency.Microseconds()) / 1000,
			Replayable: payload != nil && payloadErr == nil,
			replay:     payload,
		}
		if spanContext.IsValid() {
			record.TraceID = spanContext.TraceID().String()
			record.SpanID = spanContext.SpanID().String()
		}
		if payloadErr != nil {
			record.replay = &replayPayload{captureErr: payloadErr}
		}
		m.store.Add(record)
		m.metrics.Observe(labels, r.Method, status, latency.Seconds())
		m.writeLog(record)
	})

	instrumented := otelhttp.NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, instrumentedRequest *http.Request) {
			original, _ := instrumentedRequest.Context().Value(originalRequestKey{}).(*http.Request)
			if original == nil {
				core.ServeHTTP(w, instrumentedRequest)
				return
			}
			request := original.Clone(instrumentedRequest.Context())
			request.Header = original.Header.Clone()
			core.ServeHTTP(w, request)
		}),
		"minisky.gateway",
		otelhttp.WithTracerProvider(m.tracerProvider),
		otelhttp.WithPropagators(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + NormalizeRoute(r.URL.Path)
		}),
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labels := RequestLabels{Service: normalizeService(r.Host), Route: NormalizeRoute(r.URL.Path)}
		if resolve != nil {
			labels = resolve(r)
		}
		labels.Service = normalizeService(labels.Service)
		labels.Route = NormalizeRoute(labels.Route)

		safeRequest := r.Clone(context.WithValue(r.Context(), originalRequestKey{}, r))
		safeURL := *r.URL
		safeURL.Path = labels.Route
		safeURL.RawPath = ""
		safeURL.RawQuery = ""
		safeURL.ForceQuery = false
		safeURL.User = nil
		safeURL.Host = "minisky.local"
		safeURL.Opaque = ""
		safeURL.Fragment = ""
		safeURL.RawFragment = ""
		safeRequest.URL = &safeURL
		safeRequest.Host = "minisky.local"
		safeRequest.Header = sanitizedPropagationHeaders(r.Header)
		instrumented.ServeHTTP(w, safeRequest)
	})
}

type originalRequestKey struct{}

func sanitizedPropagationHeaders(headers http.Header) http.Header {
	carrier := propagation.HeaderCarrier(make(http.Header))
	if traceparent := headers.Get("traceparent"); traceparent != "" {
		carrier.Set("traceparent", traceparent)
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return make(http.Header)
	}
	sanitized := propagation.HeaderCarrier(make(http.Header))
	propagation.TraceContext{}.Inject(ctx, sanitized)
	return http.Header(sanitized)
}

func (m *Manager) telemetryService(service string) string {
	service = normalizeService(service)
	if _, ok := m.knownServices[service]; ok {
		return service
	}
	return "other"
}

func sanitizeRequestID(value string) string {
	if value == "" || len(value) > MaxRequestIDLength {
		return ""
	}
	for _, r := range value {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || r == '.') {
			return ""
		}
	}
	return value
}

func generateRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err == nil {
		return hex.EncodeToString(value[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func normalizeService(service string) string {
	host := strings.TrimSpace(strings.ToLower(service))
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		host = parsed
	}
	host = strings.Trim(host, "[] .")
	if host == "" || host == "localhost" || net.ParseIP(host) != nil {
		return "unresolved"
	}
	if len(host) > 253 {
		return "unresolved"
	}
	for _, r := range host {
		if !(r == '.' || r == '-' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return "unresolved"
		}
	}
	return host
}

var resourceCollections = map[string]struct{}{
	"projects": {}, "locations": {}, "zones": {}, "regions": {}, "instances": {},
	"buckets": {}, "topics": {}, "subscriptions": {}, "functions": {}, "services": {},
	"jobs": {}, "queues": {}, "tasks": {}, "datasets": {}, "tables": {}, "secrets": {},
	"versions": {}, "clusters": {}, "networks": {}, "subnetworks": {}, "operations": {},
	"keyRings": {}, "cryptoKeys": {}, "repositories": {}, "packages": {}, "builds": {},
}

var staticRouteSegments = map[string]struct{}{
	"storage": {}, "upload": {}, "bigquery": {}, "compute": {}, "dns": {},
	"b": {}, "o": {}, "global": {}, "internal": {}, "models": {}, "entries": {},
	"serviceAccounts": {}, "managedZones": {}, "healthChecks": {}, "urlMaps": {},
	"targetHttpProxies": {}, "forwardingRules": {}, "backendServices": {},
}

var knownAPIVersions = map[string]struct{}{
	"v1": {}, "v2": {}, "v3": {}, "v4": {},
	"v1alpha1": {}, "v1beta1": {}, "v1p1beta1": {}, "v2beta1": {},
}

func NormalizeRoute(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "_minisky" {
		parts = parts[2:]
	}
	if len(parts) == 0 {
		return "/"
	}
	for i := range parts {
		segment := parts[i]
		if i > 0 {
			previous := strings.TrimSuffix(parts[i-1], ":")
			if _, ok := resourceCollections[previous]; ok {
				parts[i] = normalizeActionSegment(segment)
				continue
			}
		}
		base, action, hasAction := strings.Cut(segment, ":")
		if _, known := knownAPIVersions[base]; known || isAPIVersion(base) {
			if _, known := knownAPIVersions[base]; !known {
				base = "{version}"
			}
			parts[i] = base
			if hasAction && isKnownRouteAction(action) {
				parts[i] += ":" + action
			}
			continue
		}
		if _, ok := resourceCollections[base]; ok {
			parts[i] = base
			if hasAction && isKnownRouteAction(action) {
				parts[i] += ":" + action
			}
			continue
		}
		if _, ok := staticRouteSegments[base]; ok {
			parts[i] = base
			if hasAction && isKnownRouteAction(action) {
				parts[i] += ":" + action
			}
			continue
		}
		if segment != "" {
			parts[i] = "{id}"
		}
	}
	route := "/" + strings.Join(parts, "/")
	if len(route) > 256 {
		return "/{unmatched}"
	}
	return route
}

func isAPIVersion(value string) bool {
	if len(value) < 2 || value[0] != 'v' {
		return false
	}
	for _, r := range value[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func normalizeActionSegment(segment string) string {
	_, action, found := strings.Cut(segment, ":")
	if !found {
		return "{id}"
	}
	if isKnownRouteAction(action) {
		return "{id}:" + action
	}
	return "{id}"
}

func isKnownRouteAction(action string) bool {
	switch action {
	case "publish", "run", "cancel", "getIamPolicy", "setIamPolicy", "testIamPermissions":
		return true
	default:
		return false
	}
}

func (m *Manager) writeLog(record Record) {
	m.logMu.Lock()
	defer m.logMu.Unlock()
	_ = json.NewEncoder(m.logWriter).Encode(record)
}

type metricKey struct {
	service, method, route, statusClass string
}

type metricValue struct {
	count        uint64
	latencySum   float64
	latencyCount uint64
}

type quotaMetricKey struct {
	service, route, scope string
}

type resourceMetricKey struct {
	service, resourceKind string
}

type Metrics struct {
	mu               sync.RWMutex
	values           map[metricKey]metricValue
	quotaRejections  map[quotaMetricKey]uint64
	resourceCounters map[resourceMetricKey]func() int
	knownServices    map[string]struct{}
}

func NewMetrics(knownServices ...string) *Metrics {
	known := make(map[string]struct{}, len(knownServices)+1)
	addKnownServices(known, knownServices...)
	addKnownServices(known, "minisky.local")
	return &Metrics{
		values:           make(map[metricKey]metricValue),
		quotaRejections:  make(map[quotaMetricKey]uint64),
		resourceCounters: make(map[resourceMetricKey]func() int),
		knownServices:    known,
	}
}

func addKnownServices(known map[string]struct{}, services ...string) {
	for _, service := range services {
		if normalized := normalizeService(service); normalized != "unresolved" {
			known[normalized] = struct{}{}
		}
	}
}

func (m *Metrics) normalizeService(service string) string {
	service = normalizeService(service)
	if _, ok := m.knownServices[service]; ok {
		return service
	}
	return "other"
}

func (m *Metrics) Observe(labels RequestLabels, method string, status int, latencySeconds float64) {
	key := metricKey{
		service:     m.normalizeService(labels.Service),
		method:      normalizeMethod(method),
		route:       NormalizeRoute(labels.Route),
		statusClass: fmt.Sprintf("%dxx", status/100),
	}
	m.mu.Lock()
	if _, exists := m.values[key]; !exists && len(m.values) >= maxMetricSeries-1 {
		key = metricKey{service: "other", method: "OTHER", route: "/{unmatched}", statusClass: "other"}
	}
	value := m.values[key]
	value.count++
	value.latencyCount++
	value.latencySum += latencySeconds
	m.values[key] = value
	m.mu.Unlock()
}

func (m *Manager) ObserveQuotaRejection(labels RequestLabels, scope string) {
	switch scope {
	case "route", "service", "project", "default":
	default:
		scope = "unknown"
	}
	key := quotaMetricKey{
		service: m.telemetryService(labels.Service),
		route:   NormalizeRoute(labels.Route),
		scope:   scope,
	}
	m.metrics.mu.Lock()
	if _, exists := m.metrics.quotaRejections[key]; !exists && len(m.metrics.quotaRejections) >= maxMetricSeries-1 {
		key = quotaMetricKey{service: "other", route: "/{unmatched}", scope: "unknown"}
	}
	m.metrics.quotaRejections[key]++
	m.metrics.mu.Unlock()
}

// RegisterResourceCounter adds a scrape-time gauge keyed only by a stable
// service and resource kind. Project IDs and resource names are intentionally
// excluded to keep the metric cardinality bounded.
func (m *Manager) RegisterResourceCounter(service, resourceKind string, counter func() int) {
	if counter == nil {
		return
	}
	key := resourceMetricKey{
		service:      m.telemetryService(service),
		resourceKind: normalizeResourceKind(resourceKind),
	}
	m.metrics.mu.Lock()
	if _, exists := m.metrics.resourceCounters[key]; !exists && len(m.metrics.resourceCounters) >= maxMetricSeries-1 {
		key = resourceMetricKey{service: "other", resourceKind: "unknown"}
	}
	m.metrics.resourceCounters[key] = counter
	m.metrics.mu.Unlock()
}

func normalizeResourceKind(kind string) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "" || len(kind) > 64 {
		return "unknown"
	}
	for _, char := range kind {
		if !(char == '_' || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')) {
			return "unknown"
		}
	}
	return kind
}

func normalizeMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

func (m *Manager) MetricsHandler() http.Handler {
	return localOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.metrics.writePrometheus(w)
	}))
}

// DiagnosticsHandler exposes the local operational API. It must be mounted
// outside the public GCP gateway so diagnostics never enter service validation.
func (m *Manager) DiagnosticsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/diagnostics/metrics", m.MetricsHandler())
	mux.HandleFunc("/api/diagnostics/requests", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		project, ok := diagnosticsProject(w, r)
		if !ok {
			return
		}
		status, _ := strconv.Atoi(r.URL.Query().Get("status"))
		writeJSON(w, http.StatusOK, map[string]any{"requests": m.store.Query(Query{
			Service: r.URL.Query().Get("service"),
			Method:  strings.ToUpper(r.URL.Query().Get("method")),
			Status:  status,
			Project: project,
		})})
	})
	mux.HandleFunc("/api/diagnostics/traces", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		project, ok := diagnosticsProject(w, r)
		if !ok {
			return
		}
		records := m.store.Query(Query{TraceID: r.URL.Query().Get("traceId"), Project: project})
		traces := records[:0]
		for _, record := range records {
			if record.TraceID != "" {
				traces = append(traces, record)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"traces": traces})
	})
	mux.HandleFunc("/api/diagnostics/requests/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/replay") {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		requestID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/diagnostics/requests/"), "/replay")
		if requestID == "" || strings.Contains(requestID, "/") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request ID"})
			return
		}
		project, ok := diagnosticsProject(w, r)
		if !ok {
			return
		}
		result, err := m.Replay(r.Context(), project, requestID)
		if err != nil {
			status := http.StatusConflict
			if errors.Is(err, ErrRecordNotFound) {
				status = http.StatusNotFound
			}
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	return localOnly(mux)
}

func diagnosticsProject(w http.ResponseWriter, r *http.Request) (string, bool) {
	headerProject := strings.TrimSpace(r.Header.Get("X-MiniSky-Project"))
	queryProject := strings.TrimSpace(r.URL.Query().Get("project"))
	if headerProject == "" || queryProject == "" || headerProject != queryProject {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "diagnostics require matching X-MiniSky-Project and project query values",
		})
		return "", false
	}
	return headerProject, true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (m *Metrics) writePrometheus(w io.Writer) {
	m.mu.RLock()
	keys := make([]metricKey, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j])
	})
	values := make(map[metricKey]metricValue, len(m.values))
	for key, value := range m.values {
		values[key] = value
	}
	quotaKeys := make([]quotaMetricKey, 0, len(m.quotaRejections))
	quotaValues := make(map[quotaMetricKey]uint64, len(m.quotaRejections))
	for key, value := range m.quotaRejections {
		quotaKeys = append(quotaKeys, key)
		quotaValues[key] = value
	}
	resourceKeys := make([]resourceMetricKey, 0, len(m.resourceCounters))
	resourceCounters := make(map[resourceMetricKey]func() int, len(m.resourceCounters))
	for key, counter := range m.resourceCounters {
		resourceKeys = append(resourceKeys, key)
		resourceCounters[key] = counter
	}
	m.mu.RUnlock()
	sort.Slice(quotaKeys, func(i, j int) bool {
		return fmt.Sprint(quotaKeys[i]) < fmt.Sprint(quotaKeys[j])
	})
	sort.Slice(resourceKeys, func(i, j int) bool {
		return fmt.Sprint(resourceKeys[i]) < fmt.Sprint(resourceKeys[j])
	})

	fmt.Fprintln(w, "# HELP minisky_gateway_requests_total Total public gateway requests.")
	fmt.Fprintln(w, "# TYPE minisky_gateway_requests_total counter")
	for _, key := range keys {
		value := values[key]
		labels := prometheusLabels(key)
		fmt.Fprintf(w, "minisky_gateway_requests_total{%s} %d\n", labels, value.count)
	}
	fmt.Fprintln(w, "# HELP minisky_gateway_request_duration_seconds Public gateway request latency.")
	fmt.Fprintln(w, "# TYPE minisky_gateway_request_duration_seconds summary")
	for _, key := range keys {
		value := values[key]
		labels := prometheusLabels(key)
		fmt.Fprintf(w, "minisky_gateway_request_duration_seconds_sum{%s} %g\n", labels, value.latencySum)
		fmt.Fprintf(w, "minisky_gateway_request_duration_seconds_count{%s} %d\n", labels, value.latencyCount)
	}
	fmt.Fprintln(w, "# HELP minisky_gateway_quota_rejections_total Total requests rejected by local quota scope.")
	fmt.Fprintln(w, "# TYPE minisky_gateway_quota_rejections_total counter")
	for _, key := range quotaKeys {
		fmt.Fprintf(
			w,
			"minisky_gateway_quota_rejections_total{service=%q,route=%q,scope=%q} %d\n",
			key.service,
			key.route,
			key.scope,
			quotaValues[key],
		)
	}
	fmt.Fprintln(w, "# HELP minisky_resources Current locally emulated resource count.")
	fmt.Fprintln(w, "# TYPE minisky_resources gauge")
	for _, key := range resourceKeys {
		count := resourceCounters[key]()
		if count < 0 {
			count = 0
		}
		fmt.Fprintf(
			w,
			"minisky_resources{service=%q,resource_kind=%q} %d\n",
			key.service,
			key.resourceKind,
			count,
		)
	}
}

func prometheusLabels(key metricKey) string {
	return fmt.Sprintf(
		`service=%q,method=%q,route=%q,status_class=%q`,
		key.service, key.method, key.route, key.statusClass,
	)
}

func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil || !isLoopback(host) {
			http.Error(w, "diagnostics are available only from loopback", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type replayPayload struct {
	method     string
	host       string
	path       string
	rawQuery   string
	headers    http.Header
	body       []byte
	captureErr error
}

type replayRestoreBody struct {
	io.Reader
	io.Closer
}

type ReplayResult struct {
	Status    int    `json:"status"`
	RequestID string `json:"requestId,omitempty"`
}

func (m *Manager) captureReplay(r *http.Request, labels RequestLabels) (*replayPayload, error) {
	payload := &replayPayload{
		method:   r.Method,
		host:     normalizeReplayHost(r.Host),
		path:     r.URL.EscapedPath(),
		rawQuery: r.URL.RawQuery,
		headers:  make(http.Header),
	}
	if replayTargetIsSensitive(labels, r.URL.Query()) {
		return nil, ErrPayloadRedacted
	}
	for _, header := range []string{"Accept", "Content-Type", "If-Match", "If-None-Match"} {
		if value := r.Header.Get(header); value != "" && len(value) <= 512 {
			payload.headers.Set(header, value)
		}
	}
	if r.Body == nil || r.Body == http.NoBody {
		return payload, nil
	}
	originalBody := r.Body
	body, err := io.ReadAll(io.LimitReader(originalBody, m.replayMaxBytes+1))
	if err != nil {
		r.Body = &replayRestoreBody{
			Reader: io.MultiReader(bytes.NewReader(body), originalBody),
			Closer: originalBody,
		}
		return nil, ErrPayloadOmitted
	}
	r.Body = &replayRestoreBody{
		Reader: io.MultiReader(bytes.NewReader(body), originalBody),
		Closer: originalBody,
	}
	if int64(len(body)) > m.replayMaxBytes {
		return nil, ErrPayloadOmitted
	}
	if len(body) == 0 {
		return payload, nil
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		return nil, ErrPayloadRedacted
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil || containsSensitiveJSON(decoded) {
		return nil, ErrPayloadRedacted
	}
	payload.body = append([]byte(nil), body...)
	return payload, nil
}

func replayTargetIsSensitive(labels RequestLabels, query url.Values) bool {
	switch labels.Service {
	case "secretmanager.googleapis.com", "cloudkms.googleapis.com",
		"iam.googleapis.com", "identitytoolkit.googleapis.com":
		return true
	}
	lowerRoute := strings.ToLower(labels.Route)
	for _, fragment := range []string{"secret", "cryptokey", "serviceaccount", "credential", "token", "encrypt", "decrypt", "signblob"} {
		if strings.Contains(lowerRoute, fragment) {
			return true
		}
	}
	for key := range query {
		if isSensitiveName(key) {
			return true
		}
	}
	return false
}

func normalizeReplayHost(hostport string) string {
	host := strings.TrimSpace(hostport)
	if parsed, port, err := net.SplitHostPort(host); err == nil {
		if isLoopback(parsed) {
			return net.JoinHostPort("127.0.0.1", port)
		}
		host = parsed
	}
	if isLoopback(strings.Trim(host, "[]")) {
		return "localhost"
	}
	return normalizeService(host)
}

func containsSensitiveJSON(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if isSensitiveName(key) {
				return true
			}
			if containsSensitiveJSON(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsSensitiveJSON(child) {
				return true
			}
		}
	}
	return false
}

func isSensitiveName(name string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(name, "_", ""), "-", ""))
	if normalized == "key" {
		return true
	}
	for _, sensitive := range []string{"authorization", "cookie", "password", "passwd", "token", "secret", "apikey", "privatekey", "credential"} {
		if strings.Contains(normalized, sensitive) {
			return true
		}
	}
	return false
}

func (m *Manager) SetReplayTarget(target http.Handler) {
	m.replayMu.Lock()
	m.replayTarget = target
	m.replayMu.Unlock()
}

func (m *Manager) Replay(ctx context.Context, project, requestID string) (ReplayResult, error) {
	if !m.replayEnabled {
		return ReplayResult{}, ErrReplayDisabled
	}
	record, ok := m.store.find(strings.TrimSpace(project), requestID)
	if !ok {
		return ReplayResult{}, ErrRecordNotFound
	}
	if record.replay == nil {
		return ReplayResult{}, ErrPayloadOmitted
	}
	if record.replay.captureErr != nil {
		return ReplayResult{}, record.replay.captureErr
	}
	m.replayMu.RLock()
	target := m.replayTarget
	m.replayMu.RUnlock()
	if target == nil {
		return ReplayResult{}, errors.New("replay target is not configured")
	}
	payload := record.replay
	requestURL := "http://localhost" + payload.path
	if payload.rawQuery != "" {
		requestURL += "?" + payload.rawQuery
	}
	request, err := http.NewRequestWithContext(ctx, payload.method, requestURL, bytes.NewReader(payload.body))
	if err != nil {
		return ReplayResult{}, fmt.Errorf("build replay request: %w", err)
	}
	request.Host = payload.host
	request.Header = payload.headers.Clone()
	request.Header.Set("X-MiniSky-Project", record.Project)
	request.Header.Set(replayHeader, "1")
	response := newBoundedReplayResponseWriter(m.replayMaxResponseBytes)
	target.ServeHTTP(response, request)
	if response.overflow {
		return ReplayResult{}, ErrReplayResponseTooLarge
	}
	return ReplayResult{
		Status:    response.statusCode(),
		RequestID: response.Header().Get(RequestIDHeader),
	}, nil
}

type boundedReplayResponseWriter struct {
	header   http.Header
	status   int
	written  int64
	limit    int64
	overflow bool
}

func newBoundedReplayResponseWriter(limit int64) *boundedReplayResponseWriter {
	return &boundedReplayResponseWriter{header: make(http.Header), limit: limit}
}

func (writer *boundedReplayResponseWriter) Header() http.Header { return writer.header }

func (writer *boundedReplayResponseWriter) WriteHeader(status int) {
	if writer.status == 0 {
		writer.status = status
	}
}

func (writer *boundedReplayResponseWriter) Write(value []byte) (int, error) {
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	remaining := writer.limit - writer.written
	if remaining <= 0 {
		writer.overflow = true
		return 0, ErrReplayResponseTooLarge
	}
	if int64(len(value)) > remaining {
		writer.written = writer.limit
		writer.overflow = true
		return int(remaining), ErrReplayResponseTooLarge
	}
	writer.written += int64(len(value))
	return len(value), nil
}

func (writer *boundedReplayResponseWriter) Flush() {
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
}

func (writer *boundedReplayResponseWriter) statusCode() int {
	if writer.status == 0 {
		return http.StatusOK
	}
	return writer.status
}

type instrumentedTransport struct{ base http.RoundTripper }

// PrivacySafeTransport is the explicit opt-in boundary for custom transports.
// Implementations attest that they do not perform telemetry on the raw request.
type PrivacySafeTransport interface {
	http.RoundTripper
	MiniSkyPrivacySafeTransport()
}

// trustedTransport marks a custom transport whose implementation is known not
// to perform its own telemetry before dispatching the request.
type trustedTransport struct{ base http.RoundTripper }

// TrustTransport explicitly opts a custom transport into MiniSky's privacy
// boundary. Callers must not use it for telemetry-decorated transports.
func TrustTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if _, ok := base.(*trustedTransport); ok {
		return base
	}
	return &trustedTransport{base: base}
}

func (transport *trustedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport.base.RoundTrip(request)
}

func (*trustedTransport) MiniSkyPrivacySafeTransport() {}

func InstrumentTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	switch typed := base.(type) {
	case *instrumentedTransport:
		return base
	case *http.Transport:
		return &instrumentedTransport{base: typed}
	case PrivacySafeTransport:
		return &instrumentedTransport{base: typed}
	default:
		return &instrumentedTransport{base: rejectUntrustedTransport{}}
	}
}

type rejectUntrustedTransport struct{}

func (rejectUntrustedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errUntrustedTransport
}

func (transport *instrumentedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	safeRequest := request.Clone(sanitizedOutboundContext(request.Context()))
	safeURL := *request.URL
	safeURL.Path = NormalizeRoute(request.URL.Path)
	safeURL.RawPath = ""
	safeURL.RawQuery = ""
	safeURL.ForceQuery = false
	safeURL.User = nil
	safeURL.Host = "outbound.local"
	safeURL.Opaque = ""
	safeURL.Fragment = ""
	safeURL.RawFragment = ""
	safeURL.OmitHost = false
	safeRequest.URL = &safeURL
	safeRequest.Host = "outbound.local"
	safeRequest.RequestURI = ""
	safeRequest.Header = make(http.Header)

	restoring := &restoringRoundTripper{
		base:     transport.base,
		original: request,
	}
	instrumented := otelhttp.NewTransport(
		restoring,
		otelhttp.WithPropagators(traceparentOnlyPropagator{}),
	)
	response, err := instrumented.RoundTrip(safeRequest)
	if restoring.actualErr != nil {
		return response, restoring.actualErr
	}
	return response, err
}

func sanitizedOutboundContext(ctx context.Context) context.Context {
	ctx = baggage.ContextWithBaggage(ctx, baggage.Baggage{})
	span := trace.SpanFromContext(ctx)
	spanContext := span.SpanContext()
	if !spanContext.IsValid() {
		return ctx
	}
	sanitized := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    spanContext.TraceID(),
		SpanID:     spanContext.SpanID(),
		TraceFlags: spanContext.TraceFlags(),
		Remote:     spanContext.IsRemote(),
	})
	return trace.ContextWithSpan(ctx, sanitizedContextSpan{
		Span:        span,
		spanContext: sanitized,
	})
}

type sanitizedContextSpan struct {
	trace.Span
	spanContext trace.SpanContext
}

func (span sanitizedContextSpan) SpanContext() trace.SpanContext { return span.spanContext }

type traceparentOnlyPropagator struct{}

func (traceparentOnlyPropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return
	}
	sanitized := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    spanContext.TraceID(),
		SpanID:     spanContext.SpanID(),
		TraceFlags: spanContext.TraceFlags(),
		Remote:     spanContext.IsRemote(),
	})
	propagation.TraceContext{}.Inject(trace.ContextWithSpanContext(ctx, sanitized), carrier)
}

func (traceparentOnlyPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return propagation.TraceContext{}.Extract(ctx, carrier)
}

func (traceparentOnlyPropagator) Fields() []string { return []string{"traceparent"} }

type restoringRoundTripper struct {
	base      http.RoundTripper
	original  *http.Request
	actualErr error
}

func (transport *restoringRoundTripper) RoundTrip(instrumentedRequest *http.Request) (*http.Response, error) {
	request := transport.original.Clone(instrumentedRequest.Context())
	request.Header = transport.original.Header.Clone()
	request.Header.Del("traceparent")
	request.Header.Del("tracestate")
	request.Header.Del("baggage")
	if traceparent := instrumentedRequest.Header.Get("traceparent"); traceparent != "" {
		request.Header.Set("traceparent", traceparent)
	}
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		transport.actualErr = err
		return response, errOutboundRequest
	}
	return response, nil
}

// Do instruments a single request while retaining every setting and behavior
// of the caller-supplied client, including timeouts and redirect policy.
func Do(client *http.Client, request *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	instrumented := *client
	instrumented.Transport = InstrumentTransport(client.Transport)
	return instrumented.Do(request)
}

func NewReverseProxy(target *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = InstrumentTransport(http.DefaultTransport)
	return proxy
}
