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
	"net/http/httptest"
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
	"go.opentelemetry.io/otel/trace"
)

const (
	RequestIDHeader    = "X-MiniSky-Request-ID"
	MaxRequestIDLength = 64
	replayHeader       = "X-MiniSky-Internal-Replay"
	defaultCapacity    = 1000
	defaultReplayLimit = 64 << 10
)

var (
	ErrRecordNotFound  = errors.New("request record not found")
	ErrReplayDisabled  = errors.New("request replay is disabled")
	ErrPayloadOmitted  = errors.New("request payload was omitted because it exceeded the capture limit")
	ErrPayloadRedacted = errors.New("request payload contained sensitive data and cannot be replayed")
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
	Capacity           int
	LogWriter          io.Writer
	ReplayEnabled      bool
	ReplayMaxBodyBytes int64
	TracerProvider     trace.TracerProvider
}

type Manager struct {
	store          *Store
	metrics        *Metrics
	logWriter      io.Writer
	logMu          sync.Mutex
	replayEnabled  bool
	replayMaxBytes int64
	replayTarget   http.Handler
	replayMu       sync.RWMutex
	tracerProvider trace.TracerProvider
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
	provider := config.TracerProvider
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &Manager{
		store:          NewStore(config.Capacity),
		metrics:        NewMetrics(),
		logWriter:      writer,
		replayEnabled:  config.ReplayEnabled,
		replayMaxBytes: limit,
		tracerProvider: provider,
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

	return otelhttp.NewHandler(
		core,
		"minisky.gateway",
		otelhttp.WithTracerProvider(m.tracerProvider),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if resolve == nil {
				return r.Method + " " + NormalizeRoute(r.URL.Path)
			}
			return r.Method + " " + NormalizeRoute(resolve(r).Route)
		}),
	)
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
		base, _, _ := strings.Cut(segment, ":")
		if isAPIVersion(base) {
			continue
		}
		if _, ok := resourceCollections[base]; ok {
			continue
		}
		if _, ok := staticRouteSegments[base]; ok {
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
	switch action {
	case "publish", "run", "cancel", "getIamPolicy", "setIamPolicy", "testIamPermissions":
		return "{id}:" + action
	default:
		return "{id}"
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
}

func NewMetrics() *Metrics {
	return &Metrics{
		values:           make(map[metricKey]metricValue),
		quotaRejections:  make(map[quotaMetricKey]uint64),
		resourceCounters: make(map[resourceMetricKey]func() int),
	}
}

func (m *Metrics) Observe(labels RequestLabels, method string, status int, latencySeconds float64) {
	key := metricKey{
		service:     normalizeService(labels.Service),
		method:      normalizeMethod(method),
		route:       NormalizeRoute(labels.Route),
		statusClass: fmt.Sprintf("%dxx", status/100),
	}
	m.mu.Lock()
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
		service: normalizeService(labels.Service),
		route:   NormalizeRoute(labels.Route),
		scope:   scope,
	}
	m.metrics.mu.Lock()
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
		service:      normalizeService(service),
		resourceKind: normalizeResourceKind(resourceKind),
	}
	m.metrics.mu.Lock()
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
	response := httptest.NewRecorder()
	target.ServeHTTP(response, request)
	return ReplayResult{
		Status:    response.Code,
		RequestID: response.Header().Get(RequestIDHeader),
	}, nil
}

type instrumentedTransport struct {
	http.RoundTripper
}

func InstrumentTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	switch base.(type) {
	case *instrumentedTransport, *otelhttp.Transport:
		return base
	}
	return &instrumentedTransport{RoundTripper: otelhttp.NewTransport(base)}
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
