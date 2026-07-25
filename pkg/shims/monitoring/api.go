package monitoring

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("monitoring.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.SvcMgr)
	})
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	api.StartCollector()
}

type TimeSeries struct {
	Metric struct {
		Type   string            `json:"type"`
		Labels map[string]string `json:"labels,omitempty"`
	} `json:"metric"`
	Resource struct {
		Type   string            `json:"type"`
		Labels map[string]string `json:"labels,omitempty"`
	} `json:"resource"`
	Points []Point `json:"points"`
}

type Point struct {
	Interval struct {
		StartTime string `json:"startTime,omitempty"`
		EndTime   string `json:"endTime"`
	} `json:"interval"`
	Value struct {
		DoubleValue *float64 `json:"doubleValue,omitempty"`
		Int64Value  *string  `json:"int64Value,omitempty"`
		BoolValue   *bool    `json:"boolValue,omitempty"`
	} `json:"value"`
}

type MetricDescriptor struct {
	Name        string            `json:"name,omitempty"`
	Type        string            `json:"type"`
	Labels      []json.RawMessage `json:"labels,omitempty"`
	MetricKind  string            `json:"metricKind"`
	ValueType   string            `json:"valueType"`
	Unit        string            `json:"unit,omitempty"`
	Description string            `json:"description,omitempty"`
	DisplayName string            `json:"displayName,omitempty"`
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

const monitoringStateEntry = "monitoring/metadata"

const maxPromQLQueryLength = 4 << 10

type monitoringMetadata struct {
	Descriptors map[string]MetricDescriptor `json:"descriptors"`
	Series      map[string][]TimeSeries     `json:"series"`
}

type API struct {
	mu          sync.RWMutex
	persistMu   sync.Mutex
	collector   sync.Once
	svcMgr      *orchestrator.ServiceManager
	store       stateStore
	descriptors map[string]MetricDescriptor
	series      map[string][]TimeSeries
	initErr     error
	now         func() time.Time
}

func NewAPI(sm *orchestrator.ServiceManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Monitoring] state disabled: %v", err)
		return newAPI(sm, nil)
	}
	api, err := NewAPIWithStore(sm, store)
	if err != nil {
		log.Printf("[Shim: Monitoring] state rehydration failed: %v", err)
		disabled := newAPI(sm, nil)
		disabled.initErr = err
		return disabled
	}
	return api
}

func NewAPIWithStore(sm *orchestrator.ServiceManager, store stateStore) (*API, error) {
	api := newAPI(sm, store)
	if store == nil {
		return api, nil
	}
	var saved monitoringMetadata
	if err := store.Load(monitoringStateEntry, &saved); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Monitoring metadata: %w", err)
	}
	if saved.Descriptors != nil {
		api.descriptors = saved.Descriptors
	}
	if saved.Series != nil {
		api.series = saved.Series
	}
	return api, nil
}

func newAPI(sm *orchestrator.ServiceManager, store stateStore) *API {
	return &API{
		svcMgr:      sm,
		store:       store,
		descriptors: make(map[string]MetricDescriptor),
		series:      make(map[string][]TimeSeries),
		now:         time.Now,
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Monitoring] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initErr != nil {
		writeError(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION", "Monitoring state is unavailable")
		return
	}

	switch {
	case strings.HasSuffix(r.URL.Path, "/timeSeries:query"):
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Monitoring Query Language is not implemented")
	case strings.HasSuffix(r.URL.Path, "/prometheus/api/v1/query_range"):
		api.handlePromQLRange(w, r)
	case strings.HasSuffix(r.URL.Path, "/prometheus/api/v1/query"):
		api.handlePromQLInstant(w, r)
	case strings.Contains(r.URL.Path, "/metricDescriptors"):
		api.handleMetricDescriptors(w, r)
	case strings.HasSuffix(r.URL.Path, "/timeSeries"):
		api.handleTimeSeries(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Monitoring resource not found")
	}
}

type promQLSample struct {
	Metric map[string]string `json:"metric"`
	Value  []any             `json:"value"`
}

type promQLCandidate struct {
	sample    promQLSample
	pointTime time.Time
}

func (api *API) handlePromQLInstant(w http.ResponseWriter, r *http.Request) {
	project, location, ok := prometheusScope(r.URL.Path)
	if !ok || project == "" || location != "global" {
		writePromQLError(w, http.StatusBadRequest, "bad_data", "only location global is supported")
		return
	}
	query, ok := promQLQueryParameter(w, r)
	if !ok {
		return
	}
	metricType, parseStatus, err := parseExactMetricSelector(query)
	if err != nil {
		writePromQLError(w, parseStatus, promQLErrorType(parseStatus), err.Error())
		return
	}
	evaluationTime, err := api.promQLEvaluationTime(r)
	if err != nil {
		writePromQLError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}

	api.mu.RLock()
	candidates := make(map[string]promQLCandidate)
	for _, series := range api.series[project] {
		if series.Metric.Type != metricType {
			continue
		}
		value, pointTime, found := latestNumericPoint(series.Points, evaluationTime)
		if !found {
			continue
		}
		labels := make(map[string]string, len(series.Metric.Labels)+1)
		for name, labelValue := range series.Metric.Labels {
			labels[name] = labelValue
		}
		labels["__name__"] = metricType
		key := stableLabelKey(labels)
		if current, exists := candidates[key]; exists && pointTime.Before(current.pointTime) {
			continue
		}
		candidates[key] = promQLCandidate{pointTime: pointTime, sample: promQLSample{
			Metric: labels,
			Value:  []any{unixSeconds(evaluationTime), value},
		}}
	}
	api.mu.RUnlock()
	samples := make([]promQLSample, 0, len(candidates))
	for _, candidate := range candidates {
		samples = append(samples, candidate.sample)
	}
	sort.SliceStable(samples, func(i, j int) bool {
		return stableLabelKey(samples[i].Metric) < stableLabelKey(samples[j].Metric)
	})
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result":     samples,
		},
	})
}

func (api *API) handlePromQLRange(w http.ResponseWriter, r *http.Request) {
	_, location, ok := prometheusScope(r.URL.Path)
	if !ok || location != "global" {
		writePromQLError(w, http.StatusBadRequest, "bad_data", "only location global is supported")
		return
	}
	writePromQLError(w, http.StatusUnprocessableEntity, "execution", "range queries are not supported")
}

func prometheusScope(path string) (project, location string, ok bool) {
	const suffix = "/prometheus/api/v1/"
	prefix, _, found := strings.Cut(path, suffix)
	if !found {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(prefix, "/"), "/")
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "location" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func promQLQueryParameter(w http.ResponseWriter, r *http.Request) (string, bool) {
	var query string
	switch r.Method {
	case http.MethodGet:
		query = r.URL.Query().Get("query")
	case http.MethodPost:
		if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/x-www-form-urlencoded") {
			writePromQLError(w, http.StatusBadRequest, "bad_data", "POST requires form-encoded parameters")
			return "", false
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxPromQLQueryLength+1024)
		if err := r.ParseForm(); err != nil {
			writePromQLError(w, http.StatusBadRequest, "bad_data", "invalid form parameters")
			return "", false
		}
		query = r.Form.Get("query")
	default:
		writePromQLError(w, http.StatusMethodNotAllowed, "bad_data", "method is not supported")
		return "", false
	}
	if query == "" {
		writePromQLError(w, http.StatusBadRequest, "bad_data", "query parameter is required")
		return "", false
	}
	if len(query) > maxPromQLQueryLength {
		writePromQLError(w, http.StatusBadRequest, "bad_data", "query exceeds 4096 bytes")
		return "", false
	}
	return query, true
}

func parseExactMetricSelector(query string) (string, int, error) {
	query = strings.TrimSpace(query)
	const prefix = `{__name__=`
	if !strings.HasPrefix(query, prefix) || !strings.HasSuffix(query, "}") {
		return "", http.StatusUnprocessableEntity, errors.New("expression is outside the supported exact metric selector subset")
	}
	quoted := strings.TrimSpace(query[len(prefix) : len(query)-1])
	if strings.Contains(quoted, ",") {
		return "", http.StatusUnprocessableEntity, errors.New("label matchers are not supported")
	}
	if len(quoted) < 2 || quoted[0] != '"' || quoted[len(quoted)-1] != '"' {
		return "", http.StatusBadRequest, errors.New("metric name must be double-quoted")
	}
	var metricType string
	if err := json.Unmarshal([]byte(quoted), &metricType); err != nil {
		return "", http.StatusBadRequest, errors.New("invalid quoted metric name")
	}
	if metricType == "" {
		return "", http.StatusBadRequest, errors.New("metric name must not be empty")
	}
	for _, char := range metricType {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("/._-", char) {
			continue
		}
		return "", http.StatusUnprocessableEntity, errors.New("metric name contains unsupported characters")
	}
	return metricType, http.StatusOK, nil
}

func (api *API) promQLEvaluationTime(r *http.Request) (time.Time, error) {
	value := r.FormValue("time")
	if value == "" {
		return api.now(), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed, nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return time.Time{}, errors.New("invalid time parameter")
	}
	whole, fraction := math.Modf(seconds)
	return time.Unix(int64(whole), int64(fraction*float64(time.Second))).UTC(), nil
}

func latestNumericPoint(points []Point, evaluationTime time.Time) (string, time.Time, bool) {
	var (
		latestTime time.Time
		latest     string
		found      bool
	)
	for _, point := range points {
		endTime, err := time.Parse(time.RFC3339Nano, point.Interval.EndTime)
		if err != nil || endTime.After(evaluationTime) {
			continue
		}
		value, ok := numericPointValue(point)
		if !ok || (found && !endTime.After(latestTime)) {
			continue
		}
		latestTime, latest, found = endTime, value, true
	}
	return latest, latestTime, found
}

func numericPointValue(point Point) (string, bool) {
	if point.Value.DoubleValue != nil {
		value := *point.Value.DoubleValue
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return "", false
		}
		return strconv.FormatFloat(value, 'g', -1, 64), true
	}
	if point.Value.Int64Value != nil {
		value, err := strconv.ParseInt(*point.Value.Int64Value, 10, 64)
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(value, 10), true
	}
	return "", false
}

func stableLabelKey(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	var key strings.Builder
	for _, name := range names {
		key.WriteString(strconv.Quote(name))
		key.WriteByte('=')
		key.WriteString(strconv.Quote(labels[name]))
		key.WriteByte(';')
	}
	return key.String()
}

func unixSeconds(value time.Time) float64 {
	return float64(value.Unix()) + float64(value.Nanosecond())/float64(time.Second)
}

func promQLErrorType(code int) string {
	if code == http.StatusBadRequest {
		return "bad_data"
	}
	return "execution"
}

func writePromQLError(w http.ResponseWriter, code int, errorType, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "error", "errorType": errorType, "error": message,
	})
}

func (api *API) handleMetricDescriptors(w http.ResponseWriter, r *http.Request) {
	project := extractProject(r.URL.Path)
	descriptorType := descriptorTypeFromPath(r.URL)
	switch r.Method {
	case http.MethodPost:
		if descriptorType != "" {
			writeError(w, http.StatusMethodNotAllowed, "INVALID_ARGUMENT", "POST is only supported on the metricDescriptors collection")
			return
		}
		var descriptor MetricDescriptor
		if err := decodeJSON(r, &descriptor); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid metric descriptor JSON")
			return
		}
		if descriptor.Type == "" || descriptor.MetricKind == "" || descriptor.ValueType == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "type, metricKind, and valueType are required")
			return
		}
		key := project + ":" + descriptor.Type
		descriptor.Name = "projects/" + project + "/metricDescriptors/" + descriptor.Type
		err := api.commitMutation(func() error {
			if _, exists := api.descriptors[key]; exists {
				return errMonitoringAlreadyExists
			}
			api.descriptors[key] = descriptor
			return nil
		})
		if errors.Is(err, errMonitoringAlreadyExists) {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Metric descriptor already exists: "+descriptor.Type)
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist metric descriptor")
			return
		}
		_ = json.NewEncoder(w).Encode(descriptor)
	case http.MethodGet:
		if descriptorType != "" {
			api.mu.RLock()
			descriptor, ok := api.descriptors[project+":"+descriptorType]
			api.mu.RUnlock()
			if !ok {
				writeError(w, http.StatusNotFound, "NOT_FOUND", "Metric descriptor not found: "+descriptorType)
				return
			}
			_ = json.NewEncoder(w).Encode(descriptor)
			return
		}
		api.mu.RLock()
		descriptors := make([]MetricDescriptor, 0)
		for key, descriptor := range api.descriptors {
			if strings.HasPrefix(key, project+":") {
				descriptors = append(descriptors, descriptor)
			}
		}
		api.mu.RUnlock()
		sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].Type < descriptors[j].Type })
		_ = json.NewEncoder(w).Encode(map[string]any{"metricDescriptors": descriptors})
	case http.MethodDelete:
		if descriptorType == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "metric descriptor type is required")
			return
		}
		key := project + ":" + descriptorType
		err := api.commitMutation(func() error {
			if _, ok := api.descriptors[key]; !ok {
				return errMonitoringNotFound
			}
			delete(api.descriptors, key)
			return nil
		})
		if errors.Is(err, errMonitoringNotFound) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Metric descriptor not found: "+descriptorType)
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist metric descriptor deletion")
			return
		}
		_, _ = w.Write([]byte("{}"))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) handleTimeSeries(w http.ResponseWriter, r *http.Request) {
	project := extractProject(r.URL.Path)
	switch r.Method {
	case http.MethodPost:
		var body struct {
			TimeSeries []TimeSeries `json:"timeSeries"`
		}
		if err := decodeJSON(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid timeSeries JSON")
			return
		}
		if len(body.TimeSeries) == 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "timeSeries must contain at least one series")
			return
		}
		if len(body.TimeSeries) > 200 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "timeSeries may contain at most 200 series per request")
			return
		}
		for _, series := range body.TimeSeries {
			if series.Metric.Type == "" || series.Resource.Type == "" || len(series.Points) == 0 {
				writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "metric.type, resource.type, and points are required")
				return
			}
		}
		if err := api.commitMutation(func() error {
			api.series[project] = append(api.series[project], body.TimeSeries...)
			if len(api.series[project]) > 10000 {
				api.series[project] = api.series[project][len(api.series[project])-10000:]
			}
			return nil
		}); err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist time series")
			return
		}
		_, _ = w.Write([]byte("{}"))
	case http.MethodGet:
		metricType, supported := metricTypeFilter(r.URL.Query().Get("filter"))
		if !supported {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "only metric.type equality filters are supported")
			return
		}
		api.mu.RLock()
		all := api.series[project]
		result := make([]TimeSeries, 0, len(all))
		for _, series := range all {
			if metricType == "" || series.Metric.Type == metricType {
				result = append(result, series)
			}
		}
		api.mu.RUnlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"timeSeries": result})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) persist() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	payload, err := json.Marshal(monitoringMetadata{Descriptors: api.descriptors, Series: api.series})
	api.mu.RUnlock()
	if err != nil {
		return err
	}
	return api.store.Save(monitoringStateEntry, json.RawMessage(payload))
}

var (
	errMonitoringAlreadyExists = errors.New("monitoring resource already exists")
	errMonitoringNotFound      = errors.New("monitoring resource not found")
)

func (api *API) commitMutation(mutate func() error) error {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.Lock()
	previousDescriptors := make(map[string]MetricDescriptor, len(api.descriptors))
	for key, descriptor := range api.descriptors {
		previousDescriptors[key] = descriptor
	}
	previousSeries := make(map[string][]TimeSeries, len(api.series))
	for project, series := range api.series {
		previousSeries[project] = append([]TimeSeries(nil), series...)
	}
	if err := mutate(); err != nil {
		api.mu.Unlock()
		return err
	}
	payload, err := json.Marshal(monitoringMetadata{Descriptors: api.descriptors, Series: api.series})
	api.mu.Unlock()
	if err == nil && api.store != nil {
		err = api.store.Save(monitoringStateEntry, json.RawMessage(payload))
	}
	if err != nil {
		api.mu.Lock()
		api.descriptors = previousDescriptors
		api.series = previousSeries
		api.mu.Unlock()
	}
	return err
}

func (api *API) GetStats() map[string][]Point {
	api.mu.RLock()
	defer api.mu.RUnlock()
	result := make(map[string][]Point)
	for _, series := range api.series {
		for _, item := range series {
			result[item.Metric.Type] = append(result[item.Metric.Type], item.Points...)
		}
	}
	return result
}

func (api *API) StartCollector() {
	if api.svcMgr == nil || api.initErr != nil {
		return
	}
	log.Printf("[Monitoring] 📈 Starting Metrics Collector...")
	api.collector.Do(func() {
		go func() {
			for {
				containers := api.svcMgr.ListManagedContainers()
				now := time.Now().Format(time.RFC3339)
				type collectedPoint struct {
					container  string
					metricType string
					point      Point
				}
				collected := make([]collectedPoint, 0, len(containers)*2)
				for _, c := range containers {
					if !strings.Contains(c.Status, "Up") {
						continue
					}

					stats, err := api.svcMgr.GetContainerStats(c.Name)
					if err != nil {
						continue
					}

					name := strings.TrimPrefix(c.Name, "minisky-")

					cpuPoint := Point{}
					cpuPoint.Interval.EndTime = now
					cpuVal := stats.CPUPercentage
					cpuPoint.Value.DoubleValue = &cpuVal
					memPoint := Point{}
					memPoint.Interval.EndTime = now
					memVal := stats.MemoryUsageMB
					memPoint.Value.DoubleValue = &memVal
					collected = append(collected,
						collectedPoint{name, "custom.googleapis.com/minisky/container/cpu_percent", cpuPoint},
						collectedPoint{name, "custom.googleapis.com/minisky/container/memory_mb", memPoint},
					)
				}
				if len(collected) > 0 {
					if err := api.commitMutation(func() error {
						for _, sample := range collected {
							api.appendCollectedSeriesLocked(sample.container, sample.metricType, sample.point)
						}
						return nil
					}); err != nil {
						log.Printf("[Monitoring] persist collected metrics: %v", err)
					}
				}
				time.Sleep(10 * time.Second)
			}
		}()
	})
}

func (api *API) appendCollectedSeriesLocked(container, metricType string, point Point) {
	series := TimeSeries{Points: []Point{point}}
	series.Metric.Type = metricType
	series.Metric.Labels = map[string]string{"container": container}
	series.Resource.Type = "generic_node"
	api.series["_minisky"] = append(api.series["_minisky"], series)
	if len(api.series["_minisky"]) > 120 {
		api.series["_minisky"] = api.series["_minisky"][len(api.series["_minisky"])-120:]
	}
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func extractProject(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == "projects" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func descriptorTypeFromPath(requestURL *url.URL) string {
	escaped := requestURL.EscapedPath()
	marker := "/metricDescriptors/"
	index := strings.Index(escaped, marker)
	if index < 0 {
		return ""
	}
	value, err := url.PathUnescape(escaped[index+len(marker):])
	if err != nil {
		return ""
	}
	return value
}

func metricTypeFilter(filter string) (string, bool) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return "", true
	}
	left, right, ok := strings.Cut(filter, "=")
	if !ok || strings.TrimSpace(left) != "metric.type" {
		return "", false
	}
	value := strings.Trim(strings.TrimSpace(right), `"`)
	if value == "" {
		return "", false
	}
	return value, true
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "status": status, "message": message},
	})
}
