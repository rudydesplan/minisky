package monitoring

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"minisky/pkg/router"
	"minisky/pkg/state"
)

func TestMetricDescriptorLifecycleAndRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "monitoring")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}

	create := monitoringRequest(api, http.MethodPost, "/v3/projects/test/metricDescriptors",
		`{"type":"custom.googleapis.com/requests","metricKind":"GAUGE","valueType":"DOUBLE","description":"request count"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	duplicate := monitoringRequest(api, http.MethodPost, "/v3/projects/test/metricDescriptors",
		`{"type":"custom.googleapis.com/requests","metricKind":"GAUGE","valueType":"DOUBLE"}`)
	assertMonitoringError(t, duplicate, http.StatusConflict, "ALREADY_EXISTS")

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	get := monitoringRequest(restarted, http.MethodGet,
		"/v3/projects/test/metricDescriptors/custom.googleapis.com%2Frequests", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), "request count") {
		t.Fatalf("get after restart status = %d, body = %s", get.Code, get.Body.String())
	}
	list := monitoringRequest(restarted, http.MethodGet, "/v3/projects/test/metricDescriptors", "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"metricDescriptors"`) {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	deleted := monitoringRequest(restarted, http.MethodDelete,
		"/v3/projects/test/metricDescriptors/custom.googleapis.com%2Frequests", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleted.Code, deleted.Body.String())
	}
	missing := monitoringRequest(restarted, http.MethodGet,
		"/v3/projects/test/metricDescriptors/custom.googleapis.com%2Frequests", "")
	assertMonitoringError(t, missing, http.StatusNotFound, "NOT_FOUND")
}

func TestTimeSeriesWriteListFilterAndMQLUnsupported(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	write := monitoringRequest(api, http.MethodPost, "/v3/projects/test/timeSeries", `{
		"timeSeries":[{
			"metric":{"type":"custom.googleapis.com/requests","labels":{"route":"/"}},
			"resource":{"type":"global","labels":{"project_id":"test"}},
			"points":[{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"doubleValue":2.5}}]
		}]
	}`)
	if write.Code != http.StatusOK {
		t.Fatalf("write status = %d, body = %s", write.Code, write.Body.String())
	}

	list := monitoringRequest(api, http.MethodGet,
		`/v3/projects/test/timeSeries?filter=metric.type%3D%22custom.googleapis.com%2Frequests%22`, "")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}
	var response struct {
		TimeSeries []TimeSeries `json:"timeSeries"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.TimeSeries) != 1 || response.TimeSeries[0].Metric.Type != "custom.googleapis.com/requests" {
		t.Fatalf("time series = %#v", response.TimeSeries)
	}

	mql := monitoringRequest(api, http.MethodPost, "/v3/projects/test/timeSeries:query", `{}`)
	assertMonitoringError(t, mql, http.StatusNotImplemented, "UNIMPLEMENTED")
}

func TestPromQLInstantQueryGETAndFormPOST(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.now = func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }
	writePromQLFixture(t, api, "test", `[
		{
			"metric":{"type":"custom.googleapis.com/requests-total","labels":{"route":"/b"}},
			"resource":{"type":"global"},
			"points":[
				{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"doubleValue":2.5}},
				{"interval":{"endTime":"2026-07-25T11:00:00Z"},"value":{"doubleValue":9}}
			]
		},
		{
			"metric":{"type":"custom.googleapis.com/requests-total","labels":{"route":"/a"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"2026-07-25T10:15:00Z"},"value":{"int64Value":"7"}}]
		},
		{
			"metric":{"type":"custom.googleapis.com/requests-total","labels":{"route":"/skip"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"not-a-time"},"value":{"boolValue":true}}]
		},
		{
			"metric":{"type":"custom.googleapis.com/requests-total","labels":{"route":"/bool"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"boolValue":true}}]
		},
		{
			"metric":{"type":"custom.googleapis.com/requests-total","labels":{"route":"/invalid-int"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"int64Value":"not-an-int"}}]
		}
	]`)
	writePromQLFixture(t, api, "other", `[
		{
			"metric":{"type":"custom.googleapis.com/requests-total","labels":{"route":"/leak"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"doubleValue":99}}]
		}
	]`)

	query := `{__name__="custom.googleapis.com/requests-total"}`
	get := monitoringRequest(api, http.MethodGet,
		"/v1/projects/test/location/global/prometheus/api/v1/query?query="+url.QueryEscape(query)+"&time=2026-07-25T10:30:00Z", "")
	assertPromQLVector(t, get, 1784975400, "custom.googleapis.com/requests-total", []promQLWant{
		{route: "/a", value: "7"},
		{route: "/b", value: "2.5"},
	})
	unixTime := monitoringRequest(api, http.MethodGet,
		"/v1/projects/test/location/global/prometheus/api/v1/query?query="+url.QueryEscape(query)+"&time=1784975400", "")
	assertPromQLVector(t, unixTime, 1784975400, "custom.googleapis.com/requests-total", []promQLWant{
		{route: "/a", value: "7"},
		{route: "/b", value: "2.5"},
	})

	form := url.Values{"query": {query}}
	post := monitoringFormRequest(api,
		"/v1/projects/test/location/global/prometheus/api/v1/query", form.Encode())
	assertPromQLVector(t, post, 1_800_000_000, "custom.googleapis.com/requests-total", []promQLWant{
		{route: "/a", value: "7"},
		{route: "/b", value: "9"},
	})
}

func TestPromQLInstantQueryDeduplicatesMatchingLabelSets(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	writePromQLFixture(t, api, "dedupe", `[
		{
			"metric":{"type":"custom.googleapis.com/dedupe","labels":{"route":"/a"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"doubleValue":1}}]
		},
		{
			"metric":{"type":"custom.googleapis.com/dedupe","labels":{"route":"/b"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"2026-07-25T10:05:00Z"},"value":{"int64Value":"3"}}]
		}
	]`)
	writePromQLFixture(t, api, "dedupe", `[
		{
			"metric":{"type":"custom.googleapis.com/dedupe","labels":{"route":"/a"}},
			"resource":{"type":"global"},
			"points":[
				{"interval":{"endTime":"2026-07-25T10:20:00Z"},"value":{"int64Value":"malformed"}},
				{"interval":{"endTime":"2026-07-25T11:00:00Z"},"value":{"doubleValue":99}},
				{"interval":{"endTime":"2026-07-25T10:15:00Z"},"value":{"doubleValue":2}}
			]
		},
		{
			"metric":{"type":"custom.googleapis.com/dedupe","labels":{"route":"/a"}},
			"resource":{"type":"global"},
			"points":[{"interval":{"endTime":"2026-07-25T10:25:00Z"},"value":{"boolValue":true}}]
		}
	]`)
	writePromQLFixture(t, api, "dedupe", `[{
		"metric":{"type":"custom.googleapis.com/dedupe","labels":{"route":"/a"}},
		"resource":{"type":"global"},
		"points":[{"interval":{"endTime":"2026-07-25T10:15:00Z"},"value":{"doubleValue":4}}]
	}]`)
	writePromQLFixture(t, api, "other", `[{
		"metric":{"type":"custom.googleapis.com/dedupe","labels":{"route":"/a"}},
		"resource":{"type":"global"},
		"points":[{"interval":{"endTime":"2026-07-25T10:29:00Z"},"value":{"doubleValue":77}}]
	}]`)

	response := monitoringRequest(api, http.MethodGet,
		"/v1/projects/dedupe/location/global/prometheus/api/v1/query?query="+
			url.QueryEscape(`{__name__="custom.googleapis.com/dedupe"}`)+"&time=2026-07-25T10:30:00Z", "")
	assertPromQLVector(t, response, 1784975400, "custom.googleapis.com/dedupe", []promQLWant{
		{route: "/a", value: "4"},
		{route: "/b", value: "3"},
	})
}

func TestPromQLInstantQueryErrorsAndBoundaries(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		method string
		path   string
		body   string
		code   int
		kind   string
	}{
		{name: "missing query", method: http.MethodGet, path: "/v1/projects/test/location/global/prometheus/api/v1/query", code: 400, kind: "bad_data"},
		{name: "unsupported expression", method: http.MethodGet, path: "/v1/projects/test/location/global/prometheus/api/v1/query?query=up", code: 422, kind: "execution"},
		{name: "label matcher", method: http.MethodGet, path: "/v1/projects/test/location/global/prometheus/api/v1/query?query=" + url.QueryEscape(`{__name__="custom.googleapis.com/x",route="/"}`), code: 422, kind: "execution"},
		{name: "invalid time", method: http.MethodGet, path: "/v1/projects/test/location/global/prometheus/api/v1/query?query=" + url.QueryEscape(`{__name__="custom.googleapis.com/x"}`) + "&time=tomorrow", code: 400, kind: "bad_data"},
		{name: "non global", method: http.MethodGet, path: "/v1/projects/test/location/us-central1/prometheus/api/v1/query?query=" + url.QueryEscape(`{__name__="custom.googleapis.com/x"}`), code: 400, kind: "bad_data"},
		{name: "query range", method: http.MethodGet, path: "/v1/projects/test/location/global/prometheus/api/v1/query_range?query=" + url.QueryEscape(`{__name__="custom.googleapis.com/x"}`), code: 422, kind: "execution"},
		{name: "too long", method: http.MethodPost, path: "/v1/projects/test/location/global/prometheus/api/v1/query", body: url.Values{"query": {strings.Repeat("x", 4097)}}.Encode(), code: 400, kind: "bad_data"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var response *httptest.ResponseRecorder
			if test.method == http.MethodPost {
				response = monitoringFormRequest(api, test.path, test.body)
			} else {
				response = monitoringRequest(api, test.method, test.path, test.body)
			}
			assertPromQLError(t, response, test.code, test.kind)
		})
	}
}

func TestPromQLInstantQueryRequiresJSONDoubleQuotedMetricName(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "backtick raw string", query: "{__name__=`custom.googleapis.com/raw`}"},
		{name: "single quoted string", query: "{__name__='a'}"},
		{name: "Go hexadecimal escape", query: `{__name__="custom.googleapis.com\x2fgo"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := monitoringRequest(api, http.MethodGet,
				"/v1/projects/test/location/global/prometheus/api/v1/query?query="+url.QueryEscape(test.query), "")
			assertPromQLError(t, response, http.StatusBadRequest, "bad_data")
		})
	}

	escapedJSON := monitoringRequest(api, http.MethodGet,
		"/v1/projects/test/location/global/prometheus/api/v1/query?query="+
			url.QueryEscape(`{__name__="custom.googleapis.com\u002fjson"}`), "")
	if escapedJSON.Code != http.StatusOK {
		t.Fatalf("JSON escaped metric status=%d body=%s", escapedJSON.Code, escapedJSON.Body.String())
	}
}

func TestPromQLInstantQueryPersistsAcrossRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "promql-restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	writePromQLFixture(t, api, "persisted", `[{
		"metric":{"type":"custom.googleapis.com/restart_value","labels":{"instance":"one"}},
		"resource":{"type":"global"},
		"points":[{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"doubleValue":42.125}}]
	}]`)
	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }
	response := monitoringRequest(restarted, http.MethodGet,
		"/v1/projects/persisted/location/global/prometheus/api/v1/query?query="+
			url.QueryEscape(`{__name__="custom.googleapis.com/restart_value"}`), "")
	assertPromQLVector(t, response, 1_800_000_000, "custom.googleapis.com/restart_value", []promQLWant{{value: "42.125"}})
}

func TestPromQLInstantQueryCanonicalGatewayPath(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.now = func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }
	writePromQLFixture(t, api, "gateway", `[{
		"metric":{"type":"custom.googleapis.com/gateway"},
		"resource":{"type":"global"},
		"points":[{"interval":{"endTime":"2026-07-25T10:00:00Z"},"value":{"int64Value":"3"}}]
	}]`)
	proxy := router.NewProxyRouterWithManager(nil)
	proxy.RegisterShim("monitoring.googleapis.com", api)
	response := monitoringRequest(proxy, http.MethodGet,
		"http://localhost/_minisky/monitoring/v1/projects/gateway/location/global/prometheus/api/v1/query?query="+
			url.QueryEscape(`{__name__="custom.googleapis.com/gateway"}`), "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"3"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestMonitoringSaveFailureRollsBackAndRetryPublishesOnce(t *testing.T) {
	store := &toggleMonitoringStore{fail: true}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"type":"custom.googleapis.com/retry","metricKind":"GAUGE","valueType":"DOUBLE"}`
	failed := monitoringRequest(api, http.MethodPost, "/v3/projects/test/metricDescriptors", body)
	if failed.Code != http.StatusInternalServerError {
		t.Fatalf("failed create status=%d body=%s", failed.Code, failed.Body.String())
	}
	store.fail = false
	retried := monitoringRequest(api, http.MethodPost, "/v3/projects/test/metricDescriptors", body)
	if retried.Code != http.StatusOK {
		t.Fatalf("retried create status=%d body=%s", retried.Code, retried.Body.String())
	}
}

func TestCorruptStateDisablesMonitoringRuntime(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-monitoring")
	store, err := state.New(root, "corrupt-monitoring")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(monitoringStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(nil)
	response := monitoringRequest(api, http.MethodPost, "/v3/projects/test/metricDescriptors",
		`{"type":"custom.googleapis.com/blocked","metricKind":"GAUGE","valueType":"DOUBLE"}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(monitoringStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

type toggleMonitoringStore struct{ fail bool }

func (*toggleMonitoringStore) Load(string, any) error { return state.ErrNotFound }
func (store *toggleMonitoringStore) Save(string, any) error {
	if store.fail {
		return errors.New("disk full")
	}
	return nil
}

func TestMonitoringRejectsMalformedAndMissingFields(t *testing.T) {
	api, err := NewAPIWithStore(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	malformed := monitoringRequest(api, http.MethodPost, "/v3/projects/test/metricDescriptors", `{`)
	assertMonitoringError(t, malformed, http.StatusBadRequest, "INVALID_ARGUMENT")
	missing := monitoringRequest(api, http.MethodPost, "/v3/projects/test/metricDescriptors", `{}`)
	assertMonitoringError(t, missing, http.StatusBadRequest, "INVALID_ARGUMENT")
	emptySeries := monitoringRequest(api, http.MethodPost, "/v3/projects/test/timeSeries", `{"timeSeries":[]}`)
	assertMonitoringError(t, emptySeries, http.StatusBadRequest, "INVALID_ARGUMENT")
}

func monitoringRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func monitoringFormRequest(handler http.Handler, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func writePromQLFixture(t *testing.T, handler http.Handler, project, seriesJSON string) {
	t.Helper()
	response := monitoringRequest(handler, http.MethodPost, "/v3/projects/"+project+"/timeSeries",
		`{"timeSeries":`+seriesJSON+`}`)
	if response.Code != http.StatusOK {
		t.Fatalf("write fixture status=%d body=%s", response.Code, response.Body.String())
	}
}

type promQLWant struct {
	route string
	value string
}

func assertPromQLVector(t *testing.T, response *httptest.ResponseRecorder, timestamp float64, metricType string, want []promQLWant) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, response.Body.String())
	}
	if envelope.Status != "success" || envelope.Data.ResultType != "vector" || len(envelope.Data.Result) != len(want) {
		t.Fatalf("response=%+v", envelope)
	}
	for i, expected := range want {
		result := envelope.Data.Result[i]
		if result.Metric["__name__"] != metricType {
			t.Fatalf("result[%d] metric=%v", i, result.Metric)
		}
		if expected.route != "" && result.Metric["route"] != expected.route {
			t.Fatalf("result[%d] route=%q want=%q", i, result.Metric["route"], expected.route)
		}
		if len(result.Value) != 2 {
			t.Fatalf("result[%d] value=%s", i, result.Value)
		}
		var gotTimestamp float64
		var gotValue string
		if err := json.Unmarshal(result.Value[0], &gotTimestamp); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(result.Value[1], &gotValue); err != nil {
			t.Fatal(err)
		}
		if gotTimestamp != timestamp || gotValue != expected.value {
			t.Fatalf("result[%d] value=[%v %q], want=[%v %q]", i, gotTimestamp, gotValue, timestamp, expected.value)
		}
	}
}

func assertPromQLError(t *testing.T, response *httptest.ResponseRecorder, code int, kind string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status=%d want=%d body=%s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, response.Body.String())
	}
	if envelope.Status != "error" || envelope.ErrorType != kind || envelope.Error == "" {
		t.Fatalf("error envelope=%+v", envelope)
	}
}

func assertMonitoringError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error: %v; body = %s", err, response.Body.String())
	}
	if envelope.Error.Code != code || envelope.Error.Status != status {
		t.Fatalf("error = %+v", envelope.Error)
	}
}
