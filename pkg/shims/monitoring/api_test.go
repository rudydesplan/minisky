package monitoring

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
