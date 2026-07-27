package cloudendpoints

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

func TestConfigRolloutCheckAndReport(t *testing.T) {
	api := newTestAPI()
	service := "example.endpoints.test"

	config := httptest.NewRecorder()
	api.ServeHTTP(config, httptest.NewRequest(http.MethodPost, "/v1/services/"+service+"/configs",
		bytes.NewBufferString(`{"name":"example.endpoints.test","id":"cfg-1","title":"Example"}`)))
	if config.Code != http.StatusOK {
		t.Fatalf("create config status=%d: %s", config.Code, config.Body.String())
	}
	var configOperation Operation
	if err := json.Unmarshal(config.Body.Bytes(), &configOperation); err != nil {
		t.Fatal(err)
	}
	created := configOperation.Response
	if created.ID != "cfg-1" || created.Name != service {
		t.Fatalf("config=%+v", created)
	}

	rollout := httptest.NewRecorder()
	api.ServeHTTP(rollout, httptest.NewRequest(http.MethodPost, "/v1/services/"+service+"/rollouts",
		bytes.NewBufferString(`{"rolloutId":"roll-1","serviceName":"example.endpoints.test","trafficPercentStrategy":{"percentages":{"cfg-1":100}}}`)))
	if rollout.Code != http.StatusOK {
		t.Fatalf("create rollout status=%d: %s", rollout.Code, rollout.Body.String())
	}

	check := httptest.NewRecorder()
	api.ServeHTTP(check, httptest.NewRequest(http.MethodPost, "/v1/services/"+service+":check",
		bytes.NewBufferString(`{"serviceName":"example.endpoints.test","operation":{"operationId":"op-1","operationName":"ListBooks","consumerId":"project:p1"}}`)))
	if check.Code != http.StatusOK {
		t.Fatalf("check status=%d: %s", check.Code, check.Body.String())
	}
	var checked CheckResponse
	if err := json.Unmarshal(check.Body.Bytes(), &checked); err != nil {
		t.Fatal(err)
	}
	if checked.ServiceConfigID != "cfg-1" || checked.ServiceRolloutID != "roll-1" || len(checked.CheckErrors) != 0 {
		t.Fatalf("check=%+v", checked)
	}

	report := httptest.NewRecorder()
	api.ServeHTTP(report, httptest.NewRequest(http.MethodPost, "/v1/services/"+service+":report",
		bytes.NewBufferString(`{"serviceName":"example.endpoints.test","operations":[{"operationId":"op-1","operationName":"ListBooks","consumerId":"project:p1"}]}`)))
	if report.Code != http.StatusOK {
		t.Fatalf("report status=%d: %s", report.Code, report.Body.String())
	}
	var reported ReportResponse
	if err := json.Unmarshal(report.Body.Bytes(), &reported); err != nil {
		t.Fatal(err)
	}
	if reported.ServiceConfigID != "cfg-1" || reported.ServiceRolloutID != "roll-1" || len(reported.ReportErrors) != 0 {
		t.Fatalf("report=%+v", reported)
	}
	if api.operations[service+":op-1"].ReportedAt == "" {
		t.Fatal("report was not correlated with checked operation")
	}
}

func TestRolloutRequiresOwnedConfigAndCheckRequiresActiveRollout(t *testing.T) {
	api := newTestAPI()
	service := "example.endpoints.test"

	check := httptest.NewRecorder()
	api.ServeHTTP(check, httptest.NewRequest(http.MethodPost, "/v1/services/"+service+":check",
		strings.NewReader(`{"serviceName":"example.endpoints.test","operation":{"operationId":"op-1"}}`)))
	if check.Code != http.StatusPreconditionFailed {
		t.Fatalf("check status=%d, want 412: %s", check.Code, check.Body.String())
	}

	rollout := httptest.NewRecorder()
	api.ServeHTTP(rollout, httptest.NewRequest(http.MethodPost, "/v1/services/"+service+"/rollouts",
		strings.NewReader(`{"rolloutId":"roll-1","serviceName":"example.endpoints.test","trafficPercentStrategy":{"percentages":{"missing":100}}}`)))
	if rollout.Code != http.StatusBadRequest {
		t.Fatalf("rollout status=%d, want 400: %s", rollout.Code, rollout.Body.String())
	}
}

func TestCloudEndpointsStateAndScopedPagination(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api := newTestAPI()
	api.stateStore = store
	for _, id := range []string{"a", "b"} {
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/services/one/configs",
			strings.NewReader(`{"name":"one","id":"`+id+`"}`)))
		if rec.Code != http.StatusOK {
			t.Fatalf("create %s: %s", id, rec.Body.String())
		}
	}
	first := httptest.NewRecorder()
	api.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/services/one/configs?pageSize=1", nil))
	var page struct {
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextPageToken == "" {
		t.Fatalf("page token err=%v body=%s", err, first.Body.String())
	}
	crossScope := httptest.NewRecorder()
	api.ServeHTTP(crossScope, httptest.NewRequest(http.MethodGet, "/v1/services/two/configs?pageSize=1&pageToken="+page.NextPageToken, nil))
	if crossScope.Code != http.StatusBadRequest {
		t.Fatalf("cross-scope status=%d: %s", crossScope.Code, crossScope.Body.String())
	}

	restarted := newTestAPI()
	restarted.stateStore = store
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.configs["one:a"] == nil || restarted.configs["one:b"] == nil {
		t.Fatal("configs did not survive restart")
	}
}

func TestConfigSaveFailureRollsBack(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingEndpointsStore{}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/services/example/configs",
		strings.NewReader(`{"name":"example","id":"cfg"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if len(api.configs) != 0 {
		t.Fatal("failed config save remained visible")
	}
}

func TestUnsupportedControlFeatureReturnsUnimplemented(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestAPI().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/services/example:allocateQuota", strings.NewReader(`{}`)))
	if rec.Code != http.StatusNotImplemented || !strings.Contains(rec.Body.String(), `"status":"UNIMPLEMENTED"`) {
		t.Fatalf("unexpected response: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

type failingEndpointsStore struct{}

func (failingEndpointsStore) Load(string, any) error { return state.ErrNotFound }
func (failingEndpointsStore) Save(string, any) error { return errors.New("disk full") }
