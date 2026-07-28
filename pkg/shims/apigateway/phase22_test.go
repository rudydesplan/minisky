package apigateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestApiConfigHierarchyAndLoopbackProxy(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Path", r.URL.Path)
		w.Header().Set("X-Backend-Host", r.Host)
		_, _ = w.Write([]byte("proxied"))
	}))
	defer backend.Close()

	api := newTestAPI()
	apiName := "projects/p1/locations/global/apis/api1"
	api.apis[apiName] = &Api{Name: apiName}
	document := `{"swagger":"2.0","x-google-backend":{"address":"` + backend.URL + `"}}`
	body := apiConfigBody(document)
	rec := httptest.NewRecorder()
	path := "/v1/" + apiName + "/configs?apiConfigId=cfg1"
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create config status=%d: %s", rec.Code, rec.Body.String())
	}

	gatewayName := "projects/p1/locations/us-central1/gateways/gw1"
	api.gateways[gatewayName] = &Gateway{Name: gatewayName, ApiConfig: apiName + "/configs/cfg1"}
	handler, err := api.GatewayProxy(gatewayName)
	if err != nil {
		t.Fatalf("GatewayProxy: %v", err)
	}
	proxyRec := httptest.NewRecorder()
	proxyReq := httptest.NewRequest(http.MethodGet, "/hello?x=1", nil)
	proxyReq.Host = "metadata.google.internal"
	handler.ServeHTTP(proxyRec, proxyReq)
	if proxyRec.Code != http.StatusOK || proxyRec.Body.String() != "proxied" {
		t.Fatalf("proxy status=%d body=%q", proxyRec.Code, proxyRec.Body.String())
	}
	if proxyRec.Header().Get("X-Backend-Path") != "/hello" {
		t.Fatalf("backend path=%q", proxyRec.Header().Get("X-Backend-Path"))
	}
	if got, want := proxyRec.Header().Get("X-Backend-Host"), strings.TrimPrefix(backend.URL, "http://"); got != want {
		t.Fatalf("backend host=%q, want pinned loopback host %q", got, want)
	}

	deleteGateway := httptest.NewRecorder()
	api.ServeHTTP(deleteGateway, httptest.NewRequest(http.MethodDelete, "/v1/"+gatewayName, nil))
	if deleteGateway.Code != http.StatusOK {
		t.Fatalf("delete gateway status=%d: %s", deleteGateway.Code, deleteGateway.Body.String())
	}
	deleteConfig := httptest.NewRecorder()
	api.ServeHTTP(deleteConfig, httptest.NewRequest(http.MethodDelete, "/v1/"+apiName+"/configs/cfg1", nil))
	if deleteConfig.Code != http.StatusOK {
		t.Fatalf("delete config status=%d: %s", deleteConfig.Code, deleteConfig.Body.String())
	}
	stale := httptest.NewRecorder()
	handler.ServeHTTP(stale, httptest.NewRequest(http.MethodGet, "/after-cleanup", nil))
	if stale.Code != http.StatusServiceUnavailable {
		t.Fatalf("stale proxy status=%d, want 503", stale.Code)
	}
}

func TestApiConfigRejectsSSRFAndUnsupportedMode(t *testing.T) {
	api := newTestAPI()
	apiName := "projects/p1/locations/global/apis/api1"
	api.apis[apiName] = &Api{Name: apiName}
	tests := []struct {
		name string
		body string
		code int
	}{
		{"non-loopback", apiConfigBody(`{"swagger":"2.0","x-google-backend":{"address":"http://169.254.169.254/latest"}}`), http.StatusBadRequest},
		{"grpc", `{"grpcServices":[{"fileDescriptorSet":{"contents":"AA=="}}]}`, http.StatusNotImplemented},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
				"/v1/"+apiName+"/configs?apiConfigId="+test.name, bytes.NewBufferString(test.body)))
			if rec.Code != test.code {
				t.Fatalf("status=%d, want %d: %s", rec.Code, test.code, rec.Body.String())
			}
		})
	}
}

func TestApiDeleteRequiresNoConfigs(t *testing.T) {
	api := newTestAPI()
	apiName := "projects/p1/locations/global/apis/api1"
	configName := apiName + "/configs/cfg1"
	api.apis[apiName] = &Api{Name: apiName}
	api.configs = map[string]*ApiConfig{configName: {Name: configName, BackendURL: "http://127.0.0.1:8080"}}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/"+apiName, nil))
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status=%d, want 412: %s", rec.Code, rec.Body.String())
	}
}

func TestApiGatewayHierarchySurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api := newTestAPI()
	api.stateStore = store
	apiName := "projects/p1/locations/global/apis/api1"
	configName := apiName + "/configs/cfg1"
	gatewayName := "projects/p1/locations/us-central1/gateways/gw1"
	api.apis[apiName] = &Api{Name: apiName}
	api.configs[configName] = &ApiConfig{Name: configName, OpenAPIDocuments: []OpenAPIDocument{{
		Document: APIConfigFile{Contents: base64.StdEncoding.EncodeToString([]byte(`{"x-google-backend":{"address":"http://127.0.0.1:8080"}}`))},
	}}}
	api.gateways[gatewayName] = &Gateway{Name: gatewayName, ApiConfig: configName}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	restarted := newTestAPI()
	restarted.stateStore = store
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.configs[configName] == nil || restarted.gateways[gatewayName] == nil {
		t.Fatal("API config hierarchy was not restored")
	}
}

func TestApiConfigSaveFailureRollsBack(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingGatewayStore{}
	apiName := "projects/p1/locations/global/apis/api1"
	api.apis[apiName] = &Api{Name: apiName}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+apiName+"/configs?apiConfigId=cfg1",
		bytes.NewBufferString(apiConfigBody(`{"x-google-backend":{"address":"http://127.0.0.1:8080"}}`))))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if len(api.configs) != 0 {
		t.Fatal("failed config save remained visible")
	}
}

type failingGatewayStore struct{}

func (failingGatewayStore) Load(string, any) error { return state.ErrNotFound }
func (failingGatewayStore) Save(string, any) error { return errors.New("disk full") }

func apiConfigBody(document string) string {
	contents := base64.StdEncoding.EncodeToString([]byte(document))
	value, _ := json.Marshal(map[string]any{
		"openapiDocuments": []any{map[string]any{
			"document": map[string]any{"path": "openapi.json", "contents": contents},
		}},
	})
	return string(value)
}
