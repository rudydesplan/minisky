package compute

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestLoadBalancerResourceLifecycle(t *testing.T) {
	tests := []struct {
		collection      string
		canonical       string
		resourceKind    string
		listKind        string
		resourcePayload string
	}{
		{"backendServices", "backendServices", "compute#backendService", "compute#backendServiceList", `{"name":"test-resource","protocol":"HTTP","healthChecks":["hc-link"]}`},
		{"healthChecks", "healthChecks", "compute#healthCheck", "compute#healthCheckList", `{"name":"test-resource","type":"HTTP","httpHealthCheck":{"port":8080}}`},
		{"urlMaps", "urlMaps", "compute#urlMap", "compute#urlMapList", `{"name":"test-resource","defaultService":"backend-link"}`},
		{"targetHttpProxies", "targetHttpProxies", "compute#targetHttpProxy", "compute#targetHttpProxyList", `{"name":"test-resource","urlMap":"url-map-link"}`},
		{"forwardingRules", "forwardingRules", "compute#forwardingRule", "compute#forwardingRuleList", `{"name":"test-resource","IPAddress":"203.0.113.10","target":"proxy-link","portRange":"80-80"}`},
		{"globalForwardingRules", "forwardingRules", "compute#forwardingRule", "compute#forwardingRuleList", `{"name":"test-resource","IPAddress":"203.0.113.11","target":"proxy-link"}`},
	}

	for _, tc := range tests {
		t.Run(tc.collection, func(t *testing.T) {
			api, opMgr := newComputeTestAPI()
			collectionPath := fmt.Sprintf("/compute/v1/projects/test-project/global/%s", tc.collection)
			canonicalSelfLink := fmt.Sprintf(
				"https://www.googleapis.com/compute/v1/projects/test-project/global/%s/test-resource",
				tc.canonical,
			)

			create := performComputeRequest(api, http.MethodPost, collectionPath, tc.resourcePayload)
			if create.Code != http.StatusOK {
				t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
			}
			var operation orchestrator.Operation
			decodeComputeResponse(t, create, &operation)
			if operation.OperationType != "insert" || operation.TargetLink != canonicalSelfLink {
				t.Fatalf("unexpected create operation: %+v", operation)
			}
			if opMgr.Get(operation.Name) == nil {
				t.Fatalf("operation %q was not registered", operation.Name)
			}

			get := performComputeRequest(api, http.MethodGet, collectionPath+"/test-resource", "")
			if get.Code != http.StatusOK {
				t.Fatalf("get status = %d, body = %s", get.Code, get.Body.String())
			}
			var resource map[string]interface{}
			decodeComputeResponse(t, get, &resource)
			if resource["kind"] != tc.resourceKind || resource["selfLink"] != canonicalSelfLink {
				t.Fatalf("unexpected resource identity: %#v", resource)
			}
			if resource["status"] != metadataOnlyStatus ||
				!strings.Contains(resource["description"].(string), "explicit supported backend configuration") {
				t.Fatalf("resource did not disclose metadata-only behavior: %#v", resource)
			}
			if resource["id"] == "" || resource["creationTimestamp"] == "" {
				t.Fatalf("resource omitted stable metadata: %#v", resource)
			}
			firstID := resource["id"]

			getAgain := performComputeRequest(api, http.MethodGet, collectionPath+"/test-resource", "")
			var resourceAgain map[string]interface{}
			decodeComputeResponse(t, getAgain, &resourceAgain)
			if resourceAgain["id"] != firstID || resourceAgain["selfLink"] != resource["selfLink"] {
				t.Fatalf("resource identity changed between reads: first=%#v second=%#v", resource, resourceAgain)
			}

			list := performComputeRequest(api, http.MethodGet, collectionPath, "")
			if list.Code != http.StatusOK {
				t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
			}
			var resources struct {
				Kind  string                   `json:"kind"`
				Items []map[string]interface{} `json:"items"`
			}
			decodeComputeResponse(t, list, &resources)
			if resources.Kind != tc.listKind || len(resources.Items) != 1 {
				t.Fatalf("unexpected list response: %#v", resources)
			}

			duplicate := performComputeRequest(api, http.MethodPost, collectionPath, tc.resourcePayload)
			assertComputeError(t, duplicate, http.StatusConflict, "ALREADY_EXISTS")

			remove := performComputeRequest(api, http.MethodDelete, collectionPath+"/test-resource", "")
			if remove.Code != http.StatusOK {
				t.Fatalf("delete status = %d, body = %s", remove.Code, remove.Body.String())
			}
			var deleteOperation orchestrator.Operation
			decodeComputeResponse(t, remove, &deleteOperation)
			if deleteOperation.OperationType != "delete" || deleteOperation.TargetLink != canonicalSelfLink {
				t.Fatalf("unexpected delete operation: %+v", deleteOperation)
			}

			assertComputeError(
				t,
				performComputeRequest(api, http.MethodGet, collectionPath+"/test-resource", ""),
				http.StatusNotFound,
				"NOT_FOUND",
			)
			assertComputeError(
				t,
				performComputeRequest(api, http.MethodDelete, collectionPath+"/test-resource", ""),
				http.StatusNotFound,
				"NOT_FOUND",
			)
		})
	}
}

func TestLoadBalancerResourceUnsupportedRoutes(t *testing.T) {
	api, _ := newComputeTestAPI()
	base := "/compute/v1/projects/test-project/global/backendServices"

	assertComputeError(t, performComputeRequest(api, http.MethodPatch, base+"/missing", `{}`), http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	assertComputeError(t, performComputeRequest(api, http.MethodPut, base+"/missing", `{}`), http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base+"/existing/setSecurityPolicy", `{}`), http.StatusNotFound, "NOT_FOUND")
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base+"/missing/unsupported", ""), http.StatusNotFound, "NOT_FOUND")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base, `{}`), http.StatusBadRequest, "INVALID_ARGUMENT")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base, `{invalid`), http.StatusBadRequest, "INVALID_ARGUMENT")
}

func TestLoadBalancerMetadataConcurrentAccess(t *testing.T) {
	api, _ := newComputeTestAPI()
	base := "/compute/v1/projects/test-project/global/urlMaps"
	create := performComputeRequest(api, http.MethodPost, base, `{"name":"concurrent","defaultService":"backend-link"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if got := performComputeRequest(api, http.MethodGet, base+"/concurrent", ""); got.Code != http.StatusOK {
					t.Errorf("concurrent get status = %d", got.Code)
				}
				if got := performComputeRequest(api, http.MethodGet, base, ""); got.Code != http.StatusOK {
					t.Errorf("concurrent list status = %d", got.Code)
				}
			}
		}()
	}
	wg.Wait()
}

func TestLoadBalancerDataPlaneHealthAwareRoundRobin(t *testing.T) {
	var mu sync.Mutex
	hits := make([]string, 0, 4)
	newBackend := func(name string, healthy bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				if !healthy {
					http.Error(w, "unhealthy", http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			mu.Lock()
			hits = append(hits, name)
			mu.Unlock()
			fmt.Fprintf(w, "%s:%s?%s", name, r.URL.Path, r.URL.RawQuery)
		}))
	}

	first := newBackend("first", true)
	defer first.Close()
	second := newBackend("second", true)
	defer second.Close()
	unhealthy := newBackend("unhealthy", false)
	defer unhealthy.Close()

	api, _ := newComputeTestAPI()
	createLoadBalancerGraph(t, api, []string{unhealthy.URL, first.URL, second.URL})
	proxyPath := "/compute/v1/projects/test-project/global/forwardingRules/frontend/proxy/hello?x=1"

	for i, want := range []string{"first:/hello?x=1", "second:/hello?x=1", "first:/hello?x=1", "second:/hello?x=1"} {
		response := performComputeRequest(api, http.MethodGet, proxyPath, "")
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.Code != http.StatusOK || string(body) != want {
			t.Fatalf("request %d = status %d, body %q; want 200, %q", i, response.Code, body, want)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if strings.Join(hits, ",") != "first,second,first,second" {
		t.Fatalf("proxied requests = %v", hits)
	}
}

func TestLoadBalancerDataPlaneComputeInstanceEndpoint(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "compute:"+r.URL.Path)
	}))
	defer backend.Close()
	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	hostPort := backendURL.Port()

	api, _ := newComputeTestAPI()
	api.instances[instanceKey("test-project", "us-central1-a", "vm-1")] = &Instance{
		Name:    "vm-1",
		project: "test-project",
		zone:    "us-central1-a",
		Status:  "RUNNING",
		HostPorts: []orchestrator.PortMapping{{
			ContainerPort: "8080",
			HostPort:      hostPort,
		}},
	}
	createLoadBalancerResourceForTest(t, api, "backendServices", `{
		"name":"backend",
		"backends":[{
			"instance":"https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/instances/vm-1",
			"port":8080
		}]
	}`)
	createLoadBalancerControlPlane(t, api)

	response := performComputeRequest(
		api,
		http.MethodGet,
		"/compute/v1/projects/test-project/global/forwardingRules/frontend/proxy/from-vm",
		"",
	)
	if response.Code != http.StatusOK || response.Body.String() != "compute:/from-vm" {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
}

func TestLoadBalancerDataPlaneUnresolvedReturns503(t *testing.T) {
	tests := []struct {
		name        string
		backendBody string
		wantMessage string
	}{
		{
			name:        "standard instance group is explicitly unsupported",
			backendBody: `{"name":"backend","backends":[{"group":"projects/test-project/zones/us-central1-a/instanceGroups/group-1"}]}`,
			wantMessage: "unsupported backend",
		},
		{
			name:        "missing backends is unresolved",
			backendBody: `{"name":"backend"}`,
			wantMessage: "has no backends",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api, _ := newComputeTestAPI()
			createLoadBalancerResourceForTest(t, api, "backendServices", tc.backendBody)
			createLoadBalancerControlPlane(t, api)

			response := performComputeRequest(
				api,
				http.MethodGet,
				"/compute/v1/projects/test-project/global/forwardingRules/frontend/proxy",
				"",
			)
			body := response.Body.String()
			assertComputeError(t, response, http.StatusServiceUnavailable, "UNAVAILABLE")
			if !strings.Contains(body, tc.wantMessage) {
				t.Fatalf("error body %q does not contain %q", body, tc.wantMessage)
			}
		})
	}
}

func createLoadBalancerGraph(t *testing.T, api *API, backendURLs []string) {
	t.Helper()
	backends := make([]map[string]string, 0, len(backendURLs))
	for _, backendURL := range backendURLs {
		backends = append(backends, map[string]string{"url": backendURL})
	}
	payload, err := json.Marshal(map[string]interface{}{
		"name":         "backend",
		"backends":     backends,
		"healthChecks": []string{"health"},
	})
	if err != nil {
		t.Fatal(err)
	}
	createLoadBalancerResourceForTest(t, api, "healthChecks", `{"name":"health","httpHealthCheck":{"requestPath":"/healthz"}}`)
	createLoadBalancerResourceForTest(t, api, "backendServices", string(payload))
	createLoadBalancerControlPlane(t, api)
}

func createLoadBalancerControlPlane(t *testing.T, api *API) {
	t.Helper()
	createLoadBalancerResourceForTest(t, api, "urlMaps", `{"name":"routes","defaultService":"backend"}`)
	createLoadBalancerResourceForTest(t, api, "targetHttpProxies", `{"name":"proxy","urlMap":"routes"}`)
	createLoadBalancerResourceForTest(t, api, "forwardingRules", `{"name":"frontend","target":"proxy"}`)
}

func createLoadBalancerResourceForTest(t *testing.T, api *API, collection, body string) {
	t.Helper()
	response := performComputeRequest(
		api,
		http.MethodPost,
		"/compute/v1/projects/test-project/global/"+collection,
		body,
	)
	if response.Code != http.StatusOK {
		t.Fatalf("create %s status = %d, body = %s", collection, response.Code, response.Body.String())
	}
}

func newComputeTestAPI() (*API, *orchestrator.OperationManager) {
	opMgr := orchestrator.NewOperationManager()
	return newAPI(opMgr, nil, nil), opMgr
}

func performComputeRequest(api *API, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, request)
	return recorder
}

func decodeComputeResponse(t *testing.T, response *httptest.ResponseRecorder, target interface{}) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

func assertComputeError(t *testing.T, response *httptest.ResponseRecorder, wantCode int, wantStatus string) {
	t.Helper()
	if response.Code != wantCode {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, wantCode, response.Body.String())
	}
	var payload struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	decodeComputeResponse(t, response, &payload)
	if payload.Error.Status != wantStatus {
		t.Fatalf("error status = %q, want %q", payload.Error.Status, wantStatus)
	}
}
