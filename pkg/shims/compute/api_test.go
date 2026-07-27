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
			if tc.collection == "urlMaps" {
				createLoadBalancerResourceForTest(t, api, "backendServices", `{"name":"backend-link"}`)
			}
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
			if resource["description"] != nil && resource["description"] != "" {
				t.Fatalf("resource changed the caller-managed description: %#v", resource)
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
	createLoadBalancerResourceForTest(t, api, "backendServices", `{"name":"backend"}`)

	assertComputeError(t, performComputeRequest(api, http.MethodPatch, base+"/missing", `{}`), http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	assertComputeError(t, performComputeRequest(api, http.MethodPut, base+"/missing", `{}`), http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base+"/existing/setSecurityPolicy", `{}`), http.StatusNotFound, "NOT_FOUND")
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base+"/missing/unsupported", ""), http.StatusNotFound, "NOT_FOUND")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base, `{}`), http.StatusBadRequest, "INVALID_ARGUMENT")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base, `{invalid`), http.StatusBadRequest, "INVALID_ARGUMENT")
	assertComputeError(
		t,
		performComputeRequest(api, http.MethodPost, "/compute/v1/projects/test-project/global/urlMaps",
			`{"name":"routes","defaultService":"backend","hostRules":[{"hosts":["*"],"pathMatcher":"missing"}]}`),
		http.StatusBadRequest,
		"INVALID_ARGUMENT",
	)
}

func TestURLMapCreateValidatesAllBackendReferencesAndUnreachableRules(t *testing.T) {
	api, _ := newComputeTestAPI()
	createLoadBalancerResourceForTest(t, api, "backendServices", `{"name":"backend"}`)
	for _, body := range []string{
		`{"name":"foreign","defaultService":"https://www.googleapis.com/compute/v1/projects/other/global/backendServices/backend"}`,
		`{"name":"wrong-collection","defaultService":"https://www.googleapis.com/compute/v1/projects/test-project/global/healthChecks/backend"}`,
		`{"name":"unreachable","defaultService":"backend","pathMatchers":[{"name":"unused","defaultService":"backend","pathRules":[{"paths":["bad"],"service":"backend"}]}]}`,
		`{"name":"missing","defaultService":"backend","pathMatchers":[{"name":"unused","defaultService":"backend","pathRules":[{"paths":["/ok/*"],"service":"absent"}]}]}`,
	} {
		response := performComputeRequest(api, http.MethodPost,
			"/compute/v1/projects/test-project/global/urlMaps", body)
		assertComputeError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
	}
}

func TestLoadBalancerURLMapRoutesByHostAndLongestPath(t *testing.T) {
	defaultBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "default:%s", r.URL.Path)
	}))
	defer defaultBackend.Close()
	apiBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "api:%s", r.URL.Path)
	}))
	defer apiBackend.Close()
	versionBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "version:%s", r.URL.Path)
	}))
	defer versionBackend.Close()

	api, _ := newComputeTestAPI()
	for name, target := range map[string]string{
		"default-backend": defaultBackend.URL,
		"api-backend":     apiBackend.URL,
		"version-backend": versionBackend.URL,
	} {
		createLoadBalancerResourceForTest(t, api, "backendServices",
			fmt.Sprintf(`{"name":%q,"backends":[{"url":%q}]}`, name, target))
	}
	createLoadBalancerResourceForTest(t, api, "urlMaps", `{
		"name":"routes",
		"defaultService":"default-backend",
		"hostRules":[{"hosts":["api.example.test"],"pathMatcher":"api-paths"}],
		"pathMatchers":[{
			"name":"api-paths",
			"defaultService":"api-backend",
			"pathRules":[
				{"paths":["/v1/*"],"service":"api-backend"},
				{"paths":["/v1/special/*"],"service":"version-backend"}
			]
		}]
	}`)
	createLoadBalancerResourceForTest(t, api, "targetHttpProxies", `{"name":"proxy","urlMap":"routes"}`)
	createLoadBalancerResourceForTest(t, api, "forwardingRules", `{"name":"frontend","target":"proxy"}`)

	tests := []struct {
		host string
		path string
		want string
	}{
		{host: "other.example.test", path: "/v1/special/item", want: "default:/v1/special/item"},
		{host: "api.example.test:8080", path: "/other", want: "api:/other"},
		{host: "api.example.test", path: "/v1/item", want: "api:/v1/item"},
		{host: "api.example.test", path: "/v1/special/item", want: "version:/v1/special/item"},
	}
	for _, test := range tests {
		request := httptest.NewRequest(http.MethodGet,
			"/compute/v1/projects/test-project/global/forwardingRules/frontend/proxy"+test.path, nil)
		request.Host = test.host
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != test.want {
			t.Fatalf("host=%q path=%q: status=%d body=%q, want %q",
				test.host, test.path, response.Code, response.Body.String(), test.want)
		}
	}
}

func TestLoadBalancerMetadataConcurrentAccess(t *testing.T) {
	api, _ := newComputeTestAPI()
	createLoadBalancerResourceForTest(t, api, "backendServices", `{"name":"backend-link"}`)
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

func TestUnmanagedInstanceGroupLifecycleAndBackendResolution(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "group:"+r.URL.Path)
	}))
	defer backend.Close()
	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}

	api, _ := newComputeTestAPI()
	api.instances[instanceKey("test-project", "us-central1-a", "vm-1")] = &Instance{
		Name:    "vm-1",
		project: "test-project",
		zone:    "us-central1-a",
		Status:  "RUNNING",
		HostPorts: []orchestrator.PortMapping{{
			ContainerPort: "80",
			HostPort:      backendURL.Port(),
		}},
	}
	base := "/compute/v1/projects/test-project/zones/us-central1-a/instanceGroups"

	create := performComputeRequest(api, http.MethodPost, base, `{
		"name":"web-group",
		"description":"Terraform unmanaged group",
		"namedPorts":[{"name":"initial","port":81}]
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	var createOperation map[string]interface{}
	decodeComputeResponse(t, create, &createOperation)
	if createOperation["zone"] != "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a" {
		t.Fatalf("zonal operation = %#v", createOperation)
	}

	add := performComputeRequest(api, http.MethodPost, base+"/web-group/addInstances", `{
		"instances":[{"instance":"https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/instances/vm-1"}]
	}`)
	if add.Code != http.StatusOK {
		t.Fatalf("addInstances status = %d, body = %s", add.Code, add.Body.String())
	}
	namedPorts := performComputeRequest(api, http.MethodPost, base+"/web-group/setNamedPorts", `{
		"namedPorts":[{"name":"http","port":80}]
	}`)
	if namedPorts.Code != http.StatusOK {
		t.Fatalf("setNamedPorts status = %d, body = %s", namedPorts.Code, namedPorts.Body.String())
	}

	get := performComputeRequest(api, http.MethodGet, base+"/web-group", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", get.Code, get.Body.String())
	}
	var group map[string]interface{}
	decodeComputeResponse(t, get, &group)
	if group["kind"] != "compute#instanceGroup" || group["network"] == "" ||
		len(group["instances"].([]interface{})) != 1 || len(group["namedPorts"].([]interface{})) != 1 {
		t.Fatalf("unexpected instance group: %#v", group)
	}

	createLoadBalancerResourceForTest(t, api, "backendServices", `{
		"name":"backend",
		"portName":"http",
		"protocol":"HTTP",
		"backends":[{"group":"https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/instanceGroups/web-group"}]
	}`)
	createLoadBalancerControlPlane(t, api)
	response := performComputeRequest(
		api,
		http.MethodGet,
		"/compute/v1/projects/test-project/global/forwardingRules/frontend/proxy/from-group",
		"",
	)
	if response.Code != http.StatusOK || response.Body.String() != "group:/from-group" {
		t.Fatalf("proxy status = %d, body = %q", response.Code, response.Body.String())
	}

	remove := performComputeRequest(api, http.MethodDelete, base+"/web-group", "")
	if remove.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", remove.Code, remove.Body.String())
	}
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base+"/web-group", ""), http.StatusNotFound, "NOT_FOUND")
}

func TestComputeGlobalOperationsExposePollingSelfLink(t *testing.T) {
	api, _ := newComputeTestAPI()
	create := performComputeRequest(
		api,
		http.MethodPost,
		"/compute/v1/projects/test-project/global/healthChecks",
		`{"name":"health","httpHealthCheck":{"port":80}}`,
	)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", create.Code, create.Body.String())
	}
	var operation map[string]interface{}
	decodeComputeResponse(t, create, &operation)
	name, _ := operation["name"].(string)
	wantSelfLink := "https://www.googleapis.com/compute/v1/projects/test-project/global/operations/" + name
	if operation["selfLink"] != wantSelfLink {
		t.Fatalf("initial operation selfLink = %v, want %s", operation["selfLink"], wantSelfLink)
	}

	poll := performComputeRequest(
		api,
		http.MethodGet,
		"/compute/v1/projects/test-project/global/operations/"+name,
		"",
	)
	if poll.Code != http.StatusOK {
		t.Fatalf("poll status = %d, body = %s", poll.Code, poll.Body.String())
	}
	var polled map[string]interface{}
	decodeComputeResponse(t, poll, &polled)
	if polled["selfLink"] != wantSelfLink {
		t.Fatalf("polled operation = %#v", polled)
	}
}

func TestComputeGlobalOperationPollingIsProjectKindAndScopeBound(t *testing.T) {
	manager := orchestrator.NewOperationManager()
	api := NewAPI(manager, nil)
	operations := []*orchestrator.Operation{
		manager.Register("compute#operation", "insert",
			"https://www.googleapis.com/compute/v1/projects/other/global/networks/n", "", ""),
		manager.Register("compute#operation", "insert",
			"https://www.googleapis.com/compute/v1/projects/test-project/zones/us/instances/vm", "us", ""),
		manager.Register("sql#operation", "CREATE",
			"https://sqladmin.googleapis.com/v1/projects/test-project/instances/db", "", ""),
	}
	for _, operation := range operations {
		response := performComputeRequest(api, http.MethodGet,
			"/compute/v1/projects/test-project/global/operations/"+operation.Name, "")
		assertComputeError(t, response, http.StatusNotFound, "NOT_FOUND")
	}
}

func TestCuratedComputeBackendImageSelection(t *testing.T) {
	tests := []struct {
		source    string
		wantImage string
		wantOK    bool
	}{
		{"projects/debian-cloud/global/images/debian-12-bookworm-v20260701", "nginx:1.27-alpine", true},
		{"https://www.googleapis.com/compute/v1/projects/debian-cloud/global/images/family/debian-12", "nginx:1.27-alpine", true},
		{"projects/debian-cloud/global/images/debian-11-bullseye", "", false},
		{"docker.io/library/redis:latest", "", false},
		{"projects/customer/global/images/arbitrary", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.source, func(t *testing.T) {
			image, command, ok := curatedComputeBackend(tc.source)
			if image != tc.wantImage || ok != tc.wantOK {
				t.Fatalf("curatedComputeBackend(%q) = %q, %v", tc.source, image, ok)
			}
			if ok && len(command) != 0 {
				t.Fatalf("curated nginx backend should use its default command, got %v", command)
			}
		})
	}
}

func TestLoadBalancerDataPlaneUnresolvedReturns503(t *testing.T) {
	tests := []struct {
		name        string
		backendBody string
		wantMessage string
	}{
		{
			name:        "missing instance group is unresolved",
			backendBody: `{"name":"backend","backends":[{"group":"projects/test-project/zones/us-central1-a/instanceGroups/missing"}]}`,
			wantMessage: "was not found",
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
	api := newAPI(opMgr, nil, nil)
	useFakeVPCIPAM(api)
	api.legacyVPC = &fakeLegacyVPCBackend{}
	return api, opMgr
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
			Code    int    `json:"code"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	decodeComputeResponse(t, response, &payload)
	if payload.Error.Code != wantCode || payload.Error.Status != wantStatus || payload.Error.Message == "" {
		t.Fatalf("error = %#v, want code=%d status=%q and non-empty message",
			payload.Error, wantCode, wantStatus)
	}
}

func TestAdvancedNetworkingUnrepresentableSurfacesReturn501(t *testing.T) {
	api, _ := newComputeTestAPI()
	for _, path := range []string{
		"/compute/v1/projects/test/regions/us-central1/routers",
		"/compute/v1/projects/test/regions/us-central1/serviceAttachments",
		"/compute/v1/projects/test/global/interconnects",
		"/compute/v1/projects/test/global/networks/default/addPeering",
		"/compute/v1/projects/test/zones/us-central1-a/instanceGroupManagers/managed",
		"/compute/v1/projects/test/regions/us-central1/instanceGroups/regional",
		"/compute/v1/projects/test/global/targetHttpsProxies/https",
		"/compute/v1/projects/test/global/targetTcpProxies/tcp",
	} {
		response := performComputeRequest(api, http.MethodPost, path, `{}`)
		assertComputeError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
	}
}
