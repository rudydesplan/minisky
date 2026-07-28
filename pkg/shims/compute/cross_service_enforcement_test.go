package compute

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/networksecurity"
	"minisky/pkg/shims/servicemesh"
)

var (
	_ networkHTTPAuthorizer = (*networksecurity.API)(nil)
	_ serviceMeshHTTPRouter = (*servicemesh.API)(nil)
)

type fakeNetworkAuthorizer struct {
	authorize func(project, location, sourceIP, host, method, path string) (bool, bool, string, error)
}

func (fake fakeNetworkAuthorizer) AuthorizeHTTP(
	project, location, sourceIP, host, method, path string,
) (bool, bool, string, error) {
	return fake.authorize(project, location, sourceIP, host, method, path)
}

type fakeServiceMeshRouter struct {
	route func(project, location, host, path string) (bool, string, string, error)
}

func (fake fakeServiceMeshRouter) RouteHTTP(
	project, location, host, path string,
) (bool, string, string, error) {
	return fake.route(project, location, host, path)
}

func TestComputePostBootWiresCrossServiceProviders(t *testing.T) {
	t.Setenv(registry.ExperimentalServicesEnv, "1")
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	api, ok := handlers["compute.googleapis.com"].(*API)
	if !ok {
		t.Fatalf("compute handler type=%T", handlers["compute.googleapis.com"])
	}
	if api.networkAuthorizer == nil || api.serviceMeshRouter == nil {
		t.Fatalf("post-boot providers authorizer=%T router=%T", api.networkAuthorizer, api.serviceMeshRouter)
	}
}

func TestComputeProxyNetworkAuthorizationDenyPreventsBackendSideEffects(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
		fmt.Fprint(w, "backend")
	}))
	defer backend.Close()
	api, _ := newComputeTestAPI()
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "", false)
	api.networkAuthorizer = fakeNetworkAuthorizer{authorize: func(
		project, location, sourceIP, host, method, path string,
	) (bool, bool, string, error) {
		if project != "project-a" || location != "global" || sourceIP != "192.0.2.10" ||
			method != http.MethodGet || path != "/admin" {
			t.Errorf("authorization input=%q %q %q %q %q %q",
				project, location, sourceIP, host, method, path)
		}
		return true, false, "projects/project-a/locations/global/authorizationPolicies/deny", nil
	}}

	denied := proxyRequestForTest(api, "project-a", http.MethodGet, "/admin", "192.0.2.10:1234", nil)
	if denied.Code != http.StatusForbidden || backendHits.Load() != 0 {
		t.Fatalf("denied status=%d hits=%d body=%s", denied.Code, backendHits.Load(), denied.Body.String())
	}
}

func TestComputeProxyNetworkAuthorizationNoPolicyAndProjectIsolation(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()
	api, _ := newComputeTestAPI()
	for _, project := range []string{"project-a", "project-b"} {
		createProtectedLoadBalancerForProxyTest(t, api, project, backend.URL, "", false)
	}
	api.networkAuthorizer = fakeNetworkAuthorizer{authorize: func(
		project, _, _, _, _, _ string,
	) (bool, bool, string, error) {
		if project == "project-a" {
			return true, false, "deny-a", nil
		}
		return false, true, "", nil
	}}

	if got := proxyRequestForTest(api, "project-a", http.MethodGet, "/", "192.0.2.1:1", nil); got.Code != http.StatusForbidden {
		t.Fatalf("project-a status=%d", got.Code)
	}
	if got := proxyRequestForTest(api, "project-b", http.MethodGet, "/", "192.0.2.1:1", nil); got.Code != http.StatusOK {
		t.Fatalf("project-b status=%d body=%s", got.Code, got.Body.String())
	}
	if backendHits.Load() != 1 {
		t.Fatalf("backend hits=%d", backendHits.Load())
	}
}

func TestComputeProxyServiceMeshHostPathSelectsExistingBackend(t *testing.T) {
	var defaultHits atomic.Int32
	defaultBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		defaultHits.Add(1)
		fmt.Fprint(w, "default")
	}))
	defer defaultBackend.Close()
	var routedHits atomic.Int32
	routedBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		routedHits.Add(1)
		fmt.Fprint(w, "routed")
	}))
	defer routedBackend.Close()

	api, _ := newComputeTestAPI()
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", defaultBackend.URL, "", false)
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/backendServices",
		fmt.Sprintf(`{"name":"routed","backends":[{"url":%q}]}`, routedBackend.URL))
	if response.Code != http.StatusOK {
		t.Fatalf("routed backend create status=%d body=%s", response.Code, response.Body.String())
	}
	api.serviceMeshRouter = fakeServiceMeshRouter{route: func(
		project, location, host, path string,
	) (bool, string, string, error) {
		if project == "project-a" && location == "global" &&
			host == "api.example.test" && path == "/v1/item" {
			return true, "routed", "projects/project-a/locations/global/httpRoutes/api", nil
		}
		return false, "", "", nil
	}}

	request := httptest.NewRequest(http.MethodGet,
		"/compute/v1/projects/project-a/global/forwardingRules/frontend/proxy/v1/item", nil)
	request.Host = "api.example.test"
	request.RemoteAddr = "192.0.2.1:1"
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Body.String() != "routed" ||
		routedHits.Load() != 1 || defaultHits.Load() != 0 {
		t.Fatalf("status=%d body=%q routed=%d default=%d",
			recorder.Code, recorder.Body.String(), routedHits.Load(), defaultHits.Load())
	}
}

func TestComputeProxyServiceMeshAmbiguityAndUnknownDestinationFailClosed(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
	}))
	defer backend.Close()
	for _, tc := range []struct {
		name        string
		destination string
		err         error
	}{
		{name: "ambiguous", err: errors.New("multiple HTTP routes match")},
		{name: "unknown destination", destination: "missing"},
		{name: "cross-project destination", destination: "projects/other/global/backendServices/backend"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, _ := newComputeTestAPI()
			createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "", false)
			api.serviceMeshRouter = fakeServiceMeshRouter{route: func(
				_, _, _, _ string,
			) (bool, string, string, error) {
				return true, tc.destination, "route", tc.err
			}}
			response := proxyRequestForTest(api, "project-a", http.MethodGet, "/", "192.0.2.1:1", nil)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	if backendHits.Load() != 0 {
		t.Fatalf("failed mesh decisions reached backend: hits=%d", backendHits.Load())
	}
}

func TestComputeProxyCrossServiceDecisionsAreRaceSafe(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()
	api, _ := newComputeTestAPI()
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "", false)
	var deny atomic.Bool
	api.networkAuthorizer = fakeNetworkAuthorizer{authorize: func(
		_, _, _, _, _, _ string,
	) (bool, bool, string, error) {
		blocked := deny.Load()
		return true, !blocked, "policy", nil
	}}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 30 {
				response := proxyRequestForTest(api, "project-a", http.MethodGet, "/", "192.0.2.1:1", nil)
				if response.Code != http.StatusOK && response.Code != http.StatusForbidden {
					t.Errorf("status=%d body=%s", response.Code, response.Body.String())
				}
			}
		}()
	}
	for range 30 {
		deny.Store(!deny.Load())
	}
	wg.Wait()
}
