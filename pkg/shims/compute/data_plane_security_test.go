package compute

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestLoadBalancerCloudArmorPriorityAllowDenyAndDefault(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
		fmt.Fprint(w, "allowed")
	}))
	defer backend.Close()

	store := &toggleComputeStore{}
	api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	createSecurityPolicyForProxyTest(t, api, "project-a", "edge", `[
		{"priority":100,"action":"deny(403)","match":{"versionedExpr":"SRC_IPS_V1","config":{"srcIpRanges":["203.0.113.0/24"]}}},
		{"priority":200,"action":"allow","match":{"versionedExpr":"SRC_IPS_V1","config":{"srcIpRanges":["198.51.100.0/24"]}}},
		{"priority":2147483647,"action":"deny(404)"}
	]`)
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "edge", false)

	tests := []struct {
		name       string
		remoteAddr string
		wantStatus int
		wantBody   string
	}{
		{"higher priority deny", "203.0.113.9:1234", http.StatusForbidden, "denied"},
		{"explicit allow", "198.51.100.4:1234", http.StatusOK, "allowed"},
		{"default deny status", "192.0.2.5:1234", http.StatusNotFound, "denied"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset", tc.remoteAddr, nil)
			if response.Code != tc.wantStatus || !strings.Contains(response.Body.String(), tc.wantBody) {
				t.Fatalf("status=%d body=%q, want %d containing %q",
					response.Code, response.Body.String(), tc.wantStatus, tc.wantBody)
			}
		})
	}
	if backendHits.Load() != 1 {
		t.Fatalf("backend hits=%d, want only explicit allow", backendHits.Load())
	}
}

func TestSecurityPolicyRejectsUnsupportedArmorExpressions(t *testing.T) {
	api, _ := newComputeTestAPI()
	base := "/compute/v1/projects/project-a/global/securityPolicies"
	for _, body := range []string{
		`{"name":"cel","rules":[{"priority":100,"action":"allow","match":{"versionedExpr":"EXPRESSION","config":{"srcIpRanges":["0.0.0.0/0"]}}}]}`,
		`{"name":"redirect","rules":[{"priority":100,"action":"redirect"}]}`,
		`{"name":"cidr","rules":[{"priority":100,"action":"allow","match":{"versionedExpr":"SRC_IPS_V1","config":{"srcIpRanges":["not-a-cidr"]}}}]}`,
	} {
		assertComputeError(t, performComputeRequest(api, http.MethodPost, base, body),
			http.StatusBadRequest, "INVALID_ARGUMENT")
	}
	if len(api.securityPolicies) != 0 {
		t.Fatalf("unsupported policies mutated state: %#v", api.securityPolicies)
	}
}

func TestLoadBalancerCloudArmorProjectIsolationAndPolicyUpdateRestart(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits.Add(1)
		fmt.Fprint(w, "backend")
	}))
	defer backend.Close()

	store := &toggleComputeStore{}
	api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{"project-a", "project-b"} {
		createSecurityPolicyForProxyTest(t, api, project, "edge", `[
			{"priority":2147483647,"action":"allow"}
		]`)
		createProtectedLoadBalancerForProxyTest(t, api, project, backend.URL, "edge", false)
	}
	updateSecurityPolicyForProxyTest(t, api, "project-a", "edge", `[
		{"priority":2147483647,"action":"deny(403)"}
	]`)

	if response := proxyRequestForTest(api, "project-a", http.MethodGet, "/", "192.0.2.1:1", nil); response.Code != http.StatusForbidden {
		t.Fatalf("project-a status=%d body=%s", response.Code, response.Body.String())
	}
	if response := proxyRequestForTest(api, "project-b", http.MethodGet, "/", "192.0.2.1:1", nil); response.Code != http.StatusOK {
		t.Fatalf("project-b status=%d body=%s", response.Code, response.Body.String())
	}

	restarted, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if response := proxyRequestForTest(restarted, "project-a", http.MethodGet, "/", "192.0.2.1:1", nil); response.Code != http.StatusForbidden {
		t.Fatalf("restart served stale policy: status=%d body=%s", response.Code, response.Body.String())
	}
	if backendHits.Load() != 1 {
		t.Fatalf("backend hits=%d, denied requests reached backend", backendHits.Load())
	}
}

func TestSecurityPolicyUpdateSaveFailureKeepsEnforcedPolicy(t *testing.T) {
	store := &toggleComputeStore{}
	api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()
	createSecurityPolicyForProxyTest(t, api, "project-a", "edge",
		`[{"priority":2147483647,"action":"allow"}]`)
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "edge", false)
	store.setFail(true)
	response := performComputeRequest(api, http.MethodPatch,
		"/compute/v1/projects/project-a/global/securityPolicies/edge",
		`{"rules":[{"priority":2147483647,"action":"deny(403)"}]}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if proxied := proxyRequestForTest(api, "project-a", http.MethodGet, "/", "192.0.2.1:1", nil); proxied.Code != http.StatusOK {
		t.Fatalf("failed update changed enforcement: status=%d body=%s", proxied.Code, proxied.Body.String())
	}
}

func TestLoadBalancerCDNCacheHitMissInvalidationAndPolicyUpdate(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit := backendHits.Add(1)
		switch r.URL.Path {
		case "/private":
			w.Header().Set("Cache-Control", "private, max-age=30")
		default:
			w.Header().Set("Cache-Control", "public, max-age=30")
		}
		fmt.Fprintf(w, "hit-%d:%s?%s", hit, r.URL.Path, r.URL.RawQuery)
	}))
	defer backend.Close()

	store := &toggleComputeStore{}
	api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	createSecurityPolicyForProxyTest(t, api, "project-a", "edge", `[
		{"priority":2147483647,"action":"allow"}
	]`)
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "edge", true)

	first := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset", "192.0.2.1:1", nil)
	second := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset", "192.0.2.1:1", nil)
	if first.Body.String() != second.Body.String() || backendHits.Load() != 1 {
		t.Fatalf("cache miss: first=%q second=%q hits=%d",
			first.Body.String(), second.Body.String(), backendHits.Load())
	}
	queryMiss := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset?v=2", "192.0.2.1:1", nil)
	if queryMiss.Code != http.StatusOK || backendHits.Load() != 2 {
		t.Fatalf("query miss status=%d hits=%d", queryMiss.Code, backendHits.Load())
	}

	invalidate := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/urlMaps/routes/invalidateCache",
		`{"path":"/asset*"}`)
	if invalidate.Code != http.StatusOK {
		t.Fatalf("invalidate status=%d body=%s", invalidate.Code, invalidate.Body.String())
	}
	afterInvalidation := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset", "192.0.2.1:1", nil)
	if afterInvalidation.Code != http.StatusOK || backendHits.Load() != 3 {
		t.Fatalf("invalidation status=%d hits=%d", afterInvalidation.Code, backendHits.Load())
	}

	updateSecurityPolicyForProxyTest(t, api, "project-a", "edge", `[
		{"priority":2147483647,"action":"deny(403)"}
	]`)
	denied := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset", "192.0.2.1:1", nil)
	if denied.Code != http.StatusForbidden || backendHits.Load() != 3 {
		t.Fatalf("policy update served cache: status=%d hits=%d", denied.Code, backendHits.Load())
	}
	updateSecurityPolicyForProxyTest(t, api, "project-a", "edge", `[
		{"priority":2147483647,"action":"allow"}
	]`)
	allowedAgain := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset", "192.0.2.1:1", nil)
	if allowedAgain.Code != http.StatusOK || backendHits.Load() != 4 {
		t.Fatalf("policy cache invalidation status=%d hits=%d", allowedAgain.Code, backendHits.Load())
	}
	restarted, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart := proxyRequestForTest(restarted, "project-a", http.MethodGet, "/asset", "192.0.2.1:1", nil)
	if afterRestart.Code != http.StatusOK || backendHits.Load() != 5 {
		t.Fatalf("restart reused transient cache: status=%d hits=%d", afterRestart.Code, backendHits.Load())
	}
}

func TestLoadBalancerCDNDoesNotCacheCredentialOrPrivateResponses(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendHits.Add(1)
		if r.URL.Path == "/private" {
			w.Header().Set("Cache-Control", "private, max-age=30")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=30")
		}
		fmt.Fprint(w, "response")
	}))
	defer backend.Close()

	api, _ := newComputeTestAPI()
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "", true)
	for range 2 {
		proxyRequestForTest(api, "project-a", http.MethodGet, "/private", "192.0.2.1:1", nil)
		proxyRequestForTest(api, "project-a", http.MethodGet, "/credential", "192.0.2.1:1",
			map[string]string{"Authorization": "Bearer secret"})
	}
	if backendHits.Load() != 4 {
		t.Fatalf("sensitive responses were cached: hits=%d", backendHits.Load())
	}
}

func TestLoadBalancerCDNBoundsAndHEADCaching(t *testing.T) {
	var backendHits atomic.Int32
	oversized := strings.Repeat("x", maxLoadBalancerCacheBytes+1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendHits.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		if r.URL.Path == "/large" {
			fmt.Fprint(w, oversized)
			return
		}
		fmt.Fprint(w, "head")
	}))
	defer backend.Close()
	api, _ := newComputeTestAPI()
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "", true)

	for range 2 {
		response := proxyRequestForTest(api, "project-a", http.MethodHead, "/head", "192.0.2.1:1", nil)
		if response.Code != http.StatusOK || response.Body.Len() != 0 {
			t.Fatalf("HEAD status=%d body=%d", response.Code, response.Body.Len())
		}
	}
	if backendHits.Load() != 1 {
		t.Fatalf("HEAD response was not cached: hits=%d", backendHits.Load())
	}
	for range 2 {
		response := proxyRequestForTest(api, "project-a", http.MethodGet, "/large", "192.0.2.1:1", nil)
		if response.Code != http.StatusOK || response.Body.Len() != len(oversized) {
			t.Fatalf("large response status=%d size=%d", response.Code, response.Body.Len())
		}
	}
	if backendHits.Load() != 3 {
		t.Fatalf("oversized response was cached: hits=%d", backendHits.Load())
	}
	if ttl, ok := boundedCacheTTL("public, max-age=3600"); !ok || ttl != maxLoadBalancerCacheTTL {
		t.Fatalf("bounded TTL=(%v,%t)", ttl, ok)
	}
}

func TestLoadBalancerCloudArmorConcurrentPolicyUpdatesAndRequests(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()
	api, _ := newComputeTestAPI()
	createSecurityPolicyForProxyTest(t, api, "project-a", "edge", `[
		{"priority":2147483647,"action":"allow"}
	]`)
	createProtectedLoadBalancerForProxyTest(t, api, "project-a", backend.URL, "edge", true)

	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				response := proxyRequestForTest(api, "project-a", http.MethodGet, "/asset", "192.0.2.1:1", nil)
				if response.Code != http.StatusOK && response.Code != http.StatusForbidden {
					t.Errorf("unexpected proxy status=%d body=%s", response.Code, response.Body.String())
				}
			}
		}()
	}
	for update := 0; update < 10; update++ {
		action := "allow"
		if update%2 == 1 {
			action = "deny(403)"
		}
		updateSecurityPolicyForProxyTest(t, api, "project-a", "edge",
			fmt.Sprintf(`[{"priority":2147483647,"action":%q}]`, action))
	}
	wg.Wait()
}

func createSecurityPolicyForProxyTest(t *testing.T, api *API, project, name, rules string) {
	t.Helper()
	response := performComputeRequest(api, http.MethodPost,
		fmt.Sprintf("/compute/v1/projects/%s/global/securityPolicies", project),
		fmt.Sprintf(`{"name":%q,"rules":%s}`, name, rules))
	if response.Code != http.StatusOK {
		t.Fatalf("create policy status=%d body=%s", response.Code, response.Body.String())
	}
}

func updateSecurityPolicyForProxyTest(t *testing.T, api *API, project, name, rules string) {
	t.Helper()
	response := performComputeRequest(api, http.MethodPatch,
		fmt.Sprintf("/compute/v1/projects/%s/global/securityPolicies/%s", project, name),
		fmt.Sprintf(`{"rules":%s}`, rules))
	if response.Code != http.StatusOK {
		t.Fatalf("update policy status=%d body=%s", response.Code, response.Body.String())
	}
}

func createProtectedLoadBalancerForProxyTest(
	t *testing.T,
	api *API,
	project string,
	backendURL string,
	policy string,
	enableCDN bool,
) {
	t.Helper()
	securityPolicy := ""
	if policy != "" {
		securityPolicy = fmt.Sprintf(`,"securityPolicy":%q`, policy)
	}
	create := func(collection, body string) {
		response := performComputeRequest(api, http.MethodPost,
			fmt.Sprintf("/compute/v1/projects/%s/global/%s", project, collection), body)
		if response.Code != http.StatusOK {
			t.Fatalf("create %s status=%d body=%s", collection, response.Code, response.Body.String())
		}
	}
	create("backendServices", fmt.Sprintf(
		`{"name":"backend","enableCDN":%t,"backends":[{"url":%q}]%s}`,
		enableCDN, backendURL, securityPolicy,
	))
	create("urlMaps", `{"name":"routes","defaultService":"backend"}`)
	create("targetHttpProxies", `{"name":"proxy","urlMap":"routes"}`)
	create("forwardingRules", `{"name":"frontend","target":"proxy"}`)
}

func proxyRequestForTest(
	api *API,
	project string,
	method string,
	path string,
	remoteAddr string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method,
		fmt.Sprintf("/compute/v1/projects/%s/global/forwardingRules/frontend/proxy%s", project, path), nil)
	request.RemoteAddr = remoteAddr
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}
