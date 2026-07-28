package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
)

func TestQuotaLimiterConcurrentResetAndGCPError(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var clockMu sync.Mutex
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	limiter, err := NewQuotaLimiter(QuotaConfig{
		Services: map[string]QuotaRule{
			"compute.googleapis.com": {Limit: 2, Window: time.Minute},
		},
	}, clock)
	if err != nil {
		t.Fatal(err)
	}

	proxy := NewProxyRouterWithManager(nil)
	var accepted atomic.Int64
	proxy.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		accepted.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	proxy.ConfigureQuota(limiter, nil)

	var wait sync.WaitGroup
	for i := 0; i < 20; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodGet, "https://compute.googleapis.com/compute/v1/projects/demo/zones/us/instances", nil)
			request.Host = "compute.googleapis.com"
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, request)
			if response.Code != http.StatusNoContent && response.Code != http.StatusTooManyRequests {
				t.Errorf("status = %d body=%s", response.Code, response.Body.String())
			}
			if response.Code == http.StatusTooManyRequests &&
				(!strings.Contains(response.Body.String(), "RESOURCE_EXHAUSTED") || response.Header().Get("Retry-After") == "") {
				t.Errorf("quota response missing GCP shape: headers=%v body=%s", response.Header(), response.Body.String())
			}
		}()
	}
	wait.Wait()
	if got := accepted.Load(); got != 2 {
		t.Fatalf("accepted = %d, want 2", got)
	}

	clockMu.Lock()
	now = now.Add(time.Minute)
	clockMu.Unlock()
	request := httptest.NewRequest(http.MethodGet, "https://compute.googleapis.com/compute/v1/projects/demo/zones/us/instances", nil)
	request.Host = "compute.googleapis.com"
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status after reset = %d body=%s", response.Code, response.Body.String())
	}
}

func TestQuotaDefaultsDisabledAndJSONScopes(t *testing.T) {
	limiter, err := ParseQuotaConfigJSON(`{
		"projects":{"demo":{"limit":1,"window":"1s"}},
		"routes":{"compute.googleapis.com /compute/v1/projects/{id}/zones/{id}/instances":{"limit":2,"window":"1m"}}
	}`, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseQuotaConfigJSON(`{"projects":{}} {}`, time.Now); err == nil {
		t.Fatal("expected trailing JSON rejection")
	}
	if decision := limiter.Allow("compute.googleapis.com", "/compute/v1/projects/demo/zones/us/instances", "demo"); !decision.Allowed {
		t.Fatalf("first request denied: %#v", decision)
	}
	if decision := limiter.Allow("compute.googleapis.com", "/compute/v1/projects/demo/zones/us/instances", "demo"); decision.Allowed || decision.Scope != "project" {
		t.Fatalf("second request = %#v, want project denial", decision)
	}

	proxy := NewProxyRouterWithManager(nil)
	proxy.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	for i := 0; i < 3; i++ {
		request := httptest.NewRequest(http.MethodGet, "https://compute.googleapis.com/compute/v1/projects/demo/zones/us/instances", nil)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("disabled default request %d status=%d", i, response.Code)
		}
	}
}

func TestQuotaLimiterOwnsConfigurationSnapshot(t *testing.T) {
	services := map[string]QuotaRule{
		"compute.googleapis.com": {Limit: 1, Window: time.Minute},
	}
	limiter, err := NewQuotaLimiter(QuotaConfig{Services: services}, time.Now)
	if err != nil {
		t.Fatal(err)
	}

	services["compute.googleapis.com"] = QuotaRule{Limit: 100, Window: time.Hour}
	delete(services, "compute.googleapis.com")

	if decision := limiter.Allow("compute.googleapis.com", "/v1/resources", "demo"); !decision.Allowed {
		t.Fatalf("first request denied: %#v", decision)
	}
	if decision := limiter.Allow("compute.googleapis.com", "/v1/resources", "demo"); decision.Allowed {
		t.Fatalf("configuration mutation changed live limiter: %#v", decision)
	}
}

func TestAuthorizedRequestsConsumeQuotaAfterAuthentication(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	limiter, err := NewQuotaLimiter(QuotaConfig{
		Services: map[string]QuotaRule{
			"compute.googleapis.com": {Limit: 1, Window: time.Minute},
		},
	}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	proxy := NewProxyRouterWithManager(nil)
	proxy.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	proxy.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: true}, nil, false, "gateway")
	proxy.ConfigureQuota(limiter, nil)

	const route = "http://localhost/_minisky/compute/compute/v1/projects/demo/zones/us/instances"
	unauthorized := httptest.NewRecorder()
	proxy.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, route, nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, body=%s", unauthorized.Code, unauthorized.Body.String())
	}

	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject:  "principal://iam.googleapis.com/projects/local-dev-project/locations/global/workloadIdentityPools/ci-pool/subject/repository:minisky",
		Audience: "gateway",
		Scopes:   []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, want := range []int{http.StatusNoContent, http.StatusTooManyRequests} {
		request := httptest.NewRequest(http.MethodGet, route, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("authorized request %d status=%d want=%d body=%s",
				index+1, response.Code, want, response.Body.String())
		}
		if want == http.StatusTooManyRequests &&
			(!strings.Contains(response.Body.String(), `"RESOURCE_EXHAUSTED"`) ||
				response.Header().Get("Retry-After") == "") {
			t.Fatalf("quota response headers=%v body=%s", response.Header(), response.Body.String())
		}
	}
}
