package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/registry"
	localsecurity "minisky/pkg/security"
	_ "minisky/pkg/shims"
	"minisky/pkg/shims/bigquery"
	"minisky/pkg/state"
)

type testAuthorizer struct {
	issuer *localsecurity.Issuer
	allow  bool
}

type countingAuthorizer struct {
	issuer     *localsecurity.Issuer
	verifies   int
	authorizes int
}

func (a *countingAuthorizer) EnforcementEnabled() bool { return true }
func (a *countingAuthorizer) Authorize(string, string, string) bool {
	a.authorizes++
	return true
}
func (a *countingAuthorizer) VerifyLocalToken(
	token string,
	audience string,
	scope string,
) (localsecurity.Claims, error) {
	a.verifies++
	return a.issuer.Verify(token, localsecurity.VerifyOptions{
		Audience: audience, RequiredScope: scope,
	})
}

type authorizationCheck struct {
	resource   string
	principal  string
	permission string
}

type resourcePermission struct {
	resource   string
	permission string
}

type recordingAuthorizer struct {
	issuer  *localsecurity.Issuer
	allowed map[resourcePermission]bool
	checks  []authorizationCheck
}

func (a *recordingAuthorizer) EnforcementEnabled() bool { return true }
func (a *recordingAuthorizer) Authorize(resource, principal, permission string) bool {
	a.checks = append(a.checks, authorizationCheck{
		resource: resource, principal: principal, permission: permission,
	})
	return a.allowed[resourcePermission{resource: resource, permission: permission}]
}
func (a *recordingAuthorizer) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	return a.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}

func (a testAuthorizer) EnforcementEnabled() bool { return true }
func (a testAuthorizer) Authorize(string, string, string) bool {
	return a.allow
}
func (a testAuthorizer) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	return a.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}

type testProjects map[string]bool

func (p testProjects) Exists(id string) bool { return p[id] }

type countingReplayShim struct {
	api       *bigquery.API
	probes    int
	principal string
}

func (s *countingReplayShim) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.principal = r.Header.Get("X-MiniSky-Principal")
	s.api.ServeHTTP(w, r)
}

func (s *countingReplayShim) IsCompletedUploadReplayCandidate(r *http.Request) bool {
	return s.api.IsCompletedUploadReplayCandidate(r)
}

func (s *countingReplayShim) ProbeCompletedUploadReplay(
	r *http.Request,
) (func(http.ResponseWriter), bool) {
	s.probes++
	return s.api.ProbeCompletedUploadReplay(r)
}

func TestStrictAuthorizationReturnsRedacted401And403(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: false}, testProjects{"demo-project": true}, true, "gateway")

	request := httptest.NewRequest(http.MethodGet, "http://localhost/_minisky/compute/compute/v1/projects/demo-project/zones/us/instances/vm", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"UNAUTHENTICATED"`) {
		t.Fatalf("missing token response=%d body=%s", response.Code, response.Body.String())
	}

	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:alice@example.com", Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "http://localhost/_minisky/compute/compute/v1/projects/demo-project/zones/us/instances/vm", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || strings.Contains(response.Body.String(), token) {
		t.Fatalf("denied response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStrictGatewayAuditUsesOnlyVerifiedPrincipal(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	const principal = "user:alice@example.com"
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: principal, Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name              string
		authorization     string
		suppliedPrincipal string
		wantStatus        int
		wantComplete      string
	}{
		{
			name:              "missing bearer with forged principal",
			suppliedPrincipal: "user:attacker@example.com",
			wantStatus:        http.StatusUnauthorized,
		},
		{
			name:              "invalid bearer with forged principal",
			authorization:     "Bearer invalid-token",
			suppliedPrincipal: "user:attacker@example.com",
			wantStatus:        http.StatusUnauthorized,
		},
		{
			name:          "verified principal denied",
			authorization: "Bearer " + token,
			wantStatus:    http.StatusForbidden,
			wantComplete:  principal,
		},
		{
			name:              "conflicting supplied principal",
			authorization:     "Bearer " + token,
			suppliedPrincipal: "user:attacker@example.com",
			wantStatus:        http.StatusUnauthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := NewProxyRouterWithManager(nil)
			router.RegisterShim("bigquery.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("denied request reached BigQuery shim")
			}))
			router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: false}, nil, false, "gateway")
			audit, err := localsecurity.OpenAuditLog(t.TempDir(), "gateway-auth", true)
			if err != nil {
				t.Fatal(err)
			}
			defer audit.Close()
			handler := audit.Wrap(router, func(r *http.Request) localsecurity.AuditEvent {
				return localsecurity.AuditEvent{
					Principal: r.Header.Get("X-MiniSky-Principal"),
					Service:   "bigquery.googleapis.com",
					Route:     "/bigquery/v2/projects/{project}/datasets",
					Project:   "demo",
				}
			})
			request := httptest.NewRequest(
				http.MethodPost,
				"http://localhost/_minisky/bigquery/bigquery/v2/projects/demo/datasets",
				bytes.NewBufferString(`{"datasetReference":{"datasetId":"denied"}}`),
			)
			request.Header.Set("Content-Type", "application/json")
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			if test.suppliedPrincipal != "" {
				request.Header.Set("X-MiniSky-Principal", test.suppliedPrincipal)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if test.wantComplete != "" && !strings.Contains(response.Body.String(), "bigquery.datasets.update") {
				t.Fatalf("verified denial did not name mapped permission: %s", response.Body.String())
			}
			var exported bytes.Buffer
			if err := audit.Export(&exported, 10); err != nil {
				t.Fatal(err)
			}
			var records []localsecurity.AuditRecord
			if err := json.Unmarshal(exported.Bytes(), &records); err != nil {
				t.Fatal(err)
			}
			if len(records) != 2 ||
				records[0].Phase != "attempt" || records[0].Principal != "" ||
				records[1].Phase != "complete" || records[1].Principal != test.wantComplete {
				t.Fatalf("audit records = %#v, want attempt principal empty and complete principal %q", records, test.wantComplete)
			}
		})
	}
}

func TestStrictGatewayAuditClearsForgedPrincipalBeforeEarlyRejection(t *testing.T) {
	const limit = 1 << 20
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("logging.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized request reached Logging shim")
	}))
	router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: false}, nil, false, "gateway")
	audit, err := localsecurity.OpenAuditLog(t.TempDir(), "gateway-early-rejection", true)
	if err != nil {
		t.Fatal(err)
	}
	defer audit.Close()
	handler := audit.Wrap(router, func(r *http.Request) localsecurity.AuditEvent {
		return localsecurity.AuditEvent{
			Principal: r.Header.Get("X-MiniSky-Principal"),
			Service:   "logging.googleapis.com",
			Route:     "/v2/entries:write",
			Project:   "demo",
		}
	})
	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/logging/v2/entries:write",
		strings.NewReader(strings.Repeat("x", limit+1)),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-MiniSky-Principal", "user:attacker@example.com")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want=%d body=%s", response.Code, http.StatusRequestEntityTooLarge, response.Body.String())
	}
	var exported bytes.Buffer
	if err := audit.Export(&exported, 10); err != nil {
		t.Fatal(err)
	}
	var records []localsecurity.AuditRecord
	if err := json.Unmarshal(exported.Bytes(), &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 ||
		records[0].Phase != "attempt" || records[0].Principal != "" ||
		records[1].Phase != "complete" || records[1].Principal != "" {
		t.Fatalf("audit records = %#v, want empty principal on attempt and early-rejection completion", records)
	}
}

func TestUnknownProjectEnforcementIsOptional(t *testing.T) {
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("bigquery.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	router.ConfigureSecurity(nil, testProjects{"known-project": true}, true, "")
	request := httptest.NewRequest(http.MethodGet, "http://localhost/_minisky/bigquery/bigquery/v2/projects/unknown-project/datasets", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), `"NOT_FOUND"`) {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStrictAuthorizationDefaultDeniesUnmappedMutations(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("logging.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: true}, nil, false, "gateway")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:admin@example.com", Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/logging/v2/entries:write",
		bytes.NewBufferString(`{"entries":[{"logName":"projects/demo/logs/app","textPayload":"denied"}]}`),
	)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), "unmapped mutation") {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStrictSTSBootstrapReachesSubjectTokenValidation(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "router-sts-bootstrap")
	shims, _ := registry.BootAll(nil, nil)
	authorizer, ok := shims["iam.googleapis.com"].(routeAuthorizer)
	if !ok {
		t.Fatal("IAM shim does not implement router authorization")
	}
	issuer, ok := shims["iam.googleapis.com"].(interface {
		IssueLocalToken(string, string, []string, time.Duration) (string, time.Time, error)
	})
	if !ok {
		t.Fatal("IAM shim does not implement local token issuance")
	}
	const scope = "https://www.googleapis.com/auth/cloud-platform"
	subjectToken, _, err := issuer.IssueLocalToken(
		"principal://iam.googleapis.com/pool/subject/workload", "sts-audience", []string{scope}, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("sts.googleapis.com", shims["sts.googleapis.com"])
	const gatewayAudience = "https://gateway.minisky.test"
	router.ConfigureSecurity(authorizer, nil, false, gatewayAudience)

	for _, test := range []struct {
		name, subject string
		want          int
	}{
		{name: "valid subject reaches handler", subject: subjectToken, want: http.StatusOK},
		{name: "invalid subject rejected by handler", subject: "invalid-subject", want: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			form := url.Values{
				"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
				"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
				"subject_token_type":   {"urn:ietf:params:oauth:token-type:access_token"},
				"subject_token":        {test.subject},
				"audience":             {"sts-audience"},
				"scope":                {scope},
			}
			request := httptest.NewRequest(http.MethodPost,
				"http://localhost/_minisky/sts/v1/token",
				strings.NewReader(form.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body.String())
			}
			if test.want == http.StatusOK {
				var body struct {
					AccessToken string `json:"access_token"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if _, err := authorizer.VerifyLocalToken(body.AccessToken, gatewayAudience, scope); err != nil {
					t.Fatalf("STS token does not match router audience: %v", err)
				}
			}
		})
	}

	request := httptest.NewRequest(http.MethodPost, "http://localhost/_minisky/sts/v1/other", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("other STS path status=%d want=%d", response.Code, http.StatusUnauthorized)
	}
}

func TestStrictIAMCredentialsUsesBearerPrincipalAndDefersAuthorization(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("iamcredentials.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-MiniSky-Principal"); got != "principal://iam.googleapis.com/pool/subject/workload" {
			t.Fatalf("principal=%q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: false}, nil, false, "gateway")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "principal://iam.googleapis.com/pool/subject/workload", Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost,
		"http://localhost/_minisky/iamcredentials/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
		strings.NewReader(`{"scope":["scope"]}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-MiniSky-Principal", "user:attacker@example.com")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("conflict status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost,
		"http://localhost/_minisky/iamcredentials/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
		strings.NewReader(`{"scope":["scope"]}`))
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestIAMMutationsHaveExplicitProjectScopedPermissions(t *testing.T) {
	tests := []struct {
		method, path, permission string
	}{
		{http.MethodPost, "/v1/projects/demo/serviceAccounts", "iam.serviceAccounts.create"},
		{http.MethodDelete, "/v1/projects/demo/serviceAccounts/account@example.test", "iam.serviceAccounts.delete"},
		{http.MethodPost, "/v1/projects/demo/serviceAccounts/account@example.test/keys", "iam.serviceAccountKeys.create"},
		{http.MethodDelete, "/v1/projects/demo/serviceAccounts/account@example.test/keys/key-1", "iam.serviceAccountKeys.delete"},
		{http.MethodPost, "/v1/projects/demo/serviceAccounts/account@example.test/keys/key-1:disable", "iam.serviceAccountKeys.disable"},
		{http.MethodPost, "/v1/projects/demo/serviceAccounts/account@example.test:setIamPolicy", "iam.serviceAccounts.setIamPolicy"},
	}
	for _, test := range tests {
		t.Run(test.permission, func(t *testing.T) {
			permission, resource := routePermission("iam.googleapis.com", httptest.NewRequest(test.method, test.path, nil))
			if permission != test.permission || resource != "projects/demo" {
				t.Fatalf("permission=%q resource=%q", permission, resource)
			}
		})
	}
}

func TestEnforceProjectsChecksLoggingBodyBeforeDispatch(t *testing.T) {
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("logging.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	router.ConfigureSecurity(nil, testProjects{"known-project": true}, true, "")
	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/logging/v2/entries:write",
		bytes.NewBufferString(`{"entries":[{"logName":"projects/unknown-project/logs/app"}]}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || !strings.Contains(response.Body.String(), "unknown-project") {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestProjectInspectionPreservesExactLimitBody(t *testing.T) {
	const limit = 1 << 20
	base := `{"projectId":"known-project","entries":[]}`
	body := base + strings.Repeat(" ", limit-len(base))
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("logging.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read accepted body: %v", err)
		}
		if string(got) != body {
			t.Fatalf("accepted body length=%d, want %d", len(got), len(body))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	router.ConfigureSecurity(nil, testProjects{"known-project": true}, true, "")
	request := httptest.NewRequest(http.MethodPost, "http://localhost/_minisky/logging/v2/entries:write",
		strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestProjectClassificationDoesNotTruncateLargerBoundedJSON(t *testing.T) {
	const size = 2 << 20
	base := `{"projectId":"known-project"}`
	body := base + strings.Repeat(" ", size-len(base))
	request := httptest.NewRequest(http.MethodPost, "http://localhost/v1/resources", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")

	if got := ProjectFromRequest(request); got != "" {
		t.Fatalf("project=%q, want body inspection skipped above bounded classifier window", got)
	}
	restored, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(restored) != body {
		t.Fatalf("restored body length=%d, want=%d", len(restored), len(body))
	}
}

func TestProjectInspectionRejectsOversizedBodyWithoutDispatch(t *testing.T) {
	const limit = 1 << 20
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("logging.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized request reached shim")
	}))
	router.ConfigureSecurity(nil, testProjects{"known-project": true}, true, "")
	request := httptest.NewRequest(http.MethodPost, "http://localhost/_minisky/logging/v2/entries:write",
		strings.NewReader(strings.Repeat("x", limit+1)))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge ||
		!strings.Contains(response.Body.String(), `"RESOURCE_EXHAUSTED"`) {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGatewayFallbackRejectsOversizedUnmatchedJSONBeforeDispatch(t *testing.T) {
	const size = (1 << 20) + 1
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("custom.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized request reached shim")
	}))
	request := httptest.NewRequest(http.MethodPost, "http://localhost/_minisky/custom/unvalidated",
		strings.NewReader(strings.Repeat("x", size)))
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge ||
		!strings.Contains(response.Body.String(), `"INVALID_ARGUMENT"`) {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestExplicitUploadLimitOverridesGatewayFallback(t *testing.T) {
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("bigquery.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			t.Fatalf("read upload body: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/bigquery/upload/bigquery/v2/projects/demo/jobs?uploadType=multipart",
		strings.NewReader("small streamed body"),
	)
	request.Header.Set("Content-Type", "multipart/related; boundary=test")
	request.ContentLength = 2 << 20
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGatewayDoesNotPrebufferNonJSONBody(t *testing.T) {
	source := &countingReadCloser{Reader: strings.NewReader("streamed body")}
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("custom.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if source.reads != 0 {
			t.Fatalf("gateway read streamed body before dispatch: reads=%d", source.reads)
		}
		if _, err := io.ReadAll(r.Body); err != nil {
			t.Fatalf("read streamed body: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodPost, "http://localhost/_minisky/custom/upload", nil)
	request.Body = source
	request.ContentLength = int64(len("streamed body"))
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGatewaySpoolsUnknownLengthBeforeDispatchAndRejectsOversize(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "unknown-spool")
	ownership := acquireRouterTestOwnership(t)
	defer ownership.Close()

	const size = (1 << 20) + 1
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("custom.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized unknown-length request reached shim")
	}))
	request := httptest.NewRequest(http.MethodPost, "http://localhost/_minisky/custom/upload", nil)
	request.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", size)))
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge ||
		!strings.Contains(response.Body.String(), `"INVALID_ARGUMENT"`) {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestCompletedBigQueryRetryBypassesGatewayDeclaredLimit(t *testing.T) {
	proxy, api, ownership := completedBigQueryProxy(t, "gateway-declared-replay")
	defer ownership.Close()
	location, expected := completeBigQueryUploadThroughProxy(t, proxy, api, "gateway-declared-job")

	request := httptest.NewRequest(http.MethodPost, location, nil)
	request.Body = panicReadCloser{}
	request.ContentLength = (50 << 20) + 1
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != expected {
		t.Fatalf("replay status/body=(%d,%q), want (200,%q)",
			response.Code, response.Body.String(), expected)
	}
}

func TestCompletedBigQueryRetryBypassesGatewaySpoolQuota(t *testing.T) {
	proxy, api, ownership := completedBigQueryProxy(t, "gateway-quota-replay")
	defer ownership.Close()
	location, expected := completeBigQueryUploadThroughProxy(t, proxy, api, "gateway-quota-job")
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	proxy.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: true}, nil, false, "gateway")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject:  "user:alice@example.com",
		Audience: "gateway",
		Scopes:   []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	quota := profileBodySpoolQuota(config.GetProfileDir())
	quota.mu.Lock()
	remaining := quota.max - quota.used
	quota.mu.Unlock()
	if !quota.reserve(remaining) {
		t.Fatal("failed to exhaust gateway request-spool quota")
	}
	defer quota.release(remaining)

	source := &countingReadCloser{Reader: strings.NewReader("abc")}
	request := httptest.NewRequest(http.MethodPost, location, nil)
	request.Body = source
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != expected {
		t.Fatalf("replay status/body=(%d,%q), want (200,%q)",
			response.Code, response.Body.String(), expected)
	}
	if source.reads != 0 {
		t.Fatalf("completed replay read unknown-length body %d times", source.reads)
	}
}

func TestCompletedBigQueryRetryRequiresAuthenticationBeforeReplay(t *testing.T) {
	proxy, api, ownership := completedBigQueryProxy(t, "gateway-auth-replay")
	defer ownership.Close()
	location, _ := completeBigQueryUploadThroughProxy(t, proxy, api, "gateway-auth-job")

	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	proxy.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: true}, nil, false, "gateway")
	request := httptest.NewRequest(http.MethodPost, location, nil)
	request.Body = panicReadCloser{}
	request.ContentLength = (50 << 20) + 1
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestUnauthenticatedUploadCandidatesNeverProbeReplayState(t *testing.T) {
	proxy, api, ownership := completedBigQueryProxy(t, "gateway-auth-oracle")
	defer ownership.Close()
	completedLocation, _ := completeBigQueryUploadThroughProxy(t, proxy, api, "oracle-completed")
	incompleteLocation := startBigQueryUploadThroughProxy(t, proxy, "oracle-incomplete")
	probe := &countingReplayShim{api: api}
	proxy.RegisterShim("bigquery.googleapis.com", probe)

	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	proxy.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: true}, nil, false, "gateway")
	unknownLocation := strings.Replace(
		completedLocation,
		"upload_id="+url.QueryEscape(uploadIDFromUploadLocation(t, completedLocation)),
		"upload_id=unknown",
		1,
	)
	malformedLocation := strings.Replace(
		completedLocation,
		"upload_id="+url.QueryEscape(uploadIDFromUploadLocation(t, completedLocation)),
		"upload_id=",
		1,
	)
	var expectedBody string
	for _, location := range []string{
		completedLocation,
		unknownLocation,
		incompleteLocation,
		malformedLocation,
	} {
		request := httptest.NewRequest(http.MethodPost, location, nil)
		request.Body = panicReadCloser{}
		request.ContentLength = (50 << 20) + 1
		request.Header.Set("Content-Type", "application/octet-stream")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("location=%s status=%d body=%s", location, response.Code, response.Body.String())
		}
		if expectedBody == "" {
			expectedBody = response.Body.String()
		} else if response.Body.String() != expectedBody {
			t.Fatalf("authentication oracle: body=%q want=%q", response.Body.String(), expectedBody)
		}
	}
	if probe.probes != 0 {
		t.Fatalf("unauthenticated candidates triggered %d replay probes, want 0", probe.probes)
	}
}

func TestAuthenticatedUploadCandidatesAuthorizeAndProbeExactlyOnce(t *testing.T) {
	proxy, api, ownership := completedBigQueryProxy(t, "gateway-auth-preflight")
	defer ownership.Close()
	completedLocation, expected := completeBigQueryUploadThroughProxy(t, proxy, api, "preflight-completed")
	incompleteLocation := startBigQueryUploadThroughProxy(t, proxy, "preflight-incomplete")
	probe := &countingReplayShim{api: api}
	proxy.RegisterShim("bigquery.googleapis.com", probe)

	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	authorizer := &countingAuthorizer{issuer: issuer}
	proxy.ConfigureSecurity(authorizer, nil, false, "gateway")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject:  "user:alice@example.com",
		Audience: "gateway",
		Scopes:   []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	unknownLocation := strings.Replace(
		completedLocation,
		"upload_id="+url.QueryEscape(uploadIDFromUploadLocation(t, completedLocation)),
		"upload_id=unknown",
		1,
	)
	for index, test := range []struct {
		location string
		status   int
		body     string
	}{
		{location: completedLocation, status: http.StatusOK, body: expected},
		{location: unknownLocation, status: http.StatusRequestEntityTooLarge},
		{location: incompleteLocation, status: http.StatusRequestEntityTooLarge},
	} {
		request := httptest.NewRequest(http.MethodPost, test.location, nil)
		request.Body = panicReadCloser{}
		request.ContentLength = (50 << 20) + 1
		request.Header.Set("Content-Type", "application/octet-stream")
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("location=%s status=%d want=%d body=%s",
				test.location, response.Code, test.status, response.Body.String())
		}
		if test.body != "" && response.Body.String() != test.body {
			t.Fatalf("completed replay body=%q want=%q", response.Body.String(), test.body)
		}
		wantCalls := index + 1
		if authorizer.verifies != wantCalls || authorizer.authorizes != wantCalls {
			t.Fatalf("after request %d verify/authorize=(%d,%d), want (%d,%d)",
				index, authorizer.verifies, authorizer.authorizes, wantCalls, wantCalls)
		}
		if probe.probes != wantCalls {
			t.Fatalf("after request %d probes=%d want=%d", index, probe.probes, wantCalls)
		}
	}

	request := httptest.NewRequest(http.MethodPost, unknownLocation, strings.NewReader("abc"))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Range", "bytes 0-2/3")
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("dispatched unknown status=%d body=%s", response.Code, response.Body.String())
	}
	if authorizer.verifies != 4 || authorizer.authorizes != 4 || probe.probes != 4 {
		t.Fatalf("dispatch verify/authorize/probe=(%d,%d,%d), want (4,4,4)",
			authorizer.verifies, authorizer.authorizes, probe.probes)
	}
	if probe.principal != "user:alice@example.com" {
		t.Fatalf("dispatched principal=%q", probe.principal)
	}
}

func TestUnknownBigQueryUploadIDStillUsesGatewayBodyLimit(t *testing.T) {
	proxy, _, ownership := completedBigQueryProxy(t, "gateway-unknown-replay")
	defer ownership.Close()
	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/upload/bigquery/v2/projects/demo/jobs?uploadType=resumable&upload_id=unknown",
		nil,
	)
	request.Body = panicReadCloser{}
	request.ContentLength = (50 << 20) + 1
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestIncompleteBigQueryUploadIDStillUsesGatewayBodyLimit(t *testing.T) {
	proxy, _, ownership := completedBigQueryProxy(t, "gateway-incomplete-replay")
	defer ownership.Close()
	metadata := `{"jobReference":{"jobId":"incomplete-job","location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"demo","datasetId":"dataset","tableId":"events"},"sourceFormat":"NEWLINE_DELIMITED_JSON"}}}`
	start := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/upload/bigquery/v2/projects/demo/jobs?uploadType=resumable",
		strings.NewReader(metadata),
	)
	start.Header.Set("Content-Type", "application/json")
	startResponse := httptest.NewRecorder()
	proxy.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusOK {
		t.Fatalf("start status=%d body=%s", startResponse.Code, startResponse.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, startResponse.Header().Get("Location"), nil)
	request.Body = panicReadCloser{}
	request.ContentLength = (50 << 20) + 1
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func completedBigQueryProxy(
	t *testing.T,
	profile string,
) (*ProxyRouter, *bigquery.API, *state.Ownership) {
	t.Helper()
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", profile)
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	api := bigquery.NewAPI(nil)
	proxy := NewProxyRouterWithManager(nil)
	proxy.RegisterShim("bigquery.googleapis.com", api)
	return proxy, api, ownership
}

func completeBigQueryUploadThroughProxy(
	t *testing.T,
	proxy *ProxyRouter,
	api *bigquery.API,
	jobID string,
) (string, string) {
	t.Helper()
	_ = api
	location := startBigQueryUploadThroughProxy(t, proxy, jobID)
	finish := httptest.NewRequest(http.MethodPost, location, strings.NewReader("abc"))
	finish.Header.Set("Content-Type", "application/octet-stream")
	finish.Header.Set("Content-Range", "bytes 0-2/3")
	finishResponse := httptest.NewRecorder()
	proxy.ServeHTTP(finishResponse, finish)
	if finishResponse.Code != http.StatusOK {
		t.Fatalf("finish status=%d body=%s", finishResponse.Code, finishResponse.Body.String())
	}
	return location, finishResponse.Body.String()
}

func startBigQueryUploadThroughProxy(
	t *testing.T,
	proxy *ProxyRouter,
	jobID string,
) string {
	t.Helper()
	metadata := fmt.Sprintf(
		`{"jobReference":{"jobId":%q,"location":"US"},"configuration":{"load":{"destinationTable":{"projectId":"demo","datasetId":"dataset","tableId":"events"},"sourceFormat":"NEWLINE_DELIMITED_JSON"}}}`,
		jobID,
	)
	start := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/upload/bigquery/v2/projects/demo/jobs?uploadType=resumable",
		strings.NewReader(metadata),
	)
	start.Header.Set("Content-Type", "application/json")
	startResponse := httptest.NewRecorder()
	proxy.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusOK {
		t.Fatalf("start status=%d body=%s", startResponse.Code, startResponse.Body.String())
	}
	return startResponse.Header().Get("Location")
}

func uploadIDFromUploadLocation(t *testing.T, location string) string {
	t.Helper()
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Query().Get("upload_id")
}

func TestGatewayRejectsSymlinkedRequestSpoolDirectory(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", stateDir)
	t.Setenv("MINISKY_PROFILE", "symlink-spool")
	ownership := acquireRouterTestOwnership(t)
	defer ownership.Close()

	external := t.TempDir()
	profileDir := filepath.Join(stateDir, "profiles", "symlink-spool")
	if err := os.Symlink(external, filepath.Join(profileDir, "request-spool")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	externalFile := filepath.Join(external, "keep")
	if err := os.WriteFile(externalFile, []byte("external"), 0o600); err != nil {
		t.Fatal(err)
	}

	proxy := NewProxyRouterWithManager(nil)
	proxy.RegisterShim("custom.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("request with unsafe spool path reached shim")
	}))
	request := httptest.NewRequest(http.MethodPost, "http://localhost/_minisky/custom/upload", nil)
	request.Body = io.NopCloser(strings.NewReader("streamed"))
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(externalFile); err != nil {
		t.Fatalf("external file was touched: %v", err)
	}
}

func acquireRouterTestOwnership(t *testing.T) *state.Ownership {
	t.Helper()
	store, err := state.New(os.Getenv("MINISKY_STATE_DIR"), os.Getenv("MINISKY_PROFILE"))
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	return ownership
}

type countingReadCloser struct {
	io.Reader
	reads int
}

func (r *countingReadCloser) Read(payload []byte) (int, error) {
	r.reads++
	return r.Reader.Read(payload)
}

func (*countingReadCloser) Close() error { return nil }

type panicReadCloser struct{}

func (panicReadCloser) Read([]byte) (int, error) {
	panic("completed retry body was read")
}

func (panicReadCloser) Close() error { return nil }

func TestGatewayAloneEnforcesCrossProjectPubSubAttachment(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("pubsub.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("denied request reached Pub/Sub shim")
	}))
	authorizer := &recordingAuthorizer{
		issuer: issuer,
		allowed: map[resourcePermission]bool{
			{resource: "projects/subscriber-project", permission: "pubsub.subscriptions.create"}: true,
		},
	}
	router.ConfigureSecurity(authorizer, nil, false, "gateway")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:subscriber@example.com", Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut,
		"http://localhost/_minisky/pubsub/v1/projects/subscriber-project/subscriptions/events",
		bytes.NewBufferString(`{"topic":"projects/publisher-project/topics/events"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden ||
		!strings.Contains(response.Body.String(), "pubsub.topics.attachSubscription") {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
	wantChecks := []authorizationCheck{
		{
			resource: "projects/subscriber-project", principal: "user:subscriber@example.com",
			permission: "pubsub.subscriptions.create",
		},
		{
			resource: "projects/publisher-project/topics/events", principal: "user:subscriber@example.com",
			permission: "pubsub.topics.attachSubscription",
		},
	}
	if !reflect.DeepEqual(authorizer.checks, wantChecks) {
		t.Fatalf("authorization checks = %#v, want %#v", authorizer.checks, wantChecks)
	}
}

func TestPubSubSubscriptionCreateAuthorizationContracts(t *testing.T) {
	const (
		principal           = "user:subscriber@example.com"
		subscriptionProject = "subscriber-project"
		subscription        = "projects/subscriber-project/subscriptions/events"
		publisherTopic      = "projects/publisher-project/topics/events"
	)
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: principal, Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	createPermission := resourcePermission{
		resource: "projects/" + subscriptionProject, permission: "pubsub.subscriptions.create",
	}
	attachPermission := resourcePermission{
		resource: publisherTopic, permission: "pubsub.topics.attachSubscription",
	}

	for _, test := range []struct {
		name          string
		body          string
		allowed       map[resourcePermission]bool
		wantStatus    int
		wantDispatch  int
		wantChecks    []authorizationCheck
		wantErrorText string
	}{
		{
			name: "same project requires only create",
			body: `{"topic":"projects/subscriber-project/topics/events","labels":{"source":"contract"}}`,
			allowed: map[resourcePermission]bool{
				createPermission: true,
			},
			wantStatus:   http.StatusNoContent,
			wantDispatch: 1,
			wantChecks: []authorizationCheck{{
				resource: createPermission.resource, principal: principal, permission: createPermission.permission,
			}},
		},
		{
			name: "cross project exact topic attach dispatches once",
			body: `{"topic":"projects/publisher-project/topics/events","labels":{"source":"contract"}}`,
			allowed: map[resourcePermission]bool{
				createPermission: true,
				attachPermission: true,
			},
			wantStatus:   http.StatusNoContent,
			wantDispatch: 1,
			wantChecks: []authorizationCheck{
				{resource: createPermission.resource, principal: principal, permission: createPermission.permission},
				{resource: attachPermission.resource, principal: principal, permission: attachPermission.permission},
			},
		},
		{
			name: "attach on wrong topic is denied",
			body: `{"topic":"projects/publisher-project/topics/events","labels":{"source":"contract"}}`,
			allowed: map[resourcePermission]bool{
				createPermission: true,
				{
					resource:   "projects/publisher-project/topics/other",
					permission: "pubsub.topics.attachSubscription",
				}: true,
			},
			wantStatus:    http.StatusForbidden,
			wantDispatch:  0,
			wantErrorText: "pubsub.topics.attachSubscription",
			wantChecks: []authorizationCheck{
				{resource: createPermission.resource, principal: principal, permission: createPermission.permission},
				{resource: attachPermission.resource, principal: principal, permission: attachPermission.permission},
			},
		},
		{
			name: "attach in wrong project is denied",
			body: `{"topic":"projects/publisher-project/topics/events","labels":{"source":"contract"}}`,
			allowed: map[resourcePermission]bool{
				createPermission: true,
				{
					resource:   "projects/other-project/topics/events",
					permission: "pubsub.topics.attachSubscription",
				}: true,
			},
			wantStatus:    http.StatusForbidden,
			wantDispatch:  0,
			wantErrorText: "pubsub.topics.attachSubscription",
			wantChecks: []authorizationCheck{
				{resource: createPermission.resource, principal: principal, permission: createPermission.permission},
				{resource: attachPermission.resource, principal: principal, permission: attachPermission.permission},
			},
		},
		{
			name: "relative topic is invalid",
			body: `{"topic":"topics/events"}`,
			allowed: map[resourcePermission]bool{
				createPermission: true,
			},
			wantStatus:    http.StatusBadRequest,
			wantDispatch:  0,
			wantErrorText: `"INVALID_ARGUMENT"`,
			wantChecks: []authorizationCheck{{
				resource: createPermission.resource, principal: principal, permission: createPermission.permission,
			}},
		},
		{
			name: "malformed canonical topic is invalid",
			body: `{"topic":"projects/publisher-project/topics"}`,
			allowed: map[resourcePermission]bool{
				createPermission: true,
			},
			wantStatus:    http.StatusBadRequest,
			wantDispatch:  0,
			wantErrorText: `"INVALID_ARGUMENT"`,
			wantChecks: []authorizationCheck{{
				resource: createPermission.resource, principal: principal, permission: createPermission.permission,
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dispatches := 0
			router := NewProxyRouterWithManager(nil)
			router.RegisterShim("pubsub.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				dispatches++
				gotBody, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("read dispatched body: %v", err)
				}
				if string(gotBody) != test.body {
					t.Fatalf("dispatched body = %q, want exact %q", gotBody, test.body)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			authorizer := &recordingAuthorizer{issuer: issuer, allowed: test.allowed}
			router.ConfigureSecurity(authorizer, nil, false, "gateway")
			request := httptest.NewRequest(
				http.MethodPut,
				"http://localhost/_minisky/pubsub/v1/"+subscription,
				strings.NewReader(test.body),
			)
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			router.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if dispatches != test.wantDispatch {
				t.Fatalf("dispatches=%d want=%d", dispatches, test.wantDispatch)
			}
			if test.wantErrorText != "" && !strings.Contains(response.Body.String(), test.wantErrorText) {
				t.Fatalf("body=%s want substring %q", response.Body.String(), test.wantErrorText)
			}
			if !reflect.DeepEqual(authorizer.checks, test.wantChecks) {
				t.Fatalf("authorization checks = %#v, want %#v", authorizer.checks, test.wantChecks)
			}
		})
	}
}

func TestServeHTTPRoutesCanonicalLocalServiceEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		service   string
		domain    string
		path      string
		wantPath  string
		wantQuery string
	}{
		{
			name:      "compute",
			service:   "compute",
			domain:    "compute.googleapis.com",
			path:      "/v1/projects/demo/zones/us-central1-a/instances",
			wantPath:  "/v1/projects/demo/zones/us-central1-a/instances",
			wantQuery: "maxResults=10",
		},
		{
			name:     "sqladmin",
			service:  "sqladmin",
			domain:   "sqladmin.googleapis.com",
			path:     "/v1/projects/demo/instances",
			wantPath: "/v1/projects/demo/instances",
		},
		{
			name:     "iam",
			service:  "iam",
			domain:   "iam.googleapis.com",
			path:     "/v1/projects/demo/serviceAccounts",
			wantPath: "/v1/projects/demo/serviceAccounts",
		},
		{
			name:     "gke",
			service:  "container",
			domain:   "container.googleapis.com",
			path:     "/v1/projects/demo/locations/us-central1/clusters",
			wantPath: "/v1/projects/demo/locations/us-central1/clusters",
		},
		{
			name:     "dns",
			service:  "dns",
			domain:   "dns.googleapis.com",
			path:     "/dns/v1/projects/demo/managedZones",
			wantPath: "/dns/v1/projects/demo/managedZones",
		},
		{
			name:     "secret manager",
			service:  "secretmanager",
			domain:   "secretmanager.googleapis.com",
			path:     "/v1/projects/demo/secrets",
			wantPath: "/v1/projects/demo/secrets",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			router := NewProxyRouterWithManager(nil)
			router.RegisterShim(tt.domain, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tt.wantPath {
					t.Errorf("path = %q, want %q", r.URL.Path, tt.wantPath)
				}
				if r.URL.Query().Get("maxResults") != tt.wantQuery {
					t.Errorf("maxResults = %q, want %q", r.URL.Query().Get("maxResults"), tt.wantQuery)
				}
				w.WriteHeader(http.StatusNoContent)
			}))

			requestURL := "http://localhost:8080/_minisky/" + tt.service + tt.path
			if tt.wantQuery != "" {
				requestURL += "?maxResults=" + tt.wantQuery
			}
			req := httptest.NewRequest(http.MethodGet, requestURL, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
			}
		})
	}
}

func TestClassifyRequestUsesCanonicalDomainAndBoundedRoute(t *testing.T) {
	t.Parallel()
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	request := httptest.NewRequest(
		http.MethodGet,
		"http://localhost:8080/_minisky/compute/v1/projects/demo/zones/us-central1-a/instances/vm-1",
		nil,
	)
	labels := router.ClassifyRequest(request)
	if labels.Service != "compute.googleapis.com" {
		t.Fatalf("service = %q", labels.Service)
	}
	if labels.Route != "/v1/projects/{id}/zones/{id}/instances/{id}" {
		t.Fatalf("route = %q", labels.Route)
	}
}

func TestServeHTTPRoutesCanonicalEndpointByRegisteredDomain(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("custom.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/resources" {
			t.Errorf("path = %q, want /v1/resources", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(
		http.MethodGet,
		"http://127.0.0.1:8080/_minisky/custom.example.test/v1/resources",
		nil,
	)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestServeHTTPRoutesEveryRegisteredCanonicalSelectorAndAlias(t *testing.T) {
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}

	router := NewProxyRouterWithManager(nil)
	for _, service := range services {
		domain := service.Domain
		router.RegisterShim(domain, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Routed-Domain", domain)
			w.Header().Set("X-Routed-Path", r.URL.Path)
			w.Header().Set("X-Routed-Query", r.URL.RawQuery)
			w.WriteHeader(http.StatusNoContent)
		}))
	}

	for _, service := range services {
		service := service
		alias, _, _ := strings.Cut(service.Domain, ".")
		for _, selector := range []string{service.Domain, alias} {
			selector := selector
			t.Run(service.Domain+"/"+selector, func(t *testing.T) {
				request := httptest.NewRequest(
					http.MethodGet,
					"http://127.0.0.1:8080/_minisky/"+selector+"/v1/projects/demo/resources?pageToken=next",
					nil,
				)
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)

				if response.Code != http.StatusNoContent {
					t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusNoContent, response.Body.String())
				}
				if got := response.Header().Get("X-Routed-Domain"); got != service.Domain {
					t.Fatalf("domain = %q, want %q", got, service.Domain)
				}
				if got := response.Header().Get("X-Routed-Path"); got != "/v1/projects/demo/resources" {
					t.Fatalf("path = %q", got)
				}
				if got := response.Header().Get("X-Routed-Query"); got != "pageToken=next" {
					t.Fatalf("query = %q", got)
				}
			})
		}
	}
}

func TestServeHTTPDoesNotGuessAmbiguousBareLocalPath(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	for _, domain := range []string{
		"compute.googleapis.com",
		"sqladmin.googleapis.com",
		"iam.googleapis.com",
		"container.googleapis.com",
		"secretmanager.googleapis.com",
	} {
		router.RegisterShim(domain, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			t.Error("handler was called for ambiguous bare /v1 path")
			w.WriteHeader(http.StatusNoContent)
		}))
	}

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/v1/projects/demo/resources", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNotImplemented, rec.Body.String())
	}
}

func TestServeHTTPPreservesLegacyLocalPathAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		domain string
		path   string
	}{
		{name: "storage", domain: "storage.googleapis.com", path: "/storage/v1/b/demo/o"},
		{name: "storage upload", domain: "storage.googleapis.com", path: "/upload/storage/v1/b/demo/o"},
		{name: "bigquery upload", domain: "bigquery.googleapis.com", path: "/upload/bigquery/v2/projects/demo/jobs"},
		{name: "bigquery", domain: "bigquery.googleapis.com", path: "/bigquery/v2/projects/demo/datasets"},
		{name: "pubsub topics", domain: "pubsub.googleapis.com", path: "/v1/projects/demo/topics"},
		{name: "pubsub subscriptions", domain: "pubsub.googleapis.com", path: "/projects/demo/subscriptions"},
		{name: "cloud functions v2", domain: "cloudfunctions.googleapis.com", path: "/v2/projects/demo/locations/us-central1/functions"},
		{name: "cloud functions v1", domain: "cloudfunctions.googleapis.com", path: "/v1/projects/demo/locations/us-central1/functions"},
		{name: "compute", domain: "compute.googleapis.com", path: "/compute/v1/projects/demo/global/networks"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			router := NewProxyRouterWithManager(nil)
			router.RegisterShim(tt.domain, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tt.path {
					t.Errorf("path = %q, want unchanged legacy path %q", r.URL.Path, tt.path)
				}
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(http.MethodGet, "http://localhost:8080"+tt.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
			}
		})
	}
}

func TestServeHTTPDisablesAmbiguousServiceAliasDeterministically(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	for _, domain := range []string{"shared.googleapis.com", "shared.example.test"} {
		domain := domain
		router.RegisterShim(domain, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Routed-Domain", domain)
			w.WriteHeader(http.StatusNoContent)
		}))
	}

	ambiguous := httptest.NewRequest(
		http.MethodGet,
		"http://localhost:8080/_minisky/shared/v1/resources",
		nil,
	)
	ambiguousRec := httptest.NewRecorder()
	router.ServeHTTP(ambiguousRec, ambiguous)
	if ambiguousRec.Code != http.StatusNotImplemented {
		t.Fatalf("ambiguous alias status = %d, want %d", ambiguousRec.Code, http.StatusNotImplemented)
	}

	for _, domain := range []string{"shared.googleapis.com", "shared.example.test"} {
		req := httptest.NewRequest(
			http.MethodGet,
			"http://localhost:8080/_minisky/"+domain+"/v1/resources",
			nil,
		)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s status = %d, want %d; body: %s", domain, rec.Code, http.StatusNoContent, rec.Body.String())
		}
		if got := rec.Header().Get("X-Routed-Domain"); got != domain {
			t.Fatalf("routed domain = %q, want %q", got, domain)
		}
	}
}

func TestServeHTTPReturnsNotImplementedForUnknownCanonicalService(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	req := httptest.NewRequest(
		http.MethodGet,
		"http://localhost:8080/_minisky/unknown/v1/resources",
		nil,
	)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNotImplemented, rec.Body.String())
	}
}

func TestServeHTTPCanonicalEndpointUsesResolvedDomainForValidation(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("sqladmin.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("handler was called for an invalid SQL Admin request")
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(
		http.MethodPost,
		"http://localhost:8080/_minisky/sqladmin/v1/projects/demo/instances",
		strings.NewReader(`{}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "sql.instances.insert") {
		t.Fatalf("body = %q, want SQL Admin validation error", rec.Body.String())
	}
}

func TestServeHTTPCanonicalComputeEndpointRejectsOversizedSubnetworkBeforeDispatch(t *testing.T) {
	t.Parallel()

	dispatched := false
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dispatched = true
		w.WriteHeader(http.StatusNoContent)
	}))
	body := `{"name":"large","ipCidrRange":"10.0.0.0/24","network":"custom","description":"` +
		strings.Repeat("x", (1<<20)+1) + `"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"http://localhost:8080/_minisky/compute/compute/v1/projects/demo/regions/us-central1/subnetworks",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge ||
		!strings.Contains(rec.Body.String(), `"INVALID_ARGUMENT"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if dispatched {
		t.Fatal("oversized request reached Compute shim")
	}
}

func TestServeHTTPRoutesLocalComputeRequest(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/compute/v1/projects/demo/zones/us-central1-a/instances", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestServeHTTPValidatesPathMappedRequest(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("compute.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("handler was called for an invalid request")
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(
		http.MethodPost,
		"http://localhost:8080/compute/v1/projects/demo/zones/us-central1-a/instances",
		strings.NewReader(`{}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestServeHTTPFlattensFirebaseSubdomain(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("firebaseio.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "https://demo.firebaseio.com/users.json", nil)
	req.Host = "demo.firebaseio.com"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestServeHTTPReturnsGCPErrorForUnknownDomain(t *testing.T) {
	t.Parallel()

	router := NewProxyRouterWithManager(nil)
	req := httptest.NewRequest(http.MethodGet, "https://unknown.googleapis.com/v1/resources", nil)
	req.Host = "unknown.googleapis.com"
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
	if contentType := rec.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
}
