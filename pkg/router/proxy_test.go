package router

import (
	"bytes"
	"compress/gzip"
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
	"minisky/pkg/evidence"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	localsecurity "minisky/pkg/security"
	_ "minisky/pkg/shims"
	"minisky/pkg/shims/aiplatform"
	"minisky/pkg/shims/appengine"
	"minisky/pkg/shims/bigquery"
	"minisky/pkg/shims/bigtable"
	"minisky/pkg/shims/cloudsql"
	"minisky/pkg/shims/compute"
	"minisky/pkg/shims/serverless"
	"minisky/pkg/state"
)

func gzipJSONBody(t *testing.T, body string) *bytes.Reader {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(compressed.Bytes())
}

func TestRouterDecodesBoundedGzipJSONBeforeValidationAndDispatch(t *testing.T) {
	const body = `{"name":"java-smoke"}`
	var received []byte
	var contentEncoding string
	var contentLength int64

	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("storage.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var err error
		received, err = io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		contentEncoding = request.Header.Get("Content-Encoding")
		contentLength = request.ContentLength
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/storage/storage/v1/b?project=demo",
		gzipJSONBody(t, body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Encoding", "gzip")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if string(received) != body {
		t.Fatalf("downstream body=%q, want %q", received, body)
	}
	if contentEncoding != "" {
		t.Fatalf("downstream Content-Encoding=%q, want removed after decoding", contentEncoding)
	}
	if contentLength != int64(len(body)) {
		t.Fatalf("downstream Content-Length=%d, want %d", contentLength, len(body))
	}
}

func TestRouterRejectsGzipJSONWhoseDecodedBodyExceedsRouteLimit(t *testing.T) {
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("storage.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized decoded body reached shim")
	}))
	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/storage/storage/v1/b?project=demo",
		gzipJSONBody(t, `{"name":"`+strings.Repeat("x", (1<<20)+1)+`"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Content-Encoding", "gzip")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge ||
		!strings.Contains(response.Body.String(), `"INVALID_ARGUMENT"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

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

func TestExperimentalGatePrecedesStrictIAMAndValidation(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "experimental-strict-gate")
	t.Setenv(registry.ExperimentalServicesEnv, "")
	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim(
		"batch.googleapis.com",
		registry.RuntimeHandler("batch.googleapis.com", handlers["batch.googleapis.com"], false),
	)
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: false}, nil, false, "gateway")

	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/batch/v1/projects/demo/locations/us/jobs",
		strings.NewReader(`{}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotImplemented ||
		!strings.Contains(response.Body.String(), `"status":"UNIMPLEMENTED"`) ||
		!strings.Contains(response.Body.String(), registry.ExperimentalServicesEnv+"=1") ||
		!strings.Contains(response.Body.String(), "promotion evidence") {
		t.Fatalf("experimental strict response=%d body=%s", response.Code, response.Body.String())
	}

	t.Setenv(registry.ExperimentalServicesEnv, "1")
	handlers, _ = registry.BootAll(orchestrator.NewOperationManager(), nil)
	router = NewProxyRouterWithManager(nil)
	router.RegisterShim(
		"batch.googleapis.com",
		registry.RuntimeHandler("batch.googleapis.com", handlers["batch.googleapis.com"], false),
	)
	router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: false}, nil, false, "gateway")
	request = httptest.NewRequest(
		http.MethodGet,
		"http://localhost/_minisky/batch/v1/projects/demo/locations/us/jobs",
		nil,
	)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("opted-in strict response=%d, want normal 401 policy; body=%s",
			response.Code, response.Body.String())
	}
}

func TestEveryExperimentalDomainPublicGatewayEvidence(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "phase18-25-public-gateway-evidence")
	inventory, err := evidence.Phase18To25()
	if err != nil {
		t.Fatal(err)
	}

	newRouter := func(handlers map[string]http.Handler) *ProxyRouter {
		proxy := NewProxyRouterWithManager(nil)
		for _, entry := range inventory {
			handler := handlers[entry.Domain]
			if handler == nil {
				t.Fatalf("missing runtime handler for %s", entry.Domain)
			}
			proxy.RegisterShim(entry.Domain, registry.RuntimeHandler(entry.Domain, handler, false))
		}
		return proxy
	}

	t.Setenv(registry.ExperimentalServicesEnv, "")
	disabledHandlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	disabled := newRouter(disabledHandlers)
	for _, entry := range inventory {
		entry := entry
		t.Run(entry.Domain+"/default-off", func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodGet,
				"http://127.0.0.1/_minisky/"+entry.Selector+registry.UnsupportedContractPath,
				nil,
			)
			response := httptest.NewRecorder()
			disabled.ServeHTTP(response, request)
			if response.Code != http.StatusNotImplemented ||
				!strings.Contains(response.Body.String(), `"status":"UNIMPLEMENTED"`) ||
				!strings.Contains(response.Body.String(), registry.ExperimentalServicesEnv+"=1") {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
		t.Run(entry.Domain+"/gate-before-validation", func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPost,
				"http://127.0.0.1/_minisky/"+entry.Selector+registry.UnsupportedContractPath,
				strings.NewReader(`{}`),
			)
			request.Header.Set("Content-Type", "application/json")
			request.ContentLength = 1 << 30
			response := httptest.NewRecorder()
			disabled.ServeHTTP(response, request)
			if response.Code != http.StatusNotImplemented {
				t.Fatalf("status=%d, want default-off 501 before body validation; body=%s",
					response.Code, response.Body.String())
			}
		})
	}

	t.Setenv(registry.ExperimentalServicesEnv, "1")
	enabledHandlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	enabled := newRouter(enabledHandlers)
	for _, entry := range inventory {
		entry := entry
		t.Run(entry.Domain+"/opt-in-dispatch", func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodGet,
				"http://127.0.0.1/_minisky/"+entry.Selector+registry.UnsupportedContractPath,
				nil,
			)
			response := httptest.NewRecorder()
			enabled.ServeHTTP(response, request)
			if response.Code != http.StatusNotImplemented ||
				!strings.Contains(response.Body.String(), "unsupported route for "+entry.Domain) ||
				strings.Contains(response.Body.String(), "experimental and disabled") {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
		t.Run(entry.Domain+"/validation-before-auth", func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPost,
				"http://127.0.0.1/_minisky/"+entry.Selector+registry.UnsupportedContractPath,
				strings.NewReader(`{}`),
			)
			request.Header.Set("Content-Type", "application/json")
			request.ContentLength = 1 << 30
			response := httptest.NewRecorder()
			enabled.ServeHTTP(response, request)
			if response.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status=%d, want 413 before dispatch; body=%s", response.Code, response.Body.String())
			}
		})
	}

	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:evidence@example.com", Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range inventory {
		if entry.IAMPath == "" {
			continue
		}
		entry := entry
		method := entry.IAMMethod
		if method == "" {
			method = http.MethodGet
		}
		iamRequest := httptest.NewRequest(method, entry.IAMPath, strings.NewReader(entry.IAMBody))
		if entry.IAMProject != "" {
			iamRequest.Header.Set("X-Goog-User-Project", entry.IAMProject)
		}
		permission, resource := routePermission(entry.Domain, iamRequest)
		if permission == "" || resource != "projects/demo" {
			t.Errorf("%s strict-IAM evidence route = (%q, %q)", entry.Domain, permission, resource)
			continue
		}
		for _, allow := range []bool{false, true} {
			allow := allow
			t.Run(fmt.Sprintf("%s/strict-iam-allow-%t", entry.Domain, allow), func(t *testing.T) {
				proxy := newRouter(enabledHandlers)
				proxy.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: allow}, nil, false, "gateway")
				request := httptest.NewRequest(
					method,
					"http://127.0.0.1/_minisky/"+entry.Selector+entry.IAMPath,
					strings.NewReader(entry.IAMBody),
				)
				if entry.IAMBody != "" {
					request.Header.Set("Content-Type", "application/json")
				}
				if entry.IAMProject != "" {
					request.Header.Set("X-Goog-User-Project", entry.IAMProject)
				}
				request.Header.Set("Authorization", "Bearer "+token)
				response := httptest.NewRecorder()
				proxy.ServeHTTP(response, request)
				if !allow && response.Code != http.StatusForbidden {
					t.Fatalf("deny status=%d, want 403; body=%s", response.Code, response.Body.String())
				}
				if allow && (response.Code == http.StatusUnauthorized || response.Code == http.StatusForbidden ||
					strings.Contains(response.Body.String(), "experimental and disabled")) {
					t.Fatalf("allowed request did not reach opted-in service: status=%d body=%s",
						response.Code, response.Body.String())
				}
			})
		}
	}
}

func TestAIPlatformExperimentalControlPlaneGatePrecedesStrictIAMAndValidation(t *testing.T) {
	t.Setenv(registry.ExperimentalServicesEnv, "")
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("aiplatform.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: false}, nil, false, "gateway")

	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/aiplatform/v1/projects/demo/locations/us/indexes",
		strings.NewReader(`{}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNotImplemented ||
		!strings.Contains(response.Body.String(), registry.ExperimentalServicesEnv+"=1") {
		t.Fatalf("control-plane gate status=%d body=%s", response.Code, response.Body.String())
	}

	predictionRequest := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/_minisky/aiplatform/v1/projects/demo/locations/us/endpoints/e:predict",
		strings.NewReader(`{"instances":[{}]}`),
	)
	predictionRequest.Header.Set("Content-Type", "application/json")
	prediction := httptest.NewRecorder()
	router.ServeHTTP(prediction, predictionRequest)
	if prediction.Code != http.StatusUnauthorized {
		t.Fatalf("existing prediction status=%d, want normal strict-IAM 401; body=%s",
			prediction.Code, prediction.Body.String())
	}
}

func TestExperimentalCanonicalRoutingDescriptors(t *testing.T) {
	t.Setenv(registry.ExperimentalServicesEnv, "1")

	for name, test := range map[string]struct {
		domain string
		method string
		path   string
		body   string
	}{
		"managed kafka cluster": {
			domain: "managedkafka.googleapis.com",
			method: http.MethodGet,
			path:   "/_minisky/managedkafka/v1/projects/demo/locations/us/clusters",
		},
		"workflow executions host": {
			domain: "workflowexecutions.googleapis.com",
			method: http.MethodGet,
			path:   "/_minisky/workflowexecutions/v1/projects/demo/locations/us/workflows/flow/executions",
		},
		"identity platform domain": {
			domain: "identityplatform.googleapis.com",
			method: http.MethodGet,
			path:   "/_minisky/identityplatform/v2/projects/demo/tenants",
		},
		"service control alias": {
			domain: "servicecontrol.googleapis.com",
			method: http.MethodPost,
			path:   "/_minisky/servicecontrol/v1/services/example.test:check",
		},
		"dialogflow cx": {
			domain: "dialogflow.googleapis.com",
			method: http.MethodGet,
			path:   "/_minisky/dialogflow/v3/projects/demo/locations/us/agents",
		},
		"text to speech": {
			domain: "texttospeech.googleapis.com",
			method: http.MethodPost,
			path:   "/_minisky/texttospeech/v1/text:synthesize",
		},
		"aiplatform control plane": {
			domain: "aiplatform.googleapis.com",
			method: http.MethodGet,
			path:   "/_minisky/aiplatform/v1/projects/demo/locations/us/indexes",
		},
		"binary authorization": {
			domain: "binaryauthorization.googleapis.com",
			method: http.MethodGet,
			path:   "/_minisky/binaryauthorization/v1/projects/demo/policy",
		},
		"natural language": {
			domain: "language.googleapis.com",
			method: http.MethodPost,
			path:   "/_minisky/language/v1/documents:analyzeSentiment",
		},
		"private ca": {
			domain: "privateca.googleapis.com",
			method: http.MethodPost,
			path:   "/_minisky/privateca/v1/projects/demo/locations/us/caPools/pool/certificates",
			body:   `{"certificateId":"cert","pemCsr":"pem","lifetime":"1h"}`,
		},
		"pubsub lite admin": {
			domain: "pubsublite.googleapis.com",
			method: http.MethodGet,
			path:   "/_minisky/pubsublite/v1/admin/projects/demo/locations/us/topics",
		},
		"service management alias": {
			domain: "servicemanagement.googleapis.com",
			method: http.MethodPost,
			path:   "/_minisky/servicemanagement/v1/services/example.test/configs",
		},
		"speech to text": {
			domain: "speech.googleapis.com",
			method: http.MethodPost,
			path:   "/_minisky/speech/v1/speech:recognize",
		},
	} {
		t.Run(name, func(t *testing.T) {
			router := NewProxyRouterWithManager(nil)
			router.RegisterShim(test.domain, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			var body io.Reader
			if test.body != "" {
				body = strings.NewReader(test.body)
			}
			request := httptest.NewRequest(test.method, "http://localhost"+test.path, body)
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusNoContent {
				t.Fatalf("route response=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestAmbiguousBareExperimentalRoutesDoNotMisroute(t *testing.T) {
	t.Parallel()

	fallback := "localhost"
	for _, path := range []string{
		"/v1/projects/demo/locations/us/clusters",
		"/v1/projects/demo/locations/us/operations/op",
		"/v1/admin/projects/demo/locations/us/topics",
		"/v1/projects/demo/locations/us/topics/topic:publish",
		"/v1/projects/demo/locations/us/indexes",
		"/v3/projects/demo/locations/us/agents",
		"/v1/projects/demo/locations/us/caPools/pool/certificates",
	} {
		if domain := legacyLocalDomain(path, fallback); domain != fallback {
			t.Fatalf("legacyLocalDomain(%q) = %q, want explicit unresolved fallback", path, domain)
		}
	}
}

func TestProjectlessVisionAnnotateRequiresExplicitProjectHeader(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/v1/images:annotate", strings.NewReader(`{"requests":[]}`))
	if project := ProjectFromRequest(request); project != "" {
		t.Fatalf("project without header = %q", project)
	}
	request.Header.Set("X-Goog-User-Project", "billing-project")
	if project := ProjectFromRequest(request); project != "billing-project" {
		t.Fatalf("project with header = %q", project)
	}
	permission, resource := routePermission("vision.googleapis.com", request)
	if permission != "vision.images.annotate" || resource != "projects/billing-project" {
		t.Fatalf("route permission = (%q, %q)", permission, resource)
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
			if test.wantComplete != "" && !strings.Contains(response.Body.String(), "bigquery.datasets.create") {
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

func TestStrictAuthorizationDefaultDeniesEveryUnmappedRoute(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:admin@example.com", Audience: "gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			dispatched := false
			router := NewProxyRouterWithManager(nil)
			router.RegisterShim("unmapped.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				dispatched = true
				w.WriteHeader(http.StatusNoContent)
			}))
			router.ConfigureSecurity(testAuthorizer{issuer: issuer, allow: true}, nil, false, "gateway")
			request := httptest.NewRequest(
				method,
				"http://localhost/_minisky/unmapped/v1/projects/demo/resources",
				bytes.NewBufferString(`{"name":"denied"}`),
			)
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden ||
				!strings.Contains(response.Body.String(), `"PERMISSION_DENIED"`) ||
				!strings.Contains(response.Body.String(), "unmapped route") {
				t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
			}
			if dispatched {
				t.Fatal("unmapped strict route reached shim")
			}
		})
	}
}

func TestPermissiveDevelopmentModeDispatchesUnmappedRoute(t *testing.T) {
	dispatched := false
	router := NewProxyRouterWithManager(nil)
	router.RegisterShim("unmapped.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dispatched = true
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(
		http.MethodGet,
		"http://localhost/_minisky/unmapped/v1/projects/demo/resources",
		nil,
	)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !dispatched {
		t.Fatalf("permissive response=%d dispatched=%v", response.Code, dispatched)
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

func TestDockerDegradedPurePassthroughShimsReturnCanonicalUnavailable(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "docker-degraded-router")
	shims, lazyDomains := registry.BootAll(orchestrator.NewOperationManager(), nil)
	proxy := NewProxyRouterWithManager(nil)
	for domain, handler := range shims {
		proxy.RegisterShim(domain, registry.RuntimeHandler(domain, handler, false))
	}
	for _, domain := range lazyDomains {
		proxy.RegisterLazyDocker(domain)
	}

	var canonicalBody string
	for _, domain := range []string{
		"storage.googleapis.com",
		"pubsub.googleapis.com",
		"identitytoolkit.googleapis.com",
		"firebasehosting.googleapis.com",
		"project.firebaseio.com",
		"firestore.googleapis.com",
	} {
		request := httptest.NewRequest(http.MethodGet, "https://"+domain+"/", nil)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status=%d body=%s", domain, response.Code, response.Body.String())
		}
		if !strings.Contains(response.Body.String(), `"code":503`) ||
			!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
			t.Fatalf("%s body=%s", domain, response.Body.String())
		}
		if canonicalBody == "" {
			canonicalBody = response.Body.String()
		} else if response.Body.String() != canonicalBody {
			t.Fatalf("%s returned non-canonical body %q, want %q", domain, response.Body.String(), canonicalBody)
		}
	}
}

func TestDockerDegradedHybridShimsPreserveControlPlaneAndGateMutations(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "docker-degraded-hybrid")
	shims, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	if _, ok := shims["compute.googleapis.com"].(*compute.API); !ok {
		t.Fatalf("Compute factory was replaced by %T", shims["compute.googleapis.com"])
	}
	if _, ok := shims["sqladmin.googleapis.com"].(*cloudsql.API); !ok {
		t.Fatalf("Cloud SQL factory was replaced by %T", shims["sqladmin.googleapis.com"])
	}
	if _, ok := shims["appengine.googleapis.com"].(*appengine.API); !ok {
		t.Fatalf("App Engine factory was replaced by %T", shims["appengine.googleapis.com"])
	}
	if _, ok := shims["bigtable.googleapis.com"].(*bigtable.API); !ok {
		t.Fatalf("Bigtable factory was replaced by %T", shims["bigtable.googleapis.com"])
	}
	if _, ok := shims["aiplatform.googleapis.com"].(*aiplatform.Handler); !ok {
		t.Fatalf("merged AI Platform factory has type %T", shims["aiplatform.googleapis.com"])
	}
	if _, ok := shims["cloudfunctions.googleapis.com"].(*serverless.API); !ok {
		t.Fatalf("Serverless factory was replaced by %T", shims["cloudfunctions.googleapis.com"])
	}

	proxy := NewProxyRouterWithManager(nil)
	for domain, handler := range shims {
		proxy.RegisterShim(domain, registry.RuntimeHandler(domain, handler, false))
	}
	for _, test := range []struct {
		domain string
		path   string
	}{
		{"compute.googleapis.com", "/compute/v1/projects/project-a/zones/us-central1-a/instances"},
		{"sqladmin.googleapis.com", "/sql/v1beta4/projects/project-a/instances"},
		{"appengine.googleapis.com", "/v1/apps/project-a"},
		{"bigtable.googleapis.com", "/v2/projects/project-a/instances"},
		{"aiplatform.googleapis.com", "/v1/projects/project-a/locations/us-central1/endpoints"},
	} {
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "https://"+test.domain+test.path, nil))
		if response.Code == http.StatusServiceUnavailable || response.Code >= http.StatusInternalServerError {
			t.Fatalf("%s metadata status=%d body=%s", test.domain, response.Code, response.Body.String())
		}
	}

	createBody := `{
		"name":"web",
		"machineType":"e2-micro",
		"disks":[{"boot":true,"autoDelete":true,"initializeParams":{"sourceImage":"projects/debian-cloud/global/images/debian-12"}}],
		"networkInterfaces":[{"subnetwork":"primary"}]
	}`
	request := httptest.NewRequest(http.MethodPost,
		"https://compute.googleapis.com/compute/v1/projects/project-a/zones/us-central1-a/instances",
		strings.NewReader(createBody))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
		t.Fatalf("Compute backend mutation status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost,
		"https://compute.googleapis.com/compute/v1/projects/project-a/global/networks",
		strings.NewReader(`{"name":"control-plane","autoCreateSubnetworks":false}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("Compute local network mutation status=%d body=%s", response.Code, response.Body.String())
	}

	for _, test := range []struct {
		method string
		path   string
		body   string
	}{
		{
			http.MethodPost,
			"/compute/v1/projects/project-a/global/firewalls",
			`{"name":"allow-http","network":"projects/project-a/global/networks/control-plane","allowed":[{"IPProtocol":"tcp","ports":["80"]}]}`,
		},
		{http.MethodPatch, "/compute/v1/projects/project-a/global/firewalls/allow-http", `{}`},
		{http.MethodPut, "/compute/v1/projects/project-a/global/firewalls/allow-http", `{}`},
		{http.MethodDelete, "/compute/v1/projects/project-a/global/firewalls/allow-http", ""},
		{http.MethodDelete, "/compute/v1/projects/project-a/global/networks/control-plane", ""},
		{
			http.MethodPost,
			"/compute/v1/projects/project-a/regions/us-central1/subnetworks",
			`{"name":"subnet-a","ipCidrRange":"10.10.0.0/24","network":"projects/project-a/global/networks/control-plane"}`,
		},
		{http.MethodDelete, "/compute/v1/projects/project-a/regions/us-central1/subnetworks/subnet-a", ""},
	} {
		request = httptest.NewRequest(test.method, "https://compute.googleapis.com"+test.path,
			strings.NewReader(test.body))
		request.Header.Set("Content-Type", "application/json")
		response = httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable ||
			!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) ||
			!strings.Contains(response.Body.String(), `"message":"MiniSky: Docker backend unavailable"`) {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, response.Code, response.Body.String())
		}
	}

	for _, path := range []string{
		"/compute/v1/projects/project-a/global/networks",
		"/compute/v1/projects/project-a/global/networks/control-plane",
	} {
		response = httptest.NewRecorder()
		proxy.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
			"https://compute.googleapis.com"+path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}

	request = httptest.NewRequest(http.MethodPost,
		"https://compute.googleapis.com/compute/v1/projects/project-a/zones/us-central1-a/instances/missing/start",
		nil)
	response = httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code == http.StatusServiceUnavailable {
		t.Fatalf("local instance action was Docker-gated: body=%s", response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost,
		"https://cloudbuild.googleapis.com/v1/projects/project-a/triggers", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusNotImplemented ||
		!strings.Contains(response.Body.String(), `"status":"UNIMPLEMENTED"`) {
		t.Fatalf("Cloud Build local trigger status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDockerDegradedVertexPostsRemainLocallyAvailable(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "docker-degraded-vertex")
	shims, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	vertex, ok := shims["aiplatform.googleapis.com"].(*aiplatform.Handler)
	if !ok {
		t.Fatalf("merged AI Platform factory has type %T", shims["aiplatform.googleapis.com"])
	}
	proxy := NewProxyRouterWithManager(nil)
	proxy.RegisterShim("aiplatform.googleapis.com",
		registry.RuntimeHandler("aiplatform.googleapis.com", vertex, false))

	post := func(path, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "https://aiplatform.googleapis.com"+path,
			strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		return response
	}

	predictPath := "/v1/projects/project-a/locations/us-central1/endpoints/test:predict"
	predictBody := `{"instances":[{"feature":2},{"feature":1}],"parameters":{"temperature":0}}`
	first := post(predictPath, predictBody)
	second := post(predictPath, predictBody)
	if first.Code != http.StatusOK || first.Body.String() != second.Body.String() ||
		!strings.Contains(first.Body.String(), `"predictions"`) {
		t.Fatalf("deterministic prediction status=%d/%d bodies=%s / %s",
			first.Code, second.Code, first.Body.String(), second.Body.String())
	}

	generatePath := "/v1/projects/project-a/locations/us-central1/publishers/google/models/gemini:generateContent"
	generateBody := `{"contents":[{"role":"user","parts":[{"text":"hello local model"}]}]}`
	generate := post(generatePath, generateBody)
	if generate.Code != http.StatusOK || !strings.Contains(generate.Body.String(), "hello local model") {
		t.Fatalf("mock generation status=%d body=%s", generate.Code, generate.Body.String())
	}

	configureMock := post("/v1/internal/config",
		`{"provider":"mock","model":"gemini-test","mockResponse":"configured response"}`)
	if configureMock.Code != http.StatusOK {
		t.Fatalf("mock config status=%d body=%s", configureMock.Code, configureMock.Body.String())
	}
	configured := post(generatePath, generateBody)
	if configured.Code != http.StatusOK || !strings.Contains(configured.Body.String(), "configured response") {
		t.Fatalf("configured mock status=%d body=%s", configured.Code, configured.Body.String())
	}

	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Errorf("Ollama path=%s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message": map[string]string{"role": "assistant", "content": "loopback response"},
		})
	}))
	defer ollama.Close()
	configureOllama := post("/v1/internal/config",
		`{"provider":"ollama","endpoint":"`+ollama.URL+`","model":"test"}`)
	if configureOllama.Code != http.StatusOK {
		t.Fatalf("Ollama config status=%d body=%s", configureOllama.Code, configureOllama.Body.String())
	}
	ollamaGenerate := post(generatePath, generateBody)
	if ollamaGenerate.Code != http.StatusOK ||
		!strings.Contains(ollamaGenerate.Body.String(), "loopback response") {
		t.Fatalf("Ollama generation status=%d body=%s", ollamaGenerate.Code, ollamaGenerate.Body.String())
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

func TestManifestImplementedDomainsHaveStrictIAMMappings(t *testing.T) {
	type routeCase struct {
		method, path, permission string
	}
	routes := map[string][]routeCase{
		"accesscontextmanager.googleapis.com": {{http.MethodGet, "/v1/accessPolicies", "accesscontextmanager.accessPolicies.list"}},
		"aiplatform.googleapis.com":           {{http.MethodPost, "/v1/projects/demo/locations/us/endpoints", "aiplatform.endpoints.create"}},
		"alloydb.googleapis.com":              {{http.MethodPost, "/v1/projects/demo/locations/us/clusters", "alloydb.clusters.create"}},
		"apigateway.googleapis.com":           {{http.MethodGet, "/v1/projects/demo/locations/us/gateways", "apigateway.gateways.list"}},
		"appengine.googleapis.com":            {{http.MethodGet, "/v1/projects/demo/apps/app/services", "appengine.services.list"}},
		"artifactregistry.googleapis.com":     {{http.MethodPatch, "/v1/projects/demo/locations/us/repositories/repo", "artifactregistry.repositories.update"}},
		"batch.googleapis.com":                {{http.MethodPost, "/v1/projects/demo/locations/us/jobs", "batch.jobs.create"}},
		"bigquery.googleapis.com":             {{http.MethodDelete, "/bigquery/v2/projects/demo/datasets/data", "bigquery.datasets.delete"}},
		"bigtable.googleapis.com":             {{http.MethodGet, "/v2/projects/demo/instances/i/tables/t", "bigtable.tables.get"}},
		"bigtableadmin.googleapis.com":        {{http.MethodGet, "/v2/projects/demo/instances", "bigtable.instances.list"}},
		"cloudasset.googleapis.com":           {{http.MethodGet, "/v1/projects/demo/assets", "cloudasset.assets.list"}},
		"cloudbuild.googleapis.com":           {{http.MethodPost, "/v1/projects/demo/builds", "cloudbuild.builds.create"}},
		"clouddeploy.googleapis.com":          {{http.MethodPost, "/v1/projects/demo/locations/us/deliveryPipelines", "clouddeploy.deliveryPipelines.create"}},
		"clouderrorreporting.googleapis.com":  {{http.MethodGet, "/v1beta1/projects/demo/events", "clouderrorreporting.errorEvents.list"}},
		"cloudfunctions.googleapis.com":       {{http.MethodDelete, "/v2/projects/demo/locations/us/functions/f", "cloudfunctions.functions.delete"}},
		"cloudkms.googleapis.com":             {{http.MethodPost, "/v1/projects/demo/locations/us/keyRings/r/cryptoKeys", "cloudkms.cryptoKeys.create"}},
		"cloudprofiler.googleapis.com":        {{http.MethodPost, "/v2/projects/demo/profiles", "cloudprofiler.profiles.create"}},
		"cloudresourcemanager.googleapis.com": {{http.MethodGet, "/v3/projects", "resourcemanager.projects.list"}},
		"cloudscheduler.googleapis.com":       {{http.MethodPost, "/v1/projects/demo/locations/us/jobs/j:run", "cloudscheduler.jobs.run"}},
		"cloudtasks.googleapis.com":           {{http.MethodDelete, "/v2/projects/demo/locations/us/queues/q/tasks/t", "cloudtasks.tasks.delete"}},
		"cloudtrace.googleapis.com":           {{http.MethodGet, "/v2/projects/demo/traces", "cloudtrace.traces.list"}},
		"composer.googleapis.com":             {{http.MethodPost, "/v1/projects/demo/locations/us/environments", "composer.environments.create"}},
		"compute.googleapis.com":              {{http.MethodPost, "/compute/v1/projects/demo/zones/us/instances", "compute.instances.create"}},
		"container.googleapis.com":            {{http.MethodDelete, "/v1/projects/demo/locations/us/clusters/c", "container.clusters.delete"}},
		"dataflow.googleapis.com":             {{http.MethodGet, "/v1b3/projects/demo/locations/us/jobs", "dataflow.jobs.list"}},
		"dataform.googleapis.com":             {{http.MethodPost, "/v1beta1/projects/demo/locations/us/repositories", "dataform.repositories.create"}},
		"dataproc.googleapis.com":             {{http.MethodPost, "/v1/projects/demo/regions/us/jobs:submit", "dataproc.jobs.submit"}},
		"datastore.googleapis.com":            {{http.MethodPost, "/v1/projects/demo:runQuery", "datastore.entities.list"}},
		"dlp.googleapis.com":                  {{http.MethodGet, "/v2/projects/demo/inspectTemplates", "dlp.inspectTemplates.list"}},
		"dns.googleapis.com":                  {{http.MethodPost, "/dns/v1/projects/demo/managedZones", "dns.managedZones.create"}},
		"documentai.googleapis.com":           {{http.MethodGet, "/v1/projects/demo/locations/us/processors", "documentai.processors.list"}},
		"eventarc.googleapis.com":             {{http.MethodPost, "/v1/projects/demo/locations/us/triggers", "eventarc.triggers.create"}},
		"file.googleapis.com":                 {{http.MethodGet, "/v1/projects/demo/locations/us/instances", "file.instances.list"}},
		"firebasehosting.googleapis.com":      {{http.MethodGet, "/v1beta1/projects/demo/sites", "firebasehosting.sites.list"}},
		"firebaseio.com":                      {{http.MethodPatch, "/projects/demo.json", "firebasedatabase.instances.update"}},
		"firestore.googleapis.com":            {{http.MethodGet, "/v1/projects/demo/databases/(default)/documents", "datastore.entities.list"}},
		"iam.googleapis.com":                  {{http.MethodGet, "/v1/projects/demo/serviceAccounts", "iam.serviceAccounts.list"}},
		"iamcredentials.googleapis.com":       {{http.MethodPost, "/v1/projects/-/serviceAccounts/a@example.test:generateAccessToken", "iam.serviceAccounts.getAccessToken"}},
		"identityplatform.googleapis.com":     {{http.MethodGet, "/v2/projects/demo/tenants", "identityplatform.tenants.list"}},
		"identitytoolkit.googleapis.com":      {{http.MethodPost, "/v1/accounts:lookup", "firebaseauth.users.get"}},
		"logging.googleapis.com":              {{http.MethodPost, "/v2/entries:list", "logging.logEntries.list"}},
		"managedkafka.googleapis.com":         {{http.MethodPost, "/v1/projects/demo/locations/us/clusters", "managedkafka.clusters.create"}},
		"networkservices.googleapis.com":      {{http.MethodGet, "/v1/projects/demo/locations/us/meshes", "networkservices.meshes.list"}},
		"metadata.google.internal":            {{http.MethodGet, "/computeMetadata/v1/instance/id", "compute.instances.get"}},
		"monitoring.googleapis.com":           {{http.MethodPost, "/v3/projects/demo/timeSeries", "monitoring.timeSeries.create"}},
		"networksecurity.googleapis.com":      {{http.MethodPost, "/v1/projects/demo/locations/us/authorizationPolicies", "networksecurity.authorizationPolicies.create"}},
		"orgpolicy.googleapis.com":            {{http.MethodGet, "/v2/projects/demo/policies", "orgpolicy.policies.list"}},
		"pubsub.googleapis.com":               {{http.MethodPost, "/v1/projects/demo/topics/t:publish", "pubsub.topics.publish"}},
		"redis.googleapis.com":                {{http.MethodGet, "/v1/projects/demo/locations/us/instances", "redis.instances.list"}},
		"run.googleapis.com":                  {{http.MethodPost, "/v2/projects/demo/locations/us/services", "run.services.create"}},
		"secretmanager.googleapis.com":        {{http.MethodGet, "/v1/projects/demo/secrets/s/versions/latest:access", "secretmanager.versions.access"}},
		"servicedirectory.googleapis.com":     {{http.MethodGet, "/v1/projects/demo/locations/us/namespaces", "servicedirectory.namespaces.list"}},
		"spanner.googleapis.com":              {{http.MethodPost, "/v1/projects/demo/instances/i/databases/d/sessions", "spanner.sessions.create"}},
		"sqladmin.googleapis.com":             {{http.MethodDelete, "/sql/v1beta4/projects/demo/instances/db", "cloudsql.instances.delete"}},
		"storage.googleapis.com":              {{http.MethodGet, "/storage/v1/b", "storage.buckets.list"}},
		"storagetransfer.googleapis.com":      {{http.MethodGet, "/v1/transferJobs", "storagetransfer.transferJobs.list"}},
		"sts.googleapis.com":                  {{http.MethodPost, "/v1/token", "iam.serviceAccounts.getAccessToken"}},
		"translate.googleapis.com":            {{http.MethodGet, "/v3/projects/demo/locations/us/glossaries", "translate.glossaries.list"}},
		"vision.googleapis.com":               {{http.MethodPost, "/v1/images:annotate", "vision.images.annotate"}},
		"workflows.googleapis.com":            {{http.MethodPost, "/v1/projects/demo/locations/us/workflows", "workflows.workflows.create"}},
		"workflowexecutions.googleapis.com":   {{http.MethodPost, "/v1/projects/demo/locations/us/workflows/flow/executions", "workflows.executions.create"}},
	}
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range services {
		if service.Support != registry.SupportImplemented {
			continue
		}
		cases := routes[service.Domain]
		if len(cases) == 0 {
			t.Errorf("implemented domain %s has no route-specific IAM case", service.Domain)
			continue
		}
		t.Run(service.Domain, func(t *testing.T) {
			for _, test := range cases {
				request := httptest.NewRequest(test.method, test.path, nil)
				permission, resource := routePermission(service.Domain, request)
				if permission != test.permission {
					t.Errorf("%s %s permission = %q, want %q", test.method, test.path, permission, test.permission)
				}
				if strings.Contains(test.path, "/projects/demo") {
					wantResource := "projects/demo"
					if service.Domain == "pubsub.googleapis.com" {
						wantResource = "projects/demo/topics/t"
					}
					if resource != wantResource {
						t.Errorf("%s %s resource = %q, want %q", test.method, test.path, resource, wantResource)
					}
				}
			}
		})
	}
}

func TestNewExperimentalDomainsHaveStrictIAMDispatch(t *testing.T) {
	tests := []struct {
		domain, method, path, permission string
	}{
		{"aiplatform.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/us/indexes", "aiplatform.indexes.create"},
		{"binaryauthorization.googleapis.com", http.MethodPut, "/v1/projects/demo/policy", "binaryauthorization.policy.update"},
		{"dialogflow.googleapis.com", http.MethodPost, "/v3/projects/demo/locations/us/agents", "dialogflow.agents.create"},
		{"language.googleapis.com", http.MethodPost, "/v1/documents:analyzeSentiment", "language.documents.analyzeSentiment"},
		{"privateca.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/us/caPools/pool/certificates", "privateca.certificates.create"},
		{"pubsublite.googleapis.com", http.MethodGet, "/v1/admin/projects/demo/locations/us/topics", "pubsublite.topics.list"},
		{"servicecontrol.googleapis.com", http.MethodPost, "/v1/services/example.test:check", "servicecontrol.services.check"},
		{"servicemanagement.googleapis.com", http.MethodPost, "/v1/services/example.test/configs", "servicemanagement.services.update"},
		{"speech.googleapis.com", http.MethodPost, "/v1/speech:recognize", "speech.recognizers.recognize"},
		{"texttospeech.googleapis.com", http.MethodPost, "/v1/text:synthesize", "texttospeech.synthesizers.synthesize"},
	}
	for _, test := range tests {
		t.Run(test.domain, func(t *testing.T) {
			permission, resource := routePermission(
				test.domain,
				httptest.NewRequest(test.method, test.path, nil),
			)
			if permission != test.permission {
				t.Fatalf("permission=%q, want %q", permission, test.permission)
			}
			if strings.Contains(test.path, "/projects/demo") && resource != "projects/demo" {
				t.Fatalf("resource=%q, want projects/demo", resource)
			}
		})
	}
}

func TestManifestImplementedDomainsHaveExplicitStrictDispatchPolicy(t *testing.T) {
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	customDomains := make(map[string]bool)
	for _, route := range strictIAMCustomRoutes {
		customDomains[route.domain] = true
	}
	for _, service := range services {
		if service.Support != registry.SupportImplemented {
			continue
		}
		explicit := len(strictIAMResourceRoutes[service.Domain]) > 0 || customDomains[service.Domain]
		switch service.Domain {
		case "firebaseio.com", "sts.googleapis.com":
			explicit = true
		}
		if !explicit {
			t.Errorf("implemented domain %s has no explicit strict dispatch policy", service.Domain)
		}
	}

	if !strictIAMPublicExemption("sts.googleapis.com", httptest.NewRequest(http.MethodPost, "/v1/token", nil)) {
		t.Fatal("STS token exchange lost its explicit public exemption")
	}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/token", nil),
		httptest.NewRequest(http.MethodPost, "/v1/other", nil),
	} {
		if strictIAMPublicExemption("sts.googleapis.com", request) {
			t.Fatalf("unexpected public exemption for %s %s", request.Method, request.URL.Path)
		}
	}
}

func TestStrictIAMRouteClassificationSemanticsAndDenials(t *testing.T) {
	tests := []struct {
		name, domain, method, path, permission string
	}{
		{"list", "cloudtasks.googleapis.com", http.MethodGet, "/v2/projects/demo/locations/us/queues", "cloudtasks.queues.list"},
		{"get", "cloudtasks.googleapis.com", http.MethodGet, "/v2/projects/demo/locations/us/queues/q", "cloudtasks.queues.get"},
		{"create", "cloudtasks.googleapis.com", http.MethodPost, "/v2/projects/demo/locations/us/queues", "cloudtasks.queues.create"},
		{"update", "cloudtasks.googleapis.com", http.MethodPatch, "/v2/projects/demo/locations/us/queues/q", "cloudtasks.queues.update"},
		{"delete", "cloudtasks.googleapis.com", http.MethodDelete, "/v2/projects/demo/locations/us/queues/q", "cloudtasks.queues.delete"},
		{"logging post read", "logging.googleapis.com", http.MethodPost, "/v2/entries:list", "logging.logEntries.list"},
		{"scheduler action", "cloudscheduler.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/us/jobs/j:run", "cloudscheduler.jobs.run"},
		{"build action", "cloudbuild.googleapis.com", http.MethodPost, "/v1/projects/demo/triggers/t:run", "cloudbuild.builds.create"},
		{"iam post read", "iam.googleapis.com", http.MethodPost, "/v1/projects/demo/serviceAccounts/a:testIamPermissions", "iam.serviceAccounts.get"},
		{"unknown path denied", "logging.googleapis.com", http.MethodPost, "/v2/unmapped", ""},
		{"unknown action denied", "cloudtasks.googleapis.com", http.MethodPost, "/v2/projects/demo/locations/us/queues/q:explode", ""},
		{"known action on wrong resource denied", "cloudtasks.googleapis.com", http.MethodPost, "/v2/projects/demo/locations/us/queues/q:run", ""},
		{"collection delete denied", "cloudtasks.googleapis.com", http.MethodDelete, "/v2/projects/demo/locations/us/queues", ""},
		{"item post denied", "cloudtasks.googleapis.com", http.MethodPost, "/v2/projects/demo/locations/us/queues/q", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			permission, _ := routePermission(test.domain, httptest.NewRequest(test.method, test.path, nil))
			if permission != test.permission {
				t.Fatalf("permission = %q, want %q", permission, test.permission)
			}
		})
	}
}

func TestStrictIAMResourceRoutesExhaustEveryOperation(t *testing.T) {
	for domain, routes := range strictIAMResourceRoutes {
		for _, route := range routes {
			for _, template := range route.collectionTemplates {
				collection := materializeIAMTemplate(template)
				item := collection + "/resource"
				itemPostPermission := ""
				if domain == "firestore.googleapis.com" && route.permissionRoot == "datastore.entities" {
					itemPostPermission = "datastore.entities.create"
				}
				tests := []struct {
					operation, method, path, permission string
				}{
					{"list", http.MethodGet, collection, route.permissionRoot + ".list"},
					{"get", http.MethodGet, item, route.permissionRoot + ".get"},
					{"create", http.MethodPost, collection, route.permissionRoot + ".create"},
					{"update", http.MethodPatch, item, route.permissionRoot + ".update"},
					{"delete", http.MethodDelete, item, route.permissionRoot + ".delete"},
					{"item post", http.MethodPost, item, itemPostPermission},
					{"deny collection delete", http.MethodDelete, collection, ""},
				}
				for _, test := range tests {
					t.Run(domain+"/"+route.permissionRoot+"/"+test.operation, func(t *testing.T) {
						permission, _ := routePermission(domain, httptest.NewRequest(test.method, test.path, nil))
						if permission != test.permission {
							t.Fatalf("permission = %q, want %q", permission, test.permission)
						}
					})
				}
			}
		}
	}
}

func materializeIAMTemplate(template string) string {
	for {
		open := strings.IndexByte(template, '{')
		if open < 0 {
			return template
		}
		close := strings.IndexByte(template[open:], '}')
		if close < 0 {
			return template
		}
		close += open
		value := "resource"
		if template[open+1:close] == "project" {
			value = "demo"
		}
		template = template[:open] + value + template[close+1:]
	}
}

func TestStrictIAMExecutableCustomRouteTable(t *testing.T) {
	tests := []struct {
		name, domain, method, path, permission string
	}{
		{"bigtable read rows", "bigtable.googleapis.com", http.MethodPost, "/v2/projects/demo/instances/i/tables/t:readRows", "bigtable.tables.readRows"},
		{"bigquery query by job", "bigquery.googleapis.com", http.MethodGet, "/bigquery/v2/projects/demo/queries/job", "bigquery.jobs.get"},
		{"bigquery job results", "bigquery.googleapis.com", http.MethodGet, "/bigquery/v2/projects/demo/jobs/job/results", "bigquery.jobs.get"},
		{"bigquery insert all", "bigquery.googleapis.com", http.MethodPost, "/bigquery/v2/projects/demo/datasets/d/tables/t/insertAll", "bigquery.tables.updateData"},
		{"kms encrypt", "cloudkms.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/global/keyRings/r/cryptoKeys/k:encrypt", "cloudkms.cryptoKeyVersions.useToEncrypt"},
		{"kms decrypt", "cloudkms.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/global/keyRings/r/cryptoKeys/k:decrypt", "cloudkms.cryptoKeyVersions.useToDecrypt"},
		{"kms destroy", "cloudkms.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/global/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1:destroy", "cloudkms.cryptoKeyVersions.destroy"},
		{"instance start", "compute.googleapis.com", http.MethodPost, "/compute/v1/projects/demo/zones/us/instances/vm/start", "compute.instances.start"},
		{"instance stop", "compute.googleapis.com", http.MethodPost, "/compute/v1/projects/demo/zones/us/instances/vm/stop", "compute.instances.stop"},
		{"instance group add", "compute.googleapis.com", http.MethodPost, "/compute/v1/projects/demo/zones/us/instanceGroups/g/addInstances", "compute.instanceGroups.update"},
		{"instance group named ports", "compute.googleapis.com", http.MethodPost, "/compute/v1/projects/demo/zones/us/instanceGroups/g/setNamedPorts", "compute.instanceGroups.update"},
		{"instance group list read", "compute.googleapis.com", http.MethodPost, "/compute/v1/projects/demo/zones/us/instanceGroups/g/listInstances", "compute.instanceGroups.get"},
		{"logging list post read", "logging.googleapis.com", http.MethodPost, "/v2/entries:list", "logging.logEntries.list"},
		{"monitoring query post read", "monitoring.googleapis.com", http.MethodPost, "/v3/projects/demo/timeSeries:query", "monitoring.timeSeries.list"},
		{"monitoring prometheus get", "monitoring.googleapis.com", http.MethodGet, "/v1/projects/demo/location/global/prometheus/api/v1/query", "monitoring.timeSeries.list"},
		{"secret access get", "secretmanager.googleapis.com", http.MethodGet, "/v1/projects/demo/secrets/s/versions/latest:access", "secretmanager.versions.access"},
		{"scheduler run", "cloudscheduler.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/us/jobs/j:run", "cloudscheduler.jobs.run"},
		{"iam get policy read", "iam.googleapis.com", http.MethodGet, "/v1/projects/demo/serviceAccounts/a:getIamPolicy", "iam.serviceAccounts.get"},
		{"vertex generate content", "aiplatform.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/us/publishers/google/models/gemini:generateContent", "aiplatform.endpoints.predict"},
		{"vertex predict", "aiplatform.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/us/endpoints/e:predict", "aiplatform.endpoints.predict"},
		{"pubsub publish", "pubsub.googleapis.com", http.MethodPost, "/v1/projects/demo/topics/t:publish", "pubsub.topics.publish"},
		{"cloudbuild trigger run", "cloudbuild.googleapis.com", http.MethodPost, "/v1/projects/demo/triggers/t:run", "cloudbuild.builds.create"},
		{"dataproc submit", "dataproc.googleapis.com", http.MethodPost, "/v1/projects/demo/regions/us/jobs:submit", "dataproc.jobs.submit"},
		{"iam credentials token", "iamcredentials.googleapis.com", http.MethodPost, "/v1/projects/-/serviceAccounts/a@example.test:generateAccessToken", "iam.serviceAccounts.getAccessToken"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			permission, _ := routePermission(test.domain, httptest.NewRequest(test.method, test.path, nil))
			if permission != test.permission {
				t.Fatalf("permission = %q, want %q", permission, test.permission)
			}
		})
	}
}

func TestStrictIAMCustomRouteTableIsExhaustivelyMatched(t *testing.T) {
	seen := make(map[string]struct{}, len(strictIAMCustomRoutes))
	for _, route := range strictIAMCustomRoutes {
		key := route.domain + " " + route.method + " " + route.template
		if _, duplicate := seen[key]; duplicate {
			t.Errorf("duplicate strict-IAM custom route %s", key)
			continue
		}
		seen[key] = struct{}{}
		path := materializeIAMTemplate(route.template)
		t.Run(key, func(t *testing.T) {
			permission, _ := routePermission(route.domain, httptest.NewRequest(route.method, path, nil))
			if permission != route.permission {
				t.Fatalf("permission = %q, want %q", permission, route.permission)
			}
		})
	}
}

func TestStrictIAMAlternateHandlerRoutes(t *testing.T) {
	tests := []struct {
		name, domain, method, path, permission, resource string
	}{
		{"pubsub topic create", "pubsub.googleapis.com", http.MethodPut, "/projects/demo/topics/t", "pubsub.topics.create", "projects/demo"},
		{"pubsub topic delete", "pubsub.googleapis.com", http.MethodDelete, "/projects/demo/topics/t", "pubsub.topics.delete", "projects/demo"},
		{"pubsub subscription create", "pubsub.googleapis.com", http.MethodPut, "/projects/demo/subscriptions/s", "pubsub.subscriptions.create", "projects/demo"},
		{"pubsub subscription delete", "pubsub.googleapis.com", http.MethodDelete, "/projects/demo/subscriptions/s", "pubsub.subscriptions.delete", "projects/demo"},
		{"pubsub publish", "pubsub.googleapis.com", http.MethodPost, "/projects/demo/topics/t:publish", "pubsub.topics.publish", "projects/demo/topics/t"},
		{"cloud build location create", "cloudbuild.googleapis.com", http.MethodPost, "/v1/projects/demo/locations/us/builds", "cloudbuild.builds.create", "projects/demo"},
		{"storage resumable upload put", "storage.googleapis.com", http.MethodPut, "/upload/storage/v1/b/bucket/o", "storage.objects.create", "projects/"},
		{"bigquery resumable upload put", "bigquery.googleapis.com", http.MethodPut, "/upload/bigquery/v2/projects/demo/jobs", "bigquery.jobs.create", "projects/demo"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			permission, resource := routePermission(test.domain, httptest.NewRequest(test.method, test.path, nil))
			if permission != test.permission || resource != test.resource {
				t.Fatalf("permission/resource = (%q, %q), want (%q, %q)",
					permission, resource, test.permission, test.resource)
			}
		})
	}
}

func TestStrictIAMNormalizedTemplatesRejectPathConfusion(t *testing.T) {
	tests := []struct {
		name, domain, method, path, permission string
	}{
		{"unknown prefix", "cloudtasks.googleapis.com", http.MethodGet, "/v2/unmapped/projects/demo/locations/us/queues", ""},
		{"wrong hierarchy", "cloudtasks.googleapis.com", http.MethodGet, "/v2/projects/demo/locations/us/widgets/queues/item", ""},
		{"unknown version", "cloudtasks.googleapis.com", http.MethodGet, "/v9/projects/demo/locations/us/queues", ""},
		{"unknown metadata route", "metadata.google.internal", http.MethodGet, "/v2/unmapped/instance/id", ""},
		{"queue named queues is item", "cloudtasks.googleapis.com", http.MethodGet, "/v2/projects/demo/locations/us/queues/queues", "cloudtasks.queues.get"},
		{"instance named instances is item", "compute.googleapis.com", http.MethodGet, "/compute/v1/projects/demo/zones/us/instances/instances", "compute.instances.get"},
		{"custom action on wrong template", "compute.googleapis.com", http.MethodPost, "/compute/v1/projects/demo/zones/us/widgets/vm/start", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			permission, _ := routePermission(test.domain, httptest.NewRequest(test.method, test.path, nil))
			if permission != test.permission {
				t.Fatalf("permission = %q, want %q", permission, test.permission)
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
	if labels.Project != "demo" {
		t.Fatalf("project = %q", labels.Project)
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
		{name: "sql admin", domain: "sqladmin.googleapis.com", path: "/sql/v1beta4/projects/demo/instances"},
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
