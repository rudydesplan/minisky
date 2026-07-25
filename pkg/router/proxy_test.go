package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
