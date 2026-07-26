package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSelectServiceDomainsFiltersByAliasAndRejectsUnknown(t *testing.T) {
	shims := map[string]http.Handler{
		"compute.googleapis.com": http.NotFoundHandler(),
		"pubsub.googleapis.com":  http.NotFoundHandler(),
	}
	selected, lazy, err := selectServiceDomains(
		shims,
		[]string{"storage.googleapis.com"},
		"compute,storage.googleapis.com",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected["compute.googleapis.com"] == nil {
		t.Fatalf("selected shims = %#v", selected)
	}
	if len(lazy) != 1 || lazy[0] != "storage.googleapis.com" {
		t.Fatalf("selected lazy domains = %#v", lazy)
	}

	if _, _, err := selectServiceDomains(shims, nil, "unknown"); err == nil {
		t.Fatal("expected unknown service error")
	}
}

func TestGatewayMuxExposesReadinessWithoutDispatching(t *testing.T) {
	dispatched := false
	handler := gatewayMux(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		dispatched = true
	}))
	request := httptest.NewRequest(http.MethodGet, "http://localhost/healthz", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"ready"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if dispatched {
		t.Fatal("readiness request reached the public gateway")
	}
}

func TestDashboardAuditProjectUsesCanonicalHeader(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "http://localhost/api/settings", nil)
	request.Header.Set("X-MiniSky-Project", "local-dev-project")
	if got := dashboardAuditProject(request); got != "local-dev-project" {
		t.Fatalf("dashboard audit project = %q, want local-dev-project", got)
	}
}

func TestDashboardUnavailableHandlerReturnsStableJSON(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://localhost/api/services", nil)
	response := httptest.NewRecorder()
	dashboardUnavailableHandler().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type=%q", response.Header().Get("Content-Type"))
	}
	if !strings.Contains(response.Body.String(), `"code":503`) ||
		!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
		t.Fatalf("body=%s", response.Body.String())
	}
}

func TestNativeIntegrationDockerBypassIsNarrow(t *testing.T) {
	for _, name := range []string{
		"MINISKY_PHASE16_LOGGING_INTEGRATION",
		"MINISKY_PHASE16_DNS_INTEGRATION",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("MINISKY_PHASE16_LOGGING_INTEGRATION", "")
			t.Setenv("MINISKY_PHASE16_DNS_INTEGRATION", "")
			t.Setenv(name, "1")
			if !nativeIntegrationDisablesDocker() {
				t.Fatalf("%s did not activate guarded Docker bypass", name)
			}
		})
	}
	t.Setenv("MINISKY_PHASE16_LOGGING_INTEGRATION", "")
	t.Setenv("MINISKY_PHASE16_DNS_INTEGRATION", "true")
	if nativeIntegrationDisablesDocker() {
		t.Fatal("non-guard value activated Docker bypass")
	}
}

type shutdownHandler struct {
	http.Handler
	called bool
}

func (h *shutdownHandler) Shutdown(context.Context) error {
	h.called = true
	return nil
}

func TestShutdownPluginsRunsLifecycleHook(t *testing.T) {
	handler := &shutdownHandler{Handler: http.NotFoundHandler()}
	if err := shutdownPlugins(context.Background(), map[string]http.Handler{"example.test": handler}); err != nil {
		t.Fatal(err)
	}
	if !handler.called {
		t.Fatal("shutdown hook was not called")
	}
}
