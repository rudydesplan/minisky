package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
	"minisky/pkg/state"
)

func TestGatewayLoopbackClientTrustsConfiguredTLS(t *testing.T) {
	serverTLS, diagnostics, err := localsecurity.PrepareTLS(localsecurity.TLSOptions{
		Mode: localsecurity.TLSAuto, ProfileDir: t.TempDir(), ServerName: "localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := gatewayLoopbackClient(serverTLS, diagnostics.CertificateFile, "", "")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestGatewayLoopbackClientRejectsMTLSWithoutClientIdentity(t *testing.T) {
	serverTLS, diagnostics, err := localsecurity.PrepareTLS(localsecurity.TLSOptions{
		Mode: localsecurity.TLSAuto, ProfileDir: t.TempDir(), ServerName: "localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	if _, err := gatewayLoopbackClient(serverTLS, diagnostics.CertificateFile, "", ""); err == nil {
		t.Fatal("mTLS client reused server identity or omitted client credentials")
	}
}

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

type degradedHealth struct{ err error }

func (health degradedHealth) PersistenceError() error { return health.err }

func TestGatewayDefaultsToLoopbackAndRequiresExplicitRemoteBind(t *testing.T) {
	if got := gatewayListenAddress("", "8080"); got != "127.0.0.1:8080" {
		t.Fatalf("default address = %q", got)
	}
	if got := gatewayListenAddress("0.0.0.0", "8080"); got != "0.0.0.0:8080" {
		t.Fatalf("remote address = %q", got)
	}
}

func TestGatewayHealthReportsPersistenceDegradation(t *testing.T) {
	const sensitive = "/Users/private/.minisky/profiles/prod/state.json: permission denied"
	handler := gatewayMux(http.NotFoundHandler(), degradedHealth{err: errors.New(sensitive)})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://localhost/healthz", nil))
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"status":"degraded"`) ||
		!strings.Contains(response.Body.String(), `"message":"persistence is degraded"`) ||
		strings.Contains(response.Body.String(), sensitive) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
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
	called atomic.Bool
}

func (h *shutdownHandler) Shutdown(context.Context) error {
	h.called.Store(true)
	return nil
}

func TestShutdownPluginsRunsLifecycleHook(t *testing.T) {
	handler := &shutdownHandler{Handler: http.NotFoundHandler()}
	if err := shutdownPlugins(context.Background(), map[string]http.Handler{"example.test": handler}); err != nil {
		t.Fatal(err)
	}
	if !handler.called.Load() {
		t.Fatal("shutdown hook was not called")
	}
}

type shutdownFuncHandler struct {
	http.Handler
	shutdown func(context.Context) error
}

func (handler *shutdownFuncHandler) Shutdown(ctx context.Context) error {
	return handler.shutdown(ctx)
}

func TestShutdownPluginsDoesNotStarvePersistenceBehindBlockedPlugin(t *testing.T) {
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	blockedDone := make(chan struct{})
	blocked := &shutdownFuncHandler{
		Handler: http.NotFoundHandler(),
		shutdown: func(context.Context) error {
			defer close(blockedDone)
			close(blockedStarted)
			<-releaseBlocked
			return nil
		},
	}
	persisted := make(chan struct{})
	persistence := &shutdownFuncHandler{
		Handler: http.NotFoundHandler(),
		shutdown: func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("persistence received expired context: %w", err)
			}
			close(persisted)
			return nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := shutdownPlugins(ctx, map[string]http.Handler{
		"blocked.example":     blocked,
		"persistence.example": persistence,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		close(releaseBlocked)
		t.Fatalf("shutdown error = %v, want context deadline exceeded", err)
	}
	select {
	case <-blockedStarted:
	default:
		close(releaseBlocked)
		t.Fatal("blocked plugin was not launched")
	}
	select {
	case <-persisted:
	default:
		close(releaseBlocked)
		t.Fatal("persistence plugin was starved by blocked plugin")
	}
	close(releaseBlocked)
	select {
	case <-blockedDone:
	case <-time.After(time.Second):
		t.Fatal("blocked plugin goroutine could not finish after shutdown returned")
	}
}

func TestTelemetryDeadlineDoesNotStarvePluginPersistence(t *testing.T) {
	telemetryStarted := make(chan struct{})
	telemetry := func(ctx context.Context) error {
		close(telemetryStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	persisted := make(chan struct{})
	persistence := &shutdownFuncHandler{
		Handler: http.NotFoundHandler(),
		shutdown: func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("persistence received expired context: %w", err)
			}
			close(persisted)
			return nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	telemetryErr, pluginErr := shutdownTelemetryAndPlugins(ctx, telemetry, map[string]http.Handler{
		"persistence.example": persistence,
	})
	if !errors.Is(telemetryErr, context.DeadlineExceeded) {
		t.Fatalf("telemetry error = %v, want context deadline exceeded", telemetryErr)
	}
	if pluginErr != nil {
		t.Fatalf("plugin shutdown error = %v", pluginErr)
	}
	select {
	case <-telemetryStarted:
	default:
		t.Fatal("telemetry shutdown was not launched")
	}
	select {
	case <-persisted:
	default:
		t.Fatal("plugin persistence was starved by telemetry shutdown")
	}
}

func TestOpenDaemonStateClaimsProfileOwnership(t *testing.T) {
	root := t.TempDir()
	_, ownership, err := openDaemonState(root, "active")
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := openDaemonState(root, "active"); !errors.Is(err, state.ErrProfileInUse) {
		t.Fatalf("second daemon error = %v, want ErrProfileInUse", err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}

	_, nextOwnership, err := openDaemonState(root, "active")
	if err != nil {
		t.Fatalf("open daemon state after release: %v", err)
	}
	if err := nextOwnership.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileOwnedSpoolsRemovesOnlyMiniSkyFiles(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, "profiles", "active")
	for _, dir := range []string{"request-spool", "uploads"} {
		if err := os.MkdirAll(filepath.Join(profileDir, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{
		filepath.Join(profileDir, "request-spool", ".request-crashed.tmp"),
		filepath.Join(profileDir, "uploads", ".upload-crashed.tmp"),
		filepath.Join(profileDir, "uploads", ".session-crashed.tmp"),
	} {
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	unowned := filepath.Join(profileDir, "uploads", "keep-me")
	if err := os.WriteFile(unowned, []byte("user data"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, ownership, err := openDaemonState(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()
	if err := reconcileOwnedSpools(store, ownership); err != nil {
		t.Fatal(err)
	}
	secondCrash := []string{
		filepath.Join(profileDir, "request-spool", ".request-second-crash.tmp"),
		filepath.Join(profileDir, "uploads", ".session-second-crash.tmp"),
	}
	for _, path := range secondCrash {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Truncate(60 << 20); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := reconcileOwnedSpools(store, ownership); err != nil {
		t.Fatalf("second restart reconciliation: %v", err)
	}

	if _, err := os.Stat(unowned); err != nil {
		t.Fatalf("unowned file was removed: %v", err)
	}
	for _, path := range []string{
		filepath.Join(profileDir, "request-spool", ".request-crashed.tmp"),
		filepath.Join(profileDir, "uploads", ".upload-crashed.tmp"),
		filepath.Join(profileDir, "uploads", ".session-crashed.tmp"),
		secondCrash[0],
		secondCrash[1],
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("owned stale file remains at %s: %v", path, err)
		}
	}
}

func TestReconcileOwnedSpoolsRejectsSymlinkedDirectory(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, "profiles", "active")
	external := t.TempDir()
	if err := os.MkdirAll(profileDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(profileDir, "request-spool")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	externalFile := filepath.Join(external, ".request-unowned.tmp")
	if err := os.WriteFile(externalFile, []byte("external"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, ownership, err := openDaemonState(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()
	if err := reconcileOwnedSpools(store, ownership); err == nil {
		t.Fatal("symlinked spool directory was accepted")
	}
	if _, err := os.Stat(externalFile); err != nil {
		t.Fatalf("external file was touched: %v", err)
	}
}
