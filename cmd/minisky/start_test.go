package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/router"
	localsecurity "minisky/pkg/security"
	"minisky/pkg/shims/accesscontextmanager"
	"minisky/pkg/shims/iam"
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
	}), strings.Repeat("a", 64))
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
	handler := gatewayMux(http.NotFoundHandler(), strings.Repeat("a", 64), degradedHealth{err: errors.New(sensitive)})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://localhost/healthz", nil))
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"status":"degraded"`) ||
		!strings.Contains(response.Body.String(), `"message":"persistence is degraded"`) ||
		strings.Contains(response.Body.String(), sensitive) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStartupSharesDefaultOffPerimeterEvaluatorWithIAMCredentials(t *testing.T) {
	t.Run("persisted denial and live mutation", func(t *testing.T) {
		t.Setenv("MINISKY_STATE_DIR", t.TempDir())
		t.Setenv("MINISKY_PROFILE", "startup-perimeter-shared")
		t.Setenv(registry.ExperimentalServicesEnv, "")
		t.Setenv("MINISKY_IAM_MODE", "")
		opMgr := orchestrator.NewOperationManager()
		seedPersistedServicePerimeter(t, opMgr)

		shims, _ := registry.BootAll(opMgr, nil)
		if _, concrete := shims["accesscontextmanager.googleapis.com"].(*accesscontextmanager.API); concrete {
			t.Fatal("default-off registry unexpectedly returned concrete Access Context Manager API")
		}
		createStartupTestServiceAccount(t, shims["iam.googleapis.com"].(*iam.API))

		gateway := router.NewProxyRouterWithManager(nil)
		perimeterAPI := configureStartupServicePerimeters(gateway, shims, opMgr)
		gateway.RegisterShim("storage.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		gateway.RegisterShim("iamcredentials.googleapis.com", shims["iamcredentials.googleapis.com"])

		assertStartupPerimeterStatus(t, gateway, "storage", http.StatusForbidden)
		assertStartupPerimeterStatus(t, gateway, "iamcredentials", http.StatusForbidden)

		deleted := httptest.NewRecorder()
		perimeterAPI.ServeHTTP(deleted, httptest.NewRequest(
			http.MethodDelete,
			"/v1/accessPolicies/1/servicePerimeters/prod",
			nil,
		))
		if deleted.Code != http.StatusOK {
			t.Fatalf("delete perimeter status=%d body=%s", deleted.Code, deleted.Body.String())
		}

		assertStartupPerimeterStatus(t, gateway, "storage", http.StatusNoContent)
		assertStartupPerimeterStatus(t, gateway, "iamcredentials", http.StatusOK)
	})

	t.Run("persistence failure fails closed", func(t *testing.T) {
		t.Setenv("MINISKY_STATE_DIR", t.TempDir())
		t.Setenv("MINISKY_PROFILE", "startup-perimeter-degraded")
		t.Setenv(registry.ExperimentalServicesEnv, "")
		t.Setenv("MINISKY_IAM_MODE", "")
		opMgr := orchestrator.NewOperationManager()
		shims, _ := registry.BootAll(opMgr, nil)
		createStartupTestServiceAccount(t, shims["iam.googleapis.com"].(*iam.API))
		if err := os.WriteFile(filepath.Join(config.GetProfileDir(), "state.json"), []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}

		gateway := router.NewProxyRouterWithManager(nil)
		perimeterAPI := configureStartupServicePerimeters(gateway, shims, opMgr)
		if perimeterAPI.PersistenceError() == nil {
			t.Fatal("corrupt persisted perimeter state did not degrade evaluator")
		}
		gateway.RegisterShim("storage.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		gateway.RegisterShim("iamcredentials.googleapis.com", shims["iamcredentials.googleapis.com"])

		assertStartupPerimeterStatus(t, gateway, "storage", http.StatusForbidden)
		assertStartupPerimeterStatus(t, gateway, "iamcredentials", http.StatusForbidden)
	})
}

func seedPersistedServicePerimeter(t *testing.T, opMgr *orchestrator.OperationManager) {
	t.Helper()
	api := accesscontextmanager.NewAPI(opMgr)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/accessPolicies",
		bytes.NewBufferString(`{"parent":"organizations/1","title":"Startup test"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create access policy status=%d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/accessPolicies/1/servicePerimeters?servicePerimeterId=prod",
		bytes.NewBufferString(`{"status":{"resources":["projects/project-a"],`+
			`"restrictedServices":["storage.googleapis.com","iamcredentials.googleapis.com"]}}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create service perimeter status=%d body=%s", response.Code, response.Body.String())
	}
}

func createStartupTestServiceAccount(t *testing.T, api *iam.API) {
	t.Helper()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/project-a/serviceAccounts",
		bytes.NewBufferString(`{"accountId":"worker"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create service account status=%d body=%s", response.Code, response.Body.String())
	}
}

func assertStartupPerimeterStatus(t *testing.T, gateway http.Handler, service string, want int) {
	t.Helper()
	var request *http.Request
	switch service {
	case "storage":
		request = httptest.NewRequest(
			http.MethodGet,
			"http://localhost/_minisky/storage/v1/projects/project-a/buckets",
			nil,
		)
	case "iamcredentials":
		request = httptest.NewRequest(
			http.MethodPost,
			"http://localhost/_minisky/iamcredentials/v1/projects/-/serviceAccounts/"+
				"worker@project-a.iam.gserviceaccount.com:generateAccessToken",
			bytes.NewBufferString(`{"scope":["scope"]}`),
		)
	default:
		t.Fatalf("unknown startup test service %q", service)
	}
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	if response.Code != want {
		t.Fatalf("%s status=%d want=%d body=%s", service, response.Code, want, response.Body.String())
	}
	if want == http.StatusForbidden &&
		!strings.Contains(response.Body.String(), "Request is prohibited by VPC Service Controls") {
		t.Fatalf("%s denial body=%s", service, response.Body.String())
	}
}

func TestDaemonReadinessProofAuthenticatesListenerRole(t *testing.T) {
	token := strings.Repeat("a", 64)
	nonce := strings.Repeat("b", 64)
	api := daemonReadinessHandler("api", token)
	ui := daemonReadinessHandler("ui", token)

	request := httptest.NewRequest(http.MethodGet, "http://localhost/_minisky/control/readiness", nil)
	request.Header.Set("X-MiniSky-Readiness-Nonce", nonce)
	apiResponse := httptest.NewRecorder()
	api.ServeHTTP(apiResponse, request)
	if apiResponse.Code != http.StatusNoContent {
		t.Fatalf("API readiness status = %d", apiResponse.Code)
	}
	apiProof := apiResponse.Header().Get("X-MiniSky-Readiness-Proof")
	if err := verifyDaemonReadinessProof("api", token, nonce, apiProof); err != nil {
		t.Fatalf("API readiness proof: %v", err)
	}
	if err := verifyDaemonReadinessProof("ui", token, nonce, apiProof); err == nil {
		t.Fatal("API listener proof authenticated as UI listener")
	}

	uiResponse := httptest.NewRecorder()
	ui.ServeHTTP(uiResponse, request)
	if err := verifyDaemonReadinessProof(
		"ui", token, nonce, uiResponse.Header().Get("X-MiniSky-Readiness-Proof"),
	); err != nil {
		t.Fatalf("UI readiness proof: %v", err)
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

type shutdownServerStub struct {
	called atomic.Bool
}

func (server *shutdownServerStub) Shutdown(context.Context) error {
	server.called.Store(true)
	return nil
}

func TestDaemonServersHaveTimeoutsAndShutDownGracefully(t *testing.T) {
	server := newDaemonHTTPServer("127.0.0.1:0", http.NotFoundHandler(), nil)
	if server.ReadHeaderTimeout <= 0 || server.ReadTimeout <= 0 ||
		server.WriteTimeout <= 0 || server.IdleTimeout <= 0 {
		t.Fatalf("daemon server timeouts are not bounded: %#v", server)
	}
	if server.WriteTimeout < 2*time.Minute {
		t.Fatalf("daemon write timeout %s cannot cover a bounded lazy backend cold start", server.WriteTimeout)
	}
	first := &shutdownServerStub{}
	second := &shutdownServerStub{}
	if err := shutdownHTTPServers(context.Background(), first, second); err != nil {
		t.Fatal(err)
	}
	if !first.called.Load() || !second.called.Load() {
		t.Fatal("graceful shutdown did not reach both servers")
	}
}

func TestDaemonListenerFailurePropagatesFromCommand(t *testing.T) {
	if startCmd.RunE == nil {
		t.Fatal("start command cannot return listener failures")
	}
	sentinel := errors.New("bind failed")
	results := make(chan error, 1)
	results <- sentinel
	if err := waitForDaemonListener(context.Background(), results); !errors.Is(err, sentinel) {
		t.Fatalf("listener result = %v, want %v", err, sentinel)
	}
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

func TestShutdownPluginsContainsLifecyclePanic(t *testing.T) {
	persisted := false
	err := shutdownPlugins(context.Background(), map[string]http.Handler{
		"panic.example": &shutdownFuncHandler{
			Handler: http.NotFoundHandler(),
			shutdown: func(context.Context) error {
				panic("plugin secret must not escape")
			},
		},
		"persistence.example": &shutdownFuncHandler{
			Handler: http.NotFoundHandler(),
			shutdown: func(context.Context) error {
				persisted = true
				return nil
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "panic.example: plugin shutdown panicked") {
		t.Fatalf("shutdown error = %v", err)
	}
	if strings.Contains(err.Error(), "plugin secret") {
		t.Fatalf("shutdown error leaked panic value: %v", err)
	}
	if !persisted {
		t.Fatal("plugin panic starved another shutdown hook")
	}
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

func TestTelemetrySetupFailureWarnsAndReturnsNoop(t *testing.T) {
	previousWriter := log.Writer()
	var logs bytes.Buffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previousWriter) })

	sentinel := errors.New("invalid OTLP endpoint")
	shutdown := telemetrySetupOrNoop(func(context.Context) error {
		return errors.New("unexpected original shutdown call")
	}, sentinel)
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("fallback shutdown: %v", err)
	}
	if !strings.Contains(logs.String(), "OpenTelemetry disabled after setup failure") ||
		!strings.Contains(logs.String(), sentinel.Error()) {
		t.Fatalf("startup warning = %q", logs.String())
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

func TestStartupFailureRunsIdentityBeforeOwnershipCleanup(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINISKY_STATE_DIR", filepath.Join(t.TempDir(), "state"))
	t.Setenv("MINISKY_PROFILE", "startup-cleanup")
	t.Setenv("MINISKY_PHASE16_LOGGING_INTEGRATION", "1")
	originalArgs := os.Args
	os.Args = []string{"minisky", "start", "--services=does-not-exist"}
	t.Cleanup(func() { os.Args = originalArgs })
	originalServices := enabledServices
	enabledServices = "does-not-exist"
	t.Cleanup(func() { enabledServices = originalServices })

	err := startCmd.RunE(startCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "select service domains") {
		t.Fatalf("startup error = %v, want service selection failure", err)
	}
	if _, err := os.Stat(daemonIdentityPath(config.GetProfileDir())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("daemon identity survived failed startup: %v", err)
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatalf("profile ownership survived failed startup: %v", err)
	}
	if err := ownership.Close(); err != nil {
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
