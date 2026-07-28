package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/dashboard"
	"minisky/pkg/observability"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/router"
	localsecurity "minisky/pkg/security"
	_ "minisky/pkg/shims" // Triggers all shim registrations
	"minisky/pkg/shims/accesscontextmanager"
	"minisky/pkg/shims/appengine"
	"minisky/pkg/shims/bigquery"
	"minisky/pkg/shims/compute"
	"minisky/pkg/shims/gke"
	"minisky/pkg/shims/iam"
	"minisky/pkg/shims/iamcredentials"
	"minisky/pkg/shims/logging"
	"minisky/pkg/shims/memorystore"
	"minisky/pkg/shims/monitoring"
	"minisky/pkg/shims/resourcemanager"
	"minisky/pkg/shims/scheduler"
	"minisky/pkg/shims/serverless"
	"minisky/pkg/state"
	"minisky/pkg/version"
	"minisky/ui"

	"github.com/spf13/cobra"
)

var (
	apiPort         string
	apiBind         string
	uiPort          string
	otelEnabled     bool
	otelEndpoint    string
	replayEnabled   bool
	replayMaxBody   int64
	tlsMode         string
	tlsCert         string
	tlsKey          string
	tlsClientCA     string
	tlsClientCert   string
	tlsClientKey    string
	enforceProjects bool
	tokenAudience   string
	enabledServices string
	quotaConfigJSON string
	auditEnabled    bool
	auditStrict     bool
)

type shutdownHTTPServer interface {
	Shutdown(context.Context) error
}

func newDaemonHTTPServer(addr string, handler http.Handler, tlsConfig *tls.Config) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}
}

func shutdownHTTPServers(ctx context.Context, servers ...shutdownHTTPServer) error {
	var result error
	for _, server := range servers {
		if server != nil {
			result = errors.Join(result, server.Shutdown(ctx))
		}
	}
	return result
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the MiniSky Daemon and API Router",
	RunE: func(cmd *cobra.Command, args []string) (result error) {
		log.Printf("Starting MiniSky Daemon (API :%s, UI :%s)...", apiPort, uiPort)
		if os.Getenv("DOCKER_API_VERSION") == "" {
			os.Setenv("DOCKER_API_VERSION", "1.44")
		}

		// Ensure the directory exists
		miniskyDir := config.GetMiniskyDir()

		// Migration step: move legacy local .minisky to global home directory
		if stat, err := os.Stat(".minisky"); err == nil && stat.IsDir() {
			absLocal, _ := filepath.Abs(".minisky")
			if absLocal != miniskyDir {
				if _, err := os.Stat(miniskyDir); os.IsNotExist(err) {
					log.Printf("📦 Found legacy local .minisky directory. Migrating data to %s...", miniskyDir)
					if err := os.Rename(".minisky", miniskyDir); err != nil {
						log.Printf("⚠️ Failed to migrate .minisky: %v", err)
					}
				} else {
					log.Printf("⚠️ Notice: A local '.minisky' folder exists here but global '%s' is already in use. Local data will be ignored.", miniskyDir)
				}
			}
		}

		os.MkdirAll(miniskyDir, 0755)

		operationStore, profileOwnership, err := openDaemonState(config.GetStateDir(), config.GetProfile())
		if err != nil {
			return fmt.Errorf("acquire profile state ownership: %w", err)
		}
		defer func() {
			result = errors.Join(result, profileOwnership.Close())
		}()
		if err := reconcileOwnedSpools(operationStore, profileOwnership); err != nil {
			return fmt.Errorf("reconcile profile request spools: %w", err)
		}
		if err := writeDaemonRuntime(config.GetProfileDir(), currentDaemonRuntime()); err != nil {
			return fmt.Errorf("persist restart configuration: %w", err)
		}
		identity, err := newDaemonIdentity(config.GetProfile())
		if err != nil {
			return fmt.Errorf("capture daemon identity: %w", err)
		}
		if err := writeDaemonIdentity(config.GetProfileDir(), identity); err != nil {
			return fmt.Errorf("persist daemon identity: %w", err)
		}
		defer func() {
			result = errors.Join(result, removeDaemonIdentity(config.GetProfileDir(), identity))
		}()
		ctx, stopSignals, err := daemonSignalContext(context.Background(), identity.ControlToken)
		if err != nil {
			return fmt.Errorf("initialize daemon process control: %w", err)
		}
		defer stopSignals()

		// ── Orchestrator boot ───────────────────────────────────────────────
		// 1. Shared LRO state machine — passed into every shim that needs async ops.
		// Polling metadata survives restart; unfinished work is marked interrupted
		// and is never replayed.
		opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
		if err != nil {
			return fmt.Errorf("restore operation state: %w", err)
		}

		// 2. Docker service manager — creates the isolated minisky-net bridge network
		//    and handles cold-starting long-lived emulator containers (GCS, Pub/Sub, etc.).
		//    Guarded native-only integration gates do not use Docker.
		svcMgr, err := initializeDockerOrchestration(
			ctx,
			nativeIntegrationDisablesDocker(),
			productionDockerStartup,
		)
		if err != nil {
			return fmt.Errorf("initialize Docker orchestration: %w", err)
		}
		dockerClosed := false
		if svcMgr != nil {
			defer func() {
				if dockerClosed {
					return
				}
				dockerShutdownCtx, cancelDockerShutdown := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancelDockerShutdown()
				if err := svcMgr.Teardown(dockerShutdownCtx); err != nil {
					log.Printf("[WARN] Docker teardown did not complete cleanly: %v", err)
					result = errors.Join(result, err)
				}
			}()
		}

		// ── Router ──────────────────────────────────────────────────────────
		proxyRouter := router.NewProxyRouterWithManager(svcMgr)
		gatewayTLS, tlsDiagnostics, err := localsecurity.PrepareTLS(localsecurity.TLSOptions{
			Mode:       localsecurity.TLSMode(tlsMode),
			ProfileDir: config.GetProfileDir(),
			CertFile:   tlsCert,
			KeyFile:    tlsKey,
			ClientCA:   tlsClientCA,
			ServerName: "localhost",
		})
		if err != nil {
			return fmt.Errorf("prepare TLS configuration: %w", err)
		}
		telemetryShutdown, telemetryErr := observability.SetupTelemetry(ctx, observability.TelemetryConfig{
			Enabled:        otelEnabled,
			Endpoint:       otelEndpoint,
			ServiceVersion: version.Version,
		})
		telemetryShutdown = telemetrySetupOrNoop(telemetryShutdown, telemetryErr)
		telemetryClosed := false
		defer func() {
			if telemetryClosed {
				return
			}
			telemetryCtx, cancelTelemetry := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancelTelemetry()
			result = errors.Join(result, telemetryShutdown(telemetryCtx))
		}()
		quotaLimiter, err := router.ParseQuotaConfigJSON(quotaConfigJSON, time.Now)
		if err != nil {
			return fmt.Errorf("parse quota configuration: %w", err)
		}
		var auditLog *localsecurity.AuditLog
		var auditHealth persistenceHealth
		if auditEnabled || auditStrict {
			auditLog, err = localsecurity.OpenAuditLog(config.GetProfileDir(), config.GetProfile(), auditStrict)
			if err != nil {
				if auditStrict {
					return fmt.Errorf("open strict mutation audit: %w", err)
				}
				log.Printf("[WARN] Mutation audit disabled after integrity/open failure: %v", err)
				auditHealth = fixedPersistenceHealth{err: err}
				auditLog = nil
			} else {
				auditHealth = auditLog
				defer func() {
					if auditLog != nil {
						result = errors.Join(result, auditLog.Close())
						auditLog = nil
					}
				}()
			}
		}

		// ── Dynamic Registry Boot ──────────────────────────────────────────
		// This replaces the long list of manual RegisterShim calls.
		// All shims that are imported (using _ below) will self-register.
		shims, lazyDomains := registry.BootAll(opMgr, svcMgr)
		exposedShims, exposedLazyDomains, err := selectServiceDomains(shims, lazyDomains, enabledServices)
		if err != nil {
			return fmt.Errorf("select service domains: %w", err)
		}
		knownServices := make([]string, 0, len(exposedShims)+len(exposedLazyDomains))
		for domain := range exposedShims {
			knownServices = append(knownServices, domain)
		}
		knownServices = append(knownServices, exposedLazyDomains...)
		gatewayObservability := observability.New(observability.Config{
			Capacity:           1000,
			ReplayEnabled:      replayEnabled,
			ReplayMaxBodyBytes: replayMaxBody,
			KnownServices:      knownServices,
		})
		iamAPI := shims["iam.googleapis.com"].(*iam.API)
		projectAPI := shims["cloudresourcemanager.googleapis.com"].(*resourcemanager.API)
		proxyRouter.ConfigureSecurity(iamAPI, projectAPI, enforceProjects, tokenAudience)
		configureStartupServicePerimeters(proxyRouter, shims, opMgr)
		proxyRouter.ConfigureQuota(quotaLimiter, gatewayObservability.ObserveQuotaRejection)

		for domain, handler := range exposedShims {
			proxyRouter.RegisterShim(domain, registry.RuntimeHandler(domain, handler, svcMgr != nil))
		}
		for _, domain := range exposedLazyDomains {
			proxyRouter.RegisterLazyDocker(domain)
		}

		// Resolve shims needed for Dashboard
		logShim := shims["logging.googleapis.com"].(*logging.API)
		monShim := shims["monitoring.googleapis.com"].(*monitoring.API)
		bqAPI := shims["bigquery.googleapis.com"].(*bigquery.API)
		gkeAPI := shims["container.googleapis.com"].(*gke.API)
		schedulerAPI := shims["cloudscheduler.googleapis.com"].(*scheduler.API)
		gatewayObservability.RegisterResourceCounter("logging.googleapis.com", "log_entry", func() int {
			return len(logShim.GetEntries())
		})
		gatewayObservability.RegisterResourceCounter("minisky.local", "operation", func() int {
			return len(opMgr.List())
		})
		gatewayScheme := "http"
		if gatewayTLS != nil {
			gatewayScheme = "https"
		}
		gatewayURL := gatewayScheme + "://" + net.JoinHostPort("127.0.0.1", apiPort)
		schedulerAPI.SetGatewayBaseURL(gatewayURL)
		gatewayClient, err := gatewayLoopbackClient(gatewayTLS, tlsDiagnostics.CertificateFile, tlsClientCert, tlsClientKey)
		if err != nil {
			return fmt.Errorf("configure internal gateway client: %w", err)
		}
		gkeAPI.ConfigureGateway(gatewayURL, gatewayClient)
		var publicGateway http.Handler = gatewayObservability.Wrap(proxyRouter, proxyRouter.ClassifyRequest)
		if auditLog != nil {
			publicGateway = auditLog.Wrap(publicGateway, func(r *http.Request) localsecurity.AuditEvent {
				labels := proxyRouter.ClassifyRequest(r)
				return localsecurity.AuditEvent{
					Principal: r.Header.Get("X-MiniSky-Principal"),
					Method:    r.Method,
					Service:   labels.Service,
					Route:     labels.Route,
					Project:   router.ProjectFromRequest(r),
				}
			})
		}
		healthChecks := []persistenceHealth{opMgr}
		if auditHealth != nil {
			healthChecks = append(healthChecks, auditHealth)
		}
		for _, handler := range shims {
			if health, ok := handler.(persistenceHealth); ok {
				healthChecks = append(healthChecks, health)
			}
		}
		gatewayHandler := gatewayMux(publicGateway, identity.ControlToken, healthChecks...)
		gatewayObservability.SetReplayTarget(gatewayHandler)

		// ── Dashboard UI ─────────────────────────────────────────────────────
		uiAddr := net.JoinHostPort("127.0.0.1", uiPort)
		log.Printf("✨ MiniSky Dashboard available at %s://localhost:%s", gatewayScheme, uiPort)
		uiMux := http.NewServeMux()
		uiMux.Handle("/_minisky/control/readiness", daemonReadinessHandler("ui", identity.ControlToken))
		var apiHandler http.Handler
		if svcMgr == nil {
			apiHandler = dashboardAPIWithoutDocker(
				gatewayObservability.DiagnosticsHandler(),
				iamAPI,
				tokenAudience,
			)
		} else {
			serverlessShim := shims["cloudfunctions.googleapis.com"].(*serverless.API)
			appEngineAPI := shims["appengine.googleapis.com"].(*appengine.API)
			memoAPI := shims["redis.googleapis.com"].(*memorystore.API)
			computeAPI := shims["compute.googleapis.com"].(*compute.API)
			apiHandler = dashboard.NewAPIHandler(
				svcMgr, bqAPI.GetBackend(), gkeAPI.GetBackend(), gkeAPI,
				serverlessShim.GetBackend(), logShim, monShim, appEngineAPI,
				memoAPI, schedulerAPI, computeAPI, projectAPI, gatewayURL,
				gatewayObservability.DiagnosticsHandler(), iamAPI, tokenAudience,
			)
		}
		if auditLog != nil {
			apiHandler = auditLog.Wrap(apiHandler, func(r *http.Request) localsecurity.AuditEvent {
				return localsecurity.AuditEvent{
					Principal: r.Header.Get("X-MiniSky-Principal"),
					Method:    r.Method, Service: "dashboard",
					Route:   observability.NormalizeRoute(r.URL.Path),
					Project: dashboardAuditProject(r),
				}
			})
		}
		uiMux.Handle("/api/", apiHandler)
		uiMux.Handle("/", ui.Handler())
		uiServer := newDaemonHTTPServer(uiAddr, uiMux, nil)
		if gatewayTLS != nil {
			uiServer.TLSConfig = gatewayTLS.Clone()
			uiServer.TLSConfig.ClientAuth = tls.NoClientCert
			uiServer.TLSConfig.ClientCAs = nil
		}

		// ── API Proxy Gateway ────────────────────────────────────────────────
		addr := gatewayListenAddress(apiBind, apiPort)
		log.Printf("🚀 MiniSky API Gateway listening on %s://%s (mTLS=%t)", gatewayScheme, addr, tlsDiagnostics.ClientCAEnabled)
		server := newDaemonHTTPServer(addr, gatewayHandler, gatewayTLS)
		serveErrors := make(chan error, 2)
		serve := func(name string, current *http.Server) {
			var err error
			if current.TLSConfig != nil {
				err = current.ListenAndServeTLS("", "")
			} else {
				err = current.ListenAndServe()
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErrors <- fmt.Errorf("%s server: %w", name, err)
				return
			}
			serveErrors <- nil
		}
		go serve("dashboard", uiServer)
		go serve("gateway", server)

		listenerErr := waitForDaemonListener(ctx, serveErrors)
		if listenerErr != nil {
			log.Printf("[ERROR] %v", listenerErr)
			stopSignals()
		} else {
			log.Println("⏹️  MiniSky shutting down gracefully...")
		}

		var shutdownErr error
		httpShutdownCtx, cancelHTTPShutdown := context.WithTimeout(context.Background(), 15*time.Second)
		if err := shutdownHTTPServers(httpShutdownCtx, server, uiServer); err != nil {
			log.Printf("[WARN] HTTP server shutdown did not complete cleanly: %v", err)
			shutdownErr = errors.Join(shutdownErr, err)
		}
		cancelHTTPShutdown()
		pluginShutdownCtx, cancelPluginShutdown := context.WithTimeout(context.Background(), 15*time.Second)
		telemetryErr, pluginErr := shutdownTelemetryAndPlugins(pluginShutdownCtx, telemetryShutdown, shims)
		telemetryClosed = true
		cancelPluginShutdown()
		if telemetryErr != nil {
			log.Printf("[WARN] OpenTelemetry shutdown did not complete cleanly: %v", telemetryErr)
			shutdownErr = errors.Join(shutdownErr, telemetryErr)
		}
		if pluginErr != nil {
			log.Printf("[WARN] Plugin shutdown did not complete cleanly: %v", pluginErr)
			shutdownErr = errors.Join(shutdownErr, pluginErr)
		}
		if auditLog != nil {
			if err := auditLog.Close(); err != nil {
				log.Printf("[WARN] Audit log close failed: %v", err)
				shutdownErr = errors.Join(shutdownErr, err)
			}
			auditLog = nil
		}
		if svcMgr != nil {
			dockerShutdownCtx, cancelDockerShutdown := context.WithTimeout(context.Background(), 15*time.Second)
			dockerErr := svcMgr.Teardown(dockerShutdownCtx)
			cancelDockerShutdown()
			dockerClosed = true
			if dockerErr != nil {
				log.Printf("[WARN] Docker teardown did not complete cleanly: %v", dockerErr)
				shutdownErr = errors.Join(shutdownErr, dockerErr)
			}
		}
		return errors.Join(listenerErr, shutdownErr)
	},
}

func configureStartupServicePerimeters(
	proxyRouter *router.ProxyRouter,
	shims map[string]http.Handler,
	opMgr *orchestrator.OperationManager,
) *accesscontextmanager.API {
	perimeterAPI, ok := shims["accesscontextmanager.googleapis.com"].(*accesscontextmanager.API)
	if !ok {
		perimeterAPI = accesscontextmanager.NewAPI(opMgr)
	}
	proxyRouter.ConfigureServicePerimeters(perimeterAPI)
	if credentialsAPI, ok := shims["iamcredentials.googleapis.com"].(*iamcredentials.API); ok {
		credentialsAPI.ConfigureServicePerimeters(perimeterAPI)
	}
	return perimeterAPI
}

func telemetrySetupOrNoop(
	shutdown func(context.Context) error,
	setupErr error,
) func(context.Context) error {
	if setupErr == nil {
		return shutdown
	}
	log.Printf("[WARN] OpenTelemetry disabled after setup failure: %v", setupErr)
	return func(context.Context) error { return nil }
}

func waitForDaemonListener(ctx context.Context, results <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-results:
		return err
	}
}

func gatewayLoopbackClient(serverTLS *tls.Config, certificateFile, clientCertFile, clientKeyFile string) (*http.Client, error) {
	if serverTLS == nil {
		return &http.Client{Timeout: 30 * time.Second}, nil
	}
	pemBytes, err := os.ReadFile(certificateFile)
	if err != nil {
		return nil, fmt.Errorf("read gateway certificate: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("gateway certificate file contains no certificates")
	}
	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "localhost",
	}
	if serverTLS.ClientAuth != tls.NoClientCert {
		if clientCertFile == "" || clientKeyFile == "" {
			return nil, errors.New("gateway mTLS requires internal client certificate and key")
		}
		clientCertificate, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load internal gateway client certificate: %w", err)
		}
		clientTLS.Certificates = []tls.Certificate{clientCertificate}
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: clientTLS},
		Timeout:   30 * time.Second,
	}, nil
}

func openDaemonState(root, profile string) (*state.Store, *state.Ownership, error) {
	store, err := state.New(root, profile)
	if err != nil {
		return nil, nil, fmt.Errorf("open operation state: %w", err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		return nil, nil, fmt.Errorf("claim exclusive profile ownership: %w", err)
	}
	return store, ownership, nil
}

func reconcileOwnedSpools(store *state.Store, ownership *state.Ownership) error {
	if err := store.ReconcileOwnedSpools(
		ownership,
		state.OwnedSpoolSpec{
			Directory: "request-spool",
			Prefixes:  []string{".request-"},
		},
		state.OwnedSpoolSpec{
			Directory: "uploads",
			Prefixes:  []string{".upload-"},
		},
	); err != nil {
		return err
	}
	router.ResetProfileBodySpoolState(store.ProfileDir())
	bigquery.ResetProfileUploadState(store.ProfileDir())
	return bigquery.ReconcileCompletedUploadSessions(store.ProfileDir())
}

func dashboardAuditProject(r *http.Request) string {
	if project := router.ProjectFromRequest(r); project != "" {
		return project
	}
	return strings.TrimSpace(r.Header.Get("X-MiniSky-Project"))
}

func nativeIntegrationDisablesDocker() bool {
	return os.Getenv("MINISKY_PHASE16_LOGGING_INTEGRATION") == "1" ||
		os.Getenv("MINISKY_PHASE16_DNS_INTEGRATION") == "1"
}

func init() {
	startCmd.Flags().StringVar(&apiPort, "port", "8080", "Port for the MiniSky API Gateway (env: MINISKY_PORT)")
	startCmd.Flags().StringVar(&apiBind, "bind", "127.0.0.1", "Gateway bind address; remote binds expose Docker-backed APIs and require trusted network controls (env: MINISKY_BIND)")
	startCmd.Flags().StringVar(&uiPort, "ui-port", "8081", "Port for the MiniSky Dashboard UI (env: MINISKY_UI_PORT)")
	startCmd.Flags().BoolVar(&otelEnabled, "otel", false, "Enable OTLP HTTP trace export (env: MINISKY_OTEL_ENABLED)")
	startCmd.Flags().StringVar(&otelEndpoint, "otel-endpoint", "", "OTLP HTTP endpoint (env: MINISKY_OTEL_ENDPOINT)")
	startCmd.Flags().BoolVar(&replayEnabled, "request-replay", false, "Enable bounded gateway request replay (env: MINISKY_REQUEST_REPLAY_ENABLED)")
	startCmd.Flags().Int64Var(&replayMaxBody, "request-replay-max-body", 65536, "Maximum replay body bytes (env: MINISKY_REQUEST_REPLAY_MAX_BODY)")
	startCmd.Flags().StringVar(&tlsMode, "tls", "", "TLS mode: auto or files (env: MINISKY_TLS_MODE)")
	startCmd.Flags().StringVar(&tlsCert, "tls-cert", "", "TLS certificate PEM path (env: MINISKY_TLS_CERT)")
	startCmd.Flags().StringVar(&tlsKey, "tls-key", "", "TLS private key PEM path (env: MINISKY_TLS_KEY)")
	startCmd.Flags().StringVar(&tlsClientCA, "tls-client-ca", "", "Client CA PEM for gateway mTLS (env: MINISKY_TLS_CLIENT_CA)")
	startCmd.Flags().StringVar(&tlsClientCert, "tls-client-cert", "", "Client certificate PEM for internal gateway mTLS (env: MINISKY_TLS_CLIENT_CERT)")
	startCmd.Flags().StringVar(&tlsClientKey, "tls-client-key", "", "Client private key PEM for internal gateway mTLS (env: MINISKY_TLS_CLIENT_KEY)")
	startCmd.Flags().BoolVar(&enforceProjects, "enforce-projects", false, "Reject requests for unknown projects (env: MINISKY_ENFORCE_PROJECTS)")
	startCmd.Flags().StringVar(&tokenAudience, "token-audience", "minisky-gateway", "Required local token audience (env: MINISKY_TOKEN_AUDIENCE)")
	startCmd.Flags().StringVar(&enabledServices, "services", "", "Comma-separated service aliases or domains to expose; empty exposes all (env: MINISKY_SERVICES)")
	startCmd.Flags().StringVar(&quotaConfigJSON, "quotas", "", "JSON local quota configuration; empty disables quotas (env: MINISKY_QUOTAS_JSON)")
	startCmd.Flags().BoolVar(&auditEnabled, "audit", false, "Enable profile-scoped mutation audit records (env: MINISKY_AUDIT_ENABLED)")
	startCmd.Flags().BoolVar(&auditStrict, "audit-strict", false, "Reject mutations if an audit attempt cannot be appended (env: MINISKY_AUDIT_STRICT)")

	// Allow environment variable overrides
	if p := os.Getenv("MINISKY_PORT"); p != "" {
		apiPort = p
	}
	if value := os.Getenv("MINISKY_BIND"); value != "" {
		apiBind = value
	}
	if p := os.Getenv("MINISKY_UI_PORT"); p != "" {
		uiPort = p
	}
	if value, err := strconv.ParseBool(os.Getenv("MINISKY_OTEL_ENABLED")); err == nil {
		otelEnabled = value
	}
	if value := os.Getenv("MINISKY_OTEL_ENDPOINT"); value != "" {
		otelEndpoint = value
	}
	if value, err := strconv.ParseBool(os.Getenv("MINISKY_REQUEST_REPLAY_ENABLED")); err == nil {
		replayEnabled = value
	}
	if value, err := strconv.ParseInt(os.Getenv("MINISKY_REQUEST_REPLAY_MAX_BODY"), 10, 64); err == nil && value > 0 {
		replayMaxBody = value
	}
	if value := os.Getenv("MINISKY_TLS_MODE"); value != "" {
		tlsMode = value
	}
	if value := os.Getenv("MINISKY_TLS_CERT"); value != "" {
		tlsCert = value
	}
	if value := os.Getenv("MINISKY_TLS_KEY"); value != "" {
		tlsKey = value
	}
	if value := os.Getenv("MINISKY_TLS_CLIENT_CA"); value != "" {
		tlsClientCA = value
	}
	if value := os.Getenv("MINISKY_TLS_CLIENT_CERT"); value != "" {
		tlsClientCert = value
	}
	if value := os.Getenv("MINISKY_TLS_CLIENT_KEY"); value != "" {
		tlsClientKey = value
	}
	if value, err := strconv.ParseBool(os.Getenv("MINISKY_ENFORCE_PROJECTS")); err == nil {
		enforceProjects = value
	}
	if value := os.Getenv("MINISKY_TOKEN_AUDIENCE"); value != "" {
		tokenAudience = value
	}
	if value := os.Getenv("MINISKY_SERVICES"); value != "" {
		enabledServices = value
	}
	if value := os.Getenv("MINISKY_QUOTAS_JSON"); value != "" {
		quotaConfigJSON = value
	}
	if value, err := strconv.ParseBool(os.Getenv("MINISKY_AUDIT_ENABLED")); err == nil {
		auditEnabled = value
	}
	if value, err := strconv.ParseBool(os.Getenv("MINISKY_AUDIT_STRICT")); err == nil {
		auditStrict = value
	}

	rootCmd.AddCommand(startCmd)
}

func selectServiceDomains(
	shims map[string]http.Handler,
	lazyDomains []string,
	raw string,
) (map[string]http.Handler, []string, error) {
	if strings.TrimSpace(raw) == "" || strings.EqualFold(strings.TrimSpace(raw), "all") {
		return shims, lazyDomains, nil
	}

	available := make(map[string]string, len(shims)+len(lazyDomains))
	for domain := range shims {
		available[domain] = domain
		alias, _, _ := strings.Cut(domain, ".")
		if existing, found := available[alias]; !found || existing == domain {
			available[alias] = domain
		} else {
			available[alias] = ""
		}
	}
	for _, domain := range lazyDomains {
		available[domain] = domain
		alias, _, _ := strings.Cut(domain, ".")
		if existing, found := available[alias]; !found || existing == domain {
			available[alias] = domain
		} else {
			available[alias] = ""
		}
	}

	selectedDomains := make(map[string]struct{})
	for _, selector := range strings.Split(raw, ",") {
		selector = strings.ToLower(strings.TrimSpace(selector))
		domain, found := available[selector]
		if !found || domain == "" {
			return nil, nil, fmt.Errorf("unknown or ambiguous service %q", selector)
		}
		selectedDomains[domain] = struct{}{}
	}
	selectedShims := make(map[string]http.Handler, len(selectedDomains))
	for domain := range selectedDomains {
		if handler := shims[domain]; handler != nil {
			selectedShims[domain] = handler
		}
	}
	selectedLazy := make([]string, 0, len(selectedDomains))
	for _, domain := range lazyDomains {
		if _, selected := selectedDomains[domain]; selected {
			selectedLazy = append(selectedLazy, domain)
		}
	}
	return selectedShims, selectedLazy, nil
}

type persistenceHealth interface {
	PersistenceError() error
}

type fixedPersistenceHealth struct {
	err error
}

func (health fixedPersistenceHealth) PersistenceError() error {
	return health.err
}

func gatewayListenAddress(bind, port string) string {
	bind = strings.TrimSpace(bind)
	if bind == "" {
		bind = "127.0.0.1"
	}
	return net.JoinHostPort(bind, port)
}

func gatewayMux(gateway http.Handler, controlToken string, healthChecks ...persistenceHealth) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/_minisky/control/readiness", daemonReadinessHandler("api", controlToken))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		degraded := false
		for _, health := range healthChecks {
			if health != nil && health.PersistenceError() != nil {
				degraded = true
			}
		}
		if degraded {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status":  "degraded",
				"message": "persistence is degraded",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
	mux.Handle("/", gateway)
	return mux
}

func daemonReadinessHandler(role, controlToken string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nonce := r.Header.Get("X-MiniSky-Readiness-Nonce")
		nonceBytes, err := hex.DecodeString(nonce)
		if r.Method != http.MethodGet || err != nil || len(nonceBytes) != 32 || len(controlToken) != 64 {
			http.NotFound(w, r)
			return
		}
		proof := hmac.New(sha256.New, []byte(controlToken))
		_, _ = proof.Write([]byte(role + ":" + nonce))
		w.Header().Set("X-MiniSky-Readiness-Proof", hex.EncodeToString(proof.Sum(nil)))
		w.WriteHeader(http.StatusNoContent)
	})
}

func dashboardUnavailableHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code":    http.StatusServiceUnavailable,
				"message": "dashboard API is unavailable while Docker orchestration is disabled",
				"status":  "UNAVAILABLE",
			},
		})
	})
}

func shutdownPlugins(ctx context.Context, shims map[string]http.Handler) error {
	type shutdownTarget struct {
		domain    string
		lifecycle interface {
			Shutdown(context.Context) error
		}
	}
	domains := make([]string, 0, len(shims))
	for domain := range shims {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	targets := make([]shutdownTarget, 0, len(domains))
	for _, domain := range domains {
		handler := shims[domain]
		lifecycle, ok := handler.(interface {
			Shutdown(context.Context) error
		})
		if !ok {
			continue
		}
		targets = append(targets, shutdownTarget{domain: domain, lifecycle: lifecycle})
	}
	if len(targets) == 0 {
		return nil
	}

	type shutdownResult struct {
		domain string
		err    error
	}
	completed := make(chan shutdownResult, len(targets))
	for _, target := range targets {
		target := target
		go func() {
			completed <- shutdownResult{
				domain: target.domain,
				err:    invokePluginShutdown(ctx, target.lifecycle),
			}
		}()
	}

	failures := make([]shutdownResult, 0, len(targets))
	remaining := len(targets)
	for remaining > 0 {
		select {
		case result := <-completed:
			remaining--
			if result.err != nil {
				failures = append(failures, result)
			}
		case <-ctx.Done():
			sort.Slice(failures, func(i, j int) bool {
				return failures[i].domain < failures[j].domain
			})
			var result error
			for _, failure := range failures {
				result = errors.Join(result, fmt.Errorf("%s: %w", failure.domain, failure.err))
			}
			return errors.Join(result, fmt.Errorf("plugin shutdown deadline: %w", ctx.Err()))
		}
	}
	sort.Slice(failures, func(i, j int) bool {
		return failures[i].domain < failures[j].domain
	})
	var result error
	for _, failure := range failures {
		result = errors.Join(result, fmt.Errorf("%s: %w", failure.domain, failure.err))
	}
	return result
}

func invokePluginShutdown(ctx context.Context, lifecycle interface {
	Shutdown(context.Context) error
}) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("plugin shutdown panicked")
		}
	}()
	return lifecycle.Shutdown(ctx)
}

func shutdownTelemetryAndPlugins(
	ctx context.Context,
	telemetryShutdown func(context.Context) error,
	shims map[string]http.Handler,
) (telemetryErr, pluginErr error) {
	telemetryDone := make(chan error, 1)
	go func() {
		telemetryDone <- telemetryShutdown(ctx)
	}()

	pluginErr = shutdownPlugins(ctx, shims)
	select {
	case telemetryErr = <-telemetryDone:
	case <-ctx.Done():
		select {
		case telemetryErr = <-telemetryDone:
		default:
			telemetryErr = fmt.Errorf("telemetry shutdown deadline: %w", ctx.Err())
		}
	}
	return telemetryErr, pluginErr
}
