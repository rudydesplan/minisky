package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/dashboard"
	"minisky/pkg/observability"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/router"
	localsecurity "minisky/pkg/security"
	_ "minisky/pkg/shims" // Triggers all shim registrations
	"minisky/pkg/shims/appengine"
	"minisky/pkg/shims/bigquery"
	"minisky/pkg/shims/compute"
	"minisky/pkg/shims/gke"
	"minisky/pkg/shims/iam"
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
	uiPort          string
	otelEnabled     bool
	otelEndpoint    string
	replayEnabled   bool
	replayMaxBody   int64
	tlsMode         string
	tlsCert         string
	tlsKey          string
	tlsClientCA     string
	enforceProjects bool
	tokenAudience   string
	enabledServices string
	quotaConfigJSON string
	auditEnabled    bool
	auditStrict     bool
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the MiniSky Daemon and API Router",
	Run: func(cmd *cobra.Command, args []string) {
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

		// Write PID file
		pidFile := filepath.Join(miniskyDir, "minisky.pid")
		if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
			log.Printf("[WARN] Failed to write PID file: %v", err)
		}

		// ── Orchestrator boot ───────────────────────────────────────────────
		// 1. Shared LRO state machine — passed into every shim that needs async ops.
		// Polling metadata survives restart; unfinished work is marked interrupted
		// and is never replayed.
		operationStore, err := state.New(config.GetStateDir(), config.GetProfile())
		if err != nil {
			log.Fatalf("[FATAL] Cannot open operation state: %v", err)
		}
		opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
		if err != nil {
			log.Fatalf("[FATAL] Cannot restore operation state: %v", err)
		}

		ctx := context.Background()
		// 2. Docker service manager — creates the isolated minisky-net bridge network
		//    and handles cold-starting long-lived emulator containers (GCS, Pub/Sub, etc.).
		//    The guarded Phase 16 Logging gate is native-only and does not use Docker.
		var svcMgr *orchestrator.ServiceManager
		if os.Getenv("MINISKY_PHASE16_LOGGING_INTEGRATION") == "1" {
			log.Printf("[WARN] Docker orchestration disabled; Docker-backed services are unavailable")
		} else {
			svcMgr, err = orchestrator.NewServiceManager()
			if err != nil {
				log.Fatalf("[FATAL] Cannot connect to Docker: %v", err)
			}
			if err := svcMgr.EnsureNetwork(ctx); err != nil {
				log.Fatalf("[FATAL] Cannot create isolated minisky-net network: %v", err)
			}
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
			log.Fatalf("[FATAL] Invalid TLS configuration: %v", err)
		}
		telemetryShutdown, telemetryErr := observability.SetupTelemetry(ctx, observability.TelemetryConfig{
			Enabled:        otelEnabled,
			Endpoint:       otelEndpoint,
			ServiceVersion: version.Version,
		})
		if telemetryErr != nil {
			log.Printf("[WARN] OpenTelemetry disabled after setup failure: %v", telemetryErr)
			telemetryShutdown = func(context.Context) error { return nil }
		}
		gatewayObservability := observability.New(observability.Config{
			Capacity:           1000,
			ReplayEnabled:      replayEnabled,
			ReplayMaxBodyBytes: replayMaxBody,
		})
		quotaLimiter, err := router.ParseQuotaConfigJSON(quotaConfigJSON, time.Now)
		if err != nil {
			log.Fatalf("[FATAL] Invalid quota configuration: %v", err)
		}
		var auditLog *localsecurity.AuditLog
		if auditEnabled || auditStrict {
			auditLog, err = localsecurity.OpenAuditLog(config.GetProfileDir(), config.GetProfile(), auditStrict)
			if err != nil {
				if auditStrict {
					log.Fatalf("[FATAL] Strict mutation audit unavailable: %v", err)
				}
				log.Printf("[WARN] Mutation audit disabled after integrity/open failure: %v", err)
				auditLog = nil
			}
		}

		// ── Dynamic Registry Boot ──────────────────────────────────────────
		// This replaces the long list of manual RegisterShim calls.
		// All shims that are imported (using _ below) will self-register.
		shims, lazyDomains := registry.BootAll(opMgr, svcMgr)
		exposedShims, exposedLazyDomains, err := selectServiceDomains(shims, lazyDomains, enabledServices)
		if err != nil {
			log.Fatalf("[FATAL] Invalid service selection: %v", err)
		}
		iamAPI := shims["iam.googleapis.com"].(*iam.API)
		projectAPI := shims["cloudresourcemanager.googleapis.com"].(*resourcemanager.API)
		proxyRouter.ConfigureSecurity(iamAPI, projectAPI, enforceProjects, tokenAudience)
		proxyRouter.ConfigureQuota(quotaLimiter, gatewayObservability.ObserveQuotaRejection)

		for domain, handler := range exposedShims {
			proxyRouter.RegisterShim(domain, registry.ContractHandler(domain, handler))
		}
		for _, domain := range exposedLazyDomains {
			proxyRouter.RegisterLazyDocker(domain)
		}

		// Resolve shims needed for Dashboard
		logShim := shims["logging.googleapis.com"].(*logging.API)
		monShim := shims["monitoring.googleapis.com"].(*monitoring.API)
		serverlessShim := shims["cloudfunctions.googleapis.com"].(*serverless.API)
		bqAPI := shims["bigquery.googleapis.com"].(*bigquery.API)
		gkeAPI := shims["container.googleapis.com"].(*gke.API)
		appEngineAPI := shims["appengine.googleapis.com"].(*appengine.API)
		memoAPI := shims["redis.googleapis.com"].(*memorystore.API)
		schedulerAPI := shims["cloudscheduler.googleapis.com"].(*scheduler.API)
		computeAPI := shims["compute.googleapis.com"].(*compute.API)
		gatewayScheme := "http"
		if gatewayTLS != nil {
			gatewayScheme = "https"
		}
		gatewayURL := gatewayScheme + "://" + net.JoinHostPort("127.0.0.1", apiPort)
		schedulerAPI.SetGatewayBaseURL(gatewayURL)
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
		gatewayHandler := gatewayMux(publicGateway)
		gatewayObservability.SetReplayTarget(gatewayHandler)

		// ── Graceful Shutdown ────────────────────────────────────────────────
		go func() {
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
			<-quit
			log.Println("⏹️  MiniSky shutting down — tearing down isolated network...")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := telemetryShutdown(shutdownCtx); err != nil {
				log.Printf("[WARN] OpenTelemetry shutdown did not complete cleanly: %v", err)
			}
			if err := shutdownPlugins(shutdownCtx, shims); err != nil {
				log.Printf("[WARN] Plugin shutdown did not complete cleanly: %v", err)
			}
			if auditLog != nil {
				if err := auditLog.Close(); err != nil {
					log.Printf("[WARN] Audit log close failed: %v", err)
				}
			}
			cancel()
			if svcMgr != nil {
				svcMgr.Teardown(context.Background())
			}
			os.Remove(pidFile)
			os.Exit(0)
		}()

		// ── Dashboard UI ─────────────────────────────────────────────────────
		go func() {
			addr := net.JoinHostPort("127.0.0.1", uiPort)
			log.Printf("✨ MiniSky Dashboard available at %s://localhost:%s", gatewayScheme, uiPort)

			uiMux := http.NewServeMux()

			// REST API for dynamic dashboard control
			var apiHandler http.Handler
			if svcMgr == nil {
				apiHandler = dashboardUnavailableHandler()
			} else {
				apiHandler = dashboard.NewAPIHandler(
					svcMgr,
					bqAPI.GetBackend(),
					gkeAPI.GetBackend(),
					serverlessShim.GetBackend(),
					logShim,
					monShim,
					appEngineAPI,
					memoAPI,
					schedulerAPI,
					computeAPI,
					projectAPI,
					gatewayURL,
					gatewayObservability.DiagnosticsHandler(),
					iamAPI,
					tokenAudience,
				)
			}
			if auditLog != nil {
				apiHandler = auditLog.Wrap(apiHandler, func(r *http.Request) localsecurity.AuditEvent {
					return localsecurity.AuditEvent{
						Principal: r.Header.Get("X-MiniSky-Principal"),
						Method:    r.Method,
						Service:   "dashboard",
						Route:     observability.NormalizeRoute(r.URL.Path),
						Project:   dashboardAuditProject(r),
					}
				})
			}
			uiMux.Handle("/api/", apiHandler)
			// Fallback to static dist
			uiMux.Handle("/", ui.Handler())

			uiServer := &http.Server{Addr: addr, Handler: uiMux}
			if gatewayTLS != nil {
				uiServer.TLSConfig = gatewayTLS.Clone()
				uiServer.TLSConfig.ClientAuth = tls.NoClientCert
				uiServer.TLSConfig.ClientCAs = nil
			}
			var serveErr error
			if uiServer.TLSConfig != nil {
				serveErr = uiServer.ListenAndServeTLS("", "")
			} else {
				serveErr = uiServer.ListenAndServe()
			}
			if serveErr != nil {
				log.Fatalf("UI Server crashed: %v", serveErr)
			}
		}()

		// ── API Proxy Gateway ────────────────────────────────────────────────
		addr := ":" + apiPort
		log.Printf("🚀 MiniSky API Gateway listening on %s://localhost:%s (mTLS=%t)", gatewayScheme, apiPort, tlsDiagnostics.ClientCAEnabled)
		server := &http.Server{Addr: addr, Handler: gatewayHandler, TLSConfig: gatewayTLS}
		var serveErr error
		if gatewayTLS != nil {
			serveErr = server.ListenAndServeTLS("", "")
		} else {
			serveErr = server.ListenAndServe()
		}
		if serveErr != nil {
			log.Fatalf("Failed to start router: %v", serveErr)
		}
	},
}

func dashboardAuditProject(r *http.Request) string {
	if project := router.ProjectFromRequest(r); project != "" {
		return project
	}
	return strings.TrimSpace(r.Header.Get("X-MiniSky-Project"))
}

func init() {
	startCmd.Flags().StringVar(&apiPort, "port", "8080", "Port for the MiniSky API Gateway (env: MINISKY_PORT)")
	startCmd.Flags().StringVar(&uiPort, "ui-port", "8081", "Port for the MiniSky Dashboard UI (env: MINISKY_UI_PORT)")
	startCmd.Flags().BoolVar(&otelEnabled, "otel", false, "Enable OTLP HTTP trace export (env: MINISKY_OTEL_ENABLED)")
	startCmd.Flags().StringVar(&otelEndpoint, "otel-endpoint", "", "OTLP HTTP endpoint (env: MINISKY_OTEL_ENDPOINT)")
	startCmd.Flags().BoolVar(&replayEnabled, "request-replay", false, "Enable bounded gateway request replay (env: MINISKY_REQUEST_REPLAY_ENABLED)")
	startCmd.Flags().Int64Var(&replayMaxBody, "request-replay-max-body", 65536, "Maximum replay body bytes (env: MINISKY_REQUEST_REPLAY_MAX_BODY)")
	startCmd.Flags().StringVar(&tlsMode, "tls", "", "TLS mode: auto or files (env: MINISKY_TLS_MODE)")
	startCmd.Flags().StringVar(&tlsCert, "tls-cert", "", "TLS certificate PEM path (env: MINISKY_TLS_CERT)")
	startCmd.Flags().StringVar(&tlsKey, "tls-key", "", "TLS private key PEM path (env: MINISKY_TLS_KEY)")
	startCmd.Flags().StringVar(&tlsClientCA, "tls-client-ca", "", "Client CA PEM for gateway mTLS (env: MINISKY_TLS_CLIENT_CA)")
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

func gatewayMux(gateway http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
	mux.Handle("/", gateway)
	return mux
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
	var result error
	for domain, handler := range shims {
		lifecycle, ok := handler.(interface {
			Shutdown(context.Context) error
		})
		if !ok {
			continue
		}
		if err := lifecycle.Shutdown(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("%s: %w", domain, err))
		}
	}
	return result
}
