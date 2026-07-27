package router

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"

	"minisky/pkg/observability"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	localsecurity "minisky/pkg/security"
	"minisky/pkg/validator"
)

type callerSuppliedPrincipalContextKey struct{}

type callerSuppliedPrincipal string

// ProxyRouter intercepts and routes all incoming GCP API requests.
type ProxyRouter struct {
	mu              sync.RWMutex
	routes          map[string]http.Handler
	lazyDomains     map[string]bool // domains that should trigger Docker orchestration
	endpointAliases map[string]string
	validator       *validator.Validator
	serviceMgr      *orchestrator.ServiceManager
	authorizer      routeAuthorizer
	projects        projectRegistry
	enforceProjects bool
	tokenAudience   string
	quota           *QuotaLimiter
	quotaObserver   func(observability.RequestLabels, string)
}

// ConfigureQuota installs optional fixed-window local quotas. A nil limiter
// preserves the backward-compatible unlimited behavior.
func (p *ProxyRouter) ConfigureQuota(limiter *QuotaLimiter, observer func(observability.RequestLabels, string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.quota = limiter
	p.quotaObserver = observer
}

type routeAuthorizer interface {
	EnforcementEnabled() bool
	Authorize(resource, principal, permission string) bool
	VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error)
}

type projectRegistry interface {
	Exists(projectID string) bool
}

type completedUploadReplayProbe interface {
	IsCompletedUploadReplayCandidate(*http.Request) bool
	ProbeCompletedUploadReplay(*http.Request) (func(http.ResponseWriter), bool)
}

// NewProxyRouterWithManager creates the router with a pre-initialized ServiceManager injected.
func NewProxyRouterWithManager(sm *orchestrator.ServiceManager) *ProxyRouter {
	return &ProxyRouter{
		routes:          make(map[string]http.Handler),
		lazyDomains:     make(map[string]bool),
		endpointAliases: make(map[string]string),
		validator:       validator.NewValidator(),
		serviceMgr:      sm,
	}
}

// ConfigureSecurity installs the shared strict-mode authorization and optional
// project-existence gates. Permissive mode remains unchanged.
func (p *ProxyRouter) ConfigureSecurity(authorizer routeAuthorizer, projects projectRegistry, enforceProjects bool, audience string) {
	audience = strings.TrimSpace(audience)
	if audience == "" {
		audience = "minisky-gateway"
	}
	p.mu.Lock()
	p.authorizer = authorizer
	p.projects = projects
	p.enforceProjects = enforceProjects
	p.tokenAudience = audience
	p.mu.Unlock()
	if configurer, ok := authorizer.(interface{ SetTokenAudience(string) }); ok {
		configurer.SetTokenAudience(audience)
	}
}

// NewProxyRouter creates a standalone router (for backward compatibility).
func NewProxyRouter() *ProxyRouter {
	sm, err := orchestrator.NewServiceManager()
	if err != nil {
		log.Printf("[WARN] Failed to initialize Docker ServiceManager: %v", err)
	}
	return NewProxyRouterWithManager(sm)
}

// RegisterProxy maps a domain to a fixed external backend URL.
func (p *ProxyRouter) RegisterProxy(domain string, targetURL string) error {
	target, err := url.Parse(targetURL)
	if err != nil {
		return err
	}
	domain = normalizeDomain(domain)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = observability.InstrumentTransport(http.DefaultTransport)
	p.mu.Lock()
	p.routes[domain] = proxy
	p.registerEndpointAliasLocked(domain)
	p.mu.Unlock()
	log.Printf("[Router] Registered External Proxy: %s -> %s", domain, targetURL)
	return nil
}

// RegisterShim maps a domain to an internal Go handler (no Docker required).
func (p *ProxyRouter) RegisterShim(domain string, handler http.Handler) {
	domain = normalizeDomain(domain)
	p.mu.Lock()
	p.routes[domain] = handler
	p.lazyDomains[domain] = false
	p.registerEndpointAliasLocked(domain)
	p.mu.Unlock()
	log.Printf("[Router] Registered Internal Shim: %s", domain)
}

// RegisterLazyDocker marks a domain for lazy Docker-backed orchestration.
// On first request, the orchestrator boots the container and dynamically wires the proxy.
func (p *ProxyRouter) RegisterLazyDocker(domain string) {
	domain = normalizeDomain(domain)
	p.mu.Lock()
	p.lazyDomains[domain] = true
	p.registerEndpointAliasLocked(domain)
	p.mu.Unlock()
	log.Printf("[Router] Registered Lazy Docker Backend: %s (boots on first request)", domain)
}

func (p *ProxyRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	suppliedPrincipal := callerSuppliedPrincipal(strings.TrimSpace(r.Header.Get("X-MiniSky-Principal")))
	r.Header.Del("X-MiniSky-Principal")
	r = r.WithContext(context.WithValue(r.Context(), callerSuppliedPrincipalContextKey{}, suppliedPrincipal))

	targetDomain := normalizeDomain(r.Host)

	// 1. Support Path-based Routing for local requests (Terraform/CLI)
	if isLocalhost(r.Host) {
		if selector, servicePath, canonical := canonicalEndpoint(r.URL.Path); canonical {
			domain, exists := p.resolveEndpoint(selector)
			if !exists {
				p.writeUnimplemented(w, selector)
				return
			}
			targetDomain = domain
			rewriteRequestPath(r, servicePath)
		} else {
			targetDomain = legacyLocalDomain(r.URL.Path, targetDomain)
		}
		log.Printf("[Router] Path-mapped local request: %s -> %s", r.URL.Path, targetDomain)
	}

	// 2. Subdomain Flattening (e.g. project-id.firebaseio.com -> firebaseio.com)
	if strings.HasSuffix(targetDomain, ".firebaseio.com") {
		targetDomain = "firebaseio.com"
	} else if strings.HasSuffix(targetDomain, ".googleapis.com") {
		// Some services use [service].googleapis.com, we want the base domain if needed
		// But usually we register the full domain. For now, keep as is unless it's a known multi-subdomain service.
	}

	log.Printf("[Router] %s %s%s", r.Method, targetDomain, r.URL.Path)

	if registry.GateExperimentalRequest(w, r, targetDomain) {
		return
	}

	p.mu.RLock()
	gatedHandler := p.routes[targetDomain]
	p.mu.RUnlock()
	if registry.IsExperimentalDisabled(gatedHandler) {
		gatedHandler.ServeHTTP(w, r)
		return
	}
	if targetDomain == "vision.googleapis.com" &&
		r.Method == http.MethodPost &&
		r.URL.Path == "/v1/images:annotate" &&
		projectFromRequest(r) == "" {
		p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"X-Goog-User-Project is required for projectless images:annotate")
		return
	}
	if registry.IsExperimentalService(targetDomain) &&
		strings.Contains(path.Base(r.URL.Path), ":") {
		permission, _ := routePermission(targetDomain, r)
		if permission == "" {
			p.writeUnimplemented(w, targetDomain+" custom method")
			return
		}
	}

	authorized := false
	// Exact BigQuery completion candidates run the same authentication and
	// authorization decision as ordinary dispatch before durable replay state
	// is touched. Unknown and incomplete sessions continue through normal body
	// enforcement without executing authorization twice.
	p.mu.RLock()
	replayHandler := p.routes[targetDomain]
	p.mu.RUnlock()
	if probe, ok := replayHandler.(completedUploadReplayProbe); ok &&
		probe.IsCompletedUploadReplayCandidate(r) {
		if !p.authorizeRequest(w, r, targetDomain) {
			return
		}
		authorized = true
		if !p.validator.ValidateRequestPreBodyForDomain(w, r, targetDomain) {
			return
		}
		if replay, completed := probe.ProbeCompletedUploadReplay(r); completed {
			if !p.validateProject(w, r, targetDomain) {
				return
			}
			if !p.checkQuota(w, r, targetDomain) {
				return
			}
			replay(w)
			return
		}
	}

	// 2. Schema Validation
	cleanupBody, validBody := p.enforceRequestBodyLimit(w, r, targetDomain)
	if !validBody {
		return
	}
	defer cleanupBody()
	if !p.inspectProjectBody(w, r) {
		return
	}
	if !p.validator.ValidateRequestForDomain(w, r, targetDomain) {
		return
	}
	if !authorized && !p.authorizeRequest(w, r, targetDomain) {
		return
	}
	if !p.validateProject(w, r, targetDomain) {
		return
	}
	if !p.checkQuota(w, r, targetDomain) {
		return
	}
	// 2. Check if this is a lazy-loaded Docker backend
	p.mu.RLock()
	isLazy := p.lazyDomains[targetDomain]
	p.mu.RUnlock()

	if isLazy && p.serviceMgr == nil {
		p.writeColdStartUnavailable(w, targetDomain)
		return
	}
	if isLazy && p.serviceMgr != nil {
		internalURL, err := p.serviceMgr.EnsureServiceRunning(r.Context(), targetDomain)
		if err != nil {
			log.Printf("[Router ERROR] Orchestrator failed for '%s': %v", targetDomain, err)
			// Clear the stale wired route so the next request will re-attempt the cold start
			p.mu.Lock()
			delete(p.routes, targetDomain)
			p.mu.Unlock()
			p.writeColdStartUnavailable(w, targetDomain)
			return
		}
		if internalURL != "" {
			// Dynamically wire (or re-wire if container moved IP) the discovered internal URL
			target, _ := url.Parse(internalURL)
			proxy := httputil.NewSingleHostReverseProxy(target)
			proxy.Transport = observability.InstrumentTransport(http.DefaultTransport)
			p.mu.Lock()
			p.routes[targetDomain] = proxy
			p.mu.Unlock()
			log.Printf("[Router] Dynamically wired: %s -> %s", targetDomain, internalURL)
		}
	}

	// 3. Dispatch to handler
	p.mu.RLock()
	handler, exists := p.routes[targetDomain]
	p.mu.RUnlock()

	if !exists {
		p.writeUnimplemented(w, targetDomain)
		return
	}

	handler.ServeHTTP(w, r)
}

func (p *ProxyRouter) enforceRequestBodyLimit(
	w http.ResponseWriter,
	r *http.Request,
	domain string,
) (func(), bool) {
	noCleanup := func() {}
	if r.Body == nil || r.Body == http.NoBody {
		return noCleanup, true
	}
	limit := p.validator.RequestBodyLimit(domain, r.Method, r.URL.Path)
	if r.ContentLength > limit {
		p.writeBodyLimitError(w, r, limit)
		return noCleanup, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	isJSON := strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json")
	if !isMutationMethod(r.Method) {
		return noCleanup, true
	}
	if !isJSON && r.ContentLength < 0 {
		spool, err := spoolUnknownRequestBody(r.Body, limit)
		_ = r.Body.Close()
		if err != nil {
			switch {
			case errors.Is(err, errRequestBodyTooLarge):
				p.writeBodyLimitError(w, r, limit)
			case errors.Is(err, errRequestSpoolQuota):
				p.writeAuthError(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED",
					"Profile aggregate request spool quota exceeded")
			default:
				p.writeAuthError(w, http.StatusInternalServerError, "INTERNAL", "Unable to spool request body")
			}
			return noCleanup, false
		}
		r.Body = spool.file
		r.ContentLength = spool.size
		return spool.Close, true
	}
	if !isJSON {
		return noCleanup, true
	}

	encodedBody, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			p.writeBodyLimitError(w, r, limit)
			return noCleanup, false
		}
		p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to read JSON request body")
		return noCleanup, false
	}
	body := encodedBody
	contentEncoding := strings.TrimSpace(r.Header.Get("Content-Encoding"))
	if contentEncoding != "" {
		if !strings.EqualFold(contentEncoding, "gzip") {
			p.writeAuthError(w, http.StatusUnsupportedMediaType, "INVALID_ARGUMENT",
				"Unsupported Content-Encoding; expected gzip")
			return noCleanup, false
		}
		compressed, gzipErr := gzip.NewReader(bytes.NewReader(encodedBody))
		if gzipErr != nil {
			p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to decode gzip JSON request body")
			return noCleanup, false
		}
		body, err = io.ReadAll(io.LimitReader(compressed, limit+1))
		closeErr := compressed.Close()
		if err != nil || closeErr != nil {
			p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to decode gzip JSON request body")
			return noCleanup, false
		}
		if int64(len(body)) > limit {
			p.writeBodyLimitError(w, r, limit)
			return noCleanup, false
		}
		r.Header.Del("Content-Encoding")
		r.ContentLength = int64(len(body))
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return noCleanup, true
}

func (p *ProxyRouter) inspectProjectBody(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil || !strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		return true
	}
	p.mu.RLock()
	authorizer := p.authorizer
	projectValidation := p.enforceProjects && p.projects != nil
	quotaEnabled := p.quota != nil
	p.mu.RUnlock()
	if !projectValidation && !quotaEnabled && (authorizer == nil || !authorizer.EnforcementEnabled()) {
		return true
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			p.writeBodyLimitError(w, r, maxBytesErr.Limit)
			return false
		}
		p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to inspect request body")
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return true
}

func (p *ProxyRouter) checkQuota(w http.ResponseWriter, r *http.Request, domain string) bool {
	p.mu.RLock()
	limiter := p.quota
	observer := p.quotaObserver
	p.mu.RUnlock()
	if limiter == nil {
		return true
	}
	labels := observability.RequestLabels{
		Service: domain,
		Route:   observability.NormalizeRoute(r.URL.Path),
	}
	decision := limiter.Allow(domain, r.URL.Path, projectFromRequest(r))
	if decision.Allowed {
		return true
	}
	if observer != nil {
		observer(labels, decision.Scope)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.Itoa(quotaRetryAfterSeconds(decision.RetryAfter)))
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code":    http.StatusTooManyRequests,
		"status":  "RESOURCE_EXHAUSTED",
		"message": "MiniSky local " + decision.Scope + " quota exceeded",
	}})
	return false
}

func (p *ProxyRouter) authorizeRequest(w http.ResponseWriter, r *http.Request, domain string) bool {
	p.mu.RLock()
	authorizer := p.authorizer
	audience := p.tokenAudience
	p.mu.RUnlock()
	if authorizer == nil || !authorizer.EnforcementEnabled() {
		return true
	}
	suppliedPrincipal, _ := r.Context().Value(callerSuppliedPrincipalContextKey{}).(callerSuppliedPrincipal)
	if strictIAMPublicExemption(domain, r) {
		return true
	}

	principal := ""
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if authorization != "" {
		scheme, token, ok := strings.Cut(authorization, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			p.writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer authentication is malformed")
			return false
		}
		claims, err := authorizer.VerifyLocalToken(strings.TrimSpace(token), audience, requiredOAuthScope(domain))
		if err != nil {
			p.writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer credential is invalid or expired")
			return false
		}
		principal = strings.TrimSpace(claims.Subject)
		if principal == "" || suppliedPrincipal != "" && string(suppliedPrincipal) != principal {
			p.writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer identity conflicts with the supplied principal")
			return false
		}
		r.Header.Set("X-MiniSky-Principal", principal)
	}
	if principal == "" {
		p.writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication is required in strict IAM mode")
		return false
	}
	if domain == "iamcredentials.googleapis.com" &&
		r.Method == http.MethodPost &&
		strings.HasPrefix(r.URL.Path, "/v1/projects/") &&
		strings.HasSuffix(r.URL.Path, ":generateAccessToken") {
		return true
	}

	permission, resource := routePermission(domain, r)
	if permission == "" {
		p.writeAuthError(w, http.StatusForbidden, "PERMISSION_DENIED", "Strict IAM denied unmapped route")
		return false
	}
	if !authorizer.Authorize(resource, principal, permission) {
		p.writeAuthError(w, http.StatusForbidden, "PERMISSION_DENIED", "Caller lacks permission "+permission)
		return false
	}
	if domain == "pubsub.googleapis.com" && permission == "pubsub.subscriptions.create" {
		topic, topicProject, valid := pubsubTopicFromBody(r)
		if !valid {
			p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
				"field 'topic' must use projects/{project}/topics/{topic}")
			return false
		}
		subscriptionProject := projectFromRequest(r)
		if topicProject != subscriptionProject &&
			!authorizer.Authorize(topic, principal, "pubsub.topics.attachSubscription") {
			p.writeAuthError(w, http.StatusForbidden, "PERMISSION_DENIED",
				"Caller lacks permission pubsub.topics.attachSubscription")
			return false
		}
	}
	if domain == "eventarc.googleapis.com" && permission == "eventarc.triggers.create" {
		workflow, workflowProject, valid := eventarcWorkflowFromBody(r)
		triggerProject := projectFromRequest(r)
		if workflow != "" && !valid {
			p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
				"field 'destination.workflow' must use projects/{project}/locations/{location}/workflows/{workflow}")
			return false
		}
		if workflowProject != "" && workflowProject != triggerProject &&
			!authorizer.Authorize(workflow, principal, "workflows.executions.create") {
			p.writeAuthError(w, http.StatusForbidden, "PERMISSION_DENIED",
				"Caller lacks permission workflows.executions.create on cross-project workflow target")
			return false
		}
	}
	return true
}

func strictIAMPublicExemption(domain string, r *http.Request) bool {
	return domain == "sts.googleapis.com" &&
		r.Method == http.MethodPost &&
		r.URL.Path == "/v1/token"
}

func pubsubTopicFromBody(r *http.Request) (topic, project string, valid bool) {
	if r.Body == nil {
		return "", "", false
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", "", false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var input struct {
		Topic string `json:"topic"`
	}
	if json.Unmarshal(body, &input) != nil {
		return "", "", false
	}
	parts := strings.Split(input.Topic, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[1] == "" ||
		parts[2] != "topics" || parts[3] == "" {
		return input.Topic, "", false
	}
	return input.Topic, parts[1], true
}

func eventarcWorkflowFromBody(r *http.Request) (workflow, project string, valid bool) {
	if r.Body == nil {
		return "", "", true
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", "", false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var input struct {
		Destination struct {
			Workflow string `json:"workflow"`
		} `json:"destination"`
	}
	if json.Unmarshal(body, &input) != nil || input.Destination.Workflow == "" {
		return "", "", true
	}
	parts := strings.Split(strings.Trim(input.Destination.Workflow, "/"), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[1] == "" ||
		parts[2] != "locations" || parts[3] == "" || parts[4] != "workflows" || parts[5] == "" {
		return input.Destination.Workflow, "", false
	}
	return input.Destination.Workflow, parts[1], true
}

func (p *ProxyRouter) validateProject(w http.ResponseWriter, r *http.Request, domain string) bool {
	p.mu.RLock()
	projects := p.projects
	enabled := p.enforceProjects
	p.mu.RUnlock()
	if !enabled || projects == nil || domain == "cloudresourcemanager.googleapis.com" {
		return true
	}
	requestProjects := projectsFromRequest(r)
	for _, project := range requestProjects {
		if project != "" && !projects.Exists(project) {
			p.writeAuthError(w, http.StatusNotFound, "NOT_FOUND", "Project "+project+" does not exist")
			return false
		}
	}
	return true
}

func routePermission(domain string, r *http.Request) (string, string) {
	project := projectFromRequest(r)
	resource := "projects/" + project
	path := strings.TrimSuffix(r.URL.Path, "/")
	for _, route := range strictIAMCustomRoutes {
		if route.domain == domain && route.method == r.Method && matchIAMRouteTemplate(path, route.template) {
			if domain == "pubsub.googleapis.com" && route.permission == "pubsub.topics.publish" {
				topic := strings.TrimPrefix(path, "/v1/")
				topic = strings.TrimPrefix(topic, "/")
				return route.permission, strings.TrimSuffix(topic, ":publish")
			}
			return route.permission, resource
		}
	}
	specs := strictIAMResourceRoutes[domain]
	for _, spec := range specs {
		for _, collectionTemplate := range spec.collectionTemplates {
			switch {
			case matchIAMRouteTemplate(path, collectionTemplate):
				switch r.Method {
				case http.MethodGet, http.MethodHead:
					return spec.permissionRoot + ".list", resource
				case http.MethodPost:
					return spec.permissionRoot + ".create", resource
				}
			case matchIAMRouteTemplate(path, collectionTemplate+"/{resource}"):
				switch r.Method {
				case http.MethodGet, http.MethodHead:
					return spec.permissionRoot + ".get", resource
				case http.MethodPut:
					if spec.putCreates {
						return spec.permissionRoot + ".create", resource
					}
					return spec.permissionRoot + ".update", resource
				case http.MethodPatch:
					return spec.permissionRoot + ".update", resource
				case http.MethodDelete:
					return spec.permissionRoot + ".delete", resource
				}
			}
		}
	}
	if permission := strictIAMRootRoutePermission(domain, r.Method, path); permission != "" {
		return permission, resource
	}
	return "", resource
}

type strictIAMResourceRoute struct {
	collectionTemplates []string
	permissionRoot      string
	putCreates          bool
}

type strictIAMCustomRoute struct {
	domain     string
	method     string
	template   string
	permission string
}

var strictIAMResourceRoutes = map[string][]strictIAMResourceRoute{
	"accesscontextmanager.googleapis.com": {
		{[]string{"/v1/accessPolicies"}, "accesscontextmanager.accessPolicies", false},
		{[]string{"/v1/accessPolicies/{policy}/accessLevels"}, "accesscontextmanager.accessLevels", false},
		{[]string{"/v1/accessPolicies/{policy}/servicePerimeters"}, "accesscontextmanager.servicePerimeters", false},
	},
	"aiplatform.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/endpoints"}, "aiplatform.endpoints", false},
		{[]string{"/v1/projects/{project}/locations/{location}/indexes"}, "aiplatform.indexes", false},
	},
	"alloydb.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/clusters"}, "alloydb.clusters", false},
		{[]string{"/v1/projects/{project}/locations/{location}/clusters/{cluster}/instances"}, "alloydb.instances", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "alloydb.operations", false},
	},
	"apigateway.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/gateways"}, "apigateway.gateways", false},
		{[]string{"/v1/projects/{project}/locations/{location}/apis"}, "apigateway.apis", false},
		{[]string{"/v1/projects/{project}/locations/{location}/apis/{api}/configs"}, "apigateway.apiConfigs", false},
	},
	"appengine.googleapis.com": {
		{[]string{"/v1/projects/{project}/apps/{app}/services"}, "appengine.services", false},
		{[]string{"/v1/projects/{project}/apps/{app}/services/{service}/versions"}, "appengine.versions", false},
		{[]string{"/v1/projects/{project}/apps/{app}/operations"}, "appengine.applications", false},
	},
	"artifactregistry.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/repositories"}, "artifactregistry.repositories", false},
		{[]string{"/v1/projects/{project}/locations/{location}/repositories/{repository}/packages"}, "artifactregistry.packages", false},
		{[]string{"/v1/projects/{project}/locations/{location}/repositories/{repository}/packages/{package}/versions"}, "artifactregistry.versions", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "artifactregistry.repositories", false},
	},
	"batch.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/jobs"}, "batch.jobs", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "batch.operations", false},
	},
	"bigquery.googleapis.com": {
		{[]string{"/bigquery/v2/projects/{project}/datasets", "/v2/projects/{project}/datasets"}, "bigquery.datasets", false},
		{[]string{"/bigquery/v2/projects/{project}/datasets/{dataset}/tables", "/v2/projects/{project}/datasets/{dataset}/tables"}, "bigquery.tables", false},
		{[]string{"/bigquery/v2/projects/{project}/jobs", "/v2/projects/{project}/jobs"}, "bigquery.jobs", false},
	},
	"bigtable.googleapis.com": {
		{[]string{"/v2/projects/{project}/instances/{instance}/tables"}, "bigtable.tables", false},
	},
	"bigtableadmin.googleapis.com": {
		{[]string{"/v2/projects/{project}/instances"}, "bigtable.instances", false},
		{[]string{"/v2/projects/{project}/instances/{instance}/clusters"}, "bigtable.clusters", false},
		{[]string{"/v2/projects/{project}/instances/{instance}/tables"}, "bigtable.tables", false},
	},
	"cloudasset.googleapis.com": {
		{[]string{"/v1/projects/{project}/assets"}, "cloudasset.assets", false},
	},
	"cloudbuild.googleapis.com": {
		{[]string{"/v1/projects/{project}/builds", "/v1/projects/{project}/locations/{location}/builds"}, "cloudbuild.builds", false},
		{[]string{"/v1/projects/{project}/triggers"}, "cloudbuild.builds", false},
	},
	"clouddeploy.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/deliveryPipelines"}, "clouddeploy.deliveryPipelines", false},
		{[]string{"/v1/projects/{project}/locations/{location}/deliveryPipelines/{pipeline}/releases"}, "clouddeploy.releases", false},
		{[]string{"/v1/projects/{project}/locations/{location}/deliveryPipelines/{pipeline}/releases/{release}/rollouts"}, "clouddeploy.rollouts", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "clouddeploy.operations", false},
	},
	"clouderrorreporting.googleapis.com": {
		{[]string{"/v1beta1/projects/{project}/events"}, "clouderrorreporting.errorEvents", false},
		{[]string{"/v1beta1/projects/{project}/groupStats"}, "clouderrorreporting.errorGroupStats", false},
	},
	"cloudfunctions.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/functions", "/v2/projects/{project}/locations/{location}/functions"}, "cloudfunctions.functions", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations", "/v2/projects/{project}/locations/{location}/operations"}, "cloudfunctions.functions", false},
	},
	"cloudkms.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/keyRings"}, "cloudkms.keyRings", false},
		{[]string{"/v1/projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys"}, "cloudkms.cryptoKeys", false},
		{[]string{"/v1/projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}/cryptoKeyVersions"}, "cloudkms.cryptoKeyVersions", false},
	},
	"cloudprofiler.googleapis.com": {
		{[]string{"/v2/projects/{project}/profiles"}, "cloudprofiler.profiles", false},
	},
	"cloudresourcemanager.googleapis.com": {
		{[]string{"/v3/projects"}, "resourcemanager.projects", false},
		{[]string{"/v3/folders"}, "resourcemanager.folders", false},
		{[]string{"/v3/organizations"}, "resourcemanager.organizations", false},
	},
	"cloudscheduler.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/jobs"}, "cloudscheduler.jobs", false},
	},
	"cloudtasks.googleapis.com": {
		{[]string{"/v2/projects/{project}/locations/{location}/queues"}, "cloudtasks.queues", false},
		{[]string{"/v2/projects/{project}/locations/{location}/queues/{queue}/tasks"}, "cloudtasks.tasks", false},
	},
	"cloudtrace.googleapis.com": {
		{[]string{"/v2/projects/{project}/traces"}, "cloudtrace.traces", false},
	},
	"composer.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/environments"}, "composer.environments", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "composer.operations", false},
	},
	"compute.googleapis.com": {
		{[]string{"/compute/v1/projects/{project}/zones"}, "compute.zones", false},
		{[]string{"/compute/v1/projects/{project}/zones/{zone}/instances"}, "compute.instances", false},
		{[]string{"/compute/v1/projects/{project}/zones/{zone}/instanceGroups"}, "compute.instanceGroups", false},
		{[]string{"/compute/v1/projects/{project}/regions/{region}/subnetworks"}, "compute.subnetworks", false},
		{[]string{"/compute/v1/projects/{project}/global/networks"}, "compute.networks", false},
		{[]string{"/compute/v1/projects/{project}/global/firewalls"}, "compute.firewalls", false},
		{[]string{"/compute/v1/projects/{project}/global/securityPolicies"}, "compute.securityPolicies", false},
		{[]string{"/compute/v1/projects/{project}/global/backendServices"}, "compute.backendServices", false},
		{[]string{"/compute/v1/projects/{project}/global/healthChecks"}, "compute.healthChecks", false},
		{[]string{"/compute/v1/projects/{project}/global/urlMaps"}, "compute.urlMaps", false},
		{[]string{"/compute/v1/projects/{project}/global/targetHttpProxies"}, "compute.targetHttpProxies", false},
		{[]string{"/compute/v1/projects/{project}/global/forwardingRules"}, "compute.forwardingRules", false},
		{[]string{"/compute/v1/projects/{project}/global/images"}, "compute.images", false},
		{[]string{"/compute/v1/projects/{project}/zones/{zone}/operations", "/compute/v1/projects/{project}/regions/{region}/operations", "/compute/v1/projects/{project}/global/operations"}, "compute.operations", false},
	},
	"container.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/clusters", "/v1/projects/{project}/zones/{zone}/clusters"}, "container.clusters", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations", "/v1/projects/{project}/zones/{zone}/operations"}, "container.operations", false},
	},
	"dataflow.googleapis.com": {
		{[]string{"/v1b3/projects/{project}/locations/{location}/jobs"}, "dataflow.jobs", false},
	},
	"dataform.googleapis.com": {
		{[]string{"/v1beta1/projects/{project}/locations/{location}/repositories"}, "dataform.repositories", false},
		{[]string{"/v1beta1/projects/{project}/locations/{location}/repositories/{repository}/workspaces"}, "dataform.workspaces", false},
	},
	"dataproc.googleapis.com": {
		{[]string{"/v1/projects/{project}/regions/{region}/clusters"}, "dataproc.clusters", false},
		{[]string{"/v1/projects/{project}/regions/{region}/jobs"}, "dataproc.jobs", false},
		{[]string{"/v1/projects/{project}/regions/{region}/operations"}, "dataproc.operations", false},
	},
	"dlp.googleapis.com": {
		{[]string{"/v2/projects/{project}/inspectTemplates"}, "dlp.inspectTemplates", false},
		{[]string{"/v2/projects/{project}/deidentifyTemplates"}, "dlp.deidentifyTemplates", false},
		{[]string{"/v2/projects/{project}/jobTriggers"}, "dlp.jobTriggers", false},
	},
	"dns.googleapis.com": {
		{[]string{"/dns/v1/projects/{project}/managedZones"}, "dns.managedZones", false},
		{[]string{"/dns/v1/projects/{project}/managedZones/{zone}/rrsets"}, "dns.resourceRecordSets", false},
		{[]string{"/dns/v1/projects/{project}/managedZones/{zone}/changes"}, "dns.changes", false},
	},
	"documentai.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/processors"}, "documentai.processors", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "documentai.operations", false},
	},
	"dialogflow.googleapis.com": {
		{[]string{"/v3/projects/{project}/locations/{location}/agents"}, "dialogflow.agents", false},
	},
	"eventarc.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/triggers"}, "eventarc.triggers", false},
		{[]string{"/v1/projects/{project}/locations/{location}/channels"}, "eventarc.channels", false},
	},
	"file.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/instances"}, "file.instances", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "file.operations", false},
	},
	"firebasehosting.googleapis.com": {
		{[]string{"/v1beta1/projects/{project}/sites"}, "firebasehosting.sites", false},
		{[]string{"/v1beta1/sites/{site}/releases"}, "firebasehosting.releases", false},
		{[]string{"/v1beta1/sites/{site}/versions"}, "firebasehosting.versions", false},
	},
	"firestore.googleapis.com": {
		{[]string{"/v1/projects/{project}/databases/{database}/documents"}, "datastore.entities", false},
		{[]string{"/v1/projects/{project}/databases/{database}/collectionGroups/{group}/indexes"}, "datastore.indexes", false},
	},
	"iam.googleapis.com": {
		{[]string{"/v1/projects/{project}/serviceAccounts"}, "iam.serviceAccounts", false},
		{[]string{"/v1/projects/{project}/serviceAccounts/{account}/keys"}, "iam.serviceAccountKeys", false},
		{[]string{"/v1/projects/{project}/locations/{location}/workloadIdentityPools"}, "iam.workloadIdentityPools", false},
		{[]string{"/v1/projects/{project}/locations/{location}/workloadIdentityPools/{pool}/providers"}, "iam.workloadIdentityPoolProviders", false},
	},
	"identityplatform.googleapis.com": {
		{[]string{"/v2/projects/{project}/tenants"}, "identityplatform.tenants", false},
		{[]string{"/v2/projects/{project}/oauthIdpConfigs"}, "identityplatform.oauthIdpConfigs", false},
		{[]string{"/v2/projects/{project}/tenants/{tenant}/oauthIdpConfigs"}, "identityplatform.oauthIdpConfigs", false},
	},
	"logging.googleapis.com": {
		{[]string{"/v2/projects/{project}/sinks"}, "logging.sinks", false},
	},
	"managedkafka.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/clusters"}, "managedkafka.clusters", false},
		{[]string{"/v1/projects/{project}/locations/{location}/clusters/{cluster}/topics"}, "managedkafka.topics", false},
	},
	"monitoring.googleapis.com": {
		{[]string{"/v3/projects/{project}/metricDescriptors"}, "monitoring.metricDescriptors", false},
		{[]string{"/v3/projects/{project}/timeSeries"}, "monitoring.timeSeries", false},
	},
	"networkservices.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/meshes"}, "networkservices.meshes", false},
		{[]string{"/v1/projects/{project}/locations/{location}/httpRoutes"}, "networkservices.httpRoutes", false},
	},
	"networksecurity.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/authorizationPolicies"}, "networksecurity.authorizationPolicies", false},
		{[]string{"/v1/projects/{project}/locations/{location}/serverTlsPolicies"}, "networksecurity.serverTlsPolicies", false},
	},
	"orgpolicy.googleapis.com": {
		{[]string{"/v2/projects/{project}/policies"}, "orgpolicy.policies", false},
	},
	"pubsub.googleapis.com": {
		{[]string{"/v1/projects/{project}/topics", "/projects/{project}/topics"}, "pubsub.topics", true},
		{[]string{"/v1/projects/{project}/subscriptions", "/projects/{project}/subscriptions"}, "pubsub.subscriptions", true},
		{[]string{"/v1/projects/{project}/snapshots", "/projects/{project}/snapshots"}, "pubsub.snapshots", true},
	},
	"pubsublite.googleapis.com": {
		{[]string{"/v1/admin/projects/{project}/locations/{location}/topics"}, "pubsublite.topics", false},
		{[]string{"/v1/admin/projects/{project}/locations/{location}/subscriptions"}, "pubsublite.subscriptions", false},
		{[]string{"/v1/admin/projects/{project}/locations/{location}/reservations"}, "pubsublite.reservations", false},
	},
	"redis.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/instances"}, "redis.instances", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "redis.operations", false},
	},
	"run.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/services", "/v2/projects/{project}/locations/{location}/services"}, "run.services", false},
		{[]string{"/v2/projects/{project}/locations/{location}/jobs"}, "run.jobs", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations", "/v2/projects/{project}/locations/{location}/operations"}, "run.operations", false},
	},
	"secretmanager.googleapis.com": {
		{[]string{"/v1/projects/{project}/secrets"}, "secretmanager.secrets", false},
		{[]string{"/v1/projects/{project}/secrets/{secret}/versions"}, "secretmanager.versions", false},
	},
	"servicedirectory.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/namespaces"}, "servicedirectory.namespaces", false},
		{[]string{"/v1/projects/{project}/locations/{location}/namespaces/{namespace}/services"}, "servicedirectory.services", false},
		{[]string{"/v1/projects/{project}/locations/{location}/namespaces/{namespace}/services/{service}/endpoints"}, "servicedirectory.endpoints", false},
	},
	"spanner.googleapis.com": {
		{[]string{"/v1/projects/{project}/instances"}, "spanner.instances", false},
		{[]string{"/v1/projects/{project}/instances/{instance}/databases"}, "spanner.databases", false},
		{[]string{"/v1/projects/{project}/instances/{instance}/databases/{database}/sessions"}, "spanner.sessions", false},
	},
	"sqladmin.googleapis.com": {
		{[]string{"/sql/v1beta4/projects/{project}/instances"}, "cloudsql.instances", false},
		{[]string{"/sql/v1beta4/projects/{project}/instances/{instance}/databases"}, "cloudsql.databases", false},
		{[]string{"/sql/v1beta4/projects/{project}/instances/{instance}/users"}, "cloudsql.users", false},
		{[]string{"/sql/v1beta4/projects/{project}/operations"}, "cloudsql.operations", false},
	},
	"storage.googleapis.com": {
		{[]string{"/storage/v1/b"}, "storage.buckets", false},
		{[]string{"/storage/v1/b/{bucket}/o"}, "storage.objects", false},
	},
	"storagetransfer.googleapis.com": {
		{[]string{"/v1/transferJobs"}, "storagetransfer.transferJobs", false},
		{[]string{"/v1/transferOperations"}, "storagetransfer.transferOperations", false},
	},
	"translate.googleapis.com": {
		{[]string{"/v3/projects/{project}/locations/{location}/glossaries"}, "translate.glossaries", false},
	},
	"workflows.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/workflows"}, "workflows.workflows", false},
		{[]string{"/v1/projects/{project}/locations/{location}/operations"}, "workflows.operations", false},
	},
	"workflowexecutions.googleapis.com": {
		{[]string{"/v1/projects/{project}/locations/{location}/workflows/{workflow}/executions"}, "workflows.executions", false},
	},
}

var strictIAMCustomRoutes = []strictIAMCustomRoute{
	{"aiplatform.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/endpoints/{endpoint}:predict", "aiplatform.endpoints.predict"},
	{"aiplatform.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/publishers/{publisher}/models/{model}:generateContent", "aiplatform.endpoints.predict"},
	{"aiplatform.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/publishers/{publisher}/models/{model}:streamGenerateContent", "aiplatform.endpoints.predict"},
	{"aiplatform.googleapis.com", http.MethodGet, "/v1/internal/config", "aiplatform.endpoints.get"},
	{"aiplatform.googleapis.com", http.MethodPost, "/v1/internal/config", "aiplatform.endpoints.update"},
	{"aiplatform.googleapis.com", http.MethodGet, "/v1/internal/models", "aiplatform.models.list"},
	{"aiplatform.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/models:upload", "aiplatform.models.upload"},
	{"aiplatform.googleapis.com", http.MethodGet, "/v1/projects/{project}/locations/{location}/models", "aiplatform.models.list"},
	{"aiplatform.googleapis.com", http.MethodGet, "/v1/projects/{project}/locations/{location}/models/{model}", "aiplatform.models.get"},
	{"aiplatform.googleapis.com", http.MethodPatch, "/v1/projects/{project}/locations/{location}/models/{model}", "aiplatform.models.update"},
	{"aiplatform.googleapis.com", http.MethodDelete, "/v1/projects/{project}/locations/{location}/models/{model}", "aiplatform.models.delete"},
	{"aiplatform.googleapis.com", http.MethodGet, "/v1/projects/{project}/locations/{location}/operations/{operation}", "aiplatform.operations.get"},
	{"aiplatform.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/indexEndpoints/{indexEndpoint}:findNeighbors", "aiplatform.indexEndpoints.findNeighbors"},
	{"appengine.googleapis.com", http.MethodGet, "/v1/projects/{project}/apps", "appengine.applications.get"},
	{"appengine.googleapis.com", http.MethodPost, "/v1/projects/{project}/apps", "appengine.applications.create"},
	{"appengine.googleapis.com", http.MethodPost, "/deploy", "appengine.versions.create"},
	{"bigquery.googleapis.com", http.MethodGet, "/bigquery/v2/projects/{project}/queries/{job}", "bigquery.jobs.get"},
	{"bigquery.googleapis.com", http.MethodGet, "/bigquery/v2/projects/{project}/jobs/{job}/results", "bigquery.jobs.get"},
	{"bigquery.googleapis.com", http.MethodPost, "/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}/insertAll", "bigquery.tables.updateData"},
	{"bigquery.googleapis.com", http.MethodPost, "/upload/bigquery/v2/projects/{project}/jobs", "bigquery.jobs.create"},
	{"bigquery.googleapis.com", http.MethodPut, "/upload/bigquery/v2/projects/{project}/jobs", "bigquery.jobs.create"},
	{"bigtable.googleapis.com", http.MethodPost, "/v2/projects/{project}/instances/{instance}/tables/{table}:readRows", "bigtable.tables.readRows"},
	{"bigtableadmin.googleapis.com", http.MethodGet, "/v2/operations/{operation}", "bigtable.instances.get"},
	{"cloudbuild.googleapis.com", http.MethodPost, "/v1/projects/{project}/triggers/{trigger}:run", "cloudbuild.builds.create"},
	{"cloudbuild.googleapis.com", http.MethodPost, "/v1/projects/{project}/builds/{build}:cancel", "cloudbuild.builds.update"},
	{"cloudfunctions.googleapis.com", http.MethodPost, "/v2/deploy", "cloudfunctions.functions.create"},
	{"cloudfunctions.googleapis.com", http.MethodDelete, "/v2/delete", "cloudfunctions.functions.delete"},
	{"cloudfunctions.googleapis.com", http.MethodGet, "/v2/logs/{resource}", "cloudfunctions.functions.get"},
	{"cloudkms.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}:encrypt", "cloudkms.cryptoKeyVersions.useToEncrypt"},
	{"cloudkms.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}:decrypt", "cloudkms.cryptoKeyVersions.useToDecrypt"},
	{"cloudkms.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{cryptoKey}/cryptoKeyVersions/{version}:destroy", "cloudkms.cryptoKeyVersions.destroy"},
	{"cloudscheduler.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/jobs/{job}:run", "cloudscheduler.jobs.run"},
	{"cloudscheduler.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/jobs/{job}:pause", "cloudscheduler.jobs.pause"},
	{"cloudscheduler.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/jobs/{job}:resume", "cloudscheduler.jobs.resume"},
	{"compute.googleapis.com", http.MethodPost, "/compute/v1/projects/{project}/zones/{zone}/instances/{instance}/start", "compute.instances.start"},
	{"compute.googleapis.com", http.MethodPost, "/compute/v1/projects/{project}/zones/{zone}/instances/{instance}/stop", "compute.instances.stop"},
	{"compute.googleapis.com", http.MethodPost, "/compute/v1/projects/{project}/zones/{zone}/instanceGroups/{group}/addInstances", "compute.instanceGroups.update"},
	{"compute.googleapis.com", http.MethodPost, "/compute/v1/projects/{project}/zones/{zone}/instanceGroups/{group}/setNamedPorts", "compute.instanceGroups.update"},
	{"compute.googleapis.com", http.MethodPost, "/compute/v1/projects/{project}/zones/{zone}/instanceGroups/{group}/listInstances", "compute.instanceGroups.get"},
	{"compute.googleapis.com", http.MethodGet, "/compute/v1/projects/{project}/global/images/family/{family}", "compute.images.get"},
	{"dataproc.googleapis.com", http.MethodPost, "/v1/projects/{project}/regions/{region}/jobs:submit", "dataproc.jobs.submit"},
	{"dataproc.googleapis.com", http.MethodPost, "/v1/projects/{project}/regions/{region}/jobs/{job}:cancel", "dataproc.jobs.cancel"},
	{"dialogflow.googleapis.com", http.MethodPost, "/v3/projects/{project}/locations/{location}/agents/{agent}/sessions/{session}:detectIntent", "dialogflow.sessions.detectIntent"},
	{"datastore.googleapis.com", http.MethodPost, "/v1/projects/{project}:runQuery", "datastore.entities.list"},
	{"datastore.googleapis.com", http.MethodPost, "/v1/projects/{project}:lookup", "datastore.entities.get"},
	{"datastore.googleapis.com", http.MethodPost, "/v1/projects/{project}:commit", "datastore.entities.update"},
	{"datastore.googleapis.com", http.MethodPost, "/v1/projects/{project}:allocateIds", "datastore.entities.create"},
	{"datastore.googleapis.com", http.MethodPost, "/v1/projects/{project}:beginTransaction", "datastore.entities.get"},
	{"datastore.googleapis.com", http.MethodPost, "/v1/projects/{project}:rollback", "datastore.entities.update"},
	{"firestore.googleapis.com", http.MethodPost, "/v1/projects/{project}/databases/{database}/documents:runQuery", "datastore.entities.list"},
	{"firestore.googleapis.com", http.MethodPost, "/v1/projects/{project}/databases/{database}/documents:batchGet", "datastore.entities.get"},
	{"firestore.googleapis.com", http.MethodPost, "/v1/projects/{project}/databases/{database}/documents:commit", "datastore.entities.update"},
	{"firestore.googleapis.com", http.MethodPost, "/v1/projects/{project}/databases/{database}/documents:batchWrite", "datastore.entities.update"},
	{"firestore.googleapis.com", http.MethodPost, "/v1/projects/{project}/databases/{database}/documents:listCollectionIds", "datastore.entities.list"},
	{"firestore.googleapis.com", http.MethodGet, "/v1/projects/{project}/databases/{database}/documents/{document...}", "datastore.entities.get"},
	{"firestore.googleapis.com", http.MethodPost, "/v1/projects/{project}/databases/{database}/documents/{parent...}", "datastore.entities.create"},
	{"firestore.googleapis.com", http.MethodPatch, "/v1/projects/{project}/databases/{database}/documents/{document...}", "datastore.entities.update"},
	{"firestore.googleapis.com", http.MethodDelete, "/v1/projects/{project}/databases/{database}/documents/{document...}", "datastore.entities.delete"},
	{"iam.googleapis.com", http.MethodPost, "/v1/projects/{project}/serviceAccounts/{account}:setIamPolicy", "iam.serviceAccounts.setIamPolicy"},
	{"iam.googleapis.com", http.MethodGet, "/v1/projects/{project}/serviceAccounts/{account}:getIamPolicy", "iam.serviceAccounts.get"},
	{"iam.googleapis.com", http.MethodPost, "/v1/projects/{project}/serviceAccounts/{account}:testIamPermissions", "iam.serviceAccounts.get"},
	{"iam.googleapis.com", http.MethodPost, "/v1/projects/{project}/serviceAccounts/{account}/keys/{key}:disable", "iam.serviceAccountKeys.disable"},
	{"iamcredentials.googleapis.com", http.MethodPost, "/v1/projects/{project}/serviceAccounts/{account}:generateAccessToken", "iam.serviceAccounts.getAccessToken"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:lookup", "firebaseauth.users.get"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:signUp", "firebaseauth.users.create"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:update", "firebaseauth.users.update"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:delete", "firebaseauth.users.delete"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:signInWithPassword", "firebaseauth.users.get"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:signInWithCustomToken", "firebaseauth.users.get"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:signInWithIdp", "firebaseauth.users.get"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:sendOobCode", "firebaseauth.users.get"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:resetPassword", "firebaseauth.users.update"},
	{"identitytoolkit.googleapis.com", http.MethodPost, "/v1/accounts:createAuthUri", "firebaseauth.users.get"},
	{"logging.googleapis.com", http.MethodPost, "/v2/entries:list", "logging.logEntries.list"},
	{"logging.googleapis.com", http.MethodPost, "/v2/entries:write", "logging.logEntries.create"},
	{"language.googleapis.com", http.MethodPost, "/v1/documents:analyzeSentiment", "language.documents.analyzeSentiment"},
	{"language.googleapis.com", http.MethodPost, "/v1/documents:analyzeEntities", "language.documents.analyzeEntities"},
	{"language.googleapis.com", http.MethodPost, "/v1/documents:analyzeSyntax", "language.documents.analyzeSyntax"},
	{"language.googleapis.com", http.MethodPost, "/v1/documents:annotateText", "language.documents.annotateText"},
	{"language.googleapis.com", http.MethodPost, "/v1/documents:classifyText", "language.documents.classifyText"},
	{"language.googleapis.com", http.MethodPost, "/v1/documents:moderateText", "language.documents.moderateText"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/project/project-id", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/project/numeric-project-id", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/project/attributes", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/project/attributes/{attribute}", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/name", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/id", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/zone", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/machine-type", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/hostname", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/attributes", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/attributes/{attribute}", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/service-accounts", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/service-accounts/{account}/email", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/service-accounts/{account}/aliases", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/service-accounts/{account}/scopes", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/service-accounts/{account}/token", "compute.instances.get"},
	{"metadata.google.internal", http.MethodGet, "/computeMetadata/v1/instance/service-accounts/{account}/identity", "compute.instances.get"},
	{"monitoring.googleapis.com", http.MethodPost, "/v3/projects/{project}/timeSeries:query", "monitoring.timeSeries.list"},
	{"monitoring.googleapis.com", http.MethodGet, "/v1/projects/{project}/location/{location}/prometheus/api/v1/query", "monitoring.timeSeries.list"},
	{"monitoring.googleapis.com", http.MethodPost, "/v1/projects/{project}/location/{location}/prometheus/api/v1/query", "monitoring.timeSeries.list"},
	{"monitoring.googleapis.com", http.MethodGet, "/v1/projects/{project}/location/{location}/prometheus/api/v1/query_range", "monitoring.timeSeries.list"},
	{"pubsub.googleapis.com", http.MethodPost, "/v1/projects/{project}/topics/{topic}:publish", "pubsub.topics.publish"},
	{"pubsub.googleapis.com", http.MethodPost, "/projects/{project}/topics/{topic}:publish", "pubsub.topics.publish"},
	{"pubsub.googleapis.com", http.MethodPost, "/v1/projects/{project}/subscriptions/{subscription}:pull", "pubsub.subscriptions.consume"},
	{"pubsub.googleapis.com", http.MethodPost, "/v1/projects/{project}/subscriptions/{subscription}:acknowledge", "pubsub.subscriptions.consume"},
	{"pubsub.googleapis.com", http.MethodPost, "/v1/projects/{project}/subscriptions/{subscription}:modifyAckDeadline", "pubsub.subscriptions.consume"},
	{"run.googleapis.com", http.MethodPost, "/v2/deploy", "run.services.create"},
	{"run.googleapis.com", http.MethodDelete, "/v2/delete", "run.services.delete"},
	{"run.googleapis.com", http.MethodGet, "/v2/logs/{resource}", "run.services.get"},
	{"redis.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/instances/{instance}:export", "redis.instances.export"},
	{"redis.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/instances/{instance}:import", "redis.instances.import"},
	{"redis.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/instances/{instance}:failover", "redis.instances.failover"},
	{"redis.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/instances/{instance}:upgrade", "redis.instances.update"},
	{"secretmanager.googleapis.com", http.MethodPost, "/v1/projects/{project}/secrets/{secret}:addVersion", "secretmanager.versions.add"},
	{"secretmanager.googleapis.com", http.MethodGet, "/v1/projects/{project}/secrets/{secret}/versions/{version}:access", "secretmanager.versions.access"},
	{"spanner.googleapis.com", http.MethodPost, "/v1/projects/{project}/instances/{instance}/databases/{database}/sessions:batchCreate", "spanner.sessions.create"},
	{"spanner.googleapis.com", http.MethodPost, "/v1/projects/{project}/instances/{instance}/databases/{database}/sessions/{session}:executeSql", "spanner.databases.read"},
	{"spanner.googleapis.com", http.MethodPost, "/v1/projects/{project}/instances/{instance}/databases/{database}/sessions/{session}:executeStreamingSql", "spanner.databases.read"},
	{"spanner.googleapis.com", http.MethodPost, "/v1/projects/{project}/instances/{instance}/databases/{database}/sessions/{session}:commit", "spanner.databases.write"},
	{"spanner.googleapis.com", http.MethodPost, "/v1/projects/{project}/instances/{instance}/databases/{database}/sessions/{session}:rollback", "spanner.databases.write"},
	{"storage.googleapis.com", http.MethodPost, "/upload/storage/v1/b/{bucket}/o", "storage.objects.create"},
	{"storage.googleapis.com", http.MethodPut, "/upload/storage/v1/b/{bucket}/o", "storage.objects.create"},
	{"binaryauthorization.googleapis.com", http.MethodGet, "/v1/projects/{project}/policy", "binaryauthorization.policy.get"},
	{"binaryauthorization.googleapis.com", http.MethodPut, "/v1/projects/{project}/policy", "binaryauthorization.policy.update"},
	{"binaryauthorization.googleapis.com", http.MethodPost, "/v1/projects/{project}/policy:evaluate", "binaryauthorization.policy.evaluate"},
	{"privateca.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/caPools/{pool}/certificates", "privateca.certificates.create"},
	{"servicecontrol.googleapis.com", http.MethodPost, "/v1/services/{service}:check", "servicecontrol.services.check"},
	{"servicecontrol.googleapis.com", http.MethodPost, "/v1/services/{service}:report", "servicecontrol.services.report"},
	{"servicemanagement.googleapis.com", http.MethodPost, "/v1/services/{service}/configs", "servicemanagement.services.update"},
	{"servicemanagement.googleapis.com", http.MethodPost, "/v1/services/{service}/rollouts", "servicemanagement.services.update"},
	{"speech.googleapis.com", http.MethodPost, "/v1/speech:recognize", "speech.recognizers.recognize"},
	{"speech.googleapis.com", http.MethodPost, "/v1/speech:longrunningrecognize", "speech.recognizers.recognize"},
	{"texttospeech.googleapis.com", http.MethodPost, "/v1/text:synthesize", "texttospeech.synthesizers.synthesize"},
	// Phase 18-25 custom action routes
	{"cloudtrace.googleapis.com", http.MethodPost, "/v2/projects/{project}/traces:batchWrite", "cloudtrace.traces.batchWrite"},
	{"cloudasset.googleapis.com", http.MethodGet, "/v1/projects/{project}:searchAllResources", "cloudasset.assets.searchAllResources"},
	{"cloudasset.googleapis.com", http.MethodPost, "/v1/projects/{project}:exportAssets", "cloudasset.assets.exportAssets"},
	{"clouderrorreporting.googleapis.com", http.MethodPost, "/v1beta1/projects/{project}/events:report", "clouderrorreporting.events.report"},
	{"dlp.googleapis.com", http.MethodPost, "/v2/projects/{project}/content:inspect", "dlp.content.inspect"},
	{"dlp.googleapis.com", http.MethodPost, "/v2/projects/{project}/content:deidentify", "dlp.content.deidentify"},
	{"documentai.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/processors/{processor}:process", "documentai.processors.process"},
	{"vision.googleapis.com", http.MethodPost, "/v1/images:annotate", "vision.images.annotate"},
	{"translate.googleapis.com", http.MethodPost, "/v3/projects/{project}/locations/{location}:translateText", "translate.locations.translateText"},
	{"translate.googleapis.com", http.MethodGet, "/v3/projects/{project}/locations/{location}/supportedLanguages", "translate.locations.getSupportedLanguages"},
	{"workflowexecutions.googleapis.com", http.MethodPost, "/v1/projects/{project}/locations/{location}/workflows/{workflow}/executions/{execution}:cancel", "workflows.executions.cancel"},
}

func matchIAMRouteTemplate(path, template string) bool {
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	templateParts := strings.Split(strings.Trim(template, "/"), "/")
	variadic := len(templateParts) > 0 && strings.HasSuffix(templateParts[len(templateParts)-1], "...}")
	if variadic {
		if len(pathParts) < len(templateParts) {
			return false
		}
	} else if len(pathParts) != len(templateParts) {
		return false
	}
	for i, expected := range templateParts {
		if variadic && i == len(templateParts)-1 {
			return pathParts[i] != ""
		}
		actual := pathParts[i]
		open := strings.IndexByte(expected, '{')
		close := strings.IndexByte(expected, '}')
		if open < 0 || close < open {
			if actual != expected {
				return false
			}
			continue
		}
		if strings.Count(expected, "{") != 1 || strings.Count(expected, "}") != 1 {
			return false
		}
		prefix, suffix := expected[:open], expected[close+1:]
		if !strings.HasPrefix(actual, prefix) || !strings.HasSuffix(actual, suffix) ||
			len(actual) <= len(prefix)+len(suffix) {
			return false
		}
	}
	return true
}

func strictIAMRootRoutePermission(domain, method, path string) string {
	switch domain {
	case "firebaseio.com":
		if !strings.HasSuffix(path, ".json") {
			return ""
		}
		switch method {
		case http.MethodGet, http.MethodHead:
			return "firebasedatabase.instances.get"
		case http.MethodPut, http.MethodPatch:
			return "firebasedatabase.instances.update"
		case http.MethodDelete:
			return "firebasedatabase.instances.delete"
		}
	case "sts.googleapis.com":
		if method == http.MethodPost && strings.TrimSuffix(path, "/") == "/v1/token" {
			return "iam.serviceAccounts.getAccessToken"
		}
	}
	return ""
}

func isMutationMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}

func requiredOAuthScope(domain string) string {
	switch domain {
	case "storage.googleapis.com":
		return "https://www.googleapis.com/auth/devstorage.full_control"
	default:
		return "https://www.googleapis.com/auth/cloud-platform"
	}
}

func projectFromRequest(r *http.Request) string {
	projects := projectsFromRequest(r)
	if len(projects) > 0 {
		return projects[0]
	}
	return ""
}

func projectsFromRequest(r *http.Request) []string {
	seen := make(map[string]struct{})
	var projects []string
	add := func(project string) {
		project = strings.TrimSpace(strings.TrimSuffix(project, ":"))
		project, _, _ = strings.Cut(project, ":")
		project = strings.TrimSuffix(project, ".json")
		if project == "" {
			return
		}
		if _, exists := seen[project]; exists {
			return
		}
		seen[project] = struct{}{}
		projects = append(projects, project)
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for index, part := range parts {
		if part == "projects" && index+1 < len(parts) {
			add(parts[index+1])
		}
	}
	for _, query := range []string{"project", "projectId"} {
		if project := r.URL.Query().Get(query); project != "" {
			add(project)
		}
	}
	if project := r.Header.Get("X-Goog-User-Project"); project != "" {
		add(project)
	}
	if r.Body != nil && strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		originalBody := r.Body
		body, err := io.ReadAll(io.LimitReader(originalBody, (1<<20)+1))
		r.Body = &joinedReadCloser{
			Reader: io.MultiReader(bytes.NewReader(body), originalBody),
			Closer: originalBody,
		}
		if err == nil {
			if len(body) <= 1<<20 {
				var value any
				if json.Unmarshal(body, &value) == nil {
					collectBodyProjects(value, add)
				}
			}
		}
	}
	return projects
}

type joinedReadCloser struct {
	io.Reader
	io.Closer
}

func collectBodyProjects(value any, add func(string)) {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			switch key {
			case "project", "projectId", "project_id":
				if project, ok := nested.(string); ok {
					add(project)
				}
			case "name", "parent", "logName", "workflow":
				if resource, ok := nested.(string); ok {
					parts := strings.Split(strings.Trim(resource, "/"), "/")
					for index, part := range parts {
						if part == "projects" && index+1 < len(parts) {
							add(parts[index+1])
						}
					}
				}
			}
			collectBodyProjects(nested, add)
		}
	case []any:
		for _, nested := range typed {
			collectBodyProjects(nested, add)
		}
	}
}

// ProjectFromRequest returns the bounded project identifier used by gateway
// quota and audit controls.
func ProjectFromRequest(r *http.Request) string {
	return projectFromRequest(r)
}

func (p *ProxyRouter) writeAuthError(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "status": status, "message": message,
	}})
}

func (p *ProxyRouter) writeBodyLimitError(w http.ResponseWriter, r *http.Request, limit int64) {
	status := "INVALID_ARGUMENT"
	if strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		p.mu.RLock()
		authorizer := p.authorizer
		inspectionEnabled := p.enforceProjects && p.projects != nil || p.quota != nil
		p.mu.RUnlock()
		if inspectionEnabled || authorizer != nil && authorizer.EnforcementEnabled() {
			status = "RESOURCE_EXHAUSTED"
		}
	}
	message := "Request body exceeds " + strconv.FormatInt(limit, 10) + " bytes."
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusRequestEntityTooLarge)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code":    http.StatusRequestEntityTooLarge,
		"status":  status,
		"message": message,
		"details": []map[string]any{{
			"@type":   "type.googleapis.com/google.rpc.BadRequest",
			"message": message,
		}},
	}})
}

// ClassifyRequest returns bounded labels for gateway telemetry without mutating
// the request. It mirrors the routing decision made by ServeHTTP.
func (p *ProxyRouter) ClassifyRequest(r *http.Request) observability.RequestLabels {
	domain := normalizeDomain(r.Host)
	servicePath := r.URL.Path
	if isLocalhost(r.Host) {
		if selector, path, canonical := canonicalEndpoint(r.URL.Path); canonical {
			if resolved, ok := p.resolveEndpoint(selector); ok {
				domain = resolved
			} else {
				domain = "unresolved"
			}
			servicePath = path
		} else {
			domain = legacyLocalDomain(r.URL.Path, domain)
		}
	}
	if strings.HasSuffix(domain, ".firebaseio.com") {
		domain = "firebaseio.com"
	}
	return observability.RequestLabels{
		Service: domain,
		Route:   observability.NormalizeRoute(servicePath),
		Project: ProjectFromRequest(r),
	}
}

func normalizeDomain(domain string) string {
	host := strings.TrimSpace(domain)
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.ToLower(strings.Trim(strings.TrimSpace(host), "[] ."))
}

func isLocalhost(hostport string) bool {
	host := normalizeDomain(hostport)
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// registerEndpointAliasLocked registers the first DNS label as the convenient
// canonical endpoint name. Conflicting aliases are disabled rather than being
// resolved according to registration order; the full registered domain remains
// available as an unambiguous selector.
func (p *ProxyRouter) registerEndpointAliasLocked(domain string) {
	alias, _, _ := strings.Cut(domain, ".")
	current, exists := p.endpointAliases[alias]
	if !exists {
		p.endpointAliases[alias] = domain
		return
	}
	if current != domain {
		p.endpointAliases[alias] = ""
	}
}

func (p *ProxyRouter) resolveEndpoint(selector string) (string, bool) {
	selector = normalizeDomain(selector)

	p.mu.RLock()
	defer p.mu.RUnlock()

	if _, exists := p.routes[selector]; exists {
		return selector, true
	}
	if _, exists := p.lazyDomains[selector]; exists {
		return selector, true
	}
	domain, exists := p.endpointAliases[selector]
	return domain, exists && domain != ""
}

func canonicalEndpoint(path string) (selector, servicePath string, canonical bool) {
	const prefix = "/_minisky/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}

	remainder := strings.TrimPrefix(path, prefix)
	selector, servicePath, _ = strings.Cut(remainder, "/")
	if selector == "" {
		return "", "", true
	}
	return selector, "/" + servicePath, true
}

func rewriteRequestPath(r *http.Request, path string) {
	r.URL.Path = path
	r.URL.RawPath = ""
	r.RequestURI = r.URL.RequestURI()
}

func legacyLocalDomain(path, fallbackDomain string) string {
	if strings.HasPrefix(path, "/storage/") || strings.HasPrefix(path, "/upload/storage/") {
		return "storage.googleapis.com"
	}
	if strings.HasPrefix(path, "/bigquery/") || strings.HasPrefix(path, "/upload/bigquery/") {
		return "bigquery.googleapis.com"
	}
	if (strings.HasPrefix(path, "/v1/projects/") || strings.HasPrefix(path, "/projects/")) &&
		(strings.Contains(path, "/topics") || strings.Contains(path, "/subscriptions")) &&
		!strings.Contains(path, "/locations/") {
		return "pubsub.googleapis.com"
	}
	if strings.HasPrefix(path, "/compute/") {
		return "compute.googleapis.com"
	}
	if strings.HasPrefix(path, "/sql/") {
		return "sqladmin.googleapis.com"
	}

	// Phase 18-25 services
	if strings.Contains(path, "/triggers") && strings.Contains(path, "/locations/") && !strings.Contains(path, "/functions") {
		return "eventarc.googleapis.com"
	}
	if strings.Contains(path, "/workflows/") && strings.Contains(path, "/executions") {
		return "workflowexecutions.googleapis.com"
	}
	if strings.Contains(path, "/workflows") {
		return "workflows.googleapis.com"
	}
	if strings.Contains(path, "/v1b3/") && strings.Contains(path, "/jobs") {
		return "dataflow.googleapis.com"
	}
	if strings.Contains(path, "/environments") && strings.Contains(path, "/locations/") {
		return "composer.googleapis.com"
	}
	if strings.Contains(path, "/clusters") && strings.Contains(path, "/topics") {
		return "managedkafka.googleapis.com"
	}
	if strings.HasPrefix(path, "/v1beta1/") && strings.Contains(path, "/repositories") {
		return "dataform.googleapis.com"
	}
	if strings.Contains(path, "/clusters") && strings.Contains(path, "/instances") && !strings.Contains(path, "/container/") {
		return "alloydb.googleapis.com"
	}
	// AlloyDB and Managed Kafka both use /clusters — ambiguous via path alone.
	// These services must use Host-header routing (custom_endpoint in Terraform).
	// Only route to AlloyDB if path also contains /instances (unambiguous).
	if strings.Contains(path, "/clusters") && strings.Contains(path, "/instances") && !strings.Contains(path, "/container/") && strings.Contains(path, "/locations/") {
		return "alloydb.googleapis.com"
	}
	if strings.Contains(path, "/tenants") && strings.HasPrefix(path, "/v2/") {
		return "identityplatform.googleapis.com"
	}
	if strings.HasPrefix(path, "/v1/transferJobs") {
		return "storagetransfer.googleapis.com"
	}
	if strings.Contains(path, "/traces") {
		return "cloudtrace.googleapis.com"
	}
	if strings.Contains(path, "/events:report") || strings.Contains(path, "/groupStats") {
		return "clouderrorreporting.googleapis.com"
	}
	if strings.Contains(path, "/profiles") && strings.HasPrefix(path, "/v2/") {
		return "cloudprofiler.googleapis.com"
	}
	if strings.Contains(path, "/gateways") || (strings.Contains(path, "/apis") && strings.Contains(path, "/locations/") && !strings.Contains(path, "/workflows/")) {
		return "apigateway.googleapis.com"
	}
	if strings.Contains(path, "/deliveryPipelines") {
		return "clouddeploy.googleapis.com"
	}
	if strings.HasPrefix(path, "/v1/images:annotate") {
		return "vision.googleapis.com"
	}
	if strings.Contains(path, ":translateText") || strings.Contains(path, "/supportedLanguages") {
		return "translate.googleapis.com"
	}
	if strings.Contains(path, "/processors") && !strings.Contains(path, "/dataproc/") {
		return "documentai.googleapis.com"
	}
	if strings.Contains(path, "/assets") || strings.Contains(path, ":searchAllResources") || strings.Contains(path, ":exportAssets") {
		return "cloudasset.googleapis.com"
	}
	if strings.Contains(path, "/inspectTemplates") || strings.Contains(path, "/content:inspect") || strings.Contains(path, "/content:deidentify") {
		return "dlp.googleapis.com"
	}
	if strings.Contains(path, "/policies") && strings.HasPrefix(path, "/v2/") && !strings.Contains(path, "/accessPolicies") {
		return "orgpolicy.googleapis.com"
	}
	if strings.Contains(path, "/authorizationPolicies") {
		return "networksecurity.googleapis.com"
	}
	if strings.Contains(path, "/accessPolicies") {
		return "accesscontextmanager.googleapis.com"
	}
	if strings.Contains(path, "/meshes") || strings.Contains(path, "/httpRoutes") {
		return "networkservices.googleapis.com"
	}
	// Batch: /v1/projects/*/locations/*/jobs (NOT /v1b3/ which is Dataflow)
	if !strings.HasPrefix(path, "/v1b3/") && strings.HasPrefix(path, "/v1/projects/") &&
		strings.Contains(path, "/locations/") && strings.Contains(path, "/jobs") &&
		!strings.Contains(path, "/workflows/") {
		return "batch.googleapis.com"
	}
	// Filestore: /v1/projects/*/locations/*/instances (when NOT under /clusters/)
	if strings.HasPrefix(path, "/v1/projects/") && strings.Contains(path, "/locations/") &&
		strings.Contains(path, "/instances") && !strings.Contains(path, "/clusters/") {
		return "file.googleapis.com"
	}
	// Service Directory: /v1/projects/*/locations/*/namespaces (with or without /services)
	if strings.Contains(path, "/namespaces") && strings.Contains(path, "/locations/") &&
		!strings.Contains(path, "/clusters/") {
		return "servicedirectory.googleapis.com"
	}

	// These location-scoped collections are shared by multiple APIs. They need
	// an explicit host or canonical selector and must not enter the catch-all.
	if strings.Contains(path, "/locations/") &&
		(strings.Contains(path, "/clusters") ||
			strings.Contains(path, "/operations/") ||
			strings.Contains(path, "/topics") ||
			strings.Contains(path, "/subscriptions") ||
			strings.Contains(path, "/indexes") ||
			strings.Contains(path, "/models") ||
			strings.Contains(path, "/agents") ||
			strings.Contains(path, "/caPools")) {
		return fallbackDomain
	}

	// Fallback: Cloud Functions / Cloud Run catch-all for /v1 or /v2 with locations
	if strings.HasPrefix(path, "/v2/") ||
		(strings.HasPrefix(path, "/v1/projects/") && strings.Contains(path, "/locations/")) {
		return "cloudfunctions.googleapis.com"
	}
	return fallbackDomain
}

func (p *ProxyRouter) writeUnimplemented(w http.ResponseWriter, service string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte(`{"error":{"code":501,"message":"MiniSky: '` + service + `' is not yet implemented","status":"UNIMPLEMENTED"}}`))
}

func (p *ProxyRouter) writeColdStartUnavailable(w http.ResponseWriter, domain string) {
	registry.WriteDockerUnavailable(w)
}
