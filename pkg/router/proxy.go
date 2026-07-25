package router

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"minisky/pkg/observability"
	"minisky/pkg/orchestrator"
	localsecurity "minisky/pkg/security"
	"minisky/pkg/validator"
)

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
	p.mu.Lock()
	defer p.mu.Unlock()
	p.authorizer = authorizer
	p.projects = projects
	p.enforceProjects = enforceProjects
	p.tokenAudience = strings.TrimSpace(audience)
	if p.tokenAudience == "" {
		p.tokenAudience = "minisky-gateway"
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

	// 2. Schema Validation
	if !p.inspectProjectBody(w, r) {
		return
	}
	if !p.validator.ValidateRequestForDomain(w, r, targetDomain) {
		return
	}
	if !p.authorizeRequest(w, r, targetDomain) {
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
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil {
		p.writeAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Unable to inspect request body")
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	if len(body) <= 1<<20 {
		return true
	}
	p.writeAuthError(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED",
		"JSON request exceeds the 1 MiB project inspection limit")
	return false
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
		principal = claims.Subject
		r.Header.Set("X-MiniSky-Principal", principal)
	}
	if principal == "" {
		p.writeAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication is required in strict IAM mode")
		return false
	}

	permission, resource := routePermission(domain, r)
	if permission == "" {
		if isMutationMethod(r.Method) {
			p.writeAuthError(w, http.StatusForbidden, "PERMISSION_DENIED", "Strict IAM denied unmapped mutation route")
			return false
		}
		return true
	}
	if !authorizer.Authorize(resource, principal, permission) {
		p.writeAuthError(w, http.StatusForbidden, "PERMISSION_DENIED", "Caller lacks permission "+permission)
		return false
	}
	if domain == "pubsub.googleapis.com" && permission == "pubsub.subscriptions.create" {
		topic := pubsubTopicFromBody(r)
		subscriptionProject := projectFromRequest(r)
		topicProject := projectFromResourceName(topic)
		if topicProject != "" && topicProject != subscriptionProject &&
			!authorizer.Authorize(topic, principal, "pubsub.topics.attachSubscription") {
			p.writeAuthError(w, http.StatusForbidden, "PERMISSION_DENIED",
				"Caller lacks permission pubsub.topics.attachSubscription")
			return false
		}
	}
	return true
}

func pubsubTopicFromBody(r *http.Request) string {
	if r.Body == nil {
		return ""
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ""
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var input struct {
		Topic string `json:"topic"`
	}
	if json.Unmarshal(body, &input) != nil {
		return ""
	}
	return input.Topic
}

func projectFromResourceName(resource string) string {
	parts := strings.Split(strings.Trim(resource, "/"), "/")
	for index, part := range parts {
		if part == "projects" && index+1 < len(parts) {
			return parts[index+1]
		}
	}
	return ""
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
	switch domain {
	case "bigquery.googleapis.com":
		if r.Method == http.MethodGet {
			return "bigquery.datasets.get", resource
		}
		return "bigquery.datasets.update", resource
	case "compute.googleapis.com":
		if strings.Contains(r.URL.Path, "/instances") {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/instances"):
				return "compute.instances.list", resource
			case r.Method == http.MethodGet:
				return "compute.instances.get", resource
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/start"):
				return "compute.instances.start", resource
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/stop"):
				return "compute.instances.stop", resource
			case r.Method == http.MethodPost:
				return "compute.instances.create", resource
			case r.Method == http.MethodDelete:
				return "compute.instances.delete", resource
			}
		}
	case "pubsub.googleapis.com":
		switch {
		case strings.Contains(r.URL.Path, ":publish"):
			return "pubsub.topics.publish", strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/"), ":publish")
		case strings.Contains(r.URL.Path, "/subscriptions"):
			switch r.Method {
			case http.MethodGet:
				return "pubsub.subscriptions.get", resource
			case http.MethodPut, http.MethodPost:
				return "pubsub.subscriptions.create", resource
			case http.MethodDelete:
				return "pubsub.subscriptions.delete", resource
			}
		case strings.Contains(r.URL.Path, "/topics"):
			switch r.Method {
			case http.MethodGet:
				return "pubsub.topics.get", resource
			case http.MethodPut, http.MethodPost:
				return "pubsub.topics.create", resource
			case http.MethodDelete:
				return "pubsub.topics.delete", resource
			}
		}
	case "storage.googleapis.com":
		if r.Method == http.MethodGet {
			return "storage.objects.get", resource
		}
		switch r.Method {
		case http.MethodPost:
			if strings.Contains(r.URL.Path, "/o") {
				return "storage.objects.create", resource
			}
			return "storage.buckets.create", resource
		case http.MethodPut, http.MethodPatch:
			return "storage.objects.update", resource
		case http.MethodDelete:
			if strings.Contains(r.URL.Path, "/o/") {
				return "storage.objects.delete", resource
			}
			return "storage.buckets.delete", resource
		}
	case "iam.googleapis.com":
		switch {
		case strings.HasSuffix(r.URL.Path, ":setIamPolicy"):
			return "iam.serviceAccounts.setIamPolicy", resource
		case strings.Contains(r.URL.Path, "/keys/") && strings.HasSuffix(r.URL.Path, ":disable"):
			return "iam.serviceAccountKeys.disable", resource
		case strings.Contains(r.URL.Path, "/keys"):
			switch r.Method {
			case http.MethodPost:
				return "iam.serviceAccountKeys.create", resource
			case http.MethodDelete:
				return "iam.serviceAccountKeys.delete", resource
			}
		case strings.Contains(r.URL.Path, "/serviceAccounts"):
			switch r.Method {
			case http.MethodPost:
				return "iam.serviceAccounts.create", resource
			case http.MethodDelete:
				return "iam.serviceAccounts.delete", resource
			}
		}
	}
	return "", resource
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
	if r.Body != nil && strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err == nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
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

func collectBodyProjects(value any, add func(string)) {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			switch key {
			case "project", "projectId", "project_id":
				if project, ok := nested.(string); ok {
					add(project)
				}
			case "name", "parent", "logName":
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
	if strings.HasPrefix(path, "/bigquery/") {
		return "bigquery.googleapis.com"
	}
	if (strings.HasPrefix(path, "/v1/projects/") || strings.HasPrefix(path, "/projects/")) &&
		(strings.Contains(path, "/topics") || strings.Contains(path, "/subscriptions")) {
		return "pubsub.googleapis.com"
	}
	if strings.HasPrefix(path, "/v2/") ||
		(strings.HasPrefix(path, "/v1/projects/") && strings.Contains(path, "/locations/")) {
		return "cloudfunctions.googleapis.com"
	}
	if strings.HasPrefix(path, "/compute/") {
		return "compute.googleapis.com"
	}
	return fallbackDomain
}

func (p *ProxyRouter) writeUnimplemented(w http.ResponseWriter, service string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte(`{"error":{"code":501,"message":"MiniSky: '` + service + `' is not yet implemented","status":"UNIMPLEMENTED"}}`))
}

func (p *ProxyRouter) writeColdStartUnavailable(w http.ResponseWriter, domain string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write([]byte(`{"error":{"code":503,"message":"MiniSky: Cold-start failed for ` + domain + `","status":"UNAVAILABLE"}}`))
}
