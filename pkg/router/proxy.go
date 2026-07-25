package router

import (
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"

	"minisky/pkg/orchestrator"
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
	if !p.validator.ValidateRequestForDomain(w, r, targetDomain) {
		return
	}

	// 2. Check if this is a lazy-loaded Docker backend
	p.mu.RLock()
	isLazy := p.lazyDomains[targetDomain]
	p.mu.RUnlock()

	if isLazy && p.serviceMgr != nil {
		internalURL, err := p.serviceMgr.EnsureServiceRunning(r.Context(), targetDomain)
		if err != nil {
			log.Printf("[Router ERROR] Orchestrator failed for '%s': %v", targetDomain, err)
			// Clear the stale wired route so the next request will re-attempt the cold start
			p.mu.Lock()
			delete(p.routes, targetDomain)
			p.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":{"code":503,"message":"MiniSky: Cold-start failed for ` + targetDomain + `"}}`))
			return
		}
		if internalURL != "" {
			// Dynamically wire (or re-wire if container moved IP) the discovered internal URL
			target, _ := url.Parse(internalURL)
			proxy := httputil.NewSingleHostReverseProxy(target)
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
