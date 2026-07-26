package registry

import (
	"log"
	"net/http"
	"sync"

	"minisky/pkg/orchestrator"
)

// Context provides shared resources to shims during initialization.
type Context struct {
	OpMgr  *orchestrator.OperationManager
	SvcMgr *orchestrator.ServiceManager
	shims  map[string]http.Handler
	shared map[string]http.Handler
	mu     sync.RWMutex
}

// SharedHandler returns one handler instance for related domains in this boot.
func (c *Context) SharedHandler(key string, create func() http.Handler) http.Handler {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shared == nil {
		c.shared = make(map[string]http.Handler)
	}
	if handler := c.shared[key]; handler != nil {
		return handler
	}
	handler := create()
	c.shared[key] = handler
	return handler
}

// GetShim allows one shim to find another for cross-service events (e.g. Pub/Sub -> Serverless).
func (c *Context) GetShim(domain string) http.Handler {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.shims[domain]
}

// Factory is a function that creates a shim instance.
type Factory func(ctx *Context) http.Handler

var (
	registryMu       sync.RWMutex
	factories        = make(map[string]Factory)
	lazyDocker       = make(map[string]bool)
	requiresDocker   = make(map[string]bool)
	dockerOperations = make(map[string]func(*http.Request) bool)
)

// Register maps a domain to a shim factory.
func Register(domain string, factory Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	factories[domain] = factory
	log.Printf("[Registry] Registered Shim Factory for %s", domain)
}

// RegisterLazyDocker marks a domain as a pure Docker-backed service.
func RegisterLazyDocker(domain string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	lazyDocker[domain] = true
	log.Printf("[Registry] Registered Lazy Docker Factory for %s", domain)
}

// RequireDocker marks registered custom shims whose entire public surface
// requires an initialized Docker backend. BootAll substitutes one canonical
// unavailable handler without invoking their factories when Docker is absent.
func RequireDocker(domains ...string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, domain := range domains {
		metadata, ok := serviceManifest[domain]
		if !ok || metadata.fidelity != FidelityPassthrough ||
			metadata.persistence != PersistenceDocker {
			panic("registry.RequireDocker is restricted to pure Docker passthrough services: " + domain)
		}
		requiresDocker[domain] = true
	}
}

// RequireDockerMutations marks in-process factories whose read-only control
// plane remains available while backend mutations require Docker.
func RequireDockerMutations(domains ...string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, domain := range domains {
		dockerOperations[domain] = func(request *http.Request) bool {
			return request.Method != http.MethodGet && request.Method != http.MethodHead
		}
	}
}

// RequireDockerOperations records the exact request boundary at which a
// hybrid factory needs its Docker-backed service manager.
func RequireDockerOperations(domain string, requiresDocker func(*http.Request) bool) {
	registryMu.Lock()
	defer registryMu.Unlock()
	dockerOperations[domain] = requiresDocker
}

func requiresDockerMutation(domain string, request *http.Request) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	requiresDocker := dockerOperations[domain]
	if requiresDocker == nil {
		return false
	}
	return requiresDocker(request)
}

func WriteDockerUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"error":{"code":503,"message":"MiniSky: Docker backend unavailable","status":"UNAVAILABLE"}}`))
}

func dockerUnavailableHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		WriteDockerUnavailable(w)
	})
}

// PostBoot is implemented by shims that need to wire themselves to other services
// after all shims have been instantiated (e.g. Pub/Sub observer setup).
type PostBoot interface {
	OnPostBoot(ctx *Context)
}

// ProjectDiscoverer is implemented by shims that track resources by project ID.
type ProjectDiscoverer interface {
	ListProjects() []string
}

// BootAll initializes all registered shims and returns the mapping.
func BootAll(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager) (map[string]http.Handler, []string) {
	ctx := &Context{
		OpMgr:  opMgr,
		SvcMgr: svcMgr,
		shims:  make(map[string]http.Handler),
		shared: make(map[string]http.Handler),
	}

	// First pass: Instantiate all shims
	registryMu.Lock()
	for domain, factory := range factories {
		if svcMgr == nil && requiresDocker[domain] {
			ctx.shims[domain] = dockerUnavailableHandler()
			continue
		}
		shim := factory(ctx)
		ctx.shims[domain] = shim
	}
	registryMu.Unlock()

	// Second pass: Wire dependencies (PostBoot)
	for _, shim := range ctx.shims {
		if pb, ok := shim.(PostBoot); ok {
			pb.OnPostBoot(ctx)
		}
	}

	// Return the initialized shims and the list of lazy domains
	lazyList := []string{}
	for domain := range lazyDocker {
		lazyList = append(lazyList, domain)
	}

	return ctx.shims, lazyList
}
