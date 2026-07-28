package registry

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
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
	if _, exists := factories[domain]; exists || lazyDocker[domain] {
		panic("duplicate registry domain: " + domain)
	}
	factories[domain] = factory
	log.Printf("[Registry] Registered Shim Factory for %s", domain)
}

// RegisterLazyDocker marks a domain as a pure Docker-backed service.
func RegisterLazyDocker(domain string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := factories[domain]; exists || lazyDocker[domain] {
		panic("duplicate registry domain: " + domain)
	}
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
	if operations, declared := dockerOperationContracts[domain]; declared {
		for _, operation := range operations {
			if matchesDockerOperation(request, operation) {
				return true
			}
		}
		return false
	}

	registryMu.RLock()
	defer registryMu.RUnlock()
	requiresDocker := dockerOperations[domain]
	if requiresDocker == nil {
		return false
	}
	return requiresDocker(request)
}

func matchesDockerOperation(request *http.Request, operation DockerOperation) bool {
	if request.Method != operation.HTTPMethod || !matchesPathGlob(request.URL.Path, operation.PathGlob) {
		return false
	}
	switch operation.RequestBody {
	case "":
		return true
	case DockerRequestBodyBatchRunnable:
		return isSupportedBatchRunnable(request)
	case DockerRequestBodyAlloyDBPrimary:
		return requestJSONFieldEquals(request, "instanceType", "PRIMARY")
	default:
		return false
	}
}

func matchesPathGlob(path, glob string) bool {
	pathSegments := strings.Split(strings.Trim(path, "/"), "/")
	globSegments := strings.Split(strings.Trim(glob, "/"), "/")
	if len(pathSegments) != len(globSegments) {
		return false
	}
	for index := range globSegments {
		if globSegments[index] != "*" && globSegments[index] != pathSegments[index] {
			return false
		}
		if pathSegments[index] == "" {
			return false
		}
	}
	return true
}

const dockerPreflightBodyLimit = 1<<20 + 1

type replayRequestBody struct {
	io.Reader
	io.Closer
}

func inspectRequestJSON(request *http.Request, target any) bool {
	if request.Body == nil {
		return false
	}
	original := request.Body
	prefix, err := io.ReadAll(io.LimitReader(original, dockerPreflightBodyLimit))
	if err != nil {
		request.Body = replayRequestBody{Reader: io.MultiReader(bytes.NewReader(prefix), original), Closer: original}
		return false
	}
	if len(prefix) == dockerPreflightBodyLimit {
		request.Body = replayRequestBody{Reader: io.MultiReader(bytes.NewReader(prefix), original), Closer: original}
		return false
	}
	_ = original.Close()
	request.Body = io.NopCloser(bytes.NewReader(prefix))
	decoder := json.NewDecoder(bytes.NewReader(prefix))
	if err := decoder.Decode(target); err != nil {
		return false
	}
	return decoder.Decode(&struct{}{}) == io.EOF
}

func requestJSONFieldEquals(request *http.Request, field, want string) bool {
	body := make(map[string]json.RawMessage)
	if !inspectRequestJSON(request, &body) {
		return false
	}
	raw, ok := body[field]
	if !ok {
		return false
	}
	var got string
	return json.Unmarshal(raw, &got) == nil && got == want
}

func isSupportedBatchRunnable(request *http.Request) bool {
	var body struct {
		TaskGroups []struct {
			TaskSpec *struct {
				Runnables []struct {
					Container *struct {
						ImageURI string `json:"imageUri"`
					} `json:"container"`
				} `json:"runnables"`
			} `json:"taskSpec"`
			TaskCount   string `json:"taskCount"`
			Parallelism string `json:"parallelism"`
		} `json:"taskGroups"`
	}
	if !inspectRequestJSON(request, &body) || len(body.TaskGroups) != 1 {
		return false
	}
	group := body.TaskGroups[0]
	if group.TaskCount != "" && group.TaskCount != "1" {
		return false
	}
	if group.Parallelism != "" && group.Parallelism != "1" {
		return false
	}
	if group.TaskSpec == nil || len(group.TaskSpec.Runnables) != 1 {
		return false
	}
	container := group.TaskSpec.Runnables[0].Container
	return container != nil && container.ImageURI != ""
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
	experimentalEnabled := experimentalServicesEnabled()

	// First pass: Instantiate all shims
	registryMu.Lock()
	for domain, factory := range factories {
		if experimentalServiceContracts[domain] && !experimentalEnabled {
			ctx.shims[domain] = experimentalDisabledHandler(domain)
			continue
		}
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
