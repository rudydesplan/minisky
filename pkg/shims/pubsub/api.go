package pubsub

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"
	"sync"

	"minisky/pkg/observability"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/serverless"
)

func init() {
	registry.Register("pubsub.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.SvcMgr)
	})
}

type EventObserver interface {
	HandleEvent(eventType, resource, payload string)
}

type acknowledgedEventObserver interface {
	HandleEventWithAck(eventType, resource, payload string) error
}

type API struct {
	svcMgr    *orchestrator.ServiceManager
	proxy     *httputil.ReverseProxy
	mu        sync.RWMutex
	observers []EventObserver
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if slsShim, ok := ctx.GetShim("cloudfunctions.googleapis.com").(*serverless.API); ok {
		api.SetObserver(slsShim)
	}
	if eventarcShim := ctx.GetShim("eventarc.googleapis.com"); eventarcShim != nil {
		if observer, ok := eventarcShim.(EventObserver); ok {
			api.AddObserver(observer)
		}
	}
}

func NewAPI(sm *orchestrator.ServiceManager) *API {
	return &API{
		svcMgr: sm,
	}
}

func (api *API) SetObserver(obs EventObserver) {
	api.AddObserver(obs)
}

// AddObserver registers an event recipient without replacing existing recipients.
// Registering the same observer more than once is a no-op.
func (api *API) AddObserver(obs EventObserver) {
	if obs == nil {
		return
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	for _, existing := range api.observers {
		if sameObserver(existing, obs) {
			return
		}
	}
	api.observers = append(api.observers, obs)
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Ensure Pub/Sub emulator is running
	targetURL, err := api.svcMgr.EnsureServiceRunning(r.Context(), "pubsub.googleapis.com")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// 2. Intercept Publish
	if r.Method == http.MethodPost && strings.Contains(r.URL.Path, ":publish") {
		api.handlePublish(w, r, targetURL)
		return
	}

	// 3. Normal Proxy
	target, _ := url.Parse(targetURL)
	proxy := observability.NewReverseProxy(target)

	// Ensure /v1 prefix for emulator compatibility
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		r.URL.Path = "/v1" + r.URL.Path
	}

	proxy.ServeHTTP(w, r)
}

func (api *API) handlePublish(w http.ResponseWriter, r *http.Request, targetURL string) {
	// Reconstruct the topic name from URL
	// /v1/projects/{project}/topics/{topic}:publish
	parts := strings.Split(r.URL.Path, "/")
	topic := ""
	for i, p := range parts {
		if p == "topics" && i+1 < len(parts) {
			topic = strings.Split(parts[i+1], ":")[0]
			break
		}
	}

	// Ensure /v1 prefix for emulator compatibility (same as ServeHTTP)
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		r.URL.Path = "/v1" + r.URL.Path
	}

	// Read body to notify observer
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewBuffer(body)) // reset for proxy

	// Proxy the request
	target, _ := url.Parse(targetURL)
	proxy := observability.NewReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.StatusCode >= 200 && response.StatusCode < 300 && topic != "" {
			resource := "projects/" + extractProject(r.URL.Path) + "/topics/" + topic
			log.Printf("[PubSub Shim] 📢 Intercepted publish to topic: %s", resource)
			for _, observer := range api.eventObservers() {
				if acknowledged, ok := observer.(acknowledgedEventObserver); ok {
					if err := acknowledged.HandleEventWithAck("google.cloud.pubsub.topic.v1.messagePublished", resource, string(body)); err != nil {
						return err
					}
				} else {
					observer.HandleEvent("google.cloud.pubsub.topic.v1.messagePublished", resource, string(body))
				}
			}
		}
		return nil
	}

	log.Printf("[PubSub Shim] %s %s", r.Method, r.URL.Path)
	proxy.ServeHTTP(w, r)
}

func extractProject(path string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == "projects" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func (api *API) eventObservers() []EventObserver {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return append([]EventObserver(nil), api.observers...)
}

func sameObserver(a, b EventObserver) bool {
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if av.Type() != bv.Type() {
		return false
	}
	if av.Type().Comparable() {
		return av.Interface() == bv.Interface()
	}
	switch av.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice:
		return av.Pointer() == bv.Pointer()
	default:
		return false
	}
}
