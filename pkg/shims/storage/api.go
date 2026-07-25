package storage

import (
	"encoding/json"
	"log"
	"net/http"
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
	registry.Register("storage.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.SvcMgr)
	})
}

// EventObserver is implemented by shims that want to receive GCS events (like Serverless).
type EventObserver interface {
	HandleEvent(eventType, resource, payload string)
}

type API struct {
	svcMgr    *orchestrator.ServiceManager
	mu        sync.RWMutex
	observers []EventObserver
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if slsShim, ok := ctx.GetShim("cloudfunctions.googleapis.com").(*serverless.API); ok {
		api.SetObserver(slsShim)
	}
}

func NewAPI(sm *orchestrator.ServiceManager) *API {
	return &API{svcMgr: sm}
}

func (api *API) SetObserver(o EventObserver) {
	api.AddObserver(o)
}

// AddObserver registers an event recipient without replacing existing recipients.
// Registering the same observer more than once is a no-op.
func (api *API) AddObserver(observer EventObserver) {
	if observer == nil {
		return
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	for _, existing := range api.observers {
		if sameObserver(existing, observer) {
			return
		}
	}
	api.observers = append(api.observers, observer)
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Ensure the GCS emulator is running
	targetURL, err := api.svcMgr.EnsureServiceRunning(r.Context(), "storage.googleapis.com")
	if err != nil {
		http.Error(w, "GCS Emulator cold-start failed", http.StatusServiceUnavailable)
		return
	}

	target, _ := url.Parse(targetURL)
	proxy := observability.NewReverseProxy(target)

	// Intercept the response to trigger events
	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			api.handlePotentialEvent(r, resp)
		}
		return nil
	}

	proxy.ServeHTTP(w, r)
}

func (api *API) handlePotentialEvent(req *http.Request, resp *http.Response) {
	observers := api.eventObservers()
	if len(observers) == 0 {
		return
	}

	path := req.URL.Path
	// Detect uploads: POST /b/{bucket}/o or POST /upload/storage/v1/b/{bucket}/o
	if req.Method == "POST" && (strings.Contains(path, "/b/") && strings.HasSuffix(path, "/o")) {
		bucket := extractSegmentAfter(path, "b")
		object := req.URL.Query().Get("name")

		if object != "" {
			log.Printf("[Storage Event] File finalized: gs://%s/%s", bucket, object)
			api.notify(observers, bucket, object, "google.storage.object.finalize")
		}
	}

	// Detect deletions: DELETE /b/{bucket}/o/{object}
	if req.Method == "DELETE" && strings.Contains(path, "/o/") {
		bucket := extractSegmentAfter(path, "b")
		object := extractSegmentAfter(path, "o")
		log.Printf("[Storage Event] File deleted: gs://%s/%s", bucket, object)
		api.notify(observers, bucket, object, "google.storage.object.delete")
	}
}

func (api *API) notify(observers []EventObserver, bucket, object, eventType string) {
	payload, err := json.Marshal(map[string]string{
		"bucket": bucket,
		"name":   object,
	})
	if err != nil {
		return
	}
	for _, observer := range observers {
		observer.HandleEvent(eventType, bucket, string(payload))
	}
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

func extractSegmentAfter(path, keyword string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == keyword && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}
