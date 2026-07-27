package pubsublite

import (
	"encoding/json"
	"net/http"

	"minisky/pkg/registry"
)

func init() {
	registry.Register("pubsublite.googleapis.com", func(ctx *registry.Context) http.Handler {
		return ctx.SharedHandler("pubsublite.googleapis.com", func() http.Handler {
			return NewAPI()
		})
	})
}

// API is a self-contained experimental Pub/Sub Lite boundary.
type API struct{}

func NewAPI() *API {
	return &API{}
}

func (api *API) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    http.StatusNotImplemented,
			"message": "Pub/Sub Lite requires partitioned publish and consume behavior that MiniSky does not implement",
			"status":  "UNIMPLEMENTED",
			"details": []any{},
		},
	})
}
