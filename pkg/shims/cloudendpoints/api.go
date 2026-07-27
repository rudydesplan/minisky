// Package cloudendpoints defines MiniSky's explicit Cloud Endpoints boundary.
// Service Management configuration, rollout, and Service Control check/report
// are recognized but deferred until a safe executable control plane exists.
package cloudendpoints

import (
	"encoding/json"
	"net/http"
	"strings"

	"minisky/pkg/registry"
)

const maxControlRequestBytes = 1 << 20

func init() {
	factory := func(ctx *registry.Context) http.Handler {
		return ctx.SharedHandler("cloudendpoints", func() http.Handler { return NewAPI() })
	}
	registry.Register("servicemanagement.googleapis.com", factory)
	registry.Register("servicecontrol.googleapis.com", factory)
}

type API struct{}

func NewAPI() *API { return &API{} }

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.Body = http.MaxBytesReader(w, r.Body, maxControlRequestBytes)
	path := r.URL.Path
	recognized := r.Method == http.MethodPost &&
		(strings.Contains(path, "/services/") &&
			(strings.HasSuffix(path, "/configs") ||
				strings.HasSuffix(path, "/rollouts") ||
				strings.HasSuffix(path, ":check") ||
				strings.HasSuffix(path, ":report")))
	if recognized {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Cloud Endpoints Service Management and Service Control execution is deferred")
		return
	}
	writeError(w, http.StatusNotFound, "NOT_FOUND", "Cloud Endpoints resource not found")
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "status": status, "details": []any{},
	}})
}
