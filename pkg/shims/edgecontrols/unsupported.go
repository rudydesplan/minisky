// Package edgecontrols defines the truthful boundary for Cloud Armor and CDN.
package edgecontrols

import (
	"encoding/json"
	"net/http"
	"strings"
)

// UnsupportedHandler is intentionally not registered for compute.googleapis.com:
// doing so would replace the existing Compute shim. A future Compute integration
// can delegate recognized Armor/CDN paths here until a real data-plane hook exists.
func UnsupportedHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(request.URL.Path, "/securityPolicies") ||
			strings.Contains(request.URL.Path, ":setCdnPolicy") {
			w.WriteHeader(http.StatusNotImplemented)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    501,
					"message": "Cloud Armor/CDN enforcement is unavailable without Compute load-balancer integration",
					"status":  "UNIMPLEMENTED",
					"details": []any{},
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code": 404, "message": "resource not found", "status": "NOT_FOUND", "details": []any{},
			},
		})
	})
}
