package cloudendpoints

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDeferredEndpointsReturnExplicitUnimplemented(t *testing.T) {
	paths := []string{
		"/v1/services/example.endpoints.test/configs",
		"/v1/services/example.endpoints.test/rollouts",
		"/v1/services/example.endpoints.test:check",
		"/v1/services/example.endpoints.test:report",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			NewAPI().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status=%d, want 501: %s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"status":"UNIMPLEMENTED"`) {
				t.Fatalf("unexpected error: %s", rec.Body.String())
			}
		})
	}
}
