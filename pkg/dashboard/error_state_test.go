package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOperationalHandlersReturnExplicitDependencyErrors(t *testing.T) {
	api := &API{}
	tests := []struct {
		name    string
		path    string
		handler http.HandlerFunc
	}{
		{name: "services", path: "/api/services", handler: api.handleServices},
		{name: "logging", path: "/api/manage/logging/entries", handler: api.handleLoggingEntries},
		{name: "container logs", path: "/api/manage/logging/container", handler: api.handleContainerLogs},
		{name: "monitoring", path: "/api/manage/monitoring/stats", handler: api.handleMonitoringStats},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			test.handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), `"error"`) {
				t.Fatalf("missing explicit error response: %s", response.Body.String())
			}
		})
	}
}

func TestOperationalHandlersRejectUnsupportedMethods(t *testing.T) {
	api := &API{}
	for path, handler := range map[string]http.HandlerFunc{
		"/api/manage/logging/entries":   api.handleLoggingEntries,
		"/api/manage/logging/container": api.handleContainerLogs,
		"/api/manage/monitoring/stats":  api.handleMonitoringStats,
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d", path, response.Code)
		}
	}
}
