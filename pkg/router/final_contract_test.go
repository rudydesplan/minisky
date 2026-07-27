package router

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLegacyLocalWorkflowExecutionRoutesToExecutionService(t *testing.T) {
	path := "/v1/projects/demo/locations/us/workflows/flow/executions"
	if got := legacyLocalDomain(path, "localhost"); got != "workflowexecutions.googleapis.com" {
		t.Fatalf("legacyLocalDomain(%q) = %q", path, got)
	}
	permission, _ := routePermission("workflowexecutions.googleapis.com",
		httptest.NewRequest(http.MethodPost, path, nil))
	if permission != "workflows.executions.create" {
		t.Fatalf("permission = %q", permission)
	}
}

func TestStrictIAMMapsTenantOAuthRoutes(t *testing.T) {
	for _, test := range []struct {
		method, path, permission string
	}{
		{http.MethodPost, "/v2/projects/demo/tenants/tenant/oauthIdpConfigs", "identityplatform.oauthIdpConfigs.create"},
		{http.MethodGet, "/v2/projects/demo/tenants/tenant/oauthIdpConfigs/config", "identityplatform.oauthIdpConfigs.get"},
		{http.MethodPatch, "/v2/projects/demo/tenants/tenant/oauthIdpConfigs/config", "identityplatform.oauthIdpConfigs.update"},
		{http.MethodDelete, "/v2/projects/demo/tenants/tenant/oauthIdpConfigs/config", "identityplatform.oauthIdpConfigs.delete"},
	} {
		request := httptest.NewRequest(test.method, test.path, nil)
		permission, _ := routePermission("identityplatform.googleapis.com", request)
		if permission != test.permission {
			t.Errorf("%s %s permission = %q, want %q", test.method, test.path, permission, test.permission)
		}
	}
}

func TestVertexFindNeighborsUsesIndexEndpointRoute(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/locations/us/indexEndpoints/endpoint:findNeighbors", nil)
	permission, _ := routePermission("aiplatform.googleapis.com", request)
	if permission != "aiplatform.indexEndpoints.findNeighbors" {
		t.Fatalf("permission = %q", permission)
	}
}
