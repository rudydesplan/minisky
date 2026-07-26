package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestArtifactRegistryManagementRouteAddsOneVersionPrefix(t *testing.T) {
	var path, host string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.EscapedPath()
		host = r.Host
		w.WriteHeader(http.StatusNoContent)
	}))
	defer gateway.Close()

	api := &API{gatewayURL: gateway.URL}
	request := httptest.NewRequest(http.MethodGet,
		"/api/manage/artifactregistry/projects/demo/locations/us-central1/repositories/apps/packages/team%2Fapi/versions", nil)
	response := httptest.NewRecorder()
	api.handleManageArtifactRegistry().ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if want := "/v1/projects/demo/locations/us-central1/repositories/apps/packages/team%2Fapi/versions"; path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
	if host != "artifactregistry.googleapis.com" {
		t.Fatalf("host = %q", host)
	}
}
