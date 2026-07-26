package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"minisky/pkg/shims/gke"
)

func TestGKEKubeconfigDownloadRejectsBackendOnlyIdentity(t *testing.T) {
	backend := gke.NewKindBackend()
	api := &API{gkeBackend: backend}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/api/manage/gke/projects/demo/zones/zone/clusters/cluster/config", nil)
	api.handleManageGke().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("download = %d %q", rec.Code, rec.Body.String())
	}
}

func TestGKEKubeconfigDownloadRejectsIncompleteIdentity(t *testing.T) {
	api := &API{gkeBackend: gke.NewKindBackend()}
	rec := httptest.NewRecorder()
	api.handleManageGke().ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/api/manage/gke/projects/demo/clusters/cluster/config", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
}

func TestGKEKubeconfigDispatchUsesCanonicalRouteParser(t *testing.T) {
	api := &API{gkeBackend: gke.NewKindBackend()}
	tests := []struct {
		path string
		want int
	}{
		{path: "/api/manage/gke/projects/demo/zones/zone/clusters/cluster/config", want: http.StatusServiceUnavailable},
		{path: "/api/manage/gke/projects/demo/zones/zone/clusters/cluster/config/", want: http.StatusServiceUnavailable},
		{path: "/api/manage/gke/projects/demo/zones/zone/clusters/cluster//config", want: http.StatusBadRequest},
		{path: "/api/manage/gke/projects/demo/zones/zone/clusters/cluster%2Fconfig", want: http.StatusBadRequest},
	}
	for _, test := range tests {
		rec := httptest.NewRecorder()
		api.handleManageGke().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, test.path, nil))
		if rec.Code != test.want {
			t.Fatalf("path=%q status=%d want=%d body=%s", test.path, rec.Code, test.want, rec.Body.String())
		}
	}
}
