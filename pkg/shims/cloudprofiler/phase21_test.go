package cloudprofiler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateProfileRequiresMatchingDeploymentProject(t *testing.T) {
	api := newTestAPI()
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/projects/p1/profiles",
		strings.NewReader(`{"profileType":["CPU"],"deployment":{"projectId":"other","target":"svc"}}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
	}
	if len(api.profiles) != 0 {
		t.Fatal("invalid deployment mutated profile state")
	}
}

func TestCreateOfflineProfileRejectsInvalidTypeAndOversizedDecodedPayload(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"invalid type", `{"profileType":"NOT_REAL","deployment":{"projectId":"p1","target":"svc"},"profileBytes":"eA=="}`},
		{"oversized decoded payload", `{"profileType":"CPU","deployment":{"projectId":"p1","target":"svc"},"profileBytes":"` + strings.Repeat("YQ==", 300000) + `"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newTestAPI()
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/projects/p1/profiles:createOffline", strings.NewReader(test.body)))
			if rec.Code != http.StatusBadRequest && rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status=%d, want bounded rejection: %s", rec.Code, rec.Body.String())
			}
			if len(api.profiles) != 0 {
				t.Fatal("invalid profile mutated state")
			}
		})
	}
}
