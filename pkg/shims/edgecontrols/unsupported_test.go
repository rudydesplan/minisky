package edgecontrols

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestArmorAndCDNRequestsFailExplicitly(t *testing.T) {
	handler := UnsupportedHandler()
	for _, path := range []string{
		"/compute/v1/projects/p/global/securityPolicies/policy",
		"/compute/v1/projects/p/global/backendServices/api:setCdnPolicy",
	} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("%s status = %d, body = %s", path, rec.Code, rec.Body.String())
		}
	}
}
