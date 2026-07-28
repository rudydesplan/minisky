package translate

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIdentityTranslationUsesV3ResponseFieldsOnly(t *testing.T) {
	response := httptest.NewRecorder()
	NewAPI().ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v3/projects/p/locations/global:translateText",
		strings.NewReader(`{"contents":["hello"],"sourceLanguageCode":"en","targetLanguageCode":"en"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "detectedSourceLanguage") {
		t.Fatalf("v3 response contains v2-only field: %s", response.Body.String())
	}
}

func TestTranslateRejectsTrailingJSON(t *testing.T) {
	response := httptest.NewRecorder()
	NewAPI().ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v3/projects/p/locations/global:translateText",
		strings.NewReader(`{"contents":["hello"],"sourceLanguageCode":"en","targetLanguageCode":"en"} {}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}
