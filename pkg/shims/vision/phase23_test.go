package vision

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAnnotateNamesUnsupportedImageContext(t *testing.T) {
	response := httptest.NewRecorder()
	NewAPI().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/images:annotate",
		strings.NewReader(`{"requests":[{"image":{"content":"YQ=="},"features":[{"type":"TEXT_DETECTION"}],"imageContext":{"languageHints":["en"]}}]}`)))
	if response.Code != http.StatusNotImplemented ||
		!strings.Contains(response.Body.String(), "requests.imageContext") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}
