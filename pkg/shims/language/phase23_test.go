package language

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAnalyzeRejectsTrailingJSONAndNamesUnsupportedEncoding(t *testing.T) {
	api := NewAPI()
	for _, test := range []struct {
		name        string
		body        string
		code        int
		wantMessage string
	}{
		{
			name:        "trailing JSON",
			body:        `{"document":{"type":"PLAIN_TEXT","content":"hello"}} {}`,
			code:        http.StatusBadRequest,
			wantMessage: "invalid request body",
		},
		{
			name:        "encoding",
			body:        `{"document":{"type":"PLAIN_TEXT","content":"hello"},"encodingType":"UTF8"}`,
			code:        http.StatusNotImplemented,
			wantMessage: "encodingType",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(
				http.MethodPost, "/v1/documents:analyzeSyntax", strings.NewReader(test.body)))
			if response.Code != test.code || !strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
}
