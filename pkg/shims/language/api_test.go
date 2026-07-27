package language

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAnalyzeSentimentBoundaries(t *testing.T) {
	api := NewAPI()
	for _, test := range []struct {
		name string
		body string
		code int
	}{
		{"missing document", `{}`, 400},
		{"external URL", `{"document":{"type":"PLAIN_TEXT","gcsContentUri":"https://example.com/private"}}`, 400},
		{"html", `{"document":{"type":"HTML","content":"<p>Hello</p>"}}`, 501},
		{"plain", `{"document":{"type":"PLAIN_TEXT","content":"Hello"}}`, 501},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/documents:analyzeSentiment", strings.NewReader(test.body))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != test.code {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), `"score"`) {
				t.Fatal("unsupported analysis returned a fabricated score")
			}
		})
	}
}
