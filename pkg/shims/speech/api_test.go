package speech

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecognizeRejectsMalformedAndFailsHonestly(t *testing.T) {
	api := NewAPI()
	cases := []struct {
		name string
		body string
		code int
	}{
		{"malformed", `{`, 400},
		{"missing language", `{"config":{},"audio":{"content":"YQ=="}}`, 400},
		{"external URL", `{"config":{"languageCode":"en-US"},"audio":{"uri":"https://user:secret@example.com/a.wav"}}`, 400},
		{"valid unsupported", `{"config":{"languageCode":"en-US"},"audio":{"content":"YQ=="}}`, 501},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/speech:recognize", strings.NewReader(test.body))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != test.code {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.code, response.Body.String())
			}
			if test.code == 501 && strings.Contains(response.Body.String(), "confidence") == false {
				// The message explicitly states that no confidence was generated.
				t.Fatal("unsupported boundary did not disclose confidence behavior")
			}
		})
	}
}
