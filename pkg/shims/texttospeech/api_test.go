package texttospeech

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSynthesizeValidationPrecedesUnsupported(t *testing.T) {
	api := NewAPI()
	for _, test := range []struct {
		name string
		body string
		code int
	}{
		{"malformed", `{`, 400},
		{"mixed modalities", `{"input":{"text":"hello","ssml":"<speak>hello</speak>"},"voice":{"languageCode":"en-US"},"audioConfig":{"audioEncoding":"LINEAR16"}}`, 400},
		{"ssml", `{"input":{"ssml":"<speak>hello</speak>"},"voice":{"languageCode":"en-US"},"audioConfig":{"audioEncoding":"LINEAR16"}}`, 501},
		{"plain text", `{"input":{"text":"hello"},"voice":{"languageCode":"en-US"},"audioConfig":{"audioEncoding":"LINEAR16"}}`, 501},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/text:synthesize", strings.NewReader(test.body))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != test.code {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "audioContent") {
				t.Fatal("unsupported synthesis returned fabricated audio")
			}
		})
	}
}
