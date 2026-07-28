package texttospeech

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSynthesizeRejectsTrailingJSONAndNamesUnsupportedVoice(t *testing.T) {
	api := NewAPI()
	for _, test := range []struct {
		name        string
		body        string
		code        int
		wantMessage string
	}{
		{
			name:        "trailing JSON",
			body:        `{"input":{"text":"hello"},"voice":{"languageCode":"en-US"},"audioConfig":{"audioEncoding":"LINEAR16"}} {}`,
			code:        http.StatusBadRequest,
			wantMessage: "invalid request body",
		},
		{
			name:        "named voice",
			body:        `{"input":{"text":"hello"},"voice":{"languageCode":"en-US","name":"en-US-Neural2-A"},"audioConfig":{"audioEncoding":"LINEAR16"}}`,
			code:        http.StatusNotImplemented,
			wantMessage: "voice.name",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(
				http.MethodPost, "/v1/text:synthesize", strings.NewReader(test.body)))
			if response.Code != test.code || !strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
}
