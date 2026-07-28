package speech

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecognizeRejectsTrailingJSONAndNamesUnsupportedModel(t *testing.T) {
	api := NewAPI()
	for _, test := range []struct {
		name        string
		body        string
		code        int
		wantMessage string
	}{
		{
			name:        "trailing JSON",
			body:        `{"config":{"languageCode":"en-US"},"audio":{"content":"YQ=="}} {}`,
			code:        http.StatusBadRequest,
			wantMessage: "invalid request body",
		},
		{
			name:        "model",
			body:        `{"config":{"languageCode":"en-US","model":"latest_long"},"audio":{"content":"YQ=="}}`,
			code:        http.StatusNotImplemented,
			wantMessage: "config.model",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(
				http.MethodPost, "/v1/speech:recognize", strings.NewReader(test.body)))
			if response.Code != test.code || !strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
}
