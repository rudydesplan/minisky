package dialogflowcx

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestDetectIntentRejectsMixedModalitiesAndNamesUnsupportedAudio(t *testing.T) {
	api := NewAPIWithStore(nil)
	name := "projects/p/locations/us/agents/a"
	api.agents[name] = &Agent{Name: name, DisplayName: "a", DefaultLanguageCode: "en", TimeZone: "UTC"}
	for _, test := range []struct {
		name        string
		body        string
		code        int
		wantMessage string
	}{
		{
			name:        "mixed",
			body:        `{"queryInput":{"text":{"text":"hello"},"audio":{"config":{}},"languageCode":"en"}}`,
			code:        http.StatusBadRequest,
			wantMessage: "exactly one",
		},
		{
			name:        "audio",
			body:        `{"queryInput":{"audio":{"config":{}},"languageCode":"en"}}`,
			code:        http.StatusNotImplemented,
			wantMessage: "queryInput.audio",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
				"/v3/"+name+"/sessions/s:detectIntent", strings.NewReader(test.body)))
			if response.Code != test.code || !strings.Contains(response.Body.String(), test.wantMessage) {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateAgentRejectsStateGrowthPastLimit(t *testing.T) {
	api := NewAPIWithStore(nil)
	for index := 0; index < maxAgents; index++ {
		name := "projects/p/locations/us/agents/existing-" + strconv.Itoa(index)
		api.agents[name] = &Agent{Name: name, DisplayName: "existing", DefaultLanguageCode: "en", TimeZone: "UTC"}
	}
	response := createAgent(t, api, "overflow")
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}
