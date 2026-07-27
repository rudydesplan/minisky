package documentai

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestProcessDocumentEnforcesDecodedPayloadLimit(t *testing.T) {
	api := newTestAPI()
	name := "projects/p/locations/us/processors/proc-1"
	api.processors[name] = &Processor{Name: name, Type: "OCR_PROCESSOR", State: "ENABLED"}
	content := base64.StdEncoding.EncodeToString(make([]byte, maxRawDocumentBytes+1))
	body := `{"rawDocument":{"content":"` + content + `","mimeType":"application/pdf"}}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/"+name+":process", strings.NewReader(body)))
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), "rawDocument.content") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestCreateProcessorRejectsTrailingJSON(t *testing.T) {
	response := httptest.NewRecorder()
	newTestAPI().ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/p/locations/us/processors",
		strings.NewReader(`{"type":"OCR_PROCESSOR"} {}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestCreateProcessorRejectsStateGrowthPastLimit(t *testing.T) {
	api := newTestAPI()
	for index := 0; index < maxProcessors; index++ {
		name := "projects/p/locations/us/processors/existing-" + strconv.Itoa(index)
		api.processors[name] = &Processor{Name: name, Type: "OCR_PROCESSOR", State: "ENABLED"}
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/p/locations/us/processors", strings.NewReader(`{"type":"OCR_PROCESSOR"}`)))
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestBoundedLogPathRemovesControlsAndCapsLength(t *testing.T) {
	got := boundedLogPath("/v1/" + strings.Repeat("x", maxLoggedPathBytes) + "\nsecret")
	if len(got) > maxLoggedPathBytes || strings.ContainsAny(got, "\r\n\t") {
		t.Fatalf("unsafe bounded log path %q", got)
	}
}
