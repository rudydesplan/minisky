package aiplatform

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func TestCreateIndexRejectsTrailingJSON(t *testing.T) {
	response := perform(NewAPIWithStore(nil), http.MethodPost,
		"/v1/projects/p/locations/us/indexes", `{"displayName":"index"} {}`)
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), "invalid request body") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestCreateIndexRejectsStateGrowthPastLimit(t *testing.T) {
	api := NewAPIWithStore(nil)
	for index := 0; index < maxIndexes; index++ {
		name := "projects/p/locations/us/indexes/existing-" + strconv.Itoa(index)
		api.indexes[name] = &Index{Name: name, DisplayName: "existing"}
	}
	response := perform(api, http.MethodPost, "/v1/projects/p/locations/us/indexes",
		`{"displayName":"overflow"}`)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestCreateIndexNamesUnsupportedMetadataField(t *testing.T) {
	response := perform(NewAPIWithStore(nil), http.MethodPost,
		"/v1/projects/p/locations/us/indexes",
		`{"displayName":"index","metadata":{"config":{"dimensions":2}}}`)
	if response.Code != http.StatusNotImplemented ||
		!strings.Contains(response.Body.String(), "metadata") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}
