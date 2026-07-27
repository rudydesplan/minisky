package pubsublite

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDeferredHandlerNeverAcknowledgesMutations(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			request := httptest.NewRequest(method,
				"/v1/admin/projects/p/locations/us-central1-a/topics/t1",
				bytes.NewBufferString(`{"partitionConfig":{"count":1}}`))
			response := httptest.NewRecorder()
			NewAPI().ServeHTTP(response, request)

			if response.Code != http.StatusNotImplemented {
				t.Fatalf("expected 501, got %d: %s", response.Code, response.Body.String())
			}
			var envelope struct {
				Error struct {
					Code   int    `json:"code"`
					Status string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Error.Code != http.StatusNotImplemented || envelope.Error.Status != "UNIMPLEMENTED" {
				t.Fatalf("unexpected error envelope: %+v", envelope.Error)
			}
		})
	}
}
