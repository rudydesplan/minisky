package secretmanager

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestSecretsRehydrateAfterRestart(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/projects/demo/secrets?secretId=token", strings.NewReader(`{"replication":{"automatic":{}}}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create secret: %d: %s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/projects/demo/secrets/token:addVersion", strings.NewReader(`{"payload":{"data":"c2Vuc2l0aXZl"}}`))
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("add version: %d: %s", response.Code, response.Body.String())
	}

	restarted, err := NewAPIWithStore(nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/projects/demo/secrets/token/versions/latest:access", nil)
	response = httptest.NewRecorder()
	restarted.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "c2Vuc2l0aXZl") {
		t.Fatalf("access restored secret: %d: %s", response.Code, response.Body.String())
	}
}
