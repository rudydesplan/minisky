package cloudkms

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestKeyMaterialRehydratesAfterRestart(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	call := func(api *API, method, path, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		return response
	}
	if response := call(api, http.MethodPost, "/v1/projects/demo/locations/global/keyRings?keyRingId=ring", `{}`); response.Code != http.StatusOK {
		t.Fatalf("create ring: %d: %s", response.Code, response.Body.String())
	}
	if response := call(api, http.MethodPost, "/v1/projects/demo/locations/global/keyRings/ring/cryptoKeys?cryptoKeyId=key", `{}`); response.Code != http.StatusOK {
		t.Fatalf("create key: %d: %s", response.Code, response.Body.String())
	}
	encrypted := call(api, http.MethodPost, "/v1/projects/demo/locations/global/keyRings/ring/cryptoKeys/key:encrypt", `{"plaintext":"cmVzdGFydA=="}`)
	if encrypted.Code != http.StatusOK {
		t.Fatalf("encrypt: %d: %s", encrypted.Code, encrypted.Body.String())
	}
	ciphertext := bytes.TrimSpace(encrypted.Body.Bytes())
	start := bytes.Index(ciphertext, []byte(`"ciphertext":"`)) + len(`"ciphertext":"`)
	end := start + bytes.IndexByte(ciphertext[start:], '"')

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	response := call(restarted, http.MethodPost, "/v1/projects/demo/locations/global/keyRings/ring/cryptoKeys/key:decrypt",
		`{"ciphertext":"`+string(ciphertext[start:end])+`"}`)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "cmVzdGFydA==") {
		t.Fatalf("decrypt restored key: %d: %s", response.Code, response.Body.String())
	}
}
