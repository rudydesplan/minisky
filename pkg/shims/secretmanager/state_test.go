package secretmanager

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestSecretManagerProductionConstructorFailsClosedOnCorruptState(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "secret-corrupt")
	store, err := state.New(root, "secret-corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(secretManagerStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(nil, nil)
	response := secretRequest(api, http.MethodGet, "/v1/projects/demo/secrets", "")
	if response.Code != http.StatusServiceUnavailable || api.PersistenceError() == nil {
		t.Fatalf("status=%d degraded=%v body=%s", response.Code, api.PersistenceError(), response.Body.String())
	}
}

func TestSecretManagerDegradedResponseRedactsPersistenceCause(t *testing.T) {
	const sensitive = "/private/secrets/state.json: payload-key-123"
	api := newAPI(nil, nil, nil)
	api.markPersistenceDegraded(errors.New(sensitive))
	response := secretRequest(api, http.MethodGet, "/v1/projects/demo/secrets", "")
	body := response.Body.String()
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(body, `"status":"UNAVAILABLE"`) ||
		!strings.Contains(body, `"message":"Secret Manager persistence is unavailable"`) ||
		strings.Contains(body, sensitive) {
		t.Fatalf("status=%d body=%s", response.Code, body)
	}
}

func TestSecretMutationsLeaveStateUnchangedWhenSaveFails(t *testing.T) {
	store := &failingSecretStore{}
	api := newAPI(nil, nil, store)

	store.fail = true
	response := secretRequest(api, http.MethodPost, "/v1/projects/demo/secrets?secretId=failed", `{}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(api.store) != 0 || len(store.data) != 0 {
		t.Fatalf("failed create changed state: live=%#v persisted=%s", api.store, store.data)
	}

	store.fail = false
	if response = secretRequest(api, http.MethodPost, "/v1/projects/demo/secrets?secretId=kept", `{}`); response.Code != http.StatusOK {
		t.Fatalf("seed create: %d: %s", response.Code, response.Body.String())
	}
	before := store.snapshot()

	store.fail = true
	response = secretRequest(api, http.MethodPost, "/v1/projects/demo/secrets/kept:addVersion", `{"payload":{"data":"bmV2ZXI="}}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("version status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := len(api.store["demo"]["kept"].versions); got != 0 {
		t.Fatalf("failed version changed live state: %d versions", got)
	}
	if got := store.snapshot(); string(got) != string(before) {
		t.Fatalf("failed version changed persisted state: got=%s want=%s", got, before)
	}

	response = secretRequest(api, http.MethodDelete, "/v1/projects/demo/secrets/kept", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	if api.store["demo"]["kept"] == nil {
		t.Fatal("failed delete removed live secret")
	}
	if got := store.snapshot(); string(got) != string(before) {
		t.Fatalf("failed delete changed persisted state: got=%s want=%s", got, before)
	}
}

func TestSecretCreateReconcilesPostCommitSaveError(t *testing.T) {
	store := &failingSecretStore{commitThenFail: true}
	api := newAPI(nil, nil, store)
	response := secretRequest(api, http.MethodPost,
		"/v1/projects/demo/secrets?secretId=committed", `{}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	if api.store["demo"]["committed"] == nil {
		t.Fatal("post-commit save was not reconciled into live state")
	}
	restarted, err := newAPIWithStore(nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.store["demo"]["committed"] == nil {
		t.Fatal("post-commit save did not survive restart")
	}
}

func TestSecretAmbiguousPostCommitReadbackFailureFailsClosed(t *testing.T) {
	store := &failingSecretStore{commitThenFail: true}
	api := newAPI(nil, nil, store)
	store.loadErr = errors.New("secret readback unavailable")
	response := secretRequest(api, http.MethodPost,
		"/v1/projects/demo/secrets?secretId=ambiguous", `{}`)
	if response.Code != http.StatusInternalServerError || api.persistenceErr == nil {
		t.Fatalf("create status=%d degraded=%v body=%s",
			response.Code, api.persistenceErr, response.Body.String())
	}
	blocked := secretRequest(api, http.MethodGet, "/v1/projects/demo/secrets", "")
	var envelope struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if err := json.NewDecoder(blocked.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if blocked.Code != http.StatusServiceUnavailable ||
		envelope.Error.Code != http.StatusServiceUnavailable ||
		envelope.Error.Status != "UNAVAILABLE" ||
		envelope.Error.Message != "Secret Manager persistence is unavailable" ||
		strings.Contains(blocked.Body.String(), "secret readback unavailable") {
		t.Fatalf("degraded API did not fail closed: %d: %s", blocked.Code, blocked.Body.String())
	}
}

type failingSecretStore struct {
	mu             sync.Mutex
	data           []byte
	fail           bool
	commitThenFail bool
	loadErr        error
}

func (s *failingSecretStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *failingSecretStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("injected secret save failure")
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	if err == nil && s.commitThenFail {
		s.commitThenFail = false
		return errors.New("injected post-commit secret save failure")
	}
	return err
}

func (s *failingSecretStore) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}

func secretRequest(api *API, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}
