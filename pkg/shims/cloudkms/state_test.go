package cloudkms

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestCloudKMSProductionConstructorFailsClosedOnCorruptState(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "kms-corrupt")
	store, err := state.New(root, "kms-corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(cloudKMSStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI()
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/demo/locations/global/keyRings", nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || api.PersistenceError() == nil {
		t.Fatalf("status=%d degraded=%v body=%s", response.Code, api.PersistenceError(), response.Body.String())
	}
}

func TestCloudKMSDegradedResponseRedactsPersistenceCause(t *testing.T) {
	const sensitive = "/private/kms/state.json: key-material-123"
	api := newAPI(nil)
	api.markPersistenceDegraded(errors.New(sensitive))
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/demo/locations/global/keyRings", nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	body := response.Body.String()
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(body, `"status":"UNAVAILABLE"`) ||
		!strings.Contains(body, `"message":"Cloud KMS persistence is unavailable"`) ||
		strings.Contains(body, sensitive) {
		t.Fatalf("status=%d body=%s", response.Code, body)
	}
}

func TestKMSMutationsLeaveStateUnchangedWhenSaveFails(t *testing.T) {
	store := &failingKMSStore{fail: true}
	api := newAPI(store)
	call := func(method, path, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		return response
	}
	ringPath := "/v1/projects/demo/locations/global/keyRings"
	if response := call(http.MethodPost, ringPath+"?keyRingId=failed", `{}`); response.Code != http.StatusInternalServerError {
		t.Fatalf("failed ring create status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(api.store) != 0 || len(store.snapshot()) != 0 {
		t.Fatalf("failed ring create changed state: live=%#v persisted=%s", api.store, store.snapshot())
	}

	store.setFail(false)
	if response := call(http.MethodPost, ringPath+"?keyRingId=ring", `{}`); response.Code != http.StatusOK {
		t.Fatalf("seed ring: %d: %s", response.Code, response.Body.String())
	}
	ringBaseline := store.snapshot()
	store.setFail(true)
	keyPath := ringPath + "/ring/cryptoKeys"
	if response := call(http.MethodPost, keyPath+"?cryptoKeyId=failed", `{}`); response.Code != http.StatusInternalServerError {
		t.Fatalf("failed key create status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(api.store["demo/global"]["ring"].keys) != 0 || string(store.snapshot()) != string(ringBaseline) {
		t.Fatal("failed key create changed live or persisted state")
	}

	store.setFail(false)
	if response := call(http.MethodPost, keyPath+"?cryptoKeyId=key", `{}`); response.Code != http.StatusOK {
		t.Fatalf("seed key: %d: %s", response.Code, response.Body.String())
	}
	baseline := store.snapshot()
	store.setFail(true)

	if response := call(http.MethodPatch, keyPath+"/key", `{"labels":{"changed":"true"}}`); response.Code != http.StatusInternalServerError {
		t.Fatalf("failed key update status = %d, body = %s", response.Code, response.Body.String())
	}
	key := api.store["demo/global"]["ring"].keys["key"]
	if key.Labels != nil || string(store.snapshot()) != string(baseline) {
		t.Fatal("failed key update changed live or persisted state")
	}

	if response := call(http.MethodPost, keyPath+"/key/cryptoKeyVersions", `{}`); response.Code != http.StatusInternalServerError {
		t.Fatalf("failed version create status = %d, body = %s", response.Code, response.Body.String())
	}
	if len(key.versions) != 1 || string(store.snapshot()) != string(baseline) {
		t.Fatal("failed version create changed live or persisted state")
	}

	if response := call(http.MethodPost, keyPath+"/key/cryptoKeyVersions/1:destroy", `{}`); response.Code != http.StatusInternalServerError {
		t.Fatalf("failed version destroy status = %d, body = %s", response.Code, response.Body.String())
	}
	if key.versions[0].State != "ENABLED" || len(key.versions[0].aesKey) == 0 ||
		string(store.snapshot()) != string(baseline) {
		t.Fatal("failed version destroy changed live or persisted state")
	}
}

func TestKMSCreateReconcilesPostCommitSaveError(t *testing.T) {
	store := &failingKMSStore{commitThenFail: true}
	api := newAPI(store)
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/locations/global/keyRings?keyRingId=committed", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	if api.store["demo/global"]["committed"] == nil {
		t.Fatal("post-commit save was not reconciled into live state")
	}
	restarted, err := newAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.store["demo/global"]["committed"] == nil {
		t.Fatal("post-commit save did not survive restart")
	}
}

func TestKMSAmbiguousPostCommitReadbackFailureFailsClosed(t *testing.T) {
	store := &failingKMSStore{commitThenFail: true}
	api := newAPI(store)
	store.loadErr = errors.New("KMS readback unavailable")
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/locations/global/keyRings?keyRingId=ambiguous", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || api.persistenceErr == nil {
		t.Fatalf("create status=%d degraded=%v body=%s",
			response.Code, api.persistenceErr, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet,
		"/v1/projects/demo/locations/global/keyRings", nil)
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	var envelope struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusServiceUnavailable ||
		envelope.Error.Code != http.StatusServiceUnavailable ||
		envelope.Error.Status != "UNAVAILABLE" ||
		envelope.Error.Message != "Cloud KMS persistence is unavailable" ||
		strings.Contains(response.Body.String(), "KMS readback unavailable") {
		t.Fatalf("degraded API did not fail closed: %d: %s", response.Code, response.Body.String())
	}
}

type failingKMSStore struct {
	mu             sync.Mutex
	data           []byte
	fail           bool
	commitThenFail bool
	loadErr        error
}

func (s *failingKMSStore) Load(_ string, target any) error {
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

func (s *failingKMSStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("injected KMS save failure")
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	if err == nil && s.commitThenFail {
		s.commitThenFail = false
		return errors.New("injected post-commit KMS save failure")
	}
	return err
}

func (s *failingKMSStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *failingKMSStore) snapshot() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...)
}
