package iam

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestIAMMetadataSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "iam-profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}

	create := httptest.NewRequest(http.MethodPost, "/v1/projects/test/serviceAccounts", bytes.NewBufferString(
		`{"accountId":"worker","serviceAccount":{"displayName":"Worker"}}`,
	))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, create)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	setTestPolicy(t, api, []Binding{{
		Role:    "roles/storage.objectViewer",
		Members: []string{"user:alice@example.com"},
	}})

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/projects/test/serviceAccounts/worker@test.iam.gserviceaccount.com", nil)
	response = httptest.NewRecorder()
	restarted.ServeHTTP(response, get)
	if response.Code != http.StatusOK {
		t.Fatalf("get after restart status = %d, body = %s", response.Code, response.Body.String())
	}

	var account ServiceAccount
	decodeResponse(t, response, &account)
	if account.DisplayName != "Worker" {
		t.Fatalf("display name = %q, want Worker", account.DisplayName)
	}
}

func TestIAMMissingStateIsEmptyAndCorruptStateIsReported(t *testing.T) {
	store, err := state.New(t.TempDir(), "iam-profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatalf("missing state: %v", err)
	}
	if len(api.serviceAccounts) != 0 {
		t.Fatalf("missing state loaded accounts: %#v", api.serviceAccounts)
	}

	if err := store.Save(iamStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAPIWithStore(store); err == nil {
		t.Fatal("corrupt state was not reported")
	}
	var persisted string
	if err := store.Load(iamStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != "corrupt" {
		t.Fatalf("corrupt state was overwritten with %q", persisted)
	}
}

func TestIAMProductionConstructorFailsClosedOnCorruptState(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "iam-corrupt")
	store, err := state.New(root, "iam-corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(iamStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/serviceAccounts", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if api.PersistenceError() == nil {
		t.Fatal("corrupt IAM state was not exposed as degradation")
	}
}

func TestIAMDegradedResponseRedactsPersistenceCause(t *testing.T) {
	const sensitive = "/private/iam/state.json: secret-token-123"
	api := newAPI(nil)
	api.persistenceErr = errors.New(sensitive)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/serviceAccounts", nil))
	body := response.Body.String()
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(body, `"status":"UNAVAILABLE"`) ||
		!strings.Contains(body, `"message":"IAM persistence is unavailable"`) ||
		strings.Contains(body, sensitive) {
		t.Fatalf("status=%d body=%s", response.Code, body)
	}
}

func TestIAMPersistenceDoesNotHoldAPILock(t *testing.T) {
	store := &checkingIAMStore{}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	store.api = api

	request := httptest.NewRequest(http.MethodPost, testResource+":setIamPolicy", bytes.NewBufferString(
		`{"policy":{"bindings":[]}}`,
	))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !store.saved {
		t.Fatal("mutation did not save state")
	}
}

type checkingIAMStore struct {
	api   *API
	saved bool
}

func (s *checkingIAMStore) Load(string, any) error { return state.ErrNotFound }

func (s *checkingIAMStore) Save(_ string, value any) error {
	if !s.api.mu.TryRLock() {
		return errors.New("IAM API lock held during save")
	}
	s.api.mu.RUnlock()
	if _, err := json.Marshal(value); err != nil {
		return err
	}
	s.saved = true
	return nil
}
