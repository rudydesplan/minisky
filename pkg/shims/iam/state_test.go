package iam

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestIAMRejectsMalformedPersistedServiceAccountsWithoutReplacingPriorState(t *testing.T) {
	validKey, valid := validPersistedServiceAccount("worker")
	tests := []struct {
		name     string
		key      string
		account  *ServiceAccount
		mutate   func(*ServiceAccount)
		wantText string
	}{
		{name: "nil entry", key: validKey, wantText: "nil"},
		{name: "map key mismatch", key: "other-project:" + valid.Email, account: valid, wantText: "map key"},
		{name: "name mismatch", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			account.Name = "projects/test-project/serviceAccounts/other@test-project.iam.gserviceaccount.com"
		}, wantText: "name"},
		{name: "email mismatch", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			account.Email = "another@test-project.iam.gserviceaccount.com"
		}, wantText: "map key"},
		{name: "project mismatch", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			account.ProjectId = "other-project"
		}, wantText: "project"},
		{name: "unique ID empty", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			account.UniqueId = ""
		}, wantText: "unique ID"},
		{name: "unique ID leading zero", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			account.UniqueId = "0123"
		}, wantText: "unique ID"},
		{name: "unique ID non decimal", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			account.UniqueId = "123x"
		}, wantText: "unique ID"},
		{name: "short local part", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			setPersistedAccountID(account, "short")
		}, wantText: "email"},
		{name: "long local part", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			setPersistedAccountID(account, "a"+strings.Repeat("b", 30))
		}, wantText: "email"},
		{name: "uppercase local part", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			setPersistedAccountID(account, "Worker")
		}, wantText: "email"},
		{name: "colon local part", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			setPersistedAccountID(account, "worker:id")
		}, wantText: "email"},
		{name: "encoded local part", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			setPersistedAccountID(account, "worker%2Fid")
		}, wantText: "email"},
		{name: "separator local part", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			setPersistedAccountID(account, "worker/id")
		}, wantText: "email"},
		{name: "control local part", key: validKey, account: valid, mutate: func(account *ServiceAccount) {
			setPersistedAccountID(account, "worker\nid")
		}, wantText: "email"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := cloneServiceAccount(test.account)
			if account != nil && test.mutate != nil {
				test.mutate(account)
			}
			store := &iamMetadataLoadStore{metadata: iamMetadata{
				ServiceAccounts: map[string]*ServiceAccount{test.key: account},
			}}
			api := newAPI(store)
			priorKey, prior := validPersistedServiceAccount("prior-account")
			api.serviceAccounts[priorKey] = prior

			err := api.loadMetadata()

			if err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("load error=%v want containing %q", err, test.wantText)
			}
			if api.PersistenceError() == nil {
				t.Fatal("invalid persisted account did not make load failure sticky")
			}
			if len(api.serviceAccounts) != 1 || !reflect.DeepEqual(api.serviceAccounts[priorKey], prior) {
				t.Fatalf("invalid load replaced prior accounts: %#v", api.serviceAccounts)
			}
			if _, err := NewAPIWithStore(store); err == nil {
				t.Fatal("constructor accepted invalid persisted account")
			}
		})
	}
}

func TestIAMStateImportRejectsNilServiceAccountAndPreservesProfile(t *testing.T) {
	store, err := state.New(t.TempDir(), "iam-import")
	if err != nil {
		t.Fatal(err)
	}
	validKey, valid := validPersistedServiceAccount("worker")
	prior := iamMetadata{ServiceAccounts: map[string]*ServiceAccount{validKey: valid}}
	if err := store.Save(iamStateEntry, prior); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(iamMetadata{
		ServiceAccounts: map[string]*ServiceAccount{validKey: nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(state.Snapshot{
		Format:  state.SnapshotFormat,
		Version: state.Version,
		Entries: map[string]json.RawMessage{iamStateEntry: payload},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Import(bytes.NewReader(snapshot)); err == nil {
		t.Fatal("import accepted nil IAM service account")
	}
	var after iamMetadata
	if err := store.Load(iamStateEntry, &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.ServiceAccounts, prior.ServiceAccounts) {
		t.Fatalf("failed import replaced prior state: %#v", after.ServiceAccounts)
	}
}

func TestIAMRejectsDuplicatePersistedServiceAccountUniqueIDs(t *testing.T) {
	firstKey, first := validPersistedServiceAccount("worker")
	secondKey, second := validPersistedServiceAccount("another")
	second.UniqueId = first.UniqueId
	store := &iamMetadataLoadStore{metadata: iamMetadata{
		ServiceAccounts: map[string]*ServiceAccount{
			firstKey:  first,
			secondKey: second,
		},
	}}
	api := newAPI(store)
	priorKey, prior := validPersistedServiceAccount("prior-account")
	api.serviceAccounts[priorKey] = prior

	if err := api.loadMetadata(); err == nil || !strings.Contains(err.Error(), "duplicate unique ID") {
		t.Fatalf("load error=%v", err)
	}
	if len(api.serviceAccounts) != 1 || !reflect.DeepEqual(api.serviceAccounts[priorKey], prior) {
		t.Fatalf("duplicate unique ID replaced prior state: %#v", api.serviceAccounts)
	}
}

func TestIAMRestartPreservesValidServiceAccountIDBoundaries(t *testing.T) {
	store, err := state.New(t.TempDir(), "iam-boundaries")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	accountIDs := []string{"worker", "a" + strings.Repeat("b", 29)}
	for _, accountID := range accountIDs {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(
			http.MethodPost,
			"/v1/projects/test-project/serviceAccounts",
			bytes.NewBufferString(`{"accountId":"`+accountID+`"}`),
		))
		if response.Code != http.StatusOK {
			t.Fatalf("create %q status=%d body=%s", accountID, response.Code, response.Body.String())
		}
	}

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, accountID := range accountIDs {
		email := accountID + "@test-project.iam.gserviceaccount.com"
		if got, _, found := restarted.ResolveServiceAccount(email); !found || got != email {
			t.Fatalf("resolve %q email=%q found=%t", accountID, got, found)
		}
	}
}

func TestIAMMetadataSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "iam-profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}

	create := httptest.NewRequest(http.MethodPost, "/v1/projects/test-project/serviceAccounts", bytes.NewBufferString(
		`{"accountId":"worker","serviceAccount":{"displayName":"Worker"}}`,
	))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, create)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	setTestPolicy(t, api, []Binding{{
		Role:    "roles/minisky.viewer",
		Members: []string{"user:alice@example.com"},
	}})

	t.Setenv("MINISKY_IAM_MODE", "strict")
	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	get := httptest.NewRequest(http.MethodGet,
		"/v1/projects/test-project/serviceAccounts/worker@test-project.iam.gserviceaccount.com", nil)
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
	if !restarted.Authorize("projects/test-project", "user:alice@example.com", "minisky.dashboard.view") {
		t.Fatal("persisted viewer binding did not authorize after restart")
	}
	if restarted.Authorize("projects/test-project", "user:alice@example.com", "minisky.dashboard.manage") {
		t.Fatal("persisted viewer binding elevated after restart")
	}
}

type iamMetadataLoadStore struct {
	metadata iamMetadata
}

func (store *iamMetadataLoadStore) Load(_ string, target any) error {
	raw, err := json.Marshal(store.metadata)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

func (*iamMetadataLoadStore) Save(string, any) error { return nil }

func validPersistedServiceAccount(accountID string) (string, *ServiceAccount) {
	const project = "test-project"
	email := accountID + "@" + project + ".iam.gserviceaccount.com"
	return project + ":" + email, &ServiceAccount{
		Name:      "projects/" + project + "/serviceAccounts/" + email,
		ProjectId: project,
		UniqueId:  "1234567890",
		Email:     email,
	}
}

func setPersistedAccountID(account *ServiceAccount, accountID string) {
	const project = "test-project"
	account.Email = accountID + "@" + project + ".iam.gserviceaccount.com"
	account.Name = "projects/" + project + "/serviceAccounts/" + account.Email
}

func cloneServiceAccount(account *ServiceAccount) *ServiceAccount {
	if account == nil {
		return nil
	}
	clone := *account
	return &clone
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
