package identityplatform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestCreateTenant(t *testing.T) {
	api := newTestAPI()
	body := `{"displayName":"My Tenant"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/tenants", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var tenant Tenant
	_ = json.Unmarshal(w.Body.Bytes(), &tenant)
	if tenant.Name == "" {
		t.Fatal("expected name to be set")
	}
	if tenant.DisplayName != "My Tenant" {
		t.Fatalf("unexpected displayName: %s", tenant.DisplayName)
	}
	if tenant.CreateTime == "" {
		t.Fatal("expected createTime")
	}
}

func TestCreateTenantMissingDisplayName(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/tenants", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetTenant(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/t1"] = &Tenant{
		Name:        "projects/test/tenants/t1",
		DisplayName: "Test",
		CreateTime:  "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v2/projects/test/tenants/t1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var tenant Tenant
	_ = json.Unmarshal(w.Body.Bytes(), &tenant)
	if tenant.DisplayName != "Test" {
		t.Fatalf("unexpected displayName: %s", tenant.DisplayName)
	}
}

func TestGetTenantNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v2/projects/test/tenants/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListTenants(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/a"] = &Tenant{Name: "projects/test/tenants/a", DisplayName: "A"}
	api.tenants["projects/test/tenants/b"] = &Tenant{Name: "projects/test/tenants/b", DisplayName: "B"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v2/projects/test/tenants", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	tenants := resp["tenants"].([]any)
	if len(tenants) != 2 {
		t.Fatalf("expected 2 tenants, got %d", len(tenants))
	}
}

func TestUpdateTenant(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/t1"] = &Tenant{
		Name:        "projects/test/tenants/t1",
		DisplayName: "Old",
		CreateTime:  "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	body := `{"displayName":"New"}`
	req := httptest.NewRequest(http.MethodPatch, "/v2/projects/test/tenants/t1?updateMask=displayName", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	tenant := api.tenants["projects/test/tenants/t1"]
	api.mu.RUnlock()
	if tenant.DisplayName != "New" {
		t.Fatalf("expected updated displayName, got %s", tenant.DisplayName)
	}
}

func TestDeleteTenant(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/t1"] = &Tenant{Name: "projects/test/tenants/t1"}
	api.oauthConfigs["projects/test/tenants/t1/oauthIdpConfigs/c1"] = &OAuthIdpConfig{Name: "projects/test/tenants/t1/oauthIdpConfigs/c1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v2/projects/test/tenants/t1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, tenantExists := api.tenants["projects/test/tenants/t1"]
	_, configExists := api.oauthConfigs["projects/test/tenants/t1/oauthIdpConfigs/c1"]
	api.mu.RUnlock()
	if tenantExists {
		t.Fatal("tenant should be deleted")
	}
	if configExists {
		t.Fatal("oauth config should be cascade deleted")
	}
}

func TestCreateOAuthIdpConfig(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/t1"] = &Tenant{Name: "projects/test/tenants/t1"}
	api.mu.Unlock()

	body := `{"clientId":"client123","issuer":"https://accounts.google.com","enabled":true}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/tenants/t1/oauthIdpConfigs?oauthIdpConfigId=oidc.google", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var cfg OAuthIdpConfig
	_ = json.Unmarshal(w.Body.Bytes(), &cfg)
	if cfg.Name != "projects/test/tenants/t1/oauthIdpConfigs/oidc.google" {
		t.Fatalf("unexpected name: %s", cfg.Name)
	}
	if cfg.ClientID != "client123" {
		t.Fatalf("unexpected clientId: %s", cfg.ClientID)
	}
}

func TestOAuthSecretIsNeverReturned(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/t1"] = &Tenant{Name: "projects/test/tenants/t1"}
	api.mu.Unlock()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v2/projects/test/tenants/t1/oauthIdpConfigs?oauthIdpConfigId=oidc.example",
		bytes.NewBufferString(`{"clientId":"client","clientSecret":"top-secret","enabled":true}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", response.Code, response.Body.String())
	}
	if bytes.Contains(response.Body.Bytes(), []byte("top-secret")) ||
		bytes.Contains(response.Body.Bytes(), []byte("clientSecret")) {
		t.Fatalf("secret leaked in response: %s", response.Body.String())
	}
}

func TestProjectAndTenantConfigUseInjectedAuthBackend(t *testing.T) {
	backend := &fakeAuthBackend{}
	api := newAPI(newTestAPI().opMgr, nil)
	api.authBackend = backend
	project := httptest.NewRecorder()
	api.ServeHTTP(project, httptest.NewRequest(http.MethodPatch,
		"/admin/v2/projects/test/config?updateMask=authorizedDomains",
		bytes.NewBufferString(`{"authorizedDomains":["localhost"]}`)))
	if project.Code != http.StatusOK || backend.projectCalls != 1 {
		t.Fatalf("project config = %d %s calls=%d", project.Code, project.Body.String(), backend.projectCalls)
	}

	api.tenants["projects/test/tenants/t1"] = &Tenant{Name: "projects/test/tenants/t1"}
	tenant := httptest.NewRecorder()
	api.ServeHTTP(tenant, httptest.NewRequest(http.MethodPatch,
		"/v2/projects/test/tenants/t1/config?updateMask=disableAuth",
		bytes.NewBufferString(`{"disableAuth":true}`)))
	if tenant.Code != http.StatusOK || backend.tenantCalls != 1 {
		t.Fatalf("tenant config = %d %s calls=%d", tenant.Code, tenant.Body.String(), backend.tenantCalls)
	}
}

func TestInitializeProjectConfigForProvider(t *testing.T) {
	api := newAPI(newTestAPI().opMgr, nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v2/projects/test:initializeAuth", bytes.NewBufferString(`{}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("initialize status = %d: %s", response.Code, response.Body.String())
	}
	var config ProjectConfig
	if err := json.Unmarshal(response.Body.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	if config.Name != "projects/test/config" {
		t.Fatalf("config name = %q", config.Name)
	}
}

func TestInitializeProjectConfigRollsBackOnPersistenceFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		existing   *ProjectConfig
		wantStatus int
	}{
		{name: "restores absence", wantStatus: http.StatusServiceUnavailable},
		{name: "restores prior state", existing: &ProjectConfig{
			Name:              "projects/test/config",
			AuthorizedDomains: []string{"prior.example"},
		}, wantStatus: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &failingIdentityPlatformStore{
				mockStore: mockStore{data: make(map[string][]byte)},
				fail:      true,
			}
			api := newAPI(newTestAPI().opMgr, store)
			if test.existing != nil {
				api.projectConfigs[test.existing.Name] = cloneProjectConfig(test.existing)
				store.fail = false
				if err := api.persistState(); err != nil {
					t.Fatal(err)
				}
				store.fail = true
			}

			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
				"/v2/projects/test:initializeAuth", bytes.NewBufferString(`{}`)))
			if response.Code != test.wantStatus {
				t.Fatalf("initialize status = %d: %s", response.Code, response.Body.String())
			}

			got := api.projectConfigs["projects/test/config"]
			if test.existing == nil {
				if got != nil {
					t.Fatalf("failed initialization left in-memory config: %#v", got)
				}
			} else if got == nil || len(got.AuthorizedDomains) != 1 ||
				got.AuthorizedDomains[0] != "prior.example" {
				t.Fatalf("failed initialization did not restore prior config: %#v", got)
			}

			restarted := newAPI(newTestAPI().opMgr, store)
			if err := restarted.loadState(); err != nil {
				t.Fatal(err)
			}
			reloaded := restarted.projectConfigs["projects/test/config"]
			if test.existing == nil {
				if reloaded != nil {
					t.Fatalf("failed initialization persisted config across restart: %#v", reloaded)
				}
			} else if reloaded == nil || len(reloaded.AuthorizedDomains) != 1 ||
				reloaded.AuthorizedDomains[0] != "prior.example" {
				t.Fatalf("restart did not retain prior config: %#v", reloaded)
			}
		})
	}
}

func TestInitializeProjectConfigReconcilesPostCommitSaveError(t *testing.T) {
	store := &postCommitIdentityPlatformStore{
		mockStore: mockStore{data: make(map[string][]byte)},
		failNext:  true,
	}
	api := newAPI(newTestAPI().opMgr, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v2/projects/test:initializeAuth", bytes.NewBufferString(`{}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("post-commit initialize status = %d: %s", response.Code, response.Body.String())
	}
	if api.projectConfigs["projects/test/config"] == nil {
		t.Fatal("post-commit initialization was rolled back in memory")
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.projectConfigs["projects/test/config"] == nil {
		t.Fatal("successful reconciliation did not survive restart")
	}
}

func TestPatchTenantConfigReconcilesPostCommitSaveError(t *testing.T) {
	store := &postCommitIdentityPlatformStore{
		mockStore: mockStore{data: make(map[string][]byte)},
	}
	api := newAPI(newTestAPI().opMgr, state.NewGuardedEntryStore(store, nil))
	backend := &fakeAuthBackend{}
	api.authBackend = backend
	tenantName := "projects/test/tenants/t1"
	configName := tenantName + "/config"
	api.tenants[tenantName] = &Tenant{Name: tenantName}
	api.tenantConfigs[configName] = &TenantConfig{Name: configName, DisplayName: "before"}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.failNext = true

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v2/"+configName+"?updateMask=displayName",
		bytes.NewBufferString(`{"displayName":"after"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.tenantConfigs[configName]; got == nil || got.DisplayName != "after" {
		t.Fatalf("visible config = %#v", got)
	}
	if backend.tenantCalls != 1 {
		t.Fatalf("backend calls = %d, want committed update without rollback", backend.tenantCalls)
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if got := restarted.tenantConfigs[configName]; got == nil || got.DisplayName != "after" {
		t.Fatalf("durable config = %#v", got)
	}
}

func TestPatchProjectConfigReconcilesPostCommitSaveError(t *testing.T) {
	store := &postCommitIdentityPlatformStore{
		mockStore: mockStore{data: make(map[string][]byte)},
	}
	api := newAPI(newTestAPI().opMgr, state.NewGuardedEntryStore(store, nil))
	backend := &fakeAuthBackend{}
	api.authBackend = backend
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.failNext = true

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/admin/v2/projects/test/config?updateMask=authorizedDomains",
		bytes.NewBufferString(`{"authorizedDomains":["localhost"]}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if backend.projectCalls != 1 {
		t.Fatalf("backend calls = %d, want committed update without rollback", backend.projectCalls)
	}

	var durable identityPlatformMetadata
	if err := store.Load(identityPlatformStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	config := durable.ProjectConfigs["projects/test/config"]
	if config == nil || !reflect.DeepEqual(config.AuthorizedDomains, []string{"localhost"}) {
		t.Fatalf("durable project config = %#v", config)
	}
}

func TestProjectConfigAmbiguousSaveAndReadbackFailureFailsClosed(t *testing.T) {
	store := &postCommitIdentityPlatformStore{
		mockStore: mockStore{data: make(map[string][]byte)},
	}
	api := newAPI(newTestAPI().opMgr, state.NewGuardedEntryStore(store, nil))
	backend := &fakeAuthBackend{}
	api.authBackend = backend
	name := "projects/test/config"
	api.projectConfigs[name] = &ProjectConfig{Name: name, AuthorizedDomains: []string{"before.example"}}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	store.loadErr = errors.New("injected project config readback failure")

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/admin/v2/projects/test/config?updateMask=authorizedDomains",
		bytes.NewBufferString(`{"authorizedDomains":["after.example"]}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if api.initializationError() == nil {
		t.Fatal("ambiguous project config durability did not become sticky")
	}
	if backend.projectCalls != 2 || backend.lastProjectConfig == nil ||
		!reflect.DeepEqual(backend.lastProjectConfig.AuthorizedDomains, []string{"before.example"}) {
		t.Fatalf("backend reconciliation calls=%d config=%#v", backend.projectCalls, backend.lastProjectConfig)
	}
	blocked := httptest.NewRecorder()
	api.ServeHTTP(blocked, httptest.NewRequest(http.MethodGet, "/admin/v2/projects/test/config", nil))
	if blocked.Code != http.StatusServiceUnavailable {
		t.Fatalf("subsequent status = %d: %s", blocked.Code, blocked.Body.String())
	}
}

func TestTenantConfigAmbiguousSaveAndReadbackFailureFailsClosed(t *testing.T) {
	store := &postCommitIdentityPlatformStore{
		mockStore: mockStore{data: make(map[string][]byte)},
	}
	api := newAPI(newTestAPI().opMgr, state.NewGuardedEntryStore(store, nil))
	backend := &fakeAuthBackend{}
	api.authBackend = backend
	tenantName := "projects/test/tenants/t1"
	configName := tenantName + "/config"
	api.tenants[tenantName] = &Tenant{Name: tenantName}
	api.tenantConfigs[configName] = &TenantConfig{Name: configName, DisplayName: "before"}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	store.loadErr = errors.New("injected tenant config readback failure")

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v2/"+configName+"?updateMask=displayName",
		bytes.NewBufferString(`{"displayName":"after"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if api.initializationError() == nil {
		t.Fatal("ambiguous tenant config durability did not become sticky")
	}
	if backend.tenantCalls != 2 || backend.lastTenantConfig == nil ||
		backend.lastTenantConfig.DisplayName != "before" {
		t.Fatalf("backend reconciliation calls=%d config=%#v", backend.tenantCalls, backend.lastTenantConfig)
	}
	blocked := httptest.NewRecorder()
	api.ServeHTTP(blocked, httptest.NewRequest(http.MethodGet, "/v2/"+configName, nil))
	if blocked.Code != http.StatusServiceUnavailable {
		t.Fatalf("subsequent status = %d: %s", blocked.Code, blocked.Body.String())
	}
}

func TestInitializeProjectConfigSerializesProvisionalStateAndRollback(t *testing.T) {
	store := &blockingFirstIdentityPlatformStore{
		mockStore:   mockStore{data: make(map[string][]byte)},
		saveEntered: make(chan struct{}),
		releaseSave: make(chan struct{}),
	}
	api := newAPI(newTestAPI().opMgr, store)
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
			"/v2/projects/test:initializeAuth", bytes.NewBufferString(`{}`)))
		firstDone <- response
	}()
	<-store.saveEntered

	getDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
			"/admin/v2/projects/test/config", nil))
		getDone <- response
	}()
	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
			"/v2/projects/test:initializeAuth", bytes.NewBufferString(`{}`)))
		secondDone <- response
	}()

	for name, done := range map[string]<-chan *httptest.ResponseRecorder{
		"get": getDone, "second initialize": secondDone,
	} {
		select {
		case response := <-done:
			t.Fatalf("%s observed provisional initialization: %d %s",
				name, response.Code, response.Body.String())
		case <-time.After(25 * time.Millisecond):
		}
	}

	close(store.releaseSave)
	if response := <-firstDone; response.Code != http.StatusServiceUnavailable {
		t.Fatalf("first initialize status = %d: %s", response.Code, response.Body.String())
	}
	if response := <-secondDone; response.Code != http.StatusOK {
		t.Fatalf("second initialize status = %d: %s", response.Code, response.Body.String())
	}
	if response := <-getDone; response.Code != http.StatusOK {
		t.Fatalf("get status = %d: %s", response.Code, response.Body.String())
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.projectConfigs["projects/test/config"] == nil {
		t.Fatal("serialized successful initialization was lost after restart")
	}
}

func TestPatchTenantConfigSerializesBackendMemorySaveAndRollback(t *testing.T) {
	store := &blockingFirstIdentityPlatformStore{
		mockStore:   mockStore{data: make(map[string][]byte)},
		saveEntered: make(chan struct{}),
		releaseSave: make(chan struct{}),
	}
	tenantName := "projects/test/tenants/t1"
	configName := tenantName + "/config"
	initial := identityPlatformMetadata{
		Tenants:       map[string]*Tenant{tenantName: {Name: tenantName}},
		TenantConfigs: map[string]*TenantConfig{configName: {Name: configName, DisplayName: "before"}},
	}
	if err := store.mockStore.Save(identityPlatformStateEntry, initial); err != nil {
		t.Fatal(err)
	}
	api := newAPI(newTestAPI().opMgr, store)
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	backend := newSequencedAuthBackend()
	api.authBackend = backend

	patch := func(displayName string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
			"/v2/"+configName+"?updateMask=displayName",
			bytes.NewBufferString(`{"displayName":"`+displayName+`"}`)))
		return response
	}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- patch("first") }()
	<-store.saveEntered
	firstBackend := <-backend.calls
	if firstBackend.DisplayName != "first" {
		t.Fatalf("first backend config = %#v", firstBackend)
	}

	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { secondDone <- patch("second") }()
	select {
	case config := <-backend.calls:
		t.Fatalf("second backend call overtook first transaction: %#v", config)
	case <-time.After(25 * time.Millisecond):
	}

	close(store.releaseSave)
	if response := <-firstDone; response.Code != http.StatusServiceUnavailable {
		t.Fatalf("first status = %d: %s", response.Code, response.Body.String())
	}
	rollback := <-backend.calls
	if rollback.DisplayName != "before" {
		t.Fatalf("rollback config = %#v", rollback)
	}
	secondBackend := <-backend.calls
	if secondBackend.DisplayName != "second" {
		t.Fatalf("second backend config = %#v", secondBackend)
	}
	if response := <-secondDone; response.Code != http.StatusOK {
		t.Fatalf("second status = %d: %s", response.Code, response.Body.String())
	}

	if got := api.tenantConfigs[configName]; got == nil || got.DisplayName != "second" {
		t.Fatalf("visible final config = %#v", got)
	}
	var durable identityPlatformMetadata
	if err := store.Load(identityPlatformStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if got := durable.TenantConfigs[configName]; got == nil || got.DisplayName != "second" {
		t.Fatalf("durable final config = %#v", got)
	}
}

func TestPatchTenantConfigSaveFailureRestoresPriorConfig(t *testing.T) {
	store := &failingIdentityPlatformStore{
		mockStore: mockStore{data: make(map[string][]byte)},
	}
	api := newAPI(newTestAPI().opMgr, store)
	backend := &fakeAuthBackend{}
	api.authBackend = backend
	tenantName := "projects/test/tenants/t1"
	configName := tenantName + "/config"
	api.tenants[tenantName] = &Tenant{Name: tenantName}
	api.tenantConfigs[configName] = &TenantConfig{
		Name: configName, DisplayName: "before", DisableAuth: false,
	}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.fail = true

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v2/"+configName+"?updateMask=displayName",
		bytes.NewBufferString(`{"displayName":"after"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.tenantConfigs[configName]; got == nil || got.DisplayName != "before" {
		t.Fatalf("visible config after failed save = %#v", got)
	}
	if backend.tenantCalls != 2 || backend.lastTenantConfig == nil ||
		backend.lastTenantConfig.DisplayName != "before" {
		t.Fatalf("backend rollback calls=%d config=%#v", backend.tenantCalls, backend.lastTenantConfig)
	}
}

func TestCorruptStateFailsClosedWithoutOverwritingSnapshot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-identity-platform")
	store, err := state.New(root, "corrupt-identity-platform")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(identityPlatformStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}

	api := NewAPI(nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/v2/projects/test/tenants", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(identityPlatformStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

func TestIdentityToolkitUserWorkflowUsesInjectedAuthHandler(t *testing.T) {
	var paths []string
	api := newAPI(newTestAPI().opMgr, nil)
	api.authHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"localId":"user-1","idToken":"token"}`))
	})

	for _, path := range []string{
		"/v1/accounts:signUp",
		"/v1/accounts:signInWithPassword",
		"/v1/accounts:lookup",
	} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path,
			bytes.NewBufferString(`{"email":"user@example.test","password":"secret","returnSecureToken":true}`)))
		if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"localId":"user-1"`)) {
			t.Fatalf("%s = %d %s", path, response.Code, response.Body.String())
		}
	}
	if len(paths) != 3 {
		t.Fatalf("forwarded paths = %#v", paths)
	}
}

func TestIdentityToolkitUserWorkflowWithoutBackendIsUnsupported(t *testing.T) {
	api := newTestAPI()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/accounts:signUp",
		bytes.NewBufferString(`{"email":"user@example.test","password":"secret"}`)))
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Status != "UNIMPLEMENTED" {
		t.Fatalf("error status = %q", envelope.Error.Status)
	}
}

func TestCreateOAuthIdpConfigNoParent(t *testing.T) {
	api := newTestAPI()
	body := `{"clientId":"c"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/tenants/missing/oauthIdpConfigs?oauthIdpConfigId=c1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateOAuthIdpConfigMissingID(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/t1"] = &Tenant{Name: "projects/test/tenants/t1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/tenants/t1/oauthIdpConfigs", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateOAuthIdpConfigDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.tenants["projects/test/tenants/t1"] = &Tenant{Name: "projects/test/tenants/t1"}
	api.oauthConfigs["projects/test/tenants/t1/oauthIdpConfigs/dup"] = &OAuthIdpConfig{Name: "projects/test/tenants/t1/oauthIdpConfigs/dup"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/tenants/t1/oauthIdpConfigs?oauthIdpConfigId=dup", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOAuthIdpConfig(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.oauthConfigs["projects/test/tenants/t1/oauthIdpConfigs/c1"] = &OAuthIdpConfig{
		Name:     "projects/test/tenants/t1/oauthIdpConfigs/c1",
		ClientID: "abc",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v2/projects/test/tenants/t1/oauthIdpConfigs/c1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListOAuthIdpConfigs(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.oauthConfigs["projects/test/tenants/t1/oauthIdpConfigs/a"] = &OAuthIdpConfig{Name: "projects/test/tenants/t1/oauthIdpConfigs/a"}
	api.oauthConfigs["projects/test/tenants/t1/oauthIdpConfigs/b"] = &OAuthIdpConfig{Name: "projects/test/tenants/t1/oauthIdpConfigs/b"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v2/projects/test/tenants/t1/oauthIdpConfigs", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	configs := resp["oauthIdpConfigs"].([]any)
	if len(configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(configs))
	}
}

func TestDeleteOAuthIdpConfig(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.oauthConfigs["projects/test/tenants/t1/oauthIdpConfigs/c1"] = &OAuthIdpConfig{Name: "projects/test/tenants/t1/oauthIdpConfigs/c1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v2/projects/test/tenants/t1/oauthIdpConfigs/c1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)

	api.mu.Lock()
	api.tenants["projects/p/tenants/t1"] = &Tenant{Name: "projects/p/tenants/t1", DisplayName: "T1"}
	api.oauthConfigs["projects/p/tenants/t1/oauthIdpConfigs/c1"] = &OAuthIdpConfig{Name: "projects/p/tenants/t1/oauthIdpConfigs/c1", ClientID: "abc"}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := newAPI(newTestAPI().opMgr, store)
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	if _, ok := api2.tenants["projects/p/tenants/t1"]; !ok {
		t.Fatal("tenant not found after reload")
	}
	if _, ok := api2.oauthConfigs["projects/p/tenants/t1/oauthIdpConfigs/c1"]; !ok {
		t.Fatal("oauth config not found after reload")
	}
	api2.mu.RUnlock()
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"displayName":"Tenant %d"}`, idx)
			req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/tenants", bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d", w.Code)
			}
		}(i)
	}
	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

type failingIdentityPlatformStore struct {
	mockStore
	fail bool
}

type postCommitIdentityPlatformStore struct {
	mockStore
	failNext bool
	loadErr  error
}

func (s *postCommitIdentityPlatformStore) Load(name string, target any) error {
	if s.loadErr != nil {
		return s.loadErr
	}
	return s.mockStore.Load(name, target)
}

func (s *postCommitIdentityPlatformStore) Save(name string, value any) error {
	if err := s.mockStore.Save(name, value); err != nil {
		return err
	}
	if s.failNext {
		s.failNext = false
		return errors.New("injected post-commit save failure")
	}
	return nil
}

type blockingFirstIdentityPlatformStore struct {
	mockStore
	saveEntered chan struct{}
	releaseSave chan struct{}
	once        sync.Once
}

func (s *blockingFirstIdentityPlatformStore) Save(name string, value any) error {
	first := false
	s.once.Do(func() {
		first = true
		close(s.saveEntered)
		<-s.releaseSave
	})
	if first {
		return errors.New("injected pre-commit save failure")
	}
	return s.mockStore.Save(name, value)
}

func (s *failingIdentityPlatformStore) Save(name string, value any) error {
	if s.fail {
		return fmt.Errorf("injected save failure")
	}
	return s.mockStore.Save(name, value)
}

type fakeAuthBackend struct {
	projectCalls      int
	tenantCalls       int
	lastProjectConfig *ProjectConfig
	lastTenantConfig  *TenantConfig
}

type sequencedAuthBackend struct {
	calls chan *TenantConfig
}

func newSequencedAuthBackend() *sequencedAuthBackend {
	return &sequencedAuthBackend{calls: make(chan *TenantConfig, 8)}
}

func (b *sequencedAuthBackend) ApplyProjectConfig(context.Context, string, *ProjectConfig) error {
	return nil
}

func (b *sequencedAuthBackend) ApplyTenantConfig(_ context.Context, _, _ string, config *TenantConfig) error {
	b.calls <- cloneTenantConfig(config)
	return nil
}

func (f *fakeAuthBackend) ApplyProjectConfig(_ context.Context, _ string, config *ProjectConfig) error {
	f.projectCalls++
	f.lastProjectConfig = cloneProjectConfig(config)
	return nil
}

func (f *fakeAuthBackend) ApplyTenantConfig(_ context.Context, _, _ string, config *TenantConfig) error {
	f.tenantCalls++
	f.lastTenantConfig = cloneTenantConfig(config)
	return nil
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return fmt.Errorf("not found: %w", state.ErrNotFound)
	}
	return json.Unmarshal(raw, target)
}

func (m *mockStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
