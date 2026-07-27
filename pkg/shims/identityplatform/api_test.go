package identityplatform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

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

type fakeAuthBackend struct {
	projectCalls int
	tenantCalls  int
}

func (f *fakeAuthBackend) ApplyProjectConfig(context.Context, string, *ProjectConfig) error {
	f.projectCalls++
	return nil
}

func (f *fakeAuthBackend) ApplyTenantConfig(context.Context, string, string, *TenantConfig) error {
	f.tenantCalls++
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
