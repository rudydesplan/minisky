package networksecurity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestCreatePolicy(t *testing.T) {
	api := newTestAPI()
	body := `{"action":"ALLOW","description":"allow all","rules":[{"sources":[{"principals":["*"]}],"destinations":[{"hosts":["*.example.com"],"ports":[443]}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/authorizationPolicies?authorizationPolicyId=my-policy", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != false {
		t.Fatal("expected LRO not done")
	}

	api.mu.RLock()
	policy := api.policies["projects/test/locations/global/authorizationPolicies/my-policy"]
	api.mu.RUnlock()
	if policy == nil {
		t.Fatal("policy not stored")
	}
	if policy.Action != "ALLOW" {
		t.Fatalf("unexpected action: %s", policy.Action)
	}
	if len(policy.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(policy.Rules))
	}
}

func TestCreatePolicyMissingAction(t *testing.T) {
	api := newTestAPI()
	body := `{"description":"no action"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/authorizationPolicies?authorizationPolicyId=p1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreatePolicyInvalidAction(t *testing.T) {
	api := newTestAPI()
	body := `{"action":"INVALID"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/authorizationPolicies?authorizationPolicyId=p1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreatePolicyMissingID(t *testing.T) {
	api := newTestAPI()
	body := `{"action":"DENY"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/authorizationPolicies", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreatePolicyDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/test/locations/global/authorizationPolicies/dup"] = &AuthorizationPolicy{
		Name:   "projects/test/locations/global/authorizationPolicies/dup",
		Action: "ALLOW",
	}
	api.mu.Unlock()

	body := `{"action":"DENY"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/authorizationPolicies?authorizationPolicyId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetPolicy(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/test/locations/global/authorizationPolicies/p1"] = &AuthorizationPolicy{
		Name:        "projects/test/locations/global/authorizationPolicies/p1",
		Action:      "DENY",
		Description: "deny all",
		CreateTime:  "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/authorizationPolicies/p1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var policy AuthorizationPolicy
	_ = json.Unmarshal(w.Body.Bytes(), &policy)
	if policy.Action != "DENY" {
		t.Fatalf("unexpected action: %s", policy.Action)
	}
}

func TestGetPolicyNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/authorizationPolicies/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListPolicies(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/test/locations/global/authorizationPolicies/a"] = &AuthorizationPolicy{Name: "projects/test/locations/global/authorizationPolicies/a", Action: "ALLOW"}
	api.policies["projects/test/locations/global/authorizationPolicies/b"] = &AuthorizationPolicy{Name: "projects/test/locations/global/authorizationPolicies/b", Action: "DENY"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/authorizationPolicies", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	policies := resp["authorizationPolicies"].([]any)
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
}

func TestPatchPolicy(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/test/locations/global/authorizationPolicies/p1"] = &AuthorizationPolicy{
		Name:        "projects/test/locations/global/authorizationPolicies/p1",
		Action:      "ALLOW",
		Description: "old",
		CreateTime:  "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	body := `{"description":"new"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/global/authorizationPolicies/p1?updateMask=description", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for patch")
	}

	api.mu.RLock()
	policy := api.policies["projects/test/locations/global/authorizationPolicies/p1"]
	api.mu.RUnlock()
	if policy.Description != "new" {
		t.Fatalf("expected updated description, got %s", policy.Description)
	}
	if policy.Action != "ALLOW" {
		t.Fatal("action should be preserved")
	}
}

func TestDeletePolicy(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/test/locations/global/authorizationPolicies/p1"] = &AuthorizationPolicy{
		Name:   "projects/test/locations/global/authorizationPolicies/p1",
		Action: "ALLOW",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/global/authorizationPolicies/p1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for delete")
	}

	api.mu.RLock()
	_, exists := api.policies["projects/test/locations/global/authorizationPolicies/p1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("policy should be deleted")
	}
}

func TestDeletePolicyNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/global/authorizationPolicies/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	body := `{"action":"ALLOW"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/authorizationPolicies?authorizationPolicyId=op-test", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed: %d: %s", w.Code, w.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	opPath := createResp["name"].(string)

	req = httptest.NewRequest(http.MethodGet, "/v1/"+opPath, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)

	api.mu.Lock()
	api.policies["projects/p/locations/l/authorizationPolicies/p1"] = &AuthorizationPolicy{
		Name:   "projects/p/locations/l/authorizationPolicies/p1",
		Action: "DENY",
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := newAPI(newTestAPI().opMgr, store)
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	policy, ok := api2.policies["projects/p/locations/l/authorizationPolicies/p1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("policy not found after reload")
	}
	if policy.Action != "DENY" {
		t.Fatalf("unexpected action after reload: %s", policy.Action)
	}
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"action":"ALLOW"}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/projects/test/locations/global/authorizationPolicies?authorizationPolicyId=p-%d", idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
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
