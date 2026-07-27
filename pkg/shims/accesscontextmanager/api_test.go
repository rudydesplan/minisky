package accesscontextmanager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestCreateAccessPolicy(t *testing.T) {
	api := newTestAPI()
	body := `{"title":"My Policy","parent":"organizations/123456"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true")
	}

	api.mu.RLock()
	if len(api.policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(api.policies))
	}
	api.mu.RUnlock()
}

func TestPatchAccessPolicyRollsBackWhenStateSaveFails(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(orchestrator.NewOperationManager(), store)
	name := "accessPolicies/1"
	api.policies[name] = &AccessPolicy{Name: name, Parent: "organizations/1", Title: "before"}
	store.saveErr = errors.New("disk full")

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/accessPolicies/1?updateMask=title", bytes.NewBufferString(`{"title":"after"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got := api.policies[name].Title; got != "before" {
		t.Fatalf("committed title after failed save = %q", got)
	}
}

func TestPatchAccessPolicyDoesNotFabricateOperationWhenRegistrationFails(t *testing.T) {
	opStore := &alwaysFailStore{err: errors.New("operation save failed")}
	manager, err := orchestrator.NewOperationManagerWithStore(opStore)
	if err != nil {
		t.Fatal(err)
	}
	api := newAPI(manager, &mockStore{data: make(map[string][]byte)})
	name := "accessPolicies/1"
	api.policies[name] = &AccessPolicy{Name: name, Parent: "organizations/1", Title: "before"}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/accessPolicies/1?updateMask=title", bytes.NewBufferString(`{"title":"after"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got := api.policies[name].Title; got != "before" {
		t.Fatalf("resource committed without operation: %q", got)
	}
	if len(manager.List()) != 0 {
		t.Fatalf("orphan operations = %#v", manager.List())
	}
}

func TestListAccessPoliciesRejectsInvalidAndStaleTokens(t *testing.T) {
	api := newTestAPI()
	api.policies["accessPolicies/1"] = &AccessPolicy{Name: "accessPolicies/1", Parent: "organizations/1"}
	api.policies["accessPolicies/2"] = &AccessPolicy{Name: "accessPolicies/2", Parent: "organizations/1"}
	first := httptest.NewRecorder()
	api.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/accessPolicies?pageSize=1", nil))
	var page struct {
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.NextPageToken == "" {
		t.Fatal("missing opaque token")
	}
	api.policies["accessPolicies/3"] = &AccessPolicy{Name: "accessPolicies/3", Parent: "organizations/1"}
	stale := httptest.NewRecorder()
	api.ServeHTTP(stale, httptest.NewRequest(http.MethodGet,
		"/v1/accessPolicies?pageSize=1&pageToken="+page.NextPageToken, nil))
	if stale.Code != http.StatusBadRequest {
		t.Fatalf("stale token status=%d body=%s", stale.Code, stale.Body.String())
	}
	invalid := httptest.NewRecorder()
	api.ServeHTTP(invalid, httptest.NewRequest(http.MethodGet,
		"/v1/accessPolicies?pageToken=not-opaque", nil))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid token status=%d body=%s", invalid.Code, invalid.Body.String())
	}
}

func TestCreateAccessPolicyMissingParent(t *testing.T) {
	api := newTestAPI()
	body := `{"title":"No Parent"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateAccessPolicyInvalidParent(t *testing.T) {
	api := newTestAPI()
	body := `{"title":"Bad Parent","parent":"projects/123"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetAccessPolicy(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["accessPolicies/1"] = &AccessPolicy{
		Name:   "accessPolicies/1",
		Title:  "Test",
		Parent: "organizations/123",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/accessPolicies/1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var policy AccessPolicy
	_ = json.Unmarshal(w.Body.Bytes(), &policy)
	if policy.Title != "Test" {
		t.Fatalf("unexpected title: %s", policy.Title)
	}
}

func TestGetAccessPolicyNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/accessPolicies/999", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListAccessPolicies(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["accessPolicies/1"] = &AccessPolicy{Name: "accessPolicies/1", Parent: "organizations/1"}
	api.policies["accessPolicies/2"] = &AccessPolicy{Name: "accessPolicies/2", Parent: "organizations/1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/accessPolicies", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	policies := resp["accessPolicies"].([]any)
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
}

func TestDeleteAccessPolicyCascade(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["accessPolicies/1"] = &AccessPolicy{Name: "accessPolicies/1", Parent: "organizations/1"}
	api.perimeters["accessPolicies/1/servicePerimeters/sp1"] = &ServicePerimeter{Name: "accessPolicies/1/servicePerimeters/sp1"}
	api.levels["accessPolicies/1/accessLevels/al1"] = &AccessLevel{Name: "accessPolicies/1/accessLevels/al1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/accessPolicies/1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, policyExists := api.policies["accessPolicies/1"]
	_, perimeterExists := api.perimeters["accessPolicies/1/servicePerimeters/sp1"]
	_, levelExists := api.levels["accessPolicies/1/accessLevels/al1"]
	api.mu.RUnlock()
	if policyExists {
		t.Fatal("policy should be deleted")
	}
	if perimeterExists {
		t.Fatal("perimeter should be cascade deleted")
	}
	if levelExists {
		t.Fatal("level should be cascade deleted")
	}
}

func TestCreateServicePerimeter(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["accessPolicies/1"] = &AccessPolicy{Name: "accessPolicies/1", Parent: "organizations/1"}
	api.mu.Unlock()

	body := `{"title":"My Perimeter","status":{"resources":["projects/123"],"restrictedServices":["storage.googleapis.com"]}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies/1/servicePerimeters?servicePerimeterId=sp1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	sp, ok := api.perimeters["accessPolicies/1/servicePerimeters/sp1"]
	api.mu.RUnlock()
	if !ok {
		t.Fatal("perimeter not stored")
	}
	if sp.Title != "My Perimeter" {
		t.Fatalf("unexpected title: %s", sp.Title)
	}
}

func TestCreateServicePerimeterNoParent(t *testing.T) {
	api := newTestAPI()
	body := `{"title":"orphan"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies/999/servicePerimeters?servicePerimeterId=sp1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetServicePerimeter(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.perimeters["accessPolicies/1/servicePerimeters/sp1"] = &ServicePerimeter{
		Name:  "accessPolicies/1/servicePerimeters/sp1",
		Title: "SP1",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/accessPolicies/1/servicePerimeters/sp1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateAccessLevel(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["accessPolicies/1"] = &AccessPolicy{Name: "accessPolicies/1", Parent: "organizations/1"}
	api.mu.Unlock()

	body := `{"title":"Corp Network","basic":{"conditions":[{"ipSubnetworks":["10.0.0.0/8"],"regions":["US"]}]}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies/1/accessLevels?accessLevelId=corp-net", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	level, ok := api.levels["accessPolicies/1/accessLevels/corp-net"]
	api.mu.RUnlock()
	if !ok {
		t.Fatal("level not stored")
	}
	if level.Title != "Corp Network" {
		t.Fatalf("unexpected title: %s", level.Title)
	}
	if level.Basic == nil || len(level.Basic.Conditions) != 1 {
		t.Fatal("expected basic conditions")
	}
}

func TestCreateAccessLevelNoParent(t *testing.T) {
	api := newTestAPI()
	body := `{"title":"orphan"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies/999/accessLevels?accessLevelId=al1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteServicePerimeter(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.perimeters["accessPolicies/1/servicePerimeters/sp1"] = &ServicePerimeter{Name: "accessPolicies/1/servicePerimeters/sp1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/accessPolicies/1/servicePerimeters/sp1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, exists := api.perimeters["accessPolicies/1/servicePerimeters/sp1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("perimeter should be deleted")
	}
}

func TestDeleteAccessLevel(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.levels["accessPolicies/1/accessLevels/al1"] = &AccessLevel{Name: "accessPolicies/1/accessLevels/al1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/accessPolicies/1/accessLevels/al1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, exists := api.levels["accessPolicies/1/accessLevels/al1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("level should be deleted")
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)

	api.mu.Lock()
	api.seqNum = 3
	api.policies["accessPolicies/3"] = &AccessPolicy{Name: "accessPolicies/3", Parent: "organizations/1", Title: "P3"}
	api.perimeters["accessPolicies/3/servicePerimeters/sp1"] = &ServicePerimeter{Name: "accessPolicies/3/servicePerimeters/sp1", Title: "SP1"}
	api.levels["accessPolicies/3/accessLevels/al1"] = &AccessLevel{Name: "accessPolicies/3/accessLevels/al1", Title: "AL1"}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := newAPI(newTestAPI().opMgr, store)
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	if api2.seqNum != 3 {
		t.Fatalf("expected seqNum=3, got %d", api2.seqNum)
	}
	if _, ok := api2.policies["accessPolicies/3"]; !ok {
		t.Fatal("policy not found after reload")
	}
	if _, ok := api2.perimeters["accessPolicies/3/servicePerimeters/sp1"]; !ok {
		t.Fatal("perimeter not found after reload")
	}
	if _, ok := api2.levels["accessPolicies/3/accessLevels/al1"]; !ok {
		t.Fatal("level not found after reload")
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
			body := fmt.Sprintf(`{"title":"Policy %d","parent":"organizations/%d"}`, idx, idx)
			req := httptest.NewRequest(http.MethodPost, "/v1/accessPolicies", bytes.NewBufferString(body))
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
	mu      sync.Mutex
	data    map[string][]byte
	saveErr error
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
	if m.saveErr != nil {
		return m.saveErr
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}

type alwaysFailStore struct{ err error }

func (s *alwaysFailStore) Load(string, any) error { return state.ErrNotFound }
func (s *alwaysFailStore) Save(string, any) error { return s.err }
