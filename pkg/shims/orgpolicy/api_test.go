package orgpolicy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestCreatePolicy(t *testing.T) {
	api := newTestAPI()
	body := `{"name":"projects/my-project/policies/compute.disableSerialPortAccess","spec":{"rules":[{"enforce":true}]}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/policies", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp Policy
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp.Name != "projects/my-project/policies/compute.disableSerialPortAccess" {
		t.Fatalf("unexpected name: %s", resp.Name)
	}
	if resp.Spec == nil {
		t.Fatal("expected spec")
	}
	if resp.Spec.UpdateTime == "" {
		t.Fatal("expected spec.updateTime")
	}
	if len(resp.Spec.Rules) != 1 || !resp.Spec.Rules[0].Enforce {
		t.Fatalf("unexpected rules: %+v", resp.Spec.Rules)
	}
}

func TestCreatePolicyMissingName(t *testing.T) {
	api := newTestAPI()
	body := `{"spec":{"rules":[{"enforce":true}]}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/policies", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreatePolicyDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/my-project/policies/dup"] = &Policy{Name: "projects/my-project/policies/dup"}
	api.mu.Unlock()

	body := `{"name":"projects/my-project/policies/dup","spec":{}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/policies", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetPolicy(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/my-project/policies/test-get"] = &Policy{
		Name: "projects/my-project/policies/test-get",
		Spec: &PolicySpec{Rules: []PolicyRule{{Enforce: true}}},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/policies/test-get", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp Policy
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp.Name != "projects/my-project/policies/test-get" {
		t.Fatalf("unexpected name: %s", resp.Name)
	}
}

func TestGetPolicyNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/policies/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListPolicies(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/my-project/policies/a"] = &Policy{Name: "projects/my-project/policies/a"}
	api.policies["projects/my-project/policies/b"] = &Policy{Name: "projects/my-project/policies/b"}
	api.policies["projects/other/policies/c"] = &Policy{Name: "projects/other/policies/c"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/policies", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["policies"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 policies for my-project, got %d", len(items))
	}
}

func TestPatchPolicy(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/my-project/policies/patch-test"] = &Policy{
		Name: "projects/my-project/policies/patch-test",
		Spec: &PolicySpec{Rules: []PolicyRule{{Enforce: false}}},
	}
	api.mu.Unlock()

	body := `{"spec":{"rules":[{"enforce":true}]}}`
	req := httptest.NewRequest(http.MethodPatch, "/v2/projects/my-project/policies/patch-test", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp Policy
	_ = json.NewDecoder(w.Body).Decode(&resp)
	if resp.Spec == nil || len(resp.Spec.Rules) != 1 || !resp.Spec.Rules[0].Enforce {
		t.Fatalf("unexpected spec after patch: %+v", resp.Spec)
	}
	if resp.Spec.UpdateTime == "" {
		t.Fatal("expected spec.updateTime after patch")
	}
}

func TestPatchPolicyNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"spec":{"rules":[]}}`
	req := httptest.NewRequest(http.MethodPatch, "/v2/projects/my-project/policies/nope", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestDeletePolicy(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.policies["projects/my-project/policies/del"] = &Policy{Name: "projects/my-project/policies/del"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v2/projects/my-project/policies/del", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, exists := api.policies["projects/my-project/policies/del"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("policy should be deleted")
	}
}

func TestDeletePolicyNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v2/projects/my-project/policies/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListConstraints(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/constraints", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["constraints"].([]any)
	if len(items) != 5 {
		t.Fatalf("expected 5 pre-seeded constraints, got %d", len(items))
	}
}

func TestConstraintsReadOnly(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/constraints", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestConcurrentPolicyCreation(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	conflicts := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := `{"name":"projects/my-project/policies/race","spec":{}}`
			req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/policies", strings.NewReader(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code == http.StatusConflict {
				mu.Lock()
				conflicts++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if conflicts != 9 {
		t.Fatalf("expected 9 conflicts, got %d", conflicts)
	}
}

func TestPolicyCRUDLifecycle(t *testing.T) {
	api := newTestAPI()

	// Create
	body := `{"name":"projects/test/policies/lifecycle","spec":{"rules":[{"enforce":true}]}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/test/policies", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d: %s", w.Code, w.Body.String())
	}

	// Get
	req = httptest.NewRequest(http.MethodGet, "/v2/projects/test/policies/lifecycle", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("get: %d", w.Code)
	}

	// Patch
	body = `{"spec":{"rules":[{"enforce":false}]}}`
	req = httptest.NewRequest(http.MethodPatch, "/v2/projects/test/policies/lifecycle", strings.NewReader(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("patch: %d: %s", w.Code, w.Body.String())
	}
	var patched Policy
	_ = json.NewDecoder(w.Body).Decode(&patched)
	if patched.Spec.Rules[0].Enforce {
		t.Fatal("expected enforce=false after patch")
	}

	// Delete
	req = httptest.NewRequest(http.MethodDelete, "/v2/projects/test/policies/lifecycle", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("delete: %d", w.Code)
	}

	// Verify deleted
	req = httptest.NewRequest(http.MethodGet, "/v2/projects/test/policies/lifecycle", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", w.Code)
	}
}

func TestPolicyCreateRestartListDeleteLifecycle(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart-lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	newPersistentAPI := func(t *testing.T) *API {
		t.Helper()
		api := &API{
			stateStore: store, policies: make(map[string]*Policy),
			constraints: make(map[string]*Constraint),
		}
		api.seedConstraints()
		if err := api.loadState(); err != nil {
			t.Fatal(err)
		}
		return api
	}

	first := newPersistentAPI(t)
	body := `{"name":"projects/test/policies/compute.requireOsLogin","spec":{"rules":[{"enforce":true}]}}`
	create := httptest.NewRecorder()
	first.ServeHTTP(create, httptest.NewRequest(
		http.MethodPost, "/v2/projects/test/policies", strings.NewReader(body),
	))
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	restarted := newPersistentAPI(t)
	get := httptest.NewRecorder()
	restarted.ServeHTTP(get, httptest.NewRequest(
		http.MethodGet, "/v2/projects/test/policies/compute.requireOsLogin", nil,
	))
	if get.Code != http.StatusOK {
		t.Fatalf("post-restart get status=%d body=%s", get.Code, get.Body.String())
	}
	list := httptest.NewRecorder()
	restarted.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/v2/projects/test/policies", nil))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "compute.requireOsLogin") {
		t.Fatalf("post-restart list status=%d body=%s", list.Code, list.Body.String())
	}
	deleteResponse := httptest.NewRecorder()
	restarted.ServeHTTP(deleteResponse, httptest.NewRequest(
		http.MethodDelete, "/v2/projects/test/policies/compute.requireOsLogin", nil,
	))
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}

	afterDeleteRestart := newPersistentAPI(t)
	missing := httptest.NewRecorder()
	afterDeleteRestart.ServeHTTP(missing, httptest.NewRequest(
		http.MethodGet, "/v2/projects/test/policies/compute.requireOsLogin", nil,
	))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("post-delete restart get status=%d body=%s", missing.Code, missing.Body.String())
	}
}
