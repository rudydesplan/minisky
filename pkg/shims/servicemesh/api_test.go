package servicemesh

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

func TestCreateMesh(t *testing.T) {
	api := newTestAPI()
	body := `{"description":"my mesh","labels":{"env":"dev"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/meshes?meshId=my-mesh", bytes.NewBufferString(body))
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
	mesh := api.meshes["projects/test/locations/global/meshes/my-mesh"]
	api.mu.RUnlock()
	if mesh == nil {
		t.Fatal("mesh not stored")
	}
	if mesh.Description != "my mesh" {
		t.Fatalf("unexpected description: %s", mesh.Description)
	}
}

func TestCreateMeshMissingID(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/meshes", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateMeshDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.meshes["projects/test/locations/global/meshes/dup"] = &Mesh{Name: "projects/test/locations/global/meshes/dup"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/meshes?meshId=dup", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetMesh(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.meshes["projects/test/locations/global/meshes/m1"] = &Mesh{
		Name:        "projects/test/locations/global/meshes/m1",
		Description: "test mesh",
		CreateTime:  "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/meshes/m1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var mesh Mesh
	_ = json.Unmarshal(w.Body.Bytes(), &mesh)
	if mesh.Description != "test mesh" {
		t.Fatalf("unexpected description: %s", mesh.Description)
	}
}

func TestGetMeshNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/meshes/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListMeshes(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.meshes["projects/test/locations/global/meshes/a"] = &Mesh{Name: "projects/test/locations/global/meshes/a"}
	api.meshes["projects/test/locations/global/meshes/b"] = &Mesh{Name: "projects/test/locations/global/meshes/b"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/meshes", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	meshes := resp["meshes"].([]any)
	if len(meshes) != 2 {
		t.Fatalf("expected 2 meshes, got %d", len(meshes))
	}
}

func TestPatchMesh(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.meshes["projects/test/locations/global/meshes/m1"] = &Mesh{
		Name:        "projects/test/locations/global/meshes/m1",
		Description: "old",
		CreateTime:  "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	body := `{"description":"new"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/global/meshes/m1?updateMask=description", bytes.NewBufferString(body))
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
	mesh := api.meshes["projects/test/locations/global/meshes/m1"]
	api.mu.RUnlock()
	if mesh.Description != "new" {
		t.Fatalf("expected updated description, got %s", mesh.Description)
	}
}

func TestDeleteMesh(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.meshes["projects/test/locations/global/meshes/m1"] = &Mesh{Name: "projects/test/locations/global/meshes/m1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/global/meshes/m1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, exists := api.meshes["projects/test/locations/global/meshes/m1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("mesh should be deleted")
	}
}

func TestCreateHttpRoute(t *testing.T) {
	api := newTestAPI()
	body := `{"hostnames":["*.example.com"],"meshes":["projects/test/locations/global/meshes/m1"],"rules":[{"matches":[{"prefixMatch":"/"}],"action":{"destinations":[{"serviceName":"svc","weight":100}]}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/httpRoutes?httpRouteId=route1", bytes.NewBufferString(body))
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
	route := api.httpRoutes["projects/test/locations/global/httpRoutes/route1"]
	api.mu.RUnlock()
	if route == nil {
		t.Fatal("route not stored")
	}
	if len(route.Hostnames) != 1 || route.Hostnames[0] != "*.example.com" {
		t.Fatalf("unexpected hostnames: %v", route.Hostnames)
	}
	if len(route.Meshes) != 1 {
		t.Fatalf("expected 1 mesh reference, got %d", len(route.Meshes))
	}
}

func TestCreateHttpRouteMissingHostnames(t *testing.T) {
	api := newTestAPI()
	body := `{"meshes":["projects/test/locations/global/meshes/m1"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/httpRoutes?httpRouteId=r1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateHttpRouteMissingID(t *testing.T) {
	api := newTestAPI()
	body := `{"hostnames":["example.com"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/httpRoutes", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetHttpRoute(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.httpRoutes["projects/test/locations/global/httpRoutes/r1"] = &HttpRoute{
		Name:      "projects/test/locations/global/httpRoutes/r1",
		Hostnames: []string{"example.com"},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/httpRoutes/r1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListHttpRoutes(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.httpRoutes["projects/test/locations/global/httpRoutes/a"] = &HttpRoute{Name: "projects/test/locations/global/httpRoutes/a", Hostnames: []string{"a.com"}}
	api.httpRoutes["projects/test/locations/global/httpRoutes/b"] = &HttpRoute{Name: "projects/test/locations/global/httpRoutes/b", Hostnames: []string{"b.com"}}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/httpRoutes", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	routes := resp["httpRoutes"].([]any)
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}
}

func TestDeleteHttpRoute(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.httpRoutes["projects/test/locations/global/httpRoutes/r1"] = &HttpRoute{Name: "projects/test/locations/global/httpRoutes/r1", Hostnames: []string{"x.com"}}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/global/httpRoutes/r1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, exists := api.httpRoutes["projects/test/locations/global/httpRoutes/r1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("route should be deleted")
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	body := `{"description":"op test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/meshes?meshId=op-test", bytes.NewBufferString(body))
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
	api.meshes["projects/p/locations/l/meshes/m1"] = &Mesh{
		Name:        "projects/p/locations/l/meshes/m1",
		Description: "persist test",
	}
	api.httpRoutes["projects/p/locations/l/httpRoutes/r1"] = &HttpRoute{
		Name:      "projects/p/locations/l/httpRoutes/r1",
		Hostnames: []string{"test.com"},
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
	mesh, ok := api2.meshes["projects/p/locations/l/meshes/m1"]
	if !ok {
		t.Fatal("mesh not found after reload")
	}
	if mesh.Description != "persist test" {
		t.Fatalf("unexpected description after reload: %s", mesh.Description)
	}
	route, ok := api2.httpRoutes["projects/p/locations/l/httpRoutes/r1"]
	if !ok {
		t.Fatal("route not found after reload")
	}
	if len(route.Hostnames) != 1 {
		t.Fatal("hostnames lost after reload")
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
			body := `{"description":"concurrent"}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/projects/test/locations/global/meshes?meshId=m-%d", idx), bytes.NewBufferString(body))
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
