package apigateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestCreateApi(t *testing.T) {
	api := newTestAPI()
	body := `{"displayName":"My API","managedService":"my-svc.endpoints.test.cloud.goog"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/apis?apiId=myapi", bytes.NewBufferString(body))
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
	name, _ := resp["name"].(string)
	if name == "" {
		t.Fatal("expected operation name in response")
	}
	meta, _ := resp["metadata"].(map[string]any)
	if meta == nil || meta["verb"] != "create" {
		t.Fatalf("unexpected metadata: %v", meta)
	}
	if meta["target"] != "projects/test/locations/global/apis/myapi" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}

	// Verify stored
	api.mu.RLock()
	stored := api.apis["projects/test/locations/global/apis/myapi"]
	api.mu.RUnlock()
	if stored == nil {
		t.Fatal("api not stored")
	}
	if stored.State != "ACTIVE" {
		t.Fatalf("expected ACTIVE state, got %s", stored.State)
	}
	if stored.DisplayName != "My API" {
		t.Fatalf("expected displayName 'My API', got %s", stored.DisplayName)
	}
	if stored.ManagedService != "my-svc.endpoints.test.cloud.goog" {
		t.Fatalf("expected managedService, got %s", stored.ManagedService)
	}
}

func TestCreateApiMissingId(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/apis", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateApiDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.apis["projects/test/locations/global/apis/dup"] = &Api{Name: "projects/test/locations/global/apis/dup"}
	api.mu.Unlock()

	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/apis?apiId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetApi(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.apis["projects/test/locations/global/apis/myapi"] = &Api{
		Name:        "projects/test/locations/global/apis/myapi",
		DisplayName: "Test",
		State:       "ACTIVE",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/apis/myapi", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var got Api
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got.DisplayName != "Test" {
		t.Fatalf("unexpected displayName: %s", got.DisplayName)
	}
}

func TestGetApiNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/apis/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListApis(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.apis["projects/test/locations/global/apis/a"] = &Api{Name: "projects/test/locations/global/apis/a"}
	api.apis["projects/test/locations/global/apis/b"] = &Api{Name: "projects/test/locations/global/apis/b"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/apis?pageSize=10", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["apis"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 apis, got %d", len(items))
	}
}

func TestDeleteApi(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.apis["projects/test/locations/global/apis/del"] = &Api{Name: "projects/test/locations/global/apis/del"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/global/apis/del", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected done=true for delete LRO")
	}

	api.mu.RLock()
	_, exists := api.apis["projects/test/locations/global/apis/del"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("api should be deleted")
	}
}

func TestDeleteApiNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/global/apis/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestCreateGateway(t *testing.T) {
	api := newTestAPI()
	apiName := "projects/test/locations/global/apis/myapi"
	configName := apiName + "/configs/cfg1"
	api.apis[apiName] = &Api{Name: apiName}
	api.configs[configName] = &ApiConfig{Name: configName, BackendURL: "http://127.0.0.1:8080"}
	body := `{"displayName":"My GW","apiConfig":"projects/test/locations/global/apis/myapi/configs/cfg1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/gateways?gatewayId=mygw", bytes.NewBufferString(body))
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
	stored := api.gateways["projects/test/locations/us-central1/gateways/mygw"]
	api.mu.RUnlock()
	if stored == nil {
		t.Fatal("gateway not stored")
	}
	if stored.State != "ACTIVE" {
		t.Fatalf("expected ACTIVE, got %s", stored.State)
	}
	if stored.DefaultHostname == "" {
		t.Fatal("expected defaultHostname to be set")
	}
	if stored.ApiConfig != "projects/test/locations/global/apis/myapi/configs/cfg1" {
		t.Fatalf("unexpected apiConfig: %s", stored.ApiConfig)
	}
}

func TestCreateGatewayMissingId(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/gateways", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateGatewayDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.gateways["projects/test/locations/us-central1/gateways/dup"] = &Gateway{Name: "projects/test/locations/us-central1/gateways/dup"}
	api.mu.Unlock()

	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/gateways?gatewayId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

func TestGetGateway(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.gateways["projects/test/locations/us-central1/gateways/mygw"] = &Gateway{
		Name:        "projects/test/locations/us-central1/gateways/mygw",
		DisplayName: "GW",
		State:       "ACTIVE",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/gateways/mygw", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestGetGatewayNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/gateways/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListGateways(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.gateways["projects/test/locations/us-central1/gateways/a"] = &Gateway{Name: "projects/test/locations/us-central1/gateways/a"}
	api.gateways["projects/test/locations/us-central1/gateways/b"] = &Gateway{Name: "projects/test/locations/us-central1/gateways/b"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/gateways", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["gateways"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 gateways, got %d", len(items))
	}
}

func TestDeleteGateway(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.gateways["projects/test/locations/us-central1/gateways/del"] = &Gateway{Name: "projects/test/locations/us-central1/gateways/del"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/gateways/del", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected done=true for delete LRO")
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	// Create an api to generate an operation
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/apis?apiId=optest", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed: %d", w.Code)
	}
	var createResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	opFullName, _ := createResp["name"].(string)
	if opFullName == "" {
		t.Fatal("no operation name returned")
	}

	// Poll the operation
	req = httptest.NewRequest(http.MethodGet, "/v1/"+opFullName, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperationNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/global/operations/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestPatchApi(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.apis["projects/test/locations/global/apis/myapi"] = &Api{
		Name:        "projects/test/locations/global/apis/myapi",
		DisplayName: "Old",
		State:       "ACTIVE",
	}
	api.mu.Unlock()

	body := `{"displayName":"New"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/global/apis/myapi", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected done=true for patch LRO")
	}

	api.mu.RLock()
	updated := api.apis["projects/test/locations/global/apis/myapi"]
	api.mu.RUnlock()
	if updated.DisplayName != "New" {
		t.Fatalf("expected displayName 'New', got %s", updated.DisplayName)
	}
}

func TestPatchGateway(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.gateways["projects/test/locations/us-central1/gateways/mygw"] = &Gateway{
		Name:      "projects/test/locations/us-central1/gateways/mygw",
		ApiConfig: "old-config",
		State:     "ACTIVE",
	}
	api.mu.Unlock()

	body := `{"apiConfig":"new-config"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/gateways/mygw", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	updated := api.gateways["projects/test/locations/us-central1/gateways/mygw"]
	api.mu.RUnlock()
	if updated.ApiConfig != "new-config" {
		t.Fatalf("expected apiConfig 'new-config', got %s", updated.ApiConfig)
	}
}

func TestPatchApiRollsBackWhenStateSaveFails(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingGatewayStore{}
	name := "projects/test/locations/global/apis/myapi"
	api.apis[name] = &Api{Name: name, DisplayName: "Old", State: "ACTIVE"}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/v1/"+name,
		bytes.NewBufferString(`{"displayName":"New"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if got := api.apis[name].DisplayName; got != "Old" {
		t.Fatalf("committed displayName after failed save = %q", got)
	}
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("orphan operations = %+v", operations)
	}
}

func TestPatchApiRollsBackWhenOperationRegistrationFails(t *testing.T) {
	manager, err := orchestrator.NewOperationManagerWithStore(failingGatewayStore{})
	if err != nil {
		t.Fatal(err)
	}
	api := NewAPI(manager)
	name := "projects/test/locations/global/apis/myapi"
	api.apis[name] = &Api{Name: name, DisplayName: "Old", State: "ACTIVE"}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/v1/"+name,
		bytes.NewBufferString(`{"displayName":"New"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if got := api.apis[name].DisplayName; got != "Old" {
		t.Fatalf("committed displayName after operation failure = %q", got)
	}
	if operations := manager.List(); len(operations) != 0 {
		t.Fatalf("orphan operations = %+v", operations)
	}
}

func TestConcurrentApiCreation(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	conflicts := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := `{}`
			req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/global/apis?apiId=race", bytes.NewBufferString(body))
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
