package servicedirectory

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestCreateNamespace(t *testing.T) {
	api := newTestAPI()
	body := `{"labels":{"env":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces?namespaceId=myns", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var ns Namespace
	_ = json.Unmarshal(w.Body.Bytes(), &ns)
	if ns.Name != "projects/test/locations/us-central1/namespaces/myns" {
		t.Fatalf("unexpected name: %s", ns.Name)
	}
	if ns.UID == "" {
		t.Fatal("expected uid")
	}
	if ns.CreateTime == "" {
		t.Fatal("expected createTime")
	}
	if ns.Labels["env"] != "test" {
		t.Fatalf("expected label env=test, got %v", ns.Labels)
	}
}

func TestOfficialAnnotationsAndOptionalEndpointAddressPort(t *testing.T) {
	api := newTestAPI()
	namespace := "projects/test/locations/us-central1/namespaces/ns"
	service := namespace + "/services/svc"
	api.namespaces[namespace] = &Namespace{Name: namespace}
	api.services[service] = &Service{Name: service}

	request := httptest.NewRequest(http.MethodPost,
		"/v1/"+service+"/endpoints?endpointId=endpoint",
		bytes.NewBufferString(`{"annotations":{"team":"platform"}}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var endpoint Endpoint
	if err := json.Unmarshal(response.Body.Bytes(), &endpoint); err != nil {
		t.Fatal(err)
	}
	if endpoint.Name != service+"/endpoints/endpoint" || endpoint.Address != "" || endpoint.Port != 0 {
		t.Fatalf("endpoint = %#v", endpoint)
	}
	if endpoint.Annotations["team"] != "platform" {
		t.Fatalf("annotations = %#v", endpoint.Annotations)
	}
}

func TestNamespacePageTokenRejectsCrossParentAndStaleSnapshot(t *testing.T) {
	api := newTestAPI()
	firstParent := "projects/p/locations/us"
	secondParent := "projects/p/locations/eu"
	api.namespaces[firstParent+"/namespaces/a"] = &Namespace{Name: firstParent + "/namespaces/a"}
	api.namespaces[firstParent+"/namespaces/b"] = &Namespace{Name: firstParent + "/namespaces/b"}
	api.namespaces[secondParent+"/namespaces/a"] = &Namespace{Name: secondParent + "/namespaces/a"}

	first := httptest.NewRecorder()
	api.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/"+firstParent+"/namespaces?pageSize=1", nil))
	var page struct {
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	crossParent := httptest.NewRecorder()
	api.ServeHTTP(crossParent, httptest.NewRequest(http.MethodGet,
		"/v1/"+secondParent+"/namespaces?pageSize=1&pageToken="+page.NextPageToken, nil))
	if crossParent.Code != http.StatusBadRequest {
		t.Fatalf("cross-parent status=%d body=%s", crossParent.Code, crossParent.Body.String())
	}

	api.namespaces[firstParent+"/namespaces/c"] = &Namespace{Name: firstParent + "/namespaces/c"}
	stale := httptest.NewRecorder()
	api.ServeHTTP(stale, httptest.NewRequest(http.MethodGet,
		"/v1/"+firstParent+"/namespaces?pageSize=1&pageToken="+page.NextPageToken, nil))
	if stale.Code != http.StatusBadRequest {
		t.Fatalf("stale status=%d body=%s", stale.Code, stale.Body.String())
	}
}

func TestCreateNamespaceMissingId(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateIDsMustBeCanonicalSinglePathComponents(t *testing.T) {
	tests := []struct {
		name string
		seed func(*API)
		url  string
		body string
	}{
		{
			name: "namespace",
			seed: func(*API) {},
			url:  "/v1/projects/test/locations/us/namespaces?namespaceId=bad%2Fid",
			body: `{}`,
		},
		{
			name: "service",
			seed: func(api *API) {
				parent := "projects/test/locations/us/namespaces/ns"
				api.namespaces[parent] = &Namespace{Name: parent}
			},
			url:  "/v1/projects/test/locations/us/namespaces/ns/services?serviceId=..",
			body: `{}`,
		},
		{
			name: "endpoint",
			seed: func(api *API) {
				namespace := "projects/test/locations/us/namespaces/ns"
				service := namespace + "/services/svc"
				api.namespaces[namespace] = &Namespace{Name: namespace}
				api.services[service] = &Service{Name: service}
			},
			url:  "/v1/projects/test/locations/us/namespaces/ns/services/svc/endpoints?endpointId=bad%5Cid",
			body: `{}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newTestAPI()
			test.seed(api)
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, test.url, bytes.NewBufferString(test.body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateNamespaceDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/dup"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/dup"}
	api.mu.Unlock()

	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces?namespaceId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

func TestGetNamespace(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/myns"] = &Namespace{
		Name: "projects/test/locations/us-central1/namespaces/myns",
		UID:  "uid-1",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/namespaces/myns", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var ns Namespace
	_ = json.Unmarshal(w.Body.Bytes(), &ns)
	if ns.UID != "uid-1" {
		t.Fatalf("unexpected uid: %s", ns.UID)
	}
}

func TestGetNamespaceNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/namespaces/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListNamespaces(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/a"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/a"}
	api.namespaces["projects/test/locations/us-central1/namespaces/b"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/b"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/namespaces", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["namespaces"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(items))
	}
}

func TestDeleteNamespace(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/del"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/del"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/namespaces/del", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestCreateService(t *testing.T) {
	api := newTestAPI()
	// Create parent namespace first
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/myns"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/myns"}
	api.mu.Unlock()

	body := `{"metadata":{"version":"v1"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces/myns/services?serviceId=mysvc", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var svc Service
	_ = json.Unmarshal(w.Body.Bytes(), &svc)
	if svc.Name != "projects/test/locations/us-central1/namespaces/myns/services/mysvc" {
		t.Fatalf("unexpected name: %s", svc.Name)
	}
	if svc.UID == "" {
		t.Fatal("expected uid")
	}
}

func TestCreateServiceParentNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces/nope/services?serviceId=svc", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateEndpoint(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/myns"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/myns"}
	api.services["projects/test/locations/us-central1/namespaces/myns/services/mysvc"] = &Service{Name: "projects/test/locations/us-central1/namespaces/myns/services/mysvc"}
	api.mu.Unlock()

	body := `{"address":"10.0.0.1","port":8080,"network":"projects/test/global/networks/default"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces/myns/services/mysvc/endpoints?endpointId=ep1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var ep Endpoint
	_ = json.Unmarshal(w.Body.Bytes(), &ep)
	if ep.Address != "10.0.0.1" {
		t.Fatalf("unexpected address: %s", ep.Address)
	}
	if ep.Port != 8080 {
		t.Fatalf("unexpected port: %d", ep.Port)
	}
	if ep.Network != "projects/test/global/networks/default" {
		t.Fatalf("unexpected network: %s", ep.Network)
	}
}

func TestCreateEndpointParentNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"address":"10.0.0.1","port":80}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces/myns/services/nope/endpoints?endpointId=ep1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestPatchNamespace(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/myns"] = &Namespace{
		Name:   "projects/test/locations/us-central1/namespaces/myns",
		Labels: map[string]string{"old": "val"},
	}
	api.mu.Unlock()

	body := `{"labels":{"new":"val2"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/namespaces/myns", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var ns Namespace
	_ = json.Unmarshal(w.Body.Bytes(), &ns)
	if ns.Labels["new"] != "val2" {
		t.Fatalf("expected updated labels, got %v", ns.Labels)
	}
}

func TestPatchEndpoint(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/myns"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/myns"}
	api.services["projects/test/locations/us-central1/namespaces/myns/services/mysvc"] = &Service{Name: "projects/test/locations/us-central1/namespaces/myns/services/mysvc"}
	api.endpoints["projects/test/locations/us-central1/namespaces/myns/services/mysvc/endpoints/ep1"] = &Endpoint{
		Name:    "projects/test/locations/us-central1/namespaces/myns/services/mysvc/endpoints/ep1",
		Address: "10.0.0.1",
		Port:    80,
	}
	api.mu.Unlock()

	body := `{"address":"10.0.0.2","port":443}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/namespaces/myns/services/mysvc/endpoints/ep1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var ep Endpoint
	_ = json.Unmarshal(w.Body.Bytes(), &ep)
	if ep.Address != "10.0.0.2" || ep.Port != 443 {
		t.Fatalf("unexpected endpoint: %+v", ep)
	}
}

func TestDeleteNamespaceWithChildrenFailsPrecondition(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.namespaces["projects/test/locations/us-central1/namespaces/myns"] = &Namespace{Name: "projects/test/locations/us-central1/namespaces/myns"}
	api.services["projects/test/locations/us-central1/namespaces/myns/services/svc1"] = &Service{Name: "projects/test/locations/us-central1/namespaces/myns/services/svc1"}
	api.endpoints["projects/test/locations/us-central1/namespaces/myns/services/svc1/endpoints/ep1"] = &Endpoint{Name: "projects/test/locations/us-central1/namespaces/myns/services/svc1/endpoints/ep1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/namespaces/myns", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected 412, got %d", w.Code)
	}

	api.mu.RLock()
	defer api.mu.RUnlock()
	if len(api.namespaces) != 1 {
		t.Fatal("namespace should remain")
	}
	if len(api.services) != 1 {
		t.Fatal("service should remain")
	}
	if len(api.endpoints) != 1 {
		t.Fatal("endpoint should remain")
	}
}

func TestConcurrentNamespaceCreation(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	conflicts := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := `{}`
			req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces?namespaceId=race", bytes.NewBufferString(body))
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

func TestFullHierarchy(t *testing.T) {
	api := newTestAPI()

	// Create namespace
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces?namespaceId=ns1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create ns: %d", w.Code)
	}

	// Create service
	body = `{"metadata":{"app":"web"}}`
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces/ns1/services?serviceId=svc1", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create svc: %d: %s", w.Code, w.Body.String())
	}

	// Create endpoint
	body = `{"address":"192.168.1.1","port":9090}`
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/namespaces/ns1/services/svc1/endpoints?endpointId=ep1", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create ep: %d: %s", w.Code, w.Body.String())
	}

	// Get endpoint
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/namespaces/ns1/services/svc1/endpoints/ep1", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("get ep: %d", w.Code)
	}

	// List endpoints
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/namespaces/ns1/services/svc1/endpoints", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list ep: %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["endpoints"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(items))
	}

	// Delete endpoint
	req = httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/namespaces/ns1/services/svc1/endpoints/ep1", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("delete ep: %d", w.Code)
	}

	// Verify deleted
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/namespaces/ns1/services/svc1/endpoints/ep1", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", w.Code)
	}
}
