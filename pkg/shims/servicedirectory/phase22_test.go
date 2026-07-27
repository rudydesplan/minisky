package servicedirectory

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"minisky/pkg/state"
)

func TestDeleteHierarchyRequiresEmptyParent(t *testing.T) {
	api := newTestAPI()
	namespace := "projects/p1/locations/us-central1/namespaces/ns1"
	service := namespace + "/services/svc1"
	api.namespaces[namespace] = &Namespace{Name: namespace}
	api.services[service] = &Service{Name: service}
	api.endpoints[service+"/endpoints/ep1"] = &Endpoint{Name: service + "/endpoints/ep1", Address: "127.0.0.1", Port: 8080}

	for _, path := range []string{"/v1/" + namespace, "/v1/" + service} {
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, path, nil))
		if rec.Code != http.StatusFailedDependency && rec.Code != http.StatusPreconditionFailed {
			t.Fatalf("%s status=%d, want precondition failure: %s", path, rec.Code, rec.Body.String())
		}
	}
	if len(api.namespaces) != 1 || len(api.services) != 1 || len(api.endpoints) != 1 {
		t.Fatal("failed parent delete changed hierarchy")
	}
}

func TestResolveServiceReturnsScopedEndpoints(t *testing.T) {
	api := newTestAPI()
	namespace := "projects/p1/locations/us-central1/namespaces/ns1"
	service := namespace + "/services/svc1"
	other := namespace + "/services/other"
	api.namespaces[namespace] = &Namespace{Name: namespace}
	api.services[service] = &Service{Name: service}
	api.services[other] = &Service{Name: other}
	api.endpoints[service+"/endpoints/ep1"] = &Endpoint{Name: service + "/endpoints/ep1", Address: "127.0.0.1", Port: 8080}
	api.endpoints[other+"/endpoints/ep2"] = &Endpoint{Name: other + "/endpoints/ep2", Address: "127.0.0.1", Port: 9090}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+service+":resolve", bytes.NewBufferString(`{}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Service *Service `json:"service"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if response.Service == nil || len(response.Service.Endpoints) != 1 || response.Service.Endpoints[0].Port != 8080 {
		t.Fatalf("unexpected resolved service: %+v", response.Service)
	}
}

func TestServiceDirectoryPageTokenIsParentBound(t *testing.T) {
	api := newTestAPI()
	for _, project := range []string{"p1", "p2"} {
		for _, id := range []string{"a", "b"} {
			name := "projects/" + project + "/locations/us-central1/namespaces/" + id
			api.namespaces[name] = &Namespace{Name: name}
		}
	}
	first := httptest.NewRecorder()
	api.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/projects/p1/locations/us-central1/namespaces?pageSize=1", nil))
	var response struct {
		NextPageToken string `json:"nextPageToken"`
	}
	_ = json.Unmarshal(first.Body.Bytes(), &response)
	if response.NextPageToken == "" {
		t.Fatal("missing page token")
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/projects/p2/locations/us-central1/namespaces?pageSize=1&pageToken="+response.NextPageToken, nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
	}
}

func TestServiceDirectoryHierarchySurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api := newTestAPI()
	api.stateStore = store
	namespace := "projects/p1/locations/us-central1/namespaces/ns1"
	service := namespace + "/services/svc1"
	endpoint := service + "/endpoints/ep1"
	api.namespaces[namespace] = &Namespace{Name: namespace}
	api.services[service] = &Service{Name: service}
	api.endpoints[endpoint] = &Endpoint{Name: endpoint, Address: "127.0.0.1", Port: 8080}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	restarted := newTestAPI()
	restarted.stateStore = store
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.namespaces[namespace] == nil || restarted.services[service] == nil || restarted.endpoints[endpoint] == nil {
		t.Fatal("service hierarchy was not restored")
	}
}

func TestNamespaceSaveFailureRollsBack(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingDirectoryStore{}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
		"/v1/projects/p1/locations/us-central1/namespaces?namespaceId=ns1", bytes.NewBufferString(`{}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if len(api.namespaces) != 0 {
		t.Fatal("failed namespace save remained visible")
	}
}

type failingDirectoryStore struct{}

func (failingDirectoryStore) Load(string, any) error { return state.ErrNotFound }
func (failingDirectoryStore) Save(string, any) error { return errors.New("disk full") }
