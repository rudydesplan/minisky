package accesscontextmanager

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/router"
)

func TestCheckAccessServicePerimeterDecision(t *testing.T) {
	api := newTestAPI()
	api.perimeters["accessPolicies/1/servicePerimeters/prod"] = &ServicePerimeter{
		Name: "accessPolicies/1/servicePerimeters/prod",
		Status: &PerimeterStatus{
			Resources:          []string{"projects/123"},
			RestrictedServices: []string{"storage.googleapis.com"},
		},
	}

	denied := api.CheckAccess(AccessRequest{
		Project: "projects/123",
		Service: "storage.googleapis.com",
	})
	if denied.Allowed || denied.Reason != "restricted by service perimeter" {
		t.Fatalf("denied = %#v", denied)
	}

	allowed := api.CheckAccess(AccessRequest{
		Project: "projects/123",
		Service: "pubsub.googleapis.com",
	})
	if !allowed.Allowed {
		t.Fatalf("allowed = %#v", allowed)
	}
}

func TestCheckAccessFailsClosedForInvalidProject(t *testing.T) {
	api := newTestAPI()
	for _, project := range []string{"../project", "projects/..", "projects/project a", "projects/project/a"} {
		decision := api.CheckAccess(AccessRequest{Project: project, Service: "storage.googleapis.com"})
		if decision.Allowed || decision.Reason != "invalid project resource" {
			t.Fatalf("project %q decision = %#v", project, decision)
		}
	}
}

func TestCheckAccessOverlappingPerimetersFailClosedDeterministically(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(orchestrator.NewOperationManager(), store)
	levelName := "accessPolicies/1/accessLevels/corp"
	api.levels[levelName] = &AccessLevel{
		Name: levelName,
		Basic: &BasicLevel{Conditions: []Condition{{
			IpSubnetworks: []string{"10.0.0.0/8"},
		}}},
	}
	for _, perimeter := range []*ServicePerimeter{
		{
			Name: "accessPolicies/1/servicePerimeters/z-allow",
			Status: &PerimeterStatus{
				Resources:          []string{"projects/123"},
				RestrictedServices: []string{"storage.googleapis.com"},
				AccessLevels:       []string{levelName},
			},
		},
		{
			Name: "accessPolicies/1/servicePerimeters/a-deny",
			Status: &PerimeterStatus{
				Resources:          []string{"projects/123"},
				RestrictedServices: []string{"storage.googleapis.com"},
			},
		},
	} {
		api.perimeters[perimeter.Name] = perimeter
	}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	restarted := newAPI(orchestrator.NewOperationManager(), store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}

	for _, evaluator := range []*API{api, restarted} {
		for range 20 {
			decision := evaluator.CheckAccess(AccessRequest{
				Project: "projects/123", Service: "storage.googleapis.com", SourceIP: "10.1.2.3",
			})
			if decision.Allowed ||
				decision.Perimeter != "accessPolicies/1/servicePerimeters/a-deny" ||
				decision.Reason != "restricted by service perimeter" {
				t.Fatalf("decision = %#v", decision)
			}
		}
	}
}

func TestCheckAccessRequestUsesPersistedAccessLevelConditions(t *testing.T) {
	api := newTestAPI()
	levelName := "accessPolicies/1/accessLevels/corp"
	api.levels[levelName] = &AccessLevel{
		Name: levelName,
		Basic: &BasicLevel{Conditions: []Condition{{
			IpSubnetworks: []string{"10.0.0.0/8"},
			Regions:       []string{"US"},
		}}},
	}
	api.perimeters["accessPolicies/1/servicePerimeters/prod"] = &ServicePerimeter{
		Name: "accessPolicies/1/servicePerimeters/prod",
		Status: &PerimeterStatus{
			Resources:          []string{"projects/123"},
			RestrictedServices: []string{"storage.googleapis.com"},
			AccessLevels:       []string{levelName},
		},
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/accessPolicies/1:checkAccess",
		strings.NewReader(`{"project":"projects/123","service":"storage.googleapis.com","sourceIp":"10.1.2.3","region":"US"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var decision AccessDecision
	if err := json.Unmarshal(response.Body.Bytes(), &decision); err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed || decision.Reason != "access level matched" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestPersistedServicePerimeterControlsRealGatewayDispatch(t *testing.T) {
	api := newTestAPI()
	api.perimeters["accessPolicies/1/servicePerimeters/prod"] = &ServicePerimeter{
		Name: "accessPolicies/1/servicePerimeters/prod",
		Status: &PerimeterStatus{
			Resources:          []string{"projects/project-a"},
			RestrictedServices: []string{"storage.googleapis.com"},
		},
	}

	tests := []struct {
		name        string
		domain      string
		path        string
		userProject string
		wantStatus  int
	}{
		{
			name:       "project and service membership denied",
			domain:     "storage.googleapis.com",
			path:       "/storage/v1/b?project=project-a",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "service outside perimeter allowed",
			domain:     "pubsub.googleapis.com",
			path:       "/v1/projects/project-a/topics",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "other project allowed",
			domain:     "storage.googleapis.com",
			path:       "/storage/v1/b?project=project-b",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "invalid project denied without strict IAM",
			domain:     "storage.googleapis.com",
			path:       "/storage/v1/b?project=project_a",
			wantStatus: http.StatusForbidden,
		},
		{
			name:        "conflicting project context cannot bypass boundary",
			domain:      "storage.googleapis.com",
			path:        "/storage/v1/b?project=project-b",
			userProject: "project-a",
			wantStatus:  http.StatusForbidden,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dispatches := 0
			gateway := router.NewProxyRouterWithManager(nil)
			gateway.RegisterShim(test.domain, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				dispatches++
				w.WriteHeader(http.StatusNoContent)
			}))
			gateway.ConfigureServicePerimeters(api)

			request := httptest.NewRequest(
				http.MethodGet,
				"http://127.0.0.1/_minisky/"+strings.Split(test.domain, ".")[0]+test.path,
				nil,
			)
			if test.userProject != "" {
				request.Header.Set("X-Goog-User-Project", test.userProject)
			}
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			wantDispatches := 1
			if test.wantStatus == http.StatusForbidden {
				wantDispatches = 0
			}
			if dispatches != wantDispatches {
				t.Fatalf("dispatches=%d want=%d", dispatches, wantDispatches)
			}
		})
	}
}

func TestAccessContextStateLoadFailureIsStickyAndFailsClosed(t *testing.T) {
	store := &mockStore{data: map[string][]byte{
		acmStateEntry: []byte(`{"perimeters":`),
	}}
	api := newAPI(orchestrator.NewOperationManager(), store)
	api.perimeters["accessPolicies/old/servicePerimeters/preserved"] = &ServicePerimeter{
		Name: "accessPolicies/old/servicePerimeters/preserved",
		Status: &PerimeterStatus{
			Resources:          []string{"projects/preserved"},
			RestrictedServices: []string{"storage.googleapis.com"},
		},
	}
	if err := api.loadState(); err == nil {
		t.Fatal("expected corrupt state load failure")
	}
	if api.PersistenceError() == nil {
		t.Fatal("state load failure was not sticky")
	}
	if _, ok := api.perimeters["accessPolicies/old/servicePerimeters/preserved"]; !ok {
		t.Fatal("restore failure replaced prior in-memory state")
	}
	store.mu.Lock()
	corruptBefore := append([]byte(nil), store.data[acmStateEntry]...)
	store.mu.Unlock()

	gateway := router.NewProxyRouterWithManager(nil)
	dispatched := false
	gateway.RegisterShim("storage.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		dispatched = true
	}))
	gateway.ConfigureServicePerimeters(api)
	gatewayResponse := httptest.NewRecorder()
	gateway.ServeHTTP(gatewayResponse, httptest.NewRequest(
		http.MethodGet,
		"http://127.0.0.1/_minisky/storage/storage/v1/b?project=unconfigured",
		nil,
	))
	if gatewayResponse.Code != http.StatusForbidden || dispatched {
		t.Fatalf("gateway status=%d dispatched=%t body=%s",
			gatewayResponse.Code, dispatched, gatewayResponse.Body.String())
	}

	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/accessPolicies", nil),
		httptest.NewRequest(http.MethodPost, "/v1/accessPolicies", strings.NewReader(`{"parent":"organizations/1"}`)),
	} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status=%d body=%s", request.Method, response.Code, response.Body.String())
		}
		var envelope struct {
			Error struct {
				Code   int    `json:"code"`
				Status string `json:"status"`
			} `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Error.Code != http.StatusServiceUnavailable || envelope.Error.Status != "UNAVAILABLE" {
			t.Fatalf("error envelope = %#v", envelope.Error)
		}
	}
	store.mu.Lock()
	corruptAfter := append([]byte(nil), store.data[acmStateEntry]...)
	store.mu.Unlock()
	if string(corruptAfter) != string(corruptBefore) {
		t.Fatalf("degraded read/mutation replaced corrupt durable state: %q", corruptAfter)
	}
	if _, ok := api.perimeters["accessPolicies/old/servicePerimeters/preserved"]; !ok {
		t.Fatal("degraded read/mutation changed preserved in-memory state")
	}
}

type accessContextLoadFailStore struct {
	err error
}

func (store accessContextLoadFailStore) Load(string, any) error {
	return store.err
}

func (accessContextLoadFailStore) Save(string, any) error {
	return nil
}

func TestAccessContextStateLoadIOFailurePreservesPriorState(t *testing.T) {
	api := newAPI(orchestrator.NewOperationManager(), accessContextLoadFailStore{
		err: errors.New("state unavailable"),
	})
	api.policies["accessPolicies/preserved"] = &AccessPolicy{Name: "accessPolicies/preserved"}

	if err := api.loadState(); err == nil {
		t.Fatal("expected state load failure")
	}
	if api.PersistenceError() == nil {
		t.Fatal("state load failure was not sticky")
	}
	if _, ok := api.policies["accessPolicies/preserved"]; !ok {
		t.Fatal("load failure replaced prior state")
	}
	matched, allowed := api.EvaluateServicePerimeter(
		"projects/unconfigured", "storage.googleapis.com", "", "",
	)
	if !matched || allowed {
		t.Fatalf("degraded decision configured=%t allowed=%t", matched, allowed)
	}
}

func TestAccessContextInvalidPersistedResourceNamesFailClosed(t *testing.T) {
	persisted, err := json.Marshal(acmMetadata{
		Perimeters: map[string]*ServicePerimeter{
			"accessPolicies/1/servicePerimeters/prod": {
				Name: "accessPolicies/1/servicePerimeters/other",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &mockStore{data: map[string][]byte{acmStateEntry: persisted}}
	api := newAPI(orchestrator.NewOperationManager(), store)
	api.perimeters["accessPolicies/preserved/servicePerimeters/prod"] = &ServicePerimeter{
		Name: "accessPolicies/preserved/servicePerimeters/prod",
	}

	if err := api.loadState(); err == nil {
		t.Fatal("expected invalid persisted resource failure")
	}
	if api.PersistenceError() == nil {
		t.Fatal("invalid persisted resource did not degrade enforcement")
	}
	if len(api.perimeters) != 1 ||
		api.perimeters["accessPolicies/preserved/servicePerimeters/prod"] == nil {
		t.Fatalf("invalid restore replaced prior state: %#v", api.perimeters)
	}
}

func TestServicePerimeterGatewayDecisionSurvivesRestart(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	before := newAPI(orchestrator.NewOperationManager(), store)
	before.perimeters["accessPolicies/1/servicePerimeters/prod"] = &ServicePerimeter{
		Name: "accessPolicies/1/servicePerimeters/prod",
		Status: &PerimeterStatus{
			Resources:          []string{"projects/project-a"},
			RestrictedServices: []string{"storage.googleapis.com"},
		},
	}
	if err := before.persistState(); err != nil {
		t.Fatal(err)
	}

	after := newAPI(orchestrator.NewOperationManager(), store)
	if err := after.loadState(); err != nil {
		t.Fatal(err)
	}
	gateway := router.NewProxyRouterWithManager(nil)
	dispatched := false
	gateway.RegisterShim("storage.googleapis.com", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		dispatched = true
	}))
	gateway.ConfigureServicePerimeters(after)

	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet,
		"http://127.0.0.1/_minisky/storage/storage/v1/b?project=project-a",
		nil,
	))
	if response.Code != http.StatusForbidden || dispatched {
		t.Fatalf("restart decision status=%d dispatched=%t body=%s", response.Code, dispatched, response.Body.String())
	}
}

func TestFailedServicePerimeterSaveDoesNotChangeGatewayDecision(t *testing.T) {
	api := newAPI(orchestrator.NewOperationManager(), &alwaysFailStore{err: errors.New("save failed")})
	api.policies["accessPolicies/1"] = &AccessPolicy{Name: "accessPolicies/1"}
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(
		http.MethodPost,
		"/v1/accessPolicies/1/servicePerimeters?servicePerimeterId=prod",
		strings.NewReader(`{"status":{"resources":["projects/project-a"],"restrictedServices":["storage.googleapis.com"]}}`),
	))
	if create.Code != http.StatusServiceUnavailable {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	gateway := router.NewProxyRouterWithManager(nil)
	dispatches := 0
	gateway.RegisterShim("storage.googleapis.com", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dispatches++
		w.WriteHeader(http.StatusNoContent)
	}))
	gateway.ConfigureServicePerimeters(api)
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet,
		"http://127.0.0.1/_minisky/storage/storage/v1/b?project=project-a",
		nil,
	))
	if response.Code != http.StatusNoContent || dispatches != 1 {
		t.Fatalf("rolled-back decision status=%d dispatches=%d body=%s",
			response.Code, dispatches, response.Body.String())
	}
}
