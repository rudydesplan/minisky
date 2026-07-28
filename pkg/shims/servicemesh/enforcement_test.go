package servicemesh

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
)

func enforceableRoute(name, host, matchPath, destination string) *HttpRoute {
	return &HttpRoute{
		Name: name, Hostnames: []string{host},
		Rules: []RouteRule{{
			Matches: []RouteMatch{{PrefixMatch: matchPath}},
			Action: &RouteAction{Destinations: []RouteDestination{{
				ServiceName: destination, Weight: 100,
			}}},
		}},
	}
}

func TestRouteHTTPMatchesHostPathAndProject(t *testing.T) {
	api := newTestAPI()
	api.httpRoutes["projects/p/locations/global/httpRoutes/api"] = enforceableRoute(
		"projects/p/locations/global/httpRoutes/api", "api.example.test", "/v1/", "backend",
	)
	for _, tc := range []struct {
		name        string
		project     string
		host        string
		path        string
		wantMatched bool
	}{
		{name: "match", project: "p", host: "api.example.test", path: "/v1/item", wantMatched: true},
		{name: "host miss", project: "p", host: "other.example.test", path: "/v1/item"},
		{name: "path miss", project: "p", host: "api.example.test", path: "/v2/item"},
		{name: "project isolation", project: "other", host: "api.example.test", path: "/v1/item"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matched, destination, route, err := api.RouteHTTP(tc.project, "global", tc.host, tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if matched != tc.wantMatched {
				t.Fatalf("matched=%t destination=%q route=%q", matched, destination, route)
			}
			if matched && (destination != "backend" || route == "") {
				t.Fatalf("destination=%q route=%q", destination, route)
			}
		})
	}
}

func TestRouteHTTPFailsClosedOnAmbiguityAndUnsupportedSemantics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		routes []*HttpRoute
	}{
		{name: "multiple routes", routes: []*HttpRoute{
			enforceableRoute("projects/p/locations/global/httpRoutes/a", "api.example.test", "/", "a"),
			enforceableRoute("projects/p/locations/global/httpRoutes/b", "api.example.test", "/", "b"),
		}},
		{name: "multiple destinations", routes: []*HttpRoute{{
			Name: "projects/p/locations/global/httpRoutes/a", Hostnames: []string{"api.example.test"},
			Rules: []RouteRule{{Action: &RouteAction{Destinations: []RouteDestination{
				{ServiceName: "a", Weight: 50}, {ServiceName: "b", Weight: 50},
			}}}},
		}}},
		{name: "regex", routes: []*HttpRoute{{
			Name: "projects/p/locations/global/httpRoutes/a", Hostnames: []string{"api.example.test"},
			Rules: []RouteRule{{
				Matches: []RouteMatch{{RegexMatch: ".*"}},
				Action:  &RouteAction{Destinations: []RouteDestination{{ServiceName: "a", Weight: 100}}},
			}},
		}}},
		{name: "weighted", routes: []*HttpRoute{
			enforceableRoute("projects/p/locations/global/httpRoutes/a", "api.example.test", "/", "a"),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newTestAPI()
			for _, route := range tc.routes {
				api.httpRoutes[route.Name] = route
			}
			if tc.name == "weighted" {
				api.httpRoutes[tc.routes[0].Name].Rules[0].Action.Destinations[0].Weight = 25
			}
			if _, _, _, err := api.RouteHTTP("p", "global", "api.example.test", "/v1"); err == nil {
				t.Fatal("expected fail-closed route error")
			}
		})
	}
}

func TestRouteHTTPUsesPersistedUpdateAfterRestart(t *testing.T) {
	store := &mockStore{data: map[string][]byte{}}
	api := newAPI(orchestrator.NewOperationManager(), store)
	name := "projects/p/locations/global/httpRoutes/api"
	api.httpRoutes[name] = enforceableRoute(name, "api.example.test", "/v1/", "before")
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	api.httpRoutes[name] = enforceableRoute(name, "api.example.test", "/v1/", "after")
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	restarted := newAPI(orchestrator.NewOperationManager(), store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	matched, destination, _, err := restarted.RouteHTTP("p", "global", "api.example.test", "/v1/item")
	if err != nil || !matched || destination != "after" {
		t.Fatalf("matched=%t destination=%q err=%v", matched, destination, err)
	}
}

type failServiceMeshStore struct {
	*mockStore
	fail bool
}

func (store *failServiceMeshStore) Save(name string, value any) error {
	if store.fail {
		return errors.New("injected save failure")
	}
	return store.mockStore.Save(name, value)
}

func TestHttpRoutePatchSaveFailureKeepsEnforcedRoute(t *testing.T) {
	store := &failServiceMeshStore{mockStore: &mockStore{data: map[string][]byte{}}}
	api := newAPI(orchestrator.NewOperationManager(), store)
	name := "projects/p/locations/global/httpRoutes/api"
	api.httpRoutes[name] = enforceableRoute(name, "api.example.test", "/v1/", "before")
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.fail = true
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, "/v1/"+name+"?updateMask=rules",
		strings.NewReader(`{"rules":[{"matches":[{"prefixMatch":"/v1/"}],"action":{"destinations":[{"serviceName":"after","weight":100}]}}]}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	matched, destination, _, err := api.RouteHTTP("p", "global", "api.example.test", "/v1/item")
	if err != nil || !matched || destination != "before" {
		t.Fatalf("matched=%t destination=%q err=%v", matched, destination, err)
	}
}

func TestRouteHTTPConcurrentRouteChanges(t *testing.T) {
	api := newTestAPI()
	name := "projects/p/locations/global/httpRoutes/api"
	api.httpRoutes[name] = enforceableRoute(name, "api.example.test", "/", "a")
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				matched, destination, _, err := api.RouteHTTP("p", "global", "api.example.test", "/item")
				if err != nil || !matched || destination != "a" && destination != "b" {
					t.Errorf("matched=%t destination=%q err=%v", matched, destination, err)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for index := range 100 {
			api.mu.Lock()
			if index%2 == 0 {
				api.httpRoutes[name].Rules[0].Action.Destinations[0].ServiceName = "a"
			} else {
				api.httpRoutes[name].Rules[0].Action.Destinations[0].ServiceName = "b"
			}
			api.mu.Unlock()
		}
	}()
	wg.Wait()
}
