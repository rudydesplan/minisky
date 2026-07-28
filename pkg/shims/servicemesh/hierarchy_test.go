package servicemesh

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidateReferencesRequiresOfficialHierarchy(t *testing.T) {
	api := newTestAPI()
	api.meshes["projects/p/locations/global/meshes/main"] = &Mesh{
		Name: "projects/p/locations/global/meshes/main",
	}

	valid := &HttpRoute{
		Name:   "projects/p/locations/global/httpRoutes/route",
		Meshes: []string{"projects/p/locations/global/meshes/main"},
	}
	if err := api.ValidateReferences(valid); err != nil {
		t.Fatalf("valid references: %v", err)
	}

	crossProject := &HttpRoute{
		Name:   "projects/p/locations/global/httpRoutes/route",
		Meshes: []string{"projects/other/locations/global/meshes/main"},
	}
	if err := api.ValidateReferences(crossProject); err == nil {
		t.Fatal("expected cross-project hierarchy error")
	}
}

func TestRouteDecisionIsMetadataOnly(t *testing.T) {
	api := newTestAPI()
	api.httpRoutes["projects/p/locations/global/httpRoutes/route"] = &HttpRoute{
		Name:      "projects/p/locations/global/httpRoutes/route",
		Hostnames: []string{"api.local"},
		Rules: []RouteRule{{
			Matches: []RouteMatch{{PrefixMatch: "/v1/"}},
			Action: &RouteAction{Destinations: []RouteDestination{{
				ServiceName: "projects/p/locations/global/backendServices/api",
				Weight:      100,
			}}},
		}},
	}
	decision := api.ResolveRoute("p", "global", "api.local", "/v1/items")
	if !decision.Matched || decision.Enforcement != "METADATA_ONLY" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestPatchHttpRouteRejectsCrossProjectMesh(t *testing.T) {
	api := newTestAPI()
	name := "projects/p/locations/global/httpRoutes/route"
	api.httpRoutes[name] = &HttpRoute{
		Name: name, Hostnames: []string{"api.local"},
		Meshes: []string{"projects/p/locations/global/meshes/main"},
	}

	request := httptest.NewRequest(http.MethodPatch,
		"/v1/"+name+"?updateMask=meshes",
		strings.NewReader(`{"meshes":["projects/other/locations/global/meshes/main"]}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if got := api.httpRoutes[name].Meshes; len(got) != 1 ||
		got[0] != "projects/p/locations/global/meshes/main" {
		t.Fatalf("route meshes = %#v", got)
	}
}

func TestResolveRequestUsesStoredRouteHierarchy(t *testing.T) {
	api := newTestAPI()
	api.httpRoutes["projects/p/locations/global/httpRoutes/route"] = &HttpRoute{
		Name:      "projects/p/locations/global/httpRoutes/route",
		Hostnames: []string{"api.local"},
		Rules: []RouteRule{{
			Matches: []RouteMatch{{PrefixMatch: "/v1/"}},
			Action: &RouteAction{Destinations: []RouteDestination{{
				ServiceName: "projects/p/locations/global/backendServices/api",
				Weight:      100,
			}}},
		}},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/p/locations/global/httpRoutes:resolve",
		strings.NewReader(`{"project":"p","location":"global","host":"api.local","path":"/v1/items"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var decision RouteDecision
	if err := json.Unmarshal(response.Body.Bytes(), &decision); err != nil {
		t.Fatal(err)
	}
	if !decision.Matched || len(decision.Destinations) != 1 {
		t.Fatalf("decision = %#v", decision)
	}
}
