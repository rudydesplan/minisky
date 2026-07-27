package servicemesh

import "testing"

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
