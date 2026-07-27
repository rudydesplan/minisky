package accesscontextmanager

import "testing"

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
	decision := api.CheckAccess(AccessRequest{Project: "../project", Service: "storage.googleapis.com"})
	if decision.Allowed || decision.Reason != "invalid project resource" {
		t.Fatalf("decision = %#v", decision)
	}
}
