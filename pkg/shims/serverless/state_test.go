package serverless

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestFunctionAndServiceMetadataRehydrateWithoutWorkers(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.functions["demo:us-central1:fn"] = &Function{
		Name:       "projects/demo/locations/us-central1/functions/fn",
		State:      "ACTIVE",
		SourceCode: "secret source",
		ServiceConfig: &ServiceConfig{
			EnvironmentVariables: map[string]string{"TOKEN": "sensitive"},
		},
	}
	api.services["demo:us-central1:svc"] = &Service{
		Name: "projects/demo/locations/us-central1/services/svc",
		Template: &RevisionTemplate{Containers: []Container{{
			Image: "example/image",
			Env:   []EnvVar{{Name: "TOKEN", Value: "sensitive"}},
		}}},
	}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewAPIWithStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.functions["demo:us-central1:fn"].SourceCode != "secret source" {
		t.Fatal("function metadata was not restored")
	}
	if restarted.services["demo:us-central1:svc"].Template.Containers[0].Image != "example/image" {
		t.Fatal("service metadata was not restored")
	}
	if restarted.client != http.DefaultClient {
		t.Fatal("transient HTTP client must not be persisted")
	}
}

func TestServerlessOperationPollingIsFullyScoped(t *testing.T) {
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	operations := []*orchestrator.Operation{
		manager.Register("cloudfunctions#operation", "CREATE",
			"projects/other/locations/us/functions/fn", "", "us"),
		manager.Register("cloudfunctions#operation", "CREATE",
			"projects/demo/locations/eu/functions/fn", "", "eu"),
		manager.Register("compute#operation", "insert",
			"https://www.googleapis.com/compute/v1/projects/demo/zones/us/instances/vm", "us", ""),
	}
	for _, operation := range operations {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
			"/v1/projects/demo/locations/us/operations/"+operation.Name, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("foreign operation %s status=%d body=%s", operation.Kind, response.Code, response.Body.String())
		}
	}
}
