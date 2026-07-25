package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"minisky/pkg/config"
	"minisky/pkg/shims/bigquery"
	"minisky/pkg/shims/gke"
	"minisky/pkg/shims/serverless"
)

func TestSettingsReportsEffectiveBackendState(t *testing.T) {
	t.Setenv(config.RuntimeProfileEnv, "simulation")
	t.Setenv("MINISKY_BQ_BACKEND", "")
	t.Setenv("MINISKY_GKE_BACKEND", "")
	t.Setenv("MINISKY_SERVERLESS_BACKEND", "")

	api := &API{
		bqBackend:   bigquery.NewDuckDBBackend(),
		gkeBackend:  gke.NewKindBackend(),
		servBackend: serverless.NewBuildpacksBackend(),
	}
	defer api.bqBackend.Close()

	request := httptest.NewRequest(http.MethodGet, "/api/settings", nil)
	response := httptest.NewRecorder()
	api.handleSettings(response, request)

	var body struct {
		BigQuery   bool                           `json:"bq_duckdb"`
		GKE        bool                           `json:"gke_kind"`
		Serverless bool                           `json:"serverless_pack"`
		Profile    config.RuntimeProfile          `json:"runtime_profile"`
		Backends   map[string]config.BackendState `json:"backends"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Profile.Name != config.RuntimeProfileSimulation {
		t.Fatalf("profile = %#v, want simulation", body.Profile)
	}
	for name, enabled := range map[string]bool{
		"bigquery": body.BigQuery, "gke": body.GKE, "serverless": body.Serverless,
	} {
		state := body.Backends[name]
		if state.Enabled != enabled {
			t.Errorf("%s state enabled = %t, legacy setting = %t", name, state.Enabled, enabled)
		}
		if state.Backend != config.RuntimeProfileSimulation {
			t.Errorf("%s backend = %q, want simulation", name, state.Backend)
		}
	}
}
