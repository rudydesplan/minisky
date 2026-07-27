package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	servicecontrol "google.golang.org/api/servicecontrol/v1"
)

func TestValidateLoopbackEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
		"http://localhost:8080",
	} {
		if err := validateLoopbackEndpoint(endpoint); err != nil {
			t.Errorf("validateLoopbackEndpoint(%q): %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{
		"", "https://127.0.0.1:8080", "http://192.0.2.1:8080",
		"http://169.254.169.254:80", "http://localhost",
		"http://user@localhost:8080", "http://localhost:8080/path",
		"http://localhost:8080?query=1", "http://localhost:8080/#fragment",
	} {
		if err := validateLoopbackEndpoint(endpoint); err == nil {
			t.Errorf("validateLoopbackEndpoint(%q) unexpectedly succeeded", endpoint)
		}
	}
}

func TestConfigRequiresOptInSafeIdentifiersAndBoundedEvidencePath(t *testing.T) {
	setValidEnv(t)
	t.Setenv(optInEnv, "1")
	t.Setenv("MINISKY_PHASE21_22_MODE", "create")
	if _, err := configFromEnv(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := []struct {
		name, key, value, want string
	}{
		{"non-loopback", "MINISKY_ENDPOINT", "http://example.com:8080", "loopback"},
		{"unsafe ID", "MINISKY_PHASE21_22_API_ID", "../api", "API ID"},
		{"relative evidence", "MINISKY_PHASE21_22_EVIDENCE", "evidence.json", "absolute"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(optInEnv, "1")
			t.Setenv("MINISKY_PHASE21_22_MODE", "create")
			t.Setenv(test.key, test.value)
			if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want substring %q", err, test.want)
			}
		})
	}

	setValidEnv(t)
	t.Setenv("MINISKY_PHASE21_22_MODE", "create")
	t.Setenv(optInEnv, "")
	if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), optInEnv) {
		t.Fatalf("missing opt-in error=%v", err)
	}
}

func TestGeneratedClientsUseCanonicalFullDomainPaths(t *testing.T) {
	responses := map[string]string{
		"/_minisky/cloudtrace.googleapis.com/v1/projects/demo/traces":                                   `{"traces":[]}`,
		"/_minisky/clouderrorreporting.googleapis.com/v1beta1/projects/demo/groupStats":                 `{"errorGroupStats":[]}`,
		"/_minisky/cloudprofiler.googleapis.com/v2/projects/demo/profiles":                              `{"profiles":[]}`,
		"/_minisky/apigateway.googleapis.com/v1/projects/demo/locations/global/apis":                    `{"apis":[]}`,
		"/_minisky/servicedirectory.googleapis.com/v1/projects/demo/locations/us-central1/namespaces":   `{"namespaces":[]}`,
		"/_minisky/servicemanagement.googleapis.com/v1/services/example.endpoints.test/configs":         `{"serviceConfigs":[]}`,
		"/_minisky/servicecontrol.googleapis.com/v1/services/example.endpoints.test:allocateQuota":      `{"allocateErrors":[]}`,
		"/_minisky/clouddeploy.googleapis.com/v1/projects/demo/locations/us-central1/deliveryPipelines": `{"deliveryPipelines":[]}`,
		"/_minisky/binaryauthorization.googleapis.com/v1/projects/demo/policy":                          `{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"ALWAYS_ALLOW"}}`,
	}
	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.Path]
		if !ok {
			http.Error(w, fmt.Sprintf("unexpected path %q", r.URL.Path), http.StatusNotFound)
			return
		}
		seen[r.URL.Path] = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	c, err := newClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.traceRead.Projects.Traces.List("demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.errors.Projects.GroupStats.List("projects/demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.profiler.Projects.Profiles.List("projects/demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.gateway.Projects.Locations.Apis.List("projects/demo/locations/global").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.directory.Projects.Locations.Namespaces.List("projects/demo/locations/us-central1").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.management.Services.Configs.List("example.endpoints.test").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.control.Services.AllocateQuota("example.endpoints.test", &servicecontrol.AllocateQuotaRequest{}).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.deploy.Projects.Locations.DeliveryPipelines.List("projects/demo/locations/us-central1").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.binauthz.Projects.GetPolicy("projects/demo/policy").Do(); err != nil {
		t.Fatal(err)
	}
	for path := range responses {
		if !seen[path] {
			t.Errorf("generated client did not request %s", path)
		}
	}
}

func TestMiniSkyLocalGatewayProxyIsExecutableAndLoopbackBound(t *testing.T) {
	if err := proveMiniSkyLocalGatewayProxy(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceRoundTripIsStrictBoundedAndProjectScoped(t *testing.T) {
	cfg := testConfig(t)
	record := validEvidence(cfg)
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(cfg.evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("evidence mode=%o want=600", info.Mode().Perm())
	}
	got, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got != record {
		t.Fatalf("evidence=%+v want=%+v", got, record)
	}

	t.Run("unknown field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte(`{"version":1,"unexpected":true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("oversized", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte(strings.Repeat("x", maxEvidenceBytes+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg); err == nil || !strings.Contains(err.Error(), "size limit") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("other project", func(t *testing.T) {
		other := cfg
		other.project = "other"
		if _, err := readEvidence(cfg.evidencePath, other); err == nil || !strings.Contains(err.Error(), "do not match") {
			t.Fatalf("error=%v", err)
		}
	})
}

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:8080")
	t.Setenv("MINISKY_PROJECT_ID", "demo")
	t.Setenv("MINISKY_PHASE21_22_LOCATION", "us-central1")
	t.Setenv("MINISKY_PHASE21_22_API_ID", "api")
	t.Setenv("MINISKY_PHASE21_22_CONFIG_ID", "config")
	t.Setenv("MINISKY_PHASE21_22_GATEWAY_ID", "gateway")
	t.Setenv("MINISKY_PHASE21_22_NAMESPACE_ID", "namespace")
	t.Setenv("MINISKY_PHASE21_22_SERVICE_ID", "service")
	t.Setenv("MINISKY_PHASE21_22_ENDPOINT_ID", "endpoint")
	t.Setenv("MINISKY_PHASE21_22_PIPELINE_ID", "pipeline")
	t.Setenv("MINISKY_PHASE21_22_RELEASE_ID", "release")
	t.Setenv("MINISKY_PHASE21_22_ENDPOINTS_SERVICE", "example.endpoints.test")
	t.Setenv("MINISKY_PHASE21_22_EVIDENCE", filepath.Join(t.TempDir(), "evidence.json"))
}

func testConfig(t *testing.T) config {
	t.Helper()
	setValidEnv(t)
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func validEvidence(cfg config) evidence {
	parent := locationParent(cfg)
	apiName := "projects/" + cfg.project + "/locations/global/apis/" + cfg.apiID
	namespace := parent + "/namespaces/" + cfg.namespaceID
	service := namespace + "/services/" + cfg.serviceID
	pipeline := parent + "/deliveryPipelines/" + cfg.pipelineID
	release := pipeline + "/releases/" + cfg.releaseID
	return evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location, TraceID: traceID,
		ErrorMessage: "phase21 error", APIName: apiName, APIConfigName: apiName + "/configs/" + cfg.configID,
		GatewayName: parent + "/gateways/" + cfg.gatewayID, NamespaceName: namespace,
		DirectoryService: service, DirectoryEndpoint: service + "/endpoints/" + cfg.endpointID,
		EndpointsService: cfg.endpointsService, EndpointsConfig: "config", EndpointsRollout: "rollout",
		PipelineName: pipeline, ReleaseName: release,
		AllowedRollout: release + "/rollouts/allowed", DeniedRollout: release + "/rollouts/denied",
	}
}
