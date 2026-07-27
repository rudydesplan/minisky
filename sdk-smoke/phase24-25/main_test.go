package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		"http://localhost", "http://user@localhost:8080",
		"http://localhost:8080/path", "http://localhost:8080?query=1",
	} {
		if err := validateLoopbackEndpoint(endpoint); err == nil {
			t.Errorf("validateLoopbackEndpoint(%q) unexpectedly succeeded", endpoint)
		}
	}
}

func TestConfigRequiresSafeInputsAndAbsoluteEvidence(t *testing.T) {
	setValidEnv(t)
	if _, err := configFromEnv(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{"non-loopback endpoint", "MINISKY_ENDPOINT", "http://example.com:8080", "loopback"},
		{"unsafe project", "MINISKY_PROJECT_ID", "../project", "project"},
		{"unsafe certificate ID", "MINISKY_PHASE24_25_CERTIFICATE_ID", "certificate/id", "certificate ID"},
		{"relative evidence", "MINISKY_PHASE24_25_EVIDENCE", "evidence.json", "absolute"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(test.key, test.value)
			if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want substring %q", err, test.want)
			}
		})
	}
}

func TestGeneratedClientsUseCanonicalFullDomainPaths(t *testing.T) {
	responses := map[string]string{
		"/_minisky/privateca.googleapis.com/v1/projects/demo/locations/us-central1/caPools/local/certificates":  `{"certificates":[]}`,
		"/_minisky/dlp.googleapis.com/v2/projects/demo/inspectTemplates":                                        `{"inspectTemplates":[]}`,
		"/_minisky/orgpolicy.googleapis.com/v2/projects/demo/policies":                                          `{"policies":[]}`,
		"/_minisky/accesscontextmanager.googleapis.com/v1/accessPolicies":                                       `{"accessPolicies":[]}`,
		"/_minisky/cloudasset.googleapis.com/v1/projects/demo:searchAllResources":                               `{"results":[]}`,
		"/_minisky/binaryauthorization.googleapis.com/v1/projects/demo/policy":                                  `{"name":"projects/demo/policy","defaultAdmissionRule":{"evaluationMode":"DISALLOWED"}}`,
		"/_minisky/networksecurity.googleapis.com/v1/projects/demo/locations/us-central1/authorizationPolicies": `{"authorizationPolicies":[]}`,
		"/_minisky/networkservices.googleapis.com/v1/projects/demo/locations/us-central1/meshes":                `{"meshes":[]}`,
		"/_minisky/networkservices.googleapis.com/v1/projects/demo/locations/global/httpRoutes":                 `{"httpRoutes":[]}`,
		"/_minisky/storage.googleapis.com/storage/v1/b":                                                         `{"items":[]}`,
		"/_minisky/compute.googleapis.com/compute/v1/projects/demo/global/backendServices":                      `{"items":[]}`,
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
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)

	clients, err := newGeneratedClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	parent := "projects/demo/locations/us-central1"
	if _, err := clients.privateCA.Projects.Locations.CaPools.Certificates.List(parent + "/caPools/local").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.dlp.Projects.InspectTemplates.List("projects/demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.org.Projects.Policies.List("projects/demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.access.AccessPolicies.List().Parent("organizations/123").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.asset.V1.SearchAllResources("projects/demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.binary.Projects.GetPolicy("projects/demo/policy").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.network.Projects.Locations.AuthorizationPolicies.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.services.Projects.Locations.Meshes.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.services.Projects.Locations.HttpRoutes.List("projects/demo/locations/global").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.storage.Buckets.List("demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.compute.BackendServices.List("demo").Do(); err != nil {
		t.Fatal(err)
	}
	for path := range responses {
		if !seen[path] {
			t.Errorf("generated client did not request %s", path)
		}
	}
}

func TestGeneratedClientErrorsRetainGCPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "cloudasset.googleapis.com"):
			w.WriteHeader(http.StatusNotImplemented)
			fmt.Fprint(w, `{"error":{"code":501,"message":"export disabled","status":"UNIMPLEMENTED"}}`)
		case strings.Contains(r.URL.Path, "storage.googleapis.com"):
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":{"code":403,"message":"perimeter denied","status":"PERMISSION_DENIED"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	clients, err := newGeneratedClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, exportErr := clients.asset.V1.ExportAssets("projects/demo", nil).Do()
	if err := expectGoogleStatus(exportErr, 501, "UNIMPLEMENTED"); err != nil {
		t.Fatal(err)
	}
	if err := provePerimeterDenial(context.Background(), clients.storage, "demo"); err != nil {
		t.Fatal(err)
	}
}

func TestDirectLocalComputeBoundariesUseCanonicalScopedPaths(t *testing.T) {
	var sawBackendExtension, sawProxy bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_minisky/compute.googleapis.com/compute/v1/projects/demo/global/backendServices":
			sawBackendExtension = true
			if r.Method != http.MethodPost {
				t.Errorf("backend extension method=%s", r.Method)
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"name":"phase25-default"}`)
		case "/_minisky/compute.googleapis.com/compute/v1/projects/demo/global/forwardingRules/phase25-frontend/proxy/admin":
			sawProxy = true
			if r.Method != http.MethodHead || r.Host != "localhost" {
				t.Errorf("proxy method=%s host=%q", r.Method, r.Host)
			}
			w.WriteHeader(http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	cfg := config{endpoint: server.URL, project: "demo"}
	if err := createLocalBackendURL(t.Context(), cfg, defaultBackendID, "http://127.0.0.1:18081"); err != nil {
		t.Fatal(err)
	}
	response, err := directProxyRequest(t.Context(), cfg, http.MethodHead, "localhost", "/admin")
	if err != nil {
		t.Fatal(err)
	}
	if response.status != http.StatusForbidden || !sawBackendExtension || !sawProxy {
		t.Fatalf("response=%+v backend=%t proxy=%t", response, sawBackendExtension, sawProxy)
	}
}

func TestEvidenceRoundTripIsBoundedStrictAndProjectIsolated(t *testing.T) {
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

	t.Run("project mismatch", func(t *testing.T) {
		bad := cfg
		bad.project = "other"
		if _, err := readEvidence(cfg.evidencePath, bad); err == nil || !strings.Contains(err.Error(), "identifiers") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("unknown field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		version := fmt.Sprintf(`"version":%d`, evidenceVersion)
		data = []byte(strings.Replace(string(data), version, version+`,"unexpected":true`, 1))
		if err := os.WriteFile(path, data, 0o600); err != nil {
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
		if _, err := readEvidence(path, cfg); err == nil || !strings.Contains(err.Error(), "limit") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestCertificateRequestPEMIsValidAndContainsNoPrivateKey(t *testing.T) {
	encoded, err := certificateRequestPEM("phase24-25.local")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(encoded, "PRIVATE KEY") {
		t.Fatal("CSR output contains private key material")
	}
	block, rest := pem.Decode([]byte(encoded))
	if block == nil || block.Type != "CERTIFICATE REQUEST" || len(strings.TrimSpace(string(rest))) != 0 {
		t.Fatalf("invalid PEM CSR: %q", encoded)
	}
	request, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := request.CheckSignature(); err != nil {
		t.Fatal(err)
	}
	if request.Subject.CommonName != "phase24-25.local" {
		t.Fatalf("commonName=%q", request.Subject.CommonName)
	}
}

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:8080")
	t.Setenv("MINISKY_PROJECT_ID", "demo")
	t.Setenv("MINISKY_PHASE24_25_LOCATION", "us-central1")
	t.Setenv("MINISKY_PHASE24_25_CERTIFICATE_ID", "certificate")
	t.Setenv("MINISKY_PHASE24_25_TEMPLATE_ID", "template")
	t.Setenv("MINISKY_PHASE24_25_PERIMETER_ID", "perimeter")
	t.Setenv("MINISKY_PHASE24_25_NETWORK_POLICY_ID", "network-policy")
	t.Setenv("MINISKY_PHASE24_25_MESH_ID", "mesh")
	t.Setenv("MINISKY_PHASE24_25_PROXY_POLICY_ID", "proxy-policy")
	t.Setenv("MINISKY_PHASE24_25_PROXY_MESH_ID", "proxy-mesh")
	t.Setenv("MINISKY_PHASE24_25_DEFAULT_BACKEND", "http://127.0.0.1:18081")
	t.Setenv("MINISKY_PHASE24_25_ROUTED_BACKEND", "http://127.0.0.1:18082")
	t.Setenv("MINISKY_PHASE24_25_EVIDENCE", filepath.Join(t.TempDir(), "evidence.json"))
}

func testConfig(t *testing.T) config {
	t.Helper()
	setValidEnv(t)
	t.Setenv("MINISKY_PHASE24_25_MODE", "create")
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func validEvidence(cfg config) evidence {
	project := projectParent(cfg)
	parent := locationParent(cfg)
	accessPolicy := "accessPolicies/1"
	return evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		CertificateName:  certificateName(cfg),
		DLPTemplateName:  project + "/inspectTemplates/" + cfg.templateID,
		OrgPolicyName:    project + "/policies/compute.requireOsLogin",
		AccessPolicyName: accessPolicy, ServicePerimeterName: accessPolicy + "/servicePerimeters/" + cfg.perimeterID,
		BinaryPolicyName:       project + "/policy",
		NetworkPolicyName:      parent + "/authorizationPolicies/" + cfg.networkID,
		MeshName:               parent + "/meshes/" + cfg.meshID,
		ProxyNetworkPolicyName: proxyParent(cfg) + "/authorizationPolicies/" + cfg.proxyPolicyID,
		ProxyMeshName:          proxyParent(cfg) + "/meshes/" + cfg.proxyMeshID,
		HTTPRouteName:          proxyParent(cfg) + "/httpRoutes/" + httpRouteID,
		DefaultBackendName:     defaultBackendID,
		RoutedBackendName:      routedBackendID,
		DLPFindingCount:        1, CloudAssetResultCount: 0,
		CloudAssetSearchVerified: true, PerimeterGatewayDenied: true,
		ProxyDenyNoBackendCall: true, MeshRouteSelectedBackend: true,
		DefaultBackendHits: 0, RoutedBackendHits: 1,
		ComputeBackendSetup:      "GENERATED_COMPUTE_STANDARD_RESOURCES_DIRECT_LOCAL_BACKEND_EXTENSION",
		ProxyRequestKind:         "DIRECT_LOCAL_DATA_PLANE",
		PrivateCADeleteSupport:   "UNAVAILABLE_IN_GENERATED_API",
		OrgPolicyEvaluateSupport: "UNAVAILABLE_IN_GENERATED_API",
		BinaryEvaluateSupport:    "UNIMPLEMENTED",
		NetworkEvaluateSupport:   "UNAVAILABLE_IN_GENERATED_API",
		CloudAssetExportSupport:  "UNIMPLEMENTED",
	}
}
