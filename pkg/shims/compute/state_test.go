package compute

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestComputeMetadataRehydratesAndPreservesLoadBalancerDataPlane(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	opMgr := orchestrator.NewOperationManager()
	svcMgr := &orchestrator.ServiceManager{}
	api, err := NewAPIWithStore(opMgr, svcMgr, store)
	if err != nil {
		t.Fatal(err)
	}

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "restored:"+r.URL.Path)
	}))
	defer backend.Close()

	api.instances["test-project:us-central1-a:vm"] = &Instance{
		Name: "vm", Status: "RUNNING", Description: "live container",
		HostPorts: []orchestrator.PortMapping{{ContainerPort: "8080", HostPort: "1234"}},
	}
	api.networks["test-project:network"] = &Network{Name: "network"}
	api.firewalls["test-project:allow-http"] = &FirewallRule{Name: "allow-http"}
	api.instanceGroups["test-project:us-central1-a:web"] = &InstanceGroup{
		Name:       "web",
		Zone:       computeZoneSelfLink("test-project", "us-central1-a"),
		NamedPorts: []NamedPort{{Name: "http", Port: 80}},
		Instances:  []string{selfLinkInstance("test-project", "us-central1-a", "vm")},
		Size:       1,
	}
	createLoadBalancerResourceForTest(t, api, "healthChecks",
		`{"name":"health","httpHealthCheck":{"requestPath":"/healthz"}}`)
	createLoadBalancerResourceForTest(t, api, "backendServices",
		fmt.Sprintf(`{"name":"backend","backends":[{"url":%q}],"healthChecks":["health"]}`, backend.URL))
	createLoadBalancerResourceForTest(t, api, "urlMaps", `{"name":"routes","defaultService":"backend"}`)
	createLoadBalancerResourceForTest(t, api, "targetHttpProxies", `{"name":"proxy","urlMap":"routes"}`)
	createLoadBalancerResourceForTest(t, api, "forwardingRules", `{"name":"frontend","target":"proxy"}`)

	restarted, err := NewAPIWithStore(opMgr, svcMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.opMgr != opMgr || restarted.svcMgr != svcMgr {
		t.Fatal("rehydration replaced orchestrator dependencies")
	}
	instance := restarted.instances["test-project:us-central1-a:vm"]
	if instance == nil || instance.Status != metadataOnlyStatus ||
		!strings.Contains(instance.Description, "not been reconciled") ||
		len(instance.HostPorts) != 0 || instance.project != "test-project" || instance.zone != "us-central1-a" {
		t.Fatalf("restored instance did not disclose metadata-only state: %#v", instance)
	}
	if restarted.networks["test-project:network"] == nil || restarted.firewalls["test-project:allow-http"] == nil {
		t.Fatal("network or firewall metadata was not restored")
	}
	group := restarted.instanceGroups["test-project:us-central1-a:web"]
	if group == nil || group.Size != 1 || len(group.NamedPorts) != 1 || len(group.Instances) != 1 {
		t.Fatalf("instance-group metadata was not restored: %#v", group)
	}

	response := performComputeRequest(
		restarted,
		http.MethodGet,
		"/compute/v1/projects/test-project/global/forwardingRules/frontend/proxy/hello",
		"",
	)
	if response.Code != http.StatusOK || response.Body.String() != "restored:/hello" {
		t.Fatalf("restored load balancer = status %d body %q", response.Code, response.Body.String())
	}
}

func TestComputeStateMissingAndCorrupt(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	missing, err := state.New(root, "missing")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, nil, missing)
	if err != nil {
		t.Fatalf("missing state should start empty: %v", err)
	}
	if len(api.instances) != 0 || len(api.loadBalancers) != 0 {
		t.Fatal("missing state did not start empty")
	}

	corrupt, err := state.New(root, "corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(corrupt.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corrupt.ProfileDir(), "state.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAPIWithStore(nil, nil, corrupt); err == nil ||
		!strings.Contains(err.Error(), "load Compute metadata") {
		t.Fatalf("corrupt state error = %v", err)
	}
}
