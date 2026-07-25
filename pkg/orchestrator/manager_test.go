package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmulatorVolumesUseProfileScopedRuntimePaths(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "restart")

	for _, service := range []string{"datastore", "firestore"} {
		got, err := resolveEmulatorVolume(service+".googleapis.com", "./data/"+service+":/data")
		if err != nil {
			t.Fatal(err)
		}
		want := filepath.Join(root, "profiles", "restart", "runtime", service) + ":/data"
		if got != want {
			t.Fatalf("%s volume = %q, want %q", service, got, want)
		}
	}
}

func TestDockerOwnershipRequiresManagerAndProfileLabels(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")

	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{name: "owned", labels: map[string]string{"managed-by": "minisky", "minisky.profile": "restart"}, want: true},
		{name: "missing manager", labels: map[string]string{"minisky.profile": "restart"}, want: false},
		{name: "other manager", labels: map[string]string{"managed-by": "someone-else", "minisky.profile": "restart"}, want: false},
		{name: "other profile", labels: map[string]string{"managed-by": "minisky", "minisky.profile": "other"}, want: false},
		{name: "legacy unlabeled", labels: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isOwnedDockerResource(tt.labels); got != tt.want {
				t.Fatalf("owned = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestTerminalExecTargetRequiresActiveProfileOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "terminal-test")
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || !strings.Contains(request.URL.Path, "/containers/") {
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
		}
		return dockerResponse(http.StatusOK, `{
			"State":{"Status":"running"},
			"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"other-profile"}}
		}`), nil
	})}}

	if err := manager.validateExecTarget("minisky-vm"); err == nil {
		t.Fatal("terminal exec accepted a container owned by another profile")
	}
}

func TestVPCNetworkUsesOwnedLabelsAndValidatedIPAM(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "network-test")
	var createPayload map[string]any
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/networks/"):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/networks/create":
			if err := json.NewDecoder(request.Body).Decode(&createPayload); err != nil {
				t.Fatal(err)
			}
			return dockerResponse(http.StatusCreated, `{"Id":"network"}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}}
	if err := manager.CreateVPCNetworkWithSubnet(context.Background(), "private", "10.42.0.0/24"); err != nil {
		t.Fatal(err)
	}
	labels := createPayload["Labels"].(map[string]any)
	if labels["managed-by"] != "minisky" || labels["minisky.profile"] != "network-test" ||
		labels["minisky.service"] != "compute-network" {
		t.Fatalf("labels = %#v", labels)
	}
	ipam := createPayload["IPAM"].(map[string]any)
	config := ipam["Config"].([]any)[0].(map[string]any)
	if config["Subnet"] != "10.42.0.0/24" {
		t.Fatalf("IPAM = %#v", ipam)
	}
	if err := manager.CreateVPCNetworkWithSubnet(context.Background(), "private", "not-a-cidr"); err == nil {
		t.Fatal("invalid CIDR was accepted")
	}
}

func TestVPCNetworkRefusesUnownedCreateAndDelete(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "network-test")
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			t.Fatalf("unowned network caused mutation: %s %s", request.Method, request.URL)
		}
		return dockerResponse(http.StatusOK, `{"Labels":{"managed-by":"someone-else"}}`), nil
	})}}
	if err := manager.CreateVPCNetwork(context.Background(), "private"); err == nil {
		t.Fatal("unowned same-name network was adopted")
	}
	if err := manager.DeleteVPCNetwork(context.Background(), "private"); err == nil {
		t.Fatal("unowned network was deleted")
	}
}

func TestEmulatorAdditionalPortsAreLoopbackPublished(t *testing.T) {
	var payload struct {
		ExposedPorts map[string]any `json:"ExposedPorts"`
		HostConfig   struct {
			PortBindings map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"PortBindings"`
		} `json:"HostConfig"`
	}
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		return dockerResponse(http.StatusCreated, `{}`), nil
	})}}
	err := manager.createContainer(ContainerConfig{
		Name: "spanner", Image: "spanner", ContainerPort: "9020/tcp",
		AdditionalPorts: []string{"9010/tcp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range []string{"9010/tcp", "9020/tcp"} {
		if _, ok := payload.ExposedPorts[port]; !ok {
			t.Fatalf("port %s was not exposed: %#v", port, payload.ExposedPorts)
		}
		bindings := payload.HostConfig.PortBindings[port]
		if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" || bindings[0].HostPort != "0" {
			t.Fatalf("bindings for %s = %#v", port, bindings)
		}
	}
}

func TestRedisProvisioningRefusesUnownedExistingVolume(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-test")
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(request.URL.Path, "/containers/"):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case strings.Contains(request.URL.Path, "/images/"):
			return dockerResponse(http.StatusOK, `{}`), nil
		case strings.Contains(request.URL.Path, "/volumes/"):
			return dockerResponse(http.StatusOK, `{"Labels":{"managed-by":"someone-else"}}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}}
	if _, err := manager.ProvisionRedis(context.Background(), "resource", "redis:test"); err == nil {
		t.Fatal("unowned existing Redis volume was adopted")
	}
}

func dockerResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}
