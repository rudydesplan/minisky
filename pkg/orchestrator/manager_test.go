package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWaitUntilHTTPReadyAcceptsAnyHTTPResponse(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	if err := waitUntilHTTPReady(server.URL, time.Second); err != nil {
		t.Fatalf("wait for HTTP response: %v", err)
	}
}

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

func TestPullImageUsesContextAndEncodedImageName(t *testing.T) {
	const image = "gcr.io/cloud-spanner-emulator/emulator:latest"
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/images/create" {
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
		}
		if got := request.URL.Query().Get("fromImage"); got != image {
			t.Fatalf("fromImage = %q, want %q", got, image)
		}
		return dockerResponse(http.StatusOK, `{"status":"Pull complete"}`), nil
	})}}

	if err := manager.pullImageInternal(context.Background(), image); err != nil {
		t.Fatal(err)
	}
}

func TestPullImageReportsDockerHTTPAndStreamErrors(t *testing.T) {
	tests := []struct {
		name string
		code int
		body string
		want string
	}{
		{name: "http status", code: http.StatusInternalServerError, body: `{"message":"registry unavailable"}`, want: "registry unavailable"},
		{name: "stream error", code: http.StatusOK, body: `{"errorDetail":{"message":"manifest unknown"},"error":"manifest unknown"}`, want: "manifest unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return dockerResponse(tt.code, tt.body), nil
			})}}
			err := manager.pullImageInternal(context.Background(), "example.invalid/image:tag")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("pull error = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestPullImageHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		cancel()
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}}

	err := manager.pullImageInternal(ctx, "example.invalid/image:tag")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pull error = %v, want context cancellation", err)
	}
}

func TestDeleteServerlessVMRequiresExactCurrentProfileOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-test")
	identity := ServerlessIdentity{
		ResourceType: ServerlessFunction,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "Hello_World",
	}
	ownedLabels, err := identity.labels()
	if err != nil {
		t.Fatal(err)
	}
	encodedOwnedLabels, err := json.Marshal(ownedLabels)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		labels     string
		status     int
		wantError  bool
		wantDelete bool
	}{
		{
			name:       "owned",
			labels:     string(encodedOwnedLabels),
			status:     http.StatusOK,
			wantDelete: true,
		},
		{
			name:      "cross profile",
			labels:    strings.Replace(string(encodedOwnedLabels), `"serverless-test"`, `"other"`, 1),
			status:    http.StatusOK,
			wantError: true,
		},
		{
			name:      "unrelated service",
			labels:    strings.Replace(string(encodedOwnedLabels), `"serverless"`, `"compute-instance"`, 1),
			status:    http.StatusOK,
			wantError: true,
		},
		{
			name:      "different canonical resource",
			labels:    strings.Replace(string(encodedOwnedLabels), identity.CanonicalResource(), "projects/other/locations/us-central1/functions/Hello_World", 1),
			status:    http.StatusOK,
			wantError: true,
		},
		{
			name:   "missing is idempotent",
			status: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleted := false
			manager := &ServiceManager{
				portRegistry: make(map[string][]PortMapping),
				dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					switch request.Method {
					case http.MethodGet:
						body := `{}`
						if tt.status == http.StatusOK {
							body = `{"State":{"Status":"running"},"Config":{"Labels":` + tt.labels + `}}`
						}
						return dockerResponse(tt.status, body), nil
					case http.MethodPost:
						return dockerResponse(http.StatusNoContent, `{}`), nil
					case http.MethodDelete:
						deleted = true
						return dockerResponse(http.StatusNoContent, `{}`), nil
					default:
						t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
						return nil, nil
					}
				})},
			}

			err := manager.DeleteServerlessVM(identity)
			if (err != nil) != tt.wantError {
				t.Fatalf("DeleteServerlessVM error = %v, wantError=%t", err, tt.wantError)
			}
			if deleted != tt.wantDelete {
				t.Fatalf("Docker delete called = %t, want %t", deleted, tt.wantDelete)
			}
		})
	}
}

func TestServerlessIdentitySeparatesTypeProjectAndLocation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-test")
	identities := []ServerlessIdentity{
		{ResourceType: ServerlessFunction, Project: "project-a", Location: "us-central1", Name: "hello"},
		{ResourceType: ServerlessService, Project: "project-a", Location: "us-central1", Name: "hello"},
		{ResourceType: ServerlessFunction, Project: "project-b", Location: "us-central1", Name: "hello"},
		{ResourceType: ServerlessFunction, Project: "project-a", Location: "europe-west1", Name: "hello"},
	}
	names := make(map[string]bool, len(identities))
	images := make(map[string]bool, len(identities))
	for _, identity := range identities {
		name, err := identity.ContainerName()
		if err != nil {
			t.Fatal(err)
		}
		if names[name] {
			t.Fatalf("duplicate container name %q for identity %#v", name, identity)
		}
		names[name] = true
		image, err := identity.ImageName()
		if err != nil {
			t.Fatal(err)
		}
		if images[image] {
			t.Fatalf("duplicate image name %q for identity %#v", image, identity)
		}
		images[image] = true
	}
}

func TestProvisionServerlessVMUsesIdentityNameAndOwnershipLabels(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-test")
	identity := ServerlessIdentity{
		ResourceType: ServerlessFunction,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "Hello_World",
	}
	containerName, err := identity.ContainerName()
	if err != nil {
		t.Fatal(err)
	}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer app.Close()
	appPort := strings.TrimPrefix(app.URL, "http://127.0.0.1:")

	var createPayload struct {
		Labels map[string]string `json:"Labels"`
	}
	inspectCount := 0
	manager := &ServiceManager{
		portRegistry: make(map[string][]PortMapping),
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/json"):
				inspectCount++
				if inspectCount == 1 {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, `{"NetworkSettings":{"Ports":{"8080/tcp":[{"HostIp":"127.0.0.1","HostPort":"`+appPort+`"}]}}}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				if got := request.URL.Query().Get("name"); got != containerName {
					t.Fatalf("container name = %q", got)
				}
				if err := json.NewDecoder(request.Body).Decode(&createPayload); err != nil {
					t.Fatal(err)
				}
				return dockerResponse(http.StatusCreated, `{}`), nil
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
				return dockerResponse(http.StatusNoContent, `{}`), nil
			default:
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
				return nil, nil
			}
		})},
	}

	gotURL, err := manager.ProvisionServerlessVM(identity, "example/service:local", []string{"PORT=8080"})
	if err != nil {
		t.Fatal(err)
	}
	if gotURL != app.URL {
		t.Fatalf("service URL = %q, want %q", gotURL, app.URL)
	}
	if createPayload.Labels["managed-by"] != "minisky" ||
		createPayload.Labels["minisky.profile"] != "serverless-test" ||
		createPayload.Labels["minisky.service"] != "serverless" ||
		createPayload.Labels["minisky.resource"] != identity.CanonicalResource() ||
		createPayload.Labels["minisky.resource-type"] != string(ServerlessFunction) {
		t.Fatalf("serverless labels = %#v", createPayload.Labels)
	}
}

func TestProvisionServerlessVMCleansOwnedContainerAfterPostCreateFailure(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-cleanup")
	identity := ServerlessIdentity{
		ResourceType: ServerlessService,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "cleanup",
	}
	labels, err := identity.labels()
	if err != nil {
		t.Fatal(err)
	}
	encodedLabels, err := json.Marshal(labels)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		stage         string
		cleanupStatus int
		cleanupLabels string
		want          string
		wantCleanup   bool
	}{
		{name: "start", stage: "start", cleanupStatus: http.StatusNoContent, cleanupLabels: string(encodedLabels), want: "start Serverless container", wantCleanup: true},
		{name: "port discovery", stage: "discover", cleanupStatus: http.StatusNoContent, cleanupLabels: string(encodedLabels), want: "port discovery", wantCleanup: true},
		{name: "readiness", stage: "readiness", cleanupStatus: http.StatusNoContent, cleanupLabels: string(encodedLabels), want: "readiness failed", wantCleanup: true},
		{name: "cleanup failure is appended", stage: "start", cleanupStatus: http.StatusInternalServerError, cleanupLabels: string(encodedLabels), want: "cleanup owned backend failed", wantCleanup: true},
		{name: "ownership refusal stays safe", stage: "start", cleanupStatus: http.StatusNoContent, cleanupLabels: `{"managed-by":"someone-else"}`, want: "refusing to delete unowned", wantCleanup: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			created := false
			inspectAfterCreate := 0
			deleteCalls := 0
			manager := &ServiceManager{
				portRegistry: make(map[string][]PortMapping),
				serverlessReady: func(string, time.Duration) error {
					if tt.stage == "readiness" {
						return errors.New("readiness failed")
					}
					return nil
				},
				dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					switch {
					case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/json"):
						if !created {
							return dockerResponse(http.StatusNotFound, `{}`), nil
						}
						inspectAfterCreate++
						if tt.stage == "discover" && inspectAfterCreate == 1 {
							return dockerResponse(http.StatusOK, `{"NetworkSettings":{"Ports":{}}}`), nil
						}
						if tt.stage == "readiness" && inspectAfterCreate == 1 {
							return dockerResponse(http.StatusOK, `{"NetworkSettings":{"Ports":{"8080/tcp":[{"HostIp":"127.0.0.1","HostPort":"12345"}]}}}`), nil
						}
						return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":`+tt.cleanupLabels+`}}`), nil
					case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
						created = true
						return dockerResponse(http.StatusCreated, `{}`), nil
					case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
						if tt.stage == "start" {
							return dockerResponse(http.StatusInternalServerError, `{"message":"start failed"}`), nil
						}
						return dockerResponse(http.StatusNoContent, `{}`), nil
					case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/stop"):
						return dockerResponse(http.StatusNoContent, `{}`), nil
					case request.Method == http.MethodDelete:
						deleteCalls++
						return dockerResponse(tt.cleanupStatus, `{}`), nil
					default:
						t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
						return nil, nil
					}
				})},
			}

			_, err := manager.ProvisionServerlessVM(identity, "example/service:local", nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("provision error = %v, want text %q", err, tt.want)
			}
			if got := deleteCalls > 0; got != tt.wantCleanup {
				t.Fatalf("cleanup delete called = %t, want %t", got, tt.wantCleanup)
			}
		})
	}
}

func TestServerlessLifecycleGateRejectsSameIdentityAndAllowsUnrelatedIdentity(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-lifecycle")
	identityA := ServerlessIdentity{
		ResourceType: ServerlessFunction,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "shared",
	}
	identityB := identityA
	identityB.Name = "other"
	nameA, err := identityA.ContainerName()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	manager := &ServiceManager{
		portRegistry: make(map[string][]PortMapping),
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet {
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			}
			if strings.Contains(request.URL.Path, nameA) {
				select {
				case <-started:
				default:
					close(started)
				}
				<-release
			}
			return dockerResponse(http.StatusNotFound, `{}`), nil
		})},
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- manager.DeleteServerlessVM(identityA)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first lifecycle did not reach backend")
	}

	if err := manager.DeleteServerlessVM(identityA); !errors.Is(err, ErrServerlessLifecycleInProgress) {
		t.Fatalf("same-identity delete error = %v", err)
	}
	if err := manager.DeleteServerlessVM(identityB); err != nil {
		t.Fatalf("unrelated identity was blocked: %v", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first delete failed: %v", err)
	}
	manager.serverlessMu.Lock()
	active := len(manager.serverlessActive)
	manager.serverlessMu.Unlock()
	if active != 0 {
		t.Fatalf("active lifecycle entries after release = %d", active)
	}
}

func dockerResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}
