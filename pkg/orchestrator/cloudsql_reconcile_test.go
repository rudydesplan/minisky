package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const testCloudSQLContainerID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
const testCloudSQLImageID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type cloudSQLDockerFixture struct {
	t *testing.T

	mu                               sync.Mutex
	containerFound                   bool
	containerState                   string
	containerImage                   string
	containerImageID                 string
	imageInspectID                   string
	imageRepoDigests                 []string
	imageFound                       bool
	imagePulls                       int
	createdContainerImageID          string
	containerLabels                  map[string]string
	mountName                        string
	mountTarget                      string
	hostIP                           string
	hostPort                         string
	volumeFound                      bool
	volumeLabels                     map[string]string
	volumeName                       string
	volumeCreatedAt                  string
	volumeMountpoint                 string
	started                          int
	created                          int
	volumeCreates                    int
	readyCalls                       int
	authReadyCalls                   int
	volumeInspects                   int
	removed                          int
	volumeDeletes                    int
	changeVolumeAfterContainerDelete bool
	disappearVolumeAfterCreate       bool
	replaceVolumeAfterCreate         bool
	failInspect                      error
	failContainerCreate              error
	containerBody                    string
	adminExecTarget                  string
}

func (fixture *cloudSQLDockerFixture) RoundTrip(request *http.Request) (*http.Response, error) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.failInspect != nil && request.Method == http.MethodGet {
		return nil, fixture.failInspect
	}
	switch {
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
		if !fixture.containerFound {
			return dockerResponse(http.StatusNotFound, `{}`), nil
		}
		if fixture.containerBody != "" {
			return dockerResponse(http.StatusOK, fixture.containerBody), nil
		}
		body, err := json.Marshal(map[string]any{
			"Id":    testCloudSQLContainerID,
			"Image": fixture.containerImageID,
			"State": map[string]any{"Status": fixture.containerState},
			"Config": map[string]any{
				"Image":  fixture.containerImage,
				"Labels": fixture.containerLabels,
			},
			"Mounts": []map[string]any{{
				"Type": "volume", "Name": fixture.mountName, "Destination": fixture.mountTarget,
			}},
			"NetworkSettings": map[string]any{
				"Ports": map[string]any{
					testCloudSQLRuntimeConfig().ContainerPort: []map[string]string{{
						"HostIp": fixture.hostIP, "HostPort": fixture.hostPort,
					}},
				},
			},
		})
		if err != nil {
			fixture.t.Fatal(err)
		}
		return dockerResponse(http.StatusOK, string(body)), nil
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/volumes/"):
		fixture.volumeInspects++
		if fixture.disappearVolumeAfterCreate && fixture.created > 0 {
			fixture.volumeFound = false
		}
		if fixture.replaceVolumeAfterCreate && fixture.created > 0 {
			fixture.volumeFound = true
			fixture.volumeCreatedAt = "2026-07-29T00:00:01Z"
			fixture.volumeLabels = nil
		}
		if !fixture.volumeFound {
			return dockerResponse(http.StatusNotFound, `{}`), nil
		}
		body, err := json.Marshal(map[string]any{
			"Name":       fixture.volumeName,
			"CreatedAt":  fixture.volumeCreatedAt,
			"Mountpoint": fixture.volumeMountpoint,
			"Labels":     fixture.volumeLabels,
		})
		if err != nil {
			fixture.t.Fatal(err)
		}
		return dockerResponse(http.StatusOK, string(body)), nil
	case request.Method == http.MethodPost &&
		strings.HasPrefix(request.URL.Path, "/containers/") &&
		strings.HasSuffix(request.URL.Path, "/start"):
		fixture.started++
		fixture.containerState = "running"
		return dockerResponse(http.StatusNoContent, ""), nil
	case request.Method == http.MethodPost &&
		strings.HasPrefix(request.URL.Path, "/containers/") &&
		strings.HasSuffix(request.URL.Path, "/exec"):
		fixture.adminExecTarget = strings.TrimSuffix(
			strings.TrimPrefix(request.URL.Path, "/containers/"),
			"/exec",
		)
		return dockerResponse(http.StatusCreated, `{"Id":"exec-id"}`), nil
	case request.Method == http.MethodPost && request.URL.Path == "/exec/exec-id/start":
		return dockerResponse(http.StatusOK, ""), nil
	case request.Method == http.MethodGet && request.URL.Path == "/exec/exec-id/json":
		return dockerResponse(http.StatusOK, `{"Running":false,"ExitCode":0}`), nil
	case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
		fixture.created++
		var payload struct {
			Image      string            `json:"Image"`
			Labels     map[string]string `json:"Labels"`
			HostConfig struct {
				Binds []string `json:"Binds"`
			} `json:"HostConfig"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			fixture.t.Fatal(err)
		}
		if payload.Image != testCloudSQLSpec().ImageID ||
			!exactLabels(payload.Labels, cloudSQLLabels(testCloudSQLSpec())) {
			fixture.t.Fatalf("recreated container payload = %#v", payload)
		}
		containerName, volumeName, _ := cloudSQLDockerNames(
			testCloudSQLSpec().Project,
			testCloudSQLSpec().Instance,
		)
		if request.URL.Query().Get("name") != containerName ||
			len(payload.HostConfig.Binds) != 1 ||
			payload.HostConfig.Binds[0] != volumeName+":"+testCloudSQLRuntimeConfig().MountTarget {
			fixture.t.Fatalf("recreated container identity = %q %#v", request.URL.String(), payload)
		}
		fixture.containerFound = true
		fixture.containerState = "created"
		fixture.containerImage = payload.Image
		fixture.containerImageID = fixture.createdContainerImageID
		fixture.containerLabels = payload.Labels
		fixture.mountName = volumeName
		fixture.mountTarget = testCloudSQLRuntimeConfig().MountTarget
		if fixture.failContainerCreate != nil {
			return nil, fixture.failContainerCreate
		}
		return dockerResponse(http.StatusCreated, `{"Id":"`+testCloudSQLContainerID+`"}`), nil
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
		if !fixture.imageFound {
			return dockerResponse(http.StatusNotFound, `{}`), nil
		}
		body, err := json.Marshal(map[string]any{
			"Id":          fixture.imageInspectID,
			"RepoDigests": fixture.imageRepoDigests,
		})
		if err != nil {
			fixture.t.Fatal(err)
		}
		return dockerResponse(http.StatusOK, string(body)), nil
	case request.Method == http.MethodPost && request.URL.Path == "/images/create":
		fixture.imagePulls++
		fixture.imageFound = true
		return dockerResponse(http.StatusOK, "{}\n"), nil
	case request.Method == http.MethodPost && request.URL.Path == "/volumes/create":
		fixture.volumeCreates++
		return dockerResponse(http.StatusCreated, `{}`), nil
	case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/containers/"):
		fixture.removed++
		fixture.containerFound = false
		if fixture.changeVolumeAfterContainerDelete {
			fixture.volumeCreatedAt = "2026-07-29T00:00:01Z"
		}
		return dockerResponse(http.StatusNoContent, ""), nil
	case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/volumes/"):
		fixture.volumeDeletes++
		fixture.volumeFound = false
		return dockerResponse(http.StatusNoContent, ""), nil
	default:
		body, _ := io.ReadAll(request.Body)
		fixture.t.Fatalf("unexpected Docker request %s %s: %s", request.Method, request.URL, body)
		return nil, errors.New("unexpected Docker request")
	}
}

func TestReconcileCloudSQLRunningAndStoppedExactOwnedContainers(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	for _, test := range []struct {
		name       string
		state      string
		wantStarts int
	}{
		{name: "running", state: "running"},
		{name: "stopped", state: "exited", wantStarts: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := testCloudSQLSpec()
			fixture := newCloudSQLDockerFixture(t, spec)
			fixture.containerState = test.state
			manager := cloudSQLTestManager(fixture)
			endpoint, err := manager.ReconcileCloudSQLVM(context.Background(), spec)
			if err != nil {
				t.Fatal(err)
			}
			if endpoint != "http://127.0.0.1:55432" {
				t.Fatalf("endpoint = %q", endpoint)
			}
			if fixture.started != test.wantStarts || fixture.created != 0 ||
				fixture.volumeCreates != 0 || fixture.readyCalls != 1 {
				t.Fatalf("starts=%d creates=%d volumeCreates=%d readyCalls=%d",
					fixture.started, fixture.created, fixture.volumeCreates, fixture.readyCalls)
			}
		})
	}
}

func TestReconcileCloudSQLVolumeOnlyRecreatesContainerWithoutCreatingVolume(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	manager := cloudSQLTestManager(fixture)

	endpoint, err := manager.ReconcileCloudSQLVM(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://127.0.0.1:55432" {
		t.Fatalf("endpoint = %q", endpoint)
	}
	if fixture.created != 1 || fixture.started != 1 ||
		fixture.volumeCreates != 0 || fixture.readyCalls != 1 {
		t.Fatalf("creates=%d starts=%d volumeCreates=%d readyCalls=%d",
			fixture.created, fixture.started, fixture.volumeCreates, fixture.readyCalls)
	}
}

func TestReconcileCloudSQLCreationIntentRecoversMissingVolumeIdentity(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	exact := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, exact)
	intent := exact
	intent.VolumeIdentity = ""
	manager := cloudSQLTestManager(fixture)

	endpoint, resolved, err := manager.ReconcileCloudSQLVMResolved(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://127.0.0.1:55432" || resolved.VolumeIdentity != exact.VolumeIdentity {
		t.Fatalf("endpoint=%q resolved=%#v", endpoint, resolved)
	}
	if fixture.started != 0 || fixture.created != 0 || fixture.volumeCreates != 0 {
		t.Fatalf("recovery unexpectedly mutated Docker: starts=%d creates=%d volumeCreates=%d",
			fixture.started, fixture.created, fixture.volumeCreates)
	}
}

func TestReconcileCloudSQLVolumeOnlyRejectsDisappearingVolumeBeforeStart(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	fixture.disappearVolumeAfterCreate = true
	manager := cloudSQLTestManager(fixture)

	endpoint, err := manager.ReconcileCloudSQLVM(context.Background(), spec)
	if err == nil || endpoint != "" {
		t.Fatalf("endpoint=%q error=%v, want fail-closed volume identity error", endpoint, err)
	}
	if fixture.created != 1 || fixture.started != 0 || fixture.removed != 1 ||
		fixture.volumeCreates != 0 || fixture.readyCalls != 0 || fixture.volumeInspects < 2 {
		t.Fatalf(
			"creates=%d starts=%d removed=%d volumeCreates=%d readyCalls=%d volumeInspects=%d",
			fixture.created,
			fixture.started,
			fixture.removed,
			fixture.volumeCreates,
			fixture.readyCalls,
			fixture.volumeInspects,
		)
	}
}

func TestDeleteCloudSQLVolumeRevalidatesImmutableIdentityAtFinalRemove(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.changeVolumeAfterContainerDelete = true
	manager := cloudSQLTestManager(fixture)

	if err := manager.DeleteCloudSQLVMContext(context.Background(), spec); err == nil {
		t.Fatal("delete accepted volume identity change before final remove")
	}
	if fixture.removed != 1 || fixture.volumeDeletes != 0 {
		t.Fatalf("containerDeletes=%d volumeDeletes=%d, want 1 and 0",
			fixture.removed, fixture.volumeDeletes)
	}
}

func TestCloudSQLAdminExecTargetsInspectedImmutableContainerID(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	manager := cloudSQLTestManager(fixture)
	if err := manager.ExecuteCloudSQLAdmin(
		context.Background(),
		spec,
		"CREATE_DATABASE",
		"app",
		"",
	); err != nil {
		t.Fatal(err)
	}
	if fixture.adminExecTarget != testCloudSQLContainerID {
		t.Fatalf("admin exec target=%q, want inspected immutable ID %q",
			fixture.adminExecTarget, testCloudSQLContainerID)
	}
}

func TestProvisionCloudSQLRequiresProtocolReadiness(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	spec.ImageID = ""
	spec.VolumeIdentity = ""
	fixture := newCloudSQLDockerFixture(t, testCloudSQLSpec())
	fixture.containerFound = false
	manager := cloudSQLTestManager(fixture)
	manager.cloudSQLAuthReady = func(context.Context, string, map[string]string, string, time.Duration) error {
		fixture.mu.Lock()
		fixture.authReadyCalls++
		fixture.mu.Unlock()
		return context.DeadlineExceeded
	}

	prepared, err := manager.PrepareCloudSQLVM(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, resolved, created, err := manager.ProvisionCloudSQLVM(context.Background(), prepared)
	if err == nil || endpoint != "" || !created {
		t.Fatalf("endpoint=%q resolved=%#v created=%v err=%v", endpoint, resolved, created, err)
	}
	if fixture.started != 1 || fixture.readyCalls != 1 || fixture.authReadyCalls != 1 {
		t.Fatalf("starts=%d readyCalls=%d authReadyCalls=%d, want 1, 1, and 1",
			fixture.started, fixture.readyCalls, fixture.authReadyCalls)
	}
	if resolved.ImageID == "" || resolved.VolumeIdentity == "" {
		t.Fatalf("readiness failure lost durable backend identities: %#v", resolved)
	}
}

func TestProvisionCloudSQLAmbiguousCreateRetainsExactVolumeForRecovery(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	fixture.failContainerCreate = errors.New("ambiguous Docker response")
	manager := cloudSQLTestManager(fixture)

	_, resolved, _, err := manager.ProvisionCloudSQLVM(context.Background(), spec)
	if err == nil {
		t.Fatal("ambiguous create returned nil")
	}
	if resolved.ImageID != spec.ImageID || resolved.VolumeIdentity != spec.VolumeIdentity {
		t.Fatalf("ambiguous create lost recovery identities: %#v", resolved)
	}
	if fixture.volumeDeletes != 0 || !fixture.volumeFound {
		t.Fatalf("ambiguous create deleted recovery volume: deletes=%d found=%v",
			fixture.volumeDeletes, fixture.volumeFound)
	}
}

func TestReconcileCloudSQLReplacementVolumeRaceLeavesUnknownVolumeUntouched(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	fixture.replaceVolumeAfterCreate = true
	manager := cloudSQLTestManager(fixture)

	if _, err := manager.ReconcileCloudSQLVM(context.Background(), spec); err == nil {
		t.Fatal("replacement-volume race returned nil")
	}
	if fixture.started != 0 || fixture.removed != 1 || fixture.volumeDeletes != 0 {
		t.Fatalf("starts=%d containerDeletes=%d volumeDeletes=%d",
			fixture.started, fixture.removed, fixture.volumeDeletes)
	}
}

func TestCloudSQLConfigRejectsUnsupportedAndFallbackVersions(t *testing.T) {
	for _, version := range []string{"", "POSTGRES", "POSTGRES_15", "POSTGRES_18_1", "MYSQL_8", "MYSQL_8_1", "MYSQL_10_0"} {
		t.Run(version, func(t *testing.T) {
			if _, err := cloudSQLConfigForVersion(version); err == nil {
				t.Fatalf("cloudSQLConfigForVersion(%q) accepted unsupported fallback", version)
			}
		})
	}
	for _, version := range []string{"POSTGRES_16", "POSTGRES_17", "POSTGRES_18", "MYSQL_8_0", "MYSQL_8_4", "MYSQL_9_0"} {
		t.Run(version, func(t *testing.T) {
			if _, err := cloudSQLConfigForVersion(version); err != nil {
				t.Fatalf("cloudSQLConfigForVersion(%q): %v", version, err)
			}
		})
	}
}

func TestReconcileCloudSQLRejectsUnsafeBackendStatesWithoutMutation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	tests := []struct {
		name   string
		mutate func(*cloudSQLDockerFixture, CloudSQLBackendSpec)
	}{
		{
			name: "missing",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerFound = false
				fixture.volumeFound = false
			},
		},
		{
			name: "foreign container",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerLabels["minisky.profile"] = "other"
			},
		},
		{
			name: "wrong container service",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerLabels["minisky.service"] = "other"
			},
		},
		{
			name: "wrong container resource",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerLabels["minisky.resource"] = "other"
			},
		},
		{
			name: "database version label mismatch",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerLabels["minisky.database-version"] = "POSTGRES_14"
			},
		},
		{
			name: "foreign volume",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerFound = false
				fixture.volumeLabels["minisky.resource"] = "other"
			},
		},
		{
			name: "image mismatch",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerImage = "postgres:14"
			},
		},
		{
			name: "immutable image ID mismatch",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerImageID = "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
			},
		},
		{
			name: "immutable volume identity mismatch",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.volumeCreatedAt = "2026-07-29T00:00:01Z"
			},
		},
		{
			name: "mount mismatch",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.mountName = "foreign-volume"
			},
		},
		{
			name: "non-loopback endpoint",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.hostIP = "0.0.0.0"
			},
		},
		{
			name: "Docker unavailable",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.failInspect = errors.New("Docker socket unavailable")
			},
		},
		{
			name: "unreadable container inspect",
			mutate: func(fixture *cloudSQLDockerFixture, _ CloudSQLBackendSpec) {
				fixture.containerBody = "{broken"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := testCloudSQLSpec()
			fixture := newCloudSQLDockerFixture(t, spec)
			test.mutate(fixture, spec)
			manager := cloudSQLTestManager(fixture)
			if _, err := manager.ReconcileCloudSQLVM(context.Background(), spec); err == nil {
				t.Fatal("unsafe backend reconciliation returned nil")
			}
			if fixture.started != 0 || fixture.created != 0 || fixture.volumeCreates != 0 {
				t.Fatalf("unsafe reconciliation mutated Docker: starts=%d creates=%d volumeCreates=%d",
					fixture.started, fixture.created, fixture.volumeCreates)
			}
		})
	}
}

func TestReconcileCloudSQLReadinessFailureDoesNotReportEndpoint(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	manager := cloudSQLTestManager(fixture)
	manager.cloudSQLReady = func(context.Context, string, string, time.Duration) error {
		return context.DeadlineExceeded
	}
	endpoint, err := manager.ReconcileCloudSQLVM(context.Background(), spec)
	if err == nil || endpoint != "" {
		t.Fatalf("endpoint=%q error=%v", endpoint, err)
	}
	if fixture.started != 0 || fixture.created != 0 {
		t.Fatalf("running readiness failure mutated Docker: starts=%d creates=%d",
			fixture.started, fixture.created)
	}
}

func TestReconcileCloudSQLVolumeOnlyRequiresPersistedBootstrapInputs(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	spec.BootstrapPolicy = ""
	fixture := newCloudSQLDockerFixture(t, testCloudSQLSpec())
	fixture.containerFound = false
	manager := cloudSQLTestManager(fixture)
	if _, err := manager.ReconcileCloudSQLVM(context.Background(), spec); err == nil {
		t.Fatal("volume-only reconciliation accepted missing bootstrap policy")
	}
	if fixture.created != 0 || fixture.volumeCreates != 0 {
		t.Fatalf("missing bootstrap inputs created resources: containers=%d volumes=%d",
			fixture.created, fixture.volumeCreates)
	}
}

func TestReconcileCloudSQLVolumeOnlyRejectsMutableTagDrift(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	fixture.imageInspectID = "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	manager := cloudSQLTestManager(fixture)

	if _, err := manager.ReconcileCloudSQLVM(context.Background(), spec); err == nil {
		t.Fatal("volume-only reconciliation accepted mutable image tag drift")
	}
	if fixture.created != 0 || fixture.started != 0 || fixture.volumeCreates != 0 {
		t.Fatalf("tag drift mutated Docker: creates=%d starts=%d volumeCreates=%d",
			fixture.created, fixture.started, fixture.volumeCreates)
	}
}

func TestReconcileCloudSQLVolumeOnlyRejectsImageRaceBeforeStart(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	fixture.createdContainerImageID = "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	manager := cloudSQLTestManager(fixture)

	if _, err := manager.ReconcileCloudSQLVM(context.Background(), spec); err == nil {
		t.Fatal("volume-only reconciliation accepted changed image identity after create")
	}
	if fixture.created != 1 || fixture.started != 0 || fixture.removed != 1 {
		t.Fatalf("image race cleanup: creates=%d starts=%d removed=%d",
			fixture.created, fixture.started, fixture.removed)
	}
}

func TestWaitUntilPostgresHandshakeReadyRequiresProtocolResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		request := make([]byte, 128)
		if _, err := connection.Read(request); err != nil {
			serverDone <- err
			return
		}
		_, err = connection.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 5})
		serverDone <- err
	}()
	if err := waitUntilPostgresHandshakeReady(
		context.Background(),
		listener.Addr().String(),
		time.Second,
	); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestPrepareCloudSQLImagePullRequiresAcquisitionIntent(t *testing.T) {
	spec := testCloudSQLSpec()
	spec.ImageID = ""
	spec.VolumeIdentity = ""
	spec.CreationIntent = false
	spec.ImageAcquisitionIntent = false
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	fixture.imageFound = false
	fixture.imageInspectID = testCloudSQLImageID
	manager := cloudSQLTestManager(fixture)

	if _, err := manager.PrepareCloudSQLVM(context.Background(), spec); err == nil {
		t.Fatal("image acquisition without durable intent succeeded")
	}
	fixture.mu.Lock()
	pulls := fixture.imagePulls
	fixture.mu.Unlock()
	if pulls != 0 {
		t.Fatalf("image acquisition without durable intent pulled %d images", pulls)
	}

	spec.ImageAcquisitionIntent = true
	prepared, err := manager.PrepareCloudSQLVM(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ImageID != testCloudSQLImageID || !prepared.CreationIntent {
		t.Fatalf("prepared spec=%#v", prepared)
	}
	fixture.mu.Lock()
	pulls = fixture.imagePulls
	fixture.mu.Unlock()
	if pulls != 1 {
		t.Fatalf("authorized image acquisition pulls=%d, want 1", pulls)
	}
}

func TestInspectCloudSQLImageConfirmsPinnedRepositoryDigest(t *testing.T) {
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	manager := cloudSQLTestManager(fixture)
	const digestRef = "registry.example/minisky/postgres@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	fixture.imageRepoDigests = []string{
		"registry.example/minisky/postgres@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	if _, err := manager.inspectCloudSQLImageID(context.Background(), digestRef); err == nil {
		t.Fatal("mismatched pinned repository digest was accepted")
	}
	fixture.imageRepoDigests = append(fixture.imageRepoDigests, digestRef)
	imageID, err := manager.inspectCloudSQLImageID(context.Background(), digestRef)
	if err != nil {
		t.Fatal(err)
	}
	if imageID != spec.ImageID {
		t.Fatalf("image ID=%q, want %q", imageID, spec.ImageID)
	}
}

func TestProvisionCloudSQLRevalidatesAuthorizedImageBeforeVolumeMutation(t *testing.T) {
	spec := testCloudSQLSpec()
	fixture := newCloudSQLDockerFixture(t, spec)
	fixture.containerFound = false
	fixture.volumeFound = false
	fixture.imageInspectID = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	manager := cloudSQLTestManager(fixture)

	if _, _, _, err := manager.ProvisionCloudSQLVM(context.Background(), spec); err == nil {
		t.Fatal("provision accepted a changed image identity")
	}
	fixture.mu.Lock()
	volumeCreates := fixture.volumeCreates
	containers := fixture.created
	fixture.mu.Unlock()
	if volumeCreates != 0 || containers != 0 {
		t.Fatalf("changed image identity mutated backend: volumes=%d containers=%d", volumeCreates, containers)
	}
}

func newCloudSQLDockerFixture(t *testing.T, spec CloudSQLBackendSpec) *cloudSQLDockerFixture {
	t.Helper()
	_, volumeName, _ := cloudSQLDockerNames(spec.Project, spec.Instance)
	return &cloudSQLDockerFixture{
		t:                       t,
		containerFound:          true,
		containerState:          "running",
		containerImage:          spec.Image,
		containerImageID:        spec.ImageID,
		imageInspectID:          spec.ImageID,
		imageFound:              true,
		createdContainerImageID: spec.ImageID,
		containerLabels:         cloudSQLLabels(spec),
		mountName:               volumeName,
		mountTarget:             testCloudSQLRuntimeConfig().MountTarget,
		hostIP:                  "127.0.0.1",
		hostPort:                "55432",
		volumeFound:             true,
		volumeLabels:            cloudSQLLabels(spec),
		volumeName:              volumeName,
		volumeCreatedAt:         "2026-07-29T00:00:00Z",
		volumeMountpoint:        "/var/lib/docker/volumes/" + volumeName + "/_data",
	}
}

func cloudSQLTestManager(fixture *cloudSQLDockerFixture) *ServiceManager {
	return &ServiceManager{
		dockerClient:  &http.Client{Transport: fixture},
		dockerTimeout: time.Second,
		cloudSQLReady: func(context.Context, string, string, time.Duration) error {
			fixture.mu.Lock()
			fixture.readyCalls++
			fixture.mu.Unlock()
			return nil
		},
		cloudSQLAuthReady: func(context.Context, string, map[string]string, string, time.Duration) error {
			fixture.mu.Lock()
			fixture.authReadyCalls++
			fixture.mu.Unlock()
			return nil
		},
	}
}

func testCloudSQLSpec() CloudSQLBackendSpec {
	runtimeConfig := testCloudSQLRuntimeConfig()
	return CloudSQLBackendSpec{
		Project:                "project",
		Instance:               "sql",
		DatabaseVersion:        "POSTGRES_18",
		OwnershipFingerprint:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		BootstrapPolicy:        CloudSQLBootstrapPolicyV1,
		Image:                  runtimeConfig.Image,
		ImageID:                testCloudSQLImageID,
		ImageAcquisitionIntent: true,
		CreationIntent:         true,
		VolumeIdentity: cloudSQLVolumeInspect{
			Name:       cloudSQLVolumeNameForTest(),
			CreatedAt:  "2026-07-29T00:00:00Z",
			Mountpoint: "/var/lib/docker/volumes/" + cloudSQLVolumeNameForTest() + "/_data",
		}.identity(),
	}
}

func cloudSQLVolumeNameForTest() string {
	_, volumeName, _ := cloudSQLDockerNames("project", "sql")
	return volumeName
}

func testCloudSQLRuntimeConfig() cloudSQLRuntimeConfig {
	runtimeConfig, err := cloudSQLConfigForVersion("POSTGRES_18")
	if err != nil {
		panic(err)
	}
	return runtimeConfig
}
