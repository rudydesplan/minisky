package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func testRedisBackendSpec() RedisBackendSpec {
	return RedisBackendSpec{
		ResourceID:  "redis-0123456789abcdef0123456789abcdef",
		Image:       "valkey/valkey:7.2.12-alpine@sha256:28ca383369c5497fb4d63092e852a1c9e23c5a0b5553bb8f0f54a0b7fa0ddd4b",
		RepoDigest:  "valkey/valkey@sha256:28ca383369c5497fb4d63092e852a1c9e23c5a0b5553bb8f0f54a0b7fa0ddd4b",
		ImageID:     "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408",
		Platform:    "linux/amd64",
		ContainerID: strings.Repeat("c", 64),
		Generation:  1,
	}
}

func TestRedisBackendSpecRequiresPinnedLinuxAMD64Identity(t *testing.T) {
	valid := testRedisBackendSpec()
	if err := validateRedisBackendSpec(valid, false); err != nil {
		t.Fatalf("valid immutable Redis backend spec: %v", err)
	}
	unresolved := Redis72BackendSpec(valid.ResourceID)
	if err := validateRedisBackendSpec(unresolved, false); err != nil {
		t.Fatalf("valid unresolved Redis backend spec: %v", err)
	}
	if err := validateRedisBackendSpec(unresolved, true); err == nil {
		t.Fatal("persisted Redis backend accepted without resolved image and volume identities")
	}
	tests := []struct {
		name   string
		mutate func(*RedisBackendSpec)
	}{
		{name: "mutable image", mutate: func(spec *RedisBackendSpec) { spec.Image = "valkey/valkey:7.2.12-alpine" }},
		{name: "repo digest drift", mutate: func(spec *RedisBackendSpec) { spec.RepoDigest = "valkey/valkey@sha256:" + strings.Repeat("a", 64) }},
		{name: "malformed image ID", mutate: func(spec *RedisBackendSpec) { spec.ImageID = "sha256:not-an-id" }},
		{name: "unsupported platform", mutate: func(spec *RedisBackendSpec) { spec.Platform = "linux/arm64" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			test.mutate(&spec)
			if err := validateRedisBackendSpec(spec, false); err == nil {
				t.Fatalf("invalid spec accepted: %#v", spec)
			}
		})
	}
}

func TestRedisImageInspectRejectsRepoDigestImageIDAndPlatformDrift(t *testing.T) {
	valid := testRedisBackendSpec()
	tests := []struct {
		name string
		body string
	}{
		{
			name: "repo digest",
			body: `{"Id":"` + valid.ImageID + `","RepoDigests":["valkey/valkey@sha256:` + strings.Repeat("a", 64) + `"],"Os":"linux","Architecture":"amd64"}`,
		},
		{
			name: "image ID",
			body: `{"Id":"sha256:` + strings.Repeat("b", 64) + `","RepoDigests":["` + valid.RepoDigest + `"],"Os":"linux","Architecture":"amd64"}`,
		},
		{
			name: "platform",
			body: `{"Id":"` + valid.ImageID + `","RepoDigests":["` + valid.RepoDigest + `"],"Os":"linux","Architecture":"arm64"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return dockerResponse(http.StatusOK, test.body), nil
			})}}
			if _, err := manager.inspectRedisImageIdentity(context.Background(), valid); err == nil {
				t.Fatalf("%s drift was accepted", test.name)
			}
		})
	}
}

func TestRedisImageInspectResolvesDaemonIDAndTaggedRepoDigest(t *testing.T) {
	spec := Redis72BackendSpec("redis-backend")
	const daemonImageID = "sha256:28ca383369c5497fb4d63092e852a1c9e23c5a0b5553bb8f0f54a0b7fa0ddd4b"
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return dockerResponse(http.StatusOK, fmt.Sprintf(
			`{"Id":%q,"RepoDigests":[%q],"Os":"linux","Architecture":"amd64"}`,
			daemonImageID,
			spec.Image,
		)), nil
	})}}
	imageID, err := manager.inspectRedisImageIdentity(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if imageID != daemonImageID {
		t.Fatalf("resolved image ID=%q, want daemon identity %q", imageID, daemonImageID)
	}
}

func TestRedisContainerValidationRejectsCommandAndNetworkDrift(t *testing.T) {
	spec := testRedisBackendSpec()
	container := redisContainerInspect{
		ID:      strings.Repeat("a", 64),
		Name:    "/redis-container",
		ImageID: spec.ImageID,
	}
	spec.ContainerID = container.ID
	container.Config.Image = spec.ImageID
	container.Config.Labels = redisContainerLabels(spec)
	container.Config.Cmd = []string{
		"valkey-server",
		"--appendonly", "yes",
		"--appendfsync", "always",
		"--dir", "/data",
	}
	container.HostConfig.NetworkMode = networkName
	container.NetworkSettings.Networks = map[string]struct {
		NetworkID string `json:"NetworkID"`
	}{networkName: {NetworkID: "network-id"}}
	container.Mounts = append(container.Mounts, struct {
		Type        string `json:"Type"`
		Name        string `json:"Name"`
		Destination string `json:"Destination"`
		RW          bool   `json:"RW"`
	}{Type: "volume", Name: "redis-volume", Destination: "/data", RW: true})
	if err := validateRedisContainer(container, "redis-container", "redis-volume", spec); err != nil {
		t.Fatalf("valid exact Redis container: %v", err)
	}
	for name, mutate := range map[string]func(*redisContainerInspect){
		"command": func(candidate *redisContainerInspect) {
			candidate.Config.Cmd = []string{"valkey-server", "--appendonly", "no"}
		},
		"network": func(candidate *redisContainerInspect) {
			candidate.HostConfig.NetworkMode = "bridge"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := container
			mutate(&candidate)
			if err := validateRedisContainer(candidate, "redis-container", "redis-volume", spec); err == nil {
				t.Fatalf("Redis container %s drift was accepted", name)
			}
		})
	}
}

func TestCreateRedisContainerAllowsPreStartNetworkAttachmentLag(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-network-lag")
	spec := testRedisBackendSpec()
	spec.ResourceID = "redis-network-lag-resource"
	spec.ContainerID = ""
	spec.VolumeIdentity = "sha256:" + strings.Repeat("a", 64)
	spec.VolumeProvenance = "sha256:" + strings.Repeat("b", 64)
	spec.ContainerIdentity = "sha256:" + strings.Repeat("d", 64)
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	const containerID = "6123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
			return dockerResponse(http.StatusCreated, `{"Id":"`+containerID+`"}`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/containers/"+containerID+"/json":
			labels, _ := json.Marshal(redisContainerLabels(spec))
			command, _ := json.Marshal(redisServerCommand())
			return dockerResponse(http.StatusOK, fmt.Sprintf(`{
				"Id":%q,"Name":%q,"Image":%q,"State":{"Status":"created"},
				"Config":{"Image":%q,"Labels":%s,"Cmd":%s},
				"HostConfig":{"NetworkMode":"minisky-net"},
				"Mounts":[{"Type":"volume","Name":%q,"Destination":"/data","RW":true}],
				"NetworkSettings":{"Networks":{}}
			}`, containerID, "/"+containerName, spec.ImageID, spec.ImageID,
				labels, command, volumeName)), nil
		default:
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		}
	})}}

	container, disposition, err := manager.createRedisContainer(
		context.Background(), containerName, volumeName, spec)
	if err != nil || disposition != redisCreateExact || container.ID != containerID {
		t.Fatalf("pre-start attachment lag err=%v disposition=%v container=%#v",
			err, disposition, container)
	}
}

func TestRedisPostStartAttachmentVisibilityAndCompensation(t *testing.T) {
	for _, test := range []struct {
		name           string
		visibleAfter   int
		wantSuccess    bool
		wantCompensate bool
	}{
		{name: "delayed visibility", visibleAfter: 3, wantSuccess: true},
		{name: "never attached", visibleAfter: 1 << 30, wantCompensate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", "redis-post-start")
			spec := Redis72BackendSpec("redis-post-start-" + strings.ReplaceAll(test.name, " ", "-"))
			containerName, volumeName := redisDockerNames(spec.ResourceID)
			const (
				imageID     = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
				containerID = "7123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			)
			volumePresent, containerPresent, running := false, false, false
			containerDeleted, volumeDeleted, containerInspects := false, false, 0
			var volumeLabels, containerLabels map[string]string
			manager := &ServiceManager{}
			manager.redisReady = func(context.Context, string, time.Duration) error { return nil }
			manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch {
				case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
					return dockerResponse(http.StatusOK, fmt.Sprintf(
						`{"Id":%q,"RepoDigests":[%q],"Os":"linux","Architecture":"amd64"}`,
						imageID, spec.RepoDigest)), nil
				case request.Method == http.MethodGet && request.URL.Path == "/volumes/"+volumeName:
					if !volumePresent {
						return dockerResponse(http.StatusNotFound, `{}`), nil
					}
					labels, _ := json.Marshal(volumeLabels)
					return dockerResponse(http.StatusOK, fmt.Sprintf(
						`{"Name":%q,"CreatedAt":"2026-07-29T13:00:00Z","Mountpoint":%q,"Labels":%s}`,
						volumeName, "/var/lib/docker/volumes/"+volumeName+"/_data", labels)), nil
				case request.Method == http.MethodPost && request.URL.Path == "/volumes/create":
					var payload struct {
						Labels map[string]string `json:"Labels"`
					}
					if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
						t.Fatal(err)
					}
					volumeLabels, volumePresent = payload.Labels, true
					return dockerResponse(http.StatusCreated, `{}`), nil
				case request.Method == http.MethodGet &&
					(request.URL.Path == "/containers/"+containerName+"/json" ||
						request.URL.Path == "/containers/"+containerID+"/json"):
					if !containerPresent {
						return dockerResponse(http.StatusNotFound, `{}`), nil
					}
					containerInspects++
					labels, _ := json.Marshal(containerLabels)
					command, _ := json.Marshal(redisServerCommand())
					status, networks := "created", `{}`
					if running {
						status = "running"
						if containerInspects >= test.visibleAfter {
							networks = `{"minisky-net":{"NetworkID":"network-id"}}`
						}
					}
					return dockerResponse(http.StatusOK, fmt.Sprintf(`{
						"Id":%q,"Name":%q,"Image":%q,"State":{"Status":%q},
						"Config":{"Image":%q,"Labels":%s,"Cmd":%s},
						"HostConfig":{"NetworkMode":"minisky-net"},
						"Mounts":[{"Type":"volume","Name":%q,"Destination":"/data","RW":true}],
						"NetworkSettings":{"Ports":{"6379/tcp":[{"HostIp":"127.0.0.1","HostPort":"46379"}]},"Networks":%s}
					}`, containerID, "/"+containerName, imageID, status, imageID,
						labels, command, volumeName, networks)), nil
				case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
					var payload struct {
						Labels map[string]string `json:"Labels"`
					}
					if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
						t.Fatal(err)
					}
					containerLabels, containerPresent = payload.Labels, true
					return dockerResponse(http.StatusCreated, `{"Id":"`+containerID+`"}`), nil
				case request.Method == http.MethodPost && request.URL.Path == "/containers/"+containerID+"/start":
					running = true
					return dockerResponse(http.StatusNoContent, ""), nil
				case request.Method == http.MethodGet && request.URL.Path == "/networks/minisky-net":
					containers := `{}`
					if containerInspects >= test.visibleAfter {
						containers = fmt.Sprintf(`{%q:{"Name":%q}}`, containerID, containerName)
					}
					return dockerResponse(http.StatusOK, fmt.Sprintf(
						`{"Id":"network-id","Labels":{"managed-by":"minisky","minisky.profile":"redis-post-start"},"Containers":%s}`,
						containers)), nil
				case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+containerID:
					containerPresent, containerDeleted = false, true
					return dockerResponse(http.StatusNoContent, ""), nil
				case request.Method == http.MethodDelete && request.URL.Path == "/volumes/"+volumeName:
					volumePresent, volumeDeleted = false, true
					return dockerResponse(http.StatusNoContent, ""), nil
				default:
					return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
				}
			})}
			ctx := context.Background()
			if !test.wantSuccess {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			_, _, err := manager.ProvisionRedisExact(ctx, spec)
			if test.wantSuccess && err != nil {
				t.Fatal(err)
			}
			if !test.wantSuccess && err == nil {
				t.Fatal("never-attached Redis backend unexpectedly succeeded")
			}
			if test.wantCompensate != (containerDeleted && volumeDeleted) {
				t.Fatalf("compensation container=%t volume=%t, want %t",
					containerDeleted, volumeDeleted, test.wantCompensate)
			}
		})
	}
}

func TestProvisionAndReconcileRedisUseImmutableImageAndExactVolume(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-identity")
	spec := Redis72BackendSpec("redis-0123456789abcdef0123456789abcdef")
	const runtimeImageID = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	const (
		firstContainerID  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		secondContainerID = "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		createdAt         = "2026-07-29T08:00:00Z"
		mountpoint        = "/var/lib/docker/volumes/minisky-test/_data"
	)
	imagePresent := false
	volumePresent := false
	containerPresent := false
	containerRunning := false
	volumeCreates := 0
	containerCreates := 0
	containerDeletes := 0
	replacementInvalid := false
	rejectContainerDelete := false
	var volumePayloadLabels map[string]string
	var containerPayloadLabels map[string]string
	var containerCreateHostPorts []string
	manager := &ServiceManager{
		redisReady: func(context.Context, string, time.Duration) error { return nil },
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			currentContainerID := firstContainerID
			if containerCreates > 1 {
				currentContainerID = secondContainerID
			}
			switch {
			case request.Method == http.MethodGet &&
				(request.URL.Path == "/containers/"+containerName+"/json" ||
					request.URL.Path == "/containers/"+firstContainerID+"/json" ||
					request.URL.Path == "/containers/"+secondContainerID+"/json"):
				if !containerPresent {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				status := "created"
				if containerRunning {
					status = "running"
				}
				command := `["valkey-server","--appendonly","yes","--appendfsync","always","--dir","/data"]`
				if replacementInvalid && containerCreates > 1 {
					command = `["valkey-server","--appendonly","no"]`
				}
				containerLabels, marshalErr := json.Marshal(containerPayloadLabels)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				return dockerResponse(http.StatusOK, fmt.Sprintf(`{
					"Id":%q,
					"Name":%q,
					"Image":%q,
					"State":{"Status":%q},
					"Config":{"Image":%q,"Labels":%s,"Cmd":%s},
					"HostConfig":{"NetworkMode":"minisky-net"},
					"Mounts":[{"Type":"volume","Name":%q,"Destination":"/data","RW":true}],
					"NetworkSettings":{"Ports":{"6379/tcp":[{"HostIp":"127.0.0.1","HostPort":"46379"}]},"Networks":{"minisky-net":{"NetworkID":"network-id"}}}
				}`, currentContainerID, "/"+containerName, runtimeImageID, status, runtimeImageID, containerLabels, command, volumeName)), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				if !imagePresent {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, fmt.Sprintf(
					`{"Id":%q,"RepoDigests":[%q],"Os":"linux","Architecture":"amd64"}`,
					runtimeImageID, spec.RepoDigest,
				)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/images/create":
				if request.URL.Query().Get("fromImage") != spec.Image ||
					request.URL.Query().Get("platform") != spec.Platform {
					t.Fatalf("pull query = %q", request.URL.RawQuery)
				}
				imagePresent = true
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodGet && request.URL.Path == "/volumes/"+volumeName:
				if !volumePresent {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				encodedLabels, marshalErr := json.Marshal(volumePayloadLabels)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				return dockerResponse(http.StatusOK, fmt.Sprintf(
					`{"Name":%q,"CreatedAt":%q,"Mountpoint":%q,"Labels":%s}`,
					volumeName, createdAt, mountpoint, encodedLabels,
				)), nil
			case request.Method == http.MethodPost && request.URL.Path == "/volumes/create":
				var payload struct {
					Name   string            `json:"Name"`
					Labels map[string]string `json:"Labels"`
				}
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				if payload.Name != volumeName {
					t.Fatalf("volume create name=%q, want %q", payload.Name, volumeName)
				}
				volumePayloadLabels = payload.Labels
				volumeCreates++
				volumePresent = true
				return dockerResponse(http.StatusCreated, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				var payload struct {
					Image      string            `json:"Image"`
					Labels     map[string]string `json:"Labels"`
					HostConfig struct {
						PortBindings map[string][]struct {
							HostPort string `json:"HostPort"`
						} `json:"PortBindings"`
					} `json:"HostConfig"`
				}
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				if payload.Image != runtimeImageID {
					t.Fatalf("container image = %q, want immutable ID %q", payload.Image, runtimeImageID)
				}
				containerPayloadLabels = payload.Labels
				bindings := payload.HostConfig.PortBindings["6379/tcp"]
				if len(bindings) != 1 {
					t.Fatalf("container port bindings=%#v", payload.HostConfig.PortBindings)
				}
				containerCreateHostPorts = append(containerCreateHostPorts, bindings[0].HostPort)
				containerCreates++
				containerPresent = true
				createdID := firstContainerID
				if containerCreates > 1 {
					createdID = secondContainerID
				}
				return dockerResponse(http.StatusCreated, `{"Id":"`+createdID+`"}`), nil
			case request.Method == http.MethodPost &&
				(request.URL.Path == "/containers/"+firstContainerID+"/start" ||
					request.URL.Path == "/containers/"+secondContainerID+"/start"):
				containerRunning = true
				return dockerResponse(http.StatusNoContent, `{}`), nil
			case request.Method == http.MethodDelete &&
				request.URL.Path == "/containers/"+secondContainerID:
				containerDeletes++
				if rejectContainerDelete {
					return dockerResponse(http.StatusInternalServerError, `{"message":"delete unavailable"}`), nil
				}
				containerPresent = false
				containerRunning = false
				return dockerResponse(http.StatusNoContent, `{}`), nil
			case request.Method == http.MethodGet &&
				(request.URL.Path == "/networks/network-id" || request.URL.Path == "/networks/minisky-net"):
				return dockerResponse(http.StatusOK, fmt.Sprintf(
					`{"Id":"network-id","Labels":{"managed-by":"minisky","minisky.profile":"redis-identity"},"Containers":{%q:{"Name":%q}}}`,
					currentContainerID, containerName,
				)), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		})},
	}

	endpoint, resolved, err := manager.ProvisionRedisExact(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "127.0.0.1:46379" ||
		resolved.ImageID != runtimeImageID ||
		resolved.VolumeIdentity == "" ||
		resolved.HostPort != "46379" {
		t.Fatalf("endpoint=%q resolved=%#v", endpoint, resolved)
	}
	if volumeCreates != 1 || containerCreates != 1 {
		t.Fatalf("volume creates=%d container creates=%d", volumeCreates, containerCreates)
	}
	if volumePayloadLabels["managed-by"] != "minisky" ||
		volumePayloadLabels["minisky.profile"] != "redis-identity" ||
		volumePayloadLabels["minisky.service"] != "memorystore-redis" ||
		volumePayloadLabels["minisky.resource"] != spec.ResourceID ||
		!redisImmutableIDPattern.MatchString(volumePayloadLabels["minisky.volume-identity"]) ||
		volumePayloadLabels["minisky.volume-identity"] != resolved.VolumeProvenance ||
		len(volumePayloadLabels) != 5 {
		t.Fatalf("volume create labels=%#v", volumePayloadLabels)
	}
	for key, want := range map[string]string{
		"minisky.generation":         "1",
		"minisky.container-name":     containerName,
		"minisky.image-id":           runtimeImageID,
		"minisky.volume-name":        volumeName,
		"minisky.volume-identity":    resolved.VolumeIdentity,
		"minisky.volume-provenance":  resolved.VolumeProvenance,
		"minisky.container-identity": resolved.ContainerIdentity,
	} {
		if containerPayloadLabels[key] != want {
			t.Fatalf("container create label %q=%q, want %q", key, containerPayloadLabels[key], want)
		}
	}
	runtimes := manager.redisRuntimeSnapshot()
	if len(runtimes) != 0 {
		t.Fatalf("uncommitted provision was published: %#v", runtimes)
	}
	if provisional, ok := manager.redisProvisionals[resolved.ResourceID]; !ok || provisional.Spec != resolved {
		t.Fatalf("validated provision was not staged: %#v", manager.redisProvisionals)
	}
	if err := manager.PublishRedisRuntime(context.Background(), resolved); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.redisProvisionals[resolved.ResourceID]; ok {
		t.Fatal("published provision retained provisional provenance")
	}
	runtimes = manager.redisRuntimeSnapshot()
	if len(runtimes) != 1 || runtimes[0].Spec != resolved ||
		runtimes[0].ContainerName != containerName ||
		runtimes[0].VolumeName != volumeName {
		t.Fatalf("provisioned runtime registry = %#v", runtimes)
	}

	containerPresent = false
	containerRunning = false
	replacementInvalid = true
	rejectContainerDelete = true
	_, failedSpec, owned, err := manager.ReconcileRedisExact(context.Background(), resolved)
	if err == nil {
		t.Fatal("post-create replacement validation failure was accepted")
	}
	provisional, provisionalFound := manager.redisProvisionals[resolved.ResourceID]
	if !owned || failedSpec != resolved || containerDeletes != 0 || !containerPresent ||
		!volumePresent || !provisionalFound || provisional.Spec.ContainerID != secondContainerID ||
		!provisional.CleanupBlocked || provisional.Diagnostic == "" {
		t.Fatalf("failed replacement owned=%t spec=%#v deletes=%d volume=%t",
			owned, failedSpec, containerDeletes, volumePresent)
	}
	if err := manager.retryStagedRedisProvisionalLocked(context.Background(), resolved.ResourceID); err == nil {
		t.Fatal("unknown failed replacement permitted automatic retry cleanup")
	}
	containerPresent = false // explicit operator cleanup of the preserved unknown container
	delete(manager.redisProvisionals, resolved.ResourceID)
	replacementInvalid = false
	rejectContainerDelete = false
	endpoint, reconciled, owned, err := manager.ReconcileRedisExact(context.Background(), resolved)
	if err != nil {
		t.Fatal(err)
	}
	if !owned || endpoint != "127.0.0.1:46379" ||
		reconciled.Generation != resolved.Generation+1 ||
		reconciled.ContainerID == resolved.ContainerID ||
		reconciled.ImageID != resolved.ImageID ||
		reconciled.VolumeIdentity != resolved.VolumeIdentity ||
		reconciled.VolumeProvenance != resolved.VolumeProvenance ||
		reconciled.HostPort != resolved.HostPort {
		t.Fatalf("endpoint=%q owned=%t reconciled=%#v", endpoint, owned, reconciled)
	}
	if got, want := containerCreateHostPorts, []string{"0", "46379", "46379"}; !slices.Equal(got, want) {
		t.Fatalf("container create host ports=%v, want %v", got, want)
	}
	if volumeCreates != 1 || containerCreates != 3 {
		t.Fatalf("replacement volume creates=%d container creates=%d", volumeCreates, containerCreates)
	}
	runtimes = manager.redisRuntimeSnapshot()
	if len(runtimes) != 1 || runtimes[0].Spec != resolved {
		t.Fatalf("unpublished replacement changed runtime registry = %#v", runtimes)
	}
	if err := manager.PublishRedisRuntime(context.Background(), reconciled); err != nil {
		t.Fatal(err)
	}
	runtimes = manager.redisRuntimeSnapshot()
	if len(runtimes) != 1 || runtimes[0].Spec != reconciled ||
		runtimes[0].Spec.VolumeIdentity != resolved.VolumeIdentity {
		t.Fatalf("reconciled runtime registry = %#v", runtimes)
	}
	if _, ok := manager.redisProvisionals[resolved.ResourceID]; ok {
		t.Fatal("published replacement retained provisional provenance")
	}
}

func TestProvisionRedisCompensatesPostCreateValidationFailure(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-rollback")
	spec := Redis72BackendSpec("redis-rollback-resource")
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	const (
		imageID     = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
		containerID = "4123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		createdAt   = "2026-07-29T08:00:00Z"
	)
	volumePresent := false
	volumeDeleted := false
	containerPresent := false
	containerDeleted := false
	var volumeLabels map[string]string
	var containerLabels map[string]string
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet &&
			(request.URL.Path == "/containers/"+containerName+"/json" ||
				request.URL.Path == "/containers/"+containerID+"/json"):
			if !containerPresent {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			labels, _ := json.Marshal(containerLabels)
			return dockerResponse(http.StatusOK, fmt.Sprintf(`{
				"Id":%q,"Name":%q,"Image":%q,"State":{"Status":"created"},
				"Config":{"Image":%q,"Labels":%s,"Cmd":["valkey-server","--appendonly","yes","--appendfsync","always","--dir","/data"]},
				"HostConfig":{"NetworkMode":"minisky-net"},
				"Mounts":[{"Type":"volume","Name":%q,"Destination":"/data","RW":true}],
				"NetworkSettings":{"Networks":{"minisky-net":{"NetworkID":"network-id"}}}
			}`, containerID, "/"+containerName, imageID, imageID, labels, volumeName)), nil
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
			return dockerResponse(http.StatusOK, fmt.Sprintf(
				`{"Id":%q,"RepoDigests":[%q],"Os":"linux","Architecture":"amd64"}`,
				imageID, spec.RepoDigest,
			)), nil
		case request.Method == http.MethodGet && request.URL.Path == "/volumes/"+volumeName:
			if !volumePresent {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			labels, marshalErr := json.Marshal(volumeLabels)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			return dockerResponse(http.StatusOK, fmt.Sprintf(
				`{"Name":%q,"CreatedAt":%q,"Mountpoint":"/var/lib/docker/volumes/redis-rollback/_data","Labels":%s}`,
				volumeName, createdAt, labels,
			)), nil
		case request.Method == http.MethodPost && request.URL.Path == "/volumes/create":
			var payload struct {
				Labels map[string]string `json:"Labels"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			volumeLabels = payload.Labels
			volumePresent = true
			return dockerResponse(http.StatusCreated, `{}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
			var payload struct {
				Labels map[string]string `json:"Labels"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			containerLabels = payload.Labels
			containerPresent = true
			return dockerResponse(http.StatusCreated, `{"Id":"invalid"}`), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+containerID:
			containerPresent = false
			containerDeleted = true
			return dockerResponse(http.StatusNoContent, `{}`), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/volumes/"+volumeName:
			volumePresent = false
			volumeDeleted = true
			return dockerResponse(http.StatusNoContent, `{}`), nil
		default:
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		}
	})}}
	if _, _, err := manager.ProvisionRedisExact(context.Background(), spec); err == nil {
		t.Fatal("container creation failure was accepted")
	}
	if containerPresent || !containerDeleted || volumePresent || !volumeDeleted {
		t.Fatalf("post-create compensation container present=%t deleted=%t volume present=%t deleted=%t",
			containerPresent, containerDeleted, volumePresent, volumeDeleted)
	}
}

func TestRedisProvisionRollbackPreservesSameNameReplacementVolume(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-volume-replacement")
	spec := Redis72BackendSpec("redis-volume-replacement-resource")
	spec.ImageID = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	spec.VolumeProvenance = "sha256:" + strings.Repeat("a", 64)
	spec.ContainerIdentity = "sha256:" + strings.Repeat("b", 64)
	spec.Generation = 1
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	volume := redisVolumeInspect{
		Name:       volumeName,
		CreatedAt:  "2026-07-29T12:00:00Z",
		Mountpoint: "/var/lib/docker/volumes/" + volumeName + "/_data",
		Labels:     redisVolumeLabels(spec),
	}
	spec.VolumeIdentity, _ = volume.identity()
	spec.ContainerID = strings.Repeat("5", 64)
	containerPresent := true
	volumeDeletes := 0
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/"+spec.ContainerID+"/json":
			if !containerPresent {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			labels, _ := json.Marshal(redisContainerLabels(spec))
			command, _ := json.Marshal(redisServerCommand())
			return dockerResponse(http.StatusOK, fmt.Sprintf(`{
				"Id":%q,"Name":%q,"Image":%q,"State":{"Status":"created"},
				"Config":{"Image":%q,"Labels":%s,"Cmd":%s},
				"HostConfig":{"NetworkMode":"minisky-net"},
				"Mounts":[{"Type":"volume","Name":%q,"Destination":"/data","RW":true}],
				"NetworkSettings":{"Networks":{"minisky-net":{"NetworkID":"network-id"}}}
			}`, spec.ContainerID, "/"+containerName, spec.ImageID, spec.ImageID,
				labels, command, volumeName)), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+spec.ContainerID:
			containerPresent = false
			// Deterministic inspect-to-delete hook: another actor replaces the
			// same-named volume while retaining copied ownership labels.
			volume.CreatedAt = "2026-07-29T12:00:01Z"
			volume.Mountpoint = "/var/lib/docker/volumes/replacement/_data"
			return dockerResponse(http.StatusNoContent, ""), nil
		case request.Method == http.MethodGet && request.URL.Path == "/volumes/"+volumeName:
			payload, _ := json.Marshal(volume)
			return dockerResponse(http.StatusOK, string(payload)), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/volumes/"+volumeName:
			volumeDeletes++
			return dockerResponse(http.StatusNoContent, ""), nil
		default:
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		}
	})}}
	manager.stageRedisProvisional(spec, true)

	err := manager.rollbackRedisProvisionLocked(context.Background(), spec, errors.New("injected create failure"))
	if err == nil || volumeDeletes != 0 {
		t.Fatalf("rollback err=%v volumeDeletes=%d, want failure and preserved replacement", err, volumeDeletes)
	}
	if _, ok := manager.redisProvisionals[spec.ResourceID]; !ok {
		t.Fatal("failed rollback discarded provisional cleanup diagnostics")
	}
}

func TestRedisReconcileCompensationIgnoresExhaustedRequestContext(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-reconcile-cancel")
	spec := Redis72BackendSpec("redis-reconcile-cancel-resource")
	spec.ImageID = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	spec.VolumeIdentity = "sha256:" + strings.Repeat("a", 64)
	spec.VolumeProvenance = "sha256:" + strings.Repeat("b", 64)
	spec.ContainerIdentity = "sha256:" + strings.Repeat("d", 64)
	spec.ContainerID = strings.Repeat("8", 64)
	spec.Generation = 2
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	containerPresent, deleted := true, false
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Context().Err() != nil {
			return nil, fmt.Errorf("cleanup inherited exhausted request context: %w", request.Context().Err())
		}
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/"+spec.ContainerID+"/json":
			if !containerPresent {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			labels, _ := json.Marshal(redisContainerLabels(spec))
			command, _ := json.Marshal(redisServerCommand())
			return dockerResponse(http.StatusOK, fmt.Sprintf(`{
				"Id":%q,"Name":%q,"Image":%q,"State":{"Status":"created"},
				"Config":{"Image":%q,"Labels":%s,"Cmd":%s},
				"HostConfig":{"NetworkMode":"minisky-net"},
				"Mounts":[{"Type":"volume","Name":%q,"Destination":"/data","RW":true}],
				"NetworkSettings":{"Networks":{}}
			}`, spec.ContainerID, "/"+containerName, spec.ImageID, spec.ImageID,
				labels, command, volumeName)), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+spec.ContainerID:
			containerPresent, deleted = false, true
			return dockerResponse(http.StatusNoContent, ""), nil
		default:
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		}
	})}}
	manager.stageRedisProvisional(spec, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := manager.compensateRedisReplacementLocked(ctx, spec, context.Canceled)
	if !errors.Is(err, context.Canceled) || !deleted || containerPresent {
		t.Fatalf("compensation err=%v deleted=%t present=%t", err, deleted, containerPresent)
	}
	if _, ok := manager.redisProvisionals[spec.ResourceID]; ok {
		t.Fatal("successful canceled-context compensation retained provisional provenance")
	}
}

func TestRedisAmbiguousContainerCreateRecovery(t *testing.T) {
	for _, mode := range []string{
		"create-side-effect-transport-error",
		"invalid-id",
		"absent",
		"foreign-collision",
		"inspect-failure",
		"compensation-failure",
		"volume-identity-replaced-before-delete",
	} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", "redis-ambiguous")
			spec := Redis72BackendSpec("redis-ambiguous-resource")
			spec.ImageID = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
			spec.VolumeProvenance = "sha256:" + strings.Repeat("e", 64)
			spec.ContainerIdentity = "sha256:" + strings.Repeat("d", 64)
			spec.Generation = 1
			containerName, volumeName := redisDockerNames(spec.ResourceID)
			volume := redisVolumeInspect{
				Name:       volumeName,
				CreatedAt:  "2026-07-29T12:00:00Z",
				Mountpoint: "/var/lib/docker/volumes/" + volumeName + "/_data",
				Labels:     redisVolumeLabels(spec),
			}
			spec.VolumeIdentity, _ = volume.identity()
			const containerID = "5123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			containerPresent := false
			volumePresent := true
			containerDeleted := false
			volumeDeleted := false
			manager := &ServiceManager{}
			manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch {
				case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
					if mode != "absent" && mode != "inspect-failure" {
						containerPresent = true
					}
					if mode == "invalid-id" || mode == "volume-identity-replaced-before-delete" {
						return dockerResponse(http.StatusCreated, `{"Id":"invalid"}`), nil
					}
					return nil, errors.New("injected ambiguous create transport failure")
				case request.Method == http.MethodGet &&
					(request.URL.Path == "/containers/"+containerName+"/json" ||
						request.URL.Path == "/containers/"+containerID+"/json"):
					if mode == "inspect-failure" && request.URL.Path == "/containers/"+containerName+"/json" {
						return nil, errors.New("injected deterministic-name inspect failure")
					}
					if !containerPresent {
						return dockerResponse(http.StatusNotFound, `{}`), nil
					}
					labels := redisContainerLabels(spec)
					command := redisServerCommand()
					if mode == "foreign-collision" {
						labels = map[string]string{"managed-by": "foreign"}
						command = []string{"valkey-server"}
					}
					encodedLabels, _ := json.Marshal(labels)
					encodedCommand, _ := json.Marshal(command)
					return dockerResponse(http.StatusOK, fmt.Sprintf(`{
						"Id":%q,"Name":%q,"Image":%q,"State":{"Status":"created"},
						"Config":{"Image":%q,"Labels":%s,"Cmd":%s},
						"HostConfig":{"NetworkMode":"minisky-net"},
						"Mounts":[{"Type":"volume","Name":%q,"Destination":"/data","RW":true}],
						"NetworkSettings":{"Networks":{"minisky-net":{"NetworkID":"network-id"}}}
					}`, containerID, "/"+containerName, spec.ImageID, spec.ImageID,
						encodedLabels, encodedCommand, volumeName)), nil
				case request.Method == http.MethodGet && request.URL.Path == "/volumes/"+volumeName:
					if !volumePresent {
						return dockerResponse(http.StatusNotFound, `{}`), nil
					}
					encoded, _ := json.Marshal(volume)
					return dockerResponse(http.StatusOK, string(encoded)), nil
				case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
					return dockerResponse(http.StatusOK, fmt.Sprintf(
						`{"Id":%q,"RepoDigests":[%q],"Os":"linux","Architecture":"amd64"}`,
						spec.ImageID, spec.RepoDigest,
					)), nil
				case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+containerID:
					if mode == "compensation-failure" {
						return dockerResponse(http.StatusInternalServerError, `{"message":"delete failed"}`), nil
					}
					containerPresent = false
					containerDeleted = true
					if mode == "volume-identity-replaced-before-delete" {
						volume.CreatedAt = "2026-07-29T12:00:01Z"
						volume.Mountpoint = "/var/lib/docker/volumes/replacement/_data"
					}
					return dockerResponse(http.StatusNoContent, ""), nil
				case request.Method == http.MethodDelete && request.URL.Path == "/volumes/"+volumeName:
					volumePresent = false
					volumeDeleted = true
					return dockerResponse(http.StatusNoContent, ""), nil
				default:
					t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
					return nil, nil
				}
			})}

			container, disposition, err := manager.createRedisContainer(
				context.Background(), containerName, volumeName, spec)
			if err == nil {
				t.Fatal("ambiguous create unexpectedly succeeded")
			}
			spec.ContainerID = container.ID
			switch mode {
			case "create-side-effect-transport-error", "invalid-id", "compensation-failure",
				"volume-identity-replaced-before-delete":
				if disposition != redisCreateExact || spec.ContainerID != containerID {
					t.Fatalf("exact recovery disposition=%v err=%v container=%#v", disposition, err, container)
				}
				manager.stageRedisProvisionalDiagnostic(spec, true, container, err, false)
				cleanupErr := manager.discardRedisProvisionalLocked(context.Background(), spec, true)
				if mode == "compensation-failure" {
					if cleanupErr == nil || !containerPresent || !volumePresent ||
						manager.redisProvisionals[spec.ResourceID].Diagnostic == "" {
						t.Fatalf("failed compensation state err=%v container=%t volume=%t provisional=%#v",
							cleanupErr, containerPresent, volumePresent, manager.redisProvisionals)
					}
				} else if mode == "volume-identity-replaced-before-delete" {
					if cleanupErr == nil || !containerDeleted || volumeDeleted || !volumePresent {
						t.Fatalf("replacement compensation err=%v containerDeleted=%t volumeDeleted=%t",
							cleanupErr, containerDeleted, volumeDeleted)
					}
				} else if cleanupErr != nil || !containerDeleted || !volumeDeleted {
					t.Fatalf("exact compensation err=%v containerDeleted=%t volumeDeleted=%t",
						cleanupErr, containerDeleted, volumeDeleted)
				}
			case "absent":
				if disposition != redisCreateAbsent || container.ID != "" {
					t.Fatalf("absent recovery disposition=%v container=%#v", disposition, container)
				}
				manager.stageRedisProvisionalDiagnostic(spec, true, container, err, false)
				if cleanupErr := manager.discardRedisProvisionalLocked(context.Background(), spec, true); cleanupErr != nil {
					t.Fatal(cleanupErr)
				}
				if !volumeDeleted {
					t.Fatal("absent ambiguous create retained exact new volume")
				}
			default:
				if disposition != redisCreateUnknown {
					t.Fatalf("unknown recovery disposition=%v", disposition)
				}
				manager.stageRedisProvisionalDiagnostic(spec, true, container, err, true)
				if retryErr := manager.retryStagedRedisProvisionalLocked(context.Background(), spec.ResourceID); retryErr == nil {
					t.Fatal("unknown ambiguous create permitted automatic cleanup")
				}
				if containerDeleted || volumeDeleted {
					t.Fatal("unknown ambiguous create deleted resources")
				}
			}
		})
	}
}
