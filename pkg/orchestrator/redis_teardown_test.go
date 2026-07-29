package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type redisTeardownFixture struct {
	spec              RedisBackendSpec
	containerName     string
	volumeName        string
	networkID         string
	createdAt         string
	mountpoint        string
	containerPresent  bool
	volumePresent     bool
	networkPresent    bool
	containerDeletes  int
	networkDeletes    int
	containerInspects int
	drift             string
}

func newRedisTeardownFixture(t *testing.T, drift string) (*ServiceManager, *redisTeardownFixture) {
	t.Helper()
	t.Setenv("MINISKY_PROFILE", "redis-teardown")
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	spec := Redis72BackendSpec("redis-runtime-resource")
	spec.ImageID = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	spec.VolumeIdentity = "sha256:e86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	spec.VolumeProvenance = "sha256:f86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	spec.ContainerIdentity = "sha256:d86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	spec.ContainerID = strings.Repeat("c", 64)
	spec.Generation = 7
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	fixture := &redisTeardownFixture{
		spec:             spec,
		containerName:    containerName,
		volumeName:       volumeName,
		networkID:        strings.Repeat("n", 64),
		createdAt:        "2026-07-29T10:00:00Z",
		mountpoint:       "/var/lib/docker/volumes/" + volumeName + "/_data",
		containerPresent: true,
		volumePresent:    true,
		networkPresent:   true,
		drift:            drift,
	}
	volumeIdentity, err := (redisVolumeInspect{
		Name:       volumeName,
		CreatedAt:  fixture.createdAt,
		Mountpoint: fixture.mountpoint,
	}).identity()
	if err != nil {
		t.Fatal(err)
	}
	fixture.spec.VolumeIdentity = volumeIdentity
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.registerRedisRuntime(redisRuntimeIdentity{
		Spec:             fixture.spec,
		Profile:          "redis-teardown",
		ContainerName:    containerName,
		VolumeName:       volumeName,
		VolumeCreatedAt:  fixture.createdAt,
		VolumeMountpoint: fixture.mountpoint,
		NetworkID:        fixture.networkID,
		Endpoint:         "127.0.0.1:46379",
	})
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet &&
			(request.URL.Path == "/containers/"+fixture.spec.ContainerID+"/json" ||
				request.URL.Path == "/containers/"+fixture.containerName+"/json"):
			fixture.containerInspects++
			if drift == "inspect-error" {
				return dockerResponse(http.StatusInternalServerError, `{"message":"inspect unavailable"}`), nil
			}
			if !fixture.containerPresent {
				return dockerResponse(http.StatusNotFound, `{"message":"not found"}`), nil
			}
			return dockerResponse(http.StatusOK, fixture.containerJSON()), nil
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
			return dockerResponse(http.StatusNotFound, `{"message":"not found"}`), nil
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
			imageID := fixture.spec.ImageID
			if drift == "image-inspect" {
				imageID = "sha256:" + strings.Repeat("d", 64)
			}
			return dockerResponse(http.StatusOK, fmt.Sprintf(
				`{"Id":%q,"RepoDigests":[%q],"Os":"linux","Architecture":"amd64"}`,
				imageID, fixture.spec.RepoDigest,
			)), nil
		case request.Method == http.MethodGet && request.URL.Path == "/volumes/"+fixture.volumeName:
			if drift == "volume-inspect-error" {
				return dockerResponse(http.StatusInternalServerError, `{"message":"volume inspect unavailable"}`), nil
			}
			if !fixture.volumePresent {
				return dockerResponse(http.StatusNotFound, `{"message":"not found"}`), nil
			}
			return dockerResponse(http.StatusOK, fixture.volumeJSON()), nil
		case request.Method == http.MethodGet &&
			(request.URL.Path == "/networks/"+fixture.networkID || request.URL.Path == "/networks/minisky-net"):
			if drift == "network-inspect-error" {
				return dockerResponse(http.StatusInternalServerError, `{"message":"network inspect unavailable"}`), nil
			}
			if !fixture.networkPresent {
				return dockerResponse(http.StatusNotFound, `{"message":"not found"}`), nil
			}
			return dockerResponse(http.StatusOK, fixture.networkJSON()), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+fixture.spec.ContainerID:
			fixture.containerDeletes++
			fixture.containerPresent = false
			return dockerResponse(http.StatusNoContent, ""), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/networks/"+fixture.networkID:
			fixture.networkDeletes++
			fixture.networkPresent = false
			return dockerResponse(http.StatusNoContent, ""), nil
		case request.Method == http.MethodPost:
			return dockerResponse(http.StatusNoContent, ""), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}
	return manager, fixture
}

func (fixture *redisTeardownFixture) containerJSON() string {
	spec := fixture.spec
	name := "/" + fixture.containerName
	imageID := spec.ImageID
	command := redisServerCommand()
	networkMode := networkName
	networkID := fixture.networkID
	mountName := fixture.volumeName
	mountDestination := "/data"
	state := "running"
	hostIP := "127.0.0.1"
	hostPort := "46379"
	labels := redisContainerLabels(spec)
	switch fixture.drift {
	case "id":
		spec.ContainerID = strings.Repeat("d", 64)
	case "name":
		name = "/copied-name"
	case "image":
		imageID = "sha256:" + strings.Repeat("d", 64)
	case "command":
		command = []string{"valkey-server", "--appendonly", "no"}
	case "network-mode":
		networkMode = "bridge"
	case "network-attachment":
		networkID = strings.Repeat("x", 64)
	case "mount":
		mountDestination = "/copied"
	case "volume":
		mountName = "copied-volume"
	case "stopped":
		state = "exited"
	case "dead":
		state = "dead"
	case "copied-labels":
		labels["minisky.generation"] = "6"
	case "loopback":
		hostIP = "0.0.0.0"
	case "port":
		hostPort = "46380"
	case "toctou":
		if fixture.containerInspects >= 3 {
			command = []string{"valkey-server", "--appendonly", "no"}
		}
	}
	encodedLabels, _ := json.Marshal(labels)
	encodedCommand, _ := json.Marshal(command)
	networks := fmt.Sprintf(`{"minisky-net":{"NetworkID":%q}}`, networkID)
	if fixture.drift == "extra-network" {
		networks = fmt.Sprintf(
			`{"minisky-net":{"NetworkID":%q},"foreign-net":{"NetworkID":"foreign-id"}}`,
			networkID)
	}
	return fmt.Sprintf(`{
		"Id":%q,"Name":%q,"Image":%q,"State":{"Status":%q},
		"Config":{"Image":%q,"Labels":%s,"Cmd":%s},
		"HostConfig":{"NetworkMode":%q},
		"Mounts":[{"Type":"volume","Name":%q,"Destination":%q,"RW":true}],
		"NetworkSettings":{"Ports":{"6379/tcp":[{"HostIp":%q,"HostPort":%q}]},"Networks":%s}
	}`, spec.ContainerID, name, imageID, state, imageID, encodedLabels, encodedCommand,
		networkMode, mountName, mountDestination, hostIP, hostPort, networks)
}

func (fixture *redisTeardownFixture) volumeJSON() string {
	createdAt := fixture.createdAt
	mountpoint := fixture.mountpoint
	if fixture.drift == "volume-created" {
		createdAt = "2026-07-29T10:00:01Z"
	}
	if fixture.drift == "volume-mountpoint" {
		mountpoint += "-copied"
	}
	labels := redisVolumeLabels(fixture.spec)
	encodedLabels, _ := json.Marshal(labels)
	return fmt.Sprintf(`{"Name":%q,"CreatedAt":%q,"Mountpoint":%q,"Labels":%s}`,
		fixture.volumeName, createdAt, mountpoint, encodedLabels)
}

func (fixture *redisTeardownFixture) networkJSON() string {
	endpoints := map[string]map[string]string{}
	if fixture.containerPresent {
		endpoints[fixture.spec.ContainerID] = map[string]string{"Name": fixture.containerName}
	}
	if fixture.drift == "mixed-foreign" {
		endpoints[strings.Repeat("f", 64)] = map[string]string{"Name": "foreign"}
	}
	encodedEndpoints, _ := json.Marshal(endpoints)
	return fmt.Sprintf(`{"Id":%q,"Labels":{"managed-by":"minisky","minisky.profile":"redis-teardown"},"Containers":%s}`,
		fixture.networkID, encodedEndpoints)
}

func TestRedisGracefulTeardownRemovesExactContainerRetainsVolumeAndRemovesNetwork(t *testing.T) {
	manager, fixture := newRedisTeardownFixture(t, "")
	if err := manager.Teardown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fixture.containerPresent || fixture.containerDeletes != 1 {
		t.Fatalf("exact Redis container cleanup present=%t deletes=%d",
			fixture.containerPresent, fixture.containerDeletes)
	}
	if !fixture.volumePresent {
		t.Fatal("graceful Redis teardown deleted the retained AOF volume")
	}
	if fixture.networkPresent || fixture.networkDeletes != 1 {
		t.Fatalf("exact network cleanup present=%t deletes=%d", fixture.networkPresent, fixture.networkDeletes)
	}
	if got := manager.redisRuntimeSnapshot(); len(got) != 0 {
		t.Fatalf("graceful teardown retained runtime identities: %#v", got)
	}
}

func TestRedisGracefulTeardownFailsClosedOnIdentityDrift(t *testing.T) {
	for _, drift := range []string{
		"mixed-foreign",
		"inspect-error",
		"network-inspect-error",
		"volume-inspect-error",
		"id",
		"name",
		"image",
		"image-inspect",
		"command",
		"network-mode",
		"network-attachment",
		"extra-network",
		"mount",
		"volume",
		"volume-created",
		"volume-mountpoint",
		"stopped",
		"dead",
		"copied-labels",
		"loopback",
		"port",
		"toctou",
	} {
		t.Run(drift, func(t *testing.T) {
			manager, fixture := newRedisTeardownFixture(t, drift)
			if err := manager.Teardown(context.Background()); err == nil {
				t.Fatal("drifted Redis runtime was accepted")
			}
			if fixture.containerDeletes != 0 || fixture.networkDeletes != 0 ||
				!fixture.containerPresent || !fixture.volumePresent || !fixture.networkPresent {
				t.Fatalf("drifted runtime was mutated: %#v", fixture)
			}
		})
	}
}

func TestRedisMixedNetworkAttachmentsFailClosedAcrossRuntimePaths(t *testing.T) {
	for _, path := range []string{"publication", "reconcile", "admin-delete", "teardown"} {
		t.Run(path, func(t *testing.T) {
			manager, fixture := newRedisTeardownFixture(t, "extra-network")
			var err error
			switch path {
			case "publication":
				manager.stageRedisProvisional(fixture.spec, false)
				err = manager.PublishRedisRuntime(context.Background(), fixture.spec)
			case "reconcile":
				_, _, _, err = manager.ReconcileRedisExact(context.Background(), fixture.spec)
			case "admin-delete":
				err = manager.DeleteRedisExact(context.Background(), fixture.spec)
			case "teardown":
				err = manager.Teardown(context.Background())
			}
			if err == nil {
				t.Fatalf("%s accepted an extra foreign network attachment", path)
			}
			if fixture.containerDeletes != 0 || fixture.networkDeletes != 0 ||
				!fixture.containerPresent || !fixture.volumePresent || !fixture.networkPresent {
				t.Fatalf("%s mutated mixed-attachment resources: %#v", path, fixture)
			}
		})
	}
}

func TestRedisRuntimeRegistryIsConcurrencySafe(t *testing.T) {
	manager := &ServiceManager{}
	const workers = 64
	var wait sync.WaitGroup
	wait.Add(workers)
	for index := 0; index < workers; index++ {
		go func(index int) {
			defer wait.Done()
			resourceID := fmt.Sprintf("resource-%d", index)
			manager.registerRedisRuntime(redisRuntimeIdentity{
				Spec: RedisBackendSpec{ResourceID: resourceID},
			})
			_ = manager.redisRuntimeSnapshot()
			manager.clearRedisRuntime(resourceID)
		}(index)
	}
	wait.Wait()
	if runtimes := manager.redisRuntimeSnapshot(); len(runtimes) != 0 {
		t.Fatalf("concurrent registry operations retained identities: %#v", runtimes)
	}
}

func TestRedisMultiRuntimeTeardownIsRetryable(t *testing.T) {
	for _, mode := range []string{"second-delete-failure", "second-inspect-drift", "late-foreign"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", "redis-multi-teardown")
			t.Setenv("MINISKY_STATE_DIR", t.TempDir())
			networkID := strings.Repeat("n", 64)
			fixtures := []*redisTeardownFixture{
				multiRedisTeardownFixture("a-resource", strings.Repeat("a", 64), networkID),
				multiRedisTeardownFixture("b-resource", strings.Repeat("b", 64), networkID),
			}
			if mode == "second-inspect-drift" {
				fixtures[1].drift = "toctou"
			}
			failSecondDelete := mode == "second-delete-failure"
			lateForeign := false
			networkPresent := true
			manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
			for _, fixture := range fixtures {
				manager.registerRedisRuntime(redisRuntimeIdentity{
					Spec:             fixture.spec,
					Profile:          "redis-multi-teardown",
					ContainerName:    fixture.containerName,
					VolumeName:       fixture.volumeName,
					VolumeCreatedAt:  fixture.createdAt,
					VolumeMountpoint: fixture.mountpoint,
					NetworkID:        networkID,
					Endpoint:         "127.0.0.1:46379",
				})
			}
			manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				for index, fixture := range fixtures {
					if request.Method == http.MethodGet &&
						request.URL.Path == "/containers/"+fixture.spec.ContainerID+"/json" {
						fixture.containerInspects++
						if !fixture.containerPresent {
							return dockerResponse(http.StatusNotFound, `{}`), nil
						}
						return dockerResponse(http.StatusOK, fixture.containerJSON()), nil
					}
					if request.Method == http.MethodGet &&
						request.URL.Path == "/volumes/"+fixture.volumeName {
						return dockerResponse(http.StatusOK, fixture.volumeJSON()), nil
					}
					if request.Method == http.MethodDelete &&
						request.URL.Path == "/containers/"+fixture.spec.ContainerID {
						if index == 1 && failSecondDelete {
							return dockerResponse(http.StatusInternalServerError, `{"message":"delete failed"}`), nil
						}
						fixture.containerPresent = false
						if mode == "late-foreign" && index == 0 {
							lateForeign = true
						}
						return dockerResponse(http.StatusNoContent, ""), nil
					}
				}
				switch {
				case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
					return dockerResponse(http.StatusNotFound, `{}`), nil
				case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
					return dockerResponse(http.StatusOK, fmt.Sprintf(
						`{"Id":%q,"RepoDigests":[%q],"Os":"linux","Architecture":"amd64"}`,
						fixtures[0].spec.ImageID, fixtures[0].spec.RepoDigest,
					)), nil
				case request.Method == http.MethodGet &&
					(request.URL.Path == "/networks/"+networkID || request.URL.Path == "/networks/minisky-net"):
					if !networkPresent {
						return dockerResponse(http.StatusNotFound, `{}`), nil
					}
					endpoints := map[string]map[string]string{}
					for _, fixture := range fixtures {
						if fixture.containerPresent {
							endpoints[fixture.spec.ContainerID] = map[string]string{"Name": fixture.containerName}
						}
					}
					if lateForeign {
						endpoints[strings.Repeat("f", 64)] = map[string]string{"Name": "foreign"}
					}
					encoded, _ := json.Marshal(endpoints)
					return dockerResponse(http.StatusOK, fmt.Sprintf(
						`{"Id":%q,"Labels":{"managed-by":"minisky","minisky.profile":"redis-multi-teardown"},"Containers":%s}`,
						networkID, encoded,
					)), nil
				case request.Method == http.MethodDelete && request.URL.Path == "/networks/"+networkID:
					networkPresent = false
					return dockerResponse(http.StatusNoContent, ""), nil
				default:
					t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
					return nil, nil
				}
			})}

			if err := manager.Teardown(context.Background()); err == nil {
				t.Fatal("first partial teardown unexpectedly succeeded")
			}
			if fixtures[0].containerPresent {
				t.Fatal("first prevalidated exact runtime was not removed")
			}
			if !networkPresent {
				t.Fatal("partial teardown removed the owned network")
			}
			if mode == "late-foreign" {
				if fixtures[1].containerPresent || len(manager.redisRuntimeSnapshot()) != 0 {
					t.Fatal("late foreign endpoint prevented clearing successfully removed exact runtimes")
				}
				return
			}
			if !fixtures[1].containerPresent || len(manager.redisRuntimeSnapshot()) != 1 {
				t.Fatalf("unresolved runtime state container=%t registry=%#v",
					fixtures[1].containerPresent, manager.redisRuntimeSnapshot())
			}
			failSecondDelete = false
			fixtures[1].drift = ""
			if err := manager.Teardown(context.Background()); err != nil {
				t.Fatalf("retry teardown: %v", err)
			}
			if fixtures[1].containerPresent || networkPresent || len(manager.redisRuntimeSnapshot()) != 0 {
				t.Fatalf("retry did not finish exact teardown: second=%t network=%t registry=%#v",
					fixtures[1].containerPresent, networkPresent, manager.redisRuntimeSnapshot())
			}
		})
	}
}

func multiRedisTeardownFixture(resourceID, containerID, networkID string) *redisTeardownFixture {
	spec := Redis72BackendSpec(resourceID)
	spec.ImageID = "sha256:c86d9fab6d31c4b40bc4f1e4c5f41ce66c9b4f904f48873812f30b93424ab408"
	spec.VolumeIdentity = "sha256:" + strings.Repeat(resourceID[:1], 64)
	spec.VolumeProvenance = "sha256:" + strings.Repeat("e", 64)
	spec.ContainerIdentity = "sha256:" + strings.Repeat("d", 64)
	spec.ContainerID = containerID
	spec.Generation = 3
	containerName, volumeName := redisDockerNames(resourceID)
	fixture := &redisTeardownFixture{
		spec:             spec,
		containerName:    containerName,
		volumeName:       volumeName,
		networkID:        networkID,
		createdAt:        "2026-07-29T11:00:00Z",
		mountpoint:       "/var/lib/docker/volumes/" + volumeName + "/_data",
		containerPresent: true,
		volumePresent:    true,
		networkPresent:   true,
	}
	fixture.spec.VolumeIdentity, _ = (redisVolumeInspect{
		Name:       volumeName,
		CreatedAt:  fixture.createdAt,
		Mountpoint: fixture.mountpoint,
	}).identity()
	return fixture
}

func TestRedisGracefulTeardownRemovesEmptyOwnedNetwork(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-empty-network")
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	deleted := false
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks/minisky-net":
			return dockerResponse(http.StatusOK,
				`{"Id":"empty-network-id","Labels":{"managed-by":"minisky","minisky.profile":"redis-empty-network"},"Containers":{}}`), nil
		case request.Method == http.MethodDelete && request.URL.Path == "/networks/empty-network-id":
			deleted = true
			return dockerResponse(http.StatusNoContent, ""), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}
	if err := manager.Teardown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("zero-endpoint owned network was not removed")
	}
}
