//go:build !windows

package cloudsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const (
	cloudSQLDockerIntegrationOptIn       = "MINISKY_DOCKER_CLOUDSQL_INTEGRATION"
	cloudSQLRestartIntegrationOptInAlias = "MINISKY_CLOUDSQL_RESTART_INTEGRATION"
)

func TestCloudSQLRestartReconciliationLiveDocker(t *testing.T) {
	if !cloudSQLLiveIntegrationEnabled(os.Getenv) {
		t.Skip("set MINISKY_DOCKER_CLOUDSQL_INTEGRATION=1 (Make/CI) or MINISKY_CLOUDSQL_RESTART_INTEGRATION=1 to run")
	}
	acquireCloudSQLDockerIntegrationLock(t)

	profile := fmt.Sprintf("cloudsql-restart-%d", time.Now().UnixNano())
	const project = "restart-evidence"
	const instance = "postgres16"
	key := instanceKey(project, instance)
	names := cloudSQLIntegrationNamesFor(profile, project, instance)
	t.Setenv("MINISKY_PROFILE", profile)

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	manager, err := orchestrator.NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	docker := newCloudSQLIntegrationDockerClient(manager)
	if err := docker.ping(ctx); err != nil {
		t.Fatalf("Docker daemon unavailable after explicit Cloud SQL live opt-in: %v", err)
	}
	identityLabels := cloudSQLIntegrationIdentityLabels(profile, project, instance)
	if err := docker.assertIdentityAbsent(ctx, names, identityLabels); err != nil {
		t.Fatalf("Docker inventory cannot be verified; refusing mutation: %v", err)
	}
	networkLabels := orchestrator.DockerOwnershipLabels()
	if available, owner, err := docker.networkAvailable(ctx, "minisky-net", networkLabels); err != nil {
		t.Fatalf("Docker network inventory cannot be verified; refusing mutation: %v", err)
	} else if !available {
		t.Fatalf("shared minisky-net is owned by unrelated profile %q", owner)
	}

	sentinelName := "minisky-cloudsql-sentinel-" + fmt.Sprint(time.Now().UnixNano())
	sentinelLabels := map[string]string{
		"managed-by":      "cloudsql-integration-test",
		"minisky.profile": profile,
		"minisky.test":    "restart-reconciliation-sentinel",
	}
	stateRoot := t.TempDir()
	store, err := state.New(stateRoot, profile)
	if err != nil {
		t.Fatal(err)
	}
	var cleanupRuntime *cloudSQLRuntimeProvenance
	cleanupNetwork := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cleanupCancel()
		if err := cleanupCloudSQLIntegrationResources(
			cleanupCtx, docker, manager, store, key, names, identityLabels, cleanupRuntime,
		); err != nil {
			t.Errorf("cleanup exact-owned Cloud SQL resources: %v", err)
		}
		if err := docker.removeExactVolume(cleanupCtx, sentinelName, sentinelLabels); err != nil {
			t.Errorf("cleanup unrelated sentinel volume: %v", err)
		}
		if cleanupNetwork {
			if err := docker.removeExactNetwork(cleanupCtx, "minisky-net", networkLabels); err != nil {
				t.Errorf("cleanup exact-owned integration network: %v", err)
			}
		}
	})

	if err := docker.createCloudSQLSentinelVolume(ctx, sentinelName, sentinelLabels); err != nil {
		t.Fatal(err)
	}
	if err := manager.EnsureNetwork(ctx); err != nil {
		t.Fatalf("collision-safe network setup failed: %v", err)
	}
	cleanupNetwork = true

	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), manager, store)
	if err != nil {
		t.Fatal(err)
	}
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/"+project+"/instances",
		strings.NewReader(`{"name":"postgres16","databaseVersion":"POSTGRES_16","region":"us-central1","settings":{"tier":"db-f1-micro"}}`),
	))
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var createOperation SqlOperation
	if err := json.Unmarshal(create.Body.Bytes(), &createOperation); err != nil {
		t.Fatal(err)
	}
	if operation, err := waitCloudSQLIntegrationOperation(ctx, api.opMgr, createOperation.Name); err != nil {
		t.Fatal(err)
	} else if operation.Error != nil {
		t.Fatalf("create operation failed: %#v", operation.Error)
	}

	createdInstance := cloudSQLIntegrationInstance(t, api, key)
	createdAddress := cloudSQLIntegrationAddress(t, createdInstance)
	runtimeBefore := loadCloudSQLIntegrationRuntime(t, store, key)
	if runtimeBefore.Profile != profile ||
		runtimeBefore.Project != project ||
		runtimeBefore.Instance != instance ||
		runtimeBefore.DatabaseVersion != "POSTGRES_16" ||
		runtimeBefore.BootstrapPolicy != cloudSQLBootstrapPolicyV1 ||
		!runtimeBefore.CreationIntent ||
		runtimeBefore.Image == "" ||
		!validCloudSQLImmutableID(runtimeBefore.ImageID) ||
		!validCloudSQLImmutableID(runtimeBefore.VolumeIdentity) ||
		!validCloudSQLOwnershipFingerprint(runtimeBefore.OwnershipFingerprint) {
		t.Fatalf("persisted exact runtime provenance=%#v", runtimeBefore)
	}
	if local, err := cloudSQLHasLocalProvenance(store, key, runtimeBefore); err != nil || !local {
		t.Fatalf("exact local runtime provenance local=%t err=%v", local, err)
	}
	cleanupRuntime = cloneCloudSQLRuntime(runtimeBefore)
	expectedLabels := cloudSQLIntegrationExpectedLabels(runtimeBefore)
	createdInventory := assertCloudSQLDockerInventory(
		t, ctx, docker, names, identityLabels, expectedLabels, runtimeBefore, true, true,
	)
	if createdInventory.Address != createdAddress {
		t.Fatalf("published address=%q Docker address=%q", createdAddress, createdInventory.Address)
	}
	if err := writeCloudSQLDurableRow(ctx, createdAddress, "survives-restart"); err != nil {
		t.Fatal(err)
	}
	if value, err := readCloudSQLDurableRow(ctx, createdAddress); err != nil || value != "survives-restart" {
		t.Fatalf("initial SQL read value=%q err=%v", value, err)
	}

	restartedManager, err := orchestrator.NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	restartedStore, err := state.New(stateRoot, profile)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), restartedManager, restartedStore)
	if err != nil {
		t.Fatal(err)
	}
	if before := cloudSQLIntegrationInstance(t, restarted, key); before.State != "SUSPENDED" ||
		before.BackendStatus != "RECONCILING" || len(before.IpAddresses) != 0 {
		t.Fatalf("pre-reconcile restart state=%#v", before)
	}
	if err := restarted.reconcileRestored(ctx); err != nil {
		t.Fatal(err)
	}
	aliveAddress := cloudSQLIntegrationAddress(t, cloudSQLIntegrationInstance(t, restarted, key))
	aliveInventory := assertCloudSQLDockerInventory(
		t, ctx, docker, names, identityLabels, expectedLabels, runtimeBefore, true, true,
	)
	if aliveInventory.ContainerID != createdInventory.ContainerID ||
		aliveInventory.VolumeIdentity != createdInventory.VolumeIdentity {
		t.Fatalf("live reconciliation duplicated or replaced Docker resources: before=%#v after=%#v",
			createdInventory, aliveInventory)
	}
	assertCloudSQLRuntimeUnchanged(t, restartedStore, key, runtimeBefore)
	if value, err := readCloudSQLDurableRow(ctx, aliveAddress); err != nil || value != "survives-restart" {
		t.Fatalf("live-container restart read value=%q err=%v", value, err)
	}

	// Boundary: exact local provenance only; no legacy recovery; no portable credentials or runtime tokens.
	assertPortableCloudSQLSnapshotIsMetadataOnly(t, ctx, restartedStore, restartedManager, key)
	boundaryInventory := assertCloudSQLDockerInventory(
		t, ctx, docker, names, identityLabels, expectedLabels, runtimeBefore, true, true,
	)
	if boundaryInventory.ContainerID != createdInventory.ContainerID {
		t.Fatal("metadata-only boundary checks replayed Docker side effects")
	}

	if err := removeExactCloudSQLContainerRetainingVolume(
		ctx, docker, names.Container, createdInventory.ContainerID, expectedLabels,
	); err != nil {
		t.Fatal(err)
	}
	assertCloudSQLDockerInventory(
		t, ctx, docker, names, identityLabels, expectedLabels, runtimeBefore, false, true,
	)

	recoveryManager, err := orchestrator.NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	recoveryStore, err := state.New(stateRoot, profile)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := NewAPIWithStore(orchestrator.NewOperationManager(), recoveryManager, recoveryStore)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.reconcileRestored(ctx); err != nil {
		t.Fatal(err)
	}
	recoveredAddress := cloudSQLIntegrationAddress(t, cloudSQLIntegrationInstance(t, recovered, key))
	recoveredInventory := assertCloudSQLDockerInventory(
		t, ctx, docker, names, identityLabels, expectedLabels, runtimeBefore, true, true,
	)
	if recoveredInventory.ContainerID == createdInventory.ContainerID {
		t.Fatal("volume-only recovery did not create a replacement container")
	}
	if recoveredInventory.VolumeIdentity != runtimeBefore.VolumeIdentity {
		t.Fatalf("volume-only recovery changed persisted volume provenance: %#v", recoveredInventory)
	}
	assertCloudSQLRuntimeUnchanged(t, recoveryStore, key, runtimeBefore)
	if value, err := readCloudSQLDurableRow(ctx, recoveredAddress); err != nil || value != "survives-restart" {
		t.Fatalf("volume-only recovery read value=%q err=%v", value, err)
	}

	deleteResponse := httptest.NewRecorder()
	recovered.ServeHTTP(deleteResponse, httptest.NewRequest(
		http.MethodDelete,
		"/v1/projects/"+project+"/instances/"+instance,
		nil,
	))
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	var deleteOperation SqlOperation
	if err := json.Unmarshal(deleteResponse.Body.Bytes(), &deleteOperation); err != nil {
		t.Fatal(err)
	}
	if operation, err := waitCloudSQLIntegrationOperation(ctx, recovered.opMgr, deleteOperation.Name); err != nil {
		t.Fatal(err)
	} else if operation.Error != nil {
		t.Fatalf("delete operation failed: %#v", operation.Error)
	}
	assertCloudSQLDockerInventory(
		t, ctx, docker, names, identityLabels, expectedLabels, runtimeBefore, false, false,
	)
	if err := docker.assertExactVolume(ctx, sentinelName, sentinelLabels); err != nil {
		t.Fatalf("Cloud SQL deletion altered unrelated sentinel: %v", err)
	}
}

type cloudSQLIntegrationDockerClient struct {
	manager *orchestrator.ServiceManager
}

type cloudSQLIntegrationNames struct {
	Container string
	Volume    string
}

type cloudSQLIntegrationInventory struct {
	ContainerID    string
	Address        string
	VolumeIdentity string
}

type cloudSQLIntegrationContainerInspect struct {
	ID      string `json:"Id"`
	ImageID string `json:"Image"`
	State   struct {
		Status string `json:"Status"`
	} `json:"State"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	Mounts []struct {
		Type        string `json:"Type"`
		Name        string `json:"Name"`
		Destination string `json:"Destination"`
	} `json:"Mounts"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
	} `json:"NetworkSettings"`
}

type cloudSQLIntegrationVolumeInspect struct {
	Name       string            `json:"Name"`
	CreatedAt  string            `json:"CreatedAt"`
	Mountpoint string            `json:"Mountpoint"`
	Labels     map[string]string `json:"Labels"`
}

func TestCloudSQLIntegrationContainerProvenanceAcceptsImmutableConfigImage(t *testing.T) {
	runtime := &cloudSQLRuntimeProvenance{
		Image:   "postgres:16.13-alpine",
		ImageID: "sha256:4e6e670bb069649261c9c18031f0aded7bb249a5b6664ddec29c013a89310d50",
	}
	expectedLabels := map[string]string{"managed-by": "minisky"}
	container := &cloudSQLIntegrationContainerInspect{
		ID:      strings.Repeat("a", 64),
		ImageID: runtime.ImageID,
	}
	container.State.Status = "running"
	container.Config.Image = runtime.ImageID
	container.Config.Labels = expectedLabels

	if err := validateCloudSQLIntegrationContainerProvenance(container, expectedLabels, runtime); err != nil {
		t.Fatalf("immutable-ID container provenance rejected: %v", err)
	}
	container.Config.Image = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := validateCloudSQLIntegrationContainerProvenance(container, expectedLabels, runtime); err == nil {
		t.Fatal("foreign immutable Config.Image was accepted")
	}
	container.Config.Image = runtime.Image
	if err := validateCloudSQLIntegrationContainerProvenance(container, expectedLabels, runtime); err != nil {
		t.Fatalf("legacy exact-reference container provenance rejected: %v", err)
	}
}

func validateCloudSQLIntegrationContainerProvenance(
	container *cloudSQLIntegrationContainerInspect,
	expectedLabels map[string]string,
	runtime *cloudSQLRuntimeProvenance,
) error {
	if container == nil || runtime == nil {
		return errors.New("Cloud SQL container runtime provenance is unavailable")
	}
	if container.ID == "" {
		return errors.New("Cloud SQL container immutable ID is absent")
	}
	if container.State.Status != "running" {
		return fmt.Errorf("Cloud SQL container state is %q, want running", container.State.Status)
	}
	if container.Config.Image != runtime.Image && container.Config.Image != runtime.ImageID {
		return fmt.Errorf(
			"Cloud SQL container Config.Image %q does not match persisted reference %q or immutable image ID %q",
			container.Config.Image,
			runtime.Image,
			runtime.ImageID,
		)
	}
	if container.ImageID != runtime.ImageID {
		return fmt.Errorf(
			"Cloud SQL container image ID %q does not match persisted immutable image ID %q",
			container.ImageID,
			runtime.ImageID,
		)
	}
	if !reflect.DeepEqual(container.Config.Labels, expectedLabels) {
		return errors.New("Cloud SQL container labels do not match exact persisted provenance")
	}
	return nil
}

func newCloudSQLIntegrationDockerClient(
	manager *orchestrator.ServiceManager,
) *cloudSQLIntegrationDockerClient {
	return &cloudSQLIntegrationDockerClient{manager: manager}
}

func (docker *cloudSQLIntegrationDockerClient) request(
	ctx context.Context,
	method string,
	path string,
	body any,
) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://localhost"+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := docker.manager.DoDockerRequest(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	return response.StatusCode, payload, err
}

func (docker *cloudSQLIntegrationDockerClient) ping(ctx context.Context) error {
	status, _, err := docker.request(ctx, http.MethodGet, "/_ping", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("Docker ping returned HTTP %d", status)
	}
	return nil
}

func (docker *cloudSQLIntegrationDockerClient) container(
	ctx context.Context,
	name string,
) (*cloudSQLIntegrationContainerInspect, bool, error) {
	status, payload, err := docker.request(
		ctx, http.MethodGet, "/containers/"+url.PathEscape(name)+"/json", nil,
	)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("inspect container returned HTTP %d", status)
	}
	var inspected cloudSQLIntegrationContainerInspect
	if err := json.Unmarshal(payload, &inspected); err != nil {
		return nil, false, err
	}
	return &inspected, true, nil
}

func (docker *cloudSQLIntegrationDockerClient) volume(
	ctx context.Context,
	name string,
) (*cloudSQLIntegrationVolumeInspect, bool, error) {
	status, payload, err := docker.request(ctx, http.MethodGet, "/volumes/"+url.PathEscape(name), nil)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("inspect volume returned HTTP %d", status)
	}
	var inspected cloudSQLIntegrationVolumeInspect
	if err := json.Unmarshal(payload, &inspected); err != nil {
		return nil, false, err
	}
	return &inspected, true, nil
}

func (docker *cloudSQLIntegrationDockerClient) identityCounts(
	ctx context.Context,
	labels map[string]string,
) (int, int, error) {
	filter := dockerLabelFilter(labels)
	status, payload, err := docker.request(
		ctx, http.MethodGet, "/containers/json?all=true&filters="+filter, nil,
	)
	if err != nil || status != http.StatusOK {
		return 0, 0, fmt.Errorf("list identity containers returned HTTP %d: %w", status, err)
	}
	var containers []json.RawMessage
	if err := json.Unmarshal(payload, &containers); err != nil {
		return 0, 0, err
	}
	status, payload, err = docker.request(ctx, http.MethodGet, "/volumes?filters="+filter, nil)
	if err != nil || status != http.StatusOK {
		return 0, 0, fmt.Errorf("list identity volumes returned HTTP %d: %w", status, err)
	}
	var volumes struct {
		Volumes []json.RawMessage `json:"Volumes"`
	}
	if err := json.Unmarshal(payload, &volumes); err != nil {
		return 0, 0, err
	}
	return len(containers), len(volumes.Volumes), nil
}

func (docker *cloudSQLIntegrationDockerClient) assertIdentityAbsent(
	ctx context.Context,
	names cloudSQLIntegrationNames,
	labels map[string]string,
) error {
	containers, volumes, err := docker.identityCounts(ctx, labels)
	if err != nil {
		return err
	}
	container, containerFound, err := docker.container(ctx, names.Container)
	if err != nil {
		return err
	}
	volume, volumeFound, err := docker.volume(ctx, names.Volume)
	if err != nil {
		return err
	}
	if containers != 0 || volumes != 0 || containerFound || volumeFound {
		return fmt.Errorf("unique identity collides with containers=%d volumes=%d container=%#v volume=%#v",
			containers, volumes, container, volume)
	}
	return nil
}

func (docker *cloudSQLIntegrationDockerClient) inventory(
	ctx context.Context,
	names cloudSQLIntegrationNames,
	identityLabels map[string]string,
	expectedLabels map[string]string,
	runtime *cloudSQLRuntimeProvenance,
	wantContainer bool,
	wantVolume bool,
) (cloudSQLIntegrationInventory, error) {
	containerCount, volumeCount, err := docker.identityCounts(ctx, identityLabels)
	if err != nil {
		return cloudSQLIntegrationInventory{}, err
	}
	if containerCount != boolInt(wantContainer) || volumeCount != boolInt(wantVolume) {
		return cloudSQLIntegrationInventory{}, fmt.Errorf(
			"identity inventory containers=%d volumes=%d, want %d/%d",
			containerCount, volumeCount, boolInt(wantContainer), boolInt(wantVolume),
		)
	}
	container, containerFound, err := docker.container(ctx, names.Container)
	if err != nil {
		return cloudSQLIntegrationInventory{}, err
	}
	volume, volumeFound, err := docker.volume(ctx, names.Volume)
	if err != nil {
		return cloudSQLIntegrationInventory{}, err
	}
	if containerFound != wantContainer || volumeFound != wantVolume {
		return cloudSQLIntegrationInventory{}, fmt.Errorf(
			"exact inventory container=%t volume=%t, want %t/%t",
			containerFound, volumeFound, wantContainer, wantVolume,
		)
	}
	inventory := cloudSQLIntegrationInventory{}
	if containerFound {
		if err := validateCloudSQLIntegrationContainerProvenance(
			container,
			expectedLabels,
			runtime,
		); err != nil {
			return inventory, err
		}
		mounted := false
		for _, mount := range container.Mounts {
			if mount.Type == "volume" &&
				mount.Name == names.Volume &&
				mount.Destination == "/var/lib/postgresql/data" {
				mounted = true
			}
		}
		bindings := container.NetworkSettings.Ports["5432/tcp"]
		if !mounted ||
			len(bindings) != 1 ||
			bindings[0].HostIP != "127.0.0.1" ||
			bindings[0].HostPort == "" {
			return inventory, errors.New("Cloud SQL container mount or published port is not exact")
		}
		inventory.ContainerID = container.ID
		inventory.Address = net.JoinHostPort(bindings[0].HostIP, bindings[0].HostPort)
	}
	if volumeFound {
		if volume.Name != names.Volume ||
			volume.CreatedAt == "" ||
			volume.Mountpoint == "" ||
			!reflect.DeepEqual(volume.Labels, expectedLabels) {
			return inventory, errors.New("Cloud SQL volume provenance mismatch")
		}
		sum := sha256.Sum256([]byte(volume.Name + "\x00" + volume.CreatedAt + "\x00" + volume.Mountpoint))
		inventory.VolumeIdentity = fmt.Sprintf("sha256:%x", sum[:])
		if inventory.VolumeIdentity != runtime.VolumeIdentity {
			return inventory, errors.New("Cloud SQL volume immutable identity changed")
		}
	}
	return inventory, nil
}

func assertCloudSQLDockerInventory(
	t *testing.T,
	ctx context.Context,
	docker *cloudSQLIntegrationDockerClient,
	names cloudSQLIntegrationNames,
	identityLabels map[string]string,
	expectedLabels map[string]string,
	runtime *cloudSQLRuntimeProvenance,
	wantContainer bool,
	wantVolume bool,
) cloudSQLIntegrationInventory {
	t.Helper()
	inventory, err := docker.inventory(
		ctx, names, identityLabels, expectedLabels, runtime, wantContainer, wantVolume,
	)
	if err != nil {
		t.Fatalf("Docker inventory cannot be verified: %v", err)
	}
	return inventory
}

func removeExactCloudSQLContainerRetainingVolume(
	ctx context.Context,
	docker *cloudSQLIntegrationDockerClient,
	name string,
	id string,
	expectedLabels map[string]string,
) error {
	container, found, err := docker.container(ctx, name)
	if err != nil {
		return fmt.Errorf("inventory unavailable; refusing container removal: %w", err)
	}
	if !found || container.ID != id || !reflect.DeepEqual(container.Config.Labels, expectedLabels) {
		return errors.New("refusing to remove Cloud SQL container without exact ownership")
	}
	status, payload, err := docker.request(
		ctx, http.MethodDelete, "/containers/"+url.PathEscape(id)+"?force=true&v=false", nil,
	)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("remove exact Cloud SQL container returned HTTP %d: %s", status, payload)
	}
	return nil
}

func cleanupCloudSQLIntegrationResources(
	ctx context.Context,
	docker *cloudSQLIntegrationDockerClient,
	manager *orchestrator.ServiceManager,
	store *state.Store,
	key string,
	names cloudSQLIntegrationNames,
	identityLabels map[string]string,
	fallback *cloudSQLRuntimeProvenance,
) error {
	container, containerFound, err := docker.container(ctx, names.Container)
	if err != nil {
		return fmt.Errorf("Docker inventory unavailable; refusing cleanup: %w", err)
	}
	volume, volumeFound, err := docker.volume(ctx, names.Volume)
	if err != nil {
		return fmt.Errorf("Docker inventory unavailable; refusing cleanup: %w", err)
	}
	if !containerFound && !volumeFound {
		return nil
	}
	runtime := cloneCloudSQLRuntime(fallback)
	var persisted cloudSQLMetadata
	if loadErr := store.Load(cloudSQLStateEntry, &persisted); loadErr == nil && persisted.Runtimes[key] != nil {
		runtime = cloneCloudSQLRuntime(persisted.Runtimes[key])
	}
	if runtime == nil {
		return errors.New("exact runtime provenance unavailable; refusing cleanup")
	}
	expectedLabels := cloudSQLIntegrationExpectedLabels(runtime)
	if containerFound && !reflect.DeepEqual(container.Config.Labels, expectedLabels) {
		return errors.New("container ownership mismatch; refusing cleanup")
	}
	if volumeFound && !reflect.DeepEqual(volume.Labels, expectedLabels) {
		return errors.New("volume ownership mismatch; refusing cleanup")
	}
	if _, err := docker.inventory(
		ctx, names, identityLabels, expectedLabels, runtime, containerFound, volumeFound,
	); err != nil {
		return fmt.Errorf("exact inventory validation failed; refusing cleanup: %w", err)
	}
	if err := manager.DeleteCloudSQLVMContext(ctx, orchestrator.CloudSQLBackendSpec{
		Project:              runtime.Project,
		Instance:             runtime.Instance,
		DatabaseVersion:      runtime.DatabaseVersion,
		OwnershipFingerprint: runtime.OwnershipFingerprint,
		BootstrapPolicy:      runtime.BootstrapPolicy,
		Image:                runtime.Image,
		ImageID:              runtime.ImageID,
		VolumeIdentity:       runtime.VolumeIdentity,
		CreationIntent:       runtime.CreationIntent,
	}); err != nil {
		return err
	}
	if err := docker.assertIdentityAbsent(ctx, names, identityLabels); err != nil {
		return fmt.Errorf("verify exact cleanup: %w", err)
	}
	return nil
}

func (docker *cloudSQLIntegrationDockerClient) createCloudSQLSentinelVolume(
	ctx context.Context,
	name string,
	labels map[string]string,
) error {
	if _, found, err := docker.volume(ctx, name); err != nil {
		return fmt.Errorf("verify sentinel absence: %w", err)
	} else if found {
		return errors.New("refusing to replace pre-existing sentinel volume")
	}
	status, payload, err := docker.request(
		ctx, http.MethodPost, "/volumes/create", map[string]any{"Name": name, "Labels": labels},
	)
	if err != nil {
		return err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return fmt.Errorf("create sentinel volume returned HTTP %d: %s", status, payload)
	}
	return docker.assertExactVolume(ctx, name, labels)
}

func (docker *cloudSQLIntegrationDockerClient) assertExactVolume(
	ctx context.Context,
	name string,
	labels map[string]string,
) error {
	volume, found, err := docker.volume(ctx, name)
	if err != nil {
		return err
	}
	if !found || volume.Name != name || !reflect.DeepEqual(volume.Labels, labels) {
		return fmt.Errorf("volume %q is not the exact expected resource", name)
	}
	return nil
}

func (docker *cloudSQLIntegrationDockerClient) removeExactVolume(
	ctx context.Context,
	name string,
	labels map[string]string,
) error {
	volume, found, err := docker.volume(ctx, name)
	if err != nil {
		return fmt.Errorf("inventory unavailable; refusing volume cleanup: %w", err)
	}
	if !found {
		return nil
	}
	if volume.Name != name || !reflect.DeepEqual(volume.Labels, labels) {
		return fmt.Errorf("refusing inexact volume cleanup")
	}
	status, payload, err := docker.request(ctx, http.MethodDelete, "/volumes/"+url.PathEscape(name), nil)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent && status != http.StatusNotFound {
		return fmt.Errorf("remove exact volume returned HTTP %d: %s", status, payload)
	}
	return nil
}

func (docker *cloudSQLIntegrationDockerClient) networkAvailable(
	ctx context.Context,
	name string,
	labels map[string]string,
) (bool, string, error) {
	status, payload, err := docker.request(ctx, http.MethodGet, "/networks/"+url.PathEscape(name), nil)
	if err != nil {
		return false, "", err
	}
	if status == http.StatusNotFound {
		return true, "", nil
	}
	if status != http.StatusOK {
		return false, "", fmt.Errorf("inspect network returned HTTP %d", status)
	}
	var inspected struct {
		Labels map[string]string `json:"Labels"`
	}
	if err := json.Unmarshal(payload, &inspected); err != nil {
		return false, "", err
	}
	return reflect.DeepEqual(inspected.Labels, labels), inspected.Labels["minisky.profile"], nil
}

func (docker *cloudSQLIntegrationDockerClient) removeExactNetwork(
	ctx context.Context,
	name string,
	labels map[string]string,
) error {
	status, payload, err := docker.request(ctx, http.MethodGet, "/networks/"+url.PathEscape(name), nil)
	if err != nil {
		return fmt.Errorf("inventory unavailable; refusing network cleanup: %w", err)
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status != http.StatusOK {
		return fmt.Errorf("inspect network returned HTTP %d", status)
	}
	var inspected struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.Unmarshal(payload, &inspected); err != nil {
		return err
	}
	if inspected.ID == "" || !reflect.DeepEqual(inspected.Labels, labels) {
		return fmt.Errorf("refusing to remove inexact network %q", name)
	}
	status, payload, err = docker.request(
		ctx, http.MethodDelete, "/networks/"+url.PathEscape(inspected.ID), nil,
	)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent && status != http.StatusNotFound {
		return fmt.Errorf("remove exact network returned HTTP %d: %s", status, payload)
	}
	return nil
}

func assertPortableCloudSQLSnapshotIsMetadataOnly(
	t *testing.T,
	ctx context.Context,
	store *state.Store,
	manager *orchestrator.ServiceManager,
	key string,
) {
	t.Helper()
	var snapshot bytes.Buffer
	if err := store.Export(&snapshot); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(snapshot.Bytes(), []byte(`"password"`)) ||
		bytes.Contains(snapshot.Bytes(), []byte(cloudSQLLocalRuntimeDir)) {
		t.Fatalf("portable snapshot contains credentials or local runtime authorization: %s", snapshot.Bytes())
	}
	profile := store.Profile()
	imported, err := state.New(t.TempDir(), profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := imported.Import(bytes.NewReader(snapshot.Bytes())); err != nil {
		t.Fatal(err)
	}
	importedAPI, err := NewAPIWithStore(orchestrator.NewOperationManager(), manager, imported)
	if err != nil {
		t.Fatal(err)
	}
	if err := importedAPI.reconcileRestored(ctx); err != nil {
		t.Fatal(err)
	}
	if instance := cloudSQLIntegrationInstance(t, importedAPI, key); instance.State != "SUSPENDED" ||
		instance.BackendStatus != metadataOnlyBackendState || len(instance.IpAddresses) != 0 {
		t.Fatalf("portable snapshot recovered a local backend: %#v", instance)
	}

	legacy, err := state.New(t.TempDir(), profile)
	if err != nil {
		t.Fatal(err)
	}
	active := cloudSQLIntegrationInstance(t, importedAPI, key)
	active.State = "RUNNABLE"
	active.BackendStatus = ""
	if err := legacy.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{key: active},
	}); err != nil {
		t.Fatal(err)
	}
	legacyAPI, err := NewAPIWithStore(orchestrator.NewOperationManager(), manager, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyAPI.reconcileRestored(ctx); err != nil {
		t.Fatal(err)
	}
	if instance := cloudSQLIntegrationInstance(t, legacyAPI, key); instance.State != "SUSPENDED" ||
		instance.BackendStatus != metadataOnlyBackendState || len(instance.IpAddresses) != 0 {
		t.Fatalf("legacy metadata recovered a local backend: %#v", instance)
	}
}

func waitCloudSQLIntegrationOperation(
	ctx context.Context,
	manager *orchestrator.OperationManager,
	name string,
) (*orchestrator.Operation, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if operation := manager.Get(name); operation != nil && operation.Done {
			return operation, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("operation %q did not finish: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func cloudSQLIntegrationInstance(t *testing.T, api *API, key string) *DatabaseInstance {
	t.Helper()
	api.mu.RLock()
	defer api.mu.RUnlock()
	instance := cloneDatabaseInstance(api.instances[key])
	if instance == nil {
		t.Fatalf("Cloud SQL instance %q is absent", key)
	}
	return instance
}

func cloudSQLIntegrationAddress(t *testing.T, instance *DatabaseInstance) string {
	t.Helper()
	if instance.State != "RUNNABLE" || len(instance.IpAddresses) != 1 {
		t.Fatalf("Cloud SQL instance is not runnable: %#v", instance)
	}
	address := instance.IpAddresses[0].IpAddress
	host, port, err := net.SplitHostPort(address)
	parsed := net.ParseIP(host)
	if err != nil || parsed == nil || !parsed.IsLoopback() || port == "" {
		t.Fatalf("Cloud SQL published address is not exact loopback: %q", address)
	}
	return address
}

func writeCloudSQLDurableRow(ctx context.Context, address, value string) error {
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	connection, err := pgx.Connect(queryCtx, cloudSQLIntegrationPostgresURL(address))
	if err != nil {
		return fmt.Errorf("connect through published Cloud SQL host port: %w", err)
	}
	_, queryErr := connection.Exec(queryCtx,
		`CREATE TABLE IF NOT EXISTS minisky_restart_evidence (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`)
	if queryErr == nil {
		_, queryErr = connection.Exec(queryCtx,
			`INSERT INTO minisky_restart_evidence (id, value) VALUES (1, $1)
			 ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value`, value)
	}
	return errors.Join(queryErr, connection.Close(queryCtx))
}

func readCloudSQLDurableRow(ctx context.Context, address string) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	connection, err := pgx.Connect(queryCtx, cloudSQLIntegrationPostgresURL(address))
	if err != nil {
		return "", fmt.Errorf("connect through published Cloud SQL host port: %w", err)
	}
	var value string
	queryErr := connection.QueryRow(queryCtx,
		`SELECT value FROM minisky_restart_evidence WHERE id = 1`).Scan(&value)
	return value, errors.Join(queryErr, connection.Close(queryCtx))
}

func cloudSQLIntegrationPostgresURL(address string) string {
	return (&url.URL{
		Scheme: "postgres", User: url.UserPassword("postgres", "minisky"),
		Host: address, Path: "postgres", RawQuery: "sslmode=disable",
	}).String()
}

func cloudSQLIntegrationNamesFor(profile, project, instance string) cloudSQLIntegrationNames {
	resourceID := profile + "/" + project + "/" + instance
	sum := sha256.Sum256([]byte(resourceID))
	safe := strings.ToLower(instance)
	safe = strings.Map(func(character rune) rune {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' {
			return character
		}
		return '-'
	}, safe)
	safe = strings.Trim(safe, "-")
	if len(safe) > 24 {
		safe = safe[:24]
	}
	if safe == "" {
		safe = "instance"
	}
	suffix := fmt.Sprintf("%x", sum[:6])
	return cloudSQLIntegrationNames{
		Container: "minisky-sql-" + safe + "-" + suffix,
		Volume:    "minisky-db-" + safe + "-" + suffix,
	}
}

func cloudSQLIntegrationIdentityLabels(profile, project, instance string) map[string]string {
	return map[string]string{
		"managed-by":       "minisky",
		"minisky.profile":  profile,
		"minisky.service":  "cloudsql",
		"minisky.resource": profile + "/" + project + "/" + instance,
		"minisky.project":  project,
		"minisky.instance": instance,
	}
}

func cloudSQLIntegrationExpectedLabels(runtime *cloudSQLRuntimeProvenance) map[string]string {
	labels := cloudSQLIntegrationIdentityLabels(runtime.Profile, runtime.Project, runtime.Instance)
	labels["minisky.database-version"] = runtime.DatabaseVersion
	labels["minisky.ownership"] = runtime.OwnershipFingerprint
	labels["minisky.bootstrap-policy"] = runtime.BootstrapPolicy
	labels["minisky.image"] = runtime.Image
	labels["minisky.image-id"] = runtime.ImageID
	labels["minisky.creation-intent"] = fmt.Sprint(runtime.CreationIntent)
	return labels
}

func dockerLabelFilter(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, key+"="+labels[key])
	}
	encoded, _ := json.Marshal(map[string][]string{"label": values})
	return url.QueryEscape(string(encoded))
}

func loadCloudSQLIntegrationRuntime(
	t *testing.T,
	store *state.Store,
	key string,
) *cloudSQLRuntimeProvenance {
	t.Helper()
	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	runtime := cloneCloudSQLRuntime(persisted.Runtimes[key])
	if runtime == nil {
		t.Fatalf("Cloud SQL runtime %q is absent", key)
	}
	return runtime
}

func assertCloudSQLRuntimeUnchanged(
	t *testing.T,
	store *state.Store,
	key string,
	want *cloudSQLRuntimeProvenance,
) {
	t.Helper()
	got := loadCloudSQLIntegrationRuntime(t, store, key)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Cloud SQL runtime provenance drifted: got=%#v want=%#v", got, want)
	}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func acquireCloudSQLDockerIntegrationLock(t *testing.T) {
	t.Helper()
	lock := filepath.Join(os.TempDir(), "minisky-net-integration.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		if os.IsExist(err) {
			t.Fatalf("another MiniSky Docker integration is active after explicit opt-in (%s)", lock)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(lock); err != nil && !os.IsNotExist(err) {
			t.Errorf("release Docker integration lock: %v", err)
		}
	})
}

func cloudSQLLiveIntegrationEnabled(getenv func(string) string) bool {
	return getenv(cloudSQLDockerIntegrationOptIn) == "1" ||
		getenv(cloudSQLRestartIntegrationOptInAlias) == "1"
}
