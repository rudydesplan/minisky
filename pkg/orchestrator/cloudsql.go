package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/config"
)

const CloudSQLBootstrapPolicyV1 = "minisky-fixed-v1"

var (
	cloudSQLOwnershipFingerprintPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
	cloudSQLImageIDPattern              = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

// CloudSQLBackendSpec is the immutable persisted contract required to inspect
// or reactivate one exact-owned local Cloud SQL data plane.
type CloudSQLBackendSpec struct {
	Project                string
	Instance               string
	DatabaseVersion        string
	OwnershipFingerprint   string
	BootstrapPolicy        string
	Image                  string
	ImageID                string
	VolumeIdentity         string
	ImageAcquisitionIntent bool
	CreationIntent         bool
}

type cloudSQLRuntimeConfig struct {
	Image         string
	ContainerPort string
	MountTarget   string
	Env           []string
}

type cloudSQLContainerInspect struct {
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

type cloudSQLVolumeInspect struct {
	Name       string            `json:"Name"`
	CreatedAt  string            `json:"CreatedAt"`
	Mountpoint string            `json:"Mountpoint"`
	Labels     map[string]string `json:"Labels"`
}

func (volume cloudSQLVolumeInspect) identity() string {
	sum := sha256.Sum256([]byte(volume.Name + "\x00" + volume.CreatedAt + "\x00" + volume.Mountpoint))
	return fmt.Sprintf("sha256:%x", sum[:])
}

func resolveCloudSQLBackendSpec(spec CloudSQLBackendSpec) (CloudSQLBackendSpec, cloudSQLRuntimeConfig, error) {
	if spec.Project == "" || spec.Instance == "" {
		return spec, cloudSQLRuntimeConfig{}, errors.New("Cloud SQL project and instance are required")
	}
	if !cloudSQLOwnershipFingerprintPattern.MatchString(spec.OwnershipFingerprint) {
		return spec, cloudSQLRuntimeConfig{}, errors.New("Cloud SQL ownership fingerprint is invalid")
	}
	if spec.BootstrapPolicy != CloudSQLBootstrapPolicyV1 {
		return spec, cloudSQLRuntimeConfig{}, fmt.Errorf(
			"unsupported Cloud SQL bootstrap policy %q",
			spec.BootstrapPolicy,
		)
	}

	runtimeConfig, err := cloudSQLConfigForVersion(spec.DatabaseVersion)
	if err != nil {
		return spec, cloudSQLRuntimeConfig{}, err
	}
	if spec.Image == "" {
		spec.Image = runtimeConfig.Image
	} else if spec.Image != runtimeConfig.Image {
		return spec, cloudSQLRuntimeConfig{}, fmt.Errorf(
			"Cloud SQL persisted image %q is incompatible with database version %q",
			spec.Image,
			spec.DatabaseVersion,
		)
	}
	if spec.Image == "" {
		return spec, cloudSQLRuntimeConfig{}, errors.New("Cloud SQL persisted image is required")
	}
	if spec.ImageID != "" && !cloudSQLImageIDPattern.MatchString(spec.ImageID) {
		return spec, cloudSQLRuntimeConfig{}, errors.New("Cloud SQL immutable image ID is invalid")
	}
	if spec.VolumeIdentity != "" && !cloudSQLImageIDPattern.MatchString(spec.VolumeIdentity) {
		return spec, cloudSQLRuntimeConfig{}, errors.New("Cloud SQL immutable volume identity is invalid")
	}
	runtimeConfig.Image = spec.Image
	return spec, runtimeConfig, nil
}

func cloudSQLConfigForVersion(version string) (cloudSQLRuntimeConfig, error) {
	registry := config.GetImageRegistry()
	var runtimeConfig cloudSQLRuntimeConfig
	engine := ""
	target := ""
	switch version {
	case "POSTGRES_16":
		engine, target = "postgres", "16"
	case "POSTGRES_17":
		engine, target = "postgres", "17"
	case "POSTGRES_18":
		engine, target = "postgres", "18"
	case "MYSQL_8_0":
		engine, target = "mysql", "8.0"
	case "MYSQL_8_4":
		engine, target = "mysql", "8.4"
	case "MYSQL_9_0":
		engine, target = "mysql", "9.0"
	default:
		return cloudSQLRuntimeConfig{}, fmt.Errorf("unsupported database version: %s", version)
	}
	switch engine {
	case "postgres":
		for _, candidate := range registry.Sql.Postgres.Versions {
			if candidate.Version == target {
				runtimeConfig.Image = candidate.Image
				break
			}
		}
		runtimeConfig.ContainerPort = "5432/tcp"
		runtimeConfig.MountTarget = "/var/lib/postgresql/data"
		runtimeConfig.Env = append([]string(nil), "POSTGRES_PASSWORD=minisky", "PGDATA=/var/lib/postgresql/data")
	case "mysql":
		for _, candidate := range registry.Sql.Mysql.Versions {
			if candidate.Version == target {
				runtimeConfig.Image = candidate.Image
				break
			}
		}
		runtimeConfig.ContainerPort = "3306/tcp"
		runtimeConfig.MountTarget = "/var/lib/mysql"
		runtimeConfig.Env = []string{"MYSQL_ROOT_PASSWORD=minisky"}
	}
	if runtimeConfig.Image == "" {
		return cloudSQLRuntimeConfig{}, fmt.Errorf(
			"database version %q has no exact configured engine image",
			version,
		)
	}
	return runtimeConfig, nil
}

// CloudSQLImageForDatabaseVersion returns the exact configured engine image
// for one explicitly supported Cloud SQL database version.
func CloudSQLImageForDatabaseVersion(version string) (string, error) {
	runtimeConfig, err := cloudSQLConfigForVersion(version)
	if err != nil {
		return "", err
	}
	return runtimeConfig.Image, nil
}

// PrepareCloudSQLVM acquires the exact configured image when necessary,
// resolves its immutable identity, and records creation intent without creating
// a container or volume. ImageAcquisitionIntent must already be durably
// authorized by the caller, and the returned spec must be durably persisted
// before ProvisionCloudSQLVM.
func (sm *ServiceManager) PrepareCloudSQLVM(
	ctx context.Context,
	requested CloudSQLBackendSpec,
) (CloudSQLBackendSpec, error) {
	spec, _, err := resolveCloudSQLBackendSpec(requested)
	if err != nil {
		return requested, err
	}
	if !requested.ImageAcquisitionIntent {
		return spec, errors.New("Cloud SQL image acquisition requires durable generation-scoped intent")
	}
	containerName, _, _ := cloudSQLDockerNames(spec.Project, spec.Instance)
	status, _, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return spec, fmt.Errorf("inspect Cloud SQL container before preparation: %w", err)
	}
	if status != "not_found" {
		return spec, fmt.Errorf("Cloud SQL container %q already exists", containerName)
	}
	exists, err := sm.imageExistsContext(ctx, spec.Image)
	if err != nil {
		return spec, fmt.Errorf("inspect Cloud SQL image %q: %w", spec.Image, err)
	}
	if !exists {
		if err := sm.pullImageInternal(ctx, spec.Image); err != nil {
			return spec, fmt.Errorf("pull Cloud SQL image %q: %w", spec.Image, err)
		}
	}
	spec.ImageID, err = sm.inspectCloudSQLImageID(ctx, spec.Image)
	if err != nil {
		return spec, err
	}
	spec.CreationIntent = true
	return spec, nil
}

func cloudSQLLabels(spec CloudSQLBackendSpec) map[string]string {
	_, _, resourceID := cloudSQLDockerNames(spec.Project, spec.Instance)
	return map[string]string{
		"managed-by":               "minisky",
		"minisky.profile":          config.GetProfile(),
		"minisky.service":          "cloudsql",
		"minisky.resource":         resourceID,
		"minisky.project":          spec.Project,
		"minisky.instance":         spec.Instance,
		"minisky.database-version": spec.DatabaseVersion,
		"minisky.ownership":        spec.OwnershipFingerprint,
		"minisky.bootstrap-policy": spec.BootstrapPolicy,
		"minisky.image":            spec.Image,
		"minisky.image-id":         spec.ImageID,
		"minisky.creation-intent":  strconv.FormatBool(spec.CreationIntent),
	}
}

// ReconcileCloudSQLVM returns an endpoint only for an exact-owned,
// version-compatible, protocol-ready backend. It may start an exact compatible
// stopped container, or recreate a missing container around the existing exact
// volume when the persisted bootstrap contract is complete.
func (sm *ServiceManager) ReconcileCloudSQLVM(
	ctx context.Context,
	persisted CloudSQLBackendSpec,
) (string, error) {
	endpoint, _, err := sm.ReconcileCloudSQLVMResolved(ctx, persisted)
	return endpoint, err
}

// ReconcileCloudSQLVMResolved also returns immutable identities discovered
// while recovering a durably recorded, interrupted create.
func (sm *ServiceManager) ReconcileCloudSQLVMResolved(
	ctx context.Context,
	persisted CloudSQLBackendSpec,
) (string, CloudSQLBackendSpec, error) {
	spec, runtimeConfig, err := resolveCloudSQLBackendSpec(persisted)
	if err != nil {
		return "", persisted, err
	}
	if persisted.Image == "" {
		return "", spec, errors.New("Cloud SQL persisted image is required for restart reconciliation")
	}
	if persisted.ImageID == "" {
		return "", spec, errors.New("Cloud SQL immutable image ID is required for restart reconciliation")
	}
	if persisted.VolumeIdentity == "" && !persisted.CreationIntent {
		return "", spec, errors.New("Cloud SQL immutable volume identity is required for restart reconciliation")
	}
	containerName, volumeName, _ := cloudSQLDockerNames(spec.Project, spec.Instance)
	expectedLabels := cloudSQLLabels(spec)

	container, found, err := sm.inspectCloudSQLContainer(ctx, containerName)
	if err != nil {
		return "", spec, fmt.Errorf("inspect Cloud SQL container: %w", err)
	}
	volume, volumeFound, err := sm.inspectExactCloudSQLVolume(ctx, volumeName, expectedLabels)
	if err != nil {
		return "", spec, err
	}
	if volumeFound && spec.VolumeIdentity == "" && spec.CreationIntent {
		spec.VolumeIdentity = volume.identity()
	}
	if volumeFound && volume.identity() != spec.VolumeIdentity {
		return "", spec, fmt.Errorf("exact Cloud SQL volume %q immutable identity changed", volumeName)
	}
	if found {
		if !volumeFound {
			return "", spec, fmt.Errorf("exact Cloud SQL volume %q is missing", volumeName)
		}
		endpoint, err := sm.activateCloudSQLContainer(
			ctx,
			container,
			spec,
			runtimeConfig,
			volumeName,
			expectedLabels,
		)
		return endpoint, spec, err
	}
	if !volumeFound {
		return "", spec, fmt.Errorf("Cloud SQL backend %q is absent", containerName)
	}
	endpoint, err := sm.recreateCloudSQLContainer(
		ctx,
		containerName,
		volumeName,
		spec,
		runtimeConfig,
		expectedLabels,
		volume.identity(),
	)
	return endpoint, spec, err
}

func (sm *ServiceManager) activateCloudSQLContainer(
	ctx context.Context,
	container cloudSQLContainerInspect,
	spec CloudSQLBackendSpec,
	runtimeConfig cloudSQLRuntimeConfig,
	volumeName string,
	expectedLabels map[string]string,
) (string, error) {
	if err := validateCloudSQLContainer(
		container,
		runtimeConfig,
		volumeName,
		expectedLabels,
	); err != nil {
		return "", err
	}
	switch container.State.Status {
	case "created", "exited":
		if err := sm.startContainerContext(ctx, container.ID); err != nil {
			return "", fmt.Errorf("start exact-owned Cloud SQL container: %w", err)
		}
		var found bool
		var err error
		container, found, err = sm.inspectCloudSQLContainer(ctx, container.ID)
		if err != nil {
			return "", fmt.Errorf("inspect restarted Cloud SQL container: %w", err)
		}
		if !found {
			return "", errors.New("exact-owned Cloud SQL container disappeared during restart")
		}
		if err := validateCloudSQLContainer(
			container,
			runtimeConfig,
			volumeName,
			expectedLabels,
		); err != nil {
			return "", err
		}
	case "running":
	default:
		return "", fmt.Errorf("exact-owned Cloud SQL container is %s", container.State.Status)
	}
	if container.State.Status != "running" {
		return "", fmt.Errorf(
			"exact-owned Cloud SQL container is %s after start",
			container.State.Status,
		)
	}
	endpoint, err := cloudSQLEndpoint(container, runtimeConfig.ContainerPort)
	if err != nil {
		return "", err
	}
	ready := sm.cloudSQLReady
	if ready == nil {
		ready = waitUntilCloudSQLReady
	}
	address := strings.TrimPrefix(endpoint, "http://")
	if err := ready(ctx, address, spec.DatabaseVersion, 30*time.Second); err != nil {
		return "", fmt.Errorf("Cloud SQL readiness failed: %w", err)
	}
	authReady := sm.cloudSQLAuthReady
	if authReady == nil {
		authReady = sm.waitUntilCloudSQLAuthenticatedReady
	}
	if err := authReady(
		ctx,
		container.ID,
		expectedLabels,
		spec.DatabaseVersion,
		30*time.Second,
	); err != nil {
		return "", fmt.Errorf("Cloud SQL authenticated readiness failed: %w", err)
	}
	return endpoint, nil
}

func (sm *ServiceManager) recreateCloudSQLContainer(
	ctx context.Context,
	containerName string,
	volumeName string,
	spec CloudSQLBackendSpec,
	runtimeConfig cloudSQLRuntimeConfig,
	expectedLabels map[string]string,
	expectedVolumeIdentity string,
) (string, error) {
	imageID, err := sm.inspectCloudSQLImageID(ctx, spec.Image)
	if err != nil {
		return "", err
	}
	if imageID != spec.ImageID {
		return "", fmt.Errorf(
			"Cloud SQL image %q resolved to %q, want immutable image ID %q",
			spec.Image,
			imageID,
			spec.ImageID,
		)
	}
	payload := map[string]any{
		"Image": spec.ImageID,
		"Env":   append(sm.standardEnv(), runtimeConfig.Env...),
		"ExposedPorts": map[string]any{
			runtimeConfig.ContainerPort: struct{}{},
		},
		"HostConfig": map[string]any{
			"NetworkMode": networkName,
			"PortBindings": map[string]any{
				runtimeConfig.ContainerPort: []map[string]string{{
					"HostIp": "127.0.0.1", "HostPort": "0",
				}},
			},
			"Binds": []string{volumeName + ":" + runtimeConfig.MountTarget},
		},
		"Labels": expectedLabels,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode Cloud SQL container recreation: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(containerName),
		bytes.NewReader(encoded),
	)
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := sm.doDocker(request)
	if err != nil {
		return "", fmt.Errorf("recreate Cloud SQL container: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return "", fmt.Errorf(
			"recreate Cloud SQL container returned HTTP %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(detail)),
		)
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		return "", fmt.Errorf("decode recreated Cloud SQL container: %w", err)
	}
	if !dockerContainerIDPattern.MatchString(created.ID) {
		return "", fmt.Errorf("recreated Cloud SQL container returned invalid immutable ID %q", created.ID)
	}
	createdContainer, found, inspectErr := sm.inspectCloudSQLContainer(ctx, created.ID)
	if inspectErr != nil || !found {
		cleanupErr := sm.removeCreatedCloudSQLContainer(ctx, created.ID, expectedLabels)
		if inspectErr != nil {
			return "", errors.Join(fmt.Errorf("inspect recreated Cloud SQL container before start: %w", inspectErr), cleanupErr)
		}
		return "", errors.Join(errors.New("recreated Cloud SQL container disappeared before start"), cleanupErr)
	}
	if err := validateCloudSQLContainer(
		createdContainer,
		runtimeConfig,
		volumeName,
		expectedLabels,
	); err != nil {
		return "", errors.Join(err, sm.removeCreatedCloudSQLContainer(ctx, created.ID, expectedLabels))
	}
	volume, found, volumeErr := sm.inspectExactCloudSQLVolume(ctx, volumeName, expectedLabels)
	if volumeErr != nil || !found || volume.identity() != expectedVolumeIdentity {
		cleanupErr := sm.removeCreatedCloudSQLContainer(ctx, created.ID, expectedLabels)
		if volumeErr != nil {
			return "", errors.Join(
				fmt.Errorf("revalidate Cloud SQL volume before start: %w", volumeErr),
				cleanupErr,
			)
		}
		if !found {
			return "", errors.Join(
				errors.New("exact Cloud SQL volume disappeared before recreated container start"),
				cleanupErr,
			)
		}
		return "", errors.Join(
			errors.New("exact Cloud SQL volume immutable identity changed before recreated container start"),
			cleanupErr,
		)
	}
	if err := sm.startContainerContext(ctx, created.ID); err != nil {
		return "", fmt.Errorf("start recreated Cloud SQL container: %w", err)
	}
	container, found, err := sm.inspectCloudSQLContainer(ctx, created.ID)
	if err != nil {
		return "", fmt.Errorf("inspect recreated Cloud SQL container: %w", err)
	}
	if !found {
		return "", errors.New("recreated Cloud SQL container disappeared after start")
	}
	return sm.activateCloudSQLContainer(
		ctx,
		container,
		spec,
		runtimeConfig,
		volumeName,
		expectedLabels,
	)
}

func (sm *ServiceManager) inspectCloudSQLContainer(
	ctx context.Context,
	identity string,
) (container cloudSQLContainerInspect, found bool, result error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(identity)+"/json",
		nil,
	)
	if err != nil {
		return container, false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return container, false, err
	}
	defer func() {
		result = errors.Join(result, response.Body.Close())
	}()
	if response.StatusCode == http.StatusNotFound {
		return container, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return container, false, fmt.Errorf(
			"Docker returned HTTP %d inspecting Cloud SQL container",
			response.StatusCode,
		)
	}
	if err := json.NewDecoder(response.Body).Decode(&container); err != nil {
		return container, false, fmt.Errorf("decode Cloud SQL container inspect: %w", err)
	}
	if !dockerContainerIDPattern.MatchString(container.ID) {
		return container, false, fmt.Errorf(
			"Cloud SQL container inspect returned invalid immutable ID %q",
			container.ID,
		)
	}
	return container, true, nil
}

func (sm *ServiceManager) inspectExactCloudSQLVolume(
	ctx context.Context,
	name string,
	expectedLabels map[string]string,
) (volume cloudSQLVolumeInspect, found bool, result error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://localhost/volumes/"+url.PathEscape(name),
		nil,
	)
	if err != nil {
		return volume, false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return volume, false, fmt.Errorf("inspect Cloud SQL volume: %w", err)
	}
	defer func() {
		result = errors.Join(result, response.Body.Close())
	}()
	if response.StatusCode == http.StatusNotFound {
		return volume, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return volume, false, fmt.Errorf(
			"inspect Cloud SQL volume returned HTTP %d",
			response.StatusCode,
		)
	}
	if err := json.NewDecoder(response.Body).Decode(&volume); err != nil {
		return volume, false, fmt.Errorf("decode Cloud SQL volume ownership: %w", err)
	}
	if volume.Name != name || volume.CreatedAt == "" || volume.Mountpoint == "" {
		return volume, false, fmt.Errorf("Cloud SQL volume %q has incomplete immutable identity", name)
	}
	if !exactLabels(volume.Labels, expectedLabels) {
		return volume, false, fmt.Errorf(
			"%w: Cloud SQL volume %q",
			ErrDockerOwnershipConflict,
			name,
		)
	}
	return volume, true, nil
}

func (sm *ServiceManager) inspectCloudSQLImageID(ctx context.Context, image string) (string, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://localhost/images/"+url.PathEscape(image)+"/json",
		nil,
	)
	if err != nil {
		return "", err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return "", fmt.Errorf("inspect Cloud SQL image identity: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("inspect Cloud SQL image identity returned HTTP %d", response.StatusCode)
	}
	var imageInspect struct {
		ID          string   `json:"Id"`
		RepoDigests []string `json:"RepoDigests"`
	}
	if err := json.NewDecoder(response.Body).Decode(&imageInspect); err != nil {
		return "", fmt.Errorf("decode Cloud SQL image identity: %w", err)
	}
	if !cloudSQLImageIDPattern.MatchString(imageInspect.ID) {
		return "", fmt.Errorf("Cloud SQL image inspect returned invalid immutable ID %q", imageInspect.ID)
	}
	if strings.Contains(image, "@sha256:") && !slices.Contains(imageInspect.RepoDigests, image) {
		return "", fmt.Errorf(
			"Cloud SQL image %q inspect did not confirm the pinned repository digest",
			image,
		)
	}
	return imageInspect.ID, nil
}

func (sm *ServiceManager) removeCreatedCloudSQLContainer(
	ctx context.Context,
	id string,
	expectedLabels map[string]string,
) error {
	container, found, err := sm.inspectCloudSQLContainer(ctx, id)
	if err != nil {
		return fmt.Errorf("inspect failed Cloud SQL container cleanup target: %w", err)
	}
	if !found {
		return nil
	}
	if container.State.Status != "created" || !exactLabels(container.Config.Labels, expectedLabels) {
		return errors.New("refusing to remove Cloud SQL container not proven newly-created and stopped")
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		"http://localhost/containers/"+url.PathEscape(id),
		nil,
	)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return fmt.Errorf("remove failed Cloud SQL recreated container: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("remove failed Cloud SQL recreated container returned HTTP %d", response.StatusCode)
	}
	return nil
}

func validateCloudSQLContainer(
	container cloudSQLContainerInspect,
	runtimeConfig cloudSQLRuntimeConfig,
	volumeName string,
	expectedLabels map[string]string,
) error {
	if !exactLabels(container.Config.Labels, expectedLabels) {
		return fmt.Errorf(
			"%w: Cloud SQL container has mismatched ownership",
			ErrDockerOwnershipConflict,
		)
	}
	expectedImageID := expectedLabels["minisky.image-id"]
	if container.Config.Image != runtimeConfig.Image && container.Config.Image != expectedImageID {
		return fmt.Errorf(
			"exact-owned Cloud SQL container image %q does not match persisted image %q or immutable ID %q",
			container.Config.Image,
			runtimeConfig.Image,
			expectedImageID,
		)
	}
	if container.ImageID != expectedImageID {
		return fmt.Errorf(
			"exact-owned Cloud SQL container image ID %q does not match persisted immutable image ID %q",
			container.ImageID,
			expectedImageID,
		)
	}
	if len(container.Mounts) != 1 ||
		container.Mounts[0].Type != "volume" ||
		container.Mounts[0].Name != volumeName ||
		container.Mounts[0].Destination != runtimeConfig.MountTarget {
		return errors.New("exact-owned Cloud SQL container volume mount does not match persisted runtime")
	}
	return nil
}

func cloudSQLEndpoint(container cloudSQLContainerInspect, containerPort string) (string, error) {
	bindings := container.NetworkSettings.Ports[containerPort]
	if len(bindings) != 1 {
		return "", fmt.Errorf(
			"Cloud SQL container has %d host bindings for %s, want exactly one",
			len(bindings),
			containerPort,
		)
	}
	host := net.ParseIP(bindings[0].HostIP)
	if host == nil || !host.IsLoopback() {
		return "", fmt.Errorf("Cloud SQL host binding %q is not loopback", bindings[0].HostIP)
	}
	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("Cloud SQL host port %q is invalid", bindings[0].HostPort)
	}
	return "http://" + net.JoinHostPort(bindings[0].HostIP, strconv.Itoa(port)), nil
}

func waitUntilCloudSQLReady(
	ctx context.Context,
	endpoint string,
	databaseVersion string,
	timeout time.Duration,
) error {
	switch {
	case strings.HasPrefix(databaseVersion, "POSTGRES"):
		return waitUntilPostgresHandshakeReady(ctx, endpoint, timeout)
	case strings.HasPrefix(databaseVersion, "MYSQL"):
		return waitUntilMySQLReady(ctx, endpoint, timeout)
	default:
		return fmt.Errorf("unsupported Cloud SQL database version %q", databaseVersion)
	}
}

func waitUntilPostgresHandshakeReady(ctx context.Context, endpoint string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	parameters := []byte("user\x00postgres\x00database\x00postgres\x00\x00")
	packet := make([]byte, 8+len(parameters))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(packet[4:8], 196608)
	copy(packet[8:], parameters)
	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	for {
		connection, err := dialer.DialContext(waitCtx, "tcp", endpoint)
		if err == nil {
			deadline := time.Now().Add(500 * time.Millisecond)
			if contextDeadline, ok := waitCtx.Deadline(); ok && contextDeadline.Before(deadline) {
				deadline = contextDeadline
			}
			_ = connection.SetDeadline(deadline)
			if _, err = connection.Write(packet); err == nil {
				header := make([]byte, 5)
				if _, err = io.ReadFull(connection, header); err == nil {
					length := int(binary.BigEndian.Uint32(header[1:]))
					if header[0] == 'R' && length >= 8 && length <= 1<<20 {
						_ = connection.Close()
						return nil
					}
				}
			}
			_ = connection.Close()
		}
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf(
				"PostgreSQL at %q not protocol-ready after %s: %w",
				endpoint,
				timeout,
				waitCtx.Err(),
			)
		case <-timer.C:
		}
	}
}

func waitUntilMySQLReady(ctx context.Context, endpoint string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	for {
		connection, err := dialer.DialContext(waitCtx, "tcp", endpoint)
		if err == nil {
			deadline := time.Now().Add(500 * time.Millisecond)
			if contextDeadline, ok := waitCtx.Deadline(); ok && contextDeadline.Before(deadline) {
				deadline = contextDeadline
			}
			_ = connection.SetDeadline(deadline)
			reader := bufio.NewReader(io.LimitReader(connection, 4096))
			header := make([]byte, 4)
			if _, err = io.ReadFull(reader, header); err == nil {
				payloadLength := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
				if payloadLength > 0 && payloadLength <= 4096 {
					protocol, readErr := reader.ReadByte()
					if readErr == nil && protocol == 10 {
						_ = connection.Close()
						return nil
					}
				}
			}
			_ = connection.Close()
		}
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf(
				"MySQL at %q not protocol-ready after %s: %w",
				endpoint,
				timeout,
				waitCtx.Err(),
			)
		case <-timer.C:
		}
	}
}

func (sm *ServiceManager) waitUntilCloudSQLAuthenticatedReady(
	ctx context.Context,
	containerName string,
	expectedLabels map[string]string,
	databaseVersion string,
	timeout time.Duration,
) error {
	var command []string
	switch {
	case strings.HasPrefix(databaseVersion, "POSTGRES"):
		command = []string{
			"psql",
			"postgresql://postgres:minisky@127.0.0.1:5432/postgres",
			"-v", "ON_ERROR_STOP=1",
			"-c", "SELECT 1",
		}
	case strings.HasPrefix(databaseVersion, "MYSQL"):
		command = []string{
			"mysql",
			"-h127.0.0.1",
			"-uroot",
			"-pminisky",
			"-e", "SELECT 1",
		}
	default:
		return fmt.Errorf("unsupported Cloud SQL database version %q", databaseVersion)
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		if err := sm.runOwnedCloudSQLCommand(
			waitCtx,
			containerName,
			expectedLabels,
			command,
		); err == nil {
			return nil
		} else {
			lastErr = err
		}
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf(
				"database login probe did not succeed after %s: %w",
				timeout,
				errors.Join(waitCtx.Err(), lastErr),
			)
		case <-timer.C:
		}
	}
}
