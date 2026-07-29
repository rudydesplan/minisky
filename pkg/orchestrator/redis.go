package orchestrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/config"
)

var (
	redisImmutableIDPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	redisContainerIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

const redisAttachmentVisibilityTimeout = 5 * time.Second

// RedisBackendSpec is the private, persisted identity required to reconcile one
// exact-owned Valkey data plane. It is not part of the GCP response model.
type RedisBackendSpec struct {
	ResourceID        string `json:"resourceId"`
	Image             string `json:"image"`
	RepoDigest        string `json:"repoDigest"`
	ImageID           string `json:"imageId,omitempty"`
	Platform          string `json:"platform"`
	VolumeIdentity    string `json:"volumeIdentity,omitempty"`
	VolumeProvenance  string `json:"volumeProvenance,omitempty"`
	ContainerIdentity string `json:"containerIdentity,omitempty"`
	ContainerID       string `json:"containerId,omitempty"`
	Generation        uint64 `json:"generation,omitempty"`
	HostPort          string `json:"hostPort,omitempty"`
}

// Redis72BackendSpec returns the immutable backend identity for the only
// supported Redis compatibility path.
func Redis72BackendSpec(resourceID string) RedisBackendSpec {
	return RedisBackendSpec{
		ResourceID: resourceID,
		Image:      config.Redis72ValkeyImage,
		RepoDigest: redisRepoDigest(config.Redis72ValkeyImage),
		Platform:   config.Redis72ValkeyPlatform,
	}
}

func redisRepoDigest(image string) string {
	at := strings.LastIndex(image, "@sha256:")
	if at <= 0 {
		return ""
	}
	repository := image[:at]
	if slash := strings.LastIndex(repository, "/"); slash >= 0 {
		if colon := strings.LastIndex(repository, ":"); colon > slash {
			repository = repository[:colon]
		}
	} else if colon := strings.LastIndex(repository, ":"); colon >= 0 {
		repository = repository[:colon]
	}
	return repository + image[at:]
}

func validateRedisBackendSpec(spec RedisBackendSpec, requireResolved bool) error {
	if spec.ResourceID == "" || len(spec.ResourceID) > 256 {
		return errors.New("Redis backend resource identity is invalid")
	}
	if spec.Image != config.Redis72ValkeyImage ||
		spec.RepoDigest != redisRepoDigest(config.Redis72ValkeyImage) ||
		spec.Platform != config.Redis72ValkeyPlatform {
		return errors.New("Redis backend image identity does not match the supported REDIS_7_2 Valkey image")
	}
	if spec.ImageID != "" && !redisImmutableIDPattern.MatchString(spec.ImageID) {
		return errors.New("Redis backend image ID is invalid")
	}
	if spec.VolumeIdentity != "" && !redisImmutableIDPattern.MatchString(spec.VolumeIdentity) {
		return errors.New("Redis backend volume identity is invalid")
	}
	if spec.VolumeProvenance != "" && !redisImmutableIDPattern.MatchString(spec.VolumeProvenance) {
		return errors.New("Redis backend volume provenance is invalid")
	}
	if spec.ContainerIdentity != "" && !redisImmutableIDPattern.MatchString(spec.ContainerIdentity) {
		return errors.New("Redis backend container identity is invalid")
	}
	if spec.ContainerID != "" && !redisContainerIDPattern.MatchString(spec.ContainerID) {
		return errors.New("Redis backend container ID is invalid")
	}
	if (spec.ContainerID == "") != (spec.Generation == 0) {
		return errors.New("Redis backend container identity and generation must be resolved together")
	}
	if spec.HostPort != "" {
		port, err := strconv.Atoi(spec.HostPort)
		if err != nil || port < 1 || port > 65535 {
			return errors.New("Redis backend host port is invalid")
		}
	}
	if requireResolved && spec.ImageID == "" {
		return errors.New("Redis backend image ID is required")
	}
	if requireResolved && spec.VolumeIdentity == "" {
		return errors.New("Redis backend volume identity is required")
	}
	if requireResolved && spec.VolumeProvenance == "" {
		return errors.New("Redis backend volume provenance is required")
	}
	if requireResolved && spec.ContainerID == "" {
		return errors.New("Redis backend container identity is required")
	}
	if requireResolved && spec.ContainerIdentity == "" {
		return errors.New("Redis backend container provenance is required")
	}
	return nil
}

func newRedisProvenanceIdentity() (string, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("generate Redis runtime provenance: %w", err)
	}
	sum := sha256.Sum256(nonce[:])
	return fmt.Sprintf("sha256:%x", sum[:]), nil
}

func (sm *ServiceManager) inspectRedisImageIdentity(ctx context.Context, spec RedisBackendSpec) (string, error) {
	if err := validateRedisBackendSpec(spec, false); err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/images/"+url.PathEscape(spec.Image)+"/json", nil)
	if err != nil {
		return "", err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("inspect Redis image identity returned HTTP %d", response.StatusCode)
	}
	var identity struct {
		ID           string   `json:"Id"`
		RepoDigests  []string `json:"RepoDigests"`
		OS           string   `json:"Os"`
		Architecture string   `json:"Architecture"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&identity); err != nil {
		return "", fmt.Errorf("decode Redis image identity: %w", err)
	}
	if !redisImmutableIDPattern.MatchString(identity.ID) {
		return "", fmt.Errorf("Redis image inspect returned invalid immutable image ID %q", identity.ID)
	}
	if spec.ImageID != "" && identity.ID != spec.ImageID {
		return "", fmt.Errorf("Redis image ID %q does not match expected immutable image ID %q", identity.ID, spec.ImageID)
	}
	confirmedDigest := false
	for _, digest := range identity.RepoDigests {
		if digest == spec.RepoDigest || digest == spec.Image {
			confirmedDigest = true
			break
		}
	}
	if !confirmedDigest {
		return "", fmt.Errorf("Redis image inspect did not confirm repository digest %q", spec.RepoDigest)
	}
	if identity.OS+"/"+identity.Architecture != spec.Platform {
		return "", fmt.Errorf("Redis image platform %q does not match supported platform %q",
			identity.OS+"/"+identity.Architecture, spec.Platform)
	}
	return identity.ID, nil
}

type redisVolumeInspect struct {
	Name       string            `json:"Name"`
	CreatedAt  string            `json:"CreatedAt"`
	Mountpoint string            `json:"Mountpoint"`
	Labels     map[string]string `json:"Labels"`
}

func (volume redisVolumeInspect) identity() (string, error) {
	if volume.Name == "" || volume.CreatedAt == "" || volume.Mountpoint == "" {
		return "", errors.New("Redis volume inspect omitted immutable identity fields")
	}
	sum := sha256.Sum256([]byte(volume.Name + "\x00" + volume.CreatedAt + "\x00" + volume.Mountpoint))
	return fmt.Sprintf("sha256:%x", sum[:]), nil
}

type redisContainerInspect struct {
	ID      string `json:"Id"`
	Name    string `json:"Name"`
	ImageID string `json:"Image"`
	State   struct {
		Status string `json:"Status"`
	} `json:"State"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
		Cmd    []string          `json:"Cmd"`
	} `json:"Config"`
	HostConfig struct {
		NetworkMode string `json:"NetworkMode"`
	} `json:"HostConfig"`
	Mounts []struct {
		Type        string `json:"Type"`
		Name        string `json:"Name"`
		Destination string `json:"Destination"`
		RW          bool   `json:"RW"`
	} `json:"Mounts"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
		Networks map[string]struct {
			NetworkID string `json:"NetworkID"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

type redisNetworkInspect struct {
	ID         string            `json:"Id"`
	Labels     map[string]string `json:"Labels"`
	Containers map[string]struct {
		Name string `json:"Name"`
	} `json:"Containers"`
}

type redisRuntimeIdentity struct {
	Spec             RedisBackendSpec
	Profile          string
	ContainerName    string
	VolumeName       string
	VolumeCreatedAt  string
	VolumeMountpoint string
	NetworkID        string
	Endpoint         string
}

type redisProvisionalRuntime struct {
	Spec              RedisBackendSpec
	RemoveVolume      bool
	ObservedContainer redisContainerInspect
	Diagnostic        string
	CleanupBlocked    bool
}

type redisCreateDisposition int

const (
	redisCreateExact redisCreateDisposition = iota
	redisCreateAbsent
	redisCreateUnknown
)

func (sm *ServiceManager) stageRedisProvisional(spec RedisBackendSpec, removeVolume bool) {
	if sm.redisProvisionals == nil {
		sm.redisProvisionals = make(map[string]redisProvisionalRuntime)
	}
	sm.redisProvisionals[spec.ResourceID] = redisProvisionalRuntime{
		Spec:         spec,
		RemoveVolume: removeVolume,
	}
}

func (sm *ServiceManager) stageRedisProvisionalDiagnostic(
	spec RedisBackendSpec,
	removeVolume bool,
	observed redisContainerInspect,
	cause error,
	cleanupBlocked bool,
) {
	sm.stageRedisProvisional(spec, removeVolume)
	provisional := sm.redisProvisionals[spec.ResourceID]
	provisional.ObservedContainer = observed
	if cause != nil {
		provisional.Diagnostic = cause.Error()
	}
	provisional.CleanupBlocked = cleanupBlocked
	sm.redisProvisionals[spec.ResourceID] = provisional
}

func (sm *ServiceManager) retryStagedRedisProvisionalLocked(
	ctx context.Context,
	resourceID string,
) error {
	provisional, ok := sm.redisProvisionals[resourceID]
	if !ok {
		return nil
	}
	if provisional.CleanupBlocked {
		return fmt.Errorf("Redis provisional cleanup is blocked by ambiguous container creation: %s",
			provisional.Diagnostic)
	}
	if err := sm.discardRedisProvisionalLocked(ctx, provisional.Spec, provisional.RemoveVolume); err != nil {
		return fmt.Errorf("retry Redis provisional compensation: %w", err)
	}
	return nil
}

func (sm *ServiceManager) registerRedisRuntime(identity redisRuntimeIdentity) {
	sm.redisRuntimeMu.Lock()
	defer sm.redisRuntimeMu.Unlock()
	if sm.redisRuntimes == nil {
		sm.redisRuntimes = make(map[string]redisRuntimeIdentity)
	}
	sm.redisRuntimes[identity.Spec.ResourceID] = identity
}

func (sm *ServiceManager) clearRedisRuntime(resourceID string) {
	sm.redisRuntimeMu.Lock()
	defer sm.redisRuntimeMu.Unlock()
	delete(sm.redisRuntimes, resourceID)
}

func (sm *ServiceManager) redisRuntimeSnapshot() []redisRuntimeIdentity {
	sm.redisRuntimeMu.RLock()
	defer sm.redisRuntimeMu.RUnlock()
	identities := make([]redisRuntimeIdentity, 0, len(sm.redisRuntimes))
	for _, identity := range sm.redisRuntimes {
		identities = append(identities, identity)
	}
	slices.SortFunc(identities, func(a, b redisRuntimeIdentity) int {
		return strings.Compare(a.Spec.ResourceID, b.Spec.ResourceID)
	})
	return identities
}

func (sm *ServiceManager) redisRuntimeFor(resourceID string) (redisRuntimeIdentity, bool) {
	sm.redisRuntimeMu.RLock()
	defer sm.redisRuntimeMu.RUnlock()
	identity, ok := sm.redisRuntimes[resourceID]
	return identity, ok
}

func redisResourceLabels(spec RedisBackendSpec) map[string]string {
	labels := ownedDockerLabels()
	labels["minisky.service"] = "memorystore-redis"
	labels["minisky.resource"] = spec.ResourceID
	return labels
}

func redisVolumeLabels(spec RedisBackendSpec) map[string]string {
	labels := redisResourceLabels(spec)
	labels["minisky.volume-identity"] = spec.VolumeProvenance
	return labels
}

func redisContainerLabels(spec RedisBackendSpec) map[string]string {
	labels := redisResourceLabels(spec)
	labels["minisky.generation"] = fmt.Sprintf("%d", spec.Generation)
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	labels["minisky.container-identity"] = spec.ContainerIdentity
	labels["minisky.container-name"] = containerName
	labels["minisky.image-id"] = spec.ImageID
	labels["minisky.volume-name"] = volumeName
	labels["minisky.volume-identity"] = spec.VolumeIdentity
	labels["minisky.volume-provenance"] = spec.VolumeProvenance
	return labels
}

func redisServerCommand() []string {
	return []string{
		"valkey-server",
		"--appendonly", "yes",
		"--appendfsync", "always",
		"--dir", "/data",
	}
}

func (sm *ServiceManager) pullRedisImage(ctx context.Context, spec RedisBackendSpec) error {
	pullCtx, cancel := context.WithTimeout(ctx, dockerImagePullTimeout)
	defer cancel()
	endpoint := "http://localhost/images/create?" + url.Values{
		"fromImage": {spec.Image},
		"platform":  {spec.Platform},
	}.Encode()
	request, err := http.NewRequestWithContext(pullCtx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if readErr != nil {
			return fmt.Errorf("Redis image pull returned HTTP %d and unreadable error body: %w",
				response.StatusCode, readErr)
		}
		return fmt.Errorf("Redis image pull returned HTTP %d: %s",
			response.StatusCode, strings.TrimSpace(string(body)))
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	for {
		var event struct {
			Error       string `json:"error"`
			ErrorDetail struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("decode Redis image pull stream: %w", err)
		}
		if event.ErrorDetail.Message != "" {
			return fmt.Errorf("Redis image pull failed: %s", event.ErrorDetail.Message)
		}
		if event.Error != "" {
			return fmt.Errorf("Redis image pull failed: %s", event.Error)
		}
	}
}

func (sm *ServiceManager) ensureRedisImage(
	ctx context.Context,
	spec RedisBackendSpec,
) (RedisBackendSpec, error) {
	exists, err := sm.imageExistsContext(ctx, spec.Image)
	if err != nil {
		return spec, fmt.Errorf("inspect Redis image: %w", err)
	}
	if !exists {
		if err := sm.pullRedisImage(ctx, spec); err != nil {
			return spec, fmt.Errorf("pull Redis image: %w", err)
		}
	}
	imageID, err := sm.inspectRedisImageIdentity(ctx, spec)
	if err != nil {
		return spec, err
	}
	spec.ImageID = imageID
	return spec, nil
}

func (sm *ServiceManager) inspectRedisVolume(
	ctx context.Context,
	name string,
) (redisVolumeInspect, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/volumes/"+url.PathEscape(name), nil)
	if err != nil {
		return redisVolumeInspect{}, false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return redisVolumeInspect{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return redisVolumeInspect{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return redisVolumeInspect{}, false,
			fmt.Errorf("inspect Redis volume returned HTTP %d", response.StatusCode)
	}
	var volume redisVolumeInspect
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&volume); err != nil {
		return redisVolumeInspect{}, false, fmt.Errorf("decode Redis volume inspect: %w", err)
	}
	return volume, true, nil
}

func (sm *ServiceManager) inspectRedisContainer(
	ctx context.Context,
	name string,
) (redisContainerInspect, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(name)+"/json", nil)
	if err != nil {
		return redisContainerInspect{}, false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return redisContainerInspect{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return redisContainerInspect{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return redisContainerInspect{}, false,
			fmt.Errorf("inspect Redis container returned HTTP %d", response.StatusCode)
	}
	var container redisContainerInspect
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&container); err != nil {
		return redisContainerInspect{}, false, fmt.Errorf("decode Redis container inspect: %w", err)
	}
	return container, true, nil
}

func (sm *ServiceManager) inspectRedisNetwork(
	ctx context.Context,
	identity string,
) (redisNetworkInspect, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/networks/"+url.PathEscape(identity), nil)
	if err != nil {
		return redisNetworkInspect{}, false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return redisNetworkInspect{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return redisNetworkInspect{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return redisNetworkInspect{}, false,
			fmt.Errorf("inspect Redis network returned HTTP %d", response.StatusCode)
	}
	var network redisNetworkInspect
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&network); err != nil {
		return redisNetworkInspect{}, false, fmt.Errorf("decode Redis network inspect: %w", err)
	}
	return network, true, nil
}

func (sm *ServiceManager) resolveRedisRuntimeIdentity(
	ctx context.Context,
	spec RedisBackendSpec,
	containerName string,
	volumeName string,
	container redisContainerInspect,
	volume redisVolumeInspect,
) (redisRuntimeIdentity, error) {
	if err := validateRedisBackendSpec(spec, true); err != nil {
		return redisRuntimeIdentity{}, err
	}
	if err := validateRedisContainer(container, containerName, volumeName, spec); err != nil {
		return redisRuntimeIdentity{}, err
	}
	if _, err := validateRedisVolume(volume, volumeName, spec); err != nil {
		return redisRuntimeIdentity{}, err
	}
	network, err := sm.validateRedisContainerNetwork(ctx, container, containerName, "")
	if err != nil {
		return redisRuntimeIdentity{}, err
	}
	loopbackEndpoint, err := redisEndpointFromInspect(container)
	if err != nil {
		return redisRuntimeIdentity{}, err
	}
	return redisRuntimeIdentity{
		Spec:             spec,
		Profile:          config.GetProfile(),
		ContainerName:    containerName,
		VolumeName:       volumeName,
		VolumeCreatedAt:  volume.CreatedAt,
		VolumeMountpoint: volume.Mountpoint,
		NetworkID:        network.ID,
		Endpoint:         loopbackEndpoint,
	}, nil
}

func validateRedisVolume(
	volume redisVolumeInspect,
	name string,
	spec RedisBackendSpec,
) (string, error) {
	if volume.Name != name || !hasExpectedDurableOwnership(volume.Labels, redisVolumeLabels(spec)) {
		return "", fmt.Errorf("Redis volume %q is not exact-owned", name)
	}
	if spec.VolumeProvenance == "" || volume.Labels["minisky.volume-identity"] != spec.VolumeProvenance {
		return "", fmt.Errorf("Redis volume %q provenance identity changed", name)
	}
	identity, err := volume.identity()
	if err != nil {
		return "", err
	}
	if spec.VolumeIdentity != "" && identity != spec.VolumeIdentity {
		return "", fmt.Errorf("Redis volume %q immutable identity changed", name)
	}
	return identity, nil
}

func validateRedisContainer(
	container redisContainerInspect,
	name string,
	volumeName string,
	spec RedisBackendSpec,
) error {
	if err := validateRedisContainerDefinition(container, name, volumeName, spec); err != nil {
		return err
	}
	if len(container.NetworkSettings.Networks) != 1 {
		return fmt.Errorf("Redis container %q has %d network attachments, want exactly one",
			name, len(container.NetworkSettings.Networks))
	}
	attachment, ok := container.NetworkSettings.Networks[networkName]
	if !ok || attachment.NetworkID == "" {
		return fmt.Errorf("Redis container %q is not attached to %s", name, networkName)
	}
	return nil
}

func (sm *ServiceManager) validateRedisContainerNetwork(
	ctx context.Context,
	container redisContainerInspect,
	containerName string,
	expectedNetworkID string,
) (redisNetworkInspect, error) {
	if len(container.NetworkSettings.Networks) != 1 {
		return redisNetworkInspect{}, fmt.Errorf(
			"Redis container %q has %d network attachments, want exactly one",
			containerName, len(container.NetworkSettings.Networks))
	}
	attachment, ok := container.NetworkSettings.Networks[networkName]
	if !ok || attachment.NetworkID == "" {
		return redisNetworkInspect{}, fmt.Errorf("Redis container %q is not attached to %s",
			containerName, networkName)
	}
	network, found, err := sm.inspectRedisNetwork(ctx, networkName)
	if err != nil {
		return redisNetworkInspect{}, err
	}
	if !found || network.ID == "" || network.ID != attachment.NetworkID ||
		(expectedNetworkID != "" && network.ID != expectedNetworkID) ||
		!isOwnedDockerResource(network.Labels) {
		return redisNetworkInspect{}, fmt.Errorf("Redis network %q immutable identity drifted", networkName)
	}
	endpoint, ok := network.Containers[container.ID]
	if !ok || endpoint.Name != containerName {
		return redisNetworkInspect{}, fmt.Errorf("Redis network %q does not contain the exact backend endpoint", networkName)
	}
	return network, nil
}

func validateRedisContainerDefinition(
	container redisContainerInspect,
	name string,
	volumeName string,
	spec RedisBackendSpec,
) error {
	if !redisContainerIDPattern.MatchString(container.ID) {
		return fmt.Errorf("Redis container %q has invalid immutable ID", name)
	}
	if strings.TrimPrefix(container.Name, "/") != name {
		return fmt.Errorf("Redis container %q deterministic name drifted", name)
	}
	if container.ImageID != spec.ImageID || container.Config.Image != spec.ImageID {
		return fmt.Errorf("Redis container %q image identity drifted", name)
	}
	if !slices.Equal(container.Config.Cmd, redisServerCommand()) ||
		container.HostConfig.NetworkMode != networkName {
		return fmt.Errorf("Redis container %q runtime configuration drifted", name)
	}
	if !hasExpectedDurableOwnership(container.Config.Labels, redisContainerLabels(spec)) {
		return fmt.Errorf("Redis container %q is not exact-owned", name)
	}
	if spec.ContainerID != "" && container.ID != spec.ContainerID {
		return fmt.Errorf("Redis container %q immutable ID changed", name)
	}
	if len(container.Mounts) != 1 ||
		container.Mounts[0].Type != "volume" ||
		container.Mounts[0].Name != volumeName ||
		container.Mounts[0].Destination != "/data" ||
		!container.Mounts[0].RW {
		return fmt.Errorf("Redis container %q has an unexpected data volume", name)
	}
	return nil
}

func redisEndpointFromInspect(container redisContainerInspect) (string, error) {
	bindings := container.NetworkSettings.Ports["6379/tcp"]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" || bindings[0].HostPort == "" {
		return "", errors.New("Redis container has no unique loopback port binding")
	}
	return bindings[0].HostIP + ":" + bindings[0].HostPort, nil
}

func (sm *ServiceManager) createRedisVolume(
	ctx context.Context,
	name string,
	spec RedisBackendSpec,
) (redisVolumeInspect, error) {
	payload, err := json.Marshal(map[string]any{
		"Name":   name,
		"Labels": redisVolumeLabels(spec),
	})
	if err != nil {
		return redisVolumeInspect{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/volumes/create", bytes.NewReader(payload))
	if err != nil {
		return redisVolumeInspect{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := sm.doDocker(request)
	if err != nil {
		return redisVolumeInspect{}, err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return redisVolumeInspect{}, fmt.Errorf("create Redis volume returned HTTP %d", response.StatusCode)
	}
	volume, found, err := sm.inspectRedisVolume(ctx, name)
	if err != nil {
		return redisVolumeInspect{}, err
	}
	if !found {
		return redisVolumeInspect{}, fmt.Errorf("created Redis volume %q is absent", name)
	}
	if _, err := validateRedisVolume(volume, name, spec); err != nil {
		return redisVolumeInspect{}, err
	}
	return volume, nil
}

func (sm *ServiceManager) createRedisContainer(
	ctx context.Context,
	name string,
	volumeName string,
	spec RedisBackendSpec,
) (redisContainerInspect, redisCreateDisposition, error) {
	const containerPort = "6379/tcp"
	hostPort := spec.HostPort
	if hostPort == "" {
		hostPort = "0"
	}
	payload, err := json.Marshal(map[string]any{
		"Image":        spec.ImageID,
		"Cmd":          redisServerCommand(),
		"ExposedPorts": map[string]any{containerPort: struct{}{}},
		"HostConfig": map[string]any{
			"NetworkMode": networkName,
			"PortBindings": map[string]any{
				containerPort: []map[string]string{{"HostIp": "127.0.0.1", "HostPort": hostPort}},
			},
			"Binds": []string{volumeName + ":/data"},
		},
		"Labels": redisContainerLabels(spec),
	})
	if err != nil {
		return redisContainerInspect{}, redisCreateAbsent, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(name), bytes.NewReader(payload))
	if err != nil {
		return redisContainerInspect{}, redisCreateAbsent, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := sm.doDocker(request)
	if err != nil {
		return sm.recoverRedisContainerCreate(ctx, name, volumeName, spec,
			fmt.Errorf("create Redis container transport: %w", err))
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return sm.recoverRedisContainerCreate(ctx, name, volumeName, spec,
			fmt.Errorf("create Redis container returned HTTP %d: %s",
				response.StatusCode, strings.TrimSpace(string(body))))
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&created); err != nil {
		return sm.recoverRedisContainerCreate(ctx, name, volumeName, spec,
			fmt.Errorf("decode Redis container create response: %w", err))
	}
	if !redisContainerIDPattern.MatchString(created.ID) {
		return sm.recoverRedisContainerCreate(ctx, name, volumeName, spec,
			errors.New("Redis container create returned an invalid immutable ID"))
	}
	container, found, err := sm.inspectRedisContainer(ctx, created.ID)
	if err != nil {
		return sm.recoverRedisContainerCreate(ctx, name, volumeName, spec,
			fmt.Errorf("inspect created Redis container %q: %w", created.ID, err))
	}
	if !found {
		return sm.recoverRedisContainerCreate(ctx, name, volumeName, spec,
			errors.New("created Redis container is absent"))
	}
	if err := validateRedisContainerDefinition(container, name, volumeName, spec); err != nil {
		return container, redisCreateUnknown, err
	}
	return container, redisCreateExact, nil
}

func (sm *ServiceManager) recoverRedisContainerCreate(
	ctx context.Context,
	name string,
	volumeName string,
	spec RedisBackendSpec,
	cause error,
) (redisContainerInspect, redisCreateDisposition, error) {
	container, found, err := sm.inspectRedisContainer(ctx, name)
	if err != nil {
		return redisContainerInspect{}, redisCreateUnknown, errors.Join(cause,
			fmt.Errorf("inspect deterministic Redis container after ambiguous create: %w", err))
	}
	if !found {
		return redisContainerInspect{}, redisCreateAbsent, cause
	}
	if err := validateRedisContainerDefinition(container, name, volumeName, spec); err != nil {
		return container, redisCreateUnknown, errors.Join(cause,
			fmt.Errorf("validate deterministic Redis container after ambiguous create: %w", err))
	}
	volume, found, err := sm.inspectRedisVolume(ctx, volumeName)
	if err != nil {
		return container, redisCreateUnknown, errors.Join(cause,
			fmt.Errorf("inspect Redis volume after ambiguous create: %w", err))
	}
	if !found {
		return container, redisCreateUnknown, errors.Join(cause,
			fmt.Errorf("exact Redis volume %q is missing after ambiguous create", volumeName))
	}
	if _, err := validateRedisVolume(volume, volumeName, spec); err != nil {
		return container, redisCreateUnknown, errors.Join(cause,
			fmt.Errorf("validate Redis volume after ambiguous create: %w", err))
	}
	imageSpec := spec
	imageSpec.ContainerIdentity = ""
	imageSpec.Generation = 0
	if _, err := sm.inspectRedisImageIdentity(ctx, imageSpec); err != nil {
		return container, redisCreateUnknown, errors.Join(cause,
			fmt.Errorf("validate Redis image after ambiguous create: %w", err))
	}
	return container, redisCreateExact, cause
}

func (sm *ServiceManager) startAndCheckRedis(
	ctx context.Context,
	name string,
	volumeName string,
	spec RedisBackendSpec,
	container redisContainerInspect,
) (string, error) {
	if container.State.Status != "running" {
		if err := sm.startContainerContext(ctx, container.ID); err != nil {
			return "", err
		}
	}
	running, err := sm.waitForRedisAttachment(ctx, name, volumeName, spec)
	if err != nil {
		return "", err
	}
	if running.State.Status != "running" {
		return "", fmt.Errorf("Redis container is %s after start", running.State.Status)
	}
	endpoint, err := redisEndpointFromInspect(running)
	if err != nil {
		return "", err
	}
	if spec.HostPort != "" && endpoint != "127.0.0.1:"+spec.HostPort {
		return "", errors.New("Redis container loopback host port drifted")
	}
	ready := sm.redisReady
	if ready == nil {
		ready = waitUntilRedisReady
	}
	if err := ready(ctx, endpoint, 30*time.Second); err != nil {
		return "", err
	}
	return endpoint, nil
}

func (sm *ServiceManager) waitForRedisAttachment(
	ctx context.Context,
	name string,
	volumeName string,
	spec RedisBackendSpec,
) (redisContainerInspect, error) {
	attachmentCtx, cancel := context.WithTimeout(ctx, redisAttachmentVisibilityTimeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		container, found, err := sm.inspectRedisContainer(attachmentCtx, spec.ContainerID)
		if err != nil {
			return redisContainerInspect{}, err
		}
		if !found {
			return redisContainerInspect{}, errors.New("Redis container disappeared after start")
		}
		if err := validateRedisContainerDefinition(container, name, volumeName, spec); err != nil {
			return redisContainerInspect{}, err
		}
		if container.State.Status != "running" {
			return redisContainerInspect{}, fmt.Errorf("Redis container is %s after start", container.State.Status)
		}
		if len(container.NetworkSettings.Networks) > 1 {
			return redisContainerInspect{}, fmt.Errorf(
				"Redis container %q has %d network attachments, want exactly one",
				name, len(container.NetworkSettings.Networks))
		}
		attachment, attached := container.NetworkSettings.Networks[networkName]
		if len(container.NetworkSettings.Networks) == 1 && !attached {
			return redisContainerInspect{}, fmt.Errorf("Redis container %q has a foreign network attachment", name)
		}
		if attached && attachment.NetworkID != "" {
			network, found, err := sm.inspectRedisNetwork(attachmentCtx, networkName)
			if err != nil {
				return redisContainerInspect{}, err
			}
			if !found || network.ID != attachment.NetworkID || !isOwnedDockerResource(network.Labels) {
				return redisContainerInspect{}, fmt.Errorf("Redis network %q immutable identity drifted", networkName)
			}
			if endpoint, ok := network.Containers[container.ID]; ok {
				if endpoint.Name != name {
					return redisContainerInspect{}, fmt.Errorf(
						"Redis network %q contains a foreign backend endpoint", networkName)
				}
				return container, nil
			}
		}
		select {
		case <-attachmentCtx.Done():
			return redisContainerInspect{}, fmt.Errorf(
				"Redis container %q attachment to %s was not visible before timeout: %w",
				name, networkName, attachmentCtx.Err())
		case <-ticker.C:
		}
	}
}

// ProvisionRedisExact creates a new exact-owned Valkey backend from the
// immutable image ID and returns the resolved immutable volume identity.
func (sm *ServiceManager) ProvisionRedisExact(
	ctx context.Context,
	spec RedisBackendSpec,
) (string, RedisBackendSpec, error) {
	if err := validateRedisBackendSpec(spec, false); err != nil {
		return "", spec, err
	}
	requested := spec
	if spec.VolumeIdentity != "" {
		return "", spec, errors.New("new Redis provisioning cannot reuse a persisted volume identity")
	}
	sm.emulatorMu.Lock()
	defer sm.emulatorMu.Unlock()
	if sm.redisClosing {
		return "", spec, errors.New("Redis provisioning refused during service-manager teardown")
	}
	if err := sm.retryStagedRedisProvisionalLocked(ctx, spec.ResourceID); err != nil {
		return "", spec, err
	}
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	if _, found, err := sm.inspectRedisContainer(ctx, containerName); err != nil {
		return "", spec, err
	} else if found {
		return "", spec, fmt.Errorf("Redis container %q already exists", containerName)
	}
	spec, err := sm.ensureRedisImage(ctx, spec)
	if err != nil {
		return "", spec, err
	}
	if _, found, err := sm.inspectRedisVolume(ctx, volumeName); err != nil {
		return "", spec, err
	} else if found {
		return "", spec, fmt.Errorf("refusing to adopt pre-existing Redis volume %q", volumeName)
	}
	spec.VolumeProvenance, err = newRedisProvenanceIdentity()
	if err != nil {
		return "", spec, err
	}
	volume, err := sm.createRedisVolume(ctx, volumeName, spec)
	if err != nil {
		return "", requested, sm.rollbackRedisProvisionLocked(ctx, spec, err)
	}
	spec.VolumeIdentity, err = volume.identity()
	if err != nil {
		return "", requested, sm.rollbackRedisProvisionLocked(ctx, spec, err)
	}
	sm.stageRedisProvisional(spec, true)
	spec.ContainerIdentity, err = newRedisProvenanceIdentity()
	if err != nil {
		return "", requested, sm.rollbackRedisProvisionLocked(ctx, spec, err)
	}
	spec.Generation = 1
	sm.stageRedisProvisional(spec, true)
	container, disposition, err := sm.createRedisContainer(ctx, containerName, volumeName, spec)
	if err != nil {
		spec.ContainerID = container.ID
		sm.stageRedisProvisionalDiagnostic(spec, true, container, err,
			disposition == redisCreateUnknown)
		if disposition == redisCreateUnknown {
			return "", requested, err
		}
		return "", requested, sm.rollbackRedisProvisionLocked(ctx, spec, err)
	}
	spec.ContainerID = container.ID
	sm.stageRedisProvisional(spec, true)
	endpoint, err := sm.startAndCheckRedis(ctx, containerName, volumeName, spec, container)
	if err != nil {
		return "", requested, sm.rollbackRedisProvisionLocked(ctx, spec, err)
	}
	spec.HostPort = strings.TrimPrefix(endpoint, "127.0.0.1:")
	sm.stageRedisProvisional(spec, true)
	running, found, err := sm.inspectRedisContainer(ctx, spec.ContainerID)
	if err != nil || !found {
		if err == nil {
			err = errors.New("Redis container disappeared before runtime registration")
		}
		return "", requested, sm.rollbackRedisProvisionLocked(ctx, spec, err)
	}
	if _, err := sm.resolveRedisRuntimeIdentity(ctx, spec, containerName, volumeName, running, volume); err != nil {
		return "", requested, sm.rollbackRedisProvisionLocked(ctx, spec, err)
	}
	return endpoint, spec, nil
}

func (sm *ServiceManager) rollbackRedisProvisionLocked(
	ctx context.Context,
	spec RedisBackendSpec,
	cause error,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*dockerRequestTimeout)
	defer cancel()
	if cleanupErr := sm.discardRedisProvisionalLocked(cleanupCtx, spec, true); cleanupErr != nil {
		return errors.Join(cause, fmt.Errorf("roll back failed Redis provisioning: %w", cleanupErr))
	}
	return cause
}

func (sm *ServiceManager) compensateRedisReplacementLocked(
	ctx context.Context,
	spec RedisBackendSpec,
	cause error,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*dockerRequestTimeout)
	defer cancel()
	if cleanupErr := sm.discardRedisProvisionalLocked(cleanupCtx, spec, false); cleanupErr != nil {
		return errors.Join(cause, fmt.Errorf("compensate failed Redis replacement: %w", cleanupErr))
	}
	return cause
}

// ReconcileRedisExact validates every persisted backend identity and recreates
// only a missing exact container around the same immutable volume.
func (sm *ServiceManager) ReconcileRedisExact(
	ctx context.Context,
	spec RedisBackendSpec,
) (string, RedisBackendSpec, bool, error) {
	if err := validateRedisBackendSpec(spec, true); err != nil {
		return "", spec, false, err
	}
	persisted := spec
	sm.emulatorMu.Lock()
	defer sm.emulatorMu.Unlock()
	if sm.redisClosing {
		return "", persisted, false, errors.New("Redis reconciliation refused during service-manager teardown")
	}
	if err := sm.retryStagedRedisProvisionalLocked(ctx, spec.ResourceID); err != nil {
		return "", persisted, false, err
	}
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	container, containerFound, err := sm.inspectRedisContainer(ctx, containerName)
	if err != nil {
		return "", spec, false, err
	}
	volume, volumeFound, err := sm.inspectRedisVolume(ctx, volumeName)
	if err != nil {
		return "", spec, false, err
	}
	if !containerFound && !volumeFound {
		return "", spec, false, nil
	}
	if !volumeFound {
		return "", spec, false, fmt.Errorf("exact Redis volume %q is missing", volumeName)
	}
	if _, err := validateRedisVolume(volume, volumeName, spec); err != nil {
		return "", spec, false, err
	}
	resolved, err := sm.ensureRedisImage(ctx, spec)
	if err != nil {
		return "", spec, false, err
	}
	spec = resolved
	if !containerFound {
		if spec.Generation == ^uint64(0) {
			return "", persisted, true, errors.New("Redis backend generation overflow")
		}
		spec.Generation++
		spec.ContainerID = ""
		spec.ContainerIdentity, err = newRedisProvenanceIdentity()
		if err != nil {
			return "", persisted, true, err
		}
		sm.stageRedisProvisional(spec, false)
		var disposition redisCreateDisposition
		container, disposition, err = sm.createRedisContainer(ctx, containerName, volumeName, spec)
		if err != nil {
			spec.ContainerID = container.ID
			sm.stageRedisProvisionalDiagnostic(spec, false, container, err,
				disposition == redisCreateUnknown)
			if disposition == redisCreateUnknown {
				return "", persisted, true, err
			}
			return "", persisted, true, sm.compensateRedisReplacementLocked(ctx, spec, err)
		}
		spec.ContainerID = container.ID
		sm.stageRedisProvisional(spec, false)
	} else if err := validateRedisContainer(container, containerName, volumeName, spec); err != nil {
		return "", persisted, false, err
	}
	endpoint, err := sm.startAndCheckRedis(ctx, containerName, volumeName, spec, container)
	if err != nil {
		if !containerFound {
			err = sm.compensateRedisReplacementLocked(ctx, spec, err)
		}
		return endpoint, persisted, true, err
	}
	if spec.HostPort == "" {
		spec.HostPort = strings.TrimPrefix(endpoint, "127.0.0.1:")
	}
	running, found, err := sm.inspectRedisContainer(ctx, spec.ContainerID)
	if err != nil {
		if !containerFound {
			err = sm.compensateRedisReplacementLocked(ctx, spec, err)
		}
		return "", persisted, true, err
	}
	if !found {
		return "", persisted, true, errors.New("Redis replacement disappeared before provisional validation")
	}
	if _, err := sm.resolveRedisRuntimeIdentity(ctx, spec, containerName, volumeName, running, volume); err != nil {
		if !containerFound {
			err = sm.compensateRedisReplacementLocked(ctx, spec, err)
		}
		return "", persisted, true, err
	}
	sm.stageRedisProvisional(spec, false)
	return endpoint, spec, true, nil
}

// PublishRedisRuntime validates and exposes only runtime provenance that the
// Memorystore shim has already persisted successfully.
func (sm *ServiceManager) PublishRedisRuntime(ctx context.Context, spec RedisBackendSpec) error {
	if err := validateRedisBackendSpec(spec, true); err != nil {
		return err
	}
	sm.emulatorMu.Lock()
	defer sm.emulatorMu.Unlock()
	if sm.redisClosing {
		return errors.New("publish Redis runtime refused during service-manager teardown")
	}
	provisional, ok := sm.redisProvisionals[spec.ResourceID]
	if !ok || provisional.Spec != spec {
		return errors.New("publish Redis runtime: provisional provenance does not match persisted spec")
	}
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	container, found, err := sm.inspectRedisContainer(ctx, spec.ContainerID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("publish Redis runtime: exact container %q is missing", spec.ContainerID)
	}
	volume, found, err := sm.inspectRedisVolume(ctx, volumeName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("publish Redis runtime: exact volume %q is missing", volumeName)
	}
	identity, err := sm.resolveRedisRuntimeIdentity(ctx, spec, containerName, volumeName, container, volume)
	if err != nil {
		return err
	}
	if container.State.Status != "running" {
		return fmt.Errorf("publish Redis runtime: exact container %q is %s",
			spec.ContainerID, container.State.Status)
	}
	sm.registerRedisRuntime(identity)
	delete(sm.redisProvisionals, spec.ResourceID)
	return nil
}

func (sm *ServiceManager) UnpublishRedisRuntime(spec RedisBackendSpec) {
	sm.redisRuntimeMu.Lock()
	defer sm.redisRuntimeMu.Unlock()
	if current, ok := sm.redisRuntimes[spec.ResourceID]; ok &&
		current.Spec.ContainerID == spec.ContainerID &&
		current.Spec.Generation == spec.Generation {
		delete(sm.redisRuntimes, spec.ResourceID)
	}
}

// DiscardRedisProvisional removes a validated uncommitted container and,
// only for initial provisioning, its newly-created exact volume.
func (sm *ServiceManager) DiscardRedisProvisional(
	ctx context.Context,
	spec RedisBackendSpec,
	removeVolume bool,
) error {
	sm.emulatorMu.Lock()
	defer sm.emulatorMu.Unlock()
	return sm.discardRedisProvisionalLocked(ctx, spec, removeVolume)
}

func (sm *ServiceManager) discardRedisProvisionalLocked(
	ctx context.Context,
	spec RedisBackendSpec,
	removeVolume bool,
) error {
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	if spec.ContainerID != "" {
		container, found, err := sm.inspectRedisContainer(ctx, spec.ContainerID)
		if err != nil {
			return err
		}
		if found {
			if err := validateRedisContainerDefinition(container, containerName, volumeName, spec); err != nil {
				return fmt.Errorf("validate Redis provisional container %q: %w", spec.ContainerID, err)
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
				"http://localhost/containers/"+url.PathEscape(spec.ContainerID)+"?force=true", nil)
			if err != nil {
				return err
			}
			response, err := sm.doDocker(request)
			if err != nil {
				return err
			}
			status := response.StatusCode
			response.Body.Close()
			if status != http.StatusNoContent && status != http.StatusNotFound {
				return fmt.Errorf("discard Redis provisional container returned HTTP %d", status)
			}
		}
	}
	if !removeVolume {
		delete(sm.redisProvisionals, spec.ResourceID)
		return nil
	}
	volume, found, err := sm.inspectRedisVolume(ctx, volumeName)
	if err != nil {
		return err
	}
	if !found {
		delete(sm.redisProvisionals, spec.ResourceID)
		return nil
	}
	if _, err := validateRedisVolume(volume, volumeName, spec); err != nil {
		return fmt.Errorf("validate Redis provisional volume immediately before deletion: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/volumes/"+url.PathEscape(volumeName), nil)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("discard Redis provisional volume returned HTTP %d", response.StatusCode)
	}
	delete(sm.redisProvisionals, spec.ResourceID)
	return nil
}

func (sm *ServiceManager) validateRedisRuntimeSet(
	ctx context.Context,
	identities []redisRuntimeIdentity,
) (redisNetworkInspect, map[string]bool, error) {
	if len(identities) == 0 {
		return redisNetworkInspect{}, nil, nil
	}
	networkID := identities[0].NetworkID
	network, found, err := sm.inspectRedisNetwork(ctx, networkID)
	if err != nil {
		return redisNetworkInspect{}, nil, err
	}
	if !found || network.ID != networkID || !isOwnedDockerResource(network.Labels) {
		return redisNetworkInspect{}, nil, fmt.Errorf("Redis runtime network %q is not exact-owned", networkName)
	}
	registered := make(map[string]redisRuntimeIdentity, len(identities))
	for _, identity := range identities {
		registered[identity.Spec.ContainerID] = identity
	}
	for containerID, endpoint := range network.Containers {
		identity, ok := registered[containerID]
		if !ok || endpoint.Name != identity.ContainerName {
			return redisNetworkInspect{}, nil, fmt.Errorf(
				"%w: Redis runtime network contains unregistered endpoint %q",
				ErrDockerOwnershipConflict, containerID)
		}
	}
	present := make(map[string]bool, len(identities))
	for _, identity := range identities {
		if identity.NetworkID != networkID {
			return redisNetworkInspect{}, nil, errors.New("Redis runtime registry spans multiple network identities")
		}
		containerPresent, err := sm.validateRedisRuntimeIdentity(ctx, identity, network)
		if err != nil {
			return redisNetworkInspect{}, nil, err
		}
		present[identity.Spec.ResourceID] = containerPresent
	}
	return network, present, nil
}

func (sm *ServiceManager) validateRedisRuntimeIdentity(
	ctx context.Context,
	identity redisRuntimeIdentity,
	network redisNetworkInspect,
) (bool, error) {
	spec := identity.Spec
	if err := validateRedisBackendSpec(spec, true); err != nil {
		return false, err
	}
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	if identity.Profile != config.GetProfile() ||
		identity.ContainerName != containerName ||
		identity.VolumeName != volumeName {
		return false, errors.New("Redis runtime registry identity does not match the active profile and deterministic names")
	}
	volume, found, err := sm.inspectRedisVolume(ctx, volumeName)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("exact Redis volume %q is missing", volumeName)
	}
	if _, err := validateRedisVolume(volume, volumeName, spec); err != nil {
		return false, err
	}
	if volume.CreatedAt != identity.VolumeCreatedAt || volume.Mountpoint != identity.VolumeMountpoint {
		return false, fmt.Errorf("exact Redis volume %q runtime identity drifted", volumeName)
	}
	if _, err := sm.inspectRedisImageIdentity(ctx, spec); err != nil {
		return false, err
	}
	container, found, err := sm.inspectRedisContainer(ctx, spec.ContainerID)
	if err != nil {
		return false, err
	}
	if !found {
		if _, attached := network.Containers[spec.ContainerID]; attached {
			return false, fmt.Errorf("%w: missing Redis container %q retains a network endpoint",
				ErrDockerOwnershipConflict, spec.ContainerID)
		}
		return false, nil
	}
	if err := validateRedisContainer(container, containerName, volumeName, spec); err != nil {
		return false, err
	}
	if container.State.Status != "running" {
		return false, fmt.Errorf("exact Redis container %q is %s during graceful teardown",
			spec.ContainerID, container.State.Status)
	}
	inspectedNetwork, err := sm.validateRedisContainerNetwork(
		ctx, container, containerName, identity.NetworkID)
	if err != nil {
		return false, err
	}
	if inspectedNetwork.ID != network.ID {
		return false, fmt.Errorf("exact Redis container %q network identity drifted", spec.ContainerID)
	}
	endpoint, err := redisEndpointFromInspect(container)
	if err != nil {
		return false, err
	}
	if endpoint != identity.Endpoint {
		return false, fmt.Errorf("exact Redis container %q loopback endpoint drifted", spec.ContainerID)
	}
	networkEndpoint, ok := network.Containers[spec.ContainerID]
	if !ok || networkEndpoint.Name != containerName {
		return false, fmt.Errorf("%w: network endpoint does not match exact Redis runtime identity",
			ErrDockerOwnershipConflict)
	}
	return true, nil
}

func (sm *ServiceManager) teardownRedisRuntimes(ctx context.Context) (bool, error) {
	identities := sm.redisRuntimeSnapshot()
	sm.emulatorMu.Lock()
	defer sm.emulatorMu.Unlock()
	provisionalIDs := make([]string, 0, len(sm.redisProvisionals))
	for resourceID := range sm.redisProvisionals {
		provisionalIDs = append(provisionalIDs, resourceID)
	}
	slices.Sort(provisionalIDs)
	var provisionalFailures error
	for _, resourceID := range provisionalIDs {
		provisional := sm.redisProvisionals[resourceID]
		if provisional.CleanupBlocked {
			provisionalFailures = errors.Join(provisionalFailures,
				fmt.Errorf("uncommitted Redis runtime %q requires manual cleanup: %s",
					resourceID, provisional.Diagnostic))
			continue
		}
		if err := sm.discardRedisProvisionalLocked(ctx, provisional.Spec, provisional.RemoveVolume); err != nil {
			provisionalFailures = errors.Join(provisionalFailures,
				fmt.Errorf("discard uncommitted Redis runtime %q: %w", resourceID, err))
		}
	}
	if provisionalFailures != nil {
		return true, provisionalFailures
	}
	if len(identities) == 0 {
		return false, nil
	}
	if _, _, err := sm.validateRedisRuntimeSet(ctx, identities); err != nil {
		return true, err
	}
	network, present, err := sm.validateRedisRuntimeSet(ctx, identities)
	if err != nil {
		return true, fmt.Errorf("Redis runtime changed before graceful teardown: %w", err)
	}
	var failures error
	for _, identity := range identities {
		if !present[identity.Spec.ResourceID] {
			sm.clearRedisRuntime(identity.Spec.ResourceID)
			continue
		}
		stillPresent, err := sm.validateRedisRuntimeIdentity(ctx, identity, network)
		if err != nil {
			failures = errors.Join(failures,
				fmt.Errorf("Redis runtime changed immediately before container removal: %w", err))
			continue
		}
		if !stillPresent {
			sm.clearRedisRuntime(identity.Spec.ResourceID)
			continue
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://localhost/containers/"+url.PathEscape(identity.Spec.ContainerID)+"?force=true", nil)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		response, err := sm.doDocker(request)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		status := response.StatusCode
		closeErr := response.Body.Close()
		if closeErr != nil {
			failures = errors.Join(failures, fmt.Errorf("close Redis container delete response: %w", closeErr))
			continue
		}
		if status != http.StatusNoContent {
			failures = errors.Join(failures,
				fmt.Errorf("delete exact Redis container %q returned HTTP %d",
					identity.Spec.ContainerID, status))
			continue
		}
		remaining, found, err := sm.inspectRedisContainer(ctx, identity.Spec.ContainerID)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		if found {
			failures = errors.Join(failures,
				fmt.Errorf("exact Redis container %q remained after removal: %#v",
					identity.Spec.ContainerID, remaining.State.Status))
			continue
		}
		volume, found, err := sm.inspectRedisVolume(ctx, identity.VolumeName)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		if !found {
			failures = errors.Join(failures,
				fmt.Errorf("retained Redis volume %q disappeared during graceful teardown", identity.VolumeName))
			continue
		}
		if _, err := validateRedisVolume(volume, identity.VolumeName, identity.Spec); err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		sm.clearRedisRuntime(identity.Spec.ResourceID)
	}
	rechecked, found, err := sm.inspectRedisNetwork(ctx, network.ID)
	if err != nil {
		return true, errors.Join(failures, err)
	}
	if !found || rechecked.ID != network.ID || !isOwnedDockerResource(rechecked.Labels) {
		return true, errors.Join(failures,
			fmt.Errorf("Redis runtime network %q changed before removal", networkName))
	}
	if len(rechecked.Containers) != 0 {
		return true, errors.Join(failures,
			fmt.Errorf("%w: Redis runtime network still has %d endpoints after exact container removal",
				ErrDockerOwnershipConflict, len(rechecked.Containers)))
	}
	if unresolved := sm.redisRuntimeSnapshot(); len(unresolved) != 0 {
		return true, errors.Join(failures,
			fmt.Errorf("Redis graceful teardown retained %d unresolved runtime identities", len(unresolved)))
	}
	if failures != nil {
		return true, failures
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/networks/"+url.PathEscape(network.ID), nil)
	if err != nil {
		return true, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return true, err
	}
	status := response.StatusCode
	closeErr := response.Body.Close()
	if closeErr != nil {
		return true, fmt.Errorf("close Redis network delete response: %w", closeErr)
	}
	if status != http.StatusNoContent {
		return true, fmt.Errorf("delete exact Redis network returned HTTP %d", status)
	}
	return true, nil
}

// DeleteRedisExact removes only the validated container and immutable volume.
// Docker does not provide an atomic compare-and-delete for named volumes, so
// this remains a cooperative-daemon boundary after the final re-inspection.
func (sm *ServiceManager) DeleteRedisExact(ctx context.Context, spec RedisBackendSpec) error {
	sm.emulatorMu.Lock()
	defer sm.emulatorMu.Unlock()
	if err := sm.deleteRedisExactLocked(ctx, spec); err != nil {
		return err
	}
	sm.clearRedisRuntime(spec.ResourceID)
	return nil
}

func (sm *ServiceManager) deleteRedisExactLocked(ctx context.Context, spec RedisBackendSpec) error {
	if err := validateRedisBackendSpec(spec, false); err != nil {
		return err
	}
	containerName, volumeName := redisDockerNames(spec.ResourceID)
	container, containerFound, err := sm.inspectRedisContainer(ctx, containerName)
	if err != nil {
		return err
	}
	volume, volumeFound, err := sm.inspectRedisVolume(ctx, volumeName)
	if err != nil {
		return err
	}
	if !containerFound && !volumeFound {
		return nil
	}
	if spec.VolumeIdentity == "" {
		return errors.New("Redis volume identity is required before destructive cleanup")
	}
	if !volumeFound {
		return fmt.Errorf("exact Redis volume %q is missing", volumeName)
	}
	if _, err := validateRedisVolume(volume, volumeName, spec); err != nil {
		return err
	}
	if containerFound {
		if err := validateRedisContainer(container, containerName, volumeName, spec); err != nil {
			return err
		}
		expectedNetworkID := ""
		if identity, ok := sm.redisRuntimeFor(spec.ResourceID); ok {
			expectedNetworkID = identity.NetworkID
		}
		if _, err := sm.validateRedisContainerNetwork(
			ctx, container, containerName, expectedNetworkID); err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://localhost/containers/"+url.PathEscape(container.ID)+"?force=true", nil)
		if err != nil {
			return err
		}
		response, err := sm.doDocker(request)
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("delete Redis container returned HTTP %d", response.StatusCode)
		}
	}
	rechecked, found, err := sm.inspectRedisVolume(ctx, volumeName)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if _, err := validateRedisVolume(rechecked, volumeName, spec); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/volumes/"+url.PathEscape(volumeName), nil)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete Redis volume returned HTTP %d", response.StatusCode)
	}
	return nil
}
