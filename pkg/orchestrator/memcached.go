package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/config"
)

const (
	memcachedContainerPort      = "11211/tcp"
	memcacheCompensationTimeout = 10 * time.Second
	memcacheSupportedNodeCount  = 1
)

var (
	memcacheBackendIDPattern = regexp.MustCompile(`^memcache-[0-9a-f]{32}$`)
	memcacheProfilePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	dockerContainerIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type dockerPortBinding struct {
	HostIP   string `json:"HostIp"`
	HostPort string `json:"HostPort"`
}

type memcachedContainerInspect struct {
	ID    string `json:"Id"`
	State struct {
		Status string `json:"Status"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
		Image  string            `json:"Image"`
	} `json:"Config"`
	NetworkSettings struct {
		Ports map[string][]dockerPortBinding `json:"Ports"`
	} `json:"NetworkSettings"`
}

type memcacheBackendState struct {
	owned  bool
	exists bool
}

// ProvisionMemcache implements the shim's standard-library-only backend
// contract. One local container represents exactly one local node.
func (sm *ServiceManager) ProvisionMemcache(
	ctx context.Context,
	resourceID string,
	nodeCount int,
	cpuCount int,
	memoryMB int,
	version string,
	params map[string]string,
) ([]string, bool, bool, error) {
	if err := validateMemcacheBackendSpec(resourceID, nodeCount, cpuCount, memoryMB, version, params); err != nil {
		return nil, false, false, err
	}
	image, err := memcacheImageForVersion(version)
	if err != nil {
		return nil, false, false, err
	}
	protocolVersion, err := memcacheProtocolVersion(version)
	if err != nil {
		return nil, false, false, err
	}
	state := memcacheBackendState{}
	endpoint, err := sm.provisionMemcache(ctx, resourceID, image, protocolVersion, &state)
	if err != nil {
		return nil, state.owned, state.exists, err
	}
	return []string{endpoint}, true, true, nil
}

func (sm *ServiceManager) provisionMemcache(
	ctx context.Context,
	resourceID string,
	image string,
	protocolVersion string,
	state *memcacheBackendState,
) (endpoint string, resultErr error) {
	name := memcachedDockerName(resourceID)
	container, found, err := sm.inspectMemcachedContainer(ctx, name)
	if err != nil {
		return "", fmt.Errorf("inspect Memcached container: %w", err)
	}
	if found {
		state.exists = true
		state.owned = exactLabels(container.Config.Labels, memcachedLabels(resourceID))
		endpoint, err := sm.activateMemcachedContainer(
			ctx, container, resourceID, image, protocolVersion,
		)
		if err != nil && state.owned {
			sm.markMemcacheUncertain(resourceID, err)
		}
		if err == nil {
			sm.clearMemcacheUncertain(resourceID)
		}
		return endpoint, err
	}

	if strings.TrimSpace(image) == "" {
		return "", errors.New("Memcached image is not configured")
	}
	exists, err := sm.imageExistsContext(ctx, image)
	if err != nil {
		return "", fmt.Errorf("inspect Memcached image: %w", err)
	}
	if !exists {
		if err := sm.pullImageInternal(ctx, image); err != nil {
			return "", fmt.Errorf("pull Memcached image %q: %w", image, err)
		}
	}

	payload, err := json.Marshal(map[string]any{
		"Image": image,
		"ExposedPorts": map[string]any{
			memcachedContainerPort: struct{}{},
		},
		"HostConfig": map[string]any{
			"PortBindings": map[string]any{
				memcachedContainerPort: []map[string]string{{
					"HostIp": "127.0.0.1", "HostPort": "0",
				}},
			},
		},
		"Labels": memcachedLabels(resourceID),
	})
	if err != nil {
		return "", fmt.Errorf("encode Memcached container configuration: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?"+url.Values{"name": {name}}.Encode(),
		bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("build Memcached create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	createdID := ""
	defer func() {
		if resultErr == nil || createdID == "" {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), memcacheCompensationTimeout)
		defer cancel()
		owned, exists, cleanupErr := sm.compensateMemcacheCreate(cleanupCtx, name, createdID, resourceID)
		state.owned = owned
		state.exists = exists
		if cleanupErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("compensate Memcached create: %w", cleanupErr))
		}
	}()
	response, requestErr := sm.doDocker(request)
	if requestErr != nil {
		return sm.recoverUncapturedMemcacheCreate(
			ctx, name, resourceID, image, protocolVersion, state,
			fmt.Errorf("create Memcached container: %w", requestErr),
		)
	}
	var create struct {
		ID string `json:"Id"`
	}
	if response.StatusCode == http.StatusCreated {
		err = json.NewDecoder(response.Body).Decode(&create)
	}
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		detail := fmt.Errorf("create Memcached container returned HTTP %d", response.StatusCode)
		return sm.recoverUncapturedMemcacheCreate(
			ctx, name, resourceID, image, protocolVersion, state, errors.Join(detail, closeErr),
		)
	}
	if err != nil {
		return sm.recoverUncapturedMemcacheCreate(
			ctx, name, resourceID, image, protocolVersion, state,
			fmt.Errorf("decode Memcached create response: %w", err),
		)
	}
	if create.ID == "" {
		return sm.recoverUncapturedMemcacheCreate(
			ctx, name, resourceID, image, protocolVersion, state,
			errors.New("Memcached create response returned no immutable container ID"),
		)
	}
	if !dockerContainerIDPattern.MatchString(create.ID) {
		return sm.recoverUncapturedMemcacheCreate(
			ctx, name, resourceID, image, protocolVersion, state,
			fmt.Errorf("Memcached create response returned malformed immutable container ID %q", create.ID),
		)
	}
	createdID = create.ID
	if closeErr != nil {
		return "", fmt.Errorf("close Memcached create response: %w", closeErr)
	}

	created, found, err := sm.inspectMemcachedContainer(ctx, create.ID)
	if err != nil {
		return "", fmt.Errorf("inspect created Memcached container: %w", err)
	}
	if !found {
		return "", errors.New("created Memcached container disappeared before start")
	}
	endpoint, err = sm.activateMemcachedContainer(
		ctx, created, resourceID, image, protocolVersion,
	)
	if err != nil {
		return "", err
	}
	createdID = ""
	state.owned = true
	state.exists = true
	return endpoint, nil
}

// UpdateMemcache supports the bounded one-node local contract. Since Memcached
// data is ephemeral and the local backend has no independently scalable nodes,
// a supported update reconciles the exact-owned single container in place.
func (sm *ServiceManager) UpdateMemcache(
	ctx context.Context,
	resourceID string,
	nodeCount int,
	cpuCount int,
	memoryMB int,
	version string,
	params map[string]string,
) ([]string, bool, bool, error) {
	if err := validateMemcacheBackendSpec(resourceID, nodeCount, cpuCount, memoryMB, version, params); err != nil {
		return nil, false, false, err
	}
	image, err := memcacheImageForVersion(version)
	if err != nil {
		return nil, false, false, err
	}
	protocolVersion, err := memcacheProtocolVersion(version)
	if err != nil {
		return nil, false, false, err
	}
	return sm.reconcileMemcache(ctx, resourceID, image, protocolVersion)
}

// ReconcileMemcache validates the persisted expected spec before observing the
// backend. It reports Exists=false for true absence, may restart only an
// exact-owned stopped container, and requires the exact image and protocol
// version before returning READY-compatible endpoints.
func (sm *ServiceManager) ReconcileMemcache(
	ctx context.Context,
	resourceID string,
	nodeCount int,
	cpuCount int,
	memoryMB int,
	version string,
	params map[string]string,
) ([]string, bool, bool, error) {
	if err := validateMemcacheBackendSpec(resourceID, nodeCount, cpuCount, memoryMB, version, params); err != nil {
		return nil, false, false, err
	}
	image, err := memcacheImageForVersion(version)
	if err != nil {
		return nil, false, false, err
	}
	protocolVersion, err := memcacheProtocolVersion(version)
	if err != nil {
		return nil, false, false, err
	}
	return sm.reconcileMemcache(ctx, resourceID, image, protocolVersion)
}

func (sm *ServiceManager) reconcileMemcache(
	ctx context.Context,
	resourceID string,
	expectedImage string,
	protocolVersion string,
) ([]string, bool, bool, error) {
	container, found, err := sm.inspectMemcachedContainer(ctx, memcachedDockerName(resourceID))
	if err != nil {
		return nil, false, false, fmt.Errorf("inspect Memcached container: %w", err)
	}
	if !found {
		sm.clearMemcacheUncertain(resourceID)
		return nil, false, false, nil
	}
	if !exactLabels(container.Config.Labels, memcachedLabels(resourceID)) {
		sm.clearMemcacheUncertain(resourceID)
		return nil, false, true, fmt.Errorf("%w: Memcached container %q",
			ErrDockerOwnershipConflict, memcachedDockerName(resourceID))
	}
	endpoint, err := sm.activateMemcachedContainer(
		ctx, container, resourceID, expectedImage, protocolVersion,
	)
	if err != nil {
		sm.markMemcacheUncertain(resourceID, err)
		return nil, true, true, err
	}
	sm.clearMemcacheUncertain(resourceID)
	return []string{endpoint}, true, true, nil
}

// DeleteMemcache removes only the immutable container ID proven to carry the
// exact ownership labels. True absence and a remove-after-inspect race are
// idempotent successes.
func (sm *ServiceManager) DeleteMemcache(ctx context.Context, resourceID string) error {
	if err := validateMemcacheBackendIdentity(resourceID); err != nil {
		return err
	}
	if uncertain := sm.memcacheUncertainty(resourceID); uncertain != nil {
		return fmt.Errorf("Memcached backend ownership is uncertain; reconcile before deletion: %w", uncertain)
	}
	name := memcachedDockerName(resourceID)
	container, found, err := sm.inspectMemcachedContainer(ctx, name)
	if err != nil {
		return fmt.Errorf("inspect Memcached container: %w", err)
	}
	if !found {
		return nil
	}
	if !exactLabels(container.Config.Labels, memcachedLabels(resourceID)) {
		return fmt.Errorf("%w: refusing to delete Memcached container %q",
			ErrDockerOwnershipConflict, name)
	}
	if container.ID == "" {
		return errors.New("refusing to delete Memcached container without immutable ID")
	}
	return sm.deleteMemcacheContainerByID(ctx, container.ID)
}

func (sm *ServiceManager) recoverUncapturedMemcacheCreate(
	ctx context.Context,
	name string,
	resourceID string,
	expectedImage string,
	protocolVersion string,
	state *memcacheBackendState,
	createErr error,
) (string, error) {
	container, found, err := sm.inspectMemcachedContainer(ctx, name)
	if err != nil {
		state.owned = true
		state.exists = true
		uncertainErr := errors.Join(createErr, fmt.Errorf("inspect ambiguous Memcached create: %w", err))
		sm.markMemcacheUncertain(resourceID, uncertainErr)
		return "", uncertainErr
	}
	if !found {
		state.owned = false
		state.exists = false
		sm.clearMemcacheUncertain(resourceID)
		return "", createErr
	}
	state.exists = true
	if !exactLabels(container.Config.Labels, memcachedLabels(resourceID)) {
		state.owned = false
		sm.clearMemcacheUncertain(resourceID)
		return "", errors.Join(createErr, fmt.Errorf("%w: Memcached container %q",
			ErrDockerOwnershipConflict, name))
	}
	state.owned = true
	endpoint, err := sm.activateMemcachedContainer(
		ctx, container, resourceID, expectedImage, protocolVersion,
	)
	if err != nil {
		recoveryErr := errors.Join(createErr, fmt.Errorf("recover exact-owned Memcached create: %w", err))
		sm.markMemcacheUncertain(resourceID, recoveryErr)
		return "", recoveryErr
	}
	sm.clearMemcacheUncertain(resourceID)
	return endpoint, nil
}

func (sm *ServiceManager) compensateMemcacheCreate(
	ctx context.Context,
	name string,
	createdID string,
	resourceID string,
) (owned bool, exists bool, result error) {
	if !dockerContainerIDPattern.MatchString(createdID) {
		return true, true, fmt.Errorf("refusing to compensate uncaptured Memcached container %q", name)
	}
	container, found, err := sm.inspectMemcachedContainer(ctx, createdID)
	if err != nil {
		return true, true, fmt.Errorf("inspect captured Memcached container: %w", err)
	}
	if !found {
		return false, false, nil
	}
	if !exactLabels(container.Config.Labels, memcachedLabels(resourceID)) {
		return false, true, fmt.Errorf("%w: refusing to compensate Memcached container %q",
			ErrDockerOwnershipConflict, name)
	}
	if !dockerContainerIDPattern.MatchString(container.ID) {
		return true, true, fmt.Errorf("refusing to compensate malformed immutable container ID %q", container.ID)
	}
	if createdID != "" && container.ID != createdID {
		return true, true, fmt.Errorf("%w: Memcached immutable ID changed during compensation",
			ErrDockerOwnershipConflict)
	}
	if err := sm.deleteMemcacheContainerByID(ctx, container.ID); err != nil {
		return true, true, err
	}
	return false, false, nil
}

func (sm *ServiceManager) deleteMemcacheContainerByID(ctx context.Context, id string) (result error) {
	if !dockerContainerIDPattern.MatchString(id) {
		return fmt.Errorf("refusing to delete malformed immutable Memcached container ID %q", id)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/containers/"+url.PathEscape(id)+"?force=true", nil)
	if err != nil {
		return fmt.Errorf("build Memcached delete request: %w", err)
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return fmt.Errorf("delete Memcached container: %w", err)
	}
	defer func() {
		result = errors.Join(result, response.Body.Close())
	}()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("delete Memcached container returned HTTP %d: %s",
			response.StatusCode, strings.TrimSpace(string(detail)))
	}
	return nil
}

func (sm *ServiceManager) activateMemcachedContainer(
	ctx context.Context,
	container memcachedContainerInspect,
	resourceID string,
	expectedImage string,
	protocolVersion string,
) (string, error) {
	if !exactLabels(container.Config.Labels, memcachedLabels(resourceID)) {
		return "", fmt.Errorf("%w: Memcached container %q",
			ErrDockerOwnershipConflict, memcachedDockerName(resourceID))
	}
	if container.ID == "" {
		return "", errors.New("Memcached container inspect returned no immutable ID")
	}
	if !dockerContainerIDPattern.MatchString(container.ID) {
		return "", fmt.Errorf("Memcached container inspect returned malformed immutable ID %q", container.ID)
	}
	if expectedImage == "" || container.Config.Image != expectedImage {
		return "", fmt.Errorf(
			"exact-owned Memcached container image %q does not match requested image %q",
			container.Config.Image,
			expectedImage,
		)
	}
	switch container.State.Status {
	case "created", "exited":
		if err := sm.startMemcachedContainer(ctx, container.ID); err != nil {
			return "", err
		}
		var found bool
		var err error
		container, found, err = sm.inspectMemcachedContainer(ctx, container.ID)
		if err != nil {
			return "", fmt.Errorf("inspect restarted Memcached container: %w", err)
		}
		if !found {
			return "", errors.New("exact-owned Memcached container disappeared during restart")
		}
		if !exactLabels(container.Config.Labels, memcachedLabels(resourceID)) {
			return "", fmt.Errorf("%w: Memcached container changed ownership during restart",
				ErrDockerOwnershipConflict)
		}
		if container.Config.Image != expectedImage {
			return "", fmt.Errorf(
				"restarted Memcached container image %q does not match requested image %q",
				container.Config.Image,
				expectedImage,
			)
		}
	case "running":
	default:
		return "", fmt.Errorf("exact-owned Memcached container is %s", container.State.Status)
	}
	if container.State.Status != "running" {
		return "", fmt.Errorf("exact-owned Memcached container is %s after start", container.State.Status)
	}
	endpoint, err := memcachedEndpoint(container)
	if err != nil {
		return "", err
	}
	ready := sm.memcachedReady
	if ready == nil {
		ready = waitUntilMemcachedReady
	}
	if err := ready(ctx, endpoint, protocolVersion, 30*time.Second); err != nil {
		return "", fmt.Errorf("Memcached readiness failed: %w", err)
	}
	return endpoint, nil
}

func (sm *ServiceManager) startMemcachedContainer(ctx context.Context, id string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/"+url.PathEscape(id)+"/start", nil)
	if err != nil {
		return fmt.Errorf("build Memcached start request: %w", err)
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return fmt.Errorf("start Memcached container: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotModified {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("start Memcached container returned HTTP %d: %s",
			response.StatusCode, strings.TrimSpace(string(detail)))
	}
	return nil
}

func (sm *ServiceManager) inspectMemcachedContainer(
	ctx context.Context,
	identity string,
) (container memcachedContainerInspect, found bool, result error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(identity)+"/json", nil)
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
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return container, false, fmt.Errorf("Docker returned HTTP %d: %s",
			response.StatusCode, strings.TrimSpace(string(detail)))
	}
	if err := json.NewDecoder(response.Body).Decode(&container); err != nil {
		return container, false, fmt.Errorf("decode Docker container inspect: %w", err)
	}
	if container.ID == "" {
		return container, false, errors.New("Docker container inspect returned no immutable ID")
	}
	if !dockerContainerIDPattern.MatchString(container.ID) {
		return container, false, fmt.Errorf("Docker container inspect returned malformed immutable ID %q", container.ID)
	}
	return container, true, nil
}

func memcachedEndpoint(container memcachedContainerInspect) (string, error) {
	bindings := container.NetworkSettings.Ports[memcachedContainerPort]
	if len(bindings) != 1 {
		return "", fmt.Errorf("Memcached container has %d host bindings for %s, want exactly one",
			len(bindings), memcachedContainerPort)
	}
	host := net.ParseIP(bindings[0].HostIP)
	if host == nil || !host.IsLoopback() {
		return "", fmt.Errorf("Memcached host binding %q is not loopback", bindings[0].HostIP)
	}
	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("Memcached host port %q is invalid", bindings[0].HostPort)
	}
	return net.JoinHostPort(bindings[0].HostIP, strconv.Itoa(port)), nil
}

func memcachedDockerName(resourceID string) string {
	hash := sha256.Sum256([]byte(config.GetProfile() + "\x00" + resourceID))
	return fmt.Sprintf("minisky-memcached-%x", hash[:8])
}

func memcachedLabels(resourceID string) map[string]string {
	return map[string]string{
		"managed-by":       "minisky",
		"minisky.profile":  config.GetProfile(),
		"minisky.service":  "memorystore-memcached",
		"minisky.resource": resourceID,
	}
}

func (sm *ServiceManager) markMemcacheUncertain(resourceID string, err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.memcacheUncertain == nil {
		sm.memcacheUncertain = make(map[string]error)
	}
	sm.memcacheUncertain[resourceID] = err
}

func (sm *ServiceManager) clearMemcacheUncertain(resourceID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.memcacheUncertain, resourceID)
}

func (sm *ServiceManager) memcacheUncertainty(resourceID string) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.memcacheUncertain[resourceID]
}

func memcacheImageForVersion(version string) (string, error) {
	configVersion, err := memcacheProtocolVersion(version)
	if err != nil {
		return "", err
	}
	for _, candidate := range config.GetImageRegistry().Memorystore.Memcached.Versions {
		if candidate.Version == configVersion && strings.TrimSpace(candidate.Image) != "" {
			return candidate.Image, nil
		}
	}
	return "", fmt.Errorf("Memcached version %q has no configured image", version)
}

func memcacheProtocolVersion(version string) (string, error) {
	switch version {
	case "MEMCACHE_1_5":
		return "1.5.16", nil
	case "MEMCACHE_1_6_15":
		return "1.6.15", nil
	default:
		return "", fmt.Errorf("unsupported Memcached version %q", version)
	}
}

func validateMemcacheBackendIdentity(resourceID string) error {
	if !memcacheBackendIDPattern.MatchString(resourceID) {
		return fmt.Errorf("invalid Memcached backend resource ID %q", resourceID)
	}
	profile := config.GetProfile()
	if !memcacheProfilePattern.MatchString(profile) || profile == "." || profile == ".." {
		return fmt.Errorf("invalid active MiniSky profile %q", profile)
	}
	return nil
}

func validateMemcacheBackendSpec(
	resourceID string,
	nodeCount int,
	cpuCount int,
	memoryMB int,
	version string,
	params map[string]string,
) error {
	if err := validateMemcacheBackendIdentity(resourceID); err != nil {
		return err
	}
	if nodeCount != memcacheSupportedNodeCount {
		return fmt.Errorf("local Memcached backend supports exactly one local node, got %d", nodeCount)
	}
	if cpuCount < 1 || memoryMB < 1 {
		return errors.New("local Memcached backend requires positive CPU and memory values")
	}
	if version != "MEMCACHE_1_5" && version != "MEMCACHE_1_6_15" {
		return fmt.Errorf("unsupported Memcached version %q", version)
	}
	if len(params) != 0 {
		return errors.New("local Memcached backend does not support custom parameters")
	}
	return nil
}

func waitUntilMemcachedReady(
	ctx context.Context,
	endpoint string,
	expectedVersion string,
	timeout time.Duration,
) error {
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
			if _, err = io.WriteString(connection, "version\r\n"); err == nil {
				var response string
				response, err = bufio.NewReader(io.LimitReader(connection, 512)).ReadString('\n')
				if err == nil && response == "VERSION "+expectedVersion+"\r\n" {
					_ = connection.Close()
					return nil
				}
			}
			_ = connection.Close()
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return waitCtx.Err()
		case <-timer.C:
		}
	}
}
