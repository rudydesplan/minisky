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
	"log"
	"minisky/pkg/config"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	networkName            = "minisky-net"
	dockerImagePullTimeout = 2 * time.Minute
)

var ErrServerlessLifecycleInProgress = errors.New("Serverless lifecycle already in progress")

// ServiceManager handles native REST-driven lifecycle events over the Docker Unix Socket.
type ServiceManager struct {
	mu               sync.RWMutex
	serverlessMu     sync.Mutex
	dockerClient     *http.Client
	sockPath         string
	portRegistry     map[string][]PortMapping   // containerName → host ports
	fwRules          map[string][]FirewallEntry // vpcName → rules
	serverlessActive map[ServerlessIdentity]struct{}
	serverlessReady  func(string, time.Duration) error
}

// ContainerConfig describes one backend emulator container.
type ContainerConfig struct {
	Name            string
	Image           string
	ContainerPort   string // e.g. "4443/tcp"
	AdditionalPorts []string
	Cmd             []string
	Volume          string
	Env             []string
}

// PortMapping tracks a host:container port pair for a VM.
type PortMapping struct {
	ContainerPort string
	HostPort      string
	Protocol      string
}

// FirewallEntry is a simplified snapshot for Level-3 enforcement.
type FirewallEntry struct {
	Name      string
	VpcName   string
	Direction string   // INGRESS, EGRESS
	Action    string   // allow, deny
	Protocol  string   // tcp, udp, icmp, all
	Ports     []string // empty = all ports
	Ranges    []string // CIDR source/dest ranges
}

func NewServiceManager() (*ServiceManager, error) {
	sockPath := resolveDockerSocket()
	// On Unix, ensure DOCKER_HOST is set if we found a socket
	if !strings.HasPrefix(sockPath, "//./pipe/") && os.Getenv("DOCKER_HOST") == "" {
		os.Setenv("DOCKER_HOST", "unix://"+sockPath)
	}
	log.Printf("[ServiceManager] Docker socket resolved: %s", sockPath)
	sm := &ServiceManager{
		sockPath:         sockPath,
		portRegistry:     make(map[string][]PortMapping),
		fwRules:          make(map[string][]FirewallEntry),
		serverlessActive: make(map[ServerlessIdentity]struct{}),
		serverlessReady:  waitUntilHTTPReady,
	}
	transport := &http.Transport{
		DialContext: sm.dialDocker,
	}
	sm.dockerClient = &http.Client{Transport: transport}
	return sm, nil
}

// EnsureNetwork creates the isolated minisky-net bridge network if it doesn't exist.
func (sm *ServiceManager) EnsureNetwork(ctx context.Context) error {
	// Check if it already exists
	resp, err := sm.dockerClient.Get("http://localhost/networks/" + networkName)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var network struct {
			Labels map[string]string `json:"Labels"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&network); err != nil {
			return fmt.Errorf("inspect existing network ownership: %w", err)
		}
		if !isOwnedDockerResource(network.Labels) {
			return fmt.Errorf("network %q exists but is not owned by active MiniSky profile", networkName)
		}
		log.Printf("[Orchestrator] Network '%s' already exists.", networkName)
		return nil
	}
	if resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("network inspect failed with status %d", resp.StatusCode)
	}

	// Create isolated bridge network
	payload := map[string]interface{}{
		"Name":   networkName,
		"Driver": "bridge",
		"Labels": ownedDockerLabels(),
	}
	data, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://localhost/networks/create", bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	createResp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer createResp.Body.Close()
	if createResp.StatusCode >= 400 {
		b, _ := io.ReadAll(createResp.Body)
		return fmt.Errorf("network create failed %d: %s", createResp.StatusCode, b)
	}
	log.Printf("[Orchestrator] Created isolated network '%s'.", networkName)
	return nil
}

// EnsureServiceRunning boots the container if needed and returns its internal bridge URL.
func (sm *ServiceManager) EnsureServiceRunning(ctx context.Context, domain string, env ...string) (string, error) {
	reg := config.GetImageRegistry()
	cfg, exists := reg.Emulators[domain]
	if !exists {
		// Native Go shims never need Docker containers
		return "", nil
	}

	// Map config to internal ContainerConfig
	iconfig := ContainerConfig{
		Name:            cfg.Name,
		Image:           cfg.Image,
		ContainerPort:   cfg.Port,
		AdditionalPorts: cfg.AdditionalPorts,
		Cmd:             cfg.Cmd,
		Volume:          cfg.Volume,
		Env:             env,
	}
	volume, err := resolveEmulatorVolume(domain, iconfig.Volume)
	if err != nil {
		return "", err
	}
	iconfig.Volume = volume

	status, labels, err := sm.inspectContainer(cfg.Name)
	if err != nil {
		return "", fmt.Errorf("status check failed: %v", err)
	}
	if status != "not_found" && !isOwnedDockerResource(labels) {
		return "", fmt.Errorf("container %q exists but is not owned by active MiniSky profile", cfg.Name)
	}

	if status != "running" {
		log.Printf("[Orchestrator] Cold-starting '%s' for domain '%s'...", iconfig.Name, domain)
		if status == "not_found" {
			exists, err := sm.ImageExistsPublic(iconfig.Image)
			if err != nil {
				log.Printf("[Orchestrator] Image check error: %v", err)
			}
			if !exists {
				log.Printf("[Orchestrator] Pulling image '%s'...", iconfig.Image)
				if err := sm.pullImageInternal(ctx, iconfig.Image); err != nil {
					return "", fmt.Errorf("pull image %q: %w", iconfig.Image, err)
				}
				log.Printf("[Orchestrator] Image '%s' pull complete.", iconfig.Image)
			} else {
				log.Printf("[Orchestrator] Image '%s' already exists locally, skipping pull.", iconfig.Image)
			}
			log.Printf("[Orchestrator] Creating container '%s'...", iconfig.Name)
			if err := sm.createContainer(iconfig); err != nil {
				return "", fmt.Errorf("create container: %v", err)
			}
			log.Printf("[Orchestrator] Container '%s' created.", iconfig.Name)
		}
		log.Printf("[Orchestrator] Starting container '%s'...", iconfig.Name)
		if err := sm.startContainer(iconfig.Name); err != nil {
			return "", fmt.Errorf("start container: %v", err)
		}
		log.Printf("[Orchestrator] Container '%s' started.", iconfig.Name)
	}

	// Discover the internal bridge IP — no host port binding needed
	log.Printf("[Orchestrator] Discovering internal URL for '%s'...", iconfig.Name)
	internalURL, err := sm.discoverInternalURL(iconfig)
	if err != nil {
		return "", fmt.Errorf("port discovery: %v", err)
	}

	// Wait until the emulator is truly ready inside the network
	containerPort := strings.Split(iconfig.ContainerPort, "/")[0]
	log.Printf("[Orchestrator] Waiting for HTTP readiness probe at %s...", internalURL)
	if err := waitUntilHTTPReady(internalURL, 60*time.Second); err != nil {
		return "", fmt.Errorf("readiness probe failed: %v", err)
	}

	log.Printf("[Orchestrator] ✅ '%s' is ONLINE at internal %s (port %s)", iconfig.Name, internalURL, containerPort)
	return internalURL, nil
}

// StopServiceContainer stops the underlying docker container for a given service domain.
func (sm *ServiceManager) StopAndRemoveContainer(name string) error {
	// 1. Stop
	stopURL := fmt.Sprintf("http://localhost/containers/%s/stop", name)
	reqStop, _ := http.NewRequest("POST", stopURL, nil)
	respStop, err := sm.dockerClient.Do(reqStop)
	if err == nil {
		respStop.Body.Close()
	}

	// 2. Remove
	rmURL := fmt.Sprintf("http://localhost/containers/%s?force=true", name)
	reqRm, _ := http.NewRequest("DELETE", rmURL, nil)
	respRm, err := sm.dockerClient.Do(reqRm)
	if err != nil {
		return err
	}
	defer respRm.Body.Close()
	return nil
}

func (sm *ServiceManager) StopServiceContainer(ctx context.Context, domain string) error {
	reg := config.GetImageRegistry()
	cfg, exists := reg.Emulators[domain]
	if !exists {
		return fmt.Errorf("domain %s not found in registry", domain)
	}

	status, err := sm.checkStatus(cfg.Name)
	if err != nil {
		return fmt.Errorf("status check failed: %v", err)
	}

	if status == "running" {
		log.Printf("[Orchestrator] Stopping service container '%s'...", cfg.Name)
		stopURL := fmt.Sprintf("http://localhost/containers/%s/stop", cfg.Name)
		req, _ := http.NewRequestWithContext(ctx, "POST", stopURL, nil)
		resp, err := sm.dockerClient.Do(req)
		if err != nil {
			return fmt.Errorf("stop container network error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotModified {
			return fmt.Errorf("stop rejected %d", resp.StatusCode)
		}
		log.Printf("[Orchestrator] Container '%s' stopped successfully.", cfg.Name)
	} else {
		log.Printf("[Orchestrator] Container '%s' is already not running (status: %s)", cfg.Name, status)
	}

	return nil
}

// discoverInternalURL reads the host-bound port assigned by Docker and returns
// a localhost URL reachable from the host (compatible with Docker Desktop / VM environments).
func (sm *ServiceManager) discoverInternalURL(config ContainerConfig) (string, error) {
	resp, err := sm.dockerClient.Get(fmt.Sprintf("http://localhost/containers/%s/json", config.Name))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var info struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIp   string
				HostPort string
			}
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "", err
	}

	bindings, ok := info.NetworkSettings.Ports[config.ContainerPort]
	if !ok || len(bindings) == 0 || bindings[0].HostPort == "" {
		return "", fmt.Errorf("container '%s' has no host port binding for %s", config.Name, config.ContainerPort)
	}

	return fmt.Sprintf("http://127.0.0.1:%s", bindings[0].HostPort), nil
}

// GetContainerHostPort reads the host-bound port assigned by Docker.
func (sm *ServiceManager) GetContainerHostPort(containerName string, containerPort string) (string, error) {
	resp, err := sm.dockerClient.Get(fmt.Sprintf("http://localhost/containers/%s/json", containerName))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var info struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIp   string
				HostPort string
			}
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "", err
	}

	bindings, ok := info.NetworkSettings.Ports[containerPort]
	if !ok || len(bindings) == 0 || bindings[0].HostPort == "" {
		return "", fmt.Errorf("container '%s' has no host port binding for %s", containerName, containerPort)
	}

	return bindings[0].HostPort, nil
}

// Teardown stops and removes only resources whose labels prove ownership by the
// active profile. Name matches alone are never sufficient for destructive work.
func (sm *ServiceManager) Teardown(ctx context.Context) {
	reg := config.GetImageRegistry()
	for _, cfg := range reg.Emulators {
		status, labels, err := sm.inspectContainer(cfg.Name)
		if err != nil || status == "not_found" || !isOwnedDockerResource(labels) {
			continue
		}
		stopURL := fmt.Sprintf("http://localhost/containers/%s/stop", cfg.Name)
		req, _ := http.NewRequestWithContext(ctx, "POST", stopURL, nil)
		sm.dockerClient.Do(req)

		rmURL := fmt.Sprintf("http://localhost/containers/%s?force=true", cfg.Name)
		req, _ = http.NewRequestWithContext(ctx, "DELETE", rmURL, nil)
		sm.dockerClient.Do(req)
		log.Printf("[Orchestrator] Removed container '%s'", cfg.Name)
	}
	resp, err := sm.dockerClient.Get("http://localhost/networks/" + networkName)
	if err == nil {
		defer resp.Body.Close()
		var network struct {
			Labels map[string]string `json:"Labels"`
		}
		if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&network) == nil &&
			isOwnedDockerResource(network.Labels) {
			rmNetURL := "http://localhost/networks/" + networkName
			req, _ := http.NewRequestWithContext(ctx, "DELETE", rmNetURL, nil)
			sm.dockerClient.Do(req)
			log.Printf("[Orchestrator] Removed network '%s'", networkName)
		}
	}
}

// PruneExitedContainers removes exited containers owned by the active profile.
func (sm *ServiceManager) PruneExitedContainers(ctx context.Context) error {
	resp, err := sm.dockerClient.Get("http://localhost/containers/json?all=true&filters={\"status\":[\"exited\",\"created\",\"dead\"]}")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var containers []struct {
		Id     string
		Names  []string
		Labels map[string]string
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return err
	}

	for _, c := range containers {
		if !isOwnedDockerResource(c.Labels) {
			continue
		}
		name := "unknown"
		if len(c.Names) > 0 {
			name = c.Names[0]
		}
		log.Printf("[Orchestrator] Pruning container: %s (%s)", name, c.Id)
		rmURL := fmt.Sprintf("http://localhost/containers/%s?force=true", c.Id)
		req, _ := http.NewRequestWithContext(ctx, "DELETE", rmURL, nil)
		sm.dockerClient.Do(req)
	}
	return nil
}

// PruneUnusedImages removes all minisky-fn-* and minisky-svc-* images that are not used by any container.
func (sm *ServiceManager) PruneUnusedImages(ctx context.Context) error {
	// 1. Get all containers to see which images are in use
	resp, err := sm.dockerClient.Get("http://localhost/containers/json?all=true")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var containers []struct {
		Image   string
		ImageID string
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return err
	}

	usedImages := make(map[string]bool)
	for _, c := range containers {
		usedImages[c.Image] = true
		usedImages[c.ImageID] = true
	}

	// 2. List all images
	imgResp, err := sm.dockerClient.Get("http://localhost/images/json")
	if err != nil {
		return err
	}
	defer imgResp.Body.Close()

	var images []struct {
		Id       string
		RepoTags []string
	}
	if err := json.NewDecoder(imgResp.Body).Decode(&images); err != nil {
		return err
	}

	for _, img := range images {
		isMiniSky := false
		tagName := ""
		for _, tag := range img.RepoTags {
			if strings.Contains(tag, "minisky-fn-") || strings.Contains(tag, "minisky-svc-") {
				isMiniSky = true
				tagName = tag
				break
			}
		}

		if isMiniSky {
			// Check if used
			if usedImages[img.Id] || (tagName != "" && usedImages[tagName]) {
				continue
			}

			log.Printf("[Orchestrator] Pruning unused MiniSky image: %s (%s)", tagName, img.Id)
			rmURL := fmt.Sprintf("http://localhost/images/%s?force=true", img.Id)
			req, _ := http.NewRequestWithContext(ctx, "DELETE", rmURL, nil)
			sm.dockerClient.Do(req)
		}
	}

	return nil
}

func (sm *ServiceManager) checkStatus(name string) (string, error) {
	status, _, err := sm.inspectContainer(name)
	return status, err
}

func (sm *ServiceManager) inspectContainer(name string) (string, map[string]string, error) {
	resp, err := sm.dockerClient.Get(fmt.Sprintf("http://localhost/containers/%s/json", name))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "not_found", nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("container inspect failed with status %d", resp.StatusCode)
	}
	var inspected struct {
		State struct {
			Status string `json:"Status"`
		} `json:"State"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&inspected); err != nil {
		return "", nil, err
	}
	return inspected.State.Status, inspected.Config.Labels, nil
}

// CheckStatusPublic allows external packages to see if a container is running.
func (sm *ServiceManager) CheckStatusPublic(name string) (string, error) {
	return sm.checkStatus(name)
}

func (sm *ServiceManager) pullImageInternal(ctx context.Context, image string) error {
	pullCtx, cancel := context.WithTimeout(ctx, dockerImagePullTimeout)
	defer cancel()

	endpoint := "http://localhost/images/create?" + url.Values{"fromImage": {image}}.Encode()
	req, err := http.NewRequestWithContext(pullCtx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if readErr != nil {
			return fmt.Errorf("Docker image pull returned HTTP %d and unreadable error body: %w", resp.StatusCode, readErr)
		}
		var dockerError struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(body, &dockerError) == nil && dockerError.Message != "" {
			return fmt.Errorf("Docker image pull returned HTTP %d: %s", resp.StatusCode, dockerError.Message)
		}
		return fmt.Errorf("Docker image pull returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	decoder := json.NewDecoder(resp.Body)
	for {
		var event struct {
			Error       string `json:"error"`
			ErrorDetail struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if err := decoder.Decode(&event); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode Docker image pull stream: %w", err)
		}
		if event.ErrorDetail.Message != "" {
			return fmt.Errorf("Docker image pull failed: %s", event.ErrorDetail.Message)
		}
		if event.Error != "" {
			return fmt.Errorf("Docker image pull failed: %s", event.Error)
		}
	}
}

func (sm *ServiceManager) ImageExistsPublic(image string) (bool, error) {
	// Docker inspect image endpoint
	url := fmt.Sprintf("http://localhost/images/%s/json", image)
	resp, err := sm.dockerClient.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// ProvisionComputeVM actively boots a Data Plane Docker container mimicking a GCE VM.
func (sm *ServiceManager) ProvisionComputeVM(ctx context.Context, containerName string, osImage string, vpcName string, ports []string, env []string, cmd []string) error {
	log.Printf("[Orchestrator] Provisioning compute VM: %s (image: %s vpc: %s ports: %d env: %d cmd: %v)", containerName, osImage, vpcName, len(ports), len(env), cmd)
	if !validDockerResourceName(containerName) {
		return fmt.Errorf("invalid Compute container name")
	}
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil {
		return fmt.Errorf("inspect Compute container: %w", err)
	}
	if status != "not_found" {
		if !isOwnedComputeVM(labels, containerName) {
			return fmt.Errorf("Compute container %q exists but is not owned by this profile and resource", containerName)
		}
		if status != "running" {
			if err := sm.startContainer(containerName); err != nil {
				return err
			}
		}
		return sm.updatePortRegistry(containerName)
	}

	exists, err := sm.ImageExistsPublic(osImage)
	if err != nil {
		log.Printf("[Orchestrator] Image check error for %s: %v", osImage, err)
	}
	if !exists {
		if err := sm.pullImageInternal(ctx, osImage); err != nil {
			return fmt.Errorf("pull data plane image %q: %w", osImage, err)
		}
	} else {
		log.Printf("[Orchestrator] Image '%s' already exists locally, skipping pull.", osImage)
	}

	netMode := networkName
	if vpcName != "" && vpcName != "default" {
		netMode = "minisky-vpc-" + vpcName
	}

	exposedPorts := make(map[string]interface{})
	portBindings := make(map[string]interface{})
	for _, port := range ports {
		if !strings.Contains(port, "/") {
			port += "/tcp"
		}
		exposedPorts[port] = struct{}{}
		portBindings[port] = []map[string]interface{}{
			{"HostIp": "127.0.0.1", "HostPort": ""},
		}
	}

	resourceLabels := ownedDockerLabels()
	resourceLabels["minisky.service"] = "compute-instance"
	resourceLabels["minisky.resource"] = containerName
	payload := map[string]interface{}{
		"Image":        osImage,
		"Env":          append(sm.standardEnv(), env...),
		"ExposedPorts": exposedPorts,
		"Labels":       resourceLabels,
		"HostConfig": map[string]interface{}{
			"NetworkMode":  netMode,
			"PortBindings": portBindings,
		},
	}
	if len(cmd) > 0 {
		payload["Cmd"] = cmd
	}
	data, _ := json.Marshal(payload)
	endpoint := "http://localhost/containers/create?name=" + url.QueryEscape(containerName)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vm creation rejected %d: %s", resp.StatusCode, b)
	}

	if err := sm.startContainer(containerName); err != nil {
		return err
	}

	return sm.updatePortRegistry(containerName)
}

// ProvisionRedis creates or reconciles an owned Redis container with an owned
// named volume. The only published endpoint is a Docker-assigned loopback port.
func (sm *ServiceManager) ProvisionRedis(ctx context.Context, resourceID, image string) (string, error) {
	containerName, volumeName := redisDockerNames(resourceID)
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil {
		return "", err
	}
	if status != "not_found" {
		if !isOwnedRedisResource(labels, resourceID) {
			return "", fmt.Errorf("Redis container %q exists but is not owned by this profile and resource", containerName)
		}
		if status != "running" {
			if err := sm.startContainer(containerName); err != nil {
				return "", err
			}
		}
		return sm.redisEndpoint(containerName)
	}

	exists, err := sm.ImageExistsPublic(image)
	if err != nil {
		return "", fmt.Errorf("inspect Redis image: %w", err)
	}
	if !exists {
		if err := sm.pullImageInternal(ctx, image); err != nil {
			return "", fmt.Errorf("pull Redis image: %w", err)
		}
	}
	resourceLabels := ownedDockerLabels()
	resourceLabels["minisky.service"] = "memorystore-redis"
	resourceLabels["minisky.resource"] = resourceID
	volumeInspect, err := sm.dockerClient.Get("http://localhost/volumes/" + url.PathEscape(volumeName))
	if err != nil {
		return "", fmt.Errorf("inspect Redis volume: %w", err)
	}
	if volumeInspect.StatusCode == http.StatusOK {
		var existing struct {
			Labels map[string]string `json:"Labels"`
		}
		decodeErr := json.NewDecoder(volumeInspect.Body).Decode(&existing)
		volumeInspect.Body.Close()
		if decodeErr != nil {
			return "", fmt.Errorf("decode Redis volume ownership: %w", decodeErr)
		}
		if !isOwnedRedisResource(existing.Labels, resourceID) {
			return "", fmt.Errorf("Redis volume %q exists but is not owned by this profile and resource", volumeName)
		}
	} else {
		statusCode := volumeInspect.StatusCode
		volumeInspect.Body.Close()
		if statusCode != http.StatusNotFound {
			return "", fmt.Errorf("inspect Redis volume returned %d", statusCode)
		}
		volumePayload, _ := json.Marshal(map[string]any{"Name": volumeName, "Labels": resourceLabels})
		volumeRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://localhost/volumes/create", bytes.NewReader(volumePayload))
		volumeRequest.Header.Set("Content-Type", "application/json")
		volumeResponse, err := sm.dockerClient.Do(volumeRequest)
		if err != nil {
			return "", fmt.Errorf("create Redis volume: %w", err)
		}
		defer volumeResponse.Body.Close()
		if volumeResponse.StatusCode != http.StatusCreated && volumeResponse.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(volumeResponse.Body)
			return "", fmt.Errorf("create Redis volume returned %d: %s", volumeResponse.StatusCode, body)
		}
	}

	const containerPort = "6379/tcp"
	payload := map[string]any{
		"Image":        image,
		"Cmd":          []string{"redis-server", "--appendonly", "yes", "--appendfsync", "always", "--dir", "/data"},
		"ExposedPorts": map[string]any{containerPort: struct{}{}},
		"HostConfig": map[string]any{
			"NetworkMode": networkName,
			"PortBindings": map[string]any{
				containerPort: []map[string]string{{"HostIp": "127.0.0.1", "HostPort": "0"}},
			},
			"Binds": []string{volumeName + ":/data"},
		},
		"Labels": resourceLabels,
	}
	encoded, _ := json.Marshal(payload)
	createRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(containerName), bytes.NewReader(encoded))
	createRequest.Header.Set("Content-Type", "application/json")
	createResponse, err := sm.dockerClient.Do(createRequest)
	if err != nil {
		return "", fmt.Errorf("create Redis container: %w", err)
	}
	defer createResponse.Body.Close()
	if createResponse.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResponse.Body)
		return "", fmt.Errorf("create Redis container returned %d: %s", createResponse.StatusCode, body)
	}
	if err := sm.startContainer(containerName); err != nil {
		return "", err
	}
	endpoint, err := sm.redisEndpoint(containerName)
	if err != nil {
		return "", err
	}
	if err := sm.waitUntilReady(endpoint, 30*time.Second); err != nil {
		return "", err
	}
	return endpoint, nil
}

// ReconcileRedis returns the loopback endpoint only when the existing backend
// has exact profile and resource ownership labels.
func (sm *ServiceManager) ReconcileRedis(_ context.Context, resourceID string) (string, bool, error) {
	containerName, _ := redisDockerNames(resourceID)
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil {
		return "", false, err
	}
	if status == "not_found" {
		return "", false, nil
	}
	if !isOwnedRedisResource(labels, resourceID) {
		return "", false, nil
	}
	if status != "running" {
		return "", true, fmt.Errorf("owned Redis container is %s", status)
	}
	endpoint, err := sm.redisEndpoint(containerName)
	return endpoint, true, err
}

// DeleteRedis removes only the exactly owned container and volume.
func (sm *ServiceManager) DeleteRedis(ctx context.Context, resourceID string) error {
	containerName, volumeName := redisDockerNames(resourceID)
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil {
		return err
	}
	if status != "not_found" {
		if !isOwnedRedisResource(labels, resourceID) {
			return fmt.Errorf("refusing to delete unowned Redis container %q", containerName)
		}
		request, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://localhost/containers/"+url.PathEscape(containerName)+"?force=true", nil)
		response, err := sm.dockerClient.Do(request)
		if err != nil {
			return err
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("delete Redis container returned %d", response.StatusCode)
		}
	}

	inspectResponse, err := sm.dockerClient.Get("http://localhost/volumes/" + url.PathEscape(volumeName))
	if err != nil {
		return err
	}
	if inspectResponse.StatusCode == http.StatusNotFound {
		inspectResponse.Body.Close()
		return nil
	}
	if inspectResponse.StatusCode != http.StatusOK {
		statusCode := inspectResponse.StatusCode
		inspectResponse.Body.Close()
		return fmt.Errorf("inspect Redis volume returned %d", statusCode)
	}
	var volume struct {
		Labels map[string]string `json:"Labels"`
	}
	decodeErr := json.NewDecoder(inspectResponse.Body).Decode(&volume)
	inspectResponse.Body.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if !isOwnedRedisResource(volume.Labels, resourceID) {
		return fmt.Errorf("refusing to delete unowned Redis volume %q", volumeName)
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/volumes/"+url.PathEscape(volumeName), nil)
	response, err := sm.dockerClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete Redis volume returned %d", response.StatusCode)
	}
	return nil
}

func (sm *ServiceManager) redisEndpoint(containerName string) (string, error) {
	port, err := sm.GetContainerHostPort(containerName, "6379/tcp")
	if err != nil {
		return "", err
	}
	return net.JoinHostPort("127.0.0.1", port), nil
}

func redisDockerNames(resourceID string) (string, string) {
	hash := sha256.Sum256([]byte(config.GetProfile() + "\x00" + resourceID))
	suffix := fmt.Sprintf("%x", hash[:8])
	return "minisky-redis-" + suffix, "minisky-redis-data-" + suffix
}

func isOwnedRedisResource(labels map[string]string, resourceID string) bool {
	return isOwnedDockerResource(labels) &&
		labels["minisky.service"] == "memorystore-redis" &&
		labels["minisky.resource"] == resourceID
}

// ProvisionCloudSQLVM starts a fully-interactive PostgreSQL or MySQL docker database data plane.
func (sm *ServiceManager) ProvisionBuildStep(ctx context.Context, containerName string, image string, binds []string, env []string, cmd []string) error {
	log.Printf("[Orchestrator] Provisioning build step: %s (image: %s binds: %v cmd: %v)", containerName, image, binds, cmd)

	exists, _ := sm.ImageExistsPublic(image)
	if !exists {
		if err := sm.pullImageInternal(ctx, image); err != nil {
			return fmt.Errorf("pull build image %q: %w", image, err)
		}
	}

	payload := map[string]interface{}{
		"Image":      image,
		"WorkingDir": "/workspace",
		"Env":        append(sm.standardEnv(), env...),
		"HostConfig": map[string]interface{}{
			"NetworkMode": networkName,
			"Binds":       binds,
		},
		"Labels": map[string]string{
			"managed-by": "minisky-build",
			"build-id":   containerName,
		},
	}
	if len(cmd) > 0 {
		payload["Cmd"] = cmd
	}
	data, _ := json.Marshal(payload)
	url := fmt.Sprintf("http://localhost/containers/create?name=%s", containerName)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("build step creation rejected %d: %s", resp.StatusCode, b)
	}

	return sm.startContainer(containerName)
}

func (sm *ServiceManager) ProvisionCloudSQLVM(ctx context.Context, instanceName string, version string, rootPassword string) (string, error) {
	var image string
	var env []string
	var expPort string

	reg := config.GetImageRegistry()
	if strings.HasPrefix(version, "POSTGRES") {
		// Version can be "POSTGRES_18", "POSTGRES_17", or just "POSTGRES"
		vparts := strings.Split(version, "_")
		if len(vparts) > 1 {
			targetV := vparts[1]
			for _, v := range reg.Sql.Postgres.Versions {
				if v.Version == targetV {
					image = v.Image
					break
				}
			}
		}
		if image == "" {
			image = reg.Sql.Postgres.DefaultImage
		}
		env = append(sm.standardEnv(),
			"POSTGRES_PASSWORD="+rootPassword,
			"PGDATA=/var/lib/postgresql/data",
		)
		expPort = "5432/tcp"
	} else if strings.HasPrefix(version, "MYSQL") {
		vparts := strings.Split(version, "_")
		if len(vparts) > 1 {
			targetV := vparts[1]
			// Handle legacy version strings like MYSQL_8_0
			if len(vparts) > 2 {
				targetV = vparts[1] + "." + vparts[2]
			}
			for _, v := range reg.Sql.Mysql.Versions {
				if v.Version == targetV || strings.HasPrefix(v.Version, vparts[1]) {
					image = v.Image
					break
				}
			}
		}
		if image == "" {
			image = reg.Sql.Mysql.DefaultImage
		}
		env = append(sm.standardEnv(), "MYSQL_ROOT_PASSWORD="+rootPassword)
		expPort = "3306/tcp"
	} else {
		return "", fmt.Errorf("unsupported database version: %s", version)
	}

	containerName := "minisky-sql-" + instanceName
	log.Printf("[Orchestrator] Provisioning Cloud SQL VM: %s (image: %s)", containerName, image)

	exists, err := sm.ImageExistsPublic(image)
	if err != nil {
		log.Printf("[Orchestrator] Image check error for %s: %v", image, err)
	}
	if !exists {
		if err := sm.pullImageInternal(ctx, image); err != nil {
			return "", fmt.Errorf("pull Cloud SQL image %q: %w", image, err)
		}
	} else {
		log.Printf("[Orchestrator] Image '%s' already exists locally, skipping pull.", image)
	}

	// Clean up any stale container
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://localhost/containers/%s?force=true", containerName), nil)
	sm.dockerClient.Do(req)

	// Volumes - mount a docker volume for persistence
	volName := "minisky-db-" + instanceName
	sm.dockerClient.Post("http://localhost/volumes/create", "application/json", strings.NewReader(`{"Name": "`+volName+`"}`))

	var mountTarget string
	if strings.HasPrefix(version, "MYSQL") {
		mountTarget = "/var/lib/mysql"
	} else {
		mountTarget = "/var/lib/postgresql/data"
	}

	payload := map[string]interface{}{
		"Image": image,
		"Env":   env,
		"ExposedPorts": map[string]interface{}{
			expPort: struct{}{},
		},
		"HostConfig": map[string]interface{}{
			"NetworkMode": networkName,
			"PortBindings": map[string]interface{}{
				expPort: []map[string]string{
					{"HostIp": "127.0.0.1", "HostPort": "0"},
				},
			},
			"Binds": []string{
				fmt.Sprintf("%s:%s", volName, mountTarget),
			},
		},
		"Labels": map[string]string{
			"managed-by": "minisky-sql",
			"instance":   instanceName,
		},
	}

	b, _ := json.Marshal(payload)
	cReq, _ := http.NewRequest("POST", "http://localhost/containers/create?name="+containerName, bytes.NewBuffer(b))
	cReq.Header.Set("Content-Type", "application/json")
	resp, err := sm.dockerClient.Do(cReq)
	if err != nil {
		return "", fmt.Errorf("create SQL container: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create SQL container %d: %s", resp.StatusCode, string(respBody))
	}

	if err := sm.startContainer(containerName); err != nil {
		return "", fmt.Errorf("start SQL container: %v", err)
	}

	config := ContainerConfig{Name: containerName, ContainerPort: expPort}
	internalURL, err := sm.discoverInternalURL(config)
	if err != nil {
		return "", fmt.Errorf("port discovery: %v", err)
	}

	log.Printf("[Orchestrator] ✅ SQL Instance '%s' ONLINE at %s", instanceName, internalURL)
	return internalURL, nil
}

// DeleteCloudSQLVM stops and forcefully removes a Cloud SQL node.
func (sm *ServiceManager) DeleteCloudSQLVM(instanceName string) error {
	containerName := "minisky-sql-" + instanceName
	log.Printf("[Orchestrator] Tearing down Cloud SQL VM: %s", containerName)

	stopURL := fmt.Sprintf("http://localhost/containers/%s/stop?t=2", containerName)
	req, _ := http.NewRequest("POST", stopURL, nil)
	sm.dockerClient.Do(req)

	rmURL := fmt.Sprintf("http://localhost/containers/%s?force=true", containerName)
	req, _ = http.NewRequest("DELETE", rmURL, nil)
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// DeleteComputeVM permanently destroys a physical Data Plane compute instance.
func (sm *ServiceManager) DeleteComputeVM(containerName string) error {
	log.Printf("[Orchestrator] Tearing down Data Plane VM: %s", containerName)
	if !validDockerResourceName(containerName) {
		return fmt.Errorf("invalid Compute container name")
	}
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil {
		return err
	}
	if status == "not_found" {
		return nil
	}
	if !isOwnedComputeVM(labels, containerName) {
		return fmt.Errorf("refusing to delete unowned Compute container %q", containerName)
	}

	stopURL := fmt.Sprintf("http://localhost/containers/%s/stop?t=2", url.PathEscape(containerName))
	req, _ := http.NewRequest("POST", stopURL, nil)
	if response, stopErr := sm.dockerClient.Do(req); stopErr == nil {
		response.Body.Close()
	}

	rmURL := fmt.Sprintf("http://localhost/containers/%s?force=true", url.PathEscape(containerName))
	req, _ = http.NewRequest("DELETE", rmURL, nil)
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete Compute container returned %d", resp.StatusCode)
	}
	sm.mu.Lock()
	delete(sm.portRegistry, containerName)
	sm.mu.Unlock()
	return nil
}

func isOwnedComputeVM(labels map[string]string, containerName string) bool {
	return isOwnedDockerResource(labels) &&
		labels["minisky.service"] == "compute-instance" &&
		labels["minisky.resource"] == containerName
}

// DeleteServerlessVM deletes only the current profile's exact MiniSky-owned
// serverless container. A missing owned container is an idempotent success.
func (sm *ServiceManager) DeleteServerlessVM(identity ServerlessIdentity) error {
	containerName, err := identity.ContainerName()
	if err != nil {
		return err
	}
	release, ok := sm.tryServerlessLifecycle(identity)
	if !ok {
		return fmt.Errorf("%w for %s", ErrServerlessLifecycleInProgress, identity.CanonicalResource())
	}
	defer release()
	return sm.deleteServerlessVM(identity, containerName)
}

func (sm *ServiceManager) deleteServerlessVM(identity ServerlessIdentity, containerName string) error {
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil {
		return err
	}
	if status == "not_found" {
		return nil
	}
	if !isOwnedServerlessVM(labels, identity) {
		return fmt.Errorf("refusing to delete unowned Serverless container %q", containerName)
	}

	stopURL := fmt.Sprintf("http://localhost/containers/%s/stop?t=2", url.PathEscape(containerName))
	req, _ := http.NewRequest(http.MethodPost, stopURL, nil)
	if response, stopErr := sm.dockerClient.Do(req); stopErr == nil {
		response.Body.Close()
	}

	rmURL := fmt.Sprintf("http://localhost/containers/%s?force=true", url.PathEscape(containerName))
	req, _ = http.NewRequest(http.MethodDelete, rmURL, nil)
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete Serverless container returned %d", resp.StatusCode)
	}
	sm.mu.Lock()
	delete(sm.portRegistry, containerName)
	sm.mu.Unlock()
	return nil
}

func isOwnedServerlessVM(labels map[string]string, identity ServerlessIdentity) bool {
	expected, err := identity.labels()
	if err != nil {
		return false
	}
	for key, value := range expected {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// ProvisionServerlessVM starts a container from a custom image (typically built by Buildpacks).
func (sm *ServiceManager) ProvisionServerlessVM(identity ServerlessIdentity, image string, env []string) (string, error) {
	containerName, err := identity.ContainerName()
	if err != nil {
		return "", err
	}
	release, ok := sm.tryServerlessLifecycle(identity)
	if !ok {
		return "", fmt.Errorf("%w for %s", ErrServerlessLifecycleInProgress, identity.CanonicalResource())
	}
	defer release()
	log.Printf("[Orchestrator] Provisioning Serverless VM: %s (image: %s)", containerName, image)

	if err := sm.deleteServerlessVM(identity, containerName); err != nil {
		return "", err
	}

	expPort := "8080/tcp"
	labels, err := identity.labels()
	if err != nil {
		return "", err
	}
	payload := map[string]interface{}{
		"Image": image,
		"Env":   append(sm.standardEnv(), env...),
		"ExposedPorts": map[string]interface{}{
			expPort: struct{}{},
		},
		"HostConfig": map[string]interface{}{
			"NetworkMode": networkName,
			"PortBindings": map[string]interface{}{
				expPort: []map[string]string{
					{"HostIp": "127.0.0.1", "HostPort": "0"},
				},
			},
		},
		"Labels": labels,
	}

	b, _ := json.Marshal(payload)
	createURL := "http://localhost/containers/create?" + url.Values{"name": {containerName}}.Encode()
	cReq, _ := http.NewRequest("POST", createURL, bytes.NewBuffer(b))
	cReq.Header.Set("Content-Type", "application/json")
	resp, err := sm.dockerClient.Do(cReq)
	if err != nil {
		return "", fmt.Errorf("create Serverless container: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create Serverless container %d: %s", resp.StatusCode, string(respBody))
	}

	if err := sm.startContainer(containerName); err != nil {
		return "", sm.cleanupFailedServerlessProvision(identity, containerName, fmt.Errorf("start Serverless container: %w", err))
	}

	config := ContainerConfig{Name: containerName, ContainerPort: expPort}
	internalURL, err := sm.discoverInternalURL(config)
	if err != nil {
		return "", sm.cleanupFailedServerlessProvision(identity, containerName, fmt.Errorf("port discovery: %w", err))
	}
	ready := sm.serverlessReady
	if ready == nil {
		ready = waitUntilHTTPReady
	}
	if err := ready(internalURL, 60*time.Second); err != nil {
		return "", sm.cleanupFailedServerlessProvision(identity, containerName, fmt.Errorf("serverless readiness probe failed: %w", err))
	}

	log.Printf("[Orchestrator] ✅ Serverless Instance '%s' ONLINE at %s", identity.CanonicalResource(), internalURL)
	return internalURL, nil
}

func (sm *ServiceManager) cleanupFailedServerlessProvision(identity ServerlessIdentity, containerName string, cause error) error {
	if cleanupErr := sm.deleteServerlessVM(identity, containerName); cleanupErr != nil {
		message := cleanupErr.Error()
		if len(message) > 256 {
			message = message[:256]
		}
		return fmt.Errorf("%w; cleanup owned backend failed: %s", cause, message)
	}
	return cause
}

func (sm *ServiceManager) tryServerlessLifecycle(identity ServerlessIdentity) (func(), bool) {
	sm.serverlessMu.Lock()
	if sm.serverlessActive == nil {
		sm.serverlessActive = make(map[ServerlessIdentity]struct{})
	}
	if _, active := sm.serverlessActive[identity]; active {
		sm.serverlessMu.Unlock()
		return nil, false
	}
	sm.serverlessActive[identity] = struct{}{}
	sm.serverlessMu.Unlock()

	return func() {
		sm.serverlessMu.Lock()
		delete(sm.serverlessActive, identity)
		sm.serverlessMu.Unlock()
	}, true
}

// GetContainerLogs returns the last 'tail' lines of stdout/stderr from a container.
func (sm *ServiceManager) GetContainerLogs(containerName string, tail int) (string, error) {
	url := fmt.Sprintf("http://localhost/containers/%s/logs?stdout=true&stderr=true&tail=%d", containerName, tail)
	return sm.fetchLogs(url)
}

// GetContainerLogsSince returns stdout/stderr logs since a specific unix timestamp
func (sm *ServiceManager) GetContainerLogsSince(containerName string, since int64) (string, error) {
	url := fmt.Sprintf("http://localhost/containers/%s/logs?stdout=true&stderr=true&timestamps=true&since=%d", containerName, since)
	return sm.fetchLogs(url)
}

func (sm *ServiceManager) fetchLogs(url string) (string, error) {
	resp, err := sm.dockerClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "Log source not found.", nil
	}

	// Docker logs stream format: [8]byte header + payload.
	body, _ := io.ReadAll(resp.Body)

	// Quick header strip for standard docker logs stream headers (8 bytes)
	var result strings.Builder
	for i := 0; i < len(body); {
		if i+8 > len(body) {
			break
		}
		i += 8
		// read until end of chunk or next header
		next := i
		for next < len(body) && (next+8 > len(body) || (body[next] != 1 && body[next] != 2)) {
			next++
		}
		result.Write(body[i:next])
		i = next
	}

	if result.Len() == 0 && len(body) > 0 {
		return string(body), nil
	}

	return result.String(), nil
}

// RunCommandInContainer executes a non-interactive command inside a container.
func (sm *ServiceManager) RunCommandInContainer(name string, cmd []string) (string, error) {
	log.Printf("[Orchestrator] Executing command in '%s': %v", name, cmd)

	// 1. Create the exec instance
	payload := map[string]interface{}{
		"AttachStdin":  false,
		"AttachStdout": true,
		"AttachStderr": true,
		"Tty":          false,
		"Cmd":          cmd,
	}
	body, _ := json.Marshal(payload)
	createURL := fmt.Sprintf("http://localhost/containers/%s/exec", name)
	resp, err := sm.dockerClient.Post(createURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create exec (%d): %s", resp.StatusCode, b)
	}

	var execData struct{ Id string }
	json.NewDecoder(resp.Body).Decode(&execData)

	// 2. Start the exec instance
	startPayload := `{"Detach": false, "Tty": false}`
	startURL := fmt.Sprintf("http://localhost/exec/%s/start", execData.Id)
	startResp, err := sm.dockerClient.Post(startURL, "application/json", strings.NewReader(startPayload))
	if err != nil {
		return "", err
	}
	defer startResp.Body.Close()

	if startResp.StatusCode >= 400 {
		b, _ := io.ReadAll(startResp.Body)
		return "", fmt.Errorf("failed to start exec (%d): %s", startResp.StatusCode, b)
	}

	// 3. Collect output (Docker stream format)
	rawOutput, _ := io.ReadAll(startResp.Body)

	// Helper to strip headers
	var result strings.Builder
	for i := 0; i < len(rawOutput); {
		if i+8 > len(rawOutput) {
			break
		}
		// Skip header
		i += 8
		next := i
		// Read until next header or end
		for next < len(rawOutput) && (next+8 > len(rawOutput) || (rawOutput[next] != 1 && rawOutput[next] != 2)) {
			next++
		}
		result.Write(rawOutput[i:next])
		i = next
	}

	if result.Len() == 0 && len(rawOutput) > 0 {
		return string(rawOutput), nil
	}

	return result.String(), nil
}

func (sm *ServiceManager) createContainer(c ContainerConfig) error {
	// Bind container port to a random localhost port — works with Docker Desktop
	// (which runs in a VM where internal bridge IPs aren't host-reachable).
	exposedPorts := map[string]interface{}{c.ContainerPort: struct{}{}}
	portBindings := map[string]interface{}{
		c.ContainerPort: []map[string]string{
			{"HostIp": "127.0.0.1", "HostPort": "0"},
		},
	}
	for _, port := range c.AdditionalPorts {
		exposedPorts[port] = struct{}{}
		portBindings[port] = []map[string]string{
			{"HostIp": "127.0.0.1", "HostPort": "0"},
		}
	}
	hostCfg := map[string]interface{}{
		"NetworkMode":  networkName,
		"PortBindings": portBindings,
	}
	if c.Volume != "" {
		vol := c.Volume
		lastColon := strings.LastIndex(vol, ":")
		if lastColon > 0 {
			hostPath := vol[:lastColon]
			containerPath := vol[lastColon+1:]
			if strings.ContainsAny(hostPath, `/\`) || hostPath == "." || hostPath == ".." {
				if !filepath.IsAbs(hostPath) {
					if abs, err := filepath.Abs(hostPath); err == nil {
						vol = abs + ":" + containerPath
					}
				}
			}
		}
		hostCfg["Binds"] = []string{vol}
	}

	payload := map[string]interface{}{
		"Image":        c.Image,
		"Cmd":          c.Cmd,
		"ExposedPorts": exposedPorts,
		"HostConfig":   hostCfg,
		"Env":          c.Env,
		"Labels":       ownedDockerLabels(),
	}
	data, _ := json.Marshal(payload)
	url := fmt.Sprintf("http://localhost/containers/create?name=%s", c.Name)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create rejected %d: %s", resp.StatusCode, b)
	}
	return nil
}

func resolveEmulatorVolume(domain, configured string) (string, error) {
	runtimeName := ""
	switch domain {
	case "datastore.googleapis.com":
		runtimeName = "datastore"
	case "firestore.googleapis.com":
		runtimeName = "firestore"
	default:
		return configured, nil
	}

	containerPath := "/data"
	if separator := strings.LastIndex(configured, ":"); separator >= 0 {
		containerPath = configured[separator+1:]
	}
	hostPath := filepath.Join(config.GetRuntimeDir(), runtimeName)
	if err := os.MkdirAll(hostPath, 0o700); err != nil {
		return "", fmt.Errorf("create Datastore profile runtime directory: %w", err)
	}
	return hostPath + ":" + containerPath, nil
}

func ownedDockerLabels() map[string]string {
	return map[string]string{
		"managed-by":      "minisky",
		"minisky.profile": config.GetProfile(),
	}
}

func isOwnedDockerResource(labels map[string]string) bool {
	return labels["managed-by"] == "minisky" && labels["minisky.profile"] == config.GetProfile()
}

func (sm *ServiceManager) startContainer(name string) error {
	url := fmt.Sprintf("http://localhost/containers/%s/start", name)
	req, _ := http.NewRequest("POST", url, nil)
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotModified {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("start rejected %d: %s", resp.StatusCode, b)
	}
	return nil
}

func (sm *ServiceManager) waitUntilReady(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("'%s' not reachable after %s", addr, timeout)
}

func waitUntilHTTPReady(target string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := client.Get(target)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("%q did not return an HTTP response after %s", target, timeout)
}

// resolveDockerSocket and dialDocker are implemented in OS-specific files (dialer_unix.go, dialer_windows.go)

// ─────────────────────────────────────────────────────────────────────────────
// Level 1: VPC Network Management
// ─────────────────────────────────────────────────────────────────────────────

func (sm *ServiceManager) CreateVPCNetwork(ctx context.Context, name string) error {
	return sm.CreateVPCNetworkWithSubnet(ctx, name, "")
}

// CreateVPCNetworkWithSubnet maps one bounded GCP subnetwork/IPAM slice to an
// owned Docker bridge. Docker remains the enforcement boundary; no host-global
// routes or iptables rules are modified.
func (sm *ServiceManager) CreateVPCNetworkWithSubnet(ctx context.Context, name, cidr string) error {
	if !validDockerResourceName(name) {
		return fmt.Errorf("invalid VPC network name")
	}
	if cidr != "" {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil || !prefix.Addr().Is4() || prefix.Bits() < 8 ||
			prefix.Addr().IsLoopback() || prefix.Addr().IsMulticast() ||
			prefix.Addr().IsUnspecified() {
			return fmt.Errorf("invalid IPv4 subnetwork CIDR %q", cidr)
		}
		cidr = prefix.Masked().String()
	}
	netName := "minisky-vpc-" + name
	log.Printf("[Orchestrator] Creating VPC Docker network '%s'", netName)
	inspect, err := sm.dockerClient.Get("http://localhost/networks/" + url.PathEscape(netName))
	if err != nil {
		return err
	}
	if inspect.StatusCode == http.StatusOK {
		var existing struct {
			Labels map[string]string `json:"Labels"`
		}
		decodeErr := json.NewDecoder(inspect.Body).Decode(&existing)
		inspect.Body.Close()
		if decodeErr != nil {
			return fmt.Errorf("inspect existing VPC network: %w", decodeErr)
		}
		if !isOwnedVPCNetwork(existing.Labels, name) {
			return fmt.Errorf("VPC network %q exists but is not owned by the active MiniSky profile", netName)
		}
		return nil
	}
	inspect.Body.Close()
	if inspect.StatusCode != http.StatusNotFound {
		return fmt.Errorf("inspect VPC network returned %d", inspect.StatusCode)
	}
	labels := ownedDockerLabels()
	labels["minisky.service"] = "compute-network"
	labels["minisky.resource"] = name
	payload := map[string]interface{}{
		"Name":   netName,
		"Driver": "bridge",
		"Labels": labels,
	}
	if cidr != "" {
		payload["IPAM"] = map[string]any{
			"Driver": "default",
			"Config": []map[string]string{{"Subnet": cidr}},
		}
	}
	data, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://localhost/networks/create", bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vpc network create failed %d: %s", resp.StatusCode, b)
	}
	return nil
}

func (sm *ServiceManager) DeleteVPCNetwork(ctx context.Context, name string) error {
	if !validDockerResourceName(name) {
		return fmt.Errorf("invalid VPC network name")
	}
	netName := "minisky-vpc-" + name
	log.Printf("[Orchestrator] Deleting VPC Docker network '%s'", netName)
	inspect, err := sm.dockerClient.Get("http://localhost/networks/" + url.PathEscape(netName))
	if err != nil {
		return err
	}
	if inspect.StatusCode == http.StatusNotFound {
		inspect.Body.Close()
		return nil
	}
	if inspect.StatusCode != http.StatusOK {
		inspect.Body.Close()
		return fmt.Errorf("inspect VPC network returned %d", inspect.StatusCode)
	}
	var existing struct {
		Labels map[string]string `json:"Labels"`
	}
	decodeErr := json.NewDecoder(inspect.Body).Decode(&existing)
	inspect.Body.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if !isOwnedVPCNetwork(existing.Labels, name) {
		return fmt.Errorf("refusing to delete unowned VPC network %q", netName)
	}
	req, _ := http.NewRequestWithContext(ctx, "DELETE", "http://localhost/networks/"+url.PathEscape(netName), nil)
	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("vpc network delete failed %d", resp.StatusCode)
	}
	return nil
}

func isOwnedVPCNetwork(labels map[string]string, name string) bool {
	return isOwnedDockerResource(labels) &&
		labels["minisky.service"] == "compute-network" &&
		labels["minisky.resource"] == name
}

func validDockerResourceName(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for index, char := range name {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') ||
			(char == '-' && index > 0) {
			continue
		}
		return false
	}
	return name[len(name)-1] != '-'
}

// ─────────────────────────────────────────────────────────────────────────────
// Level 2: Port Binding & Firewall Re-application
// ─────────────────────────────────────────────────────────────────────────────

func (sm *ServiceManager) updatePortRegistry(containerName string) error {
	resp, err := sm.dockerClient.Get(fmt.Sprintf("http://localhost/containers/%s/json", containerName))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var info struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIp   string
				HostPort string
			}
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	mappings := []PortMapping{}
	for cPort, bindings := range info.NetworkSettings.Ports {
		if len(bindings) > 0 && bindings[0].HostPort != "" {
			parts := strings.Split(cPort, "/")
			p := parts[0]
			proto := "tcp"
			if len(parts) > 1 {
				proto = parts[1]
			}
			mappings = append(mappings, PortMapping{
				ContainerPort: p,
				HostPort:      bindings[0].HostPort,
				Protocol:      proto,
			})
		}
	}
	sm.portRegistry[containerName] = mappings
	return nil
}

func (sm *ServiceManager) GetVMPortMappings(containerName string) []PortMapping {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.portRegistry[containerName]
}

func (sm *ServiceManager) ApplyFirewallPortsToVPC(vpcName string, containerNames []string, osImages []string) error {
	allowedPorts := []string{}
	sm.mu.RLock()
	rules := sm.fwRules[vpcName]
	sm.mu.RUnlock()

	for _, r := range rules {
		if r.Action == "allow" && r.Direction == "INGRESS" {
			for _, p := range r.Ports {
				allowedPorts = append(allowedPorts, p)
			}
		}
	}

	log.Printf("[Orchestrator] Applying firewall ports %v to VPC '%s' (recreating %d VMs)", allowedPorts, vpcName, len(containerNames))
	for i, cName := range containerNames {
		osImage := osImages[i]
		sm.DeleteComputeVM(cName)
		sm.ProvisionComputeVM(context.Background(), cName, osImage, vpcName, allowedPorts, []string{}, []string{"tail", "-f", "/dev/null"})
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Level 3: Proxy-Level Enforcement
// ─────────────────────────────────────────────────────────────────────────────

func (sm *ServiceManager) RegisterFirewallRule(vpc string, entry FirewallEntry) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.fwRules[vpc] == nil {
		sm.fwRules[vpc] = []FirewallEntry{}
	}
	sm.fwRules[vpc] = append(sm.fwRules[vpc], entry)
}

func (sm *ServiceManager) RemoveFirewallRule(vpc, ruleName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	rules := sm.fwRules[vpc]
	for i, r := range rules {
		if r.Name == ruleName {
			sm.fwRules[vpc] = append(rules[:i], rules[i+1:]...)
			break
		}
	}
}

func (sm *ServiceManager) CheckFirewallAllows(vpcName, protocol, port, sourceIP string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	rules := sm.fwRules[vpcName]

	allowed := false
	for _, r := range rules {
		if r.Direction == "INGRESS" {
			protoMatch := r.Protocol == "all" || strings.ToLower(r.Protocol) == strings.ToLower(protocol)
			portMatch := len(r.Ports) == 0
			for _, p := range r.Ports {
				if p == port {
					portMatch = true
					break
				}
			}
			rangeMatch := len(r.Ranges) == 0
			for _, src := range r.Ranges {
				if src == "0.0.0.0/0" || src == sourceIP {
					rangeMatch = true
					break
				}
			}

			if protoMatch && portMatch && rangeMatch {
				if r.Action == "deny" {
					return false
				}
				if r.Action == "allow" {
					allowed = true
				}
			}
		}
	}
	return allowed
}

// StreamContainerExec initiates an interactive session with a container.
// It returns a hijacked physical connection to the Docker daemon.
func (sm *ServiceManager) StreamContainerExec(name, user string) (net.Conn, error) {
	if err := sm.validateExecTarget(name); err != nil {
		return nil, err
	}
	if user != "" && !validExecUser(user) {
		return nil, fmt.Errorf("invalid exec user")
	}
	// 1. Create the exec instance
	// We try bash first, falling back to sh if needed
	payload := map[string]interface{}{
		"AttachStdin":  true,
		"AttachStdout": true,
		"AttachStderr": true,
		"Tty":          true,
		"Cmd":          []string{"/bin/bash"},
	}
	if user != "" {
		payload["User"] = user
	}
	body, _ := json.Marshal(payload)
	createURL := fmt.Sprintf("http://localhost/containers/%s/exec", url.PathEscape(name))
	resp, err := sm.dockerClient.Post(createURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		// Try fallback to /bin/sh
		payload["Cmd"] = []string{"/bin/sh"}
		body, _ = json.Marshal(payload)
		resp, err = sm.dockerClient.Post(createURL, "application/json", bytes.NewBuffer(body))
		if err != nil || resp.StatusCode != http.StatusCreated {
			return nil, fmt.Errorf("failed to create exec: %d", resp.StatusCode)
		}
		defer resp.Body.Close()
	}

	var execData struct{ Id string }
	json.NewDecoder(resp.Body).Decode(&execData)

	// 2. Start and Hijack the connection
	// We must dial the socket directly to bypass http.Client's pooling and response handling
	conn, err := sm.dialDocker(context.Background(), "", "")
	if err != nil {
		return nil, err
	}

	startPayload := `{"Detach": false, "Tty": true}`
	reqStr := fmt.Sprintf("POST /exec/%s/start HTTP/1.1\r\n"+
		"Host: localhost\r\n"+
		"Content-Type: application/json\r\n"+
		"Connection: Upgrade\r\n"+
		"Upgrade: tcp\r\n"+
		"Content-Length: %d\r\n\r\n%s",
		execData.Id, len(startPayload), startPayload)

	if _, err := conn.Write([]byte(reqStr)); err != nil {
		conn.Close()
		return nil, err
	}

	// Read the response header to ensure it started correctly
	// We use a buffered reader to parse the response, then return a wrapper
	// that continues reading from the same buffer to avoid data loss.
	bufReader := bufio.NewReader(conn)
	r, err := http.ReadResponse(bufReader, &http.Request{Method: "POST"})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read exec start response: %v", err)
	}
	if r.StatusCode != http.StatusOK && r.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("unexpected exec start status: %s", r.Status)
	}

	return &bufferedConn{Conn: conn, r: bufReader}, nil
}

func (sm *ServiceManager) validateExecTarget(name string) error {
	if !validDockerResourceName(name) {
		return fmt.Errorf("invalid container name")
	}
	status, labels, err := sm.inspectContainer(name)
	if err != nil {
		return fmt.Errorf("inspect terminal target: %w", err)
	}
	if status != "running" {
		return fmt.Errorf("terminal target is not running")
	}
	if !isOwnedDockerResource(labels) {
		return fmt.Errorf("terminal target is not owned by the active MiniSky profile")
	}
	return nil
}

func validExecUser(user string) bool {
	if user == "" || len(user) > 64 {
		return false
	}
	for _, char := range user {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

type bufferedConn struct {
	net.Conn
	r io.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) {
	return b.r.Read(p)
}

func (sm *ServiceManager) DoDockerRequest(req *http.Request) (*http.Response, error) {
	return sm.dockerClient.Do(req)
}

// GetContainerIP retrieves the internal IP address of a container.
func (sm *ServiceManager) GetContainerIP(name string) string {
	resp, err := sm.dockerClient.Get(fmt.Sprintf("http://localhost/containers/%s/json", name))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	var info struct {
		NetworkSettings struct {
			Networks map[string]struct {
				IPAddress string
			}
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return ""
	}

	// Prioritize minisky-net
	if net, ok := info.NetworkSettings.Networks[networkName]; ok && net.IPAddress != "" {
		return net.IPAddress
	}

	// Fallback to first available IP
	for _, net := range info.NetworkSettings.Networks {
		if net.IPAddress != "" {
			return net.IPAddress
		}
	}

	return ""
}

type ContainerSummary struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Image  string `json:"image"`
}

// ListManagedContainers lists all minisky-* containers.
func (sm *ServiceManager) ListManagedContainers() []ContainerSummary {
	resp, err := sm.dockerClient.Get(`http://localhost/containers/json?all=true&filters={"name":["minisky-"]}`)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var raw []struct {
		Names  []string `json:"Names"`
		Status string   `json:"Status"`
		Image  string   `json:"Image"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil
	}
	out := make([]ContainerSummary, 0, len(raw))
	for _, c := range raw {
		name := ""
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		out = append(out, ContainerSummary{Name: name, Status: c.Status, Image: c.Image})
	}
	return out
}

// ContainerStats represents the CPU and Memory usage of a container.
type ContainerStats struct {
	CPUPercentage float64
	MemoryUsageMB float64
}

// GetContainerStats retrieves the resource usage stats of a container.
func (sm *ServiceManager) GetContainerStats(name string) (*ContainerStats, error) {
	url := fmt.Sprintf("http://localhost/containers/%s/stats?stream=false", name)
	resp, err := sm.dockerClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get stats: status %d", resp.StatusCode)
	}

	var raw struct {
		CPUStats struct {
			CPUUsage struct {
				TotalUsage uint64 `json:"total_usage"`
			} `json:"cpu_usage"`
			SystemCPUUsage uint64 `json:"system_cpu_usage"`
			OnlineCPUs     uint32 `json:"online_cpus"`
		} `json:"cpu_stats"`
		PreCPUStats struct {
			CPUUsage struct {
				TotalUsage uint64 `json:"total_usage"`
			} `json:"cpu_usage"`
			SystemCPUUsage uint64 `json:"system_cpu_usage"`
		} `json:"precpu_stats"`
		MemoryStats struct {
			Usage uint64            `json:"usage"`
			Stats map[string]uint64 `json:"stats"`
		} `json:"memory_stats"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	stats := &ContainerStats{}

	// Calculate CPU percentage
	cpuDelta := float64(raw.CPUStats.CPUUsage.TotalUsage) - float64(raw.PreCPUStats.CPUUsage.TotalUsage)
	systemDelta := float64(raw.CPUStats.SystemCPUUsage) - float64(raw.PreCPUStats.SystemCPUUsage)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		stats.CPUPercentage = (cpuDelta / systemDelta) * float64(raw.CPUStats.OnlineCPUs) * 100.0
	}

	// Calculate Memory Usage in MB (subtract inactive_file cache if available)
	memUsage := raw.MemoryStats.Usage
	if inactiveFile, ok := raw.MemoryStats.Stats["inactive_file"]; ok {
		if memUsage > inactiveFile {
			memUsage -= inactiveFile
		}
	}
	stats.MemoryUsageMB = float64(memUsage) / 1024.0 / 1024.0

	return stats, nil
}
func (sm *ServiceManager) standardEnv() []string {
	return []string{
		"SECRET_MANAGER_EMULATOR_HOST=minisky-secretmanager:8080",
		"PUBSUB_EMULATOR_HOST=minisky-pubsub:8085",
		"FIRESTORE_EMULATOR_HOST=minisky-firestore:8082",
		"DATASTORE_EMULATOR_HOST=minisky-datastore:8081",
		"BIGTABLE_EMULATOR_HOST=minisky-bigtable:8086",
		"STORAGE_EMULATOR_HOST=http://minisky-gcs:4443",
		"GOOGLE_CLOUD_PROJECT=default-project",
		// Internal gateway for REST shims
		"MINISKY_GATEWAY=172.17.0.1:8080",
	}
}
