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
	"log"
	"minisky/pkg/config"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	networkName            = "minisky-net"
	dockerImagePullTimeout = 15 * time.Minute
	dockerRequestTimeout   = 10 * time.Second
	alloyDBPostgresImage   = "postgres:15.8-bookworm@sha256:eb3747f5d0a92195ca486d2f15d9a4ee5e9461b0332fe87fbc59069490a5c659"
	maxBuildLogBytes       = 64 << 10
	buildCleanupTimeout    = 30 * time.Second
)

var (
	ErrDockerConfiguration     = errors.New("invalid Docker configuration")
	ErrDockerOwnershipConflict = errors.New("Docker resource ownership conflict")
)

var ErrServerlessLifecycleInProgress = errors.New("Serverless lifecycle already in progress")

// ServiceManager handles native REST-driven lifecycle events over the Docker Unix Socket.
type ServiceManager struct {
	mu                sync.RWMutex
	emulatorMu        sync.Mutex
	serverlessMu      sync.Mutex
	dockerClient      *http.Client
	dockerTimeout     time.Duration
	sockPath          string
	portRegistry      map[string][]PortMapping   // containerName → host ports
	fwRules           map[string][]FirewallEntry // vpcName → rules
	serverlessActive  map[ServerlessIdentity]struct{}
	serverlessReady   func(string, time.Duration) error
	emulatorReady     func(string, time.Duration) error
	redisReady        func(context.Context, string, time.Duration) error
	memcachedReady    func(context.Context, string, string, time.Duration) error
	cloudSQLReady     func(context.Context, string, string, time.Duration) error
	cloudSQLAuthReady func(context.Context, string, map[string]string, string, time.Duration) error
	memcacheUncertain map[string]error
	redisRuntimeMu    sync.RWMutex
	redisRuntimes     map[string]redisRuntimeIdentity
	redisProvisionals map[string]redisProvisionalRuntime
	redisClosing      bool
}

type deadlineRoundTripper struct {
	base    http.RoundTripper
	timeout time.Duration
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (body *cancelReadCloser) Close() error {
	err := body.ReadCloser.Close()
	body.cancel()
	return err
}

func (transport deadlineRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if _, ok := request.Context().Deadline(); ok || transport.timeout <= 0 {
		return transport.base.RoundTrip(request)
	}
	ctx, cancel := context.WithTimeout(request.Context(), transport.timeout)
	response, err := transport.base.RoundTrip(request.Clone(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &cancelReadCloser{ReadCloser: response.Body, cancel: cancel}
	return response, nil
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
	User            string
}

// BuildContainerResult is the terminal result of one exact-owned Cloud Build
// container. Logs are bounded to maxBuildLogBytes.
type BuildContainerResult struct {
	ExitCode      int
	Logs          string
	LogsTruncated bool
}

// AlloyDBIdentity is the immutable ownership boundary for one local AlloyDB
// PostgreSQL data plane.
type AlloyDBIdentity struct {
	Project  string
	Location string
	Cluster  string
	Instance string
}

type cleanupResource struct {
	ID     string            `json:"Id"`
	Name   string            `json:"Name"`
	Labels map[string]string `json:"Labels"`
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
	if err := validateDockerHost(os.Getenv("DOCKER_HOST")); err != nil {
		return nil, err
	}
	sockPath := resolveDockerSocket()
	// On Unix, ensure DOCKER_HOST is set if we found a socket
	if !strings.HasPrefix(sockPath, "//./pipe/") && os.Getenv("DOCKER_HOST") == "" {
		os.Setenv("DOCKER_HOST", "unix://"+sockPath)
	}
	log.Printf("[ServiceManager] Docker socket resolved: %s", sockPath)
	sm := &ServiceManager{
		sockPath:          sockPath,
		dockerTimeout:     dockerRequestTimeout,
		portRegistry:      make(map[string][]PortMapping),
		fwRules:           make(map[string][]FirewallEntry),
		serverlessActive:  make(map[ServerlessIdentity]struct{}),
		serverlessReady:   waitUntilHTTPReady,
		emulatorReady:     waitUntilHTTPReady,
		memcachedReady:    waitUntilMemcachedReady,
		cloudSQLReady:     waitUntilCloudSQLReady,
		memcacheUncertain: make(map[string]error),
	}
	sm.cloudSQLAuthReady = sm.waitUntilCloudSQLAuthenticatedReady
	transport := &http.Transport{
		DialContext: sm.dialDocker,
	}
	sm.dockerClient = &http.Client{Transport: deadlineRoundTripper{
		base:    transport,
		timeout: dockerRequestTimeout,
	}}
	return sm, nil
}

func (sm *ServiceManager) doDocker(request *http.Request) (*http.Response, error) {
	if _, ok := request.Context().Deadline(); ok || sm.dockerTimeout <= 0 {
		return sm.dockerClient.Do(request)
	}
	ctx, cancel := context.WithTimeout(request.Context(), sm.dockerTimeout)
	response, err := sm.dockerClient.Do(request.Clone(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &cancelReadCloser{ReadCloser: response.Body, cancel: cancel}
	return response, nil
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
			return fmt.Errorf(
				"%w: network %q exists but is not owned by active MiniSky profile",
				ErrDockerOwnershipConflict,
				networkName,
			)
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
	if isDurableEmulator(domain) {
		return sm.ensureDurableEmulatorRunning(ctx, domain, cfg, env...)
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

	name := emulatorContainerName(domain, cfg.Name)
	status, labels, err := sm.inspectContainerContext(ctx, name)
	if err != nil {
		return fmt.Errorf("status check failed: %v", err)
	}
	if status != "not_found" {
		if isDurableEmulator(domain) {
			if !hasExpectedDurableOwnership(labels, durableEmulatorLabels(domain)) {
				return fmt.Errorf("%w: refusing to stop emulator container %q",
					ErrDockerOwnershipConflict, name)
			}
		} else if !isOwnedDockerResource(labels) {
			return fmt.Errorf("%w: refusing to stop emulator container %q",
				ErrDockerOwnershipConflict, name)
		}
	}

	if status == "running" {
		log.Printf("[Orchestrator] Stopping service container '%s'...", name)
		stopURL := fmt.Sprintf("http://localhost/containers/%s/stop", name)
		req, _ := http.NewRequestWithContext(ctx, "POST", stopURL, nil)
		resp, err := sm.dockerClient.Do(req)
		if err != nil {
			return fmt.Errorf("stop container network error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotModified {
			return fmt.Errorf("stop rejected %d", resp.StatusCode)
		}
		log.Printf("[Orchestrator] Container '%s' stopped successfully.", name)
	} else {
		log.Printf("[Orchestrator] Container '%s' is already not running (status: %s)", name, status)
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
	return sm.getContainerHostPortContext(context.Background(), containerName, containerPort)
}

func (sm *ServiceManager) getContainerHostPortContext(
	ctx context.Context,
	containerName string,
	containerPort string,
) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://localhost/containers/%s/json", containerName), nil)
	if err != nil {
		return "", err
	}
	resp, err := sm.doDocker(request)
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
func (sm *ServiceManager) Teardown(ctx context.Context) error {
	var failures error
	sm.emulatorMu.Lock()
	sm.redisClosing = true
	sm.emulatorMu.Unlock()
	reg := config.GetImageRegistry()
	for domain, cfg := range reg.Emulators {
		if isDurableEmulator(domain) {
			if err := sm.removeDurableEmulatorContainer(ctx, domain, cfg); err != nil {
				failures = errors.Join(failures, err)
			}
			continue
		}
		id, status, labels, err := sm.inspectTeardownContainer(ctx, cfg.Name)
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("inspect container %q: %w", cfg.Name, err))
			continue
		}
		if status == "not_found" || !isOwnedDockerResource(labels) {
			continue
		}
		containerPath := "/containers/" + url.PathEscape(id)
		if err := sm.teardownDockerRequest(ctx, http.MethodPost, containerPath+"/stop",
			http.StatusNoContent, http.StatusNotModified, http.StatusNotFound); err != nil {
			failures = errors.Join(failures, fmt.Errorf("stop container %q: %w", cfg.Name, err))
		}
		if err := sm.teardownDockerRequest(ctx, http.MethodDelete, containerPath+"?force=true",
			http.StatusNoContent, http.StatusNotFound); err != nil {
			failures = errors.Join(failures, fmt.Errorf("remove container %q: %w", cfg.Name, err))
			continue
		}
		log.Printf("[Orchestrator] Removed container '%s'", cfg.Name)
	}
	redisNetworkHandled, redisTeardownErr := sm.teardownRedisRuntimes(ctx)
	if redisTeardownErr != nil {
		failures = errors.Join(failures, fmt.Errorf("teardown Redis runtimes: %w", redisTeardownErr))
	}
	if redisNetworkHandled {
		return failures
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/networks/"+networkName, nil)
	if err != nil {
		failures = errors.Join(failures, fmt.Errorf("build network inspect request: %w", err))
		return failures
	}
	resp, err := sm.doDocker(request)
	if err != nil {
		failures = errors.Join(failures, fmt.Errorf("inspect network %q: %w", networkName, err))
	} else {
		var network struct {
			ID         string            `json:"Id"`
			Labels     map[string]string `json:"Labels"`
			Containers map[string]struct {
				Name string `json:"Name"`
			} `json:"Containers"`
		}
		if resp.StatusCode == http.StatusOK {
			if decodeErr := json.NewDecoder(resp.Body).Decode(&network); decodeErr != nil {
				failures = errors.Join(failures, fmt.Errorf("decode network %q ownership: %w", networkName, decodeErr))
			} else if network.ID != "" && isOwnedDockerResource(network.Labels) {
				if len(network.Containers) != 0 {
					failures = errors.Join(failures, fmt.Errorf(
						"%w: network %q has %d unknown endpoints without registered runtime provenance",
						ErrDockerOwnershipConflict, networkName, len(network.Containers)))
				} else if removeErr := sm.teardownDockerRequest(ctx, http.MethodDelete, "/networks/"+url.PathEscape(network.ID),
					http.StatusNoContent, http.StatusNotFound); removeErr != nil {
					failures = errors.Join(failures, fmt.Errorf("remove network %q: %w", networkName, removeErr))
				} else {
					log.Printf("[Orchestrator] Removed network '%s'", networkName)
				}
			}
		} else if resp.StatusCode != http.StatusNotFound {
			failures = errors.Join(failures, fmt.Errorf("inspect network %q: Docker returned HTTP %d", networkName, resp.StatusCode))
		}
		if closeErr := resp.Body.Close(); closeErr != nil {
			failures = errors.Join(failures, fmt.Errorf("close network %q inspect response: %w", networkName, closeErr))
		}
	}
	return failures
}

func (sm *ServiceManager) inspectTeardownContainer(
	ctx context.Context,
	name string,
) (id string, status string, labels map[string]string, result error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(name)+"/json", nil)
	if err != nil {
		return "", "", nil, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return "", "", nil, err
	}
	defer func() {
		result = errors.Join(result, response.Body.Close())
	}()
	if response.StatusCode == http.StatusNotFound {
		return "", "not_found", nil, nil
	}
	if response.StatusCode != http.StatusOK {
		return "", "", nil, fmt.Errorf("Docker returned HTTP %d", response.StatusCode)
	}
	var inspected struct {
		ID    string `json:"Id"`
		State struct {
			Status string `json:"Status"`
		} `json:"State"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := json.NewDecoder(response.Body).Decode(&inspected); err != nil {
		return "", "", nil, err
	}
	if inspected.ID == "" {
		return "", "", nil, errors.New("container inspect returned no immutable ID")
	}
	return inspected.ID, inspected.State.Status, inspected.Config.Labels, nil
}

func (sm *ServiceManager) teardownDockerRequest(
	ctx context.Context,
	method string,
	path string,
	acceptedStatuses ...int,
) (result error) {
	request, err := http.NewRequestWithContext(ctx, method, "http://localhost"+path, nil)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, response.Body.Close())
	}()
	for _, status := range acceptedStatuses {
		if response.StatusCode == status {
			return nil
		}
	}
	detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if message := strings.TrimSpace(string(detail)); message != "" {
		return fmt.Errorf("Docker returned HTTP %d: %s", response.StatusCode, message)
	}
	return fmt.Errorf("Docker returned HTTP %d", response.StatusCode)
}

// CleanupProfile removes every Docker resource carrying MiniSky's exact active
// profile ownership labels. It never relies on names for destructive work:
// containers and networks use immutable IDs, while volumes use Docker's
// server-side prune with exact active-profile label filters.
func (sm *ServiceManager) CleanupProfile(ctx context.Context) error {
	return sm.cleanupDockerResources(ctx, isOwnedDockerResource, []string{
		"managed-by=minisky",
		"minisky.profile=" + config.GetProfile(),
	})
}

// CleanupAllProfiles removes all resources carrying both MiniSky's manager
// label and a non-empty profile label. It is intended only for uninstall.
func (sm *ServiceManager) CleanupAllProfiles(ctx context.Context) error {
	return sm.cleanupDockerResources(ctx, func(labels map[string]string) bool {
		return labels["managed-by"] == "minisky" && labels["minisky.profile"] != ""
	}, nil)
}

func (sm *ServiceManager) cleanupDockerResources(
	ctx context.Context,
	owned func(map[string]string) bool,
	volumeLabelFilters []string,
) error {
	var failures error

	containers, err := sm.listCleanupResources(ctx, "/containers/json?all=true", func(body io.Reader) ([]cleanupResource, error) {
		var resources []cleanupResource
		err := json.NewDecoder(body).Decode(&resources)
		return resources, err
	})
	if err != nil {
		failures = errors.Join(failures, fmt.Errorf("list profile containers: %w", err))
	} else {
		for _, resource := range containers {
			if !owned(resource.Labels) || resource.ID == "" {
				continue
			}
			if err := sm.deleteCleanupResource(
				ctx,
				"/containers/"+url.PathEscape(resource.ID)+"?"+url.Values{
					"force": {"true"},
					"v":     {"true"},
				}.Encode(),
			); err != nil {
				failures = errors.Join(failures, fmt.Errorf("remove profile container %q: %w", resource.ID, err))
			}
		}
	}

	networks, err := sm.listCleanupResources(ctx, "/networks", func(body io.Reader) ([]cleanupResource, error) {
		var resources []cleanupResource
		err := json.NewDecoder(body).Decode(&resources)
		return resources, err
	})
	if err != nil {
		failures = errors.Join(failures, fmt.Errorf("list profile networks: %w", err))
	} else {
		for _, resource := range networks {
			if !owned(resource.Labels) || resource.ID == "" {
				continue
			}
			if err := sm.deleteCleanupResource(ctx, "/networks/"+url.PathEscape(resource.ID)); err != nil {
				failures = errors.Join(failures, fmt.Errorf("remove profile network %q: %w", resource.ID, err))
			}
		}
	}

	if len(volumeLabelFilters) > 0 {
		if err := sm.pruneCleanupVolumes(ctx, volumeLabelFilters); err != nil {
			failures = errors.Join(failures, fmt.Errorf("prune exactly owned profile volumes: %w", err))
		}
		return failures
	}

	volumes, err := sm.listCleanupResources(ctx, "/volumes", func(body io.Reader) ([]cleanupResource, error) {
		var resources struct {
			Volumes []cleanupResource `json:"Volumes"`
		}
		err := json.NewDecoder(body).Decode(&resources)
		return resources.Volumes, err
	})
	if err != nil {
		failures = errors.Join(failures, fmt.Errorf("list profile volumes: %w", err))
	} else {
		profiles := make(map[string]struct{})
		for _, resource := range volumes {
			profile := resource.Labels["minisky.profile"]
			if !owned(resource.Labels) || !validCleanupProfileLabel(profile) {
				continue
			}
			profiles[profile] = struct{}{}
		}
		orderedProfiles := make([]string, 0, len(profiles))
		for profile := range profiles {
			orderedProfiles = append(orderedProfiles, profile)
		}
		sort.Strings(orderedProfiles)
		for _, profile := range orderedProfiles {
			if err := sm.pruneCleanupVolumes(ctx, []string{
				"managed-by=minisky",
				"minisky.profile=" + profile,
			}); err != nil {
				failures = errors.Join(failures, fmt.Errorf("prune exactly owned profile %q volumes: %w", profile, err))
			}
		}
	}
	return failures
}

func validCleanupProfileLabel(profile string) bool {
	if profile == "" || profile == "." || profile == ".." {
		return false
	}
	for index := 0; index < len(profile); index++ {
		char := profile[index]
		alphanumeric := char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9'
		if alphanumeric {
			continue
		}
		if index == 0 || char != '.' && char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func (sm *ServiceManager) pruneCleanupVolumes(ctx context.Context, labelFilters []string) (result error) {
	if len(labelFilters) == 0 {
		return errors.New("volume prune requires exact ownership label filters")
	}
	filters, err := json.Marshal(map[string][]string{
		"all":   {"true"},
		"label": labelFilters,
	})
	if err != nil {
		return err
	}
	endpoint := "http://localhost/volumes/prune?" + url.Values{"filters": {string(filters)}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, response.Body.Close())
	}()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Docker returned HTTP %d", response.StatusCode)
	}
	var pruned struct {
		VolumesDeleted []string `json:"VolumesDeleted"`
	}
	if err := json.NewDecoder(response.Body).Decode(&pruned); err != nil {
		return err
	}
	return nil
}

func (sm *ServiceManager) listCleanupResources(
	ctx context.Context,
	path string,
	decode func(io.Reader) ([]cleanupResource, error),
) ([]cleanupResource, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost"+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Docker returned HTTP %d", response.StatusCode)
	}
	return decode(response.Body)
}

func (sm *ServiceManager) deleteCleanupResource(ctx context.Context, path string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://localhost"+path, nil)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("Docker returned HTTP %d", response.StatusCode)
	}
	return nil
}

// ReconcileBuildResources removes only stale Cloud Build containers and
// workspaces carrying the active profile's exact ownership labels.
func (sm *ServiceManager) ReconcileBuildResources(ctx context.Context) error {
	containerResp, err := sm.dockerClient.Get("http://localhost/containers/json?all=true")
	if err != nil {
		return err
	}
	var containers []struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
	}
	decodeErr := json.NewDecoder(containerResp.Body).Decode(&containers)
	containerResp.Body.Close()
	if decodeErr != nil {
		return decodeErr
	}
	for _, container := range containers {
		if !isOwnedDockerResource(container.Labels) || container.Labels["minisky.service"] != "cloudbuild" ||
			container.Labels["minisky.resource"] == "" {
			continue
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://localhost/containers/"+url.PathEscape(container.ID)+"?force=true", nil)
		resp, err := sm.dockerClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("remove stale Cloud Build container returned %d", resp.StatusCode)
		}
	}

	volumeResp, err := sm.dockerClient.Get("http://localhost/volumes")
	if err != nil {
		return err
	}
	var volumes struct {
		Volumes []struct {
			Name   string            `json:"Name"`
			Labels map[string]string `json:"Labels"`
		} `json:"Volumes"`
	}
	decodeErr = json.NewDecoder(volumeResp.Body).Decode(&volumes)
	volumeResp.Body.Close()
	if decodeErr != nil {
		return decodeErr
	}
	for _, volume := range volumes.Volumes {
		if !isOwnedDockerResource(volume.Labels) || volume.Labels["minisky.service"] != "cloudbuild" ||
			volume.Labels["minisky.resource"] == "" {
			continue
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://localhost/volumes/"+url.PathEscape(volume.Name), nil)
		resp, err := sm.dockerClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("remove stale Cloud Build workspace returned %d", resp.StatusCode)
		}
	}
	return nil
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
	return sm.inspectContainerContext(context.Background(), name)
}

func (sm *ServiceManager) inspectContainerContext(ctx context.Context, name string) (string, map[string]string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://localhost/containers/%s/json", name), nil)
	resp, err := sm.doDocker(req)
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
	status, labels, err := sm.inspectContainer(name)
	if err != nil || status == "not_found" {
		return status, err
	}
	if !isOwnedDockerResource(labels) {
		return "", fmt.Errorf("container %q is not owned by the active MiniSky profile", name)
	}
	return status, nil
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
	return sm.imageExistsContext(context.Background(), image)
}

// ProvisionComputeVM actively boots a Data Plane Docker container mimicking a GCE VM.
func (sm *ServiceManager) ProvisionComputeVM(ctx context.Context, containerName string, osImage string, vpcName string, ports []string, env []string, cmd []string) error {
	labels := ownedDockerLabels()
	labels["minisky.service"] = "compute-instance"
	labels["minisky.resource"] = containerName
	return sm.provisionComputeVM(ctx, containerName, osImage, vpcName, ports, env, cmd, labels)
}

func (sm *ServiceManager) ProvisionComputeInstance(ctx context.Context, identity ComputeInstanceIdentity, osImage string, vpcName string, ports []string, env []string, cmd []string) error {
	containerName, err := identity.DockerName()
	if err != nil {
		return err
	}
	labels, _ := identity.labels()
	return sm.provisionComputeVM(ctx, containerName, osImage, vpcName, ports, env, cmd, labels)
}

func (sm *ServiceManager) provisionComputeVM(ctx context.Context, containerName string, osImage string, vpcName string, ports []string, env []string, cmd []string, resourceLabels map[string]string) error {
	log.Printf("[Orchestrator] Provisioning compute VM: %s (image: %s vpc: %s ports: %d env: %d cmd: %v)", containerName, osImage, vpcName, len(ports), len(env), cmd)
	if !validDockerResourceName(containerName) {
		return fmt.Errorf("invalid Compute container name")
	}
	status, labels, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return fmt.Errorf("inspect Compute container: %w", err)
	}
	if status != "not_found" {
		if !containsLabels(labels, resourceLabels) {
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
		if strings.HasPrefix(vpcName, "minisky-vpc-") && validDockerResourceName(vpcName) {
			netMode = vpcName
		} else {
			netMode = "minisky-vpc-" + vpcName
		}
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

// buildResourceLabels returns exact ownership labels for one Cloud Build workspace.
func buildResourceLabels(resourceID string) map[string]string {
	labels := ownedDockerLabels()
	labels["minisky.service"] = "cloudbuild"
	labels["minisky.resource"] = resourceID
	return labels
}

func (sm *ServiceManager) EnsureBuildWorkspace(ctx context.Context, volumeName, resourceID string) error {
	expected := buildResourceLabels(resourceID)
	resp, err := sm.dockerClient.Get("http://localhost/volumes/" + url.PathEscape(volumeName))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		var volume struct {
			Labels map[string]string `json:"Labels"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&volume)
		resp.Body.Close()
		if decodeErr != nil {
			return decodeErr
		}
		if !exactLabels(volume.Labels, expected) {
			return fmt.Errorf("build workspace %q exists but is not owned by this profile and resource", volumeName)
		}
		return nil
	}
	status := resp.StatusCode
	resp.Body.Close()
	if status != http.StatusNotFound {
		return fmt.Errorf("inspect build workspace returned %d", status)
	}
	payload, _ := json.Marshal(map[string]any{"Name": volumeName, "Labels": expected})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/volumes/create", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err = sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("create build workspace returned %d", resp.StatusCode)
	}
	return nil
}

func (sm *ServiceManager) RemoveBuildWorkspace(ctx context.Context, volumeName, resourceID string) error {
	expected := buildResourceLabels(resourceID)
	resp, err := sm.dockerClient.Get("http://localhost/volumes/" + url.PathEscape(volumeName))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil
	}
	var volume struct {
		Labels map[string]string `json:"Labels"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&volume)
	resp.Body.Close()
	if decodeErr != nil {
		return decodeErr
	}
	if !exactLabels(volume.Labels, expected) {
		return fmt.Errorf("refusing to remove unowned build workspace %q", volumeName)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, "http://localhost/volumes/"+url.PathEscape(volumeName), nil)
	resp, err = sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("remove build workspace returned %d", resp.StatusCode)
	}
	return nil
}

func (sm *ServiceManager) ProvisionBuildStep(ctx context.Context, containerName, resourceID, image string, binds []string, env []string, cmd []string) error {
	log.Printf("[Orchestrator] Provisioning build step: %s (image: %s binds: %v cmd: %v)", containerName, image, binds, cmd)
	expected := buildResourceLabels(resourceID)
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil {
		return err
	}
	if status != "not_found" {
		if !exactLabels(labels, expected) {
			return fmt.Errorf("build container %q exists but is not owned by this profile and resource", containerName)
		}
		return fmt.Errorf("owned build container %q already exists", containerName)
	}

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
		"Labels": expected,
	}
	if len(cmd) > 0 {
		payload["Cmd"] = cmd
	}
	data, _ := json.Marshal(payload)
	createURL := "http://localhost/containers/create?" + url.Values{"name": {containerName}}.Encode()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, createURL, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("build step creation rejected %d: %s", resp.StatusCode, b)
	}

	if err := sm.startContainerContext(ctx, containerName); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), buildCleanupTimeout)
		defer cancel()
		_ = sm.StopAndRemoveBuildContainer(cleanupCtx, containerName, resourceID)
		return err
	}
	return nil
}

type buildContainerInfo struct {
	ID       string
	Status   string
	Running  bool
	ExitCode int
}

// WaitBuildContainer waits for one exact-owned Cloud Build container to stop,
// then returns its real exit code and bounded stdout/stderr. Cancellation or a
// deadline removes only the container that still has exact build ownership.
func (sm *ServiceManager) WaitBuildContainer(
	ctx context.Context,
	containerName, resourceID string,
) (BuildContainerResult, error) {
	info, err := sm.inspectOwnedBuildContainer(ctx, containerName, resourceID)
	if err != nil {
		return BuildContainerResult{}, err
	}
	if !info.Running && info.Status != "exited" && info.Status != "dead" {
		return BuildContainerResult{}, fmt.Errorf(
			"build container %q is %s, not terminal", containerName, info.Status)
	}
	if info.Running {
		waitRequest, requestErr := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://localhost/containers/"+url.PathEscape(info.ID)+"/wait?condition=not-running", nil)
		if requestErr != nil {
			return BuildContainerResult{}, requestErr
		}
		waitResponse, waitErr := sm.doDocker(waitRequest)
		if waitErr != nil {
			return BuildContainerResult{}, sm.cleanupCanceledBuildContainer(
				ctx, containerName, resourceID, waitErr)
		}
		var waited struct {
			StatusCode int `json:"StatusCode"`
			Error      *struct {
				Message string `json:"Message"`
			} `json:"Error"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(waitResponse.Body, 1<<20)).Decode(&waited)
		closeErr := waitResponse.Body.Close()
		if waitResponse.StatusCode != http.StatusOK || decodeErr != nil || closeErr != nil {
			if ctx.Err() != nil {
				return BuildContainerResult{}, sm.cleanupCanceledBuildContainer(
					ctx, containerName, resourceID, errors.Join(decodeErr, closeErr))
			}
			return BuildContainerResult{}, errors.Join(
				fmt.Errorf("wait for build container returned %d", waitResponse.StatusCode),
				decodeErr, closeErr,
			)
		}
		if waited.Error != nil && waited.Error.Message != "" {
			return BuildContainerResult{}, fmt.Errorf("wait for build container: %s", waited.Error.Message)
		}
	}

	terminal, err := sm.inspectOwnedBuildContainer(ctx, info.ID, resourceID)
	if err != nil {
		if ctx.Err() != nil {
			return BuildContainerResult{}, sm.cleanupCanceledBuildContainer(
				ctx, containerName, resourceID, err)
		}
		return BuildContainerResult{}, err
	}
	if terminal.Running || terminal.Status != "exited" && terminal.Status != "dead" {
		return BuildContainerResult{}, fmt.Errorf(
			"build container %q is %s after wait, not terminal", containerName, terminal.Status)
	}
	logs, truncated, err := sm.readBuildContainerLogs(ctx, terminal.ID)
	if err != nil {
		if ctx.Err() != nil {
			return BuildContainerResult{}, sm.cleanupCanceledBuildContainer(
				ctx, containerName, resourceID, err)
		}
		return BuildContainerResult{}, err
	}
	return BuildContainerResult{
		ExitCode:      terminal.ExitCode,
		Logs:          logs,
		LogsTruncated: truncated,
	}, nil
}

func (sm *ServiceManager) inspectOwnedBuildContainer(
	ctx context.Context,
	identity, resourceID string,
) (buildContainerInfo, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(identity)+"/json", nil)
	if err != nil {
		return buildContainerInfo{}, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return buildContainerInfo{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return buildContainerInfo{}, fmt.Errorf("build container %q was not found", identity)
	}
	if response.StatusCode != http.StatusOK {
		return buildContainerInfo{}, fmt.Errorf("inspect build container returned %d", response.StatusCode)
	}
	var inspected struct {
		ID    string `json:"Id"`
		State struct {
			Status   string `json:"Status"`
			Running  bool   `json:"Running"`
			ExitCode int    `json:"ExitCode"`
		} `json:"State"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&inspected); err != nil {
		return buildContainerInfo{}, fmt.Errorf("decode build container inspect: %w", err)
	}
	if inspected.ID == "" {
		return buildContainerInfo{}, fmt.Errorf("build container %q has no immutable ID", identity)
	}
	if !exactLabels(inspected.Config.Labels, buildResourceLabels(resourceID)) {
		return buildContainerInfo{}, fmt.Errorf(
			"%w: build container %q", ErrDockerOwnershipConflict, identity)
	}
	return buildContainerInfo{
		ID:       inspected.ID,
		Status:   inspected.State.Status,
		Running:  inspected.State.Running,
		ExitCode: inspected.State.ExitCode,
	}, nil
}

func (sm *ServiceManager) cleanupCanceledBuildContainer(
	ctx context.Context,
	containerName, resourceID string,
	waitErr error,
) error {
	cause := waitErr
	if ctx.Err() != nil {
		cause = ctx.Err()
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), buildCleanupTimeout)
	defer cancel()
	if err := sm.StopAndRemoveBuildContainer(cleanupCtx, containerName, resourceID); err != nil {
		return errors.Join(cause, fmt.Errorf("cleanup canceled build container: %w", err))
	}
	return cause
}

func (sm *ServiceManager) readBuildContainerLogs(ctx context.Context, containerID string) (string, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(containerID)+
			"/logs?stdout=true&stderr=true&timestamps=false", nil)
	if err != nil {
		return "", false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return "", false, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", false, fmt.Errorf("read build container logs returned %d", response.StatusCode)
	}
	logs, truncated, err := readDockerLogStream(response.Body, maxBuildLogBytes)
	if err != nil {
		return "", false, fmt.Errorf("decode build container logs: %w", err)
	}
	return logs, truncated, nil
}

func readDockerLogStream(reader io.Reader, limit int) (string, bool, error) {
	var output strings.Builder
	truncated := false
	for {
		header := make([]byte, 8)
		if _, err := io.ReadFull(reader, header); err != nil {
			if errors.Is(err, io.EOF) {
				return output.String(), truncated, nil
			}
			return "", false, err
		}
		size := int64(binary.BigEndian.Uint32(header[4:8]))
		remaining := int64(limit - output.Len())
		readSize := min(size, max(remaining, 0))
		if readSize > 0 {
			chunk := make([]byte, readSize)
			if _, err := io.ReadFull(reader, chunk); err != nil {
				return "", false, err
			}
			output.Write(chunk)
		}
		if unread := size - readSize; unread > 0 {
			truncated = true
			if _, err := io.CopyN(io.Discard, reader, unread); err != nil {
				return "", false, err
			}
		}
	}
}

func (sm *ServiceManager) StopAndRemoveBuildContainer(ctx context.Context, containerName, resourceID string) error {
	status, labels, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return err
	}
	if status == "not_found" {
		return nil
	}
	if !exactLabels(labels, buildResourceLabels(resourceID)) {
		return fmt.Errorf("%w: refusing to remove build container %q",
			ErrDockerOwnershipConflict, containerName)
	}
	stop, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/"+url.PathEscape(containerName)+"/stop?t=2", nil)
	if response, stopErr := sm.dockerClient.Do(stop); stopErr == nil {
		response.Body.Close()
	}
	remove, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/containers/"+url.PathEscape(containerName)+"?force=true", nil)
	response, err := sm.dockerClient.Do(remove)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("remove build container returned %d", response.StatusCode)
	}
	return nil
}

func (identity AlloyDBIdentity) canonicalResource() string {
	return fmt.Sprintf("projects/%s/locations/%s/clusters/%s/instances/%s",
		identity.Project, identity.Location, identity.Cluster, identity.Instance)
}

func alloyDBDockerNames(identity AlloyDBIdentity) (string, string) {
	sum := sha256.Sum256([]byte(config.GetProfile() + "/" + identity.canonicalResource()))
	suffix := fmt.Sprintf("%x", sum[:8])
	return "minisky-alloydb-" + suffix, "minisky-alloydb-data-" + suffix
}

func alloyDBLabels(identity AlloyDBIdentity) map[string]string {
	labels := ownedDockerLabels()
	labels["minisky.service"] = "alloydb"
	labels["minisky.project"] = identity.Project
	labels["minisky.location"] = identity.Location
	labels["minisky.cluster"] = identity.Cluster
	labels["minisky.instance"] = identity.Instance
	return labels
}

func validateAlloyDBIdentity(identity AlloyDBIdentity) error {
	if identity.Project == "" || identity.Location == "" || identity.Cluster == "" || identity.Instance == "" {
		return fmt.Errorf("AlloyDB identity fields must be non-empty")
	}
	return nil
}

// ProvisionAlloyDB creates one exact-owned PostgreSQL-compatible AlloyDB data
// plane. It never adopts an existing container, even when exactly owned.
func (sm *ServiceManager) ProvisionAlloyDB(ctx context.Context, identity AlloyDBIdentity) (string, bool, error) {
	if err := validateAlloyDBIdentity(identity); err != nil {
		return "", false, err
	}
	containerName, volumeName := alloyDBDockerNames(identity)
	expected := alloyDBLabels(identity)

	status, labels, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return "", false, fmt.Errorf("inspect AlloyDB container: %w", err)
	}
	if status != "not_found" {
		if !exactLabels(labels, expected) {
			return "", false, fmt.Errorf("%w: AlloyDB container %q", ErrDockerOwnershipConflict, containerName)
		}
		return "", false, fmt.Errorf("owned AlloyDB container %q already exists", containerName)
	}

	exists, err := sm.imageExistsContext(ctx, alloyDBPostgresImage)
	if err != nil {
		return "", false, fmt.Errorf("inspect AlloyDB image: %w", err)
	}
	if !exists {
		if err := sm.pullImageInternal(ctx, alloyDBPostgresImage); err != nil {
			return "", false, fmt.Errorf("pull AlloyDB image: %w", err)
		}
	}
	volumeCreated, err := sm.ensureCloudSQLVolume(ctx, volumeName, expected)
	if err != nil {
		return "", false, fmt.Errorf("ensure AlloyDB volume: %w", err)
	}
	cleanupVolume := func() {
		if volumeCreated {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = sm.deleteCloudSQLVolume(cleanupCtx, volumeName, expected)
		}
	}

	payload := map[string]any{
		"Image": alloyDBPostgresImage,
		"Env": append(sm.standardEnv(),
			"POSTGRES_HOST_AUTH_METHOD=trust",
			"POSTGRES_DB=postgres",
			"PGDATA=/var/lib/postgresql/data/pgdata",
		),
		"ExposedPorts": map[string]any{"5432/tcp": struct{}{}},
		"HostConfig": map[string]any{
			"NetworkMode": "bridge",
			"PortBindings": map[string]any{
				"5432/tcp": []map[string]string{{"HostIp": "127.0.0.1", "HostPort": "0"}},
			},
			"Binds": []string{volumeName + ":/var/lib/postgresql/data"},
		},
		"Labels": expected,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		cleanupVolume()
		return "", false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(containerName), bytes.NewReader(body))
	if err != nil {
		cleanupVolume()
		return "", false, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := sm.doDocker(request)
	if err != nil {
		cleanupVolume()
		return "", false, fmt.Errorf("create AlloyDB container: %w", err)
	}
	_, drainErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusCreated || drainErr != nil || closeErr != nil {
		cleanupVolume()
		return "", false, errors.Join(
			fmt.Errorf("create AlloyDB container returned %d", response.StatusCode), drainErr, closeErr)
	}
	created := true
	cleanupBackend := func(cause error) (string, bool, error) {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupErr := sm.DeleteAlloyDB(cleanupCtx, identity)
		if cleanupErr != nil {
			return "", created, errors.Join(cause, fmt.Errorf("cleanup owned AlloyDB backend: %w", cleanupErr))
		}
		return "", created, cause
	}
	if err := sm.startContainerContext(ctx, containerName); err != nil {
		return cleanupBackend(fmt.Errorf("start AlloyDB container: %w", err))
	}
	endpoint, err := sm.alloyDBEndpoint(ctx, containerName)
	if err != nil {
		return cleanupBackend(fmt.Errorf("discover AlloyDB endpoint: %w", err))
	}
	if err := waitUntilPostgresReady(ctx, endpoint, 30*time.Second); err != nil {
		return cleanupBackend(err)
	}
	if err := sm.runOwnedCloudSQLCommand(ctx, containerName, expected,
		[]string{"psql", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", "SELECT 1"}); err != nil {
		return cleanupBackend(fmt.Errorf("AlloyDB SQL smoke failed: %w", err))
	}
	return endpoint, created, nil
}

// ReconcileAlloyDB returns an endpoint only for an exactly owned, running and
// protocol-ready backend. Missing resources are reported without mutation.
func (sm *ServiceManager) ReconcileAlloyDB(ctx context.Context, identity AlloyDBIdentity) (string, bool, error) {
	if err := validateAlloyDBIdentity(identity); err != nil {
		return "", false, err
	}
	containerName, _ := alloyDBDockerNames(identity)
	status, labels, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return "", false, fmt.Errorf("inspect AlloyDB container: %w", err)
	}
	if status == "not_found" {
		return "", false, nil
	}
	if !exactLabels(labels, alloyDBLabels(identity)) {
		return "", true, fmt.Errorf("%w: AlloyDB container %q", ErrDockerOwnershipConflict, containerName)
	}
	if status != "running" {
		return "", true, fmt.Errorf("owned AlloyDB container is %s", status)
	}
	endpoint, err := sm.alloyDBEndpoint(ctx, containerName)
	if err != nil {
		return "", true, err
	}
	if err := waitUntilPostgresReady(ctx, endpoint, 30*time.Second); err != nil {
		return "", true, err
	}
	return endpoint, true, nil
}

// DeleteAlloyDB removes only the container and volume with the complete
// immutable identity label set.
func (sm *ServiceManager) DeleteAlloyDB(ctx context.Context, identity AlloyDBIdentity) error {
	if err := validateAlloyDBIdentity(identity); err != nil {
		return err
	}
	containerName, volumeName := alloyDBDockerNames(identity)
	expected := alloyDBLabels(identity)
	status, labels, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return fmt.Errorf("inspect AlloyDB container: %w", err)
	}
	if status != "not_found" {
		if !exactLabels(labels, expected) {
			return fmt.Errorf("refusing to delete unowned AlloyDB container %q", containerName)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://localhost/containers/"+url.PathEscape(containerName)+"?force=true", nil)
		if err != nil {
			return err
		}
		response, err := sm.doDocker(request)
		if err != nil {
			return err
		}
		_, drainErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		closeErr := response.Body.Close()
		if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
			return errors.Join(fmt.Errorf("delete AlloyDB container returned %d", response.StatusCode), drainErr, closeErr)
		}
		if drainErr != nil || closeErr != nil {
			return errors.Join(drainErr, closeErr)
		}
	}
	if err := sm.deleteCloudSQLVolume(ctx, volumeName, expected); err != nil {
		return fmt.Errorf("delete AlloyDB volume: %w", err)
	}
	return nil
}

func (sm *ServiceManager) alloyDBEndpoint(ctx context.Context, containerName string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(containerName)+"/json", nil)
	if err != nil {
		return "", err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("inspect AlloyDB port returned %d", response.StatusCode)
	}
	var info struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
	}
	if err := json.NewDecoder(response.Body).Decode(&info); err != nil {
		return "", err
	}
	bindings := info.NetworkSettings.Ports["5432/tcp"]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" || bindings[0].HostPort == "" {
		return "", fmt.Errorf("AlloyDB container has no unique loopback PostgreSQL binding")
	}
	return net.JoinHostPort(bindings[0].HostIP, bindings[0].HostPort), nil
}

func (sm *ServiceManager) imageExistsContext(ctx context.Context, image string) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/images/"+url.PathEscape(image)+"/json", nil)
	if err != nil {
		return false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("inspect image returned %d", response.StatusCode)
	}
}

func waitUntilPostgresReady(ctx context.Context, endpoint string, timeout time.Duration) error {
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
			deadline := time.Now().Add(time.Second)
			if contextDeadline, ok := waitCtx.Deadline(); ok && contextDeadline.Before(deadline) {
				deadline = contextDeadline
			}
			_ = connection.SetDeadline(deadline)
			_, err = connection.Write(packet)
			for err == nil {
				header := make([]byte, 5)
				if _, err = io.ReadFull(connection, header); err != nil {
					break
				}
				length := int(binary.BigEndian.Uint32(header[1:]))
				if length < 4 || length > 1<<20 {
					err = fmt.Errorf("invalid PostgreSQL response length")
					break
				}
				message := make([]byte, length-4)
				if _, err = io.ReadFull(connection, message); err != nil {
					break
				}
				if header[0] == 'E' {
					err = fmt.Errorf("PostgreSQL startup rejected")
					break
				}
				if header[0] == 'Z' {
					_ = connection.Close()
					return nil
				}
			}
			_ = connection.Close()
		}
		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf("PostgreSQL at %q not protocol-ready after %s: %w", endpoint, timeout, waitCtx.Err())
		case <-timer.C:
		}
	}
}

func (sm *ServiceManager) ProvisionCloudSQLVM(
	ctx context.Context,
	requested CloudSQLBackendSpec,
) (string, CloudSQLBackendSpec, bool, error) {
	spec, runtimeConfig, err := resolveCloudSQLBackendSpec(requested)
	if err != nil {
		return "", requested, false, err
	}
	containerName, volumeName, _ := cloudSQLDockerNames(spec.Project, spec.Instance)
	log.Printf("[Orchestrator] Provisioning Cloud SQL VM: %s (image: %s)", containerName, spec.Image)

	status, _, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return "", spec, false, fmt.Errorf("inspect Cloud SQL container: %w", err)
	}
	if status != "not_found" {
		return "", spec, false, fmt.Errorf("Cloud SQL container %q already exists", containerName)
	}
	if !requested.CreationIntent || requested.ImageID == "" {
		return "", spec, false, errors.New(
			"Cloud SQL creation intent and immutable image ID must be persisted before provisioning",
		)
	}
	imageID, err := sm.inspectCloudSQLImageID(ctx, spec.Image)
	if err != nil {
		return "", spec, false, err
	}
	if imageID != requested.ImageID {
		return "", spec, false, fmt.Errorf(
			"Cloud SQL image %q resolved to %q, want authorized immutable image ID %q",
			spec.Image,
			imageID,
			requested.ImageID,
		)
	}
	resourceLabels := cloudSQLLabels(spec)

	_, err = sm.ensureCloudSQLVolume(ctx, volumeName, resourceLabels)
	if err != nil {
		return "", spec, false, err
	}
	volume, volumeFound, err := sm.inspectExactCloudSQLVolume(ctx, volumeName, resourceLabels)
	if err != nil || !volumeFound {
		if err != nil {
			return "", spec, false, err
		}
		return "", spec, false, fmt.Errorf("created Cloud SQL volume %q is absent", volumeName)
	}
	spec.VolumeIdentity = volume.identity()
	payload := map[string]interface{}{
		"Image": spec.ImageID,
		"Env":   append(sm.standardEnv(), runtimeConfig.Env...),
		"ExposedPorts": map[string]interface{}{
			runtimeConfig.ContainerPort: struct{}{},
		},
		"HostConfig": map[string]interface{}{
			"NetworkMode": networkName,
			"PortBindings": map[string]interface{}{
				runtimeConfig.ContainerPort: []map[string]string{{
					"HostIp": "127.0.0.1", "HostPort": "0",
				}},
			},
			"Binds": []string{
				fmt.Sprintf("%s:%s", volumeName, runtimeConfig.MountTarget),
			},
		},
		"Labels": resourceLabels,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", spec, false, fmt.Errorf("encode Cloud SQL container: %w", err)
	}
	createRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(containerName),
		bytes.NewReader(encoded),
	)
	if err != nil {
		return "", spec, false, err
	}
	createRequest.Header.Set("Content-Type", "application/json")
	response, err := sm.doDocker(createRequest)
	if err != nil {
		return "", spec, false, fmt.Errorf("create SQL container: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return "", spec, false, fmt.Errorf(
			"create SQL container returned HTTP %d: %s",
			response.StatusCode,
			strings.TrimSpace(string(detail)),
		)
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		return "", spec, true, fmt.Errorf("decode created SQL container: %w", err)
	}
	if !dockerContainerIDPattern.MatchString(created.ID) {
		return "", spec, true, fmt.Errorf("created SQL container returned invalid immutable ID %q", created.ID)
	}
	createdContainer, found, inspectErr := sm.inspectCloudSQLContainer(ctx, created.ID)
	if inspectErr != nil || !found {
		cleanupErr := sm.removeCreatedCloudSQLContainer(ctx, created.ID, resourceLabels)
		if inspectErr != nil {
			return "", spec, true, errors.Join(
				fmt.Errorf("inspect created SQL container before start: %w", inspectErr),
				cleanupErr,
			)
		}
		return "", spec, true, errors.Join(
			errors.New("created SQL container disappeared before start"),
			cleanupErr,
		)
	}
	if err := validateCloudSQLContainer(
		createdContainer,
		runtimeConfig,
		volumeName,
		resourceLabels,
	); err != nil {
		return "", spec, true, errors.Join(
			err,
			sm.removeCreatedCloudSQLContainer(ctx, created.ID, resourceLabels),
		)
	}
	revalidatedVolume, found, volumeErr := sm.inspectExactCloudSQLVolume(ctx, volumeName, resourceLabels)
	if volumeErr != nil || !found || revalidatedVolume.identity() != spec.VolumeIdentity {
		cleanupErr := sm.removeCreatedCloudSQLContainer(ctx, created.ID, resourceLabels)
		if volumeErr != nil {
			return "", spec, true, errors.Join(volumeErr, cleanupErr)
		}
		return "", spec, true, errors.Join(
			errors.New("Cloud SQL volume identity changed before container start"),
			cleanupErr,
		)
	}
	if err := sm.startContainerContext(ctx, created.ID); err != nil {
		return "", spec, true, fmt.Errorf("start SQL container: %w", err)
	}
	container, found, err := sm.inspectCloudSQLContainer(ctx, created.ID)
	if err != nil {
		return "", spec, true, fmt.Errorf("inspect started SQL container: %w", err)
	}
	if !found {
		return "", spec, true, errors.New("created SQL container disappeared after start")
	}
	if err := validateCloudSQLContainer(
		container,
		runtimeConfig,
		volumeName,
		resourceLabels,
	); err != nil {
		return "", spec, true, err
	}
	internalURL, err := cloudSQLEndpoint(container, runtimeConfig.ContainerPort)
	if err != nil {
		return "", spec, true, fmt.Errorf("port discovery: %w", err)
	}
	ready := sm.cloudSQLReady
	if ready == nil {
		ready = waitUntilCloudSQLReady
	}
	address := strings.TrimPrefix(internalURL, "http://")
	if err := ready(ctx, address, spec.DatabaseVersion, 30*time.Second); err != nil {
		return "", spec, true, fmt.Errorf("Cloud SQL initial readiness failed: %w", err)
	}
	authReady := sm.cloudSQLAuthReady
	if authReady == nil {
		authReady = sm.waitUntilCloudSQLAuthenticatedReady
	}
	if err := authReady(
		ctx,
		container.ID,
		resourceLabels,
		spec.DatabaseVersion,
		30*time.Second,
	); err != nil {
		return "", spec, true, fmt.Errorf("Cloud SQL initial authenticated readiness failed: %w", err)
	}
	log.Printf("[Orchestrator] ✅ SQL Instance '%s' ONLINE at %s", spec.Instance, internalURL)
	return internalURL, spec, true, nil
}

// DeleteCloudSQLVM stops and forcefully removes a Cloud SQL node.
func (sm *ServiceManager) DeleteCloudSQLVM(spec CloudSQLBackendSpec) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return sm.DeleteCloudSQLVMContext(ctx, spec)
}

func (sm *ServiceManager) DeleteCloudSQLVMContext(ctx context.Context, requested CloudSQLBackendSpec) error {
	spec, runtimeConfig, err := resolveCloudSQLBackendSpec(requested)
	if err != nil {
		return err
	}
	if requested.Image == "" {
		return errors.New("Cloud SQL persisted image is required for deletion")
	}
	if requested.ImageID == "" ||
		(requested.VolumeIdentity == "" && !requested.CreationIntent) {
		return errors.New("Cloud SQL immutable image and volume identities are required for deletion")
	}
	containerName, volumeName, _ := cloudSQLDockerNames(spec.Project, spec.Instance)
	expected := cloudSQLLabels(spec)
	log.Printf("[Orchestrator] Tearing down Cloud SQL VM: %s", containerName)
	volume, volumeFound, err := sm.inspectExactCloudSQLVolume(ctx, volumeName, expected)
	if err != nil {
		return err
	}
	if volumeFound && spec.VolumeIdentity == "" && spec.CreationIntent {
		spec.VolumeIdentity = volume.identity()
	}
	if volumeFound && volume.identity() != spec.VolumeIdentity {
		return fmt.Errorf("refusing to delete Cloud SQL volume %q with changed immutable identity", volumeName)
	}
	container, found, err := sm.inspectCloudSQLContainer(ctx, containerName)
	if err != nil {
		return err
	}
	if found {
		if err := validateCloudSQLContainer(container, runtimeConfig, volumeName, expected); err != nil {
			return fmt.Errorf("refusing to delete incompatible Cloud SQL container %q: %w", containerName, err)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://localhost/containers/"+url.PathEscape(container.ID)+"?force=true", nil)
		resp, err := sm.dockerClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("delete Cloud SQL container returned %d", resp.StatusCode)
		}
	}
	return sm.deleteCloudSQLVolume(ctx, volumeName, expected, spec.VolumeIdentity)
}

// ExecuteCloudSQLAdmin applies a bounded database or user mutation to the
// exact profile-owned Cloud SQL container. Command arguments are never logged.
func (sm *ServiceManager) ExecuteCloudSQLAdmin(
	ctx context.Context,
	requested CloudSQLBackendSpec,
	action, name, password string,
) error {
	spec, runtimeConfig, err := resolveCloudSQLBackendSpec(requested)
	if err != nil {
		return err
	}
	if requested.Image == "" {
		return errors.New("Cloud SQL persisted image is required for administration")
	}
	if requested.ImageID == "" {
		return errors.New("Cloud SQL immutable image ID is required for administration")
	}
	command, err := cloudSQLAdminCommand(spec.DatabaseVersion, action, name, password)
	if err != nil {
		return err
	}

	containerName, volumeName, _ := cloudSQLDockerNames(spec.Project, spec.Instance)
	expected := cloudSQLLabels(spec)
	container, found, err := sm.inspectCloudSQLContainer(ctx, containerName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Cloud SQL container %q is absent", containerName)
	}
	if err := validateCloudSQLContainer(container, runtimeConfig, volumeName, expected); err != nil {
		return err
	}
	return sm.runOwnedCloudSQLCommand(ctx, container.ID, expected, command)
}

func cloudSQLAdminCommand(version, action, name, password string) ([]string, error) {
	if !validCloudSQLAdminName(name) {
		return nil, fmt.Errorf("invalid Cloud SQL database or user name")
	}
	var command []string
	switch {
	case strings.HasPrefix(version, "POSTGRES"):
		identifier := `"` + name + `"`
		statement := ""
		switch action {
		case "CREATE_DATABASE":
			statement = "CREATE DATABASE " + identifier
		case "DELETE_DATABASE":
			statement = "DROP DATABASE " + identifier
		case "CREATE_USER":
			escapedPassword := strings.ReplaceAll(password, `'`, `''`)
			statement = "CREATE USER " + identifier + " WITH PASSWORD '" + escapedPassword + "'"
		case "DELETE_USER":
			statement = "DROP USER " + identifier
		default:
			return nil, fmt.Errorf("unsupported Cloud SQL admin action")
		}
		command = []string{"psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", statement}
	case strings.HasPrefix(version, "MYSQL"):
		identifier := "`" + name + "`"
		statement := ""
		switch action {
		case "CREATE_DATABASE":
			statement = "CREATE DATABASE " + identifier
		case "DELETE_DATABASE":
			statement = "DROP DATABASE " + identifier
		case "CREATE_USER":
			escapedPassword := strings.ReplaceAll(password, `\`, `\\`)
			escapedPassword = strings.ReplaceAll(escapedPassword, `'`, `''`)
			statement = "CREATE USER '" + name + "'@'%' IDENTIFIED BY '" + escapedPassword + "'"
		case "DELETE_USER":
			statement = "DROP USER '" + name + "'@'%'"
		default:
			return nil, fmt.Errorf("unsupported Cloud SQL admin action")
		}
		command = []string{"mysql", "-uroot", "-pminisky", "-e", statement}
	default:
		return nil, fmt.Errorf("unsupported Cloud SQL database version")
	}
	return command, nil
}

func (sm *ServiceManager) runOwnedCloudSQLCommand(
	ctx context.Context,
	containerID string,
	expectedLabels map[string]string,
	command []string,
) error {
	status, labels, err := sm.inspectContainerContext(ctx, containerID)
	if err != nil {
		return fmt.Errorf("inspect Cloud SQL command target: %w", err)
	}
	if status != "running" || !exactLabels(labels, expectedLabels) {
		return fmt.Errorf("Cloud SQL command target is unavailable or not exactly owned")
	}
	payload, err := json.Marshal(map[string]interface{}{
		"AttachStdin": false, "AttachStdout": true, "AttachStderr": true,
		"Tty": false, "Cmd": command,
	})
	if err != nil {
		return err
	}
	createRequest, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/"+url.PathEscape(containerID)+"/exec", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	createRequest.Header.Set("Content-Type", "application/json")
	createResponse, err := sm.dockerClient.Do(createRequest)
	if err != nil {
		return err
	}
	defer createResponse.Body.Close()
	if createResponse.StatusCode != http.StatusCreated {
		return fmt.Errorf("create Cloud SQL admin exec returned %d", createResponse.StatusCode)
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(createResponse.Body).Decode(&created); err != nil {
		return fmt.Errorf("decode Cloud SQL admin exec: %w", err)
	}
	if created.ID == "" {
		return fmt.Errorf("decode Cloud SQL admin exec: missing exec ID")
	}

	startRequest, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/exec/"+url.PathEscape(created.ID)+"/start",
		strings.NewReader(`{"Detach":false,"Tty":false}`))
	if err != nil {
		return err
	}
	startRequest.Header.Set("Content-Type", "application/json")
	startResponse, err := sm.dockerClient.Do(startRequest)
	if err != nil {
		return err
	}
	_, drainErr := io.Copy(io.Discard, io.LimitReader(startResponse.Body, 1<<20))
	closeErr := startResponse.Body.Close()
	if startResponse.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("start Cloud SQL admin exec returned %d", startResponse.StatusCode)
	}
	if drainErr != nil || closeErr != nil {
		return errors.Join(drainErr, closeErr)
	}

	inspectRequest, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/exec/"+url.PathEscape(created.ID)+"/json", nil)
	if err != nil {
		return err
	}
	inspectResponse, err := sm.dockerClient.Do(inspectRequest)
	if err != nil {
		return err
	}
	defer inspectResponse.Body.Close()
	if inspectResponse.StatusCode != http.StatusOK {
		return fmt.Errorf("inspect Cloud SQL admin exec returned %d", inspectResponse.StatusCode)
	}
	var result struct {
		Running  bool `json:"Running"`
		ExitCode int  `json:"ExitCode"`
	}
	if err := json.NewDecoder(inspectResponse.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode Cloud SQL admin result: %w", err)
	}
	if result.Running || result.ExitCode != 0 {
		return fmt.Errorf("Cloud SQL admin command failed")
	}
	return nil
}

func validCloudSQLAdminName(name string) bool {
	if name == "" || len(name) > 63 {
		return false
	}
	for _, char := range name {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

func (sm *ServiceManager) ensureCloudSQLVolume(ctx context.Context, name string, labels map[string]string) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/volumes/"+url.PathEscape(name), nil)
	if err != nil {
		return false, err
	}
	resp, err := sm.doDocker(request)
	if err != nil {
		return false, fmt.Errorf("inspect Cloud SQL volume: %w", err)
	}
	if resp.StatusCode == http.StatusOK {
		var volume struct {
			Labels map[string]string `json:"Labels"`
		}
		err := json.NewDecoder(resp.Body).Decode(&volume)
		resp.Body.Close()
		if err != nil {
			return false, fmt.Errorf("decode Cloud SQL volume ownership: %w", err)
		}
		if !exactLabels(volume.Labels, labels) {
			return false, fmt.Errorf("Cloud SQL volume %q exists but is not owned by this profile and resource", name)
		}
		return false, nil
	}
	status := resp.StatusCode
	resp.Body.Close()
	if status != http.StatusNotFound {
		return false, fmt.Errorf("inspect Cloud SQL volume returned %d", status)
	}
	payload, _ := json.Marshal(map[string]any{"Name": name, "Labels": labels})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/volumes/create", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err = sm.dockerClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("create Cloud SQL volume: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("create Cloud SQL volume returned %d", resp.StatusCode)
	}
	return true, nil
}

func (sm *ServiceManager) deleteCloudSQLVolume(
	ctx context.Context,
	name string,
	labels map[string]string,
	expectedIdentity ...string,
) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/volumes/"+url.PathEscape(name), nil)
	if err != nil {
		return err
	}
	resp, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		status := resp.StatusCode
		resp.Body.Close()
		return fmt.Errorf("inspect Cloud SQL volume returned %d", status)
	}
	var volume cloudSQLVolumeInspect
	err = json.NewDecoder(resp.Body).Decode(&volume)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("decode Cloud SQL volume ownership: %w", err)
	}
	if !exactLabels(volume.Labels, labels) {
		return fmt.Errorf("refusing to delete unowned Cloud SQL volume %q", name)
	}
	if len(expectedIdentity) > 0 && expectedIdentity[0] != "" {
		if volume.Name != name || volume.CreatedAt == "" || volume.Mountpoint == "" ||
			volume.identity() != expectedIdentity[0] {
			return fmt.Errorf("refusing to delete Cloud SQL volume %q with changed immutable identity", name)
		}
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, "http://localhost/volumes/"+url.PathEscape(name), nil)
	resp, err = sm.dockerClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete Cloud SQL volume returned %d", resp.StatusCode)
	}
	return nil
}

func cloudSQLDockerNames(project, instance string) (string, string, string) {
	resourceID := config.GetProfile() + "/" + project + "/" + instance
	sum := sha256.Sum256([]byte(resourceID))
	safe := strings.ToLower(instance)
	safe = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
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
	return "minisky-sql-" + safe + "-" + suffix,
		"minisky-db-" + safe + "-" + suffix, resourceID
}

// DeleteComputeVM permanently destroys a physical Data Plane compute instance.
func (sm *ServiceManager) DeleteComputeVM(containerName string) error {
	return sm.DeleteComputeVMContext(context.Background(), containerName)
}

// DeleteComputeVMContext removes only the exact current-profile Compute
// container identity and bounds all Docker calls by ctx.
func (sm *ServiceManager) DeleteComputeVMContext(ctx context.Context, containerName string) error {
	labels := ownedDockerLabels()
	labels["minisky.service"] = "compute-instance"
	labels["minisky.resource"] = containerName
	return sm.deleteComputeVMContext(ctx, containerName, labels)
}

func (sm *ServiceManager) DeleteComputeInstance(
	ctx context.Context,
	identity ComputeInstanceIdentity,
) error {
	containerName, err := identity.DockerName()
	if err != nil {
		return err
	}
	labels, _ := identity.labels()
	return sm.deleteComputeVMContext(ctx, containerName, labels)
}

// DeleteLegacyComputeVM removes only the pre-scoped container name when it has
// the exact legacy labels for the current profile. It is cleanup-only and is
// never used to adopt or provision a Compute instance.
func (sm *ServiceManager) DeleteLegacyComputeVM(instanceName string) error {
	if !gcpNetworkName.MatchString(instanceName) {
		return fmt.Errorf("invalid legacy Compute instance name")
	}
	containerName := "minisky-vm-" + instanceName
	labels := ownedDockerLabels()
	labels["minisky.service"] = "compute-instance"
	labels["minisky.resource"] = containerName
	return sm.deleteComputeVM(containerName, labels)
}

func (sm *ServiceManager) deleteComputeVM(containerName string, expectedLabels map[string]string) error {
	return sm.deleteComputeVMContext(context.Background(), containerName, expectedLabels)
}

func (sm *ServiceManager) deleteComputeVMContext(ctx context.Context, containerName string, expectedLabels map[string]string) error {
	log.Printf("[Orchestrator] Tearing down Data Plane VM: %s", containerName)
	if !validDockerResourceName(containerName) {
		return fmt.Errorf("invalid Compute container name")
	}
	status, labels, err := sm.inspectContainerContext(ctx, containerName)
	if err != nil {
		return err
	}
	if status == "not_found" {
		return nil
	}
	if !containsLabels(labels, expectedLabels) {
		return fmt.Errorf("refusing to delete unowned Compute container %q", containerName)
	}

	stopURL := fmt.Sprintf("http://localhost/containers/%s/stop?t=2", url.PathEscape(containerName))
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, stopURL, nil)
	if response, stopErr := sm.dockerClient.Do(req); stopErr == nil {
		response.Body.Close()
	}

	rmURL := fmt.Sprintf("http://localhost/containers/%s?force=true", url.PathEscape(containerName))
	req, _ = http.NewRequestWithContext(ctx, http.MethodDelete, rmURL, nil)
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
	expected := ownedDockerLabels()
	expected["minisky.service"] = "compute-instance"
	expected["minisky.resource"] = containerName
	return containsLabels(labels, expected)
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
	if err := sm.requireOwnedContainer(containerName); err != nil {
		return "", err
	}
	url := fmt.Sprintf("http://localhost/containers/%s/logs?stdout=true&stderr=true&tail=%d", containerName, tail)
	return sm.fetchLogs(url)
}

// GetContainerLogsSince returns stdout/stderr logs since a specific unix timestamp
func (sm *ServiceManager) GetContainerLogsSince(containerName string, since int64) (string, error) {
	if err := sm.requireOwnedContainer(containerName); err != nil {
		return "", err
	}
	url := fmt.Sprintf("http://localhost/containers/%s/logs?stdout=true&stderr=true&timestamps=true&since=%d", containerName, since)
	return sm.fetchLogs(url)
}

func (sm *ServiceManager) requireOwnedContainer(name string) error {
	status, labels, err := sm.inspectContainer(name)
	if err != nil {
		return err
	}
	if status == "not_found" {
		return fmt.Errorf("container %q not found", name)
	}
	if !isOwnedDockerResource(labels) {
		return fmt.Errorf("container %q is not owned by the active MiniSky profile", name)
	}
	return nil
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
	if !validDockerResourceName(name) {
		return "", fmt.Errorf("invalid container name")
	}
	status, labels, err := sm.inspectContainer(name)
	if err != nil {
		return "", fmt.Errorf("inspect command target: %w", err)
	}
	if status != "running" {
		return "", fmt.Errorf("command target %q is not running", name)
	}
	if !isOwnedComputeVM(labels, name) {
		return "", fmt.Errorf("command target %q is not owned by this profile and Compute resource", name)
	}
	log.Printf("[Orchestrator] Executing command in owned container %q (%d arguments)", name, len(cmd))

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
	case "storage.googleapis.com":
		runtimeName = "storage"
	case "pubsub.googleapis.com":
		runtimeName = "pubsub"
	default:
		return configured, nil
	}

	containerPath := "/data"
	if separator := strings.LastIndex(configured, ":"); separator >= 0 {
		containerPath = configured[separator+1:]
	}
	hostPath := filepath.Join(config.GetRuntimeDir(), runtimeName)
	if err := os.MkdirAll(hostPath, 0o700); err != nil {
		return "", fmt.Errorf("create %s profile runtime directory: %w", runtimeName, err)
	}
	info, err := os.Lstat(hostPath)
	if err != nil {
		return "", fmt.Errorf("inspect %s profile runtime directory: %w", runtimeName, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%s profile runtime path is not a directory", runtimeName)
	}
	if err := os.Chmod(hostPath, 0o700); err != nil {
		return "", fmt.Errorf("secure %s profile runtime directory: %w", runtimeName, err)
	}
	return hostPath + ":" + containerPath, nil
}

func isDurableEmulator(domain string) bool {
	return domain == "storage.googleapis.com" || domain == "pubsub.googleapis.com"
}

func emulatorContainerName(domain, configured string) string {
	if !isDurableEmulator(domain) {
		return configured
	}
	service := strings.TrimSuffix(domain, ".googleapis.com")
	sum := sha256.Sum256([]byte(config.GetProfile() + "\x00" + domain))
	return fmt.Sprintf("minisky-%s-%x", service, sum[:8])
}

func durableEmulatorLabels(domain string) map[string]string {
	labels := ownedDockerLabels()
	labels["minisky.service"] = domain
	return labels
}

func currentDockerUser() (string, error) {
	if runtime.GOOS == "windows" {
		return "", nil
	}
	current, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("resolve host identity for Storage emulator: %w", err)
	}
	for name, value := range map[string]string{"uid": current.Uid, "gid": current.Gid} {
		if _, err := strconv.ParseUint(value, 10, 32); err != nil {
			return "", fmt.Errorf("resolve host identity for Storage emulator: invalid %s %q", name, value)
		}
	}
	return current.Uid + ":" + current.Gid, nil
}

func durableEmulatorContainerUser(domain string) (string, error) {
	if domain != "storage.googleapis.com" {
		return "", nil
	}
	return currentDockerUser()
}

func durableEmulatorConfig(
	domain string,
	cfg config.EmulatorConfig,
	env []string,
) (ContainerConfig, map[string]string, error) {
	if !isDurableEmulator(domain) {
		return ContainerConfig{}, nil, fmt.Errorf("domain %q has no durable emulator contract", domain)
	}
	volume, err := resolveEmulatorVolume(domain, ":/data")
	if err != nil {
		return ContainerConfig{}, nil, err
	}
	command := append([]string(nil), cfg.Cmd...)
	containerUser, err := durableEmulatorContainerUser(domain)
	if err != nil {
		return ContainerConfig{}, nil, err
	}
	switch domain {
	case "storage.googleapis.com":
		command, err = ensureCommandFlag(command, "-backend", "filesystem")
		if err != nil {
			return ContainerConfig{}, nil, err
		}
		command, err = ensureCommandFlag(command, "-filesystem-root", "/data")
		if err != nil {
			return ContainerConfig{}, nil, err
		}
	case "pubsub.googleapis.com":
		command, err = ensureCommandFlag(command, "--data-dir", "/data")
		if err != nil {
			return ContainerConfig{}, nil, err
		}
	}
	labels := durableEmulatorLabels(domain)
	if containerUser != "" {
		labels["minisky.runtime-user"] = containerUser
	}
	return ContainerConfig{
		Name:            emulatorContainerName(domain, cfg.Name),
		Image:           cfg.Image,
		ContainerPort:   cfg.Port,
		AdditionalPorts: append([]string(nil), cfg.AdditionalPorts...),
		Cmd:             command,
		Volume:          volume,
		Env:             append([]string(nil), env...),
		User:            containerUser,
	}, labels, nil
}

func ensureCommandFlag(command []string, flagName, expected string) ([]string, error) {
	for index, argument := range command {
		switch {
		case argument == flagName:
			if index+1 >= len(command) || command[index+1] != expected {
				return nil, fmt.Errorf("%w: %s must be %q",
					ErrDockerConfiguration, flagName, expected)
			}
			return command, nil
		case strings.HasPrefix(argument, flagName+"="):
			if strings.TrimPrefix(argument, flagName+"=") != expected {
				return nil, fmt.Errorf("%w: %s must be %q",
					ErrDockerConfiguration, flagName, expected)
			}
			return command, nil
		}
	}
	return append(command, flagName+"="+expected), nil
}

type durableEmulatorInspect struct {
	ID     string
	Status string
	Labels map[string]string
	Mounts []struct {
		Type        string `json:"Type"`
		Source      string `json:"Source"`
		Destination string `json:"Destination"`
		RW          bool   `json:"RW"`
	} `json:"Mounts"`
	Ports map[string][]struct {
		HostIP   string `json:"HostIp"`
		HostPort string `json:"HostPort"`
	}
}

func (sm *ServiceManager) ensureDurableEmulatorRunning(
	ctx context.Context,
	domain string,
	cfg config.EmulatorConfig,
	env ...string,
) (string, error) {
	container, expectedLabels, err := durableEmulatorConfig(domain, cfg, env)
	if err != nil {
		return "", err
	}
	sm.emulatorMu.Lock()
	defer sm.emulatorMu.Unlock()

	inspected, found, err := sm.inspectDurableEmulator(ctx, container.Name)
	if err != nil {
		return "", fmt.Errorf("inspect %s emulator: %w", domain, err)
	}
	if found {
		if err := validateDurableEmulator(inspected, container, expectedLabels); err != nil {
			return "", err
		}
		if inspected.Status != "running" {
			if err := sm.startContainerContext(ctx, container.Name); err != nil {
				return "", fmt.Errorf("start %s emulator: %w", domain, err)
			}
		}
	} else {
		exists, err := sm.imageExistsContext(ctx, container.Image)
		if err != nil {
			return "", fmt.Errorf("inspect %s emulator image: %w", domain, err)
		}
		if !exists {
			if err := sm.pullImageInternal(ctx, container.Image); err != nil {
				return "", fmt.Errorf("pull %s emulator image: %w", domain, err)
			}
		}
		if err := sm.createDurableEmulator(ctx, container, expectedLabels); err != nil {
			if !errors.Is(err, errDockerCreateConflict) {
				return "", fmt.Errorf("create %s emulator: %w", domain, err)
			}
			winner, winnerFound, inspectErr := sm.inspectDurableEmulator(ctx, container.Name)
			if inspectErr != nil {
				return "", errors.Join(err, inspectErr)
			}
			if !winnerFound {
				return "", err
			}
			if validateErr := validateDurableEmulator(winner, container, expectedLabels); validateErr != nil {
				return "", errors.Join(err, validateErr)
			}
		}
		if err := sm.startContainerContext(ctx, container.Name); err != nil {
			return "", fmt.Errorf("start %s emulator: %w", domain, err)
		}
	}

	running, found, err := sm.inspectDurableEmulator(ctx, container.Name)
	if err != nil {
		return "", fmt.Errorf("discover %s emulator endpoint: %w", domain, err)
	}
	if !found {
		return "", fmt.Errorf("%s emulator disappeared after start", domain)
	}
	if err := validateDurableEmulator(running, container, expectedLabels); err != nil {
		return "", err
	}
	if running.Status != "running" {
		return "", fmt.Errorf("%s emulator is %s after start", domain, running.Status)
	}
	bindings := running.Ports[container.ContainerPort]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" || bindings[0].HostPort == "" {
		return "", fmt.Errorf("%s emulator has no unique loopback port binding", domain)
	}
	endpoint := "http://" + net.JoinHostPort(bindings[0].HostIP, bindings[0].HostPort)
	ready := sm.emulatorReady
	if ready == nil {
		ready = waitUntilHTTPReady
	}
	if err := ready(endpoint, 60*time.Second); err != nil {
		return "", fmt.Errorf("%s emulator readiness: %w", domain, err)
	}
	return endpoint, nil
}

func validateDurableEmulator(
	inspected durableEmulatorInspect,
	container ContainerConfig,
	expectedLabels map[string]string,
) error {
	if !hasExpectedDurableOwnership(inspected.Labels, expectedLabels) {
		return fmt.Errorf("%w: emulator container %q", ErrDockerOwnershipConflict, container.Name)
	}
	separator := strings.LastIndex(container.Volume, ":")
	if separator <= 0 {
		return fmt.Errorf("%w: emulator container %q has ambiguous mount configuration",
			ErrDockerConfiguration, container.Name)
	}
	expectedSource := filepath.Clean(container.Volume[:separator])
	expectedDestination := container.Volume[separator+1:]
	if len(inspected.Mounts) != 1 ||
		inspected.Mounts[0].Type != "bind" ||
		filepath.Clean(inspected.Mounts[0].Source) != expectedSource ||
		inspected.Mounts[0].Destination != expectedDestination ||
		!inspected.Mounts[0].RW {
		return fmt.Errorf("%w: emulator container %q has an unexpected data mount",
			ErrDockerOwnershipConflict, container.Name)
	}
	return nil
}

func hasExpectedDurableOwnership(actual, expected map[string]string) bool {
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	for key := range actual {
		if _, owned := expected[key]; owned {
			continue
		}
		normalized := strings.ToLower(key)
		if strings.HasPrefix(normalized, "minisky.") {
			return false
		}
	}
	return true
}

func (sm *ServiceManager) inspectDurableEmulator(
	ctx context.Context,
	name string,
) (durableEmulatorInspect, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(name)+"/json", nil)
	if err != nil {
		return durableEmulatorInspect{}, false, err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return durableEmulatorInspect{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return durableEmulatorInspect{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return durableEmulatorInspect{}, false,
			fmt.Errorf("Docker returned HTTP %d", response.StatusCode)
	}
	var raw struct {
		ID    string `json:"Id"`
		State struct {
			Status string `json:"Status"`
		} `json:"State"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
		Mounts []struct {
			Type        string `json:"Type"`
			Source      string `json:"Source"`
			Destination string `json:"Destination"`
			RW          bool   `json:"RW"`
		} `json:"Mounts"`
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&raw); err != nil {
		return durableEmulatorInspect{}, false, err
	}
	return durableEmulatorInspect{
		ID:     raw.ID,
		Status: raw.State.Status,
		Labels: raw.Config.Labels,
		Mounts: raw.Mounts,
		Ports:  raw.NetworkSettings.Ports,
	}, true, nil
}

var errDockerCreateConflict = errors.New("Docker container create conflict")

func (sm *ServiceManager) createDurableEmulator(
	ctx context.Context,
	container ContainerConfig,
	labels map[string]string,
) error {
	exposedPorts := map[string]any{container.ContainerPort: struct{}{}}
	portBindings := map[string]any{
		container.ContainerPort: []map[string]string{{"HostIp": "127.0.0.1", "HostPort": "0"}},
	}
	for _, port := range container.AdditionalPorts {
		exposedPorts[port] = struct{}{}
		portBindings[port] = []map[string]string{{"HostIp": "127.0.0.1", "HostPort": "0"}}
	}
	payload := map[string]any{
		"Image":        container.Image,
		"Cmd":          container.Cmd,
		"Env":          container.Env,
		"ExposedPorts": exposedPorts,
		"Labels":       labels,
		"HostConfig": map[string]any{
			"NetworkMode":  "bridge",
			"PortBindings": portBindings,
			"Binds":        []string{container.Volume},
		},
	}
	if container.User != "" {
		payload["User"] = container.User
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(container.Name), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		return errDockerCreateConflict
	}
	if response.StatusCode != http.StatusCreated {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("Docker returned HTTP %d: %s",
			response.StatusCode, strings.TrimSpace(string(detail)))
	}
	return nil
}

func (sm *ServiceManager) removeDurableEmulatorContainer(
	ctx context.Context,
	domain string,
	cfg config.EmulatorConfig,
) error {
	if !isDurableEmulator(domain) {
		return fmt.Errorf("domain %q has no durable emulator contract", domain)
	}
	service := strings.TrimSuffix(domain, ".googleapis.com")
	containerUser, err := durableEmulatorContainerUser(domain)
	if err != nil {
		return err
	}
	container := ContainerConfig{
		Name:   emulatorContainerName(domain, cfg.Name),
		Volume: filepath.Join(config.GetRuntimeDir(), service) + ":/data",
		User:   containerUser,
	}
	expectedLabels := durableEmulatorLabels(domain)
	if containerUser != "" {
		expectedLabels["minisky.runtime-user"] = containerUser
	}
	inspected, found, err := sm.inspectDurableEmulator(ctx, container.Name)
	if err != nil {
		return fmt.Errorf("inspect %s emulator for cleanup: %w", domain, err)
	}
	if !found {
		return nil
	}
	if err := validateDurableEmulator(inspected, container, expectedLabels); err != nil {
		return fmt.Errorf("refusing to remove %s emulator: %w", domain, err)
	}
	if inspected.ID == "" {
		return fmt.Errorf("refusing to remove %s emulator without immutable container ID", domain)
	}
	if inspected.Status == "running" {
		stopRequest, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://localhost/containers/"+url.PathEscape(inspected.ID)+"/stop?t=10", nil)
		if err != nil {
			return err
		}
		stopResponse, err := sm.doDocker(stopRequest)
		if err != nil {
			return err
		}
		stopStatus := stopResponse.StatusCode
		closeErr := stopResponse.Body.Close()
		if closeErr != nil {
			return closeErr
		}
		if stopStatus != http.StatusNoContent && stopStatus != http.StatusNotModified &&
			stopStatus != http.StatusNotFound {
			return fmt.Errorf("stop %s emulator returned HTTP %d", domain, stopStatus)
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/containers/"+url.PathEscape(inspected.ID)+"?force=true", nil)
	if err != nil {
		return err
	}
	response, err := sm.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("remove %s emulator returned HTTP %d", domain, response.StatusCode)
	}
	return nil
}

// DockerOwnershipLabels returns the canonical labels shared by every Docker
// resource owned by the active MiniSky profile.
func DockerOwnershipLabels() map[string]string {
	return map[string]string{
		"managed-by":      "minisky",
		"minisky.profile": config.GetProfile(),
	}
}

func ownedDockerLabels() map[string]string {
	return DockerOwnershipLabels()
}

func isOwnedDockerResource(labels map[string]string) bool {
	return labels["managed-by"] == "minisky" && labels["minisky.profile"] == config.GetProfile()
}

func (sm *ServiceManager) startContainer(name string) error {
	return sm.startContainerContext(context.Background(), name)
}

func (sm *ServiceManager) startContainerContext(ctx context.Context, name string) error {
	url := fmt.Sprintf("http://localhost/containers/%s/start", name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := sm.doDocker(req)
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

func waitUntilRedisReady(ctx context.Context, addr string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	for {
		connection, err := dialer.DialContext(waitCtx, "tcp", addr)
		if err == nil {
			deadline := time.Now().Add(500 * time.Millisecond)
			if contextDeadline, ok := waitCtx.Deadline(); ok && contextDeadline.Before(deadline) {
				deadline = contextDeadline
			}
			_ = connection.SetDeadline(deadline)
			if _, err = io.WriteString(connection, "*1\r\n$4\r\nPING\r\n"); err == nil {
				var response string
				response, err = bufio.NewReader(connection).ReadString('\n')
				if err == nil && response == "+PONG\r\n" {
					_ = connection.Close()
					return nil
				}
			}
			_ = connection.Close()
		}

		timer := time.NewTimer(300 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf("Redis at %q not protocol-ready after %s: %w", addr, timeout, waitCtx.Err())
		case <-timer.C:
		}
	}
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
		prefix, err := NormalizeVPCIPv4Prefix(cidr)
		if err != nil {
			return fmt.Errorf("invalid IPv4 subnetwork CIDR %q", cidr)
		}
		cidr = prefix.String()
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

func (sm *ServiceManager) GetComputeInstancePortMappings(identity ComputeInstanceIdentity) []PortMapping {
	containerName, err := identity.DockerName()
	if err != nil {
		return nil
	}
	expected, _ := identity.labels()
	status, labels, err := sm.inspectContainer(containerName)
	if err != nil || status != "running" || !containsLabels(labels, expected) {
		return nil
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return append([]PortMapping(nil), sm.portRegistry[containerName]...)
}

func (sm *ServiceManager) ApplyFirewallPortsToVPC(vpcName string, containerNames []string, osImages []string) error {
	if len(containerNames) != len(osImages) {
		return fmt.Errorf("Compute firewall target/image count mismatch")
	}
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
		if err := sm.DeleteComputeVM(cName); err != nil {
			return fmt.Errorf("remove Compute VM %q for firewall update: %w", cName, err)
		}
		if err := sm.ProvisionComputeVM(context.Background(), cName, osImage, vpcName, allowedPorts, nil, []string{"tail", "-f", "/dev/null"}); err != nil {
			return fmt.Errorf("recreate Compute VM %q for firewall update: %w", cName, err)
		}
	}
	return nil
}

func (sm *ServiceManager) ApplyFirewallPortsToComputeInstances(
	firewallRuleKey string,
	dockerVPCName string,
	identities []ComputeInstanceIdentity,
	osImages []string,
) error {
	if len(identities) != len(osImages) {
		return fmt.Errorf("Compute firewall target/image count mismatch")
	}
	allowedPorts := []string{}
	sm.mu.RLock()
	rules := sm.fwRules[firewallRuleKey]
	sm.mu.RUnlock()
	for _, rule := range rules {
		if rule.Action == "allow" && rule.Direction == "INGRESS" {
			allowedPorts = append(allowedPorts, rule.Ports...)
		}
	}
	for index, identity := range identities {
		timeout := sm.dockerTimeout
		if timeout <= 0 {
			timeout = dockerRequestTimeout
		}
		deleteCtx, cancel := context.WithTimeout(context.Background(), timeout)
		err := sm.DeleteComputeInstance(deleteCtx, identity)
		cancel()
		if err != nil {
			return err
		}
		if err := sm.ProvisionComputeInstance(
			context.Background(),
			identity,
			osImages[index],
			dockerVPCName,
			allowedPorts,
			nil,
			[]string{"tail", "-f", "/dev/null"},
		); err != nil {
			return err
		}
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

func (sm *ServiceManager) GetComputeInstanceIP(identity ComputeInstanceIdentity) string {
	name, err := identity.DockerName()
	if err != nil || sm.dockerClient == nil {
		return ""
	}
	resp, err := sm.dockerClient.Get("http://localhost/containers/" + url.PathEscape(name) + "/json")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var info struct {
		Config struct {
			Labels map[string]string
		}
		NetworkSettings struct {
			Networks map[string]struct {
				IPAddress string
			}
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return ""
	}
	expected, _ := identity.labels()
	if !containsLabels(info.Config.Labels, expected) {
		return ""
	}
	if network, ok := info.NetworkSettings.Networks[networkName]; ok && network.IPAddress != "" {
		return network.IPAddress
	}
	for _, network := range info.NetworkSettings.Networks {
		if network.IPAddress != "" {
			return network.IPAddress
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
		Names  []string          `json:"Names"`
		Status string            `json:"Status"`
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil
	}
	out := make([]ContainerSummary, 0, len(raw))
	for _, c := range raw {
		if !isOwnedDockerResource(c.Labels) {
			continue
		}
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
	if err := sm.requireOwnedContainer(name); err != nil {
		return nil, err
	}
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
