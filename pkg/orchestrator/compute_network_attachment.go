package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

const computeCreateAmbiguityTimeout = 30 * time.Second

// ComputeInstanceNetwork identifies the single custom-mode VPC attachment
// supported by the bounded Compute data plane.
type ComputeInstanceNetwork struct {
	VPC  VPCNetworkIdentity
	CIDR string
}

// ComputeInstanceRuntime is Docker-observed attachment state. Docker IDs remain
// internal and are used only to prove identity and avoid name-based adoption.
type ComputeInstanceRuntime struct {
	ContainerID string
	Status      string
	NetworkName string
	NetworkID   string
	IPAddress   string
}

type dockerComputeInstanceInspect struct {
	ID    string `json:"Id"`
	State struct {
		Status string `json:"Status"`
	} `json:"State"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	NetworkSettings struct {
		Networks map[string]struct {
			NetworkID string `json:"NetworkID"`
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

// ReconcileComputeInstanceOnVPC observes an existing exact owned container and
// bridge without creating, connecting, or adopting either resource.
func (sm *ServiceManager) ReconcileComputeInstanceOnVPC(
	ctx context.Context,
	identity ComputeInstanceIdentity,
	attachment ComputeInstanceNetwork,
) (ComputeInstanceRuntime, bool, error) {
	bridge, prefix, err := sm.exactComputeVPCBridge(ctx, attachment)
	if err != nil {
		return ComputeInstanceRuntime{}, false, err
	}
	containerName, err := identity.DockerName()
	if err != nil {
		return ComputeInstanceRuntime{}, false, err
	}
	expectedLabels, _ := identity.labels()
	inspected, found, err := sm.inspectComputeInstance(ctx, containerName)
	if err != nil || !found {
		return ComputeInstanceRuntime{}, found, err
	}
	if !containsLabels(inspected.Config.Labels, expectedLabels) {
		return ComputeInstanceRuntime{}, true, fmt.Errorf(
			"Compute container %q is not exactly owned by %s",
			containerName,
			identity.CanonicalResource(),
		)
	}
	if inspected.ID == "" {
		return ComputeInstanceRuntime{}, true, fmt.Errorf("owned Compute container %q has no immutable Docker ID", containerName)
	}
	if len(inspected.NetworkSettings.Networks) != 1 {
		return ComputeInstanceRuntime{}, true, fmt.Errorf(
			"owned Compute container %q must have exactly one network attachment",
			containerName,
		)
	}
	endpoint, attached := inspected.NetworkSettings.Networks[bridge.Name]
	if !attached || endpoint.NetworkID != bridge.ID {
		return ComputeInstanceRuntime{}, true, fmt.Errorf(
			"owned Compute container %q is not attached to the exact owned VPC bridge",
			containerName,
		)
	}
	address, parseErr := netip.ParseAddr(endpoint.IPAddress)
	if parseErr != nil || !address.Is4() || !prefix.Contains(address) {
		return ComputeInstanceRuntime{}, true, fmt.Errorf(
			"owned Compute container %q has no truthful primary IPv4 address in %s",
			containerName,
			prefix,
		)
	}
	return ComputeInstanceRuntime{
		ContainerID: inspected.ID,
		Status:      inspected.State.Status,
		NetworkName: bridge.Name,
		NetworkID:   bridge.ID,
		IPAddress:   address.String(),
	}, true, nil
}

// ProvisionComputeInstanceOnVPC creates the VM directly on the immutable ID of
// the exact owned bridge. It never connects an existing ambiguous container.
func (sm *ServiceManager) ProvisionComputeInstanceOnVPC(
	ctx context.Context,
	identity ComputeInstanceIdentity,
	osImage string,
	attachment ComputeInstanceNetwork,
	ports []string,
	env []string,
	cmd []string,
) (ComputeInstanceRuntime, error) {
	if current, found, err := sm.ReconcileComputeInstanceOnVPC(ctx, identity, attachment); err != nil {
		return ComputeInstanceRuntime{}, err
	} else if found {
		if current.Status != "running" {
			containerName, _ := identity.DockerName()
			if err := sm.startContainer(containerName); err != nil {
				return ComputeInstanceRuntime{}, err
			}
		}
		return sm.requireRunningComputeInstanceOnVPC(ctx, identity, attachment)
	}

	bridge, _, err := sm.exactComputeVPCBridge(ctx, attachment)
	if err != nil {
		return ComputeInstanceRuntime{}, err
	}
	containerName, _ := identity.DockerName()
	resourceLabels, _ := identity.labels()
	exists, inspectErr := sm.ImageExistsPublic(osImage)
	if inspectErr != nil {
		return ComputeInstanceRuntime{}, fmt.Errorf("inspect data plane image %q: %w", osImage, inspectErr)
	}
	if !exists {
		if err := sm.pullImageInternal(ctx, osImage); err != nil {
			return ComputeInstanceRuntime{}, fmt.Errorf("pull data plane image %q: %w", osImage, err)
		}
	}

	exposedPorts, portBindings := computePortBindings(ports)
	payload := map[string]any{
		"Image":        osImage,
		"Env":          append(sm.standardEnv(), env...),
		"ExposedPorts": exposedPorts,
		"Labels":       resourceLabels,
		"HostConfig": map[string]any{
			"NetworkMode":  bridge.ID,
			"PortBindings": portBindings,
		},
	}
	if len(cmd) > 0 {
		payload["Cmd"] = cmd
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ComputeInstanceRuntime{}, err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://localhost/containers/create?"+url.Values{"name": {containerName}}.Encode(),
		bytes.NewReader(encoded),
	)
	if err != nil {
		return ComputeInstanceRuntime{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := sm.dockerClient.Do(request)
	if err != nil {
		createErr := fmt.Errorf("create Compute container: %w", err)
		reconcileCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			computeCreateAmbiguityTimeout,
		)
		defer cancel()
		if cleanupErr := sm.cleanupAmbiguousComputeCreate(
			reconcileCtx,
			containerName,
			resourceLabels,
		); cleanupErr != nil {
			return ComputeInstanceRuntime{}, fmt.Errorf("%w; reconcile ambiguous create: %v", createErr, cleanupErr)
		}
		return ComputeInstanceRuntime{}, createErr
	}
	defer drainAndClose(response.Body)
	if response.StatusCode != http.StatusCreated {
		return ComputeInstanceRuntime{}, dockerStatusError("create Compute container", response)
	}

	cleanup := func(cause error) error {
		cleanupCtx := context.WithoutCancel(ctx)
		if cleanupErr := sm.deleteComputeVMContext(cleanupCtx, containerName, resourceLabels); cleanupErr != nil {
			return fmt.Errorf("%w; cleanup exact owned Compute container failed: %v", cause, cleanupErr)
		}
		return cause
	}
	if err := sm.startContainer(containerName); err != nil {
		return ComputeInstanceRuntime{}, cleanup(fmt.Errorf("start Compute container: %w", err))
	}
	if err := sm.updatePortRegistry(containerName); err != nil {
		return ComputeInstanceRuntime{}, cleanup(fmt.Errorf("inspect Compute port mappings: %w", err))
	}
	runtime, err := sm.requireRunningComputeInstanceOnVPC(ctx, identity, attachment)
	if err != nil {
		return ComputeInstanceRuntime{}, cleanup(err)
	}
	return runtime, nil
}

func (sm *ServiceManager) cleanupAmbiguousComputeCreate(
	ctx context.Context,
	containerName string,
	expectedLabels map[string]string,
) error {
	inspected, found, err := sm.inspectComputeInstance(ctx, containerName)
	if err != nil || !found {
		return err
	}
	if inspected.ID == "" || !containsLabels(inspected.Config.Labels, expectedLabels) {
		return fmt.Errorf(
			"refusing to remove container %q without exact immutable Compute ownership",
			containerName,
		)
	}
	return sm.deleteComputeVMContext(ctx, containerName, expectedLabels)
}

func (sm *ServiceManager) requireRunningComputeInstanceOnVPC(
	ctx context.Context,
	identity ComputeInstanceIdentity,
	attachment ComputeInstanceNetwork,
) (ComputeInstanceRuntime, error) {
	runtime, found, err := sm.ReconcileComputeInstanceOnVPC(ctx, identity, attachment)
	if err != nil {
		return ComputeInstanceRuntime{}, err
	}
	if !found {
		return ComputeInstanceRuntime{}, errors.New("owned Compute container disappeared during provisioning")
	}
	if runtime.Status != "running" {
		return ComputeInstanceRuntime{}, fmt.Errorf("owned Compute container is %s, want running", runtime.Status)
	}
	return runtime, nil
}

func (sm *ServiceManager) exactComputeVPCBridge(
	ctx context.Context,
	attachment ComputeInstanceNetwork,
) (dockerNetworkInspect, netip.Prefix, error) {
	prefix, err := NormalizeVPCIPv4Prefix(attachment.CIDR)
	if err != nil {
		return dockerNetworkInspect{}, netip.Prefix{}, err
	}
	name, err := attachment.VPC.DockerName()
	if err != nil {
		return dockerNetworkInspect{}, netip.Prefix{}, err
	}
	expectedLabels, _ := attachment.VPC.labels()
	bridge, found, err := sm.inspectVPCNetwork(ctx, name)
	if err != nil {
		return dockerNetworkInspect{}, netip.Prefix{}, err
	}
	if !found {
		return dockerNetworkInspect{}, netip.Prefix{}, fmt.Errorf("exact owned VPC bridge %q was not found", name)
	}
	cidr, cidrErr := exactNetworkCIDR(bridge)
	if !exactLabels(bridge.Labels, expectedLabels) || bridge.Driver != "bridge" ||
		bridge.ID == "" || cidrErr != nil || cidr != prefix.String() {
		return dockerNetworkInspect{}, netip.Prefix{}, fmt.Errorf(
			"VPC bridge %q is not the exact owned bridge for %s",
			name,
			attachment.VPC.CanonicalResource(),
		)
	}
	return bridge, prefix, nil
}

func (sm *ServiceManager) inspectComputeInstance(
	ctx context.Context,
	name string,
) (dockerComputeInstanceInspect, bool, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(name)+"/json",
		nil,
	)
	if err != nil {
		return dockerComputeInstanceInspect{}, false, err
	}
	response, err := sm.dockerClient.Do(request)
	if err != nil {
		return dockerComputeInstanceInspect{}, false, fmt.Errorf("inspect Compute container: %w", err)
	}
	defer drainAndClose(response.Body)
	if response.StatusCode == http.StatusNotFound {
		return dockerComputeInstanceInspect{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return dockerComputeInstanceInspect{}, false, dockerStatusError("inspect Compute container", response)
	}
	var inspected dockerComputeInstanceInspect
	if err := json.NewDecoder(io.LimitReader(response.Body, maxDockerErrorBody)).Decode(&inspected); err != nil {
		return dockerComputeInstanceInspect{}, false, fmt.Errorf("decode Compute container inspection: %w", err)
	}
	return inspected, true, nil
}

func computePortBindings(ports []string) (map[string]any, map[string]any) {
	exposed := make(map[string]any, len(ports))
	bindings := make(map[string]any, len(ports))
	for _, port := range ports {
		if !strings.Contains(port, "/") {
			port += "/tcp"
		}
		exposed[port] = struct{}{}
		bindings[port] = []map[string]string{{"HostIp": "127.0.0.1", "HostPort": ""}}
	}
	return exposed, bindings
}
