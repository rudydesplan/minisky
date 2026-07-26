package compute

import (
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/orchestrator"
)

var (
	errUnsupportedAutoNetwork  = errors.New("auto-mode VPC attachment is not implemented")
	errUnsupportedMultipleNICs = errors.New("multiple network interfaces are not implemented")
	errUnsupportedNetworkNIC   = errors.New("requested network-interface semantics are not implemented")
)

func computeInstanceContainerName(project, zone string, instance *Instance) (string, error) {
	if instance != nil && instance.Labels != nil && instance.Labels["managed-by"] == "gke" {
		return instance.Name, nil
	}
	name := ""
	if instance != nil {
		name = instance.Name
	}
	return (orchestratorComputeIdentity(project, zone, name)).DockerName()
}

func (api *API) computeInstancePortMappings(project, zone string, instance *Instance) []orchestrator.PortMapping {
	if api.svcMgr == nil || instance == nil {
		return nil
	}
	if instance.Labels != nil && instance.Labels["managed-by"] == "gke" {
		return api.svcMgr.GetVMPortMappings(instance.Name)
	}
	return api.svcMgr.GetComputeInstancePortMappings(orchestratorComputeIdentity(project, zone, instance.Name))
}

func (api *API) computeInstanceIP(project, zone string, instance *Instance) string {
	if instance == nil {
		return ""
	}
	if len(instance.NetworkInterfaces) == 1 &&
		instance.NetworkInterfaces[0].Network != networkSelfLink(project, "default") {
		return instance.NetworkInterfaces[0].NetworkIP
	}
	if api.svcMgr == nil {
		return ""
	}
	if instance.Labels != nil && instance.Labels["managed-by"] == "gke" {
		return api.svcMgr.GetContainerIP(instance.Name)
	}
	return api.svcMgr.GetComputeInstanceIP(orchestratorComputeIdentity(project, zone, instance.Name))
}

func orchestratorComputeIdentity(project, zone, name string) orchestrator.ComputeInstanceIdentity {
	return orchestrator.ComputeInstanceIdentity{Project: project, Zone: zone, Instance: name}
}

func (api *API) resolveInstanceNetworkInterfaces(
	project string,
	zone string,
	interfaces []NetworkInterface,
) ([]NetworkInterface, error) {
	if len(interfaces) > 1 {
		return nil, errUnsupportedMultipleNICs
	}
	if len(interfaces) == 0 {
		return []NetworkInterface{{
			Kind: "compute#networkInterface", Name: "nic0",
			Network: networkSelfLink(project, "default"), NetworkIP: "10.128.0.2",
		}}, nil
	}
	resolved := append([]NetworkInterface(nil), interfaces...)
	region, err := regionFromZone(zone)
	if err != nil {
		return nil, err
	}
	first := &resolved[0]
	if first.Name == "" {
		first.Name = "nic0"
	}
	if first.Kind == "" {
		first.Kind = "compute#networkInterface"
	}
	if len(first.AccessConfigs) > 0 {
		return nil, fmt.Errorf("%w: external IPv4/NAT access configs", errUnsupportedNetworkNIC)
	}
	if first.NetworkIP != "" {
		return nil, fmt.Errorf("%w: caller-selected static networkIP", errUnsupportedNetworkNIC)
	}
	if first.StackType != "" && first.StackType != "IPV4_ONLY" {
		return nil, fmt.Errorf("%w: IPv6 stack types", errUnsupportedNetworkNIC)
	}
	if len(first.IPv6AccessConfigs) > 0 {
		return nil, fmt.Errorf("%w: IPv6 access configs", errUnsupportedNetworkNIC)
	}
	first.StackType = "IPV4_ONLY"

	if first.Subnetwork != "" {
		subnetName, err := resolveInstanceSubnetworkReference(project, region, first.Subnetwork)
		if err != nil {
			return nil, err
		}
		api.mu.RLock()
		subnet := cloneSubnetwork(api.subnetworks[subnetworkKey(project, region, subnetName)])
		if subnet == nil {
			api.mu.RUnlock()
			return nil, fmt.Errorf("subnetwork %q was not found", subnetName)
		}
		parentName := extractNameFromURL(subnet.Network)
		parent := api.networks[project+":"+parentName]
		parentValid := parent != nil && !parent.AutoCreateSubnetworks &&
			subnet.Network == networkSelfLink(project, parentName)
		api.mu.RUnlock()
		if !parentValid {
			return nil, errors.New("subnetwork parent network is missing or unsupported")
		}
		if first.Network != "" {
			networkName, err := resolveSubnetworkNetwork(project, first.Network)
			if err != nil || networkSelfLink(project, networkName) != subnet.Network {
				return nil, errors.New("network and subnetwork must reference the same parent VPC")
			}
		}
		first.Network = subnet.Network
		first.Subnetwork = subnet.SelfLink
		return resolved, nil
	}

	networkName := "default"
	if first.Network != "" {
		var err error
		networkName, err = resolveSubnetworkNetwork(project, first.Network)
		if err != nil {
			return nil, err
		}
	}
	if networkName == "default" {
		first.Network = networkSelfLink(project, "default")
		first.Subnetwork = ""
		return resolved, nil
	}

	api.mu.RLock()
	network := api.networks[project+":"+networkName]
	if network == nil {
		api.mu.RUnlock()
		return nil, fmt.Errorf("network %q was not found", networkName)
	}
	if network.AutoCreateSubnetworks {
		api.mu.RUnlock()
		return nil, errUnsupportedAutoNetwork
	}
	parent := networkSelfLink(project, networkName)
	var match *Subnetwork
	for key, subnet := range api.subnetworks {
		if strings.HasPrefix(key, project+":"+region+":") && subnet != nil && subnet.Network == parent {
			if match != nil {
				api.mu.RUnlock()
				return nil, errors.New("custom network has ambiguous subnetworks")
			}
			match = cloneSubnetwork(subnet)
		}
	}
	api.mu.RUnlock()
	if match == nil {
		return nil, errors.New("custom network has no bounded subnetwork in the instance region")
	}
	first.Network = parent
	first.Subnetwork = match.SelfLink
	return resolved, nil
}

func (api *API) computeInstanceNetworkAttachment(
	project string,
	interfaces []NetworkInterface,
) (orchestrator.ComputeInstanceNetwork, bool, error) {
	if len(interfaces) != 1 || interfaces[0].Network == networkSelfLink(project, "default") {
		return orchestrator.ComputeInstanceNetwork{}, false, nil
	}
	subnetworkReference := interfaces[0].Subnetwork
	if subnetworkReference == "" {
		return orchestrator.ComputeInstanceNetwork{}, false, errors.New("custom VPC instance is missing its primary subnetwork")
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	for _, subnetwork := range api.subnetworks {
		if subnetwork == nil || subnetwork.SelfLink != subnetworkReference {
			continue
		}
		parentProject, network, err := subnetworkVPCIdentity(subnetwork)
		if err != nil || parentProject != project {
			return orchestrator.ComputeInstanceNetwork{}, false, errors.New("instance subnetwork has an invalid parent VPC")
		}
		return orchestrator.ComputeInstanceNetwork{
			VPC:  orchestrator.VPCNetworkIdentity{Project: project, Network: network},
			CIDR: subnetwork.IPCidrRange,
		}, true, nil
	}
	return orchestrator.ComputeInstanceNetwork{}, false, errors.New("instance subnetwork metadata was not found")
}

func resolveInstanceSubnetworkReference(project, region, value string) (string, error) {
	if gceResourceName.MatchString(value) {
		return value, nil
	}
	const marker = "https://www.googleapis.com/compute/v1/projects/"
	remaining := value
	if strings.HasPrefix(remaining, marker) {
		remaining = strings.TrimPrefix(remaining, marker)
	} else if strings.HasPrefix(remaining, "projects/") {
		remaining = strings.TrimPrefix(remaining, "projects/")
	} else {
		return "", errors.New("subnetwork must be a name or canonical same-project regional reference")
	}
	parts := strings.Split(remaining, "/")
	if len(parts) != 5 || parts[0] != project || parts[1] != "regions" ||
		parts[2] != region || parts[3] != "subnetworks" || !gceResourceName.MatchString(parts[4]) {
		return "", errors.New("subnetwork reference must match the instance project and zone region")
	}
	return parts[4], nil
}

func regionFromZone(zone string) (string, error) {
	index := strings.LastIndex(zone, "-")
	if index <= 0 || index == len(zone)-1 {
		return "", errors.New("invalid Compute zone")
	}
	return zone[:index], nil
}

func resolvedInstanceVPCDockerNetwork(
	project string,
	interfaces []NetworkInterface,
) (string, string, error) {
	if len(interfaces) == 0 {
		return networkSelfLink(project, "default"), "default", nil
	}
	network, err := resolveSubnetworkNetwork(project, interfaces[0].Network)
	if err != nil {
		return "", "", err
	}
	if network == "default" {
		return networkSelfLink(project, "default"), "default", nil
	}
	dockerName, err := (orchestrator.VPCNetworkIdentity{Project: project, Network: network}).DockerName()
	if err != nil {
		return "", "", err
	}
	return networkSelfLink(project, network), dockerName, nil
}
