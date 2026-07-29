package compute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const computeStateEntry = "compute/metadata"

type computeMetadataStore interface {
	Load(string, any) error
	Save(string, any) error
}

type vpcIPAMBackend interface {
	EnsureVPCNetworkIPAM(
		context.Context,
		orchestrator.VPCNetworkIdentity,
		string,
	) (orchestrator.VPCNetworkIPAMState, error)
	DeleteVPCNetworkIPAM(context.Context, orchestrator.VPCNetworkIdentity, string) error
}

type computeMetadata struct {
	Instances        map[string]*Instance              `json:"instances"`
	Networks         map[string]*Network               `json:"networks"`
	Subnetworks      map[string]*Subnetwork            `json:"subnetworks"`
	NextSubnetID     uint64                            `json:"nextSubnetworkId"`
	SecurityPolicies map[string]*SecurityPolicy        `json:"securityPolicies"`
	Firewalls        map[string]*FirewallRule          `json:"firewalls"`
	InstanceGroups   map[string]*InstanceGroup         `json:"instanceGroups"`
	LoadBalancers    map[string]map[string]interface{} `json:"loadBalancers"`
}

// NewAPIWithStore constructs a Compute shim backed by the supplied profile store.
// Persisted Docker-backed instances are restored as metadata only; loading state
// never creates or adopts containers.
func NewAPIWithStore(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	store *state.Store,
) (*API, error) {
	return newAPIWithMetadataStore(opMgr, svcMgr, store)
}

func newAPIWithMetadataStore(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	store computeMetadataStore,
) (*API, error) {
	api := newAPI(opMgr, svcMgr, store)
	if store == nil {
		return api, nil
	}

	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Compute metadata: %w", err)
	}
	if persisted.Instances != nil {
		api.instances = persisted.Instances
	}
	if persisted.Networks != nil {
		api.networks = persisted.Networks
	}
	for key, network := range api.networks {
		if network == nil {
			delete(api.networks, key)
			continue
		}
		if network.NetworkFirewallPolicyEnforcementOrder == "" {
			network.NetworkFirewallPolicyEnforcementOrder = "AFTER_CLASSIC_FIREWALL"
		}
	}
	if persisted.Subnetworks != nil {
		api.subnetworks = persisted.Subnetworks
	}
	api.nextSubnetworkID = persisted.NextSubnetID
	if persisted.Firewalls != nil {
		api.firewalls = persisted.Firewalls
	}
	if persisted.SecurityPolicies != nil {
		api.securityPolicies = persisted.SecurityPolicies
	}
	if persisted.InstanceGroups != nil {
		api.instanceGroups = persisted.InstanceGroups
	}
	if persisted.LoadBalancers != nil {
		api.loadBalancers = persisted.LoadBalancers
	}
	if err := validatePersistedSubnetworkGraph(api.networks, api.subnetworks, api.nextSubnetworkID); err != nil {
		api.setInitializationError(fmt.Errorf("validate persisted Compute subnetworks: %w", err))
		return api, nil
	}
	if err := validatePersistedSecurityPolicyGraph(api.securityPolicies, api.loadBalancers); err != nil {
		api.setInitializationError(fmt.Errorf("validate persisted Compute security policies: %w", err))
		return api, nil
	}
	if api.nextSubnetworkID == 0 {
		api.nextSubnetworkID = 1
	}
	for key, instance := range api.instances {
		if instance == nil {
			delete(api.instances, key)
			continue
		}
		parts := strings.SplitN(key, ":", 3)
		if len(parts) == 3 {
			instance.project = parts[0]
			instance.zone = parts[1]
		}
		if instance.Status == "DELETING" {
			instance.HostPorts = nil
			continue
		}
		instance.Status = metadataOnlyStatus
		instance.HostPorts = nil
		instance.Description = rehydratedInstanceDescription
	}
	return api, nil
}

// ReconcileVPCIPAM restores the exact Docker bridges promised by persisted
// subnetworks. Loading through NewAPIWithStore remains side-effect free.
func (api *API) ReconcileVPCIPAM(ctx context.Context) error {
	api.setInitializationError(nil)
	api.mu.RLock()
	validationErr := validatePersistedSubnetworkGraph(api.networks, api.subnetworks, api.nextSubnetworkID)
	api.mu.RUnlock()
	if validationErr != nil {
		err := fmt.Errorf("validate persisted Compute subnetworks: %w", validationErr)
		api.setInitializationError(err)
		return err
	}
	if api.vpcIPAM == nil {
		api.mu.RLock()
		hasSubnetworks := len(api.subnetworks) > 0
		api.mu.RUnlock()
		if hasSubnetworks {
			err := errors.New("VPC IPAM backend is unavailable")
			api.setInitializationError(err)
			return err
		}
		return nil
	}

	api.mu.RLock()
	keys := make([]string, 0, len(api.subnetworks))
	for key := range api.subnetworks {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	subnetworks := make([]*Subnetwork, 0, len(keys))
	for _, key := range keys {
		if subnetwork := cloneSubnetwork(api.subnetworks[key]); subnetwork != nil {
			subnetworks = append(subnetworks, subnetwork)
		}
	}
	api.mu.RUnlock()

	for _, subnetwork := range subnetworks {
		project, network, err := subnetworkVPCIdentity(subnetwork)
		if err != nil {
			reconcileErr := fmt.Errorf("reconcile subnetwork %q: %w", subnetwork.Name, err)
			api.setInitializationError(reconcileErr)
			return reconcileErr
		}
		identity := orchestrator.VPCNetworkIdentity{Project: project, Network: network}
		if _, err := api.vpcIPAM.EnsureVPCNetworkIPAM(ctx, identity, subnetwork.IPCidrRange); err != nil {
			reconcileErr := fmt.Errorf("reconcile %s: %w", identity.CanonicalResource(), err)
			api.setInitializationError(reconcileErr)
			return reconcileErr
		}
	}
	api.setInitializationError(nil)
	return nil
}

// ReconcileComputeInstances rehydrates only existing exact owned custom-VPC
// containers. It never creates, connects, or adopts a Docker endpoint.
func (api *API) ReconcileComputeInstances(ctx context.Context) error {
	api.mu.RLock()
	keys := make([]string, 0, len(api.instances))
	for key := range api.instances {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	instances := make(map[string]*Instance, len(keys))
	for _, key := range keys {
		instances[key] = api.instances[key].DeepCopy()
	}
	api.mu.RUnlock()

	changed := false
	for _, key := range keys {
		instance := instances[key]
		if instance == nil || (instance.Labels != nil && instance.Labels["managed-by"] == "gke") {
			continue
		}
		attachment, custom, err := api.computeInstanceNetworkAttachment(instance.project, instance.NetworkInterfaces)
		if err != nil {
			return fmt.Errorf("reconcile Compute instance %q: %w", instance.Name, err)
		}
		if !custom {
			if instance.Status == "DELETING" {
				return fmt.Errorf(
					"reconcile Compute instance %q: deletion marker cannot be confirmed without a custom VPC attachment",
					instance.Name,
				)
			}
			continue
		}
		if api.computeNetwork == nil {
			return errors.New("Compute custom-network backend is unavailable")
		}
		runtime, found, err := api.computeNetwork.ReconcileComputeInstanceOnVPC(
			ctx,
			orchestratorComputeIdentity(instance.project, instance.zone, instance.Name),
			attachment,
		)
		if err != nil {
			return fmt.Errorf("reconcile Compute instance %q: %w", instance.Name, err)
		}
		if !found {
			if instance.Status == "DELETING" {
				api.mu.Lock()
				if current := api.instances[key]; current != nil && current.Status == "DELETING" {
					delete(api.instances, key)
					changed = true
				}
				api.mu.Unlock()
			}
			continue
		}
		if instance.Status == "DELETING" {
			return fmt.Errorf(
				"reconcile Compute instance %q: deletion marker still has an exact owned container",
				instance.Name,
			)
		}
		if runtime.Status != "running" || runtime.IPAddress == "" {
			return fmt.Errorf("reconcile Compute instance %q: exact owned container is not running", instance.Name)
		}
		api.mu.Lock()
		current := api.instances[key]
		if current != nil && len(current.NetworkInterfaces) == 1 {
			current.Status = "RUNNING"
			current.NetworkInterfaces[0].NetworkIP = runtime.IPAddress
			if current.Description == rehydratedInstanceDescription {
				current.Description = ""
			}
			changed = true
		}
		api.mu.Unlock()
	}
	if changed {
		if err := api.persistMetadata(); err != nil {
			return fmt.Errorf("persist reconciled Compute instances: %w", err)
		}
	}
	return nil
}

func validatePersistedSubnetworkGraph(
	networks map[string]*Network,
	subnetworks map[string]*Subnetwork,
	nextID uint64,
) error {
	for key, network := range networks {
		if network != nil && network.NetworkFirewallPolicyEnforcementOrder != "" &&
			network.NetworkFirewallPolicyEnforcementOrder != "AFTER_CLASSIC_FIREWALL" {
			return fmt.Errorf("network %q has unsupported firewall policy enforcement order", key)
		}
	}
	type seenSubnet struct {
		project string
		prefix  netip.Prefix
	}
	parents := map[string]bool{}
	names := map[string]bool{}
	ids := map[uint64]bool{}
	seen := make([]seenSubnet, 0, len(subnetworks))
	maxID := uint64(0)
	for key, subnet := range subnetworks {
		if subnet == nil {
			return fmt.Errorf("subnetwork %q is nil", key)
		}
		parts := strings.Split(key, ":")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] != subnet.Name ||
			!gceResourceName.MatchString(parts[1]) || !gceResourceName.MatchString(subnet.Name) {
			return fmt.Errorf("subnetwork key %q does not match its identity", key)
		}
		project, region := parts[0], parts[1]
		if subnet.Kind != "compute#subnetwork" ||
			subnet.Region != regionSelfLink(project, region) ||
			subnet.SelfLink != subnetworkSelfLink(project, region, subnet.Name) ||
			subnet.Purpose != "PRIVATE" || subnet.StackType != "IPV4_ONLY" ||
			subnet.State != "READY" || subnet.PrivateIPGoogleAccess {
			return fmt.Errorf("subnetwork %q has invalid stable fields", key)
		}
		if _, err := time.Parse(time.RFC3339, subnet.CreationTimestamp); err != nil {
			return fmt.Errorf("subnetwork %q has invalid creation timestamp", key)
		}
		prefix, err := validateSubnetworkRequest(subnetworkInsertRequest{
			Name: subnet.Name, IPCidrRange: subnet.IPCidrRange, Network: subnet.Network,
			Purpose: subnet.Purpose, StackType: subnet.StackType,
		})
		if err != nil || subnet.GatewayAddress != prefix.Addr().Next().String() ||
			subnet.Fingerprint != subnetworkFingerprint(subnet) {
			return fmt.Errorf("subnetwork %q has invalid CIDR, gateway, or fingerprint", key)
		}
		id, err := strconv.ParseUint(subnet.ID, 10, 64)
		if err != nil || id == 0 || ids[id] {
			return fmt.Errorf("subnetwork %q has invalid or duplicate ID", key)
		}
		ids[id] = true
		if id > maxID {
			maxID = id
		}
		parentProject, parentName, err := subnetworkVPCIdentity(subnet)
		if err != nil || parentProject != project {
			return fmt.Errorf("subnetwork %q has invalid parent identity", key)
		}
		parentKey := project + ":" + parentName
		parent := networks[parentKey]
		if parent == nil || parent.Kind != "compute#network" || parent.ID == "" ||
			parent.Name != parentName || parent.SelfLink != networkSelfLink(project, parentName) ||
			parent.AutoCreateSubnetworks {
			return fmt.Errorf("subnetwork %q parent is missing or not custom mode", key)
		}
		if parents[parentKey] {
			return fmt.Errorf("multiple subnetworks use parent %q", parentKey)
		}
		parents[parentKey] = true
		nameKey := project + ":" + subnet.Name
		if names[nameKey] {
			return fmt.Errorf("duplicate subnetwork name %q in project", subnet.Name)
		}
		names[nameKey] = true
		for _, previous := range seen {
			if previous.project == project && previous.prefix.Overlaps(prefix) {
				return fmt.Errorf("subnetwork %q overlaps another project CIDR", key)
			}
		}
		seen = append(seen, seenSubnet{project: project, prefix: prefix})
	}
	minimumNext := maxID + 1
	if minimumNext == 1 && nextID == 0 {
		return nil
	}
	if nextID < minimumNext {
		return fmt.Errorf("next subnetwork ID is %d, want at least %d", nextID, minimumNext)
	}
	return nil
}

func validatePersistedSecurityPolicyGraph(
	policies map[string]*SecurityPolicy,
	loadBalancers map[string]map[string]interface{},
) error {
	for key, policy := range policies {
		if policy == nil {
			return fmt.Errorf("security policy %q is nil", key)
		}
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" ||
			policy.Kind != "compute#securityPolicy" || policy.ID == "" ||
			policy.Name != parts[1] ||
			policy.SelfLink != securityPolicySelfLink(parts[0], parts[1]) {
			return fmt.Errorf("security policy %q has invalid identity", key)
		}
		if _, err := time.Parse(time.RFC3339, policy.CreationTimestamp); err != nil {
			return fmt.Errorf("security policy %q has invalid creation timestamp", key)
		}
		if err := validateSecurityPolicyRules(policy.Rules); err != nil {
			return fmt.Errorf("security policy %q: %w", key, err)
		}
	}
	for key, resource := range loadBalancers {
		if resource == nil || !strings.Contains(key, ":backendServices:") {
			continue
		}
		reference, _ := resource["securityPolicy"].(string)
		if reference == "" {
			continue
		}
		project := strings.SplitN(key, ":", 2)[0]
		name, err := securityPolicyReferenceName(reference, project)
		if err != nil {
			return fmt.Errorf("backend service %q has invalid security policy: %w", key, err)
		}
		if policies[project+":"+name] == nil {
			return fmt.Errorf("backend service %q references missing security policy %q", key, name)
		}
	}
	return nil
}

func (api *API) setInitializationError(err error) {
	api.initMu.Lock()
	api.initializationErr = err
	api.initMu.Unlock()
}

func (api *API) initializationError() error {
	api.initMu.RLock()
	err := api.initializationErr
	api.initMu.RUnlock()
	if api.opMgr != nil {
		err = errors.Join(err, api.opMgr.PersistenceError())
	}
	return err
}

func (api *API) PersistenceError() error {
	return api.initializationError()
}

func subnetworkVPCIdentity(subnetwork *Subnetwork) (string, string, error) {
	const marker = "https://www.googleapis.com/compute/v1/projects/"
	if subnetwork == nil || !strings.HasPrefix(subnetwork.Network, marker) {
		return "", "", errors.New("subnetwork has invalid parent network")
	}
	parts := strings.Split(strings.TrimPrefix(subnetwork.Network, marker), "/")
	if len(parts) != 4 || parts[1] != "global" || parts[2] != "networks" {
		return "", "", errors.New("subnetwork has invalid parent network")
	}
	identity := orchestrator.VPCNetworkIdentity{Project: parts[0], Network: parts[3]}
	if err := identity.Validate(); err != nil {
		return "", "", err
	}
	return identity.Project, identity.Network, nil
}

func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	payload, err := api.marshalMetadataLocked()
	api.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("snapshot Compute metadata: %w", err)
	}
	return api.stateStore.Save(computeStateEntry, json.RawMessage(payload))
}

func (api *API) marshalMetadataLocked() ([]byte, error) {
	return json.Marshal(computeMetadata{
		Instances:        api.instances,
		Networks:         api.networks,
		Subnetworks:      api.subnetworks,
		NextSubnetID:     api.nextSubnetworkID,
		SecurityPolicies: api.securityPolicies,
		Firewalls:        api.firewalls,
		InstanceGroups:   api.instanceGroups,
		LoadBalancers:    api.loadBalancers,
	})
}
