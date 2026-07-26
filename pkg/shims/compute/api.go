package compute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/observability"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("compute.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr, ctx.SvcMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

// Instance represents a GCE VM with its full lifecycle state.
type Instance struct {
	Kind              string                     `json:"kind"`
	ID                string                     `json:"id"`
	Name              string                     `json:"name"`
	Zone              string                     `json:"zone"`
	MachineType       string                     `json:"machineType"`
	Status            string                     `json:"status"`
	SelfLink          string                     `json:"selfLink"`
	Description       string                     `json:"description"`
	Labels            map[string]string          `json:"labels,omitempty"`
	Metadata          *InstanceMetadata          `json:"metadata,omitempty"`
	NetworkInterfaces []NetworkInterface         `json:"networkInterfaces"`
	Disks             []AttachedDisk             `json:"disks"`
	CreationTimestamp string                     `json:"creationTimestamp"`
	HostPorts         []orchestrator.PortMapping `json:"hostPorts,omitempty"`
	// internal tracking only
	project          string
	zone             string
	Fingerprint      string      `json:"fingerprint"`
	LabelFingerprint string      `json:"labelFingerprint"`
	Scheduling       *Scheduling `json:"scheduling,omitempty"`
	CanIpForward     bool        `json:"canIpForward"`
}

func (i *Instance) DeepCopy() *Instance {
	newInst := *i
	if i.Labels != nil {
		newInst.Labels = make(map[string]string)
		for k, v := range i.Labels {
			newInst.Labels[k] = v
		}
	}
	if i.Metadata != nil {
		newInst.Metadata = &InstanceMetadata{
			Kind: i.Metadata.Kind,
		}
		newInst.Metadata.Items = append([]MetadataItem{}, i.Metadata.Items...)
	}
	if i.NetworkInterfaces != nil {
		newInst.NetworkInterfaces = make([]NetworkInterface, len(i.NetworkInterfaces))
		copy(newInst.NetworkInterfaces, i.NetworkInterfaces)
		for j := range newInst.NetworkInterfaces {
			if newInst.NetworkInterfaces[j].AccessConfigs != nil {
				newInst.NetworkInterfaces[j].AccessConfigs = append([]AccessConfig{}, newInst.NetworkInterfaces[j].AccessConfigs...)
			}
		}
	}
	if i.Disks != nil {
		newInst.Disks = make([]AttachedDisk, len(i.Disks))
		copy(newInst.Disks, i.Disks)
		for j := range newInst.Disks {
			if newInst.Disks[j].InitializeParams != nil {
				params := *newInst.Disks[j].InitializeParams
				newInst.Disks[j].InitializeParams = &params
			}
		}
	}
	if i.HostPorts != nil {
		newInst.HostPorts = append([]orchestrator.PortMapping{}, i.HostPorts...)
	}
	if i.Scheduling != nil {
		s := *i.Scheduling
		newInst.Scheduling = &s
	}
	return &newInst
}

type Scheduling struct {
	OnHostMaintenance string `json:"onHostMaintenance"`
	AutomaticRestart  bool   `json:"automaticRestart"`
	Preemptible       bool   `json:"preemptible"`
}

type InstanceMetadata struct {
	Kind  string         `json:"kind"`
	Items []MetadataItem `json:"items,omitempty"`
}

type MetadataItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type NetworkInterface struct {
	Kind          string         `json:"kind"`
	Name          string         `json:"name"`
	Network       string         `json:"network"`
	NetworkIP     string         `json:"networkIP"`
	Subnetwork    string         `json:"subnetwork,omitempty"`
	AccessConfigs []AccessConfig `json:"accessConfigs,omitempty"`
}

type AccessConfig struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Type  string `json:"type"` // ONE_TO_ONE_NAT
	NatIP string `json:"natIP,omitempty"`
}

type AttachedDisk struct {
	Kind             string                `json:"kind"`
	Type             string                `json:"type"` // PERSISTENT, SCRATCH
	Mode             string                `json:"mode"` // READ_WRITE, READ_ONLY
	Source           string                `json:"source,omitempty"`
	InitializeParams *DiskInitializeParams `json:"initializeParams,omitempty"`
	DeviceName       string                `json:"deviceName"`
	Boot             bool                  `json:"boot"`
	AutoDelete       bool                  `json:"autoDelete"`
}

type DiskInitializeParams struct {
	SourceImage string `json:"sourceImage,omitempty"`
	DiskSizeGB  string `json:"diskSizeGb,omitempty"`
	DiskType    string `json:"diskType,omitempty"`
}

type NamedPort struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}

// InstanceGroup is the bounded unmanaged zonal group used by the classic HTTP
// load-balancer compatibility fixture. Managed and regional groups remain out
// of scope.
type InstanceGroup struct {
	Kind              string      `json:"kind"`
	ID                string      `json:"id"`
	Name              string      `json:"name"`
	Description       string      `json:"description,omitempty"`
	Zone              string      `json:"zone"`
	Network           string      `json:"network"`
	NamedPorts        []NamedPort `json:"namedPorts"`
	Instances         []string    `json:"instances"`
	Size              int         `json:"size"`
	SelfLink          string      `json:"selfLink"`
	CreationTimestamp string      `json:"creationTimestamp"`
}

// Network represents a VPC network.
type Network struct {
	Kind                                  string `json:"kind"`
	ID                                    string `json:"id"`
	Name                                  string `json:"name"`
	Description                           string `json:"description,omitempty"`
	SelfLink                              string `json:"selfLink"`
	AutoCreateSubnetworks                 bool   `json:"autoCreateSubnetworks"`
	NetworkFirewallPolicyEnforcementOrder string `json:"networkFirewallPolicyEnforcementOrder"`
	CreationTimestamp                     string `json:"creationTimestamp"`
}

// SecurityPolicy represents a Cloud Armor WAF rule set.
type SecurityPolicy struct {
	Kind              string               `json:"kind"`
	ID                string               `json:"id"`
	Name              string               `json:"name"`
	Description       string               `json:"description,omitempty"`
	SelfLink          string               `json:"selfLink"`
	Rules             []SecurityPolicyRule `json:"rules"`
	CreationTimestamp string               `json:"creationTimestamp"`
}

type SecurityPolicyRule struct {
	Priority    int        `json:"priority"`
	Action      string     `json:"action"`
	Description string     `json:"description,omitempty"`
	Match       *RuleMatch `json:"match,omitempty"`
}

type RuleMatch struct {
	VersionedExpr string           `json:"versionedExpr,omitempty"` // SRC_IPS_V1
	Config        *RuleMatchConfig `json:"config,omitempty"`
}

type RuleMatchConfig struct {
	SrcIPRanges []string `json:"srcIpRanges,omitempty"`
}

// FirewallRule represents a VPC firewall rule.
type FirewallRule struct {
	Kind              string          `json:"kind"`
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Description       string          `json:"description,omitempty"`
	Network           string          `json:"network"`
	Priority          int             `json:"priority"`
	Direction         string          `json:"direction"` // INGRESS, EGRESS
	Action            string          `json:"action"`    // allow, deny
	SourceRanges      []string        `json:"sourceRanges,omitempty"`
	DestinationRanges []string        `json:"destinationRanges,omitempty"`
	Allowed           []FirewallAllow `json:"allowed,omitempty"`
	Denied            []FirewallAllow `json:"denied,omitempty"`
	TargetTags        []string        `json:"targetTags,omitempty"`
	Disabled          bool            `json:"disabled"`
	SelfLink          string          `json:"selfLink"`
	CreationTimestamp string          `json:"creationTimestamp"`
}

type FirewallAllow struct {
	IPProtocol string   `json:"IPProtocol"` // tcp, udp, icmp, all
	Ports      []string `json:"ports,omitempty"`
}

const (
	metadataOnlyStatus            = "METADATA_ONLY"
	rehydratedInstanceDescription = "Metadata restored from profile state; the Docker-backed VM has not been reconciled."
)

type loadBalancerCollection struct {
	path         string
	canonical    string
	resourceKind string
	listKind     string
}

var loadBalancerCollections = map[string]loadBalancerCollection{
	"backendServices": {
		path: "backendServices", canonical: "backendServices",
		resourceKind: "compute#backendService", listKind: "compute#backendServiceList",
	},
	"healthChecks": {
		path: "healthChecks", canonical: "healthChecks",
		resourceKind: "compute#healthCheck", listKind: "compute#healthCheckList",
	},
	"urlMaps": {
		path: "urlMaps", canonical: "urlMaps",
		resourceKind: "compute#urlMap", listKind: "compute#urlMapList",
	},
	"targetHttpProxies": {
		path: "targetHttpProxies", canonical: "targetHttpProxies",
		resourceKind: "compute#targetHttpProxy", listKind: "compute#targetHttpProxyList",
	},
	"forwardingRules": {
		path: "forwardingRules", canonical: "forwardingRules",
		resourceKind: "compute#forwardingRule", listKind: "compute#forwardingRuleList",
	},
	"globalForwardingRules": {
		path: "globalForwardingRules", canonical: "forwardingRules",
		resourceKind: "compute#forwardingRule", listKind: "compute#forwardingRuleList",
	},
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim struct
// ─────────────────────────────────────────────────────────────────────────────

// API is the high-fidelity Compute Engine v1 shim.
type legacyVMBackend interface {
	DeleteLegacyComputeVM(string) error
}

type legacyVPCBackend interface {
	DeleteLegacyVPCNetwork(context.Context, string) error
}

type firewallBackend interface {
	RegisterFirewallRule(string, orchestrator.FirewallEntry)
	RemoveFirewallRule(string, string)
	ApplyFirewallPortsToComputeInstances(
		string,
		string,
		[]orchestrator.ComputeInstanceIdentity,
		[]string,
	) error
}

type API struct {
	mu                sync.RWMutex
	persistMu         sync.Mutex
	initMu            sync.RWMutex
	opMgr             *orchestrator.OperationManager
	svcMgr            *orchestrator.ServiceManager
	vpcIPAM           vpcIPAMBackend
	legacyVM          legacyVMBackend
	legacyVPC         legacyVPCBackend
	firewall          firewallBackend
	initializationErr error
	stateStore        computeMetadataStore
	instances         map[string]*Instance   // key: project+":"+zone+":"+name
	networks          map[string]*Network    // key: project+":"+name
	subnetworks       map[string]*Subnetwork // key: project+":"+region+":"+name
	nextSubnetworkID  uint64
	securityPolicies  map[string]*SecurityPolicy // key: project+":"+name
	firewalls         map[string]*FirewallRule   // key: project+":"+name
	instanceGroups    map[string]*InstanceGroup  // key: project+":"+zone+":"+name
	loadBalancers     map[string]map[string]interface{}
	roundRobin        map[string]uint64
	httpClient        *http.Client
}

// NewAPI builds the Compute shim with the shared LRO manager and service manager.
func NewAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Compute Engine] state initialization failed: %v", err)
		api := newAPI(opMgr, svcMgr, nil)
		api.setInitializationError(fmt.Errorf("initialize Compute state: %w", err))
		return api
	}
	api, err := NewAPIWithStore(opMgr, svcMgr, store)
	if err != nil {
		log.Printf("[Shim: Compute Engine] state rehydration failed: %v", err)
		api = newAPI(opMgr, svcMgr, store)
		api.setInitializationError(err)
		return api
	}
	if err := api.ReconcileVPCIPAM(context.Background()); err != nil {
		log.Printf("[Shim: Compute Engine] VPC IPAM reconciliation failed: %v", err)
		api.setInitializationError(err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager, store computeMetadataStore) *API {
	api := &API{
		opMgr:            opMgr,
		svcMgr:           svcMgr,
		stateStore:       store,
		instances:        make(map[string]*Instance),
		networks:         make(map[string]*Network),
		subnetworks:      make(map[string]*Subnetwork),
		nextSubnetworkID: 1,
		securityPolicies: make(map[string]*SecurityPolicy),
		firewalls:        make(map[string]*FirewallRule),
		instanceGroups:   make(map[string]*InstanceGroup),
		loadBalancers:    make(map[string]map[string]interface{}),
		roundRobin:       make(map[string]uint64),
		httpClient:       &http.Client{Timeout: 2 * time.Second},
	}
	if svcMgr != nil {
		api.vpcIPAM = svcMgr
		api.legacyVM = svcMgr
		api.legacyVPC = svcMgr
		api.firewall = svcMgr
	}
	return api
}

// ─────────────────────────────────────────────────────────────────────────────
// Top-level routing
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) ListProjects() []string {
	api.mu.RLock()
	defer api.mu.RUnlock()

	projects := make(map[string]bool)
	for _, inst := range api.instances {
		if inst.project != "" {
			projects[inst.project] = true
		}
	}
	for k := range api.networks {
		p := strings.Split(k, ":")[0]
		projects[p] = true
	}
	for k := range api.firewalls {
		p := strings.Split(k, ":")[0]
		projects[p] = true
	}
	for k := range api.loadBalancers {
		p := strings.SplitN(k, ":", 2)[0]
		projects[p] = true
	}

	res := []string{}
	for p := range projects {
		res = append(res, p)
	}
	return res
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Compute Engine] %s %s", r.Method, r.URL.Path)

	path := r.URL.Path
	if api.initializationError() != nil {
		w.Header().Set("Content-Type", "application/json")
		writeErrorStatus(
			w,
			http.StatusServiceUnavailable,
			"FAILED_PRECONDITION",
			"Compute backend reconciliation is incomplete",
		)
		return
	}
	if project, forwardingRule, proxyPath, ok := parseLoadBalancerProxyPath(path); ok {
		api.proxyLoadBalancerRequest(w, r, project, forwardingRule, proxyPath)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(path, "/instances") && strings.Contains(path, "/zones/"):
		api.routeInstances(w, r, path)
	case strings.Contains(path, "/instanceGroups") && strings.Contains(path, "/zones/"):
		api.routeInstanceGroups(w, r, path)
	case strings.Contains(path, "/operations/"):
		api.routeOperations(w, r, path)
	case strings.Contains(path, "/regions/") && strings.Contains(path, "/subnetworks"):
		api.routeSubnetworks(w, r, path)
	case strings.Contains(path, "/instanceGroupManagers") ||
		(strings.Contains(path, "/regions/") && strings.Contains(path, "/instanceGroups")) ||
		strings.Contains(path, "/targetHttpsProxies") ||
		strings.Contains(path, "/targetTcpProxies") ||
		strings.Contains(path, "/targetSslProxies"):
		w.WriteHeader(http.StatusNotImplemented)
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Managed or regional instance groups and HTTPS, TCP, or SSL proxies are outside the bounded classic global HTTP load-balancer surface")
	case strings.Contains(path, "/zones/") && !strings.Contains(path, "/instances"):
		api.routeZones(w, r, path)
	case isUnsupportedAdvancedNetworkPath(path):
		w.WriteHeader(http.StatusNotImplemented)
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Cloud NAT, peering, PSC service attachments, VPN, and interconnect data planes are not representable safely in MiniSky")
	case strings.Contains(path, "/global/networks"):
		api.routeNetworks(w, r, path)
	case strings.Contains(path, "/global/firewalls"):
		api.routeFirewalls(w, r, path)
	case strings.Contains(path, "/global/securityPolicies"):
		api.routeSecurityPolicies(w, r, path)
	case strings.Contains(path, "/global/backendServices") ||
		strings.Contains(path, "/global/healthChecks") ||
		strings.Contains(path, "/global/urlMaps") ||
		strings.Contains(path, "/global/forwardingRules") ||
		strings.Contains(path, "/global/globalForwardingRules") ||
		strings.Contains(path, "/global/targetHttpProxies"):
		api.routeLoadBalancer(w, r, path)
	case strings.Contains(path, "/global/images"):
		api.routeImages(w, r, path)
	default:
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Compute resource not found: "+path)
	}
}

func isUnsupportedAdvancedNetworkPath(path string) bool {
	for _, fragment := range []string{
		"/routers",
		"/serviceAttachments",
		"/interconnects",
		"/vpnGateways",
		"/vpnTunnels",
		"/addPeering",
		"/removePeering",
		"/updatePeering",
	} {
		if strings.Contains(path, fragment) {
			return true
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// Instances
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeInstances(w http.ResponseWriter, r *http.Request, path string) {
	project, zone := extractProjectZone(path)

	// Action suffixes (start / stop / reset)
	switch {
	case strings.HasSuffix(strings.TrimRight(path, "/"), "/start"):
		name := extractSegmentBefore(path, "/start")
		api.instanceAction(w, r, project, zone, name, "start")
		return
	case strings.HasSuffix(strings.TrimRight(path, "/"), "/stop"):
		name := extractSegmentBefore(path, "/stop")
		api.instanceAction(w, r, project, zone, name, "stop")
		return
	}

	// Determine instance name (if present)
	instanceName := extractAfterInstances(path)

	switch r.Method {
	case http.MethodPost:
		api.insertInstance(w, r, project, zone)
	case http.MethodGet:
		if instanceName != "" {
			api.getInstance(w, r, project, zone, instanceName)
		} else {
			api.listInstances(w, r, project, zone)
		}
	case http.MethodDelete:
		api.deleteInstance(w, r, project, zone, instanceName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// insertInstance — handles instances.insert.
// Creates an in-memory instance in PROVISIONING state and kicks off an LRO.
func (api *API) insertInstance(w http.ResponseWriter, r *http.Request, project, zone string) {
	var body struct {
		Name              string             `json:"name"`
		MachineType       string             `json:"machineType"`
		Description       string             `json:"description"`
		Labels            map[string]string  `json:"labels"`
		Metadata          *InstanceMetadata  `json:"metadata"`
		NetworkInterfaces []NetworkInterface `json:"networkInterfaces"`
		Disks             []AttachedDisk     `json:"disks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Request body parse error: "+err.Error())
		return
	}

	name := body.Name
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Field 'name' is required for instances.insert")
		return
	}
	isGKE := body.Labels != nil && body.Labels["managed-by"] == "gke"
	if !isGKE {
		if _, err := orchestratorComputeIdentity(project, zone, name).DockerName(); err != nil {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}

	selfLink := selfLinkInstance(project, zone, name)
	targetLink := selfLink
	zoneFull := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", project, zone)

	// Normalise MachineType (accept short or full form)
	machineType := body.MachineType
	if machineType == "" {
		machineType = "n1-standard-1"
	}
	if !strings.HasPrefix(machineType, "https://") {
		machineType = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/machineTypes/%s",
			project, zone, machineType)
	}

	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	netIfaces, err := api.resolveInstanceNetworkInterfaces(project, zone, body.NetworkInterfaces)
	if err != nil {
		if errors.Is(err, errUnsupportedAutoNetwork) || errors.Is(err, errUnsupportedMultipleNICs) {
			writeErrorStatus(w, http.StatusNotImplemented, "UNIMPLEMENTED", err.Error())
		} else {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		}
		return
	}

	// Default boot disk
	disks := body.Disks
	if len(disks) == 0 {
		disks = []AttachedDisk{{
			Kind:       "compute#attachedDisk",
			Type:       "PERSISTENT",
			Mode:       "READ_WRITE",
			DeviceName: name,
			Boot:       true,
			AutoDelete: true,
		}}
	}

	// Resolve only curated local backends from the provider's boot image. A
	// Compute source image is never passed through as an arbitrary Docker image.
	osImage := "ubuntu:26.04"
	var dockerCommand []string
	sourceImage := ""
	for _, disk := range disks {
		if disk.Boot && disk.InitializeParams != nil && disk.InitializeParams.SourceImage != "" {
			sourceImage = disk.InitializeParams.SourceImage
			break
		}
	}
	if sourceImage != "" {
		var supported bool
		osImage, dockerCommand, supported = curatedComputeBackend(sourceImage)
		if !supported {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
				"boot disk initializeParams.sourceImage is not supported by the curated local Compute backend")
			return
		}
	}

	inst := &Instance{
		Kind:              "compute#instance",
		ID:                randomNumericID(),
		Name:              name,
		Zone:              zoneFull,
		MachineType:       machineType,
		Status:            "PROVISIONING",
		SelfLink:          selfLink,
		Description:       body.Description,
		Labels:            body.Labels,
		Metadata:          body.Metadata,
		NetworkInterfaces: netIfaces,
		Disks:             disks,
		CreationTimestamp: time.Now().UTC().Format(time.RFC3339),
		project:           project,
		zone:              zone,
		Fingerprint:       "minisky-mock-fingerprint",
		LabelFingerprint:  "minisky-label-fingerprint",
		Scheduling: &Scheduling{
			OnHostMaintenance: "MIGRATE",
			AutomaticRestart:  true,
			Preemptible:       false,
		},
		CanIpForward: false,
	}

	if inst.Metadata == nil {
		inst.Metadata = &InstanceMetadata{
			Kind:  "compute#metadata",
			Items: []MetadataItem{},
		}
	} else if inst.Metadata.Items == nil {
		inst.Metadata.Items = []MetadataItem{}
	}

	key := instanceKey(project, zone, name)
	api.mu.Lock()
	if api.instances[key] != nil {
		api.mu.Unlock()
		writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS", "Instance "+name+" already exists")
		return
	}
	api.instances[key] = inst
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.Unlock()
	saveErr := snapshotErr
	if saveErr == nil {
		saveErr = api.saveMetadataPayload(payload)
	}
	if saveErr != nil {
		api.mu.Lock()
		if api.instances[key] == inst {
			delete(api.instances, key)
		}
		api.mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist instance metadata: "+saveErr.Error())
		return
	}

	// Register LRO
	op, err := api.opMgr.RegisterDurable("compute#operation", "insert", targetLink, zone, "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	op.Kind = "compute#operation"

	// Drive state machine asynchronously: PROVISIONING → PROVISIONING_DOCKER → RUNNING
	opName := op.Name
	api.opMgr.RunAsync(opName, func() error {
		// 1. Initial Staging phase (simulates resource allocation)
		api.mu.Lock()
		if i, ok := api.instances[key]; ok {
			i.Status = "STAGING"
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			log.Printf("[Shim: Compute Engine] persist staging instance: %v", err)
		}
		time.Sleep(2 * time.Second)

		if isGKE {
			// Kind already manages the docker daemon side. Mark running directly.
			api.mu.Lock()
			if i, ok := api.instances[key]; ok {
				i.Status = "RUNNING"
			}
			api.mu.Unlock()
			if err := api.persistMetadata(); err != nil {
				log.Printf("[Shim: Compute Engine] persist running instance: %v", err)
			}
			return nil
		}

		// 2. Provisioning phase (simulates Docker container startup)
		api.mu.Lock()
		if i, ok := api.instances[key]; ok {
			i.Status = "PROVISIONING"
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			log.Printf("[Shim: Compute Engine] persist provisioning instance: %v", err)
		}

		var interfaces []NetworkInterface
		api.mu.RLock()
		if i, ok := api.instances[key]; ok {
			interfaces = append(interfaces, i.NetworkInterfaces...)
		}
		api.mu.RUnlock()
		logicalVPC, dockerVPC, nameErr := resolvedInstanceVPCDockerNetwork(project, interfaces)
		if nameErr != nil {
			return nameErr
		}
		allowedPorts := api.getAllowedPortsForVPC(logicalVPC)

		// Tell the Orchestrator to physically spin up the Docker container!
		err := api.svcMgr.ProvisionComputeInstance(
			context.Background(),
			orchestratorComputeIdentity(project, zone, name),
			osImage,
			dockerVPC,
			allowedPorts,
			[]string{},
			dockerCommand,
		)

		// Keep the simulated delay outside the metadata lock.
		if err == nil {
			time.Sleep(1500 * time.Millisecond)
		}
		api.mu.Lock()
		if i, ok := api.instances[key]; ok {
			if err != nil {
				i.Status = "TERMINATED"
				i.Description = fmt.Sprintf("Failed to provision docker data plane: %v", err)
				api.mu.Unlock()
				if persistErr := api.persistMetadata(); persistErr != nil {
					log.Printf("[Shim: Compute Engine] persist failed instance: %v", persistErr)
				}
				return err
			}

			i.Status = "RUNNING"
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			log.Printf("[Shim: Compute Engine] persist running instance: %v", err)
		}
		return nil
	})

	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) getInstance(w http.ResponseWriter, r *http.Request, project, zone, name string) {
	key := instanceKey(project, zone, name)
	api.mu.RLock()
	inst, ok := api.instances[key]

	if !ok {
		api.mu.RUnlock()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("Instance '%s' not found in zone '%s'", name, zone))
		return
	}

	// Deep copy under lock to avoid racing with background updates
	instCopy := inst.DeepCopy()
	api.mu.RUnlock()

	// Inject dynamic host ports from orchestrator
	instCopy.HostPorts = api.computeInstancePortMappings(project, zone, instCopy)
	if len(instCopy.NetworkInterfaces) > 0 {
		ip := api.computeInstanceIP(project, zone, instCopy)
		if ip == "" {
			ip = "10.128.0.2" // Fallback to avoid empty IP which can crash some providers
		}
		instCopy.NetworkInterfaces[0].NetworkIP = ip
	}

	if instCopy.Fingerprint == "" {
		instCopy.Fingerprint = "minisky-mock-fingerprint"
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(instCopy)
}

func (api *API) listInstances(w http.ResponseWriter, r *http.Request, project, zone string) {
	prefix := instanceKey(project, zone, "")
	api.mu.RLock()
	defer api.mu.RUnlock()

	items := []*Instance{}
	for k, v := range api.instances {
		if strings.HasPrefix(k, prefix) {
			copyOfInst := v.DeepCopy()
			copyOfInst.HostPorts = api.computeInstancePortMappings(project, zone, copyOfInst)
			if len(copyOfInst.NetworkInterfaces) > 0 {
				ip := api.computeInstanceIP(project, zone, copyOfInst)
				if ip == "" {
					ip = "10.128.0.2"
				}
				copyOfInst.NetworkInterfaces[0].NetworkIP = ip
			}
			items = append(items, copyOfInst)
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":  "compute#instanceList",
		"id":    fmt.Sprintf("projects/%s/zones/%s/instances", project, zone),
		"items": items,
	})
}

func (api *API) deleteInstance(w http.ResponseWriter, r *http.Request, project, zone, name string) {
	key := instanceKey(project, zone, name)
	api.mu.Lock()
	inst, ok := api.instances[key]
	if !ok {
		api.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("Instance '%s' not found", name))
		return
	}

	if r.Header.Get("X-Minisky-GKE-Bypass") != "true" && inst.Labels != nil && inst.Labels["managed-by"] == "gke" {
		api.mu.Unlock()
		w.WriteHeader(http.StatusForbidden)
		writeError(w, 403, "FORBIDDEN", "This instance is managed by Kubernetes Engine and cannot be manually deleted.")
		return
	}
	isGKE := inst.Labels != nil && inst.Labels["managed-by"] == "gke"

	// Mark as DELETING so the UI shows the "winding down" process
	inst.Status = "DELETING"
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist deleting instance metadata: "+err.Error())
		return
	}

	containerName, _ := computeInstanceContainerName(project, zone, inst)
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete",
		selfLinkInstance(project, zone, name), zone, "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		// Simulate winding down time
		time.Sleep(3 * time.Second)

		if isGKE {
			api.svcMgr.DeleteComputeVM(containerName)
		} else {
			_ = api.svcMgr.DeleteComputeInstance(orchestratorComputeIdentity(project, zone, name))
		}

		legacyCleanup := api.removeInstanceAndLegacyCleanupEligibility(key, name)
		if !isGKE {
			api.cleanupLegacyComputeVM(name, legacyCleanup)
		}
		if err := api.persistMetadata(); err != nil {
			log.Printf("[Shim: Compute Engine] persist deleted instance: %v", err)
		}
		return nil
	})

	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) removeInstanceAndLegacyCleanupEligibility(key, name string) bool {
	api.mu.Lock()
	defer api.mu.Unlock()
	delete(api.instances, key)
	for _, instance := range api.instances {
		if instance != nil && instance.Name == name {
			return false
		}
	}
	return true
}

func (api *API) cleanupLegacyComputeVM(name string, eligible bool) {
	if eligible && api.legacyVM != nil {
		_ = api.legacyVM.DeleteLegacyComputeVM(name)
	}
}

func (api *API) instanceAction(w http.ResponseWriter, r *http.Request, project, zone, name, action string) {
	key := instanceKey(project, zone, name)
	api.mu.Lock()
	inst, ok := api.instances[key]
	if ok {
		switch action {
		case "start":
			inst.Status = "RUNNING"
		case "stop":
			inst.Status = "TERMINATED"
		}
	}
	api.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("Instance '%s' not found", name))
		return
	}
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist instance action: "+err.Error())
		return
	}

	op, err := api.opMgr.RegisterDurable("compute#operation", action,
		selfLinkInstance(project, zone, name), zone, "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error { return nil })
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

// ─────────────────────────────────────────────────────────────────────────────
// Unmanaged zonal instance groups
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeInstanceGroups(w http.ResponseWriter, r *http.Request, path string) {
	project, zone := extractProjectZone(path)
	name := extractSegmentAfter(path, "instanceGroups")
	action := ""
	for _, candidate := range []string{"addInstances", "setNamedPorts", "listInstances"} {
		if strings.HasSuffix(strings.TrimRight(path, "/"), "/"+candidate) {
			action = candidate
			break
		}
	}
	if action != "" {
		name = extractSegmentBefore(path, "/"+action)
		api.instanceGroupAction(w, r, project, zone, name, action)
		return
	}

	switch r.Method {
	case http.MethodPost:
		if name != "" {
			writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Unsupported instance-group route")
			return
		}
		api.createInstanceGroup(w, r, project, zone)
	case http.MethodGet:
		if name == "" {
			api.listInstanceGroups(w, project, zone)
			return
		}
		api.getInstanceGroup(w, project, zone, name)
	case http.MethodDelete:
		api.deleteInstanceGroup(w, project, zone, name)
	default:
		writeErrorStatus(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Unsupported instance-group method")
	}
}

func (api *API) createInstanceGroup(w http.ResponseWriter, r *http.Request, project, zone string) {
	var body struct {
		Name        string      `json:"name"`
		Description string      `json:"description"`
		Network     string      `json:"network"`
		NamedPorts  []NamedPort `json:"namedPorts"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Request body parse error: "+err.Error())
		return
	}
	if body.Name == "" {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Field 'name' is required")
		return
	}
	if body.Network == "" {
		body.Network = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/default", project)
	}
	for _, namedPort := range body.NamedPorts {
		if namedPort.Name == "" || namedPort.Port < 1 || namedPort.Port > 65535 {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Named ports require a name and port from 1 through 65535")
			return
		}
	}
	sort.Slice(body.NamedPorts, func(i, j int) bool { return body.NamedPorts[i].Name < body.NamedPorts[j].Name })
	group := &InstanceGroup{
		Kind:              "compute#instanceGroup",
		ID:                randomNumericID(),
		Name:              body.Name,
		Description:       body.Description,
		Zone:              computeZoneSelfLink(project, zone),
		Network:           body.Network,
		NamedPorts:        append([]NamedPort(nil), body.NamedPorts...),
		Instances:         []string{},
		SelfLink:          instanceGroupSelfLink(project, zone, body.Name),
		CreationTimestamp: time.Now().UTC().Format(time.RFC3339),
	}
	key := instanceGroupKey(project, zone, body.Name)
	api.mu.Lock()
	if api.instanceGroups[key] != nil {
		api.mu.Unlock()
		writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS", fmt.Sprintf("Instance group %q already exists", body.Name))
		return
	}
	api.instanceGroups[key] = group
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist instance-group metadata: "+err.Error())
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", "insert", group.SelfLink, zone, "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error { return nil })
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) getInstanceGroup(w http.ResponseWriter, project, zone, name string) {
	api.mu.RLock()
	group := cloneInstanceGroup(api.instanceGroups[instanceGroupKey(project, zone, name)])
	api.mu.RUnlock()
	if group == nil {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("Instance group %q not found", name))
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(group)
}

func (api *API) listInstanceGroups(w http.ResponseWriter, project, zone string) {
	prefix := instanceGroupKey(project, zone, "")
	api.mu.RLock()
	items := make([]*InstanceGroup, 0)
	for key, group := range api.instanceGroups {
		if strings.HasPrefix(key, prefix) {
			items = append(items, cloneInstanceGroup(group))
		}
	}
	api.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":  "compute#instanceGroupList",
		"id":    fmt.Sprintf("projects/%s/zones/%s/instanceGroups", project, zone),
		"items": items,
	})
}

func (api *API) deleteInstanceGroup(w http.ResponseWriter, project, zone, name string) {
	key := instanceGroupKey(project, zone, name)
	api.mu.Lock()
	group := api.instanceGroups[key]
	if group != nil {
		delete(api.instanceGroups, key)
	}
	api.mu.Unlock()
	if group == nil {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("Instance group %q not found", name))
		return
	}
	if err := api.persistMetadata(); err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist instance-group deletion: "+err.Error())
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete", group.SelfLink, zone, "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error { return nil })
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) instanceGroupAction(w http.ResponseWriter, r *http.Request, project, zone, name, action string) {
	if r.Method != http.MethodPost {
		writeErrorStatus(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Instance-group actions require POST")
		return
	}
	key := instanceGroupKey(project, zone, name)
	api.mu.Lock()
	group := api.instanceGroups[key]
	if group == nil {
		api.mu.Unlock()
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("Instance group %q not found", name))
		return
	}
	if action == "listInstances" {
		items := make([]map[string]string, 0, len(group.Instances))
		for _, instance := range group.Instances {
			items = append(items, map[string]string{"instance": instance, "status": "RUNNING"})
		}
		api.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"kind": "compute#instanceGroupsListInstances", "items": items})
		return
	}

	switch action {
	case "addInstances":
		var body struct {
			Instances []struct {
				Instance string `json:"instance"`
			} `json:"instances"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.mu.Unlock()
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Request body parse error: "+err.Error())
			return
		}
		existing := make(map[string]bool, len(group.Instances))
		for _, instance := range group.Instances {
			existing[instance] = true
		}
		for _, member := range body.Instances {
			memberProject, memberZone, _, ok := parseInstanceReference(member.Instance)
			if !ok || memberProject != project || memberZone != zone {
				api.mu.Unlock()
				writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Instance-group members must be zonal Compute instance references")
				return
			}
			if !existing[member.Instance] {
				group.Instances = append(group.Instances, member.Instance)
				existing[member.Instance] = true
			}
		}
		sort.Strings(group.Instances)
		group.Size = len(group.Instances)
	case "setNamedPorts":
		var body struct {
			NamedPorts []NamedPort `json:"namedPorts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.mu.Unlock()
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Request body parse error: "+err.Error())
			return
		}
		for _, namedPort := range body.NamedPorts {
			if namedPort.Name == "" || namedPort.Port < 1 || namedPort.Port > 65535 {
				api.mu.Unlock()
				writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Named ports require a name and port from 1 through 65535")
				return
			}
		}
		group.NamedPorts = append([]NamedPort(nil), body.NamedPorts...)
		sort.Slice(group.NamedPorts, func(i, j int) bool { return group.NamedPorts[i].Name < group.NamedPorts[j].Name })
	}
	targetLink := group.SelfLink
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist instance-group update: "+err.Error())
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", action, targetLink, zone, "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error { return nil })
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

// ─────────────────────────────────────────────────────────────────────────────
// Zones
// ─────────────────────────────────────────────────────────────────────────────

// routeZones handles GET requests for zone resources, e.g. from Terraform's
// zone-validation step before creating a Compute instance.
func (api *API) routeZones(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	zone := extractSegmentAfter(path, "zones")

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if zone == "" {
		// List zones — return a minimal list of common zones.
		zones := []map[string]interface{}{}
		for _, z := range []string{"us-central1-a", "us-central1-b", "us-east1-b", "europe-west1-b"} {
			zones = append(zones, map[string]interface{}{
				"kind":     "compute#zone",
				"id":       randomNumericID(),
				"name":     z,
				"status":   "UP",
				"selfLink": fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", project, z),
				"region":   fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s", project, strings.Join(strings.Split(z, "-")[:2], "-")),
			})
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"kind":  "compute#zoneList",
			"items": zones,
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":     "compute#zone",
		"id":       randomNumericID(),
		"name":     zone,
		"status":   "UP",
		"selfLink": fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", project, zone),
		"region":   fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s", project, strings.Join(strings.Split(zone, "-")[:2], "-")),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Images
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeImages(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	// Example path: /compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2604-lts
	family := ""
	if strings.Contains(path, "/family/") {
		parts := strings.Split(path, "/family/")
		family = parts[len(parts)-1]
	}

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Return a synthetic image
	imageName := family
	if imageName == "" {
		imageName = "minisky-mock-image"
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":              "compute#image",
		"id":                randomNumericID(),
		"name":              imageName,
		"status":            "READY",
		"sourceType":        "RAW",
		"selfLink":          fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/images/%s", project, imageName),
		"creationTimestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Operations
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeOperations(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if strings.Contains(path, "/regions/") {
		if r.Method != http.MethodGet || len(parts) != 8 ||
			parts[0] != "compute" || parts[1] != "v1" || parts[2] != "projects" ||
			parts[3] == "" || parts[4] != "regions" || parts[5] == "" ||
			parts[6] != "operations" || parts[7] == "" {
			writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Regional operation not found")
			return
		}
		if api.opMgr == nil {
			writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Regional operation not found")
			return
		}
		op := api.opMgr.Get(parts[7])
		targetProjectPrefix := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/", parts[3])
		if op == nil || op.Kind != "compute#operation" || op.Region != parts[5] ||
			!strings.HasPrefix(op.TargetLink, targetProjectPrefix) {
			writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Operation not found: "+parts[7])
			return
		}
		w.WriteHeader(http.StatusOK)
		writeComputeOperation(w, parts[3], op)
		return
	}
	// Find "operations" segment and take next segment as name
	opName := ""
	for i, p := range parts {
		if p == "operations" && i+1 < len(parts) {
			opName = parts[i+1]
			break
		}
	}

	if opName == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Missing operation name in path")
		return
	}

	op := api.opMgr.Get(opName)
	if op == nil {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Operation not found: "+opName)
		return
	}
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

// ─────────────────────────────────────────────────────────────────────────────
// Networks (VPC)
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeNetworks(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	name := extractAfterGlobal(path, "networks")

	switch r.Method {
	case http.MethodPost:
		api.insertNetwork(w, r, project)

	case http.MethodGet:
		if name != "" {
			key := project + ":" + name
			api.mu.RLock()
			n, ok := api.networks[key]
			api.mu.RUnlock()

			if !ok && name == "default" {
				// Return a virtual default network
				n = &Network{
					Kind:              "compute#network",
					ID:                "0",
					Name:              "default",
					SelfLink:          fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/default", project),
					CreationTimestamp: "2024-01-01T00:00:00Z",
				}
				ok = true
			}

			if !ok {
				w.WriteHeader(http.StatusNotFound)
				writeError(w, 404, "NOT_FOUND", "Network "+name+" not found")
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(n)
		} else {
			prefix := project + ":"
			api.mu.RLock()
			items := []*Network{}
			hasDefault := false
			for k, v := range api.networks {
				if strings.HasPrefix(k, prefix) {
					items = append(items, v)
					if v.Name == "default" {
						hasDefault = true
					}
				}
			}
			api.mu.RUnlock()

			if !hasDefault {
				items = append(items, &Network{
					Kind:              "compute#network",
					ID:                "0",
					Name:              "default",
					SelfLink:          fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/default", project),
					CreationTimestamp: "2024-01-01T00:00:00Z",
				})
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"kind":  "compute#networkList",
				"items": items,
			})
		}

	case http.MethodDelete:
		api.deleteNetwork(w, r, project, name)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Security Policies (Cloud Armor)
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeSecurityPolicies(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	name := extractAfterGlobal(path, "securityPolicies")

	switch r.Method {
	case http.MethodPost:
		var body struct {
			Name        string               `json:"name"`
			Description string               `json:"description"`
			Rules       []SecurityPolicyRule `json:"rules"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		// Always add a default allow-all rule at priority 2147483647 (GCP convention)
		rules := body.Rules
		hasDefault := false
		for _, rule := range rules {
			if rule.Priority == 2147483647 {
				hasDefault = true
				break
			}
		}
		if !hasDefault {
			rules = append(rules, SecurityPolicyRule{
				Priority:    2147483647,
				Action:      "allow",
				Description: "default allow-all rule",
			})
		}

		sp := &SecurityPolicy{
			Kind:              "compute#securityPolicy",
			ID:                randomNumericID(),
			Name:              body.Name,
			Description:       body.Description,
			Rules:             rules,
			SelfLink:          fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/securityPolicies/%s", project, body.Name),
			CreationTimestamp: time.Now().UTC().Format(time.RFC3339),
		}
		key := project + ":" + body.Name
		api.mu.Lock()
		api.securityPolicies[key] = sp
		api.mu.Unlock()

		op, err := api.opMgr.RegisterDurable("compute#operation", "insert", sp.SelfLink, "", "")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
			return
		}
		api.opMgr.RunAsync(op.Name, func() error { return nil })
		w.WriteHeader(http.StatusOK)
		writeComputeOperation(w, project, op)

	case http.MethodGet:
		if name != "" {
			key := project + ":" + name
			api.mu.RLock()
			sp, ok := api.securityPolicies[key]
			api.mu.RUnlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				writeError(w, 404, "NOT_FOUND", "SecurityPolicy "+name+" not found")
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(sp)
		} else {
			prefix := project + ":"
			api.mu.RLock()
			items := []*SecurityPolicy{}
			for k, v := range api.securityPolicies {
				if strings.HasPrefix(k, prefix) {
					items = append(items, v)
				}
			}
			api.mu.RUnlock()
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"kind":  "compute#securityPolicyList",
				"items": items,
			})
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Load Balancer metadata resources
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeLoadBalancer(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	collection, name, valid := parseLoadBalancerPath(path)
	if !valid {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Unsupported Compute load-balancer route: "+path)
		return
	}

	switch r.Method {
	case http.MethodPost:
		if name != "" {
			w.WriteHeader(http.StatusNotFound)
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Unsupported Compute load-balancer route: "+path)
			return
		}
		api.createLoadBalancerResource(w, r, project, collection)
	case http.MethodGet:
		if name == "" {
			api.listLoadBalancerResources(w, project, collection)
			return
		}
		api.getLoadBalancerResource(w, project, collection, name)
	case http.MethodDelete:
		if name == "" {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "A resource name is required for delete")
			return
		}
		api.deleteLoadBalancerResource(w, project, collection, name)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("Method %s is not supported for %s", r.Method, collection.path))
	}
}

type loadBalancerBackend struct {
	target      *url.URL
	healthPath  string
	description string
}

func (api *API) proxyLoadBalancerRequest(
	w http.ResponseWriter,
	r *http.Request,
	project string,
	forwardingRuleName string,
	proxyPath string,
) {
	backends, backendService, err := api.resolveLoadBalancerBackends(project, forwardingRuleName)
	if err != nil {
		writeLoadBalancerUnavailable(w, err.Error())
		return
	}

	healthy := make([]loadBalancerBackend, 0, len(backends))
	for _, backend := range backends {
		if api.backendIsHealthy(r, backend) {
			healthy = append(healthy, backend)
		}
	}
	if len(healthy) == 0 {
		writeLoadBalancerUnavailable(w, fmt.Sprintf(
			"backend service %q has no healthy resolvable backends",
			backendService,
		))
		return
	}

	api.mu.Lock()
	cursor := api.roundRobin[loadBalancerKey(project, "backendServices", backendService)]
	api.roundRobin[loadBalancerKey(project, "backendServices", backendService)] = cursor + 1
	api.mu.Unlock()
	backend := healthy[cursor%uint64(len(healthy))]

	outbound := r.Clone(r.Context())
	outbound.URL.Path = proxyPath
	outbound.URL.RawPath = ""
	proxy := observability.NewReverseProxy(backend.target)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
		writeLoadBalancerUnavailable(w, fmt.Sprintf(
			"backend %s became unavailable: %v",
			backend.description,
			proxyErr,
		))
	}
	proxy.ServeHTTP(w, outbound)
}

func (api *API) resolveLoadBalancerBackends(
	project string,
	forwardingRuleName string,
) ([]loadBalancerBackend, string, error) {
	api.mu.RLock()
	forwardingRule := api.loadBalancers[loadBalancerKey(project, "forwardingRules", forwardingRuleName)]
	if forwardingRule == nil {
		api.mu.RUnlock()
		return nil, "", fmt.Errorf("forwarding rule %q was not found", forwardingRuleName)
	}
	targetProxyName := resourceReferenceName(forwardingRule["target"])
	targetProxy := api.loadBalancers[loadBalancerKey(project, "targetHttpProxies", targetProxyName)]
	if targetProxy == nil {
		api.mu.RUnlock()
		return nil, "", fmt.Errorf("forwarding rule %q does not resolve to a target HTTP proxy", forwardingRuleName)
	}
	urlMapName := resourceReferenceName(targetProxy["urlMap"])
	urlMap := api.loadBalancers[loadBalancerKey(project, "urlMaps", urlMapName)]
	if urlMap == nil {
		api.mu.RUnlock()
		return nil, "", fmt.Errorf("target HTTP proxy %q does not resolve to a URL map", targetProxyName)
	}
	if hasNonEmptyList(urlMap["hostRules"]) || hasNonEmptyList(urlMap["pathMatchers"]) {
		api.mu.RUnlock()
		return nil, "", fmt.Errorf("URL map %q uses unsupported host or path rules; only defaultService routing is supported", urlMapName)
	}
	backendServiceName := resourceReferenceName(urlMap["defaultService"])
	backendService := api.loadBalancers[loadBalancerKey(project, "backendServices", backendServiceName)]
	if backendService == nil {
		api.mu.RUnlock()
		return nil, "", fmt.Errorf("URL map %q does not resolve to a default backend service", urlMapName)
	}
	if protocol, _ := backendService["protocol"].(string); protocol != "" &&
		!strings.EqualFold(protocol, "HTTP") && !strings.EqualFold(protocol, "HTTPS") {
		api.mu.RUnlock()
		return nil, backendServiceName, fmt.Errorf("backend service %q uses unsupported protocol %q", backendServiceName, protocol)
	}
	rawBackends, hasBackends := backendService["backends"].([]interface{})
	portName, _ := backendService["portName"].(string)
	rawHealthChecks, _ := backendService["healthChecks"].([]interface{})
	healthPath, healthErr := api.resolveHealthPathLocked(project, rawHealthChecks)
	api.mu.RUnlock()

	if healthErr != nil {
		return nil, backendServiceName, healthErr
	}
	if !hasBackends || len(rawBackends) == 0 {
		return nil, backendServiceName, fmt.Errorf("backend service %q has no backends", backendServiceName)
	}

	backends := make([]loadBalancerBackend, 0, len(rawBackends))
	var unsupported []string
	for index, rawBackend := range rawBackends {
		resolved, err := api.resolveLoadBalancerBackend(project, rawBackend, portName, healthPath)
		if err != nil {
			unsupported = append(unsupported, fmt.Sprintf("backend %d: %v", index, err))
			continue
		}
		backends = append(backends, resolved...)
	}
	if len(backends) == 0 {
		return nil, backendServiceName, fmt.Errorf(
			"backend service %q has only unsupported backends (%s); supported forms require an explicit HTTP(S) 'url' or a Compute 'instance' plus 'port'",
			backendServiceName,
			strings.Join(unsupported, "; "),
		)
	}
	return backends, backendServiceName, nil
}

func (api *API) resolveHealthPathLocked(project string, references []interface{}) (string, error) {
	if len(references) == 0 {
		return "", nil
	}
	if len(references) > 1 {
		return "", fmt.Errorf("multiple health checks are unsupported")
	}
	name := resourceReferenceName(references[0])
	healthCheck := api.loadBalancers[loadBalancerKey(project, "healthChecks", name)]
	if healthCheck == nil {
		return "", fmt.Errorf("health check %q was not found", name)
	}
	for _, field := range []string{"httpHealthCheck", "httpsHealthCheck"} {
		config, ok := healthCheck[field].(map[string]interface{})
		if !ok {
			continue
		}
		if requestPath, _ := config["requestPath"].(string); requestPath != "" {
			if !strings.HasPrefix(requestPath, "/") {
				return "", fmt.Errorf("health check %q has an invalid requestPath", name)
			}
			return requestPath, nil
		}
		return "/", nil
	}
	return "", fmt.Errorf("health check %q uses an unsupported type; only HTTP(S) health checks are supported", name)
}

func (api *API) resolveLoadBalancerBackend(
	project string,
	rawBackend interface{},
	portName string,
	healthPath string,
) ([]loadBalancerBackend, error) {
	config, ok := rawBackend.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("configuration must be an object")
	}
	if rawURL, _ := config["url"].(string); rawURL != "" {
		target, err := url.Parse(rawURL)
		if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
			return nil, fmt.Errorf("'url' must be an absolute HTTP(S) URL")
		}
		return []loadBalancerBackend{{target: target, healthPath: healthPath, description: rawURL}}, nil
	}

	if groupReference, _ := config["group"].(string); groupReference != "" {
		groupProject, zone, groupName, ok := parseInstanceGroupReference(groupReference)
		if !ok || groupProject != project {
			return nil, fmt.Errorf("'group' must reference an unmanaged zonal instance group in project %q", project)
		}
		api.mu.RLock()
		group := cloneInstanceGroup(api.instanceGroups[instanceGroupKey(project, zone, groupName)])
		api.mu.RUnlock()
		if group == nil {
			return nil, fmt.Errorf("instance group %q was not found", groupName)
		}
		port := 0
		for _, namedPort := range group.NamedPorts {
			if namedPort.Name == portName || (portName == "" && namedPort.Name == "http") {
				port = namedPort.Port
				break
			}
		}
		if port == 0 {
			return nil, fmt.Errorf("instance group %q has no named port %q", groupName, portName)
		}
		resolved := make([]loadBalancerBackend, 0, len(group.Instances))
		for _, member := range group.Instances {
			instanceBackend, err := api.resolveInstanceBackend(project, member, port, healthPath)
			if err != nil {
				continue
			}
			resolved = append(resolved, instanceBackend)
		}
		if len(resolved) == 0 {
			return nil, fmt.Errorf("instance group %q has no running member with a host mapping for port %d", groupName, port)
		}
		return resolved, nil
	}

	instanceReference, _ := config["instance"].(string)
	port, err := numericBackendPort(config["port"])
	if instanceReference == "" || err != nil {
		return nil, fmt.Errorf("missing explicit 'url', unmanaged zonal 'group', or valid Compute 'instance' and 'port'")
	}
	backend, err := api.resolveInstanceBackend(project, instanceReference, port, healthPath)
	if err != nil {
		return nil, err
	}
	return []loadBalancerBackend{backend}, nil
}

func (api *API) resolveInstanceBackend(
	project string,
	instanceReference string,
	port int,
	healthPath string,
) (loadBalancerBackend, error) {
	instanceProject, zone, instanceName, ok := parseInstanceReference(instanceReference)
	if !ok || instanceProject != project {
		return loadBalancerBackend{}, fmt.Errorf("'instance' must reference an instance in project %q", project)
	}

	api.mu.RLock()
	instance := api.instances[instanceKey(project, zone, instanceName)]
	if instance == nil {
		api.mu.RUnlock()
		return loadBalancerBackend{}, fmt.Errorf("Compute instance %q was not found", instanceName)
	}
	status := instance.Status
	mappings := append([]orchestrator.PortMapping(nil), instance.HostPorts...)
	instanceCopy := instance.DeepCopy()
	api.mu.RUnlock()
	if status != "" && status != "RUNNING" {
		return loadBalancerBackend{}, fmt.Errorf("Compute instance %q is not running", instanceName)
	}
	if api.svcMgr != nil {
		if current := api.computeInstancePortMappings(project, zone, instanceCopy); len(current) > 0 {
			mappings = current
		}
	}
	for _, mapping := range mappings {
		if mapping.ContainerPort == strconv.Itoa(port) && mapping.HostPort != "" {
			target, _ := url.Parse("http://127.0.0.1:" + mapping.HostPort)
			return loadBalancerBackend{
				target:      target,
				healthPath:  healthPath,
				description: fmt.Sprintf("Compute instance %s port %d", instanceName, port),
			}, nil
		}
	}
	return loadBalancerBackend{}, fmt.Errorf("Compute instance %q has no host mapping for port %d", instanceName, port)
}

func (api *API) backendIsHealthy(r *http.Request, backend loadBalancerBackend) bool {
	if backend.healthPath == "" {
		return true
	}
	healthURL := *backend.target
	healthURL.Path = healthPathJoin(backend.target.Path, backend.healthPath)
	healthURL.RawQuery = ""
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, healthURL.String(), nil)
	if err != nil {
		return false
	}
	response, err := api.httpClient.Do(request)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	return response.StatusCode >= 200 && response.StatusCode < 400
}

func writeLoadBalancerUnavailable(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Load balancer unresolved: "+message)
}

func parseLoadBalancerProxyPath(path string) (string, string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i := 0; i+3 < len(parts); i++ {
		if parts[i] != "global" ||
			(parts[i+1] != "forwardingRules" && parts[i+1] != "globalForwardingRules") ||
			parts[i+2] == "" ||
			parts[i+3] != "proxy" {
			continue
		}
		project := ""
		for j := 0; j+1 < i; j++ {
			if parts[j] == "projects" {
				project = parts[j+1]
				break
			}
		}
		if project == "" {
			return "", "", "", false
		}
		proxyPath := "/"
		if len(parts) > i+4 {
			proxyPath += strings.Join(parts[i+4:], "/")
		}
		return project, parts[i+2], proxyPath, true
	}
	return "", "", "", false
}

func resourceReferenceName(value interface{}) string {
	reference, _ := value.(string)
	reference = strings.TrimRight(reference, "/")
	if reference == "" {
		return ""
	}
	if parsed, err := url.Parse(reference); err == nil {
		reference = parsed.Path
	}
	parts := strings.Split(strings.TrimRight(reference, "/"), "/")
	return parts[len(parts)-1]
}

func numericBackendPort(value interface{}) (int, error) {
	var port int
	switch typed := value.(type) {
	case float64:
		port = int(typed)
		if typed != float64(port) {
			return 0, fmt.Errorf("port must be an integer")
		}
	case string:
		parsed, err := strconv.Atoi(typed)
		if err != nil {
			return 0, err
		}
		port = parsed
	default:
		return 0, fmt.Errorf("port is required")
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("port must be between 1 and 65535")
	}
	return port, nil
}

func parseInstanceReference(reference string) (string, string, string, bool) {
	parsed, err := url.Parse(reference)
	if err != nil {
		return "", "", "", false
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	var project, zone, instance string
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "projects":
			project = parts[i+1]
		case "zones":
			zone = parts[i+1]
		case "instances":
			instance = parts[i+1]
		}
	}
	return project, zone, instance, project != "" && zone != "" && instance != ""
}

func parseInstanceGroupReference(reference string) (string, string, string, bool) {
	parsed, err := url.Parse(reference)
	if err != nil {
		return "", "", "", false
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	var project, zone, group string
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "projects":
			project = parts[i+1]
		case "zones":
			zone = parts[i+1]
		case "instanceGroups":
			group = parts[i+1]
		}
	}
	return project, zone, group, project != "" && zone != "" && group != ""
}

func curatedComputeBackend(sourceImage string) (string, []string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(sourceImage))
	parsed, err := url.Parse(normalized)
	if err == nil && parsed.Path != "" {
		normalized = strings.Trim(parsed.Path, "/")
	} else {
		normalized = strings.Trim(normalized, "/")
	}
	if strings.Contains(normalized, "projects/debian-cloud/global/images/") &&
		strings.Contains(normalized, "debian-12") {
		return "nginx:1.27-alpine", nil, true
	}
	return "", nil, false
}

func healthPathJoin(basePath, healthPath string) string {
	if basePath == "" || basePath == "/" {
		return healthPath
	}
	return strings.TrimRight(basePath, "/") + "/" + strings.TrimLeft(healthPath, "/")
}

func hasNonEmptyList(value interface{}) bool {
	items, ok := value.([]interface{})
	return ok && len(items) > 0
}

func (api *API) createLoadBalancerResource(
	w http.ResponseWriter,
	r *http.Request,
	project string,
	collection loadBalancerCollection,
) {
	var resource map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&resource); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Request body parse error: "+err.Error())
		return
	}
	name, _ := resource["name"].(string)
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Field 'name' is required")
		return
	}
	if collection.canonical == "urlMaps" &&
		(hasNonEmptyList(resource["hostRules"]) || hasNonEmptyList(resource["pathMatchers"])) {
		w.WriteHeader(http.StatusNotImplemented)
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Only URL map defaultService routing is supported; hostRules and pathMatchers are not implemented")
		return
	}

	selfLink := loadBalancerSelfLink(project, collection.canonical, name)
	key := loadBalancerKey(project, collection.canonical, name)
	resource["kind"] = collection.resourceKind
	resource["id"] = randomNumericID()
	resource["name"] = name
	resource["selfLink"] = selfLink
	resource["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)

	api.mu.Lock()
	if _, exists := api.loadBalancers[key]; exists {
		api.mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		writeError(w, http.StatusConflict, "ALREADY_EXISTS",
			fmt.Sprintf("%s %q already exists", collection.resourceKind, name))
		return
	}
	api.loadBalancers[key] = resource
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist load-balancer metadata: "+err.Error())
		return
	}

	op, err := api.opMgr.RegisterDurable("compute#operation", "insert", selfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
	api.opMgr.RunAsync(op.Name, func() error { return nil })
}

func (api *API) getLoadBalancerResource(
	w http.ResponseWriter,
	project string,
	collection loadBalancerCollection,
	name string,
) {
	key := loadBalancerKey(project, collection.canonical, name)
	api.mu.RLock()
	resource, ok := api.loadBalancers[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("%s %q not found", collection.resourceKind, name))
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resource)
}

func (api *API) listLoadBalancerResources(
	w http.ResponseWriter,
	project string,
	collection loadBalancerCollection,
) {
	prefix := loadBalancerKey(project, collection.canonical, "")
	api.mu.RLock()
	items := make([]map[string]interface{}, 0)
	for key, resource := range api.loadBalancers {
		if strings.HasPrefix(key, prefix) {
			items = append(items, resource)
		}
	}
	api.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i]["name"].(string) < items[j]["name"].(string)
	})

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":  collection.listKind,
		"id":    fmt.Sprintf("projects/%s/global/%s", project, collection.canonical),
		"items": items,
	})
}

func (api *API) deleteLoadBalancerResource(
	w http.ResponseWriter,
	project string,
	collection loadBalancerCollection,
	name string,
) {
	key := loadBalancerKey(project, collection.canonical, name)
	api.mu.Lock()
	_, ok := api.loadBalancers[key]
	if ok {
		delete(api.loadBalancers, key)
	}
	api.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("%s %q not found", collection.resourceKind, name))
		return
	}
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist load-balancer deletion: "+err.Error())
		return
	}

	selfLink := loadBalancerSelfLink(project, collection.canonical, name)
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete", selfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
	api.opMgr.RunAsync(op.Name, func() error { return nil })
}

func parseLoadBalancerPath(path string) (loadBalancerCollection, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		collection, ok := loadBalancerCollections[part]
		if !ok || i == 0 || parts[i-1] != "global" {
			continue
		}
		switch len(parts) - i {
		case 1:
			return collection, "", true
		case 2:
			if parts[i+1] != "" {
				return collection, parts[i+1], true
			}
		}
		return loadBalancerCollection{}, "", false
	}
	return loadBalancerCollection{}, "", false
}

func loadBalancerKey(project, collection, name string) string {
	return project + ":" + collection + ":" + name
}

func loadBalancerSelfLink(project, collection, name string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/%s/%s",
		project, collection, name)
}

// ─────────────────────────────────────────────────────────────────────────────
// Path parsing helpers
// ─────────────────────────────────────────────────────────────────────────────

// extractProject returns the project from a path like /compute/v1/projects/{project}/...
func extractProject(path string) string {
	return extractSegmentAfter(path, "projects")
}

// extractProjectZone returns (project, zone) from a zones-scoped path.
func extractProjectZone(path string) (string, string) {
	return extractSegmentAfter(path, "projects"), extractSegmentAfter(path, "zones")
}

func extractSegmentAfter(path, keyword string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == keyword && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func extractSegmentBefore(path, suffix string) string {
	path = strings.TrimSuffix(strings.TrimRight(path, "/"), suffix)
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	return parts[len(parts)-1]
}

// extractAfterInstances returns the instance name component (if present).
func extractAfterInstances(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "instances" && i+1 < len(parts) {
			name := parts[i+1]
			// Exclude action suffixes
			if name != "" && name != "start" && name != "stop" && name != "reset" {
				return name
			}
		}
	}
	return ""
}

// extractAfterGlobal returns the resource name after /global/{collection}/{name}.
func extractAfterGlobal(path, collection string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == collection && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func instanceKey(project, zone, name string) string {
	return project + ":" + zone + ":" + name
}

func instanceGroupKey(project, zone, name string) string {
	return project + ":" + zone + ":" + name
}

func instanceGroupSelfLink(project, zone, name string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instanceGroups/%s",
		project, zone, name)
}

func computeZoneSelfLink(project, zone string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s", project, zone)
}

func cloneInstanceGroup(group *InstanceGroup) *InstanceGroup {
	if group == nil {
		return nil
	}
	clone := *group
	clone.NamedPorts = append([]NamedPort(nil), group.NamedPorts...)
	clone.Instances = append([]string(nil), group.Instances...)
	return &clone
}

func selfLinkInstance(project, zone, name string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s",
		project, zone, name)
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"status":  status,
			"message": message,
		},
	})
}

func writeErrorStatus(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	writeError(w, code, status, message)
}

func writeComputeOperation(w http.ResponseWriter, project string, operation *orchestrator.Operation) {
	if operation == nil {
		_ = json.NewEncoder(w).Encode(nil)
		return
	}
	payload, _ := json.Marshal(operation)
	var response map[string]interface{}
	_ = json.Unmarshal(payload, &response)
	scope := "global"
	if operation.Zone != "" {
		scope = "zones/" + operation.Zone
		response["zone"] = computeZoneSelfLink(project, operation.Zone)
	} else if operation.Region != "" {
		scope = "regions/" + operation.Region
		response["region"] = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s", project, operation.Region)
	}
	response["selfLink"] = fmt.Sprintf(
		"https://www.googleapis.com/compute/v1/projects/%s/%s/operations/%s",
		project,
		scope,
		operation.Name,
	)
	_ = json.NewEncoder(w).Encode(response)
}

func randomNumericID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// ─────────────────────────────────────────────────────────────────────────────
// Firewall Rules
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeFirewalls(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	name := extractAfterGlobal(path, "firewalls")

	switch r.Method {
	case http.MethodPost:
		api.createFirewall(w, r, project)
	case http.MethodGet:
		if name != "" {
			api.getFirewall(w, project, name)
		} else {
			api.listFirewalls(w, project)
		}
	case http.MethodPatch, http.MethodPut:
		api.patchFirewall(w, r, project, name)
	case http.MethodDelete:
		api.deleteFirewall(w, project, name)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) createFirewall(w http.ResponseWriter, r *http.Request, project string) {
	var body FirewallRule
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if body.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "'name' is required")
		return
	}
	if body.Priority == 0 {
		body.Priority = 1000
	}
	if body.Direction == "" {
		body.Direction = "INGRESS"
	}
	if len(body.Allowed) > 0 {
		body.Action = "allow"
	} else if len(body.Denied) > 0 {
		body.Action = "deny"
	}
	if body.Network == "" {
		body.Network = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/default", project)
	}
	networkIdentity, firewallKey, err := parseCanonicalVPCNetwork(body.Network)
	if err != nil || networkIdentity.Project != project {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"firewall network must be a canonical network in the request project")
		return
	}
	body.Network = firewallKey
	body.Kind = "compute#firewall"
	body.ID = randomNumericID()
	body.SelfLink = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/firewalls/%s", project, body.Name)
	body.CreationTimestamp = time.Now().UTC().Format(time.RFC3339)

	key := project + ":" + body.Name
	api.mu.Lock()
	api.firewalls[key] = &body
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist firewall metadata: "+err.Error())
		return
	}

	if api.firewall == nil {
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"firewall backend is unavailable")
		return
	}
	api.firewall.RegisterFirewallRule(firewallKey, firewallEntryFromRule(&body))

	op, err := api.opMgr.RegisterDurable("compute#operation", "insert", body.SelfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error {
		return api.reapplyFirewallToVPC(body.Network)
	})
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) getFirewall(w http.ResponseWriter, project, name string) {
	key := project + ":" + name
	api.mu.RLock()
	fw, ok := api.firewalls[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Firewall "+name+" not found")
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(fw)
}

func (api *API) listFirewalls(w http.ResponseWriter, project string) {
	prefix := project + ":"
	api.mu.RLock()
	items := []*FirewallRule{}
	for k, v := range api.firewalls {
		if strings.HasPrefix(k, prefix) {
			items = append(items, v)
		}
	}
	api.mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":  "compute#firewallList",
		"items": items,
	})
}

func (api *API) patchFirewall(w http.ResponseWriter, r *http.Request, project, name string) {
	key := project + ":" + name
	api.mu.Lock()
	fw, ok := api.firewalls[key]
	if !ok {
		api.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Firewall "+name+" not found")
		return
	}
	var patch FirewallRule
	json.NewDecoder(r.Body).Decode(&patch)
	if len(patch.Allowed) > 0 {
		fw.Allowed = patch.Allowed
		fw.Denied = nil
		fw.Action = "allow"
	}
	if len(patch.Denied) > 0 {
		fw.Denied = patch.Denied
		fw.Allowed = nil
		fw.Action = "deny"
	}
	if len(patch.SourceRanges) > 0 {
		fw.SourceRanges = patch.SourceRanges
	}
	if patch.Description != "" {
		fw.Description = patch.Description
	}
	if patch.Priority != 0 {
		fw.Priority = patch.Priority
	}
	result := fw
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist firewall update: "+err.Error())
		return
	}
	if api.firewall == nil {
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"firewall backend is unavailable")
		return
	}
	api.firewall.RemoveFirewallRule(result.Network, result.Name)
	api.firewall.RegisterFirewallRule(result.Network, firewallEntryFromRule(result))

	op, err := api.opMgr.RegisterDurable("compute#operation", "patch", result.SelfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error {
		return api.reapplyFirewallToVPC(result.Network)
	})
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) deleteFirewall(w http.ResponseWriter, project, name string) {
	key := project + ":" + name
	api.mu.Lock()
	fw, ok := api.firewalls[key]
	networkURL := ""
	if ok {
		networkURL = fw.Network
		delete(api.firewalls, key)
	}
	api.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Firewall "+name+" not found")
		return
	}
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist firewall deletion: "+err.Error())
		return
	}
	if api.firewall == nil {
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"firewall backend is unavailable")
		return
	}
	api.firewall.RemoveFirewallRule(networkURL, name)
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete", "", "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error {
		return api.reapplyFirewallToVPC(networkURL)
	})
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

// ─────────────────────────────────────────────────────────────────────────────
// Firewall/VPC Helpers
// ─────────────────────────────────────────────────────────────────────────────

func extractNameFromURL(urlStr string) string {
	parts := strings.Split(urlStr, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

func (api *API) getAllowedPortsForVPC(vpcName string) []string {
	api.mu.RLock()
	defer api.mu.RUnlock()
	ports := []string{}
	for _, rule := range api.firewalls {
		if rule.Network == vpcName && rule.Direction == "INGRESS" && rule.Action == "allow" {
			for _, allowed := range rule.Allowed {
				for _, p := range allowed.Ports {
					ports = append(ports, p)
				}
			}
		}
	}
	return ports
}

func (api *API) reapplyFirewallToVPC(networkURL string) error {
	firewallKey, dockerVPC, identities, osImages, err := api.firewallTargetsForNetwork(networkURL)
	if err != nil {
		return err
	}
	if len(identities) == 0 {
		return nil
	}
	if api.firewall == nil {
		return errors.New("firewall backend is unavailable")
	}
	return api.firewall.ApplyFirewallPortsToComputeInstances(
		firewallKey,
		dockerVPC,
		identities,
		osImages,
	)
}

func (api *API) firewallTargetsForNetwork(
	networkURL string,
) (string, string, []orchestrator.ComputeInstanceIdentity, []string, error) {
	networkIdentity, firewallKey, err := parseCanonicalVPCNetwork(networkURL)
	if err != nil {
		return "", "", nil, nil, err
	}
	dockerVPC := "default"
	if networkIdentity.Network != "default" {
		dockerVPC, err = networkIdentity.DockerName()
		if err != nil {
			return "", "", nil, nil, err
		}
	}
	var identities []orchestrator.ComputeInstanceIdentity
	var osImages []string

	api.mu.RLock()
	for _, inst := range api.instances {
		if inst != nil && inst.project == networkIdentity.Project &&
			len(inst.NetworkInterfaces) > 0 &&
			inst.NetworkInterfaces[0].Network == firewallKey {
			if inst.Labels != nil && inst.Labels["managed-by"] == "gke" {
				continue
			}
			identity := orchestratorComputeIdentity(inst.project, inst.zone, inst.Name)
			if err := identity.Validate(); err != nil {
				continue
			}
			identities = append(identities, identity)
			img := "ubuntu:latest"
			for _, d := range inst.Disks {
				if strings.Contains(strings.ToLower(d.Source), "centos") {
					img = "centos:latest"
				}
			}
			osImages = append(osImages, img)
		}
	}
	api.mu.RUnlock()
	return firewallKey, dockerVPC, identities, osImages, nil
}

func parseCanonicalVPCNetwork(value string) (orchestrator.VPCNetworkIdentity, string, error) {
	const marker = "https://www.googleapis.com/compute/v1/projects/"
	if !strings.HasPrefix(value, marker) {
		return orchestrator.VPCNetworkIdentity{}, "", errors.New("network is not canonical")
	}
	parts := strings.Split(strings.TrimPrefix(value, marker), "/")
	if len(parts) != 4 || parts[1] != "global" || parts[2] != "networks" {
		return orchestrator.VPCNetworkIdentity{}, "", errors.New("network is not canonical")
	}
	identity := orchestrator.VPCNetworkIdentity{Project: parts[0], Network: parts[3]}
	if err := identity.Validate(); err != nil {
		return orchestrator.VPCNetworkIdentity{}, "", err
	}
	return identity, networkSelfLink(identity.Project, identity.Network), nil
}

func firewallEntryFromRule(rule *FirewallRule) orchestrator.FirewallEntry {
	entry := orchestrator.FirewallEntry{
		Name: rule.Name, VpcName: extractNameFromURL(rule.Network),
		Direction: rule.Direction, Action: rule.Action, Protocol: "all",
		Ranges: append(append([]string{}, rule.SourceRanges...), rule.DestinationRanges...),
	}
	for _, allowed := range rule.Allowed {
		entry.Ports = append(entry.Ports, allowed.Ports...)
	}
	return entry
}
