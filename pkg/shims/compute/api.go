package compute

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
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
	state.MustRegisterEntryValidator(computeStateEntry, state.StrictEntryValidator[computeMetadata](nil))
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
	Kind              string         `json:"kind"`
	Name              string         `json:"name"`
	Network           string         `json:"network"`
	NetworkIP         string         `json:"networkIP"`
	Subnetwork        string         `json:"subnetwork,omitempty"`
	AccessConfigs     []AccessConfig `json:"accessConfigs,omitempty"`
	StackType         string         `json:"stackType,omitempty"`
	IPv6AccessConfigs []AccessConfig `json:"ipv6AccessConfigs,omitempty"`
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

type computeNetworkBackend interface {
	ProvisionComputeInstanceOnVPC(
		context.Context,
		orchestrator.ComputeInstanceIdentity,
		string,
		orchestrator.ComputeInstanceNetwork,
		[]string,
		[]string,
		[]string,
	) (orchestrator.ComputeInstanceRuntime, error)
	ReconcileComputeInstanceOnVPC(
		context.Context,
		orchestrator.ComputeInstanceIdentity,
		orchestrator.ComputeInstanceNetwork,
	) (orchestrator.ComputeInstanceRuntime, bool, error)
	DeleteComputeInstance(context.Context, orchestrator.ComputeInstanceIdentity) error
}

const defaultComputeDeleteTimeout = 30 * time.Second

const (
	maxLoadBalancerCacheEntries = 64
	maxLoadBalancerCacheBytes   = 1 << 20
	maxLoadBalancerHeaderBytes  = 32 << 10
	maxLoadBalancerCacheTTL     = 60 * time.Second
)

type loadBalancerCacheKey struct {
	project        string
	backendService string
	policyName     string
	policyVersion  string
	method         string
	host           string
	uri            string
}

type loadBalancerCacheEntry struct {
	status    int
	header    http.Header
	body      []byte
	expiresAt time.Time
	urlMap    string
	path      string
	host      string
}

type API struct {
	mu                   sync.RWMutex
	persistMu            sync.Mutex
	initMu               sync.RWMutex
	cacheMu              sync.Mutex
	opMgr                *orchestrator.OperationManager
	svcMgr               *orchestrator.ServiceManager
	vpcIPAM              vpcIPAMBackend
	legacyVM             legacyVMBackend
	legacyVPC            legacyVPCBackend
	firewall             firewallBackend
	computeNetwork       computeNetworkBackend
	networkAuthorizer    networkHTTPAuthorizer
	serviceMeshRouter    serviceMeshHTTPRouter
	initializationErr    error
	stateStore           computeMetadataStore
	instances            map[string]*Instance   // key: project+":"+zone+":"+name
	networks             map[string]*Network    // key: project+":"+name
	subnetworks          map[string]*Subnetwork // key: project+":"+region+":"+name
	nextSubnetworkID     uint64
	securityPolicies     map[string]*SecurityPolicy // key: project+":"+name
	firewalls            map[string]*FirewallRule   // key: project+":"+name
	instanceGroups       map[string]*InstanceGroup  // key: project+":"+zone+":"+name
	loadBalancers        map[string]map[string]interface{}
	roundRobin           map[string]uint64
	loadBalancerCache    map[loadBalancerCacheKey]loadBalancerCacheEntry
	cacheClearedAt       time.Time
	cacheInvalidatedAt   map[string]time.Time
	policyInvalidatedAt  map[string]time.Time
	httpClient           *http.Client
	computeDeleteTimeout time.Duration
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
	if err := api.initializationError(); err != nil {
		log.Printf("[Shim: Compute Engine] persisted metadata validation failed: %v", err)
		return api
	}
	if err := api.ReconcileVPCIPAM(context.Background()); err != nil {
		log.Printf("[Shim: Compute Engine] VPC IPAM reconciliation failed: %v", err)
		api.setInitializationError(err)
		return api
	}
	if err := api.ReconcileComputeInstances(context.Background()); err != nil {
		log.Printf("[Shim: Compute Engine] Compute instance reconciliation failed: %v", err)
		api.setInitializationError(err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager, store computeMetadataStore) *API {
	api := &API{
		opMgr:                opMgr,
		svcMgr:               svcMgr,
		stateStore:           store,
		instances:            make(map[string]*Instance),
		networks:             make(map[string]*Network),
		subnetworks:          make(map[string]*Subnetwork),
		nextSubnetworkID:     1,
		securityPolicies:     make(map[string]*SecurityPolicy),
		firewalls:            make(map[string]*FirewallRule),
		instanceGroups:       make(map[string]*InstanceGroup),
		loadBalancers:        make(map[string]map[string]interface{}),
		roundRobin:           make(map[string]uint64),
		loadBalancerCache:    make(map[loadBalancerCacheKey]loadBalancerCacheEntry),
		cacheInvalidatedAt:   make(map[string]time.Time),
		policyInvalidatedAt:  make(map[string]time.Time),
		httpClient:           &http.Client{Timeout: 2 * time.Second},
		computeDeleteTimeout: defaultComputeDeleteTimeout,
	}
	if svcMgr != nil {
		api.vpcIPAM = svcMgr
		api.legacyVM = svcMgr
		api.legacyVPC = svcMgr
		api.firewall = svcMgr
		api.computeNetwork = svcMgr
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
	if api.initializationError() != nil && !strings.Contains(path, "/operations/") {
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
		if errors.Is(err, errUnsupportedAutoNetwork) ||
			errors.Is(err, errUnsupportedMultipleNICs) ||
			errors.Is(err, errUnsupportedNetworkNIC) {
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

	// Drive state machine asynchronously: PROVISIONING → RUNNING.
	opName := op.Name
	operationContext := context.WithoutCancel(r.Context())
	api.opMgr.RunAsync(opName, func() error {
		api.mu.Lock()
		if i, ok := api.instances[key]; ok {
			i.Status = "STAGING"
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			return fmt.Errorf("persist staging instance: %w", err)
		}

		if isGKE {
			api.mu.Lock()
			if i, ok := api.instances[key]; ok {
				i.Status = "RUNNING"
			}
			api.mu.Unlock()
			return api.persistMetadata()
		}

		api.mu.Lock()
		if i, ok := api.instances[key]; ok {
			i.Status = "PROVISIONING"
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			return fmt.Errorf("persist provisioning instance: %w", err)
		}

		var interfaces []NetworkInterface
		api.mu.RLock()
		if i, ok := api.instances[key]; ok {
			interfaces = append(interfaces, i.NetworkInterfaces...)
		}
		api.mu.RUnlock()
		identity := orchestratorComputeIdentity(project, zone, name)
		attachment, customNetwork, attachmentErr := api.computeInstanceNetworkAttachment(project, interfaces)
		if attachmentErr != nil {
			return api.rollbackFailedInstanceProvision(key, inst, attachmentErr)
		}
		var provisionErr error
		networkIP := ""
		if customNetwork {
			if api.computeNetwork == nil {
				provisionErr = errors.New("Compute custom-network backend is unavailable")
			} else {
				allowedPorts := api.getAllowedPortsForVPC(networkSelfLink(project, attachment.VPC.Network))
				provisionCtx, cancel := context.WithTimeout(operationContext, 3*time.Minute)
				runtime, err := api.computeNetwork.ProvisionComputeInstanceOnVPC(
					provisionCtx,
					identity,
					osImage,
					attachment,
					allowedPorts,
					nil,
					dockerCommand,
				)
				cancel()
				provisionErr = err
				networkIP = runtime.IPAddress
				if provisionErr == nil && networkIP == "" {
					provisionErr = errors.New("Compute backend did not return a truthful primary IPv4 address")
				}
			}
		} else if api.svcMgr == nil {
			provisionErr = errors.New("Compute backend is unavailable")
		} else {
			logicalVPC, dockerVPC, nameErr := resolvedInstanceVPCDockerNetwork(project, interfaces)
			if nameErr != nil {
				provisionErr = nameErr
			} else {
				provisionErr = api.svcMgr.ProvisionComputeInstance(
					operationContext,
					identity,
					osImage,
					dockerVPC,
					api.getAllowedPortsForVPC(logicalVPC),
					nil,
					dockerCommand,
				)
			}
		}
		if provisionErr != nil {
			return api.rollbackFailedInstanceProvision(key, inst, provisionErr)
		}

		api.mu.Lock()
		var runningSnapshot *Instance
		if i, ok := api.instances[key]; ok {
			if customNetwork && len(i.NetworkInterfaces) == 1 {
				i.NetworkInterfaces[0].NetworkIP = networkIP
			}
			i.Status = "RUNNING"
			runningSnapshot = i.DeepCopy()
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			return api.reconcileRunningInstanceSaveFailure(
				operationContext,
				key,
				inst,
				runningSnapshot,
				identity,
				customNetwork,
				attachment,
				err,
			)
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

	deleteBaseContext := context.WithoutCancel(r.Context())
	api.opMgr.RunAsync(op.Name, func() error {
		deleteContext, cancelDelete := context.WithTimeout(deleteBaseContext, api.computeDeleteTimeout)
		defer cancelDelete()
		if isGKE {
			if api.svcMgr != nil {
				if err := api.svcMgr.DeleteComputeVMContext(deleteContext, containerName); err != nil {
					return api.restoreInstanceAfterDeleteFailure(key, inst, err)
				}
			}
		} else {
			if api.computeNetwork == nil {
				return api.restoreInstanceAfterDeleteFailure(
					key,
					inst,
					errors.New("Compute deletion backend is unavailable"),
				)
			}
			if err := api.computeNetwork.DeleteComputeInstance(
				deleteContext,
				orchestratorComputeIdentity(project, zone, name),
			); err != nil {
				return api.restoreInstanceAfterDeleteFailure(key, inst, err)
			}
		}

		legacyCleanup := api.removeInstanceAndLegacyCleanupEligibility(key, name)
		if !isGKE {
			api.cleanupLegacyComputeVM(name, legacyCleanup)
		}
		if err := api.persistMetadata(); err != nil {
			return api.reconcileDeletedInstanceSaveFailure(key, err)
		}
		return nil
	})

	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) restoreInstanceAfterDeleteFailure(key string, instance *Instance, cause error) error {
	api.mu.Lock()
	if current := api.instances[key]; current == instance {
		current.Status = "RUNNING"
	}
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		combined := fmt.Errorf("delete Compute instance: %w; restore metadata: %v", cause, err)
		api.setInitializationError(combined)
		return combined
	}
	return cause
}

func (api *API) reconcileDeletedInstanceSaveFailure(key string, saveErr error) error {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	if absent, _ := api.persistedInstanceMatches(key, nil); absent {
		return nil
	}

	api.mu.RLock()
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.RUnlock()
	retryErr := snapshotErr
	if retryErr == nil {
		retryErr = api.saveMetadataPayload(payload)
	}
	if retryErr == nil {
		return nil
	}
	if absent, _ := api.persistedInstanceMatches(key, nil); absent {
		return nil
	}

	combined := fmt.Errorf(
		"persist deleted instance: %w; retry absent metadata: %v",
		saveErr,
		retryErr,
	)
	api.setInitializationError(combined)
	return combined
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

func (api *API) rollbackFailedInstanceProvision(key string, instance *Instance, cause error) error {
	log.Printf("[Shim: Compute Engine] instance provisioning failed for %s: %v", key, cause)
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.Lock()
	current := api.instances[key]
	if current == instance {
		delete(api.instances, key)
	}
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.Unlock()
	if snapshotErr == nil {
		snapshotErr = api.saveMetadataPayload(payload)
	}
	if snapshotErr != nil {
		api.mu.Lock()
		if api.instances[key] == nil {
			api.instances[key] = instance
		}
		api.mu.Unlock()
		combined := fmt.Errorf("provision Compute instance: %w; rollback metadata: %v", cause, snapshotErr)
		api.setInitializationError(combined)
		return combined
	}
	return cause
}

func (api *API) reconcileRunningInstanceSaveFailure(
	baseContext context.Context,
	key string,
	instance *Instance,
	expected *Instance,
	identity orchestrator.ComputeInstanceIdentity,
	customNetwork bool,
	attachment orchestrator.ComputeInstanceNetwork,
	saveErr error,
) error {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	if committed, _ := api.persistedInstanceMatches(key, expected); committed {
		return nil
	}

	cleanupContext, cancelCleanup := context.WithTimeout(
		context.WithoutCancel(baseContext),
		api.computeDeleteTimeout,
	)
	defer cancelCleanup()
	var cleanupErr error
	if customNetwork {
		if api.computeNetwork == nil {
			cleanupErr = errors.New("Compute custom-network cleanup backend is unavailable")
		} else {
			cleanupErr = api.computeNetwork.DeleteComputeInstance(cleanupContext, identity)
		}
	} else if api.svcMgr == nil {
		cleanupErr = errors.New("Compute cleanup backend is unavailable")
	} else {
		containerName, nameErr := identity.DockerName()
		if nameErr != nil {
			cleanupErr = nameErr
		} else {
			cleanupErr = api.svcMgr.DeleteComputeVMContext(cleanupContext, containerName)
		}
	}

	if cleanupErr != nil {
		runtimeConfirmed := false
		var reconcileErr error
		if customNetwork && api.computeNetwork != nil {
			reconcileContext, cancelReconcile := context.WithTimeout(
				context.WithoutCancel(baseContext),
				api.computeDeleteTimeout,
			)
			runtime, found, err := api.computeNetwork.ReconcileComputeInstanceOnVPC(
				reconcileContext,
				identity,
				attachment,
			)
			cancelReconcile()
			reconcileErr = err
			expectedIP := ""
			if expected != nil && len(expected.NetworkInterfaces) == 1 {
				expectedIP = expected.NetworkInterfaces[0].NetworkIP
			}
			runtimeConfirmed = err == nil && found && runtime.Status == "running" &&
				runtime.IPAddress != "" && runtime.IPAddress == expectedIP
			if err == nil && !runtimeConfirmed {
				reconcileErr = errors.New("exact owned running container and primary IPv4 were not confirmed")
			}
		}
		if !runtimeConfirmed {
			combined := fmt.Errorf(
				"persist running instance: %w; cleanup exact owned container: %v; "+
					"runtime ownership reconciliation failed: %v",
				saveErr,
				cleanupErr,
				reconcileErr,
			)
			api.setInitializationError(combined)
			return combined
		}
		api.mu.RLock()
		payload, snapshotErr := api.marshalMetadataLocked()
		api.mu.RUnlock()
		retryErr := snapshotErr
		if retryErr == nil {
			retryErr = api.saveMetadataPayload(payload)
		}
		if retryErr == nil {
			return nil
		}
		if committed, _ := api.persistedInstanceMatches(key, expected); committed {
			return nil
		}
		combined := fmt.Errorf(
			"persist running instance: %w; cleanup exact owned container: %v; retry metadata: %v",
			saveErr,
			cleanupErr,
			retryErr,
		)
		api.setInitializationError(combined)
		return combined
	}

	api.mu.Lock()
	if api.instances[key] == instance {
		delete(api.instances, key)
	}
	payload, snapshotErr := api.marshalMetadataLocked()
	api.mu.Unlock()
	rollbackErr := snapshotErr
	if rollbackErr == nil {
		rollbackErr = api.saveMetadataPayload(payload)
	}
	if rollbackErr != nil {
		if absent, _ := api.persistedInstanceMatches(key, nil); !absent {
			combined := fmt.Errorf(
				"persist running instance: %w; compensate metadata: %v",
				saveErr,
				rollbackErr,
			)
			api.setInitializationError(combined)
			return combined
		}
	}
	return fmt.Errorf("persist running instance: %w; exact owned container was removed", saveErr)
}

func (api *API) persistedInstanceMatches(key string, expected *Instance) (bool, error) {
	if api.stateStore == nil {
		return expected == nil, nil
	}
	var persisted computeMetadata
	if err := api.stateStore.Load(computeStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return expected == nil, nil
		}
		return false, err
	}
	actual := persisted.Instances[key]
	if actual == nil || expected == nil {
		return actual == nil && expected == nil, nil
	}
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		return false, err
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return false, err
	}
	return string(actualJSON) == string(expectedJSON), nil
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
		op, err := api.opMgr.GetScoped(parts[7], orchestrator.OperationScope{
			ServiceKind: "compute#operation",
			Project:     parts[3],
			Location:    parts[5],
		})
		targetProjectPrefix := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/", parts[3])
		if err != nil || op.Kind != "compute#operation" || op.Region != parts[5] ||
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

	targetProjectPrefix := fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/", project)
	requestedZone := extractSegmentAfter(path, "zones")
	location := requestedZone
	op, err := api.opMgr.GetScoped(opName, orchestrator.OperationScope{
		ServiceKind: "compute#operation",
		Project:     project,
		Location:    location,
	})
	requestedScopeValid := false
	switch {
	case strings.Contains(path, "/global/operations/"):
		requestedScopeValid = err == nil && op.Zone == "" && op.Region == "" && op.Location == ""
	case requestedZone != "":
		requestedScopeValid = err == nil && op.Zone == requestedZone && op.Region == ""
	}
	if err != nil || op.Kind != "compute#operation" || !requestedScopeValid ||
		!strings.HasPrefix(op.TargetLink, targetProjectPrefix) {
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

func (api *API) updateSecurityPolicy(w http.ResponseWriter, r *http.Request, project, name string) {
	var patch struct {
		Description *string              `json:"description"`
		Rules       []SecurityPolicyRule `json:"rules"`
	}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if patch.Description == nil && patch.Rules == nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "At least one mutable field is required")
		return
	}

	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	key := project + ":" + name
	api.mu.RLock()
	updated := cloneSecurityPolicy(api.securityPolicies[key])
	api.mu.RUnlock()
	if updated == nil {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "SecurityPolicy "+name+" not found")
		return
	}
	if patch.Description != nil {
		updated.Description = *patch.Description
	}
	if patch.Rules != nil {
		updated.Rules = cloneSecurityPolicyRules(patch.Rules)
		if err := validateSecurityPolicyRules(updated.Rules); err != nil {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}

	op, err := api.opMgr.RegisterDurable("compute#operation", "patch", updated.SelfLink, "", "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	previous, err := api.metadataSnapshot()
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("snapshot security-policy metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	next, err := cloneComputeMetadata(previous)
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("clone security-policy metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	next.SecurityPolicies[key] = cloneSecurityPolicy(updated)
	if err := api.saveMetadataTransaction(previous, next); err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("persist security-policy update: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	if err := api.opMgr.FinalizeDurable(op.Name, 0, ""); err != nil {
		if errors.Is(err, orchestrator.ErrOperationTerminalConflict) {
			message := api.failClosedControlPlaneConflict(err)
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		message := api.compensateControlPlaneCommit(previous, next, err)
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	api.mu.Lock()
	api.securityPolicies[key] = cloneSecurityPolicy(updated)
	api.mu.Unlock()
	api.invalidateLoadBalancerCacheForPolicy(project, name)
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, api.opMgr.Get(op.Name))
}

// ─────────────────────────────────────────────────────────────────────────────
// Security Policies (Cloud Armor)
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeSecurityPolicies(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	name := extractAfterGlobal(path, "securityPolicies")

	switch r.Method {
	case http.MethodPost:
		if name != "" {
			writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Unsupported security-policy route")
			return
		}
		var body struct {
			Name        string               `json:"name"`
			Description string               `json:"description"`
			Rules       []SecurityPolicyRule `json:"rules"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
			return
		}
		if !gceResourceName.MatchString(body.Name) {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "A valid security policy name is required")
			return
		}

		// Always add a default allow-all rule at priority 2147483647 (GCP convention)
		rules := cloneSecurityPolicyRules(body.Rules)
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
		if err := validateSecurityPolicyRules(rules); err != nil {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}

		sp := &SecurityPolicy{
			Kind:              "compute#securityPolicy",
			ID:                randomNumericID(),
			Name:              body.Name,
			Description:       body.Description,
			Rules:             rules,
			SelfLink:          securityPolicySelfLink(project, body.Name),
			CreationTimestamp: time.Now().UTC().Format(time.RFC3339),
		}
		key := project + ":" + body.Name
		api.persistMu.Lock()
		defer api.persistMu.Unlock()
		api.mu.RLock()
		_, exists := api.securityPolicies[key]
		api.mu.RUnlock()
		if exists {
			writeErrorStatus(w, http.StatusConflict, "ALREADY_EXISTS", "SecurityPolicy "+body.Name+" already exists")
			return
		}
		op, err := api.opMgr.RegisterDurable("compute#operation", "insert", sp.SelfLink, "", "")
		if err != nil {
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
			return
		}
		previous, err := api.metadataSnapshot()
		if err != nil {
			message := api.failControlPlaneOperation(op.Name, fmt.Errorf("snapshot security-policy metadata: %w", err))
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		next, err := cloneComputeMetadata(previous)
		if err != nil {
			message := api.failControlPlaneOperation(op.Name, fmt.Errorf("clone security-policy metadata: %w", err))
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		if next.SecurityPolicies == nil {
			next.SecurityPolicies = make(map[string]*SecurityPolicy)
		}
		next.SecurityPolicies[key] = cloneSecurityPolicy(sp)
		if err := api.saveMetadataTransaction(previous, next); err != nil {
			message := api.failControlPlaneOperation(op.Name, fmt.Errorf("persist security-policy metadata: %w", err))
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		if err := api.opMgr.FinalizeDurable(op.Name, 0, ""); err != nil {
			if errors.Is(err, orchestrator.ErrOperationTerminalConflict) {
				message := api.failClosedControlPlaneConflict(err)
				writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
				return
			}
			message := api.compensateControlPlaneCommit(previous, next, err)
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		api.mu.Lock()
		api.securityPolicies[key] = cloneSecurityPolicy(sp)
		api.mu.Unlock()
		op = api.opMgr.Get(op.Name)
		w.WriteHeader(http.StatusOK)
		writeComputeOperation(w, project, op)

	case http.MethodGet:
		if name != "" {
			key := project + ":" + name
			api.mu.RLock()
			sp := cloneSecurityPolicy(api.securityPolicies[key])
			api.mu.RUnlock()
			if sp == nil {
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
					items = append(items, cloneSecurityPolicy(v))
				}
			}
			api.mu.RUnlock()
			sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"kind":  "compute#securityPolicyList",
				"items": items,
			})
		}

	case http.MethodPatch:
		if name == "" {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "A security policy name is required")
			return
		}
		api.updateSecurityPolicy(w, r, project, name)

	case http.MethodDelete:
		if name == "" {
			writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "A security policy name is required")
			return
		}
		api.deleteSecurityPolicy(w, project, name)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) deleteSecurityPolicy(w http.ResponseWriter, project, name string) {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	key := project + ":" + name
	api.mu.RLock()
	policy := cloneSecurityPolicy(api.securityPolicies[key])
	inUse := false
	for lbKey, resource := range api.loadBalancers {
		if !strings.HasPrefix(lbKey, project+":backendServices:") || resource == nil {
			continue
		}
		reference, _ := resource["securityPolicy"].(string)
		referencedName, err := securityPolicyReferenceName(reference, project)
		if err == nil && referencedName == name {
			inUse = true
			break
		}
	}
	api.mu.RUnlock()
	if policy == nil {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "SecurityPolicy "+name+" not found")
		return
	}
	if inUse {
		writeErrorStatus(w, http.StatusBadRequest, "FAILED_PRECONDITION",
			"SecurityPolicy "+name+" is referenced by a backend service")
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete", policy.SelfLink, "", "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	previous, err := api.metadataSnapshot()
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("snapshot security-policy metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	next, err := cloneComputeMetadata(previous)
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("clone security-policy metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	delete(next.SecurityPolicies, key)
	if err := api.saveMetadataTransaction(previous, next); err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("persist security-policy deletion: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	if err := api.opMgr.FinalizeDurable(op.Name, 0, ""); err != nil {
		if errors.Is(err, orchestrator.ErrOperationTerminalConflict) {
			message := api.failClosedControlPlaneConflict(err)
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		message := api.compensateControlPlaneCommit(previous, next, err)
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	api.mu.Lock()
	delete(api.securityPolicies, key)
	api.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, api.opMgr.Get(op.Name))
}

func securityPolicySelfLink(project, name string) string {
	return fmt.Sprintf(
		"https://www.googleapis.com/compute/v1/projects/%s/global/securityPolicies/%s",
		project,
		name,
	)
}

func securityPolicyReferenceName(reference, project string) (string, error) {
	if reference == "" {
		return "", errors.New("reference is required")
	}
	if !strings.Contains(reference, "/") {
		if !gceResourceName.MatchString(reference) {
			return "", errors.New("reference has an invalid policy name")
		}
		return reference, nil
	}
	parsed, err := url.Parse(reference)
	if err != nil {
		return "", errors.New("reference is invalid")
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	for index := range parts {
		if parts[index] != "projects" || index+4 >= len(parts) {
			continue
		}
		if parts[index+1] != project || parts[index+2] != "global" ||
			parts[index+3] != "securityPolicies" || parts[index+4] == "" ||
			index+5 != len(parts) {
			return "", errors.New("reference must target a security policy in the request project")
		}
		return parts[index+4], nil
	}
	return "", errors.New("reference must be a policy name or canonical self link")
}

func validateSecurityPolicyRules(rules []SecurityPolicyRule) error {
	if len(rules) == 0 || len(rules) > 128 {
		return errors.New("security policy requires between 1 and 128 rules")
	}
	priorities := make(map[int]struct{}, len(rules))
	hasDefault := false
	for _, rule := range rules {
		if rule.Priority < 0 {
			return errors.New("security policy rule priority must be non-negative")
		}
		if _, exists := priorities[rule.Priority]; exists {
			return fmt.Errorf("security policy rule priority %d is duplicated", rule.Priority)
		}
		priorities[rule.Priority] = struct{}{}
		if _, err := securityPolicyAction(rule.Action); err != nil {
			return fmt.Errorf("security policy rule priority %d: %w", rule.Priority, err)
		}
		if rule.Match != nil {
			if rule.Match.VersionedExpr != "SRC_IPS_V1" || rule.Match.Config == nil ||
				len(rule.Match.Config.SrcIPRanges) == 0 || len(rule.Match.Config.SrcIPRanges) > 64 {
				return fmt.Errorf("security policy rule priority %d uses an unsupported match", rule.Priority)
			}
			for _, value := range rule.Match.Config.SrcIPRanges {
				if _, err := netip.ParsePrefix(value); err != nil {
					return fmt.Errorf("security policy rule priority %d has invalid source range %q", rule.Priority, value)
				}
			}
		}
		if rule.Priority == 2147483647 {
			if rule.Match != nil {
				return errors.New("security policy default rule must match all requests")
			}
			hasDefault = true
		}
	}
	if !hasDefault {
		return errors.New("security policy requires a default rule at priority 2147483647")
	}
	return nil
}

func securityPolicyAction(action string) (int, error) {
	if action == "allow" {
		return 0, nil
	}
	if strings.HasPrefix(action, "deny(") && strings.HasSuffix(action, ")") {
		code, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(action, "deny("), ")"))
		if err == nil && (code == http.StatusForbidden || code == http.StatusNotFound || code == http.StatusBadGateway) {
			return code, nil
		}
	}
	return 0, fmt.Errorf("unsupported action %q; supported actions are allow, deny(403), deny(404), and deny(502)", action)
}

func cloneSecurityPolicy(policy *SecurityPolicy) *SecurityPolicy {
	if policy == nil {
		return nil
	}
	clone := *policy
	clone.Rules = cloneSecurityPolicyRules(policy.Rules)
	return &clone
}

func cloneSecurityPolicyRules(rules []SecurityPolicyRule) []SecurityPolicyRule {
	if rules == nil {
		return nil
	}
	clones := make([]SecurityPolicyRule, len(rules))
	for index := range rules {
		clones[index] = rules[index]
		if rules[index].Match != nil {
			match := *rules[index].Match
			if match.Config != nil {
				config := *match.Config
				config.SrcIPRanges = append([]string(nil), match.Config.SrcIPRanges...)
				match.Config = &config
			}
			clones[index].Match = &match
		}
	}
	return clones
}

// ─────────────────────────────────────────────────────────────────────────────
// Load Balancer metadata resources
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeLoadBalancer(w http.ResponseWriter, r *http.Request, path string) {
	project := extractProject(path)
	if strings.HasSuffix(path, "/invalidateCache") {
		api.invalidateLoadBalancerCache(w, r, project, path)
		return
	}
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

func (api *API) invalidateLoadBalancerCache(
	w http.ResponseWriter,
	r *http.Request,
	project string,
	requestPath string,
) {
	if r.Method != http.MethodPost {
		writeErrorStatus(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Cache invalidation requires POST")
		return
	}
	parts := strings.Split(strings.Trim(requestPath, "/"), "/")
	if len(parts) != 8 || parts[0] != "compute" || parts[1] != "v1" ||
		parts[2] != "projects" || parts[3] != project || parts[4] != "global" ||
		parts[5] != "urlMaps" || parts[6] == "" || parts[7] != "invalidateCache" {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "Unsupported cache invalidation route")
		return
	}
	var request struct {
		Path string `json:"path"`
		Host string `json:"host"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if !supportedCacheInvalidationPath(request.Path) ||
		(request.Host != "" && !supportedCacheInvalidationHost(request.Host)) {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"Only an absolute path with an optional trailing wildcard and an optional exact host are supported")
		return
	}
	urlMapName := parts[6]
	api.mu.RLock()
	urlMap := api.loadBalancers[loadBalancerKey(project, "urlMaps", urlMapName)]
	api.mu.RUnlock()
	if urlMap == nil {
		writeErrorStatus(w, http.StatusNotFound, "NOT_FOUND", "URL map "+urlMapName+" not found")
		return
	}
	target := loadBalancerSelfLink(project, "urlMaps", urlMapName)
	op, err := api.opMgr.RegisterDurable("compute#operation", "invalidateCache", target, "", "")
	if err != nil {
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.cacheMu.Lock()
	api.cacheInvalidatedAt[project+":"+urlMapName] = time.Now()
	for key, entry := range api.loadBalancerCache {
		if key.project == project && entry.urlMap == urlMapName &&
			(request.Host == "" || entry.host == normalizedRequestHost(request.Host)) &&
			cacheInvalidationPathMatches(request.Path, entry.path) {
			delete(api.loadBalancerCache, key)
		}
	}
	api.cacheMu.Unlock()
	if err := api.opMgr.FinalizeDurable(op.Name, 0, ""); err != nil {
		api.setInitializationError(err)
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, api.opMgr.Get(op.Name))
}

func supportedCacheInvalidationPath(path string) bool {
	return strings.HasPrefix(path, "/") && !strings.ContainsAny(path, "?#") &&
		!strings.Contains(path[:max(0, len(path)-1)], "*")
}

func supportedCacheInvalidationHost(host string) bool {
	return host != "" && !strings.ContainsAny(host, "/*:") && supportedURLMapHost(host)
}

func cacheInvalidationPathMatches(pattern, path string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(path, strings.TrimSuffix(pattern, "*"))
	}
	return path == pattern
}

type loadBalancerBackend struct {
	target      *url.URL
	healthPath  string
	description string
}

type resolvedLoadBalancerRoute struct {
	backends       []loadBalancerBackend
	backendService string
	urlMap         string
	policyName     string
	policyVersion  string
	policy         *SecurityPolicy
	cdnEnabled     bool
}

type networkHTTPAuthorizer interface {
	AuthorizeHTTP(project, location, sourceIP, host, method, path string) (matched, allowed bool, policy string, err error)
}

type serviceMeshHTTPRouter interface {
	RouteHTTP(project, location, host, path string) (matched bool, destination, route string, err error)
}

// OnPostBoot wires optional cross-service request-path providers after every
// shim has been instantiated. Missing providers preserve the existing Compute
// proxy behavior.
func (api *API) OnPostBoot(ctx *registry.Context) {
	if authorizer, ok := ctx.GetShim("networksecurity.googleapis.com").(networkHTTPAuthorizer); ok {
		api.networkAuthorizer = authorizer
	}
	if router, ok := ctx.GetShim("networkservices.googleapis.com").(serviceMeshHTTPRouter); ok {
		api.serviceMeshRouter = router
	}
}

func (api *API) proxyLoadBalancerRequest(
	w http.ResponseWriter,
	r *http.Request,
	project string,
	forwardingRuleName string,
	proxyPath string,
) {
	host := normalizedRequestHost(r.Host)
	if api.networkAuthorizer != nil {
		sourceIP := r.RemoteAddr
		if parsed, _, splitErr := net.SplitHostPort(sourceIP); splitErr == nil {
			sourceIP = parsed
		}
		matched, allowed, _, authorizeErr := api.networkAuthorizer.AuthorizeHTTP(
			project, "global", sourceIP, host, r.Method, proxyPath,
		)
		if authorizeErr != nil {
			writeLoadBalancerUnavailable(w, "authorization policy evaluation failed: "+authorizeErr.Error())
			return
		}
		if matched && !allowed {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, "request denied by authorization policy\n")
			return
		}
	}

	backendOverride := ""
	if api.serviceMeshRouter != nil {
		matched, destination, _, routeErr := api.serviceMeshRouter.RouteHTTP(
			project, "global", host, proxyPath,
		)
		if routeErr != nil {
			writeLoadBalancerUnavailable(w, "service mesh route evaluation failed: "+routeErr.Error())
			return
		}
		if matched {
			var destinationErr error
			backendOverride, destinationErr = localBackendServiceName(project, destination)
			if destinationErr != nil {
				writeLoadBalancerUnavailable(w, "service mesh destination is unavailable: "+destinationErr.Error())
				return
			}
		}
	}

	route, err := api.resolveLoadBalancerBackends(
		project, forwardingRuleName, r.Host, proxyPath, backendOverride,
	)
	if err != nil {
		writeLoadBalancerUnavailable(w, err.Error())
		return
	}
	allowed, denyStatus, err := evaluateSecurityPolicy(r, route.policy)
	if err != nil {
		writeLoadBalancerUnavailable(w, "security policy evaluation failed: "+err.Error())
		return
	}
	if !allowed {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(denyStatus)
		_, _ = io.WriteString(w, "request denied by security policy\n")
		return
	}

	cacheKey, cacheableRequest := loadBalancerRequestCacheKey(
		r, project, route.backendService, route.policyName, route.policyVersion, proxyPath,
	)
	if route.cdnEnabled && cacheableRequest && api.serveLoadBalancerCache(w, cacheKey) {
		return
	}
	cacheStartedAt := time.Now()

	healthy := make([]loadBalancerBackend, 0, len(route.backends))
	for _, backend := range route.backends {
		if api.backendIsHealthy(r, backend) {
			healthy = append(healthy, backend)
		}
	}
	if len(healthy) == 0 {
		writeLoadBalancerUnavailable(w, fmt.Sprintf(
			"backend service %q has no healthy resolvable backends",
			route.backendService,
		))
		return
	}

	api.mu.Lock()
	cursor := api.roundRobin[loadBalancerKey(project, "backendServices", route.backendService)]
	api.roundRobin[loadBalancerKey(project, "backendServices", route.backendService)] = cursor + 1
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
	if !route.cdnEnabled || !cacheableRequest {
		proxy.ServeHTTP(w, outbound)
		return
	}
	capture := newLoadBalancerCacheCapture(w)
	proxy.ServeHTTP(capture, outbound)
	api.storeLoadBalancerCache(
		cacheKey, route.urlMap, proxyPath, normalizedRequestHost(r.Host), cacheStartedAt, capture,
	)
}

func (api *API) resolveLoadBalancerBackends(
	project string,
	forwardingRuleName string,
	host string,
	requestPath string,
	backendOverride string,
) (*resolvedLoadBalancerRoute, error) {
	api.mu.RLock()
	forwardingRule := api.loadBalancers[loadBalancerKey(project, "forwardingRules", forwardingRuleName)]
	if forwardingRule == nil {
		api.mu.RUnlock()
		return nil, fmt.Errorf("forwarding rule %q was not found", forwardingRuleName)
	}
	targetProxyName := resourceReferenceName(forwardingRule["target"])
	targetProxy := api.loadBalancers[loadBalancerKey(project, "targetHttpProxies", targetProxyName)]
	if targetProxy == nil {
		api.mu.RUnlock()
		return nil, fmt.Errorf("forwarding rule %q does not resolve to a target HTTP proxy", forwardingRuleName)
	}
	urlMapName := resourceReferenceName(targetProxy["urlMap"])
	urlMap := api.loadBalancers[loadBalancerKey(project, "urlMaps", urlMapName)]
	if urlMap == nil {
		api.mu.RUnlock()
		return nil, fmt.Errorf("target HTTP proxy %q does not resolve to a URL map", targetProxyName)
	}
	backendServiceName, routeErr := resolveURLMapService(urlMap, host, requestPath)
	if routeErr != nil {
		api.mu.RUnlock()
		return nil, fmt.Errorf("URL map %q cannot route request: %w", urlMapName, routeErr)
	}
	if backendOverride != "" {
		backendServiceName = backendOverride
	}
	backendService := api.loadBalancers[loadBalancerKey(project, "backendServices", backendServiceName)]
	if backendService == nil {
		api.mu.RUnlock()
		if backendOverride != "" {
			return nil, fmt.Errorf("service mesh route resolves to missing backend service %q", backendServiceName)
		}
		return nil, fmt.Errorf("URL map %q does not resolve to a default backend service", urlMapName)
	}
	if protocol, _ := backendService["protocol"].(string); protocol != "" &&
		!strings.EqualFold(protocol, "HTTP") && !strings.EqualFold(protocol, "HTTPS") {
		api.mu.RUnlock()
		return nil, fmt.Errorf("backend service %q uses unsupported protocol %q", backendServiceName, protocol)
	}
	policyName := ""
	var policy *SecurityPolicy
	if reference, _ := backendService["securityPolicy"].(string); reference != "" {
		var policyErr error
		policyName, policyErr = securityPolicyReferenceName(reference, project)
		if policyErr != nil {
			api.mu.RUnlock()
			return nil, fmt.Errorf("backend service %q has invalid security policy: %w", backendServiceName, policyErr)
		}
		policy = cloneSecurityPolicy(api.securityPolicies[project+":"+policyName])
		if policy == nil {
			api.mu.RUnlock()
			return nil, fmt.Errorf("backend service %q references missing security policy %q", backendServiceName, policyName)
		}
	}
	cdnEnabled, _ := backendService["enableCDN"].(bool)
	rawBackends, hasBackends := backendService["backends"].([]interface{})
	portName, _ := backendService["portName"].(string)
	rawHealthChecks, _ := backendService["healthChecks"].([]interface{})
	healthPath, healthErr := api.resolveHealthPathLocked(project, rawHealthChecks)
	api.mu.RUnlock()

	if healthErr != nil {
		return nil, healthErr
	}
	if !hasBackends || len(rawBackends) == 0 {
		return nil, fmt.Errorf("backend service %q has no backends", backendServiceName)
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
		return nil, fmt.Errorf(
			"backend service %q has only unsupported backends (%s); supported forms require an explicit HTTP(S) 'url' or a Compute 'instance' plus 'port'",
			backendServiceName,
			strings.Join(unsupported, "; "),
		)
	}
	return &resolvedLoadBalancerRoute{
		backends:       backends,
		backendService: backendServiceName,
		urlMap:         urlMapName,
		policyName:     policyName,
		policyVersion:  securityPolicyCacheVersion(policy),
		policy:         policy,
		cdnEnabled:     cdnEnabled,
	}, nil
}

func localBackendServiceName(project, reference string) (string, error) {
	if reference == "" {
		return "", errors.New("destination serviceName is empty")
	}
	if !strings.Contains(reference, "/") {
		if !validComputeResourceName(reference) {
			return "", fmt.Errorf("invalid backend service name %q", reference)
		}
		return reference, nil
	}
	parts := strings.Split(strings.Trim(reference, "/"), "/")
	if len(parts) == 5 && parts[0] == "projects" && parts[2] == "global" &&
		parts[3] == "backendServices" && parts[1] == project && validComputeResourceName(parts[4]) {
		return parts[4], nil
	}
	if len(parts) == 6 && parts[0] == "projects" && parts[2] == "locations" &&
		parts[3] == "global" && parts[4] == "backendServices" &&
		parts[1] == project && validComputeResourceName(parts[5]) {
		return parts[5], nil
	}
	return "", fmt.Errorf("destination %q is not a global Compute backend service in project %q", reference, project)
}

func validComputeResourceName(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			character == '-' && index > 0 && index < len(value)-1 {
			continue
		}
		return false
	}
	return true
}

// evaluateSecurityPolicy implements only ordered source-CIDR allow/deny rules.
// It does not emulate CEL, WAF signatures, rate limiting, redirects, or adaptive protection.
func evaluateSecurityPolicy(r *http.Request, policy *SecurityPolicy) (bool, int, error) {
	if policy == nil {
		return true, 0, nil
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = strings.Trim(r.RemoteAddr, "[]")
	}
	source, err := netip.ParseAddr(host)
	if err != nil {
		return false, 0, errors.New("request source address is unavailable")
	}
	rules := cloneSecurityPolicyRules(policy.Rules)
	sort.SliceStable(rules, func(i, j int) bool { return rules[i].Priority < rules[j].Priority })
	for _, rule := range rules {
		matches := rule.Match == nil
		if rule.Match != nil {
			if rule.Match.VersionedExpr != "SRC_IPS_V1" || rule.Match.Config == nil {
				return false, 0, fmt.Errorf("rule priority %d uses an unsupported match", rule.Priority)
			}
			for _, value := range rule.Match.Config.SrcIPRanges {
				prefix, parseErr := netip.ParsePrefix(value)
				if parseErr != nil {
					return false, 0, fmt.Errorf("rule priority %d has an invalid source range", rule.Priority)
				}
				if prefix.Contains(source) {
					matches = true
					break
				}
			}
		}
		if !matches {
			continue
		}
		denyStatus, actionErr := securityPolicyAction(rule.Action)
		if actionErr != nil {
			return false, 0, actionErr
		}
		if denyStatus == 0 {
			return true, 0, nil
		}
		return false, denyStatus, nil
	}
	return false, 0, errors.New("security policy has no matching default rule")
}

type loadBalancerCacheCapture struct {
	target   http.ResponseWriter
	status   int
	body     []byte
	overflow bool
}

func newLoadBalancerCacheCapture(target http.ResponseWriter) *loadBalancerCacheCapture {
	return &loadBalancerCacheCapture{target: target}
}

func (capture *loadBalancerCacheCapture) Header() http.Header {
	return capture.target.Header()
}

func (capture *loadBalancerCacheCapture) Unwrap() http.ResponseWriter {
	return capture.target
}

func (capture *loadBalancerCacheCapture) WriteHeader(status int) {
	if capture.status == 0 {
		capture.status = status
	}
	capture.target.WriteHeader(status)
}

func (capture *loadBalancerCacheCapture) Write(body []byte) (int, error) {
	if capture.status == 0 {
		capture.status = http.StatusOK
	}
	if !capture.overflow {
		if len(capture.body)+len(body) <= maxLoadBalancerCacheBytes {
			capture.body = append(capture.body, body...)
		} else {
			capture.body = nil
			capture.overflow = true
		}
	}
	return capture.target.Write(body)
}

// loadBalancerRequestCacheKey gates the bounded, transient in-process CDN subset.
// It is not a shared edge cache and does not emulate Cloud CDN revalidation or cache modes.
func loadBalancerRequestCacheKey(
	r *http.Request,
	project string,
	backendService string,
	policyName string,
	policyVersion string,
	proxyPath string,
) (loadBalancerCacheKey, bool) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead ||
		r.ContentLength > 0 || len(r.TransferEncoding) != 0 ||
		r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" ||
		r.Header.Get("Range") != "" || r.Header.Get("If-None-Match") != "" ||
		r.Header.Get("If-Modified-Since") != "" ||
		headerContainsCacheDirective(r.Header.Get("Cache-Control"), "no-cache") ||
		headerContainsCacheDirective(r.Header.Get("Cache-Control"), "no-store") ||
		strings.EqualFold(strings.TrimSpace(r.Header.Get("Pragma")), "no-cache") {
		return loadBalancerCacheKey{}, false
	}
	uri := proxyPath
	if r.URL.RawQuery != "" {
		uri += "?" + r.URL.RawQuery
	}
	return loadBalancerCacheKey{
		project:        project,
		backendService: backendService,
		policyName:     policyName,
		policyVersion:  policyVersion,
		method:         r.Method,
		host:           normalizedRequestHost(r.Host),
		uri:            uri,
	}, true
}

func normalizedRequestHost(host string) string {
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		host = parsed
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func headerContainsCacheDirective(value, wanted string) bool {
	for _, directive := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(strings.SplitN(directive, "=", 2)[0]), wanted) {
			return true
		}
	}
	return false
}

func (api *API) serveLoadBalancerCache(w http.ResponseWriter, key loadBalancerCacheKey) bool {
	now := time.Now()
	api.cacheMu.Lock()
	entry, ok := api.loadBalancerCache[key]
	if ok && !entry.expiresAt.After(now) {
		delete(api.loadBalancerCache, key)
		ok = false
	}
	api.cacheMu.Unlock()
	if !ok {
		return false
	}
	copyHTTPHeader(w.Header(), entry.header)
	w.WriteHeader(entry.status)
	if key.method != http.MethodHead {
		_, _ = w.Write(entry.body)
	}
	return true
}

func (api *API) storeLoadBalancerCache(
	key loadBalancerCacheKey,
	urlMap string,
	path string,
	host string,
	startedAt time.Time,
	capture *loadBalancerCacheCapture,
) {
	if capture.status != http.StatusOK || capture.overflow ||
		httpHeaderSize(capture.Header()) > maxLoadBalancerHeaderBytes ||
		capture.Header().Get("Set-Cookie") != "" || capture.Header().Get("Vary") != "" ||
		capture.Header().Get("Content-Range") != "" {
		return
	}
	ttl, ok := boundedCacheTTL(capture.Header().Get("Cache-Control"))
	if !ok {
		return
	}
	entry := loadBalancerCacheEntry{
		status:    capture.status,
		header:    capture.Header().Clone(),
		body:      append([]byte(nil), capture.body...),
		expiresAt: time.Now().Add(ttl),
		urlMap:    urlMap,
		path:      path,
		host:      host,
	}
	api.cacheMu.Lock()
	if api.cacheClearedAt.After(startedAt) ||
		api.cacheInvalidatedAt[key.project+":"+urlMap].After(startedAt) ||
		api.policyInvalidatedAt[key.project+":"+key.policyName].After(startedAt) {
		api.cacheMu.Unlock()
		return
	}
	if len(api.loadBalancerCache) < maxLoadBalancerCacheEntries {
		api.loadBalancerCache[key] = entry
	}
	api.cacheMu.Unlock()
}

func httpHeaderSize(header http.Header) int {
	size := 0
	for key, values := range header {
		size += len(key)
		for _, value := range values {
			size += len(value)
		}
	}
	return size
}

func boundedCacheTTL(value string) (time.Duration, bool) {
	public := false
	seconds := -1
	for _, rawDirective := range strings.Split(value, ",") {
		directive := strings.TrimSpace(strings.ToLower(rawDirective))
		switch {
		case directive == "public":
			public = true
		case directive == "private" || directive == "no-store" || directive == "no-cache":
			return 0, false
		case strings.HasPrefix(directive, "s-maxage="):
			parsed, err := strconv.Atoi(strings.TrimPrefix(directive, "s-maxage="))
			if err != nil {
				return 0, false
			}
			seconds = parsed
		case strings.HasPrefix(directive, "max-age=") && seconds < 0:
			parsed, err := strconv.Atoi(strings.TrimPrefix(directive, "max-age="))
			if err != nil {
				return 0, false
			}
			seconds = parsed
		}
	}
	if !public || seconds <= 0 {
		return 0, false
	}
	maxSeconds := int(maxLoadBalancerCacheTTL / time.Second)
	if seconds > maxSeconds {
		return maxLoadBalancerCacheTTL, true
	}
	return time.Duration(seconds) * time.Second, true
}

func copyHTTPHeader(destination, source http.Header) {
	for key := range destination {
		destination.Del(key)
	}
	for key, values := range source {
		destination[key] = append([]string(nil), values...)
	}
}

func (api *API) invalidateLoadBalancerCacheForPolicy(project, policy string) {
	api.cacheMu.Lock()
	api.policyInvalidatedAt[project+":"+policy] = time.Now()
	for key := range api.loadBalancerCache {
		if key.project == project && key.policyName == policy {
			delete(api.loadBalancerCache, key)
		}
	}
	api.cacheMu.Unlock()
}

func securityPolicyCacheVersion(policy *SecurityPolicy) string {
	if policy == nil {
		return ""
	}
	payload, _ := json.Marshal(policy)
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum[:])
}

func (api *API) invalidateAllLoadBalancerCache() {
	api.cacheMu.Lock()
	api.cacheClearedAt = time.Now()
	clear(api.loadBalancerCache)
	api.cacheMu.Unlock()
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
	response, err := observability.Do(api.httpClient, request)
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

func resolveURLMapService(urlMap map[string]interface{}, requestHost, requestPath string) (string, error) {
	defaultService := resourceReferenceName(urlMap["defaultService"])
	if defaultService == "" {
		return "", fmt.Errorf("defaultService is required")
	}
	rawHostRules, _ := urlMap["hostRules"].([]interface{})
	rawPathMatchers, _ := urlMap["pathMatchers"].([]interface{})
	if len(rawHostRules) == 0 && len(rawPathMatchers) == 0 {
		return defaultService, nil
	}

	pathMatchers := make(map[string]map[string]interface{}, len(rawPathMatchers))
	for _, raw := range rawPathMatchers {
		matcher, ok := raw.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("pathMatcher must be an object")
		}
		name, _ := matcher["name"].(string)
		if name == "" || pathMatchers[name] != nil {
			return "", fmt.Errorf("pathMatcher names must be non-empty and unique")
		}
		if resourceReferenceName(matcher["defaultService"]) == "" {
			return "", fmt.Errorf("pathMatcher %q requires defaultService", name)
		}
		pathMatchers[name] = matcher
	}

	host := strings.ToLower(requestHost)
	if parsedHost, _, err := net.SplitHostPort(requestHost); err == nil {
		host = strings.ToLower(parsedHost)
	}
	for _, raw := range rawHostRules {
		rule, ok := raw.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("hostRule must be an object")
		}
		matcherName, _ := rule["pathMatcher"].(string)
		matcher := pathMatchers[matcherName]
		if matcher == nil {
			return "", fmt.Errorf("hostRule references unknown pathMatcher %q", matcherName)
		}
		rawHosts, ok := rule["hosts"].([]interface{})
		if !ok || len(rawHosts) == 0 {
			return "", fmt.Errorf("hostRule requires hosts")
		}
		matched := false
		for _, rawPattern := range rawHosts {
			pattern, ok := rawPattern.(string)
			if !ok || !supportedURLMapHost(pattern) {
				return "", fmt.Errorf("unsupported host pattern")
			}
			if urlMapHostMatches(strings.ToLower(pattern), host) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		service, err := resolveURLMapPathMatcher(matcher, requestPath)
		if err != nil {
			return "", err
		}
		return service, nil
	}
	return defaultService, nil
}

func resolveURLMapPathMatcher(matcher map[string]interface{}, requestPath string) (string, error) {
	selected := resourceReferenceName(matcher["defaultService"])
	selectedLength := -1
	rawRules, _ := matcher["pathRules"].([]interface{})
	for _, raw := range rawRules {
		rule, ok := raw.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("pathRule must be an object")
		}
		service := resourceReferenceName(rule["service"])
		rawPaths, ok := rule["paths"].([]interface{})
		if service == "" || !ok || len(rawPaths) == 0 {
			return "", fmt.Errorf("pathRule requires paths and service")
		}
		for _, rawPattern := range rawPaths {
			pattern, ok := rawPattern.(string)
			if !ok || !supportedURLMapPath(pattern) {
				return "", fmt.Errorf("unsupported path pattern")
			}
			prefix := strings.TrimSuffix(pattern, "*")
			matches := requestPath == pattern
			if strings.HasSuffix(pattern, "*") {
				matches = strings.HasPrefix(requestPath, prefix)
			}
			if matches && len(prefix) > selectedLength {
				selected = service
				selectedLength = len(prefix)
			}
		}
	}
	return selected, nil
}

func supportedURLMapHost(pattern string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		pattern = strings.TrimPrefix(pattern, "*.")
	}
	return pattern != "" && !strings.ContainsAny(pattern, "/*:")
}

func urlMapHostMatches(pattern, host string) bool {
	switch {
	case pattern == "*":
		return true
	case strings.HasPrefix(pattern, "*."):
		suffix := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(host, suffix) && host != strings.TrimPrefix(suffix, ".")
	default:
		return host == pattern
	}
}

func supportedURLMapPath(pattern string) bool {
	if !strings.HasPrefix(pattern, "/") || strings.ContainsAny(pattern, "?#") {
		return false
	}
	return !strings.Contains(pattern[:max(0, len(pattern)-1)], "*")
}

func (api *API) validateURLMap(project string, urlMap map[string]interface{}) error {
	validateService := func(value interface{}) error {
		name, err := backendServiceReferenceName(value, project)
		if err != nil {
			return err
		}
		api.mu.RLock()
		exists := api.loadBalancers[loadBalancerKey(project, "backendServices", name)] != nil
		api.mu.RUnlock()
		if !exists {
			return fmt.Errorf("backend service %q does not exist in project %q", name, project)
		}
		return nil
	}
	if err := validateService(urlMap["defaultService"]); err != nil {
		return fmt.Errorf("defaultService: %w", err)
	}
	rawMatchers, _ := urlMap["pathMatchers"].([]interface{})
	matchers := make(map[string]struct{}, len(rawMatchers))
	for _, raw := range rawMatchers {
		matcher, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("pathMatcher must be an object")
		}
		name, _ := matcher["name"].(string)
		if name == "" {
			return fmt.Errorf("pathMatcher name is required")
		}
		if _, duplicate := matchers[name]; duplicate {
			return fmt.Errorf("pathMatcher names must be unique")
		}
		matchers[name] = struct{}{}
		if err := validateService(matcher["defaultService"]); err != nil {
			return fmt.Errorf("pathMatcher %q defaultService: %w", name, err)
		}
		rawRules, _ := matcher["pathRules"].([]interface{})
		for _, rawRule := range rawRules {
			rule, ok := rawRule.(map[string]interface{})
			if !ok {
				return fmt.Errorf("pathMatcher %q pathRule must be an object", name)
			}
			paths, ok := rule["paths"].([]interface{})
			if !ok || len(paths) == 0 {
				return fmt.Errorf("pathMatcher %q pathRule requires paths", name)
			}
			for _, rawPath := range paths {
				path, ok := rawPath.(string)
				if !ok || !supportedURLMapPath(path) {
					return fmt.Errorf("pathMatcher %q has unsupported path pattern", name)
				}
			}
			if err := validateService(rule["service"]); err != nil {
				return fmt.Errorf("pathMatcher %q pathRule service: %w", name, err)
			}
		}
	}
	rawHostRules, _ := urlMap["hostRules"].([]interface{})
	for _, raw := range rawHostRules {
		rule, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("hostRule must be an object")
		}
		matcher, _ := rule["pathMatcher"].(string)
		if _, ok := matchers[matcher]; !ok {
			return fmt.Errorf("hostRule references unknown pathMatcher %q", matcher)
		}
		hosts, ok := rule["hosts"].([]interface{})
		if !ok || len(hosts) == 0 {
			return fmt.Errorf("hostRule requires hosts")
		}
		for _, rawHost := range hosts {
			host, ok := rawHost.(string)
			if !ok || !supportedURLMapHost(host) {
				return fmt.Errorf("hostRule has unsupported host pattern")
			}
		}
	}
	return nil
}

func backendServiceReferenceName(value interface{}, project string) (string, error) {
	reference, _ := value.(string)
	if reference == "" {
		return "", fmt.Errorf("reference is required")
	}
	if !strings.Contains(reference, "/") {
		return reference, nil
	}
	path := reference
	if parsed, err := url.Parse(reference); err == nil && parsed.Path != "" {
		path = parsed.Path
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for index := range parts {
		if parts[index] != "projects" || index+4 >= len(parts) {
			continue
		}
		if parts[index+1] != project {
			return "", fmt.Errorf("reference project %q does not match %q", parts[index+1], project)
		}
		if parts[index+2] != "global" || parts[index+3] != "backendServices" ||
			parts[index+4] == "" || index+5 != len(parts) {
			return "", fmt.Errorf("reference must target the global backendServices collection")
		}
		return parts[index+4], nil
	}
	return "", fmt.Errorf("reference must be a backend service name or canonical self link")
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
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	if collection.canonical == "urlMaps" {
		if err := api.validateURLMap(project, resource); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
				"invalid URL map routing configuration: "+err.Error())
			return
		}
	}
	if collection.canonical == "backendServices" {
		if reference, _ := resource["securityPolicy"].(string); reference != "" {
			policyName, err := securityPolicyReferenceName(reference, project)
			if err != nil {
				writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT",
					"invalid backend security policy: "+err.Error())
				return
			}
			api.mu.RLock()
			policy := api.securityPolicies[project+":"+policyName]
			api.mu.RUnlock()
			if policy == nil {
				writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT",
					"backend security policy was not found")
				return
			}
			resource["securityPolicy"] = securityPolicySelfLink(project, policyName)
		}
	}

	selfLink := loadBalancerSelfLink(project, collection.canonical, name)
	key := loadBalancerKey(project, collection.canonical, name)
	resource["kind"] = collection.resourceKind
	resource["id"] = randomNumericID()
	resource["name"] = name
	resource["selfLink"] = selfLink
	resource["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)

	api.mu.RLock()
	if _, exists := api.loadBalancers[key]; exists {
		api.mu.RUnlock()
		w.WriteHeader(http.StatusConflict)
		writeError(w, http.StatusConflict, "ALREADY_EXISTS",
			fmt.Sprintf("%s %q already exists", collection.resourceKind, name))
		return
	}
	api.mu.RUnlock()

	op, err := api.opMgr.RegisterDurable("compute#operation", "insert", selfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	previous, err := api.metadataSnapshot()
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("snapshot load-balancer metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	next, err := cloneComputeMetadata(previous)
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("clone load-balancer metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	if next.LoadBalancers == nil {
		next.LoadBalancers = make(map[string]map[string]interface{})
	}
	next.LoadBalancers[key] = resource
	if err := api.saveMetadataTransaction(previous, next); err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("persist load-balancer metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	if err := api.opMgr.FinalizeDurable(op.Name, 0, ""); err != nil {
		if errors.Is(err, orchestrator.ErrOperationTerminalConflict) {
			message := api.failClosedControlPlaneConflict(err)
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		message := api.compensateControlPlaneCommit(previous, next, err)
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	api.mu.Lock()
	api.loadBalancers[key] = next.LoadBalancers[key]
	api.mu.Unlock()
	api.invalidateAllLoadBalancerCache()
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, api.opMgr.Get(op.Name))
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
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	_, ok := api.loadBalancers[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("%s %q not found", collection.resourceKind, name))
		return
	}

	selfLink := loadBalancerSelfLink(project, collection.canonical, name)
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete", selfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	previous, err := api.metadataSnapshot()
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("snapshot load-balancer metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	next, err := cloneComputeMetadata(previous)
	if err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("clone load-balancer metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	delete(next.LoadBalancers, key)
	if err := api.saveMetadataTransaction(previous, next); err != nil {
		message := api.failControlPlaneOperation(op.Name, fmt.Errorf("persist load-balancer deletion: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	if err := api.opMgr.FinalizeDurable(op.Name, 0, ""); err != nil {
		if errors.Is(err, orchestrator.ErrOperationTerminalConflict) {
			message := api.failClosedControlPlaneConflict(err)
			writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
			return
		}
		message := api.compensateControlPlaneCommit(previous, next, err)
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	api.mu.Lock()
	delete(api.loadBalancers, key)
	api.mu.Unlock()
	api.invalidateAllLoadBalancerCache()
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, api.opMgr.Get(op.Name))
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

	if api.firewall == nil {
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"firewall backend is unavailable")
		return
	}

	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	key := project + ":" + body.Name
	op, err := api.opMgr.RegisterDurable("compute#operation", "insert", body.SelfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	previous, err := api.metadataSnapshot()
	if err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("snapshot firewall metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	snapshot, err := cloneComputeMetadata(previous)
	if err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("clone firewall metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	if snapshot.Firewalls == nil {
		snapshot.Firewalls = make(map[string]*FirewallRule)
	}
	snapshot.Firewalls[key] = cloneFirewallRule(&body)
	if err := api.saveMetadataTransaction(previous, snapshot); err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("persist firewall metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	api.mu.Lock()
	api.firewalls[key] = cloneFirewallRule(&body)
	api.mu.Unlock()
	api.firewall.RegisterFirewallRule(firewallKey, firewallEntryFromRule(&body))
	api.runFirewallOperation(op.Name, func() error {
		if err := api.reapplyFirewallToVPC(body.Network); err != nil {
			return api.rollbackFirewallMutation(key, previous.Firewalls[key], err, func() error {
				api.firewall.RemoveFirewallRule(body.Network, body.Name)
				return api.reapplyFirewallToVPC(body.Network)
			})
		}
		return nil
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
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	fw, ok := api.firewalls[key]
	fw = cloneFirewallRule(fw)
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Firewall "+name+" not found")
		return
	}
	var patch FirewallRule
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeErrorStatus(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
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
	if api.firewall == nil {
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"firewall backend is unavailable")
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", "patch", fw.SelfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	previous, err := api.metadataSnapshot()
	if err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("snapshot firewall metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	snapshot, err := cloneComputeMetadata(previous)
	if err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("clone firewall metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	snapshot.Firewalls[key] = cloneFirewallRule(fw)
	if err := api.saveMetadataTransaction(previous, snapshot); err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("persist firewall update: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	api.mu.Lock()
	api.firewalls[key] = cloneFirewallRule(fw)
	api.mu.Unlock()
	api.firewall.RemoveFirewallRule(fw.Network, fw.Name)
	api.firewall.RegisterFirewallRule(fw.Network, firewallEntryFromRule(fw))
	api.runFirewallOperation(op.Name, func() error {
		if err := api.reapplyFirewallToVPC(fw.Network); err != nil {
			previousRule := cloneFirewallRule(previous.Firewalls[key])
			return api.rollbackFirewallMutation(key, previousRule, err, func() error {
				api.firewall.RemoveFirewallRule(fw.Network, fw.Name)
				api.firewall.RegisterFirewallRule(previousRule.Network, firewallEntryFromRule(previousRule))
				return api.reapplyFirewallToVPC(previousRule.Network)
			})
		}
		return nil
	})
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) deleteFirewall(w http.ResponseWriter, project, name string) {
	key := project + ":" + name
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	fw, ok := api.firewalls[key]
	fw = cloneFirewallRule(fw)
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Firewall "+name+" not found")
		return
	}
	if api.firewall == nil {
		writeErrorStatus(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION",
			"firewall backend is unavailable")
		return
	}
	op, err := api.opMgr.RegisterDurable("compute#operation", "delete", fw.SelfLink, "", "")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	previous, err := api.metadataSnapshot()
	if err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("snapshot firewall metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	snapshot, err := cloneComputeMetadata(previous)
	if err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("clone firewall metadata: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	delete(snapshot.Firewalls, key)
	if err := api.saveMetadataTransaction(previous, snapshot); err != nil {
		message := api.failFirewallOperation(op.Name, fmt.Errorf("persist firewall deletion: %w", err))
		writeErrorStatus(w, http.StatusInternalServerError, "INTERNAL", message)
		return
	}
	api.mu.Lock()
	delete(api.firewalls, key)
	api.mu.Unlock()
	api.firewall.RemoveFirewallRule(fw.Network, name)
	api.runFirewallOperation(op.Name, func() error {
		if err := api.reapplyFirewallToVPC(fw.Network); err != nil {
			return api.rollbackFirewallMutation(key, previous.Firewalls[key], err, func() error {
				api.firewall.RegisterFirewallRule(fw.Network, firewallEntryFromRule(fw))
				return api.reapplyFirewallToVPC(fw.Network)
			})
		}
		return nil
	})
	w.WriteHeader(http.StatusOK)
	writeComputeOperation(w, project, op)
}

func (api *API) metadataSnapshot() (computeMetadata, error) {
	api.mu.RLock()
	payload, err := api.marshalMetadataLocked()
	api.mu.RUnlock()
	if err != nil {
		return computeMetadata{}, err
	}
	var snapshot computeMetadata
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return computeMetadata{}, err
	}
	return snapshot, nil
}

func (api *API) saveMetadata(snapshot computeMetadata) error {
	if api.stateStore == nil {
		return nil
	}
	return api.stateStore.Save(computeStateEntry, snapshot)
}

func (api *API) saveMetadataTransaction(previous, next computeMetadata) error {
	if api.stateStore == nil {
		return nil
	}
	saveErr := api.saveMetadata(next)
	if saveErr == nil {
		return nil
	}
	var durable computeMetadata
	loadErr := api.stateStore.Load(computeStateEntry, &durable)
	switch {
	case loadErr == nil && computeMetadataEqual(durable, next):
		return nil
	case loadErr == nil && computeMetadataEqual(durable, previous):
		return saveErr
	case errors.Is(loadErr, state.ErrNotFound) && computeMetadataEmpty(previous):
		return saveErr
	default:
		ambiguous := fmt.Errorf("Compute metadata save outcome is ambiguous: save: %w; read back: %v", saveErr, loadErr)
		api.setInitializationError(ambiguous)
		return ambiguous
	}
}

func (api *API) runFirewallOperation(operationName string, work func() error) {
	go func() {
		workErr := work()
		code, message := 0, ""
		if workErr != nil {
			code = http.StatusInternalServerError
			message = workErr.Error()
		}
		if err := api.opMgr.FinalizeDurable(operationName, code, message); err != nil {
			if workErr != nil {
				api.setInitializationError(fmt.Errorf("firewall operation failed: %w; persist terminal operation: %w", workErr, err))
			} else {
				api.setInitializationError(err)
			}
		}
	}()
}

func (api *API) rollbackFirewallMutation(
	key string,
	previousRule *FirewallRule,
	mutationErr error,
	restoreBackend func() error,
) error {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	current, err := api.metadataSnapshot()
	if err != nil {
		combined := fmt.Errorf("firewall mutation failed: %w; snapshot rollback failed: %v", mutationErr, err)
		api.setInitializationError(combined)
		return combined
	}
	snapshot, err := cloneComputeMetadata(current)
	if err != nil {
		combined := fmt.Errorf("firewall mutation failed: %w; clone rollback failed: %v", mutationErr, err)
		api.setInitializationError(combined)
		return combined
	}
	if previousRule == nil {
		delete(snapshot.Firewalls, key)
	} else {
		snapshot.Firewalls[key] = cloneFirewallRule(previousRule)
	}
	if err := api.saveMetadataTransaction(current, snapshot); err != nil {
		combined := fmt.Errorf("firewall mutation failed: %w; persist rollback failed: %v", mutationErr, err)
		api.setInitializationError(combined)
		return combined
	}
	api.mu.Lock()
	if previousRule == nil {
		delete(api.firewalls, key)
	} else {
		api.firewalls[key] = cloneFirewallRule(previousRule)
	}
	api.mu.Unlock()
	if err := restoreBackend(); err != nil {
		combined := fmt.Errorf("firewall mutation failed: %w; backend compensation failed: %v", mutationErr, err)
		api.setInitializationError(combined)
		return combined
	}
	return mutationErr
}

func (api *API) failFirewallOperation(operationName string, mutationErr error) string {
	if err := api.opMgr.FinalizeDurable(operationName, http.StatusInternalServerError, mutationErr.Error()); err != nil {
		combined := fmt.Errorf("%w; persist failed operation: %w", mutationErr, err)
		api.setInitializationError(combined)
		return combined.Error()
	}
	return mutationErr.Error()
}

func (api *API) failControlPlaneOperation(operationName string, mutationErr error) string {
	if err := api.opMgr.FinalizeDurable(operationName, http.StatusInternalServerError, mutationErr.Error()); err != nil {
		combined := fmt.Errorf("%w; persist failed operation: %w", mutationErr, err)
		api.setInitializationError(combined)
		return combined.Error()
	}
	return mutationErr.Error()
}

func (api *API) failClosedControlPlaneConflict(operationErr error) string {
	conflict := fmt.Errorf("persist terminal operation: %w", operationErr)
	api.setInitializationError(conflict)
	return conflict.Error()
}

func (api *API) compensateControlPlaneCommit(previous, committed computeMetadata, operationErr error) string {
	if rollbackErr := api.saveMetadataTransaction(committed, previous); rollbackErr != nil {
		combined := fmt.Errorf(
			"persist terminal operation: %w; compensate Compute metadata: %v",
			operationErr,
			rollbackErr,
		)
		api.setInitializationError(combined)
		return combined.Error()
	}
	api.setInitializationError(operationErr)
	return operationErr.Error()
}

func cloneFirewallRule(rule *FirewallRule) *FirewallRule {
	if rule == nil {
		return nil
	}
	clone := *rule
	clone.SourceRanges = append([]string(nil), rule.SourceRanges...)
	clone.DestinationRanges = append([]string(nil), rule.DestinationRanges...)
	clone.TargetTags = append([]string(nil), rule.TargetTags...)
	clone.Allowed = cloneFirewallAllows(rule.Allowed)
	clone.Denied = cloneFirewallAllows(rule.Denied)
	return &clone
}

func cloneComputeMetadata(metadata computeMetadata) (computeMetadata, error) {
	payload, err := json.Marshal(metadata)
	if err != nil {
		return computeMetadata{}, err
	}
	var clone computeMetadata
	if err := json.Unmarshal(payload, &clone); err != nil {
		return computeMetadata{}, err
	}
	return clone, nil
}

func computeMetadataEqual(left, right computeMetadata) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func computeMetadataEmpty(metadata computeMetadata) bool {
	return len(metadata.Instances) == 0 &&
		len(metadata.Networks) == 0 &&
		len(metadata.Subnetworks) == 0 &&
		len(metadata.SecurityPolicies) == 0 &&
		len(metadata.Firewalls) == 0 &&
		len(metadata.InstanceGroups) == 0 &&
		len(metadata.LoadBalancers) == 0
}

func cloneFirewallAllows(rules []FirewallAllow) []FirewallAllow {
	if rules == nil {
		return nil
	}
	result := make([]FirewallAllow, len(rules))
	for index := range rules {
		result[index] = rules[index]
		result[index].Ports = append([]string(nil), rules[index].Ports...)
	}
	return result
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
