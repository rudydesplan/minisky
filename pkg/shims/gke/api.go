package gke

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("container.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

// Cluster mirrors the GKE container.v1 Cluster resource.
type Cluster struct {
	Name                  string      `json:"name"`
	Description           string      `json:"description,omitempty"`
	NodeConfig            *NodeConfig `json:"nodeConfig,omitempty"`
	MasterAuth            *MasterAuth `json:"masterAuth,omitempty"`
	LoggingService        string      `json:"loggingService"`
	MonitoringService     string      `json:"monitoringService"`
	Network               string      `json:"network"`
	ClusterIpv4Cidr       string      `json:"clusterIpv4Cidr"`
	Endpoint              string      `json:"endpoint"`
	InitialClusterVersion string      `json:"initialClusterVersion"`
	CurrentMasterVersion  string      `json:"currentMasterVersion"`
	Status                string      `json:"status"` // PROVISIONING, RUNNING, RECONCILING, STOPPING, ERROR, DEGRADED
	StatusMessage         string      `json:"statusMessage,omitempty"`
	NodeIpv4CidrSize      int         `json:"nodeIpv4CidrSize"`
	ServicesIpv4Cidr      string      `json:"servicesIpv4Cidr"`
	SelfLink              string      `json:"selfLink"`
	Zone                  string      `json:"zone"`
	Location              string      `json:"location"`
	CreateTime            string      `json:"createTime"`
	InitialNodeCount      int         `json:"initialNodeCount"`
}

type NodeConfig struct {
	MachineType string            `json:"machineType"`
	DiskSizeGb  int               `json:"diskSizeGb"`
	OauthScopes []string          `json:"oauthScopes"`
	Labels      map[string]string `json:"labels,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
}

type MasterAuth struct {
	Username             string `json:"username,omitempty"`
	ClusterCaCertificate string `json:"clusterCaCertificate"`
	ClientCertificate    string `json:"clientCertificate"`
	ClientKey            string `json:"clientKey"`
}

// GkeOperation mirrors the GKE Operation resource.
type GkeOperation struct {
	Name          string                       `json:"name"`
	Zone          string                       `json:"zone"`
	OperationType string                       `json:"operationType"`
	Status        string                       `json:"status"` // PENDING, RUNNING, DONE, ABORTING
	StatusMessage string                       `json:"statusMessage,omitempty"`
	SelfLink      string                       `json:"selfLink"`
	TargetLink    string                       `json:"targetLink"`
	StartTime     string                       `json:"startTime,omitempty"`
	EndTime       string                       `json:"endTime,omitempty"`
	Error         *orchestrator.OperationError `json:"error,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API is the high-fidelity GKE container.v1 shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	configMu   sync.RWMutex
	opMgr      *orchestrator.OperationManager
	backend    gkeBackend
	stateStore gkeStore
	clusters   map[string]*Cluster // key: project:zone:name
	ownerships map[string]*kubeconfigOwnership
	gatewayURL string
	httpClient *http.Client
}

type gkeBackend interface {
	Enabled() bool
	CreateClusterContext(context.Context, ClusterIdentity) (gkeBackendCreateResult, error)
	DeleteClusterContext(context.Context, ClusterIdentity) error
}

type gkeBackendCreateResult struct {
	Created   bool
	Ownership *kubeconfigOwnership
}

func NewAPI(opMgr *orchestrator.OperationManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: GKE] state disabled: %v", err)
		return newAPI(opMgr, nil)
	}
	api, err := NewAPIWithStore(opMgr, store)
	if err != nil {
		log.Printf("[Shim: GKE] state rehydration failed: %v", err)
		return newAPI(opMgr, nil)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store gkeStore) *API {
	return newAPIWithBackend(opMgr, NewKindBackend(), "", nil, store)
}

func newAPIWithBackend(
	opMgr *orchestrator.OperationManager,
	backend gkeBackend,
	gatewayURL string,
	client *http.Client,
	store gkeStore,
) *API {
	return &API{
		opMgr:      opMgr,
		backend:    backend,
		stateStore: store,
		clusters:   make(map[string]*Cluster),
		gatewayURL: strings.TrimRight(gatewayURL, "/"),
		httpClient: client,
		ownerships: make(map[string]*kubeconfigOwnership),
	}
}

// ConfigureGateway wires the actual running gateway and its TLS-capable client.
func (api *API) ConfigureGateway(baseURL string, client *http.Client) {
	api.configMu.Lock()
	defer api.configMu.Unlock()
	api.gatewayURL = strings.TrimRight(baseURL, "/")
	api.httpClient = client
}

// GetBackend exposes the backend for dynamic dashboard configuration.
func (api *API) GetBackend() *KindBackend {
	backend, _ := api.backend.(*KindBackend)
	return backend
}

// ReadKubeconfig returns a durable kubeconfig only for a persisted logical cluster.
func (api *API) ReadKubeconfig(project, zone, name string) ([]byte, error) {
	api.mu.RLock()
	_, exists := api.clusters[clusterKey(project, zone, name)]
	api.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("cluster not found")
	}
	backend, ok := api.backend.(*KindBackend)
	if !ok {
		return nil, fmt.Errorf("Kind backend unavailable")
	}
	return backend.ReadKubeconfig(ClusterIdentity{
		Profile: config.GetProfile(), Project: project, Zone: zone, Cluster: name,
	})
}

// ServeHTTP dispatches GKE container.v1 paths.
//
// Supported paths (container.googleapis.com):
//
//	POST   /v1/projects/{project}/zones/{zone}/clusters
//	GET    /v1/projects/{project}/zones/{zone}/clusters
//	GET    /v1/projects/{project}/zones/{zone}/clusters/{cluster}
//	DELETE /v1/projects/{project}/zones/{zone}/clusters/{cluster}
//	GET    /v1/projects/{project}/zones/{zone}/operations/{operation}
//	(location-based paths /v1/projects/{project}/locations/{zone}/... also handled)
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: GKE] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path

	switch {
	case strings.Contains(path, "/operations/"):
		api.getOperation(w, r, path)
	case strings.Contains(path, "/clusters"):
		api.routeClusters(w, r, path)
	default:
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "GKE resource not found: "+path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Clusters
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeClusters(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	zone := firstOf(extractSegmentAfter(path, "zones"), extractSegmentAfter(path, "locations"))
	clusterName := extractSegmentAfter(path, "clusters")

	switch r.Method {
	case http.MethodPost:
		api.createCluster(w, r, project, zone)
	case http.MethodGet:
		if clusterName != "" {
			api.getCluster(w, project, zone, clusterName)
		} else {
			api.listClusters(w, project, zone)
		}
	case http.MethodDelete:
		api.deleteCluster(w, r, project, zone, clusterName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createCluster(w http.ResponseWriter, r *http.Request, project, zone string) {
	var body struct {
		Cluster Cluster `json:"cluster"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}

	cl := body.Cluster
	name := cl.Name
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "cluster.name is required")
		return
	}

	if zone == "" {
		zone = "us-central1-c"
	}
	if cl.InitialNodeCount == 0 {
		cl.InitialNodeCount = 3
	}
	if cl.InitialNodeCount > 3 {
		log.Printf("[Shim: GKE] Clamping requested node count from %d to 3 maximum limit", cl.InitialNodeCount)
		cl.InitialNodeCount = 3
	}
	if cl.Network == "" {
		cl.Network = "default"
	}
	if cl.NodeConfig == nil {
		cl.NodeConfig = &NodeConfig{
			MachineType: "e2-medium",
			DiskSizeGb:  100,
			OauthScopes: []string{
				"https://www.googleapis.com/auth/cloud-platform",
			},
		}
	}

	cl.Zone = zone
	cl.Location = zone
	cl.Status = "PROVISIONING"
	cl.Endpoint = "127.0.0.1"
	cl.ClusterIpv4Cidr = "10.4.0.0/14"
	cl.ServicesIpv4Cidr = "10.8.0.0/20"
	cl.LoggingService = "logging.googleapis.com/kubernetes"
	cl.MonitoringService = "monitoring.googleapis.com/kubernetes"
	cl.InitialClusterVersion = "1.29.4-gke.100"
	cl.CurrentMasterVersion = "1.29.4-gke.100"
	cl.MasterAuth = &MasterAuth{
		ClusterCaCertificate: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t(minisky-fake)",
		ClientCertificate:    "",
		ClientKey:            "",
	}
	cl.SelfLink = fmt.Sprintf(
		"https://container.googleapis.com/v1/projects/%s/zones/%s/clusters/%s",
		project, zone, name)
	cl.CreateTime = time.Now().UTC().Format(time.RFC3339)

	key := clusterKey(project, zone, name)
	api.mu.Lock()
	if _, exists := api.clusters[key]; exists {
		api.mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		writeError(w, 409, "ALREADY_EXISTS", fmt.Sprintf("Cluster '%s' already exists", name))
		return
	}
	api.clusters[key] = &cl
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		api.mu.Lock()
		delete(api.clusters, key)
		api.mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist cluster metadata: "+err.Error())
		return
	}

	targetLink := cl.SelfLink
	op, err := api.opMgr.RegisterDurable("container#operation", "CREATE_CLUSTER", targetLink, zone, "")
	if err != nil {
		api.mu.Lock()
		if current := api.clusters[key]; current != nil {
			current.Status = "ERROR"
			current.StatusMessage = "operation registration failed: " + err.Error()
			current.Endpoint = ""
			current.MasterAuth = nil
		}
		api.mu.Unlock()
		if persistErr := api.persistMetadata(); persistErr != nil {
			err = errors.Join(err, fmt.Errorf("persist degraded cluster: %w", persistErr))
		}
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		backendCreated := false
		var ownership *kubeconfigOwnership
		identity := ClusterIdentity{Profile: config.GetProfile(), Project: project, Zone: zone, Cluster: name}
		if api.backend.Enabled() {
			result, err := api.backend.CreateClusterContext(ctx, identity)
			backendCreated = result.Created
			ownership = result.Ownership
			if err != nil {
				var cleanupErr error
				if backendCreated {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
					cleanupErr = cleanupGKEBackend(api.backend, cleanupCtx, identity, ownership != nil)
					cleanupCancel()
				}
				return api.rollbackCreate(key, err, cleanupErr)
			}
		} else {
			// Simulate cluster provision time
			time.Sleep(5 * time.Second)
		}

		registeredNodes, err := api.registerNodes(ctx, http.MethodPost, project, zone, &cl)
		if err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			nodeCleanupErr := api.removeRegisteredNodes(cleanupCtx, project, zone, registeredNodes)
			var backendCleanupErr error
			if backendCreated {
				backendCleanupErr = cleanupGKEBackend(api.backend, cleanupCtx, identity, ownership != nil)
			}
			cleanupCancel()
			return api.rollbackCreate(key, err, errors.Join(nodeCleanupErr, backendCleanupErr))
		}

		api.mu.Lock()
		if c, ok := api.clusters[key]; ok {
			c.Status = "RUNNING"
			c.StatusMessage = ""
		}
		if ownership != nil {
			api.ownerships[key] = ownership
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			nodeCleanupErr := api.removeRegisteredNodes(cleanupCtx, project, zone, registeredNodes)
			var backendCleanupErr error
			if backendCreated {
				backendCleanupErr = cleanupGKEBackend(api.backend, cleanupCtx, identity, ownership != nil)
			}
			cleanupCancel()
			return api.rollbackCreate(key, fmt.Errorf("persist completed cluster: %w", err),
				errors.Join(nodeCleanupErr, backendCleanupErr))
		}
		if ownership != nil {
			if backend, ok := api.backend.(*KindBackend); ok {
				if err := backend.CommitKubeconfigIntent(identity, ownership); err != nil {
					return fmt.Errorf("commit kubeconfig ownership intent: %w", err)
				}
			}
		}

		return nil
	})

	gkeOp := toGkeOperation(op, "CREATE_CLUSTER", project, zone, targetLink)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(gkeOp)
}

func (api *API) getCluster(w http.ResponseWriter, project, zone, name string) {
	key := clusterKey(project, zone, name)
	api.mu.RLock()
	cl, ok := api.clusters[key]
	cl = cloneCluster(cl)
	api.mu.RUnlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("Cluster '%s' not found in zone '%s'", name, zone))
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(cl)
}

func (api *API) listClusters(w http.ResponseWriter, project, zone string) {
	prefix := project + ":" + zone + ":"
	api.mu.RLock()
	items := []*Cluster{}
	for k, v := range api.clusters {
		if strings.HasPrefix(k, prefix) {
			items = append(items, cloneCluster(v))
		}
	}
	api.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"clusters": items,
	})
}

func (api *API) deleteCluster(w http.ResponseWriter, r *http.Request, project, zone, name string) {
	key := clusterKey(project, zone, name)
	api.mu.Lock()
	cl, ok := api.clusters[key]
	if !ok {
		api.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("Cluster '%s' not found", name))
		return
	}
	ownership := api.ownerships[key]
	identity := ClusterIdentity{Profile: config.GetProfile(), Project: project, Zone: zone, Cluster: name}
	if ownership != nil && ownership.hasBackendNonce() && !api.backend.Enabled() {
		api.mu.Unlock()
		api.respondDeleteUnavailable(w, key, identity, ownership, nil)
		return
	}
	api.mu.Unlock()
	if ownership != nil && ownership.hasBackendNonce() {
		if checker, ok := api.backend.(interface {
			CheckDeleteAvailability(context.Context, ClusterIdentity) error
		}); ok {
			if err := checker.CheckDeleteAvailability(r.Context(), identity); isBackendUnavailable(err) {
				api.respondDeleteUnavailable(w, key, identity, ownership, err)
				return
			}
		}
	}

	// Mark as STOPPING to simulate winding down in the UI
	api.mu.Lock()
	if current := api.clusters[key]; current != nil {
		current.Status = "STOPPING"
		cl = current
	}
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist cluster metadata: "+err.Error())
		return
	}

	op, err := api.opMgr.RegisterDurable("container#operation", "DELETE_CLUSTER", cl.SelfLink, zone, "")
	if err != nil {
		api.mu.Lock()
		cl.Status = "ERROR"
		cl.StatusMessage = "delete operation registration failed: " + err.Error()
		api.mu.Unlock()
		if persistErr := api.persistMetadata(); persistErr != nil {
			err = errors.Join(err, fmt.Errorf("persist degraded cluster: %w", persistErr))
		}
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}
	api.opMgr.RunAsync(op.Name, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		api.mu.RLock()
		ownedKind := api.ownerships[key] != nil && api.ownerships[key].hasBackendNonce()
		api.mu.RUnlock()
		if ownedKind || api.backend.Enabled() {
			if err := api.backend.DeleteClusterContext(ctx, identity); err != nil {
				api.markClusterError(key, err)
				return err
			}
		}
		if _, err := api.registerNodes(ctx, http.MethodDelete, project, zone, cl); err != nil {
			api.markClusterError(key, err)
			return err
		}

		// Finally remove from memory
		tombstone := cloneCluster(cl)
		api.mu.Lock()
		ownership := api.ownerships[key]
		delete(api.clusters, key)
		delete(api.ownerships, key)
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			tombstone.Status = "ERROR"
			tombstone.StatusMessage = "backend deleted; metadata removal persistence failed: " + err.Error()
			tombstone.Endpoint = ""
			tombstone.MasterAuth = nil
			api.mu.Lock()
			api.clusters[key] = tombstone
			if ownership != nil {
				api.ownerships[key] = ownership
				if backend, ok := api.backend.(*KindBackend); ok {
					backend.RestoreKubeconfigOwnership(ClusterIdentity{
						Profile: ownership.Profile, Project: ownership.Project,
						Zone: ownership.Zone, Cluster: ownership.Cluster,
					}, ownership)
				}
			}
			api.mu.Unlock()
			return fmt.Errorf("persist deleted cluster: %w", err)
		}
		if ownership != nil {
			if backend, ok := api.backend.(*KindBackend); ok {
				if err := backend.FinalizeDeleteIntent(identity, ownership); err != nil {
					return fmt.Errorf("terminalize durable deletion intent: %w", err)
				}
			}
		}

		return nil
	})
	gkeOp := toGkeOperation(op, "DELETE_CLUSTER", project, zone, cl.SelfLink)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(gkeOp)
}

func (api *API) respondDeleteUnavailable(
	w http.ResponseWriter,
	key string,
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	cause error,
) {
	message := "Kind backend cleanup is temporarily unavailable; retry deletion later"
	if cause != nil {
		message += ": " + cause.Error()
	}
	api.mu.Lock()
	if cluster := api.clusters[key]; cluster != nil {
		cluster.Status = "ERROR"
		cluster.StatusMessage = "delete cleanup pending: " + message
	}
	api.mu.Unlock()
	var intentErr error
	if backend, ok := api.backend.(*KindBackend); ok {
		intentErr = backend.MarkDeleteUnavailable(identity)
	} else {
		intentErr = writeKubeconfigIntentError(
			identity, ownership, intentDeletePending, "UNAVAILABLE: "+message)
	}
	if persistErr := api.persistMetadata(); persistErr != nil {
		intentErr = errors.Join(intentErr, fmt.Errorf("persist retryable delete state: %w", persistErr))
	}
	if intentErr != nil {
		message += ": " + intentErr.Error()
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", message)
}

// ─────────────────────────────────────────────────────────────────────────────
// Operations
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	zone := firstOf(extractSegmentAfter(path, "zones"), extractSegmentAfter(path, "locations"))
	opName := extractSegmentAfter(path, "operations")

	op := api.opMgr.Get(opName)
	if op == nil {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Operation not found: "+opName)
		return
	}
	gkeOp := toGkeOperation(op, op.OperationType, project, zone, op.TargetLink)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(gkeOp)
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func toGkeOperation(op *orchestrator.Operation, opType, project, zone, targetLink string) *GkeOperation {
	status := "PENDING"
	switch op.Status {
	case orchestrator.StatusRunning:
		status = "RUNNING"
	case orchestrator.StatusDone:
		status = "DONE"
	}
	selfLink := fmt.Sprintf(
		"https://container.googleapis.com/v1/projects/%s/zones/%s/operations/%s",
		project, zone, op.Name)
	return &GkeOperation{
		Name:          op.Name,
		Zone:          zone,
		OperationType: opType,
		Status:        status,
		SelfLink:      selfLink,
		TargetLink:    targetLink,
		StartTime:     op.StartTime,
		EndTime:       op.EndTime,
		Error:         op.Error,
	}
}

func (api *API) registerNodes(ctx context.Context, method, project, zone string, cluster *Cluster) ([]string, error) {
	api.configMu.RLock()
	gatewayURL := api.gatewayURL
	client := api.httpClient
	api.configMu.RUnlock()
	if gatewayURL == "" || client == nil {
		return nil, fmt.Errorf("MiniSky gateway URL is not configured")
	}
	kindBase := sanitizeKindName(cluster.Name)
	nodeNames := []string{kindBase + "-control-plane"}
	for i := 1; i <= cluster.InitialNodeCount; i++ {
		if i == 1 {
			nodeNames = append(nodeNames, kindBase+"-worker")
		} else {
			nodeNames = append(nodeNames, fmt.Sprintf("%s-worker%d", kindBase, i))
		}
	}
	var completed []string
	for _, nodeName := range nodeNames {
		url := fmt.Sprintf("%s/compute/v1/projects/%s/zones/%s/instances", gatewayURL, project, zone)
		var body strings.Reader
		if method == http.MethodPost {
			payload, err := json.Marshal(map[string]interface{}{
				"name": nodeName, "machineType": cluster.NodeConfig.MachineType,
				"description": "GKE Managed Node",
				"labels":      map[string]string{"managed-by": "gke", "gke-cluster": cluster.Name},
			})
			if err != nil {
				return completed, err
			}
			body = *strings.NewReader(string(payload))
		} else {
			url += "/" + nodeName
		}
		req, err := http.NewRequestWithContext(ctx, method, url, &body)
		if err != nil {
			return completed, err
		}
		req.Host = "compute.googleapis.com"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Minisky-GKE-Bypass", "true")
		resp, err := client.Do(req)
		if err != nil {
			return completed, fmt.Errorf("register managed node %s: %w", nodeName, err)
		}
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return completed, fmt.Errorf("register managed node %s: gateway returned %s", nodeName, resp.Status)
		}
		completed = append(completed, nodeName)
	}
	return completed, nil
}

func (api *API) removeRegisteredNodes(ctx context.Context, project, zone string, names []string) error {
	api.configMu.RLock()
	gatewayURL := api.gatewayURL
	client := api.httpClient
	api.configMu.RUnlock()
	if gatewayURL == "" || client == nil {
		return fmt.Errorf("MiniSky gateway URL is not configured")
	}
	var cleanupErr error
	for i := len(names) - 1; i >= 0; i-- {
		url := fmt.Sprintf("%s/compute/v1/projects/%s/zones/%s/instances/%s", gatewayURL, project, zone, names[i])
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err == nil {
			req.Host = "compute.googleapis.com"
			req.Header.Set("X-Minisky-GKE-Bypass", "true")
			var resp *http.Response
			resp, err = client.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					err = fmt.Errorf("gateway returned %s", resp.Status)
				}
			}
		}
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove managed node %s: %w", names[i], err))
		}
	}
	return cleanupErr
}

func cleanupGKEBackend(
	backend gkeBackend,
	ctx context.Context,
	identity ClusterIdentity,
	hasOwnership bool,
) error {
	if hasOwnership {
		return backend.DeleteClusterContext(ctx, identity)
	}
	if cleanup, ok := backend.(interface {
		CleanupClusterContext(context.Context, ClusterIdentity) error
	}); ok {
		return cleanup.CleanupClusterContext(ctx, identity)
	}
	return backend.DeleteClusterContext(ctx, identity)
}

func (api *API) rollbackCreate(key string, cause, cleanupErr error) error {
	api.mu.RLock()
	tombstone := cloneCluster(api.clusters[key])
	api.mu.RUnlock()
	if cleanupErr == nil {
		api.removeCluster(key)
		if err := api.persistMetadata(); err == nil {
			return cause
		} else {
			cleanupErr = fmt.Errorf("persist rollback: %w", err)
		}
	}
	combined := errors.Join(cause, cleanupErr)
	api.mu.Lock()
	if tombstone != nil {
		tombstone.Status = "ERROR"
		tombstone.StatusMessage = combined.Error()
		tombstone.Endpoint = ""
		tombstone.MasterAuth = nil
		api.clusters[key] = tombstone
	}
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		combined = errors.Join(combined, fmt.Errorf("persist degraded state: %w", err))
	}
	return combined
}

func (api *API) removeCluster(key string) {
	api.mu.Lock()
	delete(api.clusters, key)
	delete(api.ownerships, key)
	api.mu.Unlock()
}

func (api *API) markClusterError(key string, err error) {
	api.mu.Lock()
	if cluster := api.clusters[key]; cluster != nil {
		cluster.Status = "ERROR"
		cluster.StatusMessage = err.Error()
	}
	api.mu.Unlock()
	if persistErr := api.persistMetadata(); persistErr != nil {
		log.Printf("[Shim: GKE] persist failed cluster: %v", persistErr)
	}
}

func cloneCluster(cluster *Cluster) *Cluster {
	if cluster == nil {
		return nil
	}
	clone := *cluster
	if cluster.NodeConfig != nil {
		nodeConfig := *cluster.NodeConfig
		nodeConfig.OauthScopes = append([]string(nil), cluster.NodeConfig.OauthScopes...)
		nodeConfig.Tags = append([]string(nil), cluster.NodeConfig.Tags...)
		if cluster.NodeConfig.Labels != nil {
			nodeConfig.Labels = make(map[string]string, len(cluster.NodeConfig.Labels))
			for key, value := range cluster.NodeConfig.Labels {
				nodeConfig.Labels[key] = value
			}
		}
		clone.NodeConfig = &nodeConfig
	}
	if cluster.MasterAuth != nil {
		auth := *cluster.MasterAuth
		clone.MasterAuth = &auth
	}
	return &clone
}

func clusterKey(project, zone, name string) string {
	return project + ":" + zone + ":" + name
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

func firstOf(a, b string) string {
	if a != "" {
		return a
	}
	return b
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
