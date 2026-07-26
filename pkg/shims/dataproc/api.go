package dataproc

import (
	"bytes"
	"context"
	"crypto/sha256"
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

const dataprocStateEntry = "dataproc/metadata"

func init() {
	state.MustRegisterEntryValidator(dataprocStateEntry, state.StrictEntryValidator[dataprocMetadata](nil))
	registry.Register("dataproc.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr, ctx.SvcMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

// Cluster mirrors the Dataproc v1 Cluster resource.
type Cluster struct {
	ProjectId     string            `json:"projectId"`
	ClusterName   string            `json:"clusterName"`
	ClusterUuid   string            `json:"clusterUuid"`
	Config        ClusterConfig     `json:"config"`
	Status        ClusterStatus     `json:"status"`
	StatusHistory []ClusterStatus   `json:"statusHistory,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

type ClusterConfig struct {
	MasterConfig   *InstanceGroupConfig `json:"masterConfig,omitempty"`
	WorkerConfig   *InstanceGroupConfig `json:"workerConfig,omitempty"`
	SoftwareConfig *SoftwareConfig      `json:"softwareConfig,omitempty"`
}

type InstanceGroupConfig struct {
	NumInstances   int         `json:"numInstances"`
	MachineTypeUri string      `json:"machineTypeUri"`
	DiskConfig     *DiskConfig `json:"diskConfig,omitempty"`
}

type DiskConfig struct {
	BootDiskSizeGb int `json:"bootDiskSizeGb"`
}

type SoftwareConfig struct {
	ImageVersion string            `json:"imageVersion"`
	Properties   map[string]string `json:"properties,omitempty"`
}

type ClusterStatus struct {
	State          string `json:"state"` // CREATING, RUNNING, DELETING, ERROR
	Detail         string `json:"detail,omitempty"`
	StateStartTime string `json:"stateStartTime"`
}

// Job mirrors the Dataproc v1 Job resource.
type Job struct {
	Reference  JobReference      `json:"reference"`
	Placement  JobPlacement      `json:"placement"`
	Status     JobStatus         `json:"status"`
	SparkJob   *SparkJob         `json:"sparkJob,omitempty"`
	PysparkJob *PySparkJob       `json:"pysparkJob,omitempty"`
	HiveJob    *HiveJob          `json:"hiveJob,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

type JobReference struct {
	ProjectId string `json:"projectId"`
	JobId     string `json:"jobId"`
}

type JobPlacement struct {
	ClusterName string `json:"clusterName"`
	ClusterUuid string `json:"clusterUuid,omitempty"`
}

type JobStatus struct {
	State          string `json:"state"` // PENDING, SETUP_DONE, RUNNING, DONE, ERROR
	StateStartTime string `json:"stateStartTime"`
	Details        string `json:"details,omitempty"`
}

type SparkJob struct {
	MainClass      string   `json:"mainClass,omitempty"`
	MainJarFileUri string   `json:"mainJarFileUri,omitempty"`
	Args           []string `json:"args,omitempty"`
	JarFileUris    []string `json:"jarFileUris,omitempty"`
}

type PySparkJob struct {
	MainPythonFileUri string   `json:"mainPythonFileUri"`
	Args              []string `json:"args,omitempty"`
	PythonFileUris    []string `json:"pythonFileUris,omitempty"`
}

type HiveJob struct {
	QueryList    *QueryList `json:"queryList,omitempty"`
	QueryFileUri string     `json:"queryFileUri,omitempty"`
}

type QueryList struct {
	Queries []string `json:"queries"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API is the high-fidelity Dataproc v1 shim.
type API struct {
	mu              sync.RWMutex
	mutationMu      sync.Mutex
	opMgr           *orchestrator.OperationManager
	svcMgr          dataprocServiceManager
	store           dataprocStateStore
	initErr         error
	clusters        map[string]*Cluster // key: project:region:clusterName
	jobs            map[string]*Job     // key: project:region:jobId
	operationRunner func(string, func() error)
	jobRunner       func(func())
	afterAdmission  func()
}

type dataprocStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type dataprocServiceManager interface {
	ProvisionComputeVM(context.Context, string, string, string, []string, []string, []string) error
	DeleteComputeVM(string) error
	RunCommandInContainer(string, []string) (string, error)
}

type dataprocContextualDeleter interface {
	DeleteComputeVMContext(context.Context, string) error
}

type dataprocMetadata struct {
	Clusters map[string]*Cluster `json:"clusters"`
	Jobs     map[string]*Job     `json:"jobs"`
}

func NewAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Dataproc] persistence degraded: %v", err)
		api := newAPI(opMgr, svcMgr, nil)
		api.initErr = fmt.Errorf("open Dataproc state: %w", err)
		return api
	}
	api, err := NewAPIWithStore(opMgr, svcMgr, store)
	if err != nil {
		log.Printf("[Shim: Dataproc] state rehydration failed: %v", err)
		api = newAPI(opMgr, svcMgr, store)
		api.initErr = err
	}
	return api
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, svcMgr dataprocServiceManager, store dataprocStateStore) (*API, error) {
	api := newAPI(opMgr, svcMgr, store)
	if store == nil {
		return api, nil
	}
	var persisted dataprocMetadata
	if err := store.Load(dataprocStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Dataproc metadata: %w", err)
	}
	previous := cloneDataprocMetadata(persisted)
	if err := normalizeDataprocMetadata(&persisted, true); err != nil {
		return nil, fmt.Errorf("load Dataproc metadata: %w", err)
	}
	if !dataprocMetadataEqual(previous, persisted) {
		if err := api.commitMetadata(previous, persisted); err != nil {
			return nil, fmt.Errorf("persist Dataproc restart normalization: %w", err)
		}
	} else {
		api.replaceMetadata(persisted)
	}
	return api, nil
}

func newAPI(opMgr *orchestrator.OperationManager, svcMgr dataprocServiceManager, store dataprocStateStore) *API {
	api := &API{
		opMgr:    opMgr,
		svcMgr:   svcMgr,
		store:    store,
		clusters: make(map[string]*Cluster),
		jobs:     make(map[string]*Job),
	}
	if opMgr != nil {
		api.operationRunner = opMgr.RunAsync
	}
	api.jobRunner = func(work func()) { go work() }
	return api
}

// ServeHTTP dispatches Dataproc v1 paths.
//
// Supported paths (dataproc.googleapis.com):
//
//	POST   /v1/projects/{project}/regions/{region}/clusters
//	GET    /v1/projects/{project}/regions/{region}/clusters
//	GET    /v1/projects/{project}/regions/{region}/clusters/{cluster}
//	DELETE /v1/projects/{project}/regions/{region}/clusters/{cluster}
//	POST   /v1/projects/{project}/regions/{region}/jobs:submit
//	GET    /v1/projects/{project}/regions/{region}/jobs
//	GET    /v1/projects/{project}/regions/{region}/jobs/{jobId}
//	GET    /v1/projects/{project}/regions/{region}/operations/{operation}
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Dataproc] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initializationError() != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Dataproc persistence is unavailable")
		return
	}
	if api.afterAdmission != nil {
		api.afterAdmission()
	}

	path := r.URL.Path

	switch {
	case strings.Contains(path, "/operations/"):
		api.getOperation(w, r, path)
	case strings.Contains(path, "/jobs"):
		api.routeJobs(w, r, path)
	case strings.Contains(path, "/clusters"):
		api.routeClusters(w, r, path)
	default:
		writeError(w, 404, "NOT_FOUND", "Dataproc resource not found: "+path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Clusters
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeClusters(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	region := extractSegmentAfter(path, "regions")
	clusterName := extractSegmentAfter(path, "clusters")

	switch r.Method {
	case http.MethodPost:
		api.createCluster(w, r, project, region)
	case http.MethodGet:
		if clusterName != "" {
			api.getCluster(w, project, region, clusterName)
		} else {
			api.listClusters(w, project, region)
		}
	case http.MethodDelete:
		api.deleteCluster(w, r, project, region, clusterName)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createCluster(w http.ResponseWriter, r *http.Request, project, region string) {
	var body struct {
		ClusterName string            `json:"clusterName"`
		Config      ClusterConfig     `json:"config"`
		Labels      map[string]string `json:"labels"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if body.ClusterName == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "clusterName is required")
		return
	}

	// Defaults
	cfg := body.Config
	if cfg.MasterConfig == nil {
		cfg.MasterConfig = &InstanceGroupConfig{
			NumInstances:   1,
			MachineTypeUri: "n1-standard-4",
			DiskConfig:     &DiskConfig{BootDiskSizeGb: 500},
		}
	}
	if cfg.WorkerConfig == nil {
		cfg.WorkerConfig = &InstanceGroupConfig{
			NumInstances:   2,
			MachineTypeUri: "n1-standard-4",
			DiskConfig:     &DiskConfig{BootDiskSizeGb: 500},
		}
	}
	if cfg.SoftwareConfig == nil {
		cfg.SoftwareConfig = &SoftwareConfig{ImageVersion: "2.1-debian11"}
	}

	clusterUuid := fmt.Sprintf("%x-%x", time.Now().UnixNano(), time.Now().UnixNano()/2)
	cl := &Cluster{
		ProjectId:   project,
		ClusterName: body.ClusterName,
		ClusterUuid: clusterUuid,
		Config:      cfg,
		Labels:      body.Labels,
		Status: ClusterStatus{
			State:          "CREATING",
			StateStartTime: time.Now().UTC().Format(time.RFC3339),
		},
	}

	targetLink := fmt.Sprintf(
		"https://dataproc.googleapis.com/v1/projects/%s/regions/%s/clusters/%s",
		project, region, body.ClusterName)
	key := clusterKey(project, region, body.ClusterName)
	api.mutationMu.Lock()
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		return
	}
	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	if _, exists := previous.Clusters[key]; exists {
		api.mutationMu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Cluster "+body.ClusterName+" already exists")
		return
	}
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		return
	}
	op, err := api.opMgr.RegisterDurable("dataproc#operation", "CREATE", targetLink, "", region)
	if err != nil {
		api.mutationMu.Unlock()
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}

	snapshot := cloneDataprocMetadata(previous)
	snapshot.Clusters[key] = cloneCluster(cl)
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "Dataproc persistence became unavailable")
		return
	}
	if err := api.commitMetadata(previous, snapshot); err != nil {
		api.mutationMu.Unlock()
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "Dataproc cluster metadata was not persisted")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Dataproc cluster metadata")
		return
	}
	api.mutationMu.Unlock()

	work := func() error {
		clusterStr := body.ClusterName
		reg := config.GetImageRegistry()
		created := make([]string, 0, 1+cfg.WorkerConfig.NumInstances)
		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("Dataproc persistence unavailable before provisioning: %w", err)
		}

		// Provision the Master Node
		masterImage := reg.Dataproc.DefaultImage
		reqVersion := cfg.SoftwareConfig.ImageVersion
		for _, v := range reg.Dataproc.Versions {
			if strings.Contains(reqVersion, v.Version) {
				masterImage = v.Image
				break
			}
		}

		// Connectivity configuration for Cloud Storage and BigQuery emulators
		connectivityEnv := []string{
			"SPARK_HADOOP_fs_gs_impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
			"SPARK_HADOOP_google_cloud_auth_service_account_enable=false",
			"SPARK_HADOOP_fs_gs_endpoint=http://minisky-gcs:4443",
			"BIGQUERY_REST_ENDPOINT=http://host.docker.internal:8080/bigquery/v2",
		}

		masterName := dataprocDockerName(project, region, clusterStr, "m", 0)
		if api.svcMgr == nil {
			return fmt.Errorf("Dataproc Docker backend is unavailable")
		}
		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("Dataproc persistence unavailable before master provisioning: %w", err)
		}
		if err := api.svcMgr.ProvisionComputeVM(context.Background(), masterName, masterImage, "default", reg.Dataproc.MasterPorts, connectivityEnv, []string{"tail", "-f", "/dev/null"}); err != nil {
			provisionErr := fmt.Errorf("provision Dataproc master: %w", err)
			compensationErr := api.compensateProvisionedCluster(append(created, masterName))
			detail := "Master provisioning failed: " + err.Error()
			if compensationErr != nil {
				detail += "; compensation ambiguous: " + compensationErr.Error()
			}
			if persistErr := api.setClusterStatus(key, "ERROR", detail); persistErr != nil {
				api.degrade(persistErr)
				provisionErr = errors.Join(provisionErr, persistErr)
			}
			if compensationErr != nil {
				api.degrade(fmt.Errorf("Dataproc master compensation ambiguity: %w", compensationErr))
				return errors.Join(provisionErr, fmt.Errorf("Dataproc compensation: %w", compensationErr))
			}
			return provisionErr
		}
		created = append(created, masterName)

		// Provision Worker Nodes
		numWorkers := 2
		if cfg.WorkerConfig != nil {
			numWorkers = cfg.WorkerConfig.NumInstances
		}
		for i := 0; i < numWorkers; i++ {
			workerName := dataprocDockerName(project, region, clusterStr, "w", i)
			if err := api.PersistenceError(); err != nil {
				compensationErr := api.compensateProvisionedCluster(created)
				return errors.Join(
					fmt.Errorf("Dataproc persistence unavailable during provisioning: %w", err),
					compensationErr,
				)
			}
			if err := api.svcMgr.ProvisionComputeVM(context.Background(), workerName, masterImage, "default", []string{}, connectivityEnv, []string{"tail", "-f", "/dev/null"}); err != nil {
				provisionErr := fmt.Errorf("provision Dataproc worker %d: %w", i, err)
				compensationErr := api.compensateProvisionedCluster(append(created, workerName))
				detail := provisionErr.Error()
				if compensationErr != nil {
					detail += "; compensation ambiguous: " + compensationErr.Error()
				}
				if persistErr := api.setClusterStatus(key, "ERROR", detail); persistErr != nil {
					api.degrade(persistErr)
					provisionErr = errors.Join(provisionErr, persistErr)
				}
				if compensationErr != nil {
					api.degrade(fmt.Errorf("Dataproc worker compensation ambiguity: %w", compensationErr))
					return errors.Join(provisionErr, fmt.Errorf("Dataproc compensation: %w", compensationErr))
				}
				return provisionErr
			}
			created = append(created, workerName)
		}
		if err := api.setClusterStatus(key, "RUNNING", ""); err != nil {
			compensationErr := api.compensateProvisionedCluster(created)
			compensationState := errors.New("Dataproc backends were compensated after RUNNING persistence failure; durable cluster metadata may require restart normalization")
			if compensationErr != nil {
				compensationState = fmt.Errorf("Dataproc RUNNING-save compensation was incomplete: %w", compensationErr)
			}
			api.degrade(errors.Join(err, compensationState))
			if compensationErr != nil {
				return errors.Join(err, fmt.Errorf("Dataproc RUNNING-save compensation: %w", compensationErr))
			}
			return errors.Join(err, compensationState)
		}
		return nil
	}
	if api.operationRunner != nil {
		api.operationRunner(op.Name, work)
	}

	// Dataproc uses google.longrunning.Operation format
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(toLRO(op, project, region))
}

func (api *API) getCluster(w http.ResponseWriter, project, region, name string) {
	key := clusterKey(project, region, name)
	api.mu.RLock()
	cl := cloneCluster(api.clusters[key])
	api.mu.RUnlock()

	if cl == nil {
		writeError(w, 404, "NOT_FOUND", "Cluster "+name+" not found")
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(cl)
}

func (api *API) listClusters(w http.ResponseWriter, project, region string) {
	prefix := clusterKey(project, region, "")
	api.mu.RLock()
	items := []*Cluster{}
	for k, v := range api.clusters {
		if strings.HasPrefix(k, prefix) {
			items = append(items, cloneCluster(v))
		}
	}
	api.mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"clusters": items})
}

func (api *API) deleteCluster(w http.ResponseWriter, r *http.Request, project, region, name string) {
	key := clusterKey(project, region, name)
	api.mutationMu.Lock()
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		return
	}
	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	cl := cloneCluster(previous.Clusters[key])
	if cl == nil {
		api.mutationMu.Unlock()
		writeError(w, 404, "NOT_FOUND", "Cluster "+name+" not found")
		return
	}

	numWorkers := 2
	if cl.Config.WorkerConfig != nil {
		numWorkers = cl.Config.WorkerConfig.NumInstances
	}

	targetLink := fmt.Sprintf(
		"https://dataproc.googleapis.com/v1/projects/%s/regions/%s/clusters/%s",
		project, region, name)
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		return
	}
	op, err := api.opMgr.RegisterDurable("dataproc#operation", "DELETE", targetLink, "", region)
	if err != nil {
		api.mutationMu.Unlock()
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}

	deleting := cloneDataprocMetadata(previous)
	deletingCluster := deleting.Clusters[key]
	deletingCluster.Status = ClusterStatus{
		State: "DELETING", StateStartTime: time.Now().UTC().Format(time.RFC3339),
	}
	deletingCluster.StatusHistory = append(deletingCluster.StatusHistory, deletingCluster.Status)
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		_ = api.opMgr.FailDurable(op.Name, http.StatusServiceUnavailable, "Dataproc persistence became unavailable")
		return
	}
	if err := api.commitMetadata(previous, deleting); err != nil {
		api.mutationMu.Unlock()
		_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "Dataproc deletion metadata was not persisted")
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Dataproc deletion metadata")
		return
	}
	api.mutationMu.Unlock()
	work := func() error {
		// Teardown physical containers
		if api.svcMgr == nil {
			return fmt.Errorf("Dataproc Docker backend is unavailable")
		}
		if err := api.PersistenceError(); err != nil {
			return fmt.Errorf("Dataproc persistence unavailable before deletion: %w", err)
		}
		if err := api.svcMgr.DeleteComputeVM(dataprocDockerName(project, region, name, "m", 0)); err != nil {
			deleteErr := fmt.Errorf("delete Dataproc master: %w", err)
			if persistErr := api.setClusterStatus(key, "ERROR", "Master deletion failed: "+err.Error()); persistErr != nil {
				api.degrade(persistErr)
				return errors.Join(deleteErr, persistErr)
			}
			return deleteErr
		}
		for i := 0; i < numWorkers; i++ {
			if err := api.PersistenceError(); err != nil {
				return fmt.Errorf("Dataproc persistence unavailable during deletion: %w", err)
			}
			if err := api.svcMgr.DeleteComputeVM(dataprocDockerName(project, region, name, "w", i)); err != nil {
				deleteErr := fmt.Errorf("delete Dataproc worker %d: %w", i, err)
				if persistErr := api.setClusterStatus(key, "ERROR", "Worker deletion failed: "+err.Error()); persistErr != nil {
					api.degrade(persistErr)
					return errors.Join(deleteErr, persistErr)
				}
				return deleteErr
			}
		}
		return api.deleteClusterMetadata(key)
	}
	if api.operationRunner != nil {
		api.operationRunner(op.Name, work)
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(toLRO(op, project, region))
}

// ─────────────────────────────────────────────────────────────────────────────
// Jobs
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeJobs(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	region := extractSegmentAfter(path, "regions")

	// jobs:submit is a POST with a colon-verb
	if strings.HasSuffix(strings.TrimRight(path, "/"), ":submit") || r.Method == http.MethodPost {
		api.submitJob(w, r, project, region)
		return
	}

	jobId := extractSegmentAfter(path, "jobs")
	if r.Method == http.MethodGet && jobId != "" {
		api.getJob(w, project, region, jobId)
		return
	}

	if r.Method == http.MethodGet {
		api.listJobs(w, project, region)
		return
	}

	writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
}

func (api *API) submitJob(w http.ResponseWriter, r *http.Request, project, region string) {
	var body struct {
		Job Job `json:"job"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}

	job := body.Job
	supportedTypes := 0
	if job.PysparkJob != nil {
		supportedTypes++
	}
	if job.SparkJob != nil {
		supportedTypes++
	}
	if supportedTypes != 1 || job.HiveJob != nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "only Spark and PySpark jobs are supported")
		return
	}
	clusterIdentity := clusterKey(project, region, job.Placement.ClusterName)
	api.mu.RLock()
	cluster := cloneCluster(api.clusters[clusterIdentity])
	api.mu.RUnlock()
	if cluster == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Cluster "+job.Placement.ClusterName+" not found")
		return
	}
	if cluster.Status.State != "RUNNING" {
		writeError(w, http.StatusConflict, "FAILED_PRECONDITION", "Cluster "+job.Placement.ClusterName+" is not running")
		return
	}
	if job.Placement.ClusterUuid != "" && job.Placement.ClusterUuid != cluster.ClusterUuid {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Cluster identity does not match")
		return
	}
	job.Placement.ClusterUuid = cluster.ClusterUuid
	jobId := fmt.Sprintf("job-%x", time.Now().UnixNano())
	job.Reference.ProjectId = project
	job.Reference.JobId = jobId
	job.Status = JobStatus{
		State:          "PENDING",
		StateStartTime: time.Now().UTC().Format(time.RFC3339),
	}

	key := jobKey(project, region, jobId)
	api.mutationMu.Lock()
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		return
	}
	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	snapshot := cloneDataprocMetadata(previous)
	snapshot.Jobs[key] = cloneJob(&job)
	if api.rejectDegradedMutation(w) {
		api.mutationMu.Unlock()
		return
	}
	if err := api.commitMetadata(previous, snapshot); err != nil {
		api.mutationMu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Dataproc job metadata")
		return
	}
	api.mutationMu.Unlock()

	// Drive job state: PENDING → RUNNING → DONE
	work := func() {
		time.Sleep(500 * time.Millisecond)
		api.mu.RLock()
		j := cloneJob(api.jobs[key])
		api.mu.RUnlock()
		if j == nil {
			return
		}
		j.Status.State = "RUNNING"
		j.Status.StateStartTime = time.Now().UTC().Format(time.RFC3339)
		clusterName := j.Placement.ClusterName
		if err := api.persistJob(key, j); err != nil {
			api.degrade(err)
			return
		}

		api.mu.RLock()
		currentCluster := cloneCluster(api.clusters[clusterIdentity])
		api.mu.RUnlock()
		if currentCluster == nil || currentCluster.Status.State != "RUNNING" ||
			currentCluster.ClusterUuid != j.Placement.ClusterUuid {
			j.Status.State = "ERROR"
			j.Status.Details = "Dataproc cluster identity changed before execution"
			j.Status.StateStartTime = time.Now().UTC().Format(time.RFC3339)
			if err := api.persistJob(key, j); err != nil {
				api.degrade(err)
			}
			return
		}

		masterName := dataprocDockerName(project, region, clusterName, "m", 0)

		var cmd []string
		if j.PysparkJob != nil {
			cmd = []string{"spark-submit", "--master", "spark://localhost:7077", j.PysparkJob.MainPythonFileUri}
			cmd = append(cmd, j.PysparkJob.Args...)
		} else if j.SparkJob != nil {
			cmd = []string{"spark-submit", "--master", "spark://localhost:7077"}
			if j.SparkJob.MainClass != "" {
				cmd = append(cmd, "--class", j.SparkJob.MainClass)
			}
			cmd = append(cmd, j.SparkJob.MainJarFileUri)
			cmd = append(cmd, j.SparkJob.Args...)
		} else {
			// No-op for unsupported types or mocks
			time.Sleep(2 * time.Second)
		}

		if len(cmd) > 0 {
			if api.svcMgr == nil {
				j.Status.State = "ERROR"
				j.Status.Details = "Dataproc Docker backend is unavailable"
				j.Status.StateStartTime = time.Now().UTC().Format(time.RFC3339)
				if err := api.persistJob(key, j); err != nil {
					api.degrade(err)
				}
				return
			}
			if err := api.PersistenceError(); err != nil {
				return
			}
			out, err := api.svcMgr.RunCommandInContainer(masterName, cmd)
			if err != nil {
				j.Status.State = "ERROR"
				j.Status.Details = fmt.Sprintf("Spark-submit failed: %v\nOutput: %s", err, out)
			} else {
				j.Status.State = "DONE"
				j.Status.Details = out
			}
			j.Status.StateStartTime = time.Now().UTC().Format(time.RFC3339)
			if err := api.persistJob(key, j); err != nil {
				api.degrade(err)
			}
		} else {
			j.Status.State = "DONE"
			j.Status.StateStartTime = time.Now().UTC().Format(time.RFC3339)
			if err := api.persistJob(key, j); err != nil {
				api.degrade(err)
			}
		}
	}
	if api.jobRunner != nil {
		api.jobRunner(work)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&job)
}

func (api *API) getJob(w http.ResponseWriter, project, region, jobId string) {
	key := jobKey(project, region, jobId)
	api.mu.RLock()
	job := cloneJob(api.jobs[key])
	api.mu.RUnlock()

	if job == nil {
		writeError(w, 404, "NOT_FOUND", "Job "+jobId+" not found")
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)
}

func (api *API) listJobs(w http.ResponseWriter, project, region string) {
	prefix := jobKey(project, region, "")
	api.mu.RLock()
	items := []*Job{}
	for k, v := range api.jobs {
		if strings.HasPrefix(k, prefix) {
			items = append(items, cloneJob(v))
		}
	}
	api.mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{"jobs": items})
}

// ─────────────────────────────────────────────────────────────────────────────
// Operations
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	region := extractSegmentAfter(path, "regions")
	opName := extractSegmentAfter(path, "operations")

	op := api.opMgr.Get(opName)
	targetPrefix := fmt.Sprintf(
		"https://dataproc.googleapis.com/v1/projects/%s/regions/%s/",
		project,
		region,
	)
	if op == nil || op.Kind != "dataproc#operation" || op.Region != region ||
		!strings.HasPrefix(op.TargetLink, targetPrefix) {
		writeError(w, 404, "NOT_FOUND", "Operation not found: "+opName)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(toLRO(op, project, region))
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func toLRO(op *orchestrator.Operation, project, region string) map[string]interface{} {
	return map[string]interface{}{
		"name": fmt.Sprintf("projects/%s/regions/%s/operations/%s", project, region, op.Name),
		"metadata": map[string]interface{}{
			"@type":       "type.googleapis.com/google.cloud.dataproc.v1.ClusterOperationMetadata",
			"clusterName": "",
			"status": map[string]interface{}{
				"state": string(op.Status),
			},
		},
		"done":  op.Done,
		"error": op.Error,
	}
}

func clusterKey(project, region, name string) string { return project + ":" + region + ":" + name }
func jobKey(project, region, id string) string       { return project + ":" + region + ":" + id }

func dataprocDockerName(project, region, cluster, role string, index int) string {
	identity := config.GetProfile() + "\x00" + clusterKey(project, region, cluster)
	sum := sha256.Sum256([]byte(identity))
	name := fmt.Sprintf("minisky-dataproc-%x-%s", sum[:8], role)
	if role == "w" {
		name += fmt.Sprintf("-%d", index)
	}
	return name
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

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{"code": code, "status": status, "message": message},
	})
}

func (api *API) snapshotLocked() dataprocMetadata {
	payload, _ := json.Marshal(dataprocMetadata{Clusters: api.clusters, Jobs: api.jobs})
	var snapshot dataprocMetadata
	_ = json.Unmarshal(payload, &snapshot)
	_ = normalizeDataprocMetadata(&snapshot, false)
	return snapshot
}

func (api *API) commitMetadata(previous, candidate dataprocMetadata) error {
	_, err := api.commitMetadataOutcome(previous, candidate)
	return err
}

func (api *API) commitMetadataOutcome(previous, candidate dataprocMetadata) (bool, error) {
	if api.store == nil {
		api.replaceMetadata(candidate)
		return false, nil
	}
	saveErr := api.store.Save(dataprocStateEntry, candidate)
	if saveErr == nil {
		api.replaceMetadata(candidate)
		return false, nil
	}
	var observed dataprocMetadata
	loadErr := api.store.Load(dataprocStateEntry, &observed)
	if loadErr == nil {
		if err := normalizeDataprocMetadata(&observed, false); err != nil {
			loadErr = err
		} else {
			switch {
			case dataprocMetadataEqual(observed, candidate):
				api.replaceMetadata(candidate)
				return true, nil
			case dataprocMetadataEqual(observed, previous):
				return true, saveErr
			}
		}
	} else if errors.Is(loadErr, state.ErrNotFound) && dataprocMetadataEmpty(previous) {
		return true, saveErr
	}
	readbackErr := loadErr
	if readbackErr == nil {
		readbackErr = errors.New("readback differed from previous and candidate snapshots")
	}
	ambiguous := errors.Join(saveErr, fmt.Errorf("read back Dataproc metadata: %w", readbackErr))
	api.degrade(ambiguous)
	return true, ambiguous
}

func (api *API) replaceMetadata(metadata dataprocMetadata) {
	api.mu.Lock()
	api.clusters = metadata.Clusters
	api.jobs = metadata.Jobs
	api.mu.Unlock()
}

func (api *API) setClusterStatus(key, status, detail string) error {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("Dataproc persistence unavailable: %w", err)
	}
	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	snapshot := cloneDataprocMetadata(previous)
	cluster := snapshot.Clusters[key]
	if cluster == nil {
		return fmt.Errorf("Dataproc cluster disappeared")
	}
	cluster.Status = ClusterStatus{
		State: status, Detail: detail, StateStartTime: time.Now().UTC().Format(time.RFC3339),
	}
	cluster.StatusHistory = append(cluster.StatusHistory, cluster.Status)
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("Dataproc persistence unavailable before cluster outcome save: %w", err)
	}
	saveFailed, err := api.commitMetadataOutcome(previous, snapshot)
	if saveFailed {
		degradation := err
		if degradation == nil {
			degradation = errors.New("Dataproc cluster outcome save required readback reconciliation")
		}
		api.degrade(degradation)
	}
	if err != nil {
		return fmt.Errorf("persist Dataproc cluster outcome: %w", err)
	}
	if saveFailed {
		return errors.New("persist Dataproc cluster outcome: save returned an error")
	}
	return nil
}

func (api *API) compensateProvisionedCluster(created []string) error {
	var compensationErr error
	for index := len(created) - 1; index >= 0; index-- {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var err error
		if contextual, ok := api.svcMgr.(dataprocContextualDeleter); ok {
			err = contextual.DeleteComputeVMContext(ctx, created[index])
		} else {
			err = api.svcMgr.DeleteComputeVM(created[index])
		}
		cancel()
		if err != nil {
			compensationErr = errors.Join(compensationErr,
				fmt.Errorf("delete %s: %w", created[index], err))
		}
	}
	return compensationErr
}

func (api *API) deleteClusterMetadata(key string) error {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("Dataproc persistence unavailable: %w", err)
	}
	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	snapshot := cloneDataprocMetadata(previous)
	delete(snapshot.Clusters, key)
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("Dataproc persistence unavailable before deletion outcome save: %w", err)
	}
	saveFailed, err := api.commitMetadataOutcome(previous, snapshot)
	if saveFailed {
		degradation := err
		if degradation == nil {
			degradation = errors.New("Dataproc deletion completion save required readback reconciliation")
		}
		api.degrade(degradation)
	}
	if err != nil {
		return fmt.Errorf("persist Dataproc cluster deletion: %w", err)
	}
	if saveFailed {
		return errors.New("persist Dataproc cluster deletion: save returned an error")
	}
	return nil
}

func (api *API) persistJob(key string, job *Job) error {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("Dataproc persistence unavailable: %w", err)
	}
	api.mu.RLock()
	previous := api.snapshotLocked()
	api.mu.RUnlock()
	snapshot := cloneDataprocMetadata(previous)
	snapshot.Jobs[key] = cloneJob(job)
	if err := api.PersistenceError(); err != nil {
		return fmt.Errorf("Dataproc persistence unavailable before job outcome save: %w", err)
	}
	saveFailed, err := api.commitMetadataOutcome(previous, snapshot)
	if saveFailed {
		degradation := err
		if degradation == nil {
			degradation = errors.New("Dataproc job outcome save required readback reconciliation")
		}
		api.degrade(degradation)
	}
	if err != nil {
		return fmt.Errorf("persist Dataproc job outcome: %w", err)
	}
	if saveFailed {
		return errors.New("persist Dataproc job outcome: save returned an error")
	}
	return nil
}

func normalizeDataprocMetadata(metadata *dataprocMetadata, restarting bool) error {
	if metadata.Clusters == nil {
		metadata.Clusters = make(map[string]*Cluster)
	}
	if metadata.Jobs == nil {
		metadata.Jobs = make(map[string]*Job)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for key, cluster := range metadata.Clusters {
		if cluster == nil {
			return fmt.Errorf("cluster %q is null", key)
		}
		if restarting && cluster.Status.State != "ERROR" {
			cluster.Status = ClusterStatus{
				State: "ERROR", Detail: "Docker backend state is not reconciled after restart", StateStartTime: now,
			}
			cluster.StatusHistory = append(cluster.StatusHistory, cluster.Status)
		}
	}
	for key, job := range metadata.Jobs {
		if job == nil {
			return fmt.Errorf("job %q is null", key)
		}
		if restarting && job.Status.State != "DONE" && job.Status.State != "ERROR" {
			job.Status = JobStatus{
				State: "ERROR", Details: "Job execution was interrupted by restart", StateStartTime: now,
			}
		}
	}
	return nil
}

func cloneDataprocMetadata(metadata dataprocMetadata) dataprocMetadata {
	payload, _ := json.Marshal(metadata)
	var clone dataprocMetadata
	_ = json.Unmarshal(payload, &clone)
	_ = normalizeDataprocMetadata(&clone, false)
	return clone
}

func dataprocMetadataEqual(left, right dataprocMetadata) bool {
	leftPayload, leftErr := json.Marshal(left)
	rightPayload, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftPayload, rightPayload)
}

func dataprocMetadataEmpty(metadata dataprocMetadata) bool {
	return len(metadata.Clusters) == 0 && len(metadata.Jobs) == 0
}

func cloneCluster(cluster *Cluster) *Cluster {
	if cluster == nil {
		return nil
	}
	payload, _ := json.Marshal(cluster)
	var clone Cluster
	_ = json.Unmarshal(payload, &clone)
	return &clone
}

func cloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	payload, _ := json.Marshal(job)
	var clone Job
	_ = json.Unmarshal(payload, &clone)
	return &clone
}

func (api *API) PersistenceError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.initErr
}

func (api *API) initializationError() error { return api.PersistenceError() }

func (api *API) rejectDegradedMutation(w http.ResponseWriter) bool {
	if api.PersistenceError() == nil {
		return false
	}
	writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Dataproc persistence is unavailable")
	return true
}

func (api *API) degrade(err error) {
	api.mu.Lock()
	if api.initErr == nil {
		api.initErr = fmt.Errorf("Dataproc persistence is degraded: %w", err)
	} else {
		api.initErr = errors.Join(api.initErr, err)
	}
	api.mu.Unlock()
}
