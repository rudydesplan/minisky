package cloudsql

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
	state.MustRegisterEntryValidator(cloudSQLStateEntry, state.StrictEntryValidator[cloudSQLMetadata](nil))
	registry.Register("sqladmin.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr, ctx.SvcMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

// DatabaseInstance mirrors the Cloud SQL DatabaseInstance resource.
type DatabaseInstance struct {
	Kind            string           `json:"kind"`
	Name            string           `json:"name"`
	Project         string           `json:"project"`
	SelfLink        string           `json:"selfLink"`
	DatabaseVersion string           `json:"databaseVersion"` // e.g. POSTGRES_15, MYSQL_8_0
	Region          string           `json:"region"`
	State           string           `json:"state"` // PENDING_CREATE → RUNNABLE → SUSPENDED → DELETED
	Settings        InstanceSettings `json:"settings"`
	ConnectionName  string           `json:"connectionName"`
	IpAddresses     []IpMapping      `json:"ipAddresses"`
	ServerCaCert    *SslCert         `json:"serverCaCert,omitempty"`
	CreateTime      string           `json:"createTime,omitempty"`
	Etag            string           `json:"etag"`
	BackendStatus   string           `json:"backendStatus,omitempty"`
}

type InstanceSettings struct {
	Tier                string            `json:"tier"`             // e.g. db-n1-standard-2
	ActivationPolicy    string            `json:"activationPolicy"` // ALWAYS, NEVER
	BackupConfiguration *BackupConfig     `json:"backupConfiguration,omitempty"`
	DatabaseFlags       []DatabaseFlag    `json:"databaseFlags,omitempty"`
	UserLabels          map[string]string `json:"userLabels,omitempty"`
	StorageAutoResize   bool              `json:"storageAutoResize"`
	DataDiskSizeGb      string            `json:"dataDiskSizeGb"`
	DataDiskType        string            `json:"dataDiskType"` // PD_SSD, PD_HDD
	AvailabilityType    string            `json:"availabilityType"`
	PricingPlan         string            `json:"pricingPlan"`
}

type BackupConfig struct {
	Enabled   bool   `json:"enabled"`
	StartTime string `json:"startTime"`
}

type DatabaseFlag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type IpMapping struct {
	Type      string `json:"type"` // PRIMARY, OUTGOING
	IpAddress string `json:"ipAddress"`
}

type SslCert struct {
	Kind             string `json:"kind"`
	CertSerialNumber string `json:"certSerialNumber"`
	Cert             string `json:"cert"`
	CommonName       string `json:"commonName"`
	ExpirationTime   string `json:"expirationTime"`
	Sha1Fingerprint  string `json:"sha1Fingerprint"`
	Instance         string `json:"instance"`
}

// Database represents a schema within a Cloud SQL instance.
type Database struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Instance  string `json:"instance"`
	Project   string `json:"project"`
	SelfLink  string `json:"selfLink"`
	Charset   string `json:"charset"`
	Collation string `json:"collation"`
	Etag      string `json:"etag"`
}

// User represents a database user.
type User struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Host     string `json:"host"`
	Instance string `json:"instance"`
	Project  string `json:"project"`
	Password string `json:"password,omitempty"`
}

// SqlOperation mirrors the Cloud SQL operations resource.
type SqlOperation struct {
	Kind          string                       `json:"kind"`
	Name          string                       `json:"name"`
	TargetLink    string                       `json:"targetLink"`
	Status        string                       `json:"status"` // PENDING, RUNNING, DONE
	OperationType string                       `json:"operationType"`
	StartTime     string                       `json:"startTime,omitempty"`
	EndTime       string                       `json:"endTime,omitempty"`
	Error         *orchestrator.OperationError `json:"error,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API is the high-fidelity Cloud SQL (sqladmin v1) shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	adminMu    sync.Mutex
	opMgr      *orchestrator.OperationManager
	svcMgr     *orchestrator.ServiceManager
	backend    cloudSQLBackend
	stateStore cloudSQLStore
	initErr    error
	instances  map[string]*DatabaseInstance // key: project:instanceName
	databases  map[string][]*Database       // key: project:instanceName
	users      map[string][]*User           // key: project:instanceName
}

type cloudSQLBackend interface {
	Create(context.Context, string, string, string, string) (string, bool, error)
	Delete(context.Context, string, string) error
	ExecuteAdmin(context.Context, string, string, string, string, string, string) error
}

type serviceManagerBackend struct {
	manager *orchestrator.ServiceManager
}

func (b serviceManagerBackend) Create(ctx context.Context, project, name, version, password string) (string, bool, error) {
	if b.manager == nil {
		return "", false, fmt.Errorf("Cloud SQL backend is unavailable")
	}
	return b.manager.ProvisionCloudSQLVM(ctx, project, name, version, password)
}

func (b serviceManagerBackend) Delete(ctx context.Context, project, name string) error {
	if b.manager == nil {
		return fmt.Errorf("Cloud SQL backend is unavailable")
	}
	return b.manager.DeleteCloudSQLVMContext(ctx, project, name)
}

func (b serviceManagerBackend) ExecuteAdmin(
	ctx context.Context,
	project, instance, version, action, name, password string,
) error {
	if b.manager == nil {
		return fmt.Errorf("Cloud SQL backend is unavailable")
	}
	return b.manager.ExecuteCloudSQLAdmin(ctx, project, instance, version, action, name, password)
}

func NewAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Cloud SQL] state disabled: %v", err)
		api := newAPI(opMgr, svcMgr, nil)
		api.initErr = fmt.Errorf("open Cloud SQL state: %w", err)
		return api
	}
	api, err := NewAPIWithStore(opMgr, svcMgr, store)
	if err != nil {
		log.Printf("[Shim: Cloud SQL] state rehydration failed: %v", err)
		api = newAPI(opMgr, svcMgr, store)
		api.initErr = err
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager, store cloudSQLStore) *API {
	return newAPIWithDependencies(opMgr, svcMgr, serviceManagerBackend{manager: svcMgr}, store)
}

func newAPIWithBackend(opMgr *orchestrator.OperationManager, backend cloudSQLBackend, store cloudSQLStore) *API {
	return newAPIWithDependencies(opMgr, nil, backend, store)
}

func newAPIWithDependencies(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	backend cloudSQLBackend,
	store cloudSQLStore,
) *API {
	return &API{
		opMgr:      opMgr,
		svcMgr:     svcMgr,
		backend:    backend,
		stateStore: store,
		instances:  make(map[string]*DatabaseInstance),
		databases:  make(map[string][]*Database),
		users:      make(map[string][]*User),
	}
}

// ServeHTTP dispatches Cloud SQL v1 paths.
//
// Supported paths (sqladmin.googleapis.com):
//
//	POST   /v1/projects/{project}/instances
//	GET    /v1/projects/{project}/instances
//	GET    /v1/projects/{project}/instances/{instance}
//	DELETE /v1/projects/{project}/instances/{instance}
//	POST   /v1/projects/{project}/instances/{instance}/databases
//	GET    /v1/projects/{project}/instances/{instance}/databases
//	POST   /v1/projects/{project}/instances/{instance}/users
//	GET    /v1/projects/{project}/instances/{instance}/users
//	GET    /v1/projects/{project}/operations/{operation}
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Cloud SQL] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.PersistenceError() != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Cloud SQL persistence is unavailable")
		return
	}

	path := r.URL.Path
	project := extractSegmentAfter(path, "projects")

	switch {
	case strings.Contains(path, "/operations/"):
		api.getOperation(w, r, path)
	case strings.Contains(path, "/databases"):
		instance := extractSegmentAfter(path, "instances")
		api.routeDatabases(w, r, project, instance, path)
	case strings.Contains(path, "/users"):
		instance := extractSegmentAfter(path, "instances")
		api.routeUsers(w, r, project, instance)
	case strings.Contains(path, "/instances"):
		api.routeInstances(w, r, project, path)
	default:
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Cloud SQL resource not found: "+path)
	}
}

func (api *API) PersistenceError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.initErr
}

// ─────────────────────────────────────────────────────────────────────────────
// Instances
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeInstances(w http.ResponseWriter, r *http.Request, project, path string) {
	instanceName := extractSegmentAfter(path, "instances")

	switch r.Method {
	case http.MethodPost:
		api.createInstance(w, r, project)
	case http.MethodGet:
		if instanceName != "" {
			api.getInstance(w, project, instanceName)
		} else {
			api.listInstances(w, project)
		}
	case http.MethodDelete:
		api.deleteInstance(w, r, project, instanceName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createInstance(w http.ResponseWriter, r *http.Request, project string) {
	var body struct {
		Name            string           `json:"name"`
		DatabaseVersion string           `json:"databaseVersion"`
		Region          string           `json:"region"`
		Settings        InstanceSettings `json:"settings"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if body.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Field 'name' is required")
		return
	}

	region := body.Region
	if region == "" {
		region = "us-central1"
	}
	dbVersion := body.DatabaseVersion
	if dbVersion == "" {
		dbVersion = "POSTGRES_15"
	}

	// Fill in opinionated defaults for missing settings
	settings := body.Settings
	if settings.Tier == "" {
		settings.Tier = "db-n1-standard-2"
	}
	if settings.ActivationPolicy == "" {
		settings.ActivationPolicy = "ALWAYS"
	}
	if settings.DataDiskSizeGb == "" {
		settings.DataDiskSizeGb = "10"
	}
	if settings.DataDiskType == "" {
		settings.DataDiskType = "PD_SSD"
	}
	if settings.AvailabilityType == "" {
		settings.AvailabilityType = "ZONAL"
	}
	if settings.PricingPlan == "" {
		settings.PricingPlan = "PER_USE"
	}

	selfLink := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/instances/%s", project, body.Name)
	inst := &DatabaseInstance{
		Kind:            "sql#instance",
		Name:            body.Name,
		Project:         project,
		SelfLink:        selfLink,
		DatabaseVersion: dbVersion,
		Region:          region,
		State:           "PENDING_CREATE",
		Settings:        settings,
		ConnectionName:  fmt.Sprintf("%s:%s:%s", project, region, body.Name),
		IpAddresses: []IpMapping{
			{Type: "PRIMARY", IpAddress: "127.0.0.1"},
			{Type: "OUTGOING", IpAddress: "127.0.0.1"},
		},
		ServerCaCert: &SslCert{
			Kind:             "sql#sslCert",
			CertSerialNumber: "0",
			Cert:             "-----BEGIN CERTIFICATE-----\n(minisky-fake-ca-cert)\n-----END CERTIFICATE-----\n",
			CommonName:       "minisky-local-ca",
			ExpirationTime:   time.Now().Add(87600 * time.Hour).UTC().Format(time.RFC3339),
			Sha1Fingerprint:  fmt.Sprintf("%x", time.Now().UnixNano()),
			Instance:         body.Name,
		},
		CreateTime: time.Now().UTC().Format(time.RFC3339),
		Etag:       newEtag(),
	}

	iKey := instanceKey(project, body.Name)
	api.mu.Lock()
	if _, exists := api.instances[iKey]; exists {
		api.mu.Unlock()
		w.WriteHeader(http.StatusConflict)
		writeError(w, 409, "ALREADY_EXISTS", fmt.Sprintf("Instance '%s' already exists", body.Name))
		return
	}
	api.instances[iKey] = inst
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		api.mu.Lock()
		delete(api.instances, iKey)
		api.mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist Cloud SQL instance metadata: "+err.Error())
		return
	}

	// Register LRO and drive state transitions asynchronously
	targetLink := selfLink
	op, err := api.opMgr.RegisterDurable("sql#operation", "CREATE", targetLink, "", region)
	if err != nil {
		api.mu.Lock()
		if current := api.instances[iKey]; current != nil {
			current.State = "ERROR"
			current.BackendStatus = "operation registration failed: " + err.Error()
			current.IpAddresses = nil
		}
		api.mu.Unlock()
		if persistErr := api.persistMetadata(); persistErr != nil {
			err = errors.Join(err, fmt.Errorf("persist degraded instance: %w", persistErr))
		}
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		internalURL, created, err := api.backend.Create(ctx, project, body.Name, dbVersion, "minisky")
		if err != nil {
			log.Printf("[Shim: Cloud SQL] Provisioning failed: %v", err)
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			var cleanupErr error
			if created {
				cleanupErr = api.backend.Delete(cleanupCtx, project, body.Name)
			}
			cleanupCancel()
			return api.rollbackCreate(iKey, err, cleanupErr)
		}

		api.mu.Lock()
		if i, ok := api.instances[iKey]; ok {
			i.State = "RUNNABLE"
			i.BackendStatus = ""
			// Extract ip:port from 'http://127.0.0.1:xxx'
			addr := strings.TrimPrefix(internalURL, "http://")
			i.IpAddresses = []IpMapping{
				{Type: "PRIMARY", IpAddress: addr},
			}
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			var cleanupErr error
			if created {
				cleanupErr = api.backend.Delete(cleanupCtx, project, body.Name)
			}
			cleanupCancel()
			return api.rollbackCreate(iKey, fmt.Errorf("persist runnable instance: %w", err), cleanupErr)
		}
		return nil
	})

	// Wrap the operation in Cloud SQL's own schema format
	sqlOp := toSqlOperation(op, "CREATE", selfLink)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sqlOp)
}

func (api *API) getInstance(w http.ResponseWriter, project, name string) {
	key := instanceKey(project, name)
	api.mu.RLock()
	inst, ok := api.instances[key]
	inst = cloneDatabaseInstance(inst)
	api.mu.RUnlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("Instance '%s' not found in project '%s'", name, project))
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(inst)
}

func (api *API) listInstances(w http.ResponseWriter, project string) {
	prefix := project + ":"
	api.mu.RLock()
	items := []*DatabaseInstance{}
	for k, v := range api.instances {
		if strings.HasPrefix(k, prefix) {
			items = append(items, cloneDatabaseInstance(v))
		}
	}
	api.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":  "sql#instancesList",
		"items": items,
	})
}

func (api *API) deleteInstance(w http.ResponseWriter, r *http.Request, project, name string) {
	key := instanceKey(project, name)
	api.adminMu.Lock()
	api.mu.Lock()
	inst, ok := api.instances[key]
	if !ok {
		api.mu.Unlock()
		api.adminMu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("Instance '%s' not found", name))
		return
	}

	previousState := inst.State
	previousBackendStatus := inst.BackendStatus
	inst.State = "PENDING_DELETE"
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		api.mu.Lock()
		if current := api.instances[key]; current == inst {
			current.State = previousState
			current.BackendStatus = previousBackendStatus
		}
		api.mu.Unlock()
		api.adminMu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist deleting Cloud SQL instance: "+err.Error())
		return
	}

	selfLink := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/instances/%s", project, name)
	op, err := api.opMgr.RegisterDurable("sql#operation", "DELETE", selfLink, "", "")
	if err != nil {
		api.mu.Lock()
		inst.State = "ERROR"
		inst.BackendStatus = "delete operation registration failed: " + err.Error()
		api.mu.Unlock()
		if persistErr := api.persistMetadata(); persistErr != nil {
			err = errors.Join(err, fmt.Errorf("persist degraded instance: %w", persistErr))
		}
		api.adminMu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, 500, "INTERNAL", "persist operation metadata: "+err.Error())
		return
	}

	api.opMgr.RunAsync(op.Name, func() error {
		defer api.adminMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := api.backend.Delete(ctx, project, name); err != nil {
			api.mu.Lock()
			if current := api.instances[key]; current != nil {
				current.State = "ERROR"
				current.BackendStatus = err.Error()
			}
			api.mu.Unlock()
			if persistErr := api.persistMetadata(); persistErr != nil {
				return fmt.Errorf("%w; persist failed deletion: %v", err, persistErr)
			}
			return err
		}

		tombstone := cloneDatabaseInstance(inst)
		api.mu.RLock()
		oldDatabases := cloneDatabases(api.databases[key])
		oldUsers := cloneUsers(api.users[key])
		api.mu.RUnlock()
		api.mu.Lock()
		delete(api.instances, key)
		delete(api.databases, key)
		delete(api.users, key)
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			tombstone.State = "ERROR"
			tombstone.BackendStatus = "backend deleted; metadata removal persistence failed: " + err.Error()
			tombstone.IpAddresses = nil
			api.mu.Lock()
			api.instances[key] = tombstone
			api.databases[key] = oldDatabases
			api.users[key] = oldUsers
			api.mu.Unlock()
			degradedErr := api.persistMetadata()
			if degradedErr != nil {
				return errors.Join(
					fmt.Errorf("persist deleted instance: %w", err),
					fmt.Errorf("persist backend-deleted reconciliation tombstone: %w", degradedErr),
				)
			}
			return fmt.Errorf("persist deleted instance: %w", err)
		}
		return nil
	})

	sqlOp := toSqlOperation(op, "DELETE", selfLink)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sqlOp)
}

func (api *API) rollbackCreate(key string, cause, cleanupErr error) error {
	api.mu.RLock()
	tombstone := cloneDatabaseInstance(api.instances[key])
	api.mu.RUnlock()
	if cleanupErr == nil {
		api.mu.Lock()
		delete(api.instances, key)
		delete(api.databases, key)
		delete(api.users, key)
		api.mu.Unlock()
		if err := api.persistMetadata(); err == nil {
			return cause
		} else {
			cleanupErr = fmt.Errorf("persist rollback: %w", err)
		}
	}
	combined := errors.Join(cause, cleanupErr)
	api.mu.Lock()
	if tombstone != nil {
		tombstone.State = "ERROR"
		tombstone.BackendStatus = combined.Error()
		tombstone.IpAddresses = nil
		api.instances[key] = tombstone
	}
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		combined = errors.Join(combined, fmt.Errorf("persist degraded state: %w", err))
	}
	return combined
}

// ─────────────────────────────────────────────────────────────────────────────
// Databases
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeDatabases(w http.ResponseWriter, r *http.Request, project, instance, path string) {
	dbName := extractSegmentAfter(path, "databases")

	switch r.Method {
	case http.MethodPost:
		var body struct {
			Name      string `json:"name"`
			Charset   string `json:"charset"`
			Collation string `json:"collation"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
			return
		}
		if !validCloudSQLPrincipal(body.Name) {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, 400, "INVALID_ARGUMENT", "database name must use letters, digits, hyphens, or underscores")
			return
		}
		api.adminMu.Lock()
		defer api.adminMu.Unlock()
		iKey := instanceKey(project, instance)
		inst, ok := api.runnableInstance(iKey)
		if !ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "Cloud SQL instance backend is not runnable")
			return
		}
		api.mu.RLock()
		for _, existing := range api.databases[iKey] {
			if existing.Name == body.Name {
				api.mu.RUnlock()
				w.WriteHeader(http.StatusConflict)
				writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Database "+body.Name+" already exists")
				return
			}
		}
		api.mu.RUnlock()
		if err := api.executeAdmin(r.Context(), project, instance, inst.DatabaseVersion, "CREATE_DATABASE", body.Name, ""); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "create database in owned backend: "+err.Error())
			return
		}
		charset := body.Charset
		if charset == "" {
			charset = "UTF8"
		}
		collation := body.Collation
		if collation == "" {
			collation = "en_US.UTF8"
		}
		db := &Database{
			Kind:      "sql#database",
			Name:      body.Name,
			Instance:  instance,
			Project:   project,
			SelfLink:  fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/instances/%s/databases/%s", project, instance, body.Name),
			Charset:   charset,
			Collation: collation,
			Etag:      newEtag(),
		}
		api.mu.Lock()
		api.databases[iKey] = append(api.databases[iKey], db)
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			compensationErr := api.executeAdmin(context.Background(), project, instance,
				inst.DatabaseVersion, "DELETE_DATABASE", body.Name, "")
			api.mu.Lock()
			if compensationErr == nil {
				api.databases[iKey] = removeDatabase(api.databases[iKey], body.Name)
			} else {
				api.markChildReconciliationLocked(iKey,
					"database backend create committed but compensation failed: "+compensationErr.Error())
			}
			api.mu.Unlock()
			if compensationErr != nil {
				err = errors.Join(err, compensationErr, api.persistMetadata())
			}
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, 500, "INTERNAL", "persist Cloud SQL database metadata: "+err.Error())
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"kind":          "sql#operation",
			"status":        "DONE",
			"operationType": "CREATE_DATABASE",
			"targetLink":    db.SelfLink,
		})

	case http.MethodGet:
		iKey := instanceKey(project, instance)
		api.mu.RLock()
		dbs := api.databases[iKey]
		dbs = cloneDatabases(dbs)
		api.mu.RUnlock()

		if dbName != "" {
			for _, d := range dbs {
				if d.Name == dbName {
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(d)
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
			writeError(w, 404, "NOT_FOUND", "Database "+dbName+" not found")
			return
		}

		if dbs == nil {
			dbs = []*Database{}
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"kind":  "sql#databasesList",
			"items": dbs,
		})

	case http.MethodDelete:
		if dbName == "" {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "database name is required")
			return
		}
		api.adminMu.Lock()
		defer api.adminMu.Unlock()
		iKey := instanceKey(project, instance)
		inst, ok := api.runnableInstance(iKey)
		if !ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "Cloud SQL instance backend is not runnable")
			return
		}
		api.mu.RLock()
		var existing *Database
		for _, database := range api.databases[iKey] {
			if database.Name == dbName {
				clone := *database
				existing = &clone
				break
			}
		}
		api.mu.RUnlock()
		if existing == nil {
			w.WriteHeader(http.StatusNotFound)
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Database "+dbName+" not found")
			return
		}
		if err := api.executeAdmin(r.Context(), project, instance, inst.DatabaseVersion, "DELETE_DATABASE", dbName, ""); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "delete database from owned backend: "+err.Error())
			return
		}
		api.mu.Lock()
		api.databases[iKey] = removeDatabase(api.databases[iKey], dbName)
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			api.mu.Lock()
			api.databases[iKey] = append(api.databases[iKey], existing)
			api.markChildReconciliationLocked(iKey,
				"database backend deleted but metadata removal persistence failed: "+err.Error())
			api.mu.Unlock()
			err = errors.Join(err, api.persistMetadata())
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL",
				"database backend deleted but metadata persistence failed: "+err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"kind": "sql#operation", "status": "DONE", "operationType": "DELETE_DATABASE",
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Users
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeUsers(w http.ResponseWriter, r *http.Request, project, instance string) {
	switch r.Method {
	case http.MethodPost:
		var body struct {
			Name     string `json:"name"`
			Host     string `json:"host"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
			return
		}
		if !validCloudSQLPrincipal(body.Name) {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "user name must use letters, digits, hyphens, or underscores")
			return
		}
		if body.Host != "" && body.Host != "%" {
			w.WriteHeader(http.StatusNotImplemented)
			writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "custom Cloud SQL user hosts are not supported")
			return
		}
		api.adminMu.Lock()
		defer api.adminMu.Unlock()
		iKey := instanceKey(project, instance)
		inst, ok := api.runnableInstance(iKey)
		if !ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "Cloud SQL instance backend is not runnable")
			return
		}
		api.mu.RLock()
		for _, existing := range api.users[iKey] {
			if existing.Name == body.Name {
				api.mu.RUnlock()
				w.WriteHeader(http.StatusConflict)
				writeError(w, http.StatusConflict, "ALREADY_EXISTS", "User "+body.Name+" already exists")
				return
			}
		}
		api.mu.RUnlock()
		if err := api.executeAdmin(r.Context(), project, instance, inst.DatabaseVersion, "CREATE_USER", body.Name, body.Password); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "create user in owned backend: "+err.Error())
			return
		}
		user := &User{
			Kind:     "sql#user",
			Name:     body.Name,
			Host:     body.Host,
			Instance: instance,
			Project:  project,
		}
		api.mu.Lock()
		api.users[iKey] = append(api.users[iKey], user)
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			compensationErr := api.executeAdmin(context.Background(), project, instance,
				inst.DatabaseVersion, "DELETE_USER", body.Name, "")
			api.mu.Lock()
			if compensationErr == nil {
				api.users[iKey] = removeUser(api.users[iKey], body.Name)
			} else {
				api.markChildReconciliationLocked(iKey,
					"user backend create committed but compensation failed: "+compensationErr.Error())
			}
			api.mu.Unlock()
			if compensationErr != nil {
				err = errors.Join(err, compensationErr, api.persistMetadata())
			}
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, 500, "INTERNAL", "persist Cloud SQL user metadata: "+err.Error())
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"kind":          "sql#operation",
			"status":        "DONE",
			"operationType": "CREATE_USER",
		})

	case http.MethodGet:
		iKey := instanceKey(project, instance)
		api.mu.RLock()
		users := api.users[iKey]
		users = cloneUsers(users)
		api.mu.RUnlock()

		if users == nil {
			users = []*User{}
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"kind":  "sql#usersList",
			"items": users,
		})

	case http.MethodDelete:
		name := r.URL.Query().Get("name")
		if !validCloudSQLPrincipal(name) {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "user name query parameter is required")
			return
		}
		host := r.URL.Query().Get("host")
		if host != "" && host != "%" {
			w.WriteHeader(http.StatusNotImplemented)
			writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "custom Cloud SQL user hosts are not supported")
			return
		}
		api.adminMu.Lock()
		defer api.adminMu.Unlock()
		iKey := instanceKey(project, instance)
		inst, ok := api.runnableInstance(iKey)
		if !ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "Cloud SQL instance backend is not runnable")
			return
		}
		api.mu.RLock()
		var existing *User
		for _, user := range api.users[iKey] {
			if user.Name == name {
				clone := *user
				existing = &clone
				break
			}
		}
		api.mu.RUnlock()
		if existing == nil {
			w.WriteHeader(http.StatusNotFound)
			writeError(w, http.StatusNotFound, "NOT_FOUND", "User "+name+" not found")
			return
		}
		if err := api.executeAdmin(r.Context(), project, instance, inst.DatabaseVersion, "DELETE_USER", name, ""); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "delete user from owned backend: "+err.Error())
			return
		}
		api.mu.Lock()
		api.users[iKey] = removeUser(api.users[iKey], name)
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			api.mu.Lock()
			api.users[iKey] = append(api.users[iKey], existing)
			api.markChildReconciliationLocked(iKey,
				"user backend deleted but metadata removal persistence failed: "+err.Error())
			api.mu.Unlock()
			err = errors.Join(err, api.persistMetadata())
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL",
				"user backend deleted but metadata persistence failed: "+err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"kind": "sql#operation", "status": "DONE", "operationType": "DELETE_USER",
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Operations (Cloud SQL uses its own operation schema)
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) getOperation(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	opName := extractSegmentAfter(path, "operations")
	op := api.opMgr.Get(opName)
	targetPrefix := fmt.Sprintf("https://sqladmin.googleapis.com/v1/projects/%s/", project)
	if op == nil || op.Kind != "sql#operation" || !strings.HasPrefix(op.TargetLink, targetPrefix) {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Operation not found: "+opName)
		return
	}
	sqlOp := toSqlOperation(op, op.OperationType, op.TargetLink)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sqlOp)
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func toSqlOperation(op *orchestrator.Operation, opType, targetLink string) *SqlOperation {
	status := "PENDING"
	switch op.Status {
	case orchestrator.StatusRunning:
		status = "RUNNING"
	case orchestrator.StatusDone:
		status = "DONE"
	}
	return &SqlOperation{
		Kind:          "sql#operation",
		Name:          op.Name,
		TargetLink:    targetLink,
		Status:        status,
		OperationType: opType,
		StartTime:     op.StartTime,
		EndTime:       op.EndTime,
		Error:         op.Error,
	}
}

func cloneDatabaseInstance(instance *DatabaseInstance) *DatabaseInstance {
	if instance == nil {
		return nil
	}
	clone := *instance
	clone.IpAddresses = append([]IpMapping(nil), instance.IpAddresses...)
	clone.Settings.DatabaseFlags = append([]DatabaseFlag(nil), instance.Settings.DatabaseFlags...)
	if instance.Settings.UserLabels != nil {
		clone.Settings.UserLabels = make(map[string]string, len(instance.Settings.UserLabels))
		for key, value := range instance.Settings.UserLabels {
			clone.Settings.UserLabels[key] = value
		}
	}
	if instance.Settings.BackupConfiguration != nil {
		backup := *instance.Settings.BackupConfiguration
		clone.Settings.BackupConfiguration = &backup
	}
	if instance.ServerCaCert != nil {
		cert := *instance.ServerCaCert
		clone.ServerCaCert = &cert
	}
	return &clone
}

func cloneDatabases(databases []*Database) []*Database {
	result := make([]*Database, 0, len(databases))
	for _, database := range databases {
		if database == nil {
			continue
		}
		clone := *database
		result = append(result, &clone)
	}
	return result
}

func cloneUsers(users []*User) []*User {
	result := make([]*User, 0, len(users))
	for _, user := range users {
		if user == nil {
			continue
		}
		clone := *user
		result = append(result, &clone)
	}
	return result
}

func (api *API) runnableInstance(key string) (*DatabaseInstance, bool) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	instance := cloneDatabaseInstance(api.instances[key])
	return instance, instance != nil && instance.State == "RUNNABLE"
}

func (api *API) markChildReconciliationLocked(key, status string) {
	if instance := api.instances[key]; instance != nil {
		instance.State = "ERROR"
		instance.BackendStatus = status
		instance.IpAddresses = nil
	}
}

func (api *API) executeAdmin(
	parent context.Context,
	project, instance, version, action, name, password string,
) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	return api.backend.ExecuteAdmin(ctx, project, instance, version, action, name, password)
}

func removeDatabase(databases []*Database, name string) []*Database {
	result := databases[:0]
	for _, database := range databases {
		if database.Name != name {
			result = append(result, database)
		}
	}
	return result
}

func removeUser(users []*User, name string) []*User {
	result := users[:0]
	for _, user := range users {
		if user.Name != name {
			result = append(result, user)
		}
	}
	return result
}

func validCloudSQLPrincipal(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}

func instanceKey(project, name string) string {
	return project + ":" + name
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
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"status":  status,
			"message": message,
		},
	})
}

func newEtag() string {
	return fmt.Sprintf("SQLETAG%x", time.Now().UnixNano())
}
