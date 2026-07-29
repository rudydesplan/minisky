package cloudsql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const (
	cloudSQLStateEntry                         = "cloudsql/metadata"
	metadataOnlyBackendState                   = "METADATA_ONLY"
	cloudSQLBootstrapPolicyV1                  = orchestrator.CloudSQLBootstrapPolicyV1
	cloudSQLReconcileTimeout                   = 30 * time.Second
	cloudSQLPostBootTimeout                    = 2 * time.Minute
	cloudSQLRequestRetryBudget                 = 250 * time.Millisecond
	cloudSQLReconcileCooldown                  = 5 * time.Second
	cloudSQLLocalRuntimeDir                    = ".cloudsql-local-runtime"
	cloudSQLRuntimePhaseImageAcquisitionIntent = "IMAGE_ACQUISITION_INTENT"
	cloudSQLRuntimePhaseCreateIntent           = "CREATE_INTENT"
	cloudSQLImageAcquisitionIncompleteState    = "IMAGE_ACQUISITION_INCOMPLETE"
)

type cloudSQLStore interface {
	Load(string, any) error
	Save(string, any) error
}

type cloudSQLLocalMarkerStore interface {
	ReadLocalMarker(string, string) ([]byte, bool, error)
	WriteLocalMarker(string, string, []byte) error
	RemoveLocalMarker(string, string, []byte) error
}

type cloudSQLMetadata struct {
	Instances map[string]*DatabaseInstance          `json:"instances"`
	Databases map[string][]*Database                `json:"databases"`
	Users     map[string][]*User                    `json:"users"`
	Runtimes  map[string]*cloudSQLRuntimeProvenance `json:"runtimes,omitempty"`
}

type cloudSQLRuntimeProvenance struct {
	Profile                string `json:"profile"`
	Project                string `json:"project"`
	Instance               string `json:"instance"`
	DatabaseVersion        string `json:"databaseVersion"`
	OwnershipFingerprint   string `json:"ownershipFingerprint"`
	BootstrapPolicy        string `json:"bootstrapPolicy"`
	Image                  string `json:"image,omitempty"`
	ImageID                string `json:"imageId,omitempty"`
	VolumeIdentity         string `json:"volumeIdentity,omitempty"`
	ImageAcquisitionIntent bool   `json:"imageAcquisitionIntent,omitempty"`
	CreationIntent         bool   `json:"creationIntent,omitempty"`
	Phase                  string `json:"phase,omitempty"`
}

// NewAPIWithStore constructs a Cloud SQL shim backed by the supplied profile
// store. Loading metadata never creates or adopts database containers.
func NewAPIWithStore(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	store cloudSQLStore,
) (*API, error) {
	return newAPIWithStoreDependencies(
		opMgr,
		svcMgr,
		serviceManagerBackend{manager: svcMgr},
		store,
	)
}

func newAPIWithStoreAndBackend(
	opMgr *orchestrator.OperationManager,
	backend cloudSQLBackend,
	store cloudSQLStore,
) (*API, error) {
	return newAPIWithStoreDependencies(opMgr, nil, backend, store)
}

func newAPIWithStoreDependencies(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	backend cloudSQLBackend,
	store cloudSQLStore,
) (*API, error) {
	api := newAPIWithDependencies(opMgr, svcMgr, backend, store)
	if store == nil {
		return api, nil
	}

	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Cloud SQL metadata: %w", err)
	}
	if persisted.Instances != nil {
		api.instances = persisted.Instances
	}
	if persisted.Databases != nil {
		api.databases = persisted.Databases
	}
	if persisted.Users != nil {
		api.users = persisted.Users
		for _, users := range api.users {
			for _, user := range users {
				if user != nil && user.Password != "" {
					user.Password = ""
					api.restartDirty = true
				}
			}
		}
	}
	if persisted.Runtimes != nil {
		api.runtimes = persisted.Runtimes
	}
	profile := cloudSQLProfile(store)
	for key, instance := range api.instances {
		if instance == nil {
			delete(api.instances, key)
			delete(api.runtimes, key)
			api.restartDirty = true
			continue
		}
		persistedInstance := cloneDatabaseInstance(instance)
		instance.IpAddresses = nil
		runtime := api.runtimes[key]
		local, err := cloudSQLHasLocalProvenance(store, key, runtime)
		if err != nil {
			return nil, fmt.Errorf("load Cloud SQL local runtime provenance: %w", err)
		}
		if local && runtime.preBackendFor(profile, key, persistedInstance) {
			instance.State = "SUSPENDED"
			instance.BackendStatus = cloudSQLImageAcquisitionIncompleteState
			api.restartDirty = true
			continue
		}
		if !local || runtime == nil || !runtime.validFor(profile, key, persistedInstance) {
			api.preservedInstances[key] = persistedInstance
			instance.State = "SUSPENDED"
			instance.BackendStatus = metadataOnlyBackendState
			continue
		}
		if instance.State == "ERROR" && instance.BackendStatus != "" {
			continue
		}
		switch instance.State {
		case "RUNNABLE", "PENDING_CREATE":
			api.reconcileSources[key] = persistedInstance
			instance.State = "SUSPENDED"
			instance.BackendStatus = "RECONCILING"
			api.reconcile[key] = struct{}{}
		case "SUSPENDED":
			if runtime.CreationIntent && strings.Contains(instance.BackendStatus, "retryable") {
				api.reconcileSources[key] = persistedInstance
				instance.BackendStatus = "RECONCILING"
				api.reconcile[key] = struct{}{}
			} else {
				instance.BackendStatus = metadataOnlyBackendState
			}
		case "PENDING_DELETE", "DELETING":
			instance.State = "ERROR"
			instance.BackendStatus = "delete interrupted by MiniSky restart; backend was not reconciled"
		default:
			instance.State = "SUSPENDED"
			instance.BackendStatus = metadataOnlyBackendState
		}
		api.restartDirty = true
	}
	return api, nil
}

// OnPostBoot reconciles after registry construction, before the booted handler
// map is returned to the gateway. Repeated or concurrent calls never replay
// Docker side effects.
func (api *API) OnPostBoot(*registry.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), cloudSQLPostBootTimeout)
	defer cancel()
	if err := api.reconcileRestored(ctx); err != nil {
		log.Printf("[Shim: Cloud SQL] restart reconciliation degraded: %v", err)
	}
}

func (api *API) reconcileRestored(ctx context.Context) error {
	return api.reconcileRestoredKeys(ctx)
}

func (api *API) reconcileRestoredKeys(ctx context.Context, requestedKeys ...string) error {
	api.reconcileMu.Lock()
	defer api.reconcileMu.Unlock()
	batchCtx, cancel := context.WithTimeout(ctx, cloudSQLReconcileTimeout)
	defer cancel()
	return api.reconcileRestoredOnce(batchCtx, requestedKeys)
}

func (api *API) reconcileRestoredOnce(ctx context.Context, requestedKeys []string) error {
	if api.stateStore == nil {
		return nil
	}
	api.adminMu.Lock()
	defer api.adminMu.Unlock()

	api.mu.RLock()
	keys := make([]string, 0, len(api.reconcile))
	if len(requestedKeys) == 0 {
		for key := range api.reconcile {
			keys = append(keys, key)
		}
	} else {
		for _, key := range requestedKeys {
			if _, ok := api.reconcile[key]; ok {
				keys = append(keys, key)
			}
		}
	}
	dirty := api.restartDirty
	api.mu.RUnlock()
	sort.Strings(keys)

	outcomes := make(map[string]*DatabaseInstance, len(keys))
	runtimeOutcomes := make(map[string]*cloudSQLRuntimeProvenance, len(keys))
	succeeded := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if ctx.Err() != nil {
			break
		}
		api.mu.RLock()
		runtime := cloneCloudSQLRuntime(api.runtimes[key])
		instance := cloneDatabaseInstance(api.instances[key])
		retryAfter := api.reconcileRetryAfter[key]
		api.mu.RUnlock()
		if runtime == nil || instance == nil {
			api.mu.Lock()
			delete(api.reconcile, key)
			delete(api.reconcileRetryAfter, key)
			delete(api.reconcileSources, key)
			api.mu.Unlock()
			dirty = true
			continue
		}
		if api.reconcileNow().Before(retryAfter) {
			continue
		}
		reconcileCtx, cancel := context.WithTimeout(ctx, cloudSQLReconcileTimeout)
		result, err := api.backend.Reconcile(reconcileCtx, runtime.backendSpec())
		cancel()
		if result.Spec.Project != "" {
			runtime.applyBackendSpec(result.Spec)
		}
		runtimeOutcomes[key] = runtime
		address := ""
		if err == nil {
			address, err = cloudSQLLoopbackAddress(result.Endpoint)
		}
		if err != nil {
			instance.State = "SUSPENDED"
			instance.BackendStatus = "restart reconciliation retryable: " + err.Error()
			instance.IpAddresses = nil
			api.mu.Lock()
			api.reconcileRetryAfter[key] = api.reconcileNow().Add(cloudSQLReconcileCooldown)
			api.mu.Unlock()
		} else {
			instance.State = "RUNNABLE"
			instance.BackendStatus = ""
			instance.IpAddresses = []IpMapping{{Type: "PRIMARY", IpAddress: address}}
			succeeded[key] = struct{}{}
		}
		outcomes[key] = instance
		dirty = true
	}

	if !dirty {
		return nil
	}
	api.persistMu.Lock()
	snapshot := api.snapshotMetadata()
	api.mu.RLock()
	for key, source := range api.reconcileSources {
		if _, attempted := outcomes[key]; !attempted && source != nil {
			snapshot.Instances[key] = cloneDatabaseInstance(source)
		}
	}
	api.mu.RUnlock()
	for key, instance := range outcomes {
		snapshot.Instances[key] = instance
	}
	for key, runtime := range runtimeOutcomes {
		snapshot.Runtimes[key] = runtime
	}
	saveErr := api.saveMetadataLocked(snapshot)
	api.persistMu.Unlock()
	if saveErr != nil {
		degraded := fmt.Errorf("persist Cloud SQL restart reconciliation: %w", saveErr)
		api.mu.Lock()
		for _, key := range keys {
			if instance := api.instances[key]; instance != nil {
				instance.State = "ERROR"
				instance.BackendStatus = degraded.Error()
				instance.IpAddresses = nil
			}
		}
		api.initErr = degraded
		api.mu.Unlock()
		return degraded
	}
	api.mu.Lock()
	for key, instance := range outcomes {
		if api.instances[key] != nil {
			api.instances[key] = instance
		}
		if _, ok := succeeded[key]; ok {
			delete(api.reconcile, key)
			delete(api.reconcileRetryAfter, key)
		}
		if runtime := runtimeOutcomes[key]; runtime != nil {
			api.runtimes[key] = runtime
		}
		delete(api.reconcileSources, key)
	}
	api.restartDirty = false
	api.mu.Unlock()
	return nil
}

func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	return api.saveMetadataLocked(api.snapshotMetadata())
}

func (api *API) snapshotMetadata() cloudSQLMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := cloudSQLMetadata{
		Instances: make(map[string]*DatabaseInstance, len(api.instances)),
		Databases: make(map[string][]*Database, len(api.databases)),
		Users:     make(map[string][]*User, len(api.users)),
		Runtimes:  make(map[string]*cloudSQLRuntimeProvenance, len(api.runtimes)),
	}
	for key, instance := range api.instances {
		if preserved := api.preservedInstances[key]; preserved != nil {
			snapshot.Instances[key] = cloneDatabaseInstance(preserved)
		} else {
			snapshot.Instances[key] = cloneDatabaseInstance(instance)
		}
	}
	for key, databases := range api.databases {
		snapshot.Databases[key] = cloneDatabases(databases)
	}
	for key, users := range api.users {
		snapshot.Users[key] = cloneUsers(users)
		for _, user := range snapshot.Users[key] {
			if user != nil {
				user.Password = ""
			}
		}
	}
	for key, runtime := range api.runtimes {
		snapshot.Runtimes[key] = cloneCloudSQLRuntime(runtime)
	}
	return snapshot
}

func (api *API) saveMetadataLocked(snapshot cloudSQLMetadata) error {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("snapshot Cloud SQL metadata: %w", err)
	}
	return api.stateStore.Save(cloudSQLStateEntry, json.RawMessage(payload))
}

func (runtime *cloudSQLRuntimeProvenance) backendSpec() cloudSQLBackendSpec {
	if runtime == nil {
		return cloudSQLBackendSpec{}
	}
	return cloudSQLBackendSpec{
		Project:                runtime.Project,
		Instance:               runtime.Instance,
		DatabaseVersion:        runtime.DatabaseVersion,
		OwnershipFingerprint:   runtime.OwnershipFingerprint,
		BootstrapPolicy:        runtime.BootstrapPolicy,
		Image:                  runtime.Image,
		ImageID:                runtime.ImageID,
		VolumeIdentity:         runtime.VolumeIdentity,
		ImageAcquisitionIntent: runtime.ImageAcquisitionIntent,
		CreationIntent:         runtime.CreationIntent,
	}
}

func (runtime *cloudSQLRuntimeProvenance) applyBackendSpec(spec cloudSQLBackendSpec) {
	if runtime == nil {
		return
	}
	runtime.Image = spec.Image
	runtime.ImageID = spec.ImageID
	runtime.VolumeIdentity = spec.VolumeIdentity
	runtime.ImageAcquisitionIntent = spec.ImageAcquisitionIntent
	runtime.CreationIntent = spec.CreationIntent
}

func (runtime *cloudSQLRuntimeProvenance) validFor(
	profile string,
	key string,
	instance *DatabaseInstance,
) bool {
	if !runtime.matchesIdentity(profile, key, instance) ||
		runtime.Phase == cloudSQLRuntimePhaseImageAcquisitionIntent ||
		(runtime.Phase != "" && runtime.Phase != cloudSQLRuntimePhaseCreateIntent) ||
		(runtime.Phase == cloudSQLRuntimePhaseCreateIntent &&
			(!runtime.ImageAcquisitionIntent || !runtime.CreationIntent)) ||
		!validCloudSQLImmutableID(runtime.ImageID) ||
		(!validCloudSQLImmutableID(runtime.VolumeIdentity) && !runtime.CreationIntent) {
		return false
	}
	return true
}

func (runtime *cloudSQLRuntimeProvenance) preBackendFor(
	profile string,
	key string,
	instance *DatabaseInstance,
) bool {
	return runtime.matchesIdentity(profile, key, instance) &&
		runtime.Phase == cloudSQLRuntimePhaseImageAcquisitionIntent &&
		runtime.ImageAcquisitionIntent &&
		runtime.ImageID == "" &&
		runtime.VolumeIdentity == "" &&
		!runtime.CreationIntent
}

func (runtime *cloudSQLRuntimeProvenance) matchesIdentity(
	profile string,
	key string,
	instance *DatabaseInstance,
) bool {
	if runtime == nil || instance == nil ||
		runtime.Profile != profile ||
		runtime.Project != instance.Project ||
		runtime.Instance != instance.Name ||
		runtime.DatabaseVersion != instance.DatabaseVersion ||
		runtime.BootstrapPolicy != cloudSQLBootstrapPolicyV1 ||
		runtime.Image == "" ||
		key != instanceKey(runtime.Project, runtime.Instance) {
		return false
	}
	return validCloudSQLOwnershipFingerprint(runtime.OwnershipFingerprint)
}

func cloneCloudSQLRuntime(runtime *cloudSQLRuntimeProvenance) *cloudSQLRuntimeProvenance {
	if runtime == nil {
		return nil
	}
	clone := *runtime
	return &clone
}

func cloudSQLProfile(store cloudSQLStore) string {
	if profiled, ok := store.(interface{ Profile() string }); ok && profiled.Profile() != "" {
		return profiled.Profile()
	}
	return config.GetProfile()
}

func cloudSQLLoopbackAddress(endpoint string) (string, error) {
	address := strings.TrimPrefix(endpoint, "http://")
	if address == endpoint || strings.Contains(address, "/") {
		return "", fmt.Errorf("Cloud SQL reconciled endpoint %q is not a loopback HTTP endpoint", endpoint)
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("parse Cloud SQL reconciled endpoint: %w", err)
	}
	parsedHost := net.ParseIP(host)
	port, portErr := strconv.Atoi(portText)
	if parsedHost == nil || !parsedHost.IsLoopback() || portErr != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("Cloud SQL reconciled endpoint %q is not safe", endpoint)
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

// validateCloudSQLMetadataImport is deliberately side-effect-free. Portable
// snapshots may carry non-secret ownership fingerprints, but restart
// reconciliation additionally requires a local provenance record that Export
// never includes.
func validateCloudSQLMetadataImport(
	_ state.EntryValidationContext,
	metadata *cloudSQLMetadata,
) error {
	if metadata == nil {
		return nil
	}
	for key, instance := range metadata.Instances {
		if instance == nil {
			return fmt.Errorf("Cloud SQL instance %q is null", key)
		}
		expectedImage, err := orchestrator.CloudSQLImageForDatabaseVersion(instance.DatabaseVersion)
		if err != nil {
			return fmt.Errorf("Cloud SQL instance %q: %w", key, err)
		}
		runtime := metadata.Runtimes[key]
		if runtime == nil {
			continue
		}
		if runtime.Profile == "" ||
			runtime.Project != instance.Project ||
			runtime.Instance != instance.Name ||
			runtime.DatabaseVersion != instance.DatabaseVersion ||
			key != instanceKey(instance.Project, instance.Name) {
			return fmt.Errorf("Cloud SQL runtime %q does not match its instance ownership", key)
		}
		if runtime.BootstrapPolicy != cloudSQLBootstrapPolicyV1 {
			return fmt.Errorf("Cloud SQL runtime %q has unsupported bootstrap policy %q", key, runtime.BootstrapPolicy)
		}
		if runtime.Image != expectedImage {
			return fmt.Errorf("Cloud SQL runtime %q image %q does not match database version", key, runtime.Image)
		}
		if !validCloudSQLOwnershipFingerprint(runtime.OwnershipFingerprint) {
			return fmt.Errorf("Cloud SQL runtime %q has invalid ownership fingerprint", key)
		}
		if runtime.preBackendFor(runtime.Profile, key, instance) {
			continue
		}
		if runtime.Phase != "" && runtime.Phase != cloudSQLRuntimePhaseCreateIntent {
			return fmt.Errorf("Cloud SQL runtime %q has unsupported phase %q", key, runtime.Phase)
		}
		if runtime.Phase == cloudSQLRuntimePhaseCreateIntent && !runtime.CreationIntent {
			return fmt.Errorf("Cloud SQL runtime %q create intent is incomplete", key)
		}
		if !validCloudSQLImmutableID(runtime.ImageID) {
			return fmt.Errorf("Cloud SQL runtime %q has invalid image ID", key)
		}
		if !validCloudSQLImmutableID(runtime.VolumeIdentity) && !runtime.CreationIntent {
			return fmt.Errorf("Cloud SQL runtime %q has invalid volume identity", key)
		}
	}
	for key := range metadata.Runtimes {
		if metadata.Instances[key] == nil {
			return fmt.Errorf("Cloud SQL runtime %q has no matching instance", key)
		}
	}
	for key, users := range metadata.Users {
		for _, user := range users {
			if user != nil && user.Password != "" {
				return fmt.Errorf("Cloud SQL user metadata %q contains a portable password", key)
			}
		}
	}
	return nil
}

func cloudSQLHasLocalProvenance(
	store cloudSQLStore,
	key string,
	runtime *cloudSQLRuntimeProvenance,
) (bool, error) {
	if runtime == nil || !validCloudSQLOwnershipFingerprint(runtime.OwnershipFingerprint) {
		return false, nil
	}
	markers, err := cloudSQLMarkers(store)
	if err != nil {
		return false, err
	}
	payload, found, err := markers.ReadLocalMarker(
		cloudSQLLocalRuntimeDir,
		cloudSQLLocalProvenanceName(key, runtime.OwnershipFingerprint),
	)
	if err != nil {
		return false, fmt.Errorf("read Cloud SQL local provenance: %w", err)
	}
	return found && strings.TrimSpace(string(payload)) == runtime.OwnershipFingerprint, nil
}

func writeCloudSQLLocalProvenance(store cloudSQLStore, key, fingerprint string) error {
	if !validCloudSQLOwnershipFingerprint(fingerprint) {
		return errors.New("invalid Cloud SQL ownership fingerprint")
	}
	markers, err := cloudSQLMarkers(store)
	if err != nil {
		return err
	}
	return markers.WriteLocalMarker(
		cloudSQLLocalRuntimeDir,
		cloudSQLLocalProvenanceName(key, fingerprint),
		[]byte(fingerprint+"\n"),
	)
}

func removeCloudSQLLocalProvenance(store cloudSQLStore, key, fingerprint string) error {
	if !validCloudSQLOwnershipFingerprint(fingerprint) {
		return errors.New("invalid Cloud SQL ownership fingerprint")
	}
	markers, err := cloudSQLMarkers(store)
	if err != nil {
		return err
	}
	return markers.RemoveLocalMarker(
		cloudSQLLocalRuntimeDir,
		cloudSQLLocalProvenanceName(key, fingerprint),
		[]byte(fingerprint+"\n"),
	)
}

func cloudSQLMarkers(store cloudSQLStore) (cloudSQLLocalMarkerStore, error) {
	if _, err := cloudSQLProfileDirectory(store); err != nil {
		return nil, err
	}
	markers, ok := store.(cloudSQLLocalMarkerStore)
	if !ok {
		return nil, errors.New("Cloud SQL local provenance requires pinned marker operations")
	}
	return markers, nil
}

func cloudSQLProfileDirectory(store cloudSQLStore) (string, error) {
	if store == nil {
		return "", errors.New("Cloud SQL local provenance requires a state store")
	}
	profiled, ok := store.(interface{ ProfileDir() string })
	if !ok {
		return "", errors.New("Cloud SQL local provenance requires a profile-directory-capable store")
	}
	profileDir := filepath.Clean(profiled.ProfileDir())
	if profiled.ProfileDir() == "" || !filepath.IsAbs(profileDir) {
		return "", errors.New("Cloud SQL local provenance requires a non-empty absolute profile directory")
	}
	return profileDir, nil
}

func cloudSQLLocalProvenancePath(profileDir, key, fingerprint string) string {
	return filepath.Join(profileDir, cloudSQLLocalRuntimeDir, cloudSQLLocalProvenanceName(key, fingerprint))
}

func cloudSQLLocalProvenanceName(key, fingerprint string) string {
	sum := sha256.Sum256([]byte(key + "\x00" + fingerprint))
	return fmt.Sprintf("%x", sum[:])
}

func validCloudSQLImmutableID(identity string) bool {
	if !strings.HasPrefix(identity, "sha256:") || len(identity) != len("sha256:")+sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(identity, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}
