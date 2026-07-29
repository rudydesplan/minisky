package cloudsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

type fakeCloudSQLBackend struct {
	mu                sync.Mutex
	createErr         error
	prepareErr        error
	reconcileErr      error
	reconcileEndpoint string
	beforePrepare     func(cloudSQLBackendSpec) error
	beforeCreate      func(cloudSQLBackendSpec) error
	deleteErr         error
	adminErr          error
	adminErrors       map[string]error
	adminCalled       chan struct{}
	prepareStarted    chan struct{}
	prepareRelease    chan struct{}
	createStarted     chan struct{}
	createRelease     chan struct{}
	prepareStartOnce  sync.Once
	createStartOnce   sync.Once
	deleteCalls       int
	prepareCalls      int
	createCalls       int
	reconcileCalls    int
	created           bool
	deleted           []string
	admin             []string
	events            []string
}

type failingCloudSQLStore struct {
	mu       sync.Mutex
	saves    int
	failFrom int
	dir      string
}

type failingOperationStore struct{}

type flakyCloudSQLStore struct {
	mu     sync.Mutex
	saves  int
	failOn map[int]bool
	data   []byte
	dir    string
}

type blockingRunnableCloudSQLStore struct {
	mu      sync.Mutex
	saves   int
	data    []byte
	dir     string
	entered chan struct{}
	release chan struct{}
	blockOn int
}

func (s *failingCloudSQLStore) ProfileDir() string {
	return cloudSQLTestMarkerStore(s.dir).ProfileDir()
}
func (s *flakyCloudSQLStore) ProfileDir() string {
	return cloudSQLTestMarkerStore(s.dir).ProfileDir()
}
func (s *blockingRunnableCloudSQLStore) ProfileDir() string {
	return cloudSQLTestMarkerStore(s.dir).ProfileDir()
}

func cloudSQLTestMarkerStore(root string) *state.Store {
	store, err := state.New(root, "cloudsql-test")
	if err != nil {
		panic(err)
	}
	return store
}

func readCloudSQLTestMarker(root, namespace, name string) ([]byte, bool, error) {
	return cloudSQLTestMarkerStore(root).ReadLocalMarker(namespace, name)
}

func writeCloudSQLTestMarker(root, namespace, name string, payload []byte) error {
	return cloudSQLTestMarkerStore(root).WriteLocalMarker(namespace, name, payload)
}

func removeCloudSQLTestMarker(root, namespace, name string, expected []byte) error {
	return cloudSQLTestMarkerStore(root).RemoveLocalMarker(namespace, name, expected)
}

func (s *failingCloudSQLStore) ReadLocalMarker(namespace, name string) ([]byte, bool, error) {
	return readCloudSQLTestMarker(s.dir, namespace, name)
}
func (s *failingCloudSQLStore) WriteLocalMarker(namespace, name string, payload []byte) error {
	return writeCloudSQLTestMarker(s.dir, namespace, name, payload)
}
func (s *failingCloudSQLStore) RemoveLocalMarker(namespace, name string, expected []byte) error {
	return removeCloudSQLTestMarker(s.dir, namespace, name, expected)
}
func (s *flakyCloudSQLStore) ReadLocalMarker(namespace, name string) ([]byte, bool, error) {
	return readCloudSQLTestMarker(s.dir, namespace, name)
}
func (s *flakyCloudSQLStore) WriteLocalMarker(namespace, name string, payload []byte) error {
	return writeCloudSQLTestMarker(s.dir, namespace, name, payload)
}
func (s *flakyCloudSQLStore) RemoveLocalMarker(namespace, name string, expected []byte) error {
	return removeCloudSQLTestMarker(s.dir, namespace, name, expected)
}
func (s *blockingRunnableCloudSQLStore) ReadLocalMarker(namespace, name string) ([]byte, bool, error) {
	return readCloudSQLTestMarker(s.dir, namespace, name)
}
func (s *blockingRunnableCloudSQLStore) WriteLocalMarker(namespace, name string, payload []byte) error {
	return writeCloudSQLTestMarker(s.dir, namespace, name, payload)
}
func (s *blockingRunnableCloudSQLStore) RemoveLocalMarker(namespace, name string, expected []byte) error {
	return removeCloudSQLTestMarker(s.dir, namespace, name, expected)
}

func (failingOperationStore) Load(string, any) error { return state.ErrNotFound }
func (failingOperationStore) Save(string, any) error { return errors.New("operation save failed") }

func (*failingCloudSQLStore) Load(string, any) error { return state.ErrNotFound }
func (s *failingCloudSQLStore) Save(string, any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saves >= s.failFrom {
		return errors.New("save failed")
	}
	return nil
}

func (s *flakyCloudSQLStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *flakyCloudSQLStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.failOn[s.saves] {
		return errors.New("injected transient save failure")
	}
	data, err := json.Marshal(value)
	if err == nil {
		s.data = data
	}
	return err
}

func (s *blockingRunnableCloudSQLStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *blockingRunnableCloudSQLStore) Save(_ string, value any) error {
	s.mu.Lock()
	s.saves++
	save := s.saves
	s.mu.Unlock()
	blockOn := s.blockOn
	if blockOn == 0 {
		blockOn = 3
	}
	if save == blockOn {
		close(s.entered)
		<-s.release
		return errors.New("blocked runnable save failed")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

func (b *fakeCloudSQLBackend) Prepare(
	_ context.Context,
	spec cloudSQLBackendSpec,
) (cloudSQLBackendSpec, error) {
	b.mu.Lock()
	b.prepareCalls++
	b.events = append(b.events, "prepare")
	b.mu.Unlock()
	if b.prepareStarted != nil {
		b.prepareStartOnce.Do(func() { close(b.prepareStarted) })
	}
	if b.prepareRelease != nil {
		<-b.prepareRelease
	}
	if b.beforePrepare != nil {
		if err := b.beforePrepare(spec); err != nil {
			return spec, err
		}
	}
	if spec.Image == "" {
		spec.Image = "postgres:18.3-alpine"
	}
	spec.ImageID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	spec.CreationIntent = true
	return spec, b.prepareErr
}

func (b *fakeCloudSQLBackend) Create(
	_ context.Context,
	spec cloudSQLBackendSpec,
) (cloudSQLCreateResult, error) {
	b.mu.Lock()
	b.createCalls++
	b.events = append(b.events, "create")
	b.mu.Unlock()
	if b.createStarted != nil {
		b.createStartOnce.Do(func() { close(b.createStarted) })
	}
	if b.createRelease != nil {
		<-b.createRelease
	}
	if b.beforeCreate != nil {
		if err := b.beforeCreate(spec); err != nil {
			return cloudSQLCreateResult{Spec: spec}, err
		}
	}
	if spec.Image == "" {
		spec.Image = "postgres:18.3-alpine"
	}
	if spec.ImageID == "" {
		spec.ImageID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	}
	if spec.VolumeIdentity == "" {
		spec.VolumeIdentity = "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	}
	spec.CreationIntent = true
	b.mu.Lock()
	b.events = append(b.events, "create-side-effect")
	b.mu.Unlock()
	return cloudSQLCreateResult{
		Endpoint: "http://127.0.0.1:55432",
		Created:  b.created,
		Spec:     spec,
	}, b.createErr
}

func (b *fakeCloudSQLBackend) Reconcile(
	_ context.Context,
	spec cloudSQLBackendSpec,
) (cloudSQLCreateResult, error) {
	b.mu.Lock()
	b.reconcileCalls++
	b.mu.Unlock()
	if b.reconcileEndpoint == "" && b.reconcileErr == nil {
		return cloudSQLCreateResult{Spec: spec}, errors.New("unexpected reconcile")
	}
	return cloudSQLCreateResult{Endpoint: b.reconcileEndpoint, Spec: spec}, b.reconcileErr
}

func (b *fakeCloudSQLBackend) Delete(_ context.Context, spec cloudSQLBackendSpec) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deleteCalls++
	b.events = append(b.events, "delete")
	b.deleted = append(b.deleted, spec.Project+":"+spec.Instance)
	return b.deleteErr
}

func (b *fakeCloudSQLBackend) ExecuteAdmin(
	_ context.Context,
	spec cloudSQLBackendSpec,
	action, name, password string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.admin = append(b.admin, strings.Join([]string{
		spec.Project,
		spec.Instance,
		spec.DatabaseVersion,
		action,
		name,
		password,
	}, ":"))
	if b.adminCalled != nil {
		select {
		case b.adminCalled <- struct{}{}:
		default:
		}
	}
	if b.adminErrors[action] != nil {
		return b.adminErrors[action]
	}
	return b.adminErr
}

func (b *fakeCloudSQLBackend) adminCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.admin)
}

func (b *fakeCloudSQLBackend) provisioningCalls() (int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.prepareCalls, b.createCalls
}

func (b *fakeCloudSQLBackend) reconcileCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reconcileCalls
}

func (b *fakeCloudSQLBackend) eventLog() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.events...)
}

func (b *fakeCloudSQLBackend) deletes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deleteCalls
}

func TestCreateInstanceReturnsProviderDefaultSettings(t *testing.T) {
	api := newAPIWithBackend(
		orchestrator.NewOperationManager(),
		&fakeCloudSQLBackend{},
		newCloudSQLTestStateStore(t),
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db","databaseVersion":"POSTGRES_18","settings":{"tier":"db-f1-micro"}}`))
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, operation.Name)

	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/instances/db", nil))
	var instance DatabaseInstance
	if err := json.Unmarshal(rec.Body.Bytes(), &instance); err != nil {
		t.Fatal(err)
	}
	if instance.Settings.AvailabilityType != "ZONAL" || instance.Settings.PricingPlan != "PER_USE" {
		t.Fatalf("provider defaults = %#v", instance.Settings)
	}
}

func TestCreatePersistsImmutableBackendIdentities(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "identity")
	store, err := state.New(t.TempDir(), "identity")
	if err != nil {
		t.Fatal(err)
	}
	api, err := newAPIWithStoreAndBackend(
		orchestrator.NewOperationManager(),
		&fakeCloudSQLBackend{},
		store,
	)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if terminal := waitCloudSQLOperation(t, api.opMgr, operation.Name); terminal.Error != nil {
		t.Fatalf("create operation = %#v", terminal)
	}
	key := instanceKey("demo", "db")
	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	runtime := persisted.Runtimes[key]
	if runtime == nil ||
		!validCloudSQLImmutableID(runtime.ImageID) ||
		!validCloudSQLImmutableID(runtime.VolumeIdentity) ||
		!validCloudSQLOwnershipFingerprint(runtime.OwnershipFingerprint) {
		t.Fatalf("persisted immutable runtime = %#v", runtime)
	}
	local, err := cloudSQLHasLocalProvenance(store, key, runtime)
	if err != nil || !local {
		t.Fatalf("local runtime provenance = %v, %v", local, err)
	}
}

func TestCreateRejectsUnsupportedDatabaseVersionBeforeMutation(t *testing.T) {
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_15"}`),
	))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "unsupported database version") {
		t.Fatalf("unsupported create status = %d, body %s", rec.Code, rec.Body.String())
	}
	if len(api.instances) != 0 || backend.created || backend.deletes() != 0 {
		t.Fatalf("unsupported create mutated state/backend: instances=%d backend=%#v", len(api.instances), backend)
	}
}

func TestCreateBackendFailureRetainsRetryableIntent(t *testing.T) {
	backend := &fakeCloudSQLBackend{createErr: errors.New("docker create failed"), created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, newCloudSQLTestStateStore(t))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db","databaseVersion":"POSTGRES_18"}`))
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
	}
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}

	op := waitCloudSQLOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Message != "docker create failed" {
		t.Fatalf("terminal operation = %#v", op)
	}
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/v1/projects/demo/operations/"+initial.Name, nil))
	var polled SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &polled); err != nil {
		t.Fatal(err)
	}
	if polled.Status != "DONE" || polled.Error == nil || polled.Error.Message != "docker create failed" {
		t.Fatalf("polled operation = %#v", polled)
	}
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[instanceKey("demo", "db")])
	runtime := cloneCloudSQLRuntime(api.runtimes[instanceKey("demo", "db")])
	api.mu.RUnlock()
	if instance == nil || instance.State != "SUSPENDED" ||
		!strings.Contains(instance.BackendStatus, "retryable") ||
		runtime == nil || !runtime.CreationIntent {
		t.Fatalf("failed create did not retain retryable intent: instance=%#v runtime=%#v", instance, runtime)
	}
	if backend.deletes() != 0 {
		t.Fatalf("ambiguous backend cleanup calls = %d, want 0", backend.deletes())
	}
}

func TestAmbiguousCreatePersistsIntentAndRecoversAfterRestart(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ambiguous")
	store, err := state.New(t.TempDir(), "ambiguous")
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeCloudSQLBackend{
		createErr: errors.New("ambiguous Docker create response"),
		created:   true,
	}
	backend.beforeCreate = func(spec cloudSQLBackendSpec) error {
		var persisted cloudSQLMetadata
		if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
			return err
		}
		runtime := persisted.Runtimes[instanceKey("demo", "db")]
		if runtime == nil ||
			runtime.Phase != "CREATE_INTENT" ||
			!runtime.CreationIntent ||
			runtime.ImageID != spec.ImageID ||
			persisted.Instances[instanceKey("demo", "db")].State != "PENDING_CREATE" {
			return fmt.Errorf("creation intent was not durable before backend create: %#v", persisted)
		}
		return nil
	}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d, body %s", rec.Code, rec.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if terminal := waitCloudSQLOperation(t, api.opMgr, operation.Name); terminal.Error == nil {
		t.Fatalf("ambiguous create operation = %#v", terminal)
	}
	key := instanceKey("demo", "db")
	if got := api.instances[key]; got == nil || got.State != "SUSPENDED" ||
		!strings.Contains(got.BackendStatus, "retryable") {
		t.Fatalf("ambiguous create metadata = %#v", got)
	}
	if runtime := api.runtimes[key]; runtime == nil || !runtime.CreationIntent ||
		!validCloudSQLImmutableID(runtime.ImageID) ||
		!validCloudSQLImmutableID(runtime.VolumeIdentity) {
		t.Fatalf("ambiguous create runtime = %#v", runtime)
	}
	if backend.deletes() != 0 {
		t.Fatalf("ambiguous create destructively cleaned %d backends", backend.deletes())
	}

	backend.createErr = nil
	backend.reconcileEndpoint = "http://127.0.0.1:55432"
	restarted, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	restarted.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet,
		"/v1/projects/demo/instances/db",
		nil,
	))
	if rec.Code != http.StatusOK {
		t.Fatalf("recovery GET status = %d, body %s", rec.Code, rec.Body.String())
	}
	if got := restarted.instances[key]; got.State != "RUNNABLE" || len(got.IpAddresses) != 1 {
		t.Fatalf("recovered ambiguous create = %#v", got)
	}
}

func TestDeleteBackendFailureFailsOperationAndRetainsMetadata(t *testing.T) {
	backend := &fakeCloudSQLBackend{deleteErr: errors.New("docker delete failed")}
	store, err := state.New(t.TempDir(), "delete-failure")
	if err != nil {
		t.Fatal(err)
	}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("demo", "db")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{
		Name: "db", Project: "demo", State: "RUNNABLE",
		SelfLink: "https://sqladmin.googleapis.com/v1/projects/demo/instances/db",
	})

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body %s", rec.Code, rec.Body.String())
	}
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}

	op := waitCloudSQLOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Message != "docker delete failed" {
		t.Fatalf("terminal operation = %#v", op)
	}
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[key])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" || instance.BackendStatus != "docker delete failed" {
		t.Fatalf("instance after failed delete = %#v", instance)
	}
}

func TestCreateRollbackSaveFailureKeepsDegradedMetadata(t *testing.T) {
	store := &failingCloudSQLStore{failFrom: 2, dir: t.TempDir()}
	backend := &fakeCloudSQLBackend{createErr: errors.New("create failed")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[instanceKey("demo", "db")])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" || instance.BackendStatus == "" {
		t.Fatalf("degraded instance = %#v", instance)
	}
}

func TestPostDeleteSaveFailureKeepsTombstone(t *testing.T) {
	store := &failingCloudSQLStore{failFrom: 2, dir: t.TempDir()}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeCloudSQLBackend{}, store)
	key := instanceKey("demo", "db")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"})
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[key])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" ||
		!strings.Contains(instance.BackendStatus, "backend deleted") {
		t.Fatalf("delete tombstone = %#v", instance)
	}
}

func TestDeleteLegacyInstanceWithoutRuntimeProvenanceFailsClosed(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "legacy")
	store, err := state.New(t.TempDir(), "legacy")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("demo", "db")
	if err := store.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			key: {
				Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18",
				State: "RUNNABLE",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeCloudSQLBackend{}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("legacy delete status = %d, body %s", rec.Code, rec.Body.String())
	}
	if backend.deletes() != 0 {
		t.Fatalf("legacy delete touched backend %d times", backend.deletes())
	}
	if api.instances[key] == nil {
		t.Fatal("legacy delete removed in-memory metadata")
	}
	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Instances[key] == nil {
		t.Fatal("legacy delete removed persisted metadata")
	}

	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if rec.Code != http.StatusConflict {
		t.Fatalf("legacy recreate status = %d, body %s", rec.Code, rec.Body.String())
	}
}

func TestDeleteLegacyPreIntentRuntimeFailsClosed(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "legacy-pre-intent")
	store, err := state.New(t.TempDir(), "legacy-pre-intent")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("demo", "db")
	runtime := &cloudSQLRuntimeProvenance{
		Profile:              "legacy-pre-intent",
		Project:              "demo",
		Instance:             "db",
		DatabaseVersion:      "POSTGRES_18",
		OwnershipFingerprint: strings.Repeat("c", 64),
		BootstrapPolicy:      cloudSQLBootstrapPolicyV1,
		Image:                "postgres:18.3-alpine",
		Phase:                "PREPARE_PENDING",
	}
	if err := writeCloudSQLLocalProvenance(store, key, runtime.OwnershipFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			key: {
				Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18",
				State: "PENDING_CREATE",
			},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{key: runtime},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeCloudSQLBackend{}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodDelete,
		"/v1/projects/demo/instances/db",
		nil,
	))
	if response.Code != http.StatusPreconditionFailed {
		t.Fatalf("legacy pre-intent delete status=%d body=%s", response.Code, response.Body.String())
	}
	if backend.deletes() != 0 {
		t.Fatal("legacy pre-intent delete claimed backend ownership")
	}
	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Instances[key] == nil || persisted.Runtimes[key] == nil {
		t.Fatalf("legacy pre-intent delete removed fail-closed metadata=%#v", persisted)
	}
}

func TestDeletePrecommitSaveFailureDoesNotTouchBackend(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{1: true}, dir: t.TempDir()}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	key := instanceKey("demo", "db")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"})

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if backend.deletes() != 0 {
		t.Fatalf("backend delete calls=%d", backend.deletes())
	}
	if api.instances[key].State != "RUNNABLE" {
		t.Fatalf("precommit state=%q", api.instances[key].State)
	}
}

func TestInstanceDeleteSerializesWithAdminMutations(t *testing.T) {
	backend := &fakeCloudSQLBackend{}
	store, err := state.New(t.TempDir(), "delete-serialization")
	if err != nil {
		t.Fatal(err)
	}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("demo", "db")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"})
	api.adminMu.Lock()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
		done <- response
	}()
	select {
	case <-done:
		t.Fatal("instance deletion bypassed the admin mutation lock")
	case <-time.After(20 * time.Millisecond):
	}
	if backend.deletes() != 0 {
		t.Fatal("backend deletion started while an admin mutation held the lock")
	}
	api.adminMu.Unlock()
	response := <-done
	var operation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, operation.Name)
}

func TestPostDeleteSaveFailurePersistsReconciliationTombstoneAcrossRestart(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{3: true}, dir: t.TempDir()}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	key := instanceKey("demo", "db")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{Name: "db", Project: "demo", State: "RUNNABLE"})
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/projects/demo/instances/db", nil))
	var initial SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)

	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	instance := restarted.instances[key]
	if instance == nil || instance.State != "ERROR" ||
		!strings.Contains(instance.BackendStatus, "backend deleted") {
		t.Fatalf("restarted tombstone=%#v", instance)
	}
}

func TestCreateCompensationFailurePersistsDivergenceAcrossRestart(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{3: true}, dir: t.TempDir()}
	backend := &fakeCloudSQLBackend{created: true, deleteErr: errors.New("compensation failed")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)

	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	instance := restarted.instances[instanceKey("demo", "db")]
	if instance == nil || instance.State != "ERROR" ||
		!strings.Contains(instance.BackendStatus, "compensation failed") {
		t.Fatalf("restarted divergence=%#v", instance)
	}
}

func TestMethodNotAllowedUsesJSONEnvelope(t *testing.T) {
	api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeCloudSQLBackend{}, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch,
		"/v1/projects/demo/instances/db", nil))
	if rec.Code != http.StatusMethodNotAllowed ||
		!strings.Contains(rec.Body.String(), `"status":"METHOD_NOT_ALLOWED"`) {
		t.Fatalf("response = %d %s", rec.Code, rec.Body.String())
	}
}

func TestOperationRegistrationFailureKeepsDegradedInstance(t *testing.T) {
	opMgr, err := orchestrator.NewOperationManagerWithStore(failingOperationStore{})
	if err != nil {
		t.Fatal(err)
	}
	api := newAPIWithBackend(opMgr, &fakeCloudSQLBackend{}, newCloudSQLTestStateStore(t))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[instanceKey("demo", "db")])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" {
		t.Fatalf("instance = %#v", instance)
	}
}

func TestFinalRunningSaveFailureCompensatesOwnedProjectBackend(t *testing.T) {
	store := &failingCloudSQLStore{failFrom: 3, dir: t.TempDir()}
	backend := &fakeCloudSQLBackend{created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/project-a/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	backend.mu.Lock()
	deleted := append([]string(nil), backend.deleted...)
	backend.mu.Unlock()
	if len(deleted) != 1 || deleted[0] != "project-a:db" {
		t.Fatalf("backend compensation = %v", deleted)
	}
	api.mu.RLock()
	instance := cloneDatabaseInstance(api.instances[instanceKey("project-a", "db")])
	api.mu.RUnlock()
	if instance == nil || instance.State != "ERROR" {
		t.Fatalf("degraded tombstone = %#v", instance)
	}
}

func TestCreateDoesNotPublishRunnableBeforeDurableSave(t *testing.T) {
	store := &blockingRunnableCloudSQLStore{
		dir:     t.TempDir(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	backend := &fakeCloudSQLBackend{created: true, adminCalled: make(chan struct{}, 1)}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	<-store.entered

	get := httptest.NewRecorder()
	api.getInstance(get, "demo", "db")
	var visible DatabaseInstance
	if err := json.Unmarshal(get.Body.Bytes(), &visible); err != nil {
		t.Fatal(err)
	}
	if visible.State == "RUNNABLE" {
		t.Error("GET observed RUNNABLE while its durable save was blocked")
	}

	adminDone := make(chan *httptest.ResponseRecorder, 1)
	adminStarted := make(chan struct{})
	go func() {
		close(adminStarted)
		recorder := httptest.NewRecorder()
		api.routeDatabases(
			recorder,
			httptest.NewRequest(http.MethodPost, "/v1/projects/demo/instances/db/databases", strings.NewReader(`{"name":"app"}`)),
			"demo",
			"db",
			"/v1/projects/demo/instances/db/databases",
		)
		adminDone <- recorder
	}()
	<-adminStarted
	select {
	case <-backend.adminCalled:
		t.Error("admin mutated backend while RUNNABLE persistence was blocked")
	default:
	}

	close(store.release)
	terminal := waitCloudSQLOperation(t, api.opMgr, operation.Name)
	if terminal.Error == nil {
		t.Fatalf("terminal operation=%#v, want persistence failure", terminal)
	}
	select {
	case recorder := <-adminDone:
		if recorder.Code != http.StatusPreconditionFailed {
			t.Fatalf("admin status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("admin request did not finish after failed runnable save")
	}
	if backend.adminCount() != 0 {
		t.Fatal("admin mutated backend after failed RUNNABLE persistence")
	}
}

func TestConcurrentDeleteCannotPassBlockedCreateLifecycle(t *testing.T) {
	for _, phase := range []string{"prepare", "create"} {
		t.Run(phase, func(t *testing.T) {
			store := newCloudSQLTestStateStore(t)
			backend := &fakeCloudSQLBackend{}
			var started, release chan struct{}
			if phase == "prepare" {
				backend.prepareErr = context.Canceled
				backend.prepareStarted = make(chan struct{})
				backend.prepareRelease = make(chan struct{})
				started, release = backend.prepareStarted, backend.prepareRelease
			} else {
				backend.createErr = context.Canceled
				backend.created = true
				backend.createStarted = make(chan struct{})
				backend.createRelease = make(chan struct{})
				started, release = backend.createStarted, backend.createRelease
			}
			api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
			create := httptest.NewRecorder()
			api.ServeHTTP(create, httptest.NewRequest(
				http.MethodPost,
				"/v1/projects/demo/instances",
				strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
			))
			if create.Code != http.StatusOK {
				t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
			}
			var createOperation SqlOperation
			if err := json.Unmarshal(create.Body.Bytes(), &createOperation); err != nil {
				t.Fatal(err)
			}
			<-started
			if phase == "prepare" {
				var persisted cloudSQLMetadata
				if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
					t.Fatal(err)
				}
				runtime := persisted.Runtimes[instanceKey("demo", "db")]
				if runtime == nil ||
					runtime.Phase != "IMAGE_ACQUISITION_INTENT" ||
					!runtime.ImageAcquisitionIntent ||
					runtime.ImageID != "" ||
					runtime.VolumeIdentity != "" ||
					runtime.CreationIntent {
					t.Fatalf("blocked prepare durable stage can claim backend ownership: %#v", runtime)
				}
			}

			deleteStarted := make(chan struct{})
			deleteDone := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				close(deleteStarted)
				recorder := httptest.NewRecorder()
				api.ServeHTTP(recorder, httptest.NewRequest(
					http.MethodDelete,
					"/v1/projects/demo/instances/db",
					nil,
				))
				deleteDone <- recorder
			}()
			<-deleteStarted
			select {
			case recorder := <-deleteDone:
				close(release)
				t.Fatalf("delete passed blocked %s: status=%d body=%s", phase, recorder.Code, recorder.Body.String())
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if terminal := waitCloudSQLOperation(t, api.opMgr, createOperation.Name); terminal.Error == nil {
				t.Fatalf("interrupted %s operation=%#v, want error", phase, terminal)
			}
			deleteResponse := <-deleteDone
			if phase == "prepare" {
				if deleteResponse.Code != http.StatusNotFound {
					t.Fatalf("post-prepare delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
				}
			} else {
				if deleteResponse.Code != http.StatusOK {
					t.Fatalf("post-create delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
				}
				var deleteOperation SqlOperation
				if err := json.Unmarshal(deleteResponse.Body.Bytes(), &deleteOperation); err != nil {
					t.Fatal(err)
				}
				if terminal := waitCloudSQLOperation(t, api.opMgr, deleteOperation.Name); terminal.Error != nil {
					t.Fatalf("delete operation=%#v", terminal)
				}
				events := backend.eventLog()
				createSideEffect := slices.Index(events, "create-side-effect")
				deleteCall := slices.Index(events, "delete")
				if createSideEffect < 0 || deleteCall < createSideEffect {
					t.Fatalf("backend order=%v, delete ran before interrupted create side effect", events)
				}
			}
			key := instanceKey("demo", "db")
			api.mu.RLock()
			instance := api.instances[key]
			api.mu.RUnlock()
			if instance != nil {
				t.Fatalf("interrupted %s left in-memory instance=%#v", phase, instance)
			}
			var persisted cloudSQLMetadata
			if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.Instances[key] != nil || persisted.Runtimes[key] != nil {
				t.Fatalf("interrupted %s left orphanable durable state=%#v", phase, persisted)
			}
		})
	}
}

func TestCreateFirstSaveFailureNeverPreparesBackend(t *testing.T) {
	store := &failingCloudSQLStore{failFrom: 1, dir: t.TempDir()}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if prepares, creates := backend.provisioningCalls(); prepares != 0 || creates != 0 {
		t.Fatalf("first-save failure prepared or created backend: prepare=%d create=%d", prepares, creates)
	}
	key := instanceKey("demo", "db")
	api.mu.RLock()
	instance := api.instances[key]
	runtime := api.runtimes[key]
	api.mu.RUnlock()
	if instance != nil || runtime != nil {
		t.Fatalf("first-save failure retained in-memory create: instance=%#v runtime=%#v", instance, runtime)
	}
	local, err := cloudSQLHasLocalProvenance(store, key, &cloudSQLRuntimeProvenance{
		OwnershipFingerprint: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if local {
		t.Fatal("first-save failure retained local provenance")
	}
}

func TestCreatePersistsImageAcquisitionIntentBeforeBackendMutation(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{}, dir: t.TempDir()}
	key := instanceKey("demo", "db")
	backend := &fakeCloudSQLBackend{}
	backend.beforePrepare = func(spec cloudSQLBackendSpec) error {
		var persisted cloudSQLMetadata
		if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
			return fmt.Errorf("load durable acquisition intent: %w", err)
		}
		runtime := persisted.Runtimes[key]
		if runtime == nil ||
			runtime.Phase != cloudSQLRuntimePhaseImageAcquisitionIntent ||
			!runtime.ImageAcquisitionIntent ||
			runtime.Image != spec.Image ||
			runtime.ImageID != "" ||
			runtime.VolumeIdentity != "" ||
			runtime.CreationIntent {
			return fmt.Errorf("backend mutation preceded exact durable image acquisition intent: %#v", runtime)
		}
		return nil
	}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if terminal := waitCloudSQLOperation(t, api.opMgr, operation.Name); terminal.Error != nil {
		t.Fatalf("create operation=%#v", terminal)
	}
}

func TestCreateIntentSaveFailureNeverCreatesOwnedBackend(t *testing.T) {
	store := &flakyCloudSQLStore{failOn: map[int]bool{2: true}, dir: t.TempDir()}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if terminal := waitCloudSQLOperation(t, api.opMgr, operation.Name); terminal.Error == nil {
		t.Fatalf("create operation=%#v, want CREATE_INTENT persistence failure", terminal)
	}
	if prepares, creates := backend.provisioningCalls(); prepares != 1 || creates != 0 {
		t.Fatalf("CREATE_INTENT save failure calls prepare=%d create=%d", prepares, creates)
	}
	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	key := instanceKey("demo", "db")
	if persisted.Instances[key] != nil || persisted.Runtimes[key] != nil {
		t.Fatalf("CREATE_INTENT save failure retained create metadata=%#v", persisted)
	}
}

func TestRestartDuringCreateIntentSaveKeepsImageAcquisitionNonOwned(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "acquisition-crash")
	store := &blockingRunnableCloudSQLStore{
		dir:     t.TempDir(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		blockOn: 2,
	}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	<-store.entered

	restarted, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("demo", "db")
	if instance := restarted.instances[key]; instance == nil ||
		instance.State != "SUSPENDED" ||
		instance.BackendStatus != cloudSQLImageAcquisitionIncompleteState {
		t.Fatalf("restart during CREATE_INTENT save instance=%#v", instance)
	}
	if runtime := restarted.runtimes[key]; runtime == nil ||
		runtime.Phase != cloudSQLRuntimePhaseImageAcquisitionIntent ||
		!runtime.ImageAcquisitionIntent ||
		runtime.ImageID != "" ||
		runtime.CreationIntent {
		t.Fatalf("restart during CREATE_INTENT save runtime=%#v", runtime)
	}
	if prepares, creates := backend.provisioningCalls(); prepares != 1 || creates != 0 {
		t.Fatalf("restart replayed owned backend: prepare=%d create=%d", prepares, creates)
	}

	close(store.release)
	if terminal := waitCloudSQLOperation(t, api.opMgr, operation.Name); terminal.Error == nil {
		t.Fatalf("interrupted CREATE_INTENT save operation=%#v, want error", terminal)
	}
	if backend.deletes() != 0 {
		t.Fatal("interrupted image acquisition attempted owned backend deletion")
	}
}

func TestImageAcquisitionFailureCanRetryWithoutOwnedBackendCleanup(t *testing.T) {
	store := newCloudSQLTestStateStore(t)
	backend := &fakeCloudSQLBackend{prepareErr: errors.New("image pull interrupted")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	create := func() SqlOperation {
		t.Helper()
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(
			http.MethodPost,
			"/v1/projects/demo/instances",
			strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
		))
		if response.Code != http.StatusOK {
			t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
		}
		var operation SqlOperation
		if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
			t.Fatal(err)
		}
		return operation
	}
	first := create()
	if terminal := waitCloudSQLOperation(t, api.opMgr, first.Name); terminal.Error == nil {
		t.Fatalf("first acquisition operation=%#v, want error", terminal)
	}
	if backend.deletes() != 0 {
		t.Fatal("image acquisition failure attempted owned backend cleanup")
	}
	backend.mu.Lock()
	backend.prepareErr = nil
	backend.mu.Unlock()
	second := create()
	if terminal := waitCloudSQLOperation(t, api.opMgr, second.Name); terminal.Error != nil {
		t.Fatalf("retry acquisition operation=%#v", terminal)
	}
	if prepares, creates := backend.provisioningCalls(); prepares != 2 || creates != 1 {
		t.Fatalf("retry calls prepare=%d create=%d", prepares, creates)
	}
}

func TestCreateFirstSaveFailureSerializesImmediateDelete(t *testing.T) {
	store := &blockingRunnableCloudSQLStore{
		dir:     t.TempDir(),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		blockOn: 1,
	}
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	createDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(
			http.MethodPost,
			"/v1/projects/demo/instances",
			strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
		))
		createDone <- response
	}()
	<-store.entered
	deleteDone := make(chan *httptest.ResponseRecorder, 1)
	deleteStarted := make(chan struct{})
	go func() {
		close(deleteStarted)
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(
			http.MethodDelete,
			"/v1/projects/demo/instances/db",
			nil,
		))
		deleteDone <- response
	}()
	<-deleteStarted
	deadline := time.Now().Add(100 * time.Millisecond)
	mutatedBeforeFirstSave := false
	for time.Now().Before(deadline) {
		api.mu.RLock()
		state := api.instances[instanceKey("demo", "db")].State
		api.mu.RUnlock()
		if state == "PENDING_DELETE" {
			mutatedBeforeFirstSave = true
			break
		}
		runtime.Gosched()
	}
	close(store.release)
	if create := <-createDone; create.Code != http.StatusInternalServerError {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	if deleted := <-deleteDone; deleted.Code != http.StatusNotFound {
		t.Fatalf("delete raced failed first save: status=%d body=%s", deleted.Code, deleted.Body.String())
	}
	if mutatedBeforeFirstSave {
		t.Fatal("delete mutated create state while its first durable save was unresolved")
	}
	if prepares, creates := backend.provisioningCalls(); prepares != 0 || creates != 0 {
		t.Fatalf("failed first save touched backend: prepare=%d create=%d", prepares, creates)
	}
	if backend.deletes() != 0 {
		t.Fatal("failed first save caused backend deletion")
	}
}

func TestRestartImageAcquisitionIntentAllowsMetadataOnlyDelete(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "pre-backend")
	store, err := state.New(t.TempDir(), "pre-backend")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("demo", "db")
	runtime := &cloudSQLRuntimeProvenance{
		Profile:                "pre-backend",
		Project:                "demo",
		Instance:               "db",
		DatabaseVersion:        "POSTGRES_18",
		OwnershipFingerprint:   strings.Repeat("b", 64),
		BootstrapPolicy:        cloudSQLBootstrapPolicyV1,
		Image:                  "postgres:18.3-alpine",
		ImageAcquisitionIntent: true,
		Phase:                  "IMAGE_ACQUISITION_INTENT",
	}
	if err := writeCloudSQLLocalProvenance(store, key, runtime.OwnershipFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			key: {
				Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18",
				State: "PENDING_CREATE",
			},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{key: runtime},
	}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeCloudSQLBackend{}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	if got := api.instances[key]; got == nil ||
		got.State != "SUSPENDED" ||
		got.BackendStatus != "IMAGE_ACQUISITION_INCOMPLETE" {
		t.Fatalf("restarted pre-backend instance=%#v", got)
	}
	if err := api.reconcileRestored(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.reconcileCount() != 0 {
		t.Fatal("pre-backend phase attempted backend reconciliation")
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodDelete,
		"/v1/projects/demo/instances/db",
		nil,
	))
	if response.Code != http.StatusOK {
		t.Fatalf("metadata-only delete status=%d body=%s", response.Code, response.Body.String())
	}
	var operation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if terminal := waitCloudSQLOperation(t, api.opMgr, operation.Name); terminal.Error != nil {
		t.Fatalf("metadata-only delete operation=%#v", terminal)
	}
	if backend.deletes() != 0 {
		t.Fatal("pre-backend metadata deletion called backend delete")
	}
	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Instances[key] != nil || persisted.Runtimes[key] != nil {
		t.Fatalf("pre-backend delete retained metadata=%#v", persisted)
	}
	local, err := cloudSQLHasLocalProvenance(store, key, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if local {
		t.Fatal("pre-backend delete retained local marker")
	}
}

func TestCreateRollbackTransientPersistenceRestoresRuntimeUntilDelete(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "rollback-runtime")
	store := &flakyCloudSQLStore{
		failOn: map[int]bool{3: true, 4: true},
		dir:    t.TempDir(),
	}
	backend := &fakeCloudSQLBackend{created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var createOperation SqlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &createOperation); err != nil {
		t.Fatal(err)
	}
	if terminal := waitCloudSQLOperation(t, api.opMgr, createOperation.Name); terminal.Error == nil {
		t.Fatalf("create operation=%#v, want failed publication", terminal)
	}
	key := instanceKey("demo", "db")
	api.mu.RLock()
	inMemoryRuntime := cloneCloudSQLRuntime(api.runtimes[key])
	api.mu.RUnlock()
	if inMemoryRuntime == nil {
		t.Fatal("rollback persistence failure dropped in-memory runtime provenance")
	}

	restarted, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.runtimes[key] == nil || restarted.instances[key] == nil {
		t.Fatalf("restart lost rollback disposition: instance=%#v runtime=%#v",
			restarted.instances[key], restarted.runtimes[key])
	}
	local, err := cloudSQLHasLocalProvenance(store, key, restarted.runtimes[key])
	if err != nil || !local {
		t.Fatalf("restart local provenance local=%t err=%v", local, err)
	}
	deleteResponse := httptest.NewRecorder()
	restarted.ServeHTTP(deleteResponse, httptest.NewRequest(
		http.MethodDelete,
		"/v1/projects/demo/instances/db",
		nil,
	))
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("eventual delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	var deleteOperation SqlOperation
	if err := json.Unmarshal(deleteResponse.Body.Bytes(), &deleteOperation); err != nil {
		t.Fatal(err)
	}
	if terminal := waitCloudSQLOperation(t, restarted.opMgr, deleteOperation.Name); terminal.Error != nil {
		t.Fatalf("eventual delete operation=%#v", terminal)
	}
	if backend.deletes() != 2 {
		t.Fatalf("backend deletes=%d, want compensation plus eventual idempotent cleanup", backend.deletes())
	}
	local, err = cloudSQLHasLocalProvenance(store, key, inMemoryRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if local {
		t.Fatal("eventual delete retained rollback marker")
	}
}

func TestDockerBackedCreateRejectsNilStateStore(t *testing.T) {
	backend := &fakeCloudSQLBackend{created: true}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/instances",
		strings.NewReader(`{"name":"db","databaseVersion":"POSTGRES_18"}`),
	))
	if response.Code == http.StatusOK {
		t.Fatalf("nil-store create status=%d body=%s", response.Code, response.Body.String())
	}
	prepareCalls, createCalls := backend.provisioningCalls()
	if prepareCalls != 0 || createCalls != 0 || backend.adminCount() != 0 || backend.deletes() != 0 {
		t.Fatalf("nil-store create touched backend: prepare=%d create=%d admin=%d delete=%d",
			prepareCalls, createCalls, backend.adminCount(), backend.deletes())
	}
}

func TestDockerBackedAdminAndDeleteRequireAnchoredProvenance(t *testing.T) {
	tests := []struct {
		name  string
		store cloudSQLStore
	}{
		{name: "nil store"},
		{name: "empty profile directory", store: &emptyProfileCloudSQLStore{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, action := range []string{"admin", "delete"} {
				t.Run(action, func(t *testing.T) {
					backend := &fakeCloudSQLBackend{}
					api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, test.store)
					key := instanceKey("demo", "db")
					api.instances[key] = &DatabaseInstance{
						Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
					}
					api.runtimes[key] = testCloudSQLRuntime(cloudSQLProfile(test.store), "demo", "db")
					recorder := httptest.NewRecorder()
					if action == "admin" {
						api.ServeHTTP(recorder, httptest.NewRequest(
							http.MethodPost,
							"/v1/projects/demo/instances/db/databases",
							strings.NewReader(`{"name":"app"}`),
						))
					} else {
						api.ServeHTTP(recorder, httptest.NewRequest(
							http.MethodDelete,
							"/v1/projects/demo/instances/db",
							nil,
						))
					}
					if recorder.Code == http.StatusOK {
						t.Fatalf("%s unexpectedly succeeded: %s", action, recorder.Body.String())
					}
					if backend.adminCount() != 0 || backend.deletes() != 0 {
						t.Fatalf("%s touched backend: admin=%d delete=%d",
							action, backend.adminCount(), backend.deletes())
					}
				})
			}
		})
	}
}

func TestCreateFailureWithoutOwnershipDoesNotCompensate(t *testing.T) {
	backend := &fakeCloudSQLBackend{createErr: errors.New("collision"), created: false}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, newCloudSQLTestStateStore(t))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/projects/project-a/instances",
		bytes.NewBufferString(`{"name":"db"}`)))
	var initial SqlOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	waitCloudSQLOperation(t, api.opMgr, initial.Name)
	if backend.deletes() != 0 {
		t.Fatalf("unowned backend compensation calls = %d", backend.deletes())
	}
}

func TestDatabaseAndUserMutationsExecuteAgainstBackendBeforeMetadata(t *testing.T) {
	backend := &fakeCloudSQLBackend{}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, newCloudSQLTestStateStore(t))
	key := instanceKey("demo", "db")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{
		Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
	})

	for _, request := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/projects/demo/instances/db/databases", `{"name":"app"}`},
		{http.MethodPost, "/v1/projects/demo/instances/db/users", `{"name":"app_user","password":"secret"}`},
		{http.MethodDelete, "/v1/projects/demo/instances/db/databases/app", ""},
		{http.MethodDelete, "/v1/projects/demo/instances/db/users?name=app_user", ""},
	} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(request.method, request.path, strings.NewReader(request.body)))
		if response.Code != http.StatusOK {
			t.Fatalf("%s %s status=%d body=%s", request.method, request.path, response.Code, response.Body.String())
		}
	}

	backend.mu.Lock()
	got := append([]string(nil), backend.admin...)
	backend.mu.Unlock()
	want := []string{
		"demo:db:POSTGRES_18:CREATE_DATABASE:app:",
		"demo:db:POSTGRES_18:CREATE_USER:app_user:secret",
		"demo:db:POSTGRES_18:DELETE_DATABASE:app:",
		"demo:db:POSTGRES_18:DELETE_USER:app_user:",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("backend admin calls = %#v, want %#v", got, want)
	}
}

func TestChildCreateCompensationFailurePersistsParentReconciliationAcrossRestart(t *testing.T) {
	tests := []struct {
		name               string
		path               string
		body               string
		compensationAction string
		assertChild        func(*testing.T, *API, string)
	}{
		{
			name: "database", path: "/v1/projects/demo/instances/db/databases",
			body: `{"name":"app"}`, compensationAction: "DELETE_DATABASE",
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.databases[key]; len(got) != 1 || got[0].Name != "app" {
					t.Fatalf("database reconciliation metadata=%#v", got)
				}
			},
		},
		{
			name: "user", path: "/v1/projects/demo/instances/db/users",
			body: `{"name":"app_user"}`, compensationAction: "DELETE_USER",
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.users[key]; len(got) != 1 || got[0].Name != "app_user" {
					t.Fatalf("user reconciliation metadata=%#v", got)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &flakyCloudSQLStore{failOn: map[int]bool{2: true}, dir: t.TempDir()}
			backend := &fakeCloudSQLBackend{adminErrors: map[string]error{
				test.compensationAction: errors.New("compensation failed"),
			}}
			api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, store)
			key := instanceKey("demo", "db")
			seedRunnableCloudSQL(api, key, &DatabaseInstance{
				Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
			})
			if err := api.persistMetadata(); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body)))
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
			if err != nil {
				t.Fatal(err)
			}
			instance := restarted.instances[key]
			if instance == nil || instance.State != "ERROR" ||
				!strings.Contains(instance.BackendStatus, "compensation failed") {
				t.Fatalf("parent reconciliation=%#v", instance)
			}
			test.assertChild(t, restarted, key)
		})
	}
}

func TestChildPostDeleteSaveFailurePersistsParentReconciliationAcrossRestart(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		seed        func(*API, string)
		assertChild func(*testing.T, *API, string)
	}{
		{
			name: "database", path: "/v1/projects/demo/instances/db/databases/app",
			seed: func(api *API, key string) {
				api.databases[key] = []*Database{{Name: "app", Project: "demo", Instance: "db"}}
			},
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.databases[key]; len(got) != 1 || got[0].Name != "app" {
					t.Fatalf("database phantom evidence=%#v", got)
				}
			},
		},
		{
			name: "user", path: "/v1/projects/demo/instances/db/users?name=app_user",
			seed: func(api *API, key string) {
				api.users[key] = []*User{{Name: "app_user", Project: "demo", Instance: "db"}}
			},
			assertChild: func(t *testing.T, api *API, key string) {
				if got := api.users[key]; len(got) != 1 || got[0].Name != "app_user" {
					t.Fatalf("user phantom evidence=%#v", got)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &flakyCloudSQLStore{failOn: map[int]bool{2: true}, dir: t.TempDir()}
			api := newAPIWithBackend(orchestrator.NewOperationManager(), &fakeCloudSQLBackend{}, store)
			key := instanceKey("demo", "db")
			seedRunnableCloudSQL(api, key, &DatabaseInstance{
				Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
			})
			test.seed(api, key)
			if err := api.persistMetadata(); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, test.path, nil))
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
			if err != nil {
				t.Fatal(err)
			}
			instance := restarted.instances[key]
			if instance == nil || instance.State != "ERROR" ||
				!strings.Contains(instance.BackendStatus, "backend deleted") {
				t.Fatalf("parent reconciliation=%#v", instance)
			}
			test.assertChild(t, restarted, key)
		})
	}
}

func TestDatabaseMutationFailsClosedWhenBackendUnavailable(t *testing.T) {
	backend := &fakeCloudSQLBackend{adminErr: errors.New("database backend unavailable")}
	api := newAPIWithBackend(orchestrator.NewOperationManager(), backend, newCloudSQLTestStateStore(t))
	key := instanceKey("demo", "db")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{
		Name: "db", Project: "demo", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
	})
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/instances/db/databases", strings.NewReader(`{"name":"app"}`)))
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(api.databases[instanceKey("demo", "db")]) != 0 {
		t.Fatal("backend failure created database metadata")
	}
}

func seedRunnableCloudSQL(api *API, key string, instance *DatabaseInstance) {
	if instance.DatabaseVersion == "" {
		instance.DatabaseVersion = "POSTGRES_18"
	}
	api.instances[key] = instance
	api.runtimes[key] = testCloudSQLRuntime(
		cloudSQLProfile(api.stateStore),
		instance.Project,
		instance.Name,
	)
	api.runtimes[key].DatabaseVersion = instance.DatabaseVersion
	if err := writeCloudSQLLocalProvenance(
		api.stateStore,
		key,
		api.runtimes[key].OwnershipFingerprint,
	); err != nil {
		panic(err)
	}
}

func newCloudSQLTestStateStore(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.New(t.TempDir(), "test")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func waitCloudSQLOperation(t *testing.T, manager *orchestrator.OperationManager, name string) *orchestrator.Operation {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if op := manager.Get(name); op != nil && op.Done {
			return op
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("operation %q did not finish", name)
	return nil
}
