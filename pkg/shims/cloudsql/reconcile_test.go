package cloudsql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

type reconcileCloudSQLBackend struct {
	mu         sync.Mutex
	endpoint   string
	err        error
	calls      int
	admin      int
	started    chan struct{}
	release    chan struct{}
	startOnce  sync.Once
	allowAdmin bool
}

func (*reconcileCloudSQLBackend) Prepare(
	context.Context,
	cloudSQLBackendSpec,
) (cloudSQLBackendSpec, error) {
	return cloudSQLBackendSpec{}, errors.New("unexpected prepare")
}

func (b *reconcileCloudSQLBackend) Create(
	context.Context,
	cloudSQLBackendSpec,
) (cloudSQLCreateResult, error) {
	return cloudSQLCreateResult{}, errors.New("unexpected create")
}

func (b *reconcileCloudSQLBackend) Reconcile(
	ctx context.Context,
	spec cloudSQLBackendSpec,
) (cloudSQLCreateResult, error) {
	b.mu.Lock()
	b.calls++
	endpoint := b.endpoint
	err := b.err
	b.mu.Unlock()
	if b.started != nil {
		b.startOnce.Do(func() { close(b.started) })
	}
	if b.release != nil {
		select {
		case <-b.release:
		case <-ctx.Done():
			return cloudSQLCreateResult{Spec: spec}, ctx.Err()
		}
	}
	return cloudSQLCreateResult{Endpoint: endpoint, Spec: spec}, err
}

func (*reconcileCloudSQLBackend) Delete(context.Context, cloudSQLBackendSpec) error {
	return errors.New("unexpected delete")
}

func (b *reconcileCloudSQLBackend) ExecuteAdmin(
	context.Context,
	cloudSQLBackendSpec,
	string,
	string,
	string,
) error {
	b.mu.Lock()
	b.admin++
	allowed := b.allowAdmin
	b.mu.Unlock()
	if allowed {
		return nil
	}
	return errors.New("unexpected admin replay")
}

func (b *reconcileCloudSQLBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *reconcileCloudSQLBackend) adminCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.admin
}

func (b *reconcileCloudSQLBackend) setResult(endpoint string, err error) {
	b.mu.Lock()
	b.endpoint = endpoint
	b.err = err
	b.mu.Unlock()
}

type reconcileFailingStore struct {
	payload []byte
	saves   int
	dir     string
}

type budgetRestartCloudSQLBackend struct {
	blockFirst bool
	mu         sync.Mutex
	calls      []string
}

func (*budgetRestartCloudSQLBackend) Prepare(
	context.Context,
	cloudSQLBackendSpec,
) (cloudSQLBackendSpec, error) {
	return cloudSQLBackendSpec{}, errors.New("unexpected prepare")
}

func (*budgetRestartCloudSQLBackend) Create(
	context.Context,
	cloudSQLBackendSpec,
) (cloudSQLCreateResult, error) {
	return cloudSQLCreateResult{}, errors.New("unexpected create")
}

func (b *budgetRestartCloudSQLBackend) Reconcile(
	ctx context.Context,
	spec cloudSQLBackendSpec,
) (cloudSQLCreateResult, error) {
	b.mu.Lock()
	b.calls = append(b.calls, spec.Instance)
	b.mu.Unlock()
	if spec.Instance == "first" {
		if b.blockFirst {
			<-ctx.Done()
			return cloudSQLCreateResult{Spec: spec}, ctx.Err()
		}
		return cloudSQLCreateResult{Spec: spec}, errors.New("first backend unavailable")
	}
	return cloudSQLCreateResult{Endpoint: "http://127.0.0.1:55432", Spec: spec}, nil
}

func (*budgetRestartCloudSQLBackend) Delete(context.Context, cloudSQLBackendSpec) error {
	return errors.New("unexpected delete")
}

func (*budgetRestartCloudSQLBackend) ExecuteAdmin(
	context.Context,
	cloudSQLBackendSpec,
	string,
	string,
	string,
) error {
	return errors.New("unexpected admin")
}

func (b *budgetRestartCloudSQLBackend) callLog() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.calls...)
}

func (s *reconcileFailingStore) ProfileDir() string {
	return cloudSQLTestMarkerStore(s.dir).ProfileDir()
}
func (s *reconcileFailingStore) ReadLocalMarker(namespace, name string) ([]byte, bool, error) {
	return readCloudSQLTestMarker(s.dir, namespace, name)
}
func (s *reconcileFailingStore) WriteLocalMarker(namespace, name string, payload []byte) error {
	return writeCloudSQLTestMarker(s.dir, namespace, name, payload)
}
func (s *reconcileFailingStore) RemoveLocalMarker(namespace, name string, expected []byte) error {
	return removeCloudSQLTestMarker(s.dir, namespace, name, expected)
}

type nonProfileCloudSQLStore struct{}

type emptyProfileCloudSQLStore struct{}

func (*nonProfileCloudSQLStore) Load(string, any) error   { return state.ErrNotFound }
func (*nonProfileCloudSQLStore) Save(string, any) error   { return nil }
func (*emptyProfileCloudSQLStore) Load(string, any) error { return state.ErrNotFound }
func (*emptyProfileCloudSQLStore) Save(string, any) error { return nil }
func (*emptyProfileCloudSQLStore) ProfileDir() string     { return "" }

func (s *reconcileFailingStore) Load(_ string, target any) error {
	return json.Unmarshal(s.payload, target)
}

func (s *reconcileFailingStore) Save(string, any) error {
	s.saves++
	return errors.New("reconcile save failed")
}

func TestCloudSQLRestartReconcilesRunnableBackendOnce(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedCloudSQLRestartState(t, store, key, "restart", "RUNNABLE")

	backend := &reconcileCloudSQLBackend{
		endpoint: "http://127.0.0.1:55432",
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	if got := api.instances[key]; got.State != "SUSPENDED" || len(got.IpAddresses) != 0 {
		t.Fatalf("pre-reconcile instance = %#v", got)
	}

	results := make(chan error, 2)
	go func() { results <- api.reconcileRestored(context.Background()) }()
	<-backend.started
	go func() { results <- api.reconcileRestored(context.Background()) }()
	close(backend.release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if backend.callCount() != 1 {
		t.Fatalf("reconcile calls = %d, want 1", backend.callCount())
	}
	instance := api.instances[key]
	if instance.State != "RUNNABLE" || instance.BackendStatus != "" ||
		len(instance.IpAddresses) != 1 || instance.IpAddresses[0].IpAddress != "127.0.0.1:55432" {
		t.Fatalf("reconciled instance = %#v", instance)
	}
	if got := api.databases[key]; len(got) != 1 || got[0].Name != "app" {
		t.Fatalf("restart database rows = %#v", got)
	}
	if got := api.users[key]; len(got) != 1 || got[0].Name != "app-user" {
		t.Fatalf("restart user rows = %#v", got)
	}
	if backend.adminCount() != 0 {
		t.Fatalf("admin replay calls = %d", backend.adminCount())
	}
	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Instances[key]; got == nil || got.State != "RUNNABLE" ||
		len(got.IpAddresses) != 1 || got.IpAddresses[0].IpAddress != "127.0.0.1:55432" {
		t.Fatalf("persisted reconciled instance = %#v", got)
	}
}

func TestCloudSQLRestartFailureRemainsRetryableAndGetRecovers(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedCloudSQLRestartState(t, store, key, "restart", "RUNNABLE")
	backend := &reconcileCloudSQLBackend{err: errors.New("owned backend readiness timeout")}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 29, 0, 0, 0, 0, time.UTC)
	api.reconcileNow = func() time.Time { return now }
	if err := api.reconcileRestored(context.Background()); err != nil {
		t.Fatal(err)
	}
	instance := api.instances[key]
	if instance.State != "SUSPENDED" || len(instance.IpAddresses) != 0 ||
		!strings.Contains(instance.BackendStatus, "readiness timeout") {
		t.Fatalf("failed reconciliation instance = %#v", instance)
	}
	backend.setResult("http://127.0.0.1:55432", nil)
	now = now.Add(cloudSQLReconcileCooldown)
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(
		http.MethodGet,
		"/v1/projects/project/instances/sql",
		nil,
	))
	if recorder.Code != http.StatusOK {
		t.Fatalf("retry GET status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	if backend.callCount() != 2 {
		t.Fatalf("reconcile calls = %d, want startup plus GET retry", backend.callCount())
	}
	if got := api.instances[key]; got.State != "RUNNABLE" ||
		len(got.IpAddresses) != 1 || got.IpAddresses[0].IpAddress != "127.0.0.1:55432" {
		t.Fatalf("retried reconciliation = %#v", got)
	}
}

func TestCloudSQLFailedReconciliationIsSingleflightDuringCooldown(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedCloudSQLRestartState(t, store, key, "restart", "RUNNABLE")
	backend := &reconcileCloudSQLBackend{err: errors.New("owned backend temporarily unavailable")}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}

	const callers = 8
	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			results <- api.reconcileRestored(context.Background())
		}()
	}
	close(start)
	for range callers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if backend.callCount() != 1 {
		t.Fatalf("failed reconcile calls=%d, want one shared attempt during cooldown", backend.callCount())
	}
}

func TestCloudSQLRelevantAdminDoesNotWaitForUnrelatedReconciliation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	pendingKey := instanceKey("project", "pending")
	runnableKey := instanceKey("project", "ready")
	pendingRuntime := testCloudSQLRuntime("restart", "project", "pending")
	runnableRuntime := testCloudSQLRuntime("restart", "project", "ready")
	if err := store.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			pendingKey: {
				Name: "pending", Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
			},
			runnableKey: {
				Name: "ready", Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
			},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{
			pendingKey: pendingRuntime, runnableKey: runnableRuntime,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeCloudSQLLocalProvenance(store, pendingKey, pendingRuntime.OwnershipFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := writeCloudSQLLocalProvenance(store, runnableKey, runnableRuntime.OwnershipFingerprint); err != nil {
		t.Fatal(err)
	}
	backend := &reconcileCloudSQLBackend{
		err:        errors.New("pending backend unavailable"),
		release:    make(chan struct{}),
		allowAdmin: true,
	}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	delete(api.reconcile, runnableKey)
	api.instances[runnableKey].State = "RUNNABLE"
	api.instances[runnableKey].BackendStatus = ""
	api.mu.Unlock()

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, httptest.NewRequest(
			http.MethodPost,
			"/v1/projects/project/instances/ready/databases",
			strings.NewReader(`{"name":"app"}`),
		))
		done <- recorder
	}()
	select {
	case recorder := <-done:
		if recorder.Code != http.StatusOK {
			close(backend.release)
			t.Fatalf("admin status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	case <-time.After(50 * time.Millisecond):
		close(backend.release)
		<-done
		t.Fatal("admin request waited for an unrelated pending reconciliation")
	}
	close(backend.release)
}

func TestCloudSQLRequestReconciliationUsesOneOverallBudget(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	instances := make(map[string]*DatabaseInstance)
	runtimes := make(map[string]*cloudSQLRuntimeProvenance)
	for _, name := range []string{"first", "second"} {
		key := instanceKey("project", name)
		runtime := testCloudSQLRuntime("restart", "project", name)
		instances[key] = &DatabaseInstance{
			Name: name, Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
		}
		runtimes[key] = runtime
		if err := writeCloudSQLLocalProvenance(store, key, runtime.OwnershipFingerprint); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: instances,
		Runtimes:  runtimes,
	}); err != nil {
		t.Fatal(err)
	}
	backend := &reconcileCloudSQLBackend{release: make(chan struct{})}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(
		http.MethodGet,
		"/v1/projects/project/instances",
		nil,
	))
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("request reconciliation exceeded one overall budget: %s", elapsed)
	}
	if backend.callCount() != 1 {
		t.Fatalf("backend calls=%d, want one attempt before overall budget elapsed", backend.callCount())
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestCloudSQLBudgetExhaustionKeepsUnattemptedInstanceRetryableAcrossRestart(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	instances := make(map[string]*DatabaseInstance)
	runtimes := make(map[string]*cloudSQLRuntimeProvenance)
	for _, name := range []string{"first", "second"} {
		key := instanceKey("project", name)
		runtime := testCloudSQLRuntime("restart", "project", name)
		instances[key] = &DatabaseInstance{
			Name: name, Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
		}
		runtimes[key] = runtime
		if err := writeCloudSQLLocalProvenance(store, key, runtime.OwnershipFingerprint); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: instances,
		Runtimes:  runtimes,
	}); err != nil {
		t.Fatal(err)
	}

	firstBackend := &budgetRestartCloudSQLBackend{blockFirst: true}
	firstAPI, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), firstBackend, store)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := firstAPI.reconcileRestored(ctx); err != nil {
		t.Fatal(err)
	}
	if got := firstBackend.callLog(); len(got) != 1 || got[0] != "first" {
		t.Fatalf("first restart calls=%v, want only budget-consuming first instance", got)
	}
	var afterFirst cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &afterFirst); err != nil {
		t.Fatal(err)
	}
	secondKey := instanceKey("project", "second")
	if second := afterFirst.Instances[secondKey]; second == nil ||
		second.State == "SUSPENDED" && second.BackendStatus == metadataOnlyBackendState {
		t.Fatalf("unattempted second instance lost durable retry disposition: %#v", second)
	}

	secondBackend := &budgetRestartCloudSQLBackend{}
	secondAPI, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), secondBackend, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondAPI.reconcileRestored(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := secondBackend.callLog(); !slices.Contains(got, "second") {
		t.Fatalf("second restart calls=%v, unattempted valid backend was not retried", got)
	}
	if second := secondAPI.instances[secondKey]; second == nil ||
		second.State != "RUNNABLE" ||
		len(second.IpAddresses) != 1 {
		t.Fatalf("second instance did not recover on second restart: %#v", second)
	}
}

func TestCloudSQLRestartReconciliationHonorsCancellation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedCloudSQLRestartState(t, store, key, "restart", "RUNNABLE")
	backend := &reconcileCloudSQLBackend{release: make(chan struct{})}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := api.reconcileRestored(ctx); err != nil {
		t.Fatal(err)
	}
	instance := api.instances[key]
	if instance.State != "SUSPENDED" || len(instance.IpAddresses) != 0 ||
		!strings.Contains(instance.BackendStatus, context.DeadlineExceeded.Error()) {
		t.Fatalf("cancelled reconciliation instance = %#v", instance)
	}
}

func TestCloudSQLRestartRecoversDurablePendingCreateIntent(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedCloudSQLRestartState(t, store, key, "restart", "PENDING_CREATE")
	backend := &reconcileCloudSQLBackend{endpoint: "http://127.0.0.1:55432"}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.reconcileRestored(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.callCount() != 1 {
		t.Fatalf("pending-create reconcile calls = %d, want 1", backend.callCount())
	}
	if got := api.instances[key]; got.State != "RUNNABLE" || len(got.IpAddresses) != 1 {
		t.Fatalf("recovered pending create = %#v", got)
	}
}

func TestCloudSQLRestartSkipsImportedAndDeletingMetadata(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "shared")
	source, err := state.New(t.TempDir(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	runnableKey := instanceKey("project", "sql")
	deletingKey := instanceKey("project", "deleting")
	metadata := cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			runnableKey: {Name: "sql", Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE"},
			deletingKey: {Name: "deleting", Project: "project", DatabaseVersion: "POSTGRES_18", State: "PENDING_DELETE"},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{
			runnableKey: testCloudSQLRuntime("shared", "project", "sql"),
			deletingKey: testCloudSQLRuntime("shared", "project", "deleting"),
		},
	}
	if err := source.Save(cloudSQLStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := source.Export(&snapshot); err != nil {
		t.Fatal(err)
	}

	imported, err := state.New(t.TempDir(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	if err := imported.Import(bytes.NewReader(snapshot.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(imported.ProfileDir(), cloudSQLLocalRuntimeDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successful import created runtime provenance marker: %v", err)
	}
	backend := &reconcileCloudSQLBackend{endpoint: "http://127.0.0.1:55432"}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), backend, imported)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.reconcileRestored(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.callCount() != 0 {
		t.Fatalf("imported/deleting metadata triggered %d backend calls", backend.callCount())
	}
	if got := api.instances[runnableKey]; got.State != "SUSPENDED" ||
		got.BackendStatus != metadataOnlyBackendState || len(got.IpAddresses) != 0 {
		t.Fatalf("imported runnable metadata = %#v", got)
	}
	if got := api.instances[deletingKey]; got.State != "SUSPENDED" ||
		got.BackendStatus != metadataOnlyBackendState || len(got.IpAddresses) != 0 {
		t.Fatalf("interrupted delete metadata = %#v", got)
	}
	var preserved cloudSQLMetadata
	if err := imported.Load(cloudSQLStateEntry, &preserved); err != nil {
		t.Fatal(err)
	}
	if preserved.Instances[runnableKey].State != "RUNNABLE" ||
		preserved.Instances[deletingKey].State != "PENDING_DELETE" ||
		preserved.Runtimes[runnableKey] == nil ||
		preserved.Runtimes[deletingKey] == nil {
		t.Fatalf("imported metadata was destructively rewritten: %#v", preserved)
	}
}

func TestCloudSQLImportRejectsUnsupportedRuntimeSemantics(t *testing.T) {
	key := instanceKey("project", "sql")
	baseInstance := &DatabaseInstance{
		Name: "sql", Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE",
	}
	tests := []struct {
		name   string
		mutate func(*DatabaseInstance, *cloudSQLRuntimeProvenance)
	}{
		{
			name: "unsupported database version",
			mutate: func(instance *DatabaseInstance, runtime *cloudSQLRuntimeProvenance) {
				instance.DatabaseVersion = "POSTGRES_15"
				runtime.DatabaseVersion = "POSTGRES_15"
			},
		},
		{
			name: "image mismatch",
			mutate: func(_ *DatabaseInstance, runtime *cloudSQLRuntimeProvenance) {
				runtime.Image = "postgres:17.9-alpine"
			},
		},
		{
			name: "bootstrap mismatch",
			mutate: func(_ *DatabaseInstance, runtime *cloudSQLRuntimeProvenance) {
				runtime.BootstrapPolicy = "foreign"
			},
		},
		{
			name: "resource mismatch",
			mutate: func(_ *DatabaseInstance, runtime *cloudSQLRuntimeProvenance) {
				runtime.Instance = "other"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instance := cloneDatabaseInstance(baseInstance)
			runtime := testCloudSQLRuntime("shared", "project", "sql")
			test.mutate(instance, runtime)
			err := validateCloudSQLMetadataImport(state.EntryValidationContext{}, &cloudSQLMetadata{
				Instances: map[string]*DatabaseInstance{key: instance},
				Runtimes:  map[string]*cloudSQLRuntimeProvenance{key: runtime},
			})
			if err == nil {
				t.Fatal("unsupported portable runtime accepted")
			}
		})
	}
}

func TestCloudSQLLocalProvenanceRequiresCapableStore(t *testing.T) {
	runtime := testCloudSQLRuntime("restart", "project", "sql")
	store := &nonProfileCloudSQLStore{}
	local, err := cloudSQLHasLocalProvenance(store, instanceKey("project", "sql"), runtime)
	if err == nil {
		t.Fatal("store without ProfileDir did not report unavailable local provenance")
	}
	if local {
		t.Fatal("store without ProfileDir proved local provenance")
	}
	if err := writeCloudSQLLocalProvenance(
		store,
		instanceKey("project", "sql"),
		runtime.OwnershipFingerprint,
	); err == nil {
		t.Fatal("store without ProfileDir accepted local provenance write")
	}
}

func TestCloudSQLLocalProvenanceRejectsEmptyProfileDirWithoutCWDMarker(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	sandbox := t.TempDir()
	if err := os.Chdir(sandbox); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(workingDirectory)
	store := &emptyProfileCloudSQLStore{}
	key := instanceKey("project", "sql")
	fingerprint := testCloudSQLRuntime("restart", "project", "sql").OwnershipFingerprint
	if err := writeCloudSQLLocalProvenance(store, key, fingerprint); err == nil {
		t.Fatal("empty ProfileDir accepted local provenance write")
	}
	if _, err := os.Lstat(filepath.Join(sandbox, cloudSQLLocalRuntimeDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty ProfileDir created a CWD marker: %v", err)
	}
}

func TestCloudSQLGenerationMarkerCleanupCannotRemoveReplacement(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	oldFingerprint := testCloudSQLRuntime("restart", "project", "sql").OwnershipFingerprint
	newFingerprint := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	for _, operation := range []string{"normal delete", "create rollback"} {
		t.Run(operation, func(t *testing.T) {
			if err := writeCloudSQLLocalProvenance(store, key, oldFingerprint); err != nil {
				t.Fatal(err)
			}
			if err := writeCloudSQLLocalProvenance(store, key, newFingerprint); err != nil {
				t.Fatal(err)
			}
			if err := removeCloudSQLLocalProvenance(store, key, oldFingerprint); err != nil {
				t.Fatal(err)
			}
			local, err := cloudSQLHasLocalProvenance(store, key, &cloudSQLRuntimeProvenance{
				OwnershipFingerprint: newFingerprint,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !local {
				t.Fatal("older operation removed replacement generation marker")
			}
		})
	}
}

func TestCloudSQLImportOwnershipFailureDoesNotCreateRuntimeProvenance(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "shared")
	source, err := state.New(t.TempDir(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedCloudSQLRestartState(t, source, key, "shared", "RUNNABLE")
	var snapshot bytes.Buffer
	if err := source.Export(&snapshot); err != nil {
		t.Fatal(err)
	}

	target, err := state.New(t.TempDir(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	activeKey := instanceKey("project", "active")
	activeRuntime := testCloudSQLRuntime("shared", "project", "active")
	if err := target.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			activeKey: {Name: "active", Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE"},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{activeKey: activeRuntime},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeCloudSQLLocalProvenance(target, activeKey, activeRuntime.OwnershipFingerprint); err != nil {
		t.Fatal(err)
	}
	activePath := cloudSQLLocalProvenancePath(target.ProfileDir(), activeKey, activeRuntime.OwnershipFingerprint)
	before, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := target.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()
	if err := target.Import(bytes.NewReader(snapshot.Bytes())); !errors.Is(err, state.ErrProfileInUse) {
		t.Fatalf("Import error = %v, want ErrProfileInUse", err)
	}
	after, err := os.ReadFile(activePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("failed import changed active runtime provenance: before=%q after=%q", before, after)
	}
	if _, err := os.Lstat(cloudSQLLocalProvenancePath(
		target.ProfileDir(),
		key,
		testCloudSQLRuntime("shared", "project", "sql").OwnershipFingerprint,
	)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed import created imported runtime provenance: %v", err)
	}
}

func TestCloudSQLImportValidationFailureDoesNotCreateRuntimeProvenance(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "shared")
	key := instanceKey("project", "sql")
	runtime := testCloudSQLRuntime("shared", "project", "sql")
	metadataPayload, err := json.Marshal(cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			key: {Name: "sql", Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE"},
		},
		Users: map[string][]*User{
			key: {{Name: "app", Project: "project", Instance: "sql", Password: "must-not-travel"}},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{key: runtime},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(state.Snapshot{
		Format:  state.SnapshotFormat,
		Version: state.Version,
		Entries: map[string]json.RawMessage{
			cloudSQLStateEntry: metadataPayload,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := state.New(t.TempDir(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Save("sentinel/metadata", map[string]string{"value": "preserved"}); err != nil {
		t.Fatal(err)
	}
	if err := target.Import(bytes.NewReader(snapshot)); err == nil {
		t.Fatal("import accepted portable Cloud SQL password")
	}
	var sentinel map[string]string
	if err := target.Load("sentinel/metadata", &sentinel); err != nil ||
		sentinel["value"] != "preserved" {
		t.Fatalf("failed import changed active profile: sentinel=%v err=%v", sentinel, err)
	}
	var imported cloudSQLMetadata
	if err := target.Load(cloudSQLStateEntry, &imported); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("failed import installed Cloud SQL metadata: %#v err=%v", imported, err)
	}
	if _, err := os.Lstat(filepath.Join(target.ProfileDir(), cloudSQLLocalRuntimeDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation failure created runtime provenance: %v", err)
	}
}

func TestCloudSQLImportWriteFailureDoesNotCreateRuntimeProvenance(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "shared")
	source, err := state.New(t.TempDir(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedCloudSQLRestartState(t, source, key, "shared", "RUNNABLE")
	var snapshot bytes.Buffer
	if err := source.Export(&snapshot); err != nil {
		t.Fatal(err)
	}
	target, err := state.New(t.TempDir(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Save("sentinel/metadata", map[string]string{"active": "true"}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target.ProfileDir(), 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(target.ProfileDir(), 0o700)
	if err := target.Import(bytes.NewReader(snapshot.Bytes())); err == nil {
		t.Fatal("import unexpectedly succeeded against read-only profile")
	}
	if _, err := os.Lstat(filepath.Join(target.ProfileDir(), cloudSQLLocalRuntimeDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("write failure created runtime provenance: %v", err)
	}
	if err := os.Chmod(target.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	var sentinel map[string]string
	if err := target.Load("sentinel/metadata", &sentinel); err != nil {
		t.Fatal(err)
	}
	if sentinel["active"] != "true" {
		t.Fatalf("failed import replaced active state: %#v", sentinel)
	}
}

func TestCloudSQLReconcileSaveFailureIsStickyAndDoesNotExposeRunnable(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")
	key := instanceKey("project", "sql")
	payload, err := json.Marshal(cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			key: {Name: "sql", Project: "project", DatabaseVersion: "POSTGRES_18", State: "RUNNABLE"},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{
			key: testCloudSQLRuntime("restart", "project", "sql"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &reconcileFailingStore{payload: payload, dir: t.TempDir()}
	runtime := testCloudSQLRuntime("restart", "project", "sql")
	if err := writeCloudSQLLocalProvenance(store, key, runtime.OwnershipFingerprint); err != nil {
		t.Fatal(err)
	}
	operations := orchestrator.NewOperationManager()
	known := operations.Register(
		"sql#operation",
		"CREATE",
		"https://sqladmin.googleapis.com/v1/projects/project/instances/sql",
		"",
		"us-central1",
	)
	backend := &reconcileCloudSQLBackend{endpoint: "http://127.0.0.1:55432"}
	api, err := newAPIWithStoreAndBackend(operations, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.reconcileRestored(context.Background()); err == nil {
		t.Fatal("reconciliation save failure returned nil")
	}
	instance := api.instances[key]
	if instance.State == "RUNNABLE" || len(instance.IpAddresses) != 0 {
		t.Fatalf("save failure exposed reconciled backend = %#v", instance)
	}
	if api.PersistenceError() == nil {
		t.Fatal("save failure did not become sticky")
	}

	mutation := httptest.NewRecorder()
	api.ServeHTTP(mutation, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/project/instances",
		strings.NewReader(`{"name":"blocked"}`),
	))
	if mutation.Code != http.StatusServiceUnavailable {
		t.Fatalf("mutation status = %d, body %s", mutation.Code, mutation.Body.String())
	}
	poll := httptest.NewRecorder()
	api.ServeHTTP(poll, httptest.NewRequest(
		http.MethodGet,
		"/v1/projects/project/operations/"+known.Name,
		nil,
	))
	if poll.Code != http.StatusOK {
		t.Fatalf("known operation poll status = %d, body %s", poll.Code, poll.Body.String())
	}
}

func seedCloudSQLRestartState(
	t *testing.T,
	store cloudSQLStore,
	key string,
	profile string,
	instanceState string,
) {
	t.Helper()
	runtime := testCloudSQLRuntime(profile, "project", "sql")
	if err := store.Save(cloudSQLStateEntry, cloudSQLMetadata{
		Instances: map[string]*DatabaseInstance{
			key: {
				Name: "sql", Project: "project", DatabaseVersion: "POSTGRES_18",
				State: instanceState, IpAddresses: []IpMapping{{Type: "PRIMARY", IpAddress: "127.0.0.1:1"}},
			},
		},
		Databases: map[string][]*Database{
			key: {{Name: "app", Project: "project", Instance: "sql"}},
		},
		Users: map[string][]*User{
			key: {{Name: "app-user", Project: "project", Instance: "sql"}},
		},
		Runtimes: map[string]*cloudSQLRuntimeProvenance{key: runtime},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeCloudSQLLocalProvenance(store, key, runtime.OwnershipFingerprint); err != nil {
		t.Fatal(err)
	}
}

func testCloudSQLRuntime(profile, project, instance string) *cloudSQLRuntimeProvenance {
	return &cloudSQLRuntimeProvenance{
		Profile:              profile,
		Project:              project,
		Instance:             instance,
		DatabaseVersion:      "POSTGRES_18",
		OwnershipFingerprint: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		BootstrapPolicy:      cloudSQLBootstrapPolicyV1,
		Image:                "postgres:18.3-alpine",
		ImageID:              "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		VolumeIdentity:       "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		CreationIntent:       true,
	}
}
