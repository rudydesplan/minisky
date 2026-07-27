package cloudbuild

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestCloudBuildDockerIdentitySeparatesProfileProjectAndBuild(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "profile-a")
	first := cloudBuildDockerIdentity("projects/project-a/builds/build-1")
	second := cloudBuildDockerIdentity("projects/project-b/builds/build-1")
	third := cloudBuildDockerIdentity("projects/project-a/builds/build-2")
	if first == second || first == third || second == third {
		t.Fatalf("Cloud Build Docker identities collided: %q %q %q", first, second, third)
	}
	t.Setenv("MINISKY_PROFILE", "profile-b")
	if otherProfile := cloudBuildDockerIdentity("projects/project-a/builds/build-1"); otherProfile == first {
		t.Fatalf("Cloud Build Docker identity collided across profiles: %q", first)
	}
}

func TestCloudBuildTriggersReturnExplicitUnimplemented(t *testing.T) {
	api := newAPI(nil, orchestrator.NewOperationManager())
	api.runAsync = func(string, func() error) {}

	const requests = 32
	var wg sync.WaitGroup
	for index := 0; index < requests; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for _, path := range []string{
				"/v1/projects/demo/triggers",
				fmt.Sprintf("/v1/projects/demo/triggers/trigger-%d:run", index),
			} {
				response := httptest.NewRecorder()
				api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
				if response.Code != http.StatusNotImplemented ||
					!strings.Contains(response.Body.String(), `"status":"UNIMPLEMENTED"`) {
					t.Errorf("%s status=%d body=%s", path, response.Code, response.Body.String())
				}
			}
		}(index)
	}
	wg.Wait()
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("unsupported triggers created operations: %#v", operations)
	}
}

func TestBuildIDAllocationAtomicallyRetriesCollision(t *testing.T) {
	api := newAPI(nil, orchestrator.NewOperationManager())
	var calls atomic.Int32
	api.randomID = func(target []byte) (int, error) {
		value := byte(1)
		if calls.Add(1) >= 3 {
			value = 2
		}
		for index := range target {
			target[index] = value
		}
		return len(target), nil
	}
	first, err := api.allocateBuildID("build-trigger-")
	if err != nil {
		t.Fatal(err)
	}
	second, err := api.allocateBuildID("build-trigger-")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || calls.Load() != 3 {
		t.Fatalf("first=%q second=%q random calls=%d", first, second, calls.Load())
	}
}

func TestCloudBuildRestartNormalizesActiveBuildWithoutReplay(t *testing.T) {
	store := newCloudBuildMemoryStore()
	store.seed(cloudBuildMetadata{
		Builds: map[string]*Build{
			"projects/demo/builds/active": {
				Id: "active", ProjectId: "demo", Status: "WORKING",
				CreateTime: "2026-07-27T10:00:00Z", StartTime: "2026-07-27T10:00:01Z",
			},
			"projects/demo/builds/complete": {
				Id: "complete", ProjectId: "demo", Status: "SUCCESS",
				CreateTime: "2026-07-27T09:00:00Z", FinishTime: "2026-07-27T09:01:00Z",
			},
		},
		Triggers: map[string]*BuildTrigger{
			"projects/demo/triggers/nightly": {Id: "nightly", Description: "nightly"},
		},
	})
	var reconciles atomic.Int32
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, func() error {
		reconciles.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var replays atomic.Int32
	api.runAsync = func(string, func() error) { replays.Add(1) }

	active := api.buildSnapshot("projects/demo/builds/active")
	if active == nil || active.Status != interruptedBuildStatus ||
		!strings.Contains(active.StatusDetail, "interrupted by MiniSky restart") ||
		active.FinishTime == "" {
		t.Fatalf("active build was not truthfully interrupted: %#v", active)
	}
	complete := api.buildSnapshot("projects/demo/builds/complete")
	if complete == nil || complete.Status != "SUCCESS" ||
		complete.FinishTime != "2026-07-27T09:01:00Z" {
		t.Fatalf("completed outcome changed on restart: %#v", complete)
	}
	if api.triggers["projects/demo/triggers/nightly"] == nil {
		t.Fatal("trigger metadata was not restored")
	}
	if replays.Load() != 0 {
		t.Fatalf("restart replayed %d builds", replays.Load())
	}
	if reconciles.Load() != 1 {
		t.Fatalf("owned-resource reconciliation calls=%d, want 1", reconciles.Load())
	}

	persisted := store.snapshot()
	if persisted.Builds["projects/demo/builds/active"].Status != interruptedBuildStatus {
		t.Fatalf("restart normalization was not durable: %#v", persisted.Builds)
	}
}

func TestCloudBuildPendingLRORestartIsTerminalWithoutReplay(t *testing.T) {
	durable, err := state.New(t.TempDir(), "cloudbuild-restart")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(durable)
	if err != nil {
		t.Fatal(err)
	}
	api, err := newAPIWithStore(nil, manager, durable, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.runAsync = func(string, func() error) {}
	response := createBuildRequest(api, `{"steps":[{"name":"ubuntu","args":["true"]}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var created orchestrator.Operation
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	restartedManager, err := orchestrator.NewOperationManagerWithStore(durable)
	if err != nil {
		t.Fatal(err)
	}
	var reconciles atomic.Int32
	restarted, err := newAPIWithStore(nil, restartedManager, durable, func() error {
		reconciles.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	restarted.runAsync = func(string, func() error) {
		t.Fatal("restart replayed build work")
	}
	operation := restartedManager.Get(created.Name)
	if operation == nil || !operation.Done || operation.Error == nil ||
		!strings.Contains(operation.Error.Message, "interrupted by MiniSky restart") {
		t.Fatalf("restarted operation is not a terminal interruption: %#v", operation)
	}
	polled := httptest.NewRecorder()
	restarted.ServeHTTP(polled, httptest.NewRequest(http.MethodGet,
		"/v1/operations/"+created.Name, nil))
	if polled.Code != http.StatusOK ||
		!strings.Contains(polled.Body.String(), "interrupted by MiniSky restart") {
		t.Fatalf("poll after restart status=%d body=%s", polled.Code, polled.Body.String())
	}
	build := restarted.buildSnapshot(operation.TargetLink[len("/v1/"):])
	if build == nil || build.Status != interruptedBuildStatus {
		t.Fatalf("restarted build is not terminal: %#v", build)
	}
	if reconciles.Load() != 1 {
		t.Fatalf("reconcile calls=%d, want 1", reconciles.Load())
	}
}

func TestCloudBuildLoadFailureRejectsMutation(t *testing.T) {
	injected := errors.New("corrupt cloudbuild metadata")
	store := newCloudBuildMemoryStore()
	store.loadErr = injected
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, nil)
	if !errors.Is(err, injected) {
		t.Fatalf("constructor error=%v, want %v", err, injected)
	}

	response := createBuildRequest(api, `{}`)
	assertCloudBuildUnavailable(t, response)
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("load failure created operations: %#v", operations)
	}
}

func TestCloudBuildSaveFailureIsTransactionalAndSticky(t *testing.T) {
	store := newCloudBuildMemoryStore()
	store.saveErr = errors.New("disk full")
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.runAsync = func(string, func() error) {
		t.Fatal("failed persistence started build work")
	}

	first := createBuildRequest(api, `{}`)
	assertCloudBuildUnavailable(t, first)
	if len(api.builds) != 0 || len(api.opMgr.List()) != 0 {
		t.Fatalf("failed save mutated state: builds=%#v operations=%#v", api.builds, api.opMgr.List())
	}

	store.saveErr = nil
	second := createBuildRequest(api, `{}`)
	assertCloudBuildUnavailable(t, second)
	if len(api.builds) != 0 || len(api.opMgr.List()) != 0 {
		t.Fatalf("degraded API accepted later mutation: builds=%#v operations=%#v", api.builds, api.opMgr.List())
	}
}

func TestCloudBuildRestartNormalizationSaveFailureRejectsMutation(t *testing.T) {
	store := newCloudBuildMemoryStore()
	store.seed(cloudBuildMetadata{Builds: map[string]*Build{
		"projects/demo/builds/active": {
			Id: "active", ProjectId: "demo", Status: "QUEUED",
			CreateTime: "2026-07-27T10:00:00Z",
		},
	}})
	injected := errors.New("ambiguous restart save")
	store.saveErr = injected
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, nil)
	if !errors.Is(err, injected) {
		t.Fatalf("constructor error=%v, want %v", err, injected)
	}
	if build := api.buildSnapshot("projects/demo/builds/active"); build == nil ||
		build.Status != interruptedBuildStatus {
		t.Fatalf("in-process restart state is not truthful: %#v", build)
	}
	store.saveErr = nil
	assertCloudBuildUnavailable(t, createBuildRequest(api, `{}`))
}

func TestCloudBuildOperationFailureRollsBackDurableMetadata(t *testing.T) {
	manager, err := orchestrator.NewOperationManagerWithStore(cloudBuildFailingOperationStore{})
	if err != nil {
		t.Fatal(err)
	}
	store := newCloudBuildMemoryStore()
	api, err := newAPIWithStore(nil, manager, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.runAsync = func(string, func() error) {
		t.Fatal("failed operation registration started build work")
	}

	response := createBuildRequest(api, `{}`)
	assertCloudBuildUnavailable(t, response)
	if len(api.builds) != 0 || len(store.snapshot().Builds) != 0 {
		t.Fatalf("operation failure left build metadata: memory=%#v durable=%#v",
			api.builds, store.snapshot().Builds)
	}
	if operations := manager.List(); len(operations) != 0 {
		t.Fatalf("operation failure left operations: %#v", operations)
	}
}

func TestCloudBuildUnavailableBackendNeverReportsSuccess(t *testing.T) {
	store := newCloudBuildMemoryStore()
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	build := Build{
		Id: "build-1", ProjectId: "demo", Status: "QUEUED",
		CreateTime: "2026-07-27T10:00:00Z",
	}
	resource := "projects/demo/builds/build-1"
	if err := api.commitBuild(resource, &build); err != nil {
		t.Fatal(err)
	}
	operation, err := api.opMgr.RegisterDurable(
		"cloudbuild#operation", "CREATE", "/v1/"+resource, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.executeBuild("demo", build, operation.Name); err == nil {
		t.Fatal("execution without a backend returned success")
	}
	stored := api.buildSnapshot(resource)
	if stored == nil || stored.Status != "FAILURE" ||
		!strings.Contains(stored.StatusDetail, "backend is unavailable") {
		t.Fatalf("unavailable backend build=%#v", stored)
	}
}

func TestCloudBuildPersistsSuccessOnlyAfterZeroExit(t *testing.T) {
	store := newCloudBuildMemoryStore()
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	backend := &recordingCloudBuildBackend{
		results: []orchestrator.BuildContainerResult{{ExitCode: 0, Logs: "completed\n"}},
	}
	api.svcMgr = backend
	build := Build{
		Id: "build-zero", ProjectId: "demo", Status: "QUEUED",
		CreateTime: "2026-07-27T10:00:00Z",
		Steps:      []Step{{Name: "example.invalid/tool@sha256:digest", Args: []string{"run"}}},
	}
	resource := "projects/demo/builds/build-zero"
	if err := api.commitBuild(resource, &build); err != nil {
		t.Fatal(err)
	}
	operation, err := api.opMgr.RegisterDurable(
		"cloudbuild#operation", "CREATE", "/v1/"+resource, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := api.executeBuild("demo", build, operation.Name); err != nil {
		t.Fatal(err)
	}
	stored := api.buildSnapshot(resource)
	if stored == nil || stored.Status != "SUCCESS" || stored.FinishTime == "" {
		t.Fatalf("zero-exit build = %#v", stored)
	}
	if backend.waits.Load() != 1 || backend.removes.Load() == 0 {
		t.Fatalf("backend waits=%d removes=%d", backend.waits.Load(), backend.removes.Load())
	}
	if durable := store.snapshot().Builds[resource]; durable == nil || durable.Status != "SUCCESS" {
		t.Fatalf("durable zero-exit build = %#v", durable)
	}
}

func TestCloudBuildPersistsNonzeroExitAndBoundedLogsAsFailure(t *testing.T) {
	store := newCloudBuildMemoryStore()
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	backend := &recordingCloudBuildBackend{
		results: []orchestrator.BuildContainerResult{{ExitCode: 17, Logs: "compiler failed\n"}},
	}
	api.svcMgr = backend
	build := Build{
		Id: "build-failure", ProjectId: "demo", Status: "QUEUED",
		CreateTime: "2026-07-27T10:00:00Z",
		Steps:      []Step{{Name: "example.invalid/tool@sha256:digest"}},
	}
	resource := "projects/demo/builds/build-failure"
	if err := api.commitBuild(resource, &build); err != nil {
		t.Fatal(err)
	}
	operation, err := api.opMgr.RegisterDurable(
		"cloudbuild#operation", "CREATE", "/v1/"+resource, "", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := api.executeBuild("demo", build, operation.Name); err == nil {
		t.Fatal("nonzero exit reported success")
	}
	stored := api.buildSnapshot(resource)
	if stored == nil || stored.Status != "FAILURE" || stored.FinishTime == "" ||
		!strings.Contains(stored.StatusDetail, "exit code 17") ||
		!strings.Contains(stored.StatusDetail, "compiler failed") {
		t.Fatalf("nonzero-exit build = %#v", stored)
	}
	if durable := store.snapshot().Builds[resource]; durable == nil || durable.Status != "FAILURE" {
		t.Fatalf("durable nonzero-exit build = %#v", durable)
	}
}

func TestCloudBuildConcurrentCreatesPersistCompleteSnapshot(t *testing.T) {
	store := newCloudBuildMemoryStore()
	api, err := newAPIWithStore(nil, orchestrator.NewOperationManager(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	api.runAsync = func(string, func() error) {}

	const builds = 32
	var wg sync.WaitGroup
	for range builds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response := createBuildRequest(api, `{}`)
			if response.Code != http.StatusOK {
				t.Errorf("create status=%d body=%s", response.Code, response.Body.String())
			}
		}()
	}
	wg.Wait()

	persisted := store.snapshot()
	if len(api.builds) != builds || len(persisted.Builds) != builds {
		t.Fatalf("build counts memory=%d durable=%d, want %d", len(api.builds), len(persisted.Builds), builds)
	}
}

func createBuildRequest(api *API, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/demo/builds", bytes.NewBufferString(body)))
	return response
}

func assertCloudBuildUnavailable(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

type cloudBuildMemoryStore struct {
	mu      sync.Mutex
	data    []byte
	loadErr error
	saveErr error
}

type cloudBuildFailingOperationStore struct{}

func (cloudBuildFailingOperationStore) Load(string, any) error {
	return state.ErrNotFound
}

func (cloudBuildFailingOperationStore) Save(string, any) error {
	return errors.New("injected operation save failure")
}

type recordingCloudBuildBackend struct {
	results []orchestrator.BuildContainerResult
	waits   atomic.Int32
	removes atomic.Int32
}

func (*recordingCloudBuildBackend) ReconcileBuildResources(context.Context) error {
	return nil
}

func (*recordingCloudBuildBackend) EnsureBuildWorkspace(context.Context, string, string) error {
	return nil
}

func (*recordingCloudBuildBackend) RemoveBuildWorkspace(context.Context, string, string) error {
	return nil
}

func (*recordingCloudBuildBackend) ProvisionBuildStep(
	context.Context, string, string, string, []string, []string, []string,
) error {
	return nil
}

func (backend *recordingCloudBuildBackend) WaitBuildContainer(
	context.Context, string, string,
) (orchestrator.BuildContainerResult, error) {
	index := int(backend.waits.Add(1)) - 1
	if index >= len(backend.results) {
		return orchestrator.BuildContainerResult{}, errors.New("missing build result")
	}
	return backend.results[index], nil
}

func (backend *recordingCloudBuildBackend) StopAndRemoveBuildContainer(
	context.Context, string, string,
) error {
	backend.removes.Add(1)
	return nil
}

func newCloudBuildMemoryStore() *cloudBuildMemoryStore {
	return &cloudBuildMemoryStore{}
}

func (s *cloudBuildMemoryStore) seed(metadata cloudBuildMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, _ = json.Marshal(metadata)
}

func (s *cloudBuildMemoryStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *cloudBuildMemoryStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data = data
	return nil
}

func (s *cloudBuildMemoryStore) snapshot() cloudBuildMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()
	var metadata cloudBuildMetadata
	_ = json.Unmarshal(s.data, &metadata)
	return metadata
}
