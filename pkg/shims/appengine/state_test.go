package appengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestAppEngineStateOpenFailureIsUnavailableAndHealthyDegraded(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "../invalid")
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	api := NewAPI(orchestrator.NewOperationManager(), nil, nil, nil)
	if api.PersistenceError() == nil {
		t.Fatal("PersistenceError is nil")
	}
	response := appEngineRequest(api, http.MethodGet, "/v1/projects/test/apps", "")
	assertAppEngineError(t, response, http.StatusServiceUnavailable, "UNAVAILABLE")
}

func TestAppEngineMetadataSurvivesRestartWithoutBackendReplay(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &appEngineBackendSpy{}
	api, err := NewAPIWithStore(manager, backend, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string, func() error) {}

	response := appEngineRequest(api, http.MethodPost, "/deploy",
		`{"project":"test","service":"default","version":"v1","runtime":"python312","code":"print(1)"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("deploy status = %d, body = %s", response.Code, response.Body.String())
	}

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restartedBackend := &appEngineBackendSpy{}
	restarted, err := NewAPIWithStore(restartedManager, restartedBackend, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	read := appEngineRequest(restarted, http.MethodGet,
		"/v1/projects/test/apps/test/services/default/versions/v1", "")
	if read.Code != http.StatusOK {
		t.Fatalf("read after restart = %d, body = %s", read.Code, read.Body.String())
	}
	var version Version
	decodeAppEngineResponse(t, read, &version)
	if version.State != "STOPPED" {
		t.Fatalf("restored serving status = %q, want STOPPED", version.State)
	}
	if restartedBackend.provisions != 0 || restartedBackend.deletes != 0 {
		t.Fatalf("restart replayed backend calls: %#v", restartedBackend)
	}
	var operation *orchestrator.Operation
	for _, candidate := range restartedManager.List() {
		if candidate.Kind == "appengine#operation" {
			operation = candidate
			break
		}
	}
	if operation == nil || !operation.Done || operation.Error == nil {
		t.Fatalf("restarted operation = %#v, want terminal interruption", operation)
	}
}

func TestAppEngineOperationOutcomeSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "operation-outcome")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(manager, nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := manager.RegisterDurable(
		"appengine#operation", "CREATE", "apps/test/services/default/versions/v1", "", "us-central1")
	if err != nil {
		t.Fatal(err)
	}
	api.runOperation(operation.Name, func() error { return nil })

	restarted, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	got := restarted.Get(operation.Name)
	if got == nil || !got.Done || got.Status != orchestrator.StatusDone || got.Error != nil {
		t.Fatalf("restarted operation = %#v, want durable success", got)
	}
}

func TestAppEngineOperationOutcomeSaveFailureDegradesControlPlane(t *testing.T) {
	store := newAppEngineFailingStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(manager, nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := manager.RegisterDurable(
		"appengine#operation", "CREATE", "apps/test/services/default/versions/v1", "", "us-central1")
	if err != nil {
		t.Fatal(err)
	}
	store.failAt = 5 // registration, two RUNNING states, 85%, then terminal DONE
	api.runOperation(operation.Name, func() error { return nil })

	if api.PersistenceError() == nil {
		t.Fatal("terminal operation save failure did not degrade App Engine")
	}
	response := appEngineRequest(api, http.MethodGet, "/v1/projects/test/operations/"+operation.Name, "")
	assertAppEngineError(t, response, http.StatusServiceUnavailable, "UNAVAILABLE")

	restarted, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	got := restarted.Get(operation.Name)
	if got == nil || !got.Done || got.Error == nil {
		t.Fatalf("restarted operation = %#v, want terminal interruption", got)
	}
}

func TestAppEngineSaveFailureReturnsGCPErrorAndRollsBack(t *testing.T) {
	store := newAppEngineFailingStore()
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	store.fail = true
	response := appEngineDeployRequest(api, "test")
	assertAppEngineError(t, response, http.StatusInternalServerError, "INTERNAL")

	api.mu.RLock()
	_, exists := api.apps["test"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("failed save left app in memory")
	}
}

func TestAppEngineMetadataAndOperationCompensationFailureDegradesControlPlane(t *testing.T) {
	store := newAppEngineFailingStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(manager, nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	store.failAtSaves = map[int]bool{
		2: true, // version metadata admission
		3: true, // terminal failure compensation
	}
	response := appEngineDeployRequest(api, "test")
	assertAppEngineError(t, response, http.StatusInternalServerError, "INTERNAL")
	if api.PersistenceError() == nil {
		t.Fatal("ambiguous operation compensation did not degrade App Engine")
	}
	blocked := appEngineRequest(api, http.MethodGet, "/v1/projects/test/apps", "")
	assertAppEngineError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")
}

func TestAppEngineMissingAppGetIsPureNotFound(t *testing.T) {
	store := newAppEngineFailingStore()
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}

	response := appEngineRequest(api, http.MethodGet, "/v1/projects/test/apps", "")
	assertAppEngineError(t, response, http.StatusNotFound, "NOT_FOUND")
	if store.saves != 0 {
		t.Fatalf("missing-resource GET performed %d state saves", store.saves)
	}
	api.mu.RLock()
	_, exists := api.apps["test"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("missing-resource GET created an app")
	}
}

func TestAppEngineOperationPollingIsProjectAndServiceScoped(t *testing.T) {
	manager := orchestrator.NewOperationManager()
	api := newAPI(manager, nil, nil, nil, nil)
	operations := []*orchestrator.Operation{
		manager.Register("appengine#operation", "CREATE",
			"apps/other/services/default/versions/v1", "", "us-central1"),
		manager.Register("compute#operation", "insert",
			"https://www.googleapis.com/compute/v1/projects/test/zones/us/instances/vm", "us", ""),
	}
	for _, operation := range operations {
		response := appEngineRequest(api, http.MethodGet,
			"/v1/projects/test/operations/"+operation.Name, "")
		assertAppEngineError(t, response, http.StatusNotFound, "NOT_FOUND")
	}
}

func TestAppEngineAmbiguousSaveReadbackReconcilesTruth(t *testing.T) {
	t.Run("candidate committed", func(t *testing.T) {
		store := newAppEngineFailingStore()
		api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
		if err != nil {
			t.Fatal(err)
		}
		store.failAfterCommit = true
		response := appEngineDeployRequest(api, "test")
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		api.mu.RLock()
		_, exists := api.apps["test"]
		api.mu.RUnlock()
		if !exists || api.PersistenceError() != nil {
			t.Fatalf("candidate not adopted: exists=%t persistence=%v", exists, api.PersistenceError())
		}
	})

	t.Run("previous preserved", func(t *testing.T) {
		store := newAppEngineFailingStore()
		api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
		if err != nil {
			t.Fatal(err)
		}
		store.fail = true
		response := appEngineDeployRequest(api, "test")
		assertAppEngineError(t, response, http.StatusInternalServerError, "INTERNAL")
		if api.PersistenceError() != nil {
			t.Fatalf("definite previous degraded API: %v", api.PersistenceError())
		}
	})

	t.Run("unknown degrades", func(t *testing.T) {
		store := newAppEngineFailingStore()
		api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
		if err != nil {
			t.Fatal(err)
		}
		store.fail = true
		store.loadErr = errors.New("readback unavailable")
		response := appEngineDeployRequest(api, "test")
		assertAppEngineError(t, response, http.StatusInternalServerError, "INTERNAL")
		if api.PersistenceError() == nil {
			t.Fatal("unknown save outcome did not degrade API")
		}
		blocked := appEngineRequest(api, http.MethodGet, "/v1/projects/other/apps", "")
		assertAppEngineError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")
	})
}

func TestAppEngineAdmittedMutationRechecksDegradationAfterLock(t *testing.T) {
	base := newAppEngineFailingStore()
	store := &appEngineInterleavingStore{
		appEngineFailingStore: base,
		saveEntered:           make(chan struct{}),
		releaseSave:           make(chan struct{}),
	}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	admitted := make(chan struct{}, 2)
	api.afterAdmission = func() { admitted <- struct{}{} }

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- appEngineDeployRequest(api, "first")
	}()
	<-admitted
	<-store.saveEntered

	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		secondDone <- appEngineDeployRequest(api, "second")
	}()
	<-admitted
	close(store.releaseSave)

	assertAppEngineError(t, <-firstDone, http.StatusInternalServerError, "INTERNAL")
	assertAppEngineError(t, <-secondDone, http.StatusServiceUnavailable, "UNAVAILABLE")
	if base.saves != 1 {
		t.Fatalf("saves = %d, want only first admitted mutation", base.saves)
	}
}

func TestAppEngineServingSaveFailureIsSticky(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*appEngineFailingStore)
	}{
		{name: "pre-commit", configure: func(store *appEngineFailingStore) { store.fail = true }},
		{name: "post-commit", configure: func(store *appEngineFailingStore) { store.failAfterCommit = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newAppEngineFailingStore()
			store.entries[appEngineStateEntry] = mustAppEngineJSON(t, appEngineMetadata{
				Versions: map[string]map[string]map[string]*Version{
					"test": {"default": {"v1": {Id: "v1", State: "STOPPED"}}},
				},
			})
			api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			test.configure(store)
			if err := api.setVersionState("test", "default", "v1", "SERVING"); err == nil {
				t.Fatal("SERVING save failure was ignored")
			}
			if api.PersistenceError() == nil {
				t.Fatal("SERVING save failure did not degrade API")
			}
		})
	}
}

func TestAppEngineServingSaveFailureCompensatesOwnedBackend(t *testing.T) {
	store := newAppEngineFailingStore()
	store.entries[appEngineStateEntry] = mustAppEngineJSON(t, appEngineMetadata{
		Versions: map[string]map[string]map[string]*Version{
			"test": {"default": {"v1": {Id: "v1", State: "STOPPED"}}},
		},
	})
	backend := &appEngineBackendSpy{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	store.fail = true
	if err := api.activateVersion("test", "default", "v1"); err == nil {
		t.Fatal("SERVING save failure was ignored")
	}
	want := appEngineIdentity("test", "default", "v1")
	if backend.deletes != 1 || backend.lastDeleted != want {
		t.Fatalf("backend compensation = deletes:%d identity:%#v, want 1 %#v",
			backend.deletes, backend.lastDeleted, want)
	}
}

func TestAppEngineCorruptStateFailsClosed(t *testing.T) {
	store := newAppEngineFailingStore()
	store.entries[appEngineStateEntry] = json.RawMessage(`{"apps":{"test":null}}`)
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store); err == nil {
		t.Fatal("corrupt metadata loaded without error")
	}
}

func TestAppEngineDeleteBackendFailureRetainsVersion(t *testing.T) {
	store := newAppEngineFailingStore()
	store.entries[appEngineStateEntry] = mustAppEngineJSON(t, appEngineMetadata{
		Apps: map[string]*App{"test": {Id: "test"}},
		Services: map[string]map[string]*Service{
			"test": {"default": {Id: "default"}},
		},
		Versions: map[string]map[string]map[string]*Version{
			"test": {"default": {"v1": {Id: "v1", State: "STOPPED"}}},
		},
	})
	backend := &appEngineBackendSpy{deleteErr: errors.New("docker unavailable")}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	response := appEngineRequest(api, http.MethodDelete,
		"/v1/projects/test/apps/test/services/default/versions/v1", "")
	assertAppEngineError(t, response, http.StatusBadGateway, "BAD_GATEWAY")
	read := appEngineRequest(api, http.MethodGet,
		"/v1/projects/test/apps/test/services/default/versions/v1", "")
	if read.Code != http.StatusOK {
		t.Fatalf("version removed after backend failure: %d %s", read.Code, read.Body.String())
	}
	var persisted appEngineMetadata
	if err := store.Load(appEngineStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	key := appEngineVersionKey("test", "default", "v1")
	if _, pending := persisted.Deletions[key]; !pending {
		t.Fatalf("delete intent was not durable: %#v", persisted.Deletions)
	}

	backend.deleteErr = nil
	retry := appEngineRequest(api, http.MethodDelete,
		"/v1/projects/test/apps/test/services/default/versions/v1", "")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry status = %d, body = %s", retry.Code, retry.Body.String())
	}
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if read := appEngineRequest(restarted, http.MethodGet,
		"/v1/projects/test/apps/test/services/default/versions/v1", ""); read.Code != http.StatusNotFound {
		t.Fatalf("deleted version after restart = %d, body = %s", read.Code, read.Body.String())
	}
}

func TestAppEngineDeleteIntentSaveFailureDoesNotTouchBackend(t *testing.T) {
	store := newAppEngineFailingStore()
	store.entries[appEngineStateEntry] = mustAppEngineJSON(t, appEngineMetadata{
		Apps:     map[string]*App{"test": {Id: "test"}},
		Services: map[string]map[string]*Service{"test": {"default": {Id: "default"}}},
		Versions: map[string]map[string]map[string]*Version{
			"test": {"default": {"v1": {Id: "v1", State: "STOPPED"}}},
		},
	})
	backend := &appEngineBackendSpy{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	store.fail = true
	response := appEngineRequest(api, http.MethodDelete,
		"/v1/projects/test/apps/test/services/default/versions/v1", "")
	assertAppEngineError(t, response, http.StatusInternalServerError, "INTERNAL")
	if backend.deletes != 0 {
		t.Fatalf("backend deletes = %d, want 0", backend.deletes)
	}
}

func TestAppEngineDeleteFinalizeFailureRemainsRetryable(t *testing.T) {
	store := newAppEngineFailingStore()
	store.entries[appEngineStateEntry] = mustAppEngineJSON(t, appEngineMetadata{
		Apps:     map[string]*App{"test": {Id: "test"}},
		Services: map[string]map[string]*Service{"test": {"default": {Id: "default"}}},
		Versions: map[string]map[string]map[string]*Version{
			"test": {"default": {"v1": {Id: "v1", State: "STOPPED"}}},
		},
	})
	backend := &appEngineBackendSpy{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	store.failAt = 2
	response := appEngineRequest(api, http.MethodDelete,
		"/v1/projects/test/apps/test/services/default/versions/v1", "")
	assertAppEngineError(t, response, http.StatusInternalServerError, "INTERNAL")
	if backend.deletes != 1 {
		t.Fatalf("backend deletes = %d, want 1", backend.deletes)
	}
	var persisted appEngineMetadata
	if err := store.Load(appEngineStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if _, pending := persisted.Deletions[appEngineVersionKey("test", "default", "v1")]; !pending {
		t.Fatal("finalize failure lost deletion intent")
	}

	store.failAt = 0
	retry := appEngineRequest(api, http.MethodDelete,
		"/v1/projects/test/apps/test/services/default/versions/v1", "")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry = %d, body = %s", retry.Code, retry.Body.String())
	}
}

func TestAppEngineRestartNormalizationIsDurablySaved(t *testing.T) {
	store := newAppEngineFailingStore()
	store.entries[appEngineStateEntry] = mustAppEngineJSON(t, appEngineMetadata{
		Versions: map[string]map[string]map[string]*Version{
			"test": {"default": {"v1": {Id: "v1", State: "SERVING"}}},
		},
	})
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store); err != nil {
		t.Fatal(err)
	}
	if store.saves != 1 {
		t.Fatalf("normalization saves = %d, want 1", store.saves)
	}
	var persisted appEngineMetadata
	if err := store.Load(appEngineStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Versions["test"]["default"]["v1"].State; got != "STOPPED" {
		t.Fatalf("persisted normalized state = %q", got)
	}
}

func TestAppEngineNormalizationSaveFailureFailsConstruction(t *testing.T) {
	store := newAppEngineFailingStore()
	store.entries[appEngineStateEntry] = mustAppEngineJSON(t, appEngineMetadata{
		Versions: map[string]map[string]map[string]*Version{
			"test": {"default": {"v1": {Id: "v1", State: "SERVING"}}},
		},
	})
	store.fail = true
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store); err == nil {
		t.Fatal("normalization save failure was ignored")
	}
}

func TestAppEngineConcurrentAppCreatesPersist(t *testing.T) {
	store, err := state.New(t.TempDir(), "concurrent")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	var requests sync.WaitGroup
	for i := 0; i < 12; i++ {
		requests.Add(1)
		go func(i int) {
			defer requests.Done()
			response := appEngineDeployRequest(api, "project-"+string(rune('a'+i)))
			if response.Code != http.StatusOK {
				t.Errorf("create status = %d, body = %s", response.Code, response.Body.String())
			}
		}(i)
	}
	requests.Wait()
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.mu.RLock()
	count := len(restarted.apps)
	restarted.mu.RUnlock()
	if count != 12 {
		t.Fatalf("apps after restart = %d, want 12", count)
	}
}

func TestAppEngineMetadataIsProfileScoped(t *testing.T) {
	root := t.TempDir()
	firstStore, err := state.New(root, "first")
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, firstStore)
	if err != nil {
		t.Fatal(err)
	}
	if response := appEngineDeployRequest(first, "first"); response.Code != http.StatusOK {
		t.Fatalf("first profile create = %d, body = %s", response.Code, response.Body.String())
	}

	secondStore, err := state.New(root, "second")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, nil, secondStore)
	if err != nil {
		t.Fatal(err)
	}
	second.mu.RLock()
	_, leaked := second.apps["first"]
	second.mu.RUnlock()
	if leaked {
		t.Fatal("App Engine metadata leaked between profiles")
	}
}

type appEngineBackendSpy struct {
	provisions  int
	deletes     int
	deleteErr   error
	lastDeleted orchestrator.ServerlessIdentity
}

func (backend *appEngineBackendSpy) ProvisionServerlessVM(orchestrator.ServerlessIdentity, string, []string) (string, error) {
	backend.provisions++
	return "", nil
}

func (backend *appEngineBackendSpy) DeleteServerlessVM(identity orchestrator.ServerlessIdentity) error {
	backend.deletes++
	backend.lastDeleted = identity
	return backend.deleteErr
}

type appEngineFailingStore struct {
	mu              sync.Mutex
	entries         map[string]json.RawMessage
	fail            bool
	failAfterCommit bool
	loadErr         error
	failAt          int
	failAtSaves     map[int]bool
	saves           int
}

type appEngineInterleavingStore struct {
	*appEngineFailingStore
	saveEntered chan struct{}
	releaseSave chan struct{}
	once        sync.Once
}

func (store *appEngineInterleavingStore) Save(name string, value any) error {
	store.once.Do(func() {
		close(store.saveEntered)
		<-store.releaseSave
		store.fail = true
		store.loadErr = errors.New("readback unavailable")
	})
	return store.appEngineFailingStore.Save(name, value)
}

func newAppEngineFailingStore() *appEngineFailingStore {
	return &appEngineFailingStore{entries: make(map[string]json.RawMessage)}
}

func (store *appEngineFailingStore) Load(name string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.loadErr != nil {
		return store.loadErr
	}
	payload := store.entries[name]
	if payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(payload, target)
}

func (store *appEngineFailingStore) Save(name string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saves++
	if store.fail || store.saves == store.failAt || store.failAtSaves[store.saves] {
		return errors.New("injected save failure")
	}
	payload, err := json.Marshal(value)
	if err == nil {
		store.entries[name] = payload
	}
	if err == nil && store.failAfterCommit {
		return errors.New("injected post-commit failure")
	}
	return err
}

func appEngineRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func appEngineDeployRequest(handler http.Handler, project string) *httptest.ResponseRecorder {
	return appEngineRequest(handler, http.MethodPost, "/deploy",
		`{"project":"`+project+`","service":"default","version":"v1","runtime":"python312","code":"print(1)"}`)
}

func decodeAppEngineResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

func assertAppEngineError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status = %d, want %d, body = %s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	decodeAppEngineResponse(t, response, &envelope)
	if envelope.Error.Code != code || envelope.Error.Status != status {
		t.Fatalf("error envelope = %#v", envelope.Error)
	}
}

func mustAppEngineJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
