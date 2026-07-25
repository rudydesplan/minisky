package serverless

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

type fakeBuildBackend struct {
	mu            sync.Mutex
	functionCalls []buildFunctionCall
	serviceCalls  []buildServiceCall
	serviceFiles  map[string]string
	functionErr   error
	serviceErr    error
}

type buildFunctionCall struct {
	identity   orchestrator.ServerlessIdentity
	name       string
	sourcePath string
	entryPoint string
}

type buildServiceCall struct {
	identity   orchestrator.ServerlessIdentity
	name       string
	sourcePath string
}

func (b *fakeBuildBackend) Enabled() bool               { return true }
func (b *fakeBuildBackend) Requested() bool             { return true }
func (b *fakeBuildBackend) Status() config.BackendState { return config.BackendState{} }
func (b *fakeBuildBackend) GetLogs(string) string       { return "" }
func (b *fakeBuildBackend) DownloadSourceFromGCS(string, string) (string, error) {
	return "", nil
}
func (b *fakeBuildBackend) BuildFunction(identity orchestrator.ServerlessIdentity, sourcePath, entryPoint string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.functionCalls = append(b.functionCalls, buildFunctionCall{identity, identity.Name, sourcePath, entryPoint})
	return "function-image", b.functionErr
}
func (b *fakeBuildBackend) BuildService(identity orchestrator.ServerlessIdentity, sourcePath string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.serviceCalls = append(b.serviceCalls, buildServiceCall{identity, identity.Name, sourcePath})
	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return "", err
	}
	b.serviceFiles = make(map[string]string, len(entries))
	for _, entry := range entries {
		contents, err := os.ReadFile(filepath.Join(sourcePath, entry.Name()))
		if err != nil {
			return "", err
		}
		b.serviceFiles[entry.Name()] = string(contents)
	}
	return "service-image", b.serviceErr
}

type fakeServerlessManager struct {
	mu             sync.Mutex
	provisionURL   string
	provisionErr   error
	provisionCalls []orchestrator.ServerlessIdentity
	deleteCalls    []orchestrator.ServerlessIdentity
	deleteErr      error
	deleteStarted  chan struct{}
	allowDelete    chan struct{}
	containers     map[orchestrator.ServerlessIdentity]string
}

func (m *fakeServerlessManager) ProvisionServerlessVM(identity orchestrator.ServerlessIdentity, image string, _ []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.provisionCalls = append(m.provisionCalls, identity)
	if m.containers != nil && m.provisionErr == nil {
		m.containers[identity] = image
	}
	return m.provisionURL, m.provisionErr
}
func (m *fakeServerlessManager) DeleteServerlessVM(identity orchestrator.ServerlessIdentity) error {
	m.mu.Lock()
	m.deleteCalls = append(m.deleteCalls, identity)
	started := m.deleteStarted
	allowed := m.allowDelete
	m.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if allowed != nil {
		<-allowed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.containers != nil && m.deleteErr == nil {
		delete(m.containers, identity)
	}
	return m.deleteErr
}

func TestDeployResourceSelectsBuilderByType(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantFn    int
		wantSvc   int
		wantEntry string
	}{
		{
			name:    "service uses service builder",
			body:    `{"type":"service","name":"hello","runtime":"python312","code":"print('ok')"}`,
			wantSvc: 1,
		},
		{
			name:      "function keeps function builder and entrypoint",
			body:      `{"type":"function","name":"hello","runtime":"python312","entryPoint":"serve","code":"def serve(request): pass"}`,
			wantFn:    1,
			wantEntry: "serve",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &fakeBuildBackend{}
			opMgr := orchestrator.NewOperationManager()
			api := newAPI(opMgr, nil, nil, nil)
			api.backend = backend
			api.svcMgr = &fakeServerlessManager{provisionURL: "http://127.0.0.1:1234"}

			request := httptest.NewRequest(http.MethodPost, "/v2/deploy", strings.NewReader(tt.body))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("deploy status = %d, body = %s", response.Code, response.Body.String())
			}
			var op orchestrator.Operation
			if err := json.NewDecoder(response.Body).Decode(&op); err != nil {
				t.Fatal(err)
			}
			waitForOperation(t, opMgr, op.Name)

			backend.mu.Lock()
			defer backend.mu.Unlock()
			if len(backend.functionCalls) != tt.wantFn || len(backend.serviceCalls) != tt.wantSvc {
				t.Fatalf("build calls: functions=%d services=%d, want functions=%d services=%d",
					len(backend.functionCalls), len(backend.serviceCalls), tt.wantFn, tt.wantSvc)
			}
			if tt.wantEntry != "" && backend.functionCalls[0].entryPoint != tt.wantEntry {
				t.Fatalf("function entrypoint = %q, want %q", backend.functionCalls[0].entryPoint, tt.wantEntry)
			}
			if tt.wantSvc == 1 {
				names := make([]string, 0, len(backend.serviceFiles))
				for name := range backend.serviceFiles {
					names = append(names, name)
				}
				sort.Strings(names)
				if got, want := strings.Join(names, ","), "Procfile,main.py,requirements.txt"; got != want {
					t.Fatalf("service source files = %q, want %q", got, want)
				}
				if got := backend.serviceFiles["main.py"]; got != "print('ok')" {
					t.Fatalf("main.py = %q", got)
				}
				if got := backend.serviceFiles["requirements.txt"]; got != "" {
					t.Fatalf("requirements.txt = %q, want empty", got)
				}
				if got := backend.serviceFiles["Procfile"]; got != "web: python main.py\n" {
					t.Fatalf("Procfile = %q", got)
				}
			}
		})
	}
}

func TestDeployServicePersistsReadyMetadata(t *testing.T) {
	store, err := state.New(t.TempDir(), "deploy")
	if err != nil {
		t.Fatal(err)
	}
	opMgr := orchestrator.NewOperationManager()
	manager := &fakeServerlessManager{provisionURL: "http://127.0.0.1:4567"}
	api, err := NewAPIWithStore(opMgr, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.backend = &fakeBuildBackend{}
	api.svcMgr = manager

	op := deployForTest(t, api, `{"type":"service","name":"hello","runtime":"python312","code":"print('ready')"}`)
	terminal := waitForOperation(t, opMgr, op.Name)
	if terminal.Error != nil {
		t.Fatalf("operation error = %v", terminal.Error)
	}
	service := api.services["default-project:us-central1:hello"]
	if service.Reconciling || service.Uri != manager.provisionURL {
		t.Fatalf("service = %#v", service)
	}

	reloaded, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	persisted := reloaded.services["default-project:us-central1:hello"]
	if persisted == nil || persisted.Reconciling || persisted.Uri != manager.provisionURL {
		t.Fatalf("persisted service = %#v", persisted)
	}
}

func TestDeployServiceRecordsBuildAndProvisionFailures(t *testing.T) {
	tests := []struct {
		name       string
		backendErr error
		managerErr error
		wantReason string
		wantText   string
	}{
		{name: "build", backendErr: errors.New("builder rejected source"), wantReason: "BuildFailed", wantText: "builder rejected source"},
		{name: "provision", managerErr: errors.New("container failed readiness"), wantReason: "ProvisionFailed", wantText: "container failed readiness"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opMgr := orchestrator.NewOperationManager()
			api := newAPI(opMgr, nil, nil, nil)
			api.backend = &fakeBuildBackend{serviceErr: tt.backendErr}
			api.svcMgr = &fakeServerlessManager{provisionURL: "http://unused", provisionErr: tt.managerErr}

			op := deployForTest(t, api, `{"type":"service","name":"broken","runtime":"python312","code":"print('broken')"}`)
			terminal := waitForOperation(t, opMgr, op.Name)
			if terminal.Error == nil || !strings.Contains(terminal.Error.Message, tt.wantText) {
				t.Fatalf("operation error = %#v, want %q", terminal.Error, tt.wantText)
			}
			service := api.services["default-project:us-central1:broken"]
			if service == nil || service.Reconciling || service.Uri != "" || len(service.Conditions) != 1 {
				t.Fatalf("failed service = %#v", service)
			}
			condition := service.Conditions[0]
			if condition.State != "CONDITION_FAILED" || condition.Reason != tt.wantReason ||
				!strings.Contains(condition.Message, tt.wantText) {
				t.Fatalf("failure condition = %#v", condition)
			}
			if len(api.functions) != 0 {
				t.Fatalf("service failure changed function entries: %#v", api.functions)
			}
		})
	}
}

func TestDeployCleansOwnedBackendAfterPersistenceFailure(t *testing.T) {
	tests := []struct {
		resourceType string
		wantType     orchestrator.ServerlessResourceType
	}{
		{resourceType: "function", wantType: orchestrator.ServerlessFunction},
		{resourceType: "service", wantType: orchestrator.ServerlessService},
	}
	for _, tt := range tests {
		t.Run(tt.resourceType, func(t *testing.T) {
			store, err := state.New(t.TempDir(), "persist-failure")
			if err != nil {
				t.Fatal(err)
			}
			opMgr := orchestrator.NewOperationManager()
			api, err := NewAPIWithStore(opMgr, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			manager := &fakeServerlessManager{provisionURL: "http://127.0.0.1:4567"}
			api.backend = &fakeBuildBackend{}
			api.svcMgr = manager

			op := deployForTest(t, api, `{"type":"`+tt.resourceType+`","name":"hello","runtime":"python312","code":"print('ready')"}`)
			if err := os.RemoveAll(store.ProfileDir()); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(store.ProfileDir(), []byte("blocks state directory"), 0600); err != nil {
				t.Fatal(err)
			}

			terminal := waitForOperation(t, opMgr, op.Name)
			if terminal.Error == nil || !strings.Contains(terminal.Error.Message, "persist deployed resource") {
				t.Fatalf("operation error = %#v, want persistence failure", terminal.Error)
			}
			manager.mu.Lock()
			if len(manager.deleteCalls) != 1 {
				t.Fatalf("cleanup calls = %#v, want one exact owned cleanup", manager.deleteCalls)
			}
			wantIdentity := orchestrator.ServerlessIdentity{
				ResourceType: tt.wantType,
				Project:      "default-project",
				Location:     "us-central1",
				Name:         "hello",
			}
			if manager.deleteCalls[0] != wantIdentity {
				t.Fatalf("cleanup identity = %#v, want %#v", manager.deleteCalls[0], wantIdentity)
			}
			manager.mu.Unlock()

			api.mu.RLock()
			if tt.resourceType == "function" {
				function := api.functions["default-project:us-central1:hello"]
				if function == nil || function.State != "FAILED" || function.Url != "" {
					t.Fatalf("function after persistence failure = %#v", function)
				}
			} else {
				service := api.services["default-project:us-central1:hello"]
				if service == nil || service.Reconciling || service.Uri != "" || len(service.Conditions) != 1 {
					t.Fatalf("service after persistence failure = %#v", service)
				}
				if service.Conditions[0].Reason != "PersistenceFailed" ||
					service.Conditions[0].State != "CONDITION_FAILED" {
					t.Fatalf("persistence failure condition = %#v", service.Conditions[0])
				}
			}
			api.mu.RUnlock()
		})
	}
}

func TestDeployPersistenceFailureReportsBoundedCleanupFailure(t *testing.T) {
	store, err := state.New(t.TempDir(), "cleanup-failure")
	if err != nil {
		t.Fatal(err)
	}
	opMgr := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(opMgr, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	cleanupMessage := "cleanup refused: " + strings.Repeat("x", 400)
	manager := &fakeServerlessManager{
		provisionURL: "http://127.0.0.1:4567",
		deleteErr:    errors.New(cleanupMessage),
	}
	api.backend = &fakeBuildBackend{}
	api.svcMgr = manager

	op := deployForTest(t, api, `{"type":"service","name":"hello","runtime":"python312","code":"print('ready')"}`)
	if err := os.RemoveAll(store.ProfileDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.ProfileDir(), []byte("blocks state directory"), 0600); err != nil {
		t.Fatal(err)
	}

	terminal := waitForOperation(t, opMgr, op.Name)
	if terminal.Error == nil ||
		!strings.Contains(terminal.Error.Message, "persist deployed resource") ||
		!strings.Contains(terminal.Error.Message, "cleanup owned backend failed") {
		t.Fatalf("operation error = %#v", terminal.Error)
	}
	if strings.Contains(terminal.Error.Message, strings.Repeat("x", 257)) {
		t.Fatalf("cleanup error was not bounded: %d bytes", len(terminal.Error.Message))
	}
	api.mu.RLock()
	service := api.services["default-project:us-central1:hello"]
	api.mu.RUnlock()
	if service == nil || len(service.Conditions) != 1 ||
		service.Conditions[0].Reason != "PersistenceFailed" {
		t.Fatalf("retryable service metadata = %#v", service)
	}
}

func TestDeleteFunctionAndServiceUseServerlessLifecycle(t *testing.T) {
	tests := []struct {
		resourceType string
		path         string
		add          func(*API)
	}{
		{
			resourceType: "function",
			path:         "/v2/projects/demo/locations/us-central1/functions/hello",
			add: func(api *API) {
				api.functions["demo:us-central1:hello"] = &Function{Name: "hello"}
			},
		},
		{
			resourceType: "service",
			path:         "/v2/projects/demo/locations/us-central1/services/hello",
			add: func(api *API) {
				api.services["demo:us-central1:hello"] = &Service{Name: "hello"}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.resourceType, func(t *testing.T) {
			opMgr := orchestrator.NewOperationManager()
			manager := &fakeServerlessManager{}
			api := newAPI(opMgr, nil, nil, nil)
			api.svcMgr = manager
			tt.add(api)

			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, tt.path, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
			}
			var body struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			parts := strings.Split(body.Name, "/")
			terminal := waitForOperation(t, opMgr, parts[len(parts)-1])
			if terminal.Error != nil {
				t.Fatalf("delete operation error = %#v", terminal.Error)
			}
			manager.mu.Lock()
			defer manager.mu.Unlock()
			if len(manager.deleteCalls) != 1 {
				t.Fatalf("serverless delete calls = %#v", manager.deleteCalls)
			}
			wantType := orchestrator.ServerlessFunction
			if tt.resourceType == "service" {
				wantType = orchestrator.ServerlessService
			}
			want := orchestrator.ServerlessIdentity{
				ResourceType: wantType,
				Project:      "demo",
				Location:     "us-central1",
				Name:         "hello",
			}
			if manager.deleteCalls[0] != want {
				t.Fatalf("serverless delete identity = %#v, want %#v", manager.deleteCalls[0], want)
			}
		})
	}
}

func TestSameShortNameUsesDistinctBackendIdentities(t *testing.T) {
	opMgr := orchestrator.NewOperationManager()
	manager := &fakeServerlessManager{provisionURL: "http://127.0.0.1:4567"}
	api := newAPI(opMgr, nil, nil, nil)
	api.backend = &fakeBuildBackend{}
	api.svcMgr = manager

	deployments := []string{
		`{"type":"function","name":"shared","project":"project-a","location":"us-central1","runtime":"python312","code":"print('a')"}`,
		`{"type":"service","name":"shared","project":"project-a","location":"us-central1","runtime":"python312","code":"print('service')"}`,
		`{"type":"function","name":"shared","project":"project-b","location":"us-central1","runtime":"python312","code":"print('b')"}`,
	}
	for _, body := range deployments {
		op := deployForTest(t, api, body)
		if terminal := waitForOperation(t, opMgr, op.Name); terminal.Error != nil {
			t.Fatalf("deployment failed: %#v", terminal.Error)
		}
	}

	manager.mu.Lock()
	if len(manager.provisionCalls) != 3 {
		t.Fatalf("provision calls = %#v", manager.provisionCalls)
	}
	identities := make(map[orchestrator.ServerlessIdentity]bool, len(manager.provisionCalls))
	for _, identity := range manager.provisionCalls {
		identities[identity] = true
	}
	manager.mu.Unlock()
	if len(identities) != 3 {
		t.Fatalf("backend identities collided: %#v", identities)
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete,
		"/v2/projects/project-a/locations/us-central1/functions/shared", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	var operation struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(response.Body).Decode(&operation); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(operation.Name, "/")
	if terminal := waitForOperation(t, opMgr, parts[len(parts)-1]); terminal.Error != nil {
		t.Fatalf("delete operation failed: %#v", terminal.Error)
	}

	api.mu.RLock()
	defer api.mu.RUnlock()
	if _, exists := api.functions["project-a:us-central1:shared"]; exists {
		t.Fatal("deleted function metadata remains")
	}
	if api.functions["project-b:us-central1:shared"] == nil {
		t.Fatal("deleting project-a function removed project-b function")
	}
	if api.services["project-a:us-central1:shared"] == nil {
		t.Fatal("deleting function removed same-name service")
	}
}

func TestDeleteKeepsMetadataWhenBackendOrPersistenceFails(t *testing.T) {
	tests := []struct {
		name         string
		resourceType string
		path         string
		backendErr   error
		blockStore   bool
		add          func(*API)
		exists       func(*API) bool
	}{
		{
			name: "function ownership failure", resourceType: "function",
			path:       "/v2/projects/demo/locations/us-central1/functions/hello",
			backendErr: errors.New("ownership mismatch"),
			add: func(api *API) {
				api.functions["demo:us-central1:hello"] = &Function{Name: "projects/demo/locations/us-central1/functions/hello"}
			},
			exists: func(api *API) bool { return api.functions["demo:us-central1:hello"] != nil },
		},
		{
			name: "service ownership failure", resourceType: "service",
			path:       "/v2/projects/demo/locations/us-central1/services/hello",
			backendErr: errors.New("ownership mismatch"),
			add: func(api *API) {
				api.services["demo:us-central1:hello"] = &Service{Name: "projects/demo/locations/us-central1/services/hello"}
			},
			exists: func(api *API) bool { return api.services["demo:us-central1:hello"] != nil },
		},
		{
			name: "function save failure", resourceType: "function",
			path:       "/v2/projects/demo/locations/us-central1/functions/hello",
			blockStore: true,
			add: func(api *API) {
				api.functions["demo:us-central1:hello"] = &Function{Name: "projects/demo/locations/us-central1/functions/hello"}
			},
			exists: func(api *API) bool { return api.functions["demo:us-central1:hello"] != nil },
		},
		{
			name: "service save failure", resourceType: "service",
			path:       "/v2/projects/demo/locations/us-central1/services/hello",
			blockStore: true,
			add: func(api *API) {
				api.services["demo:us-central1:hello"] = &Service{Name: "projects/demo/locations/us-central1/services/hello"}
			},
			exists: func(api *API) bool { return api.services["demo:us-central1:hello"] != nil },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := state.New(t.TempDir(), "delete-failure")
			if err != nil {
				t.Fatal(err)
			}
			opMgr := orchestrator.NewOperationManager()
			api, err := NewAPIWithStore(opMgr, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			manager := &fakeServerlessManager{deleteErr: tt.backendErr}
			api.svcMgr = manager
			tt.add(api)
			if err := api.persistMetadata(); err != nil {
				t.Fatal(err)
			}
			if tt.blockStore {
				if err := os.RemoveAll(store.ProfileDir()); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(store.ProfileDir(), []byte("blocks state directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}

			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, tt.path, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
			}
			var operation struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(response.Body).Decode(&operation); err != nil {
				t.Fatal(err)
			}
			parts := strings.Split(operation.Name, "/")
			terminal := waitForOperation(t, opMgr, parts[len(parts)-1])
			if terminal.Error == nil {
				t.Fatal("delete operation reported success")
			}
			api.mu.RLock()
			exists := tt.exists(api)
			api.mu.RUnlock()
			if !exists {
				t.Fatal("failed delete removed retryable metadata")
			}
		})
	}
}

func TestDeleteAndRedeploySameIdentityAreSerializedWithoutBlockingOthers(t *testing.T) {
	opMgr := orchestrator.NewOperationManager()
	identityA := orchestrator.ServerlessIdentity{
		ResourceType: orchestrator.ServerlessFunction,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "shared",
	}
	deleteStarted := make(chan struct{}, 1)
	allowDelete := make(chan struct{})
	manager := &fakeServerlessManager{
		provisionURL:  "http://127.0.0.1:4567",
		deleteStarted: deleteStarted,
		allowDelete:   allowDelete,
		containers:    map[orchestrator.ServerlessIdentity]string{identityA: "generation-a"},
	}
	api := newAPI(opMgr, nil, nil, nil)
	api.backend = &fakeBuildBackend{}
	api.svcMgr = manager
	api.functions["demo:us-central1:shared"] = &Function{
		Name:       identityA.CanonicalResource(),
		State:      "ACTIVE",
		SourceCode: "generation-a",
	}

	deleteResponse := httptest.NewRecorder()
	api.ServeHTTP(deleteResponse, httptest.NewRequest(
		http.MethodDelete,
		"/v2/projects/demo/locations/us-central1/functions/shared",
		nil,
	))
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteResponse.Code, deleteResponse.Body.String())
	}
	var deleteOperation struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(deleteResponse.Body).Decode(&deleteOperation); err != nil {
		t.Fatal(err)
	}
	deleteParts := strings.Split(deleteOperation.Name, "/")
	deleteOperationName := deleteParts[len(deleteParts)-1]
	select {
	case <-deleteStarted:
	case <-time.After(4 * time.Second):
		t.Fatal("delete did not reach controlled backend")
	}

	conflict := httptest.NewRecorder()
	api.ServeHTTP(conflict, httptest.NewRequest(
		http.MethodPost,
		"/v2/deploy",
		strings.NewReader(`{"type":"function","name":"shared","project":"demo","location":"us-central1","runtime":"python312","code":"generation-b"}`),
	))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("same-identity redeploy status = %d, body = %s", conflict.Code, conflict.Body.String())
	}
	var conflictBody struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.NewDecoder(conflict.Body).Decode(&conflictBody); err != nil {
		t.Fatal(err)
	}
	if conflictBody.Error.Code != http.StatusConflict || conflictBody.Error.Status != "ABORTED" {
		t.Fatalf("conflict body = %#v", conflictBody)
	}

	unrelated := deployForTest(t, api,
		`{"type":"function","name":"other","project":"demo","location":"us-central1","runtime":"python312","code":"other"}`)
	if terminal := waitForOperation(t, opMgr, unrelated.Name); terminal.Error != nil {
		t.Fatalf("unrelated deployment failed while delete paused: %#v", terminal.Error)
	}

	close(allowDelete)
	if terminal := waitForOperation(t, opMgr, deleteOperationName); terminal.Error != nil {
		t.Fatalf("delete failed: %#v", terminal.Error)
	}

	replacement := deployForTest(t, api,
		`{"type":"function","name":"shared","project":"demo","location":"us-central1","runtime":"python312","code":"generation-b"}`)
	if terminal := waitForOperation(t, opMgr, replacement.Name); terminal.Error != nil {
		t.Fatalf("replacement deployment failed: %#v", terminal.Error)
	}

	api.mu.RLock()
	function := api.functions["demo:us-central1:shared"]
	api.mu.RUnlock()
	if function == nil || function.SourceCode != "generation-b" || function.State != "ACTIVE" {
		t.Fatalf("replacement metadata = %#v", function)
	}
	manager.mu.Lock()
	container, exists := manager.containers[identityA]
	manager.mu.Unlock()
	if !exists || container != "function-image" {
		t.Fatalf("replacement backend = %q, exists=%t", container, exists)
	}
	api.lifecycleMu.Lock()
	activeLifecycle := len(api.activeLifecycle)
	api.lifecycleMu.Unlock()
	if activeLifecycle != 0 {
		t.Fatalf("active lifecycle entries after completion = %d", activeLifecycle)
	}
}

func deployForTest(t *testing.T, api *API, body string) *orchestrator.Operation {
	t.Helper()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v2/deploy", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("deploy status = %d, body = %s", response.Code, response.Body.String())
	}
	var op orchestrator.Operation
	if err := json.NewDecoder(response.Body).Decode(&op); err != nil {
		t.Fatal(err)
	}
	return &op
}

func waitForOperation(t *testing.T, manager *orchestrator.OperationManager, name string) *orchestrator.Operation {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		op := manager.Get(name)
		if op != nil && op.Done {
			return op
		}
		time.Sleep(10 * time.Millisecond)
	}
	op := manager.Get(name)
	t.Fatalf("operation %q did not finish: %#v", name, op)
	return nil
}
