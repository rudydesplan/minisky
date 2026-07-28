package composer

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
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestCreateEnvironment(t *testing.T) {
	api := newTestAPI()
	api.backend = &fakeAirflowBackend{endpoint: "http://127.0.0.1:18080"}
	body := `{"name":"projects/test/locations/us-central1/environments/my-env","config":{"nodeCount":3,"softwareConfig":{"imageVersion":"composer-2.0.0-airflow-2.2.3"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/environments", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	waitForEnvironmentState(t, api, "projects/test/locations/us-central1/environments/my-env", "RUNNING")
	api.mu.RLock()
	env := deepCopyEnvironment(api.environments["projects/test/locations/us-central1/environments/my-env"])
	api.mu.RUnlock()
	if env.Config == nil || env.Config.AirflowURI != "http://127.0.0.1:18080" {
		t.Fatalf("missing executable Airflow endpoint: %+v", env.Config)
	}
}

func TestCreateEnvironmentProvidesReadableConfigForMinimalProviderRequest(t *testing.T) {
	api := newTestAPI()
	api.backend = &fakeAirflowBackend{endpoint: "http://127.0.0.1:18080"}
	name := "projects/test/locations/us-central1/environments/provider"
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/environments",
		bytes.NewBufferString(`{"name":"`+name+`","labels":{"goog-terraform-provisioned":"true"}}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	waitForEnvironmentState(t, api, name, "RUNNING")
	if got := api.environments[name].Config; got == nil || got.SoftwareConfig == nil || got.SoftwareConfig.ImageVersion == "" {
		t.Fatalf("minimal provider response has unreadable config: %#v", got)
	}
}

func TestCreateEnvironmentMissingName(t *testing.T) {
	api := newTestAPI()
	body := `{"config":{"nodeCount":3}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/environments", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateEnvironmentInvalidName(t *testing.T) {
	api := newTestAPI()
	body := `{"name":"projects/other/locations/us-central1/environments/my-env"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/environments", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateEnvironmentDuplicate(t *testing.T) {
	api := newTestAPI()
	api.backend = &fakeAirflowBackend{}
	api.mu.Lock()
	api.environments["projects/test/locations/us-central1/environments/dup"] = &Environment{
		Name: "projects/test/locations/us-central1/environments/dup",
		UUID: "existing-uuid",
	}
	api.mu.Unlock()

	body := `{"name":"projects/test/locations/us-central1/environments/dup"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/environments", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetEnvironment(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.environments["projects/test/locations/us-central1/environments/e1"] = &Environment{
		Name:       "projects/test/locations/us-central1/environments/e1",
		UUID:       "uuid-123",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		State:      "RUNNING",
		Config: &EnvironmentConfig{
			NodeCount: 3,
			SoftwareConfig: &SoftwareConfig{
				ImageVersion: "composer-2.0.0-airflow-2.2.3",
			},
		},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments/e1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var env Environment
	_ = json.Unmarshal(w.Body.Bytes(), &env)
	if env.Name != "projects/test/locations/us-central1/environments/e1" {
		t.Fatalf("unexpected name: %s", env.Name)
	}
	if env.State != "RUNNING" {
		t.Fatalf("unexpected state: %s", env.State)
	}
	if env.Config == nil || env.Config.NodeCount != 3 {
		t.Fatal("expected config in response")
	}
}

func TestGetEnvironmentNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListEnvironments(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.environments["projects/test/locations/us-central1/environments/alpha"] = &Environment{Name: "projects/test/locations/us-central1/environments/alpha", UUID: "u1"}
	api.environments["projects/test/locations/us-central1/environments/beta"] = &Environment{Name: "projects/test/locations/us-central1/environments/beta", UUID: "u2"}
	api.environments["projects/test/locations/us-central1/environments/gamma"] = &Environment{Name: "projects/test/locations/us-central1/environments/gamma", UUID: "u3"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	envs := resp["environments"].([]any)
	if len(envs) != 2 {
		t.Fatalf("expected 2 environments, got %d", len(envs))
	}
	first := envs[0].(map[string]any)["name"].(string)
	second := envs[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	envs = resp["environments"].([]any)
	if len(envs) != 1 {
		t.Fatalf("expected 1 environment on second page, got %d", len(envs))
	}
}

func TestListEnvironmentsEmpty(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	envs := resp["environments"].([]any)
	if len(envs) != 0 {
		t.Fatalf("expected 0 environments, got %d", len(envs))
	}
}

func TestDeleteEnvironment(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.environments["projects/test/locations/us-central1/environments/e1"] = &Environment{
		Name: "projects/test/locations/us-central1/environments/e1",
		UUID: "uuid-1",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/environments/e1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, exists := api.environments["projects/test/locations/us-central1/environments/e1"]
	api.mu.RUnlock()
	if !exists {
		t.Fatal("unsupported delete mutated environment state")
	}
}

func TestDeleteEnvironmentNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/environments/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPatchEnvironment(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.environments["projects/test/locations/us-central1/environments/e1"] = &Environment{
		Name:       "projects/test/locations/us-central1/environments/e1",
		UUID:       "uuid-1",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		State:      "RUNNING",
		Config: &EnvironmentConfig{
			NodeCount: 3,
			SoftwareConfig: &SoftwareConfig{
				ImageVersion: "composer-2.0.0-airflow-2.2.3",
			},
		},
		Labels: map[string]string{"env": "dev"},
	}
	api.mu.Unlock()

	body := `{"labels":{"env":"prod"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/environments/e1?updateMask=labels", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	env := api.environments["projects/test/locations/us-central1/environments/e1"]
	api.mu.RUnlock()
	if env.Labels["env"] != "dev" {
		t.Fatalf("unsupported patch mutated labels: %v", env.Labels)
	}
	if env.UUID != "uuid-1" {
		t.Fatalf("uuid should be preserved, got %s", env.UUID)
	}
	if env.CreateTime != "2024-01-01T00:00:00Z" {
		t.Fatalf("createTime should be preserved, got %s", env.CreateTime)
	}
	if env.UpdateTime != "2024-01-01T00:00:00Z" {
		t.Fatal("unsupported patch mutated updateTime")
	}
}

func TestPatchEnvironmentNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"labels":{"x":"y"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/environments/missing?updateMask=labels", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	op, err := api.opMgr.RegisterScopedTargetDurable("composer#operation", "update",
		"projects/test/locations/us-central1/environments/op-test")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/"+op.Name, nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var opResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &opResp)
	meta := opResp["metadata"].(map[string]any)
	if meta["verb"] != "update" {
		t.Fatalf("expected verb=update, got %v", meta["verb"])
	}
}

func TestGetOperationNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/operations/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateEnvironmentMetadataSaveFailureRollsBackDurableOperation(t *testing.T) {
	operationStore := &mockStore{data: make(map[string][]byte)}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &mockStore{
		data:       make(map[string][]byte),
		failOnSave: map[int]error{1: errors.New("metadata save failed")},
	}
	api := &API{
		opMgr:        opMgr,
		stateStore:   metadataStore,
		environments: make(map[string]*Environment),
		backend:      &fakeAirflowBackend{},
	}

	body := `{"name":"projects/test/locations/us-central1/environments/save-fails"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/environments", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if len(api.opMgr.List()) != 0 {
		t.Fatalf("failed create left an in-memory operation: %+v", api.opMgr.List())
	}
	restarted, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.List()) != 0 {
		t.Fatalf("failed create left a durable operation: %+v", restarted.List())
	}
}

func TestPatchEnvironmentOperationRegistrationFailureDoesNotMutate(t *testing.T) {
	operationStore := &mockStore{
		data:       make(map[string][]byte),
		failOnSave: map[int]error{1: errors.New("operation save failed")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &mockStore{data: make(map[string][]byte)}
	name := "projects/test/locations/us-central1/environments/e1"
	api := &API{
		opMgr:      opMgr,
		stateStore: metadataStore,
		environments: map[string]*Environment{name: {
			Name:       name,
			CreateTime: "2024-01-01T00:00:00Z",
			UpdateTime: "2024-01-01T00:00:00Z",
			State:      "RUNNING",
			Labels:     map[string]string{"env": "dev"},
		}},
		backend: &fakeAirflowBackend{},
	}

	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/environments/e1?updateMask=labels",
		bytes.NewBufferString(`{"labels":{"env":"prod"}}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments/e1", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("environment was not externally visible after failed patch: %d: %s", get.Code, get.Body.String())
	}
	var environment Environment
	if err := json.Unmarshal(get.Body.Bytes(), &environment); err != nil {
		t.Fatal(err)
	}
	if environment.Labels["env"] != "dev" || environment.UpdateTime != "2024-01-01T00:00:00Z" {
		t.Fatalf("operation registration failure mutated environment: %+v", &environment)
	}
	if metadataStore.saveCalls != 0 {
		t.Fatalf("operation registration failure persisted metadata %d times", metadataStore.saveCalls)
	}
}

func TestDeleteEnvironmentOperationRegistrationFailureDoesNotMutateBackend(t *testing.T) {
	operationStore := &mockStore{
		data:       make(map[string][]byte),
		failOnSave: map[int]error{1: errors.New("operation save failed")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &mockStore{data: make(map[string][]byte)}
	backend := &fakeAirflowBackend{}
	name := "projects/test/locations/us-central1/environments/e1"
	api := &API{
		opMgr:        opMgr,
		stateStore:   metadataStore,
		environments: map[string]*Environment{name: {Name: name, State: "RUNNING"}},
		backend:      backend,
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/environments/e1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments/e1", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("operation registration failure hid environment: %d: %s", get.Code, get.Body.String())
	}
	if backend.deleteCalls != 0 {
		t.Fatalf("operation registration failure deleted backend %d times", backend.deleteCalls)
	}
	if metadataStore.saveCalls != 0 {
		t.Fatalf("operation registration failure persisted metadata %d times", metadataStore.saveCalls)
	}
}

func TestEnvironmentMutationTerminalOperationSaveFailureReturnsErrorAndKeepsMetadataTruth(t *testing.T) {
	const name = "projects/test/locations/us-central1/environments/e1"
	tests := []struct {
		name       string
		method     string
		body       string
		wantExists bool
		wantLabel  string
	}{
		{
			name:       "patch",
			method:     http.MethodPatch,
			body:       `{"labels":{"env":"prod"}}`,
			wantExists: true,
			wantLabel:  "prod",
		},
		{
			name:       "delete",
			method:     http.MethodDelete,
			wantExists: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operationStore := &mockStore{
				data:       make(map[string][]byte),
				failOnSave: map[int]error{2: errors.New("terminal operation save failed")},
			}
			opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			metadataStore := &mockStore{data: make(map[string][]byte)}
			api := &API{
				opMgr:      opMgr,
				stateStore: metadataStore,
				environments: map[string]*Environment{name: {
					Name:       name,
					CreateTime: "2024-01-01T00:00:00Z",
					UpdateTime: "2024-01-01T00:00:00Z",
					State:      "RUNNING",
					Labels:     map[string]string{"env": "dev"},
				}},
				backend: &fakeAirflowBackend{},
			}

			path := "/v1/projects/test/locations/us-central1/environments/e1"
			if test.method == http.MethodPatch {
				path += "?updateMask=labels"
			}
			req := httptest.NewRequest(test.method, path, bytes.NewBufferString(test.body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)

			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), `"done":true`) {
				t.Fatalf("terminal persistence failure fabricated success: %s", w.Body.String())
			}
			var response struct {
				Error struct {
					Status string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Error.Status != "UNAVAILABLE" {
				t.Fatalf("error status = %q, want UNAVAILABLE", response.Error.Status)
			}

			operations := opMgr.List()
			if len(operations) != 1 {
				t.Fatalf("operations = %+v, want one reconciled operation", operations)
			}
			inProcess := operations[0]
			if !inProcess.Done || inProcess.Error == nil ||
				!strings.Contains(inProcess.Error.Message, "interrupted by MiniSky restart") {
				t.Fatalf("in-process operation was not reconciled: %+v", inProcess)
			}

			restartedOps, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			durable := restartedOps.Get(inProcess.Name)
			if durable == nil || !durable.Done || durable.Error == nil ||
				durable.Error.Message != inProcess.Error.Message {
				t.Fatalf("operation truth diverged across restart: in-process=%+v restarted=%+v", inProcess, durable)
			}

			restarted := &API{
				opMgr:        orchestrator.NewOperationManager(),
				stateStore:   metadataStore,
				environments: make(map[string]*Environment),
			}
			if err := restarted.loadState(); err != nil {
				t.Fatal(err)
			}
			restarted.mu.RLock()
			environment, exists := restarted.environments[name]
			restarted.mu.RUnlock()
			if exists != test.wantExists {
				t.Fatalf("persisted environment existence = %v, want %v", exists, test.wantExists)
			}
			if exists && environment.Labels["env"] != test.wantLabel {
				t.Fatalf("persisted labels = %v, want env=%q", environment.Labels, test.wantLabel)
			}
		})
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		opMgr:        newTestAPI().opMgr,
		stateStore:   store,
		environments: make(map[string]*Environment),
	}

	api.mu.Lock()
	api.environments["projects/p/locations/l/environments/e1"] = &Environment{
		Name:       "projects/p/locations/l/environments/e1",
		UUID:       "uuid-persist",
		CreateTime: "2024-06-01T00:00:00Z",
		UpdateTime: "2024-06-01T00:00:00Z",
		State:      "RUNNING",
		Config: &EnvironmentConfig{
			NodeCount:  5,
			AirflowURI: "http://127.0.0.1:18080",
		},
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := &API{
		opMgr:        newTestAPI().opMgr,
		stateStore:   store,
		environments: make(map[string]*Environment),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	env, ok := api2.environments["projects/p/locations/l/environments/e1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("environment not found after reload")
	}
	if env.UUID != "uuid-persist" {
		t.Fatalf("expected uuid-persist, got %s", env.UUID)
	}
	if env.Config == nil || env.Config.NodeCount != 5 {
		t.Fatal("config lost after reload")
	}
	if env.State != "ERROR" {
		t.Fatalf("rehydrated environment must not claim a running backend, got %q", env.State)
	}
	if env.Config.AirflowURI != "" {
		t.Fatalf("rehydrated environment exposed stale Airflow endpoint %q", env.Config.AirflowURI)
	}
}

func TestReloadReconcilesExactOwnedAirflowBackendWithoutProvisioning(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	name := "projects/p/locations/l/environments/e1"
	if err := store.Save(composerStateEntry, composerMetadata{Environments: map[string]*Environment{
		name: {
			Name:  name,
			State: "RUNNING",
			Config: &EnvironmentConfig{
				AirflowURI: "http://stale.invalid",
			},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	backend := &fakeAirflowBackend{endpoint: "http://127.0.0.1:18080", owned: true}
	api := &API{
		opMgr:        orchestrator.NewOperationManager(),
		stateStore:   store,
		environments: make(map[string]*Environment),
		backend:      backend,
	}
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	environment := api.environments[name]
	if environment == nil || environment.State != "RUNNING" ||
		environment.Config == nil || environment.Config.AirflowURI != backend.endpoint {
		t.Fatalf("reconciled environment = %+v", environment)
	}
	if backend.provisionCalls != 0 || backend.reconcileCalls != 1 {
		t.Fatalf("restart provision calls = %d, reconcile calls = %d; want 0 and 1",
			backend.provisionCalls, backend.reconcileCalls)
	}
}

func TestReloadFailsClosedWithoutHealthyExactOwnedAirflowBackend(t *testing.T) {
	for _, test := range []struct {
		name      string
		owned     bool
		reconcile error
	}{
		{name: "missing", owned: false},
		{name: "wrong-owned", reconcile: errors.New("container exists but is not owned")},
		{name: "unhealthy", owned: true, reconcile: errors.New("Airflow readiness failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &mockStore{data: make(map[string][]byte)}
			name := "projects/p/locations/l/environments/e1"
			if err := store.Save(composerStateEntry, composerMetadata{Environments: map[string]*Environment{
				name: {
					Name:  name,
					State: "RUNNING",
					Config: &EnvironmentConfig{
						AirflowURI:   "http://stale.invalid",
						DagGcsPrefix: "minisky://stale/dags",
					},
				},
			}}); err != nil {
				t.Fatal(err)
			}
			backend := &fakeAirflowBackend{endpoint: "http://127.0.0.1:18080", owned: test.owned, reconcileErr: test.reconcile}
			api := &API{
				opMgr:        orchestrator.NewOperationManager(),
				stateStore:   store,
				environments: make(map[string]*Environment),
				backend:      backend,
			}

			if err := api.loadState(); err != nil {
				t.Fatal(err)
			}
			environment := api.environments[name]
			if environment == nil || environment.State != "ERROR" {
				t.Fatalf("rehydrated environment = %+v, want fail-closed ERROR", environment)
			}
			if environment.Config == nil || environment.Config.AirflowURI != "" || environment.Config.DagGcsPrefix != "" {
				t.Fatalf("fail-closed environment retained stale backend endpoints: %+v", environment.Config)
			}
			if backend.provisionCalls != 0 || backend.reconcileCalls != 1 {
				t.Fatalf("restart provision calls = %d, reconcile calls = %d; want 0 and 1",
					backend.provisionCalls, backend.reconcileCalls)
			}
		})
	}
}

func TestReloadGivesEachAirflowResourceAFairReconcileBudget(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	environments := make(map[string]*Environment)
	for _, id := range []string{"a", "b", "c"} {
		name := "projects/p/locations/l/environments/" + id
		environments[name] = &Environment{Name: name, State: "RUNNING"}
	}
	if err := store.Save(composerStateEntry, composerMetadata{Environments: environments}); err != nil {
		t.Fatal(err)
	}
	backend := &budgetAirflowBackend{}
	api := &API{
		opMgr:            orchestrator.NewOperationManager(),
		stateStore:       store,
		environments:     make(map[string]*Environment),
		backend:          backend,
		reconcileTimeout: 10 * time.Millisecond,
	}
	start := time.Now()
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if backend.reconcileCalls != 3 {
		t.Fatalf("reconcile calls = %d, want 3", backend.reconcileCalls)
	}
	if elapsed < 20*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("reconciliation elapsed = %v, want independent bounded budgets", elapsed)
	}
}

func TestConcurrentCreateAndGet(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"name":"projects/test/locations/us-central1/environments/e-%d"}`, idx)
			req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/environments", bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusNotImplemented {
				t.Errorf("unexpected status %d for create %d", w.Code, idx)
			}
		}(i)
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/environments", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

type mockStore struct {
	mu         sync.Mutex
	data       map[string][]byte
	failOnSave map[int]error
	saveCalls  int
}

type fakeAirflowBackend struct {
	endpoint       string
	owned          bool
	reconcileErr   error
	provisionCalls int
	reconcileCalls int
	deleteCalls    int
}

type budgetAirflowBackend struct {
	reconcileCalls int
}

func (*budgetAirflowBackend) Provision(context.Context, string) (string, error) {
	return "", nil
}
func (b *budgetAirflowBackend) Reconcile(ctx context.Context, _ string) (string, bool, error) {
	b.reconcileCalls++
	<-ctx.Done()
	return "", false, ctx.Err()
}
func (*budgetAirflowBackend) Delete(context.Context, string) error { return nil }

func (b *fakeAirflowBackend) Provision(context.Context, string) (string, error) {
	b.provisionCalls++
	return b.endpoint, nil
}

func (b *fakeAirflowBackend) Reconcile(context.Context, string) (string, bool, error) {
	b.reconcileCalls++
	return b.endpoint, b.owned, b.reconcileErr
}

func (b *fakeAirflowBackend) Delete(context.Context, string) error {
	b.deleteCalls++
	return nil
}

func waitForEnvironmentState(t *testing.T, api *API, name, want string) {
	t.Helper()
	for i := 0; i < 5000; i++ {
		api.mu.RLock()
		state := ""
		if environment := api.environments[name]; environment != nil {
			state = environment.State
		}
		api.mu.RUnlock()
		if state == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("environment did not reach %s", want)
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return fmt.Errorf("not found: %w", state.ErrNotFound)
	}
	return json.Unmarshal(raw, target)
}

func (m *mockStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveCalls++
	if err := m.failOnSave[m.saveCalls]; err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
