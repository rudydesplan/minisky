package dataform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestCreateRepository(t *testing.T) {
	api := newTestAPI()
	body := `{"displayName":"My Repo","gitRemoteSettings":{"url":"https://github.com/test/repo"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/test/locations/us-central1/repositories?repositoryId=my-repo", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var repo Repository
	_ = json.Unmarshal(w.Body.Bytes(), &repo)
	if repo.Name != "projects/test/locations/us-central1/repositories/my-repo" {
		t.Fatalf("unexpected name: %s", repo.Name)
	}
	if repo.CreateTime == "" {
		t.Fatal("expected createTime")
	}
	if repo.DisplayName != "My Repo" {
		t.Fatalf("unexpected displayName: %s", repo.DisplayName)
	}
}

func TestCreateRepositoryMissingID(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/test/locations/us-central1/repositories", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateRepositoryDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/dup"] = &Repository{
		Name: "projects/test/locations/us-central1/repositories/dup",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/test/locations/us-central1/repositories?repositoryId=dup", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetRepository(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/r1"] = &Repository{
		Name:       "projects/test/locations/us-central1/repositories/r1",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1beta1/projects/test/locations/us-central1/repositories/r1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var repo Repository
	_ = json.Unmarshal(w.Body.Bytes(), &repo)
	if repo.Name != "projects/test/locations/us-central1/repositories/r1" {
		t.Fatalf("unexpected name: %s", repo.Name)
	}
}

func TestGetRepositoryNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1beta1/projects/test/locations/us-central1/repositories/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListRepositories(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/alpha"] = &Repository{Name: "projects/test/locations/us-central1/repositories/alpha"}
	api.repositories["projects/test/locations/us-central1/repositories/beta"] = &Repository{Name: "projects/test/locations/us-central1/repositories/beta"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1beta1/projects/test/locations/us-central1/repositories", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	repos := resp["repositories"].([]any)
	if len(repos) != 2 {
		t.Fatalf("expected 2 repos, got %d", len(repos))
	}
}

func TestDeleteRepository(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/r1"] = &Repository{Name: "projects/test/locations/us-central1/repositories/r1"}
	api.workspaces["projects/test/locations/us-central1/repositories/r1/workspaces/w1"] = &Workspace{Name: "projects/test/locations/us-central1/repositories/r1/workspaces/w1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1beta1/projects/test/locations/us-central1/repositories/r1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	_, repoExists := api.repositories["projects/test/locations/us-central1/repositories/r1"]
	_, wsExists := api.workspaces["projects/test/locations/us-central1/repositories/r1/workspaces/w1"]
	api.mu.RUnlock()
	if repoExists {
		t.Fatal("repository should be deleted")
	}
	if wsExists {
		t.Fatal("workspace should be cascade deleted")
	}
}

func TestCreateWorkspace(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/r1"] = &Repository{Name: "projects/test/locations/us-central1/repositories/r1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/test/locations/us-central1/repositories/r1/workspaces?workspaceId=ws1", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var ws Workspace
	_ = json.Unmarshal(w.Body.Bytes(), &ws)
	if ws.Name != "projects/test/locations/us-central1/repositories/r1/workspaces/ws1" {
		t.Fatalf("unexpected name: %s", ws.Name)
	}
}

func TestCreateWorkspaceNoParent(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/test/locations/us-central1/repositories/missing/workspaces?workspaceId=ws1", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkspaceMissingID(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/r1"] = &Repository{Name: "projects/test/locations/us-central1/repositories/r1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/test/locations/us-central1/repositories/r1/workspaces", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetWorkspace(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workspaces["projects/test/locations/us-central1/repositories/r1/workspaces/ws1"] = &Workspace{
		Name:       "projects/test/locations/us-central1/repositories/r1/workspaces/ws1",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1beta1/projects/test/locations/us-central1/repositories/r1/workspaces/ws1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListWorkspaces(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/r1"] = &Repository{Name: "projects/test/locations/us-central1/repositories/r1"}
	api.workspaces["projects/test/locations/us-central1/repositories/r1/workspaces/a"] = &Workspace{Name: "projects/test/locations/us-central1/repositories/r1/workspaces/a"}
	api.workspaces["projects/test/locations/us-central1/repositories/r1/workspaces/b"] = &Workspace{Name: "projects/test/locations/us-central1/repositories/r1/workspaces/b"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1beta1/projects/test/locations/us-central1/repositories/r1/workspaces", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	wss := resp["workspaces"].([]any)
	if len(wss) != 2 {
		t.Fatalf("expected 2 workspaces, got %d", len(wss))
	}
}

func TestDeleteWorkspace(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.workspaces["projects/test/locations/us-central1/repositories/r1/workspaces/ws1"] = &Workspace{Name: "projects/test/locations/us-central1/repositories/r1/workspaces/ws1"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1beta1/projects/test/locations/us-central1/repositories/r1/workspaces/ws1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)

	api.mu.Lock()
	api.repositories["projects/p/locations/l/repositories/r1"] = &Repository{
		Name:       "projects/p/locations/l/repositories/r1",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.workspaces["projects/p/locations/l/repositories/r1/workspaces/w1"] = &Workspace{
		Name:       "projects/p/locations/l/repositories/r1/workspaces/w1",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := newAPI(newTestAPI().opMgr, store)
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	if _, ok := api2.repositories["projects/p/locations/l/repositories/r1"]; !ok {
		t.Fatal("repository not found after reload")
	}
	if _, ok := api2.workspaces["projects/p/locations/l/repositories/r1/workspaces/w1"]; !ok {
		t.Fatal("workspace not found after reload")
	}
	api2.mu.RUnlock()
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.repositories["projects/test/locations/us-central1/repositories/r1"] = &Repository{Name: "projects/test/locations/us-central1/repositories/r1"}
	api.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1beta1/projects/test/locations/us-central1/repositories/r1/workspaces?workspaceId=ws-%d", idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("unexpected status %d", w.Code)
			}
		}(i)
	}
	wg.Wait()
}

func TestCompileAndInvokeEmptyWorkspaceDurably(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)
	repoName := "projects/test/locations/us-central1/repositories/r1"
	workspaceName := repoName + "/workspaces/ws1"
	api.repositories[repoName] = &Repository{Name: repoName}
	api.workspaces[workspaceName] = &Workspace{Name: workspaceName}

	compileReq := httptest.NewRequest(http.MethodPost, "/v1beta1/"+repoName+"/compilationResults",
		bytes.NewBufferString(`{"workspace":"`+workspaceName+`"}`))
	compileW := httptest.NewRecorder()
	api.ServeHTTP(compileW, compileReq)
	if compileW.Code != http.StatusOK {
		t.Fatalf("compile: expected 200, got %d: %s", compileW.Code, compileW.Body.String())
	}
	var compilation CompilationResult
	if err := json.Unmarshal(compileW.Body.Bytes(), &compilation); err != nil {
		t.Fatal(err)
	}
	if compilation.Name == "" || compilation.Workspace != workspaceName || len(compilation.CompilationErrors) != 0 {
		t.Fatalf("unexpected compilation result: %+v", compilation)
	}

	invokeReq := httptest.NewRequest(http.MethodPost, "/v1beta1/"+repoName+"/workflowInvocations",
		bytes.NewBufferString(`{"compilationResult":"`+compilation.Name+`"}`))
	invokeW := httptest.NewRecorder()
	api.ServeHTTP(invokeW, invokeReq)
	if invokeW.Code != http.StatusOK {
		t.Fatalf("invoke: expected 200, got %d: %s", invokeW.Code, invokeW.Body.String())
	}
	var invocation WorkflowInvocation
	if err := json.Unmarshal(invokeW.Body.Bytes(), &invocation); err != nil {
		t.Fatal(err)
	}
	if invocation.State != "SUCCEEDED" || invocation.InvocationTiming == nil ||
		invocation.InvocationTiming.StartTime == "" || invocation.InvocationTiming.EndTime == "" {
		t.Fatalf("expected durable terminal empty-workflow outcome, got %+v", invocation)
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.compilationResults[compilation.Name] == nil || restarted.workflowInvocations[invocation.Name] == nil {
		t.Fatal("compilation or invocation outcome missing after restart")
	}
}

func TestCompilationRejectsForeignWorkspaceBeforeMutation(t *testing.T) {
	api := newTestAPI()
	repoName := "projects/test/locations/us-central1/repositories/r1"
	api.repositories[repoName] = &Repository{Name: repoName}

	req := httptest.NewRequest(http.MethodPost, "/v1beta1/"+repoName+"/compilationResults",
		bytes.NewBufferString(`{"workspace":"projects/test/locations/us-central1/repositories/other/workspaces/ws1"}`))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
	if len(api.compilationResults) != 0 {
		t.Fatal("foreign workspace request mutated compilation state")
	}
}

func TestDeleteRepositoryCascadesExecutionResources(t *testing.T) {
	api := newTestAPI()
	repoName := "projects/test/locations/us-central1/repositories/r1"
	api.repositories[repoName] = &Repository{Name: repoName}
	api.compilationResults[repoName+"/compilationResults/cr-1"] = &CompilationResult{Name: repoName + "/compilationResults/cr-1"}
	api.workflowInvocations[repoName+"/workflowInvocations/wi-1"] = &WorkflowInvocation{Name: repoName + "/workflowInvocations/wi-1"}

	req := httptest.NewRequest(http.MethodDelete, "/v1beta1/"+repoName, nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("delete: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(api.compilationResults) != 0 || len(api.workflowInvocations) != 0 {
		t.Fatal("repository delete left execution children behind")
	}
}

func TestConcurrentWorkspaceCreateAndParentDeleteNeverOrphans(t *testing.T) {
	const repoName = "projects/test/locations/us-central1/repositories/r1"
	for i := 0; i < 50; i++ {
		api := newTestAPI()
		api.repositories[repoName] = &Repository{Name: repoName}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			request := httptest.NewRequest(http.MethodPost, "/v1beta1/"+repoName+"/workspaces?workspaceId=ws1", bytes.NewBufferString(`{}`))
			api.ServeHTTP(httptest.NewRecorder(), request)
		}()
		go func() {
			defer wg.Done()
			request := httptest.NewRequest(http.MethodDelete, "/v1beta1/"+repoName, nil)
			api.ServeHTTP(httptest.NewRecorder(), request)
		}()
		wg.Wait()

		api.mu.RLock()
		_, repoExists := api.repositories[repoName]
		_, workspaceExists := api.workspaces[repoName+"/workspaces/ws1"]
		api.mu.RUnlock()
		if workspaceExists && !repoExists {
			t.Fatal("concurrent parent delete left an orphan workspace")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
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
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
