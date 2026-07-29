package cloudsql

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestCloudSQLMetadataRehydratesWithoutContainerRecreation(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	opMgr := orchestrator.NewOperationManager()
	backend := &fakeCloudSQLBackend{}
	api, err := NewAPIWithStore(opMgr, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.backend = backend
	key := instanceKey("project", "sql")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{
		Name: "sql", Project: "project", State: "RUNNABLE", DatabaseVersion: "POSTGRES_18",
		IpAddresses: []IpMapping{{Type: "PRIMARY", IpAddress: "127.0.0.1:5432"}},
	})
	for _, request := range []struct {
		path string
		body string
	}{
		{"/v1/projects/project/instances/sql/databases", `{"name":"app"}`},
		{"/v1/projects/project/instances/sql/users", `{"name":"app-user"}`},
	} {
		recorder := httptest.NewRecorder()
		api.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, request.path, bytes.NewBufferString(request.body)))
		if recorder.Code != http.StatusOK {
			t.Fatalf("POST %s = %d, body %s", request.path, recorder.Code, recorder.Body.String())
		}
	}
	delete(api.runtimes, key)
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewAPIWithStore(opMgr, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.backend = backend
	if restarted.opMgr != opMgr {
		t.Fatal("rehydration replaced orchestrator dependencies")
	}
	instance := restarted.instances[key]
	if instance == nil || instance.State != "SUSPENDED" ||
		instance.BackendStatus != metadataOnlyBackendState || len(instance.IpAddresses) != 0 {
		t.Fatalf("restored instance did not disclose metadata-only state: %#v", instance)
	}
	if got := restarted.databases[key]; len(got) != 1 || got[0].Name != "app" {
		t.Fatalf("restored databases = %#v", got)
	}
	if got := restarted.users[key]; len(got) != 1 || got[0].Name != "app-user" {
		t.Fatalf("restored users = %#v", got)
	}
}

func TestCloudSQLPortableMetadataOmitsRuntimeTokensAndPasswords(t *testing.T) {
	store, err := state.New(t.TempDir(), "secure")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	seedRunnableCloudSQL(api, key, &DatabaseInstance{
		Name: "sql", Project: "project", State: "RUNNABLE", DatabaseVersion: "POSTGRES_18",
	})
	api.users[key] = []*User{{
		Name: "app", Project: "project", Instance: "sql", Password: "portable-secret",
	}}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	var snapshot bytes.Buffer
	if err := store.Export(&snapshot); err != nil {
		t.Fatal(err)
	}
	payload := snapshot.String()
	if strings.Contains(payload, "portable-secret") || strings.Contains(payload, "runtimeToken") {
		t.Fatalf("portable Cloud SQL metadata contains secret provenance: %s", payload)
	}
	var exported state.Snapshot
	if err := json.Unmarshal(snapshot.Bytes(), &exported); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exported.Entries[cloudSQLStateEntry]), "ownershipFingerprint") {
		t.Fatal("portable metadata lost non-secret ownership fingerprint")
	}
}

func TestCloudSQLStateMissingAndCorrupt(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	missing, err := state.New(root, "missing")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, nil, missing)
	if err != nil {
		t.Fatalf("missing state should start empty: %v", err)
	}
	if len(api.instances) != 0 || len(api.databases) != 0 || len(api.users) != 0 {
		t.Fatal("missing state did not start empty")
	}

	corrupt, err := state.New(root, "corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(corrupt.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corrupt.ProfileDir(), "state.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAPIWithStore(nil, nil, corrupt); err == nil ||
		!strings.Contains(err.Error(), "load Cloud SQL metadata") {
		t.Fatalf("corrupt state error = %v", err)
	}
}

func TestCloudSQLPublicConstructionFailsClosedWithoutOverwritingCorruptState(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt")
	store, err := state.New(root, "corrupt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(store.ProfileDir(), "state.json")
	corrupt := []byte("{broken")
	if err := os.WriteFile(statePath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}

	api := NewAPI(orchestrator.NewOperationManager(), nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/project/instances",
		bytes.NewBufferString(`{"name":"must-not-exist"}`),
	))
	if response.Code != http.StatusServiceUnavailable ||
		!strings.Contains(response.Body.String(), `"status":"UNAVAILABLE"`) {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	after, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, corrupt) {
		t.Fatalf("corrupt state was overwritten: %q", after)
	}
}

func TestCloudSQLOperationPollingIsProjectAndServiceScoped(t *testing.T) {
	manager := orchestrator.NewOperationManager()
	api := newAPI(manager, nil, nil)
	foreignProject := manager.Register("sql#operation", "CREATE",
		"https://sqladmin.googleapis.com/v1/projects/other/instances/db", "", "us-central1")
	foreignService := manager.Register("compute#operation", "insert",
		"https://www.googleapis.com/compute/v1/projects/project/instances/vm", "", "")
	for _, operation := range []*orchestrator.Operation{foreignProject, foreignService} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
			"/v1/projects/project/operations/"+operation.Name, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("foreign operation %s status=%d body=%s", operation.Kind, response.Code, response.Body.String())
		}
	}
}
