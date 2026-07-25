package cloudsql

import (
	"bytes"
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
	svcMgr := &orchestrator.ServiceManager{}
	api, err := NewAPIWithStore(opMgr, svcMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	key := instanceKey("project", "sql")
	api.instances[key] = &DatabaseInstance{
		Name: "sql", Project: "project", State: "RUNNABLE",
		IpAddresses: []IpMapping{{Type: "PRIMARY", IpAddress: "127.0.0.1:5432"}},
	}
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

	restarted, err := NewAPIWithStore(opMgr, svcMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.opMgr != opMgr || restarted.svcMgr != svcMgr {
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
