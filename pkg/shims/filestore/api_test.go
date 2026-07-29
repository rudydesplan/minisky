package filestore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestCreateInstance(t *testing.T) {
	api := newTestAPI()
	api.dataRoot = t.TempDir()
	body := `{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}],"networks":[{"network":"default","modes":["MODE_IPV4"]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/instances?instanceId=my-inst", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != false {
		t.Fatal("expected LRO not done")
	}
	name, _ := resp["name"].(string)
	if name == "" {
		t.Fatal("expected operation name")
	}

	// Verify instance stored
	api.mu.RLock()
	inst := api.instances["projects/test/locations/us-central1/instances/my-inst"]
	api.mu.RUnlock()
	if inst == nil {
		t.Fatal("instance not stored")
	}
	if inst.Tier != "BASIC_HDD" {
		t.Fatalf("unexpected tier: %s", inst.Tier)
	}
	if inst.State != "READY" {
		t.Fatalf("unexpected state: %s", inst.State)
	}
}

func TestLocalShareReadWriteIsBoundedAndPrivate(t *testing.T) {
	api := newTestAPI()
	api.dataRoot = t.TempDir()
	body := `{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}],"networks":[{"network":"default"}]}`
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=my-inst", bytes.NewBufferString(body)))
	if create.Code != http.StatusOK {
		t.Fatalf("create failed: %d: %s", create.Code, create.Body.String())
	}

	name := "projects/test/locations/us-central1/instances/my-inst"
	if err := api.WriteShareFile(name, "share1", "nested/hello.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := api.ReadShareFile(name, "share1", "nested/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("read = %q, want hello", got)
	}
	if err := api.WriteShareFile(name, "share1", "../escape", []byte("bad")); err == nil {
		t.Fatal("expected traversal rejection")
	}

	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances/my-inst", nil))
	if strings.Contains(get.Body.String(), api.dataRoot) {
		t.Fatalf("response exposed local root: %s", get.Body.String())
	}
}

func TestCreateSaveFailureCleansLocalShares(t *testing.T) {
	api := newAPI(newTestAPI().opMgr, failingStore{})
	api.dataRoot = t.TempDir()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=failed",
		bytes.NewBufferString(`{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}]}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	entries, err := os.ReadDir(api.dataRoot)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("local backend leaked %d entries", len(entries))
	}
}

func TestCreateAmbiguousSavePreservesCommittedShareTree(t *testing.T) {
	store := &ambiguousCreateFilestoreStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)
	api.dataRoot = t.TempDir()
	name := "projects/test/locations/us-central1/instances/committed"

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=committed",
		bytes.NewBufferString(`{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}]}`)))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(api.dataRoot, api.instanceDataKey(name), "share1")); err != nil {
		t.Fatalf("committed instance share tree was removed: %v", err)
	}
	api.mu.RLock()
	instance := api.instances[name]
	api.mu.RUnlock()
	if instance == nil {
		t.Fatal("committed durable instance was not reconciled into memory")
	}
	var durable filestoreMetadata
	if err := store.Load(filestoreStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if durable.Instances[name] == nil {
		t.Fatal("durable instance missing after ambiguous create")
	}
	if api.opMgr.PersistenceError() == nil {
		t.Fatal("ambiguous create did not leave sticky degradation")
	}
}

func TestCreateUncertainSavePreservesShareTreeAndDegrades(t *testing.T) {
	store := &ambiguousCreateFilestoreStore{
		data:    make(map[string][]byte),
		loadErr: errors.New("injected readback failure"),
	}
	api := newAPI(newTestAPI().opMgr, store)
	api.dataRoot = t.TempDir()
	name := "projects/test/locations/us-central1/instances/uncertain"

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=uncertain",
		bytes.NewBufferString(`{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}]}`)))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(api.dataRoot, api.instanceDataKey(name), "share1")); err != nil {
		t.Fatalf("uncertain instance share tree was removed: %v", err)
	}
	if api.opMgr.PersistenceError() == nil {
		t.Fatal("uncertain create did not leave sticky degradation")
	}
}

func TestLocalShareDataSurvivesMetadataRestart(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	root := t.TempDir()
	api := newAPI(newTestAPI().opMgr, store)
	api.dataRoot = root
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=restart",
		bytes.NewBufferString(`{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}]}`)))
	if create.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", create.Code, create.Body.String())
	}
	name := "projects/test/locations/us-central1/instances/restart"
	if err := api.WriteShareFile(name, "share1", "hello.txt", []byte("durable")); err != nil {
		t.Fatal(err)
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	restarted.dataRoot = root
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	data, err := restarted.ReadShareFile(name, "share1", "hello.txt")
	if err != nil || string(data) != "durable" {
		t.Fatalf("restart read = %q, err = %v", data, err)
	}
}

func TestCapacityGbUsesJSONStringAcrossGetListAndRestart(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	root := t.TempDir()
	api := newAPI(newTestAPI().opMgr, store)
	api.dataRoot = root
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/instances?instanceId=restart-json",
		bytes.NewBufferString(`{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}]}`)))
	if create.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", create.Code, create.Body.String())
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	restarted.dataRoot = root
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}

	for _, stage := range []struct {
		name string
		api  *API
	}{
		{name: "before restart", api: api},
		{name: "after restart", api: restarted},
	} {
		t.Run(stage.name, func(t *testing.T) {
			get := httptest.NewRecorder()
			stage.api.ServeHTTP(get, httptest.NewRequest(http.MethodGet,
				"/v1/projects/test/locations/us-central1/instances/restart-json", nil))
			if get.Code != http.StatusOK {
				t.Fatalf("get failed: %d %s", get.Code, get.Body.String())
			}
			var instance struct {
				FileShares []struct {
					CapacityGb json.RawMessage `json:"capacityGb"`
				} `json:"fileShares"`
			}
			if err := json.Unmarshal(get.Body.Bytes(), &instance); err != nil {
				t.Fatal(err)
			}
			if got := string(instance.FileShares[0].CapacityGb); got != `"1024"` {
				t.Fatalf("get capacityGb JSON = %s, want string", got)
			}

			list := httptest.NewRecorder()
			stage.api.ServeHTTP(list, httptest.NewRequest(http.MethodGet,
				"/v1/projects/test/locations/us-central1/instances", nil))
			if list.Code != http.StatusOK {
				t.Fatalf("list failed: %d %s", list.Code, list.Body.String())
			}
			var collection struct {
				Instances []struct {
					FileShares []struct {
						CapacityGb json.RawMessage `json:"capacityGb"`
					} `json:"fileShares"`
				} `json:"instances"`
			}
			if err := json.Unmarshal(list.Body.Bytes(), &collection); err != nil {
				t.Fatal(err)
			}
			if got := string(collection.Instances[0].FileShares[0].CapacityGb); got != `"1024"` {
				t.Fatalf("list capacityGb JSON = %s, want string", got)
			}
		})
	}
}

func TestLegacyNumericCapacityStateLoadsAndEmitsJSONString(t *testing.T) {
	name := "projects/test/locations/us-central1/instances/legacy"
	store := &mockStore{data: map[string][]byte{
		filestoreStateEntry: []byte(`{"instances":{"` + name + `":{"name":"` + name +
			`","tier":"BASIC_HDD","state":"ERROR","fileShares":[{"name":"share1","capacityGb":1024}]}}}`),
	}}
	api := newAPI(newTestAPI().opMgr, store)
	api.dataRoot = t.TempDir()
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	if got := api.instances[name].FileShares[0].CapacityGb; got != 1024 {
		t.Fatalf("loaded capacityGb = %d, want 1024", got)
	}

	get := httptest.NewRecorder()
	api.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/"+name, nil))
	if get.Code != http.StatusOK {
		t.Fatalf("get failed: %d %s", get.Code, get.Body.String())
	}
	if strings.Contains(get.Body.String(), `"capacityGb":1024`) ||
		!strings.Contains(get.Body.String(), `"capacityGb":"1024"`) {
		t.Fatalf("capacityGb response is not a JSON string: %s", get.Body.String())
	}
}

func TestRestartFailsClosedWhenOwnedShareTreeIsMissing(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	name := "projects/test/locations/us-central1/instances/missing-data"
	seed := newAPI(newTestAPI().opMgr, store)
	seed.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		FileShares: []FileShare{{Name: "share1", CapacityGb: 1024}},
	}
	if err := seed.persistState(); err != nil {
		t.Fatal(err)
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	restarted.dataRoot = t.TempDir()
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if got := restarted.instances[name]; got == nil || got.State != "ERROR" {
		t.Fatalf("missing data-plane state = %#v, want ERROR", got)
	}
	if err := restarted.WriteShareFile(name, "share1", "new.txt", []byte("new")); err == nil {
		t.Fatal("write recreated a missing share tree")
	}
}

func TestRestartRejectsSymlinkedInstanceParentPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("descriptor-pinned filesystem traversal is unavailable on Windows")
	}
	store := &mockStore{data: make(map[string][]byte)}
	name := "projects/test/locations/us-central1/instances/symlinked-data"
	seed := newAPI(newTestAPI().opMgr, store)
	seed.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		FileShares: []FileShare{{Name: "share1", CapacityGb: 1024}},
	}
	if err := seed.persistState(); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(outside, "share1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, seed.instanceDataKey(name))); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	restarted := newAPI(newTestAPI().opMgr, store)
	restarted.dataRoot = root
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if got := restarted.instances[name]; got == nil || got.State != "ERROR" {
		t.Fatalf("symlinked data-plane state = %#v, want ERROR", got)
	}
}

func TestCorruptStateFailsClosedWithoutOverwritingSnapshot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-filestore")
	store, err := state.New(root, "corrupt-filestore")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(filestoreStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}

	api := NewAPI(nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/v1/projects/test/locations/us-central1/instances", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(filestoreStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

func TestCreateInstanceMissingTier(t *testing.T) {
	api := newTestAPI()
	body := `{"fileShares":[{"name":"share1","capacityGb":"1024"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/instances?instanceId=i1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateInstanceInvalidTier(t *testing.T) {
	api := newTestAPI()
	body := `{"tier":"INVALID_TIER"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/instances?instanceId=i1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateInstanceMissingID(t *testing.T) {
	api := newTestAPI()
	body := `{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/instances", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateInstanceDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.instances["projects/test/locations/us-central1/instances/dup"] = &Instance{
		Name: "projects/test/locations/us-central1/instances/dup",
		Tier: "BASIC_HDD",
	}
	api.mu.Unlock()

	body := `{"tier":"BASIC_HDD","fileShares":[{"name":"share1","capacityGb":"1024"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/instances?instanceId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetInstance(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.instances["projects/test/locations/us-central1/instances/i1"] = &Instance{
		Name:       "projects/test/locations/us-central1/instances/i1",
		Tier:       "ENTERPRISE",
		State:      "READY",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/instances/i1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var inst Instance
	_ = json.Unmarshal(w.Body.Bytes(), &inst)
	if inst.Tier != "ENTERPRISE" {
		t.Fatalf("unexpected tier: %s", inst.Tier)
	}
}

func TestGetInstanceNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/instances/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListInstances(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.instances["projects/test/locations/us-central1/instances/a"] = &Instance{Name: "projects/test/locations/us-central1/instances/a", Tier: "BASIC_HDD"}
	api.instances["projects/test/locations/us-central1/instances/b"] = &Instance{Name: "projects/test/locations/us-central1/instances/b", Tier: "BASIC_SSD"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/instances", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	instances := resp["instances"].([]any)
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}
}

func TestPatchInstance(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.instances["projects/test/locations/us-central1/instances/i1"] = &Instance{
		Name:       "projects/test/locations/us-central1/instances/i1",
		Tier:       "BASIC_HDD",
		State:      "READY",
		CreateTime: "2024-01-01T00:00:00Z",
		Labels:     map[string]string{"env": "dev"},
	}
	api.mu.Unlock()

	body := `{"labels":{"env":"prod"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/instances/i1?updateMask=labels", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for patch")
	}

	api.mu.RLock()
	inst := api.instances["projects/test/locations/us-central1/instances/i1"]
	api.mu.RUnlock()
	if inst.Labels["env"] != "prod" {
		t.Fatalf("expected labels updated, got %v", inst.Labels)
	}
	operationName, _ := resp["name"].(string)
	if !strings.HasPrefix(operationName, "projects/test/locations/us-central1/operations/") {
		t.Fatalf("operation name = %q, want canonical scoped name", operationName)
	}
	terminal, _ := resp["response"].(map[string]any)
	if terminal["@type"] != "type.googleapis.com/google.cloud.filestore.v1.Instance" ||
		terminal["name"] != "projects/test/locations/us-central1/instances/i1" {
		t.Fatalf("terminal response = %#v", terminal)
	}
}

func TestPatchInstanceRejectsBackingShareChanges(t *testing.T) {
	for name, body := range map[string]string{
		"rename":  `{"fileShares":[{"name":"renamed","capacityGb":"1024"}]}`,
		"removal": `{"fileShares":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			api := newTestAPI()
			instanceName := "projects/test/locations/us-central1/instances/i1"
			api.instances[instanceName] = &Instance{
				Name: instanceName, Tier: "BASIC_HDD", State: "READY",
				FileShares: []FileShare{{Name: "share1", CapacityGb: 1024}},
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
				"/v1/"+instanceName+"?updateMask=fileShares", bytes.NewBufferString(body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			if got := api.instances[instanceName].FileShares[0].Name; got != "share1" {
				t.Fatalf("share changed to %q", got)
			}
		})
	}
}

func TestPatchInstanceCompensatesPostCommitSaveAndOperation(t *testing.T) {
	store := &postCommitFilestoreStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)
	name := "projects/test/locations/us-central1/instances/i1"
	api.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		Labels:     map[string]string{"env": "old"},
		FileShares: []FileShare{{Name: "share1"}},
	}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.failNext = true

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/"+name+"?updateMask=labels", bytes.NewBufferString(`{"labels":{"env":"new"}}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.instances[name].Labels["env"]; got != "old" {
		t.Fatalf("visible label = %q, want old", got)
	}
	var durable filestoreMetadata
	if err := store.Load(filestoreStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if got := durable.Instances[name].Labels["env"]; got != "old" {
		t.Fatalf("durable label = %q, want compensated old", got)
	}
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("compensated patch retained operations: %#v", operations)
	}
	if api.opMgr.PersistenceError() == nil {
		t.Fatal("ambiguous resource save did not leave sticky degradation")
	}
}

func TestPatchInstancePreservesShareData(t *testing.T) {
	api := newTestAPI()
	api.dataRoot = t.TempDir()
	name := "projects/test/locations/us-central1/instances/i1"
	api.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		FileShares: []FileShare{{Name: "share1", CapacityGb: 1024}},
	}
	if err := api.createShareDirectories(name, api.instances[name].FileShares); err != nil {
		t.Fatal(err)
	}
	if err := api.WriteShareFile(name, "share1", "keep.txt", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/projects/test/locations/us-central1/instances/i1?updateMask=labels",
		bytes.NewBufferString(`{"labels":{"safe":"true"}}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("patch status = %d: %s", response.Code, response.Body.String())
	}
	data, err := api.ReadShareFile(name, "share1", "keep.txt")
	if err != nil || string(data) != "keep" {
		t.Fatalf("data after patch = %q, %v", data, err)
	}
}

func TestDeleteRemovesOwnedDataAndRecreateDoesNotExposeFiles(t *testing.T) {
	api := newTestAPI()
	api.dataRoot = t.TempDir()
	name := "projects/test/locations/us-central1/instances/i1"
	api.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		FileShares: []FileShare{{Name: "share1", CapacityGb: 1024}},
	}
	if err := api.createShareDirectories(name, api.instances[name].FileShares); err != nil {
		t.Fatal(err)
	}
	if err := api.WriteShareFile(name, "share1", "secret.txt", []byte("secret")); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/test/locations/us-central1/instances/i1", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d: %s", response.Code, response.Body.String())
	}
	api.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		FileShares: []FileShare{{Name: "share1", CapacityGb: 1024}},
	}
	if err := api.createShareDirectories(name, api.instances[name].FileShares); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ReadShareFile(name, "share1", "secret.txt"); !os.IsNotExist(err) {
		t.Fatalf("recreated instance exposed deleted data: %v", err)
	}
}

func TestDeleteCleanupFailureRestoresMetadata(t *testing.T) {
	api := newTestAPI()
	api.dataRoot = t.TempDir()
	name := "projects/test/locations/us-central1/instances/i1"
	api.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		FileShares: []FileShare{{Name: "share1"}},
	}
	if err := api.createShareDirectories(name, api.instances[name].FileShares); err != nil {
		t.Fatal(err)
	}
	api.removeInstanceData = func(string) error { return fmt.Errorf("injected cleanup failure") }
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/test/locations/us-central1/instances/i1", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d: %s", response.Code, response.Body.String())
	}
	if api.instances[name] == nil {
		t.Fatal("cleanup failure removed metadata")
	}
}

func TestFilestoreSemanticImportRejectsFilesystemComponents(t *testing.T) {
	for _, share := range []string{"../escape", "a/b", `a\b`, ".", ""} {
		metadata := filestoreMetadata{Instances: map[string]*Instance{
			"projects/p/locations/l/instances/i": {
				Name: "projects/p/locations/l/instances/i", Tier: "BASIC_HDD",
				FileShares: []FileShare{{Name: share}},
			},
		}}
		if err := validateFilestoreMetadata(state.EntryValidationContext{}, &metadata); err == nil {
			t.Fatalf("share %q accepted", share)
		}
		store, err := state.New(t.TempDir(), "import")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(metadata)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := json.Marshal(state.Snapshot{
			Format: state.SnapshotFormat, Version: state.Version,
			Entries: map[string]json.RawMessage{filestoreStateEntry: raw},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Import(bytes.NewReader(snapshot)); err == nil {
			t.Fatalf("semantic import accepted share %q", share)
		}
	}
}

func TestShareIORejectsSymlinkTraversal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	api := newTestAPI()
	api.dataRoot = t.TempDir()
	name := "projects/test/locations/us-central1/instances/i1"
	api.instances[name] = &Instance{
		Name: name, Tier: "BASIC_HDD", State: "READY",
		FileShares: []FileShare{{Name: "share1"}},
	}
	if err := api.createShareDirectories(name, api.instances[name].FileShares); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	sharePath := filepath.Join(api.instanceDataPath(name), "share1")
	if err := os.Symlink(outside, filepath.Join(sharePath, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := api.WriteShareFile(name, "share1", "link/pwned", []byte("bad")); err == nil {
		t.Fatal("write followed symlink")
	}
	if _, err := os.Stat(filepath.Join(outside, "pwned")); !os.IsNotExist(err) {
		t.Fatalf("outside file created: %v", err)
	}
}

func TestDeleteInstance(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.instances["projects/test/locations/us-central1/instances/i1"] = &Instance{
		Name: "projects/test/locations/us-central1/instances/i1",
		Tier: "BASIC_HDD",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/instances/i1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for delete")
	}

	api.mu.RLock()
	_, exists := api.instances["projects/test/locations/us-central1/instances/i1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("instance should be deleted")
	}
}

func TestDeleteInstanceNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/instances/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	body := `{"tier":"BASIC_SSD","fileShares":[{"name":"s","capacityGb":"512"}],"networks":[{"network":"default"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/instances?instanceId=op-test", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed: %d: %s", w.Code, w.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	opPath := createResp["name"].(string)

	req = httptest.NewRequest(http.MethodGet, "/v1/"+opPath, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)

	api.mu.Lock()
	api.instances["projects/p/locations/l/instances/i1"] = &Instance{
		Name:       "projects/p/locations/l/instances/i1",
		Tier:       "BASIC_HDD",
		State:      "READY",
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
	inst, ok := api2.instances["projects/p/locations/l/instances/i1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("instance not found after reload")
	}
	if inst.Tier != "BASIC_HDD" {
		t.Fatalf("unexpected tier after reload: %s", inst.Tier)
	}
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"tier":"BASIC_HDD","fileShares":[{"name":"s","capacityGb":"512"}]}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/projects/test/locations/us-central1/instances?instanceId=i-%d", idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("unexpected status %d", w.Code)
			}
		}(i)
	}
	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

type failingStore struct{}

func (failingStore) Load(string, any) error { return state.ErrNotFound }
func (failingStore) Save(string, any) error { return fmt.Errorf("injected save failure") }

type postCommitFilestoreStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	failNext bool
}

type ambiguousCreateFilestoreStore struct {
	mu      sync.Mutex
	data    map[string][]byte
	saves   int
	loadErr error
}

func (s *ambiguousCreateFilestoreStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *ambiguousCreateFilestoreStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if s.saves == 1 {
		s.data[name] = raw
		return errors.New("post-commit create save error")
	}
	return errors.New("pre-commit compensation save error")
}

func (s *postCommitFilestoreStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *postCommitFilestoreStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data[name] = raw
	if s.failNext {
		s.failNext = false
		return errors.New("post-commit save error")
	}
	return nil
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
