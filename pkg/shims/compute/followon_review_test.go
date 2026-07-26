package compute

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestFirewallTargetsAreProjectScoped(t *testing.T) {
	api, _ := newComputeTestAPI()
	for _, project := range []string{"project-a", "project-b"} {
		api.instances[instanceKey(project, "us-central1-a", "shared")] = &Instance{
			Name: "shared", project: project, zone: "us-central1-a",
			NetworkInterfaces: []NetworkInterface{{Network: networkSelfLink(project, "shared-vpc")}},
		}
	}
	key, dockerVPC, identities, _, err := api.firewallTargetsForNetwork(networkSelfLink("project-a", "shared-vpc"))
	if err != nil {
		t.Fatal(err)
	}
	wantDocker, _ := (orchestrator.VPCNetworkIdentity{Project: "project-a", Network: "shared-vpc"}).DockerName()
	if key != networkSelfLink("project-a", "shared-vpc") || dockerVPC != wantDocker ||
		len(identities) != 1 || identities[0].Project != "project-a" {
		t.Fatalf("key=%q docker=%q identities=%#v", key, dockerVPC, identities)
	}
}

func TestSubnetworkDeleteRejectsPersistedInstanceReference(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "project-a", "custom")
	subnet := persistedSubnetworkForTest("project-a", "us-central1", "subnet-a", "custom", "10.0.0.0/24", "1")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = subnet
	api.nextSubnetworkID = 2
	api.instances[instanceKey("project-a", "us-central1-a", "vm")] = &Instance{
		Name: "vm", NetworkInterfaces: []NetworkInterface{{Subnetwork: subnet.SelfLink}},
	}
	backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/regions/us-central1/subnetworks/subnet-a", "")
	assertComputeError(t, response, http.StatusBadRequest, "FAILED_PRECONDITION")
	if backend.deleteCalls != 0 || api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] == nil {
		t.Fatalf("delete calls=%d subnet=%#v", backend.deleteCalls, api.subnetworks)
	}
}

func TestSubnetworkDeleteRejectsLegacyParentNetworkReference(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "project-a", "custom")
	subnet := persistedSubnetworkForTest("project-a", "us-central1", "subnet-a", "custom", "10.0.0.0/24", "1")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = subnet
	api.nextSubnetworkID = 2
	api.instances[instanceKey("project-a", "us-central1-a", "legacy-gke")] = &Instance{
		Name: "legacy-gke", Status: metadataOnlyStatus,
		Labels:            map[string]string{"managed-by": "gke"},
		NetworkInterfaces: []NetworkInterface{{Network: subnet.Network}},
	}
	backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/regions/us-central1/subnetworks/subnet-a", "")
	assertComputeError(t, response, http.StatusBadRequest, "FAILED_PRECONDITION")
	if backend.deleteCalls != 0 {
		t.Fatalf("legacy reference reached Docker delete %d times", backend.deleteCalls)
	}
}

func TestInstanceInsertSerializesWithSubnetworkDelete(t *testing.T) {
	store := newArmableComputeStore()
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "project-a", "custom")
	subnet := persistedSubnetworkForTest("project-a", "us-central1", "subnet-a", "custom", "10.0.0.0/24", "1")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = subnet
	api.nextSubnetworkID = 2
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	store.arm()

	insertDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		insertDone <- performComputeRequest(api, http.MethodPost,
			"/compute/v1/projects/project-a/zones/us-central1-a/instances",
			`{"name":"vm","labels":{"managed-by":"gke"},"networkInterfaces":[{"subnetwork":"subnet-a"}]}`)
	}()
	<-store.started
	deleteDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		deleteDone <- performComputeRequest(api, http.MethodDelete,
			"/compute/v1/projects/project-a/regions/us-central1/subnetworks/subnet-a", "")
	}()
	select {
	case result := <-deleteDone:
		t.Fatalf("delete interleaved with insert: %d %s", result.Code, result.Body.String())
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	if result := <-insertDone; result.Code != http.StatusOK {
		t.Fatalf("insert status=%d body=%s", result.Code, result.Body.String())
	}
	assertComputeError(t, <-deleteDone, http.StatusBadRequest, "FAILED_PRECONDITION")
}

func TestSubnetworkCreateCleanupFailureFailsClosed(t *testing.T) {
	store := &toggleComputeStore{}
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "project-a", "custom")
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	store.setFail(true)
	backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
	backend.deleteErr = errors.New("cleanup failed")
	base := "/compute/v1/projects/project-a/regions/us-central1/subnetworks"
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
		`{"name":"failed","ipCidrRange":"10.0.0.0/24","network":"custom"}`),
		http.StatusInternalServerError, "INTERNAL")
	if api.subnetworks[subnetworkKey("project-a", "us-central1", "failed")] != nil ||
		api.initializationError() == nil {
		t.Fatalf("metadata=%#v init=%v", api.subnetworks, api.initializationError())
	}
	if message := api.initializationError().Error(); !strings.Contains(message, "injected save failure") ||
		!strings.Contains(message, "cleanup failed") {
		t.Fatalf("initialization error lost causal detail: %v", api.initializationError())
	}
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base, ""),
		http.StatusServiceUnavailable, "FAILED_PRECONDITION")
}

func TestSubnetworkCreatePreexistingBridgeSaveFailureFailsClosedWithoutDelete(t *testing.T) {
	store := &toggleComputeStore{}
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "project-a", "custom")
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
	identity := orchestrator.VPCNetworkIdentity{Project: "project-a", Network: "custom"}
	backend.bridges[identity] = "10.0.0.0/24"
	store.setFail(true)
	base := "/compute/v1/projects/project-a/regions/us-central1/subnetworks"
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
		`{"name":"failed","ipCidrRange":"10.0.0.0/24","network":"custom"}`),
		http.StatusInternalServerError, "INTERNAL")
	if backend.deleteCalls != 0 || api.initializationError() == nil ||
		api.subnetworks[subnetworkKey("project-a", "us-central1", "failed")] != nil {
		t.Fatalf("deleteCalls=%d init=%v metadata=%#v",
			backend.deleteCalls, api.initializationError(), api.subnetworks)
	}
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base, ""),
		http.StatusServiceUnavailable, "FAILED_PRECONDITION")
}

func TestInstanceInsertSaveFailureRollsBackMetadata(t *testing.T) {
	store := &toggleComputeStore{fail: true}
	api := newAPI(orchestrator.NewOperationManager(), nil, store)
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"vm","labels":{"managed-by":"gke"}}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if api.instances[instanceKey("project-a", "us-central1-a", "vm")] != nil ||
		len(api.opMgr.List()) != 0 {
		t.Fatalf("failed insert metadata=%#v operations=%#v", api.instances, api.opMgr.List())
	}
}

func TestConcurrentLegacyCleanupReevaluatesAfterRemoval(t *testing.T) {
	api := newAPI(orchestrator.NewOperationManager(), nil, nil)
	backend := &fakeLegacyVMBackend{}
	api.legacyVM = backend
	keys := []string{
		instanceKey("project-a", "us-central1-a", "shared"),
		instanceKey("project-b", "us-central1-b", "shared"),
	}
	for _, key := range keys {
		api.instances[key] = &Instance{Name: "shared"}
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	for _, key := range keys {
		wait.Add(1)
		go func(key string) {
			defer wait.Done()
			<-start
			eligible := api.removeInstanceAndLegacyCleanupEligibility(key, "shared")
			api.cleanupLegacyComputeVM("shared", eligible)
		}(key)
	}
	close(start)
	wait.Wait()
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.deleted) != 1 || backend.deleted[0] != "shared" {
		t.Fatalf("legacy cleanup calls=%#v", backend.deleted)
	}
}

func TestFirewallOperationFailsWhenRecreationFails(t *testing.T) {
	opMgr := orchestrator.NewOperationManager()
	api := newAPI(opMgr, nil, nil)
	backend := &fakeFirewallBackend{applyErr: errors.New("recreate after delete failed")}
	api.firewall = backend
	api.instances[instanceKey("project-a", "us-central1-a", "vm")] = &Instance{
		Name: "vm", project: "project-a", zone: "us-central1-a",
		NetworkInterfaces: []NetworkInterface{{Network: networkSelfLink("project-a", "custom")}},
	}
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"allow-app","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		poll := performComputeRequest(api, http.MethodGet,
			"/compute/v1/projects/project-a/global/operations/"+operation.Name, "")
		if poll.Code != http.StatusOK {
			t.Fatalf("poll status=%d body=%s", poll.Code, poll.Body.String())
		}
		var current orchestrator.Operation
		decodeComputeResponse(t, poll, &current)
		if current.Done {
			if current.Error == nil {
				t.Fatalf("firewall operation succeeded despite recreation failure: %#v", current)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("firewall operation did not finish")
}

type fakeLegacyVMBackend struct {
	mu      sync.Mutex
	deleted []string
}

func (backend *fakeLegacyVMBackend) DeleteLegacyComputeVM(name string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.deleted = append(backend.deleted, name)
	return nil
}

type fakeFirewallBackend struct {
	applyErr error
}

func (*fakeFirewallBackend) RegisterFirewallRule(string, orchestrator.FirewallEntry) {}
func (*fakeFirewallBackend) RemoveFirewallRule(string, string)                       {}
func (backend *fakeFirewallBackend) ApplyFirewallPortsToComputeInstances(
	string,
	string,
	[]orchestrator.ComputeInstanceIdentity,
	[]string,
) error {
	return backend.applyErr
}

type armableComputeStore struct {
	mu      sync.Mutex
	block   bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
	data    []byte
}

func newArmableComputeStore() *armableComputeStore {
	return &armableComputeStore{}
}

func (store *armableComputeStore) arm() {
	store.mu.Lock()
	store.block = true
	store.started = make(chan struct{})
	store.release = make(chan struct{})
	store.once = sync.Once{}
	store.mu.Unlock()
}

func (store *armableComputeStore) Load(_ string, out any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.data, out)
}

func (store *armableComputeStore) Save(_ string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	store.mu.Lock()
	block, started, release := store.block, store.started, store.release
	store.mu.Unlock()
	if block {
		store.once.Do(func() { close(started) })
		<-release
	}
	store.mu.Lock()
	store.data = data
	store.block = false
	store.mu.Unlock()
	return nil
}

var _ computeMetadataStore = (*armableComputeStore)(nil)
