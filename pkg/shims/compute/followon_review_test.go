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
		current := opMgr.Get(operation.Name)
		if current != nil && current.Done {
			if current.Error == nil {
				t.Fatalf("firewall operation succeeded despite recreation failure: %#v", current)
			}
			if api.firewalls["project-a:allow-app"] != nil {
				t.Fatal("failed firewall operation did not restore live metadata")
			}
			assertComputeError(t, performComputeRequest(api, http.MethodGet,
				"/compute/v1/projects/project-a/global/firewalls", ""),
				http.StatusServiceUnavailable, "FAILED_PRECONDITION")
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("firewall operation did not finish")
}

func TestFirewallBackendFailureRollsBackLiveAndPersistedMetadata(t *testing.T) {
	store := &toggleComputeStore{}
	opMgr := orchestrator.NewOperationManager()
	api, err := newTestAPIWithMetadataStore(opMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeFirewallBackend{
		applyErrors: []error{errors.New("apply failed"), nil},
	}
	api.firewall = backend
	api.instances[instanceKey("project-a", "us-central1-a", "vm")] = &Instance{
		Name: "vm", project: "project-a", zone: "us-central1-a",
		NetworkInterfaces: []NetworkInterface{{Network: networkSelfLink("project-a", "custom")}},
	}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"allow-app","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	waitForFirewallOperation(t, opMgr, operation.Name)

	if api.firewalls["project-a:allow-app"] != nil {
		t.Fatal("failed backend operation left live firewall metadata")
	}
	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Firewalls["project-a:allow-app"] != nil {
		t.Fatal("failed backend operation left persisted firewall metadata")
	}
	if api.initializationError() != nil {
		t.Fatalf("successful compensation degraded API: %v", api.initializationError())
	}
}

func TestFirewallMutationsLeaveMetadataAndBackendUnchangedWhenSaveFails(t *testing.T) {
	store := &toggleComputeStore{}
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeFirewallBackend{}
	api.firewall = backend
	base := "/compute/v1/projects/project-a/global/firewalls"
	body := `{"name":"allow-app","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`

	store.setFail(true)
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base, body),
		http.StatusInternalServerError, "INTERNAL")
	if len(api.firewalls) != 0 || backend.registerCount() != 0 || backend.removeCount() != 0 {
		t.Fatalf("failed create changed live/backend state: firewalls=%#v register=%d remove=%d",
			api.firewalls, backend.registerCount(), backend.removeCount())
	}
	operations := api.opMgr.List()
	if len(operations) != 1 || operations[0].Error == nil ||
		!strings.Contains(operations[0].Error.Message, "injected save failure") {
		t.Fatalf("failed create operation is not diagnosable: %#v", operations)
	}

	store.setFail(false)
	if response := performComputeRequest(api, http.MethodPost, base, body); response.Code != http.StatusOK {
		t.Fatalf("seed create status=%d body=%s", response.Code, response.Body.String())
	}
	var baseline computeMetadata
	if err := store.Load(computeStateEntry, &baseline); err != nil {
		t.Fatal(err)
	}
	store.setFail(true)
	key := "project-a:allow-app"
	before := cloneFirewallRule(api.firewalls[key])

	assertComputeError(t, performComputeRequest(api, http.MethodPatch, base+"/allow-app",
		`{"description":"must not stick","allowed":[{"IPProtocol":"tcp","ports":["9090"]}]}`),
		http.StatusInternalServerError, "INTERNAL")
	if got := api.firewalls[key]; got.Description != before.Description ||
		got.Allowed[0].Ports[0] != before.Allowed[0].Ports[0] ||
		backend.removeCount() != 0 {
		t.Fatalf("failed patch changed live/backend state: got=%#v before=%#v removes=%d",
			got, before, backend.removeCount())
	}

	assertComputeError(t, performComputeRequest(api, http.MethodDelete, base+"/allow-app", ""),
		http.StatusInternalServerError, "INTERNAL")
	if api.firewalls[key] == nil || backend.removeCount() != 0 {
		t.Fatal("failed delete changed live or backend state")
	}
	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Firewalls[key]; got == nil || got.Description != baseline.Firewalls[key].Description ||
		got.Allowed[0].Ports[0] != baseline.Firewalls[key].Allowed[0].Ports[0] {
		t.Fatalf("failed mutations changed persisted firewall: got=%#v baseline=%#v", got, baseline.Firewalls[key])
	}
}

func TestFirewallOperationRegistrationPrecedesMutationAndBackendWork(t *testing.T) {
	operationStore := &failingFirewallOperationStore{
		failOnSave: map[int]error{1: errors.New("injected operation save failure")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &toggleComputeStore{}
	api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeFirewallBackend{}
	api.firewall = backend
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"blocked","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if len(api.firewalls) != 0 || backend.registerCount() != 0 || metadataStore.saveCount() != 0 {
		t.Fatalf("operation failure happened after side effects: firewalls=%#v register=%d metadata saves=%d",
			api.firewalls, backend.registerCount(), metadataStore.saveCount())
	}
}

func TestFirewallOperationCompensationFailureFailsClosedWithCausalDetails(t *testing.T) {
	operationStore := &failingFirewallOperationStore{
		failOnSave: map[int]error{2: errors.New("failed-operation save failure")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &toggleComputeStore{fail: true}
	api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	api.firewall = &fakeFirewallBackend{}

	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"blocked","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	degraded := api.initializationError()
	if degraded == nil || !strings.Contains(degraded.Error(), "injected save failure") ||
		!strings.Contains(degraded.Error(), "failed-operation save failure") {
		t.Fatalf("degraded error lost causal details: %v", degraded)
	}
	blocked := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/global/firewalls", "")
	assertComputeError(t, blocked, http.StatusServiceUnavailable, "FAILED_PRECONDITION")
}

func TestSynchronousFirewallFailureReconcilesTerminalOperationPostCommitError(t *testing.T) {
	operationStore := &failingFirewallOperationStore{
		failAfterCommit: map[int]error{2: errors.New("terminal failure sync error")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &toggleComputeStore{fail: true}
	api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	api.firewall = &fakeFirewallBackend{}

	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"sync-failure","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	operations := opMgr.List()
	if len(operations) != 1 {
		t.Fatalf("operations = %#v", operations)
	}
	inProcess := operations[0]
	if !inProcess.Done || inProcess.Error == nil {
		t.Fatalf("in-process terminal operation = %#v", inProcess)
	}
	if degraded := api.initializationError(); degraded == nil ||
		!strings.Contains(degraded.Error(), "terminal failure sync error") {
		t.Fatalf("synchronous terminal failure did not degrade Compute: %v", degraded)
	}
	poll := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/global/operations/"+inProcess.Name, "")
	if poll.Code != http.StatusOK {
		t.Fatalf("poll status=%d body=%s", poll.Code, poll.Body.String())
	}

	restarted, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	durable := restarted.Get(inProcess.Name)
	if durable == nil || !durable.Done || durable.Error == nil ||
		durable.Error.Message != inProcess.Error.Message {
		t.Fatalf("terminal polling diverged across restart: in-process=%#v restarted=%#v", inProcess, durable)
	}
}

func TestSynchronousFirewallFailureReconcilesUncommittedTerminalOperation(t *testing.T) {
	operationStore := &failingFirewallOperationStore{
		failOnSave: map[int]error{2: errors.New("terminal failure before commit")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	api, err := newTestAPIWithMetadataStore(opMgr, &toggleComputeStore{fail: true})
	if err != nil {
		t.Fatal(err)
	}
	api.firewall = &fakeFirewallBackend{}

	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"sync-failure","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	inProcess := opMgr.List()[0]
	if !inProcess.Done || inProcess.Error == nil ||
		!strings.Contains(inProcess.Error.Message, "interrupted by MiniSky restart") {
		t.Fatalf("in-process terminal operation = %#v", inProcess)
	}

	restarted, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	durable := restarted.Get(inProcess.Name)
	if durable == nil || !durable.Done || durable.Error == nil ||
		durable.Error.Message != inProcess.Error.Message {
		t.Fatalf("terminal polling diverged across restart: in-process=%#v restarted=%#v", inProcess, durable)
	}
}

func TestFirewallCreateReconcilesPostCommitMetadataSaveError(t *testing.T) {
	store := &postCommitComputeStore{failAfterCommit: map[int]error{
		1: errors.New("post-rename metadata sync failure"),
	}}
	opMgr := orchestrator.NewOperationManager()
	api, err := newTestAPIWithMetadataStore(opMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	api.firewall = &fakeFirewallBackend{}
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"committed","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	if api.firewalls["project-a:committed"] == nil {
		t.Fatal("post-commit metadata was not reconciled into live state")
	}
	restarted, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.firewalls["project-a:committed"] == nil {
		t.Fatal("post-commit metadata did not survive restart")
	}
}

func TestFirewallAmbiguousPostCommitReadbackFailureFailsClosed(t *testing.T) {
	store := &postCommitComputeStore{failAfterCommit: map[int]error{
		1: errors.New("post-rename metadata sync failure"),
	}}
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	api.firewall = &fakeFirewallBackend{}
	store.loadErr = errors.New("Compute readback unavailable")
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"ambiguous","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if degraded := api.initializationError(); degraded == nil ||
		!strings.Contains(degraded.Error(), "Compute readback unavailable") {
		t.Fatalf("ambiguous save did not degrade Compute: %v", degraded)
	}
	blocked := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/global/firewalls", "")
	assertComputeError(t, blocked, http.StatusServiceUnavailable, "FAILED_PRECONDITION")
}

func TestFirewallTerminalOperationPostCommitFailureDegradesButPollsTruthfully(t *testing.T) {
	operationStore := &failingFirewallOperationStore{
		failAfterCommit: map[int]error{2: errors.New("terminal operation sync failure")},
	}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	api := newAPI(opMgr, nil, nil)
	api.firewall = &fakeFirewallBackend{}
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/firewalls",
		`{"name":"terminal","network":"https://www.googleapis.com/compute/v1/projects/project-a/global/networks/custom","allowed":[{"IPProtocol":"tcp","ports":["8080"]}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	waitForFirewallTerminal(t, opMgr, operation.Name)
	deadline := time.Now().Add(time.Second)
	for api.initializationError() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if degraded := api.initializationError(); degraded == nil ||
		!strings.Contains(degraded.Error(), "terminal operation sync failure") {
		t.Fatalf("terminal operation failure did not degrade Compute: %v", degraded)
	}
	poll := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/global/operations/"+operation.Name, "")
	if poll.Code != http.StatusOK {
		t.Fatalf("degraded operation poll status=%d body=%s", poll.Code, poll.Body.String())
	}
	var current orchestrator.Operation
	decodeComputeResponse(t, poll, &current)
	if !current.Done || current.Error != nil {
		t.Fatalf("poll did not reflect committed terminal truth: %#v", current)
	}
	restarted, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	if durable := restarted.Get(operation.Name); durable == nil || !durable.Done || durable.Error != nil {
		t.Fatalf("restart operation diverged from poll: %#v", durable)
	}
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
	mu          sync.Mutex
	applyErr    error
	applyErrors []error
	applyCalls  int
	register    int
	remove      int
}

func (backend *fakeFirewallBackend) RegisterFirewallRule(string, orchestrator.FirewallEntry) {
	backend.mu.Lock()
	backend.register++
	backend.mu.Unlock()
}
func (backend *fakeFirewallBackend) RemoveFirewallRule(string, string) {
	backend.mu.Lock()
	backend.remove++
	backend.mu.Unlock()
}
func (backend *fakeFirewallBackend) ApplyFirewallPortsToComputeInstances(
	string,
	string,
	[]orchestrator.ComputeInstanceIdentity,
	[]string,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.applyCalls < len(backend.applyErrors) {
		err := backend.applyErrors[backend.applyCalls]
		backend.applyCalls++
		return err
	}
	backend.applyCalls++
	return backend.applyErr
}

func (backend *fakeFirewallBackend) registerCount() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.register
}

func (backend *fakeFirewallBackend) removeCount() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.remove
}

func waitForFirewallOperation(t *testing.T, manager *orchestrator.OperationManager, name string) *orchestrator.Operation {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if operation := manager.Get(name); operation != nil && operation.Done {
			if operation.Error == nil {
				t.Fatalf("operation %q unexpectedly succeeded", name)
			}
			return operation
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("operation %q did not finish", name)
	return nil
}

func waitForFirewallTerminal(t *testing.T, manager *orchestrator.OperationManager, name string) *orchestrator.Operation {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if operation := manager.Get(name); operation != nil && operation.Done {
			return operation
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("operation %q did not finish", name)
	return nil
}

type failingFirewallOperationStore struct {
	mu              sync.Mutex
	data            []byte
	saveCount       int
	failOnSave      map[int]error
	failAfterCommit map[int]error
}

func (store *failingFirewallOperationStore) Load(_ string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.data, target)
}

func (store *failingFirewallOperationStore) Save(_ string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveCount++
	if err := store.failOnSave[store.saveCount]; err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err == nil {
		store.data = data
	}
	if err == nil {
		if postCommitErr := store.failAfterCommit[store.saveCount]; postCommitErr != nil {
			return postCommitErr
		}
	}
	return err
}

type postCommitComputeStore struct {
	mu              sync.Mutex
	data            []byte
	saveCount       int
	failAfterCommit map[int]error
	loadErr         error
}

func (store *postCommitComputeStore) Load(_ string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.loadErr != nil {
		return store.loadErr
	}
	if len(store.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.data, target)
}

func (store *postCommitComputeStore) Save(_ string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveCount++
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	store.data = data
	return store.failAfterCommit[store.saveCount]
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
