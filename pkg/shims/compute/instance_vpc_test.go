package compute

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestProviderShapedInstanceLifecycleOnBoundedSubnetwork(t *testing.T) {
	api, opMgr := newComputeTestAPI()
	addCustomNetwork(api, "project-a", "custom")
	subnet := persistedSubnetworkForTest(
		"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
	)
	api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] = subnet
	api.nextSubnetworkID = 2
	backend := &fakeComputeNetworkBackend{
		runtime: orchestrator.ComputeInstanceRuntime{
			ContainerID: "container-id",
			Status:      "running",
			NetworkName: "owned-bridge",
			NetworkID:   "bridge-id",
			IPAddress:   "10.42.0.2",
		},
	}
	api.computeNetwork = backend

	create := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{
			"name":"web",
			"machineType":"e2-micro",
			"disks":[{
				"boot":true,
				"autoDelete":true,
				"initializeParams":{"sourceImage":"projects/debian-cloud/global/images/debian-12"}
			}],
			"networkInterfaces":[{"subnetwork":"primary"}]
		}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var operation orchestrator.Operation
	decodeComputeResponse(t, create, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, false)

	get := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", get.Code, get.Body.String())
	}
	var instance Instance
	decodeComputeResponse(t, get, &instance)
	if instance.Status != "RUNNING" || len(instance.NetworkInterfaces) != 1 {
		t.Fatalf("instance=%#v", instance)
	}
	nic := instance.NetworkInterfaces[0]
	if nic.Name != "nic0" || nic.Kind != "compute#networkInterface" ||
		nic.Network != networkSelfLink("project-a", "custom") ||
		nic.Subnetwork != subnet.SelfLink || nic.NetworkIP != "10.42.0.2" ||
		len(nic.AccessConfigs) != 0 {
		t.Fatalf("network interface=%#v", nic)
	}
	if backend.provisionAttachment.VPC.Project != "project-a" ||
		backend.provisionAttachment.VPC.Network != "custom" ||
		backend.provisionAttachment.CIDR != "10.42.0.0/24" {
		t.Fatalf("attachment=%#v", backend.provisionAttachment)
	}

	deleteResponse := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	decodeComputeResponse(t, deleteResponse, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, false)
	missing := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	assertComputeError(t, missing, http.StatusNotFound, "NOT_FOUND")
	if backend.deleteCalls != 1 {
		t.Fatalf("delete calls=%d", backend.deleteCalls)
	}
}

func TestInstanceInsertRejectsUnsupportedNetworkSemanticsBeforeMutation(t *testing.T) {
	tests := []struct {
		name string
		nic  string
	}{
		{name: "NAT access config", nic: `{"subnetwork":"primary","accessConfigs":[{"type":"ONE_TO_ONE_NAT"}]}`},
		{name: "requested static IPv4", nic: `{"subnetwork":"primary","networkIP":"10.42.0.20"}`},
		{name: "IPv6 stack", nic: `{"subnetwork":"primary","stackType":"IPV4_IPV6"}`},
		{name: "IPv6 access config", nic: `{"subnetwork":"primary","ipv6AccessConfigs":[{"type":"DIRECT_IPV6"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api, opMgr := newComputeTestAPI()
			addCustomNetwork(api, "project-a", "custom")
			api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] =
				persistedSubnetworkForTest(
					"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
				)
			api.nextSubnetworkID = 2
			backend := &fakeComputeNetworkBackend{}
			api.computeNetwork = backend
			response := performComputeRequest(api, http.MethodPost,
				"/compute/v1/projects/project-a/zones/us-central1-a/instances",
				`{"name":"web","networkInterfaces":[`+test.nic+`]}`)
			assertComputeError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
			if len(api.instances) != 0 || len(opMgr.List()) != 0 || backend.provisionCalls != 0 {
				t.Fatalf("instances=%#v operations=%#v provisions=%d",
					api.instances, opMgr.List(), backend.provisionCalls)
			}
		})
	}
}

func TestInstanceProvisionFailureRollsBackMetadata(t *testing.T) {
	api, opMgr := newComputeTestAPI()
	addCustomNetwork(api, "project-a", "custom")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] =
		persistedSubnetworkForTest(
			"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
		)
	api.nextSubnetworkID = 2
	api.computeNetwork = &fakeComputeNetworkBackend{provisionErr: errors.New("unowned bridge")}
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"web","networkInterfaces":[{"subnetwork":"primary"}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, true)
	if api.instances[instanceKey("project-a", "us-central1-a", "web")] != nil {
		t.Fatal("failed provisioning left instance metadata")
	}
}

func TestInstanceRunningMetadataSaveFailureCompensatesDockerAndMetadata(t *testing.T) {
	store := &instanceMetadataFailureStore{
		failBefore: map[int]error{4: errors.New("injected final save failure")},
	}
	opMgr := orchestrator.NewOperationManager()
	api, err := newAPIWithMetadataStore(opMgr, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "project-a", "custom")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] =
		persistedSubnetworkForTest(
			"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
		)
	api.nextSubnetworkID = 2
	backend := &fakeComputeNetworkBackend{runtime: orchestrator.ComputeInstanceRuntime{
		Status: "running", IPAddress: "10.42.0.2",
	}}
	api.computeNetwork = backend

	create := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"web","networkInterfaces":[{"subnetwork":"primary"}]}`)
	var operation orchestrator.Operation
	decodeComputeResponse(t, create, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, true)
	key := instanceKey("project-a", "us-central1-a", "web")
	if api.instances[key] != nil || backend.deleteCalls != 1 {
		t.Fatalf("instance=%#v deleteCalls=%d", api.instances[key], backend.deleteCalls)
	}
	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Instances[key] != nil {
		t.Fatalf("persisted instance survived compensation: %#v", persisted.Instances[key])
	}

	retry := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"web","networkInterfaces":[{"subnetwork":"primary"}]}`)
	if retry.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	decodeComputeResponse(t, retry, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, false)
}

func TestInstanceRunningMetadataPostCommitErrorReconcilesAsSuccess(t *testing.T) {
	store := &postCommitComputeStore{
		failAfterCommit: map[int]error{4: errors.New("injected post-commit sync failure")},
	}
	opMgr := orchestrator.NewOperationManager()
	api, err := newAPIWithMetadataStore(opMgr, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "project-a", "custom")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] =
		persistedSubnetworkForTest(
			"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
		)
	api.nextSubnetworkID = 2
	backend := &fakeComputeNetworkBackend{runtime: orchestrator.ComputeInstanceRuntime{
		Status: "running", IPAddress: "10.42.0.2",
	}}
	api.computeNetwork = backend

	create := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"web","networkInterfaces":[{"subnetwork":"primary"}]}`)
	var operation orchestrator.Operation
	decodeComputeResponse(t, create, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, false)
	if backend.deleteCalls != 0 {
		t.Fatalf("confirmed committed instance was deleted %d times", backend.deleteCalls)
	}
	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	instance := persisted.Instances[instanceKey("project-a", "us-central1-a", "web")]
	if instance == nil || instance.Status != "RUNNING" ||
		instance.NetworkInterfaces[0].NetworkIP != "10.42.0.2" {
		t.Fatalf("persisted instance=%#v", instance)
	}
}

func TestInstanceRunningMetadataAndCleanupFailureFailsClosed(t *testing.T) {
	store := &instanceMetadataFailureStore{
		failBefore: map[int]error{
			4: errors.New("injected final save failure"),
		},
	}
	opMgr := orchestrator.NewOperationManager()
	api, err := newAPIWithMetadataStore(opMgr, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "project-a", "custom")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] =
		persistedSubnetworkForTest(
			"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
		)
	api.nextSubnetworkID = 2
	backend := &fakeComputeNetworkBackend{
		runtime: orchestrator.ComputeInstanceRuntime{
			Status: "running", IPAddress: "10.42.0.2",
		},
		deleteErr: errors.New("refusing to delete unowned container"),
	}
	api.computeNetwork = backend

	create := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"web","networkInterfaces":[{"subnetwork":"primary"}]}`)
	var operation orchestrator.Operation
	decodeComputeResponse(t, create, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, true)
	key := instanceKey("project-a", "us-central1-a", "web")
	if api.instances[key] == nil || api.instances[key].Status != "RUNNING" ||
		backend.deleteCalls != 1 || api.initializationError() == nil {
		t.Fatalf("instance=%#v deleteCalls=%d initializationError=%v",
			api.instances[key], backend.deleteCalls, api.initializationError())
	}
	retry := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"web","networkInterfaces":[{"subnetwork":"primary"}]}`)
	assertComputeError(t, retry, http.StatusServiceUnavailable, "FAILED_PRECONDITION")
}

func TestInstanceDeleteTimesOutAndRestoresMetadata(t *testing.T) {
	api, opMgr := newComputeTestAPI()
	api.computeDeleteTimeout = 10 * time.Millisecond
	key := instanceKey("project-a", "us-central1-a", "web")
	instance := &Instance{Name: "web", project: "project-a", zone: "us-central1-a", Status: "RUNNING"}
	api.instances[key] = instance
	backend := &fakeComputeNetworkBackend{waitForDeleteContext: true}
	api.computeNetwork = backend

	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, true)
	if !backend.deleteHadDeadline {
		t.Fatal("Compute deletion context had no deadline")
	}
	if backend.deleteErrObserved != context.DeadlineExceeded {
		t.Fatalf("delete context error=%v, want deadline exceeded", backend.deleteErrObserved)
	}
	if api.instances[key] == nil || api.instances[key].Status != "RUNNING" {
		t.Fatalf("instance was not restored after timeout: %#v", api.instances[key])
	}
}

func TestInstanceDeletePreCommitMetadataFailureRetriesAbsentState(t *testing.T) {
	store := &instanceMetadataFailureStore{
		failBefore: map[int]error{3: errors.New("injected delete save failure")},
	}
	api, opMgr, backend, key := newPersistedInstanceDeleteTestAPI(t, store)

	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, false)
	assertDeletedInstanceState(t, api, store, backend, key)
}

func TestInstanceDeletePostCommitMetadataErrorConfirmsAbsentState(t *testing.T) {
	store := &postCommitComputeStore{
		failAfterCommit: map[int]error{3: errors.New("injected post-commit delete sync failure")},
	}
	api, opMgr, backend, key := newPersistedInstanceDeleteTestAPI(t, store)

	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, false)
	assertDeletedInstanceState(t, api, store, backend, key)
}

func TestInstanceDeleteUnprovableMetadataFailureFailsClosedWithoutZombie(t *testing.T) {
	store := &instanceMetadataFailureStore{
		failBefore: map[int]error{
			3: errors.New("injected delete save failure"),
			4: errors.New("injected delete retry failure"),
		},
	}
	api, opMgr, backend, key := newPersistedInstanceDeleteTestAPI(t, store)

	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	var operation orchestrator.Operation
	decodeComputeResponse(t, response, &operation)
	waitForComputeOperation(t, opMgr, operation.Name, true)
	if api.instances[key] != nil || backend.deleteCalls != 1 || api.initializationError() == nil {
		t.Fatalf("instance=%#v deleteCalls=%d initializationError=%v",
			api.instances[key], backend.deleteCalls, api.initializationError())
	}
	blocked := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	assertComputeError(t, blocked, http.StatusServiceUnavailable, "FAILED_PRECONDITION")

	restarted, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.computeNetwork = &fakeComputeNetworkBackend{}
	if err := restarted.ReconcileComputeInstances(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.instances[key] != nil {
		t.Fatalf("restart retained deletion marker: %#v", restarted.instances[key])
	}
	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Instances[key] != nil {
		t.Fatalf("restart retained persisted zombie: %#v", persisted.Instances[key])
	}
}

func TestReconcileComputeInstancesRestoresExactRuntimeWithoutCreation(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "project-a", "custom")
	subnet := persistedSubnetworkForTest(
		"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
	)
	api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] = subnet
	api.nextSubnetworkID = 2
	key := instanceKey("project-a", "us-central1-a", "web")
	api.instances[key] = &Instance{
		Name: "web", project: "project-a", zone: "us-central1-a", Status: metadataOnlyStatus,
		NetworkInterfaces: []NetworkInterface{{
			Kind: "compute#networkInterface", Name: "nic0",
			Network: subnet.Network, Subnetwork: subnet.SelfLink,
		}},
	}
	backend := &fakeComputeNetworkBackend{
		found: true,
		runtime: orchestrator.ComputeInstanceRuntime{
			Status: "running", IPAddress: "10.42.0.2",
		},
	}
	api.computeNetwork = backend
	if err := api.ReconcileComputeInstances(context.Background()); err != nil {
		t.Fatal(err)
	}
	if api.instances[key].Status != "RUNNING" ||
		api.instances[key].NetworkInterfaces[0].NetworkIP != "10.42.0.2" ||
		backend.reconcileCalls != 1 || backend.provisionCalls != 0 {
		t.Fatalf("instance=%#v backend=%#v", api.instances[key], backend)
	}
}

func newPersistedInstanceDeleteTestAPI(
	t *testing.T,
	store computeMetadataStore,
) (*API, *orchestrator.OperationManager, *fakeComputeNetworkBackend, string) {
	t.Helper()
	opMgr := orchestrator.NewOperationManager()
	api, err := newAPIWithMetadataStore(opMgr, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "project-a", "custom")
	subnet := persistedSubnetworkForTest(
		"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
	)
	api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] = subnet
	api.nextSubnetworkID = 2
	key := instanceKey("project-a", "us-central1-a", "web")
	api.instances[key] = &Instance{
		Name: "web", project: "project-a", zone: "us-central1-a", Status: "RUNNING",
		NetworkInterfaces: []NetworkInterface{{
			Name: "nic0", Network: subnet.Network, Subnetwork: subnet.SelfLink, NetworkIP: "10.42.0.2",
		}},
	}
	backend := &fakeComputeNetworkBackend{}
	api.computeNetwork = backend
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	return api, opMgr, backend, key
}

func assertDeletedInstanceState(
	t *testing.T,
	api *API,
	store computeMetadataStore,
	backend *fakeComputeNetworkBackend,
	key string,
) {
	t.Helper()
	if api.instances[key] != nil || backend.deleteCalls != 1 {
		t.Fatalf("instance=%#v deleteCalls=%d", api.instances[key], backend.deleteCalls)
	}
	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Instances[key] != nil {
		t.Fatalf("persisted zombie instance=%#v", persisted.Instances[key])
	}
	missing := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances/web", "")
	assertComputeError(t, missing, http.StatusNotFound, "NOT_FOUND")
}

type fakeComputeNetworkBackend struct {
	mu                   sync.Mutex
	runtime              orchestrator.ComputeInstanceRuntime
	found                bool
	provisionErr         error
	deleteErr            error
	provisionCalls       int
	reconcileCalls       int
	deleteCalls          int
	provisionAttachment  orchestrator.ComputeInstanceNetwork
	waitForDeleteContext bool
	deleteHadDeadline    bool
	deleteErrObserved    error
}

func (backend *fakeComputeNetworkBackend) ProvisionComputeInstanceOnVPC(
	_ context.Context,
	_ orchestrator.ComputeInstanceIdentity,
	_ string,
	attachment orchestrator.ComputeInstanceNetwork,
	_ []string,
	_ []string,
	_ []string,
) (orchestrator.ComputeInstanceRuntime, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.provisionCalls++
	backend.provisionAttachment = attachment
	return backend.runtime, backend.provisionErr
}

func (backend *fakeComputeNetworkBackend) ReconcileComputeInstanceOnVPC(
	_ context.Context,
	_ orchestrator.ComputeInstanceIdentity,
	_ orchestrator.ComputeInstanceNetwork,
) (orchestrator.ComputeInstanceRuntime, bool, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.reconcileCalls++
	return backend.runtime, backend.found, nil
}

func (backend *fakeComputeNetworkBackend) DeleteComputeInstance(
	ctx context.Context,
	_ orchestrator.ComputeInstanceIdentity,
) error {
	if backend.waitForDeleteContext {
		_, backend.deleteHadDeadline = ctx.Deadline()
		<-ctx.Done()
		backend.deleteErrObserved = ctx.Err()
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.deleteCalls++
	if backend.waitForDeleteContext {
		return backend.deleteErrObserved
	}
	return backend.deleteErr
}

type instanceMetadataFailureStore struct {
	mu         sync.Mutex
	data       []byte
	saveCount  int
	failBefore map[int]error
}

func (store *instanceMetadataFailureStore) Load(_ string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.data, target)
}

func (store *instanceMetadataFailureStore) Save(_ string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveCount++
	if err := store.failBefore[store.saveCount]; err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err == nil {
		store.data = data
	}
	return err
}

func waitForComputeOperation(
	t *testing.T,
	manager *orchestrator.OperationManager,
	name string,
	wantError bool,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		operation := manager.Get(name)
		if operation != nil && operation.Done {
			if (operation.Error != nil) != wantError {
				t.Fatalf("operation=%#v wantError=%t", operation, wantError)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("operation %q did not complete", name)
}
