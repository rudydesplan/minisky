package compute

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestLegacyVPCNetworkCleanupRequiresUniqueMetadataName(t *testing.T) {
	api, _ := newComputeTestAPI()
	backend := &fakeLegacyVPCBackend{}
	api.legacyVPC = backend
	addCustomNetwork(api, "project-a", "shared")
	addCustomNetwork(api, "project-b", "shared")

	first := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/global/networks/shared", "")
	if first.Code != http.StatusOK {
		t.Fatalf("first delete status=%d body=%s", first.Code, first.Body.String())
	}
	if len(backend.deleted) != 0 {
		t.Fatalf("ambiguous legacy cleanup calls=%#v", backend.deleted)
	}
	second := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-b/global/networks/shared", "")
	if second.Code != http.StatusOK {
		t.Fatalf("final delete status=%d body=%s", second.Code, second.Body.String())
	}
	if len(backend.deleted) != 1 || backend.deleted[0] != "shared" {
		t.Fatalf("final legacy cleanup calls=%#v", backend.deleted)
	}
}

func TestLegacyVPCNetworkCleanupFailurePreservesMetadata(t *testing.T) {
	api, _ := newComputeTestAPI()
	backend := &fakeLegacyVPCBackend{err: errors.New("network has active endpoints")}
	api.legacyVPC = backend
	addCustomNetwork(api, "project-a", "shared")
	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/global/networks/shared", "")
	assertComputeError(t, response, http.StatusBadRequest, "FAILED_PRECONDITION")
	if api.networks["project-a:shared"] == nil || len(backend.deleted) != 1 {
		t.Fatalf("network=%#v cleanup=%#v", api.networks, backend.deleted)
	}
}

func TestNetworkDeleteRejectsExactInstanceParentReference(t *testing.T) {
	api, _ := newComputeTestAPI()
	backend := &fakeLegacyVPCBackend{}
	api.legacyVPC = backend
	addCustomNetwork(api, "project-a", "shared")
	api.instances[instanceKey("project-a", "us-central1-a", "vm")] = &Instance{
		Name: "vm", Status: metadataOnlyStatus,
		NetworkInterfaces: []NetworkInterface{{Network: networkSelfLink("project-a", "shared")}},
	}
	response := performComputeRequest(api, http.MethodDelete,
		"/compute/v1/projects/project-a/global/networks/shared", "")
	assertComputeError(t, response, http.StatusBadRequest, "FAILED_PRECONDITION")
	if len(backend.deleted) != 0 || api.networks["project-a:shared"] == nil {
		t.Fatalf("cleanup=%#v networks=%#v", backend.deleted, api.networks)
	}
}

func TestPersistedSpecialUseCIDRFailsBeforeDocker(t *testing.T) {
	store := &toggleComputeStore{}
	seed := newAPI(orchestrator.NewOperationManager(), nil, store)
	addCustomNetwork(seed, "project-a", "custom")
	subnet := persistedSubnetworkForTest(
		"project-a", "us-central1", "bad", "custom", "169.0.0.0/8", "1",
	)
	seed.subnetworks[subnetworkKey("project-a", "us-central1", "bad")] = subnet
	seed.nextSubnetworkID = 2
	if err := seed.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	backend := useFakeVPCIPAM(api)
	if err := api.ReconcileVPCIPAM(context.Background()); err == nil ||
		backend.ensureCalls != 0 || api.initializationError() == nil {
		t.Fatalf("error=%v ensureCalls=%d init=%v",
			err, backend.ensureCalls, api.initializationError())
	}
}

func TestInstanceInsertRejectsMultipleNetworkInterfacesBeforeMutation(t *testing.T) {
	api, opMgr := newComputeTestAPI()
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/zones/us-central1-a/instances",
		`{"name":"multi-nic","networkInterfaces":[
			{"network":"default"},
			{"network":"default"}
		]}`)
	assertComputeError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
	if len(api.instances) != 0 || len(opMgr.List()) != 0 {
		t.Fatalf("instances=%#v operations=%#v", api.instances, opMgr.List())
	}
	backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
	if backend.ensureCalls != 0 || backend.deleteCalls != 0 {
		t.Fatalf("multi-NIC request mutated Docker backend: ensure=%d delete=%d",
			backend.ensureCalls, backend.deleteCalls)
	}
}

func TestLegacyMultipleNICsBlockEveryReferencedDeletion(t *testing.T) {
	t.Run("subnetwork on secondary interface", func(t *testing.T) {
		api, _ := newComputeTestAPI()
		addCustomNetwork(api, "project-a", "custom")
		subnet := persistedSubnetworkForTest(
			"project-a", "us-central1", "subnet-a", "custom", "10.0.0.0/24", "1",
		)
		api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = subnet
		api.nextSubnetworkID = 2
		api.instances[instanceKey("project-a", "us-central1-a", "legacy")] = &Instance{
			Name: "legacy", Status: metadataOnlyStatus,
			NetworkInterfaces: []NetworkInterface{
				{Network: networkSelfLink("project-a", "default")},
				{Network: subnet.Network, Subnetwork: subnet.SelfLink},
			},
		}
		response := performComputeRequest(api, http.MethodDelete,
			"/compute/v1/projects/project-a/regions/us-central1/subnetworks/subnet-a", "")
		assertComputeError(t, response, http.StatusBadRequest, "FAILED_PRECONDITION")
		if api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] == nil {
			t.Fatal("secondary interface reference did not preserve subnetwork")
		}
	})

	t.Run("network on secondary interface", func(t *testing.T) {
		api, _ := newComputeTestAPI()
		addCustomNetwork(api, "project-a", "custom")
		api.instances[instanceKey("project-a", "us-central1-a", "legacy")] = &Instance{
			Name: "legacy", Status: metadataOnlyStatus,
			NetworkInterfaces: []NetworkInterface{
				{Network: networkSelfLink("project-a", "default")},
				{Network: networkSelfLink("project-a", "custom")},
			},
		}
		response := performComputeRequest(api, http.MethodDelete,
			"/compute/v1/projects/project-a/global/networks/custom", "")
		assertComputeError(t, response, http.StatusBadRequest, "FAILED_PRECONDITION")
		if api.networks["project-a:custom"] == nil {
			t.Fatal("secondary interface reference did not preserve network")
		}
	})
}

type fakeLegacyVPCBackend struct {
	deleted []string
	err     error
}

func (backend *fakeLegacyVPCBackend) DeleteLegacyVPCNetwork(_ context.Context, network string) error {
	backend.deleted = append(backend.deleted, network)
	return backend.err
}
