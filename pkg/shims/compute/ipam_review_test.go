package compute

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestResolveInstanceNetworkInterfaces(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "project-a", "custom")
	addCustomNetwork(api, "project-a", "empty")
	subnet := &Subnetwork{
		Kind: "compute#subnetwork", ID: "1", Name: "subnet-a", IPCidrRange: "10.0.0.0/24",
		Network:        networkSelfLink("project-a", "custom"),
		Region:         regionSelfLink("project-a", "us-central1"),
		SelfLink:       subnetworkSelfLink("project-a", "us-central1", "subnet-a"),
		GatewayAddress: "10.0.0.1", Fingerprint: "fingerprint", Purpose: "PRIVATE",
		StackType: "IPV4_ONLY", State: "READY", CreationTimestamp: "2026-01-01T00:00:00Z",
	}
	api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = subnet

	tests := []struct {
		name    string
		project string
		zone    string
		input   NetworkInterface
		wantErr bool
	}{
		{name: "subnetwork only", project: "project-a", zone: "us-central1-a", input: NetworkInterface{Subnetwork: "subnet-a"}},
		{name: "provider relative subnetwork ID", project: "project-a", zone: "us-central1-a", input: NetworkInterface{Subnetwork: "projects/project-a/regions/us-central1/subnetworks/subnet-a"}},
		{name: "network only inference", project: "project-a", zone: "us-central1-a", input: NetworkInterface{Network: "custom"}},
		{name: "custom without subnet", project: "project-a", zone: "us-central1-a", input: NetworkInterface{Network: "empty"}, wantErr: true},
		{name: "both agree", project: "project-a", zone: "us-central1-a", input: NetworkInterface{Network: networkSelfLink("project-a", "custom"), Subnetwork: subnet.SelfLink}},
		{name: "mismatch", project: "project-a", zone: "us-central1-a", input: NetworkInterface{Network: "other", Subnetwork: "subnet-a"}, wantErr: true},
		{name: "cross project", project: "project-b", zone: "us-central1-a", input: NetworkInterface{Subnetwork: subnet.SelfLink}, wantErr: true},
		{name: "cross region", project: "project-a", zone: "europe-west1-b", input: NetworkInterface{Subnetwork: subnet.SelfLink}, wantErr: true},
		{name: "missing parent", project: "project-a", zone: "us-central1-a", input: NetworkInterface{Network: "missing"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := api.resolveInstanceNetworkInterfaces(test.project, test.zone, []NetworkInterface{test.input})
			if test.wantErr {
				if err == nil {
					t.Fatalf("resolution succeeded: %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got[0].Network != subnet.Network || got[0].Subnetwork != subnet.SelfLink {
				t.Fatalf("resolved interface=%#v", got[0])
			}
		})
	}
	defaulted, err := api.resolveInstanceNetworkInterfaces("project-a", "us-central1-a", nil)
	if err != nil || defaulted[0].Subnetwork != "" ||
		defaulted[0].Network != networkSelfLink("project-a", "default") {
		t.Fatalf("default interface=%#v error=%v", defaulted, err)
	}
	logical, dockerName, err := resolvedInstanceVPCDockerNetwork("project-a", []NetworkInterface{{
		Network: subnet.Network,
	}})
	wantDocker, _ := (orchestrator.VPCNetworkIdentity{Project: "project-a", Network: "custom"}).DockerName()
	if err != nil || logical != networkSelfLink("project-a", "custom") || dockerName != wantDocker {
		t.Fatalf("bridge selection logical=%q docker=%q error=%v", logical, dockerName, err)
	}
}

func TestComputeContainerNamesSeparateScopeAndPreserveGKE(t *testing.T) {
	first := &Instance{Name: "shared", project: "project-a", zone: "us-central1-a"}
	second := &Instance{Name: "shared", project: "project-b", zone: "us-central1-a"}
	third := &Instance{Name: "shared", project: "project-a", zone: "us-central1-b"}
	names := map[string]bool{}
	for _, instance := range []*Instance{first, second, third} {
		name, err := computeInstanceContainerName(instance.project, instance.zone, instance)
		if err != nil {
			t.Fatal(err)
		}
		if names[name] {
			t.Fatalf("container name collision %q", name)
		}
		names[name] = true
	}
	gke := &Instance{Name: "kind-control-plane", Labels: map[string]string{"managed-by": "gke"}}
	name, err := computeInstanceContainerName("project-a", "us-central1-a", gke)
	if err != nil || name != gke.Name {
		t.Fatalf("GKE container name=%q error=%v", name, err)
	}
}

func TestResolveInstanceNetworkInterfacesRejectsLegacyAutoMode(t *testing.T) {
	api, _ := newComputeTestAPI()
	api.networks["project-a:auto"] = &Network{Name: "auto", AutoCreateSubnetworks: true}
	if _, err := api.resolveInstanceNetworkInterfaces(
		"project-a", "us-central1-a", []NetworkInterface{{Network: "auto"}},
	); err == nil {
		t.Fatal("legacy auto-mode network resolved to a Docker attachment")
	}
}

func TestInstanceInsertResolvesInterfaceBeforeMetadataCommit(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "project-a", "custom")
	subnet := persistedSubnetworkForTest(
		"project-a", "us-central1", "subnet-a", "custom", "10.0.0.0/24", "1",
	)
	api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = subnet
	api.nextSubnetworkID = 2
	base := "/compute/v1/projects/project-a/zones/us-central1-a/instances"
	created := performComputeRequest(api, http.MethodPost, base, `{
		"name":"vm",
		"labels":{"managed-by":"gke"},
		"networkInterfaces":[{"subnetwork":"subnet-a"}]
	}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	instance := api.instances[instanceKey("project-a", "us-central1-a", "vm")]
	if instance == nil || instance.NetworkInterfaces[0].Network != subnet.Network ||
		instance.NetworkInterfaces[0].Subnetwork != subnet.SelfLink {
		t.Fatalf("committed instance=%#v", instance)
	}
	rejected := performComputeRequest(api, http.MethodPost, base, `{
		"name":"bad",
		"labels":{"managed-by":"gke"},
		"networkInterfaces":[{"network":"default","subnetwork":"subnet-a"}]
	}`)
	assertComputeError(t, rejected, http.StatusBadRequest, "INVALID_ARGUMENT")
	if api.instances[instanceKey("project-a", "us-central1-a", "bad")] != nil {
		t.Fatal("invalid interface was committed")
	}
}

func TestNetworkInsertRejectsAutoModeBeforeMetadata(t *testing.T) {
	api, _ := newComputeTestAPI()
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/networks",
		`{"name":"auto","autoCreateSubnetworks":true}`)
	assertComputeError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
	if api.networks["project-a:auto"] != nil {
		t.Fatal("unsupported auto-mode network was committed")
	}
}

func TestPersistedSubnetworkInvariantValidationPrecedesReconcile(t *testing.T) {
	validNetwork := &Network{
		Kind: "compute#network", ID: "1", Name: "custom", SelfLink: networkSelfLink("project-a", "custom"),
		AutoCreateSubnetworks: false, CreationTimestamp: "2026-01-01T00:00:00Z",
	}
	validSubnet := func(name, cidr string) *Subnetwork {
		subnet := &Subnetwork{
			Kind: "compute#subnetwork", ID: "1", Name: name, IPCidrRange: cidr,
			Network: validNetwork.SelfLink, Region: regionSelfLink("project-a", "us-central1"),
			SelfLink:       subnetworkSelfLink("project-a", "us-central1", name),
			GatewayAddress: "10.0.0.1", Purpose: "PRIVATE", StackType: "IPV4_ONLY", State: "READY",
			CreationTimestamp: "2026-01-01T00:00:00Z",
		}
		subnet.Fingerprint = subnetworkFingerprint(subnet)
		return subnet
	}
	tests := []struct {
		name   string
		mutate func(*API)
	}{
		{name: "key mismatch", mutate: func(api *API) {
			api.subnetworks["project-a:us-central1:wrong"] = validSubnet("subnet-a", "10.0.0.0/24")
		}},
		{name: "cross project embedded URL", mutate: func(api *API) {
			subnet := validSubnet("subnet-a", "10.0.0.0/24")
			subnet.Network = networkSelfLink("project-b", "custom")
			subnet.Fingerprint = subnetworkFingerprint(subnet)
			api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = subnet
		}},
		{name: "missing parent", mutate: func(api *API) {
			api.networks = map[string]*Network{}
			api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = validSubnet("subnet-a", "10.0.0.0/24")
		}},
		{name: "duplicate parent", mutate: func(api *API) {
			api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = validSubnet("subnet-a", "10.0.0.0/24")
			second := validSubnet("subnet-b", "10.1.0.0/24")
			second.ID = "2"
			second.GatewayAddress = "10.1.0.1"
			api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-b")] = second
		}},
		{name: "overlap", mutate: func(api *API) {
			api.networks["project-a:other"] = &Network{
				Kind: "compute#network", ID: "2", Name: "other",
				SelfLink: networkSelfLink("project-a", "other"),
			}
			first := validSubnet("subnet-a", "10.0.0.0/24")
			second := validSubnet("subnet-b", "10.0.0.128/25")
			second.ID = "2"
			second.GatewayAddress = "10.0.0.129"
			second.Network = networkSelfLink("project-a", "other")
			second.SelfLink = subnetworkSelfLink("project-a", "us-central1", "subnet-b")
			second.Fingerprint = subnetworkFingerprint(second)
			api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] = first
			api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-b")] = second
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &toggleComputeStore{}
			seed := newAPI(orchestrator.NewOperationManager(), nil, store)
			seed.networks["project-a:custom"] = validNetwork
			test.mutate(seed)
			seed.nextSubnetworkID = uint64(len(seed.subnetworks) + 1)
			if err := seed.persistMetadata(); err != nil {
				t.Fatal(err)
			}
			api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
			if err != nil {
				t.Fatal(err)
			}
			if len(api.subnetworks) != len(seed.subnetworks) || api.initializationError() == nil {
				t.Fatalf("invalid loaded state was not retained fail-closed: %#v", api.subnetworks)
			}
			backend := useFakeVPCIPAM(api)
			err = api.ReconcileVPCIPAM(context.Background())
			if err == nil || backend.ensureCalls != 0 || api.initializationError() == nil {
				t.Fatalf("error=%v ensureCalls=%d init=%v", err, backend.ensureCalls, api.initializationError())
			}
			assertComputeError(t, performComputeRequest(api, http.MethodGet,
				"/compute/v1/projects/project-a/regions/us-central1/subnetworks/subnet-a", ""),
				http.StatusServiceUnavailable, "FAILED_PRECONDITION")
		})
	}
}

func TestValidPersistedSubnetworksReconcileInSortedKeyOrder(t *testing.T) {
	api := newAPI(orchestrator.NewOperationManager(), nil, nil)
	for _, network := range []string{"network-a", "network-b"} {
		addCustomNetwork(api, "project-a", network)
	}
	api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-b")] =
		persistedSubnetworkForTest("project-a", "us-central1", "subnet-b", "network-b", "10.1.0.0/24", "2")
	api.subnetworks[subnetworkKey("project-a", "us-central1", "subnet-a")] =
		persistedSubnetworkForTest("project-a", "us-central1", "subnet-a", "network-a", "10.0.0.0/24", "1")
	api.nextSubnetworkID = 3
	backend := useFakeVPCIPAM(api)
	if err := api.ReconcileVPCIPAM(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(backend.ensureOrder) != 2 ||
		backend.ensureOrder[0].Network != "network-a" ||
		backend.ensureOrder[1].Network != "network-b" {
		t.Fatalf("reconcile order=%#v", backend.ensureOrder)
	}
}

func TestDeleteCompensationFailureFailsClosed(t *testing.T) {
	store := &toggleComputeStore{}
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "test-project", "custom")
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	if response := performComputeRequest(api, http.MethodPost, base,
		`{"name":"kept","ipCidrRange":"10.0.0.0/24","network":"custom"}`); response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	store.setFail(true)
	backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
	backend.ensureErrAfter = backend.ensureCalls
	backend.ensureErr = errors.New("recreate failed")
	response := performComputeRequest(api, http.MethodDelete, base+"/kept", "")
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if api.initializationError() == nil {
		t.Fatal("failed compensation did not fail closed")
	}
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base+"/kept", ""),
		http.StatusServiceUnavailable, "FAILED_PRECONDITION")
}
