package compute

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"

	"minisky/pkg/orchestrator"
	"minisky/pkg/router"
	"minisky/pkg/state"
)

func TestSubnetworkGeneratedClientLifecycleAndPagination(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "test-project", "network-b")
	addCustomNetwork(api, "test-project", "network-a")

	gateway := router.NewProxyRouterWithManager(nil)
	gateway.RegisterShim("compute.googleapis.com", api)
	server := httptest.NewServer(gateway)
	defer server.Close()
	client, err := compute.NewService(
		context.Background(),
		option.WithEndpoint(server.URL+"/_minisky/compute/compute/v1/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, candidate := range []*compute.Subnetwork{
		{Name: "subnet-b", Description: "second", IpCidrRange: "10.20.0.0/24", Network: "network-b"},
		{Name: "subnet-a", IpCidrRange: "10.10.0.0/24", Network: networkSelfLink("test-project", "network-a")},
	} {
		op, err := client.Subnetworks.Insert("test-project", "us-central1", candidate).Do()
		if err != nil {
			t.Fatalf("insert %s: %v", candidate.Name, err)
		}
		if op.Kind != "compute#operation" || op.OperationType != "insert" ||
			op.Region != regionSelfLink("test-project", "us-central1") ||
			op.TargetLink != subnetworkSelfLink("test-project", "us-central1", candidate.Name) {
			t.Fatalf("insert operation = %#v", op)
		}
		waitForRegionalOperation(t, client, "test-project", "us-central1", op.Name)
	}

	first, err := client.Subnetworks.List("test-project", "us-central1").MaxResults(1).Do()
	if err != nil {
		t.Fatal(err)
	}
	if first.Kind != "compute#subnetworkList" || len(first.Items) != 1 ||
		first.Items[0].Name != "subnet-a" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v", first)
	}
	if _, err := client.Subnetworks.Delete("test-project", "us-central1", "subnet-a").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Subnetworks.Insert("test-project", "us-central1", &compute.Subnetwork{
		Name: "subnet-0", IpCidrRange: "10.5.0.0/24", Network: "network-a",
	}).Do(); err != nil {
		t.Fatal(err)
	}
	second, err := client.Subnetworks.List("test-project", "us-central1").
		MaxResults(1).PageToken(first.NextPageToken).Do()
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Items[0].Name != "subnet-b" || second.NextPageToken != "" {
		t.Fatalf("second page = %#v", second)
	}

	got, err := client.Subnetworks.Get("test-project", "us-central1", "subnet-b").Do()
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "compute#subnetwork" || got.Id == 0 || got.Name != "subnet-b" ||
		got.IpCidrRange != "10.20.0.0/24" || got.Network != networkSelfLink("test-project", "network-b") ||
		got.Region != regionSelfLink("test-project", "us-central1") ||
		got.SelfLink != subnetworkSelfLink("test-project", "us-central1", "subnet-b") ||
		got.GatewayAddress != "10.20.0.1" || got.Fingerprint == "" ||
		got.Purpose != "PRIVATE" || got.StackType != "IPV4_ONLY" || got.State != "READY" ||
		got.PrivateIpGoogleAccess || got.CreationTimestamp == "" {
		t.Fatalf("subnetwork = %#v", got)
	}

	deleteOp, err := client.Subnetworks.Delete("test-project", "us-central1", "subnet-b").Do()
	if err != nil {
		t.Fatal(err)
	}
	if deleteOp.OperationType != "delete" ||
		deleteOp.TargetLink != subnetworkSelfLink("test-project", "us-central1", "subnet-b") {
		t.Fatalf("delete operation = %#v", deleteOp)
	}
	waitForRegionalOperation(t, client, "test-project", "us-central1", deleteOp.Name)
	if _, err := client.Subnetworks.Get("test-project", "us-central1", "subnet-b").Do(); err == nil {
		t.Fatal("deleted subnetwork still exists")
	}
}

func TestSubnetworkAcceptsGoogleProviderDefaultCreateShape(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "test-project", "custom")
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"

	response := performComputeRequest(api, http.MethodPost, base, `{
		"name":"provider-subnet",
		"ipCidrRange":"10.42.0.0/24",
		"network":"projects/test-project/global/networks/custom",
		"region":"projects/test-project/global/regions/us-central1",
		"logConfig":{"enable":false}
	}`)
	if response.Code != http.StatusOK {
		t.Fatalf("provider-shaped create status=%d body=%s", response.Code, response.Body.String())
	}

	api.mu.RLock()
	subnetwork := cloneSubnetwork(api.subnetworks[subnetworkKey("test-project", "us-central1", "provider-subnet")])
	api.mu.RUnlock()
	if subnetwork == nil || subnetwork.Network != networkSelfLink("test-project", "custom") ||
		subnetwork.Region != regionSelfLink("test-project", "us-central1") {
		t.Fatalf("subnetwork = %#v", subnetwork)
	}
}

func TestSubnetworkRejectsUnsupportedGoogleProviderCreateValues(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "test-project", "custom")
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"

	for _, tc := range []struct {
		name   string
		body   string
		status int
	}{
		{
			name: "mismatched body region",
			body: `{"name":"wrong-region","ipCidrRange":"10.43.0.0/24","network":"custom",` +
				`"region":"projects/test-project/regions/europe-west1"}`,
			status: http.StatusBadRequest,
		},
		{
			name: "enabled flow logs",
			body: `{"name":"flow-logs","ipCidrRange":"10.44.0.0/24","network":"custom",` +
				`"logConfig":{"enable":true}}`,
			status: http.StatusNotImplemented,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertComputeError(t, performComputeRequest(api, http.MethodPost, base, tc.body),
				tc.status, map[int]string{
					http.StatusBadRequest:     "INVALID_ARGUMENT",
					http.StatusNotImplemented: "UNIMPLEMENTED",
				}[tc.status])
		})
	}
}

func TestSubnetworkPaginationUsesCollectionBoundNameCursor(t *testing.T) {
	api, _ := newComputeTestAPI()
	for _, name := range []string{"network-a", "network-b", "network-zero"} {
		addCustomNetwork(api, "test-project", name)
	}
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	for _, body := range []string{
		`{"name":"subnet-a","ipCidrRange":"10.10.0.0/24","network":"network-a"}`,
		`{"name":"subnet-b","ipCidrRange":"10.20.0.0/24","network":"network-b"}`,
	} {
		if response := performComputeRequest(api, http.MethodPost, base, body); response.Code != http.StatusOK {
			t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
		}
	}
	first := performComputeRequest(api, http.MethodGet, base+"?maxResults=1", "")
	var page struct {
		Items         []*Subnetwork `json:"items"`
		NextPageToken string        `json:"nextPageToken"`
	}
	decodeComputeResponse(t, first, &page)
	if len(page.Items) != 1 || page.Items[0].Name != "subnet-a" || page.NextPageToken == "" {
		t.Fatalf("first page = %#v", page)
	}
	token := page.NextPageToken

	if response := performComputeRequest(api, http.MethodDelete, base+"/subnet-a", ""); response.Code != http.StatusOK {
		t.Fatalf("delete cursor item status=%d body=%s", response.Code, response.Body.String())
	}
	if response := performComputeRequest(api, http.MethodPost, base,
		`{"name":"subnet-0","ipCidrRange":"10.5.0.0/24","network":"network-zero"}`); response.Code != http.StatusOK {
		t.Fatalf("insert before cursor status=%d body=%s", response.Code, response.Body.String())
	}
	second := performComputeRequest(api, http.MethodGet,
		base+"?maxResults=1&pageToken="+token, "")
	decodeComputeResponse(t, second, &page)
	if len(page.Items) != 1 || page.Items[0].Name != "subnet-b" {
		t.Fatalf("cursor page after mutation = %#v", page)
	}

	for _, path := range []string{
		"/compute/v1/projects/other/regions/us-central1/subnetworks?pageToken=" + token,
		"/compute/v1/projects/test-project/regions/europe-west1/subnetworks?pageToken=" + token,
		base + "?pageToken=" + base64TokenForTest(`{"v":1,"project":"test-project","region":"us-central1","lastName":"subnet-a","extra":true}`),
		base + "?pageToken=" + base64TokenForTest(`{"v":2,"project":"test-project","region":"us-central1","lastName":"subnet-a"}`),
		base + "?pageToken=" + base64TokenForTest(`{"v":1,"project":"test-project","region":"us-central1","lastName":"subnet-a"} {}`),
		base + "?pageToken=" + strings.Repeat("a", 4097),
	} {
		assertComputeError(t, performComputeRequest(api, http.MethodGet, path, ""),
			http.StatusBadRequest, "INVALID_ARGUMENT")
	}
}

func TestSubnetworkValidationAndUnsupportedRoutes(t *testing.T) {
	api, _ := newComputeTestAPI()
	addCustomNetwork(api, "test-project", "custom")
	addCustomNetwork(api, "test-project", "other")
	api.networks["test-project:auto"] = &Network{Name: "auto", AutoCreateSubnetworks: true}
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"invalid name", `{"name":"Bad","ipCidrRange":"10.0.0.0/24","network":"custom"}`, http.StatusBadRequest},
		{"noncanonical CIDR", `{"name":"bad-cidr","ipCidrRange":"10.0.0.1/24","network":"custom"}`, http.StatusBadRequest},
		{"loopback CIDR", `{"name":"loopback","ipCidrRange":"127.0.0.0/24","network":"custom"}`, http.StatusBadRequest},
		{"broader link-local overlap", `{"name":"link-local-overlap","ipCidrRange":"169.0.0.0/8","network":"custom"}`, http.StatusBadRequest},
		{"link-local CIDR", `{"name":"link-local","ipCidrRange":"169.254.0.0/16","network":"custom"}`, http.StatusBadRequest},
		{"class E CIDR", `{"name":"class-e","ipCidrRange":"240.0.0.0/4","network":"custom"}`, http.StatusBadRequest},
		{"broadcast-containing CIDR", `{"name":"broadcast","ipCidrRange":"255.255.255.248/29","network":"custom"}`, http.StatusBadRequest},
		{"prefix too small", `{"name":"small","ipCidrRange":"10.0.0.0/31","network":"custom"}`, http.StatusBadRequest},
		{"slash 30", `{"name":"slash-30","ipCidrRange":"10.0.0.0/30","network":"custom"}`, http.StatusBadRequest},
		{"missing network", `{"name":"missing","ipCidrRange":"10.0.0.0/24","network":"absent"}`, http.StatusBadRequest},
		{"cross project network", `{"name":"cross","ipCidrRange":"10.0.0.0/24","network":"https://www.googleapis.com/compute/v1/projects/other/global/networks/custom"}`, http.StatusBadRequest},
		{"auto network", `{"name":"automatic","ipCidrRange":"10.0.0.0/24","network":"auto"}`, http.StatusBadRequest},
		{"default network", `{"name":"defaulted","ipCidrRange":"10.0.0.0/24","network":"default"}`, http.StatusBadRequest},
		{"unknown field", `{"name":"unknown","ipCidrRange":"10.0.0.0/24","network":"custom","secondaryIpRanges":[]}`, http.StatusBadRequest},
		{"unsupported purpose", `{"name":"proxy","ipCidrRange":"10.0.0.0/24","network":"custom","purpose":"REGIONAL_MANAGED_PROXY"}`, http.StatusNotImplemented},
		{"unsupported stack", `{"name":"dual","ipCidrRange":"10.0.0.0/24","network":"custom","stackType":"IPV4_IPV6"}`, http.StatusNotImplemented},
		{"unsupported private access", `{"name":"private-access","ipCidrRange":"10.0.0.0/24","network":"custom","privateIpGoogleAccess":true}`, http.StatusNotImplemented},
		{"trailing JSON", `{"name":"trailing","ipCidrRange":"10.0.0.0/24","network":"custom"} {}`, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response := performComputeRequest(api, http.MethodPost, base, tc.body)
			if response.Code != tc.status || !strings.Contains(response.Body.String(), `"error"`) {
				t.Fatalf("status=%d body=%s, want %d JSON error", response.Code, response.Body.String(), tc.status)
			}
		})
	}
	accepted29 := performComputeRequest(api, http.MethodPost, base,
		`{"name":"slash-29","ipCidrRange":"10.30.0.0/29","network":"other"}`)
	if accepted29.Code != http.StatusOK {
		t.Fatalf("/29 status=%d body=%s", accepted29.Code, accepted29.Body.String())
	}

	oversized := `{"name":"large","ipCidrRange":"10.0.0.0/24","network":"custom","description":"` +
		strings.Repeat("x", 1<<20) + `"}`
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base, oversized),
		http.StatusRequestEntityTooLarge, "INVALID_ARGUMENT")

	created := performComputeRequest(api, http.MethodPost, base,
		`{"name":"primary","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
		`{"name":"primary","ipCidrRange":"10.1.0.0/24","network":"other"}`),
		http.StatusConflict, "ALREADY_EXISTS")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
		`{"name":"same-parent","ipCidrRange":"10.2.0.0/24","network":"custom"}`),
		http.StatusConflict, "ALREADY_EXISTS")
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
		`{"name":"overlap","ipCidrRange":"10.0.0.128/25","network":"other"}`),
		http.StatusConflict, "ALREADY_EXISTS")

	for _, method := range []string{http.MethodPatch, http.MethodPut} {
		assertComputeError(t, performComputeRequest(api, method, base+"/primary", `{}`),
			http.StatusNotImplemented, "UNIMPLEMENTED")
		assertComputeError(t, performComputeRequest(api, method, base, `{}`),
			http.StatusNotImplemented, "UNIMPLEMENTED")
	}
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base+"/primary/extra", ""),
		http.StatusNotFound, "NOT_FOUND")
}

func TestSubnetworkPersistenceRestartAndStableID(t *testing.T) {
	store, err := state.New(t.TempDir(), "subnetwork-restart")
	if err != nil {
		t.Fatal(err)
	}
	opMgr := orchestrator.NewOperationManager()
	api, err := newTestAPIWithMetadataStore(opMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "test-project", "network-a")
	addCustomNetwork(api, "test-project", "network-b")
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	create := performComputeRequest(api, http.MethodPost, base,
		`{"name":"first","ipCidrRange":"10.0.0.0/24","network":"network-a"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	first := getSubnetworkForTest(t, api, base+"/first")

	restarted, err := newTestAPIWithMetadataStore(opMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	if got := getSubnetworkForTest(t, restarted, base+"/first"); got.ID != first.ID {
		t.Fatalf("ID changed after restart: %s -> %s", first.ID, got.ID)
	}
	list := performComputeRequest(restarted, http.MethodGet, base, "")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"first"`) {
		t.Fatalf("restart list status=%d body=%s", list.Code, list.Body.String())
	}
	secondCreate := performComputeRequest(restarted, http.MethodPost, base,
		`{"name":"second","ipCidrRange":"10.1.0.0/24","network":"network-b"}`)
	if secondCreate.Code != http.StatusOK {
		t.Fatalf("second create status=%d body=%s", secondCreate.Code, secondCreate.Body.String())
	}
	second := getSubnetworkForTest(t, restarted, base+"/second")
	firstID, _ := strconv.ParseUint(first.ID, 10, 64)
	secondID, _ := strconv.ParseUint(second.ID, 10, 64)
	if secondID <= firstID {
		t.Fatalf("ID counter collided after restart: first=%d second=%d", firstID, secondID)
	}

	remove := performComputeRequest(restarted, http.MethodDelete, base+"/first", "")
	if remove.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", remove.Code, remove.Body.String())
	}
	restartedAgain, err := newTestAPIWithMetadataStore(opMgr, store)
	if err != nil {
		t.Fatal(err)
	}
	assertComputeError(t, performComputeRequest(restartedAgain, http.MethodGet, base+"/first", ""),
		http.StatusNotFound, "NOT_FOUND")
}

func TestSubnetworkSuccessfulResponseSurvivesImmediateMetadataAndOperationRestart(t *testing.T) {
	metadataStore := &toggleComputeStore{}
	operationStore := &toggleComputeStore{}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "test-project", "custom")
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	create := performComputeRequest(api, http.MethodPost, base,
		`{"name":"durable","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var createOp orchestrator.Operation
	decodeComputeResponse(t, create, &createOp)
	if createOp.Status != orchestrator.StatusDone || !createOp.Done || createOp.Progress != 100 {
		t.Fatalf("create operation not terminal: %#v", createOp)
	}

	restartedOpMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := newTestAPIWithMetadataStore(restartedOpMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	getSubnetworkForTest(t, restarted, base+"/durable")
	persistedCreate := restartedOpMgr.Get(createOp.Name)
	if persistedCreate == nil || persistedCreate.Status != orchestrator.StatusDone || !persistedCreate.Done {
		t.Fatalf("persisted create operation = %#v", persistedCreate)
	}

	remove := performComputeRequest(restarted, http.MethodDelete, base+"/durable", "")
	if remove.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", remove.Code, remove.Body.String())
	}
	var deleteOp orchestrator.Operation
	decodeComputeResponse(t, remove, &deleteOp)
	if deleteOp.Status != orchestrator.StatusDone || !deleteOp.Done || deleteOp.Progress != 100 {
		t.Fatalf("delete operation not terminal: %#v", deleteOp)
	}

	restartedOpMgr, err = orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err = newTestAPIWithMetadataStore(restartedOpMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	assertComputeError(t, performComputeRequest(restarted, http.MethodGet, base+"/durable", ""),
		http.StatusNotFound, "NOT_FOUND")
	persistedDelete := restartedOpMgr.Get(deleteOp.Name)
	if persistedDelete == nil || persistedDelete.Status != orchestrator.StatusDone || !persistedDelete.Done {
		t.Fatalf("persisted delete operation = %#v", persistedDelete)
	}
}

func TestSubnetworkTerminalOperationFailureKeepsMetadataTruth(t *testing.T) {
	tests := []struct {
		name       string
		seed       bool
		method     string
		pathSuffix string
		body       string
		wantExists bool
	}{
		{
			name: "create", method: http.MethodPost,
			body:       `{"name":"terminal-failure","ipCidrRange":"10.0.0.0/24","network":"custom"}`,
			wantExists: true,
		},
		{
			name: "delete", seed: true, method: http.MethodDelete, pathSuffix: "/terminal-failure",
			wantExists: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadataStore := &toggleComputeStore{}
			operationStore := &toggleComputeStore{failAfter: 1}
			opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
			if err != nil {
				t.Fatal(err)
			}
			addCustomNetwork(api, "test-project", "custom")
			if test.seed {
				api.mu.Lock()
				api.subnetworks[subnetworkKey("test-project", "us-central1", "terminal-failure")] =
					persistedSubnetworkForTest("test-project", "us-central1", "terminal-failure", "custom", "10.0.0.0/24", "1")
				api.nextSubnetworkID = 2
				api.mu.Unlock()
				if _, err := api.vpcIPAM.EnsureVPCNetworkIPAM(
					context.Background(),
					orchestrator.VPCNetworkIdentity{Project: "test-project", Network: "custom"},
					"10.0.0.0/24",
				); err != nil {
					t.Fatal(err)
				}
			}
			if err := api.persistMetadata(); err != nil {
				t.Fatal(err)
			}
			base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
			assertComputeError(t, performComputeRequest(api, test.method, base+test.pathSuffix, test.body),
				http.StatusInternalServerError, "INTERNAL")

			restarted, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), metadataStore)
			if err != nil {
				t.Fatal(err)
			}
			response := performComputeRequest(restarted, http.MethodGet, base+"/terminal-failure", "")
			if test.wantExists && response.Code != http.StatusOK {
				t.Fatalf("metadata truth lost: status=%d body=%s", response.Code, response.Body.String())
			}
			if !test.wantExists {
				assertComputeError(t, response, http.StatusNotFound, "NOT_FOUND")
			}
		})
	}
}

func TestSubnetworkSaveFailureRollsBack(t *testing.T) {
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	t.Run("create", func(t *testing.T) {
		metadataStore := &toggleComputeStore{}
		operationStore := &toggleComputeStore{}
		opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
		if err != nil {
			t.Fatal(err)
		}
		api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
		if err != nil {
			t.Fatal(err)
		}
		addCustomNetwork(api, "test-project", "custom")
		if err := api.persistMetadata(); err != nil {
			t.Fatal(err)
		}
		metadataStore.setFail(true)
		assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
			`{"name":"rolled-back","ipCidrRange":"10.0.0.0/24","network":"custom"}`),
			http.StatusInternalServerError, "INTERNAL")
		if metadataStore.saveCount() != 2 {
			t.Fatalf("metadata save calls=%d, want initial plus one failed commit", metadataStore.saveCount())
		}
		restartedOpMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
		if err != nil {
			t.Fatal(err)
		}
		restarted, err := newTestAPIWithMetadataStore(restartedOpMgr, metadataStore)
		if err != nil {
			t.Fatal(err)
		}
		assertComputeError(t, performComputeRequest(restarted, http.MethodGet, base+"/rolled-back", ""),
			http.StatusNotFound, "NOT_FOUND")
		ops := restartedOpMgr.List()
		if len(ops) != 1 || !ops[0].Done || ops[0].Error == nil {
			t.Fatalf("failed create operation audit = %#v", ops)
		}
	})
	t.Run("delete", func(t *testing.T) {
		metadataStore := &toggleComputeStore{}
		operationStore := &toggleComputeStore{}
		opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
		if err != nil {
			t.Fatal(err)
		}
		api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
		if err != nil {
			t.Fatal(err)
		}
		addCustomNetwork(api, "test-project", "custom")
		api.mu.Lock()
		api.subnetworks[subnetworkKey("test-project", "us-central1", "kept")] =
			persistedSubnetworkForTest("test-project", "us-central1", "kept", "custom", "10.0.0.0/24", "1")
		api.nextSubnetworkID = 2
		api.mu.Unlock()
		if _, err := api.vpcIPAM.EnsureVPCNetworkIPAM(
			context.Background(),
			orchestrator.VPCNetworkIdentity{Project: "test-project", Network: "custom"},
			"10.0.0.0/24",
		); err != nil {
			t.Fatal(err)
		}
		if err := api.persistMetadata(); err != nil {
			t.Fatal(err)
		}
		metadataStore.setFail(true)
		assertComputeError(t, performComputeRequest(api, http.MethodDelete, base+"/kept", ""),
			http.StatusInternalServerError, "INTERNAL")
		if metadataStore.saveCount() != 2 {
			t.Fatalf("metadata save calls=%d, want initial plus one failed commit", metadataStore.saveCount())
		}
		restartedOpMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
		if err != nil {
			t.Fatal(err)
		}
		restarted, err := newTestAPIWithMetadataStore(restartedOpMgr, metadataStore)
		if err != nil {
			t.Fatal(err)
		}
		getSubnetworkForTest(t, restarted, base+"/kept")
		ops := restartedOpMgr.List()
		var failedDelete bool
		for _, op := range ops {
			failedDelete = failedDelete || op.OperationType == "delete" && op.Done && op.Error != nil
		}
		if !failedDelete {
			t.Fatalf("missing failed delete operation audit: %#v", ops)
		}
	})
}

func TestSubnetworkMetadataTruthSurvivesOperationFailurePersistenceFailure(t *testing.T) {
	metadataStore := &toggleComputeStore{}
	operationStore := &toggleComputeStore{failAfter: 1}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "test-project", "custom")
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}
	metadataStore.setFail(true)
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
		`{"name":"degraded","ipCidrRange":"10.0.0.0/24","network":"custom"}`),
		http.StatusInternalServerError, "INTERNAL")
	if opMgr.PersistenceError() == nil {
		t.Fatal("operation persistence degradation was not surfaced")
	}
	restarted, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	assertComputeError(t, performComputeRequest(restarted, http.MethodGet, base+"/degraded", ""),
		http.StatusNotFound, "NOT_FOUND")
}

func TestSubnetworkOperationRegistrationFailureRollsBack(t *testing.T) {
	operationStore := &toggleComputeStore{fail: true}
	opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &toggleComputeStore{}
	api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "test-project", "custom")
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	assertComputeError(t, performComputeRequest(api, http.MethodPost, base,
		`{"name":"no-operation","ipCidrRange":"10.0.0.0/24","network":"custom"}`),
		http.StatusInternalServerError, "INTERNAL")
	assertComputeError(t, performComputeRequest(api, http.MethodGet, base+"/no-operation", ""),
		http.StatusNotFound, "NOT_FOUND")
}

func TestNetworkDeleteSerializesWithSubnetworkInsert(t *testing.T) {
	store := newBlockingComputeStore()
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "test-project", "custom")
	subnetworkBase := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	networkPath := "/compute/v1/projects/test-project/global/networks/custom"

	insertDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		insertDone <- performComputeRequest(api, http.MethodPost, subnetworkBase,
			`{"name":"serialized","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
	}()
	<-store.started

	deleteDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		deleteDone <- performComputeRequest(api, http.MethodDelete, networkPath, "")
	}()
	select {
	case response := <-deleteDone:
		t.Fatalf("network delete interleaved with insert: status=%d body=%s",
			response.Code, response.Body.String())
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	if response := <-insertDone; response.Code != http.StatusOK {
		t.Fatalf("insert status=%d body=%s", response.Code, response.Body.String())
	}
	assertComputeError(t, <-deleteDone, http.StatusBadRequest, "FAILED_PRECONDITION")
	if api.networks["test-project:custom"] == nil || api.subnetworks[subnetworkKey("test-project", "us-central1", "serialized")] == nil {
		t.Fatalf("serialized resources orphaned: networks=%#v subnetworks=%#v", api.networks, api.subnetworks)
	}
}

func TestNetworkInsertRejectsDuplicateWithoutReplacingParent(t *testing.T) {
	api, _ := newComputeTestAPI()
	api.networks["test-project:default"] = &Network{
		Kind: "compute#network", ID: "stable", Name: "default",
		Description: "original", SelfLink: networkSelfLink("test-project", "default"),
	}
	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/test-project/global/networks",
		`{"name":"default","description":"replacement"}`)
	assertComputeError(t, response, http.StatusConflict, "ALREADY_EXISTS")
	network := api.networks["test-project:default"]
	if network.ID != "stable" || network.Description != "original" {
		t.Fatalf("duplicate replaced network: %#v", network)
	}
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("duplicate create persisted operations: %#v", operations)
	}
}

func TestNetworkLifecycleIsControlPlaneOnlyUntilSubnetworkExists(t *testing.T) {
	api, _ := newComputeTestAPI()
	backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
	base := "/compute/v1/projects/test-project/global/networks"
	create := performComputeRequest(api, http.MethodPost, base,
		`{"name":"control-plane","autoCreateSubnetworks":false}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	remove := performComputeRequest(api, http.MethodDelete, base+"/control-plane", "")
	if remove.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", remove.Code, remove.Body.String())
	}
	if backend.ensureCalls != 0 || backend.deleteCalls != 0 {
		t.Fatalf("network lifecycle touched IPAM backend: ensure=%d delete=%d",
			backend.ensureCalls, backend.deleteCalls)
	}
}

func TestNetworkPreservesGoogleProviderDefaultEnforcementOrder(t *testing.T) {
	api, _ := newComputeTestAPI()
	base := "/compute/v1/projects/test-project/global/networks"
	create := performComputeRequest(api, http.MethodPost, base, `{
		"name":"provider-network",
		"autoCreateSubnetworks":false,
		"networkFirewallPolicyEnforcementOrder":"AFTER_CLASSIC_FIREWALL"
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}

	get := performComputeRequest(api, http.MethodGet, base+"/provider-network", "")
	var network struct {
		NetworkFirewallPolicyEnforcementOrder string `json:"networkFirewallPolicyEnforcementOrder"`
	}
	decodeComputeResponse(t, get, &network)
	if network.NetworkFirewallPolicyEnforcementOrder != "AFTER_CLASSIC_FIREWALL" {
		t.Fatalf("networkFirewallPolicyEnforcementOrder = %q", network.NetworkFirewallPolicyEnforcementOrder)
	}
}

func TestNetworkOperationRegistrationFailurePreservesMetadataTruth(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     string
		pathSuffix string
		seed       bool
		wantExists bool
	}{
		{name: "create", method: http.MethodPost, wantExists: false},
		{name: "delete", method: http.MethodDelete, pathSuffix: "/existing", seed: true, wantExists: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operationStore := &toggleComputeStore{fail: true}
			opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			metadataStore := &toggleComputeStore{}
			api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
			if err != nil {
				t.Fatal(err)
			}
			if tc.seed {
				addCustomNetwork(api, "test-project", "existing")
				if err := api.persistMetadata(); err != nil {
					t.Fatal(err)
				}
			}

			base := "/compute/v1/projects/test-project/global/networks"
			response := performComputeRequest(api, tc.method, base+tc.pathSuffix,
				`{"name":"created","autoCreateSubnetworks":false}`)
			assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")

			api.mu.RLock()
			name := "created"
			if tc.seed {
				name = "existing"
			}
			_, exists := api.networks["test-project:"+name]
			api.mu.RUnlock()
			if exists != tc.wantExists {
				t.Fatalf("network exists=%t, want %t", exists, tc.wantExists)
			}
		})
	}
}

func TestNetworkInsertSerializesWithSubnetworkMutation(t *testing.T) {
	store := newBlockingComputeStore()
	api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	addCustomNetwork(api, "test-project", "custom")
	original := api.networks["test-project:custom"]
	subnetworkBase := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	networkPath := "/compute/v1/projects/test-project/global/networks"

	insertSubnetworkDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		insertSubnetworkDone <- performComputeRequest(api, http.MethodPost, subnetworkBase,
			`{"name":"serialized-parent","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
	}()
	<-store.started

	insertNetworkDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				insertNetworkDone <- httptest.NewRecorder()
			}
		}()
		insertNetworkDone <- performComputeRequest(api, http.MethodPost, networkPath,
			`{"name":"custom","description":"replacement"}`)
	}()
	time.Sleep(50 * time.Millisecond)
	api.mu.RLock()
	current := api.networks["test-project:custom"]
	api.mu.RUnlock()
	if current != original {
		close(store.release)
		t.Fatalf("network was replaced while subnetwork mutation was committing: %#v", current)
	}

	close(store.release)
	if response := <-insertSubnetworkDone; response.Code != http.StatusOK {
		t.Fatalf("subnetwork insert status=%d body=%s", response.Code, response.Body.String())
	}
	response := <-insertNetworkDone
	assertComputeError(t, response, http.StatusConflict, "ALREADY_EXISTS")
	if api.networks["test-project:custom"] != original {
		t.Fatal("duplicate network replaced parent beneath child")
	}
}

func TestRegionalOperationScopeIsolation(t *testing.T) {
	api, opMgr := newComputeTestAPI()
	addCustomNetwork(api, "test-project", "custom")
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	create := performComputeRequest(api, http.MethodPost, base,
		`{"name":"scope","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
	var created orchestrator.Operation
	decodeComputeResponse(t, create, &created)

	valid := fmt.Sprintf("/compute/v1/projects/test-project/regions/us-central1/operations/%s", created.Name)
	if response := performComputeRequest(api, http.MethodGet, valid, ""); response.Code != http.StatusOK {
		t.Fatalf("valid operation status=%d body=%s", response.Code, response.Body.String())
	}
	for _, path := range []string{
		fmt.Sprintf("/compute/v1/projects/other/regions/us-central1/operations/%s", created.Name),
		fmt.Sprintf("/compute/v1/projects/test-project/regions/europe-west1/operations/%s", created.Name),
		"/compute/v1/projects/test-project/regions/us-central1/operations/missing",
	} {
		assertComputeError(t, performComputeRequest(api, http.MethodGet, path, ""),
			http.StatusNotFound, "NOT_FOUND")
	}
	foreign := opMgr.Register("sql#operation", "insert",
		"https://www.googleapis.com/sql/v1/projects/test-project/instances/db", "", "us-central1")
	assertComputeError(t, performComputeRequest(api, http.MethodGet,
		fmt.Sprintf("/compute/v1/projects/test-project/regions/us-central1/operations/%s", foreign.Name), ""),
		http.StatusNotFound, "NOT_FOUND")
}

func TestSubnetworkBackendFailuresPreserveMetadataTruth(t *testing.T) {
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	t.Run("ensure failure leaves no metadata", func(t *testing.T) {
		api, _ := newComputeTestAPI()
		addCustomNetwork(api, "test-project", "custom")
		backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
		backend.ensureErr = errors.New("Docker unavailable")

		response := performComputeRequest(api, http.MethodPost, base,
			`{"name":"failed","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
		assertComputeError(t, response, http.StatusServiceUnavailable, "FAILED_PRECONDITION")
		if api.subnetworks[subnetworkKey("test-project", "us-central1", "failed")] != nil {
			t.Fatal("backend failure committed subnetwork metadata")
		}
	})

	t.Run("delete conflict retains metadata", func(t *testing.T) {
		api, _ := newComputeTestAPI()
		addCustomNetwork(api, "test-project", "custom")
		create := performComputeRequest(api, http.MethodPost, base,
			`{"name":"attached","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
		if create.Code != http.StatusOK {
			t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
		}
		backend := api.vpcIPAM.(*fakeVPCIPAMBackend)
		backend.deleteErr = errors.New("network has active endpoints")

		response := performComputeRequest(api, http.MethodDelete, base+"/attached", "")
		assertComputeError(t, response, http.StatusServiceUnavailable, "FAILED_PRECONDITION")
		if api.subnetworks[subnetworkKey("test-project", "us-central1", "attached")] == nil {
			t.Fatal("backend delete conflict removed metadata")
		}
	})
}

func TestSubnetworkPersistenceFailureCompensatesExactBridge(t *testing.T) {
	base := "/compute/v1/projects/test-project/regions/us-central1/subnetworks"
	t.Run("create cleanup", func(t *testing.T) {
		store := &toggleComputeStore{}
		api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
		if err != nil {
			t.Fatal(err)
		}
		addCustomNetwork(api, "test-project", "custom")
		if err := api.persistMetadata(); err != nil {
			t.Fatal(err)
		}
		store.setFail(true)
		backend := api.vpcIPAM.(*fakeVPCIPAMBackend)

		response := performComputeRequest(api, http.MethodPost, base,
			`{"name":"cleanup","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
		assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
		if api.initializationError() != nil {
			t.Fatalf("successful bridge cleanup degraded API: %v", api.initializationError())
		}
		backend.mu.Lock()
		defer backend.mu.Unlock()
		if backend.ensureCalls != 1 || backend.deleteCalls != 1 || len(backend.bridges) != 0 {
			t.Fatalf("create compensation: ensure=%d delete=%d bridges=%#v",
				backend.ensureCalls, backend.deleteCalls, backend.bridges)
		}
	})

	t.Run("delete recreate", func(t *testing.T) {
		store := &toggleComputeStore{}
		api, err := newTestAPIWithMetadataStore(orchestrator.NewOperationManager(), store)
		if err != nil {
			t.Fatal(err)
		}
		addCustomNetwork(api, "test-project", "custom")
		create := performComputeRequest(api, http.MethodPost, base,
			`{"name":"recreate","ipCidrRange":"10.0.0.0/24","network":"custom"}`)
		if create.Code != http.StatusOK {
			t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
		}
		store.setFail(true)
		backend := api.vpcIPAM.(*fakeVPCIPAMBackend)

		response := performComputeRequest(api, http.MethodDelete, base+"/recreate", "")
		assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
		if api.subnetworks[subnetworkKey("test-project", "us-central1", "recreate")] == nil {
			t.Fatal("failed delete lost metadata")
		}
		if api.initializationError() != nil {
			t.Fatalf("successful bridge recreation degraded API: %v", api.initializationError())
		}
		backend.mu.Lock()
		defer backend.mu.Unlock()
		identity := orchestrator.VPCNetworkIdentity{Project: "test-project", Network: "custom"}
		if backend.deleteCalls != 1 || backend.ensureCalls != 2 ||
			backend.bridges[identity] != "10.0.0.0/24" {
			t.Fatalf("delete compensation: ensure=%d delete=%d bridges=%#v",
				backend.ensureCalls, backend.deleteCalls, backend.bridges)
		}
	})
}

func TestSubnetworkRestartReconciliationAndFailClosed(t *testing.T) {
	store := &toggleComputeStore{}
	seed := newAPI(orchestrator.NewOperationManager(), nil, store)
	addCustomNetwork(seed, "test-project", "custom")
	seed.subnetworks[subnetworkKey("test-project", "us-central1", "persisted")] =
		persistedSubnetworkForTest("test-project", "us-central1", "persisted", "custom", "10.0.0.0/24", "1")
	seed.nextSubnetworkID = 2
	if err := seed.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	loaded, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.vpcIPAM != nil {
		t.Fatal("NewAPIWithStore path unexpectedly installed or invoked a backend")
	}
	backend := useFakeVPCIPAM(loaded)
	if err := loaded.ReconcileVPCIPAM(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.ensureCalls != 1 {
		t.Fatalf("reconcile ensure calls=%d, want 1", backend.ensureCalls)
	}
	got := getSubnetworkForTest(t, loaded,
		"/compute/v1/projects/test-project/regions/us-central1/subnetworks/persisted")
	if got.State != "READY" {
		t.Fatalf("reconciled state=%q", got.State)
	}

	collided, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	collisionBackend := useFakeVPCIPAM(collided)
	collisionBackend.ensureErr = errors.New("unowned collision")
	if err := collided.ReconcileVPCIPAM(context.Background()); err == nil {
		t.Fatal("reconciliation collision succeeded")
	}
	assertComputeError(t, performComputeRequest(collided, http.MethodGet,
		"/compute/v1/projects/test-project/regions/us-central1/subnetworks/persisted", ""),
		http.StatusServiceUnavailable, "FAILED_PRECONDITION")
	if collided.subnetworks[subnetworkKey("test-project", "us-central1", "persisted")] == nil {
		t.Fatal("failed reconciliation discarded loaded metadata")
	}
}

type toggleComputeStore struct {
	mu        sync.Mutex
	payload   json.RawMessage
	fail      bool
	failAfter int
	saves     int
}

type fakeVPCIPAMBackend struct {
	mu             sync.Mutex
	bridges        map[orchestrator.VPCNetworkIdentity]string
	ensureCalls    int
	deleteCalls    int
	ensureErr      error
	ensureErrAfter int
	deleteErr      error
	ensureOrder    []orchestrator.VPCNetworkIdentity
}

func newFakeVPCIPAMBackend() *fakeVPCIPAMBackend {
	return &fakeVPCIPAMBackend{bridges: make(map[orchestrator.VPCNetworkIdentity]string)}
}

func (backend *fakeVPCIPAMBackend) EnsureVPCNetworkIPAM(
	_ context.Context,
	identity orchestrator.VPCNetworkIdentity,
	cidr string,
) (orchestrator.VPCNetworkIPAMState, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.ensureCalls++
	backend.ensureOrder = append(backend.ensureOrder, identity)
	if backend.ensureErr != nil && (backend.ensureErrAfter == 0 || backend.ensureCalls > backend.ensureErrAfter) {
		return orchestrator.VPCNetworkIPAMState{}, backend.ensureErr
	}
	existing, found := backend.bridges[identity]
	if found && existing != cidr {
		return orchestrator.VPCNetworkIPAMState{}, errors.New("CIDR mismatch")
	}
	backend.bridges[identity] = cidr
	return orchestrator.VPCNetworkIPAMState{
		Name: identity.CanonicalResource(), CIDR: cidr, Created: !found,
	}, nil
}

func (backend *fakeVPCIPAMBackend) DeleteVPCNetworkIPAM(
	_ context.Context,
	identity orchestrator.VPCNetworkIdentity,
	cidr string,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.deleteCalls++
	if backend.deleteErr != nil {
		return backend.deleteErr
	}
	if backend.bridges[identity] != cidr {
		return orchestrator.ErrVPCNetworkNotFound
	}
	delete(backend.bridges, identity)
	return nil
}

func useFakeVPCIPAM(api *API) *fakeVPCIPAMBackend {
	backend := newFakeVPCIPAMBackend()
	api.vpcIPAM = backend
	return backend
}

func newTestAPIWithMetadataStore(
	opMgr *orchestrator.OperationManager,
	store computeMetadataStore,
) (*API, error) {
	api, err := newAPIWithMetadataStore(opMgr, nil, store)
	if api != nil {
		useFakeVPCIPAM(api)
		api.legacyVPC = &fakeLegacyVPCBackend{}
		if err == nil {
			err = api.ReconcileVPCIPAM(context.Background())
		}
	}
	return api, err
}

func (s *toggleComputeStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.payload) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.payload, target)
}

func (s *toggleComputeStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.fail || s.failAfter > 0 && s.saves > s.failAfter {
		return errors.New("injected save failure")
	}
	payload, err := json.Marshal(value)
	if err == nil {
		s.payload = payload
	}
	return err
}

func (s *toggleComputeStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *toggleComputeStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

type blockingComputeStore struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingComputeStore() *blockingComputeStore {
	return &blockingComputeStore{started: make(chan struct{}), release: make(chan struct{})}
}

func (*blockingComputeStore) Load(string, any) error { return state.ErrNotFound }

func (s *blockingComputeStore) Save(string, any) error {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return nil
}

func addCustomNetwork(api *API, project, name string) {
	api.mu.Lock()
	api.networks[project+":"+name] = &Network{
		Kind:                  "compute#network",
		ID:                    "1",
		Name:                  name,
		SelfLink:              networkSelfLink(project, name),
		AutoCreateSubnetworks: false,
	}
	api.mu.Unlock()
}

func persistedSubnetworkForTest(project, region, name, network, cidr, id string) *Subnetwork {
	prefix, _ := netip.ParsePrefix(cidr)
	subnet := &Subnetwork{
		Kind: "compute#subnetwork", ID: id, Name: name, IPCidrRange: cidr,
		Network: networkSelfLink(project, network), Region: regionSelfLink(project, region),
		SelfLink: subnetworkSelfLink(project, region, name), GatewayAddress: prefix.Addr().Next().String(),
		Purpose: "PRIVATE", StackType: "IPV4_ONLY", State: "READY",
		CreationTimestamp: "2026-01-01T00:00:00Z",
	}
	subnet.Fingerprint = subnetworkFingerprint(subnet)
	return subnet
}

func getSubnetworkForTest(t *testing.T, api *API, path string) *Subnetwork {
	t.Helper()
	response := performComputeRequest(api, http.MethodGet, path, "")
	if response.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", response.Code, response.Body.String())
	}
	var got Subnetwork
	decodeComputeResponse(t, response, &got)
	return &got
}

func waitForRegionalOperation(t *testing.T, client *compute.Service, project, region, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		op, err := client.RegionOperations.Get(project, region, name).Do()
		if err != nil {
			t.Fatal(err)
		}
		if op.Status == "DONE" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("operation %q did not complete", name)
}

func base64TokenForTest(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
