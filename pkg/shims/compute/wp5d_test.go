package compute

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestSecurityPolicyCreatePersistsTerminalOperationAcrossRestart(t *testing.T) {
	metadataStore := &toggleComputeStore{}
	operationStore := &failingFirewallOperationStore{}
	manager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	api, err := newAPIWithMetadataStore(manager, nil, metadataStore)
	if err != nil {
		t.Fatal(err)
	}

	base := "/compute/v1/projects/project-a/global/securityPolicies"
	response := performComputeRequest(api, http.MethodPost, base,
		`{"name":"edge-policy","rules":[{"priority":1000,"action":"deny(403)"}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	var created orchestrator.Operation
	decodeComputeResponse(t, response, &created)
	if !created.Done || created.Status != orchestrator.StatusDone || created.Error != nil {
		t.Fatalf("create operation was not durably terminal: %#v", created)
	}
	backend := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/backendServices",
		`{"name":"edge-backend","securityPolicy":"edge-policy"}`)
	if backend.Code != http.StatusOK {
		t.Fatalf("backend create status=%d body=%s", backend.Code, backend.Body.String())
	}

	restartedManager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := newAPIWithMetadataStore(restartedManager, nil, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	get := performComputeRequest(restarted, http.MethodGet, base+"/edge-policy", "")
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"deny(403)"`) {
		t.Fatalf("restored policy status=%d body=%s", get.Code, get.Body.String())
	}
	restoredBackend := performComputeRequest(restarted, http.MethodGet,
		"/compute/v1/projects/project-a/global/backendServices/edge-backend", "")
	if restoredBackend.Code != http.StatusOK ||
		!strings.Contains(restoredBackend.Body.String(), securityPolicySelfLink("project-a", "edge-policy")) {
		t.Fatalf("restored backend status=%d body=%s",
			restoredBackend.Code, restoredBackend.Body.String())
	}
	assertComputeError(t, performComputeRequest(restarted, http.MethodDelete, base+"/edge-policy", ""),
		http.StatusBadRequest, "FAILED_PRECONDITION")
	poll := performComputeRequest(restarted, http.MethodGet,
		"/compute/v1/projects/project-a/global/operations/"+created.Name, "")
	if poll.Code != http.StatusOK {
		t.Fatalf("restart poll status=%d body=%s", poll.Code, poll.Body.String())
	}
	var durable orchestrator.Operation
	decodeComputeResponse(t, poll, &durable)
	if !durable.Done || durable.Status != orchestrator.StatusDone || durable.Error != nil {
		t.Fatalf("restart operation=%#v", durable)
	}

	removeBackend := performComputeRequest(restarted, http.MethodDelete,
		"/compute/v1/projects/project-a/global/backendServices/edge-backend", "")
	if removeBackend.Code != http.StatusOK {
		t.Fatalf("backend delete status=%d body=%s", removeBackend.Code, removeBackend.Body.String())
	}
	removePolicy := performComputeRequest(restarted, http.MethodDelete, base+"/edge-policy", "")
	if removePolicy.Code != http.StatusOK {
		t.Fatalf("policy delete status=%d body=%s", removePolicy.Code, removePolicy.Body.String())
	}
	var deleted orchestrator.Operation
	decodeComputeResponse(t, removePolicy, &deleted)
	afterDeleteManager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	afterDelete, err := newAPIWithMetadataStore(afterDeleteManager, nil, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	assertComputeError(t, performComputeRequest(afterDelete, http.MethodGet, base+"/edge-policy", ""),
		http.StatusNotFound, "NOT_FOUND")
	deletePoll := performComputeRequest(afterDelete, http.MethodGet,
		"/compute/v1/projects/project-a/global/operations/"+deleted.Name, "")
	if deletePoll.Code != http.StatusOK {
		t.Fatalf("delete restart poll status=%d body=%s", deletePoll.Code, deletePoll.Body.String())
	}
}

func TestSecurityPolicySaveFailureRollsBackAndRecordsFailure(t *testing.T) {
	metadataStore := &toggleComputeStore{fail: true}
	api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, metadataStore)
	if err != nil {
		t.Fatal(err)
	}

	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/securityPolicies",
		`{"name":"not-durable"}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if len(api.securityPolicies) != 0 {
		t.Fatalf("failed save changed live policies: %#v", api.securityPolicies)
	}
	operations := api.opMgr.List()
	if len(operations) != 1 || !operations[0].Done || operations[0].Error == nil ||
		!strings.Contains(operations[0].Error.Message, "injected save failure") {
		t.Fatalf("failed mutation operation=%#v", operations)
	}
}

func TestSecurityPolicyOperationRegistrationFailurePrecedesMutation(t *testing.T) {
	operationStore := &failingFirewallOperationStore{
		failOnSave: map[int]error{1: fmt.Errorf("operation store unavailable")},
	}
	manager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &toggleComputeStore{}
	api, err := newAPIWithMetadataStore(manager, nil, metadataStore)
	if err != nil {
		t.Fatal(err)
	}

	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/securityPolicies",
		`{"name":"blocked"}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if len(api.securityPolicies) != 0 || metadataStore.saveCount() != 0 {
		t.Fatalf("registration failure mutated policies=%#v saves=%d",
			api.securityPolicies, metadataStore.saveCount())
	}
}

func TestSecurityPolicyTerminalOperationSaveFailureCompensatesMetadata(t *testing.T) {
	operationStore := &failingFirewallOperationStore{
		failOnSave: map[int]error{2: fmt.Errorf("terminal operation save failed")},
	}
	manager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	metadataStore := &toggleComputeStore{}
	api, err := newAPIWithMetadataStore(manager, nil, metadataStore)
	if err != nil {
		t.Fatal(err)
	}

	response := performComputeRequest(api, http.MethodPost,
		"/compute/v1/projects/project-a/global/securityPolicies",
		`{"name":"compensated"}`)
	assertComputeError(t, response, http.StatusInternalServerError, "INTERNAL")
	if len(api.securityPolicies) != 0 {
		t.Fatalf("terminal operation failure changed live policies: %#v", api.securityPolicies)
	}
	restarted, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.securityPolicies) != 0 {
		t.Fatalf("terminal operation failure remained durable: %#v", restarted.securityPolicies)
	}
	if api.initializationError() == nil {
		t.Fatal("terminal operation persistence failure did not fail closed")
	}
}

func TestComputeSemanticImportRejectsInvalidSecurityPolicyGraph(t *testing.T) {
	tests := []struct {
		name     string
		metadata computeMetadata
		want     string
	}{
		{
			name: "policy identity mismatch",
			metadata: computeMetadata{SecurityPolicies: map[string]*SecurityPolicy{
				"project-a:edge": {
					Kind: "compute#securityPolicy", ID: "1", Name: "other",
					SelfLink:          securityPolicySelfLink("project-a", "edge"),
					CreationTimestamp: "2026-07-27T10:00:00Z",
					Rules: []SecurityPolicyRule{{
						Priority: 2147483647, Action: "allow",
					}},
				},
			}},
			want: "identity",
		},
		{
			name: "dangling backend policy",
			metadata: computeMetadata{LoadBalancers: map[string]map[string]interface{}{
				loadBalancerKey("project-a", "backendServices", "backend"): {
					"kind":           "compute#backendService",
					"name":           "backend",
					"selfLink":       loadBalancerSelfLink("project-a", "backendServices", "backend"),
					"securityPolicy": securityPolicySelfLink("project-a", "missing"),
				},
			}},
			want: "security policy",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			source, err := state.New(root, "source")
			if err != nil {
				t.Fatal(err)
			}
			if err := source.Save(computeStateEntry, tc.metadata); err != nil {
				t.Fatal(err)
			}
			var exported bytes.Buffer
			if err := source.Export(&exported); err != nil {
				t.Fatal(err)
			}
			imported, err := state.New(root, "imported")
			if err != nil {
				t.Fatal(err)
			}
			if err := imported.Import(bytes.NewReader(exported.Bytes())); err != nil {
				t.Fatal(err)
			}
			api, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, imported)
			if err != nil {
				t.Fatal(err)
			}
			if initErr := api.initializationError(); initErr == nil ||
				!strings.Contains(strings.ToLower(initErr.Error()), tc.want) {
				t.Fatalf("initialization error=%v, want %q", initErr, tc.want)
			}
			assertComputeError(t, performComputeRequest(api, http.MethodGet,
				"/compute/v1/projects/project-a/global/securityPolicies", ""),
				http.StatusServiceUnavailable, "FAILED_PRECONDITION")
		})
	}
}

func TestSecurityPolicyConcurrentCreateHasSingleWinner(t *testing.T) {
	store := &toggleComputeStore{}
	api, err := newAPIWithMetadataStore(orchestrator.NewOperationManager(), nil, store)
	if err != nil {
		t.Fatal(err)
	}
	const attempts = 12
	var successes atomic.Int32
	var conflicts atomic.Int32
	var failures atomic.Int32
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response := performComputeRequest(api, http.MethodPost,
				"/compute/v1/projects/project-a/global/securityPolicies",
				`{"name":"shared"}`)
			switch response.Code {
			case http.StatusOK:
				successes.Add(1)
			case http.StatusConflict:
				conflicts.Add(1)
			default:
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || conflicts.Load() != attempts-1 || failures.Load() != 0 {
		t.Fatalf("success=%d conflict=%d failure=%d",
			successes.Load(), conflicts.Load(), failures.Load())
	}
	if len(api.securityPolicies) != 1 {
		t.Fatalf("policies=%#v", api.securityPolicies)
	}
}

func TestInterruptedComputeOperationRestartsAsScopedFailureWithoutReplay(t *testing.T) {
	operationStore := &failingFirewallOperationStore{}
	manager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	target := securityPolicySelfLink("project-a", "pending")
	pending, err := manager.RegisterDurable("compute#operation", "insert", target, "", "")
	if err != nil {
		t.Fatal(err)
	}

	restartedManager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	api := newAPI(restartedManager, nil, nil)
	backend := &fakeComputeNetworkBackend{}
	api.computeNetwork = backend
	poll := performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-a/global/operations/"+pending.Name, "")
	if poll.Code != http.StatusOK {
		t.Fatalf("poll status=%d body=%s", poll.Code, poll.Body.String())
	}
	var interrupted orchestrator.Operation
	decodeComputeResponse(t, poll, &interrupted)
	if !interrupted.Done || interrupted.Error == nil ||
		!strings.Contains(interrupted.Error.Message, "side effects were not replayed") {
		t.Fatalf("interrupted operation=%#v", interrupted)
	}
	assertComputeError(t, performComputeRequest(api, http.MethodGet,
		"/compute/v1/projects/project-b/global/operations/"+pending.Name, ""),
		http.StatusNotFound, "NOT_FOUND")
	if backend.provisionCalls != 0 || backend.reconcileCalls != 0 || backend.deleteCalls != 0 {
		t.Fatalf("operation restart replayed Docker work: %#v", backend)
	}
}
