package compute

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestInstanceRunAsyncTerminalPublicationSurvivesRestart(t *testing.T) {
	for _, test := range []struct {
		name           string
		commitTerminal bool
	}{
		{name: "exact post-commit readback", commitTerminal: true},
		{name: "unverified pre-commit failure", commitTerminal: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			operationStore := newRunAsyncTerminalStore(test.commitTerminal)
			opMgr, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			metadataStore := &toggleComputeStore{}
			api, err := newTestAPIWithMetadataStore(opMgr, metadataStore)
			if err != nil {
				t.Fatal(err)
			}
			addCustomNetwork(api, "project-a", "custom")
			subnet := persistedSubnetworkForTest(
				"project-a", "us-central1", "primary", "custom", "10.42.0.0/24", "1",
			)
			api.mu.Lock()
			api.subnetworks[subnetworkKey("project-a", "us-central1", "primary")] = subnet
			api.nextSubnetworkID = 2
			api.mu.Unlock()
			if err := api.persistMetadata(); err != nil {
				t.Fatal(err)
			}
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
			observed := make(chan *orchestrator.Operation, 1)
			unsubscribe := opMgr.OnTerminal(func(operation *orchestrator.Operation) { observed <- operation })
			defer unsubscribe()

			create := performComputeRequest(api, http.MethodPost,
				"/compute/v1/projects/project-a/zones/us-central1-a/instances",
				`{"name":"web","networkInterfaces":[{"subnetwork":"primary"}]}`)
			if create.Code != http.StatusOK {
				t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
			}
			var operation orchestrator.Operation
			decodeComputeResponse(t, create, &operation)
			operationStore.waitForTerminalSave(t)
			defer operationStore.releaseTerminalSave()

			pending := opMgr.Get(operation.Name)
			if pending == nil || pending.Done || pending.Status == orchestrator.StatusDone ||
				pending.Error != nil {
				t.Fatalf("RunAsync terminal candidate became visible before save: %+v", pending)
			}
			select {
			case notified := <-observed:
				t.Fatalf("RunAsync observer fired before durable save: %+v", notified)
			default:
			}
			poll := performComputeRequest(api, http.MethodGet,
				"/compute/v1/projects/project-a/zones/us-central1-a/operations/"+operation.Name, "")
			var polled orchestrator.Operation
			decodeComputeResponse(t, poll, &polled)
			if polled.Done || polled.Status == orchestrator.StatusDone {
				t.Fatalf("RunAsync poll published terminal candidate: %+v", polled)
			}
			if instance := api.instances[instanceKey("project-a", "us-central1-a", "web")]; instance == nil ||
				instance.Status != "RUNNING" || len(instance.NetworkInterfaces) != 1 {
				t.Fatalf("active Compute network mutation = %+v", instance)
			}

			operationStore.releaseTerminalSave()
			waitForRunAsyncPersistenceOutcome(t, opMgr, operation.Name, test.commitTerminal)
			if test.commitTerminal {
				select {
				case notified := <-observed:
					if !notified.Done || notified.Error != nil {
						t.Fatalf("RunAsync terminal notification = %+v", notified)
					}
				case <-time.After(time.Second):
					t.Fatal("RunAsync exact committed result was not notified")
				}
			} else {
				select {
				case notified := <-observed:
					t.Fatalf("unverified RunAsync result was notified: %+v", notified)
				default:
				}
			}

			restartedOps, err := orchestrator.NewOperationManagerWithStore(operationStore)
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := newTestAPIWithMetadataStore(restartedOps, metadataStore)
			if err != nil {
				t.Fatal(err)
			}
			restartPoll := performComputeRequest(restarted, http.MethodGet,
				"/compute/v1/projects/project-a/zones/us-central1-a/operations/"+operation.Name, "")
			if restartPoll.Code != http.StatusOK {
				t.Fatalf("restart poll status=%d body=%s", restartPoll.Code, restartPoll.Body.String())
			}
			var restartOperation orchestrator.Operation
			decodeComputeResponse(t, restartPoll, &restartOperation)
			if !restartOperation.Done || restartOperation.Status != orchestrator.StatusDone {
				t.Fatalf("restart operation = %+v", restartOperation)
			}
			if test.commitTerminal && restartOperation.Error != nil {
				t.Fatalf("exact committed RunAsync restart = %+v", restartOperation)
			}
			if !test.commitTerminal && (restartOperation.Error == nil ||
				restartOperation.Error.Message !=
					"operation interrupted by MiniSky restart; side effects were not replayed") {
				t.Fatalf("unverified RunAsync restart = %+v", restartOperation)
			}
			var persisted computeMetadata
			if err := metadataStore.Load(computeStateEntry, &persisted); err != nil {
				t.Fatal(err)
			}
			instance := persisted.Instances[instanceKey("project-a", "us-central1-a", "web")]
			if instance == nil || instance.Status != "RUNNING" || len(instance.NetworkInterfaces) != 1 {
				t.Fatalf("restarted Compute network metadata = %+v", instance)
			}
		})
	}
}

type runAsyncTerminalStore struct {
	mu             sync.Mutex
	data           []byte
	commitTerminal bool
	blocked        bool
	started        chan struct{}
	release        chan struct{}
	releaseOnce    sync.Once
}

func newRunAsyncTerminalStore(commitTerminal bool) *runAsyncTerminalStore {
	return &runAsyncTerminalStore{
		commitTerminal: commitTerminal,
		started:        make(chan struct{}),
		release:        make(chan struct{}),
	}
}

func (store *runAsyncTerminalStore) Load(_ string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.data, target)
}

func (store *runAsyncTerminalStore) Save(_ string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var operations map[string]orchestrator.Operation
	if err := json.Unmarshal(payload, &operations); err != nil {
		return err
	}
	terminal := false
	for _, operation := range operations {
		if operation.Done || operation.Status == orchestrator.StatusDone {
			terminal = true
			break
		}
	}

	store.mu.Lock()
	if !terminal || store.blocked {
		store.data = append(store.data[:0], payload...)
		store.mu.Unlock()
		return nil
	}
	store.blocked = true
	close(store.started)
	store.mu.Unlock()

	<-store.release
	store.mu.Lock()
	if store.commitTerminal {
		store.data = append(store.data[:0], payload...)
	}
	store.mu.Unlock()
	return errors.New("RunAsync terminal operation save failure")
}

func (store *runAsyncTerminalStore) waitForTerminalSave(t *testing.T) {
	t.Helper()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("RunAsync terminal operation save did not start")
	}
}

func (store *runAsyncTerminalStore) releaseTerminalSave() {
	store.releaseOnce.Do(func() { close(store.release) })
}

func waitForRunAsyncPersistenceOutcome(
	t *testing.T,
	manager *orchestrator.OperationManager,
	name string,
	committed bool,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		operation := manager.Get(name)
		if manager.PersistenceError() != nil &&
			((committed && operation != nil && operation.Done) ||
				(!committed && operation != nil && !operation.Done)) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RunAsync persistence outcome: operation=%+v degradation=%v",
		manager.Get(name), manager.PersistenceError())
}
