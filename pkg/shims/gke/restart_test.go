package gke

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

type reconcileGKEBackend struct {
	mu             sync.Mutex
	createCalls    int
	reconcileCalls int
	exists         bool
	reconcileErr   error
}

func (*reconcileGKEBackend) Enabled() bool { return true }

func (b *reconcileGKEBackend) CreateClusterContext(context.Context, ClusterIdentity) (gkeBackendCreateResult, error) {
	b.mu.Lock()
	b.createCalls++
	b.mu.Unlock()
	return gkeBackendCreateResult{}, errors.New("restart must not replay creation")
}

func (*reconcileGKEBackend) DeleteClusterContext(context.Context, ClusterIdentity) error {
	return nil
}

func (b *reconcileGKEBackend) ReconcileClusterContext(
	_ context.Context,
	_ ClusterIdentity,
	_ *kubeconfigOwnership,
) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reconcileCalls++
	return b.exists, b.reconcileErr
}

func (b *reconcileGKEBackend) calls() (create, reconcile int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.createCalls, b.reconcileCalls
}

type failSaveStore struct {
	gkeStore
}

func (failSaveStore) Save(string, any) error { return errors.New("save failed") }

func TestRestartRestoresTerminalGKEOperationPolling(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart-operation")
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey("demo", "zone", "cluster")
	operationName := "operation-terminal"
	metadata := gkeMetadata{
		Backend: "simulation",
		Clusters: map[string]*Cluster{
			key: {Name: "cluster", Location: "zone", Status: "RUNNING"},
		},
		Operations: map[string]*GkeOperation{
			operationName: {
				Name: operationName, Zone: "zone", OperationType: "CREATE_CLUSTER",
				Status: "DONE", TargetLink: clusterTarget("demo", "zone", "cluster"),
			},
		},
	}
	metadata.OwnershipChecksum, err = kubeconfigOwnershipChecksum(
		map[string]*kubeconfigOwnership{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}

	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), store, staticGKEBackend{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	api.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
		"/v1/projects/demo/zones/zone/operations/"+operationName, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("poll status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var operation GkeOperation
	if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if operation.Status != "DONE" || operation.OperationType != "CREATE_CLUSTER" {
		t.Fatalf("restored operation=%#v", operation)
	}
}

func TestRestartNormalizesInterruptedOperationWithoutCreateReplay(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart-interrupted")
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey("demo", "zone", "cluster")
	operationName := "operation-interrupted"
	metadata := gkeMetadata{
		Backend: "kind",
		Clusters: map[string]*Cluster{
			key: {Name: "cluster", Location: "zone", Status: "PROVISIONING", Endpoint: "stale"},
		},
		Operations: map[string]*GkeOperation{
			operationName: {
				Name: operationName, Zone: "zone", OperationType: "CREATE_CLUSTER",
				Status: "RUNNING", TargetLink: clusterTarget("demo", "zone", "cluster"),
			},
		},
	}
	metadata.OwnershipChecksum, err = kubeconfigOwnershipChecksum(
		map[string]*kubeconfigOwnership{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	backend := &reconcileGKEBackend{}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), store, backend)
	if err != nil {
		t.Fatal(err)
	}
	if create, reconcile := backend.calls(); create != 0 || reconcile != 0 {
		t.Fatalf("backend calls create=%d reconcile=%d", create, reconcile)
	}
	cluster := api.clusters[key]
	if cluster.Status != "ERROR" || cluster.Endpoint != "" {
		t.Fatalf("normalized cluster=%#v", cluster)
	}
	operation := api.operations[operationName]
	if operation.Status != "DONE" || operation.Error == nil {
		t.Fatalf("normalized operation=%#v", operation)
	}

	var durable gkeMetadata
	if err := store.Load(gkeStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if durable.Operations[operationName].Status != "DONE" {
		t.Fatalf("durable operation=%#v", durable.Operations[operationName])
	}
}

func TestRestartReconcilesOnlyExactlyOwnedKindCluster(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "owned")
	store, err := state.New(t.TempDir(), "owned")
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey("demo", "zone", "cluster")
	ownership := &kubeconfigOwnership{
		Profile: "owned", Project: "demo", Zone: "zone", Cluster: "cluster",
		BackendName: "minisky-owned-0123456789abcdef0123456789abcdef",
		SHA256:      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Device:      1, Inode: 2,
	}
	metadata := gkeMetadata{
		Backend: "kind",
		Clusters: map[string]*Cluster{
			key: {Name: "cluster", Location: "zone", Status: "RUNNING", Endpoint: "127.0.0.1"},
		},
		Ownerships: map[string]*kubeconfigOwnership{key: ownership},
	}
	metadata.OwnershipChecksum, err = kubeconfigOwnershipChecksum(metadata.Ownerships)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	backend := &reconcileGKEBackend{exists: true}
	api, err := newAPIWithStoreAndBackend(orchestrator.NewOperationManager(), store, backend)
	if err != nil {
		t.Fatal(err)
	}
	if create, reconcile := backend.calls(); create != 0 || reconcile != 1 {
		t.Fatalf("backend calls create=%d reconcile=%d", create, reconcile)
	}
	if got := api.clusters[key]; got.Status != "RUNNING" {
		t.Fatalf("reconciled cluster=%#v", got)
	}
}

func TestRestartFailsClosedWhenNormalizationCannotBeSaved(t *testing.T) {
	store, err := state.New(t.TempDir(), "save-failure")
	if err != nil {
		t.Fatal(err)
	}
	metadata := gkeMetadata{
		Backend: "kind",
		Clusters: map[string]*Cluster{
			clusterKey("demo", "zone", "cluster"): {
				Name: "cluster", Location: "zone", Status: "PROVISIONING",
			},
		},
	}
	metadata.OwnershipChecksum, err = kubeconfigOwnershipChecksum(
		map[string]*kubeconfigOwnership{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	if _, err := newAPIWithStoreAndBackend(
		orchestrator.NewOperationManager(), failSaveStore{gkeStore: store}, &reconcileGKEBackend{},
	); err == nil {
		t.Fatal("restart accepted unsaved normalization")
	}
}

func TestConcurrentCreatesPersistRestartablePollingResults(t *testing.T) {
	store, err := state.New(t.TempDir(), "concurrent")
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(gateway.Close)
	manager := orchestrator.NewOperationManager()
	api := newAPIWithBackend(
		manager, &fakeGKEBackend{}, gateway.URL, gateway.Client(), store)

	const count = 4
	names := make(chan string, count)
	errs := make(chan error, count)
	var requests sync.WaitGroup
	for index := 0; index < count; index++ {
		requests.Add(1)
		go func(index int) {
			defer requests.Done()
			body := fmt.Sprintf(`{"cluster":{"name":"cluster-%d","initialNodeCount":1}}`, index)
			recorder := httptest.NewRecorder()
			api.ServeHTTP(recorder, httptest.NewRequest(
				http.MethodPost,
				"/v1/projects/demo/zones/zone/clusters",
				bytes.NewBufferString(body),
			))
			if recorder.Code != http.StatusOK {
				errs <- fmt.Errorf("create %d status=%d body=%s", index, recorder.Code, recorder.Body.String())
				return
			}
			var operation GkeOperation
			if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil {
				errs <- err
				return
			}
			names <- operation.Name
		}(index)
	}
	requests.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	close(names)
	var operationNames []string
	for name := range names {
		operationNames = append(operationNames, name)
	}
	if len(operationNames) != count {
		t.Fatalf("created operations=%d want=%d", len(operationNames), count)
	}
	for _, name := range operationNames {
		if operation := waitGKEOperation(t, manager, name); operation.Error != nil {
			t.Fatalf("operation %q failed: %#v", name, operation.Error)
		}
	}

	restarted, err := newAPIWithStoreAndBackend(
		orchestrator.NewOperationManager(), store, staticGKEBackend{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range operationNames {
		operation := restarted.operations[name]
		if operation == nil || operation.Status != "DONE" || operation.Error != nil {
			t.Fatalf("restored operation %q=%#v", name, operation)
		}
	}
}

func clusterTarget(project, zone, name string) string {
	return "https://container.googleapis.com/v1/projects/" + project +
		"/zones/" + zone + "/clusters/" + name
}
