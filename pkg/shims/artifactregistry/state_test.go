package artifactregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestRepositoryMetadataSurvivesRestart(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}

	const name = "projects/test/locations/us/repositories/apps"
	created := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps",
		`{"format":"DOCKER","description":"images","labels":{"managed_by":"terraform"}}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", created.Code, created.Body.String())
	}
	var before Repository
	decodeResponse(t, serve(t, api, http.MethodGet, "/v1/"+name, ""), &before)

	restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	read := serve(t, restarted, http.MethodGet, "/v1/"+name, "")
	if read.Code != http.StatusOK {
		t.Fatalf("read after restart status = %d, body = %s", read.Code, read.Body.String())
	}
	var after Repository
	decodeResponse(t, read, &after)
	if encodedBefore, encodedAfter := mustJSON(t, before), mustJSON(t, after); encodedBefore != encodedAfter {
		t.Fatalf("repository changed across restart:\nbefore: %s\nafter:  %s", encodedBefore, encodedAfter)
	}

	listed := serve(t, restarted, http.MethodGet, "/v1/projects/test/locations/us/repositories", "")
	var list struct {
		Repositories []Repository `json:"repositories"`
	}
	decodeResponse(t, listed, &list)
	if len(list.Repositories) != 1 || list.Repositories[0].Name != name {
		t.Fatalf("repositories after restart = %#v", list.Repositories)
	}

	deleted := serve(t, restarted, http.MethodDelete, "/v1/"+name, "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleted.Code, deleted.Body.String())
	}
	afterDelete, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	missing := serve(t, afterDelete, http.MethodGet, "/v1/"+name, "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("read after delete restart status = %d, body = %s", missing.Code, missing.Body.String())
	}
	assertErrorStatus(t, missing, "NOT_FOUND")
}

func TestRepositoryErrorsRemainGCPShapedAfterRestart(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "errors")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/projects/test/locations/us/repositories?repositoryId=apps"
	if response := serve(t, api, http.MethodPost, path, `{"format":"DOCKER"}`); response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := serve(t, restarted, http.MethodPost, path, `{"format":"DOCKER"}`)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status = %d, body = %s", duplicate.Code, duplicate.Body.String())
	}
	assertErrorStatus(t, duplicate, "ALREADY_EXISTS")
	missing := serve(t, restarted, http.MethodGet,
		"/v1/projects/test/locations/us/repositories/missing", "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, body = %s", missing.Code, missing.Body.String())
	}
	assertErrorStatus(t, missing, "NOT_FOUND")
}

func TestRepositoryPersistenceFailureDoesNotMutateTruth(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	store.fail(artifactRegistryStateEntry)
	response := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	assertErrorStatus(t, response, "INTERNAL")
	assertRepositoryMissing(t, api, "projects/test/locations/us/repositories/apps")
	store.succeed(artifactRegistryStateEntry)
	restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	assertRepositoryMissing(t, restarted, "projects/test/locations/us/repositories/apps")
}

func TestRepositoryOperationRegistrationFailureRollsBackMetadata(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	store.fail("orchestrator/operations")
	response := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	assertErrorStatus(t, response, "INTERNAL")
	assertRepositoryMissing(t, api, "projects/test/locations/us/repositories/apps")
	if operations := manager.List(); len(operations) != 0 {
		t.Fatalf("operations after failed registration = %#v", operations)
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	assertRepositoryMissing(t, restarted, "projects/test/locations/us/repositories/apps")
}

func TestDeleteOperationRegistrationFailureRollsBackMetadata(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	const name = "projects/test/locations/us/repositories/apps"
	if response := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`); response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	api.opMgr = manager
	store.fail("orchestrator/operations")
	response := serve(t, api, http.MethodDelete, "/v1/"+name, "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	assertErrorStatus(t, response, "INTERNAL")
	if read := serve(t, api, http.MethodGet, "/v1/"+name, ""); read.Code != http.StatusOK {
		t.Fatalf("memory after failed operation registration = %d, body = %s", read.Code, read.Body.String())
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if read := serve(t, restarted, http.MethodGet, "/v1/"+name, ""); read.Code != http.StatusOK {
		t.Fatalf("disk after failed operation registration = %d, body = %s", read.Code, read.Body.String())
	}
}

func TestRestartDoesNotInferOperationOutcomeFromRepositoryExistence(t *testing.T) {
	t.Parallel()
	const name = "projects/test/locations/us/repositories/apps"

	for _, operationType := range []string{"CREATE", "DELETE"} {
		operationType := operationType
		t.Run(strings.ToLower(operationType), func(t *testing.T) {
			store, err := state.New(t.TempDir(), strings.ToLower(operationType))
			if err != nil {
				t.Fatal(err)
			}
			api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			if response := serve(t, api, http.MethodPost,
				"/v1/projects/test/locations/us/repositories?repositoryId=apps",
				`{"format":"DOCKER"}`); response.Code != http.StatusOK {
				t.Fatalf("create metadata status = %d, body = %s", response.Code, response.Body.String())
			}
			if operationType == "DELETE" {
				if response := serve(t, api, http.MethodDelete, "/v1/"+name, ""); response.Code != http.StatusOK {
					t.Fatalf("delete metadata status = %d, body = %s", response.Code, response.Body.String())
				}
			}

			beforeRestart, err := orchestrator.NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			op, err := beforeRestart.RegisterDurable(
				"artifactregistry#operation", operationType, name, "", "us")
			if err != nil {
				t.Fatal(err)
			}
			afterRestart, err := orchestrator.NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			if interrupted := afterRestart.Get(op.Name); interrupted == nil || interrupted.Error == nil {
				t.Fatalf("real operation manager did not mark pending operation interrupted: %#v", interrupted)
			}
			restarted, err := NewAPIWithRegistryIndexAndStore(afterRestart, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}

			response := serve(t, restarted, http.MethodGet,
				"/v1/projects/test/locations/us/operations/"+op.Name, "")
			if response.Code != http.StatusOK {
				t.Fatalf("poll after restart status = %d, body = %s", response.Code, response.Body.String())
			}
			var result map[string]any
			decodeResponse(t, response, &result)
			if done, _ := result["done"].(bool); !done || result["error"] == nil {
				t.Fatalf("operation without persisted outcome = %#v", result)
			}
			if _, ok := result["response"]; ok {
				t.Fatalf("operation inferred success from final repository state: %#v", result)
			}
		})
	}
}

func TestCreateDeleteRestartPreservesEachOperationOutcome(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "create-delete")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	const name = "projects/test/locations/us/repositories/apps"
	create := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
	createOperation := operationName(t, create)
	deleteResponse := serve(t, api, http.MethodDelete, "/v1/"+name, "")
	deleteOperation := operationName(t, deleteResponse)

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(restartedManager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	assertSuccessfulArtifactOperation(t, restarted, createOperation, true)
	assertSuccessfulArtifactOperation(t, restarted, deleteOperation, false)
	assertRepositoryMissing(t, restarted, name)
}

func TestDeleteRecreateRestartPreservesEachOperationOutcome(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "delete-recreate")
	if err != nil {
		t.Fatal(err)
	}
	seed, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	const name = "projects/test/locations/us/repositories/apps"
	if response := serve(t, seed, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`); response.Code != http.StatusOK {
		t.Fatalf("seed create status = %d, body = %s", response.Code, response.Body.String())
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	deleteResponse := serve(t, api, http.MethodDelete, "/v1/"+name, "")
	deleteOperation := operationName(t, deleteResponse)
	create := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
	createOperation := operationName(t, create)

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(restartedManager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	assertSuccessfulArtifactOperation(t, restarted, deleteOperation, false)
	assertSuccessfulArtifactOperation(t, restarted, createOperation, true)
	if read := serve(t, restarted, http.MethodGet, "/v1/"+name, ""); read.Code != http.StatusOK {
		t.Fatalf("recreated repository status = %d, body = %s", read.Code, read.Body.String())
	}
}

func TestCrashAfterOperationRegistrationLeavesMetadataUnchanged(t *testing.T) {
	t.Parallel()
	const name = "projects/test/locations/us/repositories/apps"
	for _, operationType := range []string{"CREATE", "DELETE"} {
		operationType := operationType
		t.Run(strings.ToLower(operationType), func(t *testing.T) {
			store, err := state.New(t.TempDir(), strings.ToLower(operationType))
			if err != nil {
				t.Fatal(err)
			}
			if operationType == "DELETE" {
				seed, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
				if err != nil {
					t.Fatal(err)
				}
				if response := serve(t, seed, http.MethodPost,
					"/v1/projects/test/locations/us/repositories?repositoryId=apps",
					`{"format":"DOCKER"}`); response.Code != http.StatusOK {
					t.Fatalf("seed create status = %d, body = %s", response.Code, response.Body.String())
				}
			}
			manager, err := orchestrator.NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			api.operationRunner = func(string) {}
			var registered string
			api.afterOperationRegistration = func(operation *orchestrator.Operation) error {
				registered = operation.Name
				return errors.New("simulated crash after durable registration")
			}

			var response *httptest.ResponseRecorder
			if operationType == "CREATE" {
				response = serve(t, api, http.MethodPost,
					"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
			} else {
				response = serve(t, api, http.MethodDelete, "/v1/"+name, "")
			}
			if response.Code != http.StatusInternalServerError || registered == "" {
				t.Fatalf("%s crash response = %d %s, operation = %q",
					operationType, response.Code, response.Body.String(), registered)
			}

			restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			interrupted := restartedManager.Get(registered)
			if interrupted == nil || interrupted.Error == nil ||
				!strings.Contains(interrupted.Error.Message, "interrupted") {
				t.Fatalf("registered operation after restart = %#v", interrupted)
			}
			restarted, err := NewAPIWithRegistryIndexAndStore(restartedManager, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			read := serve(t, restarted, http.MethodGet, "/v1/"+name, "")
			wantStatus := http.StatusNotFound
			if operationType == "DELETE" {
				wantStatus = http.StatusOK
			}
			if read.Code != wantStatus {
				t.Fatalf("%s repository after crash = %d, want %d, body = %s",
					operationType, read.Code, wantStatus, read.Body.String())
			}
		})
	}
}

func TestOperationOutcomeHistoryIsBoundedAndRecentPollingSurvives(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string) {}
	const (
		name      = "projects/test/locations/us/repositories/apps"
		mutations = maxPersistedOperationOutcomes + 32
	)
	type recentOperation struct {
		name           string
		wantRepository bool
	}
	var firstOperation string
	recent := make([]recentOperation, 0, 4)
	for i := 0; i < mutations; i++ {
		create := i%2 == 0
		var response *httptest.ResponseRecorder
		if create {
			response = serve(t, api, http.MethodPost,
				"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
		} else {
			response = serve(t, api, http.MethodDelete, "/v1/"+name, "")
		}
		serviceName := operationName(t, response)
		managerName := serviceName[strings.LastIndex(serviceName, "/")+1:]
		if i == 0 {
			firstOperation = managerName
		}
		if err := manager.AdvanceDurable(managerName, 100, orchestrator.StatusDone); err != nil {
			t.Fatal(err)
		}
		recent = append(recent, recentOperation{name: serviceName, wantRepository: create})
		if len(recent) > 4 {
			recent = recent[1:]
		}
	}
	waitForOutcomeCompaction(t, store, manager, maxPersistedOperationOutcomes)
	if got := store.saveCount(artifactRegistryStateEntry); got != mutations {
		if got < mutations {
			t.Fatalf("artifact metadata saves = %d, want at least one per mutation (%d)", got, mutations)
		}
	}
	var persisted artifactRegistryMetadata
	if err := store.Load(artifactRegistryStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Outcomes) > maxPersistedOperationOutcomes {
		t.Fatalf("persisted outcomes = %d, maximum = %d",
			len(persisted.Outcomes), maxPersistedOperationOutcomes)
	}
	if _, retained := persisted.Outcomes[firstOperation]; retained {
		t.Fatalf("oldest terminal outcome %q was not evicted", firstOperation)
	}

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(restartedManager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range recent {
		assertSuccessfulArtifactOperation(t, restarted, operation.name, operation.wantRepository)
	}
	expired := serve(t, restarted, http.MethodGet,
		"/v1/projects/test/locations/us/operations/"+firstOperation, "")
	if expired.Code != http.StatusNotFound {
		t.Fatalf("expired operation status = %d, body = %s", expired.Code, expired.Body.String())
	}
	assertErrorStatus(t, expired, "NOT_FOUND")
}

func TestMemoryOnlyOperationOutcomeHistoryIsBounded(t *testing.T) {
	t.Parallel()
	manager := orchestrator.NewOperationManager()
	api := NewAPIWithRegistryIndex(manager, nil, nil)
	api.operationRunner = func(string) {}
	const (
		name      = "projects/test/locations/us/repositories/apps"
		mutations = maxPersistedOperationOutcomes + 32
	)
	type recentOperation struct {
		name           string
		wantRepository bool
	}
	var firstOperation string
	recent := make([]recentOperation, 0, 4)
	for i := 0; i < mutations; i++ {
		create := i%2 == 0
		var response *httptest.ResponseRecorder
		if create {
			response = serve(t, api, http.MethodPost,
				"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
		} else {
			response = serve(t, api, http.MethodDelete, "/v1/"+name, "")
		}
		serviceName := operationName(t, response)
		managerName := serviceName[strings.LastIndex(serviceName, "/")+1:]
		if i == 0 {
			firstOperation = serviceName
		}
		if err := manager.AdvanceDurable(managerName, 100, orchestrator.StatusDone); err != nil {
			t.Fatal(err)
		}
		recent = append(recent, recentOperation{name: serviceName, wantRepository: create})
		if len(recent) > 4 {
			recent = recent[1:]
		}
	}
	waitForMemoryOutcomeCompaction(t, api, manager, maxPersistedOperationOutcomes)
	for _, operation := range recent {
		assertSuccessfulArtifactOperation(t, api, operation.name, operation.wantRepository)
	}
	expired := serve(t, api, http.MethodGet, "/v1/"+firstOperation, "")
	if expired.Code != http.StatusNotFound {
		t.Fatalf("expired memory-only operation status = %d, body = %s",
			expired.Code, expired.Body.String())
	}
	assertErrorStatus(t, expired, "NOT_FOUND")
}

func TestMixedServiceSaturationStillCompactsArtifactOutcomes(t *testing.T) {
	t.Parallel()
	manager := orchestrator.NewOperationManager()
	entered := make(chan struct{})
	release := make(chan struct{})
	blockerSubscription := manager.ObserveTerminal(func(*orchestrator.Operation) {
		select {
		case <-entered:
		default:
			close(entered)
			<-release
		}
	})
	defer blockerSubscription.Shutdown(context.Background())
	api := NewAPIWithRegistryIndex(manager, nil, nil)
	defer api.Shutdown(context.Background())
	api.operationRunner = func(string) {}

	blocker := manager.Register("compute#operation", "insert", "instances/blocker", "", "")
	if err := manager.AdvanceDurable(blocker.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("mixed-service blocker was not dispatched")
	}

	const name = "projects/test/locations/us/repositories/apps"
	for i := 0; i <= maxPersistedOperationOutcomes; i++ {
		var response *httptest.ResponseRecorder
		if i%2 == 0 {
			response = serve(t, api, http.MethodPost,
				"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
		} else {
			response = serve(t, api, http.MethodDelete, "/v1/"+name, "")
		}
		serviceName := operationName(t, response)
		managerName := serviceName[strings.LastIndex(serviceName, "/")+1:]
		if err := manager.AdvanceDurable(managerName, 100, orchestrator.StatusDone); err != nil {
			t.Fatal(err)
		}
		other := manager.Register("compute#operation", "insert", fmt.Sprintf("instances/%d", i), "", "")
		if err := manager.AdvanceDurable(other.Name, 100, orchestrator.StatusDone); err != nil {
			t.Fatal(err)
		}
	}
	waitForMemoryOutcomeCompaction(t, api, manager, maxPersistedOperationOutcomes)
	close(release)
}

func TestArtifactShutdownRemovesTerminalObserver(t *testing.T) {
	t.Parallel()
	manager := orchestrator.NewOperationManager()
	var staleCallbacks atomic.Int64
	for range 64 {
		api := NewAPIWithRegistryIndex(manager, nil, nil)
		api.compactionHook = func() { staleCallbacks.Add(1) }
		if err := api.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := api.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	dispatched := make(chan struct{})
	unsubscribe := manager.OnTerminal(func(*orchestrator.Operation) { close(dispatched) })
	defer unsubscribe()
	op := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
	if err := manager.AdvanceDurable(op.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("terminal observer sentinel was not dispatched")
	}
	if got := staleCallbacks.Load(); got != 0 {
		t.Fatalf("disposed Artifact Registry callbacks invoked = %d", got)
	}
}

func TestArtifactObserverReplacementUnsubscribesPrevious(t *testing.T) {
	t.Parallel()
	manager := orchestrator.NewOperationManager()
	api := NewAPIWithRegistryIndex(manager, nil, nil)
	defer api.Shutdown(context.Background())
	var callbacks atomic.Int64
	callbackEntered := make(chan struct{})
	api.compactionHook = func() {
		callbacks.Add(1)
		close(callbackEntered)
	}
	for range 64 {
		if err := api.observeTerminalOperations(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	op := manager.Register("compute#operation", "insert", "instances/apps", "", "")
	if err := manager.AdvanceDurable(op.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("replacement observer was not dispatched")
	}
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := callbacks.Load(); got != 1 {
		t.Fatalf("callbacks after observer replacement = %d, want 1", got)
	}
}

func TestArtifactObserverReplacementWaitsForDequeuedCallback(t *testing.T) {
	t.Parallel()
	manager := orchestrator.NewOperationManager()
	api := NewAPIWithRegistryIndex(manager, nil, nil)
	defer api.Shutdown(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	api.compactionHook = func() {
		close(entered)
		<-release
	}
	operation := manager.Register("compute#operation", "insert", "instances/apps", "", "")
	if err := manager.AdvanceDurable(operation.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Artifact callback was not dequeued")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := api.observeTerminalOperations(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("replacement error = %v, want context deadline exceeded", err)
	}
	close(release)
	if err := api.observeTerminalOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	var replacementCallbacks atomic.Int64
	callbackDone := make(chan struct{}, 2)
	api.compactionHook = func() {
		replacementCallbacks.Add(1)
		callbackDone <- struct{}{}
	}
	operation = manager.Register("compute#operation", "insert", "instances/replacement", "", "")
	if err := manager.AdvanceDurable(operation.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("replacement terminal observer was not dispatched")
	}
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := replacementCallbacks.Load(); got != 1 {
		t.Fatalf("replacement callbacks = %d, want 1", got)
	}
}

func TestArtifactShutdownWaitsForDequeuedCompaction(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var blockOnce sync.Once
	api.compactionHook = func() {
		blockOnce.Do(func() {
			close(entered)
			<-release
		})
	}

	for sequence := 1; sequence <= maxPersistedOperationOutcomes+1; sequence++ {
		operation := manager.Register("artifactregistry#operation", "CREATE", "repositories/apps", "", "")
		if err := manager.AdvanceDurable(operation.Name, 100, orchestrator.StatusDone); err != nil {
			t.Fatal(err)
		}
		api.mu.Lock()
		api.outcomes[operation.Name] = artifactOperationOutcome{
			OperationType: "CREATE",
			Target:        "projects/test/locations/us/repositories/apps",
			Sequence:      uint64(sequence),
		}
		api.nextOutcomeSequence = uint64(sequence)
		api.mu.Unlock()
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Artifact compaction callback was not dequeued")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := api.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want context deadline exceeded", err)
	}
	close(release)
	if err := api.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	savesAfterShutdown := store.saveCount(artifactRegistryStateEntry)
	if savesAfterShutdown == 0 {
		t.Fatal("dequeued compaction did not persist before Shutdown returned")
	}

	dispatched := make(chan struct{})
	unsubscribe := manager.OnTerminal(func(*orchestrator.Operation) { close(dispatched) })
	defer unsubscribe()
	operation := manager.Register("compute#operation", "insert", "instances/apps", "", "")
	if err := manager.AdvanceDurable(operation.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("terminal observer sentinel was not dispatched")
	}
	if got := store.saveCount(artifactRegistryStateEntry); got != savesAfterShutdown {
		t.Fatalf("Artifact metadata persisted after successful Shutdown: saves=%d, want %d",
			got, savesAfterShutdown)
	}
}

func TestArtifactLifecycleGateHonorsSecondCallerContext(t *testing.T) {
	t.Parallel()
	manager := orchestrator.NewOperationManager()
	api := NewAPIWithRegistryIndex(manager, nil, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	api.compactionHook = func() {
		close(entered)
		<-release
	}
	operation := manager.Register("compute#operation", "insert", "instances/apps", "", "")
	if err := manager.AdvanceDurable(operation.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Artifact callback was not dequeued")
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- api.Shutdown(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for {
		api.mu.RLock()
		closed := api.closed
		api.mu.RUnlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first Shutdown did not acquire lifecycle serialization")
		}
		runtime.Gosched()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondDone := make(chan error, 1)
	go func() { secondDone <- api.Shutdown(ctx) }()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("second Shutdown error = %v, want context canceled", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(release)
		<-firstDone
		t.Fatal("second Shutdown blocked behind lifecycle serialization after context cancellation")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestPendingOperationOutcomesAreNeverEvicted(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string) {}
	const name = "projects/test/locations/us/repositories/apps"
	var firstOperation string
	for i := 0; i <= maxPersistedOperationOutcomes; i++ {
		var response *httptest.ResponseRecorder
		if i%2 == 0 {
			response = serve(t, api, http.MethodPost,
				"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
		} else {
			response = serve(t, api, http.MethodDelete, "/v1/"+name, "")
		}
		serviceName := operationName(t, response)
		if i == 0 {
			firstOperation = serviceName[strings.LastIndex(serviceName, "/")+1:]
		}
	}
	var persisted artifactRegistryMetadata
	if err := store.Load(artifactRegistryStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if got, want := len(persisted.Outcomes), maxPersistedOperationOutcomes+1; got != want {
		t.Fatalf("pending outcomes = %d, want %d retained beyond terminal cap", got, want)
	}
	if _, retained := persisted.Outcomes[firstOperation]; !retained {
		t.Fatalf("oldest pending outcome %q was evicted", firstOperation)
	}

	for _, operation := range manager.List() {
		if err := manager.AdvanceDurable(operation.Name, 100, orchestrator.StatusDone); err != nil {
			t.Fatal(err)
		}
	}
	waitForOutcomeCompaction(t, store, manager, maxPersistedOperationOutcomes)
	persisted = artifactRegistryMetadata{}
	if err := store.Load(artifactRegistryStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Outcomes) > maxPersistedOperationOutcomes {
		t.Fatalf("terminal overflow did not compact without a later mutation: %d", len(persisted.Outcomes))
	}
}

func TestStartupCompactsTerminalOutcomeOverflow(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string) {}
	const name = "projects/test/locations/us/repositories/apps"
	for i := 0; i <= maxPersistedOperationOutcomes; i++ {
		if i%2 == 0 {
			operationName(t, serve(t, api, http.MethodPost,
				"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`))
		} else {
			operationName(t, serve(t, api, http.MethodDelete, "/v1/"+name, ""))
		}
	}

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(restartedManager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	var persisted artifactRegistryMetadata
	if err := store.Load(artifactRegistryStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Outcomes) > maxPersistedOperationOutcomes ||
		len(restartedManager.List()) > maxPersistedOperationOutcomes {
		t.Fatalf("startup retention outcomes=%d operations=%d",
			len(persisted.Outcomes), len(restartedManager.List()))
	}
	if read := serve(t, restarted, http.MethodGet, "/v1/"+name, ""); read.Code != http.StatusOK {
		t.Fatalf("startup compaction changed repository state: %d %s", read.Code, read.Body.String())
	}
}

func TestAmbiguousMetadataSaveUsesReadback(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string) {}
	store.failAfterCommit(artifactRegistryStateEntry)

	response := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
	serviceName := operationName(t, response)
	if read := serve(t, api, http.MethodGet,
		"/v1/projects/test/locations/us/repositories/apps", ""); read.Code != http.StatusOK {
		t.Fatalf("read after ambiguous commit = %d, body = %s", read.Code, read.Body.String())
	}
	managerName := serviceName[strings.LastIndex(serviceName, "/")+1:]
	if operation := manager.Get(managerName); operation == nil || operation.Error != nil {
		t.Fatalf("operation after confirmed commit = %#v", operation)
	}
}

func TestMetadataSaveFailureDurablyFailsRegisteredOperation(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(manager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string) {}
	store.failFutureSave(artifactRegistryStateEntry, 1)

	response := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	operations := manager.List()
	if len(operations) != 1 || !operations[0].Done || operations[0].Error == nil {
		t.Fatalf("operation after metadata failure = %#v", operations)
	}
	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	persisted := restartedManager.Get(operations[0].Name)
	if persisted == nil || !persisted.Done || persisted.Error == nil {
		t.Fatalf("persisted failed operation = %#v", persisted)
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(restartedManager, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	assertRepositoryMissing(t, restarted, "projects/test/locations/us/repositories/apps")
}

func TestMetadataAndOperationFailureDegradesRuntime(t *testing.T) {
	t.Parallel()
	const name = "projects/test/locations/us/repositories/apps"
	for _, operationType := range []string{"CREATE", "DELETE"} {
		operationType := operationType
		t.Run(strings.ToLower(operationType), func(t *testing.T) {
			store := newArtifactStateStore()
			manager, err := orchestrator.NewOperationManagerWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			if operationType == "DELETE" {
				if response := serve(t, api, http.MethodPost,
					"/v1/projects/test/locations/us/repositories?repositoryId=apps",
					`{"format":"DOCKER"}`); response.Code != http.StatusOK {
					t.Fatalf("setup create status = %d, body = %s", response.Code, response.Body.String())
				}
			}
			api.opMgr = manager
			store.failFutureSave(artifactRegistryStateEntry, 1)
			store.failFutureSave("orchestrator/operations", 2)

			var response *httptest.ResponseRecorder
			if operationType == "CREATE" {
				response = serve(t, api, http.MethodPost,
					"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`)
			} else {
				response = serve(t, api, http.MethodDelete, "/v1/"+name, "")
			}
			if response.Code != http.StatusInternalServerError {
				t.Fatalf("%s status = %d, body = %s", operationType, response.Code, response.Body.String())
			}
			assertErrorStatus(t, response, "INTERNAL")

			api.mu.RLock()
			_, memoryExists := api.repos[name]
			api.mu.RUnlock()
			wantExists := operationType == "DELETE"
			if memoryExists != wantExists {
				t.Fatalf("memory truth after failed rollback exists = %t, want %t", memoryExists, wantExists)
			}
			blocked := serve(t, api, http.MethodGet, "/v1/"+name, "")
			if blocked.Code != http.StatusServiceUnavailable {
				t.Fatalf("degraded runtime status = %d, body = %s", blocked.Code, blocked.Body.String())
			}
			assertErrorStatus(t, blocked, "FAILED_PRECONDITION")

			restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
			if err != nil {
				t.Fatal(err)
			}
			read := serve(t, restarted, http.MethodGet, "/v1/"+name, "")
			wantStatus := http.StatusNotFound
			if wantExists {
				wantStatus = http.StatusOK
			}
			if read.Code != wantStatus {
				t.Fatalf("durable disk truth after restart = %d, want %d, body = %s",
					read.Code, wantStatus, read.Body.String())
			}
		})
	}
}

func TestFailedDeletePersistenceLeavesRepositoryIntact(t *testing.T) {
	t.Parallel()
	store := newArtifactStateStore()
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	const name = "projects/test/locations/us/repositories/apps"
	if response := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`); response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	store.fail(artifactRegistryStateEntry)
	response := serve(t, api, http.MethodDelete, "/v1/"+name, "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	store.succeed(artifactRegistryStateEntry)
	if read := serve(t, api, http.MethodGet, "/v1/"+name, ""); read.Code != http.StatusOK {
		t.Fatalf("memory after failed delete = %d, body = %s", read.Code, read.Body.String())
	}
	restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if read := serve(t, restarted, http.MethodGet, "/v1/"+name, ""); read.Code != http.StatusOK {
		t.Fatalf("disk after failed delete = %d, body = %s", read.Code, read.Body.String())
	}
}

func TestArtifactRegistryExportImportContainsOnlyRepositoryMetadata(t *testing.T) {
	t.Parallel()
	source, err := state.New(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil,
		staticRegistryIndex{repositories: []string{"apps/team/api"}, tags: []string{"latest"}}, source)
	if err != nil {
		t.Fatal(err)
	}
	if response := serve(t, api, http.MethodPost,
		"/v1/projects/test/locations/us/repositories?repositoryId=apps", `{"format":"DOCKER"}`); response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}

	var exported bytes.Buffer
	if err := source.Export(&exported); err != nil {
		t.Fatal(err)
	}
	var snapshot state.Snapshot
	if err := json.Unmarshal(exported.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Entries) != 1 || snapshot.Entries[artifactRegistryStateEntry] == nil {
		t.Fatalf("exported entries = %#v", snapshot.Entries)
	}
	payload := string(snapshot.Entries[artifactRegistryStateEntry])
	for _, excluded := range []string{"packages", "versions", "blobs", "manifests", "team/api", "latest"} {
		if strings.Contains(payload, excluded) {
			t.Fatalf("metadata snapshot contains Registry v2 data %q: %s", excluded, payload)
		}
	}

	destination, err := state.New(t.TempDir(), "destination")
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.Import(bytes.NewReader(exported.Bytes())); err != nil {
		t.Fatal(err)
	}
	imported, err := NewAPIWithRegistryIndexAndStore(nil, nil,
		staticRegistryIndex{repositories: []string{"apps/imported"}, tags: []string{"v2"}}, destination)
	if err != nil {
		t.Fatal(err)
	}
	if read := serve(t, imported, http.MethodGet,
		"/v1/projects/test/locations/us/repositories/apps", ""); read.Code != http.StatusOK {
		t.Fatalf("imported repository = %d, body = %s", read.Code, read.Body.String())
	}
	packages := serve(t, imported, http.MethodGet,
		"/v1/projects/test/locations/us/repositories/apps/packages", "")
	if packages.Code != http.StatusOK || !strings.Contains(packages.Body.String(), "imported") {
		t.Fatalf("packages did not derive from destination Registry v2: %d %s", packages.Code, packages.Body.String())
	}
}

func TestEmptyArtifactRegistryStateIsBackwardCompatible(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	response := serve(t, api, http.MethodGet, "/v1/projects/test/locations/us/repositories", "")
	if response.Code != http.StatusOK || response.Body.String() != "{\"repositories\":[]}\n" {
		t.Fatalf("empty list = %d %s", response.Code, response.Body.String())
	}

	if err := store.Save(artifactRegistryStateEntry, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	api, err = NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatalf("load legacy empty metadata: %v", err)
	}
	response = serve(t, api, http.MethodGet, "/v1/projects/test/locations/us/repositories", "")
	if response.Code != http.StatusOK || response.Body.String() != "{\"repositories\":[]}\n" {
		t.Fatalf("legacy empty list = %d %s", response.Code, response.Body.String())
	}
}

func TestConcurrentRepositoryPersistenceSurvivesRestart(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "concurrent")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}

	const repositories = 8
	statuses := make(chan int, repositories)
	var requests sync.WaitGroup
	for i := 0; i < repositories; i++ {
		requests.Add(1)
		go func(id int) {
			defer requests.Done()
			response := serve(t, api, http.MethodPost,
				fmt.Sprintf("/v1/projects/test/locations/us/repositories?repositoryId=repo-%d", id),
				`{"format":"DOCKER"}`)
			statuses <- response.Code
		}(i)
	}
	requests.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("concurrent create status = %d", status)
		}
	}

	restarted, err := NewAPIWithRegistryIndexAndStore(nil, nil, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	response := serve(t, restarted, http.MethodGet, "/v1/projects/test/locations/us/repositories", "")
	var listed struct {
		Repositories []Repository `json:"repositories"`
	}
	decodeResponse(t, response, &listed)
	if len(listed.Repositories) != repositories {
		t.Fatalf("repositories after concurrent restart = %d, want %d", len(listed.Repositories), repositories)
	}
}

type staticRegistryIndex struct {
	repositories []string
	tags         []string
}

func (index staticRegistryIndex) Repositories(context.Context) ([]string, error) {
	return append([]string(nil), index.repositories...), nil
}

func (index staticRegistryIndex) Tags(context.Context, string) ([]string, error) {
	return append([]string(nil), index.tags...), nil
}

type artifactStateStore struct {
	mu                 sync.Mutex
	entries            map[string]json.RawMessage
	failures           map[string]bool
	saveRuns           map[string]int
	failRuns           map[string]map[int]bool
	postCommitFailures map[string]bool
}

func newArtifactStateStore() *artifactStateStore {
	return &artifactStateStore{
		entries:            make(map[string]json.RawMessage),
		failures:           make(map[string]bool),
		saveRuns:           make(map[string]int),
		failRuns:           make(map[string]map[int]bool),
		postCommitFailures: make(map[string]bool),
	}
}

func (store *artifactStateStore) Load(name string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	payload := store.entries[name]
	if payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(payload, target)
}

func (store *artifactStateStore) Save(name string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveRuns[name]++
	if store.failures[name] || store.failRuns[name][store.saveRuns[name]] {
		return errors.New("injected save failure")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	store.entries[name] = payload
	if store.postCommitFailures[name] {
		return errors.New("injected post-commit save failure")
	}
	return nil
}

func (store *artifactStateStore) fail(name string) {
	store.mu.Lock()
	store.failures[name] = true
	store.mu.Unlock()
}

func (store *artifactStateStore) failFutureSave(name string, offset int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failRuns[name] == nil {
		store.failRuns[name] = make(map[int]bool)
	}
	store.failRuns[name][store.saveRuns[name]+offset] = true
}

func (store *artifactStateStore) failAfterCommit(name string) {
	store.mu.Lock()
	store.postCommitFailures[name] = true
	store.mu.Unlock()
}

func (store *artifactStateStore) succeed(name string) {
	store.mu.Lock()
	delete(store.failures, name)
	store.mu.Unlock()
}

func (store *artifactStateStore) saveCount(name string) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saveRuns[name]
}

func waitForOutcomeCompaction(t *testing.T, store *artifactStateStore, manager *orchestrator.OperationManager, maximum int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var persisted artifactRegistryMetadata
		err := store.Load(artifactRegistryStateEntry, &persisted)
		if err == nil && len(persisted.Outcomes) <= maximum && len(manager.List()) <= maximum {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("outcome compaction timed out: outcomes=%d operations=%d err=%v",
				len(persisted.Outcomes), len(manager.List()), err)
		}
		<-ticker.C
	}
}

func waitForMemoryOutcomeCompaction(t *testing.T, api *API, manager *orchestrator.OperationManager, maximum int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		api.mu.RLock()
		outcomes := len(api.outcomes)
		api.mu.RUnlock()
		artifactOperations := artifactOperationCount(manager)
		if outcomes <= maximum && artifactOperations <= maximum {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("memory outcome compaction timed out: outcomes=%d operations=%d",
				outcomes, artifactOperations)
		}
		<-ticker.C
	}
}

func artifactOperationCount(manager *orchestrator.OperationManager) int {
	count := 0
	for _, operation := range manager.List() {
		if operation != nil && operation.Kind == "artifactregistry#operation" {
			count++
		}
	}
	return count
}

func assertRepositoryMissing(t *testing.T, api *API, name string) {
	t.Helper()
	response := serve(t, api, http.MethodGet, "/v1/"+name, "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("repository %q status = %d, body = %s", name, response.Code, response.Body.String())
	}
	assertErrorStatus(t, response, "NOT_FOUND")
}

func operationName(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("operation response status = %d, body = %s", response.Code, response.Body.String())
	}
	var operation struct {
		Name string `json:"name"`
		Done bool   `json:"done"`
	}
	decodeResponse(t, response, &operation)
	if operation.Name == "" || operation.Done {
		t.Fatalf("initial operation = %#v", operation)
	}
	return operation.Name
}

func assertSuccessfulArtifactOperation(t *testing.T, api *API, name string, wantRepository bool) {
	t.Helper()
	response := serve(t, api, http.MethodGet, "/v1/"+name, "")
	if response.Code != http.StatusOK {
		t.Fatalf("poll %q status = %d, body = %s", name, response.Code, response.Body.String())
	}
	var operation map[string]any
	decodeResponse(t, response, &operation)
	if done, _ := operation["done"].(bool); !done || operation["error"] != nil {
		t.Fatalf("operation %q = %#v", name, operation)
	}
	result, ok := operation["response"].(map[string]any)
	if !ok {
		t.Fatalf("operation %q response = %#v", name, operation["response"])
	}
	_, hasRepository := result["name"]
	if hasRepository != wantRepository {
		t.Fatalf("operation %q repository response = %t, want %t: %#v",
			name, hasRepository, wantRepository, operation)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}
