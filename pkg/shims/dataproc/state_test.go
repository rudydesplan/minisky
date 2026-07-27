package dataproc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestDataprocStateOpenFailureIsUnavailableAndHealthyDegraded(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "../invalid")
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	api := NewAPI(orchestrator.NewOperationManager(), nil)
	if api.PersistenceError() == nil {
		t.Fatal("PersistenceError is nil")
	}
	response := dataprocRequest(api, http.MethodGet, "/v1/projects/test/regions/us/clusters", "")
	assertDataprocError(t, response, http.StatusServiceUnavailable, "UNAVAILABLE")
}

func TestDataprocRestartNormalizesTransientResourcesWithoutBackendReplay(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	seed := dataprocMetadata{
		Clusters: map[string]*Cluster{
			clusterKey("test", "us", "cluster"): {
				ProjectId: "test", ClusterName: "cluster", ClusterUuid: "uuid",
				Status: ClusterStatus{State: "RUNNING"},
			},
		},
		Jobs: map[string]*Job{
			jobKey("test", "us", "job-1"): {
				Reference: JobReference{ProjectId: "test", JobId: "job-1"},
				Status:    JobStatus{State: "RUNNING"},
			},
		},
	}
	if err := store.Save(dataprocStateEntry, seed); err != nil {
		t.Fatal(err)
	}
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}

	clusterResponse := dataprocRequest(api, http.MethodGet,
		"/v1/projects/test/regions/us/clusters/cluster", "")
	var cluster Cluster
	decodeDataprocResponse(t, clusterResponse, &cluster)
	if cluster.Status.State != "ERROR" || cluster.Status.Detail == "" {
		t.Fatalf("restored cluster status = %#v", cluster.Status)
	}
	jobResponse := dataprocRequest(api, http.MethodGet,
		"/v1/projects/test/regions/us/jobs/job-1", "")
	var job Job
	decodeDataprocResponse(t, jobResponse, &job)
	if job.Status.State != "ERROR" || job.Status.Details == "" {
		t.Fatalf("restored job status = %#v", job.Status)
	}
	if backend.provisions != 0 || backend.deletes != 0 || backend.commands != 0 {
		t.Fatalf("restart replayed backend calls: %#v", backend)
	}
}

func TestDataprocCreateAndDeleteSurviveRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "crud")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(manager, &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string, func() error) {}
	create := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters",
		`{"clusterName":"cluster","config":{"workerConfig":{"numInstances":0}}}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create = %d, body = %s", create.Code, create.Body.String())
	}
	restarted, err := NewAPIWithStore(manager, &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if read := dataprocRequest(restarted, http.MethodGet,
		"/v1/projects/test/regions/us/clusters/cluster", ""); read.Code != http.StatusOK {
		t.Fatalf("read after restart = %d, body = %s", read.Code, read.Body.String())
	}
	restarted.operationRunner = func(_ string, work func() error) {
		if err := work(); err != nil {
			t.Errorf("delete work: %v", err)
		}
	}
	deleted := dataprocRequest(restarted, http.MethodDelete,
		"/v1/projects/test/regions/us/clusters/cluster", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete = %d, body = %s", deleted.Code, deleted.Body.String())
	}
	afterDelete, err := NewAPIWithStore(manager, &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if read := dataprocRequest(afterDelete, http.MethodGet,
		"/v1/projects/test/regions/us/clusters/cluster", ""); read.Code != http.StatusNotFound {
		t.Fatalf("read after delete restart = %d, body = %s", read.Code, read.Body.String())
	}
}

func TestDataprocSaveFailureReturnsGCPErrorAndRollsBack(t *testing.T) {
	store := newDataprocFailingStore()
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string, func() error) {}
	store.fail = true
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters", `{"clusterName":"cluster"}`)
	assertDataprocError(t, response, http.StatusInternalServerError, "INTERNAL")
	api.mu.RLock()
	count := len(api.clusters)
	api.mu.RUnlock()
	if count != 0 {
		t.Fatalf("clusters after failed save = %d", count)
	}
}

func TestDataprocAmbiguousSaveReadbackReconcilesTruth(t *testing.T) {
	for _, test := range []struct {
		name            string
		configure       func(*dataprocFailingStore)
		wantStatus      int
		wantCluster     bool
		wantPersistence bool
	}{
		{
			name: "candidate committed",
			configure: func(store *dataprocFailingStore) {
				store.failAfterCommit = true
			},
			wantStatus: http.StatusOK, wantCluster: true,
		},
		{
			name: "previous preserved",
			configure: func(store *dataprocFailingStore) {
				store.fail = true
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "unknown degrades",
			configure: func(store *dataprocFailingStore) {
				store.fail = true
				store.loadErr = errors.New("readback unavailable")
			},
			wantStatus: http.StatusInternalServerError, wantPersistence: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newDataprocFailingStore()
			api, err := NewAPIWithStore(orchestrator.NewOperationManager(), &dataprocBackendSpy{}, store)
			if err != nil {
				t.Fatal(err)
			}
			api.operationRunner = func(string, func() error) {}
			test.configure(store)
			response := dataprocRequest(api, http.MethodPost,
				"/v1/projects/test/regions/us/clusters", `{"clusterName":"cluster"}`)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.wantStatus, response.Body.String())
			}
			api.mu.RLock()
			_, exists := api.clusters[clusterKey("test", "us", "cluster")]
			api.mu.RUnlock()
			if exists != test.wantCluster || (api.PersistenceError() != nil) != test.wantPersistence {
				t.Fatalf("cluster=%t persistence=%v", exists, api.PersistenceError())
			}
		})
	}
}

func TestDataprocAdmittedMutationRechecksDegradationAfterLock(t *testing.T) {
	base := newDataprocFailingStore()
	store := &dataprocInterleavingStore{
		dataprocFailingStore: base,
		saveEntered:          make(chan struct{}),
		releaseSave:          make(chan struct{}),
	}
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string, func() error) {}
	admitted := make(chan struct{}, 2)
	api.afterAdmission = func() { admitted <- struct{}{} }

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- dataprocRequest(api, http.MethodPost,
			"/v1/projects/test/regions/us/clusters", `{"clusterName":"first"}`)
	}()
	<-admitted
	<-store.saveEntered

	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		secondDone <- dataprocRequest(api, http.MethodPost,
			"/v1/projects/test/regions/us/clusters", `{"clusterName":"second"}`)
	}()
	<-admitted
	close(store.releaseSave)

	assertDataprocError(t, <-firstDone, http.StatusInternalServerError, "INTERNAL")
	assertDataprocError(t, <-secondDone, http.StatusServiceUnavailable, "UNAVAILABLE")
	if got := len(manager.List()); got != 1 {
		t.Fatalf("operations = %d, want only first admitted mutation", got)
	}
}

func TestDataprocCorruptStateFailsClosed(t *testing.T) {
	store := newDataprocFailingStore()
	store.entries[dataprocStateEntry] = json.RawMessage(`{"clusters":{"test:us:cluster":null}}`)
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store); err == nil {
		t.Fatal("corrupt metadata loaded without error")
	}
}

func TestDataprocBackendFailuresBecomeDurableErrorOutcomes(t *testing.T) {
	store, err := state.New(t.TempDir(), "backend-error")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &dataprocBackendSpy{provisionErr: errors.New("docker unavailable")}
	api, err := NewAPIWithStore(manager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		err := work()
		if err != nil {
			if failErr := manager.FailDurable(name, 500, err.Error()); failErr != nil {
				t.Fatal(failErr)
			}
		}
	}
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters", `{"clusterName":"cluster"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create = %d, body = %s", response.Code, response.Body.String())
	}
	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operations := restartedManager.List()
	if len(operations) != 1 || !operations[0].Done || operations[0].Error == nil {
		t.Fatalf("durable operation outcome = %#v", operations)
	}
	restarted, err := NewAPIWithStore(restartedManager, &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	read := dataprocRequest(restarted, http.MethodGet,
		"/v1/projects/test/regions/us/clusters/cluster", "")
	var cluster Cluster
	decodeDataprocResponse(t, read, &cluster)
	if cluster.Status.State != "ERROR" {
		t.Fatalf("cluster after backend failure restart = %#v", cluster.Status)
	}
}

func TestDataprocPartialProvisionCompensatesInReverseOrder(t *testing.T) {
	store := newDataprocFailingStore()
	manager := orchestrator.NewOperationManager()
	backend := &dataprocBackendSpy{failProvisionAt: 3, provisionErr: errors.New("worker unavailable")}
	api, err := NewAPIWithStore(manager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		if err := work(); err != nil {
			manager.Fail(name, 500, err.Error())
		}
	}
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters",
		`{"clusterName":"cluster","config":{"workerConfig":{"numInstances":2}}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create = %d, body = %s", response.Code, response.Body.String())
	}
	want := []string{
		dataprocDockerName("test", "us", "cluster", "w", 1),
		dataprocDockerName("test", "us", "cluster", "w", 0),
		dataprocDockerName("test", "us", "cluster", "m", 0),
	}
	if len(backend.deleteNames) != len(want) {
		t.Fatalf("compensation deletes = %#v", backend.deleteNames)
	}
	for i := range want {
		if backend.deleteNames[i] != want[i] {
			t.Fatalf("compensation order = %#v, want %#v", backend.deleteNames, want)
		}
	}
	if backend.contextualDeletes != len(want) {
		t.Fatalf("bounded-context deletes = %d, want %d", backend.contextualDeletes, len(want))
	}
}

func TestDataprocProvisionErrorCompensatesAttemptedOwnedIdentity(t *testing.T) {
	for _, test := range []struct {
		name       string
		failure    string
		failAt     int
		workers    int
		wantDelete []string
	}{
		{
			name: "master create succeeded start failed", failure: "start container failed",
			failAt: 1, workers: 0,
			wantDelete: []string{dataprocDockerName("test", "us", "cluster", "m", 0)},
		},
		{
			name: "master port registry update failed", failure: "update port registry failed",
			failAt: 1, workers: 0,
			wantDelete: []string{dataprocDockerName("test", "us", "cluster", "m", 0)},
		},
		{
			name: "worker create succeeded start failed", failure: "start container failed",
			failAt: 2, workers: 1,
			wantDelete: []string{
				dataprocDockerName("test", "us", "cluster", "w", 0),
				dataprocDockerName("test", "us", "cluster", "m", 0),
			},
		},
		{
			name: "worker port registry update failed", failure: "update port registry failed",
			failAt: 2, workers: 1,
			wantDelete: []string{
				dataprocDockerName("test", "us", "cluster", "w", 0),
				dataprocDockerName("test", "us", "cluster", "m", 0),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := newDataprocAmbiguousProvisionBackend(test.failAt, true, errors.New(test.failure))
			api, manager := newSynchronousDataprocAPI(t, backend)
			response := dataprocRequest(api, http.MethodPost,
				"/v1/projects/test/regions/us/clusters",
				`{"clusterName":"cluster","config":{"workerConfig":{"numInstances":`+
					fmt.Sprint(test.workers)+`}}}`)
			if response.Code != http.StatusOK {
				t.Fatalf("create = %d, body = %s", response.Code, response.Body.String())
			}
			if strings.Join(backend.deleted, ",") != strings.Join(test.wantDelete, ",") {
				t.Fatalf("owned compensation = %#v, want %#v", backend.deleted, test.wantDelete)
			}
			if backend.contextualDeletes != len(test.wantDelete) {
				t.Fatalf("bounded cleanup calls = %d, want %d", backend.contextualDeletes, len(test.wantDelete))
			}
			if api.PersistenceError() != nil {
				t.Fatalf("successful exact compensation degraded API: %v", api.PersistenceError())
			}
			if operations := manager.List(); len(operations) != 1 || operations[0].Error == nil {
				t.Fatalf("provision failure operation = %#v", operations)
			}
		})
	}
}

func TestDataprocProvisionErrorRefusesUnownedCollisionAndDegrades(t *testing.T) {
	backend := newDataprocAmbiguousProvisionBackend(2, false, errors.New("container exists but is not owned"))
	api, manager := newSynchronousDataprocAPI(t, backend)
	_ = dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters",
		`{"clusterName":"cluster","config":{"workerConfig":{"numInstances":1}}}`)

	wantAttempts := []string{
		dataprocDockerName("test", "us", "cluster", "w", 0),
		dataprocDockerName("test", "us", "cluster", "m", 0),
	}
	if strings.Join(backend.attemptedDeletes, ",") != strings.Join(wantAttempts, ",") {
		t.Fatalf("cleanup attempts = %#v, want %#v", backend.attemptedDeletes, wantAttempts)
	}
	if got := strings.Join(backend.deleted, ","); got != dataprocDockerName("test", "us", "cluster", "m", 0) {
		t.Fatalf("deleted resources = %q; unowned worker must remain", got)
	}
	if api.PersistenceError() == nil {
		t.Fatal("unowned cleanup ambiguity did not degrade API")
	}
	operations := manager.List()
	if len(operations) != 1 || operations[0].Error == nil ||
		!strings.Contains(operations[0].Error.Message, "refusing to delete unowned") {
		t.Fatalf("unowned collision operation = %#v", operations)
	}
}

func TestDataprocCompensationFailureIsReflected(t *testing.T) {
	store := newDataprocFailingStore()
	manager := orchestrator.NewOperationManager()
	backend := &dataprocBackendSpy{
		failProvisionAt: 2,
		provisionErr:    errors.New("worker unavailable"),
		deleteErr:       errors.New("cleanup unavailable"),
	}
	api, err := NewAPIWithStore(manager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		if err := work(); err != nil {
			manager.Fail(name, 500, err.Error())
		}
	}
	_ = dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters", `{"clusterName":"cluster"}`)
	operations := manager.List()
	if len(operations) != 1 || operations[0].Error == nil ||
		!strings.Contains(operations[0].Error.Message, "compensation") {
		t.Fatalf("operation after compensation failure = %#v", operations)
	}
}

func TestDataprocDuplicateDoesNotRegisterOperation(t *testing.T) {
	store := newDataprocFailingStore()
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string, func() error) {}
	path := "/v1/projects/test/regions/us/clusters"
	if response := dataprocRequest(api, http.MethodPost, path, `{"clusterName":"cluster"}`); response.Code != http.StatusOK {
		t.Fatalf("first create = %d, body = %s", response.Code, response.Body.String())
	}
	before := len(manager.List())
	duplicate := dataprocRequest(api, http.MethodPost, path, `{"clusterName":"cluster"}`)
	assertDataprocError(t, duplicate, http.StatusConflict, "ALREADY_EXISTS")
	if after := len(manager.List()); after != before {
		t.Fatalf("operations after duplicate = %d, want %d", after, before)
	}
}

func TestDataprocJobTransitionSaveFailureIsSticky(t *testing.T) {
	store := newDataprocFailingStore()
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			clusterKey("test", "us", "cluster"): {
				ProjectId: "test", ClusterName: "cluster",
				Status: ClusterStatus{State: "RUNNING"},
			},
		},
	})
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	api.clusters[clusterKey("test", "us", "cluster")].Status.State = "RUNNING"
	api.mu.Unlock()
	store.saves = 0
	api.jobRunner = func(work func()) { work() }
	store.failAt = 2
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/jobs:submit",
		`{"job":{"placement":{"clusterName":"cluster"},"pysparkJob":{"mainPythonFileUri":"gs://test/job.py"}}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("submit = %d, body = %s", response.Code, response.Body.String())
	}
	if api.PersistenceError() == nil {
		t.Fatal("job transition failure did not degrade API")
	}
	blocked := dataprocRequest(api, http.MethodGet, "/v1/projects/test/regions/us/jobs", "")
	assertDataprocError(t, blocked, http.StatusServiceUnavailable, "UNAVAILABLE")
}

func TestDataprocRejectsUnsupportedJobsWithoutPersisting(t *testing.T) {
	store := newDataprocFailingStore()
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			clusterKey("test", "us", "cluster"): {
				ProjectId: "test", ClusterName: "cluster",
				Status: ClusterStatus{State: "RUNNING"},
			},
		},
	})
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/jobs:submit",
		`{"job":{"placement":{"clusterName":"cluster"},"hiveJob":{"queryList":{"queries":["select 1"]}}}}`)
	assertDataprocError(t, response, http.StatusNotImplemented, "UNIMPLEMENTED")
	if len(api.jobs) != 0 || backend.commands != 0 {
		t.Fatalf("unsupported job mutated state or executed: jobs=%d commands=%d", len(api.jobs), backend.commands)
	}
}

func TestDataprocJobRequiresExactPersistedClusterIdentity(t *testing.T) {
	store := newDataprocFailingStore()
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			clusterKey("project-a", "us", "cluster"): {
				ProjectId: "project-a", ClusterName: "cluster",
				Status: ClusterStatus{State: "RUNNING"},
			},
		},
	})
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/project-b/regions/us/jobs:submit",
		`{"job":{"placement":{"clusterName":"cluster"},"pysparkJob":{"mainPythonFileUri":"gs://code/job.py"}}}`)
	assertDataprocError(t, response, http.StatusNotFound, "NOT_FOUND")
	if len(api.jobs) != 0 || backend.commands != 0 {
		t.Fatalf("foreign cluster job mutated state or executed: jobs=%d commands=%d", len(api.jobs), backend.commands)
	}
}

func TestDataprocDockerIdentitySeparatesProfileProjectRegionAndCluster(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "profile-a")
	first := dataprocDockerName("project-a", "us", "cluster", "m", 0)
	for _, other := range []string{
		dataprocDockerName("project-b", "us", "cluster", "m", 0),
		dataprocDockerName("project-a", "eu", "cluster", "m", 0),
		dataprocDockerName("project-a", "us", "other", "m", 0),
		dataprocDockerName("project-a", "us", "cluster", "w", 0),
	} {
		if other == first {
			t.Fatalf("Dataproc Docker identity collision: %q", first)
		}
	}
	t.Setenv("MINISKY_PROFILE", "profile-b")
	if other := dataprocDockerName("project-a", "us", "cluster", "m", 0); other == first {
		t.Fatalf("Dataproc Docker identity collided across profiles: %q", first)
	}
}

func TestDataprocOperationPollingIsFullyScoped(t *testing.T) {
	manager := orchestrator.NewOperationManager()
	api := newAPI(manager, nil, nil)
	operations := []*orchestrator.Operation{
		manager.Register("dataproc#operation", "CREATE",
			"https://dataproc.googleapis.com/v1/projects/other/regions/us/clusters/c", "", "us"),
		manager.Register("dataproc#operation", "CREATE",
			"https://dataproc.googleapis.com/v1/projects/test/regions/eu/clusters/c", "", "eu"),
		manager.Register("compute#operation", "insert",
			"https://www.googleapis.com/compute/v1/projects/test/zones/us/instances/c", "us", ""),
	}
	for _, operation := range operations {
		response := dataprocRequest(api, http.MethodGet,
			"/v1/projects/test/regions/us/operations/"+operation.Name, "")
		assertDataprocError(t, response, http.StatusNotFound, "NOT_FOUND")
	}
}

func TestDataprocRunningSaveFailureCompensatesAndDegrades(t *testing.T) {
	store := newDataprocFailingStore()
	manager := orchestrator.NewOperationManager()
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(manager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		if err := work(); err != nil {
			manager.Fail(name, 500, err.Error())
		}
	}
	store.failAt = 2
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters",
		`{"clusterName":"cluster","config":{"workerConfig":{"numInstances":2}}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create = %d, body = %s", response.Code, response.Body.String())
	}
	want := []string{
		dataprocDockerName("test", "us", "cluster", "w", 1),
		dataprocDockerName("test", "us", "cluster", "w", 0),
		dataprocDockerName("test", "us", "cluster", "m", 0),
	}
	if strings.Join(backend.deleteNames, ",") != strings.Join(want, ",") {
		t.Fatalf("RUNNING-save compensation = %#v, want %#v", backend.deleteNames, want)
	}
	if api.PersistenceError() == nil {
		t.Fatal("RUNNING save failure did not degrade API")
	}
	operations := manager.List()
	if len(operations) != 1 || operations[0].Error == nil ||
		!strings.Contains(operations[0].Error.Message, "compensated") {
		t.Fatalf("RUNNING-save operation did not record compensation ambiguity: %#v", operations)
	}
}

func TestDataprocRunningPostCommitSaveFailureStillCompensates(t *testing.T) {
	store := newDataprocFailingStore()
	manager := orchestrator.NewOperationManager()
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(manager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		if err := work(); err != nil {
			manager.Fail(name, 500, err.Error())
		}
	}
	store.failAfterCommitAt = 2
	_ = dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters",
		`{"clusterName":"cluster","config":{"workerConfig":{"numInstances":1}}}`)
	want := []string{
		dataprocDockerName("test", "us", "cluster", "w", 0),
		dataprocDockerName("test", "us", "cluster", "m", 0),
	}
	if strings.Join(backend.deleteNames, ",") != strings.Join(want, ",") {
		t.Fatalf("post-commit compensation = %#v, want %#v", backend.deleteNames, want)
	}
	if api.PersistenceError() == nil {
		t.Fatal("post-commit RUNNING save failure did not degrade API")
	}
}

func TestDataprocRunningSaveAndCompensationFailureIsSticky(t *testing.T) {
	store := newDataprocFailingStore()
	manager := orchestrator.NewOperationManager()
	backend := &dataprocBackendSpy{deleteErr: errors.New("cleanup unavailable")}
	api, err := NewAPIWithStore(manager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		if err := work(); err != nil {
			manager.Fail(name, 500, err.Error())
		}
	}
	store.failAt = 2
	_ = dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters",
		`{"clusterName":"cluster","config":{"workerConfig":{"numInstances":1}}}`)
	operations := manager.List()
	if len(operations) != 1 || operations[0].Error == nil ||
		!strings.Contains(operations[0].Error.Message, "compensation") {
		t.Fatalf("operation after final-save compensation failure = %#v", operations)
	}
	if api.PersistenceError() == nil {
		t.Fatal("final-save compensation ambiguity was not sticky")
	}
}

func TestDataprocDeletionCompletionSaveFailureIsSticky(t *testing.T) {
	store := newDataprocFailingStore()
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			clusterKey("test", "us", "cluster"): {
				ClusterName: "cluster",
				Config:      ClusterConfig{WorkerConfig: &InstanceGroupConfig{NumInstances: 0}},
				Status:      ClusterStatus{State: "ERROR"},
			},
		},
	})
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		if err := work(); err != nil {
			manager.Fail(name, 500, err.Error())
		}
	}
	store.failAt = 2
	response := dataprocRequest(api, http.MethodDelete,
		"/v1/projects/test/regions/us/clusters/cluster", "")
	if response.Code != http.StatusOK {
		t.Fatalf("delete = %d, body = %s", response.Code, response.Body.String())
	}
	if api.PersistenceError() == nil {
		t.Fatal("deletion completion save failure did not degrade API")
	}
}

func TestDataprocRestartNormalizationIsDurablySaved(t *testing.T) {
	store := newDataprocFailingStore()
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			clusterKey("test", "us", "cluster"): {ClusterName: "cluster", Status: ClusterStatus{State: "RUNNING"}},
		},
	})
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store); err != nil {
		t.Fatal(err)
	}
	if store.saves != 1 {
		t.Fatalf("normalization saves = %d, want 1", store.saves)
	}
	var persisted dataprocMetadata
	if err := store.Load(dataprocStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if got := persisted.Clusters[clusterKey("test", "us", "cluster")].Status.State; got != "ERROR" {
		t.Fatalf("persisted normalized state = %q", got)
	}
}

func TestDataprocRestartCleansExactOwnedBackendsWithoutReplay(t *testing.T) {
	store := newDataprocFailingStore()
	key := clusterKey("test", "us", "cluster")
	master := dataprocDockerName("test", "us", "cluster", "m", 0)
	worker := dataprocDockerName("test", "us", "cluster", "w", 0)
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			key: {
				ProjectId: "test", ClusterName: "cluster", ClusterUuid: "uuid",
				Config: ClusterConfig{WorkerConfig: &InstanceGroupConfig{NumInstances: 1}},
				Status: ClusterStatus{State: "RUNNING"},
			},
		},
		Runtimes: map[string]*dataprocRuntimeIntent{
			key: {ContainerNames: []string{master, worker}},
		},
	})
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, store)
	if err != nil {
		t.Fatal(err)
	}
	if backend.provisions != 0 || backend.commands != 0 {
		t.Fatalf("restart replayed work: %#v", backend)
	}
	if strings.Join(backend.deleteNames, ",") != master+","+worker {
		t.Fatalf("restart cleanup = %#v, want exact persisted ownership", backend.deleteNames)
	}
	api.mu.RLock()
	cluster := cloneCluster(api.clusters[key])
	runtimeCount := len(api.runtimes)
	api.mu.RUnlock()
	if cluster.Status.State != "ERROR" || runtimeCount != 0 {
		t.Fatalf("normalized cluster=%#v runtimes=%d", cluster, runtimeCount)
	}
	var persisted dataprocMetadata
	if err := store.Load(dataprocStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Runtimes) != 0 || persisted.Clusters[key].Status.State != "ERROR" {
		t.Fatalf("durable restart result = %#v", persisted)
	}
}

func TestDataprocRestartRefusesUnownedBackendAndPreservesCleanupIntent(t *testing.T) {
	store := newDataprocFailingStore()
	key := clusterKey("test", "us", "cluster")
	master := dataprocDockerName("test", "us", "cluster", "m", 0)
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			key: {
				ProjectId: "test", ClusterName: "cluster",
				Config: ClusterConfig{WorkerConfig: &InstanceGroupConfig{NumInstances: 0}},
				Status: ClusterStatus{State: "CREATING"},
			},
		},
		Runtimes: map[string]*dataprocRuntimeIntent{
			key: {ContainerNames: []string{master}},
		},
	})
	backend := newDataprocAmbiguousProvisionBackend(0, false, nil)
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, store); err == nil ||
		!strings.Contains(err.Error(), "unowned") {
		t.Fatalf("restart cleanup error = %v", err)
	}
	if strings.Join(backend.attemptedDeletes, ",") != master || len(backend.deleted) != 0 {
		t.Fatalf("unowned cleanup mutated backend: attempted=%#v deleted=%#v",
			backend.attemptedDeletes, backend.deleted)
	}
	var persisted dataprocMetadata
	if err := store.Load(dataprocStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Clusters[key].Status.State != "CREATING" || len(persisted.Runtimes) != 1 {
		t.Fatalf("failed reconciliation changed durable truth: %#v", persisted)
	}
}

func TestDataprocRestartPreservesTerminalJobsAndOperationOutcomes(t *testing.T) {
	store, err := state.New(t.TempDir(), "terminal")
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey("test", "us", "cluster")
	if err := store.Save(dataprocStateEntry, dataprocMetadata{
		Clusters: map[string]*Cluster{
			key: {ProjectId: "test", ClusterName: "cluster", Status: ClusterStatus{State: "ERROR", Detail: "stable"}},
		},
		Jobs: map[string]*Job{
			jobKey("test", "us", "done"): {
				Reference: JobReference{ProjectId: "test", JobId: "done"},
				Status:    JobStatus{State: "DONE", Details: "result"},
			},
			jobKey("test", "us", "failed"): {
				Reference: JobReference{ProjectId: "test", JobId: "failed"},
				Status:    JobStatus{State: "ERROR", Details: "work failed"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := manager.RegisterDurable("dataproc#operation", "CREATE",
		"https://dataproc.googleapis.com/v1/projects/test/regions/us/clusters/cluster", "", "us")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AdvanceDurable(operation.Name, 100, orchestrator.StatusDone); err != nil {
		t.Fatal(err)
	}
	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(restartedManager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	if backend.provisions != 0 || backend.deletes != 0 || backend.commands != 0 {
		t.Fatalf("terminal restart touched backend: %#v", backend)
	}
	for id, wantDetails := range map[string]string{"done": "result", "failed": "work failed"} {
		response := dataprocRequest(api, http.MethodGet,
			"/v1/projects/test/regions/us/jobs/"+id, "")
		var job Job
		decodeDataprocResponse(t, response, &job)
		if job.Status.Details != wantDetails {
			t.Fatalf("job %s after restart = %#v", id, job.Status)
		}
	}
	polled := restartedManager.Get(operation.Name)
	if polled == nil || !polled.Done || polled.Error != nil {
		t.Fatalf("terminal operation after restart = %#v", polled)
	}
}

func TestDataprocRestartInterruptsActiveOperationWithoutBackendReplay(t *testing.T) {
	store, err := state.New(t.TempDir(), "active-operation")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := manager.RegisterDurable("dataproc#operation", "CREATE",
		"https://dataproc.googleapis.com/v1/projects/test/regions/us/clusters/cluster", "", "us")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.AdvanceDurable(operation.Name, 25, orchestrator.StatusRunning); err != nil {
		t.Fatal(err)
	}

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	backend := &dataprocBackendSpy{}
	api, err := NewAPIWithStore(restartedManager, backend, store)
	if err != nil {
		t.Fatal(err)
	}
	response := dataprocRequest(api, http.MethodGet,
		"/v1/projects/test/regions/us/operations/"+operation.Name, "")
	var result struct {
		Done  bool                         `json:"done"`
		Error *orchestrator.OperationError `json:"error"`
	}
	decodeDataprocResponse(t, response, &result)
	if !result.Done || result.Error == nil ||
		!strings.Contains(result.Error.Message, "interrupted by MiniSky restart") {
		t.Fatalf("restarted operation = %#v", result)
	}
	if backend.provisions != 0 || backend.deletes != 0 || backend.commands != 0 {
		t.Fatalf("operation restart replayed backend: %#v", backend)
	}
}

func TestDataprocRestartCleanupSaveFailureKeepsRetryableIntent(t *testing.T) {
	store := newDataprocFailingStore()
	key := clusterKey("test", "us", "cluster")
	master := dataprocDockerName("test", "us", "cluster", "m", 0)
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			key: {
				ProjectId: "test", ClusterName: "cluster",
				Config: ClusterConfig{WorkerConfig: &InstanceGroupConfig{NumInstances: 0}},
				Status: ClusterStatus{State: "CREATING"},
			},
		},
		Runtimes: map[string]*dataprocRuntimeIntent{
			key: {ContainerNames: []string{master}},
		},
	})
	store.fail = true
	backend := &dataprocBackendSpy{}
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), backend, store); err == nil {
		t.Fatal("restart cleanup normalization save failure was ignored")
	}
	if strings.Join(backend.deleteNames, ",") != master {
		t.Fatalf("cleanup calls = %#v", backend.deleteNames)
	}
	var persisted dataprocMetadata
	if err := store.Load(dataprocStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Clusters[key].Status.State != "CREATING" || len(persisted.Runtimes) != 1 {
		t.Fatalf("failed cleanup commit lost retry intent: %#v", persisted)
	}
}

func TestDataprocRejectsMutationAfterOperationPersistenceFailure(t *testing.T) {
	operationStore := newDataprocFailingStore()
	manager, err := orchestrator.NewOperationManagerWithStore(operationStore)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := manager.RegisterDurable("dataproc#operation", "CREATE",
		"https://dataproc.googleapis.com/v1/projects/test/regions/us/clusters/existing", "", "us")
	if err != nil {
		t.Fatal(err)
	}
	operationStore.fail = true
	if err := manager.FailDurable(operation.Name, 500, "backend failed"); err == nil {
		t.Fatal("injected operation persistence failure returned nil")
	}
	api, err := NewAPIWithStore(manager, &dataprocBackendSpy{}, newDataprocFailingStore())
	if err != nil {
		t.Fatal(err)
	}
	response := dataprocRequest(api, http.MethodPost,
		"/v1/projects/test/regions/us/clusters", `{"clusterName":"blocked"}`)
	assertDataprocError(t, response, http.StatusServiceUnavailable, "UNAVAILABLE")
	if got := len(manager.List()); got != 1 {
		t.Fatalf("mutation registered operation after degradation: %d", got)
	}
}

func TestDataprocNormalizationSaveFailureFailsConstruction(t *testing.T) {
	store := newDataprocFailingStore()
	store.entries[dataprocStateEntry] = mustDataprocJSON(t, dataprocMetadata{
		Clusters: map[string]*Cluster{
			clusterKey("test", "us", "cluster"): {ClusterName: "cluster", Status: ClusterStatus{State: "RUNNING"}},
		},
	})
	store.fail = true
	if _, err := NewAPIWithStore(orchestrator.NewOperationManager(), nil, store); err == nil {
		t.Fatal("normalization save failure was ignored")
	}
}

func TestDataprocConcurrentCreatesPersist(t *testing.T) {
	store, err := state.New(t.TempDir(), "concurrent")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(string, func() error) {}
	var requests sync.WaitGroup
	for i := 0; i < 10; i++ {
		requests.Add(1)
		go func(i int) {
			defer requests.Done()
			name := "cluster-" + string(rune('a'+i))
			response := dataprocRequest(api, http.MethodPost,
				"/v1/projects/test/regions/us/clusters", `{"clusterName":"`+name+`"}`)
			if response.Code != http.StatusOK {
				t.Errorf("create %s = %d, body = %s", name, response.Code, response.Body.String())
			}
		}(i)
	}
	requests.Wait()
	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), &dataprocBackendSpy{}, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.mu.RLock()
	count := len(restarted.clusters)
	restarted.mu.RUnlock()
	if count != 10 {
		t.Fatalf("clusters after restart = %d, want 10", count)
	}
}

func TestDataprocMetadataIsProfileScoped(t *testing.T) {
	root := t.TempDir()
	firstStore, err := state.New(root, "first")
	if err != nil {
		t.Fatal(err)
	}
	first, err := NewAPIWithStore(orchestrator.NewOperationManager(), &dataprocBackendSpy{}, firstStore)
	if err != nil {
		t.Fatal(err)
	}
	first.operationRunner = func(string, func() error) {}
	if response := dataprocRequest(first, http.MethodPost,
		"/v1/projects/test/regions/us/clusters", `{"clusterName":"first"}`); response.Code != http.StatusOK {
		t.Fatalf("first profile create = %d, body = %s", response.Code, response.Body.String())
	}

	secondStore, err := state.New(root, "second")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAPIWithStore(orchestrator.NewOperationManager(), &dataprocBackendSpy{}, secondStore)
	if err != nil {
		t.Fatal(err)
	}
	second.mu.RLock()
	_, leaked := second.clusters[clusterKey("test", "us", "first")]
	second.mu.RUnlock()
	if leaked {
		t.Fatal("Dataproc metadata leaked between profiles")
	}
}

type dataprocBackendSpy struct {
	provisions        int
	deletes           int
	commands          int
	failProvisionAt   int
	provisionErr      error
	deleteErr         error
	commandErr        error
	provisionNames    []string
	deleteNames       []string
	contextualDeletes int
}

type dataprocAmbiguousProvisionBackend struct {
	failAt            int
	attemptedOwned    bool
	provisionErr      error
	provisions        int
	owned             map[string]bool
	attemptedDeletes  []string
	deleted           []string
	contextualDeletes int
}

func newDataprocAmbiguousProvisionBackend(failAt int, attemptedOwned bool, provisionErr error) *dataprocAmbiguousProvisionBackend {
	return &dataprocAmbiguousProvisionBackend{
		failAt: failAt, attemptedOwned: attemptedOwned, provisionErr: provisionErr,
		owned: make(map[string]bool),
	}
}

func (backend *dataprocAmbiguousProvisionBackend) ProvisionComputeVM(_ context.Context, name string, _ string, _ string, _ []string, _ []string, _ []string) error {
	backend.provisions++
	if backend.provisions == backend.failAt {
		backend.owned[name] = backend.attemptedOwned
		return backend.provisionErr
	}
	backend.owned[name] = true
	return nil
}

func (backend *dataprocAmbiguousProvisionBackend) DeleteComputeVM(name string) error {
	return backend.delete(name)
}

func (backend *dataprocAmbiguousProvisionBackend) DeleteComputeVMContext(ctx context.Context, name string) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("missing cleanup deadline")
	}
	backend.contextualDeletes++
	return backend.delete(name)
}

func (backend *dataprocAmbiguousProvisionBackend) delete(name string) error {
	backend.attemptedDeletes = append(backend.attemptedDeletes, name)
	if !backend.owned[name] {
		return fmt.Errorf("refusing to delete unowned Compute container %q", name)
	}
	backend.deleted = append(backend.deleted, name)
	delete(backend.owned, name)
	return nil
}

func (backend *dataprocAmbiguousProvisionBackend) RunCommandInContainer(string, []string) (string, error) {
	return "", nil
}

func newSynchronousDataprocAPI(t *testing.T, backend dataprocServiceManager) (*API, *orchestrator.OperationManager) {
	t.Helper()
	manager := orchestrator.NewOperationManager()
	api, err := NewAPIWithStore(manager, backend, newDataprocFailingStore())
	if err != nil {
		t.Fatal(err)
	}
	api.operationRunner = func(name string, work func() error) {
		if err := work(); err != nil {
			manager.Fail(name, 500, err.Error())
		}
	}
	return api, manager
}

func (backend *dataprocBackendSpy) ProvisionComputeVM(_ context.Context, name string, _ string, _ string, _ []string, _ []string, _ []string) error {
	backend.provisions++
	backend.provisionNames = append(backend.provisionNames, name)
	if backend.failProvisionAt == 0 || backend.provisions == backend.failProvisionAt {
		return backend.provisionErr
	}
	return nil
}

func (backend *dataprocBackendSpy) DeleteComputeVM(name string) error {
	backend.deletes++
	backend.deleteNames = append(backend.deleteNames, name)
	return backend.deleteErr
}

func (backend *dataprocBackendSpy) DeleteComputeVMContext(ctx context.Context, name string) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("missing cleanup deadline")
	}
	backend.contextualDeletes++
	return backend.DeleteComputeVM(name)
}

func (backend *dataprocBackendSpy) RunCommandInContainer(string, []string) (string, error) {
	backend.commands++
	return "", backend.commandErr
}

type dataprocFailingStore struct {
	mu                sync.Mutex
	entries           map[string]json.RawMessage
	fail              bool
	failAfterCommit   bool
	failAfterCommitAt int
	loadErr           error
	failAt            int
	saves             int
}

type dataprocInterleavingStore struct {
	*dataprocFailingStore
	saveEntered chan struct{}
	releaseSave chan struct{}
	once        sync.Once
}

func (store *dataprocInterleavingStore) Save(name string, value any) error {
	store.once.Do(func() {
		close(store.saveEntered)
		<-store.releaseSave
		store.fail = true
		store.loadErr = errors.New("readback unavailable")
	})
	return store.dataprocFailingStore.Save(name, value)
}

func newDataprocFailingStore() *dataprocFailingStore {
	return &dataprocFailingStore{entries: make(map[string]json.RawMessage)}
}

func (store *dataprocFailingStore) Load(name string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.loadErr != nil {
		return store.loadErr
	}
	payload := store.entries[name]
	if payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(payload, target)
}

func (store *dataprocFailingStore) Save(name string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saves++
	if store.fail || store.saves == store.failAt {
		return errors.New("injected save failure")
	}
	payload, err := json.Marshal(value)
	if err == nil {
		store.entries[name] = payload
	}
	if err == nil && (store.failAfterCommit || store.saves == store.failAfterCommitAt) {
		return errors.New("injected post-commit failure")
	}
	return err
}

func mustDataprocJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func dataprocRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeDataprocResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

func assertDataprocError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status = %d, want %d, body = %s", response.Code, code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	decodeDataprocResponse(t, response, &envelope)
	if envelope.Error.Code != code || envelope.Error.Status != status {
		t.Fatalf("error envelope = %#v", envelope.Error)
	}
}
