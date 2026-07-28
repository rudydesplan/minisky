package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestCreateJob(t *testing.T) {
	api := newTestAPI()
	body := `{"taskGroups":[{"taskSpec":{},"taskCount":"1","parallelism":"1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var job Job
	if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if job.Name != "projects/test/locations/us-central1/jobs/j1" {
		t.Fatalf("unexpected name: %s", job.Name)
	}
	if job.UID == "" {
		t.Fatal("expected uid to be generated")
	}
	if job.CreateTime == "" {
		t.Fatal("expected createTime to be set")
	}
	if job.UpdateTime == "" {
		t.Fatal("expected updateTime to be set")
	}
	if job.Status == nil || job.Status.State != "QUEUED" {
		t.Fatalf("expected status.state=QUEUED, got %+v", job.Status)
	}
	if len(job.TaskGroups) != 1 {
		t.Fatalf("expected 1 taskGroup, got %d", len(job.TaskGroups))
	}
}

func TestCreateJobGeneratesOptionalId(t *testing.T) {
	api := newTestAPI()
	body := `{"taskGroups":[{"taskSpec":{}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var job Job
	if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(job.Name, "/jobs/job-") {
		t.Fatalf("generated job name = %q", job.Name)
	}
}

func TestCreateJobMissingTaskGroups(t *testing.T) {
	api := newTestAPI()
	body := `{"labels":{"env":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateExecutableJobRunsAndCapturesTerminalState(t *testing.T) {
	runner := &fakeContainerRunner{
		result: containerResult{ExitCode: 0, Output: "hello from batch\n"},
	}
	api := newTestAPI()
	api.runner = runner
	body := `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox:1.36","entrypoint":"/bin/sh","commands":["-c","echo hello"]}}]},"taskCount":"1","parallelism":"1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	waitForJobState(t, api, "projects/test/locations/us-central1/jobs/j1", "SUCCEEDED")

	runner.mu.Lock()
	workload := runner.workload
	cleaned := runner.cleaned
	runner.mu.Unlock()
	if workload.ImageURI != "busybox:1.36" || workload.Entrypoint != "/bin/sh" ||
		strings.Join(workload.Commands, " ") != "-c echo hello" {
		t.Fatalf("workload = %#v", workload)
	}
	if workload.Ownership.ContainerName == "" ||
		workload.Ownership.Labels["minisky.service"] != "batch" ||
		workload.Ownership.Labels["minisky.job"] != "projects/test/locations/us-central1/jobs/j1" {
		t.Fatalf("ownership = %#v", workload.Ownership)
	}
	if cleaned.ContainerName != workload.Ownership.ContainerName {
		t.Fatalf("cleaned ownership = %#v, workload ownership = %#v", cleaned, workload.Ownership)
	}

	api.mu.RLock()
	job := deepCopyJob(api.jobs["projects/test/locations/us-central1/jobs/j1"])
	_, runtimeRetained := api.runtimes[job.Name]
	api.mu.RUnlock()
	if runtimeRetained {
		t.Fatal("terminal job retained Docker ownership intent after cleanup")
	}
	if job.Status == nil || job.Status.State != "SUCCEEDED" || len(job.Status.StatusEvents) == 0 {
		t.Fatalf("terminal job = %#v", job)
	}
	var event struct {
		Type          string `json:"type"`
		TaskState     string `json:"taskState"`
		TaskExecution struct {
			ExitCode int `json:"exitCode"`
		} `json:"taskExecution"`
	}
	if err := json.Unmarshal(job.Status.StatusEvents[len(job.Status.StatusEvents)-1], &event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "JOB_STATE_CHANGED" || event.TaskState != "SUCCEEDED" || event.TaskExecution.ExitCode != 0 {
		t.Fatalf("terminal event = %#v", event)
	}
}

func TestCreateExecutableJobFailureAndUnsupportedBoundaries(t *testing.T) {
	runner := &fakeContainerRunner{
		result: containerResult{ExitCode: 23, Output: "boom"},
	}
	api := newTestAPI()
	api.runner = runner
	body := `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox:1.36"}}]}}]}`
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs?jobId=failed", bytes.NewBufferString(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d: %s", w.Code, w.Body.String())
	}
	waitForJobState(t, api, "projects/test/locations/us-central1/jobs/failed", "FAILED")

	for name, requestBody := range map[string]string{
		"multiple tasks":     `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox"}}]},"taskCount":"2"}]}`,
		"multiple runnables": `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox"}},{"container":{"imageUri":"busybox"}}]}}]}`,
		"missing image":      `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{}}]}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
				"/v1/projects/test/locations/us-central1/jobs?jobId=unsupported-"+strings.ReplaceAll(name, " ", "-"),
				bytes.NewBufferString(requestBody)))
			if response.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestExecutableJobRunningStateSaveFailureFailsClosedAndCleans(t *testing.T) {
	store := &nthFailBatchStore{data: make(map[string][]byte), failAt: 2}
	runner := &fakeContainerRunner{}
	api := newTestAPI()
	api.stateStore = store
	api.runner = runner
	body := `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox:1.36"}}]}}]}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs?jobId=save-failure", bytes.NewBufferString(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d: %s", response.Code, response.Body.String())
	}
	name := "projects/test/locations/us-central1/jobs/save-failure"
	waitForJobState(t, api, name, "FAILED")
	select {
	case <-runner.cleanedCh():
	case <-time.After(2 * time.Second):
		t.Fatal("RUNNING persistence failure did not clean ownership intent")
	}
	api.mu.RLock()
	_, runtimeExists := api.runtimes[name]
	api.mu.RUnlock()
	if runtimeExists {
		t.Fatal("RUNNING persistence failure retained cleaned runtime")
	}
	if api.opMgr.PersistenceError() == nil {
		t.Fatal("async persistence failure did not leave sticky degradation")
	}
}

func TestExecutableJobCancellationStopsAndCleansOwnedContainer(t *testing.T) {
	runner := &fakeContainerRunner{
		started: make(chan struct{}),
		block:   make(chan struct{}),
	}
	api := newTestAPI()
	api.runner = runner
	body := `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox:1.36"}}]}}]}`
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs?jobId=cancel-me", bytes.NewBufferString(body)))
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d: %s", create.Code, create.Body.String())
	}
	<-runner.started

	cancel := httptest.NewRecorder()
	api.ServeHTTP(cancel, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs/cancel-me:cancel", bytes.NewBufferString(`{}`)))
	if cancel.Code != http.StatusOK {
		t.Fatalf("cancel status = %d: %s", cancel.Code, cancel.Body.String())
	}
	waitForJobState(t, api, "projects/test/locations/us-central1/jobs/cancel-me", "CANCELLED")
	select {
	case <-runner.cleanedCh():
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled job did not clean its owned container")
	}
}

func TestDeleteRunningExecutableJobWaitsForOwnedCleanup(t *testing.T) {
	runner := &fakeContainerRunner{
		started: make(chan struct{}),
		block:   make(chan struct{}),
	}
	defer close(runner.block)
	api := newTestAPI()
	api.runner = runner
	body := `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox:1.36"}}]}}]}`
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs?jobId=delete-running", bytes.NewBufferString(body)))
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d: %s", create.Code, create.Body.String())
	}
	<-runner.started

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/test/locations/us-central1/jobs/delete-running", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d: %s", response.Code, response.Body.String())
	}
	select {
	case <-runner.cleanedCh():
	default:
		t.Fatal("delete returned before exact-owned container cleanup")
	}
	name := "projects/test/locations/us-central1/jobs/delete-running"
	api.mu.RLock()
	_, jobExists := api.jobs[name]
	_, runtimeExists := api.runtimes[name]
	api.mu.RUnlock()
	if jobExists || runtimeExists {
		t.Fatalf("delete retained job=%t runtime=%t", jobExists, runtimeExists)
	}
}

func TestExecutableJobUnavailableFailsBeforeMutation(t *testing.T) {
	api := newTestAPI()
	api.runner = &fakeContainerRunner{checkErr: errors.New("docker unavailable")}
	body := `{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":"busybox"}}]}}]}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs?jobId=no-docker", bytes.NewBufferString(body)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if len(api.jobs) != 0 {
		t.Fatal("unavailable Docker backend mutated jobs")
	}
}

func TestRestartCleansOwnedContainerAndFailsInterruptedExecutableJob(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	name := "projects/p/locations/l/jobs/interrupted"
	ownership := containerOwnership{
		ContainerName: "minisky-batch-owned",
		Labels: map[string]string{
			"minisky.owner":   "true",
			"minisky.service": "batch",
			"minisky.job":     name,
		},
	}
	if err := store.Save(batchStateEntry, batchMetadata{
		Jobs: map[string]*Job{name: {
			Name: name, TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{}}},
			Status: &JobStatus{State: "RUNNING"},
		}},
		Runtimes: map[string]*batchRuntimeIntent{name: {Ownership: ownership}},
	}); err != nil {
		t.Fatal(err)
	}
	runner := &fakeContainerRunner{}
	api := newTestAPI()
	api.stateStore = store
	api.runner = runner
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	if got := api.jobs[name].Status.State; got != "FAILED" {
		t.Fatalf("restart state = %q, want FAILED", got)
	}
	if _, ok := api.runtimes[name]; ok {
		t.Fatal("restart retained cleaned ownership intent")
	}
	runner.mu.Lock()
	cleaned := runner.cleaned
	runner.mu.Unlock()
	if cleaned.ContainerName != ownership.ContainerName {
		t.Fatalf("cleaned = %#v", cleaned)
	}
	var durable batchMetadata
	if err := store.Load(batchStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if durable.Jobs[name].Status.State != "FAILED" || len(durable.Runtimes) != 0 {
		t.Fatalf("durable restart state = %#v", durable)
	}
}

func TestBatchImportRejectsRuntimeCleanupIntent(t *testing.T) {
	name := "projects/p/locations/l/jobs/imported"
	metadata := batchMetadata{
		Jobs: map[string]*Job{name: {
			Name: name, TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{}}},
			Status: &JobStatus{State: "RUNNING"},
		}},
		Runtimes: map[string]*batchRuntimeIntent{name: {
			Ownership: containerOwnership{
				ContainerName: "user-controlled",
				Labels: map[string]string{
					"minisky.owner": "true", "minisky.service": "batch", "minisky.job": name,
				},
			},
		}},
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(state.Snapshot{
		Format: state.SnapshotFormat, Version: state.Version,
		Entries: map[string]json.RawMessage{batchStateEntry: raw},
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.New(t.TempDir(), "import")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Import(bytes.NewReader(snapshot)); err == nil {
		t.Fatal("import accepted a Docker cleanup intent")
	}
}

func TestDockerExecutableJobIntegration(t *testing.T) {
	image := os.Getenv("MINISKY_BATCH_DOCKER_TEST_IMAGE")
	if image == "" {
		t.Skip("set MINISKY_BATCH_DOCKER_TEST_IMAGE to an available image for Docker integration")
	}
	runner := dockerCLIRunner{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := runner.Check(ctx)
	cancel()
	if err != nil {
		t.Skipf("Docker unavailable: %v", err)
	}
	api := newTestAPI()
	api.runner = runner
	body := fmt.Sprintf(
		`{"taskGroups":[{"taskSpec":{"runnables":[{"container":{"imageUri":%q,"commands":["true"]}}]}}]}`,
		image,
	)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/jobs?jobId=docker-integration",
		bytes.NewBufferString(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d: %s", response.Code, response.Body.String())
	}
	waitForJobState(t, api, "projects/test/locations/us-central1/jobs/docker-integration", "SUCCEEDED")
}

func TestCancelJobReturnsLROAndPersistsCancelledState(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us-central1/jobs/j1"
	api.jobs[name] = &Job{Name: name, TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "QUEUED"}}

	req := httptest.NewRequest(http.MethodPost, "/v1/"+name+":cancel", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var operation map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if operation["done"] != true {
		t.Fatalf("operation = %#v", operation)
	}
	result, _ := operation["response"].(map[string]any)
	if result["@type"] != "type.googleapis.com/google.cloud.batch.v1.Job" {
		t.Fatalf("cancel response = %#v", result)
	}
	if api.jobs[name].Status.State != "CANCELLED" {
		t.Fatalf("state = %s", api.jobs[name].Status.State)
	}
}

func TestCreateJobDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/dup"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/dup",
		UID:        "existing-uid",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{}}},
		Status:     &JobStatus{State: "QUEUED"},
	}
	api.mu.Unlock()

	body := `{"taskGroups":[{"taskSpec":{}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/j1"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/j1",
		UID:        "uid-123",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{Runnables: []Runnable{{Container: &Container{ImageURI: "img"}}}}}},
		Status:     &JobStatus{State: "RUNNING"},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs/j1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var job Job
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	if job.Name != "projects/test/locations/us-central1/jobs/j1" {
		t.Fatalf("unexpected name: %s", job.Name)
	}
	if job.UID != "uid-123" {
		t.Fatalf("unexpected uid: %s", job.UID)
	}
	if job.Status == nil || job.Status.State != "RUNNING" {
		t.Fatalf("unexpected status: %+v", job.Status)
	}
}

func TestGetJobNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListJobs(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/alpha"] = &Job{Name: "projects/test/locations/us-central1/jobs/alpha", UID: "u1", CreateTime: "2024-01-01T00:00:00Z", UpdateTime: "2024-01-01T00:00:00Z", TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "QUEUED"}}
	api.jobs["projects/test/locations/us-central1/jobs/beta"] = &Job{Name: "projects/test/locations/us-central1/jobs/beta", UID: "u2", CreateTime: "2024-01-01T00:00:00Z", UpdateTime: "2024-01-01T00:00:00Z", TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "RUNNING"}}
	api.jobs["projects/test/locations/us-central1/jobs/gamma"] = &Job{Name: "projects/test/locations/us-central1/jobs/gamma", UID: "u3", CreateTime: "2024-01-01T00:00:00Z", UpdateTime: "2024-01-01T00:00:00Z", TaskGroups: []TaskGroup{{}}, Status: &JobStatus{State: "SUCCEEDED"}}
	api.mu.Unlock()

	// First page: pageSize=2
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	jobs := resp["jobs"].([]any)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	// Verify sorted order
	first := jobs[0].(map[string]any)["name"].(string)
	second := jobs[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	jobs = resp["jobs"].([]any)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job on second page, got %d", len(jobs))
	}
}

func TestDeleteJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/j1"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/j1",
		UID:        "uid-1",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{}},
		Status:     &JobStatus{State: "SUCCEEDED"},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/jobs/j1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for delete")
	}
	result, _ := resp["response"].(map[string]any)
	if result["@type"] != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("delete response = %#v", result)
	}
	name, _ := resp["name"].(string)
	if name == "" {
		t.Fatal("expected operation name in response")
	}
	meta, _ := resp["metadata"].(map[string]any)
	if meta == nil {
		t.Fatal("expected metadata in response")
	}
	if meta["verb"] != "delete" {
		t.Fatalf("expected verb=delete, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/jobs/j1" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}

	// Verify job was removed
	api.mu.RLock()
	_, exists := api.jobs["projects/test/locations/us-central1/jobs/j1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("job should have been deleted")
	}
}

func TestDeleteJobNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/jobs/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPatchJobNotAllowed(t *testing.T) {
	api := newTestAPI()
	body := `{"labels":{"env":"prod"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/jobs/j1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", w.Code, w.Body.String())
	}
}

func TestMetadataOnlyJobNeverFakesTerminalSuccess(t *testing.T) {
	api := newTestAPI()
	body := `{"taskGroups":[{"taskSpec":{},"taskCount":"1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/jobs?jobId=trans", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Initially QUEUED
	api.mu.RLock()
	job := api.jobs["projects/test/locations/us-central1/jobs/trans"]
	initialState := job.Status.State
	api.mu.RUnlock()
	if initialState != "QUEUED" {
		t.Fatalf("expected initial state QUEUED, got %s", initialState)
	}

	api.mu.RLock()
	job = api.jobs["projects/test/locations/us-central1/jobs/trans"]
	finalState := job.Status.State
	api.mu.RUnlock()
	if finalState != "QUEUED" {
		t.Fatalf("expected final state QUEUED, got %s", finalState)
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	// Create and delete a job to generate an operation
	api.mu.Lock()
	api.jobs["projects/test/locations/us-central1/jobs/op-test"] = &Job{
		Name:       "projects/test/locations/us-central1/jobs/op-test",
		UID:        "uid-op",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		TaskGroups: []TaskGroup{{}},
		Status:     &JobStatus{State: "SUCCEEDED"},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/jobs/op-test", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("delete failed: %d: %s", w.Code, w.Body.String())
	}

	var deleteResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &deleteResp)
	opPath := deleteResp["name"].(string)

	// Get the operation
	req = httptest.NewRequest(http.MethodGet, "/v1/"+opPath, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var opResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &opResp)
	if opResp["done"] != true {
		t.Fatal("expected operation done=true")
	}
	meta := opResp["metadata"].(map[string]any)
	if meta["verb"] != "delete" {
		t.Fatalf("expected verb=delete, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/jobs/op-test" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}
}

func TestGetOperationNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/operations/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		jobs:       make(map[string]*Job),
	}

	// Create a job
	api.mu.Lock()
	api.jobs["projects/p/locations/l/jobs/j1"] = &Job{
		Name:       "projects/p/locations/l/jobs/j1",
		UID:        "uid-persist",
		CreateTime: "2024-06-01T00:00:00Z",
		UpdateTime: "2024-06-01T00:00:00Z",
		TaskGroups: []TaskGroup{{TaskSpec: &TaskSpec{Runnables: []Runnable{{Container: &Container{ImageURI: "img"}}}}}},
		Status:     &JobStatus{State: "SUCCEEDED"},
	}
	api.mu.Unlock()

	// Persist
	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Create a new API and reload
	api2 := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		jobs:       make(map[string]*Job),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	job, ok := api2.jobs["projects/p/locations/l/jobs/j1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("job not found after reload")
	}
	if job.UID != "uid-persist" {
		t.Fatalf("expected uid-persist, got %s", job.UID)
	}
	if job.Status == nil || job.Status.State != "SUCCEEDED" {
		t.Fatal("status lost after reload")
	}
	if len(job.TaskGroups) == 0 || job.TaskGroups[0].TaskSpec == nil {
		t.Fatal("taskGroups lost after reload")
	}
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	// Concurrent creates
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"taskGroups":[{"taskSpec":{},"taskCount":"1"}]}`
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/projects/test/locations/us-central1/jobs?jobId=job-%d", idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("unexpected status %d for create %d", w.Code, idx)
			}
		}(i)
	}

	// Concurrent gets/lists
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/jobs", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// mockStore is a simple in-memory state store for testing.
type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

type nthFailBatchStore struct {
	mu     sync.Mutex
	data   map[string][]byte
	saves  int
	failAt int
}

func (s *nthFailBatchStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.data[name]
	if !ok {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *nthFailBatchStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saves == s.failAt {
		return errors.New("injected save failure")
	}
	raw, err := json.Marshal(value)
	if err == nil {
		s.data[name] = raw
	}
	return err
}

type fakeContainerRunner struct {
	mu          sync.Mutex
	checkErr    error
	runErr      error
	result      containerResult
	workload    containerWorkload
	cleaned     containerOwnership
	started     chan struct{}
	block       chan struct{}
	cleanedDone chan struct{}
}

func (f *fakeContainerRunner) Check(context.Context) error {
	return f.checkErr
}

func (f *fakeContainerRunner) Run(ctx context.Context, workload containerWorkload) (containerResult, error) {
	f.mu.Lock()
	f.workload = workload
	started := f.started
	block := f.block
	result, err := f.result, f.runErr
	f.mu.Unlock()
	if started != nil {
		close(started)
	}
	if block != nil {
		select {
		case <-ctx.Done():
			return containerResult{}, ctx.Err()
		case <-block:
		}
	}
	return result, err
}

func (f *fakeContainerRunner) Cleanup(_ context.Context, ownership containerOwnership) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleaned = ownership
	if f.cleanedDone == nil {
		f.cleanedDone = make(chan struct{})
	}
	select {
	case <-f.cleanedDone:
	default:
		close(f.cleanedDone)
	}
	return nil
}

func (f *fakeContainerRunner) cleanedCh() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cleanedDone == nil {
		f.cleanedDone = make(chan struct{})
	}
	return f.cleanedDone
}

func waitForJobState(t *testing.T, api *API, name, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		api.mu.RLock()
		job := api.jobs[name]
		got := ""
		if job != nil && job.Status != nil {
			got = job.Status.State
		}
		api.mu.RUnlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	api.mu.RLock()
	job := deepCopyJob(api.jobs[name])
	api.mu.RUnlock()
	t.Fatalf("timed out waiting for %s: %#v", want, job)
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return fmt.Errorf("not found: %w", state.ErrNotFound)
	}
	return json.Unmarshal(raw, target)
}

func (m *mockStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
