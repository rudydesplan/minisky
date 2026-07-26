package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/router"
	"minisky/pkg/state"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestUploadRejectsDeclaredOversizeBeforeReading(t *testing.T) {
	t.Parallel()

	api := newAPI(nil, nil)
	request := httptest.NewRequest(
		http.MethodPost,
		"/upload/bigquery/v2/projects/demo/jobs?uploadType=multipart",
		nil,
	)
	request.Body = panicReadCloser{}
	request.ContentLength = maxBigQueryUploadBodyBytes + 1
	request.Header.Set("Content-Type", "multipart/form-data; boundary=test")
	response := httptest.NewRecorder()

	api.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge ||
		!strings.Contains(response.Body.String(), `"INVALID_ARGUMENT"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestMultipartUploadLimitBoundsTotalRequestBytes(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "multipart-limit")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("file", "events.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(file, strings.Repeat("x", 2048)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(body.Bytes()))
	request.ContentLength = -1
	request.Header.Set("Content-Type", writer.FormDataContentType())

	spool, err := spoolUploadBody(request.Body, request.ContentLength, 1024, newUploadQuota(4096))
	if spool != nil {
		defer spool.Close()
	}
	if !errors.Is(err, errUploadBodyTooLarge) {
		t.Fatalf("error=%v, want body-too-large", err)
	}
}

func TestGeneratedBigQueryClientJobsInsertMediaMultipart(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "generated-multipart")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	api := newAPI(nil, nil)
	proxy := router.NewProxyRouterWithManager(nil)
	proxy.RegisterShim("bigquery.googleapis.com", api)
	server := httptest.NewServer(proxy)
	defer server.Close()

	client, err := bigqueryv2.NewService(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL+"/_minisky/bigquery/bigquery/v2/"),
	)
	if err != nil {
		t.Fatal(err)
	}
	job, err := client.Jobs.Insert("demo", generatedLoadJob("multipart-job")).
		Media(strings.NewReader("{\"name\":\"Ada\"}\n"), googleapi.ChunkSize(0), googleapi.ContentType("application/json")).
		Do()
	if err != nil {
		t.Fatal(err)
	}
	if job.JobReference == nil || job.JobReference.JobId != "multipart-job" {
		t.Fatalf("job=%#v", job)
	}
}

func TestGeneratedBigQueryClientJobsInsertMediaResumable(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "generated-resumable")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	api := newAPI(nil, nil)
	proxy := router.NewProxyRouterWithManager(nil)
	proxy.RegisterShim("bigquery.googleapis.com", api)
	server := httptest.NewServer(proxy)
	defer server.Close()

	client, err := bigqueryv2.NewService(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL+"/_minisky/bigquery/bigquery/v2/"),
	)
	if err != nil {
		t.Fatal(err)
	}
	media := []byte("{\"name\":\"Grace\"}\n")
	job, err := client.Jobs.Insert("demo", generatedLoadJob("resumable-job")).
		ResumableMedia(context.Background(), bytes.NewReader(media), int64(len(media)), "application/json").
		Do()
	if err != nil {
		t.Fatal(err)
	}
	if job.JobReference == nil || job.JobReference.JobId != "resumable-job" {
		t.Fatalf("job=%#v", job)
	}
}

func TestResumableUploadRetainsSessionAcrossPreCommitFailure(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "pre-commit-retry")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{failBeforeCommit: 1}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	location := startResumableUploadForTest(t, api, "pre-commit-job")

	first := completeResumableUploadForTest(api, location, "abc")
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	assertUploadSessionCount(t, 1)
	if files := uploadSessionFileCount(t); files != 1 {
		t.Fatalf("durable session files=%d, want 1 after pre-commit failure", files)
	}
	if jobs := store.jobCount(); jobs != 0 {
		t.Fatalf("pre-commit failure durably created %d jobs", jobs)
	}

	retry := completeResumableUploadForTest(api, location, "abc")
	if retry.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	assertUploadSessionCount(t, 0)
	if files := uploadSessionFileCount(t); files != 0 {
		t.Fatalf("durable session files=%d, want 0 after commit", files)
	}
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("retry durably created %d jobs, want 1", jobs)
	}
	api.mu.RLock()
	jobCount := len(api.jobs)
	api.mu.RUnlock()
	if jobCount != 1 {
		t.Fatalf("retry created %d in-memory jobs, want 1", jobCount)
	}
}

func TestResumableUploadResolvesPostCommitFailureFromDurableTruth(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "post-commit-readback")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{failAfterCommit: 1}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	location := startResumableUploadForTest(t, api, "post-commit-job")

	response := completeResumableUploadForTest(api, location, "abc")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertUploadSessionCount(t, 0)
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("durable job count=%d, want 1", jobs)
	}
	api.mu.RLock()
	job := api.jobs["demo:post-commit-job"]
	jobCount := len(api.jobs)
	api.mu.RUnlock()
	if jobCount != 1 || job == nil {
		t.Fatalf("resolved in-memory jobs=%d job=%#v", jobCount, job)
	}
}

func TestGeneratedBigQueryClientRetriesResumablePreCommitFailure(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "generated-resumable-retry")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{failBeforeCommit: 1}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	proxy := router.NewProxyRouterWithManager(nil)
	proxy.RegisterShim("bigquery.googleapis.com", api)
	server := httptest.NewServer(proxy)
	defer server.Close()

	client, err := bigqueryv2.NewService(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL+"/_minisky/bigquery/bigquery/v2/"),
	)
	if err != nil {
		t.Fatal(err)
	}
	media := []byte("{\"name\":\"retry\"}\n")
	job, err := client.Jobs.Insert("demo", generatedLoadJob("generated-retry-job")).
		ResumableMedia(context.Background(), bytes.NewReader(media), int64(len(media)), "application/json").
		Do()
	if err != nil {
		t.Fatal(err)
	}
	if job.JobReference == nil || job.JobReference.JobId != "generated-retry-job" {
		t.Fatalf("job=%#v", job)
	}
	assertUploadSessionCount(t, 0)
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("generated retry durably created %d jobs, want 1", jobs)
	}
}

func TestCompletedResumableUploadReturnsCachedResponseAfterLostSuccess(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "lost-response")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	location := startResumableUploadForTest(t, api, "lost-response-job")
	first := completeResumableUploadForTest(api, location, "abc")
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	retry := completeResumableUploadForTest(api, location, "abc")
	if retry.Code != first.Code || retry.Body.String() != first.Body.String() {
		t.Fatalf("retry status/body=(%d,%q), want (%d,%q)",
			retry.Code, retry.Body.String(), first.Code, first.Body.String())
	}
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("lost-response retry durably created %d jobs, want 1", jobs)
	}
	assertCompletedUploadCount(t, 1)
}

func TestCommittedUploadRecoversExactResponseAfterFinalizationCrash(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", stateDir)
	t.Setenv("MINISKY_PROFILE", "commit-finalization-crash")
	storeHandle, err := state.New(stateDir, "commit-finalization-crash")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := storeHandle.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	store := &uploadCommitFailureStore{}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	scope := storeHandle.ProfileDir()
	profileState := profileUploadState(scope)
	var expectedBody string
	profileState.mu.Lock()
	profileState.beforeCompletionPersist = func(completed completedUploadSession) error {
		expectedBody = string(completed.Body)
		return errors.New("injected crash before completion finalization")
	}
	profileState.mu.Unlock()

	location := startResumableUploadForTest(t, api, "crash-window-job")
	first := completeResumableUploadForTest(api, location, "abc")
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	if expectedBody == "" {
		t.Fatal("injected crash did not observe the immutable response")
	}
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("committed jobs=%d, want 1", jobs)
	}
	if files := uploadSessionFileCount(t); files == 0 {
		t.Fatal("commit crash removed the only durable session evidence")
	}

	if err := storeHandle.ReconcileOwnedSpools(
		ownership,
		state.OwnedSpoolSpec{Directory: "uploads", Prefixes: []string{".upload-"}},
	); err != nil {
		t.Fatal(err)
	}
	ResetProfileUploadState(scope)
	if err := ReconcileCompletedUploadSessions(scope); err != nil {
		t.Fatal(err)
	}
	restarted, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	retry := completeResumableUploadForTest(restarted, location, "abc")
	if retry.Code != http.StatusOK || retry.Body.String() != expectedBody {
		t.Fatalf("restart retry status/body=(%d,%q), want (200,%q)",
			retry.Code, retry.Body.String(), expectedBody)
	}
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("restart retry durably created %d jobs, want 1", jobs)
	}
}

func TestResumableCompletionRejectsCoincidentalSameJobIdentity(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "coincidental-job-collision")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	location := startResumableUploadForTest(t, api, "collision-job")
	metadata, err := json.Marshal(generatedLoadJob("collision-job"))
	if err != nil {
		t.Fatal(err)
	}
	create := httptest.NewRequest(
		http.MethodPost,
		"/bigquery/v2/projects/demo/jobs",
		bytes.NewReader(metadata),
	)
	createResponse := httptest.NewRecorder()
	api.ServeHTTP(createResponse, create)
	if createResponse.Code != http.StatusOK {
		t.Fatalf("preexisting job status=%d body=%s", createResponse.Code, createResponse.Body.String())
	}

	response := completeResumableUploadForTest(api, location, "abc")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("collision status=%d body=%s", response.Code, response.Body.String())
	}
	assertUploadSessionCount(t, 1)
	assertCompletedUploadCount(t, 0)
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("collision created %d jobs, want 1 preexisting job", jobs)
	}
}

func TestResumableCompletionRejectsMatchingMarkerWithDifferentCreationIdentity(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "creation-identity-collision")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	location := startResumableUploadForTest(t, api, "creation-collision-job")
	metadata, err := json.Marshal(generatedLoadJob("creation-collision-job"))
	if err != nil {
		t.Fatal(err)
	}
	create := httptest.NewRequest(
		http.MethodPost,
		"/bigquery/v2/projects/demo/jobs",
		bytes.NewReader(metadata),
	)
	createResponse := httptest.NewRecorder()
	api.ServeHTTP(createResponse, create)
	if createResponse.Code != http.StatusOK {
		t.Fatalf("preexisting job status=%d body=%s", createResponse.Code, createResponse.Body.String())
	}
	api.mu.Lock()
	api.jobs["demo:creation-collision-job"].UploadSessionID = uploadIDFromLocation(t, location)
	api.jobs["demo:creation-collision-job"].Statistics.CreationTime = "1"
	api.jobs["demo:creation-collision-job"].Statistics.StartTime = "1"
	api.mu.Unlock()
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	response := completeResumableUploadForTest(api, location, "abc")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("collision status=%d body=%s", response.Code, response.Body.String())
	}
	assertUploadSessionCount(t, 1)
	assertCompletedUploadCount(t, 0)
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("collision created %d jobs, want 1 preexisting job", jobs)
	}
}

func TestConcurrentResumableCompletionsReuseFirstPreparedResponse(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "concurrent-completion")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	location := startResumableUploadForTest(t, api, "concurrent-job")

	const attempts = 32
	responses := make(chan *httptest.ResponseRecorder, attempts)
	var workers sync.WaitGroup
	for range attempts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			responses <- completeResumableUploadForTest(api, location, "abc")
		}()
	}
	workers.Wait()
	close(responses)

	var expected string
	for response := range responses {
		if response.Code != http.StatusOK {
			t.Fatalf("completion status=%d body=%s", response.Code, response.Body.String())
		}
		if expected == "" {
			expected = response.Body.String()
		} else if response.Body.String() != expected {
			t.Fatalf("completion body=%q, want byte-identical %q", response.Body.String(), expected)
		}
	}
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("concurrent completions durably created %d jobs, want 1", jobs)
	}
	api.mu.RLock()
	jobCount := len(api.jobs)
	api.mu.RUnlock()
	if jobCount != 1 {
		t.Fatalf("concurrent completions created %d in-memory jobs, want 1", jobCount)
	}
	assertCompletedUploadCount(t, 1)
}

func TestCompletedRetryBypassesDeclaredBodyLimit(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "completed-declared-limit")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	api := newAPI(nil, nil)
	location := startResumableUploadForTest(t, api, "declared-limit-job")
	first := completeResumableUploadForTest(api, location, "abc")
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, location, nil)
	request.Body = panicReadCloser{}
	request.ContentLength = maxBigQueryUploadBodyBytes + 1
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Range", "bytes 0-2/3")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != first.Code || response.Body.String() != first.Body.String() {
		t.Fatalf("oversized replay status/body=(%d,%q), want (%d,%q)",
			response.Code, response.Body.String(), first.Code, first.Body.String())
	}
}

func TestCompletedRetryBypassesExhaustedSpoolQuota(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "completed-quota")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	api := newAPI(nil, nil)
	location := startResumableUploadForTest(t, api, "quota-replay-job")
	first := completeResumableUploadForTest(api, location, "abc")
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	profileState := profileUploadState(filepath.Join(
		os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"),
	))
	profileState.quota.mu.Lock()
	remaining := profileState.quota.max - profileState.quota.used
	profileState.quota.mu.Unlock()
	if !profileState.quota.reserve(remaining) {
		t.Fatal("failed to exhaust upload quota")
	}
	defer profileState.quota.release(remaining)

	request := httptest.NewRequest(http.MethodPost, location, nil)
	request.Body = panicReadCloser{}
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Range", "bytes 0-2/3")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != first.Code || response.Body.String() != first.Body.String() {
		t.Fatalf("quota replay status/body=(%d,%q), want (%d,%q)",
			response.Code, response.Body.String(), first.Code, first.Body.String())
	}
}

func TestUnknownUploadIDStillEnforcesDeclaredBodyLimit(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "unknown-declared-limit")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	request := httptest.NewRequest(
		http.MethodPost,
		"/upload/bigquery/v2/projects/demo/jobs?uploadType=resumable&upload_id=unknown",
		nil,
	)
	request.Body = panicReadCloser{}
	request.ContentLength = maxBigQueryUploadBodyBytes + 1
	request.Header.Set("Content-Type", "application/octet-stream")
	response := httptest.NewRecorder()
	newAPI(nil, nil).ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestCompletedResumableUploadSurvivesProfileStateRestart(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "completed-restart")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	store := &uploadCommitFailureStore{}
	api, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	location := startResumableUploadForTest(t, api, "restart-response-job")
	first := completeResumableUploadForTest(api, location, "abc")
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	scope := filepath.Join(os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"))
	ResetProfileUploadState(scope)
	if err := ReconcileCompletedUploadSessions(scope); err != nil {
		t.Fatal(err)
	}
	restarted, err := newAPIWithMetadataStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	retry := completeResumableUploadForTest(restarted, location, "abc")
	if retry.Code != first.Code || retry.Body.String() != first.Body.String() {
		t.Fatalf("restart retry status/body=(%d,%q), want (%d,%q)",
			retry.Code, retry.Body.String(), first.Code, first.Body.String())
	}
	if jobs := store.jobCount(); jobs != 1 {
		t.Fatalf("restart retry durably created %d jobs, want 1", jobs)
	}
}

func TestCompletedResumableUploadExpiresDuringRestartReconciliation(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "completed-expiry")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	scope := filepath.Join(os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"))
	profileState := profileUploadState(scope)
	oldNow := time.Now().Add(-2 * bigQueryUploadSessionTTL)
	profileState.mu.Lock()
	profileState.now = func() time.Time { return oldNow }
	profileState.mu.Unlock()

	api := newAPI(nil, nil)
	location := startResumableUploadForTest(t, api, "expired-response-job")
	first := completeResumableUploadForTest(api, location, "abc")
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	ResetProfileUploadState(scope)
	if err := ReconcileCompletedUploadSessions(scope); err != nil {
		t.Fatal(err)
	}
	retry := completeResumableUploadForTest(api, location, "abc")
	if retry.Code != http.StatusNotFound {
		t.Fatalf("expired retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	if files := completedUploadFileCount(t); files != 0 {
		t.Fatalf("expired completed files=%d, want 0", files)
	}
}

func TestRandomUploadSessionIDsAreUniqueUnderConcurrency(t *testing.T) {
	const count = 2000
	ids := make(chan string, count)
	errs := make(chan error, count)
	var workers sync.WaitGroup
	for range count {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id, err := randomUploadSessionID()
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	workers.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[string]struct{}, count)
	for id := range ids {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate random upload session ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("generated %d IDs, want %d", len(seen), count)
	}
}

func TestUploadSessionIDCollisionRetriesAndReusesDeterministicJobID(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "session-collision")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	scope := filepath.Join(os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"))
	profileState := profileUploadState(scope)
	ids := []string{"collision", "collision", "unique"}
	profileState.mu.Lock()
	profileState.newSessionID = func() (string, error) {
		id := ids[0]
		ids = ids[1:]
		return id, nil
	}
	profileState.mu.Unlock()

	api := newAPI(nil, nil)
	firstLocation := startResumableUploadForTest(t, api, "")
	secondLocation := startResumableUploadForTest(t, api, "")
	if uploadIDFromLocation(t, firstLocation) != "collision" {
		t.Fatalf("first location=%s", firstLocation)
	}
	if uploadIDFromLocation(t, secondLocation) != "unique" {
		t.Fatalf("second location=%s", secondLocation)
	}

	metadata, ok := profileState.getSession("unique")
	if !ok {
		t.Fatal("collision retry did not persist the unique session")
	}
	var body struct {
		JobReference JobRef `json:"jobReference"`
	}
	if err := json.Unmarshal(metadata, &body); err != nil {
		t.Fatal(err)
	}
	if body.JobReference.JobId != "job_minisky_unique" {
		t.Fatalf("job ID=%q, want deterministic session-derived ID", body.JobReference.JobId)
	}
	first := completeResumableUploadForTest(api, secondLocation, "abc")
	retry := completeResumableUploadForTest(api, secondLocation, "abc")
	if first.Code != http.StatusOK || retry.Code != http.StatusOK ||
		first.Body.String() != retry.Body.String() {
		t.Fatalf("deterministic retry first=(%d,%q) retry=(%d,%q)",
			first.Code, first.Body.String(), retry.Code, retry.Body.String())
	}
	var job Job
	if err := json.Unmarshal(retry.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.JobReference.JobId != "job_minisky_unique" {
		t.Fatalf("retry job ID=%q", job.JobReference.JobId)
	}
	api.mu.RLock()
	jobCount := len(api.jobs)
	api.mu.RUnlock()
	if jobCount != 1 {
		t.Fatalf("deterministic retry created %d jobs, want 1", jobCount)
	}
}

func TestCompletedResumableUploadReconciliationIsBounded(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "completed-bound")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	directory, err := state.OpenOwnedSpoolDirectory(
		os.Getenv("MINISKY_STATE_DIR"), os.Getenv("MINISKY_PROFILE"), "uploads",
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for index := 0; index < maxBigQueryCompletedSessions+2; index++ {
		completed := completedUploadSession{
			UploadID:    fmt.Sprintf("bounded-%03d", index),
			Status:      http.StatusOK,
			Body:        []byte(`{"kind":"bigquery#job"}`),
			CompletedAt: now.Add(time.Duration(index) * time.Millisecond),
			ExpiresAt:   now.Add(bigQueryUploadSessionTTL),
		}
		payload, err := json.Marshal(completed)
		if err != nil {
			t.Fatal(err)
		}
		file, _, err := directory.CreateTemp(".completed-")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(payload); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := directory.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}

	scope := filepath.Join(os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"))
	ResetProfileUploadState(scope)
	if err := ReconcileCompletedUploadSessions(scope); err != nil {
		t.Fatal(err)
	}
	assertCompletedUploadCount(t, maxBigQueryCompletedSessions)
	if files := completedUploadFileCount(t); files != maxBigQueryCompletedSessions {
		t.Fatalf("bounded completed files=%d, want %d", files, maxBigQueryCompletedSessions)
	}
}

func TestUploadWithoutMultipartFileReturns400(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "missing-file")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("metadata", `{}`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/demo/jobs", bytes.NewReader(body.Bytes()))
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response := httptest.NewRecorder()

	newAPI(nil, nil).ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), `"INVALID_ARGUMENT"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestUploadSpoolQuotaIsProfileScopedAndReleased(t *testing.T) {
	t.Parallel()

	first := profileUploadQuota("profile-a")
	if first != profileUploadQuota("profile-a") {
		t.Fatal("same profile did not share aggregate upload quota")
	}
	if first == profileUploadQuota("profile-b") {
		t.Fatal("different profiles shared aggregate upload quota")
	}

	quota := newUploadQuota(10)
	if !quota.reserve(6) || quota.reserve(5) {
		t.Fatal("aggregate quota did not reject concurrent excess")
	}
	quota.release(6)
	if !quota.reserve(10) {
		t.Fatal("released aggregate quota was not reusable")
	}
	quota.release(10)
}

func TestUploadDoesNotRetainUnusedMedia(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", stateDir)
	t.Setenv("MINISKY_PROFILE", "cleanup")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("file", "events.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(file, `{"name":"Ada"}`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/upload/bigquery/v2/projects/demo/jobs", bytes.NewReader(body.Bytes()))
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response := httptest.NewRecorder()

	newAPI(nil, nil).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	uploadDir := filepath.Join(stateDir, "profiles", "cleanup", "uploads")
	entries, err := os.ReadDir(uploadDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("retained upload files: %v", entries)
	}
}

func TestResumableUploadContentRangeMatrix(t *testing.T) {
	tests := []struct {
		name         string
		contentRange string
		body         string
		wantStatus   int
		wantSession  bool
	}{
		{name: "missing", body: "abc", wantStatus: http.StatusBadRequest, wantSession: true},
		{name: "malformed", contentRange: "bytes nope", body: "abc", wantStatus: http.StatusBadRequest, wantSession: true},
		{name: "extra whitespace", contentRange: "bytes  0-2/3", body: "abc", wantStatus: http.StatusBadRequest, wantSession: true},
		{name: "wrong unit", contentRange: "octets 0-2/3", body: "abc", wantStatus: http.StatusBadRequest, wantSession: true},
		{name: "unknown total", contentRange: "bytes 0-2/*", body: "abc", wantStatus: http.StatusNotImplemented, wantSession: true},
		{name: "offset", contentRange: "bytes 1-3/4", body: "abc", wantStatus: http.StatusNotImplemented, wantSession: true},
		{name: "multi chunk", contentRange: "bytes 0-2/6", body: "abc", wantStatus: http.StatusNotImplemented, wantSession: true},
		{name: "end does not match body", contentRange: "bytes 0-3/4", body: "abc", wantStatus: http.StatusBadRequest, wantSession: true},
		{name: "total does not match body", contentRange: "bytes 0-2/4", body: "abc", wantStatus: http.StatusNotImplemented, wantSession: true},
		{name: "complete zero based", contentRange: "bytes 0-2/3", body: "abc", wantStatus: http.StatusOK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("MINISKY_STATE_DIR", t.TempDir())
			t.Setenv("MINISKY_PROFILE", strings.ReplaceAll(test.name, " ", "-"))
			ownership := acquireUploadTestOwnership(t)
			defer ownership.Close()

			api := newAPI(nil, nil)
			metadata, err := json.Marshal(generatedLoadJob("range-job"))
			if err != nil {
				t.Fatal(err)
			}
			start := httptest.NewRequest(
				http.MethodPost,
				"/upload/bigquery/v2/projects/demo/jobs?uploadType=resumable",
				bytes.NewReader(metadata),
			)
			start.Header.Set("Content-Type", "application/json")
			startResponse := httptest.NewRecorder()
			api.ServeHTTP(startResponse, start)
			if startResponse.Code != http.StatusOK {
				t.Fatalf("start status=%d body=%s", startResponse.Code, startResponse.Body.String())
			}

			location := startResponse.Header().Get("Location")
			finish := httptest.NewRequest(http.MethodPost, location, strings.NewReader(test.body))
			finish.Header.Set("Content-Type", "application/octet-stream")
			if test.contentRange != "" {
				finish.Header.Set("Content-Range", test.contentRange)
			}
			finishResponse := httptest.NewRecorder()
			api.ServeHTTP(finishResponse, finish)
			if finishResponse.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s, want %d", finishResponse.Code, finishResponse.Body.String(), test.wantStatus)
			}

			profileState := profileUploadState(filepath.Join(
				os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"),
			))
			profileState.mu.Lock()
			sessionCount := len(profileState.sessions)
			profileState.mu.Unlock()
			if test.wantSession && sessionCount != 1 {
				t.Fatalf("session count=%d, want retained session", sessionCount)
			}
			if !test.wantSession && sessionCount != 0 {
				t.Fatalf("session count=%d, want consumed session", sessionCount)
			}
			api.mu.RLock()
			jobCount := len(api.jobs)
			api.mu.RUnlock()
			if test.wantSession && jobCount != 0 {
				t.Fatalf("invalid range created %d jobs", jobCount)
			}
			if !test.wantSession && jobCount != 1 {
				t.Fatalf("complete range created %d jobs", jobCount)
			}
		})
	}
}

func TestUploadRejectsSymlinkedSpoolDirectory(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", stateDir)
	t.Setenv("MINISKY_PROFILE", "symlink-upload")
	ownership := acquireUploadTestOwnership(t)
	defer ownership.Close()

	external := t.TempDir()
	profileDir := filepath.Join(stateDir, "profiles", "symlink-upload")
	if err := os.Symlink(external, filepath.Join(profileDir, "uploads")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	externalFile := filepath.Join(external, "keep")
	if err := os.WriteFile(externalFile, []byte("external"), 0o600); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"/upload/bigquery/v2/projects/demo/jobs?uploadType=resumable",
		strings.NewReader(`{"configuration":{"load":{}}}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	newAPI(nil, nil).ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(externalFile); err != nil {
		t.Fatalf("external file was touched: %v", err)
	}
}

func acquireUploadTestOwnership(t *testing.T) *state.Ownership {
	t.Helper()
	store, err := state.New(os.Getenv("MINISKY_STATE_DIR"), os.Getenv("MINISKY_PROFILE"))
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	return ownership
}

type uploadCommitFailureStore struct {
	mu               sync.Mutex
	metadata         bigQueryMetadata
	failBeforeCommit int
	failAfterCommit  int
}

func (s *uploadCommitFailureStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.metadata.Jobs == nil && s.metadata.Datasets == nil && s.metadata.Tables == nil {
		return state.ErrNotFound
	}
	payload, err := json.Marshal(s.metadata)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, target)
}

func (s *uploadCommitFailureStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failBeforeCommit > 0 {
		s.failBeforeCommit--
		return errors.New("injected pre-commit failure")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, &s.metadata); err != nil {
		return err
	}
	if s.failAfterCommit > 0 {
		s.failAfterCommit--
		return errors.New("injected post-commit failure")
	}
	return nil
}

func (s *uploadCommitFailureStore) jobCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.metadata.Jobs)
}

func startResumableUploadForTest(t *testing.T, api *API, jobID string) string {
	t.Helper()
	metadata, err := json.Marshal(generatedLoadJob(jobID))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/upload/bigquery/v2/projects/demo/jobs?uploadType=resumable",
		bytes.NewReader(metadata),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("start status=%d body=%s", response.Code, response.Body.String())
	}
	return response.Header().Get("Location")
}

func completeResumableUploadForTest(api *API, location, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, location, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Content-Range", "bytes 0-2/3")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func assertUploadSessionCount(t *testing.T, want int) {
	t.Helper()
	profileState := profileUploadState(filepath.Join(
		os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"),
	))
	profileState.mu.Lock()
	defer profileState.mu.Unlock()
	if got := len(profileState.sessions); got != want {
		t.Fatalf("session count=%d, want %d", got, want)
	}
}

func assertCompletedUploadCount(t *testing.T, want int) {
	t.Helper()
	profileState := profileUploadState(filepath.Join(
		os.Getenv("MINISKY_STATE_DIR"), "profiles", os.Getenv("MINISKY_PROFILE"),
	))
	profileState.mu.Lock()
	defer profileState.mu.Unlock()
	if got := len(profileState.completed); got != want {
		t.Fatalf("completed session count=%d, want %d", got, want)
	}
}

func uploadSessionFileCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(
		os.Getenv("MINISKY_STATE_DIR"),
		"profiles",
		os.Getenv("MINISKY_PROFILE"),
		"uploads",
	))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".session-") {
			count++
		}
	}
	return count
}

func completedUploadFileCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(
		os.Getenv("MINISKY_STATE_DIR"),
		"profiles",
		os.Getenv("MINISKY_PROFILE"),
		"uploads",
	))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".completed-") {
			count++
		}
	}
	return count
}

func uploadIDFromLocation(t *testing.T, location string) string {
	t.Helper()
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Query().Get("upload_id")
}

func generatedLoadJob(jobID string) *bigqueryv2.Job {
	return &bigqueryv2.Job{
		JobReference: &bigqueryv2.JobReference{JobId: jobID, Location: "US"},
		Configuration: &bigqueryv2.JobConfiguration{
			Load: &bigqueryv2.JobConfigurationLoad{
				DestinationTable: &bigqueryv2.TableReference{
					ProjectId: "demo",
					DatasetId: "dataset",
					TableId:   "events",
				},
				SourceFormat: "NEWLINE_DELIMITED_JSON",
			},
		},
	}
}

type panicReadCloser struct{}

func (panicReadCloser) Read([]byte) (int, error) {
	panic("oversized request body was read")
}

func (panicReadCloser) Close() error { return nil }
