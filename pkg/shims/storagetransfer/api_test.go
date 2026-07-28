package storagetransfer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestCreateTransferJob(t *testing.T) {
	api := newTestAPI()
	body := `{"description":"My job","projectId":"test-project","status":"ENABLED","transferSpec":{"gcsDataSource":{"bucketName":"src"},"gcsDataSink":{"bucketName":"dst"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/transferJobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var job TransferJob
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	if job.Name == "" {
		t.Fatal("expected auto-generated name")
	}
	if job.Description != "My job" {
		t.Fatalf("unexpected description: %s", job.Description)
	}
	if job.Status != "ENABLED" {
		t.Fatalf("unexpected status: %s", job.Status)
	}
	if job.CreationTime == "" {
		t.Fatal("expected creationTime")
	}
}

func TestCreateTransferJobRejectsUnsupportedOrIncompleteTransferSpecs(t *testing.T) {
	for name, body := range map[string]string{
		"missing source":            `{"projectId":"test","transferSpec":{"gcsDataSink":{"bucketName":"dst"}}}`,
		"missing sink":              `{"projectId":"test","transferSpec":{"gcsDataSource":{"bucketName":"src"}}}`,
		"empty bucket":              `{"projectId":"test","transferSpec":{"gcsDataSource":{"bucketName":""},"gcsDataSink":{"bucketName":"dst"}}}`,
		"unknown field":             `{"projectId":"test","transferSpec":{"gcsDataSource":{"bucketName":"src"},"gcsDataSink":{"bucketName":"dst"},"awsS3DataSource":{"bucketName":"foreign"}}}`,
		"source path missing slash": `{"projectId":"test","transferSpec":{"gcsDataSource":{"bucketName":"src","path":"prefix"},"gcsDataSink":{"bucketName":"dst"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			api := newTestAPI()
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/transferJobs",
				bytes.NewBufferString(body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			if len(api.jobs) != 0 {
				t.Fatalf("invalid request created %d jobs", len(api.jobs))
			}
		})
	}
}

func TestCreateTransferJobAcceptsFlatGCSObjectPrefixes(t *testing.T) {
	prefixes := []string{
		"repeated//slash/",
		`back\slash/`,
		"dot/../segment/",
		"percent%2Fencoded/",
		"日本語/",
		"/leading/slash/",
		strings.Repeat("é", 511) + "a/",
	}
	for _, side := range []string{"source", "sink"} {
		for _, prefix := range prefixes {
			t.Run(side+"/"+prefix, func(t *testing.T) {
				spec := &TransferSpec{
					GcsDataSource: &GcsData{BucketName: "src"},
					GcsDataSink:   &GcsData{BucketName: "dst"},
				}
				if side == "source" {
					spec.GcsDataSource.Path = prefix
				} else {
					spec.GcsDataSink.Path = prefix
				}
				body, err := json.Marshal(TransferJob{ProjectID: "test", TransferSpec: spec})
				if err != nil {
					t.Fatal(err)
				}
				api := newTestAPI()
				response := httptest.NewRecorder()
				api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/transferJobs", bytes.NewReader(body)))
				if response.Code != http.StatusOK {
					t.Fatalf("status = %d: %s", response.Code, response.Body.String())
				}
			})
		}
	}
}

func TestPatchTransferJobAcceptsFlatGCSObjectPrefixes(t *testing.T) {
	prefixes := []string{
		"repeated//slash/",
		`back\slash/`,
		"dot/../segment/",
		"percent%2Fencoded/",
		"日本語/",
		"/leading/slash/",
		strings.Repeat("é", 511) + "a/",
	}
	for _, side := range []string{"source", "sink"} {
		for _, prefix := range prefixes {
			t.Run(side+"/"+prefix, func(t *testing.T) {
				spec := &TransferSpec{
					GcsDataSource: &GcsData{BucketName: "src"},
					GcsDataSink:   &GcsData{BucketName: "dst"},
				}
				if side == "source" {
					spec.GcsDataSource.Path = prefix
				} else {
					spec.GcsDataSink.Path = prefix
				}
				body, err := json.Marshal(map[string]any{
					"projectId": "test",
					"transferJob": map[string]any{
						"transferSpec": spec,
					},
					"updateTransferJobFieldMask": "transferSpec",
				})
				if err != nil {
					t.Fatal(err)
				}
				api := newTestAPI()
				api.jobs["transferJobs/1"] = &TransferJob{
					Name: "transferJobs/1", ProjectID: "test", Status: "ENABLED",
				}
				response := httptest.NewRecorder()
				api.ServeHTTP(response, httptest.NewRequest(
					http.MethodPatch, "/v1/transferJobs/1", bytes.NewReader(body)))
				if response.Code != http.StatusOK {
					t.Fatalf("status = %d: %s", response.Code, response.Body.String())
				}
			})
		}
	}
}

func TestGCSObjectPrefixRejectsControlsAndUTF8OverflowOnCreateAndPatch(t *testing.T) {
	invalid := []string{
		"line\nbreak/",
		"carriage\rreturn/",
		"nul\x00/",
		"delete\x7f/",
		strings.Repeat("é", 512) + "/",
	}
	for _, method := range []string{"create", "patch"} {
		for _, side := range []string{"source", "sink"} {
			for index, prefix := range invalid {
				t.Run(fmt.Sprintf("%s/%s/%d", method, side, index), func(t *testing.T) {
					spec := &TransferSpec{
						GcsDataSource: &GcsData{BucketName: "src"},
						GcsDataSink:   &GcsData{BucketName: "dst"},
					}
					if side == "source" {
						spec.GcsDataSource.Path = prefix
					} else {
						spec.GcsDataSink.Path = prefix
					}
					api := newTestAPI()
					var request *http.Request
					if method == "create" {
						body, err := json.Marshal(TransferJob{ProjectID: "test", TransferSpec: spec})
						if err != nil {
							t.Fatal(err)
						}
						request = httptest.NewRequest(http.MethodPost, "/v1/transferJobs", bytes.NewReader(body))
					} else {
						api.jobs["transferJobs/1"] = &TransferJob{
							Name: "transferJobs/1", ProjectID: "test", Status: "ENABLED",
						}
						body, err := json.Marshal(map[string]any{
							"projectId": "test",
							"transferJob": map[string]any{
								"transferSpec": spec,
							},
							"updateTransferJobFieldMask": "transferSpec",
						})
						if err != nil {
							t.Fatal(err)
						}
						request = httptest.NewRequest(http.MethodPatch, "/v1/transferJobs/1", bytes.NewReader(body))
					}
					response := httptest.NewRecorder()
					api.ServeHTTP(response, request)
					if response.Code != http.StatusBadRequest {
						t.Fatalf("status = %d: %s", response.Code, response.Body.String())
					}
				})
			}
		}
	}
}

func TestRunTransferJobCopiesObjectsAndPersistsOutcome(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	copier := &fakeObjectCopier{copied: 3, bytes: 17}
	api := newAPI(newTestAPI().opMgr, store)
	api.copier = copier
	api.jobs["transferJobs/1"] = &TransferJob{
		Name:      "transferJobs/1",
		ProjectID: "test",
		Status:    "ENABLED",
		TransferSpec: &TransferSpec{
			GcsDataSource: &GcsData{BucketName: "src", Path: "in/"},
			GcsDataSink:   &GcsData{BucketName: "dst", Path: "out/"},
		},
	}

	run := httptest.NewRecorder()
	api.ServeHTTP(run, httptest.NewRequest(http.MethodPost,
		"/v1/transferJobs/1:run", bytes.NewBufferString(`{"projectId":"test"}`)))
	if run.Code != http.StatusOK {
		t.Fatalf("run failed: %d: %s", run.Code, run.Body.String())
	}
	var operation TransferOperation
	if err := json.Unmarshal(run.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	counters, ok := operation.Metadata["counters"].(map[string]any)
	if !ok || !operation.Done || counters["objectsCopied"] != float64(3) || counters["bytesCopied"] != float64(17) {
		t.Fatalf("unexpected operation: %+v", operation)
	}
	if copier.calls != 1 {
		t.Fatalf("copy calls = %d", copier.calls)
	}

	restarted := newAPI(newTestAPI().opMgr, store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	get := httptest.NewRecorder()
	restarted.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/"+operation.Name, nil))
	if get.Code != http.StatusOK || !bytes.Contains(get.Body.Bytes(), []byte(`"done":true`)) {
		t.Fatalf("persisted operation = %d %s", get.Code, get.Body.String())
	}
}

func TestRunTransferJobReturnsGoogleLongrunningOperationShape(t *testing.T) {
	api := newTestAPI()
	api.copier = &fakeObjectCopier{copied: 2, bytes: 9}
	api.jobs["transferJobs/1"] = &TransferJob{
		Name: "transferJobs/1", ProjectID: "test", Status: "ENABLED",
		TransferSpec: &TransferSpec{
			GcsDataSource: &GcsData{BucketName: "source"},
			GcsDataSink:   &GcsData{BucketName: "sink"},
		},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/transferJobs/1:run", bytes.NewBufferString(`{"projectId":"test"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var operation map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	metadata := operation["metadata"].(map[string]any)
	if metadata["@type"] != "type.googleapis.com/google.storagetransfer.v1.TransferOperation" {
		t.Fatalf("metadata = %#v", metadata)
	}
	terminal := operation["response"].(map[string]any)
	if terminal["@type"] != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("response = %#v", terminal)
	}
	if _, exists := operation["counters"]; exists {
		t.Fatalf("counters leaked outside metadata: %#v", operation)
	}
}

func TestRunTransferJobSaveFailureDoesNotCopy(t *testing.T) {
	copier := &fakeObjectCopier{copied: 1, bytes: 4}
	api := newAPI(newTestAPI().opMgr, failingTransferStore{})
	api.copier = copier
	api.jobs["transferJobs/1"] = &TransferJob{
		Name:      "transferJobs/1",
		ProjectID: "test",
		Status:    "ENABLED",
		TransferSpec: &TransferSpec{
			GcsDataSource: &GcsData{BucketName: "src"},
			GcsDataSink:   &GcsData{BucketName: "dst"},
		},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/transferJobs/1:run", bytes.NewBufferString(`{"projectId":"test"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if copier.calls != 0 {
		t.Fatalf("copy calls = %d, want 0", copier.calls)
	}
}

func TestRunTransferJobSequenceNeverReusesFailedConcurrentReservation(t *testing.T) {
	store := &blockingFirstFailureStore{
		data:    make(map[string][]byte),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	api := newAPI(newTestAPI().opMgr, store)
	api.copier = &fakeObjectCopier{}
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("transferJobs/%d", i)
		api.jobs[name] = &TransferJob{
			Name: name, ProjectID: "test", Status: "ENABLED",
			TransferSpec: &TransferSpec{
				GcsDataSource: &GcsData{BucketName: "src"},
				GcsDataSink:   &GcsData{BucketName: "dst"},
			},
		}
	}

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
			"/v1/transferJobs/1:run", bytes.NewBufferString(`{"projectId":"test"}`)))
		firstDone <- response
	}()
	<-store.entered

	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
			"/v1/transferJobs/2:run", bytes.NewBufferString(`{"projectId":"test"}`)))
		secondDone <- response
	}()
	for {
		api.mu.RLock()
		reserved := api.operationSeq
		api.mu.RUnlock()
		if reserved == 2 {
			break
		}
	}
	close(store.release)
	if response := <-firstDone; response.Code != http.StatusServiceUnavailable {
		t.Fatalf("first status = %d: %s", response.Code, response.Body.String())
	}
	if response := <-secondDone; response.Code != http.StatusOK {
		t.Fatalf("second status = %d: %s", response.Code, response.Body.String())
	}

	third := httptest.NewRecorder()
	api.ServeHTTP(third, httptest.NewRequest(http.MethodPost,
		"/v1/transferJobs/3:run", bytes.NewBufferString(`{"projectId":"test"}`)))
	var operation TransferOperation
	if err := json.Unmarshal(third.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if operation.Name != "transferOperations/3" {
		t.Fatalf("third operation = %q, want monotonic transferOperations/3", operation.Name)
	}
}

func TestResponseRecorderCapsWhileWriting(t *testing.T) {
	recorder := newResponseRecorder(8)
	if _, err := io.Copy(recorder, bytes.NewReader(bytes.Repeat([]byte("x"), 1024))); err == nil {
		t.Fatal("oversized response write succeeded")
	}
	if recorder.body.Len() > 8 {
		t.Fatalf("recorder retained %d bytes, limit 8", recorder.body.Len())
	}
}

func TestHandlerObjectCopierStreamsObjectWithinBound(t *testing.T) {
	var sink bytes.Buffer
	var sinkContentLength int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("abcd"))
			_, _ = w.Write([]byte("efgh"))
		case http.MethodPost:
			sinkContentLength = r.ContentLength
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			sink.Write(data)
			w.WriteHeader(http.StatusOK)
		}
	})
	copier := handlerObjectCopier{handler: handler}
	if err := copier.copyObject(context.Background(), "/source", "/sink", 8); err != nil {
		t.Fatal(err)
	}
	if sink.String() != "abcdefgh" {
		t.Fatalf("streamed sink = %q", sink.String())
	}
	if sinkContentLength != 8 {
		t.Fatalf("sink Content-Length = %d, want 8", sinkContentLength)
	}
}

func TestHandlerObjectCopierRejectsObjectOutsideRequestedPrefix(t *testing.T) {
	var downloadCalls int
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/storage/v1/"):
			_, _ = io.WriteString(w, `{"items":[{"name":"outside.txt","size":"1"}]}`)
		case strings.HasPrefix(r.URL.Path, "/download/"):
			downloadCalls++
			_, _ = io.WriteString(w, "x")
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	copier := handlerObjectCopier{handler: handler}
	_, _, err := copier.Copy(context.Background(),
		GcsData{BucketName: "source", Path: "prefix/"},
		GcsData{BucketName: "sink", Path: "destination/"})
	if err == nil {
		t.Fatal("object outside requested prefix was copied")
	}
	if downloadCalls != 0 {
		t.Fatalf("download calls = %d, want 0", downloadCalls)
	}
}

func TestRunTransferJobRejectsConcurrentRunPerJob(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	api := newTestAPI()
	api.copier = objectCopierFunc(func(context.Context, GcsData, GcsData) (int64, int64, error) {
		close(started)
		<-release
		return 0, 0, nil
	})
	api.jobs["transferJobs/1"] = &TransferJob{
		Name: "transferJobs/1", ProjectID: "test", Status: "ENABLED",
		TransferSpec: &TransferSpec{
			GcsDataSource: &GcsData{BucketName: "src"},
			GcsDataSink:   &GcsData{BucketName: "dst"},
		},
	}
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
			"/v1/transferJobs/1:run", bytes.NewBufferString(`{"projectId":"test"}`)))
		firstDone <- response
	}()
	<-started

	second := httptest.NewRecorder()
	api.ServeHTTP(second, httptest.NewRequest(http.MethodPost,
		"/v1/transferJobs/1:run", bytes.NewBufferString(`{"projectId":"test"}`)))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second run status = %d: %s", second.Code, second.Body.String())
	}
	close(release)
	if first := <-firstDone; first.Code != http.StatusOK {
		t.Fatalf("first run status = %d: %s", first.Code, first.Body.String())
	}
}

func TestCreateTransferJobDefaultStatus(t *testing.T) {
	api := newTestAPI()
	body := `{"projectId":"test","transferSpec":{"gcsDataSource":{"bucketName":"src"},"gcsDataSink":{"bucketName":"dst"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/transferJobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var job TransferJob
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	if job.Status != "ENABLED" {
		t.Fatalf("expected default ENABLED status, got %s", job.Status)
	}
}

func TestCreateTransferJobInvalidStatus(t *testing.T) {
	api := newTestAPI()
	body := `{"status":"INVALID"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/transferJobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetTransferJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["transferJobs/123"] = &TransferJob{
		Name:        "transferJobs/123",
		Description: "test",
		Status:      "ENABLED",
		ProjectID:   "test",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/transferJobs/123?projectId=test", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var job TransferJob
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	if job.Description != "test" {
		t.Fatalf("unexpected description: %s", job.Description)
	}
}

func TestGetTransferJobNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/transferJobs/missing?projectId=test", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListTransferJobs(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["transferJobs/1"] = &TransferJob{Name: "transferJobs/1", Status: "ENABLED", ProjectID: "test"}
	api.jobs["transferJobs/2"] = &TransferJob{Name: "transferJobs/2", Status: "ENABLED", ProjectID: "test"}
	api.jobs["transferJobs/3"] = &TransferJob{Name: "transferJobs/3", Status: "DELETED", ProjectID: "test"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, `/v1/transferJobs?filter=%7B%22projectId%22%3A%22test%22%7D`, nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	jobs := resp["transferJobs"].([]any)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs (excluding DELETED), got %d", len(jobs))
	}
}

func TestListTransferJobsPagination(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("transferJobs/%d", i)
		api.jobs[name] = &TransferJob{Name: name, Status: "ENABLED", ProjectID: "test"}
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, `/v1/transferJobs?pageSize=2&filter=%7B%22projectId%22%3A%22test%22%7D`, nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	jobs := resp["transferJobs"].([]any)
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken")
	}
}

func TestPatchTransferJob(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["transferJobs/1"] = &TransferJob{
		Name:         "transferJobs/1",
		Description:  "old",
		Status:       "ENABLED",
		ProjectID:    "test",
		CreationTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	body := `{"projectId":"test","transferJob":{"description":"new"},"updateTransferJobFieldMask":"description"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/transferJobs/1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	job := api.jobs["transferJobs/1"]
	api.mu.RUnlock()
	if job.Description != "new" {
		t.Fatalf("expected updated description, got %s", job.Description)
	}
	if job.CreationTime != "2024-01-01T00:00:00Z" {
		t.Fatal("creationTime should be preserved")
	}
}

func TestPatchTransferJobRequiresMaskedFieldAndValidOutcomeState(t *testing.T) {
	for name, body := range map[string]string{
		"missing masked field": `{"projectId":"test","transferJob":{"description":"new"},"updateTransferJobFieldMask":"status"}`,
		"invalid status":       `{"projectId":"test","transferJob":{"status":"BROKEN"},"updateTransferJobFieldMask":"status"}`,
		"immutable project":    `{"projectId":"test","transferJob":{"projectId":"other"},"updateTransferJobFieldMask":"projectId"}`,
		"invalid source path":  `{"projectId":"test","transferJob":{"transferSpec":{"gcsDataSource":{"bucketName":"src","path":"prefix"},"gcsDataSink":{"bucketName":"dst"}}},"updateTransferJobFieldMask":"transferSpec"}`,
	} {
		t.Run(name, func(t *testing.T) {
			api := newTestAPI()
			api.jobs["transferJobs/1"] = &TransferJob{
				Name: "transferJobs/1", ProjectID: "test", Status: "ENABLED", Description: "old",
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, "/v1/transferJobs/1",
				bytes.NewBufferString(body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			if job := api.jobs["transferJobs/1"]; job.Status != "ENABLED" || job.Description != "old" {
				t.Fatalf("invalid patch mutated job: %+v", job)
			}
		})
	}
}

func TestPatchTransferJobCompensatesPostCommitSaveError(t *testing.T) {
	store := &postCommitTransferStore{data: make(map[string][]byte), failNext: true}
	api := newAPI(newTestAPI().opMgr, store)
	name := "transferJobs/1"
	api.jobs[name] = &TransferJob{Name: name, ProjectID: "test", Status: "ENABLED", Description: "old"}
	if err := store.saveDirect(storagetransferMetadata{
		Jobs: map[string]*TransferJob{name: cloneJob(api.jobs[name])},
	}); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, "/v1/"+name,
		bytes.NewBufferString(`{"projectId":"test","transferJob":{"description":"new"},"updateTransferJobFieldMask":"description"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.jobs[name].Description; got != "old" {
		t.Fatalf("visible description = %q, want old", got)
	}
	var durable storagetransferMetadata
	if err := store.Load(storagetransferStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if got := durable.Jobs[name].Description; got != "old" {
		t.Fatalf("durable description = %q, want compensated old", got)
	}
}

func TestPatchTransferJobSoftDelete(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.jobs["transferJobs/1"] = &TransferJob{
		Name:      "transferJobs/1",
		ProjectID: "test",
		Status:    "ENABLED",
	}
	api.mu.Unlock()

	body := `{"projectId":"test","transferJob":{"status":"DELETED"},"updateTransferJobFieldMask":"status"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/transferJobs/1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	api.mu.RLock()
	job := api.jobs["transferJobs/1"]
	api.mu.RUnlock()
	if job.Status != "DELETED" {
		t.Fatalf("expected DELETED status, got %s", job.Status)
	}
}

func TestPatchTransferJobNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"projectId":"test","transferJob":{"description":"x"},"updateTransferJobFieldMask":"description"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/transferJobs/missing", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newAPI(newTestAPI().opMgr, store)

	api.mu.Lock()
	api.seqNum = 5
	api.jobs["transferJobs/5"] = &TransferJob{
		Name:        "transferJobs/5",
		Description: "persist test",
		Status:      "ENABLED",
	}
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	api2 := newAPI(newTestAPI().opMgr, store)
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	if api2.seqNum != 5 {
		t.Fatalf("expected seqNum=5, got %d", api2.seqNum)
	}
	job, ok := api2.jobs["transferJobs/5"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("job not found after reload")
	}
	if job.Description != "persist test" {
		t.Fatalf("unexpected description after reload: %s", job.Description)
	}
}

func TestCorruptStateFailsClosedWithoutOverwritingSnapshot(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-storage-transfer")
	store, err := state.New(root, "corrupt-storage-transfer")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(storagetransferStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}

	api := NewAPI(nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		`/v1/transferJobs?filter=%7B%22projectId%22%3A%22test%22%7D`, nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(storagetransferStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"description":"job %d","projectId":"test","status":"ENABLED","transferSpec":{"gcsDataSource":{"bucketName":"src"},"gcsDataSink":{"bucketName":"dst"}}}`, idx)
			req := httptest.NewRequest(http.MethodPost, "/v1/transferJobs", bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d", w.Code)
			}
		}(i)
	}
	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

type fakeObjectCopier struct {
	calls  int
	copied int64
	bytes  int64
	err    error
}

type objectCopierFunc func(context.Context, GcsData, GcsData) (int64, int64, error)

func (f objectCopierFunc) Copy(ctx context.Context, source, sink GcsData) (int64, int64, error) {
	return f(ctx, source, sink)
}

type failingTransferStore struct{}

func (failingTransferStore) Load(string, any) error { return state.ErrNotFound }
func (failingTransferStore) Save(string, any) error { return fmt.Errorf("injected save failure") }

type blockingFirstFailureStore struct {
	mu      sync.Mutex
	data    map[string][]byte
	saves   int
	entered chan struct{}
	release chan struct{}
}

func (s *blockingFirstFailureStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *blockingFirstFailureStore) Save(name string, value any) error {
	s.mu.Lock()
	s.saves++
	save := s.saves
	s.mu.Unlock()
	if save == 1 {
		close(s.entered)
		<-s.release
		return errors.New("injected first save failure")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.data[name] = raw
	s.mu.Unlock()
	return nil
}

type postCommitTransferStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	failNext bool
}

func (s *postCommitTransferStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *postCommitTransferStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data[name] = raw
	if s.failNext {
		s.failNext = false
		return errors.New("post-commit save error")
	}
	return nil
}

func (s *postCommitTransferStore) saveDirect(value any) error {
	s.mu.Lock()
	failNext := s.failNext
	s.failNext = false
	s.mu.Unlock()
	err := s.Save(storagetransferStateEntry, value)
	s.mu.Lock()
	s.failNext = failNext
	s.mu.Unlock()
	return err
}

func (f *fakeObjectCopier) Copy(context.Context, GcsData, GcsData) (int64, int64, error) {
	f.calls++
	return f.copied, f.bytes, f.err
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
