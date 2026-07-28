package storagetransfer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const (
	maxTransferObjects       = 1000
	maxTransferBytes   int64 = 64 << 20
	maxTransferObject  int64 = 16 << 20
	maxListResponse    int64 = 4 << 20
	maxConcurrentRuns        = 4
)

var errResponseLimit = fmt.Errorf("internal Storage response exceeds transfer limit")

func init() {
	registry.Register("storagetransfer.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (Storage Transfer v1)
// ─────────────────────────────────────────────────────────────────────────────

// TransferJob represents a Storage Transfer job.
type TransferJob struct {
	Name                 string        `json:"name"`
	Description          string        `json:"description,omitempty"`
	Status               string        `json:"status,omitempty"`
	ProjectID            string        `json:"projectId,omitempty"`
	Schedule             *Schedule     `json:"schedule,omitempty"`
	TransferSpec         *TransferSpec `json:"transferSpec,omitempty"`
	CreationTime         string        `json:"creationTime,omitempty"`
	LastModificationTime string        `json:"lastModificationTime,omitempty"`
}

// Schedule represents a transfer schedule.
type Schedule struct {
	ScheduleStartDate *Date `json:"scheduleStartDate,omitempty"`
	ScheduleEndDate   *Date `json:"scheduleEndDate,omitempty"`
}

// Date represents a calendar date.
type Date struct {
	Year  int `json:"year,omitempty"`
	Month int `json:"month,omitempty"`
	Day   int `json:"day,omitempty"`
}

// TransferSpec describes the data source and sink.
type TransferSpec struct {
	GcsDataSource *GcsData `json:"gcsDataSource,omitempty"`
	GcsDataSink   *GcsData `json:"gcsDataSink,omitempty"`
}

// GcsData represents a GCS bucket reference.
type GcsData struct {
	BucketName string `json:"bucketName,omitempty"`
	Path       string `json:"path,omitempty"`
}

type TransferCounters struct {
	ObjectsCopied int64 `json:"objectsCopied"`
	BytesCopied   int64 `json:"bytesCopied"`
}

type TransferOperation struct {
	Name     string         `json:"name"`
	Done     bool           `json:"done"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Response map[string]any `json:"response,omitempty"`
	Error    map[string]any `json:"error,omitempty"`
}

type objectCopier interface {
	Copy(context.Context, GcsData, GcsData) (objects, bytes int64, err error)
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Storage Transfer v1 REST shim.
type API struct {
	mu           sync.RWMutex
	persistMu    sync.Mutex
	opMgr        *orchestrator.OperationManager
	stateStore   storagetransferStateStore
	jobs         map[string]*TransferJob
	seqNum       int
	copier       objectCopier
	operations   map[string]*TransferOperation
	operationSeq int
	activeRuns   map[string]int
	totalRuns    int
	initErr      error
}

// NewAPI creates a new Storage Transfer API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := newAPI(opMgr, state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: StorageTransfer] persistence degraded: %v", err)
		api.initErr = fmt.Errorf("open Storage Transfer state: %w", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: StorageTransfer] state rehydration failed: %v", err)
		api.initErr = fmt.Errorf("load Storage Transfer state: %w", err)
	}
	return api
}

func newAPI(opMgr *orchestrator.OperationManager, store storagetransferStateStore) *API {
	return &API{
		opMgr:      opMgr,
		stateStore: store,
		jobs:       make(map[string]*TransferJob),
		operations: make(map[string]*TransferOperation),
		activeRuns: make(map[string]int),
	}
}

func newTestAPI() *API {
	return newAPI(orchestrator.NewOperationManager(), nil)
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if handler := ctx.GetShim("storage.googleapis.com"); handler != nil {
		api.copier = handlerObjectCopier{handler: handler}
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: StorageTransfer] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initErr != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Storage Transfer state is unavailable")
		return
	}

	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/transferOperations/") && r.Method == http.MethodGet:
		api.getTransferOperation(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/transferJobs/") && strings.HasSuffix(r.URL.Path, ":run") && r.Method == http.MethodPost:
		api.runTransferJob(w, r)
	case r.URL.Path == "/v1/transferJobs" && r.Method == http.MethodPost:
		api.createTransferJob(w, r)
	case r.URL.Path == "/v1/transferJobs" && r.Method == http.MethodGet:
		api.listTransferJobs(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/transferJobs/") && r.Method == http.MethodGet:
		api.getTransferJob(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/transferJobs/") && r.Method == http.MethodPatch:
		api.patchTransferJob(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Storage Transfer resource not found")
	}
}

func (api *API) runTransferJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := strings.TrimSuffix(extractJobName(r.URL.Path), ":run")
	var request struct {
		ProjectID string `json:"projectId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.ProjectID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "projectId is required")
		return
	}

	api.mu.RLock()
	job := cloneJob(api.jobs[name])
	copier := api.copier
	api.mu.RUnlock()
	if job == nil || job.ProjectID != request.ProjectID {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transfer job not found: "+name)
		return
	}
	if job.Status != "ENABLED" {
		writeError(w, http.StatusFailedDependency, "FAILED_PRECONDITION", "transfer job is not enabled")
		return
	}
	if job.TransferSpec == nil || job.TransferSpec.GcsDataSource == nil || job.TransferSpec.GcsDataSink == nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bounded execution requires GCS source and sink")
		return
	}
	if copier == nil {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "local Storage backend is unavailable")
		return
	}

	api.mu.Lock()
	if api.activeRuns[name] > 0 || api.totalRuns >= maxConcurrentRuns {
		api.mu.Unlock()
		writeError(w, http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "transfer run concurrency limit reached")
		return
	}
	api.activeRuns[name]++
	api.totalRuns++
	api.mu.Unlock()
	defer func() {
		api.mu.Lock()
		api.activeRuns[name]--
		if api.activeRuns[name] == 0 {
			delete(api.activeRuns, name)
		}
		api.totalRuns--
		api.mu.Unlock()
	}()

	api.mu.Lock()
	api.operationSeq++
	operation := &TransferOperation{
		Name: "transferOperations/" + strconv.Itoa(api.operationSeq),
		Metadata: map[string]any{
			"@type":           "type.googleapis.com/google.storagetransfer.v1.TransferOperation",
			"name":            "transferOperations/" + strconv.Itoa(api.operationSeq),
			"transferJobName": name,
			"projectId":       request.ProjectID,
			"status":          "IN_PROGRESS",
			"counters":        TransferCounters{},
		},
	}
	api.operations[operation.Name] = operation
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.operations, operation.Name)
		api.mu.Unlock()
		api.compensateState(err)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "State persistence failed")
		return
	}

	objects, copiedBytes, copyErr := copier.Copy(r.Context(), *job.TransferSpec.GcsDataSource, *job.TransferSpec.GcsDataSink)
	api.mu.Lock()
	operation.Done = true
	operation.Metadata["status"] = "SUCCESS"
	operation.Metadata["counters"] = TransferCounters{ObjectsCopied: objects, BytesCopied: copiedBytes}
	if copyErr != nil {
		operation.Metadata["status"] = "FAILED"
		operation.Error = map[string]any{"code": 13, "message": copyErr.Error()}
	} else {
		operation.Response = map[string]any{"@type": "type.googleapis.com/google.protobuf.Empty"}
	}
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Transfer completed but outcome persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(cloneTransferOperation(operation))
}

func (api *API) getTransferOperation(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/")
	api.mu.RLock()
	operation := cloneTransferOperation(api.operations[name])
	api.mu.RUnlock()
	if operation == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transfer operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(operation)
}

type handlerObjectCopier struct {
	handler http.Handler
}

func (c handlerObjectCopier) Copy(ctx context.Context, source, sink GcsData) (int64, int64, error) {
	listPath := "/storage/v1/b/" + url.PathEscape(source.BucketName) + "/o?maxResults=1000"
	if source.Path != "" {
		listPath += "&prefix=" + url.QueryEscape(source.Path)
	}
	list, err := c.request(ctx, http.MethodGet, listPath, nil, 0, maxListResponse)
	if err != nil {
		return 0, 0, err
	}
	var response struct {
		Items []struct {
			Name string `json:"name"`
			Size string `json:"size"`
		} `json:"items"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(list, &response); err != nil {
		return 0, 0, fmt.Errorf("decode source object list: %w", err)
	}
	if response.NextPageToken != "" {
		return 0, 0, fmt.Errorf("transfer exceeds 1000 object local limit")
	}
	if len(response.Items) > maxTransferObjects {
		return 0, 0, fmt.Errorf("transfer exceeds 1000 object local limit")
	}
	var copiedObjects, copiedBytes int64
	for _, object := range response.Items {
		size, err := strconv.ParseInt(object.Size, 10, 64)
		if err != nil || size < 0 {
			return copiedObjects, copiedBytes, fmt.Errorf("invalid source object size")
		}
		if size > maxTransferObject || size > maxTransferBytes-copiedBytes {
			return copiedObjects, copiedBytes, fmt.Errorf("transfer exceeds local byte limit")
		}
		if source.Path != "" && !strings.HasPrefix(object.Name, source.Path) {
			return copiedObjects, copiedBytes, fmt.Errorf("source object is outside requested prefix")
		}
		relative := strings.TrimPrefix(object.Name, source.Path)
		destination := sink.Path + relative
		if err := c.copyObject(ctx,
			"/download/storage/v1/b/"+url.PathEscape(source.BucketName)+"/o/"+url.PathEscape(object.Name)+"?alt=media",
			"/upload/storage/v1/b/"+url.PathEscape(sink.BucketName)+"/o?uploadType=media&name="+url.QueryEscape(destination),
			size); err != nil {
			return copiedObjects, copiedBytes, err
		}
		copiedObjects++
		copiedBytes += size
	}
	return copiedObjects, copiedBytes, nil
}

func (c handlerObjectCopier) copyObject(ctx context.Context, sourcePath, sinkPath string, size int64) error {
	reader, writer := io.Pipe()
	sourceRecorder := newStreamingResponseWriter(writer, size)
	sourceDone := make(chan struct{})
	go func() {
		defer close(sourceDone)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourcePath, nil)
		if err != nil {
			sourceRecorder.fail(err)
			return
		}
		request.Host = "storage.googleapis.com"
		c.handler.ServeHTTP(sourceRecorder, request)
		sourceRecorder.finish()
	}()

	status := <-sourceRecorder.ready
	if status < 200 || status >= 300 {
		_ = reader.CloseWithError(fmt.Errorf("Storage request failed with status %d", status))
		<-sourceDone
		return fmt.Errorf("Storage request failed with status %d", status)
	}
	_, sinkErr := c.request(ctx, http.MethodPost, sinkPath, reader, size, maxListResponse)
	_ = reader.CloseWithError(sinkErr)
	<-sourceDone
	if sourceRecorder.err != nil {
		return sourceRecorder.err
	}
	if sourceRecorder.written != size {
		return fmt.Errorf("source object size changed during transfer")
	}
	return sinkErr
}

func (c handlerObjectCopier) request(
	ctx context.Context,
	method, path string,
	body io.Reader,
	contentLength, limit int64,
) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	request.ContentLength = contentLength
	request.Host = "storage.googleapis.com"
	recorder := newResponseRecorder(limit)
	c.handler.ServeHTTP(recorder, request)
	if recorder.err != nil {
		return nil, recorder.err
	}
	if recorder.status < 200 || recorder.status >= 300 {
		return nil, fmt.Errorf("Storage request failed with status %d", recorder.status)
	}
	return recorder.body.Bytes(), nil
}

type responseRecorder struct {
	header http.Header
	body   bytes.Buffer
	status int
	limit  int64
	err    error
}

type streamingResponseWriter struct {
	header  http.Header
	writer  *io.PipeWriter
	ready   chan int
	once    sync.Once
	status  int
	limit   int64
	written int64
	err     error
}

func newStreamingResponseWriter(writer *io.PipeWriter, limit int64) *streamingResponseWriter {
	return &streamingResponseWriter{
		header: make(http.Header), writer: writer, ready: make(chan int, 1), limit: limit,
	}
}

func (w *streamingResponseWriter) Header() http.Header { return w.header }

func (w *streamingResponseWriter) WriteHeader(status int) {
	w.once.Do(func() {
		w.status = status
		w.ready <- status
		close(w.ready)
	})
}

func (w *streamingResponseWriter) Write(data []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	if w.err != nil {
		return 0, w.err
	}
	if int64(len(data)) > w.limit-w.written {
		w.err = errResponseLimit
		return 0, w.err
	}
	n, err := w.writer.Write(data)
	w.written += int64(n)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w *streamingResponseWriter) finish() {
	w.WriteHeader(http.StatusOK)
	_ = w.writer.CloseWithError(w.err)
}

func (w *streamingResponseWriter) fail(err error) {
	w.err = err
	w.WriteHeader(http.StatusInternalServerError)
	_ = w.writer.CloseWithError(err)
}

func newResponseRecorder(limit int64) *responseRecorder {
	return &responseRecorder{header: make(http.Header), status: http.StatusOK, limit: limit}
}

func (r *responseRecorder) Header() http.Header { return r.header }
func (r *responseRecorder) Write(data []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	remaining := r.limit - int64(r.body.Len())
	if remaining < int64(len(data)) {
		if remaining > 0 {
			_, _ = r.body.Write(data[:remaining])
		}
		r.err = errResponseLimit
		return 0, r.err
	}
	return r.body.Write(data)
}
func (r *responseRecorder) WriteHeader(status int) { r.status = status }

func cloneTransferOperation(operation *TransferOperation) *TransferOperation {
	if operation == nil {
		return nil
	}
	raw, _ := json.Marshal(operation)
	var clone TransferOperation
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createTransferJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var job TransferJob
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&job); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if job.Status == "" {
		job.Status = "ENABLED"
	}
	if job.Status != "ENABLED" && job.Status != "DISABLED" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "status must be ENABLED or DISABLED")
		return
	}
	if err := validateStorageTransferJob(&job, false); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if err := validateStorageTransferSpec(job.TransferSpec); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	api.mu.Lock()
	api.seqNum++
	name := fmt.Sprintf("transferJobs/%d", api.seqNum)
	job.Name = name
	job.CreationTime = now
	job.LastModificationTime = now
	api.jobs[name] = &job
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.jobs, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(&job)
}

func (api *API) getTransferJob(w http.ResponseWriter, r *http.Request) {
	name := extractJobName(r.URL.Path)
	projectID := r.URL.Query().Get("projectId")
	if projectID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "projectId query parameter is required")
		return
	}

	api.mu.RLock()
	job, ok := api.jobs[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transfer job not found: "+name)
		return
	}
	clone := cloneJob(job)
	api.mu.RUnlock()
	if clone.ProjectID != projectID {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transfer job not found: "+name)
		return
	}

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listTransferJobs(w http.ResponseWriter, r *http.Request) {
	filter := r.URL.Query().Get("filter")
	var filterValue struct {
		ProjectID string `json:"projectId"`
	}
	if filter == "" || json.Unmarshal([]byte(filter), &filterValue) != nil || filterValue.ProjectID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "filter must be JSON containing projectId")
		return
	}
	pageSize := 50
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}
	pageToken := r.URL.Query().Get("pageToken")

	api.mu.RLock()
	var all []*TransferJob
	for _, job := range api.jobs {
		// Exclude soft-deleted jobs from listing
		if job.Status == "DELETED" || job.ProjectID != filterValue.ProjectID {
			continue
		}
		all = append(all, cloneJob(job))
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "storagetransfer.googleapis.com",
		Parent:  "projects/" + filterValue.ProjectID,
		Filter:  filter,
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(job *TransferJob) string { return job.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = []*TransferJob{}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"transferJobs":  result,
		"nextPageToken": nextToken,
	})
}

func (api *API) patchTransferJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := extractJobName(r.URL.Path)

	// Storage Transfer PATCH body wraps the job: {"transferJob": {...}, "updateTransferJobFieldMask": "..."}
	var wrapper struct {
		ProjectID                  string         `json:"projectId"`
		TransferJob                map[string]any `json:"transferJob"`
		UpdateTransferJobFieldMask string         `json:"updateTransferJobFieldMask"`
	}
	if err := json.NewDecoder(r.Body).Decode(&wrapper); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if wrapper.ProjectID == "" || len(wrapper.TransferJob) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "projectId and transferJob are required")
		return
	}
	if wrapper.UpdateTransferJobFieldMask == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "updateTransferJobFieldMask is required")
		return
	}

	api.mu.Lock()
	existing, ok := api.jobs[name]
	if !ok {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transfer job not found: "+name)
		return
	}
	if existing.ProjectID != wrapper.ProjectID {
		api.mu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transfer job not found: "+name)
		return
	}

	previous := cloneJob(existing)
	raw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(raw, &merged)

	fields := strings.Split(wrapper.UpdateTransferJobFieldMask, ",")
	updatingTransferSpec := false
	for _, field := range fields {
		field = strings.TrimSpace(field)
		switch field {
		case "description", "status", "schedule", "transferSpec":
		default:
			api.mu.Unlock()
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unsupported update field mask: "+field)
			return
		}
		v, exists := wrapper.TransferJob[field]
		if !exists {
			api.mu.Unlock()
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "masked field is missing from transferJob: "+field)
			return
		}
		merged[field] = v
		updatingTransferSpec = updatingTransferSpec || field == "transferSpec"
	}

	merged["name"] = existing.Name
	merged["creationTime"] = existing.CreationTime
	merged["lastModificationTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated TransferJob
	_ = json.Unmarshal(updatedRaw, &updated)
	if err := validateStorageTransferJob(&updated, true); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if updatingTransferSpec {
		if err := validateStorageTransferSpec(updated.TransferSpec); err != nil {
			api.mu.Unlock()
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	api.jobs[name] = &updated
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.jobs[name] = previous
		api.mu.Unlock()
		api.compensateState(err)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	_ = json.NewEncoder(w).Encode(&updated)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func extractJobName(path string) string {
	// Path: /v1/transferJobs/{id}
	trimmed := strings.TrimPrefix(path, "/v1/")
	return trimmed
}

func validateStorageTransferJob(job *TransferJob, allowDeleted bool) error {
	if job.ProjectID == "" {
		return fmt.Errorf("projectId is required")
	}
	if job.Status != "ENABLED" && job.Status != "DISABLED" && (!allowDeleted || job.Status != "DELETED") {
		return fmt.Errorf("status must be ENABLED or DISABLED")
	}
	return nil
}

func validateStorageTransferSpec(spec *TransferSpec) error {
	if spec == nil || spec.GcsDataSource == nil || spec.GcsDataSink == nil {
		return fmt.Errorf("bounded execution requires GCS source and sink")
	}
	if spec.GcsDataSource.BucketName == "" || spec.GcsDataSink.BucketName == "" {
		return fmt.Errorf("source and sink bucketName are required")
	}
	if !validGCSDataPath(spec.GcsDataSource.Path) || !validGCSDataPath(spec.GcsDataSink.Path) {
		return fmt.Errorf("GCS path must be empty or a valid slash-terminated object prefix of at most 1024 UTF-8 bytes")
	}
	return nil
}

func validGCSDataPath(value string) bool {
	if value == "" {
		return true
	}
	if !utf8.ValidString(value) || len(value) > 1024 || !strings.HasSuffix(value, "/") {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}
