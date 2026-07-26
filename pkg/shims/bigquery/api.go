package bigquery

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("bigquery.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

// Dataset mirrors the BigQuery Dataset resource.
type Dataset struct {
	Kind             string            `json:"kind"`
	ID               string            `json:"id"`
	DatasetReference DatasetRef        `json:"datasetReference"`
	Description      string            `json:"description,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Location         string            `json:"location"`
	CreationTime     string            `json:"creationTime"`
	LastModifiedTime string            `json:"lastModifiedTime"`
	Etag             string            `json:"etag"`
	SelfLink         string            `json:"selfLink"`
}

type DatasetRef struct {
	ProjectId string `json:"projectId"`
	DatasetId string `json:"datasetId"`
}

// Table mirrors the BigQuery Table resource.
type Table struct {
	Kind             string            `json:"kind"`
	ID               string            `json:"id"`
	TableReference   TableRef          `json:"tableReference"`
	Schema           *TableSchema      `json:"schema,omitempty"`
	Description      string            `json:"description,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Location         string            `json:"location"`
	CreationTime     string            `json:"creationTime"`
	LastModifiedTime string            `json:"lastModifiedTime"`
	NumRows          string            `json:"numRows"`
	NumBytes         string            `json:"numBytes"`
	Type             string            `json:"type"` // TABLE, VIEW, EXTERNAL
	Etag             string            `json:"etag"`
	SelfLink         string            `json:"selfLink"`
	// In-memory row storage (for insertAll)
	rows []map[string]interface{}
}

type TableRef struct {
	ProjectId string `json:"projectId"`
	DatasetId string `json:"datasetId"`
	TableId   string `json:"tableId"`
}

type TableSchema struct {
	Fields []FieldSchema `json:"fields"`
}

type FieldSchema struct {
	Name        string        `json:"name"`
	Type        string        `json:"type"` // STRING, INTEGER, FLOAT, BOOLEAN, RECORD, TIMESTAMP, DATE, etc.
	Mode        string        `json:"mode"` // NULLABLE, REQUIRED, REPEATED
	Description string        `json:"description,omitempty"`
	Fields      []FieldSchema `json:"fields,omitempty"` // nested RECORD
}

// Job mirrors the BigQuery Job resource.
type Job struct {
	Kind          string        `json:"kind"`
	ID            string        `json:"id"`
	JobReference  JobRef        `json:"jobReference"`
	Status        JobStatus     `json:"status"`
	Statistics    JobStatistics `json:"statistics"`
	Configuration JobConfig     `json:"configuration"`

	// Internal state
	RawRows         []QueryValues `json:"-"`
	Schema          *TableSchema  `json:"-"`
	UploadSessionID string        `json:"-"`
}

type QueryValues []interface{}

type JobRef struct {
	ProjectId string `json:"projectId"`
	JobId     string `json:"jobId"`
	Location  string `json:"location"`
}

type JobStatus struct {
	State       string      `json:"state"` // PENDING, RUNNING, DONE
	ErrorResult *ErrorProto `json:"errorResult,omitempty"`
}

type ErrorProto struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type JobStatistics struct {
	CreationTime        string `json:"creationTime"`
	StartTime           string `json:"startTime,omitempty"`
	EndTime             string `json:"endTime,omitempty"`
	TotalBytesProcessed string `json:"totalBytesProcessed"`
	TotalSlotMs         string `json:"totalSlotMs"`
}

type JobConfig struct {
	JobType string       `json:"jobType"` // QUERY, LOAD, EXTRACT, COPY
	Query   *QueryConfig `json:"query,omitempty"`
	Load    *LoadConfig  `json:"load,omitempty"`
}

type QueryConfig struct {
	Query            string      `json:"query"`
	UseLegacySql     bool        `json:"useLegacySql"`
	DefaultDataset   *DatasetRef `json:"defaultDataset,omitempty"`
	DestinationTable *TableRef   `json:"destinationTable,omitempty"`
}

type LoadConfig struct {
	SourceUris       []string `json:"sourceUris"`
	DestinationTable TableRef `json:"destinationTable"`
	SourceFormat     string   `json:"sourceFormat"` // CSV, JSON, NEWLINE_DELIMITED_JSON, PARQUET
	Autodetect       bool     `json:"autodetect"`
}

// QueryResultRow is a single row in a query response.
type QueryResultRow struct {
	F []QueryResultCell `json:"f"`
}

type QueryResultCell struct {
	V interface{} `json:"v"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API is the high-fidelity BigQuery v2 shim.
// Query execution is stubbed (returns empty results); table/dataset state is fully tracked.
type API struct {
	mu                   sync.RWMutex
	mutationMu           sync.Mutex
	opMgr                *orchestrator.OperationManager
	backend              *DuckDBBackend
	store                metadataStore
	datasets             map[string]*Dataset // key: project:datasetId
	tables               map[string]*Table   // key: project:datasetId:tableId
	jobs                 map[string]*Job     // key: project:jobId
	executeJob           func(JobConfig) ([]map[string]interface{}, error)
	executeJobWithSchema func(JobConfig) ([]QueryValues, *TableSchema, error)
	persistenceErr       error
}

type metadataStore interface {
	Load(string, any) error
	Save(string, any) error
}

func NewAPI(opMgr *orchestrator.OperationManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: BigQuery] state disabled: %v", err)
		return newAPI(opMgr, nil)
	}
	api, err := NewAPIWithStore(opMgr, store)
	if err != nil {
		log.Printf("[Shim: BigQuery] state rehydration failed: %v", err)
		api = newAPI(opMgr, store)
		api.persistenceErr = fmt.Errorf("BigQuery state is unavailable: %w", err)
		return api
	}
	return api
}

// NewAPIWithStore constructs a BigQuery shim backed by the supplied metadata
// store. It returns an error rather than overwriting unreadable persisted state.
func NewAPIWithStore(opMgr *orchestrator.OperationManager, store *state.Store) (*API, error) {
	if store == nil {
		return newAPI(opMgr, nil), nil
	}
	return newAPIWithMetadataStore(opMgr, store)
}

func newAPIWithMetadataStore(opMgr *orchestrator.OperationManager, store metadataStore) (*API, error) {
	api := newAPI(opMgr, store)
	if store == nil {
		return api, nil
	}
	var persisted bigQueryMetadata
	if err := store.Load(bigQueryStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load BigQuery metadata: %w", err)
	}
	if persisted.Datasets != nil {
		api.datasets = persisted.Datasets
	}
	if persisted.Tables != nil {
		api.tables = persisted.Tables
	}
	if persisted.Jobs != nil {
		api.jobs = persisted.Jobs
	}
	for key, uploadID := range persisted.UploadCorrelations {
		if job := api.jobs[key]; job != nil {
			job.UploadSessionID = uploadID
		}
	}
	for key, results := range persisted.JobResults {
		if job := api.jobs[key]; job != nil {
			job.RawRows = restorePersistedResultRows(results.Rows, results.Schema)
			job.Schema = cloneTableSchema(results.Schema)
		}
	}
	if api.reconcileInterruptedJobs() {
		if err := api.persistMetadata(); err != nil {
			return nil, fmt.Errorf("persist interrupted BigQuery jobs: %w", err)
		}
	}
	return api, nil
}

func newAPI(opMgr *orchestrator.OperationManager, store metadataStore) *API {
	return &API{
		opMgr:    opMgr,
		backend:  NewDuckDBBackend(),
		store:    store,
		datasets: make(map[string]*Dataset),
		tables:   make(map[string]*Table),
		jobs:     make(map[string]*Job),
	}
}

const bigQueryStateEntry = "bigquery/metadata"

const (
	terminalJobPersistenceAttempts = 3
	terminalJobPersistenceDelay    = 10 * time.Millisecond
	maxPersistedQueryResultRows    = 1000
	maxPersistedQueryResultBytes   = 1 << 20

	interruptedJobReason       = "stopped"
	queryResultsTooLargeReason = "responseTooLarge"
)

type bigQueryMetadata struct {
	Datasets           map[string]*Dataset            `json:"datasets"`
	Tables             map[string]*Table              `json:"tables"`
	Jobs               map[string]*Job                `json:"jobs"`
	JobResults         map[string]persistedJobResults `json:"jobResults,omitempty"`
	UploadCorrelations map[string]string              `json:"uploadCorrelations,omitempty"`
}

type persistedJobResults struct {
	Rows   []persistedResultRow `json:"rows"`
	Schema *TableSchema         `json:"schema"`
}

type persistedResultRow struct {
	Cells  []persistedResultCell
	Legacy map[string]persistedResultCell
}

func (row persistedResultRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(row.Cells)
}

func (row *persistedResultRow) UnmarshalJSON(payload []byte) error {
	raw := strings.TrimSpace(string(payload))
	if strings.HasPrefix(raw, "{") {
		return json.Unmarshal(payload, &row.Legacy)
	}
	return json.Unmarshal(payload, &row.Cells)
}

type persistedResultCell struct {
	Value    string                         `json:"value,omitempty"`
	Null     bool                           `json:"null,omitempty"`
	Repeated []persistedResultCell          `json:"repeated,omitempty"`
	Record   map[string]persistedResultCell `json:"record,omitempty"`
}

func (cell *persistedResultCell) UnmarshalJSON(payload []byte) error {
	raw := strings.TrimSpace(string(payload))
	switch {
	case raw == "null":
		*cell = persistedResultCell{Null: true}
		return nil
	case strings.HasPrefix(raw, "{"):
		type cellAlias persistedResultCell
		var decoded cellAlias
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return err
		}
		*cell = persistedResultCell(decoded)
		return nil
	case strings.HasPrefix(raw, `"`):
		var value string
		if err := json.Unmarshal(payload, &value); err != nil {
			return err
		}
		*cell = persistedResultCell{Value: value}
		return nil
	case raw == "true" || raw == "false" || len(raw) > 0 && raw[0] != '[':
		*cell = persistedResultCell{Value: raw}
		return nil
	default:
		return fmt.Errorf("unsupported persisted BigQuery result cell: %s", raw)
	}
}

func (api *API) persistMetadata() error {
	if api.store == nil {
		return nil
	}
	api.mu.RLock()
	metadata := snapshotBigQueryMetadata(api.datasets, api.tables, api.jobs)
	api.mu.RUnlock()
	return api.store.Save(bigQueryStateEntry, metadata)
}

func (api *API) beginMutation() bigQueryMetadata {
	api.mutationMu.Lock()
	api.mu.RLock()
	before := snapshotBigQueryMetadata(api.datasets, api.tables, api.jobs)
	api.mu.RUnlock()
	return before
}

func (api *API) abortMutation() {
	api.mutationMu.Unlock()
}

func (api *API) persistOrRollback(before bigQueryMetadata) error {
	if err := api.persistMetadata(); err != nil {
		api.mu.Lock()
		api.restoreMetadataLocked(before)
		api.mu.Unlock()
		api.mutationMu.Unlock()
		return err
	}
	api.mutationMu.Unlock()
	return nil
}

func (api *API) restoreMetadataLocked(metadata bigQueryMetadata) {
	api.datasets = metadata.Datasets
	api.tables = metadata.Tables
	api.jobs = metadata.Jobs
}

func (api *API) reconcileInterruptedJobs() bool {
	changed := false
	nowMs := fmt.Sprintf("%d", time.Now().UnixMilli())
	for _, job := range api.jobs {
		if job == nil || job.Status.State != "RUNNING" && job.Status.State != "PENDING" {
			continue
		}
		job.Status.State = "DONE"
		job.Status.ErrorResult = &ErrorProto{
			Reason:  interruptedJobReason,
			Message: "Job interrupted by MiniSky restart; execution was not resumed",
		}
		job.Statistics.EndTime = nowMs
		job.RawRows = nil
		job.Schema = nil
		changed = true
	}
	return changed
}

func (api *API) persistenceFailure() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.persistenceErr
}

// GetBackend exposes the backend for dynamic dashboard configuration.
func (api *API) GetBackend() *DuckDBBackend {
	return api.backend
}

// ServeHTTP dispatches BigQuery v2 paths.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: BigQuery] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if err := api.persistenceFailure(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", err.Error())
		return
	}

	path := r.URL.Path

	switch {
	case strings.Contains(path, "/insertAll"):
		api.insertAll(w, r, path)
	case strings.Contains(path, "/upload"):
		api.handleUpload(w, r)
	case strings.Contains(path, "/tables") && strings.Contains(path, "/datasets"):
		api.routeTables(w, r, path)
	case strings.Contains(path, "/datasets"):
		api.routeDatasets(w, r, path)
	case strings.Contains(path, "/jobs") && strings.Contains(path, "/results"):
		api.getQueryResults(w, r, path)
	case strings.Contains(path, "/queries/"):
		api.getQueryResults(w, r, path)
	case strings.Contains(path, "/jobs"):
		api.routeJobs(w, r, path)
	default:
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "BigQuery resource not found: "+path)
	}
}

func (api *API) handleUpload(w http.ResponseWriter, r *http.Request) {
	api.handleBoundedUpload(w, r)
}

// ─────────────────────────────────────────────────────────────────────────────
// Datasets
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeDatasets(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	datasetId := extractSegmentAfter(path, "datasets")

	switch r.Method {
	case http.MethodPost:
		var body struct {
			DatasetReference DatasetRef        `json:"datasetReference"`
			Description      string            `json:"description"`
			Labels           map[string]string `json:"labels"`
			Location         string            `json:"location"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.DatasetReference.DatasetId == "" {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, 400, "INVALID_ARGUMENT", "datasetReference.datasetId is required")
			return
		}
		dsID := body.DatasetReference.DatasetId
		location := body.Location
		if location == "" {
			location = "US"
		}
		nowMs := fmt.Sprintf("%d", time.Now().UnixMilli())
		ds := &Dataset{
			Kind:             "bigquery#dataset",
			ID:               fmt.Sprintf("%s:%s", project, dsID),
			DatasetReference: DatasetRef{ProjectId: project, DatasetId: dsID},
			Description:      body.Description,
			Labels:           body.Labels,
			Location:         location,
			CreationTime:     nowMs,
			LastModifiedTime: nowMs,
			Etag:             newEtag(),
			SelfLink:         fmt.Sprintf("https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s", project, dsID),
		}
		key := project + ":" + dsID
		before := api.beginMutation()
		api.mu.Lock()
		if _, exists := api.datasets[key]; exists {
			api.mu.Unlock()
			api.abortMutation()
			w.WriteHeader(http.StatusConflict)
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Dataset "+dsID+" already exists")
			return
		}
		api.datasets[key] = ds
		response := cloneDataset(ds)
		api.mu.Unlock()
		if err := api.persistOrRollback(before); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist dataset metadata")
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)

	case http.MethodGet:
		if datasetId != "" {
			key := project + ":" + datasetId
			api.mu.RLock()
			ds, ok := api.datasets[key]
			response := cloneDataset(ds)
			api.mu.RUnlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				writeError(w, 404, "NOT_FOUND", "Dataset "+datasetId+" not found")
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		} else {
			prefix := project + ":"
			api.mu.RLock()
			items := []*Dataset{}
			for k, v := range api.datasets {
				if strings.HasPrefix(k, prefix) {
					items = append(items, cloneDataset(v))
				}
			}
			api.mu.RUnlock()
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"kind":     "bigquery#datasetList",
				"datasets": items,
			})
		}

	case http.MethodPatch, http.MethodPut:
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
			return
		}
		key := project + ":" + datasetId
		before := api.beginMutation()
		api.mu.Lock()
		ds, ok := api.datasets[key]
		if !ok {
			api.mu.Unlock()
			api.abortMutation()
			w.WriteHeader(http.StatusNotFound)
			writeError(w, 404, "NOT_FOUND", "Dataset "+datasetId+" not found")
			return
		}
		updated, code, status, message := applyDatasetUpdate(ds, body, r.Method == http.MethodPatch)
		if code != 0 {
			api.mu.Unlock()
			api.abortMutation()
			w.WriteHeader(code)
			writeError(w, code, status, message)
			return
		}
		api.datasets[key] = updated
		response := cloneDataset(updated)
		api.mu.Unlock()
		if err := api.persistOrRollback(before); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist dataset metadata")
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)

	case http.MethodDelete:
		key := project + ":" + datasetId
		before := api.beginMutation()
		api.mu.Lock()
		_, ok := api.datasets[key]
		if ok {
			delete(api.datasets, key)
		}
		api.mu.Unlock()
		if !ok {
			api.abortMutation()
			w.WriteHeader(http.StatusNotFound)
			writeError(w, 404, "NOT_FOUND", "Dataset "+datasetId+" not found")
			return
		}
		if err := api.persistOrRollback(before); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist dataset metadata")
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Tables
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeTables(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	datasetId := extractSegmentAfter(path, "datasets")
	tableId := extractSegmentAfter(path, "tables")

	switch r.Method {
	case http.MethodPost:
		var body struct {
			TableReference TableRef          `json:"tableReference"`
			Schema         *TableSchema      `json:"schema"`
			Description    string            `json:"description"`
			Labels         map[string]string `json:"labels"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.TableReference.TableId == "" {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, 400, "INVALID_ARGUMENT", "tableReference.tableId is required")
			return
		}
		tID := body.TableReference.TableId
		nowMs := fmt.Sprintf("%d", time.Now().UnixMilli())
		t := &Table{
			Kind:             "bigquery#table",
			ID:               fmt.Sprintf("%s:%s.%s", project, datasetId, tID),
			TableReference:   TableRef{ProjectId: project, DatasetId: datasetId, TableId: tID},
			Schema:           body.Schema,
			Description:      body.Description,
			Labels:           body.Labels,
			Location:         "US",
			CreationTime:     nowMs,
			LastModifiedTime: nowMs,
			NumRows:          "0",
			NumBytes:         "0",
			Type:             "TABLE",
			Etag:             newEtag(),
			SelfLink: fmt.Sprintf("https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s",
				project, datasetId, tID),
		}
		key := tableKey(project, datasetId, tID)
		before := api.beginMutation()
		api.mu.RLock()
		_, exists := api.tables[key]
		api.mu.RUnlock()
		if exists {
			api.abortMutation()
			w.WriteHeader(http.StatusConflict)
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Table "+tID+" already exists")
			return
		}

		// Wire to DuckDB backend if enabled
		if api.backend.Enabled() && t.Schema != nil {
			if err := api.backend.CreateTable(project, datasetId, tID, t.Schema); err != nil {
				api.abortMutation()
				log.Printf("[Shim: BigQuery] CreateTable failed for %s.%s: %v", datasetId, tID, err)
				w.WriteHeader(http.StatusInternalServerError)
				writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to create DuckDB table")
				return
			}
		}

		api.mu.Lock()
		api.tables[key] = t
		response := cloneTable(t)
		api.mu.Unlock()
		if err := api.persistOrRollback(before); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist table metadata")
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)

	case http.MethodGet:
		if tableId != "" {
			key := tableKey(project, datasetId, tableId)
			api.mu.RLock()
			t, ok := api.tables[key]
			response := cloneTable(t)
			api.mu.RUnlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				writeError(w, 404, "NOT_FOUND", "Table "+tableId+" not found")
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
		} else {
			prefix := tableKey(project, datasetId, "")
			api.mu.RLock()
			items := []*Table{}
			for k, v := range api.tables {
				if strings.HasPrefix(k, prefix) {
					items = append(items, cloneTable(v))
				}
			}
			api.mu.RUnlock()
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"kind":       "bigquery#tableList",
				"totalItems": len(items),
				"tables":     items,
			})
		}

	case http.MethodDelete:
		key := tableKey(project, datasetId, tableId)
		before := api.beginMutation()
		api.mu.Lock()
		_, ok := api.tables[key]
		if ok {
			delete(api.tables, key)
		}
		api.mu.Unlock()
		if !ok {
			api.abortMutation()
			w.WriteHeader(http.StatusNotFound)
			writeError(w, 404, "NOT_FOUND", "Table "+tableId+" not found")
			return
		}
		if err := api.persistOrRollback(before); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist table metadata")
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// insertAll handles tabledata.insertAll (streaming inserts).
func (api *API) insertAll(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	datasetId := extractSegmentAfter(path, "datasets")
	tableId := extractSegmentAfter(path, "tables")

	var body struct {
		Rows []struct {
			InsertId string                 `json:"insertId"`
			Json     map[string]interface{} `json:"json"`
		} `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}

	key := tableKey(project, datasetId, tableId)
	before := api.beginMutation()
	api.mu.RLock()
	table, ok := api.tables[key]
	tableSnapshot := cloneTable(table)
	api.mu.RUnlock()
	if !ok {
		api.abortMutation()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Table "+tableId+" not found")
		return
	}

	rows := make([]map[string]interface{}, len(body.Rows))
	for i, row := range body.Rows {
		rows[i] = row.Json
	}
	if api.backend.Enabled() {
		if err := api.backend.InsertRows(datasetId, tableId, tableSnapshot.Schema, rows); err != nil {
			api.abortMutation()
			log.Printf("[Shim: BigQuery] insertAll DuckDB write failed for %s.%s: %v", datasetId, tableId, err)
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist streaming rows")
			return
		}
	}

	api.mu.Lock()
	table = api.tables[key]
	table.rows = append(table.rows, rows...)
	table.NumRows = fmt.Sprintf("%d", len(table.rows))
	api.mu.Unlock()
	if err := api.persistOrRollback(before); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist table metadata")
		return
	}

	// GCP returns 200 with empty insertErrors on success
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":         "bigquery#tableDataInsertAllResponse",
		"insertErrors": []interface{}{},
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Jobs
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeJobs(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	jobId := extractSegmentAfter(path, "jobs")

	switch r.Method {
	case http.MethodPost:
		api.insertJob(w, r, project)
	case http.MethodGet:
		if jobId != "" {
			api.getJob(w, project, jobId)
		} else {
			api.listJobs(w, project)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) insertJob(w http.ResponseWriter, r *http.Request, project string) {
	var body struct {
		JobReference  JobRef    `json:"jobReference"`
		Configuration JobConfig `json:"configuration"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}

	jobId := body.JobReference.JobId
	if jobId == "" {
		jobId = fmt.Sprintf("job_minisky_%x", time.Now().UnixNano())
	}
	location := body.JobReference.Location
	if location == "" {
		location = "US"
	}
	nowMs := fmt.Sprintf("%d", time.Now().UnixMilli())

	job := &Job{
		Kind: "bigquery#job",
		ID:   fmt.Sprintf("%s:%s", project, jobId),
		JobReference: JobRef{
			ProjectId: project,
			JobId:     jobId,
			Location:  location,
		},
		Configuration: body.Configuration,
		Status:        JobStatus{State: "RUNNING"},
		Statistics: JobStatistics{
			CreationTime:        nowMs,
			StartTime:           nowMs,
			TotalBytesProcessed: "0",
			TotalSlotMs:         "0",
		},
	}

	key := project + ":" + jobId
	before := api.beginMutation()
	api.mu.Lock()
	if _, exists := api.jobs[key]; exists {
		api.mu.Unlock()
		api.abortMutation()
		w.WriteHeader(http.StatusConflict)
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Job "+jobId+" already exists")
		return
	}
	api.jobs[key] = job
	response := cloneJob(job)
	api.mu.Unlock()
	if err := api.persistOrRollback(before); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist job metadata")
		return
	}

	// Finish job asynchronously
	go func() {
		rows, schema, execErr := api.runJob(body.Configuration)
		api.completeJob(key, rows, schema, execErr)
	}()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (api *API) runJob(configuration JobConfig) ([]QueryValues, *TableSchema, error) {
	if api.executeJobWithSchema != nil {
		return api.executeJobWithSchema(configuration)
	}
	if api.executeJob != nil {
		rows, err := api.executeJob(configuration)
		if err != nil {
			return nil, nil, err
		}
		schema, err := inferQuerySchema(rows)
		if err != nil {
			return nil, nil, err
		}
		return mapRowsToValues(rows, schema), schema, nil
	}
	if api.backend.Enabled() {
		if configuration.Query != nil && configuration.Query.Query != "" {
			return api.backend.ExecuteQueryWithSchema(configuration.Query.Query)
		}
		if configuration.Load != nil && len(configuration.Load.SourceUris) > 0 {
			load := configuration.Load
			return nil, nil, api.backend.LoadData(
				load.DestinationTable.ProjectId,
				load.DestinationTable.DatasetId,
				load.DestinationTable.TableId,
				load.SourceUris[0],
				load.SourceFormat,
			)
		}
		return nil, nil, nil
	}
	time.Sleep(500 * time.Millisecond) // Simulate mock execution
	return nil, nil, nil
}

func (api *API) completeJob(
	key string,
	rows []QueryValues,
	schema *TableSchema,
	execErr error,
) {
	api.mutationMu.Lock()
	api.mu.RLock()
	metadata := snapshotBigQueryMetadata(api.datasets, api.tables, api.jobs)
	job, ok := metadata.Jobs[key]
	api.mu.RUnlock()
	if !ok {
		api.mutationMu.Unlock()
		return
	}
	job.Status.State = "DONE"
	job.Statistics.EndTime = fmt.Sprintf("%d", time.Now().UnixMilli())
	if execErr != nil {
		job.Status.ErrorResult = &ErrorProto{Reason: "backendError", Message: execErr.Error()}
		job.RawRows = nil
		job.Schema = nil
	} else {
		job.RawRows, job.Schema, job.Status.ErrorResult = boundedQueryResults(rows, schema)
	}
	metadata.Jobs[key] = job
	updatePersistedJobResults(metadata.JobResults, key, job)

	var persistErr error
	for attempt := 1; attempt <= terminalJobPersistenceAttempts; attempt++ {
		persistErr = api.persistSnapshot(metadata)
		if persistErr == nil {
			api.mu.Lock()
			api.jobs[key] = cloneJob(job)
			api.mu.Unlock()
			api.mutationMu.Unlock()
			return
		}
		if attempt < terminalJobPersistenceAttempts {
			time.Sleep(terminalJobPersistenceDelay)
		}
	}
	degradedErr := fmt.Errorf(
		"persist terminal job %s after %d attempts: %w",
		key,
		terminalJobPersistenceAttempts,
		persistErr,
	)
	api.mu.Lock()
	api.jobs[key] = cloneJob(job)
	if api.persistenceErr == nil {
		api.persistenceErr = fmt.Errorf("BigQuery persistence is degraded: %w", degradedErr)
	}
	api.mu.Unlock()
	api.mutationMu.Unlock()
	log.Printf("[Shim: BigQuery] terminal job persistence degraded for %s: %v", key, persistErr)
}

func (api *API) persistSnapshot(metadata bigQueryMetadata) error {
	if api.store == nil {
		return nil
	}
	return api.store.Save(bigQueryStateEntry, metadata)
}

func boundedQueryResults(
	rows []QueryValues,
	schema *TableSchema,
) ([]QueryValues, *TableSchema, *ErrorProto) {
	if len(rows) > maxPersistedQueryResultRows {
		return nil, nil, queryResultsTooLargeError(len(rows), 0)
	}
	clonedRows := cloneQueryValuesRows(rows)
	if schema == nil {
		return nil, nil, &ErrorProto{Reason: "invalidQuery", Message: "query schema is required for positional rows"}
	} else {
		schema = cloneTableSchema(schema)
	}
	if err := validateQueryRows(clonedRows, schema); err != nil {
		return nil, nil, &ErrorProto{Reason: "invalidQuery", Message: err.Error()}
	}
	persistedRows, err := persistResultRows(clonedRows, schema)
	if err != nil {
		return nil, nil, &ErrorProto{Reason: "backendError", Message: "Query results are not JSON serializable: " + err.Error()}
	}
	persistedPayload, err := json.Marshal(persistedJobResults{Rows: persistedRows, Schema: schema})
	if err != nil {
		return nil, nil, &ErrorProto{Reason: "backendError", Message: "Query results are not JSON serializable: " + err.Error()}
	}
	structuredRows, err := queryResultRowsWire(clonedRows, schema)
	if err != nil {
		return nil, nil, &ErrorProto{Reason: "invalidQuery", Message: err.Error()}
	}
	wirePayload, err := json.Marshal(map[string]interface{}{
		"kind":                "bigquery#getQueryResultsResponse",
		"jobComplete":         true,
		"totalRows":           strconv.Itoa(len(structuredRows)),
		"schema":              schema,
		"rows":                structuredRows,
		"totalBytesProcessed": "0",
	})
	if err != nil {
		return nil, nil, &ErrorProto{Reason: "backendError", Message: "Query results are not JSON serializable: " + err.Error()}
	}
	resultBytes := max(len(persistedPayload), len(wirePayload))
	if resultBytes > maxPersistedQueryResultBytes {
		return nil, nil, queryResultsTooLargeError(len(rows), resultBytes)
	}
	return clonedRows, schema, nil
}

func queryResultsTooLargeError(rows, bytes int) *ErrorProto {
	return &ErrorProto{
		Reason: queryResultsTooLargeReason,
		Message: fmt.Sprintf(
			"Query results exceed MiniSky persistence limits (%d rows or %d bytes); got %d rows and %d bytes",
			maxPersistedQueryResultRows,
			maxPersistedQueryResultBytes,
			rows,
			bytes,
		),
	}
}

func (api *API) getJob(w http.ResponseWriter, project, jobId string) {
	key := project + ":" + jobId
	api.mu.RLock()
	job, ok := api.jobs[key]
	response := cloneJob(job)
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Job "+jobId+" not found")
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (api *API) listJobs(w http.ResponseWriter, project string) {
	prefix := project + ":"
	api.mu.RLock()
	items := []*Job{}
	for k, v := range api.jobs {
		if strings.HasPrefix(k, prefix) {
			items = append(items, cloneJob(v))
		}
	}
	api.mu.RUnlock()
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind": "bigquery#jobList",
		"jobs": items,
	})
}

// getQueryResults returns rows stored in the destination table (if available).
func (api *API) getQueryResults(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	jobId := extractSegmentAfter(path, "jobs")
	if jobId == "" {
		jobId = extractSegmentAfter(path, "queries")
	}
	key := project + ":" + jobId

	api.mu.RLock()
	job, ok := api.jobs[key]
	job = cloneJob(job)
	api.mu.RUnlock()

	done := false
	if ok && job.Status.State == "DONE" {
		done = true
	}

	schema := &TableSchema{Fields: []FieldSchema{}}
	outRows := []map[string]interface{}{}
	if ok && job.Schema != nil {
		schema = job.Schema
	}
	if ok && job.RawRows != nil && job.Schema != nil {
		var err error
		outRows, err = queryResultRowsWire(job.RawRows, job.Schema)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Stored query results are invalid: "+err.Error())
			return
		}
	}
	numRows := len(outRows)

	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"kind":                "bigquery#getQueryResultsResponse",
		"jobComplete":         done,
		"totalRows":           fmt.Sprintf("%d", numRows),
		"schema":              schema,
		"rows":                outRows,
		"totalBytesProcessed": "0",
	}

	if ok && job.Status.ErrorResult != nil {
		response["errors"] = []interface{}{job.Status.ErrorResult}
	}

	json.NewEncoder(w).Encode(response)
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func tableKey(project, dataset, table string) string { return project + ":" + dataset + ":" + table }

func snapshotBigQueryMetadata(
	datasets map[string]*Dataset,
	tables map[string]*Table,
	jobs map[string]*Job,
) bigQueryMetadata {
	snapshot := bigQueryMetadata{
		Datasets:           make(map[string]*Dataset, len(datasets)),
		Tables:             make(map[string]*Table, len(tables)),
		Jobs:               make(map[string]*Job, len(jobs)),
		JobResults:         make(map[string]persistedJobResults),
		UploadCorrelations: make(map[string]string),
	}
	for key, dataset := range datasets {
		snapshot.Datasets[key] = cloneDataset(dataset)
	}
	for key, table := range tables {
		snapshot.Tables[key] = cloneTable(table)
	}
	for key, job := range jobs {
		jobClone := cloneJob(job)
		snapshot.Jobs[key] = jobClone
		updatePersistedJobResults(snapshot.JobResults, key, jobClone)
		if jobClone != nil && jobClone.UploadSessionID != "" {
			snapshot.UploadCorrelations[key] = jobClone.UploadSessionID
		}
	}
	return snapshot
}

func updatePersistedJobResults(results map[string]persistedJobResults, key string, job *Job) {
	delete(results, key)
	if job == nil || job.Status.State != "DONE" || job.Status.ErrorResult != nil ||
		job.RawRows == nil && job.Schema == nil {
		return
	}
	rows, err := persistResultRows(job.RawRows, job.Schema)
	if err != nil {
		return
	}
	results[key] = persistedJobResults{
		Rows:   rows,
		Schema: cloneTableSchema(job.Schema),
	}
}

func persistResultRows(
	rows []QueryValues,
	schema *TableSchema,
) ([]persistedResultRow, error) {
	if rows == nil {
		return nil, nil
	}
	persisted := make([]persistedResultRow, len(rows))
	for rowIndex, row := range rows {
		if len(row) != len(schema.Fields) {
			return nil, fmt.Errorf("row %d has %d values for %d schema fields", rowIndex, len(row), len(schema.Fields))
		}
		cells := make([]persistedResultCell, len(schema.Fields))
		for fieldIndex, field := range schema.Fields {
			cell, err := persistResultCell(row[fieldIndex], field)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", field.Name, err)
			}
			cells[fieldIndex] = cell
		}
		persisted[rowIndex] = persistedResultRow{Cells: cells}
	}
	return persisted, nil
}

func persistResultCell(value interface{}, field FieldSchema) (persistedResultCell, error) {
	if value == nil {
		return persistedResultCell{Null: true}, nil
	}
	if persisted, ok := value.(persistedResultCell); ok {
		return persisted, nil
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		items, ok := resultSlice(value)
		if !ok {
			return persistedResultCell{}, fmt.Errorf("REPEATED value must be an array")
		}
		elementField := field
		elementField.Mode = "NULLABLE"
		cells := make([]persistedResultCell, len(items))
		for index, item := range items {
			cell, err := persistResultCell(item, elementField)
			if err != nil {
				return persistedResultCell{}, err
			}
			cells[index] = cell
		}
		return persistedResultCell{Repeated: cells}, nil
	}
	if isRecordType(field.Type) {
		record, ok := value.(map[string]interface{})
		if !ok {
			return persistedResultCell{}, fmt.Errorf("RECORD value must be an object")
		}
		fields := make(map[string]persistedResultCell, len(field.Fields))
		for _, child := range field.Fields {
			cell, err := persistResultCell(record[child.Name], child)
			if err != nil {
				return persistedResultCell{}, fmt.Errorf("nested field %q: %w", child.Name, err)
			}
			fields[child.Name] = cell
		}
		return persistedResultCell{Record: fields}, nil
	}
	valueText, err := formatQueryScalar(value, field.Type)
	if err != nil {
		return persistedResultCell{}, err
	}
	return persistedResultCell{Value: valueText}, nil
}

func restorePersistedResultRows(rows []persistedResultRow, schema *TableSchema) []QueryValues {
	if rows == nil {
		return nil
	}
	restored := make([]QueryValues, len(rows))
	for rowIndex, row := range rows {
		if row.Cells != nil {
			restored[rowIndex] = clonePersistedCellsAsValues(row.Cells)
			continue
		}
		restored[rowIndex] = make(QueryValues, len(schema.Fields))
		for fieldIndex, field := range schema.Fields {
			restored[rowIndex][fieldIndex] = row.Legacy[field.Name]
		}
	}
	return restored
}

func inferQuerySchema(rows []map[string]interface{}) (*TableSchema, error) {
	schema := &TableSchema{Fields: []FieldSchema{}}
	if len(rows) == 0 {
		return schema, nil
	}
	names := make([]string, 0, len(rows[0]))
	for name := range rows[0] {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field, err := inferQueryField(name, rows[0][name])
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		schema.Fields = append(schema.Fields, field)
	}
	return schema, nil
}

func mapRowsToValues(rows []map[string]interface{}, schema *TableSchema) []QueryValues {
	values := make([]QueryValues, len(rows))
	for rowIndex, row := range rows {
		values[rowIndex] = make(QueryValues, len(schema.Fields))
		for fieldIndex, field := range schema.Fields {
			values[rowIndex][fieldIndex] = row[field.Name]
		}
	}
	return values
}

func inferQueryField(name string, value interface{}) (FieldSchema, error) {
	field := FieldSchema{Name: name, Mode: "NULLABLE"}
	switch typed := value.(type) {
	case nil:
		field.Type = "STRING"
	case bool:
		field.Type = "BOOLEAN"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		field.Type = "INTEGER"
	case float32, float64:
		field.Type = "FLOAT"
	case json.Number:
		if strings.ContainsAny(typed.String(), ".eE") {
			field.Type = "NUMERIC"
		} else {
			field.Type = "INTEGER"
		}
	case []byte:
		field.Type = "BYTES"
	case time.Time:
		field.Type = "TIMESTAMP"
	case map[string]interface{}:
		field.Type = "RECORD"
		nested, err := inferQuerySchema([]map[string]interface{}{typed})
		if err != nil {
			return FieldSchema{}, err
		}
		field.Fields = nested.Fields
	default:
		if items, ok := resultSlice(value); ok {
			field.Mode = "REPEATED"
			if len(items) == 0 {
				field.Type = "STRING"
				return field, nil
			}
			element, err := inferQueryField(name, items[0])
			if err != nil {
				return FieldSchema{}, err
			}
			if strings.EqualFold(element.Mode, "REPEATED") {
				return FieldSchema{}, fmt.Errorf("nested arrays are not supported")
			}
			field.Type = element.Type
			field.Fields = element.Fields
		} else {
			field.Type = "STRING"
		}
	}
	return field, nil
}

func validateQueryRows(rows []QueryValues, schema *TableSchema) error {
	if schema == nil {
		return errors.New("query schema is required")
	}
	for rowIndex, row := range rows {
		if len(row) != len(schema.Fields) {
			return fmt.Errorf("row %d has %d values for %d schema fields", rowIndex, len(row), len(schema.Fields))
		}
		for fieldIndex, field := range schema.Fields {
			if _, err := persistResultCell(row[fieldIndex], field); err != nil {
				return fmt.Errorf("row %d field %q: %w", rowIndex, field.Name, err)
			}
		}
	}
	return nil
}

func queryResultRowsWire(
	rows []QueryValues,
	schema *TableSchema,
) ([]map[string]interface{}, error) {
	out := make([]map[string]interface{}, len(rows))
	for rowIndex, row := range rows {
		if len(row) != len(schema.Fields) {
			return nil, fmt.Errorf("row %d has %d values for %d schema fields", rowIndex, len(row), len(schema.Fields))
		}
		cells := make([]map[string]interface{}, len(schema.Fields))
		for fieldIndex, field := range schema.Fields {
			value, err := queryResultCellWire(row[fieldIndex], field)
			if err != nil {
				return nil, fmt.Errorf("row %d field %q: %w", rowIndex, field.Name, err)
			}
			cells[fieldIndex] = map[string]interface{}{"v": value}
		}
		out[rowIndex] = map[string]interface{}{"f": cells}
	}
	return out, nil
}

func queryResultCellWire(value interface{}, field FieldSchema) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if persisted, ok := value.(persistedResultCell); ok {
		if persisted.Null {
			return nil, nil
		}
		if strings.EqualFold(field.Mode, "REPEATED") {
			elementField := field
			elementField.Mode = "NULLABLE"
			items := make([]map[string]interface{}, len(persisted.Repeated))
			for index, item := range persisted.Repeated {
				wire, err := queryResultCellWire(item, elementField)
				if err != nil {
					return nil, err
				}
				items[index] = map[string]interface{}{"v": wire}
			}
			return items, nil
		}
		if isRecordType(field.Type) {
			return recordResultWire(persisted.Record, field.Fields)
		}
		return queryScalarWire(persisted.Value, field.Type)
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		items, ok := resultSlice(value)
		if !ok {
			return nil, errors.New("REPEATED value must be an array")
		}
		elementField := field
		elementField.Mode = "NULLABLE"
		wireItems := make([]map[string]interface{}, len(items))
		for index, item := range items {
			wire, err := queryResultCellWire(item, elementField)
			if err != nil {
				return nil, err
			}
			wireItems[index] = map[string]interface{}{"v": wire}
		}
		return wireItems, nil
	}
	if isRecordType(field.Type) {
		record, ok := value.(map[string]interface{})
		if !ok {
			return nil, errors.New("RECORD value must be an object")
		}
		return recordResultWire(record, field.Fields)
	}
	text, err := formatQueryScalar(value, field.Type)
	if err != nil {
		return nil, err
	}
	return queryScalarWire(text, field.Type)
}

func recordResultWire(record interface{}, fields []FieldSchema) (map[string]interface{}, error) {
	cells := make([]map[string]interface{}, len(fields))
	for index, field := range fields {
		var value interface{}
		switch typed := record.(type) {
		case map[string]interface{}:
			value = typed[field.Name]
		case map[string]persistedResultCell:
			value = typed[field.Name]
		default:
			return nil, errors.New("RECORD value must be an object")
		}
		wire, err := queryResultCellWire(value, field)
		if err != nil {
			return nil, fmt.Errorf("nested field %q: %w", field.Name, err)
		}
		cells[index] = map[string]interface{}{"v": wire}
	}
	return map[string]interface{}{"f": cells}, nil
}

func formatQueryScalar(value interface{}, fieldType string) (string, error) {
	switch strings.ToUpper(fieldType) {
	case "BYTES":
		switch typed := value.(type) {
		case []byte:
			return base64.StdEncoding.EncodeToString(typed), nil
		case string:
			return base64.StdEncoding.EncodeToString([]byte(typed)), nil
		default:
			return "", fmt.Errorf("BYTES value must be bytes")
		}
	case "BOOLEAN", "BOOL":
		switch typed := value.(type) {
		case bool:
			return strconv.FormatBool(typed), nil
		case string:
			if _, err := strconv.ParseBool(typed); err != nil {
				return "", fmt.Errorf("invalid BOOLEAN value %q", typed)
			}
			return strings.ToLower(typed), nil
		default:
			return "", fmt.Errorf("BOOLEAN value must be bool")
		}
	case "DATE", "TIME", "DATETIME", "TIMESTAMP":
		if typed, ok := value.(time.Time); ok {
			return formatQueryTime(typed, fieldType), nil
		}
		return formatQueryResultValue(value), nil
	default:
		return formatQueryResultValue(value), nil
	}
}

func queryScalarWire(value, fieldType string) (interface{}, error) {
	switch strings.ToUpper(fieldType) {
	case "BOOLEAN", "BOOL":
		if _, err := strconv.ParseBool(value); err != nil {
			return nil, err
		}
	case "FLOAT", "FLOAT64":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return nil, err
		}
	}
	return value, nil
}

func formatQueryTime(value time.Time, fieldType string) string {
	switch strings.ToUpper(fieldType) {
	case "DATE":
		return value.Format("2006-01-02")
	case "TIME":
		return trimFractionalTime(value.Format("15:04:05.000000"))
	case "DATETIME":
		return trimFractionalTime(value.Format("2006-01-02T15:04:05.000000"))
	case "TIMESTAMP":
		totalMicros := value.UnixMicro()
		if totalMicros%1_000_000 == 0 {
			return strconv.FormatInt(totalMicros/1_000_000, 10)
		}
		sign := ""
		if totalMicros < 0 {
			sign = "-"
			totalMicros = -totalMicros
		}
		return fmt.Sprintf("%s%d.%06d", sign, totalMicros/1_000_000, totalMicros%1_000_000)
	default:
		return value.Format(time.RFC3339Nano)
	}
}

func trimFractionalTime(value string) string {
	value = strings.TrimRight(value, "0")
	return strings.TrimSuffix(value, ".")
}

func resultSlice(value interface{}) ([]interface{}, bool) {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, false
	}
	if _, bytes := value.([]byte); bytes {
		return nil, false
	}
	items := make([]interface{}, reflected.Len())
	for index := range items {
		items[index] = reflected.Index(index).Interface()
	}
	return items, true
}

func isRecordType(fieldType string) bool {
	return strings.EqualFold(fieldType, "RECORD") || strings.EqualFold(fieldType, "STRUCT")
}

func formatQueryResultValue(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case json.Number:
		return typed.String()
	case int:
		return strconv.FormatInt(int64(typed), 10)
	case int8:
		return strconv.FormatInt(int64(typed), 10)
	case int16:
		return strconv.FormatInt(int64(typed), 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint8:
		return strconv.FormatUint(uint64(typed), 10)
	case uint16:
		return strconv.FormatUint(uint64(typed), 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}

func cloneDataset(dataset *Dataset) *Dataset {
	if dataset == nil {
		return nil
	}
	clone := *dataset
	clone.Labels = cloneStringMap(dataset.Labels)
	return &clone
}

func cloneTable(table *Table) *Table {
	if table == nil {
		return nil
	}
	clone := *table
	clone.Labels = cloneStringMap(table.Labels)
	clone.Schema = cloneTableSchema(table.Schema)
	clone.rows = cloneRows(table.rows)
	return &clone
}

func cloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	clone := *job
	if job.Status.ErrorResult != nil {
		errorResult := *job.Status.ErrorResult
		clone.Status.ErrorResult = &errorResult
	}
	clone.Configuration.Query = cloneQueryConfig(job.Configuration.Query)
	if job.Configuration.Load != nil {
		load := *job.Configuration.Load
		load.SourceUris = append([]string(nil), job.Configuration.Load.SourceUris...)
		clone.Configuration.Load = &load
	}
	clone.RawRows = cloneQueryValuesRows(job.RawRows)
	clone.Schema = cloneTableSchema(job.Schema)
	return &clone
}

func cloneQueryConfig(query *QueryConfig) *QueryConfig {
	if query == nil {
		return nil
	}
	clone := *query
	if query.DefaultDataset != nil {
		defaultDataset := *query.DefaultDataset
		clone.DefaultDataset = &defaultDataset
	}
	if query.DestinationTable != nil {
		destinationTable := *query.DestinationTable
		clone.DestinationTable = &destinationTable
	}
	return &clone
}

func cloneTableSchema(schema *TableSchema) *TableSchema {
	if schema == nil {
		return nil
	}
	return &TableSchema{Fields: cloneFieldSchemas(schema.Fields)}
}

func cloneFieldSchemas(fields []FieldSchema) []FieldSchema {
	if fields == nil {
		return nil
	}
	clone := make([]FieldSchema, len(fields))
	for index, field := range fields {
		clone[index] = field
		clone[index].Fields = cloneFieldSchemas(field.Fields)
	}
	return clone
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func cloneRows(rows []map[string]interface{}) []map[string]interface{} {
	if rows == nil {
		return nil
	}
	clone := make([]map[string]interface{}, len(rows))
	for index, row := range rows {
		clone[index] = cloneJSONMap(row)
	}
	return clone
}

func cloneQueryValuesRows(rows []QueryValues) []QueryValues {
	if rows == nil {
		return nil
	}
	clone := make([]QueryValues, len(rows))
	for rowIndex, row := range rows {
		clone[rowIndex] = make(QueryValues, len(row))
		for valueIndex, value := range row {
			clone[rowIndex][valueIndex] = cloneJSONValue(value)
		}
	}
	return clone
}

func clonePersistedCellsAsValues(cells []persistedResultCell) QueryValues {
	values := make(QueryValues, len(cells))
	for index, cell := range cells {
		values[index] = clonePersistedResultCell(cell)
	}
	return values
}

func cloneJSONMap(values map[string]interface{}) map[string]interface{} {
	if values == nil {
		return nil
	}
	clone := make(map[string]interface{}, len(values))
	for key, value := range values {
		clone[key] = cloneJSONValue(value)
	}
	return clone
}

func cloneJSONValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneJSONMap(typed)
	case []interface{}:
		clone := make([]interface{}, len(typed))
		for index, item := range typed {
			clone[index] = cloneJSONValue(item)
		}
		return clone
	case []map[string]interface{}:
		return cloneRows(typed)
	case persistedResultCell:
		return clonePersistedResultCell(typed)
	default:
		return typed
	}
}

func clonePersistedResultCell(cell persistedResultCell) persistedResultCell {
	clone := persistedResultCell{
		Value: cell.Value,
		Null:  cell.Null,
	}
	if cell.Repeated != nil {
		clone.Repeated = make([]persistedResultCell, len(cell.Repeated))
		for index, item := range cell.Repeated {
			clone.Repeated[index] = clonePersistedResultCell(item)
		}
	}
	if cell.Record != nil {
		clone.Record = make(map[string]persistedResultCell, len(cell.Record))
		for name, item := range cell.Record {
			clone.Record[name] = clonePersistedResultCell(item)
		}
	}
	return clone
}

func applyDatasetUpdate(
	current *Dataset,
	body map[string]json.RawMessage,
	patch bool,
) (*Dataset, int, string, string) {
	updated := cloneDataset(current)
	if !patch {
		updated.Description = ""
		updated.Labels = nil
	}
	for field, raw := range body {
		switch field {
		case "description":
			if string(raw) == "null" {
				updated.Description = ""
				continue
			}
			if err := json.Unmarshal(raw, &updated.Description); err != nil {
				return nil, http.StatusBadRequest, "INVALID_ARGUMENT", "description must be a string"
			}
		case "labels":
			if string(raw) == "null" {
				updated.Labels = nil
				continue
			}
			var labels map[string]string
			if err := json.Unmarshal(raw, &labels); err != nil {
				return nil, http.StatusBadRequest, "INVALID_ARGUMENT", "labels must be an object of string values"
			}
			updated.Labels = cloneStringMap(labels)
		case "datasetReference":
			var reference DatasetRef
			if err := json.Unmarshal(raw, &reference); err != nil ||
				reference.ProjectId != "" && reference.ProjectId != current.DatasetReference.ProjectId ||
				reference.DatasetId != "" && reference.DatasetId != current.DatasetReference.DatasetId {
				return nil, http.StatusBadRequest, "INVALID_ARGUMENT", "datasetReference is immutable"
			}
		case "location":
			var location string
			if err := json.Unmarshal(raw, &location); err != nil || location != "" && location != current.Location {
				return nil, http.StatusBadRequest, "INVALID_ARGUMENT", "location is immutable"
			}
		case "kind", "id", "creationTime", "lastModifiedTime", "etag", "selfLink":
			// Output-only fields are ignored when clients send a full resource.
		default:
			return nil, http.StatusNotImplemented, "UNIMPLEMENTED",
				"Dataset update field " + field + " is not supported"
		}
	}
	updated.LastModifiedTime = fmt.Sprintf("%d", time.Now().UnixMilli())
	updated.Etag = newEtag()
	return updated, 0, "", ""
}

func extractSegmentAfter(path, keyword string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == keyword && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{"code": code, "status": status, "message": message},
	})
}

func newEtag() string {
	return fmt.Sprintf("BQETAG%x", time.Now().UnixNano())
}
