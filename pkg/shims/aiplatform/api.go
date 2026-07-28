// Package aiplatform implements bounded Vertex AI Vector Search and Model Registry control planes.
package aiplatform

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/shims/vertexai"
	"minisky/pkg/state"
)

const (
	stateEntry          = "aiplatform/metadata"
	maxIndexes          = 1000
	maxModels           = 1000
	maxStoredOperations = 2000
)

func init() {
	state.MustRegisterEntryValidator(stateEntry, state.StrictEntryValidator(validateMetadata))
	registry.Register("aiplatform.googleapis.com", func(ctx *registry.Context) http.Handler {
		return ctx.SharedHandler("aiplatform.googleapis.com", func() http.Handler {
			return NewHandler(NewAPI(), vertexai.NewAPI(ctx.SvcMgr))
		})
	})
}

// Handler owns the single aiplatform.googleapis.com registration and delegates
// the established prediction surface separately from the experimental control plane.
type Handler struct {
	control    http.Handler
	prediction http.Handler
}

func NewHandler(control, prediction http.Handler) *Handler {
	return &Handler{control: control, prediction: prediction}
}

func (handler *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isPredictionRoute(r.URL.Path) {
		handler.prediction.ServeHTTP(w, r)
		return
	}
	handler.control.ServeHTTP(w, r)
}

func isPredictionRoute(path string) bool {
	if strings.HasPrefix(path, "/v1/internal/") ||
		strings.Contains(path, "/batchPredictionJobs") ||
		strings.Contains(path, "/featurestores") {
		return true
	}
	if strings.Contains(path, "/endpoints/") && strings.HasSuffix(path, ":predict") {
		return true
	}
	return strings.Contains(path, "/publishers/") &&
		strings.Contains(path, "/models/") &&
		(strings.HasSuffix(path, ":predict") ||
			strings.HasSuffix(path, ":generateContent") ||
			strings.HasSuffix(path, ":streamGenerateContent"))
}

type Index struct {
	Name              string          `json:"name"`
	DisplayName       string          `json:"displayName"`
	Description       string          `json:"description,omitempty"`
	Metadata          json.RawMessage `json:"metadata,omitempty"`
	MetadataSchemaURI string          `json:"metadataSchemaUri,omitempty"`
	IndexUpdateMethod string          `json:"indexUpdateMethod,omitempty"`
	CreateTime        string          `json:"createTime,omitempty"`
	UpdateTime        string          `json:"updateTime,omitempty"`
}

type Model struct {
	Name            string            `json:"name"`
	DisplayName     string            `json:"displayName"`
	Description     string            `json:"description,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	VersionID       string            `json:"versionId,omitempty"`
	CreateTime      string            `json:"createTime,omitempty"`
	UpdateTime      string            `json:"updateTime,omitempty"`
	ArtifactURI     string            `json:"artifactUri,omitempty"`
	ContainerSpec   json.RawMessage   `json:"containerSpec,omitempty"`
	PredictSchemata json.RawMessage   `json:"predictSchemata,omitempty"`
	EncryptionSpec  json.RawMessage   `json:"encryptionSpec,omitempty"`
}

type Operation struct {
	Name     string         `json:"name"`
	Done     bool           `json:"done"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Response any            `json:"response,omitempty"`
}

type metadata struct {
	Indexes    map[string]*Index     `json:"indexes"`
	Models     map[string]*Model     `json:"models"`
	Operations map[string]*Operation `json:"operations"`
	Seq        uint64                `json:"seq"`
}

type entryStore interface {
	Load(string, any) error
	Save(string, any) error
}

type API struct {
	mu       sync.RWMutex
	mutateMu sync.Mutex
	store    entryStore
	indexes  map[string]*Index
	models   map[string]*Model
	ops      map[string]*Operation
	seq      uint64
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	return NewAPIWithStore(state.NewGuardedEntryStore(store, err))
}

func NewAPIWithStore(store entryStore) *API {
	if _, guarded := store.(*state.GuardedEntryStore); store != nil && !guarded {
		store = state.NewGuardedEntryStore(store, nil)
	}
	api := &API{store: store, indexes: map[string]*Index{}, models: map[string]*Model{}, ops: map[string]*Operation{}}
	if store == nil {
		return api
	}
	var saved metadata
	if err := store.Load(stateEntry, &saved); err == nil {
		if saved.Indexes != nil {
			api.indexes = saved.Indexes
		}
		if saved.Models != nil {
			api.models = saved.Models
		}
		if saved.Operations != nil {
			api.ops = saved.Operations
		}
		api.seq = saved.Seq
	}
	return api
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, ":findNeighbors"):
		writeError(w, 501, "UNIMPLEMENTED", "vector neighbor search is not implemented; no distances were generated")
	case strings.Contains(path, "/indexEndpoints"):
		writeError(w, 501, "UNIMPLEMENTED", "index endpoint deployment and serving are not implemented")
	case r.Method == http.MethodPost && (strings.HasSuffix(path, ":predict") ||
		strings.HasSuffix(path, ":rawPredict") || strings.HasSuffix(path, ":explain")):
		writeError(w, 501, "UNIMPLEMENTED", "model prediction and explanation are not implemented")
	case strings.Contains(path, "/models/") && strings.Contains(path, "/versions"):
		writeError(w, 501, "UNIMPLEMENTED", "model version methods are not implemented")
	case strings.Contains(path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, path)
	case strings.HasSuffix(path, "/indexes"):
		if r.Method == http.MethodPost {
			api.createIndex(w, r, path)
		} else if r.Method == http.MethodGet {
			api.listIndexes(w, r, path)
		} else {
			writeError(w, 405, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	case strings.Contains(path, "/indexes/"):
		api.routeIndex(w, r, path)
	case strings.HasSuffix(path, "/models:upload") && r.Method == http.MethodPost:
		api.uploadModel(w, r, strings.TrimSuffix(path, "/models:upload"))
	case strings.HasSuffix(path, "/models") && r.Method == http.MethodGet:
		api.listModels(w, r, path)
	case strings.Contains(path, "/models/"):
		api.routeModel(w, r, path)
	default:
		writeError(w, 404, "NOT_FOUND", "Vertex AI resource not found")
	}
}

func (api *API) createIndex(w http.ResponseWriter, r *http.Request, path string) {
	var index Index
	if !decodeBounded(w, r, &index) {
		return
	}
	if index.Name != "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'name' is output only")
		return
	}
	if index.DisplayName == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'displayName' is required")
		return
	}
	if len(index.Metadata) != 0 || index.MetadataSchemaURI != "" || index.IndexUpdateMethod != "" {
		writeError(w, 501, "UNIMPLEMENTED",
			"fields 'metadata', 'metadataSchemaUri', and 'indexUpdateMethod' are not implemented")
		return
	}
	parent := strings.TrimSuffix(path, "/indexes")
	if !validParent(parent) {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	api.mutateMu.Lock()
	defer api.mutateMu.Unlock()
	api.mu.Lock()
	if len(api.indexes) >= maxIndexes || len(api.ops) >= maxStoredOperations {
		api.mu.Unlock()
		writeError(w, 429, "RESOURCE_EXHAUSTED", "index or operation state limit reached")
		return
	}
	api.seq++
	now := time.Now().UTC().Format(time.RFC3339Nano)
	index.Name = fmt.Sprintf("%s/indexes/index-%d", parent, api.seq)
	index.CreateTime, index.UpdateTime = now, now
	api.indexes[index.Name] = cloneIndex(&index)
	op := api.newOperationLocked(parent, "createIndex", index.Name, cloneIndex(&index))
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.indexes, index.Name)
		delete(api.ops, op.Name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "state persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(op)
}

func (api *API) routeIndex(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodGet:
		api.mu.RLock()
		value := cloneIndex(api.indexes[name])
		api.mu.RUnlock()
		if value == nil {
			writeError(w, 404, "NOT_FOUND", "index not found")
			return
		}
		_ = json.NewEncoder(w).Encode(value)
	case http.MethodDelete:
		api.deleteResource(w, name, true)
	case http.MethodPatch:
		writeError(w, 501, "UNIMPLEMENTED", "index update is not implemented")
	default:
		writeError(w, 405, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) uploadModel(w http.ResponseWriter, r *http.Request, parent string) {
	var request struct {
		Model       *Model `json:"model"`
		ParentModel string `json:"parentModel,omitempty"`
	}
	if !decodeBounded(w, r, &request) {
		return
	}
	if request.ParentModel != "" {
		writeError(w, 501, "UNIMPLEMENTED", "model version upload is not implemented")
		return
	}
	if request.Model == nil || request.Model.DisplayName == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'model.displayName' is required")
		return
	}
	if request.Model.Name != "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'model.name' is output only")
		return
	}
	if request.Model.ArtifactURI != "" {
		uri, err := url.Parse(request.Model.ArtifactURI)
		if err != nil || uri.Scheme != "gs" || uri.Host == "" || uri.User != nil {
			writeError(w, 400, "INVALID_ARGUMENT",
				"field 'model.artifactUri' must use a credential-free gs:// URI")
			return
		}
	}
	if request.Model.ArtifactURI != "" || len(request.Model.ContainerSpec) != 0 ||
		len(request.Model.PredictSchemata) != 0 || len(request.Model.EncryptionSpec) != 0 {
		writeError(w, 501, "UNIMPLEMENTED",
			"model artifacts, serving containers, schemas, and encryption options are not implemented")
		return
	}
	if !validParent(parent) {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	api.mutateMu.Lock()
	defer api.mutateMu.Unlock()
	api.mu.Lock()
	if len(api.models) >= maxModels || len(api.ops) >= maxStoredOperations {
		api.mu.Unlock()
		writeError(w, 429, "RESOURCE_EXHAUSTED", "model or operation state limit reached")
		return
	}
	api.seq++
	now := time.Now().UTC().Format(time.RFC3339Nano)
	request.Model.Name = fmt.Sprintf("%s/models/model-%d", parent, api.seq)
	request.Model.VersionID = "1"
	request.Model.CreateTime, request.Model.UpdateTime = now, now
	api.models[request.Model.Name] = cloneModel(request.Model)
	op := api.newOperationLocked(parent, "uploadModel", request.Model.Name,
		map[string]any{"model": request.Model.Name, "modelVersionId": "1"})
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.models, request.Model.Name)
		delete(api.ops, op.Name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "state persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(op)
}

func (api *API) routeModel(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodGet:
		api.mu.RLock()
		value := cloneModel(api.models[name])
		api.mu.RUnlock()
		if value == nil {
			writeError(w, 404, "NOT_FOUND", "model not found")
			return
		}
		_ = json.NewEncoder(w).Encode(value)
	case http.MethodDelete:
		api.deleteResource(w, name, false)
	case http.MethodPatch:
		writeError(w, 501, "UNIMPLEMENTED", "model update is not implemented")
	default:
		writeError(w, 405, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) deleteResource(w http.ResponseWriter, name string, index bool) {
	api.mutateMu.Lock()
	defer api.mutateMu.Unlock()
	api.mu.Lock()
	var previous any
	if index {
		previous = api.indexes[name]
	} else {
		previous = api.models[name]
	}
	if previous == nil {
		api.mu.Unlock()
		writeError(w, 404, "NOT_FOUND", "resource not found")
		return
	}
	if len(api.ops) >= maxStoredOperations {
		api.mu.Unlock()
		writeError(w, 429, "RESOURCE_EXHAUSTED", "operation state limit reached")
		return
	}
	if index {
		delete(api.indexes, name)
	} else {
		delete(api.models, name)
	}
	parent := name[:strings.LastIndex(name, "/")]
	parent = parent[:strings.LastIndex(parent, "/")]
	op := api.newOperationLocked(parent, "delete", name, map[string]any{})
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		if index {
			api.indexes[name] = previous.(*Index)
		} else {
			api.models[name] = previous.(*Model)
		}
		delete(api.ops, op.Name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "state persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(op)
}

func (api *API) listIndexes(w http.ResponseWriter, r *http.Request, path string) {
	parent := strings.TrimSuffix(path, "/indexes")
	api.mu.RLock()
	items := make([]*Index, 0)
	for name, value := range api.indexes {
		if strings.HasPrefix(name, parent+"/indexes/") {
			items = append(items, cloneIndex(value))
		}
	}
	api.mu.RUnlock()
	api.writePage(w, r, parent, "indexes", items, func(value *Index) string { return value.Name })
}

func (api *API) listModels(w http.ResponseWriter, r *http.Request, path string) {
	parent := strings.TrimSuffix(path, "/models")
	api.mu.RLock()
	items := make([]*Model, 0)
	for name, value := range api.models {
		if strings.HasPrefix(name, parent+"/models/") {
			items = append(items, cloneModel(value))
		}
	}
	api.mu.RUnlock()
	api.writePage(w, r, parent, "models", items, func(value *Model) string { return value.Name })
}

func (api *API) writePage(w http.ResponseWriter, r *http.Request, parent, key string, items any, keyFn any) {
	size := 100
	if raw := r.URL.Query().Get("pageSize"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			writeError(w, 400, "INVALID_ARGUMENT", "field 'pageSize' must be between 1 and 100")
			return
		}
		size = value
	}
	switch values := items.(type) {
	case []*Index:
		page, token, err := pagination.Page(values, size, r.URL.Query().Get("pageToken"),
			pagination.Scope{Service: "aiplatform.googleapis.com", Parent: parent}, keyFn.(func(*Index) string))
		if err != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "invalid pageToken")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{key: page, "nextPageToken": token})
	case []*Model:
		page, token, err := pagination.Page(values, size, r.URL.Query().Get("pageToken"),
			pagination.Scope{Service: "aiplatform.googleapis.com", Parent: parent}, keyFn.(func(*Model) string))
		if err != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "invalid pageToken")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{key: page, "nextPageToken": token})
	}
}

func (api *API) getOperation(w http.ResponseWriter, name string) {
	api.mu.RLock()
	op := cloneOperation(api.ops[name])
	api.mu.RUnlock()
	if op == nil {
		writeError(w, 404, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(op)
}

func (api *API) newOperationLocked(parent, verb, target string, response any) *Operation {
	api.seq++
	op := &Operation{
		Name: fmt.Sprintf("%s/operations/operation-%d", parent, api.seq), Done: true,
		Metadata: map[string]any{"verb": verb, "target": target}, Response: response,
	}
	api.ops[op.Name] = cloneOperation(op)
	return op
}

func (api *API) persist() error {
	if api.store == nil {
		return nil
	}
	api.mu.RLock()
	saved := metadata{Indexes: map[string]*Index{}, Models: map[string]*Model{}, Operations: map[string]*Operation{}, Seq: api.seq}
	for key, value := range api.indexes {
		saved.Indexes[key] = cloneIndex(value)
	}
	for key, value := range api.models {
		saved.Models[key] = cloneModel(value)
	}
	for key, value := range api.ops {
		saved.Operations[key] = cloneOperation(value)
	}
	api.mu.RUnlock()
	return api.store.Save(stateEntry, saved)
}

func validateMetadata(_ state.EntryValidationContext, saved *metadata) error {
	if len(saved.Indexes) > maxIndexes {
		return fmt.Errorf("indexes exceed local limit of %d", maxIndexes)
	}
	if len(saved.Models) > maxModels {
		return fmt.Errorf("models exceed local limit of %d", maxModels)
	}
	if len(saved.Operations) > maxStoredOperations {
		return fmt.Errorf("operations exceed local limit of %d", maxStoredOperations)
	}
	if err := state.ValidateResourceMaps(*saved); err != nil {
		return err
	}
	for name, op := range saved.Operations {
		if op == nil || op.Name != name || !op.Done {
			return fmt.Errorf("operation %q is invalid", name)
		}
	}
	return nil
}

func decodeBounded(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return false
	}
	return true
}

func validParent(parent string) bool {
	parts := strings.Split(parent, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[1] != "" && parts[2] == "locations" && parts[3] != ""
}

func mustMarshal(value any) []byte { raw, _ := json.Marshal(value); return raw }
func cloneIndex(value *Index) *Index {
	if value == nil {
		return nil
	}
	var out Index
	_ = json.Unmarshal(mustMarshal(value), &out)
	return &out
}
func cloneModel(value *Model) *Model {
	if value == nil {
		return nil
	}
	var out Model
	_ = json.Unmarshal(mustMarshal(value), &out)
	return &out
}
func cloneOperation(value *Operation) *Operation {
	if value == nil {
		return nil
	}
	var out Operation
	_ = json.Unmarshal(mustMarshal(value), &out)
	return &out
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "status": status, "details": []any{},
	}})
}
