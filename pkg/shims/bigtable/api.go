package bigtable

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	f := func(ctx *registry.Context) http.Handler {
		return ctx.SharedHandler("bigtable", func() http.Handler {
			return NewAPI(ctx.OpMgr, ctx.SvcMgr)
		})
	}
	registry.Register("bigtableadmin.googleapis.com", f)
	registry.Register("bigtable.googleapis.com", f)
}

// Instance mirrors the Bigtable Instance resource.
type Instance struct {
	Name          string            `json:"name"`
	DisplayName   string            `json:"displayName"`
	State         string            `json:"state"` // READY, CREATING
	Type          string            `json:"type"`  // PRODUCTION, DEVELOPMENT
	Labels        map[string]string `json:"labels"`
	BackendStatus string            `json:"backendStatus,omitempty"`
}

// Table mirrors the Bigtable Table resource.
type Table struct {
	Name           string                  `json:"name"`
	ColumnFamilies map[string]ColumnFamily `json:"columnFamilies"`
	Granularity    string                  `json:"granularity"` // MILLIS
}

type ColumnFamily struct {
	GcRule *GcRule `json:"gcRule,omitempty"`
}

type GcRule struct {
	MaxAge         string `json:"maxAge,omitempty"`
	MaxNumVersions int32  `json:"maxNumVersions,omitempty"`
}

// API is the high-fidelity Bigtable Admin & Data shim.
type API struct {
	mu         sync.RWMutex
	mutationMu sync.Mutex
	persistMu  sync.Mutex
	opMgr      *orchestrator.OperationManager
	svcMgr     *orchestrator.ServiceManager
	backend    bigtableBackend
	stateStore bigtableStore
	instances  map[string]*Instance // key: projects/{p}/instances/{i}
	tables     map[string]*Table    // key: projects/{p}/instances/{i}/tables/{t}
}

type bigtableBackend interface {
	Ensure(context.Context) (string, error)
}

type serviceManagerBackend struct {
	manager *orchestrator.ServiceManager
}

func (b serviceManagerBackend) Ensure(ctx context.Context) (string, error) {
	if b.manager == nil {
		return "", fmt.Errorf("Bigtable emulator backend is unavailable")
	}
	return b.manager.EnsureServiceRunning(ctx, "bigtable.googleapis.com")
}

func NewAPI(opMgr *orchestrator.OperationManager, svcMgr *orchestrator.ServiceManager) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Bigtable] state disabled: %v", err)
		return newAPI(opMgr, svcMgr, serviceManagerBackend{manager: svcMgr}, nil)
	}
	api, err := newAPIWithDependencies(opMgr, svcMgr, serviceManagerBackend{manager: svcMgr}, store)
	if err != nil {
		log.Printf("[Shim: Bigtable] state rehydration failed: %v", err)
		return newAPI(opMgr, svcMgr, serviceManagerBackend{manager: svcMgr}, store)
	}
	return api
}

func NewAPIWithStore(
	opMgr *orchestrator.OperationManager,
	backend bigtableBackend,
	store bigtableStore,
) (*API, error) {
	return newAPIWithDependencies(opMgr, nil, backend, store)
}

func newAPI(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	backend bigtableBackend,
	store bigtableStore,
) *API {
	return &API{
		opMgr: opMgr, svcMgr: svcMgr, backend: backend, stateStore: store,
		instances: make(map[string]*Instance), tables: make(map[string]*Table),
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Bigtable] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path

	// Data API suffix
	if strings.HasSuffix(path, ":readRows") {
		api.handleReadRows(w, r, strings.TrimSuffix(path, ":readRows"))
		return
	}

	switch {
	case strings.Contains(path, "/clusters"):
		api.routeClusters(w, r, path)
	case strings.Contains(path, "/instances") && !strings.Contains(path, "/tables"):
		api.routeInstances(w, r, path)
	case strings.Contains(path, "/tables"):
		api.routeTables(w, r, path)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable resource not found: "+path)
	}
}

func (api *API) routeInstances(w http.ResponseWriter, r *http.Request, path string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	project := ""
	instanceId := ""
	if len(parts) >= 3 {
		project = parts[2]
	}
	if len(parts) >= 5 {
		instanceId = parts[4]
	}

	switch r.Method {
	case http.MethodPost:
		var body struct {
			InstanceId string   `json:"instanceId"`
			Instance   Instance `json:"instance"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
			return
		}
		if project == "" || body.InstanceId == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project and instanceId are required")
			return
		}
		if _, err := api.ensureBackend(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Bigtable emulator unavailable: "+err.Error())
			return
		}

		name := fmt.Sprintf("projects/%s/instances/%s", project, body.InstanceId)
		inst := &Instance{
			Name:        name,
			DisplayName: body.Instance.DisplayName,
			State:       "READY",
			Type:        body.Instance.Type,
			Labels:      body.Instance.Labels,
		}

		api.mutationMu.Lock()
		defer api.mutationMu.Unlock()
		api.mu.Lock()
		if _, exists := api.instances[name]; exists {
			api.mu.Unlock()
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Bigtable instance already exists: "+name)
			return
		}
		api.instances[name] = inst
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			api.mu.Lock()
			delete(api.instances, name)
			api.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "INTERNAL", "persist Bigtable instance: "+err.Error())
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(inst)

	case http.MethodGet:
		if instanceId != "" {
			name := fmt.Sprintf("projects/%s/instances/%s", project, instanceId)
			api.mu.RLock()
			inst, ok := api.instances[name]
			inst = cloneInstance(inst)
			api.mu.RUnlock()
			if !ok {
				writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable instance not found: "+name)
				return
			}
			json.NewEncoder(w).Encode(inst)
		} else {
			api.mu.RLock()
			items := []*Instance{}
			for _, v := range api.instances {
				if strings.HasPrefix(v.Name, "projects/"+project+"/instances/") {
					items = append(items, cloneInstance(v))
				}
			}
			api.mu.RUnlock()
			json.NewEncoder(w).Encode(map[string]interface{}{"instances": items})
		}

	case http.MethodDelete:
		name := fmt.Sprintf("projects/%s/instances/%s", project, instanceId)
		api.mutationMu.Lock()
		defer api.mutationMu.Unlock()
		api.mu.Lock()
		instance, exists := api.instances[name]
		if !exists {
			api.mu.Unlock()
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable instance not found: "+name)
			return
		}
		removedTables := make(map[string]*Table)
		delete(api.instances, name)
		for tableName := range api.tables {
			if strings.HasPrefix(tableName, name+"/tables/") {
				removedTables[tableName] = api.tables[tableName]
				delete(api.tables, tableName)
			}
		}
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			api.mu.Lock()
			api.instances[name] = instance
			for tableName, table := range removedTables {
				api.tables[tableName] = table
			}
			api.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "INTERNAL", "persist Bigtable instance deletion: "+err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})

	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) routeTables(w http.ResponseWriter, r *http.Request, path string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 5 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid Bigtable table path")
		return
	}
	project := parts[2]
	instance := parts[4]
	tableId := ""
	if len(parts) >= 7 {
		tableId = parts[6]
	}

	parent := fmt.Sprintf("projects/%s/instances/%s", project, instance)

	switch r.Method {
	case http.MethodPost:
		var body struct {
			TableId string `json:"tableId"`
			Table   Table  `json:"table"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
			return
		}
		if body.TableId == "" {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "tableId is required")
			return
		}
		api.mutationMu.Lock()
		defer api.mutationMu.Unlock()
		api.mu.RLock()
		_, parentExists := api.instances[parent]
		api.mu.RUnlock()
		if !parentExists {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable instance not found: "+parent)
			return
		}
		if _, err := api.ensureBackend(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Bigtable emulator unavailable: "+err.Error())
			return
		}
		api.mu.RLock()
		_, parentExists = api.instances[parent]
		api.mu.RUnlock()
		if !parentExists {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable instance not found: "+parent)
			return
		}

		name := fmt.Sprintf("%s/tables/%s", parent, body.TableId)
		t := &Table{
			Name:           name,
			ColumnFamilies: body.Table.ColumnFamilies,
			Granularity:    "MILLIS",
		}
		if t.ColumnFamilies == nil {
			t.ColumnFamilies = make(map[string]ColumnFamily)
		}

		api.mu.Lock()
		if _, exists := api.tables[name]; exists {
			api.mu.Unlock()
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Bigtable table already exists: "+name)
			return
		}
		api.tables[name] = t
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			api.mu.Lock()
			delete(api.tables, name)
			api.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "INTERNAL", "persist Bigtable table: "+err.Error())
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(t)

	case http.MethodGet:
		if tableId != "" {
			name := fmt.Sprintf("%s/tables/%s", parent, tableId)
			api.mu.RLock()
			t, ok := api.tables[name]
			t = cloneTable(t)
			api.mu.RUnlock()
			if !ok {
				writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable table not found: "+name)
				return
			}
			json.NewEncoder(w).Encode(t)
		} else {
			api.mu.RLock()
			items := []*Table{}
			for _, v := range api.tables {
				if strings.HasPrefix(v.Name, parent+"/tables/") {
					items = append(items, cloneTable(v))
				}
			}
			api.mu.RUnlock()
			json.NewEncoder(w).Encode(map[string]interface{}{"tables": items})
		}

	case http.MethodDelete:
		name := fmt.Sprintf("%s/tables/%s", parent, tableId)
		api.mutationMu.Lock()
		defer api.mutationMu.Unlock()
		api.mu.Lock()
		table, exists := api.tables[name]
		if !exists {
			api.mu.Unlock()
			writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable table not found: "+name)
			return
		}
		delete(api.tables, name)
		api.mu.Unlock()
		if err := api.persistMetadata(); err != nil {
			api.mu.Lock()
			api.tables[name] = table
			api.mu.Unlock()
			writeError(w, http.StatusInternalServerError, "INTERNAL", "persist Bigtable table deletion: "+err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})

	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) routeClusters(w http.ResponseWriter, r *http.Request, path string) {
	writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Bigtable cluster administration is not implemented")
}

// handleReadRows implements the REST-to-gRPC bridge for Bigtable Data exploration.
func (api *API) handleReadRows(w http.ResponseWriter, r *http.Request, resourcePath string) {
	// resourcePath: /v2/projects/{p}/instances/{i}/tables/{t}
	parts := strings.Split(strings.Trim(resourcePath, "/"), "/")
	if len(parts) < 7 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid Bigtable table path")
		return
	}
	projectID := parts[2]
	instanceID := parts[4]
	tableID := parts[6]
	tableName := fmt.Sprintf("projects/%s/instances/%s/tables/%s", projectID, instanceID, tableID)
	api.mu.RLock()
	_, exists := api.tables[tableName]
	api.mu.RUnlock()
	if !exists {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Bigtable table not found: "+tableName)
		return
	}

	// 1. Get emulator address
	addr, err := api.ensureBackend(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Bigtable emulator unavailable: "+err.Error())
		return
	}
	// Strip http:// prefix if present
	addr = strings.TrimPrefix(addr, "http://")

	// 2. Connect to Bigtable gRPC
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	client, err := bigtable.NewClient(ctx, projectID, instanceID,
		option.WithEndpoint(addr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "connect to Bigtable emulator: "+err.Error())
		return
	}
	defer client.Close()

	table := client.Open(tableID)

	var rows []map[string]interface{}
	err = table.ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		rowMap := map[string]interface{}{
			"key":  row.Key(),
			"data": make(map[string]interface{}),
		}

		data := rowMap["data"].(map[string]interface{})
		for family, items := range row {
			familyData := make(map[string]interface{})
			for _, item := range items {
				// For simple exploration, we just show the latest value as a string
				familyData[item.Column] = string(item.Value)
			}
			data[family] = familyData
		}
		rows = append(rows, rowMap)
		return len(rows) < 100 // Limit to 100 rows for the UI
	})

	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Bigtable emulator read failed: "+err.Error())
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"rows": rows})
}

func (api *API) ensureBackend(ctx context.Context) (string, error) {
	if api.backend == nil {
		return "", fmt.Errorf("Bigtable emulator backend is unavailable")
	}
	return api.backend.Ensure(ctx)
}

func cloneInstance(instance *Instance) *Instance {
	if instance == nil {
		return nil
	}
	clone := *instance
	if instance.Labels != nil {
		clone.Labels = make(map[string]string, len(instance.Labels))
		for key, value := range instance.Labels {
			clone.Labels[key] = value
		}
	}
	return &clone
}

func cloneTable(table *Table) *Table {
	if table == nil {
		return nil
	}
	clone := *table
	if table.ColumnFamilies != nil {
		clone.ColumnFamilies = make(map[string]ColumnFamily, len(table.ColumnFamilies))
		for key, family := range table.ColumnFamilies {
			familyClone := family
			if family.GcRule != nil {
				rule := *family.GcRule
				familyClone.GcRule = &rule
			}
			clone.ColumnFamilies[key] = familyClone
		}
	}
	return &clone
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code": code, "message": message, "status": status, "details": []interface{}{},
		},
	})
}
