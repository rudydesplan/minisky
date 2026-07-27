package bigtable

import (
	"encoding/json"
	"errors"
	"fmt"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const (
	bigtableStateEntry        = "bigtable/metadata"
	metadataOnlyInstanceState = "STATE_NOT_KNOWN"
)

type bigtableStore interface {
	Load(string, any) error
	Save(string, any) error
}

type bigtableMetadata struct {
	Instances  map[string]*Instance          `json:"instances"`
	Clusters   map[string]*Cluster           `json:"clusters"`
	Operations map[string]*BigtableOperation `json:"operations,omitempty"`
	Tables     map[string]*Table             `json:"tables"`
}

func newAPIWithDependencies(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	backend bigtableBackend,
	store bigtableStore,
) (*API, error) {
	api := newAPI(opMgr, svcMgr, backend, store)
	if store == nil {
		return api, nil
	}
	var persisted bigtableMetadata
	if err := store.Load(bigtableStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Bigtable metadata: %w", err)
	}
	if persisted.Instances != nil {
		api.instances = persisted.Instances
	}
	if persisted.Clusters != nil {
		api.clusters = persisted.Clusters
	}
	if persisted.Operations != nil {
		api.operations = persisted.Operations
	}
	if persisted.Tables != nil {
		api.tables = persisted.Tables
	}
	for key, instance := range api.instances {
		if instance == nil {
			delete(api.instances, key)
			continue
		}
		instance.State = metadataOnlyInstanceState
		instance.BackendStatus = "metadata restored; emulator availability was not reconciled after restart"
	}
	for key, table := range api.tables {
		if table == nil {
			delete(api.tables, key)
		}
	}
	for key, cluster := range api.clusters {
		if cluster == nil {
			delete(api.clusters, key)
			continue
		}
		cluster.State = metadataOnlyInstanceState
	}
	return api, nil
}

func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	payload, err := json.Marshal(bigtableMetadata{
		Instances:  api.instances,
		Clusters:   api.clusters,
		Operations: api.operations,
		Tables:     api.tables,
	})
	api.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("snapshot Bigtable metadata: %w", err)
	}
	return api.stateStore.Save(bigtableStateEntry, json.RawMessage(payload))
}
