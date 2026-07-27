package cloudsql

import (
	"encoding/json"
	"errors"
	"fmt"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const (
	cloudSQLStateEntry       = "cloudsql/metadata"
	metadataOnlyBackendState = "METADATA_ONLY"
)

type cloudSQLStore interface {
	Load(string, any) error
	Save(string, any) error
}

type cloudSQLMetadata struct {
	Instances map[string]*DatabaseInstance `json:"instances"`
	Databases map[string][]*Database       `json:"databases"`
	Users     map[string][]*User           `json:"users"`
}

// NewAPIWithStore constructs a Cloud SQL shim backed by the supplied profile
// store. Loading metadata never creates or adopts database containers.
func NewAPIWithStore(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	store cloudSQLStore,
) (*API, error) {
	api := newAPI(opMgr, svcMgr, store)
	if store == nil {
		return api, nil
	}

	var persisted cloudSQLMetadata
	if err := store.Load(cloudSQLStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Cloud SQL metadata: %w", err)
	}
	if persisted.Instances != nil {
		api.instances = persisted.Instances
	}
	if persisted.Databases != nil {
		api.databases = persisted.Databases
	}
	if persisted.Users != nil {
		api.users = persisted.Users
	}
	for key, instance := range api.instances {
		if instance == nil {
			delete(api.instances, key)
			continue
		}
		if instance.State == "ERROR" && instance.BackendStatus != "" {
			instance.IpAddresses = nil
			continue
		}
		instance.State = "SUSPENDED"
		instance.BackendStatus = metadataOnlyBackendState
		instance.IpAddresses = nil
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
	payload, err := json.Marshal(cloudSQLMetadata{
		Instances: api.instances,
		Databases: api.databases,
		Users:     api.users,
	})
	api.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("snapshot Cloud SQL metadata: %w", err)
	}
	return api.stateStore.Save(cloudSQLStateEntry, json.RawMessage(payload))
}
