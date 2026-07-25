package serverless

import (
	"encoding/json"
	"errors"
	"fmt"

	"minisky/pkg/orchestrator"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

const serverlessStateEntry = "serverless/metadata"

type serverlessMetadata struct {
	Functions map[string]*Function `json:"functions"`
	Services  map[string]*Service  `json:"services"`
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, sm *orchestrator.ServiceManager, logger *logging.API, store *state.Store) (*API, error) {
	api := newAPI(opMgr, sm, logger, store)
	if store == nil {
		return api, nil
	}
	var persisted serverlessMetadata
	if err := store.Load(serverlessStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Serverless metadata: %w", err)
	}
	if persisted.Functions != nil {
		api.functions = persisted.Functions
	}
	if persisted.Services != nil {
		api.services = persisted.Services
	}
	// Docker reconciliation is deliberately metadata-only on restart: loading
	// state never provisions a missing function or service container.
	return api, nil
}

func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	payload, err := json.Marshal(serverlessMetadata{Functions: api.functions, Services: api.services})
	api.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("snapshot Serverless metadata: %w", err)
	}
	var snapshot serverlessMetadata
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return fmt.Errorf("copy Serverless metadata: %w", err)
	}
	return api.stateStore.Save(serverlessStateEntry, snapshot)
}
