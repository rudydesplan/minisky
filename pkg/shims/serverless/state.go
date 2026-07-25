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

// deleteMetadata commits one metadata deletion through the same serialized
// persistence path as every other state write. A failed save restores the
// in-memory entry so a backend-idempotent delete can be retried.
func (api *API) deleteMetadata(resourceType orchestrator.ServerlessResourceType, key string) error {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	var function *Function
	var service *Service
	api.mu.Lock()
	switch resourceType {
	case orchestrator.ServerlessFunction:
		function = api.functions[key]
		delete(api.functions, key)
	case orchestrator.ServerlessService:
		service = api.services[key]
		delete(api.services, key)
	default:
		api.mu.Unlock()
		return fmt.Errorf("unsupported Serverless resource type %q", resourceType)
	}
	payload, err := json.Marshal(serverlessMetadata{Functions: api.functions, Services: api.services})
	if err != nil {
		if function != nil {
			api.functions[key] = function
		}
		if service != nil {
			api.services[key] = service
		}
		api.mu.Unlock()
		return fmt.Errorf("snapshot Serverless deletion: %w", err)
	}
	api.mu.Unlock()

	if api.stateStore == nil {
		return nil
	}
	var snapshot serverlessMetadata
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		api.restoreDeletedMetadata(resourceType, key, function, service)
		return fmt.Errorf("copy Serverless deletion: %w", err)
	}
	if err := api.stateStore.Save(serverlessStateEntry, snapshot); err != nil {
		api.restoreDeletedMetadata(resourceType, key, function, service)
		return fmt.Errorf("persist Serverless deletion: %w", err)
	}
	return nil
}

func (api *API) restoreDeletedMetadata(resourceType orchestrator.ServerlessResourceType, key string, function *Function, service *Service) {
	api.mu.Lock()
	defer api.mu.Unlock()
	switch resourceType {
	case orchestrator.ServerlessFunction:
		if _, exists := api.functions[key]; !exists && function != nil {
			api.functions[key] = function
		}
	case orchestrator.ServerlessService:
		if _, exists := api.services[key]; !exists && service != nil {
			api.services[key] = service
		}
	}
}
