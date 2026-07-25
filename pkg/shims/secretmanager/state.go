package secretmanager

import (
	"errors"
	"fmt"

	"minisky/pkg/orchestrator"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

const secretManagerStateEntry = "secretmanager/metadata"

type secretManagerMetadata struct {
	Projects map[string]map[string]persistedSecret `json:"projects"`
}

type persistedSecret struct {
	Name        string            `json:"name"`
	CreateTime  string            `json:"createTime"`
	Labels      map[string]string `json:"labels,omitempty"`
	Replication map[string]any    `json:"replication"`
	Versions    []*secretVersion  `json:"versions"`
}

func NewAPIWithStore(sm *orchestrator.ServiceManager, logAPI *logging.API, store *state.Store) (*API, error) {
	api := newAPI(sm, logAPI, store)
	if store == nil {
		return api, nil
	}
	var persisted secretManagerMetadata
	if err := store.Load(secretManagerStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Secret Manager metadata: %w", err)
	}
	for project, secrets := range persisted.Projects {
		api.store[project] = make(map[string]*secret, len(secrets))
		for id, saved := range secrets {
			api.store[project][id] = &secret{
				Name: saved.Name, CreateTime: saved.CreateTime, Labels: saved.Labels,
				Replication: saved.Replication, versions: saved.Versions,
			}
		}
	}
	return api, nil
}

// persistMetadata serializes only resource data. Runtime locks and clients are
// reconstructed by NewAPIWithStore.
func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	snapshot := secretManagerMetadata{Projects: make(map[string]map[string]persistedSecret, len(api.store))}
	for project, secrets := range api.store {
		snapshot.Projects[project] = make(map[string]persistedSecret, len(secrets))
		for id, value := range secrets {
			value.mu.Lock()
			snapshot.Projects[project][id] = persistedSecret{
				Name: value.Name, CreateTime: value.CreateTime, Labels: value.Labels,
				Replication: value.Replication, Versions: append([]*secretVersion(nil), value.versions...),
			}
			value.mu.Unlock()
		}
	}
	api.mu.RUnlock()
	return api.stateStore.Save(secretManagerStateEntry, snapshot)
}
