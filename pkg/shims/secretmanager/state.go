package secretmanager

import (
	"encoding/json"
	"errors"
	"fmt"

	"minisky/pkg/orchestrator"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

const secretManagerStateEntry = "secretmanager/metadata"

type secretStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

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
	return newAPIWithStore(sm, logAPI, store)
}

func newAPIWithStore(sm *orchestrator.ServiceManager, logAPI *logging.API, store secretStateStore) (*API, error) {
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

	snapshot := api.snapshotMetadata()
	return api.saveMetadata(snapshot)
}

func (api *API) snapshotMetadata() secretManagerMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := secretManagerMetadata{Projects: make(map[string]map[string]persistedSecret, len(api.store))}
	for project, secrets := range api.store {
		snapshot.Projects[project] = make(map[string]persistedSecret, len(secrets))
		for id, value := range secrets {
			value.mu.Lock()
			versions := make([]*secretVersion, 0, len(value.versions))
			for _, version := range value.versions {
				if version == nil {
					continue
				}
				copy := *version
				versions = append(versions, &copy)
			}
			snapshot.Projects[project][id] = persistedSecret{
				Name: value.Name, CreateTime: value.CreateTime, Labels: cloneStringMap(value.Labels),
				Replication: cloneAnyMap(value.Replication), Versions: versions,
			}
			value.mu.Unlock()
		}
	}
	return snapshot
}

func (api *API) saveMetadata(snapshot secretManagerMetadata) error {
	if api.stateStore == nil {
		return nil
	}
	return api.stateStore.Save(secretManagerStateEntry, snapshot)
}

func (api *API) saveMetadataTransaction(previous, next secretManagerMetadata) error {
	if api.stateStore == nil {
		return nil
	}
	saveErr := api.stateStore.Save(secretManagerStateEntry, next)
	if saveErr == nil {
		return nil
	}
	var durable secretManagerMetadata
	loadErr := api.stateStore.Load(secretManagerStateEntry, &durable)
	switch {
	case loadErr == nil && secretMetadataEqual(durable, next):
		return nil
	case loadErr == nil && secretMetadataEqual(durable, previous):
		return saveErr
	case errors.Is(loadErr, state.ErrNotFound) && len(previous.Projects) == 0:
		return saveErr
	default:
		ambiguous := fmt.Errorf("Secret Manager save outcome is ambiguous: save: %w; read back: %v", saveErr, loadErr)
		api.markPersistenceDegraded(ambiguous)
		return ambiguous
	}
}

func cloneSecretMetadata(metadata secretManagerMetadata) secretManagerMetadata {
	payload, _ := json.Marshal(metadata)
	var clone secretManagerMetadata
	_ = json.Unmarshal(payload, &clone)
	if clone.Projects == nil {
		clone.Projects = make(map[string]map[string]persistedSecret)
	}
	return clone
}

func secretMetadataEqual(left, right secretManagerMetadata) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	result := make(map[string]any, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
