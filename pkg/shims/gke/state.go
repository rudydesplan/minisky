package gke

import (
	"errors"
	"fmt"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const gkeStateEntry = "gke/metadata"

type gkeMetadata struct {
	Backend  string              `json:"backend"`
	Clusters map[string]*Cluster `json:"clusters"`
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, store *state.Store) (*API, error) {
	api := newAPI(opMgr, store)
	if store == nil {
		return api, nil
	}
	var persisted gkeMetadata
	if err := store.Load(gkeStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load GKE metadata: %w", err)
	}
	if persisted.Clusters != nil {
		api.clusters = persisted.Clusters
	}
	// Rehydration intentionally restores metadata only. Kind/Docker workloads
	// are never recreated implicitly when their containers are absent.
	return api, nil
}

func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	backend := "simulation"
	if api.backend.Enabled() {
		backend = "kind"
	}
	snapshot := gkeMetadata{Backend: backend, Clusters: make(map[string]*Cluster, len(api.clusters))}
	for key, cluster := range api.clusters {
		copy := *cluster
		snapshot.Clusters[key] = &copy
	}
	api.mu.RUnlock()
	return api.stateStore.Save(gkeStateEntry, snapshot)
}
