package alloydb

import (
	"encoding/json"
	"errors"

	"minisky/pkg/state"
)

const alloydbStateEntry = "alloydb/metadata"

func init() {
	state.MustRegisterEntryValidator(alloydbStateEntry, state.StrictEntryValidator[alloydbMetadata](nil))
}

type alloydbStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type alloydbMetadata struct {
	Clusters map[string]*Cluster `json:"clusters"`
}

// persistState deep-copies clusters and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshotClusters()
	return api.stateStore.Save(alloydbStateEntry, alloydbMetadata{Clusters: snapshot})
}

// snapshotClusters returns a deep copy of all clusters for safe serialization.
func (api *API) snapshotClusters() map[string]*Cluster {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := make(map[string]*Cluster, len(api.clusters))
	for k, v := range api.clusters {
		snapshot[k] = deepCopyCluster(v)
	}
	return snapshot
}

// loadState rehydrates clusters from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta alloydbMetadata
	if err := api.stateStore.Load(alloydbStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Clusters != nil {
		api.clusters = meta.Clusters
	}
	return nil
}

// deepCopyCluster returns a fully independent copy of a Cluster.
func deepCopyCluster(c *Cluster) *Cluster {
	raw, _ := json.Marshal(c)
	var clone Cluster
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
