package gke

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const gkeStateEntry = "gke/metadata"

type gkeStore interface {
	Load(string, any) error
	Save(string, any) error
}

type gkeMetadata struct {
	Backend    string                          `json:"backend"`
	Clusters   map[string]*Cluster             `json:"clusters"`
	Ownerships map[string]*kubeconfigOwnership `json:"kubeconfigOwnerships,omitempty"`
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, store gkeStore) (*API, error) {
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
	if persisted.Ownerships != nil {
		api.ownerships = persisted.Ownerships
	}
	if backend, ok := api.backend.(*KindBackend); ok {
		for key, ownership := range api.ownerships {
			if ownership != nil && ownership.Profile == config.GetProfile() &&
				key == clusterKey(ownership.Project, ownership.Zone, ownership.Cluster) {
				backend.RestoreKubeconfigOwnership(ClusterIdentity{
					Profile: ownership.Profile, Project: ownership.Project,
					Zone: ownership.Zone, Cluster: ownership.Cluster,
				}, ownership)
			} else {
				delete(api.ownerships, key)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := backend.ReconcileKubeconfigIntents(ctx, config.GetProfile(), api.ownerships)
		cancel()
		if err != nil {
			log.Printf("[Shim: GKE] kubeconfig reconciliation remains pending: %v", err)
		}
	}
	// Rehydration intentionally restores metadata only. Kind/Docker workloads
	// are never recreated implicitly when their containers are absent.
	for key, cluster := range api.clusters {
		if cluster == nil {
			delete(api.clusters, key)
			continue
		}
		cluster.Status = "ERROR"
		cluster.StatusMessage = "metadata restored; backend availability was not reconciled after restart"
		cluster.Endpoint = ""
		cluster.MasterAuth = nil
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
	backend := "simulation"
	if api.backend.Enabled() {
		backend = "kind"
	}
	snapshot := gkeMetadata{
		Backend: backend, Clusters: make(map[string]*Cluster, len(api.clusters)),
		Ownerships: make(map[string]*kubeconfigOwnership, len(api.ownerships)),
	}
	for key, cluster := range api.clusters {
		snapshot.Clusters[key] = cloneCluster(cluster)
	}
	for key, ownership := range api.ownerships {
		if ownership != nil {
			clone := *ownership
			snapshot.Ownerships[key] = &clone
		}
	}
	api.mu.RUnlock()
	return api.stateStore.Save(gkeStateEntry, snapshot)
}
