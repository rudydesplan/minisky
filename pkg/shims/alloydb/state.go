package alloydb

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

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
	Clusters  map[string]*Cluster  `json:"clusters"`
	Instances map[string]*Instance `json:"instances"`
}

// persistState deep-copies clusters and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	clusters, instances := api.snapshotMetadata()
	return api.stateStore.Save(alloydbStateEntry, alloydbMetadata{Clusters: clusters, Instances: instances})
}

func (api *API) snapshotMetadata() (map[string]*Cluster, map[string]*Instance) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	clusters := make(map[string]*Cluster, len(api.clusters))
	for k, v := range api.clusters {
		clusters[k] = deepCopyCluster(v)
	}
	instances := make(map[string]*Instance, len(api.instances))
	for k, v := range api.instances {
		instances[k] = deepCopyInstance(v)
		instances[k].backendEndpoint = ""
	}
	return clusters, instances
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
	if meta.Instances != nil {
		api.instances = meta.Instances
	}
	return nil
}

// reconcileBackends observes exact-owned Docker resources without creating or
// adopting anything. Ephemeral ports are rediscovered after every restart.
func (api *API) reconcileBackends() {
	if api.backend == nil {
		return
	}
	api.mu.RLock()
	names := make([]string, 0, len(api.instances))
	for name := range api.instances {
		names = append(names, name)
	}
	api.mu.RUnlock()
	changed := false
	for _, name := range names {
		identity, ok := parseIdentityFromName(name)
		if !ok {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		endpoint, exists, err := api.backend.Reconcile(ctx, identity)
		cancel()
		api.mu.Lock()
		instance := api.instances[name]
		if instance != nil {
			instance.backendEndpoint = ""
			instance.IPAddress = ""
			switch {
			case err != nil:
				instance.State = "ERROR"
			case !exists && instance.State == "DELETING":
				delete(api.instances, name)
			case !exists:
				instance.State = "STOPPED"
			default:
				host, _, splitErr := net.SplitHostPort(endpoint)
				if splitErr != nil || host != "127.0.0.1" {
					instance.State = "ERROR"
				} else {
					instance.State = "READY"
					instance.IPAddress = host
					instance.backendEndpoint = endpoint
				}
			}
			changed = true
		}
		api.mu.Unlock()
	}
	if changed {
		if err := api.persistState(); err != nil {
			api.opMgr.MarkPersistenceFailure(err)
		}
	}
}

// deepCopyCluster returns a fully independent copy of a Cluster.
func deepCopyCluster(c *Cluster) *Cluster {
	raw, _ := json.Marshal(c)
	var clone Cluster
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
