package compute

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const computeStateEntry = "compute/metadata"

type computeMetadata struct {
	Instances      map[string]*Instance              `json:"instances"`
	Networks       map[string]*Network               `json:"networks"`
	Firewalls      map[string]*FirewallRule          `json:"firewalls"`
	InstanceGroups map[string]*InstanceGroup         `json:"instanceGroups"`
	LoadBalancers  map[string]map[string]interface{} `json:"loadBalancers"`
}

// NewAPIWithStore constructs a Compute shim backed by the supplied profile store.
// Persisted Docker-backed instances are restored as metadata only; loading state
// never creates or adopts containers.
func NewAPIWithStore(
	opMgr *orchestrator.OperationManager,
	svcMgr *orchestrator.ServiceManager,
	store *state.Store,
) (*API, error) {
	api := newAPI(opMgr, svcMgr, store)
	if store == nil {
		return api, nil
	}

	var persisted computeMetadata
	if err := store.Load(computeStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Compute metadata: %w", err)
	}
	if persisted.Instances != nil {
		api.instances = persisted.Instances
	}
	if persisted.Networks != nil {
		api.networks = persisted.Networks
	}
	if persisted.Firewalls != nil {
		api.firewalls = persisted.Firewalls
	}
	if persisted.InstanceGroups != nil {
		api.instanceGroups = persisted.InstanceGroups
	}
	if persisted.LoadBalancers != nil {
		api.loadBalancers = persisted.LoadBalancers
	}
	for key, instance := range api.instances {
		if instance == nil {
			delete(api.instances, key)
			continue
		}
		parts := strings.SplitN(key, ":", 3)
		if len(parts) == 3 {
			instance.project = parts[0]
			instance.zone = parts[1]
		}
		instance.Status = metadataOnlyStatus
		instance.HostPorts = nil
		instance.Description = rehydratedInstanceDescription
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
	payload, err := json.Marshal(computeMetadata{
		Instances:      api.instances,
		Networks:       api.networks,
		Firewalls:      api.firewalls,
		InstanceGroups: api.instanceGroups,
		LoadBalancers:  api.loadBalancers,
	})
	api.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("snapshot Compute metadata: %w", err)
	}
	return api.stateStore.Save(computeStateEntry, json.RawMessage(payload))
}
