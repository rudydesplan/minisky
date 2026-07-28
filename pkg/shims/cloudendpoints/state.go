package cloudendpoints

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const cloudEndpointsStateEntry = "cloudendpoints/metadata"

type cloudEndpointsMetadata struct {
	Configs    map[string]*ServiceConfig    `json:"configs"`
	Rollouts   map[string]*Rollout          `json:"rollouts"`
	Operations map[string]*ControlOperation `json:"operations"`
}

func init() {
	state.MustRegisterEntryValidator(cloudEndpointsStateEntry, state.StrictEntryValidator(validateCloudEndpointsMetadata))
}

func validateCloudEndpointsMetadata(_ state.EntryValidationContext, metadata *cloudEndpointsMetadata) error {
	for key, resource := range metadata.Configs {
		if resource == nil || key != resource.Name+":"+resource.ID {
			return fmt.Errorf("service config key %q does not match resource", key)
		}
	}
	for key, rollout := range metadata.Rollouts {
		if rollout == nil || key != rollout.ServiceName+":"+rollout.RolloutID {
			return fmt.Errorf("rollout key %q does not match resource", key)
		}
		configID, ok := promotedConfig(rollout.TrafficPercentStrategy)
		if !ok || metadata.Configs[rollout.ServiceName+":"+configID] == nil {
			return fmt.Errorf("rollout %q references an unknown service config", key)
		}
	}
	for key, operation := range metadata.Operations {
		if operation == nil || operation.OperationID == "" || !strings.HasSuffix(key, ":"+operation.OperationID) {
			return fmt.Errorf("control operation key %q does not match resource", key)
		}
	}
	return nil
}

func (api *API) saveSnapshot() error {
	if api.stateStore == nil {
		return nil
	}
	api.mu.RLock()
	metadata := cloudEndpointsMetadata{
		Configs:    make(map[string]*ServiceConfig, len(api.configs)),
		Rollouts:   make(map[string]*Rollout, len(api.rollouts)),
		Operations: make(map[string]*ControlOperation, len(api.operations)),
	}
	for key, resource := range api.configs {
		metadata.Configs[key] = copyConfig(resource)
	}
	for key, resource := range api.rollouts {
		metadata.Rollouts[key] = copyRollout(resource)
	}
	for key, resource := range api.operations {
		raw, _ := json.Marshal(resource)
		var clone ControlOperation
		_ = json.Unmarshal(raw, &clone)
		metadata.Operations[key] = &clone
	}
	api.mu.RUnlock()
	return api.stateStore.Save(cloudEndpointsStateEntry, metadata)
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var metadata cloudEndpointsMetadata
	if err := api.stateStore.Load(cloudEndpointsStateEntry, &metadata); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if metadata.Configs != nil {
		api.configs = metadata.Configs
	}
	if metadata.Rollouts != nil {
		api.rollouts = metadata.Rollouts
	}
	if metadata.Operations != nil {
		api.operations = metadata.Operations
	}
	return nil
}
