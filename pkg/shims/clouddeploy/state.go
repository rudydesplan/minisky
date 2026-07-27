package clouddeploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const stateEntry = "clouddeploy/metadata"

func init() {
	state.MustRegisterEntryValidator(stateEntry, state.StrictEntryValidator(validateCloudDeployMetadata))
}

type clouddeployStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type clouddeployMetadata struct {
	Pipelines map[string]*DeliveryPipeline `json:"pipelines"`
	Releases  map[string]*Release          `json:"releases"`
	Rollouts  map[string]*Rollout          `json:"rollouts"`
}

func validateCloudDeployMetadata(_ state.EntryValidationContext, metadata *clouddeployMetadata) error {
	if err := state.ValidateResourceMaps(metadata); err != nil {
		return err
	}
	for name := range metadata.Releases {
		index := strings.LastIndex(name, "/releases/")
		if index < 0 {
			return fmt.Errorf("release %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Pipelines[name[:index]]; !ok {
			return fmt.Errorf("release %q references missing delivery pipeline", name)
		}
	}
	for name := range metadata.Rollouts {
		index := strings.LastIndex(name, "/rollouts/")
		if index < 0 {
			return fmt.Errorf("rollout %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Releases[name[:index]]; !ok {
			return fmt.Errorf("rollout %q references missing release", name)
		}
	}
	return nil
}

// persistState deep-copies resources and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshot()
	return api.stateStore.Save(stateEntry, snapshot)
}

// snapshot returns a deep copy of all resources for safe serialization.
func (api *API) snapshot() clouddeployMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	pipes := make(map[string]*DeliveryPipeline, len(api.pipelines))
	for k, v := range api.pipelines {
		pipes[k] = deepCopyPipeline(v)
	}
	rels := make(map[string]*Release, len(api.releases))
	for k, v := range api.releases {
		rels[k] = deepCopyRelease(v)
	}
	rolls := make(map[string]*Rollout, len(api.rollouts))
	for k, v := range api.rollouts {
		rolls[k] = deepCopyRollout(v)
	}
	return clouddeployMetadata{Pipelines: pipes, Releases: rels, Rollouts: rolls}
}

// loadState rehydrates resources from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta clouddeployMetadata
	if err := api.stateStore.Load(stateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Pipelines != nil {
		api.pipelines = meta.Pipelines
	}
	if meta.Releases != nil {
		api.releases = meta.Releases
	}
	if meta.Rollouts != nil {
		api.rollouts = meta.Rollouts
	}
	return nil
}

func deepCopyPipeline(p *DeliveryPipeline) *DeliveryPipeline {
	raw, _ := json.Marshal(p)
	var clone DeliveryPipeline
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func deepCopyRelease(r *Release) *Release {
	raw, _ := json.Marshal(r)
	var clone Release
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func deepCopyRollout(r *Rollout) *Rollout {
	raw, _ := json.Marshal(r)
	var clone Rollout
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
