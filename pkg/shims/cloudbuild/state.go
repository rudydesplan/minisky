package cloudbuild

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"minisky/pkg/state"
)

const (
	cloudBuildStateEntry   = "cloudbuild/metadata"
	interruptedBuildStatus = "INTERNAL_ERROR"
)

type cloudBuildStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type cloudBuildMetadata struct {
	Builds   map[string]*Build        `json:"builds"`
	Triggers map[string]*BuildTrigger `json:"triggers"`
}

func init() {
	state.MustRegisterEntryValidator(
		cloudBuildStateEntry,
		state.StrictEntryValidator(validateCloudBuildMetadata),
	)
}

func validateCloudBuildMetadata(_ state.EntryValidationContext, metadata *cloudBuildMetadata) error {
	for resource, build := range metadata.Builds {
		if build == nil {
			return fmt.Errorf("build %q is nil", resource)
		}
		expected := fmt.Sprintf("projects/%s/builds/%s", build.ProjectId, build.Id)
		if build.Id == "" || build.ProjectId == "" || resource != expected {
			return fmt.Errorf("build key %q does not match identity %q", resource, expected)
		}
		switch build.Status {
		case "STATUS_UNKNOWN", "QUEUED", "WORKING", "SUCCESS", "FAILURE",
			"INTERNAL_ERROR", "TIMEOUT", "CANCELLED", "EXPIRED":
		default:
			return fmt.Errorf("build %q has invalid status %q", resource, build.Status)
		}
	}
	for resource, trigger := range metadata.Triggers {
		if trigger == nil {
			return fmt.Errorf("trigger %q is nil", resource)
		}
		if trigger.Id == "" || !strings.HasSuffix(resource, "/triggers/"+trigger.Id) {
			return fmt.Errorf("trigger key %q does not match id %q", resource, trigger.Id)
		}
	}
	return nil
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var metadata cloudBuildMetadata
	if err := api.stateStore.Load(cloudBuildStateEntry, &metadata); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("load Cloud Build metadata: %w", err)
	}
	if metadata.Builds == nil {
		metadata.Builds = make(map[string]*Build)
	}
	if metadata.Triggers == nil {
		metadata.Triggers = make(map[string]*BuildTrigger)
	}
	if err := validateCloudBuildMetadata(state.EntryValidationContext{}, &metadata); err != nil {
		return fmt.Errorf("validate Cloud Build metadata: %w", err)
	}

	normalized := false
	now := time.Now().UTC().Format(time.RFC3339)
	for resource, build := range metadata.Builds {
		if build.Status != "QUEUED" && build.Status != "WORKING" {
			continue
		}
		restored := cloneBuild(build)
		restored.Status = interruptedBuildStatus
		restored.StatusDetail = "build interrupted by MiniSky restart; execution was not replayed"
		restored.FinishTime = now
		metadata.Builds[resource] = restored
		normalized = true
	}

	api.mu.Lock()
	api.builds = metadata.Builds
	api.triggers = metadata.Triggers
	for _, build := range metadata.Builds {
		api.buildIDs[build.Id] = struct{}{}
	}
	api.mu.Unlock()

	if normalized {
		if err := api.stateStore.Save(cloudBuildStateEntry, cloneCloudBuildMetadata(metadata)); err != nil {
			return fmt.Errorf("persist interrupted Cloud Build metadata: %w", err)
		}
	}
	return nil
}

func (api *API) commitBuild(resource string, build *Build) error {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	if api.degradedErr != nil {
		return api.degradedErr
	}

	metadata := api.metadataSnapshot()
	metadata.Builds[resource] = cloneBuild(build)
	if err := validateCloudBuildMetadata(state.EntryValidationContext{}, &metadata); err != nil {
		return err
	}
	if api.stateStore != nil {
		if err := api.stateStore.Save(cloudBuildStateEntry, metadata); err != nil {
			api.degradedErr = err
			return err
		}
	}
	api.mu.Lock()
	api.builds = metadata.Builds
	api.triggers = metadata.Triggers
	api.mu.Unlock()
	return nil
}

func (api *API) removeBuild(resource string) error {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	if api.degradedErr != nil {
		return api.degradedErr
	}

	metadata := api.metadataSnapshot()
	delete(metadata.Builds, resource)
	if api.stateStore != nil {
		if err := api.stateStore.Save(cloudBuildStateEntry, metadata); err != nil {
			api.degradedErr = err
			return err
		}
	}
	api.mu.Lock()
	api.builds = metadata.Builds
	api.triggers = metadata.Triggers
	api.mu.Unlock()
	return nil
}

func (api *API) metadataSnapshot() cloudBuildMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return cloneCloudBuildMetadata(cloudBuildMetadata{
		Builds:   api.builds,
		Triggers: api.triggers,
	})
}

func (api *API) buildSnapshot(resource string) *Build {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return cloneBuild(api.builds[resource])
}

func (api *API) persistenceError() error {
	api.mutationMu.Lock()
	defer api.mutationMu.Unlock()
	return api.degradedErr
}

func cloneCloudBuildMetadata(metadata cloudBuildMetadata) cloudBuildMetadata {
	clone := cloudBuildMetadata{
		Builds:   make(map[string]*Build, len(metadata.Builds)),
		Triggers: make(map[string]*BuildTrigger, len(metadata.Triggers)),
	}
	for resource, build := range metadata.Builds {
		clone.Builds[resource] = cloneBuild(build)
	}
	for resource, trigger := range metadata.Triggers {
		clone.Triggers[resource] = cloneBuildTrigger(trigger)
	}
	return clone
}

func cloneBuild(build *Build) *Build {
	if build == nil {
		return nil
	}
	payload, _ := json.Marshal(build)
	var clone Build
	_ = json.Unmarshal(payload, &clone)
	return &clone
}

func cloneBuildTrigger(trigger *BuildTrigger) *BuildTrigger {
	if trigger == nil {
		return nil
	}
	payload, _ := json.Marshal(trigger)
	var clone BuildTrigger
	_ = json.Unmarshal(payload, &clone)
	return &clone
}
