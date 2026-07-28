package composer

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"minisky/pkg/state"
)

const composerStateEntry = "composer/metadata"

func init() {
	state.MustRegisterEntryValidator(composerStateEntry, state.StrictEntryValidator[composerMetadata](nil))
}

type composerStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type composerMetadata struct {
	Environments map[string]*Environment `json:"environments"`
}

// persistState deep-copies environments and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshotEnvironments()
	return api.stateStore.Save(composerStateEntry, composerMetadata{Environments: snapshot})
}

// snapshotEnvironments returns a deep copy of all environments for safe serialization.
func (api *API) snapshotEnvironments() map[string]*Environment {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := make(map[string]*Environment, len(api.environments))
	for k, v := range api.environments {
		snapshot[k] = deepCopyEnvironment(v)
	}
	return snapshot
}

// loadState rehydrates environments from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta composerMetadata
	if err := api.stateStore.Load(composerStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Environments != nil {
		api.environments = make(map[string]*Environment, len(meta.Environments))
		names := make([]string, 0, len(meta.Environments))
		for name := range meta.Environments {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			environment := meta.Environments[name]
			restored := deepCopyEnvironment(environment)
			restored.State = "ERROR"
			if restored.Config != nil {
				restored.Config.AirflowURI = ""
				restored.Config.DagGcsPrefix = ""
			}
			if api.backend != nil {
				timeout := api.reconcileTimeout
				if timeout <= 0 {
					timeout = 5 * time.Second
				}
				reconcileCtx, cancel := context.WithTimeout(context.Background(), timeout)
				endpoint, owned, reconcileErr := api.backend.Reconcile(reconcileCtx, name)
				cancel()
				if reconcileErr == nil && owned {
					restored.State = "RUNNING"
					if restored.Config == nil {
						restored.Config = &EnvironmentConfig{}
					}
					restored.Config.AirflowURI = endpoint
					restored.Config.DagGcsPrefix = "minisky://" + name + "/dags"
				}
			}
			api.environments[name] = restored
		}
	}
	return nil
}

// deepCopyEnvironment returns a fully independent copy of an Environment.
func deepCopyEnvironment(e *Environment) *Environment {
	raw, _ := json.Marshal(e)
	var clone Environment
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
