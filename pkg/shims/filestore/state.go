package filestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const filestoreStateEntry = "filestore/metadata"

func init() {
	state.MustRegisterEntryValidator(filestoreStateEntry, state.StrictEntryValidator[filestoreMetadata](validateFilestoreMetadata))
}

type filestoreStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type filestoreMetadata struct {
	Instances map[string]*Instance `json:"instances"`
}

func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	snapshot := make(map[string]*Instance, len(api.instances))
	for k, v := range api.instances {
		snapshot[k] = cloneInstance(v)
	}
	api.mu.RUnlock()

	return api.stateStore.Save(filestoreStateEntry, filestoreMetadata{Instances: snapshot})
}

func (api *API) compensateState(cause error) bool {
	api.opMgr.MarkPersistenceFailure(cause)
	if err := api.persistState(); err == nil {
		return true
	} else {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("filestore compensation save: %w", err))
	}
	var durable filestoreMetadata
	if err := api.stateStore.Load(filestoreStateEntry, &durable); err != nil {
		if isNotFound(err) {
			api.mu.Lock()
			api.instances = make(map[string]*Instance)
			api.mu.Unlock()
			return true
		}
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("filestore compensation readback: %w", err))
		return false
	}
	api.mu.Lock()
	api.instances = durable.Instances
	if api.instances == nil {
		api.instances = make(map[string]*Instance)
	}
	api.mu.Unlock()
	return true
}

func (api *API) compensateMutation(operationName string, cause error) bool {
	reconciled := api.compensateState(cause)
	if operationName == "" {
		return reconciled
	}
	if err := api.opMgr.RollbackScopedRegistration(operationName); err != nil {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("filestore operation compensation: %w", err))
	}
	return reconciled
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta filestoreMetadata
	if err := api.stateStore.Load(filestoreStateEntry, &meta); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	if err := validateFilestoreMetadata(state.EntryValidationContext{}, &meta); err != nil {
		return fmt.Errorf("invalid filestore state: %w", err)
	}
	if meta.Instances != nil {
		api.instances = meta.Instances
	}
	changed := false
	for name, instance := range api.instances {
		if instance.State == "READY" && !api.localShareTreeReady(name, instance.FileShares) {
			instance.State = "ERROR"
			changed = true
		}
	}
	if changed {
		if err := api.persistState(); err != nil {
			return fmt.Errorf("persist filestore data-plane reconciliation: %w", err)
		}
	}
	return nil
}

func validateFilestoreMetadata(_ state.EntryValidationContext, meta *filestoreMetadata) error {
	for key, instance := range meta.Instances {
		if err := validateInstance(key, instance); err != nil {
			return err
		}
	}
	return nil
}

func validateInstance(key string, instance *Instance) error {
	if instance == nil || key != instance.Name {
		return fmt.Errorf("instance key/name mismatch")
	}
	parts := strings.Split(instance.Name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "instances" {
		return fmt.Errorf("invalid instance name %q", instance.Name)
	}
	for _, index := range []int{1, 3, 5} {
		if !validLocalComponent(parts[index]) {
			return fmt.Errorf("invalid instance path component")
		}
	}
	if !validTiers[instance.Tier] {
		return fmt.Errorf("invalid tier %q", instance.Tier)
	}
	seen := make(map[string]struct{}, len(instance.FileShares))
	for _, share := range instance.FileShares {
		if !validLocalComponent(share.Name) {
			return fmt.Errorf("invalid file share name %q", share.Name)
		}
		if _, ok := seen[share.Name]; ok {
			return fmt.Errorf("duplicate file share %q", share.Name)
		}
		seen[share.Name] = struct{}{}
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}

func cloneInstance(inst *Instance) *Instance {
	raw, _ := json.Marshal(inst)
	var c Instance
	_ = json.Unmarshal(raw, &c)
	return &c
}
