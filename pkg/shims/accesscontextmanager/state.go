package accesscontextmanager

import (
	"encoding/json"
	"errors"
	"fmt"

	"minisky/pkg/state"
)

const acmStateEntry = "accesscontextmanager/metadata"

func init() {
	state.MustRegisterEntryValidator(acmStateEntry, state.StrictEntryValidator[acmMetadata](nil))
}

type acmStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type acmMetadata struct {
	Policies   map[string]*AccessPolicy     `json:"policies"`
	Perimeters map[string]*ServicePerimeter `json:"perimeters"`
	Levels     map[string]*AccessLevel      `json:"levels"`
	SeqNum     int                          `json:"seqNum"`
}

func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	if api.persistenceErr != nil {
		err := api.persistenceErr
		api.mu.RUnlock()
		return err
	}
	policySnap := make(map[string]*AccessPolicy, len(api.policies))
	for k, v := range api.policies {
		policySnap[k] = clonePolicy(v)
	}
	perimeterSnap := make(map[string]*ServicePerimeter, len(api.perimeters))
	for k, v := range api.perimeters {
		perimeterSnap[k] = clonePerimeter(v)
	}
	levelSnap := make(map[string]*AccessLevel, len(api.levels))
	for k, v := range api.levels {
		levelSnap[k] = cloneLevel(v)
	}
	seq := api.seqNum
	api.mu.RUnlock()

	return api.stateStore.Save(acmStateEntry, acmMetadata{
		Policies:   policySnap,
		Perimeters: perimeterSnap,
		Levels:     levelSnap,
		SeqNum:     seq,
	})
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta acmMetadata
	if err := api.stateStore.Load(acmStateEntry, &meta); err != nil {
		if isNotFound(err) {
			return nil
		}
		api.markPersistenceFailure(err)
		return err
	}
	if err := validateACMMetadata(meta); err != nil {
		api.markPersistenceFailure(err)
		return err
	}
	policies := make(map[string]*AccessPolicy, len(meta.Policies))
	for name, policy := range meta.Policies {
		policies[name] = clonePolicy(policy)
	}
	perimeters := make(map[string]*ServicePerimeter, len(meta.Perimeters))
	for name, perimeter := range meta.Perimeters {
		perimeters[name] = clonePerimeter(perimeter)
	}
	levels := make(map[string]*AccessLevel, len(meta.Levels))
	for name, level := range meta.Levels {
		levels[name] = cloneLevel(level)
	}

	api.mu.Lock()
	api.policies = policies
	api.perimeters = perimeters
	api.levels = levels
	api.seqNum = meta.SeqNum
	api.mu.Unlock()
	return nil
}

func validateACMMetadata(meta acmMetadata) error {
	if meta.SeqNum < 0 {
		return fmt.Errorf("invalid persisted Access Context Manager sequence")
	}
	for name, policy := range meta.Policies {
		if policy == nil || policy.Name != name {
			return fmt.Errorf("invalid persisted access policy %q", name)
		}
	}
	for name, perimeter := range meta.Perimeters {
		if perimeter == nil || perimeter.Name != name {
			return fmt.Errorf("invalid persisted service perimeter %q", name)
		}
	}
	for name, level := range meta.Levels {
		if level == nil || level.Name != name {
			return fmt.Errorf("invalid persisted access level %q", name)
		}
	}
	return nil
}

func (api *API) markPersistenceFailure(err error) {
	if err == nil {
		return
	}
	api.mu.Lock()
	if api.persistenceErr == nil {
		api.persistenceErr = err
	}
	api.mu.Unlock()
}

func (api *API) PersistenceError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.persistenceErr
}

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}

func clonePolicy(p *AccessPolicy) *AccessPolicy {
	raw, _ := json.Marshal(p)
	var c AccessPolicy
	_ = json.Unmarshal(raw, &c)
	return &c
}

func clonePerimeter(p *ServicePerimeter) *ServicePerimeter {
	raw, _ := json.Marshal(p)
	var c ServicePerimeter
	_ = json.Unmarshal(raw, &c)
	return &c
}

func cloneLevel(l *AccessLevel) *AccessLevel {
	raw, _ := json.Marshal(l)
	var c AccessLevel
	_ = json.Unmarshal(raw, &c)
	return &c
}
