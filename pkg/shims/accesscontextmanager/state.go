package accesscontextmanager

import (
	"encoding/json"
	"errors"

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
		return err
	}
	if meta.Policies != nil {
		api.policies = meta.Policies
	}
	if meta.Perimeters != nil {
		api.perimeters = meta.Perimeters
	}
	if meta.Levels != nil {
		api.levels = meta.Levels
	}
	api.seqNum = meta.SeqNum
	return nil
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
