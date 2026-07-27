package networksecurity

import (
	"encoding/json"
	"errors"

	"minisky/pkg/state"
)

const networksecurityStateEntry = "networksecurity/metadata"

func init() {
	state.MustRegisterEntryValidator(networksecurityStateEntry, state.StrictEntryValidator[networksecurityMetadata](nil))
}

type networksecurityStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type networksecurityMetadata struct {
	Policies map[string]*AuthorizationPolicy `json:"policies"`
}

func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	snapshot := make(map[string]*AuthorizationPolicy, len(api.policies))
	for k, v := range api.policies {
		snapshot[k] = clonePolicy(v)
	}
	api.mu.RUnlock()

	return api.stateStore.Save(networksecurityStateEntry, networksecurityMetadata{Policies: snapshot})
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta networksecurityMetadata
	if err := api.stateStore.Load(networksecurityStateEntry, &meta); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	if meta.Policies != nil {
		api.policies = meta.Policies
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}

func clonePolicy(p *AuthorizationPolicy) *AuthorizationPolicy {
	raw, _ := json.Marshal(p)
	var c AuthorizationPolicy
	_ = json.Unmarshal(raw, &c)
	return &c
}
