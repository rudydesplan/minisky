package orgpolicy

import (
	"encoding/json"
	"errors"

	"minisky/pkg/state"
)

const orgPolicyStateEntry = "orgpolicy/metadata"

func init() {
	state.MustRegisterEntryValidator(orgPolicyStateEntry, state.StrictEntryValidator[orgPolicyMetadata](nil))
}

type orgPolicyStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type orgPolicyMetadata struct {
	Policies map[string]*Policy `json:"policies"`
}

// persistState deep-copies policies and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshotPolicies()
	return api.stateStore.Save(orgPolicyStateEntry, orgPolicyMetadata{Policies: snapshot})
}

// snapshotPolicies returns a deep copy of all policies for safe serialization.
func (api *API) snapshotPolicies() map[string]*Policy {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := make(map[string]*Policy, len(api.policies))
	for k, v := range api.policies {
		snapshot[k] = deepCopyPolicy(v)
	}
	return snapshot
}

// loadState rehydrates policies from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta orgPolicyMetadata
	if err := api.stateStore.Load(orgPolicyStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Policies != nil {
		api.policies = meta.Policies
	}
	return nil
}

func deepCopyPolicy(p *Policy) *Policy {
	raw, _ := json.Marshal(p)
	var clone Policy
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
