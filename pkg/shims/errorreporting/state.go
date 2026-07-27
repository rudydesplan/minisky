package errorreporting

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"minisky/pkg/state"
)

const errorreportingStateEntry = "errorreporting/metadata"

func init() {
	state.MustRegisterEntryValidator(errorreportingStateEntry, state.StrictEntryValidator(validateErrorReportingMetadata))
}

type errorreportingStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type errorreportingMetadata struct {
	Groups map[string]*ErrorGroupStats `json:"groups"`
	Events map[string][]ErrorEvent     `json:"events"`
}

func validateErrorReportingMetadata(_ state.EntryValidationContext, metadata *errorreportingMetadata) error {
	for key, group := range metadata.Groups {
		if group == nil || group.Group == nil {
			continue
		}
		if !strings.HasSuffix(key, ":"+group.Group.GroupId) {
			return fmt.Errorf("group key %q does not match groupId %q", key, group.Group.GroupId)
		}
		count, err := strconv.Atoi(group.Count)
		if err != nil || count < len(metadata.Events[key]) {
			return fmt.Errorf("group %q has invalid count %q", key, group.Count)
		}
	}
	for key := range metadata.Events {
		if _, ok := metadata.Groups[key]; !ok {
			return fmt.Errorf("events %q reference missing group", key)
		}
	}
	return nil
}

// persistState deep-copies groups/events and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	groupSnapshot := make(map[string]*ErrorGroupStats, len(api.groups))
	for k, v := range api.groups {
		groupSnapshot[k] = deepCopyGroupStats(v)
	}
	eventSnapshot := make(map[string][]ErrorEvent, len(api.events))
	for k, v := range api.events {
		copied := make([]ErrorEvent, len(v))
		for i, e := range v {
			copied[i] = deepCopyEvent(e)
		}
		eventSnapshot[k] = copied
	}
	api.mu.RUnlock()

	return api.stateStore.Save(errorreportingStateEntry, errorreportingMetadata{
		Groups: groupSnapshot,
		Events: eventSnapshot,
	})
}

// loadState rehydrates groups/events from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta errorreportingMetadata
	if err := api.stateStore.Load(errorreportingStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Groups != nil {
		api.groups = meta.Groups
	}
	if meta.Events != nil {
		api.events = meta.Events
	}
	return nil
}

// deepCopyGroupStats returns a fully independent copy.
func deepCopyGroupStats(g *ErrorGroupStats) *ErrorGroupStats {
	raw, _ := json.Marshal(g)
	var clone ErrorGroupStats
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

// deepCopyEvent returns a fully independent copy.
func deepCopyEvent(e ErrorEvent) ErrorEvent {
	raw, _ := json.Marshal(e)
	var clone ErrorEvent
	_ = json.Unmarshal(raw, &clone)
	return clone
}

// mockStore is a simple in-memory state store for testing.
type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (m *mockStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
