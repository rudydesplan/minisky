package cloudtrace

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"minisky/pkg/state"
)

const cloudtraceStateEntry = "cloudtrace/metadata"

func init() {
	state.MustRegisterEntryValidator(cloudtraceStateEntry, state.StrictEntryValidator(validateCloudTraceMetadata))
}

type cloudtraceStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type cloudtraceMetadata struct {
	Traces map[string]*Trace `json:"traces"`
}

func validateCloudTraceMetadata(_ state.EntryValidationContext, metadata *cloudtraceMetadata) error {
	for key, trace := range metadata.Traces {
		if trace != nil && key != trace.ProjectId+":"+trace.TraceId {
			return fmt.Errorf("trace key %q does not match projectId/traceId", key)
		}
	}
	return nil
}

// persistState deep-copies traces and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshotTraces()
	return api.stateStore.Save(cloudtraceStateEntry, cloudtraceMetadata{Traces: snapshot})
}

// snapshotTraces returns a deep copy of all traces for safe serialization.
func (api *API) snapshotTraces() map[string]*Trace {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := make(map[string]*Trace, len(api.traces))
	for k, v := range api.traces {
		snapshot[k] = deepCopyTrace(v)
	}
	return snapshot
}

// loadState rehydrates traces from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta cloudtraceMetadata
	if err := api.stateStore.Load(cloudtraceStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Traces != nil {
		api.traces = meta.Traces
	}
	return nil
}

// deepCopyTrace returns a fully independent copy of a Trace.
func deepCopyTrace(t *Trace) *Trace {
	raw, _ := json.Marshal(t)
	var clone Trace
	_ = json.Unmarshal(raw, &clone)
	return &clone
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
