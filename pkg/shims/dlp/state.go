package dlp

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"minisky/pkg/state"
)

const dlpStateEntry = "dlp/metadata"

func init() {
	state.MustRegisterEntryValidator(dlpStateEntry, state.StrictEntryValidator(validateDLPMetadata))
}

type dlpStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type dlpMetadata struct {
	Templates map[string]*InspectTemplate `json:"templates"`
	Counter   int                         `json:"counter"`
}

func validateDLPMetadata(_ state.EntryValidationContext, metadata *dlpMetadata) error {
	if metadata.Counter < 0 {
		return fmt.Errorf("counter must not be negative")
	}
	for name := range metadata.Templates {
		id := name[strings.LastIndex(name, "/")+1:]
		if !strings.HasPrefix(id, "tmpl-") {
			continue
		}
		value, err := strconv.Atoi(strings.TrimPrefix(id, "tmpl-"))
		if err == nil && value > metadata.Counter {
			return fmt.Errorf("counter %d collides with template %q", metadata.Counter, name)
		}
	}
	return nil
}

// NewAPIWithStore creates an API with an explicit state store (for testing).
func NewAPIWithStore(store dlpStateStore) *API {
	if _, guarded := store.(*state.GuardedEntryStore); store != nil && !guarded {
		store = state.NewGuardedEntryStore(store, nil)
	}
	api := newAPI(store)
	api.loadState()
	return api
}

// persistState deep-copies templates and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot, counter := api.snapshotTemplates()
	return api.stateStore.Save(dlpStateEntry, dlpMetadata{Templates: snapshot, Counter: counter})
}

// snapshotTemplates returns a deep copy of all templates for safe serialization.
func (api *API) snapshotTemplates() (map[string]*InspectTemplate, int) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := make(map[string]*InspectTemplate, len(api.templates))
	for k, v := range api.templates {
		snapshot[k] = deepCopyTemplate(v)
	}
	return snapshot, api.counter
}

// loadState rehydrates templates from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta dlpMetadata
	if err := api.stateStore.Load(dlpStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Templates != nil {
		api.templates = meta.Templates
	}
	api.counter = meta.Counter
	return nil
}

// deepCopyTemplate returns a fully independent copy of an InspectTemplate.
func deepCopyTemplate(t *InspectTemplate) *InspectTemplate {
	raw, _ := json.Marshal(t)
	var clone InspectTemplate
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
