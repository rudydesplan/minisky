package documentai

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"minisky/pkg/state"
)

const stateEntry = "documentai/metadata"

func init() {
	state.MustRegisterEntryValidator(stateEntry, state.StrictEntryValidator(validateDocumentAIMetadata))
}

// documentaiMetadata is the persisted state for Document AI processors.
type documentaiMetadata struct {
	Processors map[string]*Processor `json:"processors"`
	Operations map[string]*lro       `json:"operations"`
	Seq        int                   `json:"seq"`
}

type documentAIStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

func validateDocumentAIMetadata(_ state.EntryValidationContext, metadata *documentaiMetadata) error {
	if metadata.Seq < 0 {
		return fmt.Errorf("seq must not be negative")
	}
	for name, processor := range metadata.Processors {
		if processor != nil {
			switch processor.State {
			case "CREATING", "ENABLED", "DISABLED", "DELETING", "FAILED":
			default:
				return fmt.Errorf("processor %q has invalid state %q", name, processor.State)
			}
		}
		id := name[strings.LastIndex(name, "/")+1:]
		if !strings.HasPrefix(id, "proc-") {
			continue
		}
		value, err := strconv.Atoi(strings.TrimPrefix(id, "proc-"))
		if err == nil && value > metadata.Seq {
			return fmt.Errorf("seq %d collides with processor %q", metadata.Seq, name)
		}
	}
	for name, operation := range metadata.Operations {
		if operation == nil {
			return fmt.Errorf("operation %q is nil", name)
		}
		if operation.Name != name {
			return fmt.Errorf("operation key %q does not match name %q", name, operation.Name)
		}
	}
	return nil
}

func (api *API) persist() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	snapshot := documentaiMetadata{
		Processors: make(map[string]*Processor, len(api.processors)),
		Operations: make(map[string]*lro, len(api.operations)),
		Seq:        api.seq,
	}
	for k, v := range api.processors {
		snapshot.Processors[k] = cloneProcessor(v)
	}
	for k, v := range api.operations {
		snapshot.Operations[k] = cloneOperation(v)
	}
	api.mu.RUnlock()
	return api.stateStore.Save(stateEntry, snapshot)
}

func (api *API) rehydrate() error {
	if api.stateStore == nil {
		return nil
	}
	var m documentaiMetadata
	if err := api.stateStore.Load(stateEntry, &m); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if m.Processors != nil {
		api.processors = m.Processors
	}
	if m.Operations != nil {
		api.operations = m.Operations
	}
	api.seq = m.Seq
	return nil
}

func cloneProcessor(processor *Processor) *Processor {
	if processor == nil {
		return nil
	}
	raw, _ := json.Marshal(processor)
	var clone Processor
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func cloneOperation(operation *lro) *lro {
	if operation == nil {
		return nil
	}
	raw, _ := json.Marshal(operation)
	var clone lro
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
