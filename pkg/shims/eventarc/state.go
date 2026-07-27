package eventarc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const eventarcStateEntry = "eventarc/metadata"

func init() {
	state.MustRegisterEntryValidator(eventarcStateEntry, state.StrictEntryValidator[eventarcMetadata](validateEventarcMetadata))
}

type eventarcStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type eventarcMetadata struct {
	Triggers   map[string]*Trigger  `json:"triggers"`
	Deliveries map[string]*Delivery `json:"deliveries,omitempty"`
	Payloads   map[string]string    `json:"payloads,omitempty"`
}

// persistState deep-copies triggers and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	triggers, deliveries, payloads := api.snapshotState()
	return api.stateStore.Save(eventarcStateEntry, eventarcMetadata{
		Triggers: triggers, Deliveries: deliveries, Payloads: payloads,
	})
}

func (api *API) compensateState(cause error) {
	api.opMgr.MarkPersistenceFailure(cause)
	if err := api.persistState(); err == nil {
		return
	} else {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("eventarc compensation save: %w", err))
	}
	var durable eventarcMetadata
	if err := api.stateStore.Load(eventarcStateEntry, &durable); err != nil {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("eventarc compensation readback: %w", err))
		return
	}
	api.mu.Lock()
	api.triggers = durable.Triggers
	if api.triggers == nil {
		api.triggers = make(map[string]*Trigger)
	}
	api.deliveries = durable.Deliveries
	if api.deliveries == nil {
		api.deliveries = make(map[string]*Delivery)
	}
	api.payloads = durable.Payloads
	if api.payloads == nil {
		api.payloads = make(map[string]string)
	}
	api.mu.Unlock()
}

func (api *API) compensateMutation(operationName string, cause error) {
	api.compensateState(cause)
	if operationName == "" {
		return
	}
	if err := api.opMgr.RollbackScopedRegistration(operationName); err != nil {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("eventarc operation compensation: %w", err))
	}
}

func (api *API) snapshotState() (map[string]*Trigger, map[string]*Delivery, map[string]string) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	triggers := make(map[string]*Trigger, len(api.triggers))
	for k, v := range api.triggers {
		triggers[k] = deepCopyTrigger(v)
	}
	deliveries := make(map[string]*Delivery, len(api.deliveries))
	for k, v := range api.deliveries {
		if v != nil {
			clone := *v
			deliveries[k] = &clone
		}
	}
	payloads := make(map[string]string, len(api.payloads))
	for k, v := range api.payloads {
		payloads[k] = v
	}
	return triggers, deliveries, payloads
}

// loadState rehydrates triggers from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta eventarcMetadata
	if err := api.stateStore.Load(eventarcStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if err := validateEventarcMetadata(state.EntryValidationContext{}, &meta); err != nil {
		return fmt.Errorf("invalid eventarc state: %w", err)
	}
	if meta.Triggers != nil {
		api.triggers = meta.Triggers
	}
	if meta.Deliveries != nil {
		api.deliveries = meta.Deliveries
	}
	if meta.Payloads != nil {
		api.payloads = meta.Payloads
	}
	return nil
}

func validateEventarcMetadata(context state.EntryValidationContext, meta *eventarcMetadata) error {
	if len(meta.Deliveries) > maxPersistedDeliveries {
		return fmt.Errorf("too many deliveries")
	}
	for key, trigger := range meta.Triggers {
		if trigger == nil || key != trigger.Name {
			return fmt.Errorf("trigger key/name mismatch")
		}
		project := extractAfter(trigger.Name, "projects")
		if project == "" || extractAfter(trigger.Name, "locations") == "" || extractAfter(trigger.Name, "triggers") == "" {
			return fmt.Errorf("invalid trigger name %q", trigger.Name)
		}
		if err := validateDestination(project, trigger.Destination); err != nil {
			return fmt.Errorf("trigger %q destination: %w", key, err)
		}
	}
	payloadBytes := 0
	for id, payload := range meta.Payloads {
		if !validDeliveryID(id) || len(payload) > maxEventPayload {
			return fmt.Errorf("invalid payload %q", id)
		}
		payloadBytes += len(payload)
		if payloadBytes > maxPersistedPayloads {
			return fmt.Errorf("persisted payload capacity exceeded")
		}
	}
	for key, delivery := range meta.Deliveries {
		if delivery == nil || key != delivery.ID || !validDeliveryID(delivery.ID) {
			return fmt.Errorf("invalid delivery id %q", key)
		}
		if delivery.State != "ATTEMPTING" && delivery.State != "FAILED" && delivery.State != "SUCCEEDED" {
			return fmt.Errorf("invalid delivery state %q", delivery.State)
		}
		if context.Store != nil && delivery.State != "SUCCEEDED" {
			return fmt.Errorf("replayable delivery intents cannot be imported")
		}
		if delivery.Attempts < 0 {
			return fmt.Errorf("invalid delivery attempts")
		}
		trigger := meta.Triggers[delivery.Trigger]
		if trigger == nil || trigger.Destination == nil {
			return fmt.Errorf("delivery %q references missing trigger", key)
		}
		if delivery.Workflow != trigger.Destination.Workflow {
			return fmt.Errorf("delivery %q destination does not match trigger", key)
		}
		if delivery.PayloadRef != "" {
			if _, ok := meta.Payloads[delivery.PayloadRef]; !ok {
				return fmt.Errorf("delivery %q references missing payload", key)
			}
			if delivery.Payload != "" {
				return fmt.Errorf("delivery %q contains duplicate inline payload", key)
			}
		} else if len(delivery.Payload) > maxEventPayload {
			return fmt.Errorf("delivery %q payload is oversized", key)
		}
	}
	return nil
}

func validDeliveryID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') && !strings.ContainsRune("-_", r) {
			return false
		}
	}
	return true
}

// deepCopyTrigger returns a fully independent copy of a Trigger.
func deepCopyTrigger(t *Trigger) *Trigger {
	raw, _ := json.Marshal(t)
	var clone Trigger
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
