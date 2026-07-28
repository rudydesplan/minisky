package eventarc

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"minisky/pkg/registry"
)

const (
	maxEventPayload        = 256 << 10
	maxMatchingTriggers    = 32
	maxPersistedDeliveries = 4096
	maxPersistedPayloads   = 16 << 20
	deliveryQueueSize      = 64
	pubSubPublishedType    = "google.cloud.pubsub.topic.v1.messagePublished"
)

var (
	errEventPayloadTooLarge     = errors.New("event payload exceeds 256 KiB limit")
	errTooManyMatchingTriggers  = errors.New("event matches too many triggers")
	errDeliveryCapacityExceeded = errors.New("event delivery capacity exceeded")
)

// ─────────────────────────────────────────────────────────────────────────────
// Event delivery (internal, not exposed via API)
// ─────────────────────────────────────────────────────────────────────────────

// CloudEvent represents a simplified CloudEvents envelope for local delivery.
type CloudEvent struct {
	Type    string `json:"type"`
	Source  string `json:"source"`
	Subject string `json:"subject,omitempty"`
	Data    string `json:"data"`
}

// WorkflowsExecutor is implemented by the workflows shim to receive events.
type WorkflowsExecutor interface {
	CreateExecutionFromEvent(workflowName, eventPayload string) error
}

// Delivery is a durable at-least-once delivery outcome. ATTEMPTING and FAILED
// records are replayed after the Workflows dependency is wired on restart.
type Delivery struct {
	ID          string `json:"id"`
	Trigger     string `json:"trigger"`
	Workflow    string `json:"workflow"`
	EventType   string `json:"eventType"`
	Resource    string `json:"resource"`
	Payload     string `json:"payload,omitempty"` // legacy state; new intents use PayloadRef.
	PayloadRef  string `json:"payloadRef,omitempty"`
	State       string `json:"state"`
	Attempts    int    `json:"attempts"`
	LastError   string `json:"lastError,omitempty"`
	LastAttempt string `json:"lastAttempt,omitempty"`
}

type deliveryWork struct {
	id string
}

// SetWorkflowsExecutor wires the workflows shim for event-driven execution.
func (api *API) SetWorkflowsExecutor(exec WorkflowsExecutor) {
	api.mu.Lock()
	api.executor = exec
	api.mu.Unlock()
}

// OnPostBoot wires cross-service dependencies after all shims are initialized.
func (api *API) OnPostBoot(ctx *registry.Context) {
	if wf, ok := ctx.GetShim("workflows.googleapis.com").(WorkflowsExecutor); ok {
		api.SetWorkflowsExecutor(wf)
		api.replayDeliveries()
	}
}

// HandleEvent preserves the legacy observer interface. Producers that support
// acknowledgement call HandleEventWithAck so persistence failures are visible.
func (api *API) HandleEvent(eventType, resource, payload string) {
	if err := api.HandleEventWithAck(eventType, resource, payload); err != nil {
		log.Printf("[Eventarc] event rejected: %v", err)
	}
}

// HandleEventWithAck durably records every bounded delivery intent before it
// returns success. Delivery itself is serialized through a bounded queue.
func (api *API) HandleEventWithAck(eventType, resource, payload string) error {
	log.Printf("[Eventarc] HandleEvent type=%s resource=%s", eventType, resource)
	if len(payload) > maxEventPayload {
		return errEventPayloadTooLarge
	}
	triggers := api.GetTriggers()
	matches := make([]*Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		if trigger != nil && trigger.Destination != nil && trigger.Destination.Workflow != "" &&
			matchesTrigger(trigger, eventType, resource) {
			matches = append(matches, trigger)
		}
	}
	if len(matches) > maxMatchingTriggers {
		return errTooManyMatchingTriggers
	}
	if len(matches) == 0 {
		return nil
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Name < matches[j].Name })

	payloadID := generateUUID()
	ids := make([]string, 0, len(matches))
	api.mu.Lock()
	if len(api.deliveries)+len(matches) > maxPersistedDeliveries {
		api.mu.Unlock()
		return errDeliveryCapacityExceeded
	}
	persistedPayloadBytes := 0
	for _, existing := range api.payloads {
		persistedPayloadBytes += len(existing)
	}
	if persistedPayloadBytes+len(payload) > maxPersistedPayloads {
		api.mu.Unlock()
		return errDeliveryCapacityExceeded
	}
	if api.payloads == nil {
		api.payloads = make(map[string]string)
	}
	if api.deliveries == nil {
		api.deliveries = make(map[string]*Delivery)
	}
	api.payloads[payloadID] = payload
	for _, trigger := range matches {
		log.Printf("[Eventarc] Trigger matched: %s", trigger.Name)
		id := generateUUID()
		api.deliveries[id] = &Delivery{
			ID: id, Trigger: trigger.Name, Workflow: trigger.Destination.Workflow,
			EventType: eventType, Resource: resource, PayloadRef: payloadID, State: "ATTEMPTING",
		}
		ids = append(ids, id)
	}
	api.mu.Unlock()
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		for _, id := range ids {
			delete(api.deliveries, id)
		}
		delete(api.payloads, payloadID)
		api.mu.Unlock()
		return fmt.Errorf("persist delivery intent: %w", err)
	}
	for _, id := range ids {
		api.enqueueDelivery(id)
	}
	return nil
}

func (api *API) enqueueDelivery(id string) {
	api.queueOnce.Do(func() {
		api.queue = make(chan deliveryWork, deliveryQueueSize)
		go func() {
			for work := range api.queue {
				if err := api.attemptDelivery(work.id); err != nil {
					log.Printf("[Eventarc] delivery %s failed: %v", work.id, err)
				}
			}
		}()
	})
	api.queue <- deliveryWork{id: id}
}

func (api *API) attemptDelivery(id string) error {
	api.mu.Lock()
	delivery := api.deliveries[id]
	exec := api.executor
	if delivery == nil {
		api.mu.Unlock()
		return nil
	}
	delivery.Attempts++
	delivery.LastAttempt = time.Now().UTC().Format(time.RFC3339Nano)
	workflow, payload := delivery.Workflow, delivery.Payload
	if delivery.PayloadRef != "" {
		payload = api.payloads[delivery.PayloadRef]
	}
	api.mu.Unlock()

	var err error
	if exec == nil {
		err = fmt.Errorf("workflows executor unavailable")
	} else {
		err = exec.CreateExecutionFromEvent(workflow, payload)
	}

	api.mu.Lock()
	if current := api.deliveries[id]; current != nil {
		if err != nil {
			current.State = "FAILED"
			current.LastError = err.Error()
		} else {
			current.State = "SUCCEEDED"
			current.LastError = ""
		}
	}
	api.mu.Unlock()
	if persistErr := api.persistState(); persistErr != nil {
		return fmt.Errorf("persist delivery outcome: %w", persistErr)
	}
	return err
}

func (api *API) replayDeliveries() {
	api.mu.RLock()
	ids := make([]string, 0)
	for id, delivery := range api.deliveries {
		if delivery != nil && replayEligible(delivery, api.triggers, api.payloads) {
			ids = append(ids, id)
		}
	}
	api.mu.RUnlock()
	for _, id := range ids {
		api.enqueueDelivery(id)
	}
}

func replayEligible(delivery *Delivery, triggers map[string]*Trigger, payloads map[string]string) bool {
	if delivery == nil || (delivery.State != "ATTEMPTING" && delivery.State != "FAILED") {
		return false
	}
	trigger := triggers[delivery.Trigger]
	if trigger == nil || trigger.Destination == nil || trigger.Destination.Workflow != delivery.Workflow {
		return false
	}
	if delivery.PayloadRef != "" {
		_, ok := payloads[delivery.PayloadRef]
		return ok
	}
	return len(delivery.Payload) <= maxEventPayload
}

// matchesTrigger checks if an event matches the trigger's filters.
func matchesTrigger(trigger *Trigger, eventType, resource string) bool {
	if eventType == pubSubPublishedType {
		triggerProject, triggerOK := canonicalResourceProject(trigger.Name, "triggers")
		topicProject, topicOK := canonicalResourceProject(resource, "topics")
		if !triggerOK || !topicOK || triggerProject != topicProject {
			return false
		}
		if trigger.Transport != nil && trigger.Transport.Pubsub != nil &&
			trigger.Transport.Pubsub.Topic != "" && trigger.Transport.Pubsub.Topic != resource {
			return false
		}
	}
	if len(trigger.EventFilters) == 0 {
		return true
	}
	for _, filter := range trigger.EventFilters {
		switch filter.Attribute {
		case "type":
			if !matchFilterValue(filter, eventType) {
				return false
			}
		case "resource":
			if !matchFilterValue(filter, resource) {
				return false
			}
		}
	}
	return true
}

func canonicalResourceProject(name, collection string) (string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) < 4 || parts[0] != "projects" || parts[1] == "" {
		return "", false
	}
	for i := 2; i+1 < len(parts); i++ {
		if parts[i] == collection && parts[i+1] != "" && i+2 == len(parts) {
			return parts[1], true
		}
	}
	return "", false
}

// matchFilterValue applies the filter operator (exact match or path pattern).
func matchFilterValue(filter EventFilter, value string) bool {
	if filter.Operator == "match-path-pattern" {
		return matchPathPattern(filter.Value, value)
	}
	return filter.Value == value
}

// matchPathPattern implements simple path-pattern matching with * and ** globs.
func matchPathPattern(pattern, value string) bool {
	if pattern == value {
		return true
	}
	if strings.Contains(pattern, "**") {
		prefix := strings.SplitN(pattern, "**", 2)[0]
		return strings.HasPrefix(value, prefix)
	}
	if strings.Contains(pattern, "*") {
		prefix := strings.SplitN(pattern, "*", 2)[0]
		suffix := strings.SplitN(pattern, "*", 2)[1]
		return strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix)
	}
	return false
}
