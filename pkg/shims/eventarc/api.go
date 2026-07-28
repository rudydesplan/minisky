package eventarc

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("eventarc.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (GCP Eventarc v1 contract)
// ─────────────────────────────────────────────────────────────────────────────

// Trigger represents a google.cloud.eventarc.v1.Trigger resource.
type Trigger struct {
	Name                 string            `json:"name"`
	UID                  string            `json:"uid,omitempty"`
	CreateTime           string            `json:"createTime,omitempty"`
	UpdateTime           string            `json:"updateTime,omitempty"`
	EventFilters         []EventFilter     `json:"eventFilters,omitempty"`
	Destination          *Destination      `json:"destination,omitempty"`
	Transport            *Transport        `json:"transport,omitempty"`
	ServiceAccount       string            `json:"serviceAccount,omitempty"`
	Labels               map[string]string `json:"labels,omitempty"`
	Channel              string            `json:"channel,omitempty"`
	Conditions           map[string]any    `json:"conditions,omitempty"`
	EventDataContentType string            `json:"eventDataContentType,omitempty"`
	SatisfiesPzs         bool              `json:"satisfiesPzs,omitempty"`
	Etag                 string            `json:"etag,omitempty"`
}

// EventFilter represents a single event filter criterion.
type EventFilter struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Operator  string `json:"operator,omitempty"`
}

// Destination describes where matched events are delivered.
type Destination struct {
	CloudRun      *CloudRunDest      `json:"cloudRun,omitempty"`
	CloudFunction string             `json:"cloudFunction,omitempty"`
	GKE           *GKEDest           `json:"gke,omitempty"`
	Workflow      string             `json:"workflow,omitempty"`
	HTTPEndpoint  *HTTPEndpointDest  `json:"httpEndpoint,omitempty"`
	NetworkConfig *NetworkConfigDest `json:"networkConfig,omitempty"`
}

// CloudRunDest is a Cloud Run service destination.
type CloudRunDest struct {
	Service string `json:"service"`
	Path    string `json:"path,omitempty"`
	Region  string `json:"region,omitempty"`
}

// GKEDest is a GKE service destination.
type GKEDest struct {
	Cluster   string `json:"cluster"`
	Location  string `json:"location"`
	Namespace string `json:"namespace"`
	Service   string `json:"service"`
	Path      string `json:"path,omitempty"`
}

// HTTPEndpointDest is an HTTP endpoint destination.
type HTTPEndpointDest struct {
	URI string `json:"uri"`
}

// NetworkConfigDest holds network attachment for private destinations.
type NetworkConfigDest struct {
	NetworkAttachment string `json:"networkAttachment"`
}

// Transport describes the event transport mechanism.
type Transport struct {
	Pubsub *PubsubTransport `json:"pubsub,omitempty"`
}

// PubsubTransport holds Pub/Sub transport configuration.
type PubsubTransport struct {
	Topic        string `json:"topic,omitempty"`
	Subscription string `json:"subscription,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API implements the Eventarc v1 REST shim.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	queueOnce  sync.Once
	opMgr      *orchestrator.OperationManager
	stateStore eventarcStateStore
	triggers   map[string]*Trigger
	deliveries map[string]*Delivery
	payloads   map[string]string
	queue      chan deliveryWork
	executor   WorkflowsExecutor

	newDeliveryID func() (string, error)
	afterMatch    func()
}

// NewAPI creates a new Eventarc API shim with persistence.
func NewAPI(opMgr *orchestrator.OperationManager) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := &API{
		opMgr:         opMgr,
		stateStore:    state.NewGuardedEntryStore(store, err),
		triggers:      make(map[string]*Trigger),
		deliveries:    make(map[string]*Delivery),
		payloads:      make(map[string]string),
		newDeliveryID: secureDeliveryID,
	}
	if err != nil {
		log.Printf("[Shim: Eventarc] persistence degraded: %v", err)
		return api
	}
	if err := api.loadState(); err != nil {
		log.Printf("[Shim: Eventarc] state rehydration failed: %v", err)
	}
	return api
}

// newTestAPI creates an in-memory API for testing (no persistence).
func newTestAPI() *API {
	return &API{
		opMgr:         orchestrator.NewOperationManager(),
		triggers:      make(map[string]*Trigger),
		deliveries:    make(map[string]*Delivery),
		payloads:      make(map[string]string),
		newDeliveryID: secureDeliveryID,
	}
}

// ServeHTTP routes requests to the appropriate handler.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Eventarc] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.HasSuffix(r.URL.Path, "/triggers") && r.Method == http.MethodPost:
		api.createTrigger(w, r)
	case strings.HasSuffix(r.URL.Path, "/triggers") && r.Method == http.MethodGet:
		api.listTriggers(w, r)
	case strings.Contains(r.URL.Path, "/triggers/") && r.Method == http.MethodGet:
		api.getTrigger(w, r)
	case strings.Contains(r.URL.Path, "/triggers/") && r.Method == http.MethodPatch:
		api.patchTrigger(w, r)
	case strings.Contains(r.URL.Path, "/triggers/") && r.Method == http.MethodDelete:
		api.deleteTrigger(w, r)
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Eventarc resource not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createTrigger(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}

	triggerID := r.URL.Query().Get("triggerId")
	if triggerID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "triggerId query parameter is required")
		return
	}

	var trigger Trigger
	if err := json.NewDecoder(r.Body).Decode(&trigger); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	if len(trigger.EventFilters) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "eventFilters is required and must not be empty")
		return
	}
	if trigger.Destination == nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "destination is required")
		return
	}
	if err := validateDestination(project, trigger.Destination); err != nil {
		code := http.StatusBadRequest
		status := "INVALID_ARGUMENT"
		if err == errUnsupportedDestination {
			code = http.StatusNotImplemented
			status = "UNIMPLEMENTED"
		}
		writeError(w, code, status, err.Error())
		return
	}

	name := fmt.Sprintf("projects/%s/locations/%s/triggers/%s", project, location, triggerID)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	trigger.Name = name
	trigger.UID = generateUUID()
	trigger.CreateTime = now
	trigger.UpdateTime = now
	trigger.Etag = computeEtag(&trigger)
	validateOnly, err := optionalBoolQuery(r, "validateOnly")
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if validateOnly {
		api.mu.RLock()
		_, exists := api.triggers[name]
		api.mu.RUnlock()
		if exists {
			writeError(w, http.StatusConflict, "ALREADY_EXISTS", "trigger already exists: "+triggerID)
			return
		}
		op, err := api.opMgr.RegisterScopedTargetDurable("eventarc#operation", "create", name)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to register operation")
			return
		}
		_ = api.opMgr.FinalizeScopedDurable(op.Name,
			typedResponse("type.googleapis.com/google.cloud.eventarc.v1.Trigger", &trigger), 0, "")
		_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
		return
	}

	api.mu.Lock()
	if _, exists := api.triggers[name]; exists {
		api.mu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "trigger already exists: "+triggerID)
		return
	}
	api.triggers[name] = &trigger
	api.mu.Unlock()

	// Register LRO first (if fails → rollback map, return 503)
	op, err := api.opMgr.RegisterScopedTargetDurable("eventarc#operation", "create", name)
	if err != nil {
		api.mu.Lock()
		delete(api.triggers, name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}

	// Then persist (if fails → rollback map, return 503)
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		delete(api.triggers, name)
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		response := typedResponse("type.googleapis.com/google.cloud.eventarc.v1.Trigger", &trigger)
		if err := api.opMgr.FinalizeScopedDurable(op.Name, response, 0, ""); err != nil {
			log.Printf("[Eventarc] finalize create operation: %v", err)
		}
	}()

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name": op.Name,
		"done": false,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.eventarc.v1.OperationMetadata",
			"createTime": now,
			"target":     name,
			"verb":       "create",
			"apiVersion": "v1",
		},
	})
}

func (api *API) getTrigger(w http.ResponseWriter, r *http.Request) {
	name := parseTriggerName(r.URL.Path)

	api.mu.RLock()
	trigger, ok := api.triggers[name]
	if !ok {
		api.mu.RUnlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "trigger not found: "+name)
		return
	}
	clone := deepCopyTrigger(trigger)
	api.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(clone)
}

func (api *API) listTriggers(w http.ResponseWriter, r *http.Request) {
	project, location, ok := parseParent(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/triggers/", project, location)

	pageSize := 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}
	pageToken := r.URL.Query().Get("pageToken")

	api.mu.RLock()
	all := make([]*Trigger, 0)
	for key, trigger := range api.triggers {
		if strings.HasPrefix(key, prefix) {
			all = append(all, deepCopyTrigger(trigger))
		}
	}
	api.mu.RUnlock()

	result, nextToken, err := pagination.Page(all, pageSize, pageToken, pagination.Scope{
		Service: "eventarc.triggers",
		Parent:  strings.TrimSuffix(prefix, "/triggers/"),
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(trigger *Trigger) string { return trigger.Name })
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	if result == nil {
		result = make([]*Trigger, 0)
	}

	resp := map[string]any{
		"triggers":      result,
		"nextPageToken": nextToken,
		"unreachable":   []string{},
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (api *API) patchTrigger(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	name := parseTriggerName(r.URL.Path)
	updateMask := r.URL.Query().Get("updateMask")
	validateOnly, err := optionalBoolQuery(r, "validateOnly")
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	allowMissing, err := optionalBoolQuery(r, "allowMissing")
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	api.mu.Lock()
	existing, ok := api.triggers[name]
	if !ok {
		api.mu.Unlock()
		if allowMissing {
			writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
				"Eventarc patch allowMissing creation is recognized but not implemented")
			return
		}
		writeError(w, http.StatusNotFound, "NOT_FOUND", "trigger not found: "+name)
		return
	}
	if err := validateTriggerUpdateMask(updateMask); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		api.mu.Unlock()
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	// Apply field mask: only update fields listed in updateMask
	existingRaw, _ := json.Marshal(existing)
	var merged map[string]any
	_ = json.Unmarshal(existingRaw, &merged)

	mutableFields := []string{"destination", "labels"}
	if updateMask != "" && updateMask != "*" {
		fields := strings.Split(updateMask, ",")
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if v, exists := patch[field]; exists {
				merged[field] = v
			}
		}
	} else {
		for _, field := range mutableFields {
			if v, exists := patch[field]; exists {
				merged[field] = v
			}
		}
	}

	// Preserve output-only fields
	merged["name"] = existing.Name
	merged["uid"] = existing.UID
	merged["createTime"] = existing.CreateTime
	merged["updateTime"] = time.Now().UTC().Format(time.RFC3339Nano)

	updatedRaw, _ := json.Marshal(merged)
	var updated Trigger
	_ = json.Unmarshal(updatedRaw, &updated)
	project, _, _ := parseParent(r.URL.Path)
	if err := validateDestination(project, updated.Destination); err != nil {
		api.mu.Unlock()
		code := http.StatusBadRequest
		status := "INVALID_ARGUMENT"
		if err == errUnsupportedDestination {
			code = http.StatusNotImplemented
			status = "UNIMPLEMENTED"
		}
		writeError(w, code, status, err.Error())
		return
	}
	updated.Etag = computeEtag(&updated)
	if validateOnly {
		api.mu.Unlock()
		op, err := api.opMgr.RegisterScopedTargetDurable("eventarc#operation", "update", name)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to register operation")
			return
		}
		_ = api.opMgr.FinalizeScopedDurable(op.Name,
			typedResponse("type.googleapis.com/google.cloud.eventarc.v1.Trigger", &updated), 0, "")
		_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
		return
	}
	oldTrigger := api.triggers[name]
	api.triggers[name] = &updated
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("eventarc#operation", "update", name)
	if err != nil {
		api.mu.Lock()
		api.triggers[name] = oldTrigger
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		api.mu.Lock()
		api.triggers[name] = oldTrigger
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}
	response := typedResponse("type.googleapis.com/google.cloud.eventarc.v1.Trigger", &updated)
	_ = api.opMgr.FinalizeScopedDurable(op.Name, response, 0, "")

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

func (api *API) deleteTrigger(w http.ResponseWriter, r *http.Request) {
	name := parseTriggerName(r.URL.Path)
	allowMissing, err := optionalBoolQuery(r, "allowMissing")
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	validateOnly, err := optionalBoolQuery(r, "validateOnly")
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	api.mu.Lock()
	trigger, exists := api.triggers[name]
	if !exists {
		api.mu.Unlock()
		if allowMissing {
			op, err := api.opMgr.RegisterScopedTargetDurable("eventarc#operation", "delete", name)
			if err != nil {
				writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to register operation")
				return
			}
			_ = api.opMgr.FinalizeScopedDurable(op.Name,
				json.RawMessage(`{"@type":"type.googleapis.com/google.protobuf.Empty"}`), 0, "")
			_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
			return
		}
		writeError(w, http.StatusNotFound, "NOT_FOUND", "trigger not found: "+name)
		return
	}
	if etag := r.URL.Query().Get("etag"); etag != "" && etag != trigger.Etag {
		api.mu.Unlock()
		writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "trigger etag does not match")
		return
	}
	if validateOnly {
		api.mu.Unlock()
		op, err := api.opMgr.RegisterScopedTargetDurable("eventarc#operation", "delete", name)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Failed to register operation")
			return
		}
		_ = api.opMgr.FinalizeScopedDurable(op.Name,
			json.RawMessage(`{"@type":"type.googleapis.com/google.protobuf.Empty"}`), 0, "")
		_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
		return
	}
	delete(api.triggers, name)
	removedDeliveries, removedPayloads := api.removeTriggerDeliveriesLocked(name)
	api.mu.Unlock()

	op, err := api.opMgr.RegisterScopedTargetDurable("eventarc#operation", "delete", name)
	if err != nil {
		api.mu.Lock()
		api.triggers[name] = trigger
		api.restoreDeliveriesLocked(removedDeliveries, removedPayloads)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "Failed to register operation")
		return
	}
	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.triggers[name] = trigger
		api.restoreDeliveriesLocked(removedDeliveries, removedPayloads)
		api.mu.Unlock()
		api.compensateMutation(op.Name, err)
		writeError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	_ = api.opMgr.FinalizeScopedDurable(op.Name,
		json.RawMessage(`{"@type":"type.googleapis.com/google.protobuf.Empty"}`), 0, "")

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(api.opMgr.Get(op.Name)))
}

func (api *API) removeTriggerDeliveriesLocked(triggerName string) (map[string]*Delivery, map[string]string) {
	removedDeliveries := make(map[string]*Delivery)
	candidatePayloads := make(map[string]struct{})
	for id, delivery := range api.deliveries {
		if delivery == nil || delivery.Trigger != triggerName {
			continue
		}
		removedDeliveries[id] = delivery
		if delivery.PayloadRef != "" {
			candidatePayloads[delivery.PayloadRef] = struct{}{}
		}
		delete(api.deliveries, id)
	}

	removedPayloads := make(map[string]string)
	for payloadID := range candidatePayloads {
		referenced := false
		for _, delivery := range api.deliveries {
			if delivery != nil && delivery.PayloadRef == payloadID {
				referenced = true
				break
			}
		}
		if referenced {
			continue
		}
		if payload, ok := api.payloads[payloadID]; ok {
			removedPayloads[payloadID] = payload
			delete(api.payloads, payloadID)
		}
	}
	return removedDeliveries, removedPayloads
}

func (api *API) restoreDeliveriesLocked(deliveries map[string]*Delivery, payloads map[string]string) {
	for id, delivery := range deliveries {
		api.deliveries[id] = delivery
	}
	for id, payload := range payloads {
		api.payloads[id] = payload
	}
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := api.opMgr.PollScoped(r.URL.Path, "eventarc#operation")
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "operation not found")
		return
	}
	_ = json.NewEncoder(w).Encode(orchestrator.ScopedOperationResponse(op))
}

// GetTriggers returns all triggers (for cross-service use).
func (api *API) GetTriggers() []*Trigger {
	api.mu.RLock()
	defer api.mu.RUnlock()
	result := make([]*Trigger, 0, len(api.triggers))
	for _, t := range api.triggers {
		result = append(result, deepCopyTrigger(t))
	}
	return result
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// parseParent extracts project and location from a path like
// /v1/projects/{project}/locations/{location}/...
func parseParent(path string) (project, location string, ok bool) {
	project = extractAfter(path, "projects")
	location = extractAfter(path, "locations")
	if project == "" || location == "" {
		return "", "", false
	}
	return project, location, true
}

// parseTriggerName reconstructs the full resource name from the URL path.
func parseTriggerName(path string) string {
	project := extractAfter(path, "projects")
	location := extractAfter(path, "locations")
	triggerID := extractAfter(path, "triggers")
	return fmt.Sprintf("projects/%s/locations/%s/triggers/%s", project, location, triggerID)
}

func extractAfter(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// generateUUID produces a v4-style UUID using crypto/rand.
func generateUUID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// computeEtag generates a deterministic etag from the trigger content.
func computeEtag(t *Trigger) string {
	// Zero out etag before hashing to avoid circular dependency
	saved := t.Etag
	t.Etag = ""
	raw, _ := json.Marshal(t)
	t.Etag = saved
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:8])
}

var errUnsupportedDestination = fmt.Errorf("destination is recognized but not implemented")

func validateDestination(project string, destination *Destination) error {
	if destination == nil {
		return fmt.Errorf("destination is required")
	}
	if destination.Workflow == "" {
		return errUnsupportedDestination
	}
	workflowProject := extractAfter(destination.Workflow, "projects")
	if workflowProject == "" || workflowProject != project {
		return fmt.Errorf("workflow destination must belong to project %q", project)
	}
	if extractAfter(destination.Workflow, "locations") == "" || extractAfter(destination.Workflow, "workflows") == "" {
		return fmt.Errorf("workflow destination must be a full workflow resource name")
	}
	if destination.CloudRun != nil || destination.CloudFunction != "" || destination.GKE != nil ||
		destination.HTTPEndpoint != nil || destination.NetworkConfig != nil {
		return errUnsupportedDestination
	}
	return nil
}

func validateTriggerUpdateMask(mask string) error {
	if mask == "" {
		return nil
	}
	for _, field := range strings.Split(mask, ",") {
		field = strings.TrimSpace(field)
		if field != "destination" && field != "labels" && field != "*" {
			return fmt.Errorf("Eventarc triggers only support updating destination and labels")
		}
	}
	return nil
}

func typedResponse(typeURL string, value any) json.RawMessage {
	raw, _ := json.Marshal(value)
	var response map[string]any
	_ = json.Unmarshal(raw, &response)
	response["@type"] = typeURL
	raw, _ = json.Marshal(response)
	return raw
}

func optionalBoolQuery(r *http.Request, name string) (bool, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return value, nil
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}
