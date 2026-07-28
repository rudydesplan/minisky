package eventarc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/workflows"
	"minisky/pkg/state"
)

func TestCreateTrigger(t *testing.T) {
	api := newTestAPI()
	body := `{"eventFilters":[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"}],"destination":{"workflow":"projects/test/locations/us-central1/workflows/w"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId=my-trigger", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != false {
		t.Fatal("expected LRO not done")
	}
	name, _ := resp["name"].(string)
	if name == "" {
		t.Fatal("expected operation name in response")
	}
	meta, _ := resp["metadata"].(map[string]any)
	if meta == nil {
		t.Fatal("expected metadata in response")
	}
	if meta["verb"] != "create" {
		t.Fatalf("expected verb=create, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/triggers/my-trigger" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}

	// Verify trigger was stored
	api.mu.RLock()
	trigger := api.triggers["projects/test/locations/us-central1/triggers/my-trigger"]
	api.mu.RUnlock()
	if trigger == nil {
		t.Fatal("trigger not stored")
	}
	if trigger.UID == "" {
		t.Fatal("expected uid to be generated")
	}
	if trigger.CreateTime == "" {
		t.Fatal("expected createTime to be set")
	}
	if trigger.Etag == "" {
		t.Fatal("expected etag to be set")
	}
}

func TestBootAllWiresWorkflowsWithoutPostBootOrderAssumption(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "eventarc-postboot")
	t.Setenv(registry.ExperimentalServicesEnv, "1")

	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	api, ok := handlers["eventarc.googleapis.com"].(*API)
	if !ok {
		t.Fatalf("Eventarc handler = %T", handlers["eventarc.googleapis.com"])
	}
	api.mu.RLock()
	executor := api.executor
	api.mu.RUnlock()
	if executor == nil {
		t.Fatal("Eventarc executor was not wired after the registry completed its instantiate-all pass")
	}
	if handlers["workflows.googleapis.com"] != handlers["workflowexecutions.googleapis.com"] {
		t.Fatal("workflow control and execution domains do not share the wired API")
	}
}

func TestCreateTriggerMissingTriggerId(t *testing.T) {
	api := newTestAPI()
	body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"workflow":"projects/test/locations/us-central1/workflows/w"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateTriggerValidateOnlyReturnsTypedPreviewWithoutMutation(t *testing.T) {
	api := newTestAPI()
	body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"workflow":"projects/test/locations/us-central1/workflows/w"}}`
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/triggers?triggerId=preview&validateOnly=true",
		bytes.NewBufferString(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var operation map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if operation["done"] != true {
		t.Fatalf("preview operation = %#v", operation)
	}
	result, _ := operation["response"].(map[string]any)
	if result["@type"] != "type.googleapis.com/google.cloud.eventarc.v1.Trigger" {
		t.Fatalf("preview response = %#v", result)
	}
	if len(api.triggers) != 0 {
		t.Fatal("validateOnly create mutated triggers")
	}
}

func TestCreateTriggerMissingEventFilters(t *testing.T) {
	api := newTestAPI()
	body := `{"destination":{"cloudRun":{"service":"svc"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId=t1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateTriggerMissingDestination(t *testing.T) {
	api := newTestAPI()
	body := `{"eventFilters":[{"attribute":"type","value":"test"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId=t1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateTriggerUnsupportedDestinationReturns501WithoutMutation(t *testing.T) {
	api := newTestAPI()
	body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"httpEndpoint":{"uri":"http://127.0.0.1:8080"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId=t1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
	if len(api.triggers) != 0 {
		t.Fatal("unsupported destination must not create a trigger")
	}
}

func TestCreateTriggerRejectsCrossProjectWorkflow(t *testing.T) {
	api := newTestAPI()
	body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"workflow":"projects/other/locations/us-central1/workflows/w"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId=t1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestWorkflowDeliveryOutcomeIsPersisted(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		triggers: map[string]*Trigger{
			"projects/p/locations/l/triggers/t": {
				Name:         "projects/p/locations/l/triggers/t",
				EventFilters: []EventFilter{{Attribute: "type", Value: "test"}},
				Destination:  &Destination{Workflow: "projects/p/locations/l/workflows/w"},
			},
		},
		deliveries: make(map[string]*Delivery),
	}
	api.SetWorkflowsExecutor(workflowsExecutorFunc(func(string, string, string) error { return nil }))
	api.HandleEvent("test", "resource", `{"x":1}`)
	waitForDeliveries(t, api, "SUCCEEDED", 1)

	api.mu.RLock()
	defer api.mu.RUnlock()
	if len(api.deliveries) != 1 {
		t.Fatalf("got %d deliveries, want 1", len(api.deliveries))
	}
	for _, delivery := range api.deliveries {
		if delivery.State != "SUCCEEDED" || delivery.Attempts != 1 {
			t.Fatalf("unexpected delivery: %#v", delivery)
		}
	}
}

func TestHandleEventRejectsOversizedPayloadAndFanout(t *testing.T) {
	api := newTestAPI()
	api.triggers = make(map[string]*Trigger)
	for i := 0; i < maxMatchingTriggers+1; i++ {
		name := fmt.Sprintf("projects/p/locations/l/triggers/t%d", i)
		api.triggers[name] = &Trigger{
			Name: name, EventFilters: []EventFilter{{Attribute: "type", Value: "test"}},
			Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
		}
	}
	if err := api.HandleEventWithAck("test", "resource", `{}`); !errors.Is(err, errTooManyMatchingTriggers) {
		t.Fatalf("fanout error = %v", err)
	}
	if len(api.deliveries) != 0 {
		t.Fatal("rejected fanout created delivery intents")
	}

	api.triggers = map[string]*Trigger{
		"projects/p/locations/l/triggers/t": {
			Name:        "projects/p/locations/l/triggers/t",
			Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
		},
	}
	if err := api.HandleEventWithAck("test", "resource", strings.Repeat("x", maxEventPayload+1)); !errors.Is(err, errEventPayloadTooLarge) {
		t.Fatalf("payload error = %v", err)
	}
}

func TestHandleEventPersistsOneSharedPayloadAndPropagatesIntentSaveFailure(t *testing.T) {
	store := &controllableStore{data: make(map[string][]byte)}
	api := newTestAPI()
	api.stateStore = store
	api.triggers = map[string]*Trigger{}
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("projects/p/locations/l/triggers/t%d", i)
		api.triggers[name] = &Trigger{
			Name: name, Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
		}
	}
	api.SetWorkflowsExecutor(workflowsExecutorFunc(func(string, string, string) error { return nil }))
	payload := strings.Repeat("p", 4096)
	if err := api.HandleEventWithAck("test", "resource", payload); err != nil {
		t.Fatal(err)
	}
	waitForDeliveries(t, api, "SUCCEEDED", 2)
	raw := waitForPersistedDeliveries(t, store, "SUCCEEDED", 2)
	if got := bytes.Count(raw, []byte(payload)); got != 1 {
		t.Fatalf("persisted payload copies = %d, want 1", got)
	}

	store.setFail(true)
	before := len(api.deliveries)
	if err := api.HandleEventWithAck("test", "resource", `{}`); err == nil {
		t.Fatal("intent persistence failure was acknowledged")
	}
	if len(api.deliveries) != before {
		t.Fatal("failed intent persistence changed in-memory deliveries")
	}
}

func TestHandleEventRevalidatesTriggerAfterConcurrentDelete(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := newTestAPI()
	api.stateStore = store
	triggerName := "projects/p/locations/l/triggers/t"
	api.triggers[triggerName] = &Trigger{
		Name: triggerName, Etag: "etag",
		Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
	}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}

	matched := make(chan struct{})
	release := make(chan struct{})
	api.afterMatch = func() {
		close(matched)
		<-release
	}
	eventDone := make(chan error, 1)
	go func() {
		eventDone <- api.HandleEventWithAck("test", "resource", `{"nonce":"delete-race"}`)
	}()
	<-matched

	deleteResponse := httptest.NewRecorder()
	api.ServeHTTP(deleteResponse, httptest.NewRequest(http.MethodDelete, "/v1/"+triggerName, nil))
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	close(release)
	if err := <-eventDone; err != nil {
		t.Fatal(err)
	}
	if len(api.deliveries) != 0 || len(api.payloads) != 0 {
		t.Fatalf("orphan state deliveries=%d payloads=%d", len(api.deliveries), len(api.payloads))
	}

	restarted := newTestAPI()
	restarted.stateStore = store
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if len(restarted.triggers) != 0 || len(restarted.deliveries) != 0 || len(restarted.payloads) != 0 {
		t.Fatalf("durable orphan state triggers=%d deliveries=%d payloads=%d",
			len(restarted.triggers), len(restarted.deliveries), len(restarted.payloads))
	}
}

func TestHandleEventDeliveryIDGenerationFailsClosedAndBoundsCollisions(t *testing.T) {
	triggerName := "projects/p/locations/l/triggers/t"
	newAPI := func() *API {
		api := newTestAPI()
		api.triggers[triggerName] = &Trigger{
			Name:        triggerName,
			Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
		}
		return api
	}

	t.Run("source error", func(t *testing.T) {
		api := newAPI()
		api.newDeliveryID = func() (string, error) {
			return "", errors.New("crypto source unavailable")
		}
		if err := api.HandleEventWithAck("test", "resource", `{}`); !errors.Is(err, errDeliveryIDUnavailable) {
			t.Fatalf("error = %v", err)
		}
		if len(api.deliveries) != 0 || len(api.payloads) != 0 {
			t.Fatal("ID source failure mutated delivery state")
		}
	})

	t.Run("bounded collisions", func(t *testing.T) {
		api := newAPI()
		api.payloads["collision"] = `existing`
		calls := 0
		api.newDeliveryID = func() (string, error) {
			calls++
			return "collision", nil
		}
		if err := api.HandleEventWithAck("test", "resource", `{}`); !errors.Is(err, errDeliveryIDUnavailable) {
			t.Fatalf("error = %v", err)
		}
		if calls != maxDeliveryIDAttempts {
			t.Fatalf("generator calls = %d, want bounded %d", calls, maxDeliveryIDAttempts)
		}
		if len(api.deliveries) != 0 || len(api.payloads) != 1 {
			t.Fatal("collision exhaustion mutated delivery state")
		}
	})

	t.Run("retries collision", func(t *testing.T) {
		api := newAPI()
		api.payloads["collision"] = `existing`
		ids := []string{"collision", "payload", "delivery"}
		api.newDeliveryID = func() (string, error) {
			id := ids[0]
			ids = ids[1:]
			return id, nil
		}
		api.SetWorkflowsExecutor(workflowsExecutorFunc(func(string, string, string) error { return nil }))
		if err := api.HandleEventWithAck("test", "resource", `{}`); err != nil {
			t.Fatal(err)
		}
		waitForDeliveries(t, api, "SUCCEEDED", 1)
		if api.deliveries["delivery"] == nil || api.payloads["payload"] != `{}` {
			t.Fatalf("generated state deliveries=%#v payloads=%#v", api.deliveries, api.payloads)
		}
	})
}

func TestPubSubDeliveryRequiresTriggerProjectAndTransportTopic(t *testing.T) {
	const (
		eventType = "google.cloud.pubsub.topic.v1.messagePublished"
		workflow  = "projects/p/locations/us-central1/workflows/w"
		topic     = "projects/p/topics/orders"
	)
	tests := []struct {
		name           string
		triggerName    string
		transportTopic string
		resource       string
		wantDeliveries int
	}{
		{
			name:           "same project and transport topic",
			triggerName:    "projects/p/locations/us-central1/triggers/orders",
			transportTopic: topic,
			resource:       topic,
			wantDeliveries: 1,
		},
		{
			name:           "cross-project publish",
			triggerName:    "projects/p/locations/us-central1/triggers/orders",
			transportTopic: "projects/other/topics/orders",
			resource:       "projects/other/topics/orders",
		},
		{
			name:           "transport topic mismatch",
			triggerName:    "projects/p/locations/us-central1/triggers/orders",
			transportTopic: "projects/p/topics/other",
			resource:       topic,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newTestAPI()
			api.triggers[test.triggerName] = &Trigger{
				Name: test.triggerName,
				EventFilters: []EventFilter{
					{Attribute: "type", Value: eventType},
					{Attribute: "resource", Value: test.resource},
				},
				Destination: &Destination{Workflow: workflow},
				Transport:   &Transport{Pubsub: &PubsubTransport{Topic: test.transportTopic}},
			}
			calls := 0
			api.SetWorkflowsExecutor(workflowsExecutorFunc(func(gotWorkflow, payload, _ string) error {
				calls++
				if gotWorkflow != workflow || payload != `{"nonce":"isolation"}` {
					t.Fatalf("delivery = (%q, %q)", gotWorkflow, payload)
				}
				return nil
			}))

			if err := api.HandleEventWithAck(eventType, test.resource, `{"nonce":"isolation"}`); err != nil {
				t.Fatal(err)
			}
			if test.wantDeliveries == 1 {
				waitForDeliveries(t, api, "SUCCEEDED", 1)
			}
			if len(api.deliveries) != test.wantDeliveries || calls != test.wantDeliveries {
				t.Fatalf("deliveries=%d calls=%d, want %d", len(api.deliveries), calls, test.wantDeliveries)
			}
		})
	}
}

func TestEventarcSemanticImportRejectsCraftedReplay(t *testing.T) {
	cases := map[string]eventarcMetadata{
		"missing trigger": {
			Triggers: map[string]*Trigger{},
			Payloads: map[string]string{"p": `{}`},
			Deliveries: map[string]*Delivery{"d": {
				ID: "d", Trigger: "projects/p/locations/l/triggers/missing",
				Workflow: "projects/p/locations/l/workflows/w", PayloadRef: "p", State: "FAILED",
			}},
		},
		"cross project workflow": {
			Triggers: map[string]*Trigger{"projects/p/locations/l/triggers/t": {
				Name:        "projects/p/locations/l/triggers/t",
				Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
			}},
			Payloads: map[string]string{"p": `{}`},
			Deliveries: map[string]*Delivery{"d": {
				ID: "d", Trigger: "projects/p/locations/l/triggers/t",
				Workflow: "projects/other/locations/l/workflows/w", PayloadRef: "p", State: "FAILED",
			}},
		},
		"oversized payload": {
			Triggers: map[string]*Trigger{"projects/p/locations/l/triggers/t": {
				Name:        "projects/p/locations/l/triggers/t",
				Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
			}},
			Payloads: map[string]string{"p": strings.Repeat("x", maxEventPayload+1)},
		},
	}
	for name, metadata := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateEventarcMetadata(state.EntryValidationContext{}, &metadata); err == nil {
				t.Fatal("crafted replay metadata accepted")
			}
			store, err := state.New(t.TempDir(), "import")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := json.Marshal(state.Snapshot{
				Format: state.SnapshotFormat, Version: state.Version,
				Entries: map[string]json.RawMessage{eventarcStateEntry: raw},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Import(bytes.NewReader(snapshot)); err == nil {
				t.Fatal("semantic state import accepted crafted replay")
			}
		})
	}
}

func TestEventarcSemanticImportRejectsOtherwiseValidReplayableIntent(t *testing.T) {
	triggerName := "projects/p/locations/l/triggers/t"
	metadata := eventarcMetadata{
		Triggers: map[string]*Trigger{triggerName: {
			Name:        triggerName,
			Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
		}},
		Payloads: map[string]string{"payload": `{}`},
		Deliveries: map[string]*Delivery{"delivery": {
			ID: "delivery", Trigger: triggerName,
			Workflow:   "projects/p/locations/l/workflows/w",
			PayloadRef: "payload", State: "FAILED", Attempts: 1,
		}},
	}
	store, err := state.New(t.TempDir(), "import")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(state.Snapshot{
		Format: state.SnapshotFormat, Version: state.Version,
		Entries: map[string]json.RawMessage{eventarcStateEntry: raw},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Import(bytes.NewReader(snapshot)); err == nil {
		t.Fatal("import accepted a replayable delivery intent")
	}
}

func TestFailedWorkflowDeliveryReplaysAfterRestart(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	first := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		triggers: map[string]*Trigger{
			"projects/p/locations/l/triggers/t": {
				Name:        "projects/p/locations/l/triggers/t",
				Destination: &Destination{Workflow: "projects/p/locations/l/workflows/w"},
			},
		},
		deliveries: map[string]*Delivery{
			"d1": {
				ID: "d1", Trigger: "projects/p/locations/l/triggers/t",
				Workflow: "projects/p/locations/l/workflows/w",
				Payload:  `{}`, State: "FAILED", Attempts: 1,
			},
		},
	}
	if err := first.persistState(); err != nil {
		t.Fatal(err)
	}

	restarted := &API{
		opMgr: newTestAPI().opMgr, stateStore: store,
		triggers: make(map[string]*Trigger), deliveries: make(map[string]*Delivery),
	}
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	calls := 0
	restarted.SetWorkflowsExecutor(workflowsExecutorFunc(func(string, string, string) error {
		calls++
		return nil
	}))
	restarted.replayDeliveries()
	waitForDeliveries(t, restarted, "SUCCEEDED", 1)
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if got := restarted.deliveries["d1"]; got.State != "SUCCEEDED" || got.Attempts != 2 {
		t.Fatalf("delivery = %#v", got)
	}
}

func TestInterruptedDeliveryReplayUsesOneWorkflowExecution(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", stateRoot)
	t.Setenv("MINISKY_PROFILE", "eventarc-interrupted-replay")

	opMgr := orchestrator.NewOperationManager()
	workflowName := "projects/p/locations/us-central1/workflows/events"
	workflowAPI := workflows.NewAPI(opMgr)
	createWorkflow := httptest.NewRecorder()
	workflowAPI.ServeHTTP(createWorkflow, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/p/locations/us-central1/workflows?workflowId=events",
		bytes.NewBufferString(`{"sourceContents":"[{\"return\":\"${args}\"}]"}`),
	))
	if createWorkflow.Code != http.StatusOK {
		t.Fatalf("create workflow status=%d body=%s", createWorkflow.Code, createWorkflow.Body.String())
	}

	store, err := state.New(t.TempDir(), "eventarc-interrupted-replay")
	if err != nil {
		t.Fatal(err)
	}
	triggerName := "projects/p/locations/us-central1/triggers/events"
	payload := `{"messages":[{"attributes":{"nonce":"phase18-interrupted"}}]}`
	first := &API{
		opMgr: opMgr, stateStore: store,
		triggers: map[string]*Trigger{triggerName: {
			Name: triggerName, Destination: &Destination{Workflow: workflowName},
		}},
		deliveries: map[string]*Delivery{"delivery-1": {
			ID: "delivery-1", Trigger: triggerName, Workflow: workflowName,
			Payload: payload, State: "ATTEMPTING",
		}},
		payloads: make(map[string]string),
	}
	if err := first.persistState(); err != nil {
		t.Fatal(err)
	}

	// Model daemon interruption after the Workflow accepted the stable delivery
	// identity but before Eventarc persisted its terminal outcome.
	if err := workflowAPI.CreateExecutionFromEvent(workflowName, payload, "delivery-1"); err != nil {
		t.Fatal(err)
	}

	restartedWorkflow := workflows.NewAPI(opMgr)
	restarted := &API{
		opMgr: opMgr, stateStore: store,
		triggers: make(map[string]*Trigger), deliveries: make(map[string]*Delivery),
		payloads: make(map[string]string),
	}
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	restarted.SetWorkflowsExecutor(restartedWorkflow)
	restarted.replayDeliveries()
	waitForDeliveries(t, restarted, "SUCCEEDED", 1)

	list := httptest.NewRecorder()
	restartedWorkflow.ServeHTTP(list, httptest.NewRequest(
		http.MethodGet, "/v1/"+workflowName+"/executions", nil,
	))
	if list.Code != http.StatusOK {
		t.Fatalf("list executions status=%d body=%s", list.Code, list.Body.String())
	}
	var response struct {
		Executions []workflows.Execution `json:"executions"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Executions) != 1 {
		t.Fatalf("executions = %d, want one terminal effect after replay", len(response.Executions))
	}
	if response.Executions[0].Argument != payload {
		t.Fatalf("execution argument = %q, want nonce-correlated payload %q", response.Executions[0].Argument, payload)
	}
}

func TestWorkflowExecutionFailureKeepsEventarcDeliveryNonSuccess(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", stateRoot)
	t.Setenv("MINISKY_PROFILE", "eventarc-workflow-failure")
	opMgr := orchestrator.NewOperationManager()
	workflowAPI := workflows.NewAPI(opMgr)
	workflowName := "projects/p/locations/us-central1/workflows/failing"
	create := httptest.NewRecorder()
	workflowAPI.ServeHTTP(create, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/p/locations/us-central1/workflows?workflowId=failing",
		bytes.NewBufferString(`{"sourceContents":"[{\"unsupported\":\"step\"}]"}`),
	))
	if create.Code != http.StatusOK {
		t.Fatalf("create workflow status=%d body=%s", create.Code, create.Body.String())
	}

	api := newTestAPI()
	triggerName := "projects/p/locations/us-central1/triggers/failing"
	api.triggers[triggerName] = &Trigger{
		Name: triggerName, Destination: &Destination{Workflow: workflowName},
	}
	api.SetWorkflowsExecutor(workflowAPI)
	if err := api.HandleEventWithAck("test", "resource", `{"nonce":"must-fail"}`); err != nil {
		t.Fatal(err)
	}
	waitForDeliveries(t, api, "FAILED", 1)
	for _, delivery := range api.deliveries {
		if delivery.LastError == "" {
			t.Fatal("failed Workflow execution did not keep Eventarc non-success")
		}
		executionName := workflowName + "/executions/event-" + delivery.ID
		get := httptest.NewRecorder()
		workflowAPI.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/"+executionName, nil))
		var execution workflows.Execution
		if err := json.Unmarshal(get.Body.Bytes(), &execution); err != nil {
			t.Fatal(err)
		}
		if execution.State != "FAILED" || execution.Result != "" {
			t.Fatalf("workflow execution = %#v", execution)
		}
	}
}

func TestPubSubAndStorageEventsDeliverToWorkflowAfterRestart(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", stateRoot)
	t.Setenv("MINISKY_PROFILE", "eventarc-workflow-restart")
	opMgr := orchestrator.NewOperationManager()
	workflowAPI := workflows.NewAPI(opMgr)
	workflowName := "projects/p/locations/us-central1/workflows/events"
	createWorkflow := httptest.NewRecorder()
	workflowAPI.ServeHTTP(createWorkflow, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/p/locations/us-central1/workflows?workflowId=events",
		bytes.NewBufferString(`{"sourceContents":"[{\"return\":\"delivered\"}]"}`),
	))
	if createWorkflow.Code != http.StatusOK {
		t.Fatalf("create workflow status=%d body=%s", createWorkflow.Code, createWorkflow.Body.String())
	}

	store, err := state.New(t.TempDir(), "eventarc-restart")
	if err != nil {
		t.Fatal(err)
	}
	first := &API{
		opMgr: opMgr, stateStore: store,
		triggers: map[string]*Trigger{
			"projects/p/locations/us-central1/triggers/pubsub": {
				Name: "projects/p/locations/us-central1/triggers/pubsub",
				EventFilters: []EventFilter{{
					Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished",
				}},
				Destination: &Destination{Workflow: workflowName},
			},
			"projects/p/locations/us-central1/triggers/storage": {
				Name: "projects/p/locations/us-central1/triggers/storage",
				EventFilters: []EventFilter{{
					Attribute: "type", Value: "google.cloud.storage.object.v1.finalized",
				}},
				Destination: &Destination{Workflow: workflowName},
			},
		},
		deliveries: make(map[string]*Delivery),
	}
	first.HandleEvent(
		"google.cloud.pubsub.topic.v1.messagePublished",
		"projects/p/topics/events",
		`{"message":{"data":"cHViLXN1Yg=="}}`,
	)
	first.HandleEvent(
		"google.cloud.storage.object.v1.finalized",
		"bucket",
		`{"bucket":"bucket","name":"object"}`,
	)
	waitForDeliveries(t, first, "FAILED", 2)
	waitForStoreDeliveries(t, store, "FAILED", 2)
	if len(first.deliveries) != 2 {
		t.Fatalf("failed delivery intents = %d, want 2", len(first.deliveries))
	}
	for _, delivery := range first.deliveries {
		if delivery.State != "FAILED" || delivery.Attempts != 1 {
			t.Fatalf("pre-restart delivery = %#v", delivery)
		}
	}

	restarted := &API{
		opMgr: opMgr, stateStore: store,
		triggers: make(map[string]*Trigger), deliveries: make(map[string]*Delivery),
	}
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	restarted.SetWorkflowsExecutor(workflowAPI)
	restarted.replayDeliveries()
	waitForDeliveries(t, restarted, "SUCCEEDED", 2)
	waitForStoreDeliveries(t, store, "SUCCEEDED", 2)
	for _, delivery := range restarted.deliveries {
		if delivery.State != "SUCCEEDED" || delivery.Attempts != 2 {
			t.Fatalf("post-restart delivery = %#v", delivery)
		}
	}

	list := httptest.NewRecorder()
	workflowAPI.ServeHTTP(list, httptest.NewRequest(
		http.MethodGet,
		"/v1/"+workflowName+"/executions",
		nil,
	))
	if list.Code != http.StatusOK {
		t.Fatalf("list executions status=%d body=%s", list.Code, list.Body.String())
	}
	var response struct {
		Executions []workflows.Execution `json:"executions"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Executions) != 2 {
		t.Fatalf("workflow executions = %d, want Pub/Sub and Storage deliveries", len(response.Executions))
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		allDone := true
		for _, execution := range response.Executions {
			if execution.State == "ACTIVE" {
				allDone = false
			}
		}
		if allDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("workflow executions did not finish")
		}
		time.Sleep(time.Millisecond)
		list = httptest.NewRecorder()
		workflowAPI.ServeHTTP(list, httptest.NewRequest(
			http.MethodGet, "/v1/"+workflowName+"/executions", nil,
		))
		if err := json.Unmarshal(list.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
	}
	workflowsStore, err := state.New(stateRoot, "eventarc-workflow-restart")
	if err != nil {
		t.Fatal(err)
	}
	for {
		var persisted struct {
			Executions map[string]*workflows.Execution `json:"executions"`
		}
		err := workflowsStore.Load("workflows/metadata", &persisted)
		allPersisted := err == nil && len(persisted.Executions) == 2
		for _, execution := range persisted.Executions {
			if execution.State == "ACTIVE" {
				allPersisted = false
			}
		}
		if allPersisted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow outcomes were not persisted: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

type workflowsExecutorFunc func(string, string, string) error

func (f workflowsExecutorFunc) CreateExecutionFromEvent(workflow, payload, deliveryID string) error {
	return f(workflow, payload, deliveryID)
}

func TestCreateTriggerDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.triggers["projects/test/locations/us-central1/triggers/dup"] = &Trigger{
		Name:       "projects/test/locations/us-central1/triggers/dup",
		UID:        "existing-uid",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"workflow":"projects/test/locations/us-central1/workflows/w"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetTrigger(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.triggers["projects/test/locations/us-central1/triggers/t1"] = &Trigger{
		Name:       "projects/test/locations/us-central1/triggers/t1",
		UID:        "uid-123",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		EventFilters: []EventFilter{
			{Attribute: "type", Value: "google.cloud.storage.object.v1.finalized"},
		},
		Destination: &Destination{
			CloudRun: &CloudRunDest{Service: "my-svc", Region: "us-central1"},
		},
		ServiceAccount: "sa@project.iam.gserviceaccount.com",
		Etag:           "abc123",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/triggers/t1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var trigger Trigger
	_ = json.Unmarshal(w.Body.Bytes(), &trigger)
	if trigger.Name != "projects/test/locations/us-central1/triggers/t1" {
		t.Fatalf("unexpected name: %s", trigger.Name)
	}
	if trigger.UID != "uid-123" {
		t.Fatalf("unexpected uid: %s", trigger.UID)
	}
	if trigger.ServiceAccount != "sa@project.iam.gserviceaccount.com" {
		t.Fatalf("unexpected serviceAccount: %s", trigger.ServiceAccount)
	}
}

func TestGetTriggerNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/triggers/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListTriggers(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.triggers["projects/test/locations/us-central1/triggers/alpha"] = &Trigger{Name: "projects/test/locations/us-central1/triggers/alpha", UID: "u1", CreateTime: "2024-01-01T00:00:00Z"}
	api.triggers["projects/test/locations/us-central1/triggers/beta"] = &Trigger{Name: "projects/test/locations/us-central1/triggers/beta", UID: "u2", CreateTime: "2024-01-01T00:00:00Z"}
	api.triggers["projects/test/locations/us-central1/triggers/gamma"] = &Trigger{Name: "projects/test/locations/us-central1/triggers/gamma", UID: "u3", CreateTime: "2024-01-01T00:00:00Z"}
	api.mu.Unlock()

	// First page: pageSize=2
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/triggers?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	triggers := resp["triggers"].([]any)
	if len(triggers) != 2 {
		t.Fatalf("expected 2 triggers, got %d", len(triggers))
	}
	// Verify sorted order
	first := triggers[0].(map[string]any)["name"].(string)
	second := triggers[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/triggers?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	triggers = resp["triggers"].([]any)
	if len(triggers) != 1 {
		t.Fatalf("expected 1 trigger on second page, got %d", len(triggers))
	}
}

func TestListTriggersEmpty(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/triggers", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	triggers := resp["triggers"].([]any)
	if len(triggers) != 0 {
		t.Fatalf("expected 0 triggers, got %d", len(triggers))
	}
}

func TestDeleteTrigger(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.triggers["projects/test/locations/us-central1/triggers/t1"] = &Trigger{
		Name:       "projects/test/locations/us-central1/triggers/t1",
		UID:        "uid-1",
		CreateTime: "2024-01-01T00:00:00Z",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/triggers/t1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for delete")
	}

	// Verify trigger was removed
	api.mu.RLock()
	_, exists := api.triggers["projects/test/locations/us-central1/triggers/t1"]
	api.mu.RUnlock()
	if exists {
		t.Fatal("trigger should have been deleted")
	}
}

func TestDeleteTriggerNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/triggers/missing", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeleteTriggerAllowMissingReturnsTypedOperation(t *testing.T) {
	api := newTestAPI()
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/test/locations/us-central1/triggers/missing?allowMissing=true", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var operation map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	result, _ := operation["response"].(map[string]any)
	if operation["done"] != true || result["@type"] != "type.googleapis.com/google.protobuf.Empty" {
		t.Fatalf("operation = %#v", operation)
	}
}

func TestDeleteTriggerRemovesOnlyOwnedDeliveryIntentsAndPayloads(t *testing.T) {
	api := newTestAPI()
	firstName := "projects/test/locations/us-central1/triggers/first"
	secondName := "projects/test/locations/us-central1/triggers/second"
	workflow := "projects/test/locations/us-central1/workflows/w"
	api.triggers[firstName] = &Trigger{Name: firstName, Etag: "first", Destination: &Destination{Workflow: workflow}}
	api.triggers[secondName] = &Trigger{Name: secondName, Etag: "second", Destination: &Destination{Workflow: workflow}}
	api.payloads["shared"] = `{"nonce":"shared"}`
	api.payloads["first-only"] = `{"nonce":"first"}`
	api.deliveries["first-shared"] = &Delivery{
		ID: "first-shared", Trigger: firstName, Workflow: workflow,
		PayloadRef: "shared", State: "FAILED",
	}
	api.deliveries["first-only"] = &Delivery{
		ID: "first-only", Trigger: firstName, Workflow: workflow,
		PayloadRef: "first-only", State: "FAILED",
	}
	api.deliveries["second-shared"] = &Delivery{
		ID: "second-shared", Trigger: secondName, Workflow: workflow,
		PayloadRef: "shared", State: "FAILED",
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(
		http.MethodDelete, "/v1/"+firstName, nil,
	))
	if response.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", response.Code, response.Body.String())
	}
	if _, ok := api.deliveries["first-shared"]; ok {
		t.Fatal("deleted trigger retained its shared delivery intent")
	}
	if _, ok := api.deliveries["first-only"]; ok {
		t.Fatal("deleted trigger retained its exclusive delivery intent")
	}
	if _, ok := api.payloads["first-only"]; ok {
		t.Fatal("deleted trigger retained its exclusive payload")
	}
	if _, ok := api.deliveries["second-shared"]; !ok {
		t.Fatal("delete removed another trigger's delivery")
	}
	if _, ok := api.payloads["shared"]; !ok {
		t.Fatal("delete removed a payload still referenced by another trigger")
	}
}

func TestDeleteTriggerEtagMismatchDoesNotMutate(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us-central1/triggers/t1"
	api.triggers[name] = &Trigger{
		Name: name, Etag: "current",
		Destination: &Destination{Workflow: "projects/test/locations/us-central1/workflows/w"},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/"+name+"?etag=stale", nil))
	if response.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if api.triggers[name] == nil {
		t.Fatal("etag mismatch deleted trigger")
	}
}

func TestPatchTrigger(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.triggers["projects/test/locations/us-central1/triggers/t1"] = &Trigger{
		Name:       "projects/test/locations/us-central1/triggers/t1",
		UID:        "uid-1",
		CreateTime: "2024-01-01T00:00:00Z",
		UpdateTime: "2024-01-01T00:00:00Z",
		EventFilters: []EventFilter{
			{Attribute: "type", Value: "google.cloud.storage.object.v1.finalized"},
		},
		Destination: &Destination{
			Workflow: "projects/test/locations/us-central1/workflows/w",
		},
		ServiceAccount: "old-sa@project.iam.gserviceaccount.com",
	}
	api.mu.Unlock()

	body := `{"destination":{"workflow":"projects/test/locations/us-central1/workflows/new"}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/triggers/t1?updateMask=destination", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected LRO done=true for patch")
	}

	// Verify the trigger was updated
	api.mu.RLock()
	trigger := api.triggers["projects/test/locations/us-central1/triggers/t1"]
	api.mu.RUnlock()
	if trigger.Destination == nil || trigger.Destination.Workflow != "projects/test/locations/us-central1/workflows/new" {
		t.Fatalf("expected updated destination, got %#v", trigger.Destination)
	}
	// Verify output-only fields preserved
	if trigger.UID != "uid-1" {
		t.Fatalf("uid should be preserved, got %s", trigger.UID)
	}
	if trigger.CreateTime != "2024-01-01T00:00:00Z" {
		t.Fatalf("createTime should be preserved, got %s", trigger.CreateTime)
	}
	if trigger.UpdateTime == "2024-01-01T00:00:00Z" {
		t.Fatal("updateTime should have been updated")
	}
	if trigger.ServiceAccount != "old-sa@project.iam.gserviceaccount.com" {
		t.Fatal("immutable serviceAccount should not have changed")
	}
}

func TestPatchTriggerLabelsForProviderImportReconciliation(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us-central1/triggers/t1"
	api.triggers[name] = &Trigger{
		Name:         name,
		UID:          "uid-1",
		CreateTime:   "2024-01-01T00:00:00Z",
		EventFilters: []EventFilter{{Attribute: "type", Value: "test"}},
		Destination:  &Destination{Workflow: "projects/test/locations/us-central1/workflows/w"},
	}

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/"+name+"?updateMask=labels",
		bytes.NewBufferString(`{"labels":{"goog-terraform-provisioned":"true"}}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.triggers[name].Labels["goog-terraform-provisioned"]; got != "true" {
		t.Fatalf("provider label = %q, want true", got)
	}
}

func TestPatchTriggerAllMutableFieldsForWildcardAndEmptyMasksPersist(t *testing.T) {
	for _, test := range []struct {
		name string
		mask string
	}{
		{name: "wildcard", mask: "*"},
		{name: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &mockStore{data: make(map[string][]byte)}
			api := newTestAPI()
			api.stateStore = store
			name := "projects/test/locations/us-central1/triggers/t1"
			api.triggers[name] = &Trigger{
				Name:         name,
				UID:          "uid-1",
				CreateTime:   "2024-01-01T00:00:00Z",
				EventFilters: []EventFilter{{Attribute: "type", Value: "test"}},
				Destination:  &Destination{Workflow: "projects/test/locations/us-central1/workflows/old"},
				Labels:       map[string]string{"old": "label"},
			}

			path := "/v1/" + name
			if test.mask != "" {
				path += "?updateMask=" + test.mask
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, path,
				bytes.NewBufferString(`{"destination":{"workflow":"projects/test/locations/us-central1/workflows/new"},"labels":{"new":"label"}}`)))
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			updated := api.triggers[name]
			if updated.Destination == nil ||
				updated.Destination.Workflow != "projects/test/locations/us-central1/workflows/new" {
				t.Fatalf("destination = %#v", updated.Destination)
			}
			if len(updated.Labels) != 1 || updated.Labels["new"] != "label" {
				t.Fatalf("labels = %#v", updated.Labels)
			}

			restarted := newTestAPI()
			restarted.stateStore = store
			if err := restarted.loadState(); err != nil {
				t.Fatal(err)
			}
			reloaded := restarted.triggers[name]
			if reloaded == nil || reloaded.Destination == nil ||
				reloaded.Destination.Workflow != "projects/test/locations/us-central1/workflows/new" ||
				len(reloaded.Labels) != 1 || reloaded.Labels["new"] != "label" {
				t.Fatalf("reloaded trigger = %#v", reloaded)
			}
		})
	}
}

func TestPatchTriggerRejectsImmutableFieldMaskWithoutMutation(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us-central1/triggers/t1"
	api.triggers[name] = &Trigger{
		Name: name, UID: "uid", EventFilters: []EventFilter{{Attribute: "type", Value: "test"}},
		Destination:    &Destination{Workflow: "projects/test/locations/us-central1/workflows/w"},
		ServiceAccount: "old@test.iam.gserviceaccount.com",
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/"+name+"?updateMask=serviceAccount",
		bytes.NewBufferString(`{"serviceAccount":"new@test.iam.gserviceaccount.com"}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.triggers[name].ServiceAccount; got != "old@test.iam.gserviceaccount.com" {
		t.Fatalf("immutable field changed to %q", got)
	}
}

func TestPatchTriggerValidateOnlyReturnsTypedPreviewWithoutMutation(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us-central1/triggers/t1"
	api.triggers[name] = &Trigger{
		Name: name, UID: "uid", EventFilters: []EventFilter{{Attribute: "type", Value: "test"}},
		Destination: &Destination{Workflow: "projects/test/locations/us-central1/workflows/old"},
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/"+name+"?updateMask=destination&validateOnly=true",
		bytes.NewBufferString(`{"destination":{"workflow":"projects/test/locations/us-central1/workflows/new"}}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	var operation map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	result, _ := operation["response"].(map[string]any)
	destination, _ := result["destination"].(map[string]any)
	if operation["done"] != true ||
		result["@type"] != "type.googleapis.com/google.cloud.eventarc.v1.Trigger" ||
		destination["workflow"] != "projects/test/locations/us-central1/workflows/new" {
		t.Fatalf("preview operation = %#v", operation)
	}
	if got := api.triggers[name].Destination.Workflow; got != "projects/test/locations/us-central1/workflows/old" {
		t.Fatalf("validateOnly mutated destination to %q", got)
	}
}

func TestCreateTriggerTerminalOperationContainsTypedTrigger(t *testing.T) {
	api := newTestAPI()
	body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"workflow":"projects/test/locations/us-central1/workflows/w"}}`
	create := httptest.NewRecorder()
	api.ServeHTTP(create, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us-central1/triggers?triggerId=typed",
		bytes.NewBufferString(body)))
	var initial map[string]any
	if err := json.Unmarshal(create.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	operationName, _ := initial["name"].(string)
	deadline := time.Now().Add(2 * time.Second)
	for {
		poll := httptest.NewRecorder()
		api.ServeHTTP(poll, httptest.NewRequest(http.MethodGet, "/v1/"+operationName, nil))
		var operation map[string]any
		if err := json.Unmarshal(poll.Body.Bytes(), &operation); err != nil {
			t.Fatal(err)
		}
		if operation["done"] == true {
			result, _ := operation["response"].(map[string]any)
			if result["@type"] != "type.googleapis.com/google.cloud.eventarc.v1.Trigger" ||
				result["name"] != "projects/test/locations/us-central1/triggers/typed" {
				t.Fatalf("terminal response = %#v", result)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation did not finish: %#v", operation)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPatchTriggerCompensatesPostCommitSaveAndOperation(t *testing.T) {
	store := &postCommitEventarcStore{data: make(map[string][]byte)}
	api := newTestAPI()
	api.stateStore = store
	name := "projects/test/locations/us-central1/triggers/t1"
	api.triggers[name] = &Trigger{
		Name: name, UID: "uid", EventFilters: []EventFilter{{Attribute: "type", Value: "test"}},
		Destination: &Destination{Workflow: "projects/test/locations/us-central1/workflows/w"},
	}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.failNext = true

	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch,
		"/v1/"+name+"?updateMask=destination",
		bytes.NewBufferString(`{"destination":{"workflow":"projects/test/locations/us-central1/workflows/new"}}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if got := api.triggers[name].Destination.Workflow; got != "projects/test/locations/us-central1/workflows/w" {
		t.Fatalf("visible destination = %q, want old", got)
	}
	var durable eventarcMetadata
	if err := store.Load(eventarcStateEntry, &durable); err != nil {
		t.Fatal(err)
	}
	if got := durable.Triggers[name].Destination.Workflow; got != "projects/test/locations/us-central1/workflows/w" {
		t.Fatalf("durable destination = %q, want compensated old", got)
	}
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("compensated mutation retained operations: %#v", operations)
	}
	if api.opMgr.PersistenceError() == nil {
		t.Fatal("ambiguous save error did not leave sticky degradation")
	}
}

func TestPatchTriggerNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"serviceAccount":"new@test.iam.gserviceaccount.com"}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/triggers/missing?updateMask=serviceAccount", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	// Create a trigger to generate an operation
	body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"workflow":"projects/test/locations/us-central1/workflows/w"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId=op-test", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed: %d: %s", w.Code, w.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	opPath := createResp["name"].(string)

	// Get the operation
	req = httptest.NewRequest(http.MethodGet, "/v1/"+opPath, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var opResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &opResp)
	meta := opResp["metadata"].(map[string]any)
	if meta["verb"] != "create" {
		t.Fatalf("expected verb=create, got %v", meta["verb"])
	}
	if meta["target"] != "projects/test/locations/us-central1/triggers/op-test" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}
}

func TestGetOperationNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/operations/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperationRejectsDifferentProjectScope(t *testing.T) {
	api := newTestAPI()
	op, err := api.opMgr.RegisterScopedTargetDurable(
		"eventarc#operation",
		"create",
		"projects/p1/locations/l/triggers/t",
	)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/p2/locations/l/operations/"+extractAfter(op.Name, "operations"), nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPersistAndReload(t *testing.T) {
	// Use a mock store to verify persist/load cycle
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		triggers:   make(map[string]*Trigger),
	}

	// Create a trigger
	api.mu.Lock()
	api.triggers["projects/p/locations/l/triggers/t1"] = &Trigger{
		Name:       "projects/p/locations/l/triggers/t1",
		UID:        "uid-persist",
		CreateTime: "2024-06-01T00:00:00Z",
		UpdateTime: "2024-06-01T00:00:00Z",
		EventFilters: []EventFilter{
			{Attribute: "type", Value: "test.event"},
		},
		Destination: &Destination{
			Workflow: "projects/p/locations/l/workflows/w",
		},
	}
	api.mu.Unlock()

	// Persist
	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Create a new API and reload
	api2 := &API{
		opMgr:      newTestAPI().opMgr,
		stateStore: store,
		triggers:   make(map[string]*Trigger),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	trigger, ok := api2.triggers["projects/p/locations/l/triggers/t1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("trigger not found after reload")
	}
	if trigger.UID != "uid-persist" {
		t.Fatalf("expected uid-persist, got %s", trigger.UID)
	}
	if trigger.Destination == nil || trigger.Destination.Workflow == "" {
		t.Fatal("destination lost after reload")
	}
}

func TestConcurrentCreateAndGet(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	// Concurrent creates
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := `{"eventFilters":[{"attribute":"type","value":"test"}],"destination":{"workflow":"projects/test/locations/us-central1/workflows/w"}}`
			req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/triggers?triggerId="+string(rune('a'+idx%26))+"-"+itoa(idx), bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			// Either 200 (created) or 409 (duplicate from collision) is acceptable
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("unexpected status %d for create %d", w.Code, idx)
			}
		}(i)
	}

	// Concurrent gets
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/triggers", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

func itoa(i int) string {
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}

func waitForDeliveries(t *testing.T, api *API, stateName string, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		api.mu.RLock()
		matches := 0
		for _, delivery := range api.deliveries {
			if delivery != nil && delivery.State == stateName {
				matches++
			}
		}
		api.mu.RUnlock()
		if matches == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d %s deliveries", count, stateName)
}

func waitForPersistedDeliveries(t *testing.T, store *controllableStore, stateName string, count int) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw := store.snapshot(eventarcStateEntry)
		var metadata eventarcMetadata
		if json.Unmarshal(raw, &metadata) == nil {
			matches := 0
			for _, delivery := range metadata.Deliveries {
				if delivery != nil && delivery.State == stateName {
					matches++
				}
			}
			if matches == count {
				return raw
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for persisted %s deliveries", stateName)
	return nil
}

func waitForStoreDeliveries(t *testing.T, store eventarcStateStore, stateName string, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var metadata eventarcMetadata
		if store.Load(eventarcStateEntry, &metadata) == nil {
			matches := 0
			for _, delivery := range metadata.Deliveries {
				if delivery != nil && delivery.State == stateName {
					matches++
				}
			}
			if matches == count {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d persisted %s deliveries", count, stateName)
}

// mockStore is a simple in-memory state store for testing.
type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

type controllableStore struct {
	mu   sync.Mutex
	data map[string][]byte
	fail bool
}

type postCommitEventarcStore struct {
	mu       sync.Mutex
	data     map[string][]byte
	failNext bool
}

func (s *postCommitEventarcStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw := s.data[name]
	if len(raw) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *postCommitEventarcStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data[name] = raw
	if s.failNext {
		s.failNext = false
		return errors.New("post-commit save error")
	}
	return nil
}

func (s *controllableStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.data[name]
	if !ok {
		return state.ErrNotFound
	}
	return json.Unmarshal(raw, target)
}

func (s *controllableStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("injected save failure")
	}
	raw, err := json.Marshal(value)
	if err == nil {
		s.data[name] = raw
	}
	return err
}

func (s *controllableStore) snapshot(name string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data[name]...)
}

func (s *controllableStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return fmt.Errorf("not found: %w", state.ErrNotFound)
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
