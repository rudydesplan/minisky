package pubsub

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type recordingObserver struct {
	mu     sync.Mutex
	events []recordedEvent
}

type failingAcknowledgedObserver struct{}

func (failingAcknowledgedObserver) HandleEvent(string, string, string) {}
func (failingAcknowledgedObserver) HandleEventWithAck(string, string, string) error {
	return errors.New("intent save failed")
}

type recordedEvent struct {
	eventType string
	resource  string
	payload   string
}

func (o *recordingObserver) HandleEvent(eventType, resource, payload string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, recordedEvent{eventType, resource, payload})
}

func TestPublishNotifiesAllObserversOnceAndPreservesPayload(t *testing.T) {
	const payload = `{"messages":[{"data":"aGVsbG8="}]}`
	var proxiedBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		proxiedBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	first := &recordingObserver{}
	second := &recordingObserver{}
	api := NewAPI(nil)
	api.AddObserver(first)
	api.SetObserver(second)
	api.AddObserver(first)

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/topics/orders:publish", strings.NewReader(payload))
	rec := httptest.NewRecorder()
	api.handlePublish(rec, req, upstream.URL)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if proxiedBody != payload {
		t.Fatalf("proxied payload = %q, want %q", proxiedBody, payload)
	}
	for name, observer := range map[string]*recordingObserver{"first": first, "second": second} {
		observer.mu.Lock()
		events := append([]recordedEvent(nil), observer.events...)
		observer.mu.Unlock()
		if len(events) != 1 {
			t.Fatalf("%s observer got %d events, want 1", name, len(events))
		}
		got := events[0]
		if got.eventType != "google.cloud.pubsub.topic.v1.messagePublished" ||
			got.resource != "projects/test/topics/orders" || got.payload != payload {
			t.Fatalf("%s observer event = %#v", name, got)
		}
	}
}

func TestPublishDoesNotAcknowledgeObserverPersistenceFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	api := NewAPI(nil)
	api.AddObserver(failingAcknowledgedObserver{})
	response := httptest.NewRecorder()
	api.handlePublish(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/topics/orders:publish", strings.NewReader(`{"messages":[]}`)), upstream.URL)
	if response.Code < 500 {
		t.Fatalf("status = %d, want retryable failure", response.Code)
	}
}
