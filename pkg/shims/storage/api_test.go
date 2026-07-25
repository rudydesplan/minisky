package storage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type recordingObserver struct {
	mu     sync.Mutex
	events []recordedEvent
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

func TestStorageEventsNotifyAllObserversOnceWithPayload(t *testing.T) {
	first := &recordingObserver{}
	second := &recordingObserver{}
	api := NewAPI(nil)
	api.AddObserver(first)
	api.SetObserver(second)
	api.AddObserver(first)

	req := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/photos/o?name=summer.jpg", nil)
	api.handlePotentialEvent(req, &http.Response{StatusCode: http.StatusOK})

	for name, observer := range map[string]*recordingObserver{"first": first, "second": second} {
		observer.mu.Lock()
		events := append([]recordedEvent(nil), observer.events...)
		observer.mu.Unlock()
		if len(events) != 1 {
			t.Fatalf("%s observer got %d events, want 1", name, len(events))
		}
		got := events[0]
		if got.eventType != "google.storage.object.finalize" || got.resource != "photos" {
			t.Fatalf("%s observer event = %#v", name, got)
		}
		var data map[string]string
		if err := json.Unmarshal([]byte(got.payload), &data); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if data["bucket"] != "photos" || data["name"] != "summer.jpg" {
			t.Fatalf("payload = %#v", data)
		}
	}
}

func TestStorageNonEventRequestDoesNotNotify(t *testing.T) {
	observer := &recordingObserver{}
	api := NewAPI(nil)
	api.AddObserver(observer)

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b/photos/o", nil)
	api.handlePotentialEvent(req, &http.Response{StatusCode: http.StatusOK})

	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.events) != 0 {
		t.Fatalf("got %d events, want none", len(observer.events))
	}
}
