package serverless

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTriggerMatchesEventTypeAndExactResource(t *testing.T) {
	tests := []struct {
		name      string
		trigger   *EventTrigger
		eventType string
		resource  string
		want      bool
	}{
		{
			name: "canonical pubsub topic",
			trigger: &EventTrigger{
				EventType:   "google.cloud.pubsub.topic.v1.messagePublished",
				PubsubTopic: "projects/test/topics/orders",
			},
			eventType: "google.cloud.pubsub.topic.v1.messagePublished",
			resource:  "orders",
			want:      true,
		},
		{
			name: "topic substring does not match",
			trigger: &EventTrigger{
				PubsubTopic: "projects/test/topics/orders-archive",
			},
			eventType: "google.cloud.pubsub.topic.v1.messagePublished",
			resource:  "orders",
		},
		{
			name: "different canonical topic does not match",
			trigger: &EventTrigger{
				PubsubTopic: "projects/first/topics/orders",
			},
			eventType: "google.cloud.pubsub.topic.v1.messagePublished",
			resource:  "projects/second/topics/orders",
		},
		{
			name: "wrong event type does not match",
			trigger: &EventTrigger{
				EventType:   "google.storage.object.finalize",
				PubsubTopic: "projects/test/topics/orders",
			},
			eventType: "google.cloud.pubsub.topic.v1.messagePublished",
			resource:  "orders",
		},
		{
			name: "canonical storage bucket",
			trigger: &EventTrigger{
				EventType: "google.storage.object.finalize",
				Resource:  "projects/_/buckets/photos",
			},
			eventType: "google.storage.object.finalize",
			resource:  "photos",
			want:      true,
		},
		{
			name: "eventarc trigger name is not source",
			trigger: &EventTrigger{
				Trigger: "photos",
			},
			eventType: "google.storage.object.finalize",
			resource:  "photos",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := triggerMatches(test.trigger, test.eventType, test.resource); got != test.want {
				t.Fatalf("triggerMatches() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestHandleEventDeliversPubSubAndStorageToFunctionAndService(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		resource  string
		payload   string
		trigger   *EventTrigger
	}{
		{
			name:      "pubsub",
			eventType: "google.cloud.pubsub.topic.v1.messagePublished",
			resource:  "orders",
			payload:   `{"messages":[{"data":"aGVsbG8="}]}`,
			trigger: &EventTrigger{
				EventType:   "google.cloud.pubsub.topic.v1.messagePublished",
				PubsubTopic: "projects/test/topics/orders",
			},
		},
		{
			name:      "storage",
			eventType: "google.storage.object.finalize",
			resource:  "photos",
			payload:   `{"bucket":"photos","name":"summer.jpg"}`,
			trigger: &EventTrigger{
				EventType: "google.storage.object.finalize",
				Resource:  "projects/_/buckets/photos",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deliveries := make(chan recordedDelivery, 3)
			target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				deliveries <- recordedDelivery{path: r.URL.Path, payload: string(body)}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer target.Close()

			api := &API{
				functions: map[string]*Function{
					"matching": {
						Name: "matching-function", State: "ACTIVE", Url: target.URL + "/function",
						EventTrigger: test.trigger,
					},
					"nonmatching": {
						Name: "other-function", State: "ACTIVE", Url: target.URL + "/wrong",
						EventTrigger: &EventTrigger{EventType: test.eventType, Resource: "other", PubsubTopic: "other"},
					},
				},
				services: map[string]*Service{
					"matching": {
						Name: "matching-service", Uri: target.URL + "/service",
						EventTrigger: test.trigger,
					},
					"reconciling": {
						Name: "not-ready-service", Uri: target.URL + "/not-ready", Reconciling: true,
						EventTrigger: test.trigger,
					},
				},
			}
			api.SetHTTPClient(target.Client())

			api.HandleEvent(test.eventType, test.resource, test.payload)

			got := map[string]string{}
			for len(got) < 2 {
				select {
				case delivery := <-deliveries:
					got[delivery.path] = delivery.payload
				case <-time.After(2 * time.Second):
					t.Fatalf("timed out waiting for deliveries; got %#v", got)
				}
			}
			for _, path := range []string{"/function", "/service"} {
				if got[path] != test.payload {
					t.Fatalf("%s payload = %q, want %q", path, got[path], test.payload)
				}
			}
			select {
			case delivery := <-deliveries:
				t.Fatalf("unexpected duplicate/nonmatching delivery: %#v", delivery)
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

type recordedDelivery struct {
	path    string
	payload string
}
