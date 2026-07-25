package main

import (
	"encoding/base64"
	"testing"

	"google.golang.org/api/pubsub/v1"
)

func TestPubSubSmokeConfigFromEnvironment(t *testing.T) {
	for _, test := range []struct {
		name             string
		values           map[string]string
		wantConfigured   bool
		wantErr          bool
		wantTopic        string
		wantSubscription string
	}{
		{name: "optional section absent"},
		{
			name: "canonical cross project resources",
			values: map[string]string{
				pubsubTopicEnv:        "projects/primary-project/topics/events",
				pubsubSubscriptionEnv: "projects/secondary-project/subscriptions/events",
			},
			wantConfigured:   true,
			wantTopic:        "projects/primary-project/topics/events",
			wantSubscription: "projects/secondary-project/subscriptions/events",
		},
		{
			name: "topic alone is rejected",
			values: map[string]string{
				pubsubTopicEnv: "projects/primary-project/topics/events",
			},
			wantErr: true,
		},
		{
			name: "relative topic is rejected",
			values: map[string]string{
				pubsubTopicEnv:        "topics/events",
				pubsubSubscriptionEnv: "projects/secondary-project/subscriptions/events",
			},
			wantErr: true,
		},
		{
			name: "malformed subscription is rejected",
			values: map[string]string{
				pubsubTopicEnv:        "projects/primary-project/topics/events",
				pubsubSubscriptionEnv: "projects/secondary-project/subscriptions",
			},
			wantErr: true,
		},
		{
			name: "same project fixtures are rejected",
			values: map[string]string{
				pubsubTopicEnv:        "projects/primary-project/topics/events",
				pubsubSubscriptionEnv: "projects/primary-project/subscriptions/events",
			},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, configured, err := pubsubSmokeConfigFromEnvironment(func(key string) string {
				return test.values[key]
			})
			if (err != nil) != test.wantErr {
				t.Fatalf("error=%v wantErr=%t", err, test.wantErr)
			}
			if configured != test.wantConfigured {
				t.Fatalf("configured=%t want=%t", configured, test.wantConfigured)
			}
			if config.topic != test.wantTopic || config.subscription != test.wantSubscription {
				t.Fatalf("config=%#v want topic=%q subscription=%q",
					config, test.wantTopic, test.wantSubscription)
			}
		})
	}
}

func TestMatchesPubSubPayloadExactly(t *testing.T) {
	data := []byte("phase-14-payload")
	attributes := map[string]string{"source": "go-sdk-smoke", "id": "unique-id"}
	exact := &pubsub.PubsubMessage{
		Data:       base64.StdEncoding.EncodeToString(data),
		Attributes: map[string]string{"id": "unique-id", "source": "go-sdk-smoke"},
	}
	if !matchesPubSubPayload(exact, data, attributes) {
		t.Fatal("exact payload did not match")
	}

	for _, test := range []struct {
		name    string
		message *pubsub.PubsubMessage
	}{
		{name: "nil message"},
		{
			name: "different data",
			message: &pubsub.PubsubMessage{
				Data:       base64.StdEncoding.EncodeToString([]byte("different")),
				Attributes: attributes,
			},
		},
		{
			name: "missing attribute",
			message: &pubsub.PubsubMessage{
				Data:       exact.Data,
				Attributes: map[string]string{"id": "unique-id"},
			},
		},
		{
			name: "extra attribute",
			message: &pubsub.PubsubMessage{
				Data: exact.Data,
				Attributes: map[string]string{
					"id": "unique-id", "source": "go-sdk-smoke", "extra": "not-exact",
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if matchesPubSubPayload(test.message, data, attributes) {
				t.Fatal("non-exact payload matched")
			}
		})
	}
}

func TestMatchingPubSubAckIDsOnlySelectsExactSmokeMessages(t *testing.T) {
	data := []byte("phase-14-payload-unique-id")
	attributes := map[string]string{
		"minisky-smoke-id": "unique-id",
		"source":           "go-sdk-smoke",
	}
	exactMessage := func() *pubsub.PubsubMessage {
		return &pubsub.PubsubMessage{
			Data:       base64.StdEncoding.EncodeToString(data),
			Attributes: map[string]string{"source": "go-sdk-smoke", "minisky-smoke-id": "unique-id"},
		}
	}
	received := []*pubsub.ReceivedMessage{
		nil,
		{AckId: "nil-message", Message: nil},
		{
			AckId: "unrelated-data",
			Message: &pubsub.PubsubMessage{
				Data:       base64.StdEncoding.EncodeToString([]byte("unrelated")),
				Attributes: attributes,
			},
		},
		{
			AckId: "unrelated-attributes",
			Message: &pubsub.PubsubMessage{
				Data: base64.StdEncoding.EncodeToString(data),
				Attributes: map[string]string{
					"minisky-smoke-id": "different-id",
					"source":           "go-sdk-smoke",
				},
			},
		},
		{
			AckId:   "",
			Message: exactMessage(),
		},
		{
			AckId:   "matching-one",
			Message: exactMessage(),
		},
		{
			AckId:   "matching-two",
			Message: exactMessage(),
		},
	}

	got := matchingPubSubAckIDs(received, data, attributes)
	if len(got) != 2 || got[0] != "matching-one" || got[1] != "matching-two" {
		t.Fatalf("matching ack IDs = %#v, want only exact smoke message IDs", got)
	}
}
