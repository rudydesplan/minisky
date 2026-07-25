package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/api/pubsub/v1"
)

const (
	pubsubTopicEnv        = "MINISKY_PUBSUB_PRIMARY_TOPIC"
	pubsubSubscriptionEnv = "MINISKY_PUBSUB_SECONDARY_SUBSCRIPTION"
)

type pubsubSmokeConfig struct {
	topic        string
	subscription string
}

func pubsubSmokeConfigFromEnvironment(getenv func(string) string) (pubsubSmokeConfig, bool, error) {
	topic := strings.TrimSpace(getenv(pubsubTopicEnv))
	subscription := strings.TrimSpace(getenv(pubsubSubscriptionEnv))
	if topic == "" && subscription == "" {
		return pubsubSmokeConfig{}, false, nil
	}
	if topic == "" || subscription == "" {
		return pubsubSmokeConfig{}, false, fmt.Errorf(
			"%s and %s must be set together", pubsubTopicEnv, pubsubSubscriptionEnv)
	}
	topicProject, valid := canonicalPubSubResourceProject(topic, "topics")
	if !valid {
		return pubsubSmokeConfig{}, false, fmt.Errorf(
			"%s must use projects/{project}/topics/{topic}", pubsubTopicEnv)
	}
	subscriptionProject, valid := canonicalPubSubResourceProject(subscription, "subscriptions")
	if !valid {
		return pubsubSmokeConfig{}, false, fmt.Errorf(
			"%s must use projects/{project}/subscriptions/{subscription}", pubsubSubscriptionEnv)
	}
	if topicProject == subscriptionProject {
		return pubsubSmokeConfig{}, false, fmt.Errorf(
			"%s and %s must identify different projects", pubsubTopicEnv, pubsubSubscriptionEnv)
	}
	return pubsubSmokeConfig{topic: topic, subscription: subscription}, true, nil
}

func canonicalPubSubResourceProject(name, collection string) (string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[1] == "" ||
		parts[2] != collection || parts[3] == "" {
		return "", false
	}
	return parts[1], true
}

func matchesPubSubPayload(message *pubsub.PubsubMessage, data []byte, attributes map[string]string) bool {
	if message == nil ||
		message.Data != base64.StdEncoding.EncodeToString(data) ||
		len(message.Attributes) != len(attributes) {
		return false
	}
	for key, value := range attributes {
		if message.Attributes[key] != value {
			return false
		}
	}
	return true
}

func matchingPubSubAckIDs(
	receivedMessages []*pubsub.ReceivedMessage,
	data []byte,
	attributes map[string]string,
) []string {
	ackIDs := make([]string, 0, len(receivedMessages))
	for _, received := range receivedMessages {
		if received == nil || received.AckId == "" ||
			!matchesPubSubPayload(received.Message, data, attributes) {
			continue
		}
		ackIDs = append(ackIDs, received.AckId)
	}
	return ackIDs
}

func runOptionalPubSubSmoke(
	ctx context.Context,
	gateway string,
	getenv func(string) string,
	output io.Writer,
) error {
	config, configured, err := pubsubSmokeConfigFromEnvironment(getenv)
	if err != nil {
		return err
	}
	if !configured {
		fmt.Fprintf(output, "Go SDK smoke: optional Pub/Sub section skipped (%s and %s are unset)\n",
			pubsubTopicEnv, pubsubSubscriptionEnv)
		return nil
	}

	service, err := pubsub.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(gateway+"/_minisky/pubsub/"),
	)
	if err != nil {
		return fmt.Errorf("create Pub/Sub client: %w", err)
	}
	topic, err := service.Projects.Topics.Get(config.topic).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get primary Pub/Sub topic: %w", err)
	}
	if topic.Name != config.topic {
		return fmt.Errorf("primary Pub/Sub topic name = %q, want %q", topic.Name, config.topic)
	}
	subscription, err := service.Projects.Subscriptions.Get(config.subscription).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get secondary Pub/Sub subscription: %w", err)
	}
	if subscription.Name != config.subscription {
		return fmt.Errorf("secondary Pub/Sub subscription name = %q, want %q",
			subscription.Name, config.subscription)
	}
	if subscription.Topic != config.topic {
		return fmt.Errorf("secondary Pub/Sub subscription topic = %q, want %q",
			subscription.Topic, config.topic)
	}

	uniqueID := fmt.Sprintf("minisky-go-sdk-%d", time.Now().UnixNano())
	data := []byte("phase-14-cross-project-" + uniqueID)
	attributes := map[string]string{
		"minisky-smoke-id": uniqueID,
		"source":           "go-sdk-smoke",
	}
	published, err := service.Projects.Topics.Publish(config.topic, &pubsub.PublishRequest{
		Messages: []*pubsub.PubsubMessage{{
			Data:       base64.StdEncoding.EncodeToString(data),
			Attributes: attributes,
		}},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("publish cross-project Pub/Sub message: %w", err)
	}
	if len(published.MessageIds) != 1 || published.MessageIds[0] == "" {
		return fmt.Errorf("publish returned message IDs %#v, want one non-empty ID", published.MessageIds)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		pulled, err := service.Projects.Subscriptions.Pull(config.subscription, &pubsub.PullRequest{
			MaxMessages:       10,
			ReturnImmediately: true,
		}).Context(pollCtx).Do()
		if err != nil {
			return fmt.Errorf("pull secondary Pub/Sub subscription: %w", err)
		}

		ackIDs := matchingPubSubAckIDs(pulled.ReceivedMessages, data, attributes)
		if len(ackIDs) > 0 {
			if _, err := service.Projects.Subscriptions.Acknowledge(
				config.subscription,
				&pubsub.AcknowledgeRequest{AckIds: ackIDs},
			).Context(pollCtx).Do(); err != nil {
				return fmt.Errorf("acknowledge secondary Pub/Sub messages: %w", err)
			}
			fmt.Fprintf(output, "Go SDK Pub/Sub smoke passed: topic=%s subscription=%s\n",
				config.topic, config.subscription)
			return nil
		}

		select {
		case <-pollCtx.Done():
			return fmt.Errorf("timed out waiting for exact Pub/Sub message: %w", pollCtx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
