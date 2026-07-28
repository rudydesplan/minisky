package managedkafka

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
)

const kafkaImage = "apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f"
const kafkaDockerService = "managed-kafka"

type kafkaCommandRunner interface {
	Run(context.Context, []byte, ...string) ([]byte, error)
}

type kafkaDockerCLI struct{}

func (kafkaDockerCLI) Run(ctx context.Context, input []byte, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "docker", args...)
	command.Stdin = bytes.NewReader(input)
	return command.CombinedOutput()
}

type dockerKafkaBackend struct {
	runner kafkaCommandRunner
}

func newDockerKafkaBackend() *dockerKafkaBackend {
	return &dockerKafkaBackend{runner: kafkaDockerCLI{}}
}

func kafkaContainerName(resource string) string {
	sum := sha256.Sum256([]byte(config.GetProfile() + "\x00" + resource))
	return "minisky-kafka-" + hex.EncodeToString(sum[:6])
}

func (b *dockerKafkaBackend) Provision(parent context.Context, resource string) (endpoint string, resultErr error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	name := kafkaContainerName(resource)
	created := false
	defer func() {
		if resultErr != nil && created {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = b.runner.Run(cleanupCtx, nil, "rm", "-f", "-v", name)
		}
	}()
	exists, err := b.requireOwned(ctx, name, resource)
	if err != nil {
		return "", err
	}
	if !exists {
		port, err := reservePort()
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256([]byte(resource))
		clusterID := base64.RawURLEncoding.EncodeToString(sum[:16])
		labels := kafkaDockerLabels(resource)
		output, runErr := b.runner.Run(ctx, nil, "run", "-d", "--name", name,
			"--label", "managed-by="+labels["managed-by"],
			"--label", "minisky.profile="+labels["minisky.profile"],
			"--label", "minisky.service="+labels["minisky.service"],
			"--label", "minisky.resource="+labels["minisky.resource"],
			"-p", "127.0.0.1:"+strconv.Itoa(port)+":9094",
			"-e", "CLUSTER_ID="+clusterID,
			"-e", "KAFKA_NODE_ID=1",
			"-e", "KAFKA_PROCESS_ROLES=broker,controller",
			"-e", "KAFKA_LISTENERS=INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093",
			"-e", "KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:9092,EXTERNAL://127.0.0.1:"+strconv.Itoa(port),
			"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"-e", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
			"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
			kafkaImage)
		if runErr != nil {
			return "", fmt.Errorf("start pinned Kafka container: %w: %s", runErr, strings.TrimSpace(string(output)))
		}
		created = true
	}
	for {
		if _, commandErr := b.runner.Run(ctx, nil, "exec", name, "/opt/kafka/bin/kafka-topics.sh",
			"--bootstrap-server", "localhost:9092", "--list"); commandErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for Kafka readiness: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	portOutput, err := b.runner.Run(ctx, nil, "port", name, "9094/tcp")
	if err != nil {
		return "", fmt.Errorf("discover Kafka port: %w", err)
	}
	address := strings.TrimSpace(string(portOutput))
	if index := strings.LastIndex(address, ":"); index >= 0 {
		address = "127.0.0.1:" + address[index+1:]
	}
	return address, nil
}

func (b *dockerKafkaBackend) Delete(parent context.Context, resource string) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	name := kafkaContainerName(resource)
	exists, err := b.requireOwned(ctx, name, resource)
	if err != nil || !exists {
		return err
	}
	output, err := b.runner.Run(ctx, nil, "rm", "-f", "-v", name)
	if err != nil {
		return fmt.Errorf("remove Kafka container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b *dockerKafkaBackend) CreateTopic(ctx context.Context, resource string, topic *Topic) error {
	args := []string{"exec", kafkaContainerName(resource), "/opt/kafka/bin/kafka-topics.sh",
		"--bootstrap-server", "localhost:9092", "--create", "--topic", topicID(topic.Name),
		"--partitions", strconv.Itoa(topic.PartitionCount), "--replication-factor", strconv.Itoa(topic.ReplicationFactor)}
	output, err := b.runner.Run(ctx, nil, args...)
	if err != nil {
		return fmt.Errorf("create Kafka topic: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b *dockerKafkaBackend) UpdateTopic(ctx context.Context, resource string, topic *Topic) error {
	output, err := b.runner.Run(ctx, nil, "exec", kafkaContainerName(resource), "/opt/kafka/bin/kafka-topics.sh",
		"--bootstrap-server", "localhost:9092", "--alter", "--topic", topicID(topic.Name),
		"--partitions", strconv.Itoa(topic.PartitionCount))
	if err != nil {
		return fmt.Errorf("update Kafka topic: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b *dockerKafkaBackend) DeleteTopic(ctx context.Context, resource, topicName string) error {
	output, err := b.runner.Run(ctx, nil, "exec", kafkaContainerName(resource), "/opt/kafka/bin/kafka-topics.sh",
		"--bootstrap-server", "localhost:9092", "--delete", "--topic", topicID(topicName))
	if err != nil {
		return fmt.Errorf("delete Kafka topic: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b *dockerKafkaBackend) Produce(ctx context.Context, resource, topicName string, messages []string) error {
	input := []byte(strings.Join(messages, "\n") + "\n")
	output, err := b.runner.Run(ctx, input, "exec", "-i", kafkaContainerName(resource),
		"/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092",
		"--topic", topicID(topicName))
	if err != nil {
		return fmt.Errorf("produce Kafka records: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b *dockerKafkaBackend) Consume(ctx context.Context, resource, topicName string, count int) ([]string, error) {
	output, err := b.runner.Run(ctx, nil, "exec", kafkaContainerName(resource),
		"/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092",
		"--topic", topicID(topicName), "--partition", "0", "--offset", "earliest", "--max-messages", strconv.Itoa(count),
		"--timeout-ms", "10000")
	if err != nil {
		return nil, fmt.Errorf("consume Kafka records: %w: %s", err, strings.TrimSpace(string(output)))
	}
	text := strings.TrimSpace(string(output))
	if text == "" {
		return []string{}, nil
	}
	lines := strings.Split(text, "\n")
	messages := make([]string, 0, count)
	for _, line := range lines {
		if strings.HasPrefix(line, "Processed a total of ") {
			continue
		}
		messages = append(messages, line)
	}
	if len(messages) != count {
		return nil, fmt.Errorf("consume Kafka records: expected %d messages, got %d: %s", count, len(messages), text)
	}
	return messages, nil
}

func (b *dockerKafkaBackend) requireOwned(ctx context.Context, name, resource string) (bool, error) {
	output, err := b.runner.Run(ctx, nil, "inspect", name)
	if err != nil {
		if dockerInspectNotFound(output) {
			return false, nil
		}
		return false, fmt.Errorf("inspect Kafka container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	var inspected []struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := json.Unmarshal(output, &inspected); err != nil || len(inspected) != 1 {
		return false, errors.New("decode Kafka container ownership")
	}
	labels := inspected[0].Config.Labels
	for key, value := range kafkaDockerLabels(resource) {
		if labels[key] != value {
			return false, fmt.Errorf("container %q exists but is not owned by this cluster", name)
		}
	}
	return true, nil
}

func kafkaDockerLabels(resource string) map[string]string {
	labels := orchestrator.DockerOwnershipLabels()
	labels["minisky.service"] = kafkaDockerService
	labels["minisky.resource"] = resource
	return labels
}

func dockerInspectNotFound(output []byte) bool {
	message := strings.ToLower(string(output))
	return strings.Contains(message, "no such object:") ||
		strings.Contains(message, "no such container:")
}

func reservePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func topicID(name string) string {
	parts := strings.Split(name, "/")
	return parts[len(parts)-1]
}
