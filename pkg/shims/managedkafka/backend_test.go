package managedkafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingKafkaRunner struct {
	mu             sync.Mutex
	commands       [][]string
	inputs         [][]byte
	inspectErr     error
	inspectOutput  []byte
	exists         bool
	readyFailures  int
	readinessCalls int
}

func (r *recordingKafkaRunner) Run(_ context.Context, input []byte, args ...string) ([]byte, error) {
	r.mu.Lock()
	r.commands = append(r.commands, append([]string(nil), args...))
	r.inputs = append(r.inputs, append([]byte(nil), input...))
	r.mu.Unlock()
	switch args[0] {
	case "inspect":
		if r.inspectErr != nil {
			return r.inspectOutput, r.inspectErr
		}
		if r.exists {
			return []byte(`[{"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"kafka-backend-test","minisky.service":"managed-kafka","minisky.resource":"projects/p/locations/l/clusters/c"}}}]`), nil
		}
		return []byte("error: no such object: " + args[1]), errors.New("exit status 1")
	case "run":
		r.mu.Lock()
		r.exists = true
		r.mu.Unlock()
		return []byte("container"), nil
	case "port":
		return []byte("127.0.0.1:19092\n"), nil
	default:
		if strings.Contains(strings.Join(args, " "), "kafka-topics.sh") &&
			strings.Contains(strings.Join(args, " "), "--list") {
			r.mu.Lock()
			r.readinessCalls++
			calls := r.readinessCalls
			r.mu.Unlock()
			if calls <= r.readyFailures {
				return []byte("broker unavailable"), errors.New("exit status 1")
			}
		}
		if strings.Contains(strings.Join(args, " "), "kafka-console-consumer.sh") {
			return []byte("first\nsecond\n"), nil
		}
		return []byte("ok"), nil
	}
}

func TestPinnedKafkaBackendProvisionsProducesAndConsumes(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "kafka-backend-test")
	runner := &recordingKafkaRunner{}
	backend := &dockerKafkaBackend{runner: runner}
	resource := "projects/p/locations/l/clusters/c"
	topic := resource + "/topics/events"

	bootstrap, err := backend.Provision(context.Background(), resource)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap != "127.0.0.1:19092" {
		t.Fatalf("unexpected bootstrap address %q", bootstrap)
	}
	if err := backend.Produce(context.Background(), resource, topic, []string{"first", "second"}); err != nil {
		t.Fatal(err)
	}
	messages, err := backend.Consume(context.Background(), resource, topic, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0] != "first" || messages[1] != "second" {
		t.Fatalf("unexpected consumed messages: %v", messages)
	}

	joined := kafkaCommandsText(runner.commands)
	for _, required := range []string{
		kafkaImage,
		"managed-by=minisky",
		"minisky.profile=kafka-backend-test",
		"minisky.service=managed-kafka",
		"minisky.resource=" + resource,
		"kafka-console-producer.sh",
		"kafka-console-consumer.sh",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing command fragment %q in %s", required, joined)
		}
	}
	foundInput := false
	for _, input := range runner.inputs {
		if string(input) == "first\nsecond\n" {
			foundInput = true
		}
	}
	if !foundInput {
		t.Fatal("producer did not send records to stdin")
	}

	if err := backend.Delete(context.Background(), resource); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(kafkaCommandsText(runner.commands), "rm -f -v "+kafkaContainerName(resource)) {
		t.Fatalf("Kafka cleanup did not remove anonymous volumes: %s", kafkaCommandsText(runner.commands))
	}
}

func TestKafkaBackendPropagatesDockerInspectFailure(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "kafka-backend-test")
	daemonErr := errors.New("exit status 1")
	backend := &dockerKafkaBackend{runner: &recordingKafkaRunner{
		inspectErr:    daemonErr,
		inspectOutput: []byte("permission denied while trying to connect to the Docker daemon socket"),
	}}

	err := backend.Delete(context.Background(), "projects/p/locations/l/clusters/c")
	if !errors.Is(err, daemonErr) {
		t.Fatalf("Delete error = %v, want Docker inspect error", err)
	}
}

func TestKafkaReconcileDoesNotCreateMissingContainer(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "kafka-backend-test")
	runner := &recordingKafkaRunner{}
	backend := &dockerKafkaBackend{runner: runner}

	endpoint, ok, err := backend.Reconcile(context.Background(), "projects/p/locations/l/clusters/c")
	if err != nil {
		t.Fatal(err)
	}
	if ok || endpoint != "" {
		t.Fatalf("reconcile = (%q, %v), want missing backend", endpoint, ok)
	}
	if strings.Contains(kafkaCommandsText(runner.commands), "run ") {
		t.Fatalf("reconcile created a container: %s", kafkaCommandsText(runner.commands))
	}
}

func TestKafkaReconcileRetriesReadinessUntilSuccess(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "kafka-backend-test")
	runner := &recordingKafkaRunner{exists: true, readyFailures: 2}
	backend := &dockerKafkaBackend{runner: runner}

	endpoint, owned, err := backend.Reconcile(context.Background(), "projects/p/locations/l/clusters/c")
	if err != nil {
		t.Fatal(err)
	}
	if !owned || endpoint != "127.0.0.1:19092" || runner.readinessCalls != 3 {
		t.Fatalf("reconcile = (%q, %v), readiness calls=%d", endpoint, owned, runner.readinessCalls)
	}
}

func TestKafkaReconcileWrapsReadinessDeadline(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "kafka-backend-test")
	runner := &recordingKafkaRunner{exists: true, readyFailures: 1000}
	backend := &dockerKafkaBackend{runner: runner}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, _, err := backend.Reconcile(ctx, "projects/p/locations/l/clusters/c")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reconcile error = %v, want wrapped deadline", err)
	}
}

func TestKafkaTopicMutationRejectsUnownedContainer(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "kafka-backend-test")
	runner := &recordingKafkaRunner{exists: true}
	backend := &dockerKafkaBackend{runner: runner}
	resource := "projects/p/locations/l/clusters/other"
	topic := &Topic{Name: resource + "/topics/events", PartitionCount: 1, ReplicationFactor: 1}

	err := backend.CreateTopic(context.Background(), resource, topic)
	if err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("CreateTopic error = %v, want ownership rejection", err)
	}
	if strings.Contains(kafkaCommandsText(runner.commands), "--create") {
		t.Fatalf("unowned topic mutation executed: %s", kafkaCommandsText(runner.commands))
	}
}

func kafkaCommandsText(commands [][]string) string {
	var lines []string
	for _, command := range commands {
		lines = append(lines, strings.Join(command, " "))
	}
	return strings.Join(lines, "\n")
}
