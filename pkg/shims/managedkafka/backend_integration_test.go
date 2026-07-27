package managedkafka

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDockerKafkaProducerConsumerIntegration(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_PHASE19_KAFKA") != "1" {
		t.Skip("set MINISKY_DOCKER_PHASE19_KAFKA=1 to run the pinned Kafka integration")
	}
	t.Setenv("MINISKY_PROFILE", "phase19-kafka-integration")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	backend := newDockerKafkaBackend()
	resource := fmt.Sprintf("projects/p/locations/l/clusters/integration-%d", time.Now().UnixNano())
	if _, err := backend.Provision(ctx, resource); err != nil {
		t.Fatal(err)
	}
	volumeOutput, err := backend.runner.Run(ctx, nil, "inspect", "--format",
		`{{range .Mounts}}{{if eq .Type "volume"}}{{println .Name}}{{end}}{{end}}`,
		kafkaContainerName(resource))
	if err != nil {
		t.Fatalf("inspect Kafka anonymous volumes: %v: %s", err, volumeOutput)
	}
	volumes := strings.Fields(string(volumeOutput))
	if len(volumes) == 0 {
		t.Fatal("Kafka container did not create its expected anonymous data volume")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := backend.Delete(cleanupCtx, resource); err != nil {
			t.Errorf("cleanup Kafka backend: %v", err)
		}
	})
	topic := &Topic{Name: resource + "/topics/events", PartitionCount: 1, ReplicationFactor: 1}
	if err := backend.CreateTopic(ctx, resource, topic); err != nil {
		t.Fatal(err)
	}
	if err := backend.Produce(ctx, resource, topic.Name, []string{"first", "second"}); err != nil {
		t.Fatal(err)
	}
	offsets, err := backend.runner.Run(ctx, nil, "exec", kafkaContainerName(resource),
		"/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--topic", "events")
	if err != nil {
		t.Fatalf("read topic offsets: %v: %s", err, offsets)
	}
	t.Logf("topic offsets after produce: %s", offsets)
	messages, err := backend.Consume(ctx, resource, topic.Name, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0] != "first" || messages[1] != "second" {
		t.Fatalf("unexpected messages: %v", messages)
	}
	if err := backend.Delete(ctx, resource); err != nil {
		t.Fatal(err)
	}
	for _, volume := range volumes {
		output, err := backend.runner.Run(ctx, nil, "volume", "inspect", volume)
		if err == nil || !strings.Contains(strings.ToLower(string(output)), "no such volume") {
			t.Fatalf("Kafka cleanup left anonymous volume %q: error=%v output=%s", volume, err, output)
		}
	}
}
