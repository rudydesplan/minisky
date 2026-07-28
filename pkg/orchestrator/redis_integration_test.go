package orchestrator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"minisky/pkg/config"
)

func TestValkeyVolumeSurvivesOwnedContainerRestartAndCleanup(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_REDIS_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_REDIS_INTEGRATION=1 to run")
	}
	profile := fmt.Sprintf("redis-integration-%d", time.Now().UnixNano())
	t.Setenv("MINISKY_PROFILE", profile)
	manager, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := manager.EnsureNetwork(ctx); err != nil {
		t.Fatalf("collision-safe network setup failed: %v", err)
	}
	resourceID := "integration-" + profile
	t.Cleanup(func() {
		_ = manager.DeleteRedis(context.Background(), resourceID)
		manager.Teardown(context.Background())
	})

	valkeyImage := config.GetImageRegistry().Memorystore.Valkey.DefaultImage
	endpoint, err := manager.ProvisionRedis(ctx, resourceID, valkeyImage)
	if err != nil {
		t.Fatal(err)
	}
	if err := redisSet(endpoint, "minisky-restart", "persisted"); err != nil {
		t.Fatal(err)
	}

	containerName, _ := redisDockerNames(resourceID)
	status, labels, err := manager.inspectContainer(containerName)
	if err != nil || status != "running" || !isOwnedRedisResource(labels, resourceID) {
		t.Fatalf("owned Redis inspect status=%q labels=%v err=%v", status, labels, err)
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
		"http://localhost/containers/"+containerName+"?force=true", nil)
	response, err := manager.dockerClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("remove Redis container status = %d", response.StatusCode)
	}

	endpoint, err = manager.ProvisionRedis(ctx, resourceID, valkeyImage)
	if err != nil {
		t.Fatal(err)
	}
	value, err := redisGet(endpoint, "minisky-restart")
	if err != nil {
		t.Fatal(err)
	}
	if value != "persisted" {
		t.Fatalf("GET after restart = %q", value)
	}
	if err := manager.DeleteRedis(ctx, resourceID); err != nil {
		t.Fatalf("cleanup owned Valkey backend: %v", err)
	}
}

func redisSet(endpoint, key, value string) error {
	connection, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		return err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(connection, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value); err != nil {
		return err
	}
	line, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(line) != "+OK" {
		return fmt.Errorf("SET response %q", line)
	}
	return nil
}

func redisGet(endpoint, key string) (string, error) {
	connection, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(connection, "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key); err != nil {
		return "", err
	}
	reader := bufio.NewReader(connection)
	lengthLine, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	var length int
	if _, err := fmt.Sscanf(strings.TrimSpace(lengthLine), "$%d", &length); err != nil {
		return "", fmt.Errorf("GET length response %q", lengthLine)
	}
	buffer := make([]byte, length+2)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return "", err
	}
	return string(buffer[:length]), nil
}
