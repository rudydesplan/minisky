package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"minisky/pkg/config"
)

func TestValkeyVolumeSurvivesOwnedContainerRestartAndCleanup(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_REDIS_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_REDIS_INTEGRATION=1 to run")
	}
	acquireMiniSkyDockerIntegrationLock(t)
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
	resourceID := "owned-" + profile
	missingContainerResourceID := "missing-container-" + profile
	collisionResourceID := "collision-" + profile
	var collisionVolume string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, id := range []string{resourceID, missingContainerResourceID} {
			if err := manager.DeleteRedis(cleanupCtx, id); err != nil {
				t.Errorf("cleanup Redis resource %q: %v", id, err)
			}
		}
		if collisionVolume != "" {
			if err := deleteDockerResource(cleanupCtx, manager, "/volumes/"+url.PathEscape(collisionVolume)); err != nil {
				t.Errorf("cleanup collision volume %q: %v", collisionVolume, err)
			}
		}
		if err := deleteOwnedRedisIntegrationNetwork(cleanupCtx, manager); err != nil {
			t.Errorf("cleanup Redis integration network: %v", err)
		}
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

	_, ownedVolume := redisDockerNames(resourceID)
	assertDockerResourceStatus(t, ctx, manager, "/containers/"+url.PathEscape(containerName)+"/json", http.StatusNotFound)
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(ownedVolume), http.StatusNotFound)

	if _, err := manager.ProvisionRedis(ctx, missingContainerResourceID, valkeyImage); err != nil {
		t.Fatalf("provision missing-container case: %v", err)
	}
	missingContainer, missingVolume := redisDockerNames(missingContainerResourceID)
	if err := deleteDockerResource(ctx, manager, "/containers/"+url.PathEscape(missingContainer)+"?force=true"); err != nil {
		t.Fatalf("remove exact test container: %v", err)
	}
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(missingVolume), http.StatusOK)
	if err := manager.DeleteRedis(ctx, missingContainerResourceID); err != nil {
		t.Fatalf("restart-style orphan volume cleanup: %v", err)
	}
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(missingVolume), http.StatusNotFound)

	_, collisionVolume = redisDockerNames(collisionResourceID)
	if err := createRedisCollisionVolume(ctx, manager, collisionVolume, profile); err != nil {
		t.Fatalf("create exact test collision volume: %v", err)
	}
	if _, err := manager.ProvisionRedis(ctx, collisionResourceID, valkeyImage); err == nil {
		t.Fatal("unowned Redis volume collision was adopted")
	}
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(collisionVolume), http.StatusOK)
	if err := manager.DeleteRedis(ctx, collisionResourceID); err == nil {
		t.Fatal("unowned Redis volume collision was deleted")
	}
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(collisionVolume), http.StatusOK)
}

func acquireMiniSkyDockerIntegrationLock(t *testing.T) {
	t.Helper()
	lock := filepath.Join(os.TempDir(), "minisky-net-integration.lock")
	release, err := tryAcquireMiniSkyDockerIntegrationLock(lock)
	if err != nil {
		if os.IsExist(err) {
			t.Skipf("Another MiniSky Docker integration is active (%s)", lock)
		}
		t.Fatalf("acquire shared Docker integration lock: %v", err)
	}
	t.Cleanup(func() {
		if err := release(); err != nil && !os.IsNotExist(err) {
			t.Errorf("release shared Docker integration lock: %v", err)
		}
	})
}

func tryAcquireMiniSkyDockerIntegrationLock(lock string) (func() error, error) {
	if err := os.Mkdir(lock, 0o700); err != nil {
		return nil, err
	}
	return func() error { return os.Remove(lock) }, nil
}

func TestRedisDockerIntegrationLockRefusesCollision(t *testing.T) {
	lock := filepath.Join(t.TempDir(), "minisky-net-integration.lock")
	if err := os.Mkdir(lock, 0o700); err != nil {
		t.Fatal(err)
	}
	release, err := tryAcquireMiniSkyDockerIntegrationLock(lock)
	if !os.IsExist(err) {
		t.Fatalf("collision error = %v, want existing lock refusal", err)
	}
	if release != nil {
		t.Fatal("collision returned a release function for a lock it did not own")
	}
	if _, err := os.Stat(lock); err != nil {
		t.Fatalf("collision altered pre-existing lock: %v", err)
	}
}

func createRedisCollisionVolume(
	ctx context.Context,
	manager *ServiceManager,
	name string,
	profile string,
) error {
	payload, err := json.Marshal(map[string]any{
		"Name": name,
		"Labels": map[string]string{
			"managed-by":      "integration-test",
			"minisky.profile": profile,
		},
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/volumes/create", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := manager.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return fmt.Errorf("create collision volume returned %d", response.StatusCode)
	}
	return nil
}

func deleteDockerResource(ctx context.Context, manager *ServiceManager, path string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://localhost"+path, nil)
	if err != nil {
		return err
	}
	response, err := manager.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("DELETE %s returned %d", path, response.StatusCode)
	}
	return nil
}

func deleteOwnedRedisIntegrationNetwork(ctx context.Context, manager *ServiceManager) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/networks/"+networkName, nil)
	if err != nil {
		return err
	}
	response, err := manager.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("inspect integration network returned %d", response.StatusCode)
	}
	var network struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.NewDecoder(response.Body).Decode(&network); err != nil {
		return err
	}
	if network.ID == "" || !isOwnedDockerResource(network.Labels) {
		return fmt.Errorf("refusing to remove integration network without exact ownership")
	}
	return deleteDockerResource(ctx, manager, "/networks/"+url.PathEscape(network.ID))
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
