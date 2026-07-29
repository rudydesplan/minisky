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
	foreignVolumeResourceID := "foreign-volume-" + profile
	foreignContainerResourceID := "foreign-container-" + profile
	var ownedSpec RedisBackendSpec
	var foreignVolume string
	var foreignContainer string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if ownedSpec.VolumeIdentity != "" {
			if err := manager.DeleteRedisExact(cleanupCtx, ownedSpec); err != nil {
				t.Errorf("cleanup Redis resource %q: %v", resourceID, err)
			}
		}
		if foreignContainer != "" {
			if err := deleteDockerResource(cleanupCtx, manager,
				"/containers/"+url.PathEscape(foreignContainer)+"?force=true"); err != nil {
				t.Errorf("cleanup foreign container %q: %v", foreignContainer, err)
			}
		}
		if foreignVolume != "" {
			if err := deleteDockerResource(cleanupCtx, manager, "/volumes/"+url.PathEscape(foreignVolume)); err != nil {
				t.Errorf("cleanup foreign volume %q: %v", foreignVolume, err)
			}
		}
		if err := deleteOwnedRedisIntegrationNetwork(cleanupCtx, manager); err != nil {
			t.Errorf("cleanup Redis integration network: %v", err)
		}
	})

	ownedSpec = Redis72BackendSpec(resourceID)
	endpoint, ownedSpec, err := manager.ProvisionRedisExact(ctx, ownedSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.PublishRedisRuntime(ctx, ownedSpec); err != nil {
		t.Fatal(err)
	}
	resolvedImageID := ownedSpec.ImageID
	if err := redisSet(endpoint, "minisky-restart", "persisted"); err != nil {
		t.Fatal(err)
	}

	containerName, _ := redisDockerNames(resourceID)
	container, found, err := manager.inspectRedisContainer(ctx, containerName)
	if err != nil || !found || container.State.Status != "running" {
		t.Fatalf("owned Redis inspect found=%v status=%q err=%v", found, container.State.Status, err)
	}
	firstContainerID := container.ID
	if err := validateRedisContainer(container, containerName, ownedVolumeName(resourceID), ownedSpec); err != nil {
		t.Fatalf("owned Redis identity: %v", err)
	}
	if err := manager.Teardown(ctx); err != nil {
		t.Fatalf("graceful MiniSky teardown: %v", err)
	}
	assertDockerResourceStatus(t, ctx, manager,
		"/containers/"+url.PathEscape(containerName)+"/json", http.StatusNotFound)
	assertDockerResourceStatus(t, ctx, manager,
		"/volumes/"+url.PathEscape(ownedVolumeName(resourceID)), http.StatusOK)
	assertDockerResourceStatus(t, ctx, manager,
		"/networks/"+url.PathEscape(networkName), http.StatusNotFound)
	manager, err = NewServiceManager()
	if err != nil {
		t.Fatalf("construct restarted service manager: %v", err)
	}
	if err := manager.EnsureNetwork(ctx); err != nil {
		t.Fatalf("recreate owned network after process restart: %v", err)
	}

	endpoint, ownedSpec, owned, err := manager.ReconcileRedisExact(ctx, ownedSpec)
	if err != nil {
		t.Fatal(err)
	}
	if !owned {
		t.Fatal("persisted exact Redis volume was not reconciled")
	}
	if ownedSpec.Generation != 2 || ownedSpec.ContainerID == firstContainerID {
		t.Fatalf("restart runtime identity generation=%d container=%q first=%q",
			ownedSpec.Generation, ownedSpec.ContainerID, firstContainerID)
	}
	if err := manager.PublishRedisRuntime(ctx, ownedSpec); err != nil {
		t.Fatal(err)
	}
	value, err := redisGet(endpoint, "minisky-restart")
	if err != nil {
		t.Fatal(err)
	}
	if value != "persisted" {
		t.Fatalf("GET after restart = %q", value)
	}
	replacement, found, err := manager.inspectRedisContainer(ctx, containerName)
	if err != nil || !found || replacement.ID == firstContainerID {
		t.Fatalf("container replacement found=%v first=%q replacement=%q err=%v",
			found, firstContainerID, replacement.ID, err)
	}
	if err := manager.DeleteRedisExact(ctx, ownedSpec); err != nil {
		t.Fatalf("cleanup owned Valkey backend: %v", err)
	}

	_, ownedVolume := redisDockerNames(resourceID)
	assertDockerResourceStatus(t, ctx, manager, "/containers/"+url.PathEscape(containerName)+"/json", http.StatusNotFound)
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(ownedVolume), http.StatusNotFound)
	ownedSpec = RedisBackendSpec{}

	_, foreignVolume = redisDockerNames(foreignVolumeResourceID)
	if err := createRedisCollisionVolume(ctx, manager, foreignVolume, profile); err != nil {
		t.Fatalf("create deterministic foreign volume: %v", err)
	}
	if _, _, err := manager.ProvisionRedisExact(ctx, Redis72BackendSpec(foreignVolumeResourceID)); err == nil {
		t.Fatal("foreign same-name Redis volume was adopted")
	}
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(foreignVolume), http.StatusOK)
	if err := manager.DeleteRedisExact(ctx, Redis72BackendSpec(foreignVolumeResourceID)); err == nil {
		t.Fatal("foreign same-name Redis volume was deleted")
	}
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(foreignVolume), http.StatusOK)

	foreignContainer, _ = redisDockerNames(foreignContainerResourceID)
	if err := createRedisCollisionContainer(ctx, manager, foreignContainer, resolvedImageID); err != nil {
		t.Fatalf("create deterministic foreign container: %v", err)
	}
	if _, _, err := manager.ProvisionRedisExact(ctx, Redis72BackendSpec(foreignContainerResourceID)); err == nil {
		t.Fatal("foreign same-name Redis container was adopted")
	}
	assertDockerResourceStatus(t, ctx, manager,
		"/containers/"+url.PathEscape(foreignContainer)+"/json", http.StatusOK)
	if err := manager.DeleteRedisExact(ctx, Redis72BackendSpec(foreignContainerResourceID)); err == nil {
		t.Fatal("foreign same-name Redis container was deleted")
	}
	assertDockerResourceStatus(t, ctx, manager,
		"/containers/"+url.PathEscape(foreignContainer)+"/json", http.StatusOK)
}

func ownedVolumeName(resourceID string) string {
	_, volumeName := redisDockerNames(resourceID)
	return volumeName
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

func createRedisCollisionContainer(
	ctx context.Context,
	manager *ServiceManager,
	name string,
	imageID string,
) error {
	payload, err := json.Marshal(map[string]any{
		"Image": imageID,
		"Cmd":   []string{"valkey-server"},
		"Labels": map[string]string{
			"managed-by": "integration-test",
		},
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(name), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := manager.doDocker(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return fmt.Errorf("create collision container returned %d: %s",
			response.StatusCode, strings.TrimSpace(string(body)))
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
