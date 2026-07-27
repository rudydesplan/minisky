package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

func TestCleanupAllProfilesRemovesAnonymousVolumesDockerIntegration(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_CLEANUP_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_CLEANUP_INTEGRATION=1 to run")
	}

	manager, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ping, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	pingResponse, err := manager.doDocker(ping)
	if err != nil {
		t.Skipf("Docker daemon unavailable: %v", err)
	}
	pingResponse.Body.Close()
	if pingResponse.StatusCode != http.StatusOK {
		t.Skipf("Docker daemon ping returned %d", pingResponse.StatusCode)
	}
	image := os.Getenv("MINISKY_DOCKER_CLEANUP_TEST_IMAGE")
	if image == "" {
		image = "alpine:latest"
	}
	if exists, inspectErr := manager.ImageExistsPublic(image); inspectErr != nil {
		t.Fatal(inspectErr)
	} else if !exists {
		t.Skipf("cleanup integration image %q is not available locally", image)
	}

	suffix := fmt.Sprint(time.Now().UnixNano())
	profile := "cleanup-integration-" + suffix
	cleanupContainer := func(id string) {
		t.Helper()
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			request, _ := http.NewRequestWithContext(cleanupCtx, http.MethodDelete,
				"http://localhost/containers/"+url.PathEscape(id)+"?force=true&v=true", nil)
			response, requestErr := manager.doDocker(request)
			if requestErr != nil {
				t.Errorf("cleanup Docker container %q: %v", id, requestErr)
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
				t.Errorf("cleanup Docker container %q returned %d", id, response.StatusCode)
			}
		})
	}
	ownedID := createAnonymousVolumeContainer(t, ctx, manager, "minisky-cleanup-owned-"+suffix, image, map[string]string{
		"managed-by":      "minisky",
		"minisky.profile": profile,
	})
	cleanupContainer(ownedID)
	unrelatedID := createAnonymousVolumeContainer(t, ctx, manager, "minisky-cleanup-unrelated-"+suffix, image, map[string]string{
		"managed-by":      "integration-test",
		"minisky.profile": profile,
	})
	cleanupContainer(unrelatedID)

	ownedVolume := anonymousVolumeForContainer(t, ctx, manager, ownedID)
	unrelatedVolume := anonymousVolumeForContainer(t, ctx, manager, unrelatedID)

	cleanupManager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	cleanupManager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/json":
			body, err := json.Marshal([]cleanupResource{
				{ID: ownedID, Labels: map[string]string{"managed-by": "minisky", "minisky.profile": profile}},
				{ID: unrelatedID, Labels: map[string]string{"managed-by": "integration-test", "minisky.profile": profile}},
			})
			if err != nil {
				return nil, err
			}
			return dockerResponse(http.StatusOK, string(body)), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks":
			return dockerResponse(http.StatusOK, "[]"), nil
		case request.Method == http.MethodPost && request.URL.Path == "/volumes/prune":
			return dockerResponse(http.StatusOK, `{"VolumesDeleted":[]}`), nil
		default:
			return manager.dockerClient.Do(request)
		}
	})}
	if err := cleanupManager.CleanupAllProfiles(ctx); err != nil {
		t.Fatal(err)
	}
	assertDockerResourceStatus(t, ctx, manager, "/containers/"+url.PathEscape(ownedID)+"/json", http.StatusNotFound)
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(ownedVolume), http.StatusNotFound)
	assertDockerResourceStatus(t, ctx, manager, "/containers/"+url.PathEscape(unrelatedID)+"/json", http.StatusOK)
	assertDockerResourceStatus(t, ctx, manager, "/volumes/"+url.PathEscape(unrelatedVolume), http.StatusOK)
}

func createAnonymousVolumeContainer(
	t *testing.T,
	ctx context.Context,
	manager *ServiceManager,
	name string,
	image string,
	labels map[string]string,
) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"Image":   image,
		"Labels":  labels,
		"Volumes": map[string]any{"/anonymous": map[string]any{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?"+url.Values{"name": {name}}.Encode(), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := manager.doDocker(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		detail, _ := io.ReadAll(response.Body)
		t.Fatalf("create Docker container returned %d: %s", response.StatusCode, detail)
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatal("Docker returned an empty container ID")
	}
	return created.ID
}

func anonymousVolumeForContainer(t *testing.T, ctx context.Context, manager *ServiceManager, containerID string) string {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(containerID)+"/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := manager.doDocker(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("inspect Docker container returned %d", response.StatusCode)
	}
	var inspected struct {
		Mounts []struct {
			Type        string `json:"Type"`
			Name        string `json:"Name"`
			Destination string `json:"Destination"`
		} `json:"Mounts"`
	}
	if err := json.NewDecoder(response.Body).Decode(&inspected); err != nil {
		t.Fatal(err)
	}
	for _, mount := range inspected.Mounts {
		if mount.Type == "volume" && mount.Destination == "/anonymous" && mount.Name != "" {
			return mount.Name
		}
	}
	t.Fatalf("container %q has no anonymous test volume", containerID)
	return ""
}

func assertDockerResourceStatus(
	t *testing.T,
	ctx context.Context,
	manager *ServiceManager,
	path string,
	want int,
) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := manager.doDocker(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != want {
		t.Fatalf("GET %s returned %d, want %d", path, response.StatusCode, want)
	}
}
