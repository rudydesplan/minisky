package orchestrator

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCloudBuildWaitDockerIntegration(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_CLOUDBUILD_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_CLOUDBUILD_INTEGRATION=1 to run")
	}
	image := os.Getenv("MINISKY_CLOUDBUILD_TEST_IMAGE")
	if !isDigestPinnedImage(image) {
		t.Fatalf("MINISKY_CLOUDBUILD_TEST_IMAGE must be pinned as image@sha256:<64 hex characters>")
	}

	profile := fmt.Sprintf("cloudbuild-integration-%d", time.Now().UnixNano())
	t.Setenv("MINISKY_PROFILE", profile)
	manager, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	ping, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := manager.doDocker(ping)
	if err != nil {
		t.Skipf("Docker daemon unavailable: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Skipf("Docker daemon ping returned %d", response.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := manager.EnsureNetwork(ctx); err != nil {
		t.Fatal(err)
	}
	resourceID := "projects/integration/builds/" + profile
	workspace := "minisky-build-integration-" + fmt.Sprint(time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, name := range []string{workspace + "-success", workspace + "-failure"} {
			if err := manager.StopAndRemoveBuildContainer(cleanupCtx, name, resourceID); err != nil {
				t.Errorf("cleanup build container %q: %v", name, err)
			}
		}
		if err := manager.RemoveBuildWorkspace(cleanupCtx, workspace, resourceID); err != nil {
			t.Errorf("cleanup build workspace: %v", err)
		}
		manager.Teardown(cleanupCtx)
	})
	if err := manager.EnsureBuildWorkspace(ctx, workspace, resourceID); err != nil {
		t.Fatal(err)
	}

	successName := workspace + "-success"
	if err := manager.ProvisionBuildStep(ctx, successName, resourceID, image,
		[]string{workspace + ":/workspace"}, nil,
		[]string{"/bin/sh", "-c", "printf 'integration-success\\n'; exit 0"}); err != nil {
		t.Fatal(err)
	}
	success, err := manager.WaitBuildContainer(ctx, successName, resourceID)
	if err != nil {
		t.Fatal(err)
	}
	if success.ExitCode != 0 || !strings.Contains(success.Logs, "integration-success") {
		t.Fatalf("success result = %#v", success)
	}
	if err := manager.StopAndRemoveBuildContainer(ctx, successName, resourceID); err != nil {
		t.Fatal(err)
	}

	failureName := workspace + "-failure"
	if err := manager.ProvisionBuildStep(ctx, failureName, resourceID, image,
		[]string{workspace + ":/workspace"}, nil,
		[]string{"/bin/sh", "-c", "printf 'integration-failure\\n' >&2; exit 23"}); err != nil {
		t.Fatal(err)
	}
	failure, err := manager.WaitBuildContainer(ctx, failureName, resourceID)
	if err != nil {
		t.Fatal(err)
	}
	if failure.ExitCode != 23 || !strings.Contains(failure.Logs, "integration-failure") {
		t.Fatalf("failure result = %#v", failure)
	}
	if err := manager.StopAndRemoveBuildContainer(ctx, failureName, resourceID); err != nil {
		t.Fatal(err)
	}
}

func isDigestPinnedImage(image string) bool {
	parts := strings.Split(image, "@sha256:")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
		return false
	}
	_, err := hex.DecodeString(parts[1])
	return err == nil
}
