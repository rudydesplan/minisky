package orchestrator

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestAlloyDBDockerLifecycleIntegration(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_ALLOYDB_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_ALLOYDB_INTEGRATION=1 to run")
	}
	t.Setenv("MINISKY_PROFILE", fmt.Sprintf("alloydb-integration-%d", time.Now().UnixNano()))
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

	identity := AlloyDBIdentity{
		Project: "integration", Location: "us-central1", Cluster: "cluster", Instance: "primary",
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := manager.DeleteAlloyDB(cleanupContext, identity); err != nil {
			t.Errorf("cleanup AlloyDB backend: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	endpoint, created, err := manager.ProvisionAlloyDB(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if !created || endpoint == "" {
		t.Fatalf("provision result endpoint=%q created=%t", endpoint, created)
	}
	reconciledEndpoint, exists, err := manager.ReconcileAlloyDB(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || reconciledEndpoint != endpoint {
		t.Fatalf("reconcile endpoint=%q exists=%t, want %q", reconciledEndpoint, exists, endpoint)
	}
}
