package orchestrator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMemcachedDockerLifecycleIntegration(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_MEMCACHED_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_MEMCACHED_INTEGRATION=1 to run")
	}
	acquireMiniSkyDockerIntegrationLock(t)
	profile := fmt.Sprintf("memcached-integration-%d", time.Now().UnixNano())
	t.Setenv("MINISKY_PROFILE", profile)
	manager, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pingRequest, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	pingResponse, err := manager.doDocker(pingRequest)
	if err != nil {
		t.Skipf("Docker daemon unavailable: %v", err)
	}
	_ = pingResponse.Body.Close()
	if pingResponse.StatusCode != http.StatusOK {
		t.Skipf("Docker daemon ping returned %d", pingResponse.StatusCode)
	}

	resourceID := testMemcacheBackendID
	name := memcachedDockerName(resourceID)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := manager.DeleteMemcache(cleanupCtx, resourceID); err != nil {
			t.Errorf("cleanup Memcached backend: %v", err)
		}
	})

	for _, test := range []struct {
		apiVersion      string
		protocolVersion string
	}{
		{apiVersion: "MEMCACHE_1_5", protocolVersion: "1.5.16"},
		{apiVersion: "MEMCACHE_1_6_15", protocolVersion: "1.6.15"},
	} {
		t.Run(test.apiVersion, func(t *testing.T) {
			endpoints, owned, exists, err := manager.ProvisionMemcache(
				ctx, resourceID, 1, 1, 1024, test.apiVersion, nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !owned || !exists || len(endpoints) != 1 {
				t.Fatalf("endpoints=%v owned=%v exists=%v", endpoints, owned, exists)
			}
			endpoint := endpoints[0]
			version, err := memcachedVersion(endpoint)
			if err != nil {
				t.Fatal(err)
			}
			if version != "VERSION "+test.protocolVersion {
				t.Fatalf("version response = %q", version)
			}
			if err := memcachedSet(endpoint, "restart-key", "ephemeral-value"); err != nil {
				t.Fatal(err)
			}
			if value, found, err := memcachedGet(endpoint, "restart-key"); err != nil || !found || value != "ephemeral-value" {
				t.Fatalf("GET before restart value=%q found=%v err=%v", value, found, err)
			}

			container, found, err := manager.inspectMemcachedContainer(ctx, name)
			if err != nil || !found {
				t.Fatalf("inspect backend found=%v err=%v", found, err)
			}
			stopRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost,
				"http://localhost/containers/"+url.PathEscape(container.ID)+"/stop", nil)
			stopResponse, err := manager.doDocker(stopRequest)
			if err != nil {
				t.Fatal(err)
			}
			_ = stopResponse.Body.Close()
			if stopResponse.StatusCode != http.StatusNoContent {
				t.Fatalf("stop Memcached returned %d", stopResponse.StatusCode)
			}

			endpoint, found, err = reconcileMemcacheForTest(manager, ctx, resourceID, test.apiVersion)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("stopped exact-owned backend was reported missing")
			}
			if value, exists, err := memcachedGet(endpoint, "restart-key"); err != nil {
				t.Fatal(err)
			} else if exists {
				t.Fatalf("Memcached data unexpectedly survived daemon restart: %q", value)
			}

			if err := manager.DeleteMemcache(ctx, resourceID); err != nil {
				t.Fatal(err)
			}
			inspectRequest, _ := http.NewRequestWithContext(ctx, http.MethodGet,
				"http://localhost/containers/"+url.PathEscape(name)+"/json", nil)
			inspectResponse, err := manager.doDocker(inspectRequest)
			if err != nil {
				t.Fatal(err)
			}
			defer inspectResponse.Body.Close()
			if inspectResponse.StatusCode != http.StatusNotFound {
				body, _ := io.ReadAll(io.LimitReader(inspectResponse.Body, 4096))
				t.Fatalf("deleted Memcached inspect returned %d: %s", inspectResponse.StatusCode, body)
			}
		})
	}
}

func memcachedVersion(endpoint string) (string, error) {
	connection, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(connection, "version\r\n"); err != nil {
		return "", err
	}
	response, err := bufio.NewReader(io.LimitReader(connection, 512)).ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(response, "VERSION ") || !strings.HasSuffix(response, "\r\n") {
		return "", fmt.Errorf("version response %q", response)
	}
	return strings.TrimSuffix(response, "\r\n"), nil
}
