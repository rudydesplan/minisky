//go:build !windows

package orchestrator

import "testing"

func TestUnixDockerEndpointValidationMatchesDialer(t *testing.T) {
	if err := validateDockerHost("unix:///var/run/docker.sock"); err != nil {
		t.Fatalf("canonical Unix endpoint rejected: %v", err)
	}
	for _, endpoint := range []string{
		"/var/run/docker.sock",
		"docker.sock",
		"unix://relative/docker.sock",
		"npipe:////./pipe/docker_engine",
		"tcp://127.0.0.1:2375",
	} {
		if err := validateDockerHost(endpoint); err == nil {
			t.Fatalf("unsupported Unix endpoint %q accepted", endpoint)
		}
	}
}
