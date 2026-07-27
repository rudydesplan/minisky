//go:build windows

package orchestrator

import "testing"

func TestWindowsDockerEndpointValidationMatchesDialer(t *testing.T) {
	const endpoint = "npipe:////./pipe/docker_engine"
	if err := validateDockerHost(endpoint); err != nil {
		t.Fatalf("canonical Windows endpoint rejected: %v", err)
	}
	t.Setenv("DOCKER_HOST", endpoint)
	if got := resolveDockerSocket(); got != "//./pipe/docker_engine" {
		t.Fatalf("resolved Windows endpoint = %q", got)
	}
	for _, endpoint := range []string{
		`\\.\pipe\docker_engine`,
		"//./pipe/docker_engine",
		"npipe://relative",
		"unix:///var/run/docker.sock",
		"tcp://127.0.0.1:2375",
	} {
		t.Setenv("DOCKER_HOST", endpoint)
		if err := validateDockerHost(endpoint); err == nil {
			t.Fatalf("unsupported Windows endpoint %q accepted", endpoint)
		}
	}
}
