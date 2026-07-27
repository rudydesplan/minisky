//go:build windows

package orchestrator

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/Microsoft/go-winio"
)

func validateDockerHost(host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil
	}
	const prefix = "npipe:////./pipe/"
	name, ok := strings.CutPrefix(host, prefix)
	if !ok || name == "" || strings.ContainsAny(name, `/\?#`) || name == "." || name == ".." {
		return fmt.Errorf("%w: unsupported Windows DOCKER_HOST %q", ErrDockerConfiguration, host)
	}
	return nil
}

func resolveDockerSocket() string {
	// 1. Explicit DOCKER_HOST env var
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		return strings.TrimPrefix(host, "npipe://")
	}

	// 2. Default Windows named pipe
	defaultPipe := `//./pipe/docker_engine`
	return defaultPipe
}

func (sm *ServiceManager) dialDocker(ctx context.Context, _, _ string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, sm.sockPath)
}
