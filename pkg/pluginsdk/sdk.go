// Package pluginsdk freezes the in-tree MiniSky shim contribution contract.
// It is not a dynamic plugin loader: implementations are compiled into MiniSky.
package pluginsdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"minisky/pkg/registry"
)

const (
	ProtocolVersion = "minisky.plugin/v0"
	ExecutionInTree = "in-tree"
)

var (
	namePattern    = regexp.MustCompile(`^[a-z][a-z0-9-]{1,62}$`)
	versionPattern = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$`)
	domainPattern  = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$`)
)

// Manifest describes one source-compiled shim contribution.
type Manifest struct {
	ProtocolVersion string   `json:"protocolVersion"`
	Name            string   `json:"name"`
	Version         string   `json:"version"`
	Domains         []string `json:"domains"`
	Fidelity        string   `json:"fidelity"`
	Persistence     string   `json:"persistence"`
	Execution       string   `json:"execution"`
}

func (m Manifest) Validate() error {
	if m.ProtocolVersion != ProtocolVersion {
		return fmt.Errorf("plugin protocol %q is not compatible with %q", m.ProtocolVersion, ProtocolVersion)
	}
	if !namePattern.MatchString(m.Name) {
		return errors.New("plugin name must be lowercase letters, digits, or hyphens")
	}
	if !versionPattern.MatchString(m.Version) {
		return errors.New("plugin version must be semantic version syntax")
	}
	if m.Execution != ExecutionInTree {
		return fmt.Errorf("plugin execution %q is unsupported; v0 plugins are compiled in-tree", m.Execution)
	}
	if m.Fidelity != "high" && m.Fidelity != "standard" && m.Fidelity != "passthrough" {
		return fmt.Errorf("unsupported fidelity %q", m.Fidelity)
	}
	switch m.Persistence {
	case "memory", "file", "docker", "hybrid", "static":
	default:
		return fmt.Errorf("unsupported persistence %q", m.Persistence)
	}
	if len(m.Domains) == 0 {
		return errors.New("plugin manifest requires at least one domain")
	}
	seen := make(map[string]struct{}, len(m.Domains))
	for _, domain := range m.Domains {
		domain = strings.ToLower(strings.TrimSpace(domain))
		if !domainPattern.MatchString(domain) {
			return fmt.Errorf("invalid plugin domain %q", domain)
		}
		if _, exists := seen[domain]; exists {
			return fmt.Errorf("duplicate plugin domain %q", domain)
		}
		seen[domain] = struct{}{}
	}
	return nil
}

// Plugin is the v0 lifecycle contract. OnPostBoot is called after all in-tree
// shims are constructed. Shutdown is called during graceful daemon shutdown.
type Plugin interface {
	http.Handler
	registry.PostBoot
	Shutdown(context.Context) error
}

type Factory func(*registry.Context) Plugin

// MustRegister validates a v0 manifest and registers its in-tree domains.
// Invalid source manifests panic during process initialization.
func MustRegister(manifest Manifest, factory Factory) {
	if err := manifest.Validate(); err != nil {
		panic(err)
	}
	if factory == nil {
		panic("plugin factory is required")
	}
	for _, domain := range manifest.Domains {
		domain := strings.ToLower(strings.TrimSpace(domain))
		registry.Register(domain, func(ctx *registry.Context) http.Handler {
			return factory(ctx)
		})
	}
}
