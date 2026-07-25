package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"

	"minisky/pkg/pluginsdk"

	"github.com/spf13/cobra"
)

var (
	pluginName    string
	pluginDomain  string
	pluginVersion string
)

var pluginCmd = &cobra.Command{
	Use:   "plugin",
	Short: "Work with source-compiled MiniSky plugin scaffolds",
	Long:  "MiniSky plugin SDK v0 is in-tree only; dynamic install, list, and remove commands are not implemented.",
}

var pluginScaffoldCmd = &cobra.Command{
	Use:   "scaffold DIRECTORY",
	Short: "Generate a compiling in-tree plugin scaffold",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		return writePluginScaffold(args[0], pluginName, pluginDomain, pluginVersion)
	},
}

func writePluginScaffold(target, name, domain, version string) error {
	manifest := pluginsdk.Manifest{
		ProtocolVersion: pluginsdk.ProtocolVersion,
		Name:            strings.TrimSpace(name),
		Version:         strings.TrimSpace(version),
		Domains:         []string{strings.ToLower(strings.TrimSpace(domain))},
		Fidelity:        "standard",
		Persistence:     "memory",
		Execution:       pluginsdk.ExecutionInTree,
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	target, err := filepath.Abs(target)
	if err != nil {
		return fmt.Errorf("resolve scaffold directory: %w", err)
	}
	if info, statErr := os.Lstat(target); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errors.New("scaffold target must be a real directory")
		}
		entries, readErr := os.ReadDir(target)
		if readErr != nil {
			return readErr
		}
		if len(entries) != 0 {
			return errors.New("scaffold target must be empty")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	} else if err := os.MkdirAll(target, 0o700); err != nil {
		return fmt.Errorf("create scaffold directory: %w", err)
	}

	packageName := strings.ReplaceAll(manifest.Name, "-", "_")
	source := fmt.Sprintf(`package %s

import (
	"context"
	"net/http"

	"minisky/pkg/pluginsdk"
	"minisky/pkg/registry"
)

var Manifest = pluginsdk.Manifest{
	ProtocolVersion: pluginsdk.ProtocolVersion,
	Name: %q,
	Version: %q,
	Domains: []string{%q},
	Fidelity: "standard",
	Persistence: "memory",
	Execution: pluginsdk.ExecutionInTree,
}

type Plugin struct{}

func init() {
	pluginsdk.MustRegister(Manifest, func(*registry.Context) pluginsdk.Plugin {
		return &Plugin{}
	})
}

func (*Plugin) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_, _ = w.Write([]byte("{\"error\":{\"code\":501,\"message\":\"scaffold operation is not implemented\",\"status\":\"UNIMPLEMENTED\"}}\n"))
}

func (*Plugin) OnPostBoot(*registry.Context) {}

func (*Plugin) Shutdown(context.Context) error { return nil }
`, packageName, manifest.Name, manifest.Version, manifest.Domains[0])
	formatted, err := format.Source([]byte(source))
	if err != nil {
		return fmt.Errorf("format generated plugin: %w", err)
	}
	testSource := fmt.Sprintf(`package %s

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestManifestAndUnsupportedBoundary(t *testing.T) {
	if err := Manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	(&Plugin{}).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/resources", nil))
	if response.Code != http.StatusNotImplemented || !strings.Contains(response.Body.String(), "UNIMPLEMENTED") {
		t.Fatalf("status=%%d body=%%s", response.Code, response.Body.String())
	}
}
`, packageName)
	formattedTest, err := format.Source([]byte(testSource))
	if err != nil {
		return fmt.Errorf("format generated test: %w", err)
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	manifestJSON = append(manifestJSON, '\n')
	for path, contents := range map[string][]byte{
		"plugin.go":            formatted,
		"plugin_test.go":       formattedTest,
		"plugin.manifest.json": manifestJSON,
	} {
		if err := os.WriteFile(filepath.Join(target, path), contents, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return nil
}

func init() {
	pluginScaffoldCmd.Flags().StringVar(&pluginName, "name", "", "lowercase plugin name")
	pluginScaffoldCmd.Flags().StringVar(&pluginDomain, "domain", "", "registered service domain")
	pluginScaffoldCmd.Flags().StringVar(&pluginVersion, "version", "0.1.0", "plugin semantic version")
	_ = pluginScaffoldCmd.MarkFlagRequired("name")
	_ = pluginScaffoldCmd.MarkFlagRequired("domain")
	pluginCmd.AddCommand(pluginScaffoldCmd)
	rootCmd.AddCommand(pluginCmd)
}
