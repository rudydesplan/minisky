package pluginsdk

import (
	"context"
	"net/http"
	"testing"

	"minisky/pkg/registry"
)

type testPlugin struct{}

func (*testPlugin) ServeHTTP(http.ResponseWriter, *http.Request) {}
func (*testPlugin) OnPostBoot(*registry.Context)                 {}
func (*testPlugin) Shutdown(context.Context) error               { return nil }

func TestManifestValidationAndPluginContract(t *testing.T) {
	manifest := Manifest{
		ProtocolVersion: ProtocolVersion,
		Name:            "example",
		Version:         "0.1.0",
		Domains:         []string{"example.googleapis.com"},
		Fidelity:        "standard",
		Persistence:     "memory",
		Execution:       ExecutionInTree,
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	var _ Plugin = (*testPlugin)(nil)

	manifest.Execution = "process"
	if err := manifest.Validate(); err == nil {
		t.Fatal("expected unsupported execution mode error")
	}
}
