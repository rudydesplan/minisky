package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	RuntimeProfileEnv        = "MINISKY_RUNTIME_PROFILE"
	RuntimeProfileFull       = "full"
	RuntimeProfileSimulation = "simulation"
)

// RuntimeProfile describes the selected runtime policy. Simulation is the
// backward-compatible default and never opts into container-backed services.
type RuntimeProfile struct {
	Name       string `json:"name"`
	Diagnostic string `json:"diagnostic,omitempty"`
}

// BackendSelection is the requested backend before platform and dependency
// availability are considered.
type BackendSelection struct {
	Profile    string
	Backend    string
	Requested  bool
	Source     string
	Diagnostic string
}

// BackendState is the effective state reported by constructors, doctor, and
// the dashboard.
type BackendState struct {
	Profile    string `json:"profile"`
	Backend    string `json:"backend"`
	Enabled    bool   `json:"enabled"`
	Source     string `json:"source"`
	Diagnostic string `json:"diagnostic,omitempty"`
}

func GetRuntimeProfile() RuntimeProfile {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(RuntimeProfileEnv)))
	switch value {
	case "", RuntimeProfileSimulation:
		return RuntimeProfile{Name: RuntimeProfileSimulation}
	case RuntimeProfileFull:
		return RuntimeProfile{Name: RuntimeProfileFull}
	default:
		return RuntimeProfile{
			Name:       RuntimeProfileSimulation,
			Diagnostic: fmt.Sprintf("%s=%q is invalid; using simulation (valid values: full, simulation)", RuntimeProfileEnv, value),
		}
	}
}

// ResolveBackend applies an explicit per-backend override before the runtime
// profile. The full profile requests the real backend; simulation requests the
// in-memory implementation.
func ResolveBackend(overrideEnv, realBackend string) BackendSelection {
	profile := GetRuntimeProfile()
	selection := BackendSelection{
		Profile:    profile.Name,
		Backend:    RuntimeProfileSimulation,
		Source:     "profile",
		Diagnostic: profile.Diagnostic,
	}

	if value := strings.ToLower(strings.TrimSpace(os.Getenv(overrideEnv))); value != "" {
		selection.Source = "override"
		switch value {
		case strings.ToLower(realBackend):
			selection.Backend = realBackend
			selection.Requested = true
		case RuntimeProfileSimulation, "mock", "memory", "in-memory":
			selection.Backend = RuntimeProfileSimulation
		default:
			selection.Diagnostic = joinDiagnostic(selection.Diagnostic,
				fmt.Sprintf("%s=%q is unsupported; using simulation (valid values: %s, simulation)", overrideEnv, value, realBackend))
		}
		return selection
	}

	if profile.Name == RuntimeProfileFull {
		selection.Backend = realBackend
		selection.Requested = true
	}
	return selection
}

func (selection BackendSelection) Effective(enabled bool, diagnostic string) BackendState {
	backend := selection.Backend
	if !enabled {
		backend = RuntimeProfileSimulation
	}
	return BackendState{
		Profile:    selection.Profile,
		Backend:    backend,
		Enabled:    enabled,
		Source:     selection.Source,
		Diagnostic: joinDiagnostic(selection.Diagnostic, diagnostic),
	}
}

func joinDiagnostic(current, next string) string {
	if current == "" {
		return next
	}
	if next == "" {
		return current
	}
	return current + "; " + next
}
