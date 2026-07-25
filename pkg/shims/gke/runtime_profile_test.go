package gke

import (
	"os"
	"path/filepath"
	"testing"
)

func TestKindBackendUsesFullProfileOnlyWhenDependenciesExist(t *testing.T) {
	binDir := t.TempDir()
	writeRuntimeExecutable(t, filepath.Join(binDir, "kind"))
	writeRuntimeExecutable(t, filepath.Join(binDir, "docker"))
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_GKE_BACKEND", "")

	backend := NewKindBackend()
	if state := backend.Status(); !state.Enabled || state.Backend != "kind" || state.Source != "profile" {
		t.Fatalf("backend state = %#v, want profile-selected Kind", state)
	}
}

func TestKindBackendDoesNotBecomeDefaultInSimulationProfile(t *testing.T) {
	binDir := t.TempDir()
	writeRuntimeExecutable(t, filepath.Join(binDir, "kind"))
	writeRuntimeExecutable(t, filepath.Join(binDir, "docker"))
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_RUNTIME_PROFILE", "simulation")
	t.Setenv("MINISKY_GKE_BACKEND", "")

	backend := NewKindBackend()
	if state := backend.Status(); state.Enabled || state.Backend != "simulation" {
		t.Fatalf("backend state = %#v, want simulation without Kind cluster provisioning", state)
	}
}

func TestKindBackendFallsBackWithDependencyDiagnostic(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_GKE_BACKEND", "")

	backend := NewKindBackend()
	if state := backend.Status(); state.Enabled || state.Diagnostic == "" {
		t.Fatalf("backend state = %#v, want diagnosed simulation fallback", state)
	}
}

func writeRuntimeExecutable(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
}
