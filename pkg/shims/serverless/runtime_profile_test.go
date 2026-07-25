package serverless

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildpacksBackendUsesRuntimeProfileAndDependencies(t *testing.T) {
	binDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pack"))
	writeExecutable(t, filepath.Join(binDir, "docker"))
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_SERVERLESS_BACKEND", "")

	backend := NewBuildpacksBackend()
	if !backend.Enabled() {
		t.Fatalf("backend state = %#v, want Buildpacks enabled", backend.Status())
	}
	if state := backend.Status(); state.Backend != "buildpacks" || state.Source != "profile" {
		t.Fatalf("backend state = %#v, want profile-selected Buildpacks", state)
	}
}

func TestBuildpacksBackendFallsBackWhenDependencyMissing(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_SERVERLESS_BACKEND", "")

	backend := NewBuildpacksBackend()
	state := backend.Status()
	if backend.Enabled() || state.Backend != "simulation" || state.Diagnostic == "" {
		t.Fatalf("backend state = %#v, want diagnosed simulation fallback", state)
	}
}

func TestBuildpacksExplicitOverrideWinsOverProfile(t *testing.T) {
	binDir := t.TempDir()
	writeExecutable(t, filepath.Join(binDir, "pack"))
	writeExecutable(t, filepath.Join(binDir, "docker"))
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_RUNTIME_PROFILE", "simulation")
	t.Setenv("MINISKY_SERVERLESS_BACKEND", "buildpacks")

	backend := NewBuildpacksBackend()
	if state := backend.Status(); !state.Enabled || state.Source != "override" {
		t.Fatalf("backend state = %#v, want explicit Buildpacks override", state)
	}
}

func writeExecutable(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
}
