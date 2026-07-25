package config

import "testing"

func TestRuntimeProfileDefaultsToSimulation(t *testing.T) {
	t.Setenv(RuntimeProfileEnv, "")

	profile := GetRuntimeProfile()
	if profile.Name != RuntimeProfileSimulation {
		t.Fatalf("profile = %q, want %q", profile.Name, RuntimeProfileSimulation)
	}
	if profile.Diagnostic != "" {
		t.Fatalf("diagnostic = %q, want empty", profile.Diagnostic)
	}
}

func TestRuntimeProfileFullSelectsRealBackend(t *testing.T) {
	t.Setenv(RuntimeProfileEnv, "full")
	t.Setenv("MINISKY_GKE_BACKEND", "")

	selection := ResolveBackend("MINISKY_GKE_BACKEND", "kind")
	if !selection.Requested || selection.Backend != "kind" || selection.Source != "profile" {
		t.Fatalf("selection = %#v, want profile-selected kind", selection)
	}
}

func TestExplicitBackendOverrideWinsOverFullProfile(t *testing.T) {
	t.Setenv(RuntimeProfileEnv, "full")
	t.Setenv("MINISKY_GKE_BACKEND", "simulation")

	selection := ResolveBackend("MINISKY_GKE_BACKEND", "kind")
	if selection.Requested || selection.Backend != "simulation" || selection.Source != "override" {
		t.Fatalf("selection = %#v, want explicit simulation override", selection)
	}
}

func TestExplicitRealBackendOverrideWinsOverSimulationProfile(t *testing.T) {
	t.Setenv(RuntimeProfileEnv, "simulation")
	t.Setenv("MINISKY_BQ_BACKEND", "duckdb")

	selection := ResolveBackend("MINISKY_BQ_BACKEND", "duckdb")
	if !selection.Requested || selection.Backend != "duckdb" || selection.Source != "override" {
		t.Fatalf("selection = %#v, want explicit DuckDB override", selection)
	}
}

func TestInvalidRuntimeProfileFallsBackWithDiagnostic(t *testing.T) {
	t.Setenv(RuntimeProfileEnv, "turbo")

	profile := GetRuntimeProfile()
	if profile.Name != RuntimeProfileSimulation {
		t.Fatalf("profile = %q, want simulation fallback", profile.Name)
	}
	if profile.Diagnostic == "" {
		t.Fatal("diagnostic is empty for invalid runtime profile")
	}
}

func TestUnsupportedBackendOverrideFallsBackWithDiagnostic(t *testing.T) {
	t.Setenv(RuntimeProfileEnv, "full")
	t.Setenv("MINISKY_SERVERLESS_BACKEND", "mystery")

	selection := ResolveBackend("MINISKY_SERVERLESS_BACKEND", "buildpacks")
	if selection.Requested || selection.Backend != "simulation" {
		t.Fatalf("selection = %#v, want simulation fallback", selection)
	}
	if selection.Diagnostic == "" {
		t.Fatal("diagnostic is empty for unsupported override")
	}
}
