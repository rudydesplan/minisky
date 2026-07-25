package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestEmbeddedImageRegistryLoads(t *testing.T) {
	t.Parallel()

	registry := loadRegistry()
	if registry == nil {
		t.Fatal("loadRegistry returned nil")
	}
	if len(registry.Emulators) == 0 {
		t.Fatal("embedded image registry has no emulators")
	}

	for _, domain := range []string{
		"firestore.googleapis.com",
		"datastore.googleapis.com",
		"spanner.googleapis.com",
	} {
		emulator, ok := registry.Emulators[domain]
		if !ok {
			t.Errorf("missing lazy emulator configuration for %s", domain)
			continue
		}
		if emulator.Name == "" || emulator.Image == "" || emulator.Port == "" {
			t.Errorf("incomplete emulator configuration for %s: %+v", domain, emulator)
		}
	}
}

func TestFirestoreEmulatorUsesSupportedImportExportFlags(t *testing.T) {
	t.Parallel()

	command := strings.Join(loadRegistry().Emulators["firestore.googleapis.com"].Cmd, " ")
	if strings.Contains(command, "--data-dir") {
		t.Fatal("Firestore emulator does not support --data-dir")
	}
	for _, flag := range []string{"--import-data", "--export-on-exit"} {
		if !strings.Contains(command, flag) {
			t.Errorf("Firestore emulator command is missing %s", flag)
		}
	}
}

func TestNativeKMSDoesNotDeclareUnusedDockerEmulator(t *testing.T) {
	t.Parallel()

	registry := loadRegistry()
	if _, exists := registry.Emulators["cloudkms.googleapis.com"]; exists {
		t.Fatal("native Cloud KMS shim should not declare an unused Docker emulator")
	}
}

func TestFallbackRegistryHasUsableDefaults(t *testing.T) {
	t.Parallel()

	registry := fallbackRegistry()
	if registry.Compute.DefaultImage == "" {
		t.Error("fallback compute image is empty")
	}
	if registry.Sql.Postgres.DefaultImage == "" || registry.Sql.Mysql.DefaultImage == "" {
		t.Error("fallback SQL images are incomplete")
	}
	if registry.Serverless.Builder == "" {
		t.Error("fallback serverless builder is empty")
	}
}

func TestProfilePathsAreStateRootScoped(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "restart")

	wantProfile := filepath.Join(root, "profiles", "restart")
	if got := GetProfileDir(); got != wantProfile {
		t.Fatalf("profile dir = %q, want %q", got, wantProfile)
	}
	if got := GetRuntimeDir(); got != filepath.Join(wantProfile, "runtime") {
		t.Fatalf("runtime dir = %q", got)
	}
}
