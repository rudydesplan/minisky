package config

import (
	"path/filepath"
	"regexp"
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
	if registry.Composer.AirflowImage == "" {
		t.Error("fallback Composer Airflow image is empty")
	}
}

func TestComposerAirflowImageIsCanonicalDigestPinned(t *testing.T) {
	t.Parallel()

	const want = "apache/airflow:2.10.5-python3.12@sha256:6499a680a93463846d3a6be980e85d601dc97b0d81e82eed9ef5e5cb9da31b79"
	pinnedImage := regexp.MustCompile(`^[^[:space:]@]+:[^[:space:]@]+@sha256:[0-9a-f]{64}$`)
	for name, registry := range map[string]*ImageRegistry{
		"embedded": loadRegistry(),
		"fallback": fallbackRegistry(),
	} {
		t.Run(name, func(t *testing.T) {
			if got := registry.Composer.AirflowImage; got != want {
				t.Fatalf("Composer Airflow image = %q, want canonical reference %q", got, want)
			}
			if !pinnedImage.MatchString(registry.Composer.AirflowImage) {
				t.Fatalf("Composer Airflow image is not tag-and-digest pinned: %q", registry.Composer.AirflowImage)
			}
		})
	}
}

func TestRedis72ValkeyImageIsCanonicalDigestPinned(t *testing.T) {
	t.Parallel()

	const want = "valkey/valkey:7.2.12-alpine@sha256:28ca383369c5497fb4d63092e852a1c9e23c5a0b5553bb8f0f54a0b7fa0ddd4b"
	pinnedImage := regexp.MustCompile(`^[^[:space:]@]+:[^[:space:]@]+@sha256:[0-9a-f]{64}$`)
	for name, registry := range map[string]*ImageRegistry{
		"embedded": loadRegistry(),
		"fallback": fallbackRegistry(),
	} {
		t.Run(name, func(t *testing.T) {
			if got := registry.Memorystore.Valkey.DefaultImage; got != want {
				t.Fatalf("Valkey default image = %q, want canonical linux/amd64 reference %q", got, want)
			}
			if !pinnedImage.MatchString(registry.Memorystore.Valkey.DefaultImage) {
				t.Fatalf("Valkey default image is not tag-and-digest pinned: %q", registry.Memorystore.Valkey.DefaultImage)
			}
			for _, version := range registry.Memorystore.Valkey.Versions {
				if version.Version != "7.2" {
					continue
				}
				if version.Image != want || version.Platform != "linux/amd64" {
					t.Fatalf("Valkey 7.2 immutable identity = %#v", version)
				}
			}
			if got := registry.Memorystore.Redis.DefaultImage; got != want {
				t.Fatalf("Redis fallback image = %q, want Valkey-backed canonical reference %q", got, want)
			}
		})
	}
}

func TestEmbeddedMemcachedImagesMatchProviderVersions(t *testing.T) {
	t.Parallel()
	for name, registry := range map[string]*ImageRegistry{
		"embedded": loadRegistry(),
		"fallback": fallbackRegistry(),
	} {
		t.Run(name, func(t *testing.T) {
			images := make(map[string]string, len(registry.Memorystore.Memcached.Versions))
			for _, version := range registry.Memorystore.Memcached.Versions {
				images[version.Version] = version.Image
			}
			for version, wantImage := range map[string]string{
				"1.5.16": "memcached:1.5.16-alpine",
				"1.6.15": "memcached:1.6.15-alpine",
			} {
				if got := images[version]; got != wantImage {
					t.Errorf("Memcached %s image = %q, want %q", version, got, wantImage)
				}
			}
		})
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
