//go:build darwin || linux

package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLoadAndExportWorkFromReadOnlyStateTree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test requires a non-root user")
	}

	root := t.TempDir()
	store, err := New(root, "readonly")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("service/value", map[string]string{"value": "preserved"}); err != nil {
		t.Fatal(err)
	}
	if err := setStateTreePermissions(root, 0o500, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := setStateTreePermissions(root, 0o700, 0o600); err != nil {
			t.Errorf("restore state permissions: %v", err)
		}
	})

	var got map[string]string
	if err := store.Load("service/value", &got); err != nil {
		t.Fatalf("Load from read-only state: %v", err)
	}
	if got["value"] != "preserved" {
		t.Fatalf("loaded value = %#v", got)
	}
	var snapshot bytes.Buffer
	if err := store.Export(&snapshot); err != nil {
		t.Fatalf("Export from read-only state: %v", err)
	}
	if !bytes.Contains(snapshot.Bytes(), []byte(`"value": "preserved"`)) {
		t.Fatalf("exported snapshot = %s", snapshot.Bytes())
	}
	if err := store.Save("service/blocked", true); err == nil {
		t.Fatal("Save unexpectedly succeeded in read-only state")
	}
}

func TestReadOnlyEmptyProfilePreservesEmptySemantics(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test requires a non-root user")
	}

	root := t.TempDir()
	store, err := New(root, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := setStateTreePermissions(root, 0o500, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := setStateTreePermissions(root, 0o700, 0o600); err != nil {
			t.Errorf("restore state permissions: %v", err)
		}
	})
	var value bool
	if err := store.Load("missing/value", &value); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load empty read-only profile error = %v, want ErrNotFound", err)
	}
	var output bytes.Buffer
	if err := store.Export(&output); err != nil {
		t.Fatalf("Export empty read-only profile: %v", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(output.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Format != SnapshotFormat || snapshot.Version != Version || len(snapshot.Entries) != 0 {
		t.Fatalf("empty snapshot = %#v", snapshot)
	}
}

func TestReadOnlyFreshRootPreservesEmptySemantics(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test requires a non-root user")
	}
	root := t.TempDir()
	store, err := New(root, "fresh")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(root, 0o700)
	})

	var value bool
	if err := store.Load("missing/value", &value); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load fresh read-only root error = %v, want ErrNotFound", err)
	}
	var output bytes.Buffer
	if err := store.Export(&output); err != nil {
		t.Fatalf("Export fresh read-only root: %v", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(output.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Format != SnapshotFormat || snapshot.Version != Version || len(snapshot.Entries) != 0 {
		t.Fatalf("empty snapshot = %#v", snapshot)
	}
	if _, err := os.Stat(filepath.Join(root, "profiles")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only fallback created profiles or stat failed: %v", err)
	}
}

func TestReadOnlyFallbackRejectsPartiallyWritableTree(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test requires a non-root user")
	}

	root := t.TempDir()
	store, err := New(root, "partial")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.lockDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(store.ProfileDir(), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(store.lockDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(store.lockDir, 0o700)
		_ = os.Chmod(store.ProfileDir(), 0o700)
	})

	var value bool
	err = store.Load("missing/value", &value)
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("Load partially writable tree error = %v, want lock permission failure", err)
	}
}

func TestReadOnlyClassificationUsesEffectiveCredentialSeam(t *testing.T) {
	store, err := New(t.TempDir(), "effective")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	var checked []string
	store.effectiveWriteAccess = func(path string) error {
		checked = append(checked, path)
		return unix.EACCES
	}
	if !isConfirmedReadOnlyAccess(store, os.ErrPermission) {
		t.Fatal("effective credential denial was not classified read-only")
	}
	if len(checked) == 0 {
		t.Fatal("effective credential seam was not used")
	}
}

func TestReadOnlyFallbackFailsIfPermissionsChangeAfterPin(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test requires a non-root user")
	}
	root := t.TempDir()
	store, err := New(root, "changing")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := setStateTreePermissions(root, 0o500, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = setStateTreePermissions(root, 0o700, 0o600)
	})
	store.beforeReadOnlyRecheck = func() {
		if err := os.Chmod(root, 0o700); err != nil {
			t.Errorf("make root writable: %v", err)
		}
		if err := os.Chmod(store.ProfileDir(), 0o700); err != nil {
			t.Errorf("make profile writable: %v", err)
		}
	}

	var value bool
	err = store.Load("missing/value", &value)
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("Load after permission change error = %v, want fail closed", err)
	}
}

func setStateTreePermissions(root string, directoryMode, fileMode fs.FileMode) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return os.Chmod(path, directoryMode)
		}
		return os.Chmod(path, fileMode)
	})
}
