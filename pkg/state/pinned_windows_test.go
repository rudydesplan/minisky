//go:build windows

package state

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsPinnedDirectoriesSupportStateLifecycle(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "windows")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("service/value", "saved"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	var got string
	if err := store.Load("service/value", &got); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "saved" {
		t.Fatalf("Load = %q, want saved", got)
	}
	if err := store.Delete("service/value"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := store.Load("service/value", &got); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load deleted error = %v, want ErrNotFound", err)
	}
	snapshot := bytes.NewBufferString(`{"format":"minisky-state","version":1,"entries":{"service/imported":true}}`)
	if err := store.Import(snapshot); err != nil {
		t.Fatalf("Import: %v", err)
	}
}

func TestWindowsOwnershipDeniesDirectoryReplacement(t *testing.T) {
	store, err := New(t.TempDir(), "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	if err := os.Rename(store.ProfileDir(), store.ProfileDir()+".replaced"); err == nil {
		t.Fatal("profile directory rename succeeded while ownership was held")
	}
	if err := os.Rename(store.lockDir, store.lockDir+".replaced"); err == nil {
		t.Fatal("lock directory rename succeeded while ownership was held")
	}
	if err := store.Save("service/value", true); err != nil {
		t.Fatalf("owner Save: %v", err)
	}
}

func TestWindowsPinnedSpoolReconciliation(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	spoolPath := filepath.Join(store.ProfileDir(), "request-spool")
	if err := os.MkdirAll(spoolPath, 0o700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(spoolPath, ".request-stale.tmp")
	if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileOwnedSpools(ownership, OwnedSpoolSpec{
		Directory: "request-spool",
		Prefixes:  []string{".request-"},
	}); err != nil {
		t.Fatalf("ReconcileOwnedSpools: %v", err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale spool still exists or stat failed: %v", err)
	}

	spool, err := OpenOwnedSpoolDirectory(root, "active", "request-spool")
	if err != nil {
		t.Fatal(err)
	}
	file, name, err := spool.CreateTemp(".request-")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := spool.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsStateReadRejectsReparsePoint(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "reparse")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.ProfileDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "external.json")
	if err := os.WriteFile(external, []byte(`{"format":"minisky-state-store","version":1,"entries":{"escaped":true}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(store.ProfileDir(), stateFileName)); err != nil {
		t.Skipf("creating symlink requires Windows developer mode or privilege: %v", err)
	}
	var value bool
	if err := store.Load("escaped", &value); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("Load reparse-point state error = %v, want ErrInvalidPath", err)
	}
}

func TestWindowsProfilePreparationRejectsReparsePointParent(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	if err := os.Symlink(external, filepath.Join(root, "profiles")); err != nil {
		t.Skipf("creating symlink requires Windows developer mode or privilege: %v", err)
	}
	store, err := New(root, "escaped")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("service/value", true); err == nil {
		t.Fatal("Save accepted a reparse-point profiles parent")
	}
	if _, err := os.Stat(filepath.Join(external, "escaped")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("external profile exists or stat failed: %v", err)
	}
}

func TestWindowsSpoolRejectsReparsePointEntry(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "reparse")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()
	spoolPath := filepath.Join(store.ProfileDir(), "request-spool")
	if err := os.MkdirAll(spoolPath, 0o700); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "outside.tmp")
	if err := os.WriteFile(external, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(spoolPath, ".request-link.tmp")); err != nil {
		t.Skipf("creating symlink requires Windows developer mode or privilege: %v", err)
	}
	err = store.ReconcileOwnedSpools(ownership, OwnedSpoolSpec{
		Directory: "request-spool",
		Prefixes:  []string{".request-"},
	})
	if !errors.Is(err, ErrUnsafeSpoolPath) && !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("Reconcile reparse-point spool error = %v", err)
	}
	if payload, err := os.ReadFile(external); err != nil || string(payload) != "outside" {
		t.Fatalf("external target changed: payload=%q err=%v", payload, err)
	}
}
