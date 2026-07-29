//go:build windows

package state

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsPinnedLocalMarkerLifecycle(t *testing.T) {
	store, err := New(t.TempDir(), "markers")
	if err != nil {
		t.Fatal(err)
	}
	const (
		namespace = ".cloudsql-local-runtime"
		name      = "generation"
	)
	first := []byte("first-generation\n")
	second := []byte("second-generation\n")

	if err := store.WriteLocalMarker(namespace, name, first); err != nil {
		t.Fatalf("WriteLocalMarker: %v", err)
	}
	payload, found, err := store.ReadLocalMarker(namespace, name)
	if err != nil || !found || !bytes.Equal(payload, first) {
		t.Fatalf("ReadLocalMarker found=%t payload=%q err=%v", found, payload, err)
	}
	if err := store.RemoveLocalMarker(namespace, name, second); !errors.Is(err, ErrMarkerMismatch) {
		t.Fatalf("mismatched RemoveLocalMarker error=%v, want ErrMarkerMismatch", err)
	}
	payload, found, err = store.ReadLocalMarker(namespace, name)
	if err != nil || !found || !bytes.Equal(payload, first) {
		t.Fatalf("mismatched removal changed marker found=%t payload=%q err=%v", found, payload, err)
	}
	if err := store.RemoveLocalMarker(namespace, name, first); err != nil {
		t.Fatalf("matching RemoveLocalMarker: %v", err)
	}
	if err := store.RemoveLocalMarker(namespace, name, first); err != nil {
		t.Fatalf("repeated RemoveLocalMarker: %v", err)
	}
	if _, found, err := store.ReadLocalMarker(namespace, name); err != nil || found {
		t.Fatalf("removed marker found=%t err=%v", found, err)
	}
}

func TestWindowsPinnedLocalMarkerRemovalDeletesOpenedObject(t *testing.T) {
	store, err := New(t.TempDir(), "markers")
	if err != nil {
		t.Fatal(err)
	}
	const (
		namespace = ".cloudsql-local-runtime"
		name      = "generation"
	)
	original := []byte("original-generation\n")
	replacement := []byte("replacement-generation\n")
	if err := store.WriteLocalMarker(namespace, name, original); err != nil {
		t.Fatalf("WriteLocalMarker: %v", err)
	}

	profileDir, err := openPinnedDirectory(store.ProfileDir())
	if err != nil {
		t.Fatalf("open profile directory: %v", err)
	}
	defer profileDir.close()
	markerDir, err := profileDir.openDirectory(namespace, false)
	if err != nil {
		t.Fatalf("open marker directory: %v", err)
	}
	defer markerDir.close()

	directory := filepath.Join(store.ProfileDir(), namespace)
	if err := os.Rename(directory, directory+".replaced"); err == nil {
		t.Fatal("marker directory replacement succeeded while its handle was pinned")
	}

	file, err := markerDir.openRegularFileForRemoval(name)
	if err != nil {
		t.Fatalf("open marker for removal: %v", err)
	}
	payload, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		t.Fatalf("read opened marker: %v", err)
	}
	if !bytes.Equal(payload, original) {
		file.Close()
		t.Fatalf("opened marker payload=%q, want %q", payload, original)
	}

	marker := filepath.Join(directory, name)
	retired := filepath.Join(directory, "retired-generation")
	if err := os.Rename(marker, retired); err != nil {
		file.Close()
		t.Fatalf("replace marker after opening deletion handle: %v", err)
	}
	if err := os.WriteFile(marker, replacement, 0o600); err != nil {
		file.Close()
		t.Fatalf("write replacement marker: %v", err)
	}
	if err := markerDir.removeOpenFile(file, name); err != nil {
		file.Close()
		t.Fatalf("remove opened marker object: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close removed marker: %v", err)
	}

	if payload, err := os.ReadFile(marker); err != nil || !bytes.Equal(payload, replacement) {
		t.Fatalf("replacement marker changed: payload=%q err=%v", payload, err)
	}
	if _, err := os.Stat(retired); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opened marker object still exists: %v", err)
	}
}

func TestWindowsPinnedLocalMarkerRejectsReparsePoint(t *testing.T) {
	store, err := New(t.TempDir(), "markers")
	if err != nil {
		t.Fatal(err)
	}
	const (
		namespace = ".cloudsql-local-runtime"
		name      = "generation"
	)
	payload := []byte("generation\n")
	if err := store.WriteLocalMarker(namespace, name, payload); err != nil {
		t.Fatalf("WriteLocalMarker: %v", err)
	}

	marker := filepath.Join(store.ProfileDir(), namespace, name)
	external := filepath.Join(t.TempDir(), "external")
	if err := os.WriteFile(external, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, marker); err != nil {
		t.Skipf("creating symlink requires Windows developer mode or privilege: %v", err)
	}
	if _, _, err := store.ReadLocalMarker(namespace, name); err == nil {
		t.Fatal("ReadLocalMarker followed a reparse point")
	}
	if err := store.RemoveLocalMarker(namespace, name, payload); err == nil {
		t.Fatal("RemoveLocalMarker followed a reparse point")
	}
	if got, err := os.ReadFile(external); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("external marker changed: payload=%q err=%v", got, err)
	}
}

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
