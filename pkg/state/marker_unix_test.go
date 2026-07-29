//go:build darwin || linux

package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPinnedLocalMarkerRejectsDirectoryReplacementAroundCommit(t *testing.T) {
	for _, phase := range []string{"before commit", "after commit"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			store, err := New(root, "markers")
			if err != nil {
				t.Fatal(err)
			}
			const namespace = ".cloudsql-local-runtime"
			if err := store.WriteLocalMarker(namespace, "seed", []byte("seed\n")); err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(store.ProfileDir(), namespace)
			moved := directory + ".moved"
			external := t.TempDir()
			replace := func() {
				if err := os.Rename(directory, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, directory); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "before commit" {
				store.beforeLocalMarkerCommit = replace
			} else {
				store.afterLocalMarkerCommit = replace
			}
			if err := store.WriteLocalMarker(namespace, "new", []byte("generation\n")); err == nil {
				t.Fatal("marker write accepted a replaced marker directory")
			}
			if _, err := os.Stat(filepath.Join(external, "new")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("replacement redirected marker into external directory: %v", err)
			}
			if err := os.Remove(directory); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(moved, directory); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPinnedLocalMarkerRejectsSymlinkReplacement(t *testing.T) {
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
		t.Fatal(err)
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
		t.Fatal(err)
	}
	if _, _, err := store.ReadLocalMarker(namespace, name); err == nil {
		t.Fatal("marker read followed a symlink replacement")
	}
	if err := store.RemoveLocalMarker(namespace, name, payload); err == nil {
		t.Fatal("marker removal followed a symlink replacement")
	}
	if got, err := os.ReadFile(external); err != nil || string(got) != string(payload) {
		t.Fatalf("external marker changed: payload=%q err=%v", got, err)
	}
}
