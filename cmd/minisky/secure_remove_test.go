package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSecureRemoveRejectsAncestorSymlinkEscape(t *testing.T) {
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(filepath.Join(outside, "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(outside, "state", "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "approved", "linked")
	if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := secureRemoveAll(filepath.Join(link, "state")); err == nil {
		t.Fatal("ancestor symlink escape was accepted")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("escaped state was modified: %v", err)
	}
}

func TestSecureRemoveRefusesSwappedTarget(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "state")
	moved := filepath.Join(parent, "moved-state")
	replacement := filepath.Join(parent, "replacement")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "old"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(replacement, 0o700); err != nil {
		t.Fatal(err)
	}
	replacementMarker := filepath.Join(replacement, "keep")
	if err := os.WriteFile(replacementMarker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := secureRemoveAllWithHook(target, func() {
		if renameErr := os.Rename(target, moved); renameErr != nil {
			t.Fatal(renameErr)
		}
		if renameErr := os.Rename(replacement, target); renameErr != nil {
			t.Fatal(renameErr)
		}
	})
	if err == nil {
		t.Fatal("swapped target was removed")
	}
	if _, statErr := os.Stat(filepath.Join(target, "keep")); statErr != nil {
		t.Fatalf("replacement target was modified: %v", statErr)
	}
	if _, statErr := os.Stat(moved); !errors.Is(statErr, os.ErrNotExist) && statErr != nil {
		t.Fatalf("unexpected moved target state: %v", statErr)
	}
}

func TestSecureRemovePinsAncestorAcrossSwap(t *testing.T) {
	parent := t.TempDir()
	ancestor := filepath.Join(parent, "ancestor")
	target := filepath.Join(ancestor, "state")
	movedAncestor := filepath.Join(parent, "moved-ancestor")
	replacementAncestor := filepath.Join(parent, "replacement-ancestor")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "old"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(replacementAncestor, "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	replacementMarker := filepath.Join(replacementAncestor, "state", "keep")
	if err := os.WriteFile(replacementMarker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := secureRemoveAllWithHook(target, func() {
		if renameErr := os.Rename(ancestor, movedAncestor); renameErr != nil {
			t.Fatal(renameErr)
		}
		if renameErr := os.Rename(replacementAncestor, ancestor); renameErr != nil {
			t.Fatal(renameErr)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(ancestor, "state", "keep")); err != nil {
		t.Fatalf("replacement ancestor was modified: %v", err)
	}
}

func TestSecureRemoveRejectsAncestorSwapBetweenCheckAndOpen(t *testing.T) {
	parent := t.TempDir()
	ancestor := filepath.Join(parent, "ancestor")
	moved := filepath.Join(parent, "moved")
	target := filepath.Join(ancestor, "state")
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(outside, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	swapped := false
	err := secureRemoveAllWithAncestryHook(target, func(component string) {
		if swapped || component != "ancestor" {
			return
		}
		swapped = true
		if renameErr := os.Rename(ancestor, moved); renameErr != nil {
			t.Fatal(renameErr)
		}
		if symlinkErr := os.Symlink(outside, ancestor); symlinkErr != nil {
			t.Fatal(symlinkErr)
		}
	})
	if err == nil {
		t.Fatal("ancestor swap was accepted")
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("swapped ancestor escaped trusted removal root: %v", statErr)
	}
}
