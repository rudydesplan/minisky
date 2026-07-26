//go:build darwin || linux

package state

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestProfileReplacementAtWriteWindowCannotRedirectAtomicWrite(t *testing.T) {
	if os.Getenv("MINISKY_STATE_WRITE_WINDOW_HELPER") == "1" {
		runWriteWindowReplacementHelper(t)
		return
	}

	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("daemon/value", "before"); err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	ready := filepath.Join(root, "write-ready")
	release := filepath.Join(root, "write-release")
	store.beforeStateReplace = func() {
		if err := os.WriteFile(ready, nil, 0o600); err != nil {
			t.Errorf("signal write window: %v", err)
			return
		}
		waitForTestFile(t, release)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestProfileReplacementAtWriteWindowCannotRedirectAtomicWrite$")
	command.Env = append(os.Environ(),
		"MINISKY_STATE_WRITE_WINDOW_HELPER=1",
		"MINISKY_STATE_ROOT="+root,
		"MINISKY_STATE_WRITE_READY="+ready,
		"MINISKY_STATE_WRITE_RELEASE="+release,
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	saveErr := store.Save("daemon/value", "after")
	if err := command.Wait(); err != nil {
		t.Fatalf("write-window helper: %v\n%s", err, output.String())
	}
	if !errors.Is(saveErr, ErrProfileReplaced) {
		t.Fatalf("Save error = %v, want ErrProfileReplaced", saveErr)
	}
	if _, err := os.Stat(filepath.Join(store.ProfileDir(), stateFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement profile state exists or stat failed: %v", err)
	}
	replacedState := filepath.Join(store.ProfileDir()+".replaced", stateFileName)
	payload, err := os.ReadFile(replacedState)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte(`"before"`)) || bytes.Contains(payload, []byte(`"after"`)) {
		t.Fatalf("failed write changed pinned profile: %s", payload)
	}
}

func runWriteWindowReplacementHelper(t *testing.T) {
	root := os.Getenv("MINISKY_STATE_ROOT")
	ready := os.Getenv("MINISKY_STATE_WRITE_READY")
	release := os.Getenv("MINISKY_STATE_WRITE_RELEASE")
	waitForTestFile(t, ready)

	profileDir := filepath.Join(root, "profiles", "active")
	replacedDir := profileDir + ".replaced"
	if err := os.Rename(profileDir, replacedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(profileDir, 0o700); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(replacedDir, ".state-*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("temporary state files = %v, want one", matches)
	}
	if err := os.Rename(matches[0], filepath.Join(profileDir, filepath.Base(matches[0]))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForTestFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLockAnchorPreparationRejectsPlantedSymlink(t *testing.T) {
	for _, anchor := range []string{"owner", "transaction"} {
		t.Run(anchor, func(t *testing.T) {
			root := t.TempDir()
			store, err := New(root, "active")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(store.lockDir, 0o700); err != nil {
				t.Fatal(err)
			}
			external := t.TempDir()
			path := store.ownerAnchor
			if anchor == "transaction" {
				path = store.stateAnchor
			}
			if err := os.Symlink(external, path); err != nil {
				t.Fatal(err)
			}
			if err := store.Save("service/value", true); err == nil {
				t.Fatal("Save accepted a symlinked lock anchor")
			}
			if _, err := os.Stat(filepath.Join(external, lockAnchorMarker)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("external marker exists or stat failed: %v", err)
			}
		})
	}
}

func TestProfilePreparationRejectsSymlinkedProfilesParent(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	if err := os.Symlink(external, filepath.Join(root, "profiles")); err != nil {
		t.Fatal(err)
	}
	store, err := New(root, "escaped")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("service/value", true); err == nil {
		t.Fatal("Save accepted a symlinked profiles parent")
	}
	if _, err := os.Stat(filepath.Join(external, "escaped")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("external profile exists or stat failed: %v", err)
	}
}

func TestPinnedIdentityRejectsRenameAndSymlinkBack(t *testing.T) {
	for _, component := range []string{
		"root", "profiles", "profile", "locks", "owner-anchor", "state-anchor",
	} {
		t.Run(component, func(t *testing.T) {
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

			path := root
			switch component {
			case "profiles":
				path = filepath.Join(root, "profiles")
			case "profile":
				path = store.ProfileDir()
			case "locks":
				path = store.lockDir
			case "owner-anchor":
				path = store.ownerAnchor
			case "state-anchor":
				path = store.stateAnchor
			}
			moved := path + ".moved"
			if err := os.Rename(path, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(moved, path); err != nil {
				t.Fatal(err)
			}
			if component == "root" {
				t.Cleanup(func() {
					_ = os.Remove(path)
					_ = os.RemoveAll(moved)
				})
			}
			if err := store.Save("service/value", true); err == nil {
				t.Fatalf("Save accepted renamed and symlinked-back %s", component)
			}
		})
	}
}
