package main

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestDaemonIdentityRemovalIsExactAndOwnershipProtected(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	store, err := state.New(root, "ordering")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	oldIdentity := mustDaemonIdentity(t, "ordering")
	newIdentity := oldIdentity
	newIdentity.ControlToken = strings.Repeat("a", 64)
	if err := writeDaemonIdentity(store.ProfileDir(), newIdentity); err != nil {
		t.Fatal(err)
	}
	if err := removeDaemonIdentity(store.ProfileDir(), oldIdentity); err == nil {
		t.Fatal("old daemon removed a replacement identity")
	}
	if got, err := readDaemonIdentity(store.ProfileDir()); err != nil || !reflect.DeepEqual(got, newIdentity) {
		t.Fatalf("replacement identity changed: got=%#v err=%v", got, err)
	}
	if err := removeDaemonIdentity(store.ProfileDir(), newIdentity); err != nil {
		t.Fatal(err)
	}
	competitor, err := state.New(root, "ordering")
	if err != nil {
		t.Fatal(err)
	}
	if competitorOwnership, err := competitor.AcquireOwnership(); !errors.Is(err, state.ErrProfileInUse) {
		if err == nil {
			competitorOwnership.Close()
		}
		t.Fatalf("profile lock released before identity removal completed: %v", err)
	}
}

func TestInactiveIdentityClearsBeforeOwnershipRelease(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	store, err := state.New(root, "inactive-ordering")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDaemonIdentity(store.ProfileDir(), mustDaemonIdentity(t, "inactive-ordering")); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
	err = clearInactiveDaemonIdentity(store, func() {
		if _, statErr := os.Stat(daemonIdentityPath(store.ProfileDir())); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("identity still exists before ownership release: %v", statErr)
		}
		competitor, newErr := state.New(root, "inactive-ordering")
		if newErr != nil {
			t.Fatal(newErr)
		}
		if competitorOwnership, acquireErr := competitor.AcquireOwnership(); !errors.Is(acquireErr, state.ErrProfileInUse) {
			if acquireErr == nil {
				competitorOwnership.Close()
			}
			t.Fatalf("ownership released before stale identity removal: %v", acquireErr)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func mustDaemonIdentity(t *testing.T, profile string) daemonIdentity {
	t.Helper()
	identity, err := newDaemonIdentity(profile)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}
