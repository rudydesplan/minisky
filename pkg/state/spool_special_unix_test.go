//go:build darwin || linux

package state

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestOwnedSpoolReadFileRejectsFIFOWithoutBlockingOwnershipClose(t *testing.T) {
	root := t.TempDir()
	store, ownership, spool := openTestOwnedSpool(t, root)
	name := ".request-fifo.tmp"
	path := filepath.Join(store.ProfileDir(), "request-spool", name)
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := spool.ReadFile(name, 1024)
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, ErrUnsafeSpoolPath) {
			t.Fatalf("ReadFile FIFO error = %v, want ErrUnsafeSpoolPath", err)
		}
	case <-time.After(500 * time.Millisecond):
		writer, err := unix.Open(path, unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if err == nil {
			_ = unix.Close(writer)
		}
		<-result
		t.Fatal("ReadFile blocked opening a FIFO")
	}

	closed := make(chan error, 1)
	go func() { closed <- ownership.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Ownership.Close blocked after FIFO rejection")
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedSpoolReadFileRejectsSocketAndSymlink(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "minisky-spool-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	store, ownership, spool := openTestOwnedSpool(t, root)
	defer ownership.Close()
	defer spool.Close()
	spoolPath := filepath.Join(store.ProfileDir(), "request-spool")

	socketName := ".request-socket.tmp"
	listener, err := net.Listen("unix", filepath.Join(spoolPath, socketName))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	external := filepath.Join(t.TempDir(), "outside.tmp")
	if err := os.WriteFile(external, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlinkName := ".request-symlink.tmp"
	if err := os.Symlink(external, filepath.Join(spoolPath, symlinkName)); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{socketName, symlinkName} {
		if _, err := spool.ReadFile(name, 1024); !errors.Is(err, ErrUnsafeSpoolPath) {
			t.Fatalf("ReadFile(%s) error = %v, want ErrUnsafeSpoolPath", name, err)
		}
	}
}

func openTestOwnedSpool(
	t *testing.T,
	root string,
) (*Store, *Ownership, *OwnedSpoolDirectory) {
	t.Helper()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	spool, err := OpenOwnedSpoolDirectory(root, "active", "request-spool")
	if err != nil {
		ownership.Close()
		t.Fatal(err)
	}
	return store, ownership, spool
}
