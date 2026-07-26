package state

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestOwnedSpoolInvalidatedBeforeImportAndNewOwner(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	old, err := OpenOwnedSpoolDirectory(root, "active", "request-spool")
	if err != nil {
		t.Fatal(err)
	}
	openFile, _, err := old.CreateTemp(".request-")
	if err != nil {
		t.Fatal(err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openFile.Write([]byte("after close")); err == nil {
		t.Fatal("temporary file remained writable after ownership close")
	}
	if _, _, err := old.CreateTemp(".request-"); !errors.Is(err, ErrUnsafeSpoolPath) {
		t.Fatalf("old CreateTemp error = %v, want ErrUnsafeSpoolPath", err)
	}

	snapshot := bytes.NewBufferString(`{"format":"minisky-state","version":1,"entries":{}}`)
	if err := store.Import(snapshot); err != nil {
		t.Fatal(err)
	}
	newOwnership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer newOwnership.Close()
	current, err := OpenOwnedSpoolDirectory(root, "active", "request-spool")
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	currentFile, currentName, err := current.CreateTemp(".request-")
	if err != nil {
		t.Fatal(err)
	}
	if err := currentFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := old.Remove(currentName); !errors.Is(err, ErrUnsafeSpoolPath) {
		t.Fatalf("old Remove error = %v, want ErrUnsafeSpoolPath", err)
	}
	names, err := current.List(".request-")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("old handle removed a new ownership generation file")
	}
	if err := old.Close(); err != nil {
		t.Fatalf("close invalidated spool: %v", err)
	}
}

func TestOwnedSpoolConcurrentCloseInvalidatesOperations(t *testing.T) {
	root := t.TempDir()
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
		t.Fatal(err)
	}
	defer spool.Close()

	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 100 {
				file, _, err := spool.CreateTemp(".request-")
				if err != nil {
					if !errors.Is(err, ErrUnsafeSpoolPath) {
						t.Errorf("CreateTemp during close: %v", err)
					}
					return
				}
				_ = file.Close()
			}
		}()
	}
	close(start)
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	if _, err := spool.List(".request-"); !errors.Is(err, ErrUnsafeSpoolPath) {
		t.Fatalf("List after ownership close error = %v, want ErrUnsafeSpoolPath", err)
	}
}

func TestOwnedSpoolCleanShutdown(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	spool, err := OpenOwnedSpoolDirectory(root, "active", "uploads")
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
}
