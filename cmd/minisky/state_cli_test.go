package main

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestStateExportImportCommands(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source, err := state.New(root, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Save("bigquery/metadata", map[string]string{"dataset": "analytics"}); err != nil {
		t.Fatal(err)
	}

	snapshotPath := filepath.Join(t.TempDir(), "snapshot.json")
	exportCommand := newStateCommand(root)
	exportCommand.SetArgs([]string{"export", snapshotPath, "--profile", "source"})
	exportCommand.SetOut(&bytes.Buffer{})
	exportCommand.SetErr(&bytes.Buffer{})
	if err := exportCommand.Execute(); err != nil {
		t.Fatalf("state export failed: %v", err)
	}

	target, err := state.New(root, "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Save("existing/value", map[string]bool{"present": true}); err != nil {
		t.Fatal(err)
	}

	importCommand := newStateCommand(root)
	importCommand.SetArgs([]string{"import", snapshotPath, "--profile", "target"})
	importCommand.SetOut(&bytes.Buffer{})
	importCommand.SetErr(&bytes.Buffer{})
	if err := importCommand.Execute(); err != nil {
		t.Fatalf("state import failed: %v", err)
	}

	var got map[string]string
	if err := target.Load("bigquery/metadata", &got); err != nil {
		t.Fatal(err)
	}
	if got["dataset"] != "analytics" {
		t.Fatalf("imported metadata = %#v", got)
	}
	if err := target.Load("existing/value", &map[string]bool{}); err == nil {
		t.Fatal("import did not replace the target profile snapshot")
	}
}

func TestStateImportInvalidSnapshotPreservesProfile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target, err := state.New(root, "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Save("existing/value", map[string]bool{"present": true}); err != nil {
		t.Fatal(err)
	}

	command := newStateCommand(root)
	command.SetArgs([]string{"import", "-", "--profile", "target"})
	command.SetIn(bytes.NewBufferString(`{"format":"wrong","version":1,"entries":{}}`))
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	if err := command.Execute(); err == nil {
		t.Fatal("state import accepted an invalid snapshot")
	}

	var got map[string]bool
	if err := target.Load("existing/value", &got); err != nil {
		t.Fatal(err)
	}
	if !got["present"] {
		t.Fatalf("active profile changed: %#v", got)
	}
}

func TestStateImportRejectsActiveDaemonProfile(t *testing.T) {
	root := t.TempDir()
	store, err := state.New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	command := newStateCommand(root)
	command.SetArgs([]string{"import", "-", "--profile", "active"})
	command.SetIn(bytes.NewBufferString(`{"format":"minisky-state","version":1,"entries":{}}`))
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	err = command.Execute()
	if !errors.Is(err, state.ErrProfileInUse) {
		t.Fatalf("state import error = %v, want ErrProfileInUse", err)
	}
	if !strings.Contains(err.Error(), "state profile is in use: active") {
		t.Fatalf("state import error = %q, want profile context", err)
	}
}
