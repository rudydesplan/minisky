package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func TestStateImportCLIUsesExplicitDestinationProfileForGKEOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ambient")
	root := t.TempDir()
	valid := gkeSnapshotForCLITest(t, "destination")
	command := newStateCommand(root)
	command.SetArgs([]string{"import", "-", "--profile", "destination"})
	command.SetIn(bytes.NewReader(valid))
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	if err := command.Execute(); err != nil {
		t.Fatalf("valid destination import failed: %v", err)
	}

	command = newStateCommand(root)
	command.SetArgs([]string{"import", "-", "--profile", "destination"})
	command.SetIn(bytes.NewReader(gkeSnapshotForCLITest(t, "ambient")))
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	if err := command.Execute(); err == nil {
		t.Fatal("ambient-profile ownership was accepted for explicit destination")
	}

	store, err := state.New(root, "destination")
	if err != nil {
		t.Fatal(err)
	}
	var metadata struct {
		Ownerships map[string]struct {
			Profile string `json:"profile"`
		} `json:"kubeconfigOwnerships"`
	}
	if err := store.Load("gke/metadata", &metadata); err != nil {
		t.Fatal(err)
	}
	if got := metadata.Ownerships["demo:us-central1-c:cluster"].Profile; got != "destination" {
		t.Fatalf("rejected CLI import replaced destination ownership with %q", got)
	}
}

func gkeSnapshotForCLITest(t *testing.T, profile string) []byte {
	t.Helper()
	key := "demo:us-central1-c:cluster"
	type ownership struct {
		Profile     string `json:"profile"`
		Project     string `json:"project"`
		Zone        string `json:"zone"`
		Cluster     string `json:"cluster"`
		BackendName string `json:"backendName,omitempty"`
		SHA256      string `json:"sha256,omitempty"`
		Device      uint64 `json:"device"`
		Inode       uint64 `json:"inode"`
	}
	ownerships := map[string]ownership{
		key: {
			Profile: profile, Project: "demo", Zone: "us-central1-c", Cluster: "cluster",
			BackendName: "minisky-owned-" + strings.Repeat("a", 32),
			SHA256:      strings.Repeat("b", 64), Device: 1, Inode: 2,
		},
	}
	ownershipPayload, err := json.Marshal(ownerships)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(ownershipPayload)
	entry, err := json.Marshal(map[string]any{
		"backend": "kind",
		"clusters": map[string]any{
			key: map[string]any{"name": "cluster", "location": "us-central1-c"},
		},
		"kubeconfigOwnerships":        ownerships,
		"kubeconfigOwnershipChecksum": hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(state.Snapshot{
		Format: state.SnapshotFormat, Version: state.Version,
		Entries: map[string]json.RawMessage{"gke/metadata": entry},
	})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
