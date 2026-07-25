package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func TestStoreAtomicRoundTrip(t *testing.T) {
	t.Parallel()

	store, err := New(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"name": "orders", "count": float64(3)}
	if err := store.Save("bigquery/metadata", want); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(store.Root(), "default")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := reopened.Load("bigquery/metadata", &got); err != nil {
		t.Fatal(err)
	}
	if got["name"] != want["name"] || got["count"] != want["count"] {
		t.Fatalf("round trip = %#v, want %#v", got, want)
	}

	matches, err := filepath.Glob(filepath.Join(store.ProfileDir(), "*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files left behind: %v", matches)
	}
}

func TestProfilesAreIsolated(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	first, err := New(root, "first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(root, "second")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Save("service/value", map[string]string{"value": "first"}); err != nil {
		t.Fatal(err)
	}
	if err := second.Save("service/value", map[string]string{"value": "second"}); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		store *Store
		want  string
	}{{first, "first"}, {second, "second"}} {
		var got map[string]string
		if err := tc.store.Load("service/value", &got); err != nil {
			t.Fatal(err)
		}
		if got["value"] != tc.want {
			t.Fatalf("profile value = %q, want %q", got["value"], tc.want)
		}
	}
}

func TestRejectsUnsafeProfileAndEntryNames(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for _, profile := range []string{"", ".", "..", "../escape", "nested/profile", "/absolute"} {
		if _, err := New(root, profile); !errors.Is(err, ErrInvalidPath) {
			t.Errorf("New(%q) error = %v, want ErrInvalidPath", profile, err)
		}
	}

	store, err := New(root, "safe")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "../escape", "service/../../escape", "/absolute", `service\escape`} {
		if err := store.Save(name, true); !errors.Is(err, ErrInvalidPath) {
			t.Errorf("Save(%q) error = %v, want ErrInvalidPath", name, err)
		}
	}
}

func TestImportValidationDoesNotReplaceActiveState(t *testing.T) {
	t.Parallel()

	store, err := New(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("service/value", map[string]string{"value": "active"}); err != nil {
		t.Fatal(err)
	}

	badSnapshot := []byte(`{"format":"minisky-state","version":1,"entries":{"../escape":{"value":"bad"}}}`)
	if err := store.Import(bytes.NewReader(badSnapshot)); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("Import error = %v, want ErrInvalidPath", err)
	}

	var got map[string]string
	if err := store.Load("service/value", &got); err != nil {
		t.Fatal(err)
	}
	if got["value"] != "active" {
		t.Fatalf("active state changed after failed import: %#v", got)
	}
}

func TestExportImportPortableSnapshot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source, err := New(root, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Save("bigquery/metadata", map[string]string{"dataset": "analytics"}); err != nil {
		t.Fatal(err)
	}

	var snapshot bytes.Buffer
	if err := source.Export(&snapshot); err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(snapshot.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document["format"] != SnapshotFormat {
		t.Fatalf("snapshot format = %v, want %q", document["format"], SnapshotFormat)
	}

	target, err := New(root, "target")
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Import(&snapshot); err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := target.Load("bigquery/metadata", &got); err != nil {
		t.Fatal(err)
	}
	if got["dataset"] != "analytics" {
		t.Fatalf("imported metadata = %#v", got)
	}

	if _, err := os.Stat(filepath.Join(target.ProfileDir(), stateFileName)); err != nil {
		t.Fatalf("state file missing: %v", err)
	}
}

func TestConcurrentStoreHandlesDoNotLoseUpdates(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	first, err := New(root, "shared")
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(root, "shared")
	if err != nil {
		t.Fatal(err)
	}

	const writes = 20
	var wait sync.WaitGroup
	for i := 0; i < writes; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			store := first
			if index%2 == 1 {
				store = second
			}
			if err := store.Save("service/value-"+strconv.Itoa(index), index); err != nil {
				t.Errorf("Save(%d): %v", index, err)
			}
		}(i)
	}
	wait.Wait()

	for i := 0; i < writes; i++ {
		var got int
		if err := first.Load("service/value-"+strconv.Itoa(i), &got); err != nil {
			t.Fatalf("Load(%d): %v", i, err)
		}
		if got != i {
			t.Fatalf("value %d = %d", i, got)
		}
	}
}
