package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestOptionalReadsTreatMissingPinnedHierarchyAsEmpty(t *testing.T) {
	for _, setup := range []struct {
		name string
		run  func(*testing.T, *Store)
	}{
		{name: "profiles parent missing"},
		{name: "profile missing", run: func(t *testing.T, store *Store) {
			t.Helper()
			if err := os.Mkdir(filepath.Join(store.Root(), "profiles"), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "state missing", run: func(t *testing.T, store *Store) {
			t.Helper()
			if err := os.MkdirAll(store.ProfileDir(), 0o700); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(setup.name, func(t *testing.T) {
			root := t.TempDir()
			store, err := New(root, "fresh")
			if err != nil {
				t.Fatal(err)
			}
			if setup.run != nil {
				setup.run(t, store)
			}
			var value bool
			if err := store.Load("missing/value", &value); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Load error = %v, want ErrNotFound", err)
			}
			var snapshot bytes.Buffer
			if err := store.Export(&snapshot); err != nil {
				t.Fatalf("Export: %v", err)
			}
			var decoded Snapshot
			if err := json.Unmarshal(snapshot.Bytes(), &decoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded.Entries) != 0 {
				t.Fatalf("Export entries = %#v, want empty", decoded.Entries)
			}
			if setup.name == "profiles parent missing" {
				if _, err := os.Stat(filepath.Join(root, "profiles")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("optional read created profiles parent or stat failed: %v", err)
				}
			}
		})
	}
}

func TestOptionalReadFailsClosedForWrongTypeProfilesParent(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "profiles"), []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := New(root, "fresh")
	if err != nil {
		t.Fatal(err)
	}
	var value bool
	err = store.Load("missing/value", &value)
	if err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("Load wrong-type profiles error = %v, want fail closed", err)
	}
}

func TestMutationCreatesPinnedProfileHierarchyFromFreshRoot(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "fresh")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("service/value", true); err != nil {
		t.Fatal(err)
	}
	var value bool
	if err := store.Load("service/value", &value); err != nil {
		t.Fatal(err)
	}
	if !value {
		t.Fatal("saved value was not restored")
	}
}

func TestAcquireOwnershipCreatesSecureMissingStateRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fresh-state-root")
	store, err := New(root, "fresh")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatalf("AcquireOwnership on missing root: %v", err)
	}
	defer ownership.Close()

	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("state root mode = %v, want directory 0700", info.Mode())
	}
}

func TestImportRejectsOversizedSnapshotBeforeReplacement(t *testing.T) {
	store, err := New(t.TempDir(), "import-limit")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("existing/value", map[string]string{"status": "preserved"}); err != nil {
		t.Fatal(err)
	}
	payload := `{"format":"minisky-state-snapshot","version":1,"entries":{}}` +
		strings.Repeat(" ", int(maxImportSize))
	if err := store.Import(strings.NewReader(payload)); err == nil {
		t.Fatal("oversized snapshot was accepted")
	}
	var value map[string]string
	if err := store.Load("existing/value", &value); err != nil {
		t.Fatal(err)
	}
	if value["status"] != "preserved" {
		t.Fatalf("existing state was replaced: %#v", value)
	}
}

func TestOptionalReadRefreshesHierarchyAfterTransactionLock(t *testing.T) {
	if os.Getenv("MINISKY_OPTIONAL_READ_WRITER_HELPER") == "1" {
		store, err := New(os.Getenv("MINISKY_STATE_ROOT"), "fresh")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Save("service/value", true); err != nil {
			t.Fatal(err)
		}
		return
	}
	root := t.TempDir()
	reader, err := New(root, "fresh")
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	proceed := make(chan struct{})
	reader.beforeTransactionLock = func() {
		close(ready)
		<-proceed
	}
	result := make(chan error, 1)
	var value bool
	go func() {
		result <- reader.Load("service/value", &value)
	}()
	<-ready

	command := exec.Command(os.Args[0], "-test.run=^TestOptionalReadRefreshesHierarchyAfterTransactionLock$")
	command.Env = append(os.Environ(),
		"MINISKY_OPTIONAL_READ_WRITER_HELPER=1",
		"MINISKY_STATE_ROOT="+root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("subprocess writer: %v\n%s", err, output)
	}
	close(proceed)
	if err := <-result; err != nil {
		t.Fatalf("Load after concurrent hierarchy creation: %v", err)
	}
	if !value {
		t.Fatal("optional read returned stale empty state")
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

func TestImportEntryValidatorRejectsWrongSchemaBeforeReplacement(t *testing.T) {
	const entry = "test-validator/metadata"
	MustRegisterEntryValidator(entry, func(_ EntryValidationContext, payload json.RawMessage) error {
		var value struct {
			Items []string `json:"items"`
		}
		return json.Unmarshal(payload, &value)
	})

	store, err := New(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("service/value", map[string]string{"value": "active"}); err != nil {
		t.Fatal(err)
	}

	badSnapshot := []byte(`{"format":"minisky-state","version":1,"entries":{"test-validator/metadata":{"items":"wrong"}}}`)
	if err := store.Import(bytes.NewReader(badSnapshot)); err == nil ||
		!strings.Contains(err.Error(), `state entry "test-validator/metadata"`) {
		t.Fatalf("Import error = %v, want entry schema rejection", err)
	}

	var got map[string]string
	if err := store.Load("service/value", &got); err != nil {
		t.Fatal(err)
	}
	if got["value"] != "active" {
		t.Fatalf("active state changed after failed schema validation: %#v", got)
	}
}

func TestImportEntryValidatorReceivesTargetStore(t *testing.T) {
	const entry = "context-validator/metadata"
	MustRegisterEntryValidator(entry, func(context EntryValidationContext, _ json.RawMessage) error {
		if context.Store == nil || context.Profile != context.Store.Profile() {
			return errors.New("validator did not receive target store context")
		}
		if context.Profile != "destination" {
			return fmt.Errorf("validator profile = %q", context.Profile)
		}
		return nil
	})
	store, err := New(t.TempDir(), "destination")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := []byte(`{"format":"minisky-state","version":1,"entries":{"context-validator/metadata":{}}}`)
	if err := store.Import(bytes.NewReader(snapshot)); err != nil {
		t.Fatalf("Import error = %v", err)
	}
}

func TestMustRegisterEntryValidatorIsRepeatSafe(t *testing.T) {
	const entry = "repeat-validator/metadata"
	validator := func(EntryValidationContext, json.RawMessage) error { return nil }

	MustRegisterEntryValidator(entry, validator)
	MustRegisterEntryValidator(entry, validator)
}

func TestRegisterEntryValidatorRejectsDifferentDuplicateWithTypedError(t *testing.T) {
	const entry = "conflicting-validator/metadata"
	first := func(EntryValidationContext, json.RawMessage) error { return nil }
	second := func(EntryValidationContext, json.RawMessage) error { return errors.New("different") }

	if err := RegisterEntryValidator(entry, first); err != nil {
		t.Fatal(err)
	}
	err := RegisterEntryValidator(entry, second)
	if !errors.Is(err, ErrEntryValidatorConflict) {
		t.Fatalf("RegisterEntryValidator error = %v, want ErrEntryValidatorConflict", err)
	}
}

func TestMustRegisterEntryValidatorPanicsForDifferentDuplicate(t *testing.T) {
	const entry = "conflicting-must-validator/metadata"
	MustRegisterEntryValidator(entry, func(EntryValidationContext, json.RawMessage) error { return nil })

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("MustRegisterEntryValidator did not panic")
		}
	}()
	MustRegisterEntryValidator(entry, func(EntryValidationContext, json.RawMessage) error {
		return errors.New("different")
	})
}

type failingEntryStore struct {
	loadErr   error
	saveErr   error
	saveCalls int
}

func (s *failingEntryStore) Load(string, any) error { return s.loadErr }

func (s *failingEntryStore) Save(string, any) error {
	s.saveCalls++
	return s.saveErr
}

func TestGuardedEntryStoreKeepsFailuresSticky(t *testing.T) {
	cause := errors.New("disk unavailable")
	delegate := &failingEntryStore{saveErr: cause}
	store := NewGuardedEntryStore(delegate, nil)

	if err := store.Save("service/metadata", map[string]string{"value": "first"}); !errors.Is(err, cause) {
		t.Fatalf("first Save error = %v, want %v", err, cause)
	}
	delegate.saveErr = nil
	if err := store.Save("service/metadata", map[string]string{"value": "second"}); !errors.Is(err, cause) {
		t.Fatalf("second Save error = %v, want sticky %v", err, cause)
	}
	if delegate.saveCalls != 1 {
		t.Fatalf("delegate Save calls = %d, want 1", delegate.saveCalls)
	}
}

func TestCorruptLoadPreservesBytesAndFailsClosedForMutations(t *testing.T) {
	store, err := New(t.TempDir(), "corrupt")
	if err != nil {
		t.Fatal(err)
	}
	const entry = "service/metadata"
	if err := store.Save(entry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.file)
	if err != nil {
		t.Fatal(err)
	}

	var metadata struct {
		Items []string `json:"items"`
	}
	if err := store.Load(entry, &metadata); err == nil {
		t.Fatal("corrupt state load succeeded")
	}

	var raw string
	if err := store.Load(entry, &raw); err != nil {
		t.Fatalf("non-mutating raw reload: %v", err)
	}
	if raw != "corrupt" {
		t.Fatalf("raw state = %q, want corrupt", raw)
	}
	if err := store.Save("service/other", true); err == nil {
		t.Fatal("save after corrupt load succeeded")
	}
	if err := store.Delete(entry); err == nil {
		t.Fatal("delete after corrupt load succeeded")
	}

	after, err := os.ReadFile(store.file)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("state bytes changed after corrupt load:\nbefore: %s\nafter: %s", before, after)
	}
}

func TestValidateResourceMapsRejectsMalformedMetadata(t *testing.T) {
	type resource struct {
		Name string `json:"name"`
	}
	type metadata struct {
		Resources map[string]*resource `json:"resources"`
	}

	for name, value := range map[string]metadata{
		"nil resource": {
			Resources: map[string]*resource{"projects/p/resources/r": nil},
		},
		"key name mismatch": {
			Resources: map[string]*resource{
				"projects/p/resources/r": {Name: "projects/p/resources/other"},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateResourceMaps(&value); err == nil {
				t.Fatal("ValidateResourceMaps accepted malformed metadata")
			}
		})
	}
}

func TestValidateResourceMapsAllowsIndependentResourceIDs(t *testing.T) {
	type resource struct {
		ID string `json:"id"`
	}
	type metadata struct {
		Networks map[string]*resource `json:"networks"`
	}
	value := metadata{
		Networks: map[string]*resource{"test-project:network-a": {ID: "1"}},
	}
	if err := ValidateResourceMaps(&value); err != nil {
		t.Fatalf("ValidateResourceMaps rejected valid independent ID: %v", err)
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

func TestStoreSubprocessWritersDoNotLoseUpdates(t *testing.T) {
	if os.Getenv("MINISKY_STATE_WRITER_HELPER") == "1" {
		runStateWriterHelper(t)
		return
	}

	root := t.TempDir()
	start := filepath.Join(root, "start")
	const (
		writers         = 16
		writesPerWriter = 4
	)
	commands := make([]*exec.Cmd, 0, writers)
	outputs := make([]bytes.Buffer, writers)
	for writer := 0; writer < writers; writer++ {
		command := exec.Command(os.Args[0], "-test.run=^TestStoreSubprocessWritersDoNotLoseUpdates$")
		command.Env = append(os.Environ(),
			"MINISKY_STATE_WRITER_HELPER=1",
			"MINISKY_STATE_ROOT="+root,
			"MINISKY_STATE_START="+start,
			"MINISKY_STATE_WRITER="+strconv.Itoa(writer),
		)
		command.Stdout = &outputs[writer]
		command.Stderr = &outputs[writer]
		if err := command.Start(); err != nil {
			t.Fatalf("start writer %d: %v", writer, err)
		}
		commands = append(commands, command)
	}
	if err := os.WriteFile(start, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for writer, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("writer %d: %v\n%s", writer, err, outputs[writer].String())
		}
	}

	store, err := New(root, "shared")
	if err != nil {
		t.Fatal(err)
	}
	for writer := 0; writer < writers; writer++ {
		for write := 0; write < writesPerWriter; write++ {
			name := fmt.Sprintf("writer-%d/value-%d", writer, write)
			var got int
			if err := store.Load(name, &got); err != nil {
				t.Fatalf("Load(%q): %v", name, err)
			}
			if got != writer {
				t.Fatalf("Load(%q) = %d, want %d", name, got, writer)
			}
		}
	}
}

func runStateWriterHelper(t *testing.T) {
	root := os.Getenv("MINISKY_STATE_ROOT")
	start := os.Getenv("MINISKY_STATE_START")
	writer, err := strconv.Atoi(os.Getenv("MINISKY_STATE_WRITER"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(start); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for writer start")
		}
		time.Sleep(time.Millisecond)
	}
	store, err := New(root, "shared")
	if err != nil {
		t.Fatal(err)
	}
	for write := 0; write < 4; write++ {
		name := fmt.Sprintf("writer-%d/value-%d", writer, write)
		if err := store.Save(name, writer); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProfileOwnershipBlocksImportUntilReleased(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := []byte(`{"format":"minisky-state","version":1,"entries":{}}`)
	if err := store.Import(bytes.NewReader(snapshot)); !errors.Is(err, ErrProfileInUse) {
		t.Fatalf("Import while owned error = %v, want ErrProfileInUse", err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Import(bytes.NewReader(snapshot)); err != nil {
		t.Fatalf("Import after release: %v", err)
	}
}

func TestProfileOwnershipAllowsDaemonWritesAndBlocksOtherProcesses(t *testing.T) {
	if os.Getenv("MINISKY_STATE_BLOCKED_WRITER_HELPER") == "1" {
		store, err := New(os.Getenv("MINISKY_STATE_ROOT"), "active")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Save("helper/value", true); !errors.Is(err, ErrProfileInUse) {
			t.Fatalf("Save error = %v, want ErrProfileInUse", err)
		}
		return
	}

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

	if err := store.Save("daemon/value", true); err != nil {
		t.Fatalf("owner Save: %v", err)
	}
	var snapshot bytes.Buffer
	if err := store.Export(&snapshot); err != nil {
		t.Fatalf("owner Export: %v", err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestProfileOwnershipAllowsDaemonWritesAndBlocksOtherProcesses$")
	command.Env = append(os.Environ(),
		"MINISKY_STATE_BLOCKED_WRITER_HELPER=1",
		"MINISKY_STATE_ROOT="+root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("blocked writer helper: %v\n%s", err, output)
	}
}

func TestProfileDirectoryReplacementCannotBypassOwnership(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows denies directory replacement while pinned")
	}
	if os.Getenv("MINISKY_STATE_REPLACE_HELPER") == "1" {
		root := os.Getenv("MINISKY_STATE_ROOT")
		store, err := New(root, "active")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(store.ProfileDir(), store.ProfileDir()+".replaced"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(store.ProfileDir(), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := store.Save("replacement/value", true); !errors.Is(err, ErrProfileInUse) {
			t.Fatalf("Save after replacement error = %v, want ErrProfileInUse", err)
		}
		return
	}

	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("daemon/value", true); err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestProfileDirectoryReplacementCannotBypassOwnership$")
	command.Env = append(os.Environ(),
		"MINISKY_STATE_REPLACE_HELPER=1",
		"MINISKY_STATE_ROOT="+root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("replacement helper: %v\n%s", err, output)
	}
	if err := store.Save("daemon/after-replacement", true); err == nil ||
		!strings.Contains(err.Error(), "profile directory was replaced") {
		t.Fatalf("owner Save after replacement error = %v, want replacement failure", err)
	}
	var value bool
	if err := store.Load("daemon/value", &value); !errors.Is(err, ErrProfileReplaced) {
		t.Fatalf("owner Load after replacement error = %v, want ErrProfileReplaced", err)
	}
	if err := store.Export(&bytes.Buffer{}); !errors.Is(err, ErrProfileReplaced) {
		t.Fatalf("owner Export after replacement error = %v, want ErrProfileReplaced", err)
	}
}

func TestLockDirectoryReplacementCannotBypassOwnership(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows denies directory replacement while pinned")
	}
	if os.Getenv("MINISKY_STATE_REPLACE_LOCKS_HELPER") == "1" {
		root := os.Getenv("MINISKY_STATE_ROOT")
		lockDir := filepath.Join(root, lockDirName)
		if err := os.Rename(lockDir, lockDir+".replaced"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(lockDir, 0o700); err != nil {
			t.Fatal(err)
		}
		store, err := New(root, "active")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Save("replacement/value", true); !errors.Is(err, ErrProfileInUse) {
			t.Fatalf("Save after lock replacement error = %v, want ErrProfileInUse", err)
		}
		return
	}

	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("daemon/value", true); err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestLockDirectoryReplacementCannotBypassOwnership$")
	command.Env = append(os.Environ(),
		"MINISKY_STATE_REPLACE_LOCKS_HELPER=1",
		"MINISKY_STATE_ROOT="+root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("lock replacement helper: %v\n%s", err, output)
	}
	if err := store.Save("daemon/after-lock-replacement", true); err == nil ||
		!strings.Contains(err.Error(), "state lock directory was replaced") {
		t.Fatalf("owner Save after lock replacement error = %v, want replacement failure", err)
	}
}

func TestLockFileUnlinkCannotBypassOwnership(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows denies deletion of open lock files")
	}
	if os.Getenv("MINISKY_STATE_UNLINK_LOCK_HELPER") == "1" {
		store, err := New(os.Getenv("MINISKY_STATE_ROOT"), "active")
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{store.ownerLock, store.stateLock} {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.Save("replacement/value", true); !errors.Is(err, ErrProfileInUse) {
			t.Fatalf("Save after lock unlink error = %v, want ErrProfileInUse", err)
		}
		return
	}

	root := t.TempDir()
	store, err := New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save("daemon/value", true); err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestLockFileUnlinkCannotBypassOwnership$")
	command.Env = append(os.Environ(),
		"MINISKY_STATE_UNLINK_LOCK_HELPER=1",
		"MINISKY_STATE_ROOT="+root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("lock unlink helper: %v\n%s", err, output)
	}
}

func TestProfileOwnershipIsProfileScoped(t *testing.T) {
	root := t.TempDir()
	first, err := New(root, "first")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := first.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	second, err := New(root, "second")
	if err != nil {
		t.Fatal(err)
	}
	otherOwnership, err := second.AcquireOwnership()
	if err != nil {
		t.Fatalf("acquire second profile ownership: %v", err)
	}
	if err := otherOwnership.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProfileOwnershipIsReleasedWhenProcessExits(t *testing.T) {
	if os.Getenv("MINISKY_STATE_OWNER_HELPER") == "1" {
		store, err := New(os.Getenv("MINISKY_STATE_ROOT"), "crash")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AcquireOwnership(); err != nil {
			t.Fatal(err)
		}
		return
	}

	root := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestProfileOwnershipIsReleasedWhenProcessExits$")
	command.Env = append(os.Environ(),
		"MINISKY_STATE_OWNER_HELPER=1",
		"MINISKY_STATE_ROOT="+root,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("owner helper: %v\n%s", err, output)
	}
	store, err := New(root, "crash")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatalf("acquire ownership after helper exit: %v", err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
}
