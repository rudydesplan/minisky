// Package state provides versioned, profile-scoped, atomic JSON persistence.
package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

const (
	// Version is the current on-disk and portable snapshot schema version.
	Version = 1
	// SnapshotFormat identifies portable MiniSky state snapshots.
	SnapshotFormat = "minisky-state"

	storeFormat   = "minisky-state-store"
	stateFileName = "state.json"
	lockDirName   = ".locks"
	stateLockName = "state.lock"
	ownerLockName = "owner.lock"
	maxImportSize = 64 << 20
)

var (
	ErrInvalidPath              = errors.New("invalid state path")
	ErrLockNamespaceReplaced    = errors.New("state lock directory was replaced")
	ErrMarkerMismatch           = errors.New("local state marker does not match")
	ErrNotFound                 = errors.New("state entry not found")
	ErrProfileInUse             = errors.New("state profile is in use")
	ErrProfileReplaced          = errors.New("state profile directory was replaced")
	ErrSafeOwnershipUnsupported = errors.New("safe profile ownership is unsupported")
	ErrStateRootReplaced        = errors.New("state root directory was replaced")
	ErrStateConflict            = errors.New("state version conflict")
	ErrUnsupportedVersion       = errors.New("unsupported state version")
	ErrEntryValidatorConflict   = errors.New("state entry validator conflict")

	profilePattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	entryPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	pathLocks          sync.Map
	hooksMu            sync.RWMutex
	entryValidators    = make(map[string]entryValidatorRegistration)
	portableCodecs     = make(map[string]portableCodecRegistration)
	snapshotCodecs     = make(map[string]snapshotCodecRegistration)
	snapshotValidators = make(map[string]snapshotValidatorRegistration)
	hookGeneration     uint64
)

type document struct {
	Format  string                     `json:"format"`
	Version int                        `json:"version"`
	Entries map[string]json.RawMessage `json:"entries"`
}

// Snapshot is the portable metadata-only representation used by Export and Import.
// Binary service data, including DuckDB database files, is deliberately excluded.
type Snapshot struct {
	Format  string                     `json:"format"`
	Version int                        `json:"version"`
	Entries map[string]json.RawMessage `json:"entries"`
}

// TransformResult is the committed readback from one atomic state transform.
type TransformResult struct {
	Version string
	Entries map[string]json.RawMessage
}

// EntryTransform edits a private state snapshot while its cross-instance lock
// is not held. The input map is isolated from the active document. It may be
// invoked again after an optimistic conflict and must tolerate retries.
type EntryTransform func(map[string]json.RawMessage) error

// EntryValidationContext identifies the explicit import destination.
type EntryValidationContext struct {
	Store   *Store
	Profile string
	// Entries contains the complete candidate portable snapshot during import.
	// Validators must treat it as read-only. It is nil for local-load validation.
	Entries map[string]json.RawMessage
}

// EntryValidator validates one durable state entry before snapshot replacement.
type EntryValidator func(EntryValidationContext, json.RawMessage) error

type entryValidatorRegistration struct {
	validator EntryValidator
}

// PortableEntryCodec redacts profile-local fields on export and normalizes
// imported metadata before semantic validation and atomic replacement.
type PortableEntryCodec struct {
	Export func(EntryValidationContext, json.RawMessage) (json.RawMessage, error)
	Import func(EntryValidationContext, json.RawMessage) (json.RawMessage, error)
}

// SnapshotValidationContext exposes one complete candidate snapshot.
type SnapshotValidationContext struct {
	Store   *Store
	Profile string
	Entries map[string]json.RawMessage
}

// SnapshotValidator validates relationships across sibling state entries.
type SnapshotValidator func(SnapshotValidationContext) error

// SnapshotCodec normalizes relationships spanning multiple portable entries.
type SnapshotCodec func(SnapshotValidationContext) error

type snapshotValidatorRegistration struct {
	validator SnapshotValidator
}

type snapshotCodecRegistration struct {
	codec SnapshotCodec
}

type pipelineHooks struct {
	generation             uint64
	entryValidators        map[string]entryValidatorRegistration
	portableCodecs         map[string]portableCodecRegistration
	snapshotCodecNames     []string
	snapshotCodecs         []snapshotCodecRegistration
	snapshotValidatorNames []string
	snapshotValidators     []snapshotValidatorRegistration
}

func capturePipelineHooks() pipelineHooks {
	hooksMu.RLock()
	defer hooksMu.RUnlock()
	hooks := pipelineHooks{
		generation:      hookGeneration,
		entryValidators: make(map[string]entryValidatorRegistration, len(entryValidators)),
		portableCodecs:  make(map[string]portableCodecRegistration, len(portableCodecs)),
	}
	for name, validator := range entryValidators {
		hooks.entryValidators[name] = validator
	}
	for name, codec := range portableCodecs {
		hooks.portableCodecs[name] = codec
	}
	hooks.snapshotCodecNames = sortedMapKeys(snapshotCodecs)
	hooks.snapshotCodecs = make([]snapshotCodecRegistration, len(hooks.snapshotCodecNames))
	for index, name := range hooks.snapshotCodecNames {
		hooks.snapshotCodecs[index] = snapshotCodecs[name]
	}
	hooks.snapshotValidatorNames = sortedMapKeys(snapshotValidators)
	hooks.snapshotValidators = make([]snapshotValidatorRegistration, len(hooks.snapshotValidatorNames))
	for index, name := range hooks.snapshotValidatorNames {
		hooks.snapshotValidators[index] = snapshotValidators[name]
	}
	return hooks
}

// RegisterSnapshotValidator registers one named cross-entry validator.
func RegisterSnapshotValidator(name string, validator SnapshotValidator) error {
	if strings.TrimSpace(name) == "" || validator == nil {
		return errors.New("snapshot validator name and function are required")
	}
	registration := snapshotValidatorRegistration{validator: validator}
	hooksMu.Lock()
	defer hooksMu.Unlock()
	if _, loaded := snapshotValidators[name]; loaded {
		return fmt.Errorf("snapshot validator conflict: %q", name)
	}
	snapshotValidators[name] = registration
	hookGeneration++
	return nil
}

// MustRegisterSnapshotValidator registers a cross-entry validator or panics.
func MustRegisterSnapshotValidator(name string, validator SnapshotValidator) {
	if err := RegisterSnapshotValidator(name, validator); err != nil {
		panic(err)
	}
}

// RegisterSnapshotCodec registers one named cross-entry portable normalizer.
func RegisterSnapshotCodec(name string, codec SnapshotCodec) error {
	if strings.TrimSpace(name) == "" || codec == nil {
		return errors.New("snapshot codec name and function are required")
	}
	registration := snapshotCodecRegistration{codec: codec}
	hooksMu.Lock()
	defer hooksMu.Unlock()
	if _, loaded := snapshotCodecs[name]; loaded {
		return fmt.Errorf("snapshot codec conflict: %q", name)
	}
	snapshotCodecs[name] = registration
	hookGeneration++
	return nil
}

// MustRegisterSnapshotCodec registers a cross-entry codec or panics.
func MustRegisterSnapshotCodec(name string, codec SnapshotCodec) {
	if err := RegisterSnapshotCodec(name, codec); err != nil {
		panic(err)
	}
}

func applySnapshotCodecs(context SnapshotValidationContext, hooks pipelineHooks) error {
	for index, codec := range hooks.snapshotCodecs {
		if err := codec.codec(context); err != nil {
			return fmt.Errorf("snapshot codec %q: %w", hooks.snapshotCodecNames[index], err)
		}
	}
	return nil
}

func validateSnapshot(context SnapshotValidationContext, hooks pipelineHooks) error {
	for index, validator := range hooks.snapshotValidators {
		if err := validator.validator(context); err != nil {
			return fmt.Errorf("snapshot validator %q: %w", hooks.snapshotValidatorNames[index], err)
		}
	}
	return nil
}

type portableCodecRegistration struct {
	codec PortableEntryCodec
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func validatePortableEntries(entries map[string]json.RawMessage) error {
	for _, name := range sortedMapKeys(entries) {
		if err := validateEntryName(name); err != nil {
			return err
		}
		if !json.Valid(entries[name]) {
			return fmt.Errorf("invalid JSON in state entry %q", name)
		}
	}
	return nil
}

// RegisterPortableEntryCodec registers portable transforms for an exact entry.
func RegisterPortableEntryCodec(name string, codec PortableEntryCodec) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	if codec.Export == nil || codec.Import == nil {
		return fmt.Errorf("portable state codec %q is incomplete", name)
	}
	registration := portableCodecRegistration{codec: codec}
	hooksMu.Lock()
	defer hooksMu.Unlock()
	if _, loaded := portableCodecs[name]; loaded {
		return fmt.Errorf("portable state codec conflict: %q", name)
	}
	portableCodecs[name] = registration
	hookGeneration++
	return nil
}

// MustRegisterPortableEntryCodec registers a codec or panics during init.
func MustRegisterPortableEntryCodec(name string, codec PortableEntryCodec) {
	if err := RegisterPortableEntryCodec(name, codec); err != nil {
		panic(err)
	}
}

// RegisterEntryValidator registers the schema validator for an exact state entry.
func RegisterEntryValidator(name string, validator EntryValidator) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	if validator == nil {
		return fmt.Errorf("state entry validator %q is nil", name)
	}
	registration := entryValidatorRegistration{validator: validator}
	hooksMu.Lock()
	defer hooksMu.Unlock()
	if _, loaded := entryValidators[name]; loaded {
		return fmt.Errorf("%w: %q", ErrEntryValidatorConflict, name)
	}
	entryValidators[name] = registration
	hookGeneration++
	return nil
}

// MustRegisterEntryValidator registers a validator or panics during package init.
func MustRegisterEntryValidator(name string, validator EntryValidator) {
	if err := RegisterEntryValidator(name, validator); err != nil {
		panic(err)
	}
}

// StrictEntryValidator decodes one entry with unknown-field rejection and then
// applies optional schema-specific semantic validation.
func StrictEntryValidator[T any](validate func(EntryValidationContext, *T) error) EntryValidator {
	return func(context EntryValidationContext, payload json.RawMessage) error {
		var value T
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		if err := requireEOF(decoder); err != nil {
			return err
		}
		if validate != nil {
			if err := validate(context, &value); err != nil {
				return err
			}
		}
		return ValidateResourceMaps(&value)
	}
}

// Store persists named JSON entries for one profile.
type Store struct {
	root        string
	profile     string
	profileDir  string
	file        string
	lockDir     string
	stateLock   string
	ownerLock   string
	stateAnchor string
	ownerAnchor string
	lock        *sync.RWMutex
	healthMu    sync.RWMutex
	degradedErr error

	beforeStateReplace      func()
	beforeReadOnlyRecheck   func()
	beforeTransactionLock   func()
	beforeLocalMarkerCommit func()
	afterLocalMarkerCommit  func()
	effectiveWriteAccess    func(string) error
}

// New creates a handle for a named profile rooted at root.
func New(root, profile string) (*Store, error) {
	if !validProfile(profile) {
		return nil, fmt.Errorf("%w: profile %q", ErrInvalidPath, profile)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve state root: %w", err)
	}
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("%w: empty root", ErrInvalidPath)
	}
	profileDir := filepath.Join(absoluteRoot, "profiles", profile)
	file := filepath.Join(profileDir, stateFileName)
	lockDir := filepath.Join(absoluteRoot, lockDirName)
	stateLock := filepath.Join(lockDir, profile+"."+stateLockName)
	ownerLock := filepath.Join(absoluteRoot, "."+profile+"."+ownerLockName)
	ownerAnchor := filepath.Join(absoluteRoot, "."+profile+".owner-lock-anchor")
	stateAnchor := filepath.Join(lockDir, profile+".state-lock-anchor")
	lock, _ := pathLocks.LoadOrStore(stateLock, &sync.RWMutex{})
	return &Store{
		root:                 absoluteRoot,
		profile:              profile,
		profileDir:           profileDir,
		file:                 file,
		lockDir:              lockDir,
		stateLock:            stateLock,
		ownerLock:            ownerLock,
		ownerAnchor:          ownerAnchor,
		stateAnchor:          stateAnchor,
		lock:                 lock.(*sync.RWMutex),
		effectiveWriteAccess: effectiveWriteAccess,
	}, nil
}

func (s *Store) Root() string       { return s.root }
func (s *Store) Profile() string    { return s.profile }
func (s *Store) ProfileDir() string { return s.profileDir }

// AcquireOwnership claims exclusive mutation ownership for this profile. A
// daemon holds this lease for its lifetime so offline operations such as Import
// cannot replace state while the daemon is active.
func (s *Store) AcquireOwnership() (*Ownership, error) {
	return s.acquireOwnership(true)
}

func (s *Store) acquireOwnership(registerLocal bool) (*Ownership, error) {
	if err := s.prepareStateRoot(); err != nil {
		return nil, err
	}
	if err := s.prepareProfileDirectory(); err != nil {
		return nil, err
	}
	if err := s.prepareLockDirectory(); err != nil {
		return nil, err
	}
	resources, err := s.openPinnedResources(false)
	if err != nil {
		return nil, err
	}
	if !supportsPinnedOwnership() {
		resources.close()
		return nil, fmt.Errorf("%w on this platform", ErrSafeOwnershipUnsupported)
	}
	path := s.ownerLock
	gate := ownershipGate(path)
	gate.Lock()
	defer gate.Unlock()
	if _, owned := ownedProfiles.Load(path); owned {
		resources.close()
		return nil, fmt.Errorf("%w: %s", ErrProfileInUse, s.profile)
	}
	lock, err := acquireOwnershipLock(resources, filepath.Base(path), true, true)
	if errors.Is(err, errLockUnavailable) {
		resources.close()
		return nil, fmt.Errorf("%w: %s", ErrProfileInUse, s.profile)
	}
	if err != nil {
		resources.close()
		return nil, fmt.Errorf("lock state profile ownership: %w", err)
	}
	if err := s.verifyPinnedResources(resources); err != nil {
		lock.close()
		resources.close()
		return nil, err
	}
	lease := newOwnershipLease()
	if registerLocal {
		ownedProfiles.Store(path, &ownedProfile{resources: resources, lease: lease})
	}
	return &Ownership{resources: resources, lease: lease, release: func() error {
		gate.Lock()
		defer gate.Unlock()
		leaseErr := lease.invalidate()
		if registerLocal {
			ownedProfiles.Delete(path)
		}
		return errors.Join(leaseErr, lock.close(), resources.close())
	}}, nil
}

func (s *Store) prepareStateRoot() error {
	created := false
	if err := os.Mkdir(s.root, 0o700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create state root: %w", err)
		}
	} else {
		created = true
	}
	root, err := openPinnedDirectory(s.root)
	if err != nil {
		return fmt.Errorf("pin state root after creation: %w", err)
	}
	defer root.close()
	info, err := os.Stat(s.root)
	if err != nil {
		return fmt.Errorf("inspect state root: %w", err)
	}
	if created && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("state root permissions %04o are too broad; want 0700", info.Mode().Perm())
	}
	return root.samePath()
}

// Save atomically replaces one named JSON entry.
func (s *Store) Save(name string, value any) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	if err := s.persistenceError(); err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal state entry %q: %w", name, err)
	}

	err = s.withMutationLock(func(profileDir *pinnedDirectory) error {
		doc, err := s.readLocked(profileDir)
		if err != nil {
			return err
		}
		doc.Entries[name] = payload
		return s.writeLocked(profileDir, doc)
	})
	if err != nil {
		s.markPersistenceDegraded(err)
	}
	return err
}

// SaveEntries atomically replaces multiple named JSON entries in one state
// document commit. No entry is changed if validation or the write fails.
func (s *Store) SaveEntries(entries map[string]json.RawMessage) error {
	if err := s.persistenceError(); err != nil {
		return err
	}
	prepared := make(map[string]json.RawMessage, len(entries))
	for _, name := range sortedMapKeys(entries) {
		if err := validateEntryName(name); err != nil {
			return err
		}
		payload := entries[name]
		if !json.Valid(payload) {
			return fmt.Errorf("invalid JSON in state entry %q", name)
		}
		prepared[name] = append(json.RawMessage(nil), payload...)
	}
	if len(prepared) == 0 {
		return nil
	}
	err := s.withMutationLock(func(profileDir *pinnedDirectory) error {
		doc, err := s.readLocked(profileDir)
		if err != nil {
			return err
		}
		for _, name := range sortedMapKeys(prepared) {
			doc.Entries[name] = prepared[name]
		}
		return s.writeLocked(profileDir, doc)
	})
	if err != nil {
		s.markPersistenceDegraded(err)
	}
	return err
}

// TransformEntries reloads and atomically transforms the latest committed
// private entries under the cross-instance state lock. expectedVersion may be
// empty; otherwise a mismatch returns ErrStateConflict without writing.
func (s *Store) TransformEntries(
	expectedVersion string,
	transform EntryTransform,
) (TransformResult, error) {
	if transform == nil {
		return TransformResult{}, errors.New("state entry transform is required")
	}
	if err := s.persistenceError(); err != nil {
		return TransformResult{}, err
	}
	const maxAttempts = 4
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var base document
		if err := s.withStateLock(false, func(profileDir *pinnedDirectory) error {
			doc, err := s.readLocked(profileDir)
			if err != nil {
				return err
			}
			base = document{
				Format: doc.Format, Version: doc.Version,
				Entries: cloneRawEntries(doc.Entries),
			}
			return nil
		}); err != nil {
			s.markPersistenceDegraded(err)
			return TransformResult{}, err
		}
		baseVersion, err := stateDocumentVersion(base)
		if err != nil {
			return TransformResult{}, err
		}
		baseResult := TransformResult{
			Version: baseVersion, Entries: cloneRawEntries(base.Entries),
		}
		if expectedVersion != "" && expectedVersion != baseVersion {
			return baseResult, fmt.Errorf("%w: expected %s, current %s",
				ErrStateConflict, expectedVersion, baseVersion)
		}
		hooks := capturePipelineHooks()
		entries := cloneRawEntries(base.Entries)
		if err := transform(entries); err != nil {
			return TransformResult{}, err
		}
		if err := validatePortableEntries(entries); err != nil {
			return TransformResult{}, fmt.Errorf("validate transformed state entries: %w", err)
		}
		context := EntryValidationContext{Store: s, Profile: s.profile, Entries: entries}
		for _, name := range sortedMapKeys(entries) {
			if registered, ok := hooks.entryValidators[name]; ok {
				if err := registered.validator(context, entries[name]); err != nil {
					return TransformResult{},
						fmt.Errorf("invalid transformed state entry %q: %w", name, err)
				}
			}
		}
		if err := validateSnapshot(SnapshotValidationContext{
			Store: s, Profile: s.profile, Entries: entries,
		}, hooks); err != nil {
			return TransformResult{},
				fmt.Errorf("invalid transformed state relationships: %w", err)
		}

		var result TransformResult
		conflict := false
		err = s.withMutationLock(func(profileDir *pinnedDirectory) error {
			current, err := s.readLocked(profileDir)
			if err != nil {
				return err
			}
			currentVersion, err := stateDocumentVersion(current)
			if err != nil {
				return err
			}
			result = TransformResult{
				Version: currentVersion, Entries: cloneRawEntries(current.Entries),
			}
			if currentVersion != baseVersion {
				conflict = true
				return nil
			}
			hooksMu.RLock()
			defer hooksMu.RUnlock()
			if hookGeneration != hooks.generation {
				conflict = true
				return nil
			}
			current.Entries = entries
			if err := s.writeLocked(profileDir, current); err != nil {
				return err
			}
			committed, err := s.readLocked(profileDir)
			if err != nil {
				return fmt.Errorf("read back transformed state: %w", err)
			}
			version, err := stateDocumentVersion(committed)
			if err != nil {
				return err
			}
			result = TransformResult{
				Version: version, Entries: cloneRawEntries(committed.Entries),
			}
			return nil
		})
		if err != nil {
			s.markPersistenceDegraded(err)
			return TransformResult{}, err
		}
		if !conflict {
			return result, nil
		}
		if expectedVersion != "" || attempt+1 == maxAttempts {
			return result, fmt.Errorf("%w after %d attempts", ErrStateConflict, attempt+1)
		}
	}
	panic("unreachable state transform retry")
}

func cloneRawEntries(entries map[string]json.RawMessage) map[string]json.RawMessage {
	cloned := make(map[string]json.RawMessage, len(entries))
	for name, payload := range entries {
		cloned[name] = append(json.RawMessage(nil), payload...)
	}
	return cloned
}

func stateDocumentVersion(doc document) (string, error) {
	payload, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("marshal state version: %w", err)
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum[:]), nil
}

// Load decodes one named entry into target.
func (s *Store) Load(name string, target any) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	err := s.withStateLock(false, func(profileDir *pinnedDirectory) error {
		doc, err := s.readLocked(profileDir)
		if err != nil {
			return err
		}
		payload, ok := doc.Entries[name]
		if !ok {
			return fmt.Errorf("%w: %s", ErrNotFound, name)
		}
		if err := json.Unmarshal(payload, target); err != nil {
			return fmt.Errorf("decode state entry %q: %w", name, err)
		}
		return nil
	})
	if err != nil && !errors.Is(err, ErrNotFound) {
		s.markPersistenceDegraded(err)
	}
	return err
}

func (s *Store) markPersistenceDegraded(err error) {
	if err == nil {
		return
	}
	s.healthMu.Lock()
	if s.degradedErr == nil {
		s.degradedErr = err
	}
	s.healthMu.Unlock()
}

func (s *Store) persistenceError() error {
	s.healthMu.RLock()
	defer s.healthMu.RUnlock()
	return s.degradedErr
}

// PersistenceError reports the first load or save failure observed by the store.
func (s *Store) PersistenceError() error {
	return s.persistenceError()
}

// Delete atomically removes one named entry. Missing entries are not an error.
func (s *Store) Delete(name string) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	if err := s.persistenceError(); err != nil {
		return err
	}
	err := s.withMutationLock(func(profileDir *pinnedDirectory) error {
		doc, err := s.readLocked(profileDir)
		if err != nil {
			return err
		}
		if _, ok := doc.Entries[name]; !ok {
			return nil
		}
		delete(doc.Entries, name)
		return s.writeLocked(profileDir, doc)
	})
	if err != nil {
		s.markPersistenceDegraded(err)
	}
	return err
}

// Export writes a portable metadata snapshot. It never copies arbitrary files.
func (s *Store) Export(w io.Writer) error {
	var entries map[string]json.RawMessage
	if err := s.withStateLock(false, func(profileDir *pinnedDirectory) error {
		doc, err := s.readLocked(profileDir)
		if err != nil {
			return err
		}
		entries = make(map[string]json.RawMessage, len(doc.Entries))
		for name, payload := range doc.Entries {
			entries[name] = append(json.RawMessage(nil), payload...)
		}
		return nil
	}); err != nil {
		return err
	}
	hooks := capturePipelineHooks()
	context := EntryValidationContext{Store: s, Profile: s.profile, Entries: entries}
	for _, name := range sortedMapKeys(entries) {
		payload := entries[name]
		if registered, ok := hooks.portableCodecs[name]; ok {
			normalized, err := registered.codec.Export(context, payload)
			if err != nil {
				return fmt.Errorf("export state entry %q: %w", name, err)
			}
			if !json.Valid(normalized) {
				return fmt.Errorf("portable codec returned invalid JSON for state entry %q", name)
			}
			entries[name] = normalized
		}
	}
	snapshot := Snapshot{Format: SnapshotFormat, Version: Version, Entries: entries}
	if err := applySnapshotCodecs(SnapshotValidationContext{
		Store: s, Profile: s.profile, Entries: entries,
	}, hooks); err != nil {
		return fmt.Errorf("normalize exported state snapshot: %w", err)
	}
	if err := validatePortableEntries(entries); err != nil {
		return fmt.Errorf("validate normalized exported entries: %w", err)
	}
	for _, name := range sortedMapKeys(entries) {
		if registered, ok := hooks.entryValidators[name]; ok {
			if err := registered.validator(context, entries[name]); err != nil {
				return fmt.Errorf("invalid schema for exported state entry %q: %w", name, err)
			}
		}
	}
	if err := validateSnapshot(SnapshotValidationContext{
		Store: s, Profile: s.profile, Entries: entries,
	}, hooks); err != nil {
		return fmt.Errorf("validate exported state snapshot: %w", err)
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshot); err != nil {
		return fmt.Errorf("encode state snapshot: %w", err)
	}
	return nil
}

// Import validates a complete portable snapshot before atomically replacing the
// active profile document.
func (s *Store) Import(r io.Reader) error {
	var snapshot Snapshot
	decoder := json.NewDecoder(io.LimitReader(r, maxImportSize+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("decode state snapshot: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return err
	}
	if snapshot.Format != SnapshotFormat {
		return fmt.Errorf("invalid state snapshot format %q", snapshot.Format)
	}
	if snapshot.Version != Version {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, snapshot.Version)
	}
	if snapshot.Entries == nil {
		snapshot.Entries = make(map[string]json.RawMessage)
	}
	hooks := capturePipelineHooks()
	if err := validatePortableEntries(snapshot.Entries); err != nil {
		return err
	}
	entryNames := sortedMapKeys(snapshot.Entries)
	context := EntryValidationContext{Store: s, Profile: s.profile, Entries: snapshot.Entries}
	for _, name := range entryNames {
		payload := snapshot.Entries[name]
		if registered, ok := hooks.portableCodecs[name]; ok {
			normalized, normalizeErr := registered.codec.Import(context, payload)
			if normalizeErr != nil {
				return fmt.Errorf("normalize imported state entry %q: %w", name, normalizeErr)
			}
			if !json.Valid(normalized) {
				return fmt.Errorf("portable codec returned invalid JSON for state entry %q", name)
			}
			payload = normalized
			snapshot.Entries[name] = payload
		}
	}
	if err := applySnapshotCodecs(SnapshotValidationContext{
		Store: s, Profile: s.profile, Entries: snapshot.Entries,
	}, hooks); err != nil {
		return fmt.Errorf("normalize imported state snapshot: %w", err)
	}
	if err := validatePortableEntries(snapshot.Entries); err != nil {
		return fmt.Errorf("validate normalized imported entries: %w", err)
	}
	entryNames = sortedMapKeys(snapshot.Entries)
	for _, name := range entryNames {
		if registered, ok := hooks.entryValidators[name]; ok {
			if err := registered.validator(context, snapshot.Entries[name]); err != nil {
				return fmt.Errorf("invalid schema for state entry %q: %w", name, err)
			}
		}
	}
	if err := validateSnapshot(SnapshotValidationContext{
		Store: s, Profile: s.profile, Entries: snapshot.Entries,
	}, hooks); err != nil {
		return fmt.Errorf("invalid state snapshot relationships: %w", err)
	}

	ownership, err := s.acquireOwnership(false)
	if err != nil {
		return err
	}
	s.lock.Lock()
	writeErr := s.withPinnedStateLock(ownership.resources, true, func(profileDir *pinnedDirectory) error {
		return s.writeLocked(profileDir, document{
			Format:  storeFormat,
			Version: Version,
			Entries: snapshot.Entries,
		})
	})
	s.lock.Unlock()
	closeErr := ownership.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("unlock state profile ownership: %w", closeErr)
	}
	return errors.Join(writeErr, closeErr)
}

func (s *Store) withStateLock(exclusive bool, action func(*pinnedDirectory) error) error {
	gate := ownershipGate(s.ownerLock)
	gate.RLock()
	defer gate.RUnlock()
	if exclusive {
		s.lock.Lock()
		defer s.lock.Unlock()
	} else {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}
	if owned, found := ownedProfiles.Load(s.ownerLock); found {
		return s.withPinnedStateLock(owned.(*ownedProfile).resources, exclusive, action)
	}
	if err := s.prepareLockDirectory(); err != nil {
		if !exclusive && isConfirmedReadOnlyAccess(s, err) {
			return s.withReadOnlyStateLock(err, action)
		}
		return err
	}
	resources, err := s.openPinnedResources(!exclusive)
	if err != nil {
		if !exclusive && isConfirmedReadOnlyAccess(s, err) {
			return s.withReadOnlyStateLock(err, action)
		}
		return err
	}
	defer resources.close()
	return s.withPinnedStateLock(resources, exclusive, action)
}

func (s *Store) withPinnedStateLock(
	resources *pinnedResources,
	exclusive bool,
	action func(*pinnedDirectory) error,
) error {
	if exclusive && !supportsPinnedOwnership() {
		return fmt.Errorf("%w for state mutation on this platform", ErrSafeOwnershipUnsupported)
	}
	if s.beforeTransactionLock != nil {
		s.beforeTransactionLock()
	}
	lock, err := acquireTransactionLock(resources, filepath.Base(s.stateLock), exclusive, false)
	if err != nil {
		return fmt.Errorf("lock state profile: %w", err)
	}
	if err := s.refreshOptionalProfile(resources); err != nil {
		lock.close()
		return err
	}
	var profileLock *fileLock
	if resources.profile != nil {
		profileLock, err = acquireProfileSafetyLock(resources.profile, exclusive)
		if err != nil {
			lock.close()
			return fmt.Errorf("lock state profile directory: %w", err)
		}
	}
	identityErr := s.verifyPinnedResources(resources)
	var actionErr error
	if identityErr == nil {
		actionErr = action(resources.profile)
	}
	postIdentityErr := s.verifyPinnedResources(resources)
	closeErr := lock.close()
	if profileLock != nil {
		closeErr = errors.Join(closeErr, profileLock.close())
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("unlock state profile: %w", closeErr)
	}
	return errors.Join(identityErr, actionErr, postIdentityErr, closeErr)
}

func (s *Store) refreshOptionalProfile(resources *pinnedResources) error {
	if resources.profiles == nil {
		profiles, err := resources.root.openDirectory("profiles", false)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pin optional profiles directory: %w", err)
		}
		resources.profiles = profiles
	}
	if resources.profile == nil {
		profile, err := resources.profiles.openDirectory(s.profile, false)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pin optional state profile directory: %w", err)
		}
		resources.profile = profile
	}
	return nil
}

func (s *Store) withMutationLock(action func(*pinnedDirectory) error) error {
	path := s.ownerLock
	gate := ownershipGate(path)
	gate.RLock()
	defer gate.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()
	if owned, found := ownedProfiles.Load(path); found {
		return s.withPinnedStateLock(owned.(*ownedProfile).resources, true, action)
	}
	if err := s.prepareProfileDirectory(); err != nil {
		return err
	}
	if err := s.prepareLockDirectory(); err != nil {
		return err
	}
	resources, err := s.openPinnedResources(false)
	if err != nil {
		return err
	}
	defer resources.close()
	ownership, err := acquireOwnershipLock(resources, filepath.Base(path), false, true)
	if errors.Is(err, errLockUnavailable) {
		return fmt.Errorf("%w: %s", ErrProfileInUse, s.profile)
	}
	if err != nil {
		return fmt.Errorf("lock state profile for mutation: %w", err)
	}
	actionErr := s.withPinnedStateLock(resources, true, action)
	closeErr := ownership.close()
	if closeErr != nil {
		closeErr = fmt.Errorf("unlock state profile mutation: %w", closeErr)
	}
	return errors.Join(actionErr, closeErr)
}

func (s *Store) withReadOnlyStateLock(cause error, action func(*pinnedDirectory) error) error {
	root, err := openPinnedDirectory(s.root)
	if err != nil {
		return fmt.Errorf("pin read-only state root: %w", err)
	}
	defer root.close()
	profiles, err := root.openDirectory("profiles", false)
	if errors.Is(err, os.ErrNotExist) {
		rootLock, lockErr := acquireProfileSafetyLock(root, false)
		if lockErr != nil {
			return fmt.Errorf("lock read-only state root: %w", lockErr)
		}
		if rootLock != nil {
			defer rootLock.close()
		}
		if s.beforeReadOnlyRecheck != nil {
			s.beforeReadOnlyRecheck()
		}
		if !isConfirmedReadOnlyAccess(s, cause) {
			return fmt.Errorf("read-only state changed permissions: %w", cause)
		}
		identityErr := root.samePath()
		var actionErr error
		if identityErr == nil {
			actionErr = action(nil)
		}
		postIdentityErr := root.samePath()
		if identityErr != nil {
			identityErr = fmt.Errorf("%w: %v", ErrStateRootReplaced, identityErr)
		}
		if postIdentityErr != nil {
			postIdentityErr = fmt.Errorf("%w: %v", ErrStateRootReplaced, postIdentityErr)
		}
		return errors.Join(identityErr, actionErr, postIdentityErr)
	}
	if err != nil {
		return fmt.Errorf("pin read-only profiles directory: %w", err)
	}
	defer profiles.close()
	profileDir, err := profiles.openDirectory(s.profile, false)
	if errors.Is(err, os.ErrNotExist) {
		if isReadOnlyFilesystemError(cause) {
			return action(nil)
		}
		return fmt.Errorf("pin read-only profile directory: %w", err)
	}
	if err != nil {
		return fmt.Errorf("open read-only profile directory: %w", err)
	}
	defer profileDir.close()
	profileLock, err := acquireProfileSafetyLock(profileDir, false)
	if err != nil {
		return fmt.Errorf("lock read-only profile directory: %w", err)
	}
	if profileLock != nil {
		defer profileLock.close()
	}
	if s.beforeReadOnlyRecheck != nil {
		s.beforeReadOnlyRecheck()
	}
	if !isConfirmedReadOnlyAccess(s, cause) {
		return fmt.Errorf("read-only state changed permissions: %w", cause)
	}
	stateFile, err := profileDir.openFile(stateFileName, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return action(profileDir)
	}
	if err != nil {
		return fmt.Errorf("lock read-only state profile: %w", err)
	}
	lock, err := lockOpenFile(stateFile, false, false)
	if err != nil {
		return fmt.Errorf("lock read-only state profile: %w", err)
	}
	identityErr := profileDir.samePath()
	var actionErr error
	if identityErr == nil {
		actionErr = action(profileDir)
	}
	postIdentityErr := profileDir.samePath()
	closeErr := lock.close()
	if closeErr != nil {
		closeErr = fmt.Errorf("unlock read-only state profile: %w", closeErr)
	}
	if identityErr != nil {
		identityErr = fmt.Errorf("%w: %s: %v", ErrProfileReplaced, s.profile, identityErr)
	}
	if postIdentityErr != nil {
		postIdentityErr = fmt.Errorf("%w: %s: %v", ErrProfileReplaced, s.profile, postIdentityErr)
	}
	return errors.Join(identityErr, actionErr, postIdentityErr, closeErr)
}

func (s *Store) openPinnedResources(profileOptional bool) (*pinnedResources, error) {
	root, err := openPinnedDirectory(s.root)
	if err != nil {
		return nil, fmt.Errorf("pin state root: %w", err)
	}
	locks, err := root.openDirectory(lockDirName, false)
	if err != nil {
		root.close()
		return nil, fmt.Errorf("pin state lock directory: %w", err)
	}
	profiles, err := root.openDirectory("profiles", false)
	if profileOptional && errors.Is(err, os.ErrNotExist) {
		resources := &pinnedResources{root: root, locks: locks}
		if anchorErr := openPlatformLockAnchors(s, resources); anchorErr != nil {
			resources.close()
			return nil, anchorErr
		}
		return resources, nil
	}
	if err != nil {
		locks.close()
		root.close()
		return nil, fmt.Errorf("pin profiles directory: %w", err)
	}
	profile, err := profiles.openDirectory(s.profile, false)
	if profileOptional && errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		profiles.close()
		locks.close()
		root.close()
		return nil, fmt.Errorf("pin state profile directory: %w", err)
	}
	resources := &pinnedResources{root: root, profiles: profiles, locks: locks, profile: profile}
	if err := openPlatformLockAnchors(s, resources); err != nil {
		resources.close()
		return nil, err
	}
	return resources, nil
}

func (s *Store) verifyPinnedResources(resources *pinnedResources) error {
	if err := resources.root.samePath(); err != nil {
		return fmt.Errorf("%w: %v", ErrStateRootReplaced, err)
	}
	if err := resources.locks.samePath(); err != nil {
		return fmt.Errorf("%w: %v", ErrLockNamespaceReplaced, err)
	}
	if resources.profiles != nil {
		if err := resources.profiles.samePath(); err != nil {
			return fmt.Errorf("%w: profiles: %v", ErrProfileReplaced, err)
		}
	}
	if resources.ownerAnchor != nil {
		if err := resources.ownerAnchor.samePath(); err != nil {
			return fmt.Errorf("%w: owner anchor: %v", ErrLockNamespaceReplaced, err)
		}
	}
	if resources.stateAnchor != nil {
		if err := resources.stateAnchor.samePath(); err != nil {
			return fmt.Errorf("%w: transaction anchor: %v", ErrLockNamespaceReplaced, err)
		}
	}
	if resources.profile != nil {
		if err := resources.profile.samePath(); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrProfileReplaced, s.profile, err)
		}
	}
	return nil
}

func (s *Store) prepareProfileDirectory() error {
	root, err := openPinnedDirectory(s.root)
	if err != nil {
		return fmt.Errorf("pin state root for profile: %w", err)
	}
	defer root.close()
	rootLock, err := acquireProfileSafetyLock(root, true)
	if err != nil {
		return fmt.Errorf("lock state root for profile creation: %w", err)
	}
	if rootLock != nil {
		defer rootLock.close()
	}
	profiles, err := root.openDirectory("profiles", true)
	if err != nil {
		return fmt.Errorf("create profiles directory: %w", err)
	}
	defer profiles.close()
	profile, err := profiles.openDirectory(s.profile, true)
	if err != nil {
		return fmt.Errorf("create profile directory: %w", err)
	}
	return profile.close()
}

func (s *Store) prepareLockDirectory() error {
	if err := preparePlatformLockDirectory(s); err != nil {
		return err
	}
	return preparePlatformLockAnchors(s)
}

func (s *Store) readLocked(profileDir *pinnedDirectory) (document, error) {
	if profileDir == nil {
		return emptyDocument(), nil
	}
	payload, err := profileDir.readFile(stateFileName)
	if errors.Is(err, os.ErrNotExist) {
		return emptyDocument(), nil
	}
	if err != nil {
		return document{}, fmt.Errorf("read state: %w", err)
	}
	var doc document
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		return document{}, fmt.Errorf("decode state: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return document{}, err
	}
	if doc.Format != storeFormat {
		return document{}, fmt.Errorf("invalid state store format %q", doc.Format)
	}
	if doc.Version != Version {
		return document{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, doc.Version)
	}
	if doc.Entries == nil {
		doc.Entries = make(map[string]json.RawMessage)
	}
	return doc, nil
}

func (s *Store) writeLocked(profileDir *pinnedDirectory, doc document) error {
	if profileDir == nil {
		return fmt.Errorf("write state: profile directory is not pinned")
	}
	payload, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	payload = append(payload, '\n')

	temp, tempName, err := profileDir.createTemp(".state-")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	defer temp.Close()
	defer profileDir.remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return fmt.Errorf("secure temporary state: %w", err)
	}
	if _, err := temp.Write(payload); err != nil {
		temp.Close()
		return fmt.Errorf("write temporary state: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync temporary state: %w", err)
	}
	if s.beforeStateReplace != nil {
		s.beforeStateReplace()
	}
	if err := profileDir.rename(tempName, stateFileName); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}
	if err := profileDir.sameFileAt(temp, stateFileName); err != nil {
		return fmt.Errorf("verify replaced state identity: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close replaced state: %w", err)
	}
	if err := profileDir.sync(); err != nil {
		return fmt.Errorf("sync profile directory: %w", err)
	}
	return nil
}

func emptyDocument() document {
	return document{Format: storeFormat, Version: Version, Entries: make(map[string]json.RawMessage)}
}

func validProfile(profile string) bool {
	return profilePattern.MatchString(profile) && profile != "." && profile != ".."
}

func validateEntryName(name string) error {
	if filepath.IsAbs(name) || strings.Contains(name, `\`) {
		return fmt.Errorf("%w: entry %q", ErrInvalidPath, name)
	}
	parts := strings.Split(name, "/")
	if len(parts) == 0 {
		return fmt.Errorf("%w: entry %q", ErrInvalidPath, name)
	}
	for _, part := range parts {
		if !entryPattern.MatchString(part) || part == "." || part == ".." {
			return fmt.Errorf("%w: entry %q", ErrInvalidPath, name)
		}
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("state document contains trailing JSON")
		}
		return fmt.Errorf("decode trailing state data: %w", err)
	}
	return nil
}
