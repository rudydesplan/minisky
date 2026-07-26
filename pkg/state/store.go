// Package state provides versioned, profile-scoped, atomic JSON persistence.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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
	ErrNotFound                 = errors.New("state entry not found")
	ErrProfileInUse             = errors.New("state profile is in use")
	ErrProfileReplaced          = errors.New("state profile directory was replaced")
	ErrSafeOwnershipUnsupported = errors.New("safe profile ownership is unsupported")
	ErrStateRootReplaced        = errors.New("state root directory was replaced")
	ErrUnsupportedVersion       = errors.New("unsupported state version")

	profilePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	entryPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	pathLocks      sync.Map
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

	beforeStateReplace    func()
	beforeReadOnlyRecheck func()
	beforeTransactionLock func()
	effectiveWriteAccess  func(string) error
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

// Save atomically replaces one named JSON entry.
func (s *Store) Save(name string, value any) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal state entry %q: %w", name, err)
	}

	return s.withMutationLock(func(profileDir *pinnedDirectory) error {
		doc, err := s.readLocked(profileDir)
		if err != nil {
			return err
		}
		doc.Entries[name] = payload
		return s.writeLocked(profileDir, doc)
	})
}

// Load decodes one named entry into target.
func (s *Store) Load(name string, target any) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	return s.withStateLock(false, func(profileDir *pinnedDirectory) error {
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
}

// Delete atomically removes one named entry. Missing entries are not an error.
func (s *Store) Delete(name string) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	return s.withMutationLock(func(profileDir *pinnedDirectory) error {
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
}

// Export writes a portable metadata snapshot. It never copies arbitrary files.
func (s *Store) Export(w io.Writer) error {
	var snapshot Snapshot
	if err := s.withStateLock(false, func(profileDir *pinnedDirectory) error {
		doc, err := s.readLocked(profileDir)
		if err != nil {
			return err
		}
		snapshot = Snapshot{Format: SnapshotFormat, Version: Version, Entries: doc.Entries}
		return nil
	}); err != nil {
		return err
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
	for name, payload := range snapshot.Entries {
		if err := validateEntryName(name); err != nil {
			return err
		}
		if !json.Valid(payload) {
			return fmt.Errorf("invalid JSON in state entry %q", name)
		}
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
