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
	maxImportSize = 64 << 20
)

var (
	ErrInvalidPath        = errors.New("invalid state path")
	ErrNotFound           = errors.New("state entry not found")
	ErrUnsupportedVersion = errors.New("unsupported state version")

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
	root       string
	profile    string
	profileDir string
	file       string
	lock       *sync.Mutex
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
	lock, _ := pathLocks.LoadOrStore(file, &sync.Mutex{})
	return &Store{
		root:       absoluteRoot,
		profile:    profile,
		profileDir: profileDir,
		file:       file,
		lock:       lock.(*sync.Mutex),
	}, nil
}

func (s *Store) Root() string       { return s.root }
func (s *Store) Profile() string    { return s.profile }
func (s *Store) ProfileDir() string { return s.profileDir }

// Save atomically replaces one named JSON entry.
func (s *Store) Save(name string, value any) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal state entry %q: %w", name, err)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	doc, err := s.readLocked()
	if err != nil {
		return err
	}
	doc.Entries[name] = payload
	return s.writeLocked(doc)
}

// Load decodes one named entry into target.
func (s *Store) Load(name string, target any) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	doc, err := s.readLocked()
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
}

// Delete atomically removes one named entry. Missing entries are not an error.
func (s *Store) Delete(name string) error {
	if err := validateEntryName(name); err != nil {
		return err
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	doc, err := s.readLocked()
	if err != nil {
		return err
	}
	if _, ok := doc.Entries[name]; !ok {
		return nil
	}
	delete(doc.Entries, name)
	return s.writeLocked(doc)
}

// Export writes a portable metadata snapshot. It never copies arbitrary files.
func (s *Store) Export(w io.Writer) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	doc, err := s.readLocked()
	if err != nil {
		return err
	}
	snapshot := Snapshot{Format: SnapshotFormat, Version: Version, Entries: doc.Entries}
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

	s.lock.Lock()
	defer s.lock.Unlock()
	return s.writeLocked(document{
		Format:  storeFormat,
		Version: Version,
		Entries: snapshot.Entries,
	})
}

func (s *Store) readLocked() (document, error) {
	payload, err := os.ReadFile(s.file)
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

func (s *Store) writeLocked(doc document) error {
	if err := os.MkdirAll(s.profileDir, 0o700); err != nil {
		return fmt.Errorf("create profile directory: %w", err)
	}
	if err := rejectSymlink(s.profileDir); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	payload = append(payload, '\n')

	temp, err := os.CreateTemp(s.profileDir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return fmt.Errorf("secure temporary state: %w", err)
	}
	if _, err := temp.Write(payload); err != nil {
		temp.Close()
		return fmt.Errorf("write temporary state: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync temporary state: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary state: %w", err)
	}
	if err := os.Rename(tempName, s.file); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}
	if err := syncDirectory(s.profileDir); err != nil {
		return err
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

func rejectSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect profile directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: unsafe profile directory", ErrInvalidPath)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open profile directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync profile directory: %w", err)
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
