package state

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

var ErrUnsafeSpoolPath = errors.New("unsafe owned spool path")

// OwnedSpoolSpec identifies MiniSky-owned temporary files that may be removed
// after exclusive profile ownership has been acquired.
type OwnedSpoolSpec struct {
	Directory string
	Prefixes  []string
}

// OwnedSpoolDirectory is a directory pinned beneath the exclusively owned
// profile directory. Files are created and removed relative to its handle.
type OwnedSpoolDirectory struct {
	dir      *pinnedDirectory
	lease    *ownershipLease
	mu       sync.RWMutex
	filesMu  sync.Mutex
	files    map[*os.File]struct{}
	closed   bool
	closeErr error
}

// ReconcileOwnedSpools removes only regular MiniSky temporary files with the
// configured prefixes. It refuses symlinked directories and matching symlink
// entries rather than following or deleting them.
func (s *Store) ReconcileOwnedSpools(ownership *Ownership, specs ...OwnedSpoolSpec) error {
	if ownership == nil || ownership.resources == nil || ownership.resources.profile == nil {
		return fmt.Errorf("%w: missing profile ownership", ErrUnsafeSpoolPath)
	}
	gate := ownershipGate(s.ownerLock)
	gate.RLock()
	defer gate.RUnlock()
	owned, ok := ownedProfiles.Load(s.ownerLock)
	if !ok || owned.(*ownedProfile).resources != ownership.resources {
		return fmt.Errorf("%w: profile ownership is not active", ErrUnsafeSpoolPath)
	}
	if err := s.verifyPinnedResources(ownership.resources); err != nil {
		return err
	}

	for _, spec := range specs {
		if err := validateSpoolDirectoryName(spec.Directory); err != nil {
			return err
		}
		dir, err := ownership.resources.profile.openDirectory(spec.Directory, true)
		if err != nil {
			return fmt.Errorf("%w: open %s: %v", ErrUnsafeSpoolPath, spec.Directory, err)
		}
		entries, readErr := dir.readDir()
		if readErr == nil {
			for _, entry := range entries {
				if !ownedSpoolName(entry.Name(), spec.Prefixes) {
					continue
				}
				regular, err := dir.isRegular(entry.Name())
				if err != nil {
					readErr = errors.Join(readErr, err)
					continue
				}
				if !regular {
					readErr = errors.Join(readErr,
						fmt.Errorf("%w: %s/%s is not a regular file",
							ErrUnsafeSpoolPath, spec.Directory, entry.Name()))
					continue
				}
				readErr = errors.Join(readErr, dir.remove(entry.Name()))
			}
			readErr = errors.Join(readErr, dir.sync())
		}
		closeErr := dir.close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return fmt.Errorf("reconcile owned spool %s: %w", spec.Directory, err)
		}
	}
	return nil
}

// OpenOwnedSpoolDirectory opens a profile child directory through the pinned
// profile handle held by the active daemon ownership lease.
func OpenOwnedSpoolDirectory(root, profile, name string) (*OwnedSpoolDirectory, error) {
	if err := validateSpoolDirectoryName(name); err != nil {
		return nil, err
	}
	store, err := New(root, profile)
	if err != nil {
		return nil, err
	}
	gate := ownershipGate(store.ownerLock)
	gate.RLock()
	defer gate.RUnlock()
	owned, ok := ownedProfiles.Load(store.ownerLock)
	if !ok {
		return nil, fmt.Errorf("%w: profile %s is not owned by this daemon", ErrUnsafeSpoolPath, profile)
	}
	active := owned.(*ownedProfile)
	resources := active.resources
	if err := store.verifyPinnedResources(resources); err != nil {
		return nil, err
	}
	dir, err := resources.profile.openDirectory(name, true)
	if err != nil {
		return nil, fmt.Errorf("%w: open %s: %v", ErrUnsafeSpoolPath, name, err)
	}
	spool := &OwnedSpoolDirectory{
		dir: dir, lease: active.lease,
		files: make(map[*os.File]struct{}),
	}
	active.lease.trackSpool(spool)
	return spool, nil
}

func (dir *OwnedSpoolDirectory) CreateTemp(prefix string) (*os.File, string, error) {
	if !ownedSpoolPrefix(prefix) {
		return nil, "", fmt.Errorf("%w: invalid temporary prefix %q", ErrUnsafeSpoolPath, prefix)
	}
	var file *os.File
	var name string
	err := dir.withActive(func() error {
		var err error
		file, name, err = dir.dir.createTemp(prefix)
		if err == nil {
			dir.filesMu.Lock()
			dir.files[file] = struct{}{}
			dir.filesMu.Unlock()
		}
		return err
	})
	return file, name, err
}

func (dir *OwnedSpoolDirectory) Remove(name string) error {
	if !ownedSpoolName(name, []string{".request-", ".upload-", ".session-", ".completed-"}) {
		return fmt.Errorf("%w: refusing to remove %q", ErrUnsafeSpoolPath, name)
	}
	return dir.withActive(func() error {
		return dir.dir.remove(name)
	})
}

func (dir *OwnedSpoolDirectory) List(prefix string) ([]string, error) {
	if !ownedSpoolPrefix(prefix) {
		return nil, fmt.Errorf("%w: invalid temporary prefix %q", ErrUnsafeSpoolPath, prefix)
	}
	var names []string
	err := dir.withActive(func() error {
		entries, err := dir.dir.readDir()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !ownedSpoolName(entry.Name(), []string{prefix}) {
				continue
			}
			regular, err := dir.dir.isRegular(entry.Name())
			if err != nil {
				return err
			}
			if !regular {
				return fmt.Errorf("%w: %s is not a regular file", ErrUnsafeSpoolPath, entry.Name())
			}
			names = append(names, entry.Name())
		}
		return nil
	})
	return names, err
}

func (dir *OwnedSpoolDirectory) ReadFile(name string, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 ||
		!ownedSpoolName(name, []string{".request-", ".upload-", ".session-", ".completed-"}) {
		return nil, fmt.Errorf("%w: refusing to read %q", ErrUnsafeSpoolPath, name)
	}
	var payload []byte
	err := dir.withActive(func() error {
		file, err := dir.dir.openRegularFileForRead(name)
		if err != nil {
			return fmt.Errorf("%w: refusing to read %s: %w", ErrUnsafeSpoolPath, name, err)
		}
		defer file.Close()
		payload, err = io.ReadAll(io.LimitReader(file, maxBytes+1))
		if err != nil {
			return err
		}
		if int64(len(payload)) > maxBytes {
			return fmt.Errorf("%w: %s exceeds %d bytes", ErrUnsafeSpoolPath, name, maxBytes)
		}
		return nil
	})
	return payload, err
}

func (dir *OwnedSpoolDirectory) WriteFileAtomic(name, prefix string, payload []byte) error {
	if !ownedSpoolPrefix(prefix) || !ownedSpoolName(name, []string{prefix}) {
		return fmt.Errorf("%w: refusing to write %q", ErrUnsafeSpoolPath, name)
	}
	return dir.withActive(func() error {
		file, tempName, err := dir.dir.createTemp(prefix)
		if err != nil {
			return err
		}
		cleanup := func() {
			_ = file.Close()
			_ = dir.dir.remove(tempName)
			_ = dir.dir.sync()
		}
		if _, err := file.Write(payload); err != nil {
			cleanup()
			return err
		}
		if err := file.Sync(); err != nil {
			cleanup()
			return err
		}
		if err := file.Close(); err != nil {
			_ = dir.dir.remove(tempName)
			return err
		}
		if err := dir.dir.sync(); err != nil {
			_ = dir.dir.remove(tempName)
			return err
		}
		if err := dir.dir.rename(tempName, name); err != nil {
			_ = dir.dir.remove(tempName)
			return err
		}
		return dir.dir.sync()
	})
}

func (dir *OwnedSpoolDirectory) Sync() error {
	return dir.withActive(dir.dir.sync)
}

func (dir *OwnedSpoolDirectory) Close() error {
	err := dir.closePinnedDirectory()
	dir.lease.untrackSpool(dir)
	return err
}

func (dir *OwnedSpoolDirectory) withActive(action func() error) error {
	return dir.lease.withActive(func() error {
		dir.mu.RLock()
		defer dir.mu.RUnlock()
		if dir.closed {
			return fmt.Errorf("%w: owned spool directory is closed", ErrUnsafeSpoolPath)
		}
		return action()
	})
}

func (dir *OwnedSpoolDirectory) closePinnedDirectory() error {
	dir.mu.Lock()
	defer dir.mu.Unlock()
	if dir.closed {
		return dir.closeErr
	}
	dir.closed = true
	dir.filesMu.Lock()
	files := make([]*os.File, 0, len(dir.files))
	for file := range dir.files {
		files = append(files, file)
	}
	clear(dir.files)
	dir.filesMu.Unlock()
	for _, file := range files {
		if err := file.Close(); err != nil &&
			!errors.Is(err, os.ErrInvalid) && !errors.Is(err, os.ErrClosed) {
			dir.closeErr = errors.Join(dir.closeErr, err)
		}
	}
	dir.closeErr = errors.Join(dir.closeErr, dir.dir.close())
	return dir.closeErr
}

func validateSpoolDirectoryName(name string) error {
	if name != "request-spool" && name != "uploads" {
		return fmt.Errorf("%w: invalid spool directory %q", ErrUnsafeSpoolPath, name)
	}
	return nil
}

func ownedSpoolName(name string, prefixes []string) bool {
	if strings.ContainsAny(name, `/\`) || !strings.HasSuffix(name, ".tmp") {
		return false
	}
	for _, prefix := range prefixes {
		if ownedSpoolPrefix(prefix) && strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func ownedSpoolPrefix(prefix string) bool {
	return prefix == ".request-" || prefix == ".upload-" ||
		prefix == ".session-" || prefix == ".completed-"
}
