package state

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

var (
	errLockUnavailable   = errors.New("file lock unavailable")
	errDirectoryReplaced = errors.New("directory replaced")
)

var (
	ownershipGates sync.Map
	ownedProfiles  sync.Map
)

type fileLock struct {
	file      *os.File
	closeFile bool
}

func acquireFileLockAt(dir *pinnedDirectory, name string, exclusive, nonblocking bool) (*fileLock, error) {
	existingFlags := os.O_RDONLY
	if exclusive {
		existingFlags = os.O_RDWR
	}
	for range 3 {
		file, err := dir.openFile(name, existingFlags, 0)
		if err == nil {
			return lockOpenFile(file, exclusive, nonblocking)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("open existing lock file %q: %w", name, err)
		}
		file, err = dir.openFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			return lockOpenFile(file, exclusive, nonblocking)
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create lock file %q: %w", name, err)
		}
	}
	return nil, fmt.Errorf("open lock file %q after concurrent creation", name)
}

func lockOpenFile(file *os.File, exclusive, nonblocking bool) (*fileLock, error) {
	if err := lockFile(file, exclusive, nonblocking); err != nil {
		file.Close()
		return nil, err
	}
	return &fileLock{file: file, closeFile: true}, nil
}

func (lock *fileLock) close() error {
	unlockErr := unlockFile(lock.file)
	var closeErr error
	if lock.closeFile {
		closeErr = lock.file.Close()
	}
	return errors.Join(unlockErr, closeErr)
}

// Ownership is an exclusive lease held by a daemon for one profile.
// Closing it releases the lease. The operating system also releases it if the
// process exits or crashes.
type Ownership struct {
	once      sync.Once
	release   func() error
	resources *pinnedResources
	lease     *ownershipLease
	err       error
}

type ownedProfile struct {
	resources *pinnedResources
	lease     *ownershipLease
}

type ownershipLease struct {
	mu        sync.RWMutex
	active    bool
	trackedMu sync.Mutex
	spools    map[*OwnedSpoolDirectory]struct{}
}

func newOwnershipLease() *ownershipLease {
	return &ownershipLease{
		active: true,
		spools: make(map[*OwnedSpoolDirectory]struct{}),
	}
}

func (lease *ownershipLease) withActive(action func() error) error {
	lease.mu.RLock()
	defer lease.mu.RUnlock()
	if !lease.active {
		return fmt.Errorf("%w: profile ownership lease is closed", ErrUnsafeSpoolPath)
	}
	return action()
}

func (lease *ownershipLease) trackSpool(spool *OwnedSpoolDirectory) {
	lease.trackedMu.Lock()
	defer lease.trackedMu.Unlock()
	lease.spools[spool] = struct{}{}
}

func (lease *ownershipLease) untrackSpool(spool *OwnedSpoolDirectory) {
	lease.trackedMu.Lock()
	defer lease.trackedMu.Unlock()
	delete(lease.spools, spool)
}

func (lease *ownershipLease) invalidate() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if !lease.active {
		return nil
	}
	lease.active = false
	lease.trackedMu.Lock()
	spools := make([]*OwnedSpoolDirectory, 0, len(lease.spools))
	for spool := range lease.spools {
		spools = append(spools, spool)
	}
	clear(lease.spools)
	lease.trackedMu.Unlock()
	var err error
	for _, spool := range spools {
		err = errors.Join(err, spool.closePinnedDirectory())
	}
	return err
}

type pinnedResources struct {
	root        *pinnedDirectory
	profiles    *pinnedDirectory
	locks       *pinnedDirectory
	profile     *pinnedDirectory
	ownerAnchor *pinnedDirectory
	stateAnchor *pinnedDirectory
}

func (resources *pinnedResources) close() error {
	var profileErr error
	if resources.profile != nil {
		profileErr = resources.profile.close()
	}
	var profilesErr error
	if resources.profiles != nil {
		profilesErr = resources.profiles.close()
	}
	var ownerErr, stateErr error
	if resources.ownerAnchor != nil {
		ownerErr = resources.ownerAnchor.close()
	}
	if resources.stateAnchor != nil {
		stateErr = resources.stateAnchor.close()
	}
	return errors.Join(profileErr, profilesErr, ownerErr, stateErr, resources.locks.close(), resources.root.close())
}

// Close releases profile ownership. It is safe to call more than once.
func (ownership *Ownership) Close() error {
	ownership.once.Do(func() {
		ownership.err = ownership.release()
	})
	return ownership.err
}

func ownershipGate(path string) *sync.RWMutex {
	gate, _ := ownershipGates.LoadOrStore(path, &sync.RWMutex{})
	return gate.(*sync.RWMutex)
}
