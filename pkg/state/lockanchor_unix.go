//go:build darwin || linux

package state

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const lockAnchorMarker = ".minisky-lock-anchor"

func preparePlatformLockDirectory(store *Store) error {
	root, err := openPinnedDirectory(store.root)
	if err != nil {
		return fmt.Errorf("pin state root for lock directory: %w", err)
	}
	defer root.close()
	locks, err := root.openDirectory(lockDirName, true)
	if err != nil {
		return fmt.Errorf("create state lock directory: %w", err)
	}
	return locks.close()
}

func preparePlatformLockAnchors(store *Store) error {
	root, err := openPinnedDirectory(store.root)
	if err != nil {
		return fmt.Errorf("pin state root for lock anchors: %w", err)
	}
	defer root.close()
	locks, err := root.openDirectory(lockDirName, false)
	if err != nil {
		return fmt.Errorf("pin lock directory for anchors: %w", err)
	}
	defer locks.close()

	for _, anchor := range []struct {
		parent *pinnedDirectory
		name   string
	}{
		{parent: root, name: filepath.Base(store.ownerAnchor)},
		{parent: locks, name: filepath.Base(store.stateAnchor)},
	} {
		dir, err := anchor.parent.openDirectory(anchor.name, true)
		if err != nil {
			return fmt.Errorf("create lock anchor: %w", err)
		}
		file, err := dir.openFile(lockAnchorMarker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if errors.Is(err, os.ErrExist) {
			regular, statErr := dir.isRegular(lockAnchorMarker)
			if statErr != nil || !regular {
				dir.close()
				return fmt.Errorf("%w: unsafe lock anchor marker", ErrInvalidPath)
			}
			dir.close()
			continue
		}
		if err != nil {
			dir.close()
			return fmt.Errorf("create lock anchor marker: %w", err)
		}
		closeErr := errors.Join(file.Close(), dir.sync(), dir.close())
		if closeErr != nil {
			return fmt.Errorf("close lock anchor marker: %w", closeErr)
		}
	}
	return nil
}

func openPlatformLockAnchors(store *Store, resources *pinnedResources) error {
	owner, err := resources.root.openDirectory(filepath.Base(store.ownerAnchor), false)
	if err != nil {
		return fmt.Errorf("pin owner lock anchor: %w", err)
	}
	state, err := resources.locks.openDirectory(filepath.Base(store.stateAnchor), false)
	if err != nil {
		owner.close()
		return fmt.Errorf("pin transaction lock anchor: %w", err)
	}
	resources.ownerAnchor = owner
	resources.stateAnchor = state
	return nil
}

func acquireOwnershipLock(resources *pinnedResources, _ string, exclusive, nonblocking bool) (*fileLock, error) {
	return lockPinnedDirectory(resources.ownerAnchor, exclusive, nonblocking)
}

func acquireTransactionLock(resources *pinnedResources, _ string, exclusive, nonblocking bool) (*fileLock, error) {
	return lockPinnedDirectory(resources.stateAnchor, exclusive, nonblocking)
}

func acquireProfileSafetyLock(profile *pinnedDirectory, exclusive bool) (*fileLock, error) {
	return lockPinnedDirectory(profile, exclusive, false)
}

func lockPinnedDirectory(dir *pinnedDirectory, exclusive, nonblocking bool) (*fileLock, error) {
	if err := lockFile(dir.file, exclusive, nonblocking); err != nil {
		return nil, err
	}
	return &fileLock{file: dir.file}, nil
}
