//go:build !darwin && !linux && !windows

package state

import (
	"fmt"
	"os"
)

func preparePlatformLockDirectory(store *Store) error {
	if err := os.MkdirAll(store.lockDir, 0o700); err != nil {
		return fmt.Errorf("create state lock directory: %w", err)
	}
	return nil
}

func preparePlatformLockAnchors(*Store) error {
	return nil
}

func openPlatformLockAnchors(*Store, *pinnedResources) error {
	return nil
}

func acquireOwnershipLock(resources *pinnedResources, name string, exclusive, nonblocking bool) (*fileLock, error) {
	return acquireFileLockAt(resources.root, name, exclusive, nonblocking)
}

func acquireTransactionLock(resources *pinnedResources, name string, exclusive, nonblocking bool) (*fileLock, error) {
	return acquireFileLockAt(resources.locks, name, exclusive, nonblocking)
}

func acquireProfileSafetyLock(*pinnedDirectory, bool) (*fileLock, error) {
	return nil, nil
}
