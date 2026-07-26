//go:build windows

package state

import "fmt"

func preparePlatformLockDirectory(store *Store) error {
	root, err := openPinnedDirectory(store.root)
	if err != nil {
		return fmt.Errorf("pin state root for lock directory: %w", err)
	}
	defer root.close()
	locks, err := root.openDirectory(lockDirName, true)
	if err != nil {
		return fmt.Errorf("pin state lock directory: %w", err)
	}
	return locks.close()
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
