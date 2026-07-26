//go:build darwin || linux

package state

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func isConfirmedReadOnlyAccess(store *Store, cause error) bool {
	if isReadOnlyFilesystemError(cause) {
		return true
	}
	if !errors.Is(cause, os.ErrPermission) &&
		!errors.Is(cause, syscall.EACCES) &&
		!errors.Is(cause, syscall.EPERM) {
		return false
	}
	for _, path := range []string{
		store.root,
		store.lockDir,
		store.ownerAnchor,
		store.stateAnchor,
		store.profileDir,
		store.file,
	} {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return false
		}
		if err := store.effectiveWriteAccess(path); err == nil {
			return false
		} else if !errors.Is(err, unix.EACCES) && !errors.Is(err, unix.EROFS) {
			return false
		}
	}
	return true
}

func effectiveWriteAccess(path string) error {
	return unix.Faccessat(unix.AT_FDCWD, path, unix.W_OK, unix.AT_EACCESS)
}
