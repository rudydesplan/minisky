//go:build darwin || linux

package state

import (
	"errors"
	"os"
	"syscall"
)

func lockFile(file *os.File, exclusive, nonblocking bool) error {
	operation := syscall.LOCK_SH
	if exclusive {
		operation = syscall.LOCK_EX
	}
	if nonblocking {
		operation |= syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), operation); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return errLockUnavailable
		}
		return err
	}
	return nil
}

func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

func isReadOnlyFilesystemError(err error) bool {
	return errors.Is(err, syscall.EROFS)
}
