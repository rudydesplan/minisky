//go:build !darwin && !linux && !windows

package state

import "os"

func isConfirmedReadOnlyAccess(_ *Store, cause error) bool {
	return isReadOnlyFilesystemError(cause)
}

func effectiveWriteAccess(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	return file.Close()
}
