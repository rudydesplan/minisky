//go:build windows

package state

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func isConfirmedReadOnlyAccess(store *Store, cause error) bool {
	if !isReadOnlyFilesystemError(cause) &&
		!errors.Is(cause, os.ErrPermission) &&
		!errors.Is(cause, windows.ERROR_ACCESS_DENIED) {
		return false
	}
	for _, path := range []string{
		store.root,
		store.lockDir,
		store.ownerAnchor,
		store.stateAnchor,
		store.profileDir,
	} {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return false
		}
		pointer, err := windows.UTF16PtrFromString(path)
		if err != nil {
			return false
		}
		handle, err := windows.CreateFile(
			pointer,
			windows.FILE_WRITE_DATA|windows.FILE_APPEND_DATA,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
			nil,
			windows.OPEN_EXISTING,
			windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
			0,
		)
		if err == nil {
			windows.CloseHandle(handle)
			return false
		}
		if !errors.Is(err, windows.ERROR_ACCESS_DENIED) &&
			!errors.Is(err, windows.ERROR_WRITE_PROTECT) {
			return false
		}
	}
	if _, err := os.Stat(store.file); err == nil {
		file, openErr := os.OpenFile(store.file, os.O_WRONLY, 0)
		if openErr == nil {
			file.Close()
			return false
		}
		if !errors.Is(openErr, windows.ERROR_ACCESS_DENIED) &&
			!errors.Is(openErr, windows.ERROR_WRITE_PROTECT) {
			return false
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func effectiveWriteAccess(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	return file.Close()
}
