//go:build windows

package state

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

type pinnedDirectory struct {
	file   *os.File
	path   string
	id     windowsFileID
	parent *pinnedDirectory
	name   string
}

type windowsFileID struct {
	volume    uint32
	indexHigh uint32
	indexLow  uint32
}

func openPinnedDirectory(path string) (*pinnedDirectory, error) {
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	// Omitting FILE_SHARE_DELETE pins the directory name on local NTFS for the
	// lifetime of this handle. Remote filesystems may provide weaker sharing or
	// write-through guarantees and must not be assumed equivalent to local NTFS.
	handle, err := windows.CreateFile(
		pathPointer,
		windows.FILE_LIST_DIRECTORY|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		windows.CloseHandle(handle)
		return nil, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		windows.CloseHandle(handle)
		return nil, fmt.Errorf("%w: not a directory: %s", ErrInvalidPath, path)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		windows.CloseHandle(handle)
		return nil, fmt.Errorf("%w: reparse-point directory: %s", ErrInvalidPath, path)
	}
	return &pinnedDirectory{
		file: os.NewFile(uintptr(handle), path),
		path: path,
		id: windowsFileID{
			volume:    info.VolumeSerialNumber,
			indexHigh: info.FileIndexHigh,
			indexLow:  info.FileIndexLow,
		},
	}, nil
}

func supportsPinnedOwnership() bool {
	return true
}

func (dir *pinnedDirectory) close() error {
	return dir.file.Close()
}

func (dir *pinnedDirectory) samePath() error {
	var current *pinnedDirectory
	var err error
	if dir.parent != nil {
		current, err = dir.parent.openDirectory(dir.name, false)
	} else {
		current, err = openPinnedDirectory(dir.path)
	}
	if err != nil {
		return err
	}
	defer current.close()
	if dir.id != current.id {
		return errDirectoryReplaced
	}
	return nil
}

func (dir *pinnedDirectory) openFile(name string, flags int, perm os.FileMode) (*os.File, error) {
	if !containedChildName(name) {
		return nil, fmt.Errorf("%w: invalid child file %q", ErrInvalidPath, name)
	}
	access := uint32(windows.GENERIC_READ)
	switch flags & (os.O_WRONLY | os.O_RDWR) {
	case os.O_WRONLY:
		access = windows.GENERIC_WRITE
	case os.O_RDWR:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	}
	if flags&os.O_CREATE != 0 {
		access |= windows.GENERIC_WRITE
	}
	creation := uint32(windows.OPEN_EXISTING)
	switch {
	case flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0:
		creation = windows.CREATE_NEW
	case flags&os.O_CREATE != 0 && flags&os.O_TRUNC != 0:
		creation = windows.CREATE_ALWAYS
	case flags&os.O_CREATE != 0:
		creation = windows.OPEN_ALWAYS
	case flags&os.O_TRUNC != 0:
		creation = windows.TRUNCATE_EXISTING
	}
	return dir.openChildFile(
		name,
		access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		creation,
		windows.FILE_ATTRIBUTE_NORMAL,
	)
}

func (dir *pinnedDirectory) openRegularFileForRead(name string) (*os.File, error) {
	file, err := dir.openFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrInvalidPath, name)
	}
	return file, nil
}

func (dir *pinnedDirectory) openDirectory(name string, create bool) (*pinnedDirectory, error) {
	if !containedChildName(name) {
		return nil, fmt.Errorf("%w: invalid child directory %q", ErrInvalidPath, name)
	}
	path := filepath.Join(dir.path, name)
	if create {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	child, err := openPinnedDirectory(path)
	if err != nil {
		return nil, err
	}
	child.parent = dir
	child.name = name
	return child, nil
}

func (dir *pinnedDirectory) readDir() ([]os.DirEntry, error) {
	return dir.file.ReadDir(-1)
}

func (dir *pinnedDirectory) isRegular(name string) (bool, error) {
	file, err := dir.openFile(name, os.O_RDONLY, 0)
	if err != nil {
		return false, err
	}
	defer file.Close()
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return false, err
	}
	return info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0, nil
}

func (dir *pinnedDirectory) readFile(name string) ([]byte, error) {
	file, err := dir.openFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}

func (dir *pinnedDirectory) createTemp(prefix string) (*os.File, string, error) {
	for range 100 {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, "", err
		}
		name := prefix + hex.EncodeToString(random[:]) + ".tmp"
		path, err := windows.UTF16PtrFromString(filepath.Join(dir.path, name))
		if err != nil {
			return nil, "", err
		}
		handle, err := windows.CreateFile(
			path,
			windows.GENERIC_READ|windows.GENERIC_WRITE|windows.DELETE,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
			nil,
			windows.CREATE_NEW,
			windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_WRITE_THROUGH|windows.FILE_FLAG_OPEN_REPARSE_POINT,
			0,
		)
		if errors.Is(err, windows.ERROR_FILE_EXISTS) || errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		return os.NewFile(uintptr(handle), name), name, nil
	}
	return nil, "", fmt.Errorf("exhausted temporary state names")
}

func (dir *pinnedDirectory) rename(oldName, newName string) error {
	if !containedChildName(oldName) || !containedChildName(newName) {
		return fmt.Errorf("%w: invalid rename path", ErrInvalidPath)
	}
	target, err := dir.openChildFile(
		newName,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
	)
	if err == nil {
		target.Close()
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return fmt.Errorf("inspect replacement target: %w", err)
	}
	oldPath, err := windows.UTF16PtrFromString(filepath.Join(dir.path, oldName))
	if err != nil {
		return err
	}
	newPath, err := windows.UTF16PtrFromString(filepath.Join(dir.path, newName))
	if err != nil {
		return err
	}
	return windows.MoveFileEx(
		oldPath,
		newPath,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

func (dir *pinnedDirectory) sameFileAt(file *os.File, name string) error {
	var expected windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &expected); err != nil {
		return err
	}
	current, err := dir.openChildFile(
		name,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
	)
	if err != nil {
		return err
	}
	defer current.Close()
	var actual windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(current.Fd()), &actual); err != nil {
		return err
	}
	if expected.VolumeSerialNumber != actual.VolumeSerialNumber ||
		expected.FileIndexHigh != actual.FileIndexHigh ||
		expected.FileIndexLow != actual.FileIndexLow {
		return errDirectoryReplaced
	}
	return nil
}

func (dir *pinnedDirectory) remove(name string) error {
	if !containedChildName(name) {
		return fmt.Errorf("%w: invalid remove path %q", ErrInvalidPath, name)
	}
	file, err := dir.openChildFile(
		name,
		windows.DELETE|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
	)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	deleteFile := byte(1)
	return windows.SetFileInformationByHandle(
		windows.Handle(file.Fd()),
		windows.FileDispositionInfo,
		&deleteFile,
		uint32(unsafe.Sizeof(deleteFile)),
	)
}

func (dir *pinnedDirectory) sync() error {
	// MoveFileEx with MOVEFILE_WRITE_THROUGH does not return until the move has
	// been flushed. Windows does not support fsync on directory handles.
	return nil
}

func (dir *pinnedDirectory) openChildFile(
	name string,
	access, share, creation, attributes uint32,
) (*os.File, error) {
	if !containedChildName(name) {
		return nil, fmt.Errorf("%w: invalid child file %q", ErrInvalidPath, name)
	}
	path, err := windows.UTF16PtrFromString(filepath.Join(dir.path, name))
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(
		path,
		access,
		share,
		nil,
		creation,
		attributes|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		windows.CloseHandle(handle)
		return nil, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		windows.CloseHandle(handle)
		return nil, fmt.Errorf("%w: reparse-point child %q", ErrInvalidPath, name)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		windows.CloseHandle(handle)
		return nil, fmt.Errorf("%w: child is a directory %q", ErrInvalidPath, name)
	}
	return os.NewFile(uintptr(handle), name), nil
}

func containedChildName(name string) bool {
	return name != "" && name != "." && name != ".." &&
		filepath.Base(name) == name && !strings.ContainsAny(name, `/\`)
}
