//go:build !darwin && !linux && !windows

package state

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

type pinnedDirectory struct {
	file *os.File
	path string
	info os.FileInfo
}

func openPinnedDirectory(path string) (*pinnedDirectory, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &pinnedDirectory{file: file, path: path, info: info}, nil
}

func supportsPinnedOwnership() bool       { return false }
func (dir *pinnedDirectory) close() error { return dir.file.Close() }
func (dir *pinnedDirectory) samePath() error {
	current, err := os.Stat(dir.path)
	if err != nil {
		return err
	}
	if !os.SameFile(dir.info, current) {
		return errDirectoryReplaced
	}
	return nil
}
func (dir *pinnedDirectory) openFile(name string, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir.path, name), flags, perm)
}
func (dir *pinnedDirectory) openRegularFileForRead(string) (*os.File, error) {
	return nil, ErrSafeOwnershipUnsupported
}
func (dir *pinnedDirectory) openDirectory(name string, create bool) (*pinnedDirectory, error) {
	return nil, ErrSafeOwnershipUnsupported
}
func (dir *pinnedDirectory) readDir() ([]os.DirEntry, error) {
	return nil, ErrSafeOwnershipUnsupported
}
func (dir *pinnedDirectory) isRegular(name string) (bool, error) {
	return false, ErrSafeOwnershipUnsupported
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
	file, err := os.CreateTemp(dir.path, prefix+"*.tmp")
	if err != nil {
		return nil, "", err
	}
	return file, filepath.Base(file.Name()), nil
}
func (dir *pinnedDirectory) rename(oldName, newName string) error {
	return os.Rename(filepath.Join(dir.path, oldName), filepath.Join(dir.path, newName))
}
func (dir *pinnedDirectory) sameFileAt(file *os.File, name string) error {
	expected, err := file.Stat()
	if err != nil {
		return err
	}
	actual, err := os.Stat(filepath.Join(dir.path, name))
	if err != nil {
		return err
	}
	if !os.SameFile(expected, actual) {
		return errDirectoryReplaced
	}
	return nil
}
func (dir *pinnedDirectory) remove(name string) error {
	err := os.Remove(filepath.Join(dir.path, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
func (dir *pinnedDirectory) sync() error { return syncDirectory(dir.path) }
