//go:build darwin || linux

package state

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type pinnedDirectory struct {
	file   *os.File
	path   string
	info   os.FileInfo
	parent *pinnedDirectory
	name   string
}

func openPinnedDirectory(path string) (*pinnedDirectory, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &pinnedDirectory{file: file, path: path, info: info}, nil
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
	if !os.SameFile(dir.info, current.info) {
		return errDirectoryReplaced
	}
	return nil
}

func (dir *pinnedDirectory) openFile(name string, flags int, perm os.FileMode) (*os.File, error) {
	fd, err := unix.Openat(int(dir.file.Fd()), name, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(perm.Perm()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (dir *pinnedDirectory) openRegularFileForRead(name string) (*os.File, error) {
	file, err := dir.openFile(name, unix.O_RDONLY|unix.O_NONBLOCK, 0)
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
	open := func() (int, error) {
		return unix.Openat(
			int(dir.file.Fd()),
			name,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0,
		)
	}
	fd, err := open()
	if create && errors.Is(err, unix.ENOENT) {
		if mkdirErr := unix.Mkdirat(int(dir.file.Fd()), name, 0o700); mkdirErr != nil &&
			!errors.Is(mkdirErr, unix.EEXIST) {
			return nil, mkdirErr
		}
		fd, err = open()
	}
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir.path, name)
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &pinnedDirectory{
		file: file, path: path, info: info,
		parent: dir, name: name,
	}, nil
}

func (dir *pinnedDirectory) readDir() ([]os.DirEntry, error) {
	return dir.file.ReadDir(-1)
}

func (dir *pinnedDirectory) isRegular(name string) (bool, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(dir.file.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return false, err
	}
	return stat.Mode&unix.S_IFMT == unix.S_IFREG, nil
}

func (dir *pinnedDirectory) readFile(name string) ([]byte, error) {
	file, err := dir.openFile(name, unix.O_RDONLY, 0)
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
		file, err := dir.openFile(name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o600)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		return file, name, err
	}
	return nil, "", fmt.Errorf("exhausted temporary state names")
}

func (dir *pinnedDirectory) rename(oldName, newName string) error {
	return unix.Renameat(int(dir.file.Fd()), oldName, int(dir.file.Fd()), newName)
}

func (dir *pinnedDirectory) sameFileAt(file *os.File, name string) error {
	expected, err := file.Stat()
	if err != nil {
		return err
	}
	current, err := dir.openFile(name, unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer current.Close()
	actual, err := current.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(expected, actual) {
		return errDirectoryReplaced
	}
	return nil
}

func (dir *pinnedDirectory) remove(name string) error {
	err := unix.Unlinkat(int(dir.file.Fd()), name, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

func (dir *pinnedDirectory) sync() error {
	return dir.file.Sync()
}
