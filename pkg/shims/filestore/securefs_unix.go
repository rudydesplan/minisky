//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package filestore

import (
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func secureMkdirAll(root, relative string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	fd, err := walkDirectory(rootFD, splitSecurePath(relative), true)
	if err == nil {
		err = unix.Close(fd)
	}
	return err
}

func secureWriteFile(root, relative string, data []byte) error {
	parts := splitSecurePath(relative)
	if len(parts) < 2 {
		return fmt.Errorf("invalid file path")
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	parentFD, err := walkDirectory(rootFD, parts[:len(parts)-1], true)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	fd, err := unix.Openat(parentFD, parts[len(parts)-1],
		unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), parts[len(parts)-1])
	if file == nil {
		unix.Close(fd)
		return fmt.Errorf("open file")
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}

func secureReadFile(root, relative string, limit int64) ([]byte, error) {
	parts := splitSecurePath(relative)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid file path")
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(rootFD)
	parentFD, err := walkDirectory(rootFD, parts[:len(parts)-1], false)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parentFD)
	fd, err := unix.Openat(parentFD, parts[len(parts)-1], unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), parts[len(parts)-1])
	if file == nil {
		unix.Close(fd)
		return nil, fmt.Errorf("open file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("file exceeds local substitute limit or is not regular")
	}
	return io.ReadAll(io.LimitReader(file, limit+1))
}

func secureRemoveTree(root, relative string) error {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer unix.Close(rootFD)
	if err := removeDirectoryAt(rootFD, relative); err != nil && err != unix.ENOENT {
		return err
	}
	return nil
}

func secureRename(root, from, to string) error {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	fd, err := unix.Openat(rootFD, from, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	if err := unix.Close(fd); err != nil {
		return err
	}
	return unix.Renameat(rootFD, from, rootFD, to)
}

func secureTreeUsage(root, relative string, maxFiles int, maxBytes int64) (int, int64, error) {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return 0, 0, err
	}
	defer unix.Close(rootFD)
	fd, err := walkDirectory(rootFD, splitSecurePath(relative), false)
	if err != nil {
		return 0, 0, err
	}
	defer unix.Close(fd)
	return directoryUsage(fd, maxFiles, maxBytes)
}

func secureFileSize(root, relative string) (int64, bool, error) {
	parts := splitSecurePath(relative)
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return 0, false, err
	}
	defer unix.Close(rootFD)
	parentFD, err := walkDirectory(rootFD, parts[:len(parts)-1], false)
	if err != nil {
		if err == unix.ENOENT {
			return 0, false, nil
		}
		return 0, false, err
	}
	defer unix.Close(parentFD)
	fd, err := unix.Openat(parentFD, parts[len(parts)-1], unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err == unix.ENOENT {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	file := os.NewFile(uintptr(fd), parts[len(parts)-1])
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, false, err
	}
	if !info.Mode().IsRegular() {
		return 0, false, fmt.Errorf("target is not a regular file")
	}
	return info.Size(), true, nil
}

func directoryUsage(fd, maxFiles int, maxBytes int64) (int, int64, error) {
	dup, err := unix.Dup(fd)
	if err != nil {
		return 0, 0, err
	}
	file := os.NewFile(uintptr(dup), "usage")
	entries, err := file.ReadDir(-1)
	file.Close()
	if err != nil {
		return 0, 0, err
	}
	var files int
	var bytes int64
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			return 0, 0, fmt.Errorf("symlink in local filestore")
		}
		if entry.IsDir() {
			childFD, err := unix.Openat(fd, entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			if err != nil {
				return 0, 0, err
			}
			childFiles, childBytes, childErr := directoryUsage(childFD, maxFiles-files, maxBytes-bytes)
			unix.Close(childFD)
			if childErr != nil {
				return 0, 0, childErr
			}
			files += childFiles
			bytes += childBytes
		} else {
			info, err := entry.Info()
			if err != nil || !info.Mode().IsRegular() {
				return 0, 0, fmt.Errorf("unsupported entry in local filestore")
			}
			files++
			bytes += info.Size()
		}
		if files > maxFiles || bytes > maxBytes {
			return files, bytes, fmt.Errorf("local filestore quota exceeded")
		}
	}
	return files, bytes, nil
}

func walkDirectory(rootFD int, parts []string, create bool) (int, error) {
	current, err := unix.Dup(rootFD)
	if err != nil {
		return -1, err
	}
	for _, part := range parts {
		next, openErr := unix.Openat(current, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr == unix.ENOENT && create {
			if mkdirErr := unix.Mkdirat(current, part, 0o700); mkdirErr != nil && mkdirErr != unix.EEXIST {
				unix.Close(current)
				return -1, mkdirErr
			}
			next, openErr = unix.Openat(current, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		}
		unix.Close(current)
		if openErr != nil {
			return -1, openErr
		}
		current = next
	}
	return current, nil
}

func removeDirectoryAt(parentFD int, name string) error {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	entries, readErr := file.ReadDir(-1)
	if readErr != nil {
		file.Close()
		return readErr
	}
	for _, entry := range entries {
		if entry.IsDir() {
			if err := removeDirectoryAt(fd, entry.Name()); err != nil {
				file.Close()
				return err
			}
		} else if err := unix.Unlinkat(fd, entry.Name(), 0); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	return unix.Unlinkat(parentFD, name, unix.AT_REMOVEDIR)
}

func splitSecurePath(relative string) []string {
	return strings.FieldsFunc(relative, func(r rune) bool { return r == '/' || r == '\\' })
}
