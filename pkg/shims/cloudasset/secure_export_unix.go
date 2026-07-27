//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package cloudasset

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func secureWriteAssetExport(root, relative string, payload []byte) error {
	parts, err := validateExportPath(relative)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(rootFD)
	parentFD, err := secureExportParent(rootFD, parts[:len(parts)-1])
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)

	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	tempName := ".assets-" + hex.EncodeToString(random[:])
	fd, err := unix.Openat(parentFD, tempName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	temp := os.NewFile(uintptr(fd), tempName)
	if temp == nil {
		unix.Close(fd)
		return fmt.Errorf("open export temp file")
	}
	removeTemp := true
	defer func() {
		temp.Close()
		if removeTemp {
			_ = unix.Unlinkat(parentFD, tempName, 0)
		}
	}()
	if _, err := temp.Write(payload); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := unix.Renameat(parentFD, tempName, parentFD, parts[len(parts)-1]); err != nil {
		return err
	}
	removeTemp = false
	return nil
}

func secureExportParent(rootFD int, parts []string) (int, error) {
	current, err := unix.Dup(rootFD)
	if err != nil {
		return -1, err
	}
	for _, part := range parts {
		next, openErr := unix.Openat(current, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr == unix.ENOENT {
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

func validateExportPath(relative string) ([]string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return nil, fmt.Errorf("local destination must be a relative path")
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("local destination escapes the export root")
	}
	parts := strings.FieldsFunc(clean, func(r rune) bool { return r == '/' || r == '\\' })
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid local destination")
	}
	for _, part := range parts {
		if part == "." || part == ".." || part == "" {
			return nil, fmt.Errorf("invalid local destination")
		}
	}
	return parts, nil
}
