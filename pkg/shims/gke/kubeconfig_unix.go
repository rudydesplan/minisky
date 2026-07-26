//go:build unix

package gke

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func secureKubeconfigPlatformCheck() error { return nil }

func openKubeconfigDir(path string, create bool) (int, error) {
	clean, err := filepath.Abs(path)
	if err != nil {
		return -1, err
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator)) {
		next, openErr := unix.Openat(fd, component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr == unix.ENOENT && create {
			if mkdirErr := unix.Mkdirat(fd, component, 0o700); mkdirErr != nil && mkdirErr != unix.EEXIST {
				return -1, errors.Join(mkdirErr, unix.Close(fd))
			}
			next, openErr = unix.Openat(fd, component,
				unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		}
		closeErr := unix.Close(fd)
		if openErr != nil {
			return -1, fmt.Errorf("open pinned kubeconfig directory: %w", errors.Join(openErr, closeErr))
		}
		if closeErr != nil {
			return -1, fmt.Errorf("close parent kubeconfig directory: %w",
				errors.Join(closeErr, unix.Close(next)))
		}
		fd = next
	}
	return fd, nil
}

func securePrepareKubeconfig(path string) (*secureKubeconfigTarget, error) {
	kubeconfigLifecycleMu.Lock()
	defer kubeconfigLifecycleMu.Unlock()
	dirfd, err := openKubeconfigDir(filepath.Dir(path), true)
	if err != nil {
		return nil, err
	}
	if err := checkKubeconfigEntryCapacity(dirfd); err != nil {
		return nil, errors.Join(err, unix.Close(dirfd))
	}
	if err := unix.Close(dirfd); err != nil {
		return nil, err
	}
	return securePrepareKubeconfigUnlocked(path)
}

func securePrepareKubeconfigUnlocked(path string) (*secureKubeconfigTarget, error) {
	dirfd, err := openKubeconfigDir(filepath.Dir(path), true)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(dirfd), filepath.Dir(path))
	dirInfo, err := dir.Stat()
	if err != nil {
		return nil, errors.Join(err, dir.Close())
	}
	name := filepath.Base(path)
	fd, err := unix.Openat(dirfd, name,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, errors.Join(err, dir.Close())
	}
	file := os.NewFile(uintptr(fd), path)
	fileInfo, err := file.Stat()
	if err != nil {
		zeroErr := errors.Join(file.Truncate(0), file.Chmod(0), file.Sync(), unix.Fsync(dirfd))
		return nil, errors.Join(
			fmt.Errorf("inspect created kubeconfig; retained zeroized tombstone: %w", err),
			zeroErr, file.Close(), dir.Close())
	}
	return &secureKubeconfigTarget{
		path: path, entryName: name, file: file, dir: dir,
		fileInfo: fileInfo, dirInfo: dirInfo,
	}, nil
}

func secureKubeconfigCommandPath(target *secureKubeconfigTarget, cmd *exec.Cmd) (string, error) {
	if target == nil || target.file == nil {
		return "", os.ErrInvalid
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, target.file)
	return "/dev/fd/" + strconv.Itoa(3+len(cmd.ExtraFiles)-1), nil
}

// securePublishKubeconfig guarantees that no attacker-controlled inode is
// accepted or deleted while this operation owns the lifecycle lock. A same-UID
// process can mutate the path after this function returns; later readers must
// reopen descriptor-relative with O_NOFOLLOW and validate the resulting file.
func securePublishKubeconfig(target *secureKubeconfigTarget, path string) error {
	if target == nil || target.file == nil || target.dir == nil ||
		target.path != path {
		return os.ErrPermission
	}
	fail := func(cause error) error {
		cleanupErr := secureDiscardKubeconfig(target)
		if cleanupErr != nil {
			return errors.Join(cause, fmt.Errorf("kubeconfig cleanup ambiguous: %w", cleanupErr))
		}
		return cause
	}
	descriptorInfo, err := target.file.Stat()
	if err != nil || !os.SameFile(target.fileInfo, descriptorInfo) {
		return fail(errors.Join(os.ErrPermission, err))
	}
	dirfd := int(target.dir.Fd())
	fd, err := unix.Openat(dirfd, target.entryName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fail(err)
	}
	file := os.NewFile(uintptr(fd), target.path)
	info, statErr := file.Stat()
	if statErr != nil || !os.SameFile(target.fileInfo, info) ||
		!info.Mode().IsRegular() {
		return fail(errors.Join(os.ErrPermission, statErr, file.Close()))
	}
	if err := file.Close(); err != nil {
		return fail(err)
	}
	currentDirFD, err := openKubeconfigDir(filepath.Dir(path), false)
	if err != nil {
		return fail(err)
	}
	currentDir := os.NewFile(uintptr(currentDirFD), filepath.Dir(path))
	currentDirInfo, statErr := currentDir.Stat()
	closeErr := currentDir.Close()
	if statErr != nil || !os.SameFile(target.dirInfo, currentDirInfo) {
		return fail(errors.Join(os.ErrPermission, statErr, closeErr))
	}
	if closeErr != nil {
		return fail(closeErr)
	}
	if err := target.file.Chmod(0o600); err != nil {
		return fail(err)
	}
	if info, err := target.file.Stat(); err != nil || !os.SameFile(target.fileInfo, info) ||
		!info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fail(errors.Join(os.ErrPermission, err))
	}
	if target.testFileSync != nil {
		err = target.testFileSync()
	} else {
		err = target.file.Sync()
	}
	if err != nil {
		return fail(err)
	}
	if target.testDirSync != nil {
		err = target.testDirSync()
	} else {
		err = unix.Fsync(dirfd)
	}
	if err != nil {
		return fail(err)
	}
	if target.testBeforeFinalCheck != nil {
		if err := target.testBeforeFinalCheck(); err != nil {
			return fail(err)
		}
	}
	finalFD, err := unix.Openat(dirfd, target.entryName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fail(err)
	}
	finalFile := os.NewFile(uintptr(finalFD), target.path)
	finalInfo, statErr := finalFile.Stat()
	finalCloseErr := finalFile.Close()
	if statErr != nil || finalCloseErr != nil || !os.SameFile(target.fileInfo, finalInfo) {
		return fail(errors.Join(os.ErrPermission, statErr, finalCloseErr))
	}
	if target.testFileClose != nil {
		err = target.testFileClose()
	} else {
		err = target.file.Close()
	}
	if err != nil {
		return fail(err)
	}
	target.file = nil
	if target.testDirClose != nil {
		err = target.testDirClose()
	} else {
		err = target.dir.Close()
	}
	if err != nil {
		return fail(err)
	}
	target.dir = nil
	return nil
}

// secureDiscardKubeconfig atomically moves at most one entry to an unguessable
// quarantine name. It never unlinks a pathname after inspection: owned inodes
// are zeroized through an open descriptor and retained as mode-000 tombstones;
// unexpected inodes are left untouched with an explicit tamper error. No
// automatic sweeper may delete these names without equivalent durable evidence.
func secureDiscardKubeconfig(target *secureKubeconfigTarget) error {
	kubeconfigLifecycleMu.Lock()
	defer kubeconfigLifecycleMu.Unlock()
	return secureDiscardKubeconfigUnlocked(target)
}

func secureDiscardKubeconfigUnlocked(target *secureKubeconfigTarget) error {
	if target == nil {
		return nil
	}
	if target.dir == nil {
		if target.file != nil {
			err := target.file.Close()
			target.file = nil
			return err
		}
		return nil
	}
	dirfd := int(target.dir.Fd())
	var cleanupErr error
	if target.testBeforeQuarantine != nil {
		cleanupErr = errors.Join(cleanupErr, target.testBeforeQuarantine())
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("create quarantine name: %w", err))
	} else {
		quarantineName := ".quarantine-" + hex.EncodeToString(random[:])
		if err := renameQuarantineNoReplace(dirfd, target.entryName, quarantineName); err == unix.ENOENT {
			// The created entry is already absent; do not touch any later entry.
			cleanupErr = errors.Join(cleanupErr, unix.Fsync(dirfd))
		} else if err == unix.EEXIST {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("quarantine collision: %w", err))
		} else if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("quarantine kubeconfig: %w", err))
		} else {
			if target.testAfterQuarantine != nil {
				cleanupErr = errors.Join(cleanupErr,
					target.testAfterQuarantine(filepath.Join(filepath.Dir(target.path), quarantineName)))
			}
			fd, openErr := unix.Openat(dirfd, quarantineName,
				unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if openErr != nil {
				cleanupErr = errors.Join(cleanupErr,
					fmt.Errorf("inspect quarantined kubeconfig: %w", openErr))
			} else {
				entry := os.NewFile(uintptr(fd), quarantineName)
				info, statErr := entry.Stat()
				if statErr != nil {
					cleanupErr = errors.Join(cleanupErr, statErr, entry.Close())
				} else if !os.SameFile(target.fileInfo, info) {
					cleanupErr = errors.Join(cleanupErr,
						fmt.Errorf("quarantined kubeconfig inode mismatch: %w", os.ErrPermission),
						entry.Close())
				} else {
					// Never unlink a checked pathname: a same-UID process could
					// replace it after inspection. Zeroize the still-open owned
					// inode and leave a bounded, non-sensitive quarantine tombstone.
					cleanupErr = errors.Join(cleanupErr,
						entry.Truncate(0), entry.Chmod(0), entry.Sync(), entry.Close())
				}
			}
			cleanupErr = errors.Join(cleanupErr, unix.Fsync(dirfd))
		}
	}
	var closeErr error
	if target.file != nil {
		closeErr = target.file.Close()
		target.file = nil
	}
	dirCloseErr := target.dir.Close()
	target.dir = nil
	return errors.Join(cleanupErr, closeErr, dirCloseErr)
}

func secureReadKubeconfig(path string) ([]byte, error) {
	return secureReadKubeconfigValidated(path, nil, nil)
}

func secureReadKubeconfigExpected(path string, expected os.FileInfo) ([]byte, error) {
	return secureReadKubeconfigValidated(path, expected, nil)
}

func secureReadKubeconfigOwnership(path string, ownership *kubeconfigOwnership) ([]byte, error) {
	return secureReadKubeconfigValidated(path, nil, ownership)
}

func secureReadKubeconfigValidated(
	path string,
	expected os.FileInfo,
	ownership *kubeconfigOwnership,
) ([]byte, error) {
	dirfd, err := openKubeconfigDir(filepath.Dir(path), false)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Openat(dirfd, filepath.Base(path), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.Join(err, unix.Close(dirfd))
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 ||
		(expected != nil && !os.SameFile(expected, info)) ||
		(ownership != nil && !kubeconfigOwnershipMatches(info, ownership)) {
		return nil, errors.Join(os.ErrPermission, err, file.Close(), unix.Close(dirfd))
	}
	data, readErr := io.ReadAll(file)
	return data, errors.Join(readErr, file.Close(), unix.Close(dirfd))
}

func kubeconfigOwnershipFromFileInfo(identity ClusterIdentity, info os.FileInfo) (*kubeconfigOwnership, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("kubeconfig ownership identity unavailable")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate backend ownership nonce: %w", err)
	}
	return &kubeconfigOwnership{
		Profile: identity.Profile, Project: identity.Project, Zone: identity.Zone, Cluster: identity.Cluster,
		BackendName: "minisky-owned-" + hex.EncodeToString(nonce[:]),
		Device:      uint64(stat.Dev), Inode: uint64(stat.Ino),
	}, nil
}

func kubeconfigOwnershipMatches(info os.FileInfo, ownership *kubeconfigOwnership) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && ownership != nil &&
		uint64(stat.Dev) == ownership.Device && uint64(stat.Ino) == ownership.Inode
}

// secureQuarantineOwnedKubeconfig applies the same no-unlink invariant to
// normal deletion using ownership evidence persisted with GKE metadata.
func secureQuarantineOwnedKubeconfig(path string, ownership *kubeconfigOwnership) error {
	kubeconfigLifecycleMu.Lock()
	defer kubeconfigLifecycleMu.Unlock()
	return secureQuarantineOwnedKubeconfigUnlocked(path, ownership)
}

func secureQuarantineOwnedKubeconfigUnlocked(path string, ownership *kubeconfigOwnership) error {
	if testSecureQuarantineOwned != nil {
		if err := testSecureQuarantineOwned(); err != nil {
			return err
		}
	}
	dirfd, err := openKubeconfigDir(filepath.Dir(path), false)
	if err != nil {
		return err
	}
	finalExists := true
	if fd, openErr := unix.Openat(dirfd, filepath.Base(path),
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0); openErr == nil {
		if err := unix.Close(fd); err != nil {
			return errors.Join(err, unix.Close(dirfd))
		}
	} else if openErr == unix.ENOENT {
		finalExists = false
	}
	if found, err := zeroizeExistingOwnedTombstone(dirfd, ownership); err != nil {
		return errors.Join(err, unix.Close(dirfd))
	} else if found {
		return unix.Close(dirfd)
	}
	if !finalExists {
		return errors.Join(os.ErrNotExist, unix.Close(dirfd))
	}
	var cleanupErr error
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		cleanupErr = fmt.Errorf("create quarantine name: %w", err)
	} else {
		// The entry remains a quarantine name because ownership is verified
		// only after the atomic rename. Expected entries become zero-byte,
		// mode-000 tombstones; unexpected inodes remain untouched quarantines.
		name := ".quarantine-" + hex.EncodeToString(random[:])
		if err := renameQuarantineNoReplace(dirfd, filepath.Base(path), name); err != nil {
			cleanupErr = fmt.Errorf("quarantine kubeconfig for deletion: %w", err)
		} else {
			fd, openErr := unix.Openat(dirfd, name, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if openErr != nil {
				cleanupErr = fmt.Errorf("inspect deleted kubeconfig quarantine: %w", openErr)
			} else {
				file := os.NewFile(uintptr(fd), name)
				info, statErr := file.Stat()
				if statErr != nil {
					cleanupErr = errors.Join(statErr, file.Close())
				} else if !kubeconfigOwnershipMatches(info, ownership) {
					cleanupErr = errors.Join(
						fmt.Errorf("deleted kubeconfig ownership mismatch: %w", os.ErrPermission),
						file.Close())
				} else {
					cleanupErr = errors.Join(file.Truncate(0), file.Chmod(0), file.Sync(), file.Close())
				}
			}
			cleanupErr = errors.Join(cleanupErr, unix.Fsync(dirfd))
		}
	}
	return errors.Join(cleanupErr, unix.Close(dirfd))
}

func zeroizeExistingOwnedTombstone(
	dirfd int,
	ownership *kubeconfigOwnership,
) (bool, error) {
	duplicate, err := unix.Dup(dirfd)
	if err != nil {
		return false, err
	}
	dir := os.NewFile(uintptr(duplicate), "kubeconfig-deleted-tombstones")
	names, readErr := dir.Readdirnames(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, err
	}
	for _, name := range names {
		if !strings.HasPrefix(name, ".deleted-") && !strings.HasPrefix(name, ".quarantine-") {
			continue
		}
		fd, err := unix.Openat(dirfd, name, unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			continue
		}
		file := os.NewFile(uintptr(fd), name)
		info, statErr := file.Stat()
		if statErr == nil && kubeconfigOwnershipMatches(info, ownership) {
			return true, errors.Join(
				file.Truncate(0), file.Chmod(0), file.Sync(), unix.Fsync(dirfd), file.Close())
		}
		if err := file.Close(); err != nil {
			return false, err
		}
	}
	return false, nil
}

// checkKubeconfigTombstoneCapacity enforces a hard per-profile bound. Entries
// are never reclaimed by pathname: without inode-conditional unlink, doing so
// could delete an attacker replacement. Operators must inspect a full directory.
func checkKubeconfigEntryCapacity(dirfd int) error {
	duplicate, err := unix.Dup(dirfd)
	if err != nil {
		return err
	}
	dir := os.NewFile(uintptr(duplicate), "kubeconfig-tombstones")
	names, readErr := dir.Readdirnames(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	count := 0
	for _, name := range names {
		if strings.HasSuffix(name, ".kubeconfig") ||
			strings.HasPrefix(name, ".quarantine-") ||
			strings.HasPrefix(name, ".deleted-") {
			count++
		}
	}
	if count >= maxKubeconfigEntries {
		return fmt.Errorf("%w: count=%d cap=%d", errKubeconfigEntryLimit, count, maxKubeconfigEntries)
	}
	return nil
}

func checkKubeconfigTombstoneCapacity(dirfd int) error {
	return checkKubeconfigEntryCapacity(dirfd)
}
