//go:build linux

package gke

import "golang.org/x/sys/unix"

func renameQuarantineNoReplace(dirfd int, from, to string) error {
	return unix.Renameat2(dirfd, from, dirfd, to, unix.RENAME_NOREPLACE)
}
