//go:build darwin

package gke

import "golang.org/x/sys/unix"

func renameQuarantineNoReplace(dirfd int, from, to string) error {
	return unix.RenameatxNp(dirfd, from, dirfd, to, unix.RENAME_EXCL)
}
