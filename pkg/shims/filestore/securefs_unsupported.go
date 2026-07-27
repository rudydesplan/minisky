//go:build !(linux || darwin || freebsd || openbsd || netbsd || dragonfly)

package filestore

import "fmt"

var errSecureFilesystemUnsupported = fmt.Errorf("descriptor-pinned filesystem operations are unsupported on this platform")

func secureMkdirAll(string, string) error          { return errSecureFilesystemUnsupported }
func secureWriteFile(string, string, []byte) error { return errSecureFilesystemUnsupported }
func secureReadFile(string, string, int64) ([]byte, error) {
	return nil, errSecureFilesystemUnsupported
}
func secureRemoveTree(string, string) error { return errSecureFilesystemUnsupported }
func secureRename(string, string, string) error {
	return errSecureFilesystemUnsupported
}
func secureTreeUsage(string, string, int, int64) (int, int64, error) {
	return 0, 0, errSecureFilesystemUnsupported
}
func secureFileSize(string, string) (int64, bool, error) {
	return 0, false, errSecureFilesystemUnsupported
}
