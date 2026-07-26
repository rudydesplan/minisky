//go:build !darwin && !linux && !windows

package state

import (
	"fmt"
	"os"
	"runtime"
)

func lockFile(*os.File, bool, bool) error {
	return fmt.Errorf("profile locking is unsupported on %s", runtime.GOOS)
}

func unlockFile(*os.File) error {
	return nil
}

func isReadOnlyFilesystemError(error) bool {
	return false
}
