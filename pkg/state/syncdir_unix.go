//go:build !windows

package state

import (
	"fmt"
	"os"
)

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open profile directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync profile directory: %w", err)
	}
	return nil
}
