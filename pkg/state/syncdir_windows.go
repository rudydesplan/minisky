//go:build windows

package state

// Windows does not support fsync on directory handles. The state file itself
// is synced before the atomic rename, which is the strongest portable guarantee.
func syncDirectory(string) error {
	return nil
}
