//go:build !(linux || darwin || freebsd || openbsd || netbsd || dragonfly)

package cloudasset

import "fmt"

func secureWriteAssetExport(string, string, []byte) error {
	return fmt.Errorf("descriptor-pinned export is unsupported on this platform")
}
