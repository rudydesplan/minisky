//go:build unix && !linux && !darwin

package gke

func renameQuarantineNoReplace(int, string, string) error {
	return errSecureKubeconfigUnsupported
}
