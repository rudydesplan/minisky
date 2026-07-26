//go:build windows

package gke

import (
	"os"
	"os/exec"
)

func secureKubeconfigPlatformCheck() error { return errSecureKubeconfigUnsupported }

func securePrepareKubeconfig(string) (*secureKubeconfigTarget, error) {
	return nil, errSecureKubeconfigUnsupported
}

func secureKubeconfigCommandPath(*secureKubeconfigTarget, *exec.Cmd) (string, error) {
	return "", errSecureKubeconfigUnsupported
}

func securePublishKubeconfig(*secureKubeconfigTarget, string) error {
	return errSecureKubeconfigUnsupported
}

func secureDiscardKubeconfig(*secureKubeconfigTarget) error {
	return errSecureKubeconfigUnsupported
}

func secureReadKubeconfig(string) ([]byte, error) {
	return nil, errSecureKubeconfigUnsupported
}

func secureReadKubeconfigExpected(string, os.FileInfo) ([]byte, error) {
	return nil, errSecureKubeconfigUnsupported
}

func secureReadKubeconfigOwnership(string, *kubeconfigOwnership) ([]byte, error) {
	return nil, errSecureKubeconfigUnsupported
}

func kubeconfigOwnershipFromFileInfo(ClusterIdentity, os.FileInfo) (*kubeconfigOwnership, error) {
	return nil, errSecureKubeconfigUnsupported
}

func secureQuarantineOwnedKubeconfig(string, *kubeconfigOwnership) error {
	return errSecureKubeconfigUnsupported
}
