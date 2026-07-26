package gke

import (
	"errors"
	"os"
)

var errSecureKubeconfigUnsupported = errors.New("secure kubeconfig operations unsupported on this platform")

var testSecureQuarantineOwned func() error

type secureKubeconfigTarget struct {
	path                 string
	entryName            string
	file                 *os.File
	dir                  *os.File
	fileInfo             os.FileInfo
	dirInfo              os.FileInfo
	ownership            *kubeconfigOwnership
	testFileSync         func() error
	testDirSync          func() error
	testFileClose        func() error
	testDirClose         func() error
	testBeforeFinalCheck func() error
	testBeforeQuarantine func() error
	testAfterQuarantine  func(string) error
}

type kubeconfigOwnership struct {
	Profile     string `json:"profile"`
	Project     string `json:"project"`
	Zone        string `json:"zone"`
	Cluster     string `json:"cluster"`
	BackendName string `json:"backendName,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
}

func (ownership *kubeconfigOwnership) hasBackendNonce() bool {
	if ownership == nil || len(ownership.BackendName) != len("minisky-owned-")+32 ||
		ownership.BackendName[:len("minisky-owned-")] != "minisky-owned-" {
		return false
	}
	for _, r := range ownership.BackendName[len("minisky-owned-"):] {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func (ownership *kubeconfigOwnership) matchesIdentity(identity ClusterIdentity) bool {
	return ownership != nil &&
		ownership.Profile == identity.Profile &&
		ownership.Project == identity.Project &&
		ownership.Zone == identity.Zone &&
		ownership.Cluster == identity.Cluster
}

func (ownership *kubeconfigOwnership) hasContentDigest() bool {
	if ownership == nil || len(ownership.SHA256) != 64 {
		return false
	}
	for _, r := range ownership.SHA256 {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func (ownership *kubeconfigOwnership) isDurable() bool {
	return ownership.hasBackendNonce() && ownership.hasContentDigest()
}
