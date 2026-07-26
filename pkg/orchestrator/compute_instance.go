package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"minisky/pkg/config"
)

// ComputeInstanceIdentity is the exact zonal identity of a MiniSky-managed VM.
type ComputeInstanceIdentity struct {
	Project  string
	Zone     string
	Instance string
}

func (identity ComputeInstanceIdentity) Validate() error {
	if !gcpProjectID.MatchString(identity.Project) ||
		!gcpNetworkName.MatchString(identity.Zone) ||
		!gcpNetworkName.MatchString(identity.Instance) {
		return fmt.Errorf("invalid Compute instance identity")
	}
	return nil
}

func (identity ComputeInstanceIdentity) CanonicalResource() string {
	return fmt.Sprintf("projects/%s/zones/%s/instances/%s", identity.Project, identity.Zone, identity.Instance)
}

func (identity ComputeInstanceIdentity) DockerName() (string, error) {
	if err := identity.Validate(); err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(config.GetProfile() + "\x00" + identity.CanonicalResource()))
	suffix := hex.EncodeToString(hash[:8])
	const prefix = "minisky-vm-"
	maxReadable := 63 - len(prefix) - 1 - len(suffix)
	readable := identity.Instance
	if len(readable) > maxReadable {
		readable = strings.TrimRight(readable[:maxReadable], "-")
	}
	return prefix + readable + "-" + suffix, nil
}

func (identity ComputeInstanceIdentity) labels() (map[string]string, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		"managed-by":                 "minisky",
		"minisky.profile":            config.GetProfile(),
		"minisky.service":            "compute-instance",
		"minisky.project":            identity.Project,
		"minisky.zone":               identity.Zone,
		"minisky.instance":           identity.Instance,
		"minisky.canonical-resource": identity.CanonicalResource(),
	}, nil
}
