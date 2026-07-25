package orchestrator

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"minisky/pkg/config"
)

// ServerlessResourceType distinguishes backend resources that may share the
// same project, location, and short name.
type ServerlessResourceType string

const (
	ServerlessFunction         ServerlessResourceType = "function"
	ServerlessService          ServerlessResourceType = "service"
	ServerlessAppEngineVersion ServerlessResourceType = "appengine-version"
)

// ServerlessIdentity is the complete logical identity used for backend names
// and ownership checks. Container IDs remain private implementation details.
type ServerlessIdentity struct {
	ResourceType ServerlessResourceType
	Project      string
	Location     string
	Name         string
}

func (identity ServerlessIdentity) validate() error {
	switch identity.ResourceType {
	case ServerlessFunction, ServerlessService, ServerlessAppEngineVersion:
	default:
		return fmt.Errorf("invalid Serverless resource type %q", identity.ResourceType)
	}
	if identity.Project == "" || identity.Location == "" || identity.Name == "" {
		return fmt.Errorf("Serverless resource identity requires project, location, and name")
	}
	return nil
}

// CanonicalResource returns the GCP-shaped logical resource name.
func (identity ServerlessIdentity) CanonicalResource() string {
	collection := "functions"
	switch identity.ResourceType {
	case ServerlessService:
		collection = "services"
	case ServerlessAppEngineVersion:
		collection = "appengineVersions"
	}
	return fmt.Sprintf("projects/%s/locations/%s/%s/%s", identity.Project, identity.Location, collection, identity.Name)
}

// ContainerName derives a readable Docker name with a collision-resistant
// suffix from the active profile and complete logical identity.
func (identity ServerlessIdentity) ContainerName() (string, error) {
	if err := identity.validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf("minisky-%s-%s-%s", identity.shortType(), identity.readableName(), identity.hashSuffix()), nil
}

// ImageName derives the local image tag from the same complete identity used
// for the container.
func (identity ServerlessIdentity) ImageName() (string, error) {
	if err := identity.validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf("minisky-%s-%s-%s:local", identity.shortType(), identity.readableName(), identity.hashSuffix()), nil
}

func (identity ServerlessIdentity) labels() (map[string]string, error) {
	if err := identity.validate(); err != nil {
		return nil, err
	}
	labels := ownedDockerLabels()
	labels["minisky.service"] = "serverless"
	labels["minisky.resource"] = identity.CanonicalResource()
	labels["minisky.resource-type"] = string(identity.ResourceType)
	return labels, nil
}

func (identity ServerlessIdentity) shortType() string {
	switch identity.ResourceType {
	case ServerlessFunction:
		return "fn"
	case ServerlessService:
		return "svc"
	default:
		return "gae"
	}
}

func (identity ServerlessIdentity) readableName() string {
	sanitized := strings.ToLower(identity.Name)
	sanitized = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, sanitized)
	sanitized = strings.Trim(sanitized, "-")
	if len(sanitized) > 24 {
		sanitized = strings.TrimRight(sanitized[:24], "-")
	}
	if sanitized == "" {
		return "resource"
	}
	return sanitized
}

func (identity ServerlessIdentity) hashSuffix() string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		config.GetProfile(),
		string(identity.ResourceType),
		identity.Project,
		identity.Location,
		identity.Name,
	}, "\x00")))
	return fmt.Sprintf("%x", sum[:8])
}
