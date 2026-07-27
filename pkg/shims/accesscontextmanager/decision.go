package accesscontextmanager

import (
	"net/netip"
	"strings"
)

// AccessRequest is the package-local VPC Service Controls decision input.
type AccessRequest struct {
	Project  string
	Service  string
	SourceIP string
	Region   string
}

// AccessDecision reports the bounded local perimeter decision.
type AccessDecision struct {
	Allowed   bool   `json:"allowed"`
	Reason    string `json:"reason"`
	Perimeter string `json:"perimeter,omitempty"`
}

// CheckAccess evaluates persisted service-perimeter metadata. It is callable by
// local shims but does not intercept unrelated handlers automatically.
func (api *API) CheckAccess(request AccessRequest) AccessDecision {
	if !validProjectResource(request.Project) {
		return AccessDecision{Reason: "invalid project resource"}
	}
	if strings.TrimSpace(request.Service) == "" {
		return AccessDecision{Reason: "invalid service"}
	}

	api.mu.RLock()
	defer api.mu.RUnlock()
	for _, perimeter := range api.perimeters {
		if perimeter == nil || perimeter.Status == nil ||
			!contains(perimeter.Status.Resources, request.Project) ||
			!contains(perimeter.Status.RestrictedServices, request.Service) {
			continue
		}
		for _, levelName := range perimeter.Status.AccessLevels {
			if level := api.levels[levelName]; levelMatches(level, request) {
				return AccessDecision{Allowed: true, Reason: "access level matched", Perimeter: perimeter.Name}
			}
		}
		return AccessDecision{Reason: "restricted by service perimeter", Perimeter: perimeter.Name}
	}
	return AccessDecision{Allowed: true, Reason: "not restricted by a service perimeter"}
}

func validProjectResource(project string) bool {
	return strings.HasPrefix(project, "projects/") &&
		strings.Count(project, "/") == 1 &&
		len(strings.TrimPrefix(project, "projects/")) > 0
}

func levelMatches(level *AccessLevel, request AccessRequest) bool {
	if level == nil || level.Basic == nil {
		return false
	}
	address, addressOK := netip.ParseAddr(request.SourceIP)
	for _, condition := range level.Basic.Conditions {
		regionMatch := len(condition.Regions) == 0 || contains(condition.Regions, request.Region)
		ipMatch := len(condition.IpSubnetworks) == 0
		for _, rawPrefix := range condition.IpSubnetworks {
			prefix, err := netip.ParsePrefix(rawPrefix)
			if err == nil && addressOK == nil && prefix.Contains(address) {
				ipMatch = true
				break
			}
		}
		if regionMatch && ipMatch {
			return true
		}
	}
	return false
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
