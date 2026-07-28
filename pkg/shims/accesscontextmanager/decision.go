package accesscontextmanager

import (
	"net/netip"
	"sort"
	"strings"
)

// AccessRequest is the package-local VPC Service Controls decision input.
type AccessRequest struct {
	Project  string `json:"project"`
	Service  string `json:"service"`
	SourceIP string `json:"sourceIp"`
	Region   string `json:"region"`
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
	if api.PersistenceError() != nil {
		return AccessDecision{Reason: "policy persistence is unavailable"}
	}
	if !validProjectResource(request.Project) {
		return AccessDecision{Reason: "invalid project resource"}
	}
	if strings.TrimSpace(request.Service) == "" {
		return AccessDecision{Reason: "invalid service"}
	}

	api.mu.RLock()
	defer api.mu.RUnlock()
	perimeters := make([]*ServicePerimeter, 0)
	for _, perimeter := range api.perimeters {
		if perimeter == nil || perimeter.Status == nil ||
			!contains(perimeter.Status.Resources, request.Project) ||
			!contains(perimeter.Status.RestrictedServices, request.Service) {
			continue
		}
		perimeters = append(perimeters, perimeter)
	}
	sort.Slice(perimeters, func(i, j int) bool {
		return perimeters[i].Name < perimeters[j].Name
	})
	var allowedPerimeter string
	for _, perimeter := range perimeters {
		matched := false
		for _, levelName := range perimeter.Status.AccessLevels {
			if level := api.levels[levelName]; levelMatches(level, request) {
				matched = true
				break
			}
		}
		if !matched {
			return AccessDecision{Reason: "restricted by service perimeter", Perimeter: perimeter.Name}
		}
		if allowedPerimeter == "" {
			allowedPerimeter = perimeter.Name
		}
	}
	if allowedPerimeter != "" {
		return AccessDecision{Allowed: true, Reason: "access level matched", Perimeter: allowedPerimeter}
	}
	return AccessDecision{Allowed: true, Reason: "not restricted by a service perimeter"}
}

// EvaluateServicePerimeter exposes the bounded persisted decision to the
// gateway without coupling the router to this shim package.
func (api *API) EvaluateServicePerimeter(project, service, sourceIP, region string) (bool, bool) {
	decision := api.CheckAccess(AccessRequest{
		Project:  project,
		Service:  service,
		SourceIP: sourceIP,
		Region:   region,
	})
	return decision.Perimeter != "" || !decision.Allowed, decision.Allowed
}

func validProjectResource(project string) bool {
	if !strings.HasPrefix(project, "projects/") || strings.Count(project, "/") != 1 {
		return false
	}
	segment := strings.TrimPrefix(project, "projects/")
	if segment == "" || segment == "." || segment == ".." {
		return false
	}
	for index, character := range segment {
		alphaNumeric := (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9')
		if !alphaNumeric && character != '-' {
			return false
		}
		if (index == 0 || index == len(segment)-1) && !alphaNumeric {
			return false
		}
	}
	return true
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
