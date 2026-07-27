package servicemesh

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

// RouteDecision is advisory metadata; MiniSky does not program a mesh proxy.
type RouteDecision struct {
	Matched      bool               `json:"matched"`
	Route        string             `json:"route,omitempty"`
	Destinations []RouteDestination `json:"destinations,omitempty"`
	Enforcement  string             `json:"enforcement"`
}

// ValidateReferences enforces the Network Services project/location hierarchy.
// Resource existence is not required because Network Services accepts references
// whose provisioning can be ordered independently.
func (api *API) ValidateReferences(route *HttpRoute) error {
	project, location, ok := resourceParent(route.Name, "httpRoutes")
	if !ok {
		return fmt.Errorf("invalid HTTP route name")
	}
	for _, mesh := range route.Meshes {
		meshProject, meshLocation, valid := resourceParent(mesh, "meshes")
		if !valid || meshProject != project || meshLocation != location {
			return fmt.Errorf("mesh reference must share route project and location")
		}
	}
	return nil
}

// ResolveRoute evaluates stored metadata without claiming that traffic is
// intercepted. Regex matches are intentionally unsupported.
func (api *API) ResolveRoute(project, location, host, requestPath string) RouteDecision {
	prefix := fmt.Sprintf("projects/%s/locations/%s/httpRoutes/", project, location)
	api.mu.RLock()
	routes := make([]*HttpRoute, 0)
	for name, route := range api.httpRoutes {
		if strings.HasPrefix(name, prefix) {
			routes = append(routes, cloneHttpRoute(route))
		}
	}
	api.mu.RUnlock()
	sort.Slice(routes, func(i, j int) bool { return routes[i].Name < routes[j].Name })
	for _, route := range routes {
		if !matchesHostname(route.Hostnames, host) {
			continue
		}
		for _, rule := range route.Rules {
			if matchesRoute(rule.Matches, requestPath) {
				destinations := []RouteDestination(nil)
				if rule.Action != nil {
					destinations = append(destinations, rule.Action.Destinations...)
				}
				return RouteDecision{
					Matched: true, Route: route.Name, Destinations: destinations, Enforcement: "METADATA_ONLY",
				}
			}
		}
	}
	return RouteDecision{Enforcement: "METADATA_ONLY"}
}

func resourceParent(name, collection string) (string, string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" ||
		parts[4] != collection || parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

func matchesHostname(patterns []string, host string) bool {
	for _, pattern := range patterns {
		if pattern == host {
			return true
		}
		if matched, err := path.Match(pattern, host); err == nil && matched {
			return true
		}
	}
	return false
}

func matchesRoute(matches []RouteMatch, requestPath string) bool {
	if len(matches) == 0 {
		return true
	}
	for _, match := range matches {
		if match.FullPathMatch != "" && match.FullPathMatch == requestPath {
			return true
		}
		if match.PrefixMatch != "" && strings.HasPrefix(requestPath, match.PrefixMatch) {
			return true
		}
	}
	return false
}
