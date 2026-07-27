package networksecurity

import (
	"fmt"
	"net/netip"
	"path"
	"sort"
	"strings"
)

// AuthorizeHTTP is the bounded serving-path contract used by the Compute HTTP
// proxy. It evaluates only an exact project/location scope and reports whether
// any policy matched so callers can distinguish default allow from policy allow.
func (api *API) AuthorizeHTTP(
	project, location, sourceIP, host, method, requestPath string,
) (matched, allowed bool, policyName string, err error) {
	if !validScopePart(project) || !validScopePart(location) {
		return false, false, "", fmt.Errorf("invalid authorization policy scope")
	}
	request := EvaluationRequest{
		Project: project, Location: location, SourceIP: sourceIP,
		Host: host, Port: 80, Method: method, Path: requestPath,
	}
	prefix := "projects/" + project + "/locations/" + location + "/authorizationPolicies/"
	api.mu.RLock()
	policies := make([]*AuthorizationPolicy, 0)
	for name, policy := range api.policies {
		if strings.HasPrefix(name, prefix) {
			policies = append(policies, clonePolicy(policy))
		}
	}
	api.mu.RUnlock()
	sort.Slice(policies, func(i, j int) bool { return policies[i].Name < policies[j].Name })

	allowPolicy := ""
	for _, policy := range policies {
		if !policyMatches(policy, request) {
			continue
		}
		switch policy.Action {
		case "DENY":
			return true, false, policy.Name, nil
		case "ALLOW":
			if allowPolicy == "" {
				allowPolicy = policy.Name
			}
		default:
			return false, false, "", fmt.Errorf("authorization policy %q has unsupported action %q", policy.Name, policy.Action)
		}
	}
	if allowPolicy != "" {
		return true, true, allowPolicy, nil
	}
	return false, true, "", nil
}

// EvaluationRequest is the bounded metadata-policy decision input.
type EvaluationRequest struct {
	Project   string `json:"project"`
	Location  string `json:"location"`
	Principal string `json:"principal"`
	SourceIP  string `json:"sourceIp"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Method    string `json:"method"`
	Path      string `json:"path"`
}

// EvaluationDecision is advisory until a serving data plane explicitly calls it.
type EvaluationDecision struct {
	Allowed     bool   `json:"allowed"`
	Policy      string `json:"policy,omitempty"`
	Enforcement string `json:"enforcement"`
	Reason      string `json:"reason,omitempty"`
}

// Evaluate deterministically evaluates stored rules without claiming traffic
// interception. DENY matches take precedence over ALLOW matches.
func (api *API) Evaluate(request EvaluationRequest) EvaluationDecision {
	if !validScopePart(request.Project) || !validScopePart(request.Location) {
		return EvaluationDecision{Enforcement: "METADATA_ONLY", Reason: "invalid evaluation scope"}
	}
	prefix := "projects/" + request.Project + "/locations/" + request.Location + "/authorizationPolicies/"
	api.mu.RLock()
	policies := make([]*AuthorizationPolicy, 0)
	for name, policy := range api.policies {
		if strings.HasPrefix(name, prefix) {
			policies = append(policies, clonePolicy(policy))
		}
	}
	api.mu.RUnlock()
	sort.Slice(policies, func(i, j int) bool { return policies[i].Name < policies[j].Name })

	var allowPolicy string
	for _, policy := range policies {
		if !policyMatches(policy, request) {
			continue
		}
		if policy.Action == "DENY" {
			return EvaluationDecision{Allowed: false, Policy: policy.Name, Enforcement: "METADATA_ONLY"}
		}
		if policy.Action == "ALLOW" && allowPolicy == "" {
			allowPolicy = policy.Name
		}
	}
	return EvaluationDecision{Allowed: true, Policy: allowPolicy, Enforcement: "METADATA_ONLY"}
}

func validScopePart(value string) bool {
	trimmed := strings.TrimSpace(value)
	return value == trimmed && value != "" && value != "." && value != ".." && !strings.ContainsAny(value, `/\`)
}

func policyMatches(policy *AuthorizationPolicy, request EvaluationRequest) bool {
	for _, rule := range policy.Rules {
		sourceMatch := len(rule.Sources) == 0
		for _, source := range rule.Sources {
			if matchesSource(source, request) {
				sourceMatch = true
				break
			}
		}
		destinationMatch := len(rule.Destinations) == 0
		for _, destination := range rule.Destinations {
			if matchesDestination(destination, request) {
				destinationMatch = true
				break
			}
		}
		if sourceMatch && destinationMatch {
			return true
		}
	}
	return len(policy.Rules) == 0
}

func matchesSource(source Source, request EvaluationRequest) bool {
	principalMatch := len(source.Principals) == 0 || containsMatch(source.Principals, request.Principal)
	ipMatch := len(source.IpBlocks) == 0
	address, err := netip.ParseAddr(request.SourceIP)
	for _, rawPrefix := range source.IpBlocks {
		prefix, prefixErr := netip.ParsePrefix(rawPrefix)
		if err == nil && prefixErr == nil && prefix.Contains(address) {
			ipMatch = true
			break
		}
	}
	return principalMatch && ipMatch
}

func matchesDestination(destination Destination, request EvaluationRequest) bool {
	return (len(destination.Hosts) == 0 || containsMatch(destination.Hosts, request.Host)) &&
		(len(destination.Ports) == 0 || containsPort(destination.Ports, request.Port)) &&
		(len(destination.Methods) == 0 || containsMatch(destination.Methods, request.Method)) &&
		(len(destination.Paths) == 0 || containsMatch(destination.Paths, request.Path))
}

func containsMatch(patterns []string, value string) bool {
	for _, pattern := range patterns {
		if pattern == value {
			return true
		}
		if matched, err := path.Match(pattern, value); err == nil && matched {
			return true
		}
	}
	return false
}

func containsPort(ports []int, port int) bool {
	for _, candidate := range ports {
		if candidate == port {
			return true
		}
	}
	return false
}
