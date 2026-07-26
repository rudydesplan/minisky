package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strings"

	"minisky/pkg/config"
)

const (
	vpcNetworkService  = "compute-network"
	maxDockerErrorBody = 64 << 10
)

var (
	gcpProjectID   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`)
	gcpNetworkName = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)

	ErrVPCNetworkNotFound = errors.New("owned VPC network not found")
)

// VPCNetworkIdentity is the exact project-scoped identity of a custom VPC.
type VPCNetworkIdentity struct {
	Project string
	Network string
}

func (identity VPCNetworkIdentity) Validate() error {
	if !gcpProjectID.MatchString(identity.Project) {
		return fmt.Errorf("invalid Compute project identity")
	}
	if !gcpNetworkName.MatchString(identity.Network) {
		return fmt.Errorf("invalid Compute network identity")
	}
	return nil
}

func (identity VPCNetworkIdentity) CanonicalResource() string {
	return fmt.Sprintf("projects/%s/global/networks/%s", identity.Project, identity.Network)
}

// DockerName returns a deterministic profile- and project-scoped bridge name.
func (identity VPCNetworkIdentity) DockerName() (string, error) {
	if err := identity.Validate(); err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(config.GetProfile() + "\x00" + identity.CanonicalResource()))
	suffix := hex.EncodeToString(hash[:8])
	const prefix = "minisky-vpc-"
	maxReadable := 63 - len(prefix) - 1 - len(suffix)
	readable := identity.Network
	if len(readable) > maxReadable {
		readable = strings.TrimRight(readable[:maxReadable], "-")
	}
	return prefix + readable + "-" + suffix, nil
}

func (identity VPCNetworkIdentity) labels() (map[string]string, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		"managed-by":                 "minisky",
		"minisky.profile":            config.GetProfile(),
		"minisky.service":            vpcNetworkService,
		"minisky.project":            identity.Project,
		"minisky.network":            identity.Network,
		"minisky.canonical-resource": identity.CanonicalResource(),
	}, nil
}

// VPCNetworkIPAMState is the inspectable result of an exact bridge ensure.
type VPCNetworkIPAMState struct {
	Name    string
	ID      string
	CIDR    string
	Created bool
}

type dockerNetworkInspect struct {
	Name   string            `json:"Name"`
	ID     string            `json:"Id"`
	Driver string            `json:"Driver"`
	Labels map[string]string `json:"Labels"`
	IPAM   struct {
		Driver string `json:"Driver"`
		Config []struct {
			Subnet string `json:"Subnet"`
		} `json:"Config"`
	} `json:"IPAM"`
}

// EnsureVPCNetworkIPAM creates or verifies the exact owned bridge for a custom
// VPC. Docker remains the final authority for host-wide IPAM overlap.
func (sm *ServiceManager) EnsureVPCNetworkIPAM(
	ctx context.Context,
	identity VPCNetworkIdentity,
	cidr string,
) (VPCNetworkIPAMState, error) {
	name, err := identity.DockerName()
	if err != nil {
		return VPCNetworkIPAMState{}, err
	}
	prefix, err := NormalizeVPCIPv4Prefix(cidr)
	if err != nil {
		return VPCNetworkIPAMState{}, err
	}
	labels, _ := identity.labels()

	existing, found, err := sm.inspectVPCNetwork(ctx, name)
	if err != nil {
		return VPCNetworkIPAMState{}, err
	}
	if found {
		if !exactLabels(existing.Labels, labels) {
			return VPCNetworkIPAMState{}, fmt.Errorf("VPC bridge %q is not exactly owned by %s", name, identity.CanonicalResource())
		}
		existingCIDR, cidrErr := exactNetworkCIDR(existing)
		if cidrErr != nil || existingCIDR != prefix.String() || existing.Driver != "bridge" {
			return VPCNetworkIPAMState{}, fmt.Errorf(
				"owned VPC bridge %q CIDR or driver does not match requested CIDR %s",
				name,
				prefix,
			)
		}
		return VPCNetworkIPAMState{Name: name, ID: existing.ID, CIDR: existingCIDR}, nil
	}

	if err := sm.rejectOwnedVPCOverlap(ctx, identity, prefix); err != nil {
		return VPCNetworkIPAMState{}, err
	}
	payload := map[string]any{
		"Name":   name,
		"Driver": "bridge",
		"Labels": labels,
		"IPAM": map[string]any{
			"Driver": "default",
			"Config": []map[string]string{{"Subnet": prefix.String()}},
		},
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return VPCNetworkIPAMState{}, err
	}
	response, err := sm.doVPCDockerRequest(ctx, http.MethodPost, "/networks/create", bytes.NewReader(encoded))
	if err != nil {
		if reconciled, reconcileErr := sm.reconcileAmbiguousVPCNetworkCreate(ctx, name, labels, prefix); reconcileErr == nil {
			return reconciled, nil
		}
		return VPCNetworkIPAMState{}, fmt.Errorf("create VPC bridge: %w", err)
	}
	defer drainAndClose(response.Body)
	if response.StatusCode != http.StatusCreated {
		createErr := dockerStatusError("create VPC bridge", response)
		if response.StatusCode == http.StatusConflict {
			raced, racedFound, inspectErr := sm.inspectVPCNetwork(ctx, name)
			if inspectErr != nil {
				return VPCNetworkIPAMState{}, fmt.Errorf("%w; inspect create conflict: %v", createErr, inspectErr)
			}
			if racedFound && exactLabels(raced.Labels, labels) {
				racedCIDR, cidrErr := exactNetworkCIDR(raced)
				if cidrErr == nil && racedCIDR == prefix.String() && raced.Driver == "bridge" {
					return VPCNetworkIPAMState{Name: name, ID: raced.ID, CIDR: racedCIDR}, nil
				}
			}
		}
		return VPCNetworkIPAMState{}, createErr
	}
	var created struct {
		ID string `json:"Id"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&created)
	if decodeErr != nil || created.ID == "" {
		if reconciled, reconcileErr := sm.reconcileAmbiguousVPCNetworkCreate(ctx, name, labels, prefix); reconcileErr == nil {
			return reconciled, nil
		}
		if decodeErr == nil || errors.Is(decodeErr, io.EOF) {
			decodeErr = errors.New("successful Docker create response omitted network ID")
		}
		return VPCNetworkIPAMState{}, fmt.Errorf("decode created VPC bridge: %w", decodeErr)
	}
	return VPCNetworkIPAMState{Name: name, ID: created.ID, CIDR: prefix.String(), Created: true}, nil
}

func (sm *ServiceManager) reconcileAmbiguousVPCNetworkCreate(
	ctx context.Context,
	name string,
	labels map[string]string,
	prefix netip.Prefix,
) (VPCNetworkIPAMState, error) {
	reconcileCtx := context.WithoutCancel(ctx)
	network, found, err := sm.inspectVPCNetwork(reconcileCtx, name)
	if err != nil {
		return VPCNetworkIPAMState{}, err
	}
	if !found || !exactLabels(network.Labels, labels) || network.Driver != "bridge" {
		return VPCNetworkIPAMState{}, errors.New("ambiguous VPC create did not produce the exact owned bridge")
	}
	cidr, err := exactNetworkCIDR(network)
	if err != nil || cidr != prefix.String() || network.ID == "" {
		return VPCNetworkIPAMState{}, errors.New("ambiguous VPC create produced mismatched IPAM")
	}
	return VPCNetworkIPAMState{Name: name, ID: network.ID, CIDR: cidr}, nil
}

// DeleteVPCNetworkIPAM deletes only the exact owned bridge with the expected
// CIDR. It never disconnects attached endpoints.
func (sm *ServiceManager) DeleteVPCNetworkIPAM(
	ctx context.Context,
	identity VPCNetworkIdentity,
	expectedCIDR string,
) error {
	name, err := identity.DockerName()
	if err != nil {
		return err
	}
	prefix, err := NormalizeVPCIPv4Prefix(expectedCIDR)
	if err != nil {
		return err
	}
	labels, _ := identity.labels()
	existing, found, err := sm.inspectVPCNetwork(ctx, name)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: %s", ErrVPCNetworkNotFound, identity.CanonicalResource())
	}
	if !exactLabels(existing.Labels, labels) {
		return fmt.Errorf("refusing to delete VPC bridge %q that is not exactly owned by %s", name, identity.CanonicalResource())
	}
	existingCIDR, cidrErr := exactNetworkCIDR(existing)
	if cidrErr != nil || existingCIDR != prefix.String() || existing.Driver != "bridge" {
		return fmt.Errorf("refusing to delete VPC bridge %q with unexpected CIDR or driver", name)
	}
	if existing.ID == "" {
		return fmt.Errorf("refusing to delete VPC bridge %q without an immutable Docker ID", name)
	}

	response, err := sm.doVPCDockerRequest(ctx, http.MethodDelete, "/networks/"+url.PathEscape(existing.ID), nil)
	if err != nil {
		return fmt.Errorf("delete VPC bridge: %w", err)
	}
	defer drainAndClose(response.Body)
	if response.StatusCode != http.StatusNoContent {
		return dockerStatusError("delete VPC bridge", response)
	}
	return nil
}

// DeleteLegacyVPCNetwork removes only a pre-project-scoped bridge carrying the
// exact legacy labels for the current profile. It never adopts that bridge.
func (sm *ServiceManager) DeleteLegacyVPCNetwork(ctx context.Context, network string) error {
	if !gcpNetworkName.MatchString(network) {
		return fmt.Errorf("invalid legacy Compute network name")
	}
	name := "minisky-vpc-" + network
	expected := map[string]string{
		"managed-by":       "minisky",
		"minisky.profile":  config.GetProfile(),
		"minisky.service":  vpcNetworkService,
		"minisky.resource": network,
	}
	existing, found, err := sm.inspectVPCNetwork(ctx, name)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if !exactLabels(existing.Labels, expected) || existing.Driver != "bridge" || existing.ID == "" {
		return fmt.Errorf("refusing to delete legacy VPC bridge %q without exact legacy ownership", name)
	}
	response, err := sm.doVPCDockerRequest(
		ctx,
		http.MethodDelete,
		"/networks/"+url.PathEscape(existing.ID),
		nil,
	)
	if err != nil {
		return fmt.Errorf("delete legacy VPC bridge: %w", err)
	}
	defer drainAndClose(response.Body)
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusNotFound {
		return dockerStatusError("delete legacy VPC bridge", response)
	}
	return nil
}

func (sm *ServiceManager) inspectVPCNetwork(
	ctx context.Context,
	name string,
) (dockerNetworkInspect, bool, error) {
	response, err := sm.doVPCDockerRequest(ctx, http.MethodGet, "/networks/"+url.PathEscape(name), nil)
	if err != nil {
		return dockerNetworkInspect{}, false, fmt.Errorf("inspect VPC bridge: %w", err)
	}
	defer drainAndClose(response.Body)
	if response.StatusCode == http.StatusNotFound {
		return dockerNetworkInspect{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return dockerNetworkInspect{}, false, dockerStatusError("inspect VPC bridge", response)
	}
	var network dockerNetworkInspect
	if err := json.NewDecoder(response.Body).Decode(&network); err != nil {
		return dockerNetworkInspect{}, false, fmt.Errorf("decode VPC bridge inspection: %w", err)
	}
	return network, true, nil
}

func (sm *ServiceManager) rejectOwnedVPCOverlap(
	ctx context.Context,
	identity VPCNetworkIdentity,
	requested netip.Prefix,
) error {
	filters, _ := json.Marshal(map[string][]string{
		"label": {
			"managed-by=minisky",
			"minisky.profile=" + config.GetProfile(),
			"minisky.service=" + vpcNetworkService,
		},
	})
	path := "/networks?" + url.Values{"filters": {string(filters)}}.Encode()
	response, err := sm.doVPCDockerRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("list owned VPC bridges: %w", err)
	}
	defer drainAndClose(response.Body)
	if response.StatusCode != http.StatusOK {
		return dockerStatusError("list owned VPC bridges", response)
	}
	var networks []dockerNetworkInspect
	if err := json.NewDecoder(response.Body).Decode(&networks); err != nil {
		return fmt.Errorf("decode owned VPC bridges: %w", err)
	}
	for _, network := range networks {
		if network.Labels["managed-by"] != "minisky" ||
			network.Labels["minisky.profile"] != config.GetProfile() ||
			network.Labels["minisky.service"] != vpcNetworkService ||
			network.Labels["minisky.canonical-resource"] == identity.CanonicalResource() {
			continue
		}
		existingCIDR, err := exactNetworkCIDR(network)
		if err != nil {
			return fmt.Errorf("owned VPC bridge %q has invalid IPAM: %w", network.Name, err)
		}
		existing, _ := netip.ParsePrefix(existingCIDR)
		if requested.Overlaps(existing) {
			return fmt.Errorf(
				"requested CIDR %s overlaps owned VPC bridge %q CIDR %s",
				requested,
				network.Name,
				existing,
			)
		}
	}
	return nil
}

func (sm *ServiceManager) doVPCDockerRequest(
	ctx context.Context,
	method string,
	path string,
	body io.Reader,
) (*http.Response, error) {
	timeout := sm.dockerTimeout
	if timeout <= 0 {
		timeout = dockerRequestTimeout
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	request, err := http.NewRequestWithContext(requestCtx, method, "http://localhost"+path, body)
	if err != nil {
		cancel()
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if sm.dockerClient == nil {
		cancel()
		return nil, errors.New("Docker client is unavailable")
	}
	response, err := sm.dockerClient.Do(request)
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &cancelOnCloseBody{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (body *cancelOnCloseBody) Close() error {
	err := body.ReadCloser.Close()
	body.cancel()
	return err
}

func exactNetworkCIDR(network dockerNetworkInspect) (string, error) {
	if len(network.IPAM.Config) != 1 {
		return "", fmt.Errorf("expected exactly one IPAM subnet")
	}
	prefix, err := NormalizeVPCIPv4Prefix(network.IPAM.Config[0].Subnet)
	if err != nil {
		return "", err
	}
	return prefix.String(), nil
}

func exactLabels(actual, expected map[string]string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func dockerStatusError(operation string, response *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDockerErrorBody))
	if err != nil {
		return fmt.Errorf("%s returned HTTP %d with unreadable body: %w", operation, response.StatusCode, err)
	}
	var payload struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil && payload.Message != "" {
		return fmt.Errorf("%s returned HTTP %d: %s", operation, response.StatusCode, payload.Message)
	}
	return fmt.Errorf("%s returned HTTP %d: %s", operation, response.StatusCode, strings.TrimSpace(string(body)))
}

func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}
