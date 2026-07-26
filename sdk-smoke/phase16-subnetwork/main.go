package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"minisky/pkg/orchestrator"
)

const (
	networkName    = "phase16-subnetwork-network"
	subnetworkName = "phase16-subnetwork"
	defaultRegion  = "us-central1"
)

var (
	projectPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$`)
	regionPattern  = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
	pollInterval   = 100 * time.Millisecond
)

type inputs struct {
	mode         string
	endpoint     string
	project      string
	region       string
	cidr         string
	baselinePath string
}

type baseline struct {
	Network    networkBaseline    `json:"network"`
	Subnetwork subnetworkBaseline `json:"subnetwork"`
}

type networkBaseline struct {
	Kind                  string `json:"kind"`
	ID                    uint64 `json:"id"`
	Name                  string `json:"name"`
	SelfLink              string `json:"selfLink"`
	AutoCreateSubnetworks bool   `json:"autoCreateSubnetworks"`
	CreationTimestamp     string `json:"creationTimestamp"`
}

type subnetworkBaseline struct {
	Kind                  string `json:"kind"`
	ID                    uint64 `json:"id"`
	Name                  string `json:"name"`
	IPCidrRange           string `json:"ipCidrRange"`
	Network               string `json:"network"`
	Region                string `json:"region"`
	SelfLink              string `json:"selfLink"`
	GatewayAddress        string `json:"gatewayAddress"`
	Fingerprint           string `json:"fingerprint"`
	PrivateIPGoogleAccess bool   `json:"privateIpGoogleAccess"`
	Purpose               string `json:"purpose"`
	StackType             string `json:"stackType"`
	State                 string `json:"state"`
	CreationTimestamp     string `json:"creationTimestamp"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "phase 16 subnetwork/IPAM smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadInputs()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	service, err := computeService(ctx, cfg.endpoint)
	if err != nil {
		return fmt.Errorf("create Compute client: %w", err)
	}
	switch cfg.mode {
	case "seed":
		return seed(ctx, service, cfg)
	case "verify":
		return verify(ctx, service, cfg)
	case "cleanup":
		return cleanup(ctx, service, cfg)
	case "verify-cleanup":
		return verifyCleanup(ctx, service, cfg)
	default:
		return errors.New("MINISKY_PHASE16_SUBNETWORK_MODE must be seed, verify, cleanup, or verify-cleanup")
	}
}

func loadInputs() (inputs, error) {
	cfg := inputs{
		mode:         strings.TrimSpace(os.Getenv("MINISKY_PHASE16_SUBNETWORK_MODE")),
		endpoint:     strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project:      strings.TrimSpace(os.Getenv("MINISKY_PROJECT_ID")),
		region:       env("MINISKY_REGION", defaultRegion),
		cidr:         strings.TrimSpace(os.Getenv("MINISKY_SUBNETWORK_CIDR")),
		baselinePath: strings.TrimSpace(os.Getenv("MINISKY_PHASE16_SUBNETWORK_BASELINE")),
	}
	if err := validateInputs(cfg); err != nil {
		return inputs{}, err
	}
	return cfg, nil
}

func validateInputs(cfg inputs) error {
	parsed, err := url.Parse(cfg.endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.User != nil || parsed.Path != "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("MINISKY_ENDPOINT must be an absolute loopback HTTP(S) origin")
	}
	host := parsed.Hostname()
	address, addressErr := netip.ParseAddr(host)
	if !strings.EqualFold(host, "localhost") && (addressErr != nil || !address.IsLoopback()) {
		return errors.New("MINISKY_ENDPOINT must use a loopback host")
	}
	if parsed.Port() != "" {
		port, portErr := strconv.Atoi(parsed.Port())
		if portErr != nil || port < 1 || port > 65535 {
			return errors.New("MINISKY_ENDPOINT has an invalid port")
		}
	}
	if !projectPattern.MatchString(cfg.project) {
		return errors.New("MINISKY_PROJECT_ID must be a valid lowercase project ID")
	}
	if !regionPattern.MatchString(cfg.region) {
		return errors.New("MINISKY_REGION must be a valid lowercase region name")
	}
	if _, err := orchestrator.NormalizeVPCIPv4Prefix(cfg.cidr); err != nil {
		return errors.New("MINISKY_SUBNETWORK_CIDR must be a canonical usable IPv4 /8 through /29")
	}
	if cfg.baselinePath == "" || !filepath.IsAbs(cfg.baselinePath) {
		return errors.New("MINISKY_PHASE16_SUBNETWORK_BASELINE must be an absolute file path")
	}
	return nil
}

func computeService(ctx context.Context, endpoint string) (*compute.Service, error) {
	return compute.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(strings.TrimRight(endpoint, "/")+"/_minisky/compute/compute/v1/"),
	)
}

func seed(ctx context.Context, service *compute.Service, cfg inputs) error {
	networkRequest := &compute.Network{
		Name:                  networkName,
		AutoCreateSubnetworks: false,
		ForceSendFields:       []string{"AutoCreateSubnetworks"},
	}
	operation, err := service.Networks.Insert(cfg.project, networkRequest).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("insert custom network: %w", err)
	}
	if err := waitGlobalOperation(ctx, service, cfg.project, operation); err != nil {
		return fmt.Errorf("wait for network insert: %w", err)
	}
	network, err := service.Networks.Get(cfg.project, networkName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get seeded network: %w", err)
	}
	networkSnapshot, err := snapshotNetwork(network, cfg.project)
	if err != nil {
		return err
	}

	subnetworkRequest := &compute.Subnetwork{
		Name:        subnetworkName,
		IpCidrRange: cfg.cidr,
		Network:     expectedNetworkSelfLink(cfg.project),
	}
	operation, err = service.Subnetworks.Insert(cfg.project, cfg.region, subnetworkRequest).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("insert subnetwork: %w", err)
	}
	if err := waitRegionalOperation(ctx, service, cfg.project, cfg.region, operation); err != nil {
		return fmt.Errorf("wait for subnetwork insert: %w", err)
	}
	subnetwork, err := service.Subnetworks.Get(cfg.project, cfg.region, subnetworkName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get seeded subnetwork: %w", err)
	}
	subnetworkSnapshot, err := snapshotSubnetwork(subnetwork, cfg)
	if err != nil {
		return err
	}
	if err := validateSubnetworkList(ctx, service, cfg, subnetworkSnapshot); err != nil {
		return err
	}
	if err := writeBaseline(cfg.baselinePath, baseline{
		Network: networkSnapshot, Subnetwork: subnetworkSnapshot,
	}); err != nil {
		return err
	}
	fmt.Printf("seeded custom network=%s subnetwork=%s cidr=%s\n", networkName, subnetworkName, cfg.cidr)
	return nil
}

func verify(ctx context.Context, service *compute.Service, cfg inputs) error {
	expected, err := readBaseline(cfg.baselinePath)
	if err != nil {
		return err
	}
	network, err := service.Networks.Get(cfg.project, networkName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get persisted network: %w", err)
	}
	actualNetwork, err := snapshotNetwork(network, cfg.project)
	if err != nil {
		return err
	}
	if actualNetwork != expected.Network {
		return fmt.Errorf("persisted network changed: got=%+v want=%+v", actualNetwork, expected.Network)
	}
	subnetwork, err := service.Subnetworks.Get(cfg.project, cfg.region, subnetworkName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get persisted subnetwork: %w", err)
	}
	actualSubnetwork, err := snapshotSubnetwork(subnetwork, cfg)
	if err != nil {
		return err
	}
	if actualSubnetwork != expected.Subnetwork {
		return fmt.Errorf("persisted subnetwork changed: got=%+v want=%+v", actualSubnetwork, expected.Subnetwork)
	}
	if err := validateSubnetworkList(ctx, service, cfg, expected.Subnetwork); err != nil {
		return err
	}
	fmt.Printf("verified persisted network=%s subnetwork=%s\n", networkName, subnetworkName)
	return nil
}

func cleanup(ctx context.Context, service *compute.Service, cfg inputs) error {
	operation, err := service.Subnetworks.Delete(cfg.project, cfg.region, subnetworkName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete subnetwork: %w", err)
	}
	if err := waitRegionalOperation(ctx, service, cfg.project, cfg.region, operation); err != nil {
		return fmt.Errorf("wait for subnetwork delete: %w", err)
	}
	if err := requireSubnetworkMissing(ctx, service, cfg); err != nil {
		return err
	}
	operation, err = service.Networks.Delete(cfg.project, networkName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete network: %w", err)
	}
	if err := waitGlobalOperation(ctx, service, cfg.project, operation); err != nil {
		return fmt.Errorf("wait for network delete: %w", err)
	}
	if err := requireNetworkMissing(ctx, service, cfg.project); err != nil {
		return err
	}
	fmt.Printf("deleted subnetwork=%s network=%s and confirmed generated GET 404\n", subnetworkName, networkName)
	return nil
}

func verifyCleanup(ctx context.Context, service *compute.Service, cfg inputs) error {
	if err := requireSubnetworkMissing(ctx, service, cfg); err != nil {
		return err
	}
	if err := requireNetworkMissing(ctx, service, cfg.project); err != nil {
		return err
	}
	subnetworks, err := service.Subnetworks.List(cfg.project, cfg.region).MaxResults(1).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list subnetworks after cleanup: %w", err)
	}
	if len(subnetworks.Items) != 0 || subnetworks.NextPageToken != "" {
		return fmt.Errorf("subnetwork list after cleanup is not empty: items=%d token=%q",
			len(subnetworks.Items), subnetworks.NextPageToken)
	}
	networks, err := service.Networks.List(cfg.project).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list networks after cleanup: %w", err)
	}
	for _, network := range networks.Items {
		if network != nil && network.Name == networkName {
			return errors.New("custom network remains in list after cleanup")
		}
	}
	fmt.Printf("verified cleanup remained durable for network=%s subnetwork=%s\n", networkName, subnetworkName)
	return nil
}

func waitGlobalOperation(
	ctx context.Context,
	service *compute.Service,
	project string,
	operation *compute.Operation,
) error {
	if operation == nil || operation.Name == "" {
		return errors.New("global operation response omitted name")
	}
	for {
		current, err := service.GlobalOperations.Get(project, operation.Name).Context(ctx).Do()
		if err != nil {
			return err
		}
		if current.Status == "DONE" {
			return operationResult(current)
		}
		if err := waitPoll(ctx); err != nil {
			return err
		}
	}
}

func waitRegionalOperation(
	ctx context.Context,
	service *compute.Service,
	project, region string,
	operation *compute.Operation,
) error {
	if operation == nil || operation.Name == "" {
		return errors.New("regional operation response omitted name")
	}
	for {
		current, err := service.RegionOperations.Get(project, region, operation.Name).Context(ctx).Do()
		if err != nil {
			return err
		}
		if current.Status == "DONE" {
			return operationResult(current)
		}
		if err := waitPoll(ctx); err != nil {
			return err
		}
	}
}

func waitPoll(ctx context.Context) error {
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func operationResult(operation *compute.Operation) error {
	if operation.Error == nil || len(operation.Error.Errors) == 0 {
		return nil
	}
	messages := make([]string, 0, len(operation.Error.Errors))
	for _, item := range operation.Error.Errors {
		if item == nil {
			continue
		}
		messages = append(messages, strings.TrimSpace(item.Code+": "+item.Message))
	}
	if len(messages) == 0 {
		return errors.New("operation reported an unspecified error")
	}
	return fmt.Errorf("operation %s failed: %s", operation.Name, strings.Join(messages, "; "))
}

func snapshotNetwork(network *compute.Network, project string) (networkBaseline, error) {
	expectedLink := expectedNetworkSelfLink(project)
	if network == nil || network.Kind != "compute#network" || network.Name != networkName ||
		network.SelfLink != expectedLink || network.AutoCreateSubnetworks ||
		network.Id == 0 || !validTimestamp(network.CreationTimestamp) {
		return networkBaseline{}, fmt.Errorf("unexpected custom network: %#v", network)
	}
	return networkBaseline{
		Kind: network.Kind, ID: network.Id, Name: network.Name, SelfLink: network.SelfLink,
		AutoCreateSubnetworks: network.AutoCreateSubnetworks,
		CreationTimestamp:     network.CreationTimestamp,
	}, nil
}

func snapshotSubnetwork(subnetwork *compute.Subnetwork, cfg inputs) (subnetworkBaseline, error) {
	prefix, _ := netip.ParsePrefix(cfg.cidr)
	expectedRegion := fmt.Sprintf(
		"https://www.googleapis.com/compute/v1/projects/%s/regions/%s", cfg.project, cfg.region)
	expectedSelfLink := expectedRegion + "/subnetworks/" + subnetworkName
	if subnetwork == nil || subnetwork.Kind != "compute#subnetwork" ||
		subnetwork.Id == 0 || subnetwork.Name != subnetworkName ||
		subnetwork.IpCidrRange != cfg.cidr || subnetwork.Network != expectedNetworkSelfLink(cfg.project) ||
		subnetwork.Region != expectedRegion || subnetwork.SelfLink != expectedSelfLink ||
		subnetwork.GatewayAddress != prefix.Addr().Next().String() ||
		subnetwork.Fingerprint == "" || subnetwork.PrivateIpGoogleAccess ||
		subnetwork.Purpose != "PRIVATE" || subnetwork.StackType != "IPV4_ONLY" ||
		subnetwork.State != "READY" || !validTimestamp(subnetwork.CreationTimestamp) {
		return subnetworkBaseline{}, fmt.Errorf("unexpected subnetwork: %#v", subnetwork)
	}
	return subnetworkBaseline{
		Kind: subnetwork.Kind, ID: subnetwork.Id, Name: subnetwork.Name,
		IPCidrRange: subnetwork.IpCidrRange, Network: subnetwork.Network,
		Region: subnetwork.Region, SelfLink: subnetwork.SelfLink,
		GatewayAddress: subnetwork.GatewayAddress, Fingerprint: subnetwork.Fingerprint,
		PrivateIPGoogleAccess: subnetwork.PrivateIpGoogleAccess,
		Purpose:               subnetwork.Purpose, StackType: subnetwork.StackType, State: subnetwork.State,
		CreationTimestamp: subnetwork.CreationTimestamp,
	}, nil
}

func validateSubnetworkList(
	ctx context.Context,
	service *compute.Service,
	cfg inputs,
	expected subnetworkBaseline,
) error {
	response, err := service.Subnetworks.List(cfg.project, cfg.region).MaxResults(1).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list subnetworks: %w", err)
	}
	if len(response.Items) != 1 || response.NextPageToken != "" {
		return fmt.Errorf("subnetwork list items=%d token=%q want one terminal page",
			len(response.Items), response.NextPageToken)
	}
	actual, err := snapshotSubnetwork(response.Items[0], cfg)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("listed subnetwork changed: got=%+v want=%+v", actual, expected)
	}
	return nil
}

func requireNetworkMissing(ctx context.Context, service *compute.Service, project string) error {
	_, err := service.Networks.Get(project, networkName).Context(ctx).Do()
	return requireNotFound(err, "network")
}

func requireSubnetworkMissing(ctx context.Context, service *compute.Service, cfg inputs) error {
	_, err := service.Subnetworks.Get(cfg.project, cfg.region, subnetworkName).Context(ctx).Do()
	return requireNotFound(err, "subnetwork")
}

func requireNotFound(err error, resource string) error {
	if err == nil {
		return fmt.Errorf("%s still exists", resource)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != 404 {
		return fmt.Errorf("get missing %s: %w", resource, err)
	}
	return nil
}

func writeBaseline(path string, value baseline) (returnErr error) {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode baseline: %w", err)
	}
	directory := filepath.Dir(path)
	temp, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create baseline temporary file: %w", err)
	}
	tempPath := temp.Name()
	defer func() {
		_ = temp.Close()
		if returnErr != nil {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("secure baseline temporary file: %w", err)
	}
	if _, err := temp.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write baseline: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync baseline: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close baseline: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace baseline atomically: %w", err)
	}
	return os.Chmod(path, 0o600)
}

func readBaseline(path string) (baseline, error) {
	info, err := os.Stat(path)
	if err != nil {
		return baseline{}, fmt.Errorf("stat baseline: %w", err)
	}
	if info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		return baseline{}, errors.New("baseline must be a regular file with mode 0600")
	}
	file, err := os.Open(path)
	if err != nil {
		return baseline{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	decoder.DisallowUnknownFields()
	var value baseline
	if err := decoder.Decode(&value); err != nil {
		return baseline{}, fmt.Errorf("decode baseline: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return baseline{}, errors.New("baseline must contain exactly one JSON object")
	}
	return value, nil
}

func expectedNetworkSelfLink(project string) string {
	return fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/%s",
		project, networkName)
}

func validTimestamp(value string) bool {
	_, err := time.Parse(time.RFC3339, value)
	return err == nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
