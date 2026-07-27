package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	accesscontextmanager "google.golang.org/api/accesscontextmanager/v1"
	binaryauthorization "google.golang.org/api/binaryauthorization/v1"
	cloudasset "google.golang.org/api/cloudasset/v1"
	compute "google.golang.org/api/compute/v1"
	dlp "google.golang.org/api/dlp/v2"
	"google.golang.org/api/googleapi"
	networksecurity "google.golang.org/api/networksecurity/v1"
	networkservices "google.golang.org/api/networkservices/v1"
	"google.golang.org/api/option"
	orgpolicy "google.golang.org/api/orgpolicy/v2"
	privateca "google.golang.org/api/privateca/v1"
	storage "google.golang.org/api/storage/v1"
)

const (
	optInEnv         = "MINISKY_PHASE24_25_EXPERIMENTAL_OPT_IN"
	evidenceVersion  = 2
	maxEvidenceBytes = 32 << 10
	proxyLocation    = "global"
	defaultBackendID = "phase25-default"
	routedBackendID  = "phase25-routed"
	urlMapID         = "phase25-routes"
	targetProxyID    = "phase25-proxy"
	forwardingRuleID = "phase25-frontend"
	httpRouteID      = "phase25-route"
	proxyHost        = "localhost"
	denyHost         = "localhost"
)

var resourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

type config struct {
	mode          string
	endpoint      string
	project       string
	location      string
	certificateID string
	templateID    string
	perimeterID   string
	networkID     string
	meshID        string
	proxyPolicyID string
	proxyMeshID   string
	defaultOrigin string
	routedOrigin  string
	evidencePath  string
}

type evidence struct {
	Version                  int    `json:"version"`
	Project                  string `json:"project"`
	Location                 string `json:"location"`
	CertificateName          string `json:"certificateName"`
	DLPTemplateName          string `json:"dlpTemplateName"`
	OrgPolicyName            string `json:"orgPolicyName"`
	AccessPolicyName         string `json:"accessPolicyName"`
	ServicePerimeterName     string `json:"servicePerimeterName"`
	BinaryPolicyName         string `json:"binaryPolicyName"`
	NetworkPolicyName        string `json:"networkPolicyName"`
	MeshName                 string `json:"meshName"`
	ProxyNetworkPolicyName   string `json:"proxyNetworkPolicyName"`
	ProxyMeshName            string `json:"proxyMeshName"`
	HTTPRouteName            string `json:"httpRouteName"`
	DefaultBackendName       string `json:"defaultBackendName"`
	RoutedBackendName        string `json:"routedBackendName"`
	DLPFindingCount          int    `json:"dlpFindingCount"`
	CloudAssetResultCount    int    `json:"cloudAssetResultCount"`
	CloudAssetSearchVerified bool   `json:"cloudAssetSearchVerified"`
	PerimeterGatewayDenied   bool   `json:"perimeterGatewayDenied"`
	ProxyDenyNoBackendCall   bool   `json:"proxyDenyNoBackendCall"`
	MeshRouteSelectedBackend bool   `json:"meshRouteSelectedBackend"`
	MeshRouteRestartVerified bool   `json:"meshRouteRestartVerified"`
	DefaultBackendHits       int    `json:"defaultBackendHits"`
	RoutedBackendHits        int    `json:"routedBackendHits"`
	ComputeBackendSetup      string `json:"computeBackendSetup"`
	ProxyRequestKind         string `json:"proxyRequestKind"`
	PrivateCADeleteSupport   string `json:"privateCaDeleteSupport"`
	OrgPolicyEvaluateSupport string `json:"orgPolicyEvaluateSupport"`
	BinaryEvaluateSupport    string `json:"binaryEvaluateSupport"`
	NetworkEvaluateSupport   string `json:"networkEvaluateSupport"`
	CloudAssetExportSupport  string `json:"cloudAssetExportSupport"`
}

type generatedClients struct {
	privateCA *privateca.Service
	dlp       *dlp.Service
	org       *orgpolicy.Service
	access    *accesscontextmanager.Service
	asset     *cloudasset.Service
	binary    *binaryauthorization.Service
	network   *networksecurity.Service
	services  *networkservices.Service
	storage   *storage.Service
	compute   *compute.Service
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Phase 24-25 generated Go client smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	clients, err := newGeneratedClients(ctx, cfg.endpoint)
	if err != nil {
		return err
	}
	switch cfg.mode {
	case "gate":
		return proveDefaultGate(ctx, clients, cfg)
	case "create":
		if os.Getenv(optInEnv) != "1" {
			return fmt.Errorf("create mode requires explicit %s=1", optInEnv)
		}
		return createAndRecord(ctx, clients, cfg)
	case "verify":
		if os.Getenv(optInEnv) != "1" {
			return fmt.Errorf("verify mode requires explicit %s=1", optInEnv)
		}
		return verifyRestart(ctx, clients, cfg)
	case "delete":
		if os.Getenv(optInEnv) != "1" {
			return fmt.Errorf("delete mode requires explicit %s=1", optInEnv)
		}
		return deleteAndVerify(ctx, clients, cfg)
	default:
		return fmt.Errorf("unsupported MINISKY_PHASE24_25_MODE %q", cfg.mode)
	}
}

func configFromEnv() (config, error) {
	cfg := config{
		mode:          env("MINISKY_PHASE24_25_MODE", "gate"),
		endpoint:      strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project:       env("MINISKY_PROJECT_ID", "phase24-25-project"),
		location:      env("MINISKY_PHASE24_25_LOCATION", "us-central1"),
		certificateID: env("MINISKY_PHASE24_25_CERTIFICATE_ID", "phase24-certificate"),
		templateID:    env("MINISKY_PHASE24_25_TEMPLATE_ID", "phase24-template"),
		perimeterID:   env("MINISKY_PHASE24_25_PERIMETER_ID", "phase24-perimeter"),
		networkID:     env("MINISKY_PHASE24_25_NETWORK_POLICY_ID", "phase25-deny"),
		meshID:        env("MINISKY_PHASE24_25_MESH_ID", "phase25-mesh"),
		proxyPolicyID: env("MINISKY_PHASE24_25_PROXY_POLICY_ID", "phase25-proxy-deny"),
		proxyMeshID:   env("MINISKY_PHASE24_25_PROXY_MESH_ID", "phase25-proxy-mesh"),
		defaultOrigin: strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_PHASE24_25_DEFAULT_BACKEND")), "/"),
		routedOrigin:  strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_PHASE24_25_ROUTED_BACKEND")), "/"),
		evidencePath:  strings.TrimSpace(os.Getenv("MINISKY_PHASE24_25_EVIDENCE")),
	}
	if err := validateLoopbackEndpoint(cfg.endpoint); err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{
		"project": cfg.project, "location": cfg.location, "certificate ID": cfg.certificateID,
		"template ID": cfg.templateID, "perimeter ID": cfg.perimeterID,
		"network policy ID": cfg.networkID, "mesh ID": cfg.meshID,
		"proxy policy ID": cfg.proxyPolicyID, "proxy mesh ID": cfg.proxyMeshID,
	} {
		if !resourceIDPattern.MatchString(value) {
			return config{}, fmt.Errorf("%s %q must match %s", name, value, resourceIDPattern)
		}
	}
	if cfg.evidencePath == "" || !filepath.IsAbs(cfg.evidencePath) {
		return config{}, errors.New("MINISKY_PHASE24_25_EVIDENCE must be an absolute path")
	}
	if cfg.mode != "gate" {
		if err := validateLoopbackEndpoint(cfg.defaultOrigin); err != nil {
			return config{}, fmt.Errorf("MINISKY_PHASE24_25_DEFAULT_BACKEND: %w", err)
		}
		if err := validateLoopbackEndpoint(cfg.routedOrigin); err != nil {
			return config{}, fmt.Errorf("MINISKY_PHASE24_25_ROUTED_BACKEND: %w", err)
		}
		if cfg.defaultOrigin == cfg.routedOrigin {
			return config{}, errors.New("default and routed backend origins must differ")
		}
	}
	return cfg, nil
}

func validateLoopbackEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse MINISKY_ENDPOINT: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("MINISKY_ENDPOINT must be an HTTP loopback origin without path, query, fragment, or userinfo")
	}
	host := parsed.Hostname()
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("MINISKY_ENDPOINT must target localhost or a loopback IP")
		}
	}
	if parsed.Port() == "" {
		return errors.New("MINISKY_ENDPOINT must include an explicit port")
	}
	return nil
}

func newGeneratedClients(ctx context.Context, endpoint string) (*generatedClients, error) {
	options := func(domain string) []option.ClientOption {
		return []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(endpoint + "/_minisky/" + domain + "/"),
		}
	}
	privateCAClient, err := privateca.NewService(ctx, options("privateca.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Private CA client: %w", err)
	}
	dlpClient, err := dlp.NewService(ctx, options("dlp.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create DLP client: %w", err)
	}
	orgClient, err := orgpolicy.NewService(ctx, options("orgpolicy.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Org Policy client: %w", err)
	}
	accessClient, err := accesscontextmanager.NewService(ctx, options("accesscontextmanager.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Access Context Manager client: %w", err)
	}
	assetClient, err := cloudasset.NewService(ctx, options("cloudasset.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Cloud Asset client: %w", err)
	}
	binaryClient, err := binaryauthorization.NewService(ctx, options("binaryauthorization.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Binary Authorization client: %w", err)
	}
	networkClient, err := networksecurity.NewService(ctx, options("networksecurity.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Network Security client: %w", err)
	}
	servicesClient, err := networkservices.NewService(ctx, options("networkservices.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Network Services client: %w", err)
	}
	storageClient, err := storage.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(endpoint+"/_minisky/storage.googleapis.com/storage/v1/"),
	)
	if err != nil {
		return nil, fmt.Errorf("create Storage enforcement-probe client: %w", err)
	}
	computeClient, err := compute.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(endpoint+"/_minisky/compute.googleapis.com/compute/v1/"),
	)
	if err != nil {
		return nil, fmt.Errorf("create Compute client: %w", err)
	}
	return &generatedClients{
		privateCA: privateCAClient, dlp: dlpClient, org: orgClient, access: accessClient,
		asset: assetClient, binary: binaryClient, network: networkClient,
		services: servicesClient, storage: storageClient, compute: computeClient,
	}, nil
}

func proveDefaultGate(ctx context.Context, clients *generatedClients, cfg config) error {
	parent := locationParent(cfg)
	caParent := parent + "/caPools/local"
	checks := []struct {
		name string
		call func() error
	}{
		{"Private CA", func() error {
			_, err := clients.privateCA.Projects.Locations.CaPools.Certificates.List(caParent).Context(ctx).Do()
			return err
		}},
		{"DLP", func() error {
			_, err := clients.dlp.Projects.InspectTemplates.List(projectParent(cfg)).Context(ctx).Do()
			return err
		}},
		{"Org Policy", func() error {
			_, err := clients.org.Projects.Policies.List(projectParent(cfg)).Context(ctx).Do()
			return err
		}},
		{"Access Context Manager", func() error {
			_, err := clients.access.AccessPolicies.List().Parent("organizations/123456789").Context(ctx).Do()
			return err
		}},
		{"Cloud Asset", func() error {
			_, err := clients.asset.V1.SearchAllResources(projectParent(cfg)).Context(ctx).Do()
			return err
		}},
		{"Binary Authorization", func() error {
			_, err := clients.binary.Projects.GetPolicy(projectParent(cfg) + "/policy").Context(ctx).Do()
			return err
		}},
		{"Network Security", func() error {
			_, err := clients.network.Projects.Locations.AuthorizationPolicies.List(parent).Context(ctx).Do()
			return err
		}},
		{"Network Services", func() error {
			_, err := clients.services.Projects.Locations.Meshes.List(parent).Context(ctx).Do()
			return err
		}},
	}
	for _, check := range checks {
		if err := expectGoogleStatus(check.call(), 501, "UNIMPLEMENTED"); err != nil {
			return fmt.Errorf("%s default experimental gate: %w", check.name, err)
		}
	}
	fmt.Println("default-disabled experimental gate verified with eight generated clients")
	return nil
}

func createAndRecord(ctx context.Context, clients *generatedClients, cfg config) error {
	parent := locationParent(cfg)
	project := projectParent(cfg)
	caParent := parent + "/caPools/local"
	certificateName := caParent + "/certificates/" + cfg.certificateID
	templateName := project + "/inspectTemplates/" + cfg.templateID
	orgPolicyName := project + "/policies/compute.requireOsLogin"
	binaryPolicyName := project + "/policy"
	networkPolicyName := parent + "/authorizationPolicies/" + cfg.networkID
	meshName := parent + "/meshes/" + cfg.meshID

	csr, err := certificateRequestPEM("phase24-25.local")
	if err != nil {
		return err
	}
	if _, err := clients.privateCA.Projects.Locations.CaPools.Certificates.Create(caParent, &privateca.Certificate{
		PemCsr: csr, Lifetime: "3600s",
	}).CertificateId(cfg.certificateID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("issue Private CA certificate: %w", err)
	}
	revoked, err := clients.privateCA.Projects.Locations.CaPools.Certificates.Revoke(
		certificateName, &privateca.RevokeCertificateRequest{Reason: "KEY_COMPROMISE"}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("revoke Private CA certificate: %w", err)
	}
	if revoked.Name != certificateName {
		return fmt.Errorf("revoked certificate name=%q want=%q", revoked.Name, certificateName)
	}
	if err := expectGoogleStatus(getPrivateCACertificate(ctx, clients.privateCA, certificateName), 501, "UNIMPLEMENTED"); err != nil {
		return fmt.Errorf("Private CA generated Get boundary: %w", err)
	}

	template, err := clients.dlp.Projects.InspectTemplates.Create(project,
		&dlp.GooglePrivacyDlpV2CreateInspectTemplateRequest{
			TemplateId: cfg.templateID,
			InspectTemplate: &dlp.GooglePrivacyDlpV2InspectTemplate{
				DisplayName: "Phase 24-25 generated client",
				InspectConfig: &dlp.GooglePrivacyDlpV2InspectConfig{
					InfoTypes: []*dlp.GooglePrivacyDlpV2InfoType{{Name: "EMAIL_ADDRESS"}},
				},
			},
		}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create DLP inspect template: %w", err)
	}
	if template.Name != templateName {
		return fmt.Errorf("DLP template name=%q want=%q", template.Name, templateName)
	}
	inspection, err := clients.dlp.Projects.Content.Inspect(project, &dlp.GooglePrivacyDlpV2InspectContentRequest{
		Item: &dlp.GooglePrivacyDlpV2ContentItem{Value: "phase24@example.com"},
		InspectConfig: &dlp.GooglePrivacyDlpV2InspectConfig{
			InfoTypes: []*dlp.GooglePrivacyDlpV2InfoType{{Name: "EMAIL_ADDRESS"}},
		},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("inspect DLP content: %w", err)
	}
	findingCount := 0
	if inspection.Result != nil {
		findingCount = len(inspection.Result.Findings)
	}
	if findingCount == 0 {
		return errors.New("DLP generated content inspection returned no findings")
	}
	_, unsupportedDLP := clients.dlp.Projects.Content.Inspect(project, &dlp.GooglePrivacyDlpV2InspectContentRequest{
		Item: &dlp.GooglePrivacyDlpV2ContentItem{Value: "555-0100"},
		InspectConfig: &dlp.GooglePrivacyDlpV2InspectConfig{
			InfoTypes: []*dlp.GooglePrivacyDlpV2InfoType{{Name: "PHONE_NUMBER"}},
		},
	}).Context(ctx).Do()
	if err := expectGoogleStatus(unsupportedDLP, 501, "UNIMPLEMENTED"); err != nil {
		return fmt.Errorf("DLP unsupported detector boundary: %w", err)
	}

	orgPolicy, err := clients.org.Projects.Policies.Create(project, &orgpolicy.GoogleCloudOrgpolicyV2Policy{
		Name: orgPolicyName,
		Spec: &orgpolicy.GoogleCloudOrgpolicyV2PolicySpec{
			Rules: []*orgpolicy.GoogleCloudOrgpolicyV2PolicySpecPolicyRule{{Enforce: true}},
		},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Org Policy: %w", err)
	}
	if orgPolicy.Name != orgPolicyName {
		return fmt.Errorf("Org Policy name=%q want=%q", orgPolicy.Name, orgPolicyName)
	}

	accessOp, err := clients.access.AccessPolicies.Create(&accesscontextmanager.AccessPolicy{
		Parent: "organizations/123456789", Title: "Phase 24-25 generated client",
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create access policy: %w", err)
	}
	if !accessOp.Done {
		return errors.New("access policy create operation did not complete")
	}
	accessList, err := clients.access.AccessPolicies.List().Parent("organizations/123456789").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list access policies: %w", err)
	}
	accessPolicyName := ""
	for _, policy := range accessList.AccessPolicies {
		if policy.Title == "Phase 24-25 generated client" {
			accessPolicyName = policy.Name
			break
		}
	}
	if accessPolicyName == "" {
		return errors.New("created access policy was not listed")
	}
	perimeterOp, err := clients.access.AccessPolicies.ServicePerimeters.Create(accessPolicyName,
		&accesscontextmanager.ServicePerimeter{
			Title: cfg.perimeterID,
			Status: &accesscontextmanager.ServicePerimeterConfig{
				Resources:          []string{project},
				RestrictedServices: []string{"storage.googleapis.com"},
			},
		}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create service perimeter: %w", err)
	}
	if !perimeterOp.Done {
		return errors.New("service perimeter create operation did not complete")
	}
	perimeterName := accessPolicyName + "/servicePerimeters/" + cfg.perimeterID
	if err := provePerimeterDenial(ctx, clients.storage, cfg.project); err != nil {
		return err
	}

	assetResults, err := clients.asset.V1.SearchAllResources(project).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("search Cloud Asset resources: %w", err)
	}
	_, exportErr := clients.asset.V1.ExportAssets(project, &cloudasset.ExportAssetsRequest{
		OutputConfig: &cloudasset.OutputConfig{
			GcsDestination: &cloudasset.GcsDestination{Uri: "gs://phase24-25-disabled/export.json"},
		},
	}).Context(ctx).Do()
	if err := expectGoogleStatus(exportErr, 501, "UNIMPLEMENTED"); err != nil {
		return fmt.Errorf("Cloud Asset generated export boundary: %w", err)
	}

	binaryPolicy, err := clients.binary.Projects.UpdatePolicy(binaryPolicyName, &binaryauthorization.Policy{
		Name: binaryPolicyName,
		DefaultAdmissionRule: &binaryauthorization.AdmissionRule{
			EvaluationMode: "DISALLOWED", EnforcementMode: "ENFORCED_BLOCK_AND_AUDIT_LOG",
		},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("set Binary Authorization policy: %w", err)
	}
	if binaryPolicy.Name != binaryPolicyName {
		return fmt.Errorf("Binary Authorization policy name=%q want=%q", binaryPolicy.Name, binaryPolicyName)
	}
	_, binaryEvalErr := clients.binary.Projects.Platforms.Gke.Policies.Evaluate(
		project+"/platforms/gke/policies/default", &binaryauthorization.EvaluateGkePolicyRequest{}).Context(ctx).Do()
	if err := expectGoogleStatus(binaryEvalErr, 501, "UNIMPLEMENTED"); err != nil {
		return fmt.Errorf("Binary Authorization generated evaluation boundary: %w", err)
	}

	networkOp, err := clients.network.Projects.Locations.AuthorizationPolicies.Create(parent,
		&networksecurity.AuthorizationPolicy{
			Name: networkPolicyName, Action: "DENY", Description: "Phase 25 metadata-only deny",
		}).
		AuthorizationPolicyId(cfg.networkID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Network Security policy: %w", err)
	}
	if _, err := waitNetworkOperation(ctx, clients.network, networkOp.Name); err != nil {
		return err
	}
	meshOp, err := clients.services.Projects.Locations.Meshes.Create(parent,
		&networkservices.Mesh{Name: meshName, Description: "Phase 25 generated client mesh"}).
		MeshId(cfg.meshID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Network Services mesh: %w", err)
	}
	if _, err := waitServicesOperation(ctx, clients.services, meshOp.Name); err != nil {
		return err
	}
	proxyProof, err := createProxyEnforcement(ctx, clients, cfg)
	if err != nil {
		return err
	}

	record := evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		CertificateName: certificateName, DLPTemplateName: templateName, OrgPolicyName: orgPolicyName,
		AccessPolicyName: accessPolicyName, ServicePerimeterName: perimeterName,
		BinaryPolicyName: binaryPolicyName, NetworkPolicyName: networkPolicyName, MeshName: meshName,
		ProxyNetworkPolicyName: proxyProof.networkPolicyName,
		ProxyMeshName:          proxyProof.meshName,
		HTTPRouteName:          proxyProof.routeName,
		DefaultBackendName:     defaultBackendID,
		RoutedBackendName:      routedBackendID,
		DLPFindingCount:        findingCount, CloudAssetResultCount: len(assetResults.Results),
		CloudAssetSearchVerified: true, PerimeterGatewayDenied: true,
		ProxyDenyNoBackendCall: true, MeshRouteSelectedBackend: true,
		DefaultBackendHits: 0, RoutedBackendHits: proxyProof.routedHits,
		ComputeBackendSetup:      "GENERATED_COMPUTE_STANDARD_RESOURCES_DIRECT_LOCAL_BACKEND_EXTENSION",
		ProxyRequestKind:         "DIRECT_LOCAL_DATA_PLANE",
		PrivateCADeleteSupport:   "UNAVAILABLE_IN_GENERATED_API",
		OrgPolicyEvaluateSupport: "UNAVAILABLE_IN_GENERATED_API",
		BinaryEvaluateSupport:    "UNIMPLEMENTED",
		NetworkEvaluateSupport:   "UNAVAILABLE_IN_GENERATED_API",
		CloudAssetExportSupport:  "UNIMPLEMENTED",
	}
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		return err
	}
	fmt.Println("created Phase 24-25 generated-client security/network evidence with enforced perimeter denial")
	return nil
}

type proxyEvidence struct {
	networkPolicyName string
	meshName          string
	routeName         string
	routedHits        int
}

func createProxyEnforcement(
	ctx context.Context,
	clients *generatedClients,
	cfg config,
) (proxyEvidence, error) {
	parent := projectParent(cfg) + "/locations/" + proxyLocation
	networkPolicyName := parent + "/authorizationPolicies/" + cfg.proxyPolicyID
	meshName := parent + "/meshes/" + cfg.proxyMeshID
	routeName := parent + "/httpRoutes/" + httpRouteID

	networkOp, err := clients.network.Projects.Locations.AuthorizationPolicies.Create(parent,
		&networksecurity.AuthorizationPolicy{
			Name: networkPolicyName, Action: "DENY",
			Description: "Phase 25 Compute proxy deny",
			Rules: []*networksecurity.Rule{{Destinations: []*networksecurity.Destination{{
				Hosts: []string{denyHost}, Methods: []string{http.MethodHead}, Ports: []int64{80},
			}}}},
		}).AuthorizationPolicyId(cfg.proxyPolicyID).Context(ctx).Do()
	if err != nil {
		return proxyEvidence{}, fmt.Errorf("create proxy Network Security policy: %w", err)
	}
	if _, err := waitNetworkOperation(ctx, clients.network, networkOp.Name); err != nil {
		return proxyEvidence{}, err
	}

	meshOp, err := clients.services.Projects.Locations.Meshes.Create(parent,
		&networkservices.Mesh{Name: meshName, Description: "Phase 25 Compute proxy mesh"}).
		MeshId(cfg.proxyMeshID).Context(ctx).Do()
	if err != nil {
		return proxyEvidence{}, fmt.Errorf("create proxy Network Services mesh: %w", err)
	}
	if _, err := waitServicesOperation(ctx, clients.services, meshOp.Name); err != nil {
		return proxyEvidence{}, err
	}
	routeOp, err := clients.services.Projects.Locations.HttpRoutes.Create(parent, &networkservices.HttpRoute{
		Name: routeName, Hostnames: []string{proxyHost}, Meshes: []string{meshName},
		Rules: []*networkservices.HttpRouteRouteRule{{
			Matches: []*networkservices.HttpRouteRouteMatch{{PrefixMatch: "/v1/"}},
			Action: &networkservices.HttpRouteRouteAction{
				Destinations: []*networkservices.HttpRouteDestination{{
					ServiceName: "projects/" + cfg.project + "/global/backendServices/" + routedBackendID,
					Weight:      100,
				}},
			},
		}},
	}).HttpRouteId(httpRouteID).Context(ctx).Do()
	if err != nil {
		return proxyEvidence{}, fmt.Errorf("create Network Services HTTP route: %w", err)
	}
	if _, err := waitServicesOperation(ctx, clients.services, routeOp.Name); err != nil {
		return proxyEvidence{}, err
	}

	for name, origin := range map[string]string{
		defaultBackendID: cfg.defaultOrigin,
		routedBackendID:  cfg.routedOrigin,
	} {
		if err := createLocalBackendURL(ctx, cfg, name, origin); err != nil {
			return proxyEvidence{}, err
		}
	}
	operation, err := clients.compute.UrlMaps.Insert(cfg.project,
		&compute.UrlMap{Name: urlMapID, DefaultService: defaultBackendID}).Context(ctx).Do()
	if err != nil {
		return proxyEvidence{}, fmt.Errorf("create Compute URL map: %w", err)
	}
	if err := waitComputeOperation(ctx, clients.compute, cfg.project, operation); err != nil {
		return proxyEvidence{}, err
	}
	operation, err = clients.compute.TargetHttpProxies.Insert(cfg.project,
		&compute.TargetHttpProxy{Name: targetProxyID, UrlMap: urlMapID}).Context(ctx).Do()
	if err != nil {
		return proxyEvidence{}, fmt.Errorf("create Compute target HTTP proxy: %w", err)
	}
	if err := waitComputeOperation(ctx, clients.compute, cfg.project, operation); err != nil {
		return proxyEvidence{}, err
	}
	operation, err = clients.compute.GlobalForwardingRules.Insert(cfg.project,
		&compute.ForwardingRule{Name: forwardingRuleID, Target: targetProxyID}).Context(ctx).Do()
	if err != nil {
		return proxyEvidence{}, fmt.Errorf("create Compute forwarding rule: %w", err)
	}
	if err := waitComputeOperation(ctx, clients.compute, cfg.project, operation); err != nil {
		return proxyEvidence{}, err
	}

	beforeDefault, err := readBackendHits(ctx, cfg.defaultOrigin)
	if err != nil {
		return proxyEvidence{}, err
	}
	beforeRouted, err := readBackendHits(ctx, cfg.routedOrigin)
	if err != nil {
		return proxyEvidence{}, err
	}
	if beforeDefault != 0 || beforeRouted != 0 {
		return proxyEvidence{}, fmt.Errorf("backend counters were not isolated: default=%d routed=%d", beforeDefault, beforeRouted)
	}
	denied, err := directProxyRequest(ctx, cfg, http.MethodHead, denyHost, "/admin")
	if err != nil {
		return proxyEvidence{}, err
	}
	if denied.status != http.StatusForbidden {
		return proxyEvidence{}, fmt.Errorf("direct local proxy deny status=%d body=%q", denied.status, denied.body)
	}
	afterDenyDefault, err := readBackendHits(ctx, cfg.defaultOrigin)
	if err != nil {
		return proxyEvidence{}, err
	}
	afterDenyRouted, err := readBackendHits(ctx, cfg.routedOrigin)
	if err != nil {
		return proxyEvidence{}, err
	}
	if afterDenyDefault != beforeDefault || afterDenyRouted != beforeRouted {
		return proxyEvidence{}, fmt.Errorf("authorization-denied request reached a backend: default=%d routed=%d",
			afterDenyDefault, afterDenyRouted)
	}
	routed, err := directProxyRequest(ctx, cfg, http.MethodGet, proxyHost, "/v1/item")
	if err != nil {
		return proxyEvidence{}, err
	}
	if routed.status != http.StatusOK || routed.body != "routed" {
		return proxyEvidence{}, fmt.Errorf("direct local mesh route status=%d body=%q", routed.status, routed.body)
	}
	defaultHits, err := readBackendHits(ctx, cfg.defaultOrigin)
	if err != nil {
		return proxyEvidence{}, err
	}
	routedHits, err := readBackendHits(ctx, cfg.routedOrigin)
	if err != nil {
		return proxyEvidence{}, err
	}
	if defaultHits != 0 || routedHits != 1 {
		return proxyEvidence{}, fmt.Errorf("mesh route backend counts default=%d routed=%d", defaultHits, routedHits)
	}
	fmt.Println("direct local data-plane requests verified authorization deny and mesh backend selection (not generated SDK)")
	return proxyEvidence{
		networkPolicyName: networkPolicyName, meshName: meshName, routeName: routeName, routedHits: routedHits,
	}, nil
}

func verifyRestart(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	revoked, err := clients.privateCA.Projects.Locations.CaPools.Certificates.Revoke(record.CertificateName,
		&privateca.RevokeCertificateRequest{Reason: "KEY_COMPROMISE"}).Context(ctx).Do()
	if err != nil || revoked.Name != record.CertificateName {
		return resourceResultError("restart re-read revoked certificate through revoke", record.CertificateName, valueCertificateName(revoked), err)
	}
	if got, err := clients.dlp.Projects.InspectTemplates.Get(record.DLPTemplateName).Context(ctx).Do(); err != nil || got.Name != record.DLPTemplateName {
		return resourceResultError("restart get DLP template", record.DLPTemplateName, valueDLPTemplateName(got), err)
	}
	if got, err := clients.org.Projects.Policies.Get(record.OrgPolicyName).Context(ctx).Do(); err != nil || got.Name != record.OrgPolicyName {
		return resourceResultError("restart get Org Policy", record.OrgPolicyName, valueOrgPolicyName(got), err)
	}
	if got, err := clients.access.AccessPolicies.Get(record.AccessPolicyName).Context(ctx).Do(); err != nil || got.Name != record.AccessPolicyName {
		return resourceResultError("restart get access policy", record.AccessPolicyName, valueAccessPolicyName(got), err)
	}
	if got, err := clients.access.AccessPolicies.ServicePerimeters.Get(record.ServicePerimeterName).Context(ctx).Do(); err != nil || got.Name != record.ServicePerimeterName {
		return resourceResultError("restart get service perimeter", record.ServicePerimeterName, valuePerimeterName(got), err)
	}
	if err := provePerimeterDenial(ctx, clients.storage, cfg.project); err != nil {
		return fmt.Errorf("restart %w", err)
	}
	if got, err := clients.binary.Projects.GetPolicy(record.BinaryPolicyName).Context(ctx).Do(); err != nil || got.Name != record.BinaryPolicyName {
		return resourceResultError("restart get Binary Authorization policy", record.BinaryPolicyName, valueBinaryPolicyName(got), err)
	}
	if got, err := clients.network.Projects.Locations.AuthorizationPolicies.Get(record.NetworkPolicyName).Context(ctx).Do(); err != nil || got.Name != record.NetworkPolicyName {
		return resourceResultError("restart get Network Security policy", record.NetworkPolicyName, valueNetworkPolicyName(got), err)
	}
	if got, err := clients.services.Projects.Locations.Meshes.Get(record.MeshName).Context(ctx).Do(); err != nil || got.Name != record.MeshName {
		return resourceResultError("restart get Network Services mesh", record.MeshName, valueMeshName(got), err)
	}
	if got, err := clients.network.Projects.Locations.AuthorizationPolicies.Get(record.ProxyNetworkPolicyName).Context(ctx).Do(); err != nil || got.Name != record.ProxyNetworkPolicyName {
		return resourceResultError("restart get proxy Network Security policy", record.ProxyNetworkPolicyName, valueNetworkPolicyName(got), err)
	}
	if got, err := clients.services.Projects.Locations.Meshes.Get(record.ProxyMeshName).Context(ctx).Do(); err != nil || got.Name != record.ProxyMeshName {
		return resourceResultError("restart get proxy mesh", record.ProxyMeshName, valueMeshName(got), err)
	}
	if got, err := clients.services.Projects.Locations.HttpRoutes.Get(record.HTTPRouteName).Context(ctx).Do(); err != nil || got.Name != record.HTTPRouteName {
		return resourceResultError("restart get mesh HTTP route", record.HTTPRouteName, valueHTTPRouteName(got), err)
	}
	for name, get := range map[string]func() error{
		record.DefaultBackendName: func() error {
			_, err := clients.compute.BackendServices.Get(cfg.project, record.DefaultBackendName).Context(ctx).Do()
			return err
		},
		record.RoutedBackendName: func() error {
			_, err := clients.compute.BackendServices.Get(cfg.project, record.RoutedBackendName).Context(ctx).Do()
			return err
		},
		urlMapID: func() error {
			_, err := clients.compute.UrlMaps.Get(cfg.project, urlMapID).Context(ctx).Do()
			return err
		},
		targetProxyID: func() error {
			_, err := clients.compute.TargetHttpProxies.Get(cfg.project, targetProxyID).Context(ctx).Do()
			return err
		},
		forwardingRuleID: func() error {
			_, err := clients.compute.GlobalForwardingRules.Get(cfg.project, forwardingRuleID).Context(ctx).Do()
			return err
		},
	} {
		if err := get(); err != nil {
			return fmt.Errorf("restart get Compute resource %s: %w", name, err)
		}
	}
	beforeDefault, err := readBackendHits(ctx, cfg.defaultOrigin)
	if err != nil {
		return err
	}
	beforeRouted, err := readBackendHits(ctx, cfg.routedOrigin)
	if err != nil {
		return err
	}
	denied, err := directProxyRequest(ctx, cfg, http.MethodHead, denyHost, "/admin")
	if err != nil {
		return err
	}
	afterDenyDefault, err := readBackendHits(ctx, cfg.defaultOrigin)
	if err != nil {
		return err
	}
	afterDenyRouted, err := readBackendHits(ctx, cfg.routedOrigin)
	if err != nil {
		return err
	}
	if denied.status != http.StatusForbidden || afterDenyDefault != beforeDefault || afterDenyRouted != beforeRouted {
		return fmt.Errorf("restart authorization deny status=%d backend counts before=%d/%d after=%d/%d",
			denied.status, beforeDefault, beforeRouted, afterDenyDefault, afterDenyRouted)
	}
	routed, err := directProxyRequest(ctx, cfg, http.MethodGet, proxyHost, "/v1/restarted")
	if err != nil {
		return err
	}
	finalDefault, err := readBackendHits(ctx, cfg.defaultOrigin)
	if err != nil {
		return err
	}
	finalRouted, err := readBackendHits(ctx, cfg.routedOrigin)
	if err != nil {
		return err
	}
	if routed.status != http.StatusOK || routed.body != "routed" ||
		finalDefault != beforeDefault || finalRouted != beforeRouted+1 {
		return fmt.Errorf("restart mesh route status=%d body=%q backend counts default=%d routed=%d",
			routed.status, routed.body, finalDefault, finalRouted)
	}
	record.MeshRouteRestartVerified = true
	record.DefaultBackendHits = finalDefault
	record.RoutedBackendHits = finalRouted
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		return err
	}
	fmt.Println("direct local data-plane restart enforcement verified (not generated SDK)")
	fmt.Println("restart persistence and public-gateway perimeter denial verified with generated clients")
	return nil
}

func deleteAndVerify(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	routeOp, err := clients.services.Projects.Locations.HttpRoutes.Delete(record.HTTPRouteName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete mesh HTTP route: %w", err)
	}
	if _, err := waitServicesOperation(ctx, clients.services, routeOp.Name); err != nil {
		return err
	}
	proxyMeshOp, err := clients.services.Projects.Locations.Meshes.Delete(record.ProxyMeshName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete proxy mesh: %w", err)
	}
	if _, err := waitServicesOperation(ctx, clients.services, proxyMeshOp.Name); err != nil {
		return err
	}
	proxyPolicyOp, err := clients.network.Projects.Locations.AuthorizationPolicies.Delete(record.ProxyNetworkPolicyName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete proxy Network Security policy: %w", err)
	}
	if _, err := waitNetworkOperation(ctx, clients.network, proxyPolicyOp.Name); err != nil {
		return err
	}
	for _, remove := range []struct {
		name string
		call func() (*compute.Operation, error)
	}{
		{forwardingRuleID, func() (*compute.Operation, error) {
			return clients.compute.GlobalForwardingRules.Delete(cfg.project, forwardingRuleID).Context(ctx).Do()
		}},
		{targetProxyID, func() (*compute.Operation, error) {
			return clients.compute.TargetHttpProxies.Delete(cfg.project, targetProxyID).Context(ctx).Do()
		}},
		{urlMapID, func() (*compute.Operation, error) {
			return clients.compute.UrlMaps.Delete(cfg.project, urlMapID).Context(ctx).Do()
		}},
		{routedBackendID, func() (*compute.Operation, error) {
			return clients.compute.BackendServices.Delete(cfg.project, routedBackendID).Context(ctx).Do()
		}},
		{defaultBackendID, func() (*compute.Operation, error) {
			return clients.compute.BackendServices.Delete(cfg.project, defaultBackendID).Context(ctx).Do()
		}},
	} {
		operation, err := remove.call()
		if err != nil {
			return fmt.Errorf("delete Compute resource %s: %w", remove.name, err)
		}
		if err := waitComputeOperation(ctx, clients.compute, cfg.project, operation); err != nil {
			return err
		}
	}
	if _, err := clients.dlp.Projects.InspectTemplates.Delete(record.DLPTemplateName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete DLP template: %w", err)
	}
	if _, err := clients.org.Projects.Policies.Delete(record.OrgPolicyName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Org Policy: %w", err)
	}
	perimeterOp, err := clients.access.AccessPolicies.ServicePerimeters.Delete(record.ServicePerimeterName).Context(ctx).Do()
	if err != nil || !perimeterOp.Done {
		return fmt.Errorf("delete service perimeter done=%t: %w", perimeterOp != nil && perimeterOp.Done, err)
	}
	accessOp, err := clients.access.AccessPolicies.Delete(record.AccessPolicyName).Context(ctx).Do()
	if err != nil || !accessOp.Done {
		return fmt.Errorf("delete access policy done=%t: %w", accessOp != nil && accessOp.Done, err)
	}
	networkOp, err := clients.network.Projects.Locations.AuthorizationPolicies.Delete(record.NetworkPolicyName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete Network Security policy: %w", err)
	}
	if _, err := waitNetworkOperation(ctx, clients.network, networkOp.Name); err != nil {
		return err
	}
	meshOp, err := clients.services.Projects.Locations.Meshes.Delete(record.MeshName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete Network Services mesh: %w", err)
	}
	if _, err := waitServicesOperation(ctx, clients.services, meshOp.Name); err != nil {
		return err
	}

	checks := []struct {
		name string
		call func() error
	}{
		{"DLP template", func() error {
			_, err := clients.dlp.Projects.InspectTemplates.Get(record.DLPTemplateName).Context(ctx).Do()
			return err
		}},
		{"Org Policy", func() error {
			_, err := clients.org.Projects.Policies.Get(record.OrgPolicyName).Context(ctx).Do()
			return err
		}},
		{"service perimeter", func() error {
			_, err := clients.access.AccessPolicies.ServicePerimeters.Get(record.ServicePerimeterName).Context(ctx).Do()
			return err
		}},
		{"access policy", func() error {
			_, err := clients.access.AccessPolicies.Get(record.AccessPolicyName).Context(ctx).Do()
			return err
		}},
		{"Network Security policy", func() error {
			_, err := clients.network.Projects.Locations.AuthorizationPolicies.Get(record.NetworkPolicyName).Context(ctx).Do()
			return err
		}},
		{"Network Services mesh", func() error {
			_, err := clients.services.Projects.Locations.Meshes.Get(record.MeshName).Context(ctx).Do()
			return err
		}},
		{"proxy Network Security policy", func() error {
			_, err := clients.network.Projects.Locations.AuthorizationPolicies.Get(record.ProxyNetworkPolicyName).Context(ctx).Do()
			return err
		}},
		{"proxy mesh", func() error {
			_, err := clients.services.Projects.Locations.Meshes.Get(record.ProxyMeshName).Context(ctx).Do()
			return err
		}},
		{"mesh HTTP route", func() error {
			_, err := clients.services.Projects.Locations.HttpRoutes.Get(record.HTTPRouteName).Context(ctx).Do()
			return err
		}},
		{"Compute forwarding rule", func() error {
			_, err := clients.compute.GlobalForwardingRules.Get(cfg.project, forwardingRuleID).Context(ctx).Do()
			return err
		}},
		{"Compute target proxy", func() error {
			_, err := clients.compute.TargetHttpProxies.Get(cfg.project, targetProxyID).Context(ctx).Do()
			return err
		}},
		{"Compute URL map", func() error {
			_, err := clients.compute.UrlMaps.Get(cfg.project, urlMapID).Context(ctx).Do()
			return err
		}},
		{"Compute routed backend", func() error {
			_, err := clients.compute.BackendServices.Get(cfg.project, routedBackendID).Context(ctx).Do()
			return err
		}},
		{"Compute default backend", func() error {
			_, err := clients.compute.BackendServices.Get(cfg.project, defaultBackendID).Context(ctx).Do()
			return err
		}},
	}
	for _, check := range checks {
		if err := expectGoogleStatus(check.call(), 404, "NOT_FOUND"); err != nil {
			return fmt.Errorf("verify deleted %s: %w", check.name, err)
		}
	}
	if _, err := clients.binary.Projects.UpdatePolicy(record.BinaryPolicyName, &binaryauthorization.Policy{
		Name: record.BinaryPolicyName,
		DefaultAdmissionRule: &binaryauthorization.AdmissionRule{
			EvaluationMode: "ALWAYS_ALLOW", EnforcementMode: "ENFORCED_BLOCK_AND_AUDIT_LOG",
		},
	}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("neutralize non-deletable Binary Authorization policy: %w", err)
	}
	fmt.Println("generated-client delete/404 cleanup verified; non-deletable isolated policy neutralized")
	return nil
}

func provePerimeterDenial(ctx context.Context, client *storage.Service, project string) error {
	_, err := client.Buckets.List(project).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 403, "PERMISSION_DENIED"); statusErr != nil {
		return fmt.Errorf("generated Storage public-gateway perimeter denial: %w", statusErr)
	}
	return nil
}

func waitNetworkOperation(ctx context.Context, client *networksecurity.Service, name string) (*networksecurity.Operation, error) {
	for {
		operation, err := client.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return nil, fmt.Errorf("poll Network Security operation: %w", err)
		}
		if operation.Done {
			if operation.Error != nil {
				return nil, fmt.Errorf("Network Security operation failed: %s", operation.Error.Message)
			}
			return operation, nil
		}
		if err := waitTick(ctx); err != nil {
			return nil, err
		}
	}
}

func waitServicesOperation(ctx context.Context, client *networkservices.Service, name string) (*networkservices.Operation, error) {
	for {
		operation, err := client.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return nil, fmt.Errorf("poll Network Services operation: %w", err)
		}
		if operation.Done {
			if operation.Error != nil {
				return nil, fmt.Errorf("Network Services operation failed: %s", operation.Error.Message)
			}
			return operation, nil
		}
		if err := waitTick(ctx); err != nil {
			return nil, err
		}
	}
}

func waitComputeOperation(
	ctx context.Context,
	client *compute.Service,
	project string,
	operation *compute.Operation,
) error {
	if operation == nil || operation.Name == "" {
		return errors.New("Compute global operation response omitted name")
	}
	for {
		current, err := client.GlobalOperations.Get(project, operation.Name).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("poll Compute global operation: %w", err)
		}
		if current.Status == "DONE" {
			if current.Error == nil || len(current.Error.Errors) == 0 {
				return nil
			}
			messages := make([]string, 0, len(current.Error.Errors))
			for _, item := range current.Error.Errors {
				if item != nil {
					messages = append(messages, strings.TrimSpace(item.Code+": "+item.Message))
				}
			}
			if len(messages) == 0 {
				return errors.New("Compute operation reported an unspecified error")
			}
			return fmt.Errorf("Compute operation %s failed: %s", current.Name, strings.Join(messages, "; "))
		}
		if err := waitTick(ctx); err != nil {
			return err
		}
	}
}

func createLocalBackendURL(ctx context.Context, cfg config, name, origin string) error {
	body, err := json.Marshal(map[string]any{
		"name": name, "backends": []map[string]string{{"url": origin}},
	})
	if err != nil {
		return err
	}
	endpoint := cfg.endpoint + "/_minisky/compute.googleapis.com/compute/v1/projects/" +
		url.PathEscape(cfg.project) + "/global/backendServices"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("direct local backend extension create: %w", err)
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	if readErr != nil {
		return readErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("direct local backend extension create status=%d body=%q", response.StatusCode, responseBody)
	}
	return nil
}

type proxyResponse struct {
	status int
	body   string
}

func directProxyRequest(
	ctx context.Context,
	cfg config,
	method, host, requestPath string,
) (proxyResponse, error) {
	endpoint := cfg.endpoint + "/_minisky/compute.googleapis.com/compute/v1/projects/" +
		url.PathEscape(cfg.project) + "/global/forwardingRules/" + forwardingRuleID + "/proxy" + requestPath
	request, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return proxyResponse{}, err
	}
	request.Host = host
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return proxyResponse{}, fmt.Errorf("direct local Compute proxy request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	if err != nil {
		return proxyResponse{}, err
	}
	return proxyResponse{status: response.StatusCode, body: strings.TrimSpace(string(body))}, nil
}

func readBackendHits(ctx context.Context, origin string) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/__hits", nil)
	if err != nil {
		return 0, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("read local backend counter: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64))
	if err != nil {
		return 0, err
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("backend counter status=%d body=%q", response.StatusCode, body)
	}
	hits, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil || hits < 0 {
		return 0, fmt.Errorf("invalid backend counter %q", body)
	}
	return hits, nil
}

func waitTick(ctx context.Context) error {
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func certificateRequestPEM(commonName string) (string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", fmt.Errorf("generate CSR key: %w", err)
	}
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:  pkix.Name{CommonName: commonName},
		DNSNames: []string{commonName},
	}, key)
	if err != nil {
		return "", fmt.Errorf("create CSR: %w", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: request})), nil
}

func getPrivateCACertificate(ctx context.Context, client *privateca.Service, name string) error {
	_, err := client.Projects.Locations.CaPools.Certificates.Get(name).Context(ctx).Do()
	return err
}

func expectGoogleStatus(err error, code int, status string) error {
	if err == nil {
		return fmt.Errorf("expected HTTP %d %s, got success", code, status)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return fmt.Errorf("expected googleapi.Error, got %T: %w", err, err)
	}
	if apiErr.Code != code {
		return fmt.Errorf("HTTP code=%d want=%d body=%s", apiErr.Code, code, apiErr.Body)
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(apiErr.Body), &envelope) != nil || envelope.Error.Status != status {
		return fmt.Errorf("status=%q want=%q body=%s", envelope.Error.Status, status, apiErr.Body)
	}
	return nil
}

func writeEvidence(path string, record evidence) error {
	if err := validateEvidence(record); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".phase24-25-evidence-*.tmp")
	if err != nil {
		return fmt.Errorf("create evidence temp file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("publish evidence: %w", err)
	}
	return nil
}

func readEvidence(path string, cfg config) (evidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return evidence{}, fmt.Errorf("read evidence: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxEvidenceBytes+1))
	if err != nil {
		return evidence{}, err
	}
	if len(data) > maxEvidenceBytes {
		return evidence{}, fmt.Errorf("evidence exceeds %d-byte limit", maxEvidenceBytes)
	}
	var record evidence
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return evidence{}, fmt.Errorf("decode evidence: %w", err)
	}
	if err := validateEvidence(record); err != nil {
		return evidence{}, err
	}
	if record.Project != cfg.project || record.Location != cfg.location ||
		record.CertificateName != certificateName(cfg) ||
		record.DLPTemplateName != projectParent(cfg)+"/inspectTemplates/"+cfg.templateID ||
		record.ServicePerimeterName != record.AccessPolicyName+"/servicePerimeters/"+cfg.perimeterID ||
		record.NetworkPolicyName != locationParent(cfg)+"/authorizationPolicies/"+cfg.networkID ||
		record.MeshName != locationParent(cfg)+"/meshes/"+cfg.meshID ||
		record.ProxyNetworkPolicyName != proxyParent(cfg)+"/authorizationPolicies/"+cfg.proxyPolicyID ||
		record.ProxyMeshName != proxyParent(cfg)+"/meshes/"+cfg.proxyMeshID ||
		record.HTTPRouteName != proxyParent(cfg)+"/httpRoutes/"+httpRouteID ||
		record.DefaultBackendName != defaultBackendID || record.RoutedBackendName != routedBackendID {
		return evidence{}, errors.New("evidence identifiers do not match requested smoke configuration")
	}
	if cfg.mode == "delete" && !record.MeshRouteRestartVerified {
		return evidence{}, errors.New("delete requires restart-verified mesh route evidence")
	}
	return record, nil
}

func validateEvidence(record evidence) error {
	if record.Version != evidenceVersion {
		return fmt.Errorf("evidence version=%d want=%d", record.Version, evidenceVersion)
	}
	required := map[string]string{
		"project": record.Project, "location": record.Location, "certificate": record.CertificateName,
		"DLP template": record.DLPTemplateName, "Org Policy": record.OrgPolicyName,
		"access policy": record.AccessPolicyName, "service perimeter": record.ServicePerimeterName,
		"Binary Authorization policy": record.BinaryPolicyName,
		"Network Security policy":     record.NetworkPolicyName, "mesh": record.MeshName,
		"proxy Network Security policy": record.ProxyNetworkPolicyName,
		"proxy mesh":                    record.ProxyMeshName, "HTTP route": record.HTTPRouteName,
		"default backend": record.DefaultBackendName, "routed backend": record.RoutedBackendName,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("evidence %s is empty", name)
		}
	}
	if record.DLPFindingCount < 1 || record.CloudAssetResultCount < 0 ||
		!record.CloudAssetSearchVerified || !record.PerimeterGatewayDenied ||
		!record.ProxyDenyNoBackendCall || !record.MeshRouteSelectedBackend ||
		record.DefaultBackendHits != 0 || record.RoutedBackendHits < 1 ||
		record.MeshRouteRestartVerified && record.RoutedBackendHits < 2 {
		return errors.New("evidence is missing content, inventory, or enforcement proof")
	}
	if record.ComputeBackendSetup != "GENERATED_COMPUTE_STANDARD_RESOURCES_DIRECT_LOCAL_BACKEND_EXTENSION" ||
		record.ProxyRequestKind != "DIRECT_LOCAL_DATA_PLANE" {
		return errors.New("evidence misclassifies direct local proxy boundaries")
	}
	if record.PrivateCADeleteSupport != "UNAVAILABLE_IN_GENERATED_API" ||
		record.OrgPolicyEvaluateSupport != "UNAVAILABLE_IN_GENERATED_API" ||
		record.BinaryEvaluateSupport != "UNIMPLEMENTED" ||
		record.NetworkEvaluateSupport != "UNAVAILABLE_IN_GENERATED_API" ||
		record.CloudAssetExportSupport != "UNIMPLEMENTED" {
		return errors.New("evidence unsupported-boundary classifications are invalid")
	}
	return nil
}

func projectParent(cfg config) string {
	return "projects/" + cfg.project
}

func locationParent(cfg config) string {
	return projectParent(cfg) + "/locations/" + cfg.location
}

func proxyParent(cfg config) string {
	return projectParent(cfg) + "/locations/" + proxyLocation
}

func certificateName(cfg config) string {
	return locationParent(cfg) + "/caPools/local/certificates/" + cfg.certificateID
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func resourceResultError(action, want, got string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	return fmt.Errorf("%s name=%q want=%q", action, got, want)
}

func valueCertificateName(value *privateca.Certificate) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valueDLPTemplateName(value *dlp.GooglePrivacyDlpV2InspectTemplate) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valueOrgPolicyName(value *orgpolicy.GoogleCloudOrgpolicyV2Policy) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valueAccessPolicyName(value *accesscontextmanager.AccessPolicy) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valuePerimeterName(value *accesscontextmanager.ServicePerimeter) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valueBinaryPolicyName(value *binaryauthorization.Policy) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valueNetworkPolicyName(value *networksecurity.AuthorizationPolicy) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valueMeshName(value *networkservices.Mesh) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func valueHTTPRouteName(value *networkservices.HttpRoute) string {
	if value == nil {
		return ""
	}
	return value.Name
}
