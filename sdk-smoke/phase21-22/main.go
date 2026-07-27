package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	apigateway "google.golang.org/api/apigateway/v1"
	binaryauthorization "google.golang.org/api/binaryauthorization/v1"
	clouddeploy "google.golang.org/api/clouddeploy/v1"
	errorreporting "google.golang.org/api/clouderrorreporting/v1beta1"
	cloudprofiler "google.golang.org/api/cloudprofiler/v2"
	cloudtracev1 "google.golang.org/api/cloudtrace/v1"
	cloudtracev2 "google.golang.org/api/cloudtrace/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	servicecontrol "google.golang.org/api/servicecontrol/v1"
	servicedirectory "google.golang.org/api/servicedirectory/v1"
	servicemanagement "google.golang.org/api/servicemanagement/v1"

	localgateway "minisky/pkg/shims/apigateway"
	localdirectory "minisky/pkg/shims/servicedirectory"
)

const (
	optInEnv         = "MINISKY_PHASE21_22_OPT_IN"
	evidenceVersion  = 1
	maxEvidenceBytes = 32 << 10
	traceID          = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	spanID           = "bbbbbbbbbbbbbbbb"
)

var resourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

type config struct {
	mode, endpoint, project, location, evidencePath string
	apiID, configID, gatewayID                      string
	namespaceID, serviceID, endpointID              string
	pipelineID, releaseID                           string
	endpointsService                                string
}

type evidence struct {
	Version           int    `json:"version"`
	Project           string `json:"project"`
	Location          string `json:"location"`
	TraceID           string `json:"traceId"`
	ErrorMessage      string `json:"errorMessage"`
	APIName           string `json:"apiName"`
	APIConfigName     string `json:"apiConfigName"`
	GatewayName       string `json:"gatewayName"`
	NamespaceName     string `json:"namespaceName"`
	DirectoryService  string `json:"directoryService"`
	DirectoryEndpoint string `json:"directoryEndpoint"`
	EndpointsService  string `json:"endpointsService"`
	EndpointsConfig   string `json:"endpointsConfig"`
	EndpointsRollout  string `json:"endpointsRollout"`
	PipelineName      string `json:"pipelineName"`
	ReleaseName       string `json:"releaseName"`
	AllowedRollout    string `json:"allowedRollout"`
	DeniedRollout     string `json:"deniedRollout"`
}

type clients struct {
	traceWrite *cloudtracev2.Service
	traceRead  *cloudtracev1.Service
	errors     *errorreporting.Service
	profiler   *cloudprofiler.Service
	gateway    *apigateway.Service
	directory  *servicedirectory.APIService
	management *servicemanagement.APIService
	control    *servicecontrol.Service
	deploy     *clouddeploy.Service
	binauthz   *binaryauthorization.Service
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Phase 21-22 generated client smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := newClients(ctx, cfg.endpoint)
	if err != nil {
		return err
	}
	switch cfg.mode {
	case "gate":
		return proveDefaultGate(ctx, c, cfg)
	case "create":
		return createAndRecord(ctx, c, cfg)
	case "verify":
		return verifyRestart(ctx, c, cfg)
	case "delete":
		return deleteAndVerify(ctx, c, cfg)
	default:
		return fmt.Errorf("unsupported MINISKY_PHASE21_22_MODE %q", cfg.mode)
	}
}

func configFromEnv() (config, error) {
	cfg := config{
		mode: env("MINISKY_PHASE21_22_MODE", "gate"), endpoint: strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project: env("MINISKY_PROJECT_ID", "phase21-22-project"), location: env("MINISKY_PHASE21_22_LOCATION", "us-central1"),
		apiID: env("MINISKY_PHASE21_22_API_ID", "phase22-api"), configID: env("MINISKY_PHASE21_22_CONFIG_ID", "phase22-config"),
		gatewayID:        env("MINISKY_PHASE21_22_GATEWAY_ID", "phase22-gateway"),
		namespaceID:      env("MINISKY_PHASE21_22_NAMESPACE_ID", "phase22-namespace"),
		serviceID:        env("MINISKY_PHASE21_22_SERVICE_ID", "phase22-service"),
		endpointID:       env("MINISKY_PHASE21_22_ENDPOINT_ID", "phase22-endpoint"),
		pipelineID:       env("MINISKY_PHASE21_22_PIPELINE_ID", "phase22-pipeline"),
		releaseID:        env("MINISKY_PHASE21_22_RELEASE_ID", "phase22-release"),
		endpointsService: env("MINISKY_PHASE21_22_ENDPOINTS_SERVICE", "phase22.endpoints.test"),
		evidencePath:     strings.TrimSpace(os.Getenv("MINISKY_PHASE21_22_EVIDENCE")),
	}
	if err := validateLoopbackEndpoint(cfg.endpoint); err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{
		"project": cfg.project, "location": cfg.location, "API ID": cfg.apiID, "config ID": cfg.configID,
		"gateway ID": cfg.gatewayID, "namespace ID": cfg.namespaceID, "service ID": cfg.serviceID,
		"endpoint ID": cfg.endpointID, "pipeline ID": cfg.pipelineID, "release ID": cfg.releaseID,
	} {
		if !resourceIDPattern.MatchString(value) {
			return config{}, fmt.Errorf("%s %q must match %s", name, value, resourceIDPattern)
		}
	}
	if cfg.evidencePath == "" || !filepath.IsAbs(cfg.evidencePath) {
		return config{}, errors.New("MINISKY_PHASE21_22_EVIDENCE must be an absolute path")
	}
	if cfg.mode != "gate" && os.Getenv(optInEnv) != "1" {
		return config{}, fmt.Errorf("%s mode requires explicit %s=1", cfg.mode, optInEnv)
	}
	return cfg, nil
}

func validateLoopbackEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Port() == "" {
		return errors.New("MINISKY_ENDPOINT must be an HTTP loopback origin with an explicit port and no path")
	}
	if host := parsed.Hostname(); !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("MINISKY_ENDPOINT must target localhost or a loopback IP")
		}
	}
	return nil
}

func newClients(ctx context.Context, endpoint string) (*clients, error) {
	opts := func(domain string) []option.ClientOption {
		return []option.ClientOption{option.WithoutAuthentication(), option.WithEndpoint(endpoint + "/_minisky/" + domain + "/")}
	}
	var c clients
	var err error
	if c.traceWrite, err = cloudtracev2.NewService(ctx, opts("cloudtrace.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.traceRead, err = cloudtracev1.NewService(ctx, opts("cloudtrace.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.errors, err = errorreporting.NewService(ctx, opts("clouderrorreporting.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.profiler, err = cloudprofiler.NewService(ctx, opts("cloudprofiler.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.gateway, err = apigateway.NewService(ctx, opts("apigateway.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.directory, err = servicedirectory.NewService(ctx, opts("servicedirectory.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.management, err = servicemanagement.NewService(ctx, opts("servicemanagement.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.control, err = servicecontrol.NewService(ctx, opts("servicecontrol.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.deploy, err = clouddeploy.NewService(ctx, opts("clouddeploy.googleapis.com")...); err != nil {
		return nil, err
	}
	if c.binauthz, err = binaryauthorization.NewService(ctx, opts("binaryauthorization.googleapis.com")...); err != nil {
		return nil, err
	}
	return &c, nil
}

func proveDefaultGate(ctx context.Context, c *clients, cfg config) error {
	checks := []struct {
		name string
		call func() error
	}{
		{"Cloud Trace", func() error { _, err := c.traceRead.Projects.Traces.List(cfg.project).Context(ctx).Do(); return err }},
		{"Error Reporting", func() error {
			_, err := c.errors.Projects.GroupStats.List("projects/" + cfg.project).Context(ctx).Do()
			return err
		}},
		{"Cloud Profiler", func() error {
			_, err := c.profiler.Projects.Profiles.List("projects/" + cfg.project).Context(ctx).Do()
			return err
		}},
		{"API Gateway", func() error {
			_, err := c.gateway.Projects.Locations.Apis.List("projects/" + cfg.project + "/locations/global").Context(ctx).Do()
			return err
		}},
		{"Service Directory", func() error {
			_, err := c.directory.Projects.Locations.Namespaces.List(locationParent(cfg)).Context(ctx).Do()
			return err
		}},
		{"Service Management", func() error {
			_, err := c.management.Services.Configs.List(cfg.endpointsService).Context(ctx).Do()
			return err
		}},
		{"Service Control", func() error {
			_, err := c.control.Services.AllocateQuota(cfg.endpointsService, &servicecontrol.AllocateQuotaRequest{}).Context(ctx).Do()
			return err
		}},
		{"Cloud Deploy", func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.List(locationParent(cfg)).Context(ctx).Do()
			return err
		}},
	}
	for _, check := range checks {
		if err := expectGoogleStatus(check.call(), 501, "UNIMPLEMENTED"); err != nil {
			return fmt.Errorf("%s default gate: %w", check.name, err)
		}
	}
	fmt.Println("default-disabled Phase 21-22 gate verified with eight generated clients")
	return nil
}

func createAndRecord(ctx context.Context, c *clients, cfg config) error {
	parent := locationParent(cfg)
	traceName := "projects/" + cfg.project + "/traces/" + traceID + "/spans/" + spanID
	if _, err := c.traceWrite.Projects.Traces.BatchWrite("projects/"+cfg.project,
		&cloudtracev2.BatchWriteSpansRequest{Spans: []*cloudtracev2.Span{{Name: traceName, SpanId: spanID}}}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated Cloud Trace batch write: %w", err)
	}
	traceSpans, err := localTraceGet(ctx, cfg, cfg.project, traceID)
	if err != nil || traceSpans != 1 {
		return fmt.Errorf("MiniSky-local Cloud Trace query spans=%d: %w", traceSpans, err)
	}
	otherTraces, err := localTraceList(ctx, cfg, cfg.project+"-other")
	if err != nil || otherTraces != 0 {
		return fmt.Errorf("Cloud Trace project isolation traces=%d: %w", otherTraces, err)
	}
	errorMessage := "phase21 generated error"
	if _, err := c.errors.Projects.Events.Report("projects/"+cfg.project, &errorreporting.ReportedErrorEvent{
		Message: errorMessage, ServiceContext: &errorreporting.ServiceContext{Service: "phase21-smoke"},
	}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated Error Reporting ingestion: %w", err)
	}
	groups, err := c.errors.Projects.GroupStats.List("projects/" + cfg.project).Context(ctx).Do()
	if err != nil || len(groups.ErrorGroupStats) != 1 || groups.ErrorGroupStats[0].Count != 1 {
		return fmt.Errorf("generated Error Reporting query groups=%d: %w", groupCount(groups), err)
	}
	otherGroups, err := c.errors.Projects.GroupStats.List("projects/" + cfg.project + "-other").Context(ctx).Do()
	if err != nil || groupCount(otherGroups) != 0 {
		return fmt.Errorf("Error Reporting project isolation groups=%d: %w", groupCount(otherGroups), err)
	}
	profile, err := c.profiler.Projects.Profiles.Create("projects/"+cfg.project, &cloudprofiler.CreateProfileRequest{
		Deployment: &cloudprofiler.Deployment{ProjectId: cfg.project, Target: "phase21-smoke"}, ProfileType: []string{"CPU"},
	}).Context(ctx).Do()
	if err != nil || profile.Name == "" {
		return fmt.Errorf("generated Cloud Profiler create: %w", err)
	}
	if _, err := c.profiler.Projects.Profiles.Patch(profile.Name, &cloudprofiler.Profile{
		Labels: map[string]string{"phase": "21"},
	}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated Cloud Profiler update: %w", err)
	}

	apiName := "projects/" + cfg.project + "/locations/global/apis/" + cfg.apiID
	apiConfigName := apiName + "/configs/" + cfg.configID
	gatewayName := parent + "/gateways/" + cfg.gatewayID
	if _, err := c.gateway.Projects.Locations.Apis.Create("projects/"+cfg.project+"/locations/global",
		&apigateway.ApigatewayApi{}).ApiId(cfg.apiID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated API Gateway API create: %w", err)
	}
	backendDoc := base64.StdEncoding.EncodeToString([]byte(`{"swagger":"2.0","x-google-backend":{"address":"http://127.0.0.1:1"}}`))
	if _, err := c.gateway.Projects.Locations.Apis.Configs.Create(apiName, &apigateway.ApigatewayApiConfig{
		OpenapiDocuments: []*apigateway.ApigatewayApiConfigOpenApiDocument{{
			Document: &apigateway.ApigatewayApiConfigFile{Path: "openapi.json", Contents: backendDoc},
		}},
	}).ApiConfigId(cfg.configID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated API Gateway config create: %w", err)
	}
	ssrfDoc := base64.StdEncoding.EncodeToString([]byte(`{"swagger":"2.0","x-google-backend":{"address":"http://169.254.169.254/latest"}}`))
	_, err = c.gateway.Projects.Locations.Apis.Configs.Create(apiName, &apigateway.ApigatewayApiConfig{
		OpenapiDocuments: []*apigateway.ApigatewayApiConfigOpenApiDocument{{
			Document: &apigateway.ApigatewayApiConfigFile{Path: "openapi.json", Contents: ssrfDoc},
		}},
	}).ApiConfigId("ssrf").Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("generated API Gateway SSRF rejection: %w", statusErr)
	}
	_, err = c.gateway.Projects.Locations.Apis.Configs.Get(apiName + "/configs/ssrf").Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 404, "NOT_FOUND"); statusErr != nil {
		return fmt.Errorf("generated API Gateway SSRF no-side-effect: %w", statusErr)
	}
	if _, err := c.gateway.Projects.Locations.Gateways.Create(parent,
		&apigateway.ApigatewayGateway{ApiConfig: apiConfigName}).GatewayId(cfg.gatewayID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated API Gateway gateway create: %w", err)
	}
	if err := proveMiniSkyLocalGatewayProxy(ctx); err != nil {
		return err
	}

	namespaceName := parent + "/namespaces/" + cfg.namespaceID
	serviceName := namespaceName + "/services/" + cfg.serviceID
	directoryEndpointName := serviceName + "/endpoints/" + cfg.endpointID
	if _, err := c.directory.Projects.Locations.Namespaces.Create(parent, &servicedirectory.Namespace{}).
		NamespaceId(cfg.namespaceID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated namespace create: %w", err)
	}
	if _, err := c.directory.Projects.Locations.Namespaces.Services.Create(namespaceName, &servicedirectory.Service{}).
		ServiceId(cfg.serviceID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated service create: %w", err)
	}
	if _, err := c.directory.Projects.Locations.Namespaces.Services.Endpoints.Create(serviceName,
		&servicedirectory.Endpoint{Address: "127.0.0.1", Port: 8080}).EndpointId(cfg.endpointID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated endpoint create: %w", err)
	}
	_, err = c.directory.Projects.Locations.Namespaces.Services.Resolve(serviceName,
		&servicedirectory.ResolveServiceRequest{}).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("public-gateway Service Directory resolve boundary: %w", statusErr)
	}
	resolved, err := resolveMiniSkyLocalDirectory(ctx, serviceName)
	if err != nil || resolved.Service == nil || len(resolved.Service.Endpoints) != 1 {
		return fmt.Errorf("MiniSky-local generated Service Directory resolve: %w", err)
	}
	otherNamespaces, err := c.directory.Projects.Locations.Namespaces.List(
		"projects/" + cfg.project + "-other/locations/" + cfg.location).Context(ctx).Do()
	if err != nil || namespaceCount(otherNamespaces) != 0 {
		return fmt.Errorf("Service Directory project isolation namespaces=%d: %w", namespaceCount(otherNamespaces), err)
	}

	configID := "phase22-config"
	if _, err := c.management.Services.Configs.Create(cfg.endpointsService,
		&servicemanagement.Service{Name: cfg.endpointsService, Id: configID, Title: "Phase 22"}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated Service Management config create: %w", err)
	}
	rolloutID := "phase22-rollout"
	if _, err := c.management.Services.Rollouts.Create(cfg.endpointsService, &servicemanagement.Rollout{
		RolloutId: rolloutID, ServiceName: cfg.endpointsService,
		TrafficPercentStrategy: &servicemanagement.TrafficPercentStrategy{Percentages: map[string]float64{configID: 100}},
	}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated Service Management rollout: %w", err)
	}
	controlOperation := &servicecontrol.Operation{OperationId: "phase22-operation", OperationName: "ListBooks", ConsumerId: "project:" + cfg.project}
	_, err = c.control.Services.Check(cfg.endpointsService,
		&servicecontrol.CheckRequest{Operation: controlOperation}).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("generated Service Control body-shape boundary: %w", statusErr)
	}
	checked, err := localControlCheck(ctx, cfg, controlOperation)
	if err != nil || checked.ServiceConfigId != configID || len(checked.CheckErrors) != 0 {
		return fmt.Errorf("MiniSky-local Service Control check: %w", err)
	}
	_, err = c.control.Services.Report(cfg.endpointsService,
		&servicecontrol.ReportRequest{Operations: []*servicecontrol.Operation{controlOperation}}).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return fmt.Errorf("generated Service Control report body-shape boundary: %w", statusErr)
	}
	reported, err := localControlReport(ctx, cfg, controlOperation)
	if err != nil || reported.ServiceConfigId != configID || len(reported.ReportErrors) != 0 {
		return fmt.Errorf("MiniSky-local Service Control report: %w", err)
	}
	if _, err := c.control.Services.AllocateQuota(cfg.endpointsService,
		&servicecontrol.AllocateQuotaRequest{}).Context(ctx).Do(); expectGoogleStatus(err, 501, "UNIMPLEMENTED") != nil {
		return fmt.Errorf("Service Control unsupported quota boundary: %w", err)
	}

	pipelineName := parent + "/deliveryPipelines/" + cfg.pipelineID
	releaseName := pipelineName + "/releases/" + cfg.releaseID
	if _, err := c.deploy.Projects.Locations.DeliveryPipelines.Create(parent,
		&clouddeploy.DeliveryPipeline{}).DeliveryPipelineId(cfg.pipelineID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated Cloud Deploy pipeline create: %w", err)
	}
	if _, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Create(pipelineName,
		&clouddeploy.Release{}).ReleaseId(cfg.releaseID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated Cloud Deploy release create: %w", err)
	}
	allowedRollout, deniedRollout, err := proveMiniSkyLocalRollouts(ctx, c, cfg, releaseName)
	if err != nil {
		return err
	}
	record := evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location, TraceID: traceID, ErrorMessage: errorMessage,
		APIName: apiName, APIConfigName: apiConfigName, GatewayName: gatewayName,
		NamespaceName: namespaceName, DirectoryService: serviceName, DirectoryEndpoint: directoryEndpointName,
		EndpointsService: cfg.endpointsService, EndpointsConfig: configID, EndpointsRollout: rolloutID,
		PipelineName: pipelineName, ReleaseName: releaseName, AllowedRollout: allowedRollout, DeniedRollout: deniedRollout,
	}
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		return err
	}
	fmt.Println("Phase 21-22 generated clients proved bounded telemetry, hierarchy, control and deployment slices")
	return nil
}

// proveMiniSkyLocalGatewayProxy is deliberately not labeled generated SDK:
// API Gateway exposes the executable proxy as an in-process MiniSky capability,
// not as an official Google API method or public daemon route.
func proveMiniSkyLocalGatewayProxy(ctx context.Context) error {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/phase22" {
			http.Error(w, "bad path", 400)
			return
		}
		_, _ = io.WriteString(w, "proxied")
	}))
	defer backend.Close()
	stateDir, err := os.MkdirTemp("", "minisky-phase22-local-proxy-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stateDir)
	oldState, hadState := os.LookupEnv("MINISKY_STATE_DIR")
	oldProfile, hadProfile := os.LookupEnv("MINISKY_PROFILE")
	_ = os.Setenv("MINISKY_STATE_DIR", stateDir)
	_ = os.Setenv("MINISKY_PROFILE", "phase22-local-proxy")
	defer func() {
		if hadState {
			_ = os.Setenv("MINISKY_STATE_DIR", oldState)
		} else {
			_ = os.Unsetenv("MINISKY_STATE_DIR")
		}
		if hadProfile {
			_ = os.Setenv("MINISKY_PROFILE", oldProfile)
		} else {
			_ = os.Unsetenv("MINISKY_PROFILE")
		}
	}()
	api := localgateway.NewAPI(nil)
	server := httptest.NewServer(api)
	defer server.Close()
	client, err := apigateway.NewService(ctx, option.WithoutAuthentication(), option.WithEndpoint(server.URL+"/"))
	if err != nil {
		return err
	}
	apiName := "projects/local-proxy/locations/global/apis/api"
	if _, err := client.Projects.Locations.Apis.Create("projects/local-proxy/locations/global", &apigateway.ApigatewayApi{}).
		ApiId("api").Context(ctx).Do(); err != nil {
		return fmt.Errorf("local proxy API create: %w", err)
	}
	document := base64.StdEncoding.EncodeToString([]byte(`{"swagger":"2.0","x-google-backend":{"address":"` + backend.URL + `"}}`))
	configName := apiName + "/configs/config"
	if _, err := client.Projects.Locations.Apis.Configs.Create(apiName, &apigateway.ApigatewayApiConfig{
		OpenapiDocuments: []*apigateway.ApigatewayApiConfigOpenApiDocument{{Document: &apigateway.ApigatewayApiConfigFile{Contents: document}}},
	}).ApiConfigId("config").Context(ctx).Do(); err != nil {
		return fmt.Errorf("local proxy config create: %w", err)
	}
	gatewayName := "projects/local-proxy/locations/us-central1/gateways/gateway"
	if _, err := client.Projects.Locations.Gateways.Create("projects/local-proxy/locations/us-central1",
		&apigateway.ApigatewayGateway{ApiConfig: configName}).GatewayId("gateway").Context(ctx).Do(); err != nil {
		return fmt.Errorf("local proxy gateway create: %w", err)
	}
	handler, err := api.GatewayProxy(gatewayName)
	if err != nil {
		return err
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/phase22", nil))
	if recorder.Code != 200 || recorder.Body.String() != "proxied" {
		return fmt.Errorf("MiniSky-local API Gateway proxy status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	return nil
}

// proveMiniSkyLocalRollouts uses bounded raw HTTP only for MiniSky extension
// fields (image and localTarget) that do not exist in the official generated
// Cloud Deploy Rollout type. All surrounding control-plane calls use clients.
func proveMiniSkyLocalRollouts(ctx context.Context, c *clients, cfg config, releaseName string) (string, string, error) {
	var deliveries atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		deliveries.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	policyName := "projects/" + cfg.project + "/policy"
	setPolicy := func(mode string) error {
		_, err := c.binauthz.Projects.UpdatePolicy(policyName, &binaryauthorization.Policy{
			Name: policyName, DefaultAdmissionRule: &binaryauthorization.AdmissionRule{EvaluationMode: mode},
		}).Context(ctx).Do()
		return err
	}
	if err := setPolicy("ALWAYS_ALLOW"); err != nil {
		return "", "", fmt.Errorf("generated Binary Authorization allow policy: %w", err)
	}
	allowedName := releaseName + "/rollouts/allowed"
	allowedOp, err := localRollout(ctx, cfg, releaseName, "allowed", map[string]any{
		"targetId": "local", "image": "example.test/app@sha256:allowed", "localTarget": target.URL,
	})
	if err != nil {
		return "", "", fmt.Errorf("MiniSky-local allowed rollout: %w", err)
	}
	if err := waitDeployOperation(ctx, c.deploy, allowedOp.Name, false); err != nil {
		return "", "", err
	}
	allowed, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Rollouts.Get(allowedName).Context(ctx).Do()
	if err != nil || allowed.State != "SUCCEEDED" || deliveries.Load() != 1 {
		return "", "", fmt.Errorf("allowed rollout state=%q deliveries=%d: %w", rolloutState(allowed), deliveries.Load(), err)
	}
	if err := setPolicy("DISALLOWED"); err != nil {
		return "", "", fmt.Errorf("generated Binary Authorization deny policy: %w", err)
	}
	deniedName := releaseName + "/rollouts/denied"
	deniedOp, err := localRollout(ctx, cfg, releaseName, "denied", map[string]any{
		"targetId": "local", "image": "example.test/app@sha256:denied", "localTarget": target.URL,
	})
	if err != nil {
		return "", "", fmt.Errorf("MiniSky-local denied rollout create: %w", err)
	}
	if err := waitDeployOperation(ctx, c.deploy, deniedOp.Name, true); err != nil {
		return "", "", err
	}
	denied, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Rollouts.Get(deniedName).Context(ctx).Do()
	if err != nil || denied.State != "FAILED" || deliveries.Load() != 1 {
		return "", "", fmt.Errorf("denied rollout state=%q deliveries=%d: %w", rolloutState(denied), deliveries.Load(), err)
	}
	for label, body := range map[string]map[string]any{
		"strategy":         {"targetId": "local", "image": "example.test/app@sha256:x", "localTarget": target.URL, "strategy": map[string]any{"canary": true}},
		"external-hosting": {"targetId": "production"},
	} {
		_, err := localRollout(ctx, cfg, releaseName, label, body)
		if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
			return "", "", fmt.Errorf("MiniSky-local unsupported %s: %w", label, statusErr)
		}
	}
	_, err = localRollout(ctx, cfg, releaseName, "ssrf", map[string]any{
		"targetId": "local", "image": "example.test/app@sha256:ssrf", "localTarget": "http://169.254.169.254/latest",
	})
	if statusErr := expectGoogleStatus(err, 400, "INVALID_ARGUMENT"); statusErr != nil {
		return "", "", fmt.Errorf("MiniSky-local rollout SSRF rejection: %w", statusErr)
	}
	_, err = c.deploy.Projects.Locations.DeliveryPipelines.Releases.Rollouts.Get(
		releaseName + "/rollouts/ssrf").Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 404, "NOT_FOUND"); statusErr != nil {
		return "", "", fmt.Errorf("MiniSky-local rollout SSRF no-side-effect: %w", statusErr)
	}
	return allowedName, deniedName, nil
}

func localRollout(ctx context.Context, cfg config, releaseName, id string, body map[string]any) (*clouddeploy.Operation, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	requestURL := cfg.endpoint + "/_minisky/clouddeploy.googleapis.com/v1/" + releaseName + "/rollouts?rolloutId=" + url.QueryEscape(id)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		return nil, googleError(response.StatusCode, payload)
	}
	var operation clouddeploy.Operation
	if err := json.Unmarshal(payload, &operation); err != nil {
		return nil, err
	}
	return &operation, nil
}

func verifyRestart(ctx context.Context, c *clients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	traceSpans, err := localTraceGet(ctx, cfg, cfg.project, record.TraceID)
	if err != nil || traceSpans != 1 {
		return fmt.Errorf("restart trace: %w", err)
	}
	groups, err := c.errors.Projects.GroupStats.List("projects/" + cfg.project).Context(ctx).Do()
	if err != nil || len(groups.ErrorGroupStats) != 1 {
		return fmt.Errorf("restart error groups: %w", err)
	}
	for name, call := range map[string]func() error{
		"API": func() error {
			_, err := c.gateway.Projects.Locations.Apis.Get(record.APIName).Context(ctx).Do()
			return err
		},
		"API config": func() error {
			_, err := c.gateway.Projects.Locations.Apis.Configs.Get(record.APIConfigName).Context(ctx).Do()
			return err
		},
		"gateway": func() error {
			_, err := c.gateway.Projects.Locations.Gateways.Get(record.GatewayName).Context(ctx).Do()
			return err
		},
		"namespace": func() error {
			_, err := c.directory.Projects.Locations.Namespaces.Get(record.NamespaceName).Context(ctx).Do()
			return err
		},
		"directory endpoint": func() error {
			_, err := c.directory.Projects.Locations.Namespaces.Services.Endpoints.Get(record.DirectoryEndpoint).Context(ctx).Do()
			return err
		},
		"pipeline": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Get(record.PipelineName).Context(ctx).Do()
			return err
		},
		"release": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Get(record.ReleaseName).Context(ctx).Do()
			return err
		},
		"allowed rollout": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Rollouts.Get(record.AllowedRollout).Context(ctx).Do()
			return err
		},
		"denied rollout": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Rollouts.Get(record.DeniedRollout).Context(ctx).Do()
			return err
		},
	} {
		if err := call(); err != nil {
			return fmt.Errorf("restart %s: %w", name, err)
		}
	}
	_, err = c.directory.Projects.Locations.Namespaces.Services.Resolve(record.DirectoryService,
		&servicedirectory.ResolveServiceRequest{}).Context(ctx).Do()
	if statusErr := expectGoogleStatus(err, 501, "UNIMPLEMENTED"); statusErr != nil {
		return fmt.Errorf("restart public-gateway Service Directory resolve boundary: %w", statusErr)
	}
	resolved, err := resolveMiniSkyLocalDirectory(ctx, record.DirectoryService)
	if err != nil || resolved.Service == nil || len(resolved.Service.Endpoints) != 1 {
		return fmt.Errorf("restart directory resolve: %w", err)
	}
	config, err := c.management.Services.Configs.Get(record.EndpointsService, record.EndpointsConfig).Context(ctx).Do()
	if err != nil || config.Id != record.EndpointsConfig {
		return fmt.Errorf("restart Endpoints config: %w", err)
	}
	rollout, err := c.management.Services.Rollouts.Get(record.EndpointsService, record.EndpointsRollout).Context(ctx).Do()
	if err != nil || rollout.Status != "SUCCESS" {
		return fmt.Errorf("restart Endpoints rollout: %w", err)
	}
	fmt.Println("Phase 21-22 generated-client restart verification passed")
	return nil
}

func deleteAndVerify(ctx context.Context, c *clients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	// Cloud Deploy does not provide generated delete methods for releases or
	// rollouts. These child-first DELETEs are explicitly MiniSky-local cleanup.
	for _, resource := range []string{record.DeniedRollout, record.AllowedRollout, record.ReleaseName} {
		if err := localDelete(ctx, cfg, "clouddeploy.googleapis.com", resource); err != nil {
			return err
		}
	}
	if _, err := c.deploy.Projects.Locations.DeliveryPipelines.Delete(record.PipelineName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("generated pipeline delete: %w", err)
	}
	if _, err := c.directory.Projects.Locations.Namespaces.Services.Endpoints.Delete(record.DirectoryEndpoint).Context(ctx).Do(); err != nil {
		return err
	}
	if _, err := c.directory.Projects.Locations.Namespaces.Services.Delete(record.DirectoryService).Context(ctx).Do(); err != nil {
		return err
	}
	if _, err := c.directory.Projects.Locations.Namespaces.Delete(record.NamespaceName).Context(ctx).Do(); err != nil {
		return err
	}
	if _, err := c.gateway.Projects.Locations.Gateways.Delete(record.GatewayName).Context(ctx).Do(); err != nil {
		return err
	}
	if _, err := c.gateway.Projects.Locations.Apis.Configs.Delete(record.APIConfigName).Context(ctx).Do(); err != nil {
		return err
	}
	if _, err := c.gateway.Projects.Locations.Apis.Delete(record.APIName).Context(ctx).Do(); err != nil {
		return err
	}
	checks := map[string]func() error{
		"API": func() error {
			_, err := c.gateway.Projects.Locations.Apis.Get(record.APIName).Context(ctx).Do()
			return err
		},
		"API config": func() error {
			_, err := c.gateway.Projects.Locations.Apis.Configs.Get(record.APIConfigName).Context(ctx).Do()
			return err
		},
		"gateway": func() error {
			_, err := c.gateway.Projects.Locations.Gateways.Get(record.GatewayName).Context(ctx).Do()
			return err
		},
		"namespace": func() error {
			_, err := c.directory.Projects.Locations.Namespaces.Get(record.NamespaceName).Context(ctx).Do()
			return err
		},
		"service": func() error {
			_, err := c.directory.Projects.Locations.Namespaces.Services.Get(record.DirectoryService).Context(ctx).Do()
			return err
		},
		"endpoint": func() error {
			_, err := c.directory.Projects.Locations.Namespaces.Services.Endpoints.Get(record.DirectoryEndpoint).Context(ctx).Do()
			return err
		},
		"pipeline": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Get(record.PipelineName).Context(ctx).Do()
			return err
		},
		"release": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Get(record.ReleaseName).Context(ctx).Do()
			return err
		},
		"allowed rollout": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Rollouts.Get(record.AllowedRollout).Context(ctx).Do()
			return err
		},
		"denied rollout": func() error {
			_, err := c.deploy.Projects.Locations.DeliveryPipelines.Releases.Rollouts.Get(record.DeniedRollout).Context(ctx).Do()
			return err
		},
	}
	for name, call := range checks {
		if err := expectGoogleStatus(call(), 404, "NOT_FOUND"); err != nil {
			return fmt.Errorf("deleted %s: %w", name, err)
		}
	}
	fmt.Println("Phase 21-22 generated deletes, local child cleanup and 404 verification passed")
	return nil
}

func localDelete(ctx context.Context, cfg config, domain, resource string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		cfg.endpoint+"/_minisky/"+domain+"/v1/"+resource, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode >= 300 {
		return googleError(response.StatusCode, payload)
	}
	return nil
}

// resolveMiniSkyLocalDirectory is deliberately separated from public-gateway
// evidence: the gateway currently returns explicit 501 for :resolve before
// dispatch, while the Service Directory shim implements the generated method.
func resolveMiniSkyLocalDirectory(ctx context.Context, serviceName string) (*servicedirectory.ResolveServiceResponse, error) {
	server := httptest.NewServer(localdirectory.NewAPI())
	defer server.Close()
	client, err := servicedirectory.NewService(ctx,
		option.WithoutAuthentication(), option.WithEndpoint(server.URL+"/"))
	if err != nil {
		return nil, err
	}
	return client.Projects.Locations.Namespaces.Services.Resolve(serviceName,
		&servicedirectory.ResolveServiceRequest{}).Context(ctx).Do()
}

// localTraceGet and localTraceList are deliberately MiniSky-local query
// actions. The official v1 generated client cannot decode MiniSky's v2-shaped
// hexadecimal spanId string into its uint64,string TraceSpan field. Ingestion
// remains proven through the official generated Cloud Trace v2 client.
func localTraceGet(ctx context.Context, cfg config, project, id string) (int, error) {
	var response struct {
		TraceID   string            `json:"traceId"`
		ProjectID string            `json:"projectId"`
		Spans     []json.RawMessage `json:"spans"`
	}
	err := localJSONGet(ctx, cfg.endpoint+"/_minisky/cloudtrace.googleapis.com/v1/projects/"+
		url.PathEscape(project)+"/traces/"+url.PathEscape(id), &response)
	if err == nil && (response.TraceID != id || response.ProjectID != project) {
		return 0, errors.New("MiniSky-local trace response scope mismatch")
	}
	return len(response.Spans), err
}

func localTraceList(ctx context.Context, cfg config, project string) (int, error) {
	var response struct {
		Traces        []json.RawMessage `json:"traces"`
		NextPageToken string            `json:"nextPageToken"`
	}
	err := localJSONGet(ctx, cfg.endpoint+"/_minisky/cloudtrace.googleapis.com/v1/projects/"+
		url.PathEscape(project)+"/traces", &response)
	return len(response.Traces), err
}

func localJSONGet(ctx context.Context, requestURL string, destination any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		return googleError(response.StatusCode, payload)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

// localControlCheck and localControlReport are MiniSky-local body adapters.
// Generated Service Control requests correctly carry serviceName in the path,
// but the current shim additionally requires the non-contract JSON field.
func localControlCheck(ctx context.Context, cfg config, operation *servicecontrol.Operation) (*servicecontrol.CheckResponse, error) {
	var response servicecontrol.CheckResponse
	err := localJSONPost(ctx, cfg.endpoint+"/_minisky/servicecontrol.googleapis.com/v1/services/"+
		url.PathEscape(cfg.endpointsService)+":check",
		map[string]any{"serviceName": cfg.endpointsService, "operation": operation}, &response)
	return &response, err
}

func localControlReport(ctx context.Context, cfg config, operation *servicecontrol.Operation) (*servicecontrol.ReportResponse, error) {
	var response servicecontrol.ReportResponse
	err := localJSONPost(ctx, cfg.endpoint+"/_minisky/servicecontrol.googleapis.com/v1/services/"+
		url.PathEscape(cfg.endpointsService)+":report",
		map[string]any{"serviceName": cfg.endpointsService, "operations": []*servicecontrol.Operation{operation}}, &response)
	return &response, err
}

func localJSONPost(ctx context.Context, requestURL string, body, destination any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		return googleError(response.StatusCode, data)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func waitDeployOperation(ctx context.Context, service *clouddeploy.Service, name string, wantError bool) error {
	for {
		op, err := service.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return err
		}
		if op.Done {
			if wantError && op.Error != nil && op.Error.Code == 7 &&
				strings.Contains(op.Error.Message, "Binary Authorization denied") {
				return nil
			}
			if !wantError && op.Error == nil {
				return nil
			}
			return fmt.Errorf("Cloud Deploy operation terminal error=%+v wantError=%t", op.Error, wantError)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func expectGoogleStatus(err error, code int, status string) error {
	if err == nil {
		return fmt.Errorf("expected HTTP %d %s, got success", code, status)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != code {
		return fmt.Errorf("expected googleapi.Error HTTP %d, got %T: %w", code, err, err)
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

func googleError(code int, body []byte) error {
	return &googleapi.Error{Code: code, Body: string(body)}
}

func writeEvidence(path string, record evidence) error {
	if err := validateEvidence(record); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".phase21-22-evidence-*.tmp")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
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
	return os.Rename(name, path)
}

func readEvidence(path string, cfg config) (evidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return evidence{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxEvidenceBytes+1))
	if err != nil {
		return evidence{}, err
	}
	if len(data) > maxEvidenceBytes {
		return evidence{}, errors.New("Phase 21-22 evidence exceeds size limit")
	}
	var record evidence
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return evidence{}, err
	}
	if err := validateEvidence(record); err != nil {
		return evidence{}, err
	}
	if record.Project != cfg.project || record.Location != cfg.location || record.EndpointsService != cfg.endpointsService {
		return evidence{}, errors.New("evidence identifiers do not match smoke configuration")
	}
	return record, nil
}

func validateEvidence(record evidence) error {
	if record.Version != evidenceVersion || record.TraceID != traceID {
		return errors.New("invalid Phase 21-22 evidence version or trace")
	}
	values := []string{record.Project, record.Location, record.ErrorMessage, record.APIName, record.APIConfigName,
		record.GatewayName, record.NamespaceName, record.DirectoryService, record.DirectoryEndpoint,
		record.EndpointsService, record.EndpointsConfig, record.EndpointsRollout, record.PipelineName,
		record.ReleaseName, record.AllowedRollout, record.DeniedRollout}
	for _, value := range values {
		if value == "" {
			return errors.New("Phase 21-22 evidence is incomplete")
		}
	}
	if !strings.HasPrefix(record.APIConfigName, record.APIName+"/configs/") ||
		!strings.HasPrefix(record.DirectoryService, record.NamespaceName+"/services/") ||
		!strings.HasPrefix(record.DirectoryEndpoint, record.DirectoryService+"/endpoints/") ||
		!strings.HasPrefix(record.ReleaseName, record.PipelineName+"/releases/") ||
		!strings.HasPrefix(record.AllowedRollout, record.ReleaseName+"/rollouts/") ||
		!strings.HasPrefix(record.DeniedRollout, record.ReleaseName+"/rollouts/") {
		return errors.New("Phase 21-22 evidence hierarchy is inconsistent")
	}
	return nil
}

func locationParent(cfg config) string {
	return "projects/" + cfg.project + "/locations/" + cfg.location
}
func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
func groupCount(groups *errorreporting.ListGroupStatsResponse) int {
	if groups == nil {
		return 0
	}
	return len(groups.ErrorGroupStats)
}
func namespaceCount(namespaces *servicedirectory.ListNamespacesResponse) int {
	if namespaces == nil {
		return 0
	}
	return len(namespaces.Namespaces)
}
func rolloutState(rollout *clouddeploy.Rollout) string {
	if rollout == nil {
		return ""
	}
	return rollout.State
}
