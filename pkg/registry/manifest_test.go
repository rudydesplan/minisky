package registry_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/router"
	_ "minisky/pkg/shims"
	"minisky/pkg/state"
)

func TestRegisteredServicesHaveManifestAndDocumentation(t *testing.T) {
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 71 {
		t.Fatalf("registered service count = %d, want exactly 71", len(services))
	}

	documentation, err := os.ReadFile("../../docs/service-compatibility.md")
	if err != nil {
		t.Fatal(err)
	}
	if rows := strings.Count(string(documentation), "| `"); rows != len(services) {
		t.Fatalf("machine-readable documentation rows = %d, want %d", rows, len(services))
	}
	for _, service := range services {
		service := service
		t.Run(service.Domain, func(t *testing.T) {
			switch service.Support {
			case registry.SupportImplemented:
				if service.Fidelity == "" {
					t.Error("implemented service fidelity tier is empty")
				}
				switch service.Fidelity {
				case registry.FidelityHigh, registry.FidelityStandard, registry.FidelityPassthrough:
				default:
					t.Errorf("unknown fidelity tier %q", service.Fidelity)
				}
			case registry.SupportDeferred:
				if service.Fidelity != "" {
					t.Errorf("deferred service claims fidelity tier %q", service.Fidelity)
				}
				if service.SupportReason == "" {
					t.Error("deferred service has no reason")
				}
			case registry.SupportExperimental:
				if service.Fidelity != "" {
					t.Errorf("experimental service claims fidelity tier %q", service.Fidelity)
				}
				if service.SupportReason == "" {
					t.Error("experimental service has no opt-in/evidence reason")
				}
			default:
				t.Errorf("unknown support status %q", service.Support)
			}
			if service.Persistence == "" {
				t.Error("persistence category is empty")
			}
			switch service.Persistence {
			case registry.PersistenceMemory,
				registry.PersistenceFile,
				registry.PersistenceDocker,
				registry.PersistenceHybrid,
				registry.PersistenceStatic:
			default:
				t.Errorf("unknown persistence category %q", service.Persistence)
			}
			domainReference := "`" + service.Domain + "`"
			if count := strings.Count(string(documentation), domainReference); count != 1 {
				t.Errorf("docs/service-compatibility.md lists %q %d times, want exactly once", service.Domain, count)
			}
			expectedRow := ""
			if service.Support != registry.SupportImplemented {
				expectedRow = fmt.Sprintf(
					"| `%s` | %s | %s |",
					service.Domain,
					service.Support,
					service.Persistence,
				)
			} else {
				expectedRow = fmt.Sprintf(
					"| `%s` | %s | %s |",
					service.Domain,
					service.Fidelity,
					service.Persistence,
				)
			}
			if !strings.Contains(string(documentation), expectedRow) {
				t.Errorf("docs/service-compatibility.md does not contain manifest row %q", expectedRow)
			}
			if !service.ProbeUnsupported && service.Persistence != registry.PersistenceDocker {
				t.Errorf("contract probe skipped for non-Docker persistence %q", service.Persistence)
			}
			if !service.ProbeUnsupported && service.BackendContract == "" {
				t.Error("Docker-gated service has no backend contract rationale")
			}
		})
	}
}

func TestPhase18To25DomainsAreExperimental(t *testing.T) {
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	experimental := make(map[string]registry.Service)
	for _, service := range services {
		if service.Support == registry.SupportExperimental {
			experimental[service.Domain] = service
		}
	}
	expected := []string{
		"accesscontextmanager.googleapis.com",
		"binaryauthorization.googleapis.com",
		"alloydb.googleapis.com",
		"apigateway.googleapis.com",
		"batch.googleapis.com",
		"cloudasset.googleapis.com",
		"clouddeploy.googleapis.com",
		"dialogflow.googleapis.com",
		"clouderrorreporting.googleapis.com",
		"cloudprofiler.googleapis.com",
		"cloudtrace.googleapis.com",
		"composer.googleapis.com",
		"dataflow.googleapis.com",
		"dataform.googleapis.com",
		"dlp.googleapis.com",
		"documentai.googleapis.com",
		"eventarc.googleapis.com",
		"file.googleapis.com",
		"identityplatform.googleapis.com",
		"language.googleapis.com",
		"managedkafka.googleapis.com",
		"networksecurity.googleapis.com",
		"networkservices.googleapis.com",
		"orgpolicy.googleapis.com",
		"privateca.googleapis.com",
		"pubsublite.googleapis.com",
		"servicecontrol.googleapis.com",
		"servicemanagement.googleapis.com",
		"servicedirectory.googleapis.com",
		"speech.googleapis.com",
		"storagetransfer.googleapis.com",
		"texttospeech.googleapis.com",
		"translate.googleapis.com",
		"vision.googleapis.com",
		"workflows.googleapis.com",
		"workflowexecutions.googleapis.com",
	}
	if len(experimental) != len(expected) {
		t.Fatalf("experimental domain count = %d, want %d: %#v", len(experimental), len(expected), experimental)
	}
	for _, domain := range expected {
		service, ok := experimental[domain]
		if !ok {
			t.Errorf("%s is not experimental", domain)
			continue
		}
		if service.Fidelity != "" {
			t.Errorf("%s fidelity = %q, want no promoted fidelity", domain, service.Fidelity)
		}
	}
}

func TestExperimentalHandlersRequireExplicitRuntimeOptIn(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "experimental-runtime-gate")
	t.Setenv(registry.ExperimentalServicesEnv, "")

	const domain = "batch.googleapis.com"
	const path = "/v1/projects/demo/locations/us-central1/jobs"
	wantMessage := "MiniSky: " + domain + " is experimental and disabled; set " +
		registry.ExperimentalServicesEnv +
		"=1 to opt in; promotion evidence is incomplete"

	assertBlocked := func(t *testing.T, handler http.Handler) {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want 501; body: %s", response.Code, response.Body.String())
		}
		var envelope struct {
			Error struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
				Status  string `json:"status"`
			} `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("response is not JSON: %v", err)
		}
		if envelope.Error.Code != http.StatusNotImplemented ||
			envelope.Error.Status != "UNIMPLEMENTED" ||
			envelope.Error.Message != wantMessage {
			t.Fatalf("unexpected experimental gate response: %+v", envelope.Error)
		}
	}

	booted, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	assertBlocked(t, booted[domain])
	coreResponse := httptest.NewRecorder()
	booted["bigquery.googleapis.com"].ServeHTTP(
		coreResponse,
		httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/demo/datasets", nil),
	)
	if coreResponse.Code != http.StatusOK {
		t.Fatalf("default experimental gate changed core BigQuery status = %d; body: %s",
			coreResponse.Code, coreResponse.Body.String())
	}
	contracts, err := registry.ContractHandlers(orchestrator.NewOperationManager(), nil)
	if err != nil {
		t.Fatal(err)
	}
	assertBlocked(t, contracts[domain])

	t.Setenv(registry.ExperimentalServicesEnv, "1")
	booted, _ = registry.BootAll(orchestrator.NewOperationManager(), nil)
	response := httptest.NewRecorder()
	booted[domain].ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("opted-in Batch status = %d, want 200; body: %s", response.Code, response.Body.String())
	}
	contracts, err = registry.ContractHandlers(orchestrator.NewOperationManager(), nil)
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	contracts[domain].ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("opted-in Batch contract status = %d, want 200; body: %s", response.Code, response.Body.String())
	}
}

func TestEveryExperimentalDomainUsesDefaultOffRuntimeGate(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "all-experimental-runtime-gates")
	t.Setenv(registry.ExperimentalServicesEnv, "")

	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	disabled, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	t.Setenv(registry.ExperimentalServicesEnv, "1")
	enabled, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)

	for _, service := range services {
		if service.Support != registry.SupportExperimental {
			continue
		}
		if !registry.IsExperimentalDisabled(disabled[service.Domain]) {
			t.Errorf("%s is not default-gated", service.Domain)
		}
		if registry.IsExperimentalDisabled(enabled[service.Domain]) {
			t.Errorf("%s remains gated after explicit opt-in", service.Domain)
		}
	}
}

func TestRelatedAliasesShareOneHandlerInstance(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "shared-alias-handlers")
	t.Setenv(registry.ExperimentalServicesEnv, "1")

	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	for _, domains := range [][]string{
		{
			"pubsublite.googleapis.com",
		},
		{
			"servicemanagement.googleapis.com",
			"servicecontrol.googleapis.com",
		},
	} {
		first := handlers[domains[0]]
		if first == nil {
			t.Fatalf("missing handler for %s", domains[0])
		}
		for _, domain := range domains[1:] {
			if handlers[domain] != first {
				t.Errorf("%s and %s do not share one handler instance", domains[0], domain)
			}
		}
	}
}

func TestAIPlatformFactoryPreservesPredictionAndControlPlane(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "merged-aiplatform")
	t.Setenv(registry.ExperimentalServicesEnv, "1")

	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	handler := handlers["aiplatform.googleapis.com"]
	if handler == nil {
		t.Fatal("missing aiplatform handler")
	}

	predict := httptest.NewRecorder()
	handler.ServeHTTP(predict, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/locations/us/endpoints/endpoint-1:predict",
		strings.NewReader(`{"instances":[{"value":1}]}`),
	))
	if predict.Code != http.StatusOK || !strings.Contains(predict.Body.String(), `"predictions"`) {
		t.Fatalf("prediction status=%d body=%s", predict.Code, predict.Body.String())
	}

	index := httptest.NewRecorder()
	handler.ServeHTTP(index, httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/demo/locations/us/indexes",
		strings.NewReader(`{"displayName":"demo-index"}`),
	))
	if index.Code != http.StatusOK || !strings.Contains(index.Body.String(), `"done":true`) {
		t.Fatalf("control-plane status=%d body=%s", index.Code, index.Body.String())
	}
}

func TestDuplicateDomainRegistrationPanicsWithoutReplacingFactory(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("duplicate registration did not panic")
		}
	}()
	registry.Register("aiplatform.googleapis.com", func(*registry.Context) http.Handler {
		t.Fatal("duplicate factory must never be installed")
		return nil
	})
}

func TestBootAllContainsEveryNonLazyManifestDomain(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "boot-manifest-coherence")
	t.Setenv(registry.ExperimentalServicesEnv, "")
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	handlers, lazy := registry.BootAll(orchestrator.NewOperationManager(), nil)
	lazySet := make(map[string]bool, len(lazy))
	for _, domain := range lazy {
		lazySet[domain] = true
	}
	for _, service := range services {
		_, handlerExists := handlers[service.Domain]
		if handlerExists == lazySet[service.Domain] {
			t.Errorf("%s handlerExists=%v lazy=%v, want exactly one runtime registration",
				service.Domain, handlerExists, lazySet[service.Domain])
		}
	}
	if len(handlers)+len(lazy) != 71 {
		t.Fatalf("BootAll domains = %d handlers + %d lazy = %d, want 71",
			len(handlers), len(lazy), len(handlers)+len(lazy))
	}
}

func TestManifestTruthForPersistedAndDeferredServices(t *testing.T) {
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}

	byDomain := make(map[string]registry.Service, len(services))
	for _, service := range services {
		byDomain[service.Domain] = service
	}

	expected := map[string]struct {
		fidelity    registry.FidelityTier
		persistence registry.PersistenceCategory
	}{
		"artifactregistry.googleapis.com": {registry.FidelityStandard, registry.PersistenceFile},
		"bigtableadmin.googleapis.com":    {registry.FidelityStandard, registry.PersistenceFile},
	}
	for domain, want := range expected {
		service, ok := byDomain[domain]
		if !ok {
			t.Fatalf("manifest does not contain %q", domain)
		}
		if service.Fidelity != want.fidelity || service.Persistence != want.persistence {
			t.Errorf(
				"%s manifest = %s/%s, want %s/%s",
				domain,
				service.Fidelity,
				service.Persistence,
				want.fidelity,
				want.persistence,
			)
		}
	}
}

func TestRunnableDockerServicesCannotClaimFileOnlyPersistence(t *testing.T) {
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}

	hybridDomains := map[string]bool{
		"alloydb.googleapis.com":      false,
		"batch.googleapis.com":        false,
		"composer.googleapis.com":     false,
		"managedkafka.googleapis.com": false,
		"redis.googleapis.com":        false,
	}
	for _, service := range services {
		if _, expected := hybridDomains[service.Domain]; expected {
			hybridDomains[service.Domain] = true
			if service.Persistence != registry.PersistenceHybrid {
				t.Errorf("%s persistence = %q, want %q for metadata plus an owned backend",
					service.Domain, service.Persistence, registry.PersistenceHybrid)
			}
		}
		if service.BackendContract == "" {
			continue
		}
		switch service.Persistence {
		case registry.PersistenceDocker, registry.PersistenceHybrid:
		default:
			t.Errorf(
				"%s has a runnable Docker contract but persistence = %q",
				service.Domain,
				service.Persistence,
			)
		}
	}
	for domain, found := range hybridDomains {
		if !found {
			t.Errorf("missing hybrid service %s", domain)
		}
	}
}

func TestExecutableDockerOperationContractsAreExplicit(t *testing.T) {
	expected := map[string][]registry.DockerOperation{
		"alloydb.googleapis.com": {
			{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/clusters/*/instances", RequestBody: registry.DockerRequestBodyAlloyDBPrimary},
			{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/clusters/*/instances/*"},
		},
		"batch.googleapis.com": {
			{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/jobs", RequestBody: registry.DockerRequestBodyBatchRunnable},
		},
		"composer.googleapis.com": {
			{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/environments"},
			{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/environments/*"},
		},
		"managedkafka.googleapis.com": {
			{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/clusters"},
			{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/clusters/*"},
		},
		"redis.googleapis.com": {
			{HTTPMethod: http.MethodPost, PathGlob: "/v1/projects/*/locations/*/instances"},
			{HTTPMethod: http.MethodDelete, PathGlob: "/v1/projects/*/locations/*/instances/*"},
		},
	}

	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, service := range services {
		want, executable := expected[service.Domain]
		if !executable {
			if len(service.DockerOperations) != 0 {
				t.Errorf("%s unexpectedly declares Docker operations: %#v", service.Domain, service.DockerOperations)
			}
			continue
		}
		seen[service.Domain] = true
		if len(service.DockerOperations) != len(want) {
			t.Errorf("%s Docker operation count = %d, want %d", service.Domain, len(service.DockerOperations), len(want))
			continue
		}
		for index := range want {
			if service.DockerOperations[index] != want[index] {
				t.Errorf("%s Docker operation %d = %#v, want %#v",
					service.Domain, index, service.DockerOperations[index], want[index])
			}
		}
	}
	for domain := range expected {
		if !seen[domain] {
			t.Errorf("missing executable Docker contract for %s", domain)
		}
	}
}

func TestChangedDurableEntriesRegisterImportValidators(t *testing.T) {
	entries := []string{
		"appengine/metadata",
		"artifactregistry/metadata",
		"bigtable/metadata",
		"cloudsql/metadata",
		"cloudtasks/metadata",
		"compute/metadata",
		"dataproc/metadata",
		"gke/metadata",
		"logging/metadata",
		"scheduler/metadata",
	}
	for _, entry := range entries {
		t.Run(entry, func(t *testing.T) {
			store, err := state.New(t.TempDir(), "schema-validation")
			if err != nil {
				t.Fatal(err)
			}
			snapshot := fmt.Sprintf(
				`{"format":"%s","version":%d,"entries":{%q:"wrong-schema"}}`,
				state.SnapshotFormat,
				state.Version,
				entry,
			)
			if err := store.Import(bytes.NewBufferString(snapshot)); err == nil ||
				!strings.Contains(err.Error(), `invalid schema for state entry "`+entry+`"`) {
				t.Fatalf("Import error = %v, want registered schema rejection", err)
			}
		})
	}
}

func TestMemcachedContractIsExplicitlyDeferred(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "contract-test")

	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	var memcached registry.Service
	for _, service := range services {
		if service.Domain == "memcache.googleapis.com" {
			memcached = service
			break
		}
	}
	if memcached.Domain == "" {
		t.Fatal("manifest does not contain memcache.googleapis.com")
	}
	if memcached.Support != registry.SupportDeferred {
		t.Errorf("support = %q, want %q", memcached.Support, registry.SupportDeferred)
	}
	if memcached.Fidelity != "" {
		t.Errorf("fidelity = %q, want empty for deferred service", memcached.Fidelity)
	}
	if memcached.Persistence != registry.PersistenceStatic {
		t.Errorf("persistence = %q, want %q", memcached.Persistence, registry.PersistenceStatic)
	}

	handlers, err := registry.ContractHandlers(orchestrator.NewOperationManager(), nil)
	if err != nil {
		t.Fatal(err)
	}
	handler, ok := handlers["memcache.googleapis.com"]
	if !ok {
		t.Fatal("no contract handler for memcache.googleapis.com")
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/projects/local-dev-project/locations/us-central1/instances",
		nil,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusNotImplemented, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code    int    `json:"code"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if envelope.Error.Code != http.StatusNotImplemented ||
		envelope.Error.Status != "UNIMPLEMENTED" ||
		!strings.Contains(envelope.Error.Message, "Memcached") {
		t.Fatalf("unexpected deferred Memcached response: %+v", envelope.Error)
	}
}

func TestRegisteredHandlersRejectUnsupportedRouteWithGCPError(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "contract-test")
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := registry.ContractHandlers(orchestrator.NewOperationManager(), nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, service := range services {
		service := service
		t.Run(service.Domain, func(t *testing.T) {
			if !service.ProbeUnsupported {
				t.Skip("manifest marks this service as requiring a Docker backend")
			}
			handler, ok := handlers[service.Domain]
			if !ok {
				t.Fatalf("no contract handler for %q", service.Domain)
			}

			request := httptest.NewRequest(http.MethodGet, registry.UnsupportedContractPath, nil)
			request.Host = service.Domain
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusNotImplemented, response.Body.String())
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", got)
			}
			var envelope struct {
				Error struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
					Status  string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("response is not JSON: %v", err)
			}
			if envelope.Error.Code != http.StatusNotImplemented ||
				envelope.Error.Status != "UNIMPLEMENTED" ||
				!strings.Contains(envelope.Error.Message, service.Domain) {
				t.Fatalf("unexpected GCP error envelope: %+v", envelope.Error)
			}
		})
	}
}

func TestLazyDockerContractsFailColdStartDeterministically(t *testing.T) {
	services, err := registry.Services()
	if err != nil {
		t.Fatal(err)
	}

	for _, service := range services {
		service := service
		if service.ProbeUnsupported {
			continue
		}
		t.Run(service.Domain, func(t *testing.T) {
			proxy := router.NewProxyRouterWithManager(nil)
			proxy.RegisterLazyDocker(service.Domain)

			request := httptest.NewRequest(
				http.MethodGet,
				"http://127.0.0.1:8080/_minisky/"+service.Domain+"/v1/contract",
				nil,
			)
			response := httptest.NewRecorder()
			proxy.ServeHTTP(response, request)

			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusServiceUnavailable, response.Body.String())
			}
			var envelope struct {
				Error struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
					Status  string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("response is not JSON: %v", err)
			}
			if envelope.Error.Code != http.StatusServiceUnavailable ||
				envelope.Error.Status != "UNAVAILABLE" ||
				envelope.Error.Message != "MiniSky: Docker backend unavailable" {
				t.Fatalf("unexpected cold-start error: %+v", envelope.Error)
			}
		})
	}
}
