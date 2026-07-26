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
	if len(services) == 0 {
		t.Fatal("service manifest is empty")
	}

	documentation, err := os.ReadFile("../../docs/service-compatibility.md")
	if err != nil {
		t.Fatal(err)
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
				if service.DeferredReason == "" {
					t.Error("deferred service has no reason")
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
			if service.Support == registry.SupportDeferred {
				expectedRow = fmt.Sprintf(
					"| `%s` | deferred | %s |",
					service.Domain,
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
