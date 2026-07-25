package registry_test

import (
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
			if service.Fidelity == "" {
				t.Error("fidelity tier is empty")
			}
			switch service.Fidelity {
			case registry.FidelityHigh, registry.FidelityStandard, registry.FidelityPassthrough:
			default:
				t.Errorf("unknown fidelity tier %q", service.Fidelity)
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
			expectedRow := fmt.Sprintf(
				"| `%s` | %s | %s |",
				service.Domain,
				service.Fidelity,
				service.Persistence,
			)
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
				!strings.Contains(envelope.Error.Message, service.Domain) {
				t.Fatalf("unexpected cold-start error: %+v", envelope.Error)
			}
		})
	}
}
