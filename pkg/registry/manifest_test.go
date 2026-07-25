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
			if service.Persistence == "" {
				t.Error("persistence category is empty")
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
		})
	}
}

func TestRegisteredHandlersRejectUnsupportedRouteWithGCPError(t *testing.T) {
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
