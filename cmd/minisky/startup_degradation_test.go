package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/orchestrator"
	localsecurity "minisky/pkg/security"
)

type startupDashboardAuthorizer struct {
	enabled bool
	allow   bool
}

func (authorizer *startupDashboardAuthorizer) EnforcementEnabled() bool { return authorizer.enabled }

func (authorizer *startupDashboardAuthorizer) Authorize(_, _ string, _ string) bool {
	return authorizer.allow
}

func (authorizer *startupDashboardAuthorizer) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	if token != "valid-token" || audience != "dashboard" ||
		scope != "https://www.googleapis.com/auth/cloud-platform" {
		return localsecurity.Claims{}, errors.New("invalid token")
	}
	return localsecurity.Claims{Subject: "user:diagnostics@example.com"}, nil
}

func TestDashboardWithoutDockerPreservesStrictDiagnosticsRBAC(t *testing.T) {
	diagnostics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	authorizer := &startupDashboardAuthorizer{enabled: true, allow: true}
	handler := dashboardAPIWithoutDocker(diagnostics, authorizer, "dashboard")

	for _, test := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/diagnostics/requests?project=local-dev-project"},
		{http.MethodGet, "/api/diagnostics/traces?project=local-dev-project"},
		{http.MethodPost, "/api/diagnostics/requests/request-1/replay?project=local-dev-project"},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, "http://localhost"+test.path, nil))
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s status = %d, want 401", test.method, test.path, response.Code)
		}
	}

	authorized := httptest.NewRequest(
		http.MethodGet,
		"http://localhost/api/diagnostics/requests?project=local-dev-project",
		nil,
	)
	authorized.Header.Set("Authorization", "Bearer valid-token")
	authorized.Header.Set("X-MiniSky-Project", "local-dev-project")
	diagnosticResponse := httptest.NewRecorder()
	handler.ServeHTTP(diagnosticResponse, authorized)
	if diagnosticResponse.Code != http.StatusNoContent {
		t.Fatalf("authorized diagnostics status = %d", diagnosticResponse.Code)
	}

	serviceRequest := httptest.NewRequest(http.MethodGet, "http://localhost/api/services", nil)
	serviceRequest.Header.Set("Authorization", "Bearer valid-token")
	serviceRequest.Header.Set("X-MiniSky-Project", "local-dev-project")
	serviceResponse := httptest.NewRecorder()
	handler.ServeHTTP(serviceResponse, serviceRequest)
	if serviceResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("services status = %d", serviceResponse.Code)
	}
}

func TestDockerStartupDegradesOrdinaryInitializationAndNetworkFailures(t *testing.T) {
	manager := &orchestrator.ServiceManager{}
	for _, test := range []struct {
		name string
		deps dockerStartupDependencies
	}{
		{
			name: "initialization",
			deps: dockerStartupDependencies{
				newManager: func() (*orchestrator.ServiceManager, error) {
					return nil, errors.New("docker socket unavailable")
				},
			},
		},
		{
			name: "network",
			deps: dockerStartupDependencies{
				newManager: func() (*orchestrator.ServiceManager, error) { return manager, nil },
				ensureNetwork: func(context.Context, *orchestrator.ServiceManager) error {
					return errors.New("docker network API unavailable")
				},
				teardown: func(context.Context, *orchestrator.ServiceManager) error { return nil },
			},
		},
		{
			name: "reconciliation",
			deps: dockerStartupDependencies{
				newManager:    func() (*orchestrator.ServiceManager, error) { return manager, nil },
				ensureNetwork: func(context.Context, *orchestrator.ServiceManager) error { return nil },
				reconcileBuilds: func(context.Context, *orchestrator.ServiceManager) error {
					return errors.New("docker reconciliation API unavailable")
				},
				teardown: func(context.Context, *orchestrator.ServiceManager) error { return nil },
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := initializeDockerOrchestration(context.Background(), false, test.deps)
			if err != nil {
				t.Fatalf("ordinary Docker failure was fatal: %v", err)
			}
			if got != nil {
				t.Fatalf("manager = %#v, want nil degraded mode", got)
			}
		})
	}
}

func TestDockerStartupPreservesFatalOwnershipFailure(t *testing.T) {
	manager := &orchestrator.ServiceManager{}
	got, err := initializeDockerOrchestration(context.Background(), false, dockerStartupDependencies{
		newManager: func() (*orchestrator.ServiceManager, error) { return manager, nil },
		ensureNetwork: func(context.Context, *orchestrator.ServiceManager) error {
			return fmt.Errorf("unsafe Docker state: %w", orchestrator.ErrDockerOwnershipConflict)
		},
		teardown: func(context.Context, *orchestrator.ServiceManager) error { return nil },
	})
	if !errors.Is(err, orchestrator.ErrDockerOwnershipConflict) {
		t.Fatalf("fatal ownership error = %v", err)
	}
	if got != nil {
		t.Fatalf("manager = %#v, want nil", got)
	}
}

func TestDockerStartupTeardownFailureRemainsFatal(t *testing.T) {
	manager := &orchestrator.ServiceManager{}
	teardownCalled := false
	got, err := initializeDockerOrchestration(context.Background(), false, dockerStartupDependencies{
		newManager: func() (*orchestrator.ServiceManager, error) { return manager, nil },
		ensureNetwork: func(context.Context, *orchestrator.ServiceManager) error {
			return errors.New("network initialization failed")
		},
		teardown: func(ctx context.Context, got *orchestrator.ServiceManager) error {
			teardownCalled = true
			if got != manager {
				t.Fatal("teardown received wrong manager")
			}
			if _, bounded := ctx.Deadline(); !bounded {
				t.Fatal("partial teardown context is unbounded")
			}
			return errors.New("cleanup failed")
		},
	})
	if got != nil || err == nil || !strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("manager=%#v error=%v", got, err)
	}
	if !teardownCalled {
		t.Fatal("partial manager teardown was not attempted")
	}
}

func TestDockerStartupPreservesMalformedConfigurationFailure(t *testing.T) {
	t.Setenv("DOCKER_HOST", "definitely-not-a-docker-endpoint")
	got, err := initializeDockerOrchestration(context.Background(), false, dockerStartupDependencies{
		newManager: orchestrator.NewServiceManager,
	})
	if !errors.Is(err, orchestrator.ErrDockerConfiguration) {
		t.Fatalf("malformed Docker configuration error = %v", err)
	}
	if got != nil {
		t.Fatalf("manager = %#v, want nil", got)
	}
}
