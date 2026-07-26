package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"minisky/pkg/dashboard"
	"minisky/pkg/orchestrator"
	localsecurity "minisky/pkg/security"
)

type dashboardAuthorizer interface {
	EnforcementEnabled() bool
	Authorize(resource, principal, permission string) bool
	VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error)
}

type dockerStartupDependencies struct {
	newManager      func() (*orchestrator.ServiceManager, error)
	ensureNetwork   func(context.Context, *orchestrator.ServiceManager) error
	reconcileBuilds func(context.Context, *orchestrator.ServiceManager) error
	teardown        func(context.Context, *orchestrator.ServiceManager) error
}

var productionDockerStartup = dockerStartupDependencies{
	newManager: orchestrator.NewServiceManager,
	ensureNetwork: func(ctx context.Context, manager *orchestrator.ServiceManager) error {
		return manager.EnsureNetwork(ctx)
	},
	reconcileBuilds: func(ctx context.Context, manager *orchestrator.ServiceManager) error {
		return manager.ReconcileBuildResources(ctx)
	},
	teardown: func(ctx context.Context, manager *orchestrator.ServiceManager) error {
		return manager.Teardown(ctx)
	},
}

func dashboardAPIWithoutDocker(
	diagnostics http.Handler,
	authorizer dashboardAuthorizer,
	audience string,
) http.Handler {
	return dashboard.NewUnavailableAPIHandler(diagnostics, authorizer, audience)
}

func initializeDockerOrchestration(
	ctx context.Context,
	disabled bool,
	dependencies dockerStartupDependencies,
) (*orchestrator.ServiceManager, error) {
	if disabled {
		log.Printf("[WARN] Docker orchestration disabled; Docker-backed services are unavailable")
		return nil, nil
	}
	if dependencies.newManager == nil {
		return nil, errors.New("Docker startup configuration has no service manager factory")
	}
	manager, err := dependencies.newManager()
	if err != nil {
		cleanupErr := teardownPartialDockerManager(manager, dependencies.teardown)
		if cleanupErr != nil {
			return nil, errors.Join(err, cleanupErr)
		}
		if errors.Is(err, orchestrator.ErrDockerConfiguration) ||
			errors.Is(err, orchestrator.ErrDockerOwnershipConflict) {
			return nil, err
		}
		log.Printf("[WARN] Docker unavailable during initialization; continuing without Docker-backed services: %v", err)
		return nil, nil
	}
	if manager == nil {
		return nil, errors.New("Docker startup configuration returned a nil service manager")
	}
	if dependencies.ensureNetwork != nil {
		if err := dependencies.ensureNetwork(ctx, manager); err != nil {
			cleanupErr := teardownPartialDockerManager(manager, dependencies.teardown)
			if cleanupErr != nil {
				return nil, errors.Join(err, cleanupErr)
			}
			if errors.Is(err, orchestrator.ErrDockerOwnershipConflict) {
				return nil, err
			}
			log.Printf("[WARN] Docker network unavailable; continuing without Docker-backed services: %v", err)
			return nil, nil
		}
	}
	if dependencies.reconcileBuilds != nil {
		if err := dependencies.reconcileBuilds(ctx, manager); err != nil {
			cleanupErr := teardownPartialDockerManager(manager, dependencies.teardown)
			if cleanupErr != nil {
				return nil, errors.Join(err, cleanupErr)
			}
			if errors.Is(err, orchestrator.ErrDockerOwnershipConflict) {
				return nil, err
			}
			log.Printf("[WARN] Docker reconciliation unavailable; continuing without Docker-backed services: %v", err)
			return nil, nil
		}
	}
	return manager, nil
}

func teardownPartialDockerManager(
	manager *orchestrator.ServiceManager,
	teardown func(context.Context, *orchestrator.ServiceManager) error,
) error {
	if manager == nil {
		return nil
	}
	if teardown == nil {
		return errors.New("partial Docker startup has no teardown implementation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := teardown(ctx, manager); err != nil {
		return fmt.Errorf("teardown partially initialized Docker resources: %w", err)
	}
	return nil
}
