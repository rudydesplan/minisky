.PHONY: dev test ui-build test-integration test-phase10-artifact test-phase13-wif test-phase17 benchmark

ui-build:
	cd ui && npm ci && npm run build

dev: ui-build
	go run ./cmd/minisky start

test: ui-build
	go test -race ./cmd/... ./pkg/... ./ui

test-integration:
	@test "$${MINISKY_INTEGRATION:-}" = "1" || (echo "Set MINISKY_INTEGRATION=1 to run Docker-backed integration tests" >&2; exit 1)
	MINISKY_EVENT_INTEGRATION=1 ./scripts/event-delivery-integration.sh
	MINISKY_STATE_DURABILITY_INTEGRATION=1 ./scripts/state-durability-integration.sh
	MINISKY_TERRAFORM_INTEGRATION=1 ./scripts/terraform-integration.sh
	MINISKY_PHASE10_INTEGRATION=1 ./scripts/phase10-artifact-integration.sh

test-phase10-artifact:
	MINISKY_PHASE10_INTEGRATION=1 ./scripts/phase10-artifact-integration.sh

test-phase13-wif:
	MINISKY_PHASE13_INTEGRATION=1 ./scripts/phase13-wif-integration.sh

test-phase17:
	go test ./scripts ./pkg/pluginsdk ./pkg/security ./pkg/dashboard ./pkg/router ./pkg/observability ./cmd/minisky
	node --check .github/actions/setup-minisky/index.mjs
	node --check .github/actions/setup-minisky/cleanup.mjs
	bash -n scripts/airgap-bundle.sh scripts/airgap-bundle-test.sh
	./scripts/airgap-bundle-test.sh
	MINISKY_IMAGE=ghcr.io/qamarudeenm/minisky:test docker compose -f deployments/docker-compose.yml config >/dev/null

benchmark:
	go test -run='^$$' -bench=BenchmarkGatewayRouting -benchmem -count=5 ./pkg/router
	go test -run='^$$' -bench='BenchmarkStore(Save|Load)' -benchmem -count=5 ./pkg/state
	go test -run='^$$' -bench=BenchmarkProjectMetadataLookup -benchmem -count=5 ./pkg/shims/metadata
