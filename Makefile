.PHONY: dev test ui-install ui-build ui-test test-integration test-kind test-java-sdk-compile test-java-sdk-smoke test-event-delivery test-phase10-artifact test-phase13-wif test-phase16-monitoring test-phase16-logging test-phase16-dns test-phase16-subnetwork test-phase16-subnetwork-terraform test-phase16-vertex test-phase17 test-phase17-enterprise benchmark

ui-install:
	cd ui && npm ci

ui-test: ui-install
	cd ui && npm test

ui-build: ui-test
	cd ui && npm run build

dev: ui-build
	go run ./cmd/minisky start

test: ui-test ui-build
	go test -race ./cmd/... ./pkg/... ./ui

test-integration:
	@test "$${MINISKY_INTEGRATION:-}" = "1" || (echo "Set MINISKY_INTEGRATION=1 to run Docker-backed integration tests" >&2; exit 1)
	$(MAKE) test-event-delivery
	MINISKY_STATE_DURABILITY_INTEGRATION=1 ./scripts/state-durability-integration.sh
	MINISKY_TERRAFORM_INTEGRATION=1 ./scripts/terraform-integration.sh
	MINISKY_PHASE10_INTEGRATION=1 ./scripts/phase10-artifact-integration.sh

test-kind:
	MINISKY_KIND_INTEGRATION=1 ./scripts/kind-integration.sh

test-java-sdk-compile:
	MINISKY_JAVA_SDK_SMOKE=1 MINISKY_JAVA_CONTAINER=1 MINISKY_JAVA_COMPILE_ONLY=1 ./scripts/java-sdk-smoke.sh

test-java-sdk-smoke:
	MINISKY_JAVA_SDK_SMOKE=1 MINISKY_JAVA_CONTAINER=1 ./scripts/java-sdk-smoke.sh

test-event-delivery:
	MINISKY_EVENT_INTEGRATION=1 ./scripts/event-delivery-integration.sh

test-phase10-artifact:
	MINISKY_PHASE10_INTEGRATION=1 ./scripts/phase10-artifact-integration.sh

test-phase13-wif:
	MINISKY_PHASE13_INTEGRATION=1 ./scripts/phase13-wif-integration.sh

test-phase16-monitoring:
	MINISKY_PHASE16_MONITORING_INTEGRATION=1 ./scripts/phase16-monitoring-integration.sh

test-phase16-logging:
	MINISKY_PHASE16_LOGGING_INTEGRATION=1 ./scripts/phase16-logging-integration.sh

test-phase16-dns:
	MINISKY_PHASE16_DNS_INTEGRATION=1 ./scripts/phase16-dns-integration.sh

test-phase16-subnetwork:
	MINISKY_PHASE16_SUBNETWORK_INTEGRATION=1 ./scripts/phase16-subnetwork-integration.sh

test-phase16-subnetwork-terraform:
	MINISKY_PHASE16_SUBNETWORK_TERRAFORM_INTEGRATION=1 ./scripts/phase16-subnetwork-terraform-integration.sh

test-phase16-vertex:
	MINISKY_PHASE16_VERTEX_INTEGRATION=1 ./scripts/phase16-vertex-integration.sh

test-phase17:
	go test ./scripts ./pkg/pluginsdk ./pkg/security ./pkg/dashboard ./pkg/router ./pkg/observability ./cmd/minisky
	node --check .github/actions/setup-minisky/index.mjs
	node --check .github/actions/setup-minisky/cleanup.mjs
	bash -n scripts/airgap-bundle.sh scripts/airgap-bundle-test.sh
	./scripts/airgap-bundle-test.sh
	MINISKY_IMAGE=ghcr.io/qamarudeenm/minisky:test docker compose -f deployments/docker-compose.yml config >/dev/null

test-phase17-enterprise:
	MINISKY_PHASE17_ENTERPRISE_INTEGRATION=1 ./scripts/phase17-enterprise-wif-integration.sh

benchmark:
	go test -run='^$$' -bench=BenchmarkGatewayRouting -benchmem -count=5 ./pkg/router
	go test -run='^$$' -bench='BenchmarkStore(Save|Load)' -benchmem -count=5 ./pkg/state
	go test -run='^$$' -bench=BenchmarkProjectMetadataLookup -benchmem -count=5 ./pkg/shims/metadata
