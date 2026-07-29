.PHONY: dev test ui-install ui-build ui-test check-docs-truth test-integration test-kind test-java-sdk-compile test-java-sdk-smoke test-event-delivery test-storage-persistence-pubsub-session test-cloudsql-restart-integration test-memcache-integration test-phase10-artifact test-phase11-distribution test-phase12-observability-contract test-phase12-observability test-phase13-wif test-phase16-monitoring test-phase16-logging test-phase16-dns test-phase16-subnetwork test-phase16-subnetwork-terraform test-phase16-vertex test-phase17 test-phase17-enterprise test-phase18-25-evidence test-phase18-25-sdk test-phase18-event-delivery test-phase18-eventarc-terraform test-phase18-workflows-terraform test-phase19-sdk test-phase19-heavy-backend test-phase19-composer-terraform test-phase19-managed-kafka-terraform test-phase20-sdk test-phase20-alloydb-terraform test-phase20-filestore-terraform test-phase20-identity-platform-terraform test-phase20-storage-transfer-terraform test-phase21-22-sdk test-phase21-service-directory-terraform test-phase23-sdk test-phase23-document-ai-terraform test-phase24-25-sdk test-phase24-org-policy-terraform test-phase25-binary-authorization-terraform benchmark

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

check-docs-truth:
	go run ./cmd/docs-truth -check

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

test-storage-persistence-pubsub-session:
	MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION=1 ./scripts/storage-persistence-pubsub-session-integration.sh

test-cloudsql-restart-integration:
	MINISKY_DOCKER_CLOUDSQL_INTEGRATION=1 go test -race -count=1 -timeout=10m ./pkg/shims/cloudsql -run '^TestCloudSQLRestartReconciliationLiveDocker$$'

test-memcache-integration:
	MINISKY_MEMCACHE_INTEGRATION=1 ./scripts/memcache-integration.sh

test-phase10-artifact:
	MINISKY_PHASE10_INTEGRATION=1 ./scripts/phase10-artifact-integration.sh

test-phase11-distribution:
	./scripts/phase11-distribution-test.sh --self-test
	./scripts/phase11-distribution-test.sh --static

test-phase12-observability-contract:
	./scripts/phase12-observability-integration-test.sh

test-phase12-observability: test-phase12-observability-contract
	MINISKY_PHASE12_OBSERVABILITY_INTEGRATION=1 ./scripts/phase12-observability-integration.sh

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
	node --test .github/actions/setup-minisky/index.test.mjs
	node --check .github/actions/setup-minisky/index.mjs
	node --check .github/actions/setup-minisky/cleanup.mjs
	bash -n scripts/airgap-bundle.sh scripts/airgap-bundle-test.sh
	./scripts/airgap-bundle-test.sh
	MINISKY_IMAGE=ghcr.io/qamarudeenm/minisky:test docker compose -f deployments/docker-compose.yml config >/dev/null

test-phase17-enterprise:
	MINISKY_PHASE17_ENTERPRISE_INTEGRATION=1 ./scripts/phase17-enterprise-wif-integration.sh

test-phase18-25-evidence:
	go test -count=1 ./pkg/evidence ./pkg/orchestrator ./pkg/pagination ./pkg/registry ./pkg/router ./pkg/shims/...

test-phase18-25-sdk:
	MINISKY_PHASE18_25_SDK_INTEGRATION=1 ./scripts/phase18-25-sdk-integration.sh

test-phase18-event-delivery:
	MINISKY_PHASE18_EVENT_DELIVERY_INTEGRATION=1 ./scripts/phase18-event-delivery-integration.sh

test-phase18-eventarc-terraform:
	MINISKY_PHASE18_EVENTARC_TERRAFORM_INTEGRATION=1 ./scripts/phase18-eventarc-terraform-integration.sh

test-phase18-workflows-terraform:
	MINISKY_PHASE18_WORKFLOWS_TERRAFORM_INTEGRATION=1 ./scripts/phase18-workflows-terraform-integration.sh

test-phase19-sdk:
	MINISKY_PHASE19_SDK_INTEGRATION=1 ./scripts/phase19-sdk-integration.sh

test-phase19-heavy-backend:
	MINISKY_PHASE19_SDK_INTEGRATION=1 MINISKY_PHASE19_DOCKER_INTEGRATION=1 ./scripts/phase19-sdk-integration.sh

test-phase19-composer-terraform:
	MINISKY_PHASE19_COMPOSER_TERRAFORM_INTEGRATION=1 MINISKY_PHASE19_DOCKER_INTEGRATION=1 ./scripts/phase19-composer-terraform-integration.sh

test-phase19-managed-kafka-terraform:
	MINISKY_PHASE19_MANAGED_KAFKA_TERRAFORM_INTEGRATION=1 MINISKY_PHASE19_DOCKER_INTEGRATION=1 ./scripts/phase19-managed-kafka-terraform-integration.sh

test-phase20-sdk:
	MINISKY_PHASE20_SDK_INTEGRATION=1 ./scripts/phase20-sdk-integration.sh

test-phase20-filestore-terraform:
	MINISKY_PHASE20_FILESTORE_TERRAFORM_INTEGRATION=1 ./scripts/phase20-filestore-terraform-integration.sh

test-phase20-identity-platform-terraform:
	MINISKY_PHASE20_IDENTITY_PLATFORM_TERRAFORM_INTEGRATION=1 ./scripts/phase20-identity-platform-terraform-integration.sh

test-phase20-storage-transfer-terraform:
	MINISKY_PHASE20_STORAGE_TRANSFER_TERRAFORM_INTEGRATION=1 ./scripts/phase20-storage-transfer-terraform-integration.sh

test-phase20-alloydb-terraform:
	MINISKY_PHASE20_ALLOYDB_TERRAFORM_INTEGRATION=1 MINISKY_PHASE20_ALLOYDB_DOCKER_INTEGRATION=1 ./scripts/phase20-alloydb-terraform-integration.sh

test-phase21-22-sdk:
	MINISKY_PHASE21_22_SDK_INTEGRATION=1 ./scripts/phase21-22-sdk-integration.sh

test-phase21-service-directory-terraform:
	MINISKY_PHASE21_SERVICE_DIRECTORY_TERRAFORM_INTEGRATION=1 ./scripts/phase21-service-directory-terraform-integration.sh

test-phase23-sdk:
	MINISKY_PHASE23_SDK_INTEGRATION=1 ./scripts/phase23-sdk-integration.sh

test-phase23-document-ai-terraform:
	MINISKY_PHASE23_DOCUMENT_AI_TERRAFORM_INTEGRATION=1 ./scripts/phase23-document-ai-terraform-integration.sh

test-phase24-25-sdk:
	MINISKY_PHASE24_25_SDK_INTEGRATION=1 ./scripts/phase24-25-sdk-integration.sh

test-phase24-org-policy-terraform:
	MINISKY_PHASE24_ORG_POLICY_TERRAFORM_INTEGRATION=1 ./scripts/phase24-org-policy-terraform-integration.sh

test-phase25-binary-authorization-terraform:
	MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION=1 ./scripts/phase25-binary-authorization-terraform-integration.sh

benchmark:
	go test -run='^$$' -bench=BenchmarkGatewayRouting -benchmem -count=5 ./pkg/router
	go test -run='^$$' -bench='BenchmarkStore(Save|Load)' -benchmem -count=5 ./pkg/state
	go test -run='^$$' -bench=BenchmarkProjectMetadataLookup -benchmem -count=5 ./pkg/shims/metadata
