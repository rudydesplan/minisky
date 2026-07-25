# 🛰️ MiniSky

**High-Fidelity local emulator for Google Cloud Platform.**

**Official Website:** [minisky.bmics.com.ng](https://minisky.bmics.com.ng)

MiniSky provides a seamless, professional-grade development environment that emulates GCP services locally. It allows developers to test Infrastructure-as-Code (Terraform), Serverless functions, and complex data workflows without incurring cloud costs or requiring an internet connection.

[![Go Report Card](https://goreportcard.com/badge/github.com/qamarudeenm/minisky)](https://goreportcard.com/report/github.com/qamarudeenm/minisky)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Mini Movement](https://img.shields.io/badge/Mini-Family-blue.svg)](https://github.com/topics/mini-cloud)
[![High Fidelity](https://img.shields.io/badge/Fidelity-High-green.svg)](#)

---

## ✨ Features

- **🚀 29+ GCP Services**: Support for Compute Engine, GKE, Bigtable, Pub/Sub, Storage, Cloud SQL, Vertex AI, Artifact Registry, and more.
- **🖥️ Embedded Dashboard**: Real-time observability and resource management via a premium web UI.
- **🛠️ Terraform Ready**: First-class support for the official Google Cloud Terraform provider via custom endpoint routing.
- **🔌 Dynamic Registry**: Modular Go shim registry for community-led service contributions.
- **📦 Single Binary**: Developed entirely in Go. A single, ultra-lightweight binary where all services are lazy-loaded for maximum efficiency and sub-100ms startup times.

## 📋 Prerequisites
MiniSky requires the following tools installed and running on your local machine:
- **[Docker Desktop](https://www.docker.com/products/docker-desktop/)**: Used for high-fidelity service emulation (Compute, SQL, etc.).
- **[Git](https://git-scm.com/downloads)**: Required when building or contributing from source.

## 🚀 Quick Start

### Installation

**Linux & macOS:**
```bash
curl -sSL https://minisky.bmics.com.ng/install.sh | sh
```

**Windows — Direct Download (Recommended):**

Download the self-contained `minisky.exe` from the [latest GitHub release](https://github.com/qamarudeenm/minisky/releases/latest). No installer needed — just extract and run:

```powershell
# Download and extract
Invoke-WebRequest -Uri https://github.com/qamarudeenm/minisky/releases/latest/download/minisky_windows_amd64.zip -OutFile minisky.zip
Expand-Archive minisky.zip -DestinationPath C:\minisky

# Run
C:\minisky\minisky.exe start
```

> MiniSky stores all data in `%USERPROFILE%\.minisky\` — never in your working directory.

**Windows — Scoop (Alternative):**

```powershell
# Install Scoop if not already installed
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
Invoke-RestMethod -Uri https://get.scoop.sh | Invoke-Expression

# Install MiniSky
scoop bucket add minisky https://github.com/qamarudeenm/scoop-bucket
scoop install minisky
```

### Start the Daemon
```bash
minisky start
```
- **API Gateway**: `http://localhost:8080`
- **Dashboard**: `http://localhost:8081`

### Uninstall
```bash
minisky uninstall
```
This removes all containers, networks, and data from `~/.minisky`. Then delete the binary to fully uninstall.

### Upgrading
To upgrade an existing installation to the latest version, you just need to replace the binary. Your data in `~/.minisky` is persistent and will be preserved automatically.

**Linux & macOS:**
Simply run the install script again:
```bash
curl -sSL https://minisky.bmics.com.ng/install.sh | sh
```

**Windows (Direct):**
1. Stop the running daemon (`minisky stop` or close the terminal).
2. Download the new `.zip` and overwrite your existing `minisky.exe`.

**Windows (Scoop):**
```powershell
scoop update minisky
```


## 🖥️ Platform Compatibility

MiniSky is cross-platform. BigQuery SQL execution can use the embedded
[DuckDB](https://duckdb.org) engine when MiniSky is built with CGO. Builds
without CGO retain dataset and table metadata behavior but use simulated query
execution.

| Feature | Linux amd64 | Linux arm64 | macOS arm64 | Windows amd64 | Windows WSL2 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| Compute / GKE / Storage | ✅ | ✅ | ✅ | ✅ | ✅ |
| Pub/Sub / Cloud SQL / VPC | ✅ | ✅ | ✅ | ✅ | ✅ |
| BigQuery SQL execution | ✅ DuckDB\* | ✅ DuckDB\* | ✅ DuckDB\* | ✅ DuckDB\* | ✅ DuckDB\* |
| CGO build | Yes | Yes | Yes | Yes | Yes |

\* DuckDB is enabled by the `full` runtime profile on CGO builds, or by the
explicit `MINISKY_BQ_BACKEND=duckdb` override.

### Runtime profiles

`MINISKY_RUNTIME_PROFILE` makes backend defaults explicit:

- `simulation` is the backward-compatible default. DuckDB, Buildpacks, and
  Kind remain disabled unless individually overridden. This avoids creating
  resource-intensive Kind clusters unexpectedly.
- `full` enables DuckDB automatically on CGO builds. It enables Buildpacks and
  Kind only when their required CLIs (`pack`/`kind` and `docker`) are installed;
  otherwise MiniSky reports the dependency and falls back to simulation.

Per-backend overrides take precedence over the profile:

```bash
MINISKY_BQ_BACKEND=duckdb
MINISKY_SERVERLESS_BACKEND=buildpacks
MINISKY_GKE_BACKEND=kind
```

Set any override to `simulation` to disable that real backend even under the
`full` profile. Invalid profile or backend values also fall back to simulation
with a diagnostic in startup logs, `minisky doctor`, and dashboard API state.

Published v1.2.x macOS and Windows artifacts predate native CGO support.
Upgrade to v1.3.0 or later for native DuckDB, or run the Linux build through
Docker Desktop or WSL2:

```bash
docker run --rm \
  -e MINISKY_BQ_BACKEND=duckdb \
  -p 8080:8080 -p 8081:8081 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  minisky:latest
```

---

## 🗺️ Product Roadmap

MiniSky's goal is not to maximize the number of APIs that return successful
responses. It is to provide reliable local workflows for Terraform, Google
Cloud SDKs, event-driven applications, and container-backed services. New
features are marked complete only after the same workflow passes through the
public gateway in CI.

### Completed foundation — v1.3.0

- Native DuckDB query execution on Linux amd64/arm64, macOS arm64, and Windows
  amd64.
- BigQuery conformance coverage for queries, nested schemas, streaming inserts,
  file loads, persistence, and no-CGO behavior.
- Native, checksummed release artifacts with installed-binary smoke tests.
- Strict UI linting, Go race tests, release validation, and reproducible builds.

Run `minisky doctor` for concise checks of Docker connectivity, DuckDB, optional
Kind and Pack tooling, API/UI port availability, and `~/.minisky` writability.
Required failures return a nonzero exit status; missing Kind or Pack is reported
as optional because each gates only its corresponding service.

`minisky doctor bigquery` remains available for an isolated DuckDB check without
starting Docker-backed services. `doctor --fix` is intentionally unavailable:
the current Kind/Pack installer is coupled to the running Docker service manager
and is not safe to invoke from standalone diagnostics.

### Roadmap principles

- **Fidelity before breadth:** deepen existing services before adding more
  service names.
- **Public API first:** Terraform, SDKs, CLI commands, and the dashboard must use
  the same gateway behavior.
- **Honest compatibility:** simulated, metadata-only, and executable backends
  are documented separately.
- **Durable local environments:** restart, export, and import behavior must be
  deterministic.
- **Test complete workflows:** acceptance gates cover create, observe, update,
  restart, and destroy—not only successful HTTP status codes.

### Planned execution

| Phase | Feature set | Verification | Status |
| :--- | :--- | :--- | :---: |
| 6 | Service fidelity baseline and compatibility matrices | Every registered domain has a documented fidelity tier, persistence model, and at least one contract test | 🔜 Planned |
| 7 | Terraform and SDK compatibility | CI applies and destroys a multi-service Terraform stack through localhost endpoints with no unsupported routing or plan drift | 🔜 Planned |
| 8 | Durable state, profiles, and snapshots | Supported resources survive restart; export/import round-trips into an isolated named profile | 🔜 Planned |
| 9 | Executable serverless and event delivery | Buildpacks deployments run user code; Pub/Sub, Storage, Scheduler, and Cloud Tasks reach real targets with observable retries | 🔜 Planned |
| 10 | Networking and artifact fidelity | Compute load-balancer resources are stateful and route traffic; Artifact Registry reflects pushed packages and versions | 🔜 Planned |
| 11 | Unified diagnostics, CLI, and distribution | Headless commands use the API gateway; doctor covers all runtime dependencies; package-manager and container releases are tested | 🔜 Planned |
| 12 | Observability and request diagnostics | Structured logs, distributed traces, and metrics are queryable; request replay reproduces any prior API call | 🔜 Planned |
| 13 | Security, authentication, and credential simulation | TLS/mTLS terminates locally; service account impersonation and credential rotation behave like production IAM | 🔜 Planned |
| 14 | Multi-tenancy and organization emulation | Multiple projects coexist with org-level policies; cross-project references resolve correctly | 🔜 Planned |
| 15 | Extended data services and caching | Spanner, Firestore, Datastore, and Memorystore provide executable query and caching backends | 🔜 Planned |
| 16 | ML/AI, monitoring, and advanced networking | Vertex AI serves predictions; Cloud Monitoring/Logging emits and queries metrics/logs; VPC peering and Private Service Connect route traffic | 🔜 Planned |
| 17 | CI/CD integration, plugin ecosystem, and enterprise | GitHub Actions and GitLab CI templates work out of the box; third-party shims install via a plugin SDK; enterprise audit and RBAC are enforceable | 🔜 Planned |

### Phase 6 — Fidelity baseline

- Add `docs/service-compatibility.md` with a tier for every registered API:
  **executable**, **emulator-backed**, **metadata-only**, or **experimental**.
- Add `docs/state-model.md` describing file, Docker volume, and in-memory state
  per service.
- Standardize GCP error envelopes and request validation for Storage, Pub/Sub,
  Secret Manager, Cloud KMS, Scheduler, Cloud Tasks, Cloud Build, and Artifact
  Registry.
- Add one create/get/delete contract test per registered domain. Stubbed
  operations must return an explicit unsupported error instead of fake success.

### Phase 7 — Terraform and SDK compatibility

- Replace fixed localhost path mappings with a service-aware endpoint registry
  that covers every documented custom endpoint without ambiguous `/v1` routes.
- Expand the Terraform example to Storage, Pub/Sub, BigQuery, Cloud SQL,
  Compute, IAM, and a serverless service.
- Run `terraform apply`, resource assertions, a no-drift plan, and
  `terraform destroy` in CI.
- Publish `docs/terraform-compatibility.md` with provider resources, endpoint
  configuration, long-running operation support, and tested versions.
- Add SDK smoke suites for Go and Python against the same gateway.

### Phase 8 — Durable state and team workflows

- Introduce a versioned state store with per-shim persistence adapters.
- Rehydrate Compute, IAM, DNS, BigQuery metadata, Scheduler, Secret Manager,
  Cloud KMS, GKE metadata, and serverless resources on startup.
- Reconcile persisted metadata with surviving Docker containers and volumes
  after an unclean shutdown.
- Add `minisky state export`, `minisky state import`, and named profiles under
  `~/.minisky/profiles/`.
- Verify create → restart → no-drift plan → export → clean import in CI.

### Phase 9 — Serverless and event-driven workflows

- [x] Add explicit `full` and backward-compatible `simulation` runtime profiles,
  dependency-aware real backends, and consistent doctor/dashboard state.
- Execute deployed Cloud Functions and Cloud Run user handlers rather than
  fallback containers.
- Deliver Cloud Tasks HTTP requests with retry, backoff, attempt, and terminal
  failure state.
- Route Pub/Sub and Storage events to both Functions and Cloud Run.
- Remove hard-coded scheduler gateway ports and record delivery outcomes.
- Verify upload/publish/schedule/task → handler invocation end to end.

### Phase 10 — Networking and artifact workflows

- Persist backend services, health checks, URL maps, target proxies, and
  forwarding rules in the Compute shim.
- Route local traffic through configured load-balancer resources to healthy
  Compute container backends.
- Enforce IAM policies in an opt-in strict mode while retaining a documented
  permissive development mode.
- Replace hardcoded Artifact Registry packages with an index derived from the
  local registry.
- Verify Terraform-managed load balancing and push/list/delete artifact flows.

### Phase 11 — Developer experience and distribution

- Expand `minisky doctor` to Docker, gateway ports, disk space, DuckDB, Kind,
  Buildpacks, emulator images, and platform dependencies.
- Add `doctor --fix` for dependencies that MiniSky can install safely.
- Move headless CLI commands from dashboard-only endpoints to the public API
  gateway and complete the CLI reference.
- Publish and test GHCR images plus Homebrew, Scoop, deb, and rpm packages.
- Add `make dev` and `make test-integration` as reproducible contributor entry
  points.

### Phase 12 — Observability and request diagnostics

- Emit structured JSON logs for every API request with correlation IDs,
  latency, and response status.
- Integrate OpenTelemetry tracing so spans propagate across gateway, shims,
  and Docker-backed services.
- Expose a Prometheus-compatible `/metrics` endpoint for request rates, error
  rates, and resource counts.
- Add a request replay system that captures and re-executes any prior API call
  from the dashboard or CLI.
- Surface trace and log data in the embedded dashboard with filtering and
  search.

### Phase 13 — Security, authentication, and credential simulation

- Terminate TLS locally with auto-generated or user-provided certificates;
  support mTLS between services.
- Emulate service account impersonation, short-lived tokens, and workload
  identity federation flows.
- Simulate credential rotation (key creation, expiry, and revocation) for
  service accounts and API keys.
- Validate OAuth2 scopes and audience claims against configured IAM policies
  in strict mode.
- Document security simulation boundaries and differences from production GCP.

### Phase 14 — Multi-tenancy and organization emulation

- Support multiple GCP projects running concurrently with isolated resource
  namespaces.
- Emulate organization-level policies (org policies, folder hierarchy, and
  inherited IAM bindings).
- Resolve cross-project references for Pub/Sub subscriptions, shared VPCs,
  BigQuery datasets, and Storage buckets.
- Add `minisky project create`, `minisky project list`, and
  `minisky project switch` CLI commands.
- Verify multi-project Terraform stacks with cross-project data references
  in CI.

### Phase 15 — Extended data services and caching

- Add a Spanner emulator with DDL support, read/write transactions, and
  query execution.
- Promote Firestore beyond metadata-only to support document CRUD, queries,
  real-time listeners, and security rules evaluation.
- Emulate Datastore (legacy mode) with entity operations, indexes, and
  ancestor queries.
- Add Memorystore emulation backed by a local Redis instance for caching
  and session workflows.
- Verify each data service with SDK-level integration tests and Terraform
  resource lifecycle.

### Phase 16 — ML/AI, monitoring, and advanced networking

- Emulate Vertex AI model deployment, online prediction endpoints, and batch
  prediction jobs with configurable mock responses.
- Add a feature store shim for feature ingestion, serving, and point-in-time
  lookups.
- Emulate Cloud Monitoring with metric descriptors, time-series writes, and
  MQL/PromQL queries.
- Emulate Cloud Logging with log entries, sinks, log-based metrics, and
  alerting policy evaluation.
- Implement VPC peering, Private Service Connect, and Cloud NAT with local
  network routing.
- Add DNS zone emulation with record sets and local resolution for service
  discovery.

### Phase 17 — CI/CD integration, plugin ecosystem, and enterprise

- Publish a GitHub Actions action (`uses: minisky/setup-minisky@v1`) that
  installs and starts MiniSky with configurable services.
- Provide a GitLab CI template and pre-built Docker Compose stacks for
  common multi-service topologies.
- Release a third-party shim SDK with versioned plugin API, lifecycle hooks,
  and a contribution scaffold generator.
- Host a plugin marketplace (registry) for community-contributed service
  shims with discoverability and version pinning.
- Add a benchmarking suite for throughput, latency, and resource consumption
  under concurrent load.
- Emulate resource quotas and rate limits matching GCP defaults with
  configurable overrides.
- Provide interactive tutorials, video walkthroughs, and architecture
  decision records in the documentation site.
- Add enterprise features: audit logging for all mutations, compliance mode
  with immutable logs, air-gapped installation support, and role-based
  access control (RBAC) for team environments.

---

## 📖 Documentation

- [CLI Reference](docs/cli_reference.md)
- [Terraform Guide](docs/terraform.md)
- [Service Compatibility](docs/service-compatibility.md)
- [State Model](docs/state-model.md)
- [Terraform Compatibility](docs/terraform-compatibility.md)
- [Distribution](docs/distribution.md)
- [Changelog](CHANGELOG.md)
- [Contributor Guide](CONTRIBUTING.md)

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to build and register new service shims.

## 📄 License

MiniSky is released under the [MIT License](LICENSE).