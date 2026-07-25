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
VERSION=v1.3.0
OS=linux       # use darwin on macOS
ARCH=amd64     # use arm64 where applicable
ARCHIVE="minisky_${OS}_${ARCH}.tar.gz"
BASE="https://github.com/qamarudeenm/minisky/releases/download/${VERSION}"
curl --fail --location --output "$ARCHIVE" "$BASE/$ARCHIVE"
curl --fail --location --output checksums.txt "$BASE/checksums.txt"

# Linux:
grep "  ${ARCHIVE}$" checksums.txt | sha256sum --check
# macOS:
# grep "  ${ARCHIVE}$" checksums.txt | shasum -a 256 --check

tar -xzf "$ARCHIVE"
install -m 0755 minisky "$HOME/.local/bin/minisky"
```

This downloads data only; it never pipes unverified code to a shell.

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

**Windows — Scoop:** MiniSky does not currently publish or maintain a Scoop
bucket. Use the checksummed direct-download flow above until an official bucket
is announced in the distribution guide.

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
Download the new archive and `checksums.txt`, verify the selected archive as
shown above, stop MiniSky, and replace the binary.

**Windows (Direct):**
1. Stop the running daemon (`minisky stop` or close the terminal).
2. Download the new `.zip` and overwrite your existing `minisky.exe`.

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
  'ghcr.io/qamarudeenm/minisky@sha256:<verified-release-digest>'
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
| 6 | Service fidelity baseline and compatibility matrices | Every registered domain has manifest/docs coherence and an executable in-process or backend-gated contract | ✅ Baseline |
| 7 | Terraform and SDK compatibility | The opt-in fixture covers BigQuery, IAM, Storage, cross-project Pub/Sub, Memorystore, and Spanner through canonical endpoints | ✅ Baseline |
| 8 | Durable state, profiles, and snapshots | Supported metadata survives restart; an opt-in gate verifies restart and metadata-only export/import | ✅ Foundation |
| 9 | Executable serverless and event delivery | Buildpacks deployments run user code; Pub/Sub, Storage, Scheduler, and Cloud Tasks reach real targets with observable retries | 🚧 Local slice |
| 10 | Networking and artifact fidelity | Compute load-balancer resources are stateful and route traffic; Artifact Registry reflects pushed packages and versions | 🚧 Local slice |
| 11 | Unified diagnostics, CLI, and distribution | Headless commands use the API gateway; doctor covers all runtime dependencies; package-manager and container releases are tested | 🚧 Local slice |
| 12 | Observability and request diagnostics | Structured gateway logs, trace correlation, and low-cardinality metrics are queryable; eligible requests support opt-in bounded replay | 🚧 Vertical slice |
| 13 | Security, authentication, and credential simulation | TLS protects both loopback listeners; optional client-certificate mTLS applies only to the gateway; local credentials remain non-production simulations | 🚧 Local simulation |
| 14 | Multi-tenancy and organization emulation | Multiple projects coexist with org-level policies; cross-project references resolve correctly | 🚧 Metadata slice |
| 15 | Extended data services and caching | Spanner, Firestore, Datastore, and Memorystore provide executable query and caching backends | 🚧 Bounded slices |
| 16 | ML/AI, monitoring, and advanced networking | Vertex AI serves predictions; Cloud Monitoring/Logging emits and queries metrics/logs; VPC peering and Private Service Connect route traffic | 🚧 Bounded slices |
| 17 | CI/CD integration, plugin ecosystem, and enterprise | Local CI templates, source-compiled plugin scaffolds, benchmarks, quotas, audit/RBAC controls, and offline bundles have executable checks | 🚧 Bounded local slice |

### Phase 6 — Fidelity baseline

- Maintain `docs/service-compatibility.md` with the manifest vocabulary:
  **high**, **standard**, or **passthrough** fidelity and **memory**, **file**,
  **docker**, **hybrid**, or **static** persistence.
- Add `docs/state-model.md` describing file, Docker volume, and in-memory state
  per service.
- Standardize GCP error envelopes and request validation for Storage, Pub/Sub,
  Secret Manager, Cloud KMS, Scheduler, Cloud Tasks, Cloud Build, and Artifact
  Registry.
- Keep focused lifecycle/unsupported tests for in-process shims. Pure lazy
  Docker passthroughs instead have deterministic cold-start/error contracts and
  explicit backend-gated rationale; they do not claim Docker-free CRUD tests.

### Phase 7 — Terraform and SDK compatibility

- Replace fixed localhost path mappings with a service-aware endpoint registry
  that covers every documented custom endpoint without ambiguous `/v1` routes.
- Expand the Terraform example only where the provider-visible lifecycle is
  proven. The current fixture includes BigQuery, IAM, Storage, cross-project
  Pub/Sub, Memorystore, and Spanner; Cloud SQL, Compute, and serverless remain
  outside this fixture.
- Run `terraform apply`, resource assertions, a no-drift plan, and
  `terraform destroy` in CI.
- Publish `docs/terraform-compatibility.md` with provider resources, endpoint
  configuration, long-running operation support, and tested versions.
- Add SDK smoke suites for Go and Python against the same gateway.

### Phase 8 — Durable state and team workflows

- Introduce a versioned state store with per-shim persistence adapters.
- Rehydrate Compute, IAM, DNS, BigQuery metadata, Scheduler, Secret Manager,
  Cloud KMS, GKE metadata, and serverless resources on startup.
- Keep restored Docker-backed resources metadata-only unless profile ownership
  labels prove a resource is safe to reconcile or remove.
- Add `minisky state export`, `minisky state import`, and named profiles under
  `~/.minisky/state/profiles/`.
- Verify create → restart → no-drift plan → metadata export → clean import →
  destroy through the opt-in durability CI gate.

### Phase 9 — Serverless and event-driven workflows

**Status (2026-07-25): local delivery slice implemented.** Cloud Tasks state,
Scheduler gateway wiring, strict Buildpacks failure handling, and the guarded
event-delivery harness have focused tests. The Docker/Pack end-to-end gate was
not run while an existing MiniSky network was active.

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

**Status (2026-07-25): local networking/artifact slice implemented.** Compute
load-balancer metadata/proxying, narrow strict-IAM enforcement, and the owned
Registry v2 index have focused tests. Terraform load-balancer and real
push/list/delete integration remain guarded Docker acceptance work.

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

**Status (2026-07-25): local tooling slice implemented.** Doctor, safe fixes,
gateway-first delivery/artifact commands, Make targets, and release smoke
validation are present. Homebrew, Scoop, deb, and rpm are not described as
published until native install and credentialed publication gates exist.

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
- Provide opt-in replay for eligible same-gateway calls, with bounded JSON body
  capture, sensitive-field rejection, and recursion prevention.
- Surface gateway request and trace correlation data in a dedicated Dashboard
  view and headless diagnostics CLI, separate from Cloud Logging.

### Phase 13 — Security, authentication, and credential simulation

**Status (2026-07-25): local simulation slice implemented.** Go race tests
cover TLS key permissions, mTLS gateway handshakes, token
expiry/audience/scope, key disable/delete persistence, impersonation allow/deny,
and redacted `401`/`403` responses. Provider-backed WIF and delegated
impersonation remain explicit `501 UNIMPLEMENTED` boundaries.

- Terminate TLS locally with auto-generated or user-provided certificates.
  Optional client-certificate mTLS is enforced only by the public gateway;
  MiniSky does not provide service-to-service mTLS.
- Emulate service account impersonation, short-lived tokens, and workload
  identity federation flows.
- Simulate credential rotation (key creation, expiry, and revocation) for
  service accounts and API keys.
- Validate OAuth2 scopes and audience claims against configured IAM policies
  in strict mode.
- Document security simulation boundaries and differences from production GCP.

### Phase 14 — Multi-tenancy and organization emulation

**Status (2026-07-25): metadata tenancy slice implemented.** Go race tests
cover project CRUD, persistence/export/import, unknown-project enforcement,
inherited IAM, same-name BigQuery isolation, and cross-project Pub/Sub
permission denial. The two-project Terraform/Pub/Sub fixture validates
statically; Docker-backed apply/no-drift/destroy remains an integration gate and
does not establish isolation for shared passthrough backends. Shared VPC packet
routing remains out of scope.

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

**Status (2026-07-25): bounded emulator slices implemented.** Memorystore
metadata and owned Redis lifecycle, profile-scoped Spanner/Firestore/Datastore
backends, and SDK/Terraform fixtures have local contract and static validation.
The guarded Docker data-plane suite was not run while an existing MiniSky
network was active. Firestore listeners/rules remain unsupported.

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

**Status (2026-07-25): bounded service slices implemented.** Vertex mock
prediction, Monitoring descriptors/time series, profile-scoped Logging and
sinks, DNS resolution, and subnetwork/IPAM contracts have focused tests. MQL,
log-based alerting, feature-store/batch prediction, and peering/NAT/PSC remain
explicit `501 UNIMPLEMENTED` boundaries.

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

**Status (2026-07-25): bounded local slice implemented.** The repository-local
GitHub Action is tested with a built artifact; GitLab and Compose templates,
source-compiled SDK v0 scaffolds, correctness-checked benchmarks, opt-in quotas,
profile audit records with verifiable hash-chain consistency (not externally
anchored tamper evidence), local RBAC, and checksummed offline bundles have
executable checks. See
[Phase 17 local operations](docs/phase17-local-operations.md).

The SDK is intentionally not third-party installable: there is no runtime
loader, protocol negotiation, process isolation, artifact signature
verification, or failure supervisor. A remote marketplace, published
`minisky/setup-minisky@v1` action, immutable/WORM compliance storage, external
identity/SSO/SCIM, distributed quotas, and video assets remain deferred.

---

## 📖 Documentation

- [CLI Reference](docs/cli_reference.md)
- [Terraform Guide](docs/terraform.md)
- [Service Compatibility](docs/service-compatibility.md)
- [State Model](docs/state-model.md)
- [Terraform Compatibility](docs/terraform-compatibility.md)
- [Distribution](docs/distribution.md)
- [Phase 17 Local Operations](docs/phase17-local-operations.md)
- [Architecture Decisions](docs/adr/README.md)
- [Changelog](CHANGELOG.md)
- [Contributor Guide](CONTRIBUTING.md)

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to build and register new service shims.

## 📄 License

MiniSky is released under the [MIT License](LICENSE).