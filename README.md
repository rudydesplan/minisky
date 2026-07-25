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

MiniSky is cross-platform. BigQuery SQL execution uses the embedded
[DuckDB](https://duckdb.org) engine when MiniSky is built with CGO and
`MINISKY_BQ_BACKEND=duckdb` is set. Builds without CGO retain dataset and table
metadata behavior but use mock query execution.

| Feature | Linux amd64 | Linux arm64 | macOS arm64 | Windows amd64 | Windows WSL2 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| Compute / GKE / Storage | ✅ | ✅ | ✅ | ✅ | ✅ |
| Pub/Sub / Cloud SQL / VPC | ✅ | ✅ | ✅ | ✅ | ✅ |
| BigQuery SQL execution | ✅ DuckDB\* | ✅ DuckDB\* | ✅ DuckDB\* | ✅ DuckDB\* | ✅ DuckDB\* |
| CGO build | Yes | Yes | Yes | Yes | Yes |

\* DuckDB is currently opt-in. Set `MINISKY_BQ_BACKEND=duckdb` before starting
MiniSky.

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

Run `minisky doctor bigquery` to verify DuckDB without starting Docker-backed
services.

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

- Make the local development profile enable DuckDB and Buildpacks when their
  dependencies are available; preserve an explicit simulation profile.
- Fix dashboard backend settings so CLI, API, and UI report the same state.
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

---

## 📖 Documentation

- [CLI Reference](docs/cli_reference.md)
- [Terraform Guide](docs/terraform.md)
- [Changelog](CHANGELOG.md)
- [Contributor Guide](CONTRIBUTING.md)

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to build and register new service shims.

## 📄 License

MiniSky is released under the [MIT License](LICENSE).