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

## 🗺️ Platform Roadmap — DuckDB / CGO

The objective is to ship native DuckDB query execution on every supported
platform without weakening release reproducibility. Native CI runners are
preferred over cross-compilation because CGO links platform-specific C/C++
libraries.

### Execution plan

| Phase | Deliverable | Verification | Status |
| :--- | :--- | :--- | :---: |
| 0 | BigQuery conformance tests for `SELECT 1`, nested DDL, streaming inserts, load jobs, and persistence | CGO tests pass with DuckDB; no-CGO tests assert explicit unsupported-operation errors | ✅ Complete |
| 1 | Linux amd64 and arm64 release coverage | Native CI builds and executes the conformance suite on both architectures | ✅ Complete |
| 2 | Native macOS arm64 CGO build using Apple Clang | M-series runner builds, packages, and executes real queries | ✅ Complete |
| 3 | Native Windows amd64 support using MSYS2/UCRT GCC | Native CI passes conformance and confirms no non-system MinGW runtime DLLs | ✅ Complete |
| 4 | Multi-runner release assembly | GitHub Actions publishes checksummed artifacts built and tested on native runners; GoReleaser validates package configuration | ✅ Complete |
| 5 | Installer and compatibility updates | Checksums are verified and installed CGO artifacts run `minisky doctor bigquery` | ✅ Complete |

### Platform strategy

- **macOS arm64:** native Apple Clang builds and executes the conformance suite.
- **Linux arm64:** native CI and Docker both execute the conformance suite.
- **Windows amd64:** native MSYS2/UCRT GCC builds pass conformance and link
  without non-system MinGW runtime DLLs.

GoReleaser Community cannot merge partial native builds produced by separate
runners. CI therefore uses GoReleaser for configuration and Linux snapshot
validation, while the tagged release workflow packages the already-tested
native binaries directly and generates a single checksum manifest.

Documentation is updated to mark a platform as fully supported only after its
release artifact passes the same BigQuery conformance suite used in CI.

Run `minisky doctor bigquery` to verify that an installed binary contains a
working DuckDB backend. The check uses an isolated temporary database and does
not require the Docker daemon.

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