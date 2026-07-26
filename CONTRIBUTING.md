# Contributing to MiniSky

**Website:** [minisky.bmics.com.ng](https://minisky.bmics.com.ng)

Thank you for your interest in contributing to MiniSky! This project aims to provide the most accurate, high-fidelity local emulator for Google Cloud Platform. 

## 🏗️ Architecture Overview

MiniSky consists of three main layers:
1.  **The API Gateway (Go)**: Intercepts requests to `*.googleapis.com`.
2.  **Service Shims (Go)**: Logic layers that handle metadata, LROs (Long Running Operations), and state management.
3.  **Docker Backends**: Pure emulators (like `fake-gcs-server` or `pubsub-emulator`) that handle the heavy lifting of data storage.

---

## 🛠️ Adding a New Service Shim

MiniSky uses a **Dynamic Registry** system. To add a new GCP service (e.g., `translation.googleapis.com`), follow these steps:

Start from the source-compiled SDK v0 scaffold:

```bash
go run ./cmd/minisky plugin scaffold ./pkg/shims/translation \
  --name translation \
  --domain translation.googleapis.com
go test ./pkg/shims/translation
```

The generated handler returns `501 UNIMPLEMENTED` until real behavior and tests
are added. SDK v0 is compiled in-tree; MiniSky has no runtime plugin installer
or remote marketplace.

### 1. Create the Package
Create a new directory in `pkg/shims/<service_name>/`.

### 2. Implement the API Handler
Your shim must implement the `http.Handler` interface.

```go
package myservice

import (
    "net/http"
    "minisky/pkg/registry"
)

type API struct {}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusNotImplemented)
    w.Write([]byte(`{"error":{"code":501,"message":"not implemented","status":"UNIMPLEMENTED"}}`))
}
```

### 3. Register the Shim
Add an `init()` function to your package that registers the service domain with the registry.

```go
func init() {
    registry.Register("myservice.googleapis.com", func(ctx *registry.Context) http.Handler {
        return &API{}
    })
}
```

### 4. Enable the Shim
Add a blank import to `pkg/shims/registry_init.go` to ensure your shim is compiled and its `init()` function is triggered.

```go
import (
    _ "minisky/pkg/shims/myservice"
)
```

---

## 🔗 Cross-Service Wiring (Post-Boot)

If your shim needs to interact with another shim (e.g., Pub/Sub sending events to Serverless), implement the `PostBoot` interface.

```go
func (api *API) OnPostBoot(ctx *registry.Context) {
    // Resolve another shim by its domain
    otherShim := ctx.GetShim("other.googleapis.com")
    // ... wire up observers or shared state ...
}
```

---

## 🎨 Dashboard Development

The UI is built with **React + Vite + Material UI**.
1.  Navigate to `ui/`.
2.  Add a new manager component in `src/components/`.
3.  Register your tab in `App.tsx`.
4.  The UI communicates with the backend via the Management API at `:8081/api/manage/`.

Before submitting UI changes, run:

```bash
cd ui
npm ci
npm run lint
npm run build
```

To run the guarded local serverless/event-delivery acceptance gate, start
Docker and install `curl`, `go`, `python3`, and checksum-verified Pack v0.40.8,
then run:

```bash
make test-event-delivery
```

The target supplies `MINISKY_EVENT_INTEGRATION=1`. Its `POST /v2/deploy`
requests use MiniSky's local source-deployment helper, not the Cloud Run v2
image API. The gate is intentionally local and may take several minutes on a
cold Buildpacks cache.

To verify persisted Monitoring samples through the bounded PromQL
instant-query path, run:

```bash
make test-phase16-monitoring
```

This guarded gate uses temporary profile state and dynamic loopback ports. It
does not require Docker, and it leaves MQL, range queries, and broader PromQL
grammar explicitly unsupported.

To verify Cloud Logging entries and sink metadata through the generated
`google.golang.org/api/logging/v2` client across a clean restart, run:

```bash
make test-phase16-logging
```

This guarded gate uses an isolated temporary profile, dynamic loopback ports,
and no Docker. It covers bounded filtered entry listing plus sink
create/get/list/delete; pagination, partial-success writes, sink updates, and
sink-delivery output remain outside the claim.

To verify Cloud DNS managed zones and A records through the generated
`google.golang.org/api/dns/v1` client across restart, including the loopback UDP
resolver and cleanup durability, run:

```bash
make test-phase16-dns
```

This guarded gate uses isolated temporary state, dynamic loopback TCP and UDP
ports, and no Docker. DNS mutation bodies are limited to 1 MiB, RRSet data and
change batches have MiniSky-specific safety bounds, and the gate does not imply
pagination, DNSSEC signing, recursion, private-network enforcement, or broader
DNS protocol support.

To verify the Phase 16 custom-network and regional-subnetwork slice through the
generated `google.golang.org/api/compute/v1` client and a real Docker bridge,
start Docker and install `curl`, `docker`, `go`, and `python3`, then run:

```bash
make test-phase16-subnetwork
```

This opt-in gate creates one fixed-name custom network and one regional IPv4
subnetwork in isolated temporary MiniSky state. It proves generated-client
global and regional operation polling, stable metadata across restart, exact
Docker bridge labels/IPAM, bridge-ID preservation during reconciliation, and
durable API cleanup after a third start. It does not claim auto-mode networks,
IPv6, secondary ranges, workload attachment, routes, firewall isolation, or
general GCP VPC parity.

The harness serializes access to MiniSky's fixed `minisky-net`, refuses to run
if that network already exists, and chooses a canonical `/24` that does not
overlap any CIDR reported by the current Docker daemon. Cleanup removes only
captured immutable Docker network IDs after exact ownership-label checks; it
never prunes Docker or deletes a bridge by name alone.

To verify the generated Vertex AI Go client against MiniSky's bounded
deterministic endpoint prediction path, including an exact response comparison
across a clean daemon restart, run:

```bash
make test-phase16-vertex
```

This guarded gate uses an isolated temporary profile and response evidence file,
dynamic loopback ports, and no Docker. It proves restart determinism for fixed
inputs and parameters; predictions themselves are stateless and are not
persisted.

For the Phase 17 CI, plugin, control, offline-bundle, and Compose checks, run:

```bash
make test-phase17
make benchmark
```

To run the guarded enterprise WIF/RBAC/quota/audit cross-gate, start Docker and
install `curl`, `docker`, `go`, `openssl`, `python3`, and Terraform, then run:

```bash
make test-phase17-enterprise
```

The target supplies the required `MINISKY_PHASE17_ENTERPRISE_INTEGRATION=1`
guard. It creates isolated temporary state and Docker resources but is an
explicit local integration run, not part of the default contributor checks.

---

## ✅ Pull Request Process

1.  **Format**: Ensure your code is formatted with `gofmt`.
2.  **Tests**: Add or update tests for changed behavior, then run `go test -race ./cmd/... ./pkg/...`.
3.  **Documentation**: Update the `CLI Reference` or `User Guide` if you added user-facing flags or commands.
4.  **Screenshot**: If you modified the UI, include a screenshot in your PR description.

We are excited to see what you build!
