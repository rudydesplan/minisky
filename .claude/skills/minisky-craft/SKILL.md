---
name: minisky-craft
description: Deliver a new MiniSky GCP service shim or major service capability end to end, from a verified REST contract and fidelity boundary through TDD, registration, routing, validation, LROs, persistence, backend integration, and executable evidence. Use for “add support for a GCP service,” a greenfield shim, or a substantial new service surface. Do not use for a narrow bug fix (minisky-polish), package mechanics alone (minisky-shim-builder), or a read-only audit.
license: Apache-2.0; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: Impeccable
---

# MiniSky Craft

Orchestrate a truthful vertical slice. Read the narrower skill when entering its
phase:

- `gcp-api-fidelity` for service-specific HTTP, error, list, and operation
  semantics;
- `minisky-shim-builder` for package, registry, router, validator, and manifest
  mechanics;
- `minisky-state-persistence` for durable profile metadata and rehydration;
- `minisky-contract-test` for validator, gateway, lifecycle, unsupported, and
  restart evidence.

## 1. Define a bounded contract

Before editing, write down:

- service domains, REST versions, methods, and exact paths;
- request identifiers, required fields/types, response fields, list keys, and
  success codes;
- duplicate, missing, malformed, unsupported, and delete semantics;
- synchronous behavior or exact service operation/polling paths;
- metadata simulation, executable local behavior, and emulator passthrough;
- durable metadata, transient operation state, and external files, containers,
  volumes, networks, or DuckDB data;
- target manifest fidelity: `high`, `standard`, or `passthrough`;
- target persistence: `memory`, `file`, `docker`, `hybrid`, or `static`;
- one concrete Terraform, gcloud, SDK, or HTTP workflow that defines success.

Verify the slice against the current GCP REST reference/Discovery document,
MiniSky's existing routes and shims, and the supported client's actual requests.
Do not infer broad service parity from one CRUD path.

## 2. Map the repository integration

For a custom in-process or hybrid shim:

1. package under `pkg/shims/<service>/`;
2. `init` calls `registry.Register(domain, factory)`;
3. blank import in `pkg/shims/registry_init.go`;
4. service metadata in `pkg/registry/manifest.go`;
5. targeted mutation rules in `pkg/validator/discovery.go`;
6. optional `registry.PostBoot` for cross-shim wiring.

For pure Docker passthrough with no custom handler, use
`registry.RegisterLazyDocker` in `pkg/shims/registry_init.go`.

The startup path calls `registry.BootAll`, registers custom handlers with
`router.RegisterShim`, and lazy domains with `router.RegisterLazyDocker`. The
router resolves host or canonical `/_minisky/<selector>/...` requests, rewrites
the service path, validates against the resolved domain, and then dispatches.

`registry.Context` exposes `OpMgr`, `SvcMgr`, and `GetShim`; it does not expose
a state store.

## 3. Establish executable acceptance tests

Choose the smallest failing test for one vertical slice:

- handler request parsing and service-correct GCP JSON errors;
- create -> get/list -> delete -> not found;
- duplicate or precondition behavior;
- recognized unsupported method returns explicit failure, never fake success;
- operation response and service-specific polling;
- restart round-trip and corrupt-state behavior;
- gateway/canonical endpoint and resolved-domain validation;
- `pkg/validator/phase6_test.go` mutation rule;
- `pkg/registry/manifest_test.go` registration and unsupported-route contract;
- real backend/client behavior when executable compatibility is claimed.

Run the test and confirm it fails for the intended missing behavior. Then
implement only enough to pass. Repeat per slice; do not batch speculative
features.

## 4. Implement repository-accurate behavior

### Registration and validation

Use exact `ServiceSchema`, `MethodSchema`, and `BodyField` fields:
`Domain`, `Methods`, `HTTPMethod`, `PathGlob`, `ContentType`,
`RequiredQuery`, `RequiredBody`, `Path`, `Type`, and `Message`.
`*` matches one non-empty segment and paths are not version-stripped.

Set `Content-Type` before status and emit `{"error":{...}}`; never use
`http.Error`. Keep malformed, missing, duplicate, unsupported, and method-not-
allowed behavior distinct.

### Long-running operations

For truly asynchronous methods, inject `ctx.OpMgr`, call:

```go
op := api.opMgr.Register(kind, operationType, targetLink, zone, region)
api.opMgr.RunAsync(op.Name, work)
```

Add the exact service polling path using `api.opMgr.Get`. The current manager
exposes `PENDING -> RUNNING -> DONE`, progress, `done`, timestamps, scope,
metadata, and `{code,message}` errors. It does not persist operations or add a
generic response resource.

### Persistence

Persistent shims open:

```go
store, err := state.New(config.GetStateDir(), config.GetProfile())
```

Handle both results and provide an injectable `NewAPIWithStore` for tests.
Snapshot under locks, save without holding the API mutex, serialize concurrent
saves where required, and reject corrupt state without overwriting it. Export
and import cover metadata entries only, not DuckDB files or Docker resources.

For hybrid resources, rehydrate truthfully: do not silently recreate or adopt a
container. Restore metadata-only/degraded status or reconcile owned resources
according to an explicit, tested design.

## 5. Verify in widening rings

After each red-green slice:

```bash
go test -race ./pkg/shims/<service>
go test -race ./pkg/validator ./pkg/router ./pkg/registry ./pkg/state
```

Then run `go test -race ./...` when the native toolchain supports CGO.
MiniSky imports `go-duckdb`; CI and releases use `CGO_ENABLED=1`, with Clang on
Darwin and GCC/UCRT on Windows. Do not treat a `CGO_ENABLED=0` failure as a shim
regression without diagnosis.

Run Docker-, Kind-, Buildpacks-, or backend-specific checks only when their
prerequisites are available. Run
`MINISKY_TERRAFORM_INTEGRATION=1 scripts/terraform-integration.sh` only when the
Docker-backed workflow is in scope and authorized.

## 6. Close the claim

If documentation changes are authorized, update existing compatibility docs
using only demonstrated language:

- metadata accepted/returned;
- simulated execution;
- executable backend behavior;
- emulator passthrough;
- explicit unsupported methods;
- state-exported metadata versus excluded binary/external data.

Finish when every claimed client workflow has direct evidence, the registry and
manifest agree, public routing and validation work, unsupported behavior fails
honestly, async methods poll correctly, durable metadata survives restart, and
race/focused tests pass. Report environmental checks that were not run and
qualify the corresponding support claim.
