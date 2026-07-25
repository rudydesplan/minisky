---
name: minisky-shim-builder
description: Implement or extend a MiniSky GCP service shim under pkg/shims, including domain registration, router exposure, manifest metadata, request validation, long-running operations, persistence hooks, and focused tests. Use for requests such as “add a service shim,” “register a googleapis.com domain,” “add a CRUD method,” or “wire a Docker-backed emulator.” Do not use for a broad end-to-end service design (use minisky-craft) or a bounded defect repair (use minisky-polish).
---

# MiniSky Shim Builder

Build the smallest service surface that matches a verified GCP contract. Inspect
nearby shims before choosing file layout; packages are not required to use
`api.go`, `types.go`, or any other fixed split.

## Choose the registration path

Use a custom Go handler when MiniSky owns routing, metadata, validation, LROs,
or cross-service behavior:

```go
func init() {
	registry.Register("myservice.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.OpMgr, ctx.SvcMgr)
	})
}
```

Then add exactly one blank import to `pkg/shims/registry_init.go`; this is what
executes the package `init`:

```go
_ "minisky/pkg/shims/myservice"
```

Use `registry.RegisterLazyDocker(domain)` in the `init` of
`pkg/shims/registry_init.go` only for a pure emulator passthrough with no custom
handler. `cmd/minisky/start.go` passes custom handlers to
`router.RegisterShim` and lazy domains to `router.RegisterLazyDocker`; do not
register routes directly from a shim.

If one handler serves several domains, register the same factory for each, as
Bigtable, Memorystore, and Serverless do.

## Keep registry metadata coherent

Add every registered domain to `serviceManifest` in
`pkg/registry/manifest.go` with the exact current enums:

- fidelity: `FidelityHigh`, `FidelityStandard`, or `FidelityPassthrough`;
- persistence: `PersistenceMemory`, `PersistenceFile`,
  `PersistenceDocker`, `PersistenceHybrid`, or `PersistenceStatic`;
- `probeUnsupported`: normally `true` for in-process factories and `false`
  only when constructing/probing the service requires a lazy Docker backend.

`registry.Services()` rejects missing and stale entries. Also add the domain to
`docs/service-compatibility.md` only when documentation is in the authorized
scope; `pkg/registry/manifest_test.go` checks both registration drift and docs.

## Match the gateway contract

The router resolves host-based requests and canonical local endpoints:
`/_minisky/<first-domain-label>/<service-path>` or
`/_minisky/<full-domain>/<service-path>`. It rewrites the canonical prefix,
validates against the resolved domain, then dispatches. First-label aliases
that collide are deliberately disabled. Add or change legacy path guessing in
`pkg/router/proxy.go` only when a real supported client requires it, with a
focused `proxy_test.go` case.

Implement `ServeHTTP` with exact method/path matching. Distinguish:

- malformed input: the service-correct 4xx GCP error;
- missing resource: 404 `NOT_FOUND`;
- a recognized MiniSky stub: 501 `UNIMPLEMENTED`, never placeholder success;
- an unsupported HTTP verb on an otherwise supported resource: use the real
  service behavior (often 405), not an automatic 501;
- unknown domain or canonical selector: the router already returns 501.

Do not use `http.Error`; it emits plain text. Set `Content-Type` before
`WriteHeader`, then encode `{"error":{...}}`.

## Add mutation validation

Add only verified create/update rules to the `embeddedRules []ServiceSchema` in
`pkg/validator/discovery.go`:

```go
{
	Domain: "myservice.googleapis.com",
	Methods: []MethodSchema{{
		HTTPMethod:  "POST",
		PathGlob:    "/v1/projects/*/locations/*/resources",
		ContentType: "application/json",
		RequiredQuery: []string{"resourceId"},
		RequiredBody: []BodyField{{
			Path: "config.name", Type: "string",
			Message: "field 'config.name' is required for resources.create",
		}},
	}},
}
```

The validator runs before the handler, restores JSON request bodies after
inspection, and allows domains or method/path pairs without a rule. A `*`
matches one non-empty path segment; prefixes are not stripped. Supported body
types are `string`, `integer`, `boolean`, `object`, and `array`.

## Use the existing LRO manager

When real GCP returns an asynchronous operation, use the injected
`*orchestrator.OperationManager`:

```go
op := api.opMgr.Register("myservice#operation", "CREATE", targetLink, zone, region)
api.opMgr.RunAsync(op.Name, func() error {
	return api.createResource(...)
})
_ = json.NewEncoder(w).Encode(op)
```

`Register` creates a `PENDING` operation. `RunAsync` exposes
`PENDING -> RUNNING -> DONE`, sets progress/timestamps, and records work errors
with `Fail`. Add the service-specific polling path and call
`api.opMgr.Get(opName)`; return 404 when absent. Do not invent a generic
`operations/...` name or a `response` field: the current operation shape uses
`name`, `kind`, `operationType`, `status`, `progress`, `done`, optional scope,
metadata, and error.

## Add persistence without using registry.Context

`registry.Context` exposes only `OpMgr`, `SvcMgr`, and `GetShim`. Persistent
shims currently open their profile store in `NewAPI`:

```go
store, err := state.New(config.GetStateDir(), config.GetProfile())
```

Handle both return values. Provide an injectable `NewAPIWithStore(...,
*state.Store) (*API, error)` (or a narrow store interface) so restart and corrupt
state are testable. Follow `minisky-state-persistence` for snapshots, locking,
and export/import boundaries.

## Wire cross-service behavior after boot

Factories run before every shim is available. Implement `registry.PostBoot`
only when cross-shim wiring is required:

```go
func (api *API) OnPostBoot(ctx *registry.Context) {
	if dependency, ok := ctx.GetShim("dependency.googleapis.com").(*dependency.API); ok {
		api.dependency = dependency
	}
}
```

`BootAll` performs this second pass. `ContractHandlers` intentionally omits
`PostBoot`, so unsupported-route contract tests must not depend on that wiring.

## Work test-first

Repeat one vertical slice at a time:

1. Add the smallest handler, validator, router, persistence, or LRO test.
2. Run it and confirm the intended failure.
3. Implement only enough behavior to pass.
4. Run the package test with `-race`.
5. Add the next slice.

Cover the real lifecycle and exact response shape, not only status codes. Add
shared tests when applicable:

```bash
go test -race ./pkg/shims/<service>
go test -race ./pkg/validator ./pkg/router ./pkg/registry
```

Then run `go test -race ./...` when the machine supports the repository's CGO
dependencies. MiniSky imports `go-duckdb`; release and conformance builds use
`CGO_ENABLED=1`, with Clang on Darwin and GCC/UCRT on Windows. Docker-, Kind-,
Buildpacks-, and Terraform-backed checks require their own prerequisites and
must not be reported as run when unavailable.

Finish only when registration, manifest, validation, public routing, supported
lifecycle, unsupported semantics, LRO polling (if any), persistence restart (if
any), and claimed executable backend behavior have direct evidence.
