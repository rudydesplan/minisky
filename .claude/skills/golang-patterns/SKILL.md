---
name: golang-patterns
description: Idiomatic Go guidance for MiniSky handlers, shims, registry code, state adapters, Docker orchestration, concurrency, and reviews. Use when writing or refactoring Go under cmd/, pkg/, or ui/*.go; not for Terraform, React-only work, or generic API contract design.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# Go Patterns for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve the upstream
license and attribution when redistributing this material.

## Apply This Skill

Use for Go implementation or review in `cmd/minisky`, `pkg/config`,
`pkg/dashboard`, `pkg/orchestrator`, `pkg/registry`, `pkg/router`, `pkg/shims`,
`pkg/state`, `pkg/validator`, and Go-backed embedded UI code.

Do not use it to invent a new project layout, require third-party linters that
the repository does not configure, or override GCP API fidelity requirements.
For shim response contracts, also apply `minisky-quality-floor` and
`gcp-api-fidelity`.

## Repository Shape

- The module path is `minisky`; use the Go version declared in `go.mod`.
- `cmd/minisky` owns Cobra commands and process lifecycle.
- `pkg/router` resolves host/path routing and runs request validation.
- `pkg/registry` is the source of registered service domains.
- `pkg/shims/<service>` owns service behavior; registration is performed by the
  existing registry-init pattern.
- `pkg/state` owns versioned, profile-scoped persistence.
- `ui` contains the React app plus Go embedding/API integration.

Match nearby code. Do not impose an `internal/` hierarchy or create interfaces,
functional options, or packages for a single use.

## Go Design Rules

- Prefer clear, direct code, useful zero values, early returns, and small
  consumer-defined interfaces.
- Accept `context.Context` as the first parameter for cancellable operations;
  propagate it into HTTP, Docker, and backend calls.
- Wrap errors with operation context using `%w`; use `errors.Is`/`errors.As`
  where callers need classification.
- Do not expose raw internal errors in HTTP responses. Translate them to the
  service's GCP-shaped error contract.
- Avoid mutable package globals except deliberate registration performed by
  `init()`. Protect shared shim/state maps consistently and verify with `-race`.
- Every goroutine needs an owner, cancellation path, and bounded shutdown.
  Avoid sends that can block after cancellation.
- Close response bodies and other resources; if cleanup errors are intentionally
  ignored, make that choice visible.
- Optimize only from evidence. Preallocation is useful when size is known;
  `sync.Pool` and custom concurrency machinery require benchmark evidence.

## MiniSky-Specific Invariants

- Unsupported operations return an explicit GCP-shaped unsupported error; never
  return fake success.
- Preserve the distinction between simulation, metadata-only, emulator-backed,
  and executable behavior. Do not claim backend fidelity from accepted input.
- The `simulation` runtime profile must not unexpectedly start resource-heavy
  backends. Kind, Buildpacks, and DuckDB are dependency- and profile-gated.
- Persistence writes must remain atomic and profile-scoped. Rehydration must
  not replay side effects or silently recreate missing Docker resources.
- Treat profile state and exports as sensitive: they may contain secrets, key
  material, source, and environment values.
- Keep platform-specific behavior behind the existing file/build-tag pattern;
  confirm Unix and Windows variants remain coherent.

## Validation

Run the narrow package test first, then the repository's Go quality gates:

```bash
gofmt -w <changed-go-files>
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
```

CGO-sensitive BigQuery work additionally requires the supported native path:

```bash
CGO_ENABLED=1 go test -count=1 ./pkg/shims/bigquery
```

Also test the no-CGO behavior when changing build tags or backend selection.
Do not advertise `go test ./...`, `staticcheck`, or `golangci-lint` as project
gates unless repository configuration is added and verified.
