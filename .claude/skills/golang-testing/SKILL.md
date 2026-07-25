---
name: golang-testing
description: Go testing patterns for MiniSky shims, handlers, state, routing, runtime profiles, CGO paths, and regressions. Use for *_test.go, contract tests, race failures, fuzzing, or benchmarks; not for React-only tests or Terraform lifecycle acceptance.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# Go Testing for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

Use when adding or reviewing Go tests under `cmd/`, `pkg/`, or `ui/*.go`.
Use `tdd-workflow` for the overall red/green loop and
`minisky-contract-test` for a registered service-domain lifecycle contract.
Do not require a universal coverage percentage: MiniSky CI currently gates
behavior, formatting, vet, race tests, and builds rather than a numeric target.

## Test Shape

- Put tests beside implementation as `*_test.go`; prefer table-driven subtests
  for routes, status codes, validation, backend selection, and state versions.
- Test observable behavior, not private implementation details.
- Use `t.Helper`, `t.Cleanup`, `t.TempDir`, and `httptest` where appropriate.
- Keep tests isolated from `~/.minisky`, the developer's Docker resources, and
  Terraform state. Override `HOME`/`MINISKY_STATE_DIR` with a temporary path.
- Do not use `t.Parallel()` when tests mutate environment variables, singleton
  registries, process-wide state, fixed ports, or Docker resources.
- Avoid sleeps. Synchronize on channels/state or poll with a bounded deadline.
- Assert errors rather than discarding body reads or cleanup failures.

## Required Behavioral Coverage

For handler or shim changes, cover the relevant subset:

1. create/get/update/delete behavior and stable resource identity;
2. malformed JSON, missing required fields, wrong methods, and not-found paths;
3. exact HTTP status plus GCP JSON error fields (`error.code`, `message`,
   `status`, and required details);
4. unsupported operations returning HTTP 501/`UNIMPLEMENTED`, never fake success;
5. LRO pending/terminal transitions where the service uses operations;
6. restart/rehydration for persistent shims, including no side-effect replay;
7. simulation versus real backend selection and missing-dependency fallback;
8. concurrent access for shared maps, queues, state, or operation managers.

Docker-backed tests must use unique names and guaranteed cleanup, and must skip
with a specific reason when Docker is unavailable. Kind is optional: only run
Kind-backed tests when explicitly selected and dependencies are present.

## Commands

Run the smallest target during development:

```bash
go test -count=1 ./pkg/shims/<service>
go test -run 'TestName/subcase' -count=1 ./pkg/<package>
```

Before handoff, mirror CI:

```bash
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
```

For native DuckDB paths:

```bash
CGO_ENABLED=1 go test -count=1 ./pkg/shims/bigquery
```

When build tags or fallback behavior changes, also exercise the no-CGO tests.
Use Go benchmarks (`go test -bench=. -benchmem`) only for a named performance
question; compare repeated runs under equivalent runtime/backend conditions.
Fuzz parsers and validators with bounded inputs, seed valid GCP payloads, and
preserve any useful crashing corpus.
