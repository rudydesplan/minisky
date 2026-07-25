---
name: minisky-audit
description: Audit an already implemented MiniSky shim or service domain read-only for API fidelity, registration, routing, persistence, concurrency, backend behavior, and executable coverage. Trigger on "audit this shim", "assess service readiness", "verify this service", "check fidelity", "review persistence classification", or requests for a ranked defect inventory. Exit with evidence-backed findings, verification results, and unknowns. Do not edit code, judge a proposal's architecture, fix a known defect list, or replace a repository-wide release audit.
license: Apache-2.0; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: Impeccable
---

# MiniSky Audit

## Entry and exit

Enter only when an implementation exists and the requested outcome is a
read-only inventory of defects or unsupported claims. Do not mutate files or
external systems.

Exit with:

- findings ordered by evidence-based severity;
- inspected scope and claimed manifest classification;
- exact commands and results;
- skipped checks, prerequisites, and unresolved unknowns.

Hand verified bounded defects to `minisky-polish`, a resilience objective
without a known defect list to `minisky-harden`, and a new architecture or
service surface to `minisky-craft`. Use `minisky-critique` when the primary
question is whether a proposed design choice is sound.

## Evidence to inspect

Start with the target package and tests, then inspect only relevant shared
contracts:

- `pkg/shims/<service>/` and its tests
- `pkg/shims/registry_init.go`
- `pkg/registry/registry.go`, `pkg/registry/manifest.go`, and
  `pkg/registry/manifest_test.go`
- matching `pkg/validator/discovery.go` rules and
  `pkg/validator/phase6_test.go` cases
- `pkg/orchestrator/operations.go` if the service returns operations
- `pkg/state/store.go` and restart tests for `file` or `hybrid` services
- `docs/contribution-guide.md`, `docs/cli_reference.md`, and
  `docs/user-guide.md` only for claims those files make
- `docs/terraform.md`, `terraform/`, and `scripts/terraform-integration.sh`
  only for Terraform claims

Confirm every cited path exists. Compare the implemented slice with the
specific REST API or client workflow, not an assumed universal GCP shape. Treat
`pkg/validator/discovery.go` as a curated subset of mutation rules, not full
Discovery Document enforcement.

## Audit dimensions

Report evidence and findings for each applicable dimension:

1. **API fidelity:** paths, verbs, status codes, fields, list shape, errors, and
   operation polling match the claimed service slice.
2. **Registry and routing:** factory or lazy-Docker registration, blank import,
   manifest metadata, gateway routing, and documentation agree.
3. **Persistence and concurrency:** claimed metadata survives restart;
   snapshots, nested maps, and saves are race-safe; failed required saves are
   not acknowledged as durable success; export exclusions are clear.
4. **Executable behavior:** metadata simulation, local execution, and emulator
   passthrough are distinguished; unsupported behavior fails explicitly.
5. **Verification:** focused, race, restart, backend, and client-tool evidence
   supports each claim.

Manifest values are defined in `pkg/registry/manifest.go`: fidelity
`high|standard|passthrough` and persistence
`memory|file|docker|hybrid|static`.

## Verification commands

Run the concrete target package path and report that exact command. For example,
an audit of Compute uses:

```bash
go test -race ./pkg/shims/compute
```

For a cross-shim claim, use:

```bash
go test -race ./pkg/shims/...
```

When registry or gateway validation is in scope, use:

```bash
go test -race ./pkg/validator ./pkg/registry
```

For a repository-wide Go claim, use the same scope as CI when CGO requirements
are available:

```bash
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
```

The Terraform integration creates and destroys resources, requires
`curl`, `docker`, `go`, `python3`, and `terraform`, and refuses to run without
explicit opt-in:

```bash
MINISKY_TERRAFORM_INTEGRATION=1 ./scripts/terraform-integration.sh
```

Do not run it unless the user authorized that scope and prerequisites are
available. Record checks not run; absence of evidence is not a pass.

## Severity

Assign severity from demonstrated impact, not keywords or hypothetical worst
cases:

- **Critical:** reproduced evidence shows unrecoverable data loss or corruption,
  a security-boundary escape, or false success that causes destructive
  follow-on work.
- **High:** reproduced evidence shows a required create/read/update/delete,
  routing, polling, restart, or persistence workflow is incorrect or unusable.
- **Medium:** evidence shows a meaningful validation, concurrency, error,
  compatibility, or test gap outside the normal lifecycle.
- **Low:** evidence shows documentation, diagnostics, or maintainability drift
  with no demonstrated behavioral failure.

If the behavior follows directly from a deterministic code path, cite that path
and bound the impact. Otherwise reproduce it or label it an unverified risk.
Lower severity when impact is inferred. A missing test is not proof of broken
behavior.

## Report

Return findings first, ordered by severity. Each finding includes a file and
line, observed behavior, demonstrated or bounded impact, evidence, and a
concrete recommendation.
Then list scope, manifest classification, commands and results, and skipped
checks. Use `No findings` only after inspecting every applicable dimension.
Avoid numeric scores and “production-ready” labels unless the user supplied a
scoring model and production-like workflows were actually exercised.
