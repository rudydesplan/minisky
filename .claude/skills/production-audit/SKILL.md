---
name: production-audit
description: Evidence-based release-readiness audit for MiniSky binaries, container image, CI, CGO platforms, state safety, Docker isolation, GCP/Terraform fidelity, optional backends, and documentation. Use for ship/readiness questions; not for compliance certification or implementation.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# MiniSky Production Audit

This maintainer-safe adaptation retains the useful community/ECC readiness lens
without unpinned remote execution or third-party source upload. Preserve source
attribution and applicable license notices when redistributing it.

## Applicability

Use for “ready to ship?”, release-candidate, post-merge, or “what breaks in
production?” audits. This is read-only engineering triage unless fixes are
separately requested. It is not a formal security/compliance audit and must not
claim GCP parity.

## Evidence Order

1. Establish intended version, platforms, artifacts, and changed surface.
2. Inspect CI/release configuration and actual recent results if available.
3. Inspect `go.mod`, Dockerfile, runtime profiles, state model, service and
   Terraform compatibility, and relevant implementation/tests.
4. Run local non-destructive gates proportionate to scope.
5. Report blockers, caveats, evidence, and missing evidence.

Do not run unpinned remote tools, upload source/state, mutate Git, remove Docker
resources, touch active profiles, or run Terraform apply without explicit scope
and the guarded isolated script.

## MiniSky Risk Lenses

### Build and distribution

- UI assets are built before Go embedding.
- Go version/dependencies are reproducible; `gofmt`, `vet`, race tests, and
  `-trimpath` build pass.
- Native CGO/DuckDB artifacts work on claimed platforms; Windows has no
  unintended MinGW runtime DLL dependency.
- Container runtime is compatible with glibc-linked DuckDB and includes required
  Docker CLI/Pack tooling; checksummed downloads and architecture mapping hold.

### Runtime and Docker

- Default simulation does not unexpectedly provision Kind/Buildpacks/DuckDB.
- Required versus optional doctor failures are accurate.
- Docker socket privilege is documented; container/network/volume ownership and
  cleanup cannot disturb user resources.
- Rehydration does not expose stale ports or silently recreate missing backends.

### API fidelity

- Registered domains match manifest/docs/tests.
- Errors are GCP-shaped; invalid requests fail before mutation.
- Unsupported methods return 501/`UNIMPLEMENTED`, not fake success.
- LRO transitions and compatibility tiers are honest; static or metadata-only
  behavior is not described as executable.

### State and Terraform

- State writes are atomic, versioned, profile-scoped, permission-restricted, and
  race-safe. Imports reject traversal/newer schemas before side effects.
- Sensitive profile/export content is documented and absent from logs/artifacts.
- Terraform validation passes. Any compatibility claim has isolated apply →
  assertions → no-drift plan → destroy evidence, with collision and cleanup
  guards preserved.

### Operations and UI

- Startup/shutdown, ports, diagnostics, logs, and recovery/rollback are tested.
- Dashboard lint/build pass; UI claims reflect backend state and do not expose
  sensitive values.
- Release notes identify state compatibility, unsupported behavior, and optional
  paths not tested.

## Local Gates

```bash
cd ui && npm ci && npm run lint && npm audit --audit-level=high && npm run build
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false -input=false -lockfile=readonly
terraform -chdir=terraform validate
```

Run GoReleaser, native platform, container, Terraform integration, Kind, or
Buildpacks checks only when dependencies and audit scope support them. Mark
unrun checks as missing evidence.

## Output

Lead with `ship`, `ship with caveats`, or `block`, plus confidence. Then list
blockers, high-value fixes, evidence checked, evidence missing, and one next
verification. Scores are optional; if used, explain that they prioritize risk
rather than measure certainty. A green CI run cannot erase an untested state
migration, unsafe Docker cleanup, false compatibility claim, or absent rollback.
