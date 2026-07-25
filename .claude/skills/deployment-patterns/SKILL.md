---
name: deployment-patterns
description: MiniSky CI, release, container distribution, smoke testing, rollback, and compatibility guidance. Use for GitHub Actions, GoReleaser, Docker releases, or launch preparation; not for asserting an unconfigured Kubernetes or hosted deployment strategy.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# Deployment Patterns for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

Use for `.github/workflows`, release configuration, the root Dockerfile,
packaging, checksums, native binaries, or deployment/runbook changes. Do not
invent blue-green/canary infrastructure, health endpoints, autoscaling, or
production hosting claims that are absent from the repository.

## Existing Gates

Mirror current CI rather than generic examples:

```bash
cd ui && npm ci && npm run lint && npm run build
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false -input=false -lockfile=readonly
terraform -chdir=terraform validate
```

CI also validates the GoReleaser configuration, builds a Linux amd64 snapshot,
and exercises native CGO/DuckDB paths on supported platforms. Windows CGO
artifacts are audited for unintended MinGW runtime DLL dependencies.

## Release Invariants

- Build UI assets before the Go binary so `go:embed` has real output.
- Preserve `-trimpath`, pinned dependencies/tooling, checksums, and immutable
  release artifacts.
- Keep CGO/native DuckDB constraints explicit per target platform; do not claim
  a static or no-CGO equivalent.
- The container runtime needs glibc-compatible libraries, Docker CLI, `pack`,
  and host Docker-socket access.
- Smoke-test the installed artifact, not only the source-tree command.
- `minisky doctor` should distinguish required failures from optional Kind/Pack
  dependencies and report nonzero only for required failures.
- Compatibility documentation must distinguish simulation, metadata-only,
  emulator-backed, and executable behavior.

## Terraform Release Safety

The apply/no-drift/destroy workflow is opt-in and Docker-backed. Run only
`scripts/terraform-integration.sh` with
`MINISKY_TERRAFORM_INTEGRATION=1`. It refuses existing MiniSky Docker resources,
uses temporary HOME/TF state, locks concurrent runs, and destroys its isolated
resources. Do not weaken those guards or point automation at user state.

## Rollback

Rollback means restoring a previously checksummed binary/image and preserving
compatible profile state. Before release, assess state schema compatibility,
Docker resource reconciliation, and whether older binaries can read newer
state. Never describe rollback as safe when a state migration is irreversible;
provide export/recovery instructions and test them in an isolated profile.

## Handoff

Report exact artifacts and platforms tested, optional paths not exercised
(Kind, Buildpacks, native DuckDB), compatibility changes, state migration risk,
and the concrete rollback artifact. Green unit tests alone are not release
readiness.
