---
name: codebase-onboarding
description: Evidence-based onboarding for the MiniSky Go emulator, React dashboard, shim registry, state model, Docker backends, Terraform example, CI, and releases. Use for repo walkthroughs or contributor guides; not for automatically generating CLAUDE.md or copying README claims without verification.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# MiniSky Codebase Onboarding

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

Use when a developer asks how MiniSky is structured, where to make a change, or
how to contribute. Do not write onboarding files, CLAUDE.md, or rules unless the
user asks. Do not rely solely on README/roadmap claims; verify code, tests, CI,
and compatibility docs.

## Reconnaissance

Inspect selectively:

- `go.mod` for module/toolchain and dependencies;
- `cmd/minisky` for Cobra commands and lifecycle;
- `pkg/router`, `pkg/validator`, and `pkg/registry` for request flow/contracts;
- `pkg/shims` and `pkg/shims/registry_init.go` for service implementations;
- `pkg/orchestrator` and `pkg/config` for Docker and runtime profiles;
- `pkg/state` plus `docs/state-model.md` for persistence;
- `ui/package.json`, `ui/src`, and Go embedding code for dashboard architecture;
- `terraform`, SDK smoke suites, and `scripts/terraform-integration.sh`;
- `.github/workflows`, Dockerfile, and release configuration;
- focused tests that prove claimed behavior.

Do not read every file. Trace one representative request from gateway routing →
validation → registered shim → response/LRO/state, and one Docker-backed or
persistent lifecycle if relevant.

## Facts to Explain

- MiniSky is a single Go module (`minisky`) with a Cobra binary at
  `cmd/minisky` and an embedded React/Vite dashboard.
- Service shims live under `pkg/shims`; registry/manifest data controls domains
  and compatibility gates.
- The default runtime profile is simulation. Native DuckDB needs CGO; Kind and
  Buildpacks are optional and dependency-gated.
- Persistence is incremental and profile-scoped. Some services remain memory,
  Docker, hybrid, or static; state exports can contain sensitive values.
- Host-based GCP routing and explicit localhost path routes coexist.
- Terraform compatibility must be proven with isolated apply, no-drift plan,
  and destroy—not inferred from CRUD handlers.

## Contributor Commands

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

Kind/Pack/Docker and Terraform integration are optional or guarded paths; label
them clearly rather than making them universal setup requirements.

## Output

Provide: purpose, architecture/request-flow map, key entry points, verified
commands, where to add a shim/test/state adapter/dashboard feature, runtime and
CGO constraints, current fidelity/persistence caveats, and a small first task.
Cite files and separate verified facts, inferred conventions, and unknowns.
Avoid unsupported counts, performance claims, or “full GCP parity” language.
