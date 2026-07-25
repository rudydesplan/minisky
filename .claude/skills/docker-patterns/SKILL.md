---
name: docker-patterns
description: Docker design and debugging for MiniSky's CGO-enabled image, Docker-socket orchestration, emulator containers, volumes, networks, Buildpacks, and optional Kind backend. Use for Dockerfile or container lifecycle work; not for generic Compose stacks unrelated to MiniSky.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# Docker Patterns for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

Use when changing the root `Dockerfile`, Docker-backed shims, orchestrator,
network/volume behavior, container release checks, or Docker diagnostics. Do not
assume MiniSky is a conventional stateless web container or recommend deleting
volumes/networks as routine cleanup.

## Current Image Contract

- The UI is built with Node and embedded into the Go binary.
- The Go build is CGO-enabled because native DuckDB requires libc/compiler
  compatibility at build time.
- The runtime is distroless `cc-debian12`, not Alpine or `scratch`.
- The image includes Docker CLI and `pack`; it does not include a Docker daemon.
- MiniSky requires the host Docker socket for Docker-backed services.
- Ports 8080 and 8081 are exposed.
- `TARGETARCH` supports the explicitly handled architectures only.

Do not replace the CGO build with `CGO_ENABLED=0`, copy a glibc-linked binary
into musl Alpine, or claim a fully static binary. Preserve checksum verification
for downloaded tools and keep tool/image versions pinned.

## Docker Lifecycle Rules

- Namespace managed containers, networks, and volumes consistently and detect
  collisions before mutation.
- Never remove or reuse unrelated user Docker resources.
- Treat container IDs and host ports as ephemeral. Rehydrated metadata must not
  expose stale endpoints or silently recreate missing workloads.
- Use profile-scoped durable storage only where the service can safely support
  it; document memory, file, Docker, and hybrid persistence honestly.
- Cleanup must be idempotent and bounded. Report partial failure rather than
  hiding leaked resources.
- Propagate cancellation/timeouts into Docker operations.
- Avoid privileged mode. Mounting the Docker socket is already high privilege;
  document that trust boundary and never expose the daemon remotely.
- Keep secrets out of image layers, build args, logs, labels, and exported state.

## Optional Backends

`simulation` is the default runtime profile. The `full` profile requests real
backends only when dependencies exist. Kind and Buildpacks remain optional;
DuckDB requires CGO. Explicit per-backend overrides take precedence. Missing
`kind`, `pack`, or Docker must produce a diagnostic and documented fallback,
not an unsupported success claim.

## Safe Debugging

Prefer read-only inspection first:

```bash
docker info
docker ps -a
docker network inspect minisky-net
docker logs <managed-container>
```

Commands such as `docker system prune`, `docker compose down -v`, broad container
removal, or deleting `~/.minisky` are destructive and require explicit user
approval. Never use them as generic troubleshooting steps.

## Verification

Build with the actual multi-stage Dockerfile and smoke-test the resulting image
with an isolated state directory and ports. Check both amd64 and arm64 when
architecture/tool download logic changes. For Docker-backed tests, use unique
resource names, guaranteed cleanup, and skip precisely when Docker is absent.
Confirm native DuckDB separately with `CGO_ENABLED=1`.
