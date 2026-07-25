---
name: database-migrations
description: Safe evolution of MiniSky's versioned profile state, exported snapshots, DuckDB metadata/data, emulator volumes, and persistence adapters. Use for schema versions, rehydration, import/export, or data migration; not for generic PostgreSQL/ORM migrations absent from this repository.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# State and Data Migrations for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

MiniSky currently has no application relational-schema migration framework.
Use this skill when changing `pkg/state`, persistent shim adapters, profile
layout, exported snapshots, DuckDB storage, or emulator-backed durable data.
Do not introduce Prisma/Django/golang-migrate guidance unless the repository
actually adopts that technology.

## State Contract

Profile state is versioned and rooted below
`~/.minisky/state/profiles/<name>/` or `MINISKY_STATE_DIR`. Active state uses
atomic writes, `0600` files, and `0700` profile directories. State and exports
may contain secret payloads, AES key material, source code, and environment
values; treat them as secrets.

Every migration must:

1. recognize the old schema explicitly;
2. validate before side effects;
3. transform deterministically into a new representation;
4. preserve stable resource and operation IDs;
5. atomically replace state only after successful validation;
6. fail safely on unknown newer versions;
7. avoid replaying completed side effects or implicitly creating Docker
   containers, networks, clusters, or volumes;
8. document downgrade/rollback limitations.

## Expand–Migrate–Contract

Prefer additive readers/writers first: teach the new binary to read old state,
write the new version, and keep compatibility until the release boundary is
clear. Remove legacy fields only after migration and rollback policy are proven.
For large payloads, stream/batch transformations and retain the last known-good
snapshot until completion.

Control-plane metadata and data-plane storage are separate. A migrated resource
record does not prove that a Docker volume, emulator filesystem, DuckDB file, or
external backend is present. Reconcile explicitly and mark missing workloads as
metadata-only/suspended/failed according to the service contract.

## Import and Export Safety

- Import only into an empty, isolated profile unless a merge algorithm is
  explicitly designed and tested.
- Reject path traversal, symlinks escaping the profile, malformed versions,
  duplicate identities, and oversized/untrusted inputs before writes.
- Never use the daemon working directory as a persistence root.
- Exports inherit destination permissions; warn users and avoid logging content.
- Make retries idempotent and crash-safe.

## Verification

Use fixtures for every supported old version and test:

- old → new migration and exact semantic preservation;
- repeated migration/load idempotence;
- corrupt, truncated, unknown-newer, and malicious-path rejection;
- interrupted write retaining the prior valid state;
- create → save → restart/load → observe;
- export → clean isolated import → observe;
- no Docker/LRO/event side-effect replay;
- concurrent save/load behavior under `go test -race`.

Never test against the user's active `~/.minisky` profile or Terraform state.
Use `t.TempDir`/temporary HOME and preserve a recovery copy when manually
validating real data.
