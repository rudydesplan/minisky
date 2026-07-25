# ADR 0017: In-tree plugin SDK v0

- Date: 2026-07-25
- Status: accepted

## Context

Go shared-object plugins are platform- and toolchain-sensitive and would not
match MiniSky's native CGO release matrix. A third-party process loader would
also require protocol negotiation, isolation, authenticated artifacts, bounded
failure handling, and lifecycle supervision that Phase 17 does not yet provide.

## Decision

MiniSky freezes `minisky.plugin/v0` as a source-compiled contract in
`pkg/pluginsdk`. A manifest declares exact domains, fidelity, persistence, and
the `in-tree` execution mode. Plugins implement HTTP handling, post-boot, and
bounded shutdown hooks. `minisky plugin scaffold` generates a compiling
contribution package with an explicit `501 UNIMPLEMENTED` starting boundary.

The SDK is not described as third-party installable. There is no `.so` loading,
remote marketplace, runtime download, signature verification, process
isolation, or version negotiation beyond manifest validation.

## Alternatives considered

### Go shared objects
- Benefit: small apparent loader.
- Cost: fragile OS, architecture, Go version, and dependency identity contract.
- Rejection reason: incompatible with MiniSky's supported native matrix.

### Out-of-process RPC loader
- Benefit: real failure isolation and language-neutral plugins.
- Cost: requires a larger security and lifecycle protocol than this bounded
  phase can truthfully implement.
- Rejection reason: deferred until the complete loader contract can be tested.

## Consequences

- Contributors get a versioned manifest, lifecycle contract, and executable
  scaffold test without weakening release portability.
- Adding a plugin still requires source review, a blank import, registry
  metadata, documentation, and rebuilding MiniSky.
- `docs/plugin-catalog.schema.json` is discovery metadata only.

## Compatibility and rollback

Existing shims are unchanged. Removing `pkg/pluginsdk` and the scaffold command
removes only the new contribution surface; no profile-state migration is
involved.
