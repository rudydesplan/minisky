# ADR 0013: Profiles contain projects

## Status

Accepted for the Phase 14 vertical slice.

## Decision

One MiniSky profile contains a persisted Resource Manager registry with one or
more projects. `local-dev-project` is seeded and cannot be deleted. Project IDs
namespace representative in-process shim state. The CLI default project is
profile-local configuration; explicit environment or command flags override it.

A profile remains the durability and backend boundary. Treating each project as
a profile was rejected because it prevents concurrent cross-project workflows.

## Consequences

Project and hierarchy metadata participates in state export/import. Optional
gateway project-existence enforcement preserves compatibility when disabled.
Older profiles are seeded on first load. Docker-backed data remains governed by
ADR 0016 rather than implied isolated by this registry.
