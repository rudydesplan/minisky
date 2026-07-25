# ADR 0016: Docker multi-project strategy

## Status

Accepted for the Phase 14 vertical slice.

## Decision

Representative in-process resources are keyed by canonical project ID and can
be guarded by Resource Manager existence checks. Existing Docker passthrough
emulators remain one profile-local backend unless the upstream emulator
demonstrably isolates projects. Container names and networks are not multiplied
per project as an implied security boundary.

One emulator stack per project was rejected for this slice because of its
resource cost and because process duplication alone does not establish tenant
isolation.

## Consequences

Tested metadata paths avoid project-name collisions, while some passthrough
resources can still collide. Compatibility documentation must identify that
limit. A future emulator-specific strategy can supersede this decision after
apply, no-drift, cleanup, and isolation evidence.
