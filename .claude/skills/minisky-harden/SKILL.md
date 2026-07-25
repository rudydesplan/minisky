---
name: minisky-harden
description: Discover and close previously unenumerated resilience gaps in an existing, bounded MiniSky shim or system component, including malformed input, concurrent mutation, save failure, restart, cancellation, timeout, cleanup, or Docker loss. Trigger on "harden this", "make it resilient", "find failure modes", "handle edge cases", or "test behavior under failure". Exit after the accepted failure-mode matrix has tested dispositions and scoped fixes. Do not use for a predetermined defect list, read-only audit, design verdict, migration, redesign, or new capability.
license: Apache-2.0; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: Impeccable
---

# MiniSky Harden

Discover and close resilience gaps in an existing component without redesigning
its architecture or inventing generic limits.

## Entry and exit

Enter with a bounded existing component and a resilience objective, not a
predetermined defect list. Read the component, its tests, and only the shared
contracts it uses. Establish current behavior, then agree which discovered risks
are in scope before changing code.

Exit when every accepted matrix row has a tested disposition: fixed, already
satisfied, or explicitly deferred with remaining impact. Include minimal
implementation changes, exact command results, and skipped prerequisites. Stop
and re-scope to `minisky-polish` if the input is already a fixed list of isolated
defects. Use `minisky-craft` if resilience requires a state migration, new
subsystem, or material API expansion.

## Build the failure-mode matrix

For each applicable risk, record the trigger, expected contract, current
evidence, intended test, and disposition:

1. malformed JSON, missing required fields, wrong types, oversized input, and
   unsupported methods;
2. duplicate create, not-found update/delete, invalid transitions, and partial
   mutation;
3. concurrent create/update/delete/list and stale-snapshot overwrite;
4. required save failure, corrupt state, first run, restart, and profile
   isolation;
5. request cancellation, bounded backend waits, and goroutine lifetime;
6. Docker unavailable, container loss, partial backend creation, and cleanup;
7. LRO work failure, polling visibility, and in-memory operation loss;
8. cross-service target unavailable or delivery retried, when applicable.

Do not apply every category mechanically. A memory-only shim does not need a
restart guarantee, and a metadata-only shim does not need Docker recovery.

## Contract rules

- Verify behavior against the specific service API and existing MiniSky route.
  Do not invent universal body limits, page sizes, timeouts, name regexes, or
  status codes.
- Decode and validate before mutation. Add a
  `pkg/validator/discovery.go` rule only for a mutation the gateway should
  validate; it is a curated subset, not full schema enforcement.
- Return deliberate GCP-shaped JSON errors where the route contract requires
  them. Unsupported known behavior normally uses HTTP 501 with
  `UNIMPLEMENTED`; unknown routes or verbs may use 404 or 405.
- Make check-and-insert and other state transitions atomic. Snapshot under the
  state lock, release it before slow file or Docker I/O, and serialize saves
  when out-of-order snapshots could overwrite newer state.
- `state.New` returns `(*state.Store, error)`. Treat `state.ErrNotFound` as an
  empty first run. Do not silently discard corrupt or unreadable durable state
  unless the existing contract explicitly defines that recovery.
- A required durable mutation must not report success when `Store.Save` fails.
  Decide whether to roll back memory, return an error with reconcilable state,
  or use another tested strategy.
- Propagate request context into backend work. Add a timeout only when the
  repository or upstream contract provides a justified bound.
- `OperationManager` synchronizes map access but returns live operation
  pointers; add copy/synchronization boundaries before concurrent reads. It is
  in-memory and does not provide durable recovery, a polling route, or
  service-specific LRO compatibility by itself.
- Do not expose Docker IDs, host paths, bridge names, or internal ports as GCP
  fields unless the emulated contract has an equivalent.

## Test-first loop

1. Add one focused test for the highest-impact accepted failure mode.
2. Confirm it fails for the intended reason.
3. Make the smallest change that satisfies the established contract.
4. Re-run the focused test and the package race test.
5. Repeat only for risks accepted into scope; do not chase every `if err != nil`.

Run the concrete target package path and report it exactly. For example, Compute
uses:

```bash
go test -race ./pkg/shims/compute
```

If validator or registry contracts changed:

```bash
go test -race ./pkg/validator ./pkg/registry
```

For cross-shim behavior:

```bash
go test -race ./pkg/shims/...
```

For broader Go impact, and only when CGO requirements are available, use the
exact CI scope:

```bash
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
```

Run backend, restart, Kind, or Terraform checks only when the hardened claim
depends on them. The Terraform integration creates and destroys resources and
requires explicit authorization:

```bash
MINISKY_TERRAFORM_INTEGRATION=1 ./scripts/terraform-integration.sh
```

## Severity and handoff

Prioritize using demonstrated or contract-supported impact:

- **Critical:** reproduced unrecoverable data loss or corruption, a
  security-boundary escape, or false success that causes destructive follow-on
  work.
- **High:** reproduced failure of a required lifecycle, restart, routing,
  polling, or persistence path.
- **Medium:** reproduced edge-case, concurrency, validation, cleanup, or
  compatibility failure outside the normal lifecycle.
- **Low:** bounded diagnostics or maintainability weakness without a
  demonstrated runtime failure.

If behavior follows directly from a deterministic code path, cite it and bound
the impact. Otherwise reproduce it or keep it as an unverified risk. Do not
label a hypothetical as a defect; state the evidence needed to resolve it and
list all skipped checks and prerequisites.
