---
name: minisky-quality-floor
description: Enforce MiniSky's minimum correctness guardrails as a companion to implementation work in pkg/shims, pkg/router, pkg/validator, pkg/orchestrator, pkg/registry, or pkg/state. Trigger whenever a request adds, fixes, or extends shim CRUD, routing, validation, operations, persistence, registration, or manifest metadata. Exit after the edited contracts satisfy the applicable invariants and relevant checks are reported. Never use this as a standalone audit, design critique, bounded defect workflow, or resilience campaign.
---

# MiniSky Quality Floor

## Entry and exit

Enter automatically when authorized implementation work touches one of the
paths or contracts named in the description. Pair this guardrail with the
task-specific implementation skill and apply only relevant rules.

Exit when:

- each changed contract satisfies the applicable rules below;
- the narrowest relevant tests and any required shared-contract tests pass; and
- the handoff distinguishes executed evidence from skipped or untested behavior.

This skill is a guardrail. Use `minisky-audit` for a read-only defect inventory,
`minisky-critique` for a design verdict, `minisky-polish` for known bounded
defects, and `minisky-harden` for an open-ended resilience objective.

## Correctness rules

1. **Never fake success.** A recognized unsupported operation normally returns a
   JSON error with HTTP 501 and `UNIMPLEMENTED`. Unknown routes and unsupported
   verbs may instead be 404 or 405; match the service contract.
2. **Write JSON errors deliberately.** Set the HTTP status before encoding
   `error.code`, `error.message`, and `error.status`. Include `error.details`
   only where the service contract or existing tests require it. Do not use
   `http.Error` for a route claiming GCP-shaped errors.
3. **Validate before mutation.** Decode, validate, and check conflicts or
   preconditions before changing memory, files, or Docker. Add a
   `pkg/validator/discovery.go` rule only for a mutation request the gateway
   should validate; this file is a curated subset, not an embedded Discovery
   Document.
4. **Match the specific API.** Verify paths, verbs, status codes, collection
   keys, fields, pagination, and asynchronous behavior against the supported
   service slice. Do not assume every GCP API uses `items`, `selfLink`, ETags,
   or `google.longrunning.Operation`.
5. **Keep registration coherent.** Custom shim packages call
   `registry.Register` from `init()`, are blank-imported by
   `pkg/shims/registry_init.go`, and have matching metadata in
   `pkg/registry/manifest.go`. `registry.RegisterLazyDocker` is for a pure
   Docker passthrough with no custom Go factory.
6. **Use existing manifest values.** Fidelity is `high`, `standard`, or
   `passthrough`; persistence is `memory`, `file`, `docker`, `hybrid`, or
   `static`.
7. **Treat durability honestly.** `state.New` returns `(*state.Store, error)`.
   Durable shims handle `state.ErrNotFound` as an empty first run, snapshot
   consistently, and do not return success after a failed required save.
   Export/import covers JSON metadata only, not Docker volumes or DuckDB files.
8. **Synchronize complete operations.** Protect mutable maps, make
   check-and-insert atomic, copy or snapshot while locked, and perform slow file
   or Docker I/O after releasing the state lock. Serialize concurrent saves when
   stale snapshots could overwrite newer ones.
9. **Keep backend details private.** Do not expose Docker IDs, host paths,
   bridge names, or internal ports as GCP fields unless the emulated contract
   explicitly has an equivalent.
10. **Describe LROs accurately.** `pkg/orchestrator.OperationManager`
    synchronizes map access but returns live operation pointers; callers must
    not treat those pointers as immutable snapshots across goroutines. It is
    in-memory and exposes MiniSky's `PENDING -> RUNNING -> DONE` shape. A shim
    must implement and test its service-specific polling route. The manager
    alone does not prove standard Google LRO compatibility.

## Test-first loop

1. Add the smallest failing test for the required behavior and confirm that it
   fails for the intended reason.
2. Make the smallest implementation change and rerun the focused test.
3. Run the concrete target package path, not a placeholder. For example, a
   Compute change uses:

```bash
go test -race ./pkg/shims/compute
```

4. If registry or gateway validation changed, run the exact shared checks:

```bash
go test -race ./pkg/validator ./pkg/registry
```

5. For cross-shim behavior, run:

```bash
go test -race ./pkg/shims/...
```

6. For the same Go scope used by CI, run when the environment supports the
   repository's CGO requirements:

```bash
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
```

Report the exact command actually run, including its concrete package path.
Record skipped checks and prerequisites. A unit test is not evidence that
Docker, Kind, Buildpacks, or Terraform behavior works. Never upgrade fidelity or
persistence metadata from code inspection alone.
