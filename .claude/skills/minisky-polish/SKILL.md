---
name: minisky-polish
description: Repair an accepted, bounded list of known defects or test gaps in an existing MiniSky shim or feature with regression tests and surgical compatibility fixes. Trigger on "fix these audit findings", "polish these known issues", "repair these pre-ship bugs", or a concrete narrow defect list. Exit only when every accepted item is fixed or explicitly deferred and focused checks pass. Do not use to discover unknown resilience gaps, audit read-only, redesign architecture, migrate state, or add a material API surface.
license: Apache-2.0; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: Impeccable
---

# MiniSky Polish

## Entry and exit

Enter with one or more accepted, observable defects or narrowly specified test
gaps in an existing implementation. Record the list before editing and keep the
architecture and requested scope unchanged.

Exit when every accepted item is one of:

- fixed with a regression test that failed for the intended reason before the
  fix and now passes; or
- explicitly deferred with the reason and remaining impact.

Also require focused checks to pass and skipped checks to be explicit.

Stop and re-scope to `minisky-harden` when the real request is to discover
unknown failure modes. Use `minisky-craft` when a fix requires a state migration,
new service architecture, or materially larger API surface.

## Establish the baseline

1. Read the affected package and all of its tests.
2. Check `pkg/shims/registry_init.go`, `pkg/registry/manifest.go`, and relevant
   `pkg/validator/discovery.go` rules only when the defect crosses those
   contracts.
3. For durable behavior, read the shim's state adapter, restart tests, and
   `pkg/state/store.go`.
4. From the repository root, run the concrete target package path and record
   pre-existing failures. For example, Compute uses
   `go test -race ./pkg/shims/compute`.
5. Translate each requested finding into observable expected behavior.

## Fix loop

For each defect:

1. Add the smallest reproducing test first.
2. Confirm it fails for the expected reason.
3. Make a surgical implementation change; do not clean up unrelated code.
4. Re-run the focused test, then the target package with `-race`.
5. Remove only code made obsolete by this fix.

Preserve an audit's severity unless new executable or deterministic code-path
evidence changes the impact. Prioritize:

1. critical: reproduced corruption, security-boundary escape, or destructive
   false success;
2. high: broken normal lifecycle, routing, polling, restart, or required save;
3. medium: reproduced validation, concurrency, error, cleanup, or compatibility
   failure outside the normal lifecycle;
4. low: bounded documentation, diagnostics, or maintainability drift without a
   demonstrated runtime failure.

Do not promote a missing test or hypothetical failure to a runtime defect.

Do not mechanically add `selfLink`, pagination, `details`, an LRO, or a 501.
Verify each against the service API and current routes. Unknown paths and
unsupported methods may require 404 or 405.

## Repository-specific checks

- Custom handlers use `registry.Register`; only pure Docker passthrough domains
  use `registry.RegisterLazyDocker`.
- Manifest fidelity is `high|standard|passthrough`; persistence is
  `memory|file|docker|hybrid|static`.
- Validator rules use `ServiceSchema`, `MethodSchema.HTTPMethod`, `PathGlob`,
  `RequiredBody []BodyField`, and `RequiredQuery`.
- `state.New(config.GetStateDir(), config.GetProfile())` returns a store and an
  error. `registry.Context` has no state store.
- Durable mutations must not report success after a required save fails.
- `OperationManager` synchronizes map access but returns live operation
  pointers, so callers must not assume immutable snapshots across goroutines.
  It is in-memory and does not add a polling route or service-specific
  operation response by itself.

## Final verification

Run the exact concrete package command. For example:

```bash
go test -race ./pkg/shims/compute
```

If shared contracts changed:

```bash
go test -race ./pkg/validator ./pkg/registry
```

For cross-shim impact:

```bash
go test -race ./pkg/shims/...
```

When the change can affect the broader Go build and CGO requirements are
available, use the exact CI scope:

```bash
test -z "$(gofmt -l cmd pkg ui)"
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
go build -trimpath ./cmd/minisky
```

Add restart, Docker, Kind, CGO, or Terraform checks only when the repaired claim
depends on them. The Terraform integration requires explicit authorization and:

```bash
MINISKY_TERRAFORM_INTEGRATION=1 ./scripts/terraform-integration.sh
```

Report every command, result, and unavailable prerequisite. Update manifest
metadata only when behavior and executable evidence justify it. Do not call a
missing test a fixed runtime defect unless the new test first reproduced one.
