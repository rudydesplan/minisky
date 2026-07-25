# ADR 0018: Opt-in local enterprise controls

- Date: 2026-07-25
- Status: accepted

## Context

Teams need reproducible local failure controls and attribution without changing
the permissive default or claiming production IAM, compliance, or immutable
storage.

## Decision

- Quotas are disabled unless `MINISKY_QUOTAS_JSON` or `--quotas` supplies
  bounded fixed-window route, service, project, or default rules.
- Mutation audit is opt-in and profile-scoped. JSONL records contain normalized
  metadata, never bodies, query strings, bearer credentials, or cookies. Each
  record hashes the previous record. Strict mode writes an attempt before
  dispatch and rejects the mutation if that append fails.
- `roles/minisky.viewer`, `roles/minisky.editor`, and `roles/minisky.admin`
  provide a small local dashboard/gateway permission set through the existing
  strict IAM principal and Resource Manager ancestor lookup.
- Prometheus quota rejection labels contain normalized service, route, and
  scope only; project IDs are deliberately excluded.

## Alternatives considered

An external immutable audit service and distributed rate limiter were rejected
for this local phase because neither exists in the repository and either would
create unsupported deployment and compliance claims.

## Consequences

Hash chaining detects edits, deletion, reordering, and truncation only when the
expected chain is available for comparison. A user with filesystem control can
replace the entire file. A strict attempt record cannot roll back a mutation if
the later completion append fails. These files are tamper-evident local
evidence, not WORM or compliance storage.

All controls remain disabled with the historical permissive behavior unless
explicitly configured.

## Compatibility and rollback

Disable quotas, audit, and strict IAM variables to restore permissive behavior.
Audit files are outside portable state snapshots; no state schema changes are
required.
