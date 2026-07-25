---
name: architecture-decision-records
description: Draft or maintain concise MiniSky architecture decision records for durable choices about shim fidelity, routing, state, Docker/CGO, optional backends, Terraform compatibility, or distribution. Use when the user asks to record a decision; not for routine edits or automatic file creation.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# Architecture Decision Records for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

Use when a durable architectural choice and meaningful alternatives exist—for
example GCP fidelity boundaries, host/path routing, state schema and recovery,
Docker versus simulation, CGO distribution, optional Kind/Buildpacks backends,
or Terraform endpoint behavior. Do not create ADRs for formatting, routine shim
CRUD, dependency bumps, or choices already dictated by an upstream GCP contract.
Do not create/update files unless the user asks.

## Evidence First

Before drafting, inspect the implementation, tests, `go.mod`, CI, Dockerfile,
service compatibility, state model, Terraform compatibility, and relevant prior
ADRs. Distinguish current behavior from roadmap intent. Never state planned,
experimental, metadata-only, or emulator-backed behavior as shipped fidelity.

## Format

```markdown
# ADR-NNNN: Decision title

- Date: YYYY-MM-DD
- Status: proposed | accepted | deprecated | superseded by ADR-NNNN
- Deciders: names/roles if known

## Context
What constraint or failure requires a durable choice? Include MiniSky-specific
facts and compatibility obligations.

## Decision
State the decision and its scope in present tense.

## Alternatives considered
### Alternative
- Benefits:
- Costs:
- Rejection reason:

## Consequences
- Positive:
- Negative:
- Risks and mitigations:
- Verification:

## Compatibility and rollback
Describe GCP/Terraform/state/platform impact and how to reverse or supersede it.
```

Keep an ADR short enough to review. Include rejected alternatives only when they
were actually considered; mark unknown deciders or historical dates as unknown
rather than inventing them.

## Workflow

1. Find the existing ADR location/index; do not assume `docs/adr` exists.
2. Determine the next number without renumbering history.
3. Draft from verified evidence and identify unresolved questions.
4. Present the draft when the user's request is ambiguous about writing.
5. On authorization, write the ADR and update the existing index atomically.
6. To supersede, keep the old record and cross-link both records.

A useful MiniSky ADR states effects on simulation defaults, optional Kind/Pack,
CGO/native builds, GCP error/LRO contracts, profile-state migration, Docker
reconciliation, Terraform no-drift behavior, tests, and documentation—only for
surfaces the decision actually touches.
