---
name: minisky-critique
description: Evaluate a proposed or recently implemented MiniSky design choice—architecture, API surface, state boundary, event flow, LRO strategy, service coupling, or backend selection. Trigger on "critique this design", "challenge this approach", "should we build it this way?", "review this architecture", or a design-focused PR review. Exit with one evidence-backed design verdict, concerns, assumptions, and recommendations. Stay read-only; do not inventory implementation defects, discover resilience gaps, or apply fixes.
license: Apache-2.0; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: Impeccable
---

# MiniSky Critique

Evaluate whether an approach is sound and fits MiniSky. Do not turn the review
into a line-by-line defect audit.

## Entry and exit

Enter when there is a proposal, decision, plan, architecture, or bounded
implementation and the question is whether its choices are sound. Establish the
intended service slice, fidelity target, users, constraints, and excluded
behavior before judging it.

Exit with one verdict, evidence-backed concerns, explicit assumptions,
actionable recommendations, and the evidence that would resolve open questions.
Make no edits and do not produce a defect inventory. Use `minisky-audit` when
the goal is to assess implemented behavior. Route accepted bounded repairs to
`minisky-polish`, resilience discovery to `minisky-harden`, and a new service or
major capability to `minisky-craft`.

## Evidence

Inspect only artifacts relevant to the decision:

- the proposal, plan, diff, or implementation under review;
- `pkg/registry/registry.go`, `pkg/registry/manifest.go`, and
  `pkg/shims/registry_init.go` for registration choices;
- `pkg/validator/discovery.go` for gateway mutation validation;
- `pkg/orchestrator/operations.go` for operation-manager assumptions;
- `pkg/state/store.go` for profile-scoped JSON persistence;
- `docs/cli_reference.md` and `docs/user-guide.md` for portable-state and
  user-facing behavior claims;
- `docs/terraform.md`, `terraform/`, and
  `scripts/terraform-integration.sh` only for Terraform claims;
- the service-specific GCP REST or client contract when fidelity is at issue.

Do not treat roadmap labels, comments, or absent tests as proof of runtime
behavior. Ask a focused question if the intended fidelity or supported workflow
would materially change the verdict.

## Design dimensions

1. **Contract fit:** Does the proposed path, request/response shape, error,
   lifecycle, polling, or side effect match the explicitly supported GCP slice?
2. **MiniSky fit:** Does a custom package register a factory and receive a blank
   import? Is lazy Docker registration reserved for a pure passthrough? Are
   cross-service dependencies wired after boot rather than through import
   cycles?
3. **State boundary:** Is metadata assigned honestly to
   `memory|file|docker|hybrid|static`? Does the design separate portable JSON
   metadata from Docker volumes and DuckDB files?
4. **Failure model:** Does the approach define atomic mutation, save failure,
   cancellation, backend loss, restart, and concurrent-request behavior where
   those risks apply?
5. **Tool compatibility:** Are Terraform or SDK claims tied to actual provider
   paths, polling, import, no-drift, and destroy behavior rather than JSON
   resemblance?
6. **Complexity:** Is the smallest architecture that meets the stated fidelity
   being proposed? Identify unnecessary coupling or abstractions, but do not
   demand speculative scale.

## Evidence-based concern levels

- **Blocking:** Repository or client-contract evidence shows the design cannot
  satisfy a stated primary workflow, permits unrecoverable corruption or a
  security-boundary escape, or requires incompatible client behavior.
- **Important:** Repository or contract evidence shows a material gap in
  lifecycle, persistence, concurrency, routing, operability, or maintainability
  that should be resolved before implementation or merge.
- **Minor:** A bounded design improvement with no demonstrated primary-workflow
  failure.

These are design-decision levels, not defect severities. State uncertainty
instead of inflating a concern. A hypothetical failure mode remains a question
or recommendation until repository or contract evidence establishes likelihood
and impact.

## Report

Return:

1. the subject, intended scope, and constraints;
2. strengths supported by specific evidence;
3. concerns ordered by level, each with evidence, impact, and recommendation;
4. unresolved questions and assumptions;
5. exact read-only commands run and their results, if any;
6. one verdict: `APPROVE`, `APPROVE WITH CHANGES`, or `RETHINK`.

`APPROVE` means no material design concern was found in the reviewed scope.
`APPROVE WITH CHANGES` means the approach is viable after bounded changes.
`RETHINK` means a blocking premise or architecture must change. A verdict is
about the design reviewed, not a claim that the implementation is production
ready.
