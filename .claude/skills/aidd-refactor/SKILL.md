---
name: aidd-refactor
description: Improve a bounded MiniSky area for cleanup, performance, security, or architecture while preserving intended behavior unless an approved security change requires otherwise. Use to implement accepted improvement work. Do not use for read-only diagnosis, feature development, or dependency-only maintenance.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Refactor

Make the smallest verified improvement within one explicit axis.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Resolve the target, axis, intended invariant, and success evidence. If the
   request supplies audit findings, treat only accepted findings as scope.
2. Establish a baseline with focused tests, benchmarks, profiles, or static
   checks appropriate to the claim.
3. Add or strengthen characterization/regression tests before risky structural
   changes. For security behavior changes, first encode the unsafe case when
   practical.
4. Apply one coherent refactor without unrelated cleanup:
   - cleanup: reduce complexity or duplication without contract changes;
   - performance: optimize a measured or structurally demonstrated bottleneck;
   - security: close a concrete trust-boundary weakness and disclose behavior changes;
   - architecture: improve boundaries or ownership while preserving contracts.
5. Re-run focused checks and broader validation proportional to blast radius.
   Compare performance claims against the baseline.
6. Report invariants, evidence, intentional behavior changes, and remaining risk.

## Boundaries

- Do not infer broad permission from "clean up"; ask when multiple subsystems or
  externally visible contracts are implicated.
- Do not remove compatibility behavior, upgrade dependencies, redesign UI, or
  introduce a new feature unless explicitly included.
- Do not commit, push, or open a pull request unless separately requested.
- If behavior cannot be preserved or tested, stop and present the trade-off.
