---
name: aidd-challenge
description: Challenge a proposed or just-completed MiniSky approach against its goal and agreed plan, focusing on assumptions, unnecessary complexity, and strategic gaps. Use for a second-pass design critique. Do not use for changed-line defect review, codebase audit, or implementation.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Challenge

Reconsider the approach from first principles and return a read-only verdict.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Identify the goal, accepted constraints, reference plan or decision, and the
   work being challenged.
2. Test whether the approach solves the real problem, whether a smaller solution
   exists, and whether assumptions still hold.
3. Check omitted alternatives, coupling, operational consequences, reversibility,
   and MiniSky-specific fidelity boundaries.
4. Classify each point:
   - `deal-breaker`: prevents the stated goal or creates unacceptable risk;
   - `suggestion`: worthwhile improvement that does not block acceptance;
   - `correct`: an important choice that should be retained.
5. Give a verdict of `proceed`, `revise`, or `rethink`, with evidence and explicit
   uncertainty. Use qualitative confidence; do not invent a precise percentage.

## Boundaries

- Stay at design and scope level. Use `aidd-review` for line-level defects.
- Do not manufacture objections for balance or restate the original work as a
  finding.
- Do not edit files, implement suggestions, commit, push, or open a pull request.
- Return the report in chat unless the user explicitly requests an artifact.
