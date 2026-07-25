---
name: aidd-debug
description: Reproduce, diagnose, and fix a concrete MiniSky bug through evidence and hypothesis testing. Use when behavior fails, a regression is reported, or earlier fixes did not work. Do not use for feature work, broad audits, or diff review.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Debug

Find the root cause before changing behavior, then make the smallest verified fix.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. State the observed behavior, expected behavior, scope, and reproducible signal.
2. Reproduce with the narrowest safe command or test. If reproduction is not
   possible, gather concrete evidence and label remaining uncertainty.
3. List plausible hypotheses ordered by evidence and cost to disprove.
4. Test one hypothesis at a time. Do not stack speculative fixes.
5. For a requested fix, add a failing regression test first when practical. If a
   test cannot express the failure, explain why and define another repeatable
   check before editing.
6. Implement the smallest root-cause fix without drive-by cleanup.
7. Run the regression test, nearby package checks, and broader checks in
   proportion to blast radius. Include race, restart, UI build, or Docker checks
   only when relevant.
8. Report root cause, changed behavior, evidence, checks, and residual risk.

## Boundaries

- A diagnosis-only request stops after confirming the cause; do not implement.
- Do not create branches, commits, pushes, or pull requests unless separately
  requested.
- Do not use production or third-party systems for reproduction without explicit
  authorization.
- Use `minisky-harden` only when the request expands beyond a concrete defect;
  do not turn one bug into an open-ended campaign.
