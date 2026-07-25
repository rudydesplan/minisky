---
name: aidd-test
description: Add or improve MiniSky tests for specified behavior, identify focused coverage gaps, or validate an authorized local UI journey. Use when the deliverable is tests or test evidence. Do not use for broad test-health audits, root-cause debugging, or feature implementation.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Test

Produce behavior-focused tests or journey evidence with the smallest useful scope.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Test workflow

1. Resolve the behavior, risk, and existing coverage. Prefer public contracts and
   observable outcomes over implementation details.
2. For missing or regressed behavior, write a test that fails for the intended
   reason before changing production code. If the user requested tests only, do
   not fix production behavior; report the demonstrated failure.
3. Add the minimum fixtures and cases needed for meaningful boundaries, failures,
   concurrency, restart, or compatibility risks in scope.
4. Run the narrow test first, then relevant package or suite checks. Use broader
   checks proportional to the affected surface.
5. Report tests added, commands and results, untested risks, and any flakes or
   environmental blockers.

## Journey workflow

1. Confirm the target is an authorized local or test environment.
2. Start or reuse the app safely, then validate the stated path with browser
   interaction when browser tools are available.
3. Capture observable evidence for each step and inspect console/network failures
   when relevant.
4. Do not claim automation coverage from a one-time manual journey.

## Boundaries

- Do not weaken assertions merely to make a suite pass.
- Do not add production code unless the user also asked to implement or fix it.
- Do not use production or third-party accounts without explicit authorization.
- Do not commit, push, or open a pull request unless separately requested.
