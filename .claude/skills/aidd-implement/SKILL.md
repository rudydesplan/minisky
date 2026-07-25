---
name: aidd-implement
description: Implement an existing MiniSky plan phase by phase and verify its acceptance criteria. Use when the user supplies or identifies a plan to build. Do not use to invent the plan, review a diff, commit, push, or open a pull request.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Implement

Execute the agreed plan with surgical changes and proportional evidence.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Read the complete plan, repository guidance, and affected code. Resolve only
   ambiguities that materially change behavior or scope.
2. Translate each phase into observable acceptance criteria and relevant checks.
   Do not add speculative requirements.
3. For each behavior-changing phase:
   - write or update a test that fails for the missing behavior when practical;
   - implement the minimum code needed;
   - run the focused test and nearby package checks;
   - remove only unused code introduced by the change.
4. For documentation, metadata, or mechanical phases where TDD is not meaningful,
   use static validation or a focused build/check instead.
5. After all phases, run broader validation proportional to the affected surface:
   Go tests/vet for Go changes, UI lint/build for UI changes, and specialized
   integration checks only when required by the plan.
6. Compare the result with every acceptance criterion and report completed,
   deferred, blocked, and unverified items explicitly.

## Boundaries

- Stop and ask when the plan requires a destructive choice, external mutation,
  credential use, or a materially different architecture.
- Do not create a branch or modify plan-status files unless requested.
- Do not commit at phase boundaries. Commit, push, and pull-request creation are
  separate user-authorized workflows.
- Do not mark an acceptance criterion complete from code inspection alone when a
  practical executable check exists.
