---
name: aidd-sdlc
description: Orchestrate a substantial MiniSky request from clarified requirements through plan, implementation, verification, and optional delivery. Use only when the user explicitly wants the full end-to-end workflow. Do not use for a single phase or a small focused change.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD SDLC

Coordinate the distinct workflows without assuming slash commands, named agents,
or external orchestration tools exist.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Specify: establish objective, non-goals, constraints, acceptance criteria, and
   delivery boundary. Brainstorm only when a material ambiguity remains.
2. Plan: inspect the repository and produce small phases with test and validation
   criteria.
3. Implement: execute phases with test-first development for behavior changes and
   focused checks after each coherent step.
4. Verify: run proportional broader checks and compare every acceptance criterion
   with executable evidence.
5. Review: perform a read-only defect-first review. Fix accepted blockers, rerun
   affected checks, and review the resulting diff again when warranted.
6. Deliver only to the level explicitly authorized by the user.

Use available tools directly. Specialized agents may be used when available and
useful, but delegation is never required and their absence is not a blocker.

## Safety and composition

- A request for the full workflow authorizes repository edits needed to implement
  it, but not commits, pushes, pull requests, merges, deployments, or production
  actions unless those side effects are explicitly requested.
- "Auto" may waive routine check-ins, but never waives approval for destructive,
  credentialed, external, or delivery actions.
- Do not create mandatory `aidd_docs` artifacts or status files. Persist a spec,
  plan, or report only when requested.
- Skip phases already satisfied by reliable input; state why.
- Stop for a user choice only when alternatives materially change behavior,
  architecture, safety, cost, or destructive impact.
- Use the focused AIDD skill for a single requested phase instead of this
  orchestrator.
