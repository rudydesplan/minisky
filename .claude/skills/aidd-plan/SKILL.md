---
name: aidd-plan
description: Turn a concrete MiniSky request, ticket, or specification into a phased implementation plan grounded in the repository. Use before building multi-step work. Do not use to brainstorm an unclear goal, write code, review a diff, or commit changes.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Plan

Produce an implementation-ready plan without changing application code.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Restate the objective, user-visible behavior, constraints, non-goals, and
   acceptance criteria from the source.
2. Explore the relevant MiniSky paths, tests, repository rules, and integration
   boundaries. Verify file names and commands; do not plan against imagined APIs.
3. Surface material choices and trade-offs. Ask only when a missing choice would
   change architecture, safety, or externally visible behavior.
4. Divide work into small ordered phases. For each phase include:
   - purpose and affected areas;
   - exact behavior or contract to change;
   - test-first or other verification approach;
   - acceptance criteria and dependencies;
   - risks, migrations, or rollback concerns when relevant.
5. Include a final validation matrix proportional to scope, using repository
   commands that exist. Typical checks are focused `go test`, `go vet`, and
   `npm run lint` / `npm run build` from `ui/`.
6. Return the plan in chat. Write a plan file only when the user requests one,
   using the path they provide or a clearly stated repository-local path.

## Boundaries

- Do not implement, stage, commit, push, or open a pull request.
- Do not require a wireframe for non-UI work; include one only when it resolves
  layout or interaction ambiguity.
- Prefer the smallest plan that satisfies the request. Do not add speculative
  infrastructure, compatibility, or cleanup phases.
