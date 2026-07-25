---
name: aidd-brainstorm
description: Clarify a vague MiniSky idea through focused questions, alternatives, and trade-offs until it is ready to plan. Use for half-formed ideas or under-specified requests. Do not use to inspect an existing artifact for defects, write an implementation plan, or change code.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Brainstorm

Turn uncertainty into a concise decision brief without implementing anything.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Restate the goal, known constraints, and current assumptions.
2. Identify the one unresolved choice that most changes the solution.
3. Ask a small set of related, answerable questions. Do not repeat facts already
   provided or send a generic checklist.
4. Follow the user's answer to expose alternatives, trade-offs, edge cases, and
   the smallest useful scope.
5. Repeat only while a material fork remains.
6. Summarize the agreed goal, non-goals, constraints, preferred direction,
   alternatives rejected, success signals, and open assumptions.

## Boundaries

- Match the user's level of detail; do not prematurely descend into files or APIs.
- Research the repository or current documentation only when it resolves a real
  decision. Label evidence separately from assumptions.
- Do not edit files, write code, create branches, commit, push, or open a pull
  request.
- Do not claim a slash command, named agent, or external tool is available.
- Offer planning as the next step, but do not silently start it.
