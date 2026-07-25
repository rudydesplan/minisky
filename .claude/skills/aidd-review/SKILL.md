---
name: aidd-review
description: Review a MiniSky diff read-only for actionable defects, behavior against requirements, and scope relevance. Use before shipping a change or when the user asks for code review. Do not use for a whole-codebase audit, design-only challenge, or fixing findings.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Review

Perform a defect-first review of the requested change and return findings in chat.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Resolve the review target: uncommitted changes, a commit, or a base-branch
   diff. Inspect the complete diff and relevant surrounding code.
2. Read the plan, ticket, or acceptance criteria when available. Do not invent
   requirements when none exist.
3. Review three lenses:
   - correctness: regressions, error paths, concurrency, state, security, and APIs;
   - functional fit: stated behavior and acceptance criteria;
   - relevance: unrelated edits, missing necessary changes, and repository rules.
4. Run focused non-mutating checks when they can confirm or reject a suspected
   defect. Do not install dependencies or modify files.
5. Report every actionable finding ordered by severity. Each finding must include
   a precise file range, failure scenario, impact, and smallest fix direction.
6. If no actionable findings remain, say so and list residual testing gaps.

## Boundaries

- Stay read-only: do not patch, generate `review.md`, stage, commit, push, or open
  a pull request unless the user separately asks for another workflow.
- Report only defects introduced or exposed by the reviewed change. Do not turn
  the review into a general audit or style rewrite.
- Avoid unsupported claims; distinguish confirmed behavior from review risk.
- A verdict is `changes requested`, `comment`, or `ready`, based on the strictest
  supported finding.
