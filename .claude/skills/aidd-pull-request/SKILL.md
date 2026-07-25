---
name: aidd-pull-request
description: Create a draft GitHub pull request for the current MiniSky branch when the user explicitly asks to open one. Use after the intended commits exist. Do not use to implement, commit, merge, or manage an existing pull request.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Pull Request

Open one accurate draft pull request and return its URL.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Inspect worktree status, current branch, upstream state, remote default branch,
   all commits since the merge base, and the complete base-to-HEAD diff.
2. Stop if the branch is the default branch, required commits are missing, or
   unrelated changes would be included. Do not create commits.
3. Derive the title and summary from the full change, not only the latest commit.
   Include a concise test plan with checks actually run and any remaining gaps.
4. Use the repository pull-request template when present.
5. Push the current branch only when required to fulfill the explicit pull-request
   request. Push only current `HEAD`, set upstream when needed, and never force.
6. Create a draft pull request with the verified base. Do not add labels,
   reviewers, assignees, or projects unless the user requests them.
7. Return the URL and report the base, head, and validation status.

## Safety

- A request to draft text is not permission to create or push anything; return
  text only.
- Never include secrets or knowingly unrelated commits.
- Never merge, mark ready, close, retarget, or modify another pull request.
- If authentication or permission is denied, report it and stop.
