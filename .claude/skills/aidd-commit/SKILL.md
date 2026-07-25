---
name: aidd-commit
description: Create one or more atomic git commits from the current MiniSky changes when the user explicitly asks to commit. Push only when explicitly requested. Do not use for amending, rebasing, opening a pull request, tagging, or routine implementation.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Commit

Commit only changes the user authorized.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Inspect status, staged and unstaged diffs, untracked files, and recent commit
   style. Do not alter git configuration.
2. Exclude secrets, generated noise, unrelated work, and files outside the
   requested scope. Ask before including ambiguous pre-existing changes.
3. Split independent concerns only when the user requested multiple commits or
   the intended commit would otherwise be misleading.
4. Run checks proportional to the staged change, or report why they were not run.
5. Stage explicit paths and commit with a concise message that explains intent.
6. Verify the commit and remaining worktree state.
7. Push only if the user explicitly requested a push. Never force-push the
   default branch; use no force option unless the user explicitly requested it
   and the repository policy permits it.

## Safety

- A request to implement, test, review, or prepare changes is not permission to
  commit or push them.
- Never amend unless the user explicitly asks and rewriting the commit is safe.
- Never bypass hooks or signing checks. If a hook rejects the commit, fix only
  issues inside the authorized scope and create a new commit; do not amend a
  failed commit.
- Do not open a pull request, merge, rebase, tag, or delete branches.
