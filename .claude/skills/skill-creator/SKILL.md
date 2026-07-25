---
name: skill-creator
description: Create, review, or improve Cursor/Claude agent skills and their trigger metadata. Use when a user asks to add or edit a SKILL.md, capture a repeatable workflow as a skill, assess progressive disclosure, or test whether a skill triggers for the right prompts. In MiniSky, keep project skills under .claude/skills/ and do not alter unrelated skills.
license: Apache-2.0; upstream terms at https://github.com/anthropics/skills/blob/main/skills/skill-creator/LICENSE.txt
---

# Skill Creator

Create skills that are easy to trigger, safe to follow, and no larger than their
purpose requires. Adapt the workflow to the user's request; do not require an
evaluation project for a small metadata or wording change.

> Adapted for MiniSky from Anthropic's `skill-creator` skill:
> https://github.com/anthropics/skills/tree/main/skills/skill-creator

## Scope in MiniSky

- Project-local skills live at `.claude/skills/<skill-name>/SKILL.md`.
- Treat a user-provided path as authoritative. Do not silently edit a global
  skill with the same name.
- Read the entire target `SKILL.md` and every bundled file it references before
  changing it.
- Touch only the requested skill directory. Do not modify application code,
  `PRODUCT.md`, or other skills unless explicitly asked.
- Preserve upstream attribution and license metadata when adapting a third-party
  skill. Never claim that copied material is original MiniSky work.

## 1. Capture intent

Use conversation context before asking questions. Resolve only gaps that would
materially change the skill:

1. What task should the skill enable?
2. Which realistic user requests should and should not trigger it?
3. What output or side effects are expected?
4. Which tools, files, dependencies, and permissions are actually available?
5. What evidence would show that the skill helped?

Avoid making a general skill MiniSky-specific unless its purpose concerns this
repository. Conversely, make repository assumptions explicit when they matter.

## 2. Inspect the existing skill

For an existing skill:

1. Read `SKILL.md`.
2. Enumerate and read bundled `references/`, `scripts/`, `assets/`, or examples.
3. Check that every referenced relative path exists.
4. Check that named tools and commands exist in the current environment.
5. Identify instructions that are unsafe, stale, duplicated, or too rigid.

Do not invent missing helper scripts or pretend an unavailable tool exists.
Either add a genuinely useful support file within the requested scope or rewrite
the instruction to use available capabilities.

## 3. Write effective metadata

Frontmatter requires:

```yaml
---
name: lowercase-hyphenated-name
description: What the skill does and concrete situations in which to use it.
---
```

Add fields such as `license` or `compatibility` only when accurate. A `license`
entry must point to a license file that is actually bundled or otherwise state
the applicable license unambiguously.

The description is the trigger contract:

- Include the task and recognizable user language.
- Include important boundaries that prevent false triggers.
- Prefer specific intents over an exhaustive keyword list.
- Put trigger information in the description, not only in the body.

## 4. Use progressive disclosure

Keep `SKILL.md` below 500 lines and preferably much shorter.

```text
skill-name/
├── SKILL.md
├── scripts/       # deterministic, reusable automation
├── references/    # details read only when relevant
└── assets/        # templates or output resources
```

The body should contain the decision path and essential workflow. Move detailed
schemas, variant-specific instructions, and long examples into focused support
files. Link each support file with a sentence explaining when to read it.

Do not create support files merely to satisfy this layout.

## 5. Write robust instructions

- Use direct, imperative language and explain non-obvious reasons.
- Prefer a short decision tree over one mandatory workflow.
- Distinguish read-only inspection from mutating actions.
- Require confirmation for destructive, external, credentialed, or
  production-facing actions.
- Do not mandate subagents, browsers, MCP servers, or named file-edit tools;
  make them conditional on availability and usefulness.
- State expected paths relative to the skill directory or repository root.
- Use commands verified in the repository, not commands copied from another
  project.

For MiniSky work, common validation commands include:

```bash
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
cd ui && npm run lint
cd ui && npm run build
```

Use only commands relevant to the skill's output. The repository does not
provide a root `Makefile`, a Playwright dependency, or generic skill-evaluation
scripts.

## 6. Validate proportionally

Always perform static validation:

- YAML delimiters and required fields are valid.
- `name` matches the directory.
- `SKILL.md` is under 500 lines.
- Relative links resolve.
- Tool and command assumptions are conditional or verified.
- Instructions do not surprise the user or exceed the described scope.

For behavior validation, propose 2–5 realistic prompts:

- positive prompts covering distinct intended uses;
- near-miss negative prompts that should use another skill or no skill;
- edge cases involving missing tools, permissions, or ambiguous scope.

If independent runs are available and the user wants deeper evaluation, compare
the revised skill with the prior version or no-skill baseline. Keep the prompt
and inputs identical, judge objective criteria programmatically where possible,
and use human review for subjective quality. Do not fabricate timing, token, or
trigger statistics.

## 7. Iterate and report

Revise only where evidence indicates a problem. Generalize from failures rather
than overfitting to one test prompt. Remove instructions that add work without
improving outcomes.

Report:

- files changed;
- trigger and workflow improvements;
- unresolved limitations or missing dependencies;
- static checks and behavioral tests performed.
