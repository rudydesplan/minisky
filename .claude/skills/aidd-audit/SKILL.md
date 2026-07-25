---
name: aidd-audit
description: Audit MiniSky or a bounded subsystem read-only for code quality, architecture, security, dependencies, performance, tests, or UI health. Use for a codebase health check or ranked risk inventory. Do not use to review a diff, verify one feature, or fix findings.
license: MIT; adapted from https://github.com/ai-driven-dev/framework
---

# AIDD Audit

Produce an evidence-backed, read-only assessment. Do not edit files, create
artifacts, commit, push, or open a pull request.

> Adapted for MiniSky from the
> [AI-Driven Dev framework](https://github.com/ai-driven-dev/framework).

## Workflow

1. Resolve the requested scope and pillars. If the request is broad, state the
   pillars you will inspect; ask only when the boundary materially affects the
   result.
2. Read repository guidance and inspect relevant code, tests, configuration,
   dependency manifests, and documentation.
3. Run non-mutating checks only when they provide useful evidence. Examples are
   focused `go test` or `go vet` commands and `npm run lint` or `npm run build`
   from `ui/`. Do not install, upgrade, or rewrite dependencies.
4. Rank actionable findings by impact and confidence. Include a concrete
   `file:line`, consequence, and smallest credible remediation.
5. Report checked areas, skipped areas, failed checks, and unresolved unknowns.
   Return the report in chat unless the user explicitly requests a file.

## Pillars

- Code quality: correctness risks, complexity, error handling, and maintainability.
- Architecture: boundaries, coupling, state ownership, and documented decisions.
- Security: trust boundaries, input handling, secrets, Docker access, and auth.
- Dependencies: known risk evidence, license concerns, and unnecessary packages.
- Performance: measured or structurally evident hot paths; do not invent targets.
- Tests: critical-path gaps, brittleness, concurrency, restart, and failure coverage.
- UI: accessibility, state handling, responsiveness, and design consistency.

## Boundaries

- Distinguish verified defects from risks and suggestions.
- Do not claim vulnerability, performance, coverage, or compatibility facts
  without evidence.
- Use `aidd-review` for changed-line review and `aidd-refactor` only after the
  user asks to implement accepted findings.
