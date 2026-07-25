---
name: doc-coauthoring
description: Collaboratively create or substantially revise MiniSky design documents, ADRs, contributor guides, compatibility references, and operational documentation. Use when the user wants help shaping a document through evidence, drafting, and review. Do not use for a trivial typo, generated API reference, or undocumented product claims.
---

# MiniSky Documentation Co-Authoring

Build documentation from repository evidence and an explicit reader need.

## Establish the contract

Before drafting, identify:

- the document type, path, intended reader, and decision or task it supports;
- the facts that must be verified in code, tests, workflows, or existing docs;
- non-goals and information that must remain confidential;
- whether the user wants an outline, a draft, direct edits, or review only.

Read nearby documents and the implementation they describe. Do not copy stale
claims forward merely because they already exist.

## Choose the smallest workflow

### Outline

Use when structure or scope is uncertain:

1. State the reader's question.
2. Propose a short section order.
3. Attach evidence requirements to each section.
4. Resolve only choices that materially change the document.

### Draft or revise

Use when the goal is clear:

1. Write the outcome or decision first.
2. Separate current behavior, limitations, and future work.
3. Use exact commands, paths, configuration names, and compatibility labels.
4. Prefer a short example that is executable over a broad hypothetical.
5. Link to the authoritative local document instead of duplicating volatile
   matrices or release details.

### Review

Use when text already exists:

1. Check correctness against current repository evidence.
2. Check whether a new reader can act without hidden context.
3. Remove repetition, unsupported certainty, and marketing language.
4. Flag decisions, risks, or prerequisites that are buried.
5. Preserve the author's voice unless clarity requires a change.

## MiniSky truthfulness rules

- Distinguish executable, emulator-backed, metadata-only, passthrough, and
  unsupported behavior.
- Do not describe planned roadmap work as shipped.
- Do not call a validator rule full Discovery-document conformance.
- Treat state exports as sensitive and metadata-only.
- Qualify Docker, CGO, Kind, Buildpacks, Terraform, and platform claims with the
  executable evidence that supports them.
- Keep CLI, environment-variable, endpoint, and release instructions aligned
  with current code and CI.

## Verification

Use checks appropriate to the document:

- verify every referenced repository path exists;
- execute safe commands whose output is part of the instructions;
- run `python3 .claude/skills/validate.py` when skill docs change;
- run link, workflow, Terraform, Go, or UI checks only when the edited claims
  depend on them.

Do not run destructive examples, installers, releases, or Docker cleanup merely
to validate prose. Report unverified claims and prerequisites explicitly.

## Handoff

Summarize:

- the reader outcome improved;
- material factual or structural changes;
- evidence checked and commands run;
- unresolved decisions or claims that still require verification.
