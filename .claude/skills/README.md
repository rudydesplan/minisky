# MiniSky Agent Skills

This collection contains 45 project skills. They are guidance, not authority to
perform side effects that the user did not request.

## Selection precedence

When several skills match, use the smallest applicable set:

1. A MiniSky-specific task skill (`minisky-*`, `gcp-api-fidelity`) owns the
   repository contract.
2. `minisky-quality-floor` and `tdd-workflow` are companion guardrails for
   implementation, not standalone workflows.
3. A language or infrastructure skill adds focused technique without
   overriding the MiniSky-specific contract.
4. An `aidd-*` workflow is selected only when the user requests that workflow
   or its distinct outcome.
5. Imported generic guidance never overrides repository rules, current code,
   or explicit user instructions.

Important boundaries:

- `minisky-craft` owns a greenfield service or major vertical slice;
  `minisky-shim-builder` owns bounded shim mechanics or extension.
- `minisky-polish` fixes an accepted defect list; `minisky-harden` discovers
  failure modes; `minisky-audit` inventories implementation defects;
  `minisky-critique` evaluates a design decision.
- `minisky-contract-test` owns MiniSky HTTP contract suites;
  `golang-testing` owns general Go test technique; `webapp-testing` owns live
  browser behavior.
- Commit, push, pull-request, deployment, credentialed, and destructive
  actions require explicit authorization regardless of the selected skill.

## Categories

- 10 MiniSky product skills
- 6 infrastructure and frontend skills
- 13 ECC-derived engineering skills
- 3 Apache-licensed Anthropic adaptations and 1 independently rewritten
  documentation skill
- 12 AIDD workflow skills

The imported sources and applicable terms are recorded in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Validation

Run the collection validator from the repository root:

```bash
python3 .claude/skills/validate.py
```

It checks the collection's supported frontmatter shape, unique names,
directory/name agreement, descriptions, line limits, relative links,
source-license metadata, and known stale instructions. It is not a general YAML
parser and does not prove that a skill will trigger correctly; use the positive
and near-miss prompts described by `skill-creator` for behavioral evaluation.
