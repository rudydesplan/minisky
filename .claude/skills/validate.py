#!/usr/bin/env python3
"""Validate MiniSky's project-local agent skill collection."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXPECTED_SKILLS = 45
NAME_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")

ECC_SKILLS = {
    "api-design",
    "architecture-decision-records",
    "benchmark",
    "codebase-onboarding",
    "database-migrations",
    "deployment-patterns",
    "docker-patterns",
    "golang-patterns",
    "golang-testing",
    "kubernetes-patterns",
    "production-audit",
    "security-review",
    "tdd-workflow",
}
IMPECCABLE_SKILLS = {
    "minisky-audit",
    "minisky-craft",
    "minisky-critique",
    "minisky-harden",
    "minisky-polish",
}
ANTHROPIC_APACHE_SKILLS = {"skill-creator", "webapp-testing", "mcp-builder"}

FORBIDDEN_TEXT = {
    "go test ./...": "use the repository's scoped race-test command",
    "go vet ./...": "use the repository's scoped vet command",
    "OperationManager is thread-safe": (
        "the manager returns live pointers; describe its synchronization boundary"
    ),
}


def parse_frontmatter(path: Path, lines: list[str]) -> dict[str, str]:
    if not lines or lines[0] != "---":
        raise ValueError("missing opening YAML delimiter")
    try:
        end = lines.index("---", 1)
    except ValueError as error:
        raise ValueError("missing closing YAML delimiter") from error

    result: dict[str, str] = {}
    for line in lines[1:end]:
        if line.startswith((" ", "\t")) or ":" not in line:
            continue
        key, value = line.split(":", 1)
        result[key.strip()] = value.strip()
    return result


def validate_links(path: Path, text: str) -> list[str]:
    errors: list[str] = []
    for target in LINK_RE.findall(text):
        clean = target.split("#", 1)[0].strip()
        if not clean or clean.startswith(("http://", "https://", "mailto:")):
            continue
        if not (path.parent / clean).resolve().exists():
            errors.append(f"{path}: broken relative link {target!r}")
    return errors


def expected_license(name: str) -> str | None:
    if name in ECC_SKILLS or name.startswith("aidd-"):
        return "MIT"
    if name in IMPECCABLE_SKILLS or name in ANTHROPIC_APACHE_SKILLS:
        return "Apache-2.0"
    return None


def main() -> int:
    errors: list[str] = []
    names: dict[str, Path] = {}
    skill_files = sorted(ROOT.glob("*/SKILL.md"))

    if len(skill_files) != EXPECTED_SKILLS:
        errors.append(
            f"expected {EXPECTED_SKILLS} skills, found {len(skill_files)}; "
            "update the inventory deliberately"
        )

    for path in skill_files:
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        relative = path.relative_to(ROOT)

        if len(lines) > 500:
            errors.append(f"{relative}: {len(lines)} lines exceeds the 500-line limit")

        try:
            metadata = parse_frontmatter(path, lines)
        except ValueError as error:
            errors.append(f"{relative}: {error}")
            continue

        name = metadata.get("name", "")
        description = metadata.get("description", "")
        if not NAME_RE.fullmatch(name):
            errors.append(f"{relative}: invalid or missing name {name!r}")
        if name != path.parent.name:
            errors.append(
                f"{relative}: frontmatter name {name!r} does not match directory"
            )
        if not description or description in {"|", ">"}:
            errors.append(f"{relative}: description must be a non-empty single line")
        if name in names:
            errors.append(f"{relative}: duplicate name also used by {names[name]}")
        names[name] = relative

        required_license = expected_license(name)
        license_value = metadata.get("license", "")
        if required_license and not license_value.startswith(required_license):
            errors.append(
                f"{relative}: expected {required_license} source-license metadata"
            )

        errors.extend(validate_links(path, text))
        for forbidden, replacement in FORBIDDEN_TEXT.items():
            if any(line.strip() == forbidden for line in lines):
                errors.append(f"{relative}: contains {forbidden!r}; {replacement}")

        support_entries = [entry for entry in path.parent.iterdir() if entry.name != "SKILL.md"]
        for entry in support_entries:
            if entry.is_symlink():
                errors.append(f"{relative}: symlinked support content is not allowed")

    unexpected = [
        directory
        for directory in ROOT.iterdir()
        if (
            directory.is_dir()
            and not directory.name.startswith((".", "__"))
            and not (directory / "SKILL.md").is_file()
        )
    ]
    for directory in unexpected:
        errors.append(f"{directory.relative_to(ROOT)}: skill directory lacks SKILL.md")

    if errors:
        for error in sorted(errors):
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"Validated {len(skill_files)} MiniSky skills.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
