#!/usr/bin/env python3

import argparse
import json
import os
import pathlib


ALLOWED_FILES = (
    "harness-summary.txt",
    "minisky.log",
    "collector.log",
    "probe-curl.log",
    "unknown-curl.log",
    "otlp-inspection.txt",
    "traces.json",
    "requests.json",
    "metrics.txt",
    "safe-response.json",
    "secret-response.json",
    "probe-response.json",
    "unknown-response.json",
    "after-failure-response.json",
    "cross-project-replay.json",
    "replay.json",
)


def redact_all(content: bytes, forbidden: list[bytes]) -> bytes:
    while True:
        match_start = None
        match_value = None
        for value in forbidden:
            start = content.find(value)
            if start < 0:
                continue
            if (
                match_start is None
                or start < match_start
                or (start == match_start and len(value) > len(match_value))
            ):
                match_start = start
                match_value = value
        if match_start is None:
            return content
        content = content[:match_start] + content[match_start + len(match_value) :]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--destination-dir", required=True)
    parser.add_argument("--forbidden-file", required=True)
    parser.add_argument("--max-file-bytes", type=int, required=True)
    parser.add_argument("--max-total-bytes", type=int, required=True)
    args = parser.parse_args()

    if args.max_file_bytes < 1 or args.max_total_bytes < 1:
        raise SystemExit("diagnostic limits must be positive")
    source = pathlib.Path(args.source_dir).resolve()
    destination = pathlib.Path(args.destination_dir)
    if destination.is_symlink():
        raise SystemExit("diagnostic destination must not be a symlink")
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination.chmod(0o700)
    for existing in destination.iterdir():
        if existing.is_dir() and not existing.is_symlink():
            raise SystemExit("diagnostic destination must not contain directories")
        existing.unlink()

    forbidden_payload = json.loads(pathlib.Path(args.forbidden_file).read_text())
    forbidden = set()
    for label, value in forbidden_payload.items():
        if (
            not isinstance(label, str)
            or not label.strip()
            or not isinstance(value, str)
            or not value
            or len(value.encode()) > 4096
        ):
            raise SystemExit("forbidden values must have non-empty string labels and values")
        forbidden.add(value.encode())
    if not forbidden:
        raise SystemExit("forbidden values must not be empty")
    forbidden = sorted(forbidden, key=len, reverse=True)
    overlap = max(len(value) for value in forbidden)

    remaining = args.max_total_bytes
    for name in ALLOWED_FILES:
        if remaining == 0:
            break
        candidate = source / name
        if candidate.is_symlink() or not candidate.is_file():
            continue
        limit = min(args.max_file_bytes, remaining)
        with candidate.open("rb") as handle:
            content = handle.read(limit + overlap)
        truncated = len(content) > limit
        content = redact_all(content, forbidden)
        if truncated:
            content = redact_all(content + b"\n[truncated]\n", forbidden)
        content = content[:limit]
        output = destination / name
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(output, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
        remaining -= len(content)


if __name__ == "__main__":
    main()
