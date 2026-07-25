#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  airgap-bundle.sh create --output DIR --binary FILE [--image IMAGE]
  airgap-bundle.sh verify --bundle DIR [--load-image]

Creation uses only a supplied local binary and an already-present local image.
Verification checks every SHA-256 before optionally loading the image archive.
EOF
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "SHA-256 tool not found (need sha256sum or shasum)" >&2
    return 1
  fi
}

command_name="${1:-}"
if [[ -z "$command_name" ]]; then
  usage
  exit 2
fi
shift

case "$command_name" in
  create)
    output=""
    binary=""
    image=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --output) output="${2:-}"; shift 2 ;;
        --binary) binary="${2:-}"; shift 2 ;;
        --image) image="${2:-}"; shift 2 ;;
        *) echo "Unknown create option: $1" >&2; usage; exit 2 ;;
      esac
    done
    [[ -n "$output" && -n "$binary" ]] || { usage; exit 2; }
    [[ -f "$binary" ]] || { echo "Binary not found: $binary" >&2; exit 1; }
    if [[ -e "$output" ]]; then
      [[ -d "$output" && -z "$(ls -A "$output")" ]] || {
        echo "Output directory must be absent or empty: $output" >&2
        exit 1
      }
    else
      mkdir -p "$output"
    fi
    chmod 700 "$output"
    cp "$binary" "$output/minisky"
    chmod 700 "$output/minisky"

    image_json="null"
    files=("minisky")
    if [[ -n "$image" ]]; then
      [[ "$image" =~ ^[A-Za-z0-9._/@:-]+$ ]] || { echo "Unsafe image reference" >&2; exit 1; }
      command -v docker >/dev/null 2>&1 || { echo "docker is required to bundle an image" >&2; exit 1; }
      docker image inspect "$image" >/dev/null
      docker save --output "$output/minisky-image.tar" "$image"
      files+=("minisky-image.tar")
      image_json="\"$image\""
    fi

    manifest_files='"minisky", "manifest.json"'
    if [[ -n "$image" ]]; then
      manifest_files='"minisky", "minisky-image.tar", "manifest.json"'
    fi
    cat >"$output/manifest.json" <<EOF
{
  "format": "minisky-airgap-bundle",
  "version": 1,
  "binary": "minisky",
  "image": $image_json,
  "files": [$manifest_files]
}
EOF
    chmod 600 "$output/manifest.json"
    files+=("manifest.json")
    : >"$output/SHA256SUMS"
    for file in "${files[@]}"; do
      printf '%s  %s\n' "$(sha256_file "$output/$file")" "$file" >>"$output/SHA256SUMS"
    done
    chmod 600 "$output/SHA256SUMS"
    echo "Created local bundle at $output"
    ;;

  verify)
    bundle=""
    load_image=0
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --bundle) bundle="${2:-}"; shift 2 ;;
        --load-image) load_image=1; shift ;;
        *) echo "Unknown verify option: $1" >&2; usage; exit 2 ;;
      esac
    done
    [[ -n "$bundle" ]] || { usage; exit 2; }
    [[ -f "$bundle/SHA256SUMS" && -f "$bundle/manifest.json" ]] || {
      echo "Bundle metadata is incomplete: $bundle" >&2
      exit 1
    }
    command -v python3 >/dev/null 2>&1 || { echo "python3 is required to verify a bundle" >&2; exit 1; }
    verified="$(python3 - "$bundle" <<'PY'
import hashlib
import json
import pathlib
import re
import sys

bundle = pathlib.Path(sys.argv[1])
manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
if manifest.get("format") != "minisky-airgap-bundle" or manifest.get("version") != 1:
    raise SystemExit("Unsupported air-gap manifest")
if manifest.get("binary") != "minisky":
    raise SystemExit("Manifest binary must be minisky")
image = manifest.get("image")
expected = ["minisky", "manifest.json"]
if image is not None:
    if not isinstance(image, str) or not image:
        raise SystemExit("Manifest image reference is invalid")
    expected.insert(1, "minisky-image.tar")
files = manifest.get("files")
if not isinstance(files, list) or any(not isinstance(item, str) for item in files):
    raise SystemExit("Manifest files must be a string list")
if len(files) != len(set(files)):
    raise SystemExit("Manifest contains duplicate file entries")
if set(files) != set(expected):
    raise SystemExit("Manifest files do not exactly match the expected bundle")

entries = {}
line_pattern = re.compile(r"^([A-Fa-f0-9]{64})  ([A-Za-z0-9._-]+)$")
for line in (bundle / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
    match = line_pattern.fullmatch(line)
    if match is None:
        raise SystemExit("Invalid checksum entry")
    digest, name = match.groups()
    if name in entries:
        raise SystemExit(f"Duplicate checksum entry: {name}")
    if name not in expected:
        raise SystemExit(f"Unexpected checksum entry: {name}")
    entries[name] = digest.lower()
if set(entries) != set(expected):
    missing = sorted(set(expected) - set(entries))
    raise SystemExit(f"Missing checksum entries: {', '.join(missing)}")

physical = {path.name for path in bundle.iterdir() if path.is_file()}
if physical != set(expected) | {"SHA256SUMS"}:
    raise SystemExit("Bundle contains unexpected or missing files")
for name in expected:
    actual = hashlib.sha256((bundle / name).read_bytes()).hexdigest()
    if actual != entries[name]:
        raise SystemExit(f"Checksum mismatch: {name}")
print(len(expected))
PY
)"
    if [[ "$load_image" -eq 1 ]]; then
      has_image="$(python3 -c 'import json,sys; print("yes" if json.load(open(sys.argv[1], encoding="utf-8")).get("image") else "no")' "$bundle/manifest.json")"
      [[ "$has_image" == "yes" ]] || { echo "Bundle has no image archive" >&2; exit 1; }
      command -v docker >/dev/null 2>&1 || { echo "docker is required to load the image" >&2; exit 1; }
      docker load --input "$bundle/minisky-image.tar"
    fi
    echo "Verified $verified checksummed bundle file(s)"
    ;;

  *)
    usage
    exit 2
    ;;
esac
