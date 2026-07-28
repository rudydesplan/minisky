#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temp="$(mktemp -d)"
trap 'rm -rf "$temp"' EXIT

mkdir -p "$temp/bin"
cat >"$temp/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1 $2" in
  "image inspect") exit 0 ;;
  "save --output")
    printf 'offline image bytes\n' >"$3"
    ;;
  "load --input")
    [[ -f "$3" && ! -L "$3" ]] || { echo "docker load input is not a regular file" >&2; exit 1; }
    [[ "$3" != "${DOCKER_FORBIDDEN_INPUT:-}" ]] || {
      echo "docker load used mutable bundle input" >&2
      exit 1
    }
    [[ -z "${DOCKER_MUTATE_ORIGINAL:-}" ]] || printf 'tampered after verification\n' >"$DOCKER_MUTATE_ORIGINAL"
    [[ "$(cat "$3")" == "offline image bytes" ]] || { echo "docker load input changed" >&2; exit 1; }
    [[ -n "${DOCKER_LOAD_LOG:-}" ]] && printf 'loaded\n' >>"${DOCKER_LOAD_LOG}"
    exit 0
    ;;
  *) echo "unexpected docker arguments: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "$temp/bin/docker"

cat >"$temp/minisky" <<'EOF'
#!/usr/bin/env bash
echo "minisky test artifact"
EOF
chmod +x "$temp/minisky"

PATH="$temp/bin:$PATH" "$root/scripts/airgap-bundle.sh" create \
  --output "$temp/bundle" \
  --binary "$temp/minisky" \
  --image "ghcr.io/example/minisky@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
PATH="$temp/bin:$PATH" "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/bundle"
cp -R "$temp/bundle" "$temp/load-bundle"
DOCKER_FORBIDDEN_INPUT="$temp/load-bundle/minisky-image.tar" \
  DOCKER_MUTATE_ORIGINAL="$temp/load-bundle/minisky-image.tar" \
  DOCKER_LOAD_LOG="$temp/load.log" PATH="$temp/bin:$PATH" \
  "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/load-bundle" --load-image
[[ -s "$temp/load.log" ]] || { echo "verified image was not loaded" >&2; exit 1; }

cp -R "$temp/bundle" "$temp/missing-checksum"
python3 - "$temp/missing-checksum/SHA256SUMS" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
path.write_text("\n".join(line for line in path.read_text().splitlines() if not line.endswith("minisky-image.tar")) + "\n")
PY
: >"$temp/load.log"
if DOCKER_LOAD_LOG="$temp/load.log" PATH="$temp/bin:$PATH" \
  "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/missing-checksum" --load-image; then
  echo "bundle with missing image checksum unexpectedly verified" >&2
  exit 1
fi
[[ ! -s "$temp/load.log" ]] || { echo "unchecksummed image was loaded" >&2; exit 1; }

cp -R "$temp/bundle" "$temp/duplicate-checksum"
first_checksum="$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text().splitlines()[0])' "$temp/duplicate-checksum/SHA256SUMS")"
printf '%s\n' "$first_checksum" >>"$temp/duplicate-checksum/SHA256SUMS"
if PATH="$temp/bin:$PATH" "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/duplicate-checksum"; then
  echo "bundle with duplicate checksum unexpectedly verified" >&2
  exit 1
fi

cp -R "$temp/bundle" "$temp/unexpected-entry"
printf 'unexpected\n' >"$temp/unexpected-entry/extra"
printf '%064d  extra\n' 0 >>"$temp/unexpected-entry/SHA256SUMS"
if PATH="$temp/bin:$PATH" "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/unexpected-entry"; then
  echo "bundle with unexpected checksum entry unexpectedly verified" >&2
  exit 1
fi

cp -R "$temp/bundle" "$temp/unexpected-directory"
mkdir "$temp/unexpected-directory/extra"
if PATH="$temp/bin:$PATH" "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/unexpected-directory"; then
  echo "bundle with unexpected directory unexpectedly verified" >&2
  exit 1
fi

cp -R "$temp/bundle" "$temp/symlink-entry"
cp "$temp/symlink-entry/minisky" "$temp/outside-minisky"
rm "$temp/symlink-entry/minisky"
ln -s "$temp/outside-minisky" "$temp/symlink-entry/minisky"
if PATH="$temp/bin:$PATH" "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/symlink-entry"; then
  echo "bundle with symlinked artifact unexpectedly verified" >&2
  exit 1
fi

printf 'tamper\n' >>"$temp/bundle/minisky"
if PATH="$temp/bin:$PATH" "$root/scripts/airgap-bundle.sh" verify --bundle "$temp/bundle"; then
  echo "tampered bundle unexpectedly verified" >&2
  exit 1
fi
