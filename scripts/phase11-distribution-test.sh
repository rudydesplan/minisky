#!/usr/bin/env bash

set -Eeuo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repository_root}"

usage() {
  cat <<'EOF'
Usage: phase11-distribution-test.sh [--static|--self-test]

--static validates local distribution contracts without publishing or changing
the host package database. Set MINISKY_PHASE11_DISTRIBUTION_BUILD=1 to also run
the native deb/rpm build, install, smoke-test, and uninstall lifecycle.
EOF
}

validate_distribution_contract() {
  local root=$1
  python3 - "${root}" <<'PY'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])

def read(relative):
    path = root / relative
    if not path.is_file():
        raise SystemExit(f"missing distribution input: {relative}")
    return path.read_text(encoding="utf-8")

def require(relative, text, expected):
    if expected not in text:
        raise SystemExit(f"{relative} is missing distribution contract: {expected}")

goreleaser = read(".goreleaser.yaml")
require(".goreleaser.yaml", goreleaser, "version: 2")
require(".goreleaser.yaml", goreleaser, 'name_template: "{{ .ProjectName }}_{{ .Os }}_{{ .Arch }}"')
require(".goreleaser.yaml", goreleaser, "formats: [tar.gz]")
require(".goreleaser.yaml", goreleaser, "formats: [zip]")
require(".goreleaser.yaml", goreleaser, "name_template: 'checksums.txt'")
for build_id in ("linux-amd64", "linux-arm64", "darwin-arm64", "windows-amd64"):
    require(".goreleaser.yaml", goreleaser, f"- id: {build_id}")
for packaged_build in ("      - linux-amd64", "      - linux-arm64"):
    require(".goreleaser.yaml", goreleaser, packaged_build)
for package_format in ("      - deb", "      - rpm"):
    require(".goreleaser.yaml", goreleaser, package_format)
for archive_entry in ("      - README.md", "      - CONTRIBUTING.md", "      - docs/*", "      - LICENSE*"):
    require(".goreleaser.yaml", goreleaser, archive_entry)

release = read(".github/workflows/release.yml")
for artifact in (
    "minisky_linux_amd64.tar.gz",
    "minisky_linux_arm64.tar.gz",
    "minisky_darwin_arm64.tar.gz",
    "minisky_windows_amd64.zip",
):
    require(".github/workflows/release.yml", release, artifact)
require(".github/workflows/release.yml", release, "goreleaser release --clean --skip=announce,publish,before,nfpm,homebrew,scoop")

action = read(".github/actions/setup-minisky/index.mjs")
for target in (
    '"linux-x64": ["linux", "amd64", "tar.gz", "minisky"]',
    '"linux-arm64": ["linux", "arm64", "tar.gz", "minisky"]',
    '"darwin-arm64": ["darwin", "arm64", "tar.gz", "minisky"]',
    '"win32-x64": ["windows", "amd64", "zip", "minisky.exe"]',
):
    require(".github/actions/setup-minisky/index.mjs", action, target)
require(".github/actions/setup-minisky/index.mjs", action, "minisky_${releaseOS}_${releaseArch}.${format}")

package_test = read("scripts/linux-package-integration.sh")
require("scripts/linux-package-integration.sh", package_test, 'MINISKY_LINUX_PACKAGE_INTEGRATION:-')
require("scripts/linux-package-integration.sh", package_test, "--snapshot")
require("scripts/linux-package-integration.sh", package_test, "/usr/bin/minisky doctor bigquery")

read("scripts/airgap-bundle.sh")
read("scripts/airgap-bundle-test.sh")
read("deployments/docker-compose.yml")
PY
}

native_package_mode() {
  case "${MINISKY_PHASE11_DISTRIBUTION_BUILD:-0}" in
    0) return 1 ;;
    1) return 0 ;;
    *)
      echo "MINISKY_PHASE11_DISTRIBUTION_BUILD must be 0 or 1." >&2
      return 2
      ;;
  esac
}

validate_native_host() {
  if [[ "$(uname -s)" != "Linux" ]]; then
    echo "Native package lifecycle requires Linux." >&2
    return 2
  fi
  case "$(uname -m)" in
    x86_64|aarch64) ;;
    *)
      echo "Native package lifecycle does not support architecture $(uname -m)." >&2
      return 2
      ;;
  esac
}

run_static_checks() {
  for command in docker goreleaser node python3; do
    if ! command -v "${command}" >/dev/null 2>&1; then
      echo "Required static distribution tool not found: ${command}" >&2
      return 1
    fi
  done

  validate_distribution_contract "${repository_root}"
  goreleaser check
  node --check .github/actions/setup-minisky/index.mjs
  node --check .github/actions/setup-minisky/cleanup.mjs
  bash -n \
    scripts/phase11-distribution-test.sh \
    scripts/linux-package-integration.sh \
    scripts/airgap-bundle.sh \
    scripts/airgap-bundle-test.sh
  ./scripts/airgap-bundle-test.sh
  MINISKY_IMAGE=ghcr.io/qamarudeenm/minisky:phase11-static \
    docker compose -f deployments/docker-compose.yml config >/dev/null

  echo "Phase 11 static distribution validation passed; nothing was published."
}

run_self_test() {
  local temp
  temp="$(mktemp -d)"
  trap 'rm -rf "${temp}"' RETURN

  mkdir -p \
    "${temp}/.github/actions/setup-minisky" \
    "${temp}/.github/workflows" \
    "${temp}/deployments" \
    "${temp}/scripts"
  cp .goreleaser.yaml "${temp}/.goreleaser.yaml"
  cp .github/actions/setup-minisky/index.mjs "${temp}/.github/actions/setup-minisky/index.mjs"
  cp .github/workflows/release.yml "${temp}/.github/workflows/release.yml"
  cp deployments/docker-compose.yml "${temp}/deployments/docker-compose.yml"
  cp scripts/airgap-bundle.sh scripts/airgap-bundle-test.sh \
    scripts/linux-package-integration.sh "${temp}/scripts/"

  validate_distribution_contract "${temp}"
  python3 - "${temp}/.goreleaser.yaml" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
path.write_text(path.read_text(encoding="utf-8").replace("version: 2", "version: 1", 1), encoding="utf-8")
PY
  if validate_distribution_contract "${temp}" >/dev/null 2>&1; then
    echo "Self-test accepted an invalid GoReleaser major version." >&2
    return 1
  fi
  MINISKY_PHASE11_DISTRIBUTION_BUILD=0 native_package_mode && {
    echo "Self-test treated the default guard as enabled." >&2
    return 1
  }
  MINISKY_PHASE11_DISTRIBUTION_BUILD=1 native_package_mode || {
    echo "Self-test did not accept the explicit build guard." >&2
    return 1
  }
  if MINISKY_PHASE11_DISTRIBUTION_BUILD=yes native_package_mode >/dev/null 2>&1; then
    echo "Self-test accepted an invalid build guard." >&2
    return 1
  fi

  echo "Phase 11 distribution self-test passed."
}

mode="${1:---static}"
if [[ $# -gt 1 ]]; then
  usage >&2
  exit 2
fi

case "${mode}" in
  --static)
    run_static_checks
    if native_package_mode; then
      validate_native_host
      MINISKY_LINUX_PACKAGE_INTEGRATION=1 ./scripts/linux-package-integration.sh
    else
      status=$?
      if [[ "${status}" -ne 1 ]]; then
        exit "${status}"
      fi
      echo "Native deb/rpm lifecycle skipped; set MINISKY_PHASE11_DISTRIBUTION_BUILD=1 on supported Linux to enable it."
    fi
    ;;
  --self-test)
    run_self_test
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
