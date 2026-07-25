#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_LINUX_PACKAGE_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to modify the host package database without MINISKY_LINUX_PACKAGE_INTEGRATION=1." >&2
  exit 2
fi

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Linux package validation requires a native Linux host." >&2
  exit 2
fi

case "$(uname -m)" in
  x86_64)
    goarch=amd64
    ;;
  aarch64)
    goarch=arm64
    ;;
  *)
    echo "Unsupported native Linux architecture: $(uname -m)" >&2
    exit 2
    ;;
esac

for command in dpkg-deb goreleaser python3 rpm tar; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done

if [[ ! -f ui/dist/index.html ]]; then
  echo "UI assets are missing; build or download ui/dist before packaging." >&2
  exit 1
fi

as_root() {
  if [[ "${EUID}" -eq 0 ]]; then
    "$@"
  else
    sudo -- "$@"
  fi
}

if [[ "${EUID}" -ne 0 ]]; then
  if ! command -v sudo >/dev/null 2>&1 || ! sudo -n true; then
    echo "Passwordless sudo or a root shell is required for package installation." >&2
    exit 1
  fi
fi

if dpkg-query -W -f='${Status}\n' minisky 2>/dev/null | grep -Fqx 'install ok installed'; then
  echo "Refusing to replace an existing dpkg installation of minisky." >&2
  exit 1
fi
if rpm --quiet --query minisky; then
  echo "Refusing to replace an existing rpm installation of minisky." >&2
  exit 1
fi
if [[ -e /usr/bin/minisky || -L /usr/bin/minisky ]]; then
  echo "Refusing to replace an existing /usr/bin/minisky." >&2
  exit 1
fi

lock_dir="${TMPDIR:-/tmp}/minisky-linux-package-integration.lock"
if ! mkdir "${lock_dir}" 2>/dev/null; then
  echo "Another MiniSky Linux package integration run is active." >&2
  exit 1
fi

work_dir="$(mktemp -d)"
installed_format=""

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ "${installed_format}" == "deb" ]]; then
    as_root dpkg --remove minisky >/dev/null 2>&1 || true
  elif [[ "${installed_format}" == "rpm" ]]; then
    as_root rpm --erase minisky >/dev/null 2>&1 || true
  fi
  rm -rf "${work_dir}"
  rmdir "${lock_dir}" 2>/dev/null || true
  exit "${exit_code}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

MINISKY_GORELEASER_DIST="${work_dir}/dist" \
MINISKY_GORELEASER_TARGET="linux-${goarch}" \
  goreleaser release \
    --snapshot \
    --clean \
    --skip=announce,publish,archive,before,homebrew,scoop

artifacts_file="${work_dir}/dist/artifacts.json"
if [[ ! -s "${artifacts_file}" ]]; then
  echo "GoReleaser did not emit artifact metadata." >&2
  exit 1
fi

mapfile -t packages < <(
  python3 - "${artifacts_file}" "${work_dir}/dist" "${goarch}" <<'PY'
import json
import os
import pathlib
import sys

metadata_path, dist_path, expected_arch = sys.argv[1:]
with open(metadata_path, encoding="utf-8") as stream:
    metadata = json.load(stream)
artifacts = metadata.get("artifacts", []) if isinstance(metadata, dict) else metadata
if not isinstance(artifacts, list):
    raise SystemExit("unexpected GoReleaser artifact metadata shape")

dist = pathlib.Path(dist_path).resolve()
matches = {"deb": [], "rpm": []}
for artifact in artifacts:
    if not isinstance(artifact, dict):
        continue
    if artifact.get("goos") != "linux" or artifact.get("goarch") != expected_arch:
        continue
    name = str(artifact.get("name", ""))
    package_format = next((kind for kind in matches if name.endswith("." + kind)), None)
    if package_format is None:
        continue
    raw_path = pathlib.Path(str(artifact.get("path", "")))
    artifact_path = raw_path if raw_path.is_absolute() else pathlib.Path.cwd() / raw_path
    artifact_path = artifact_path.resolve()
    if os.path.commonpath((dist, artifact_path)) != str(dist):
        raise SystemExit(f"artifact escaped the configured output directory: {artifact_path}")
    if not artifact_path.is_file():
        raise SystemExit(f"artifact metadata references a missing package: {artifact_path}")
    matches[package_format].append(str(artifact_path))

for package_format in ("deb", "rpm"):
    paths = matches[package_format]
    if len(paths) != 1:
        raise SystemExit(
            f"expected exactly one {package_format} for linux/{expected_arch}, found {len(paths)}"
        )
    print(paths[0])
PY
)

if [[ "${#packages[@]}" -ne 2 ]]; then
  echo "Package artifact discovery returned an incomplete result." >&2
  exit 1
fi

validate_paths() {
  local entries_file=$1
  local package_file=$2
  python3 - "${entries_file}" "${package_file}" <<'PY'
import pathlib
import sys

entries_path, package_path = sys.argv[1:]
entries = pathlib.Path(entries_path).read_text(encoding="utf-8").splitlines()
normalized = []
allowed = {"usr", "usr/bin", "usr/bin/minisky"}
for entry in entries:
    if "\\" in entry:
        raise SystemExit(f"unsafe package path in {package_path}: {entry!r}")
    value = entry
    while value.startswith("./"):
        value = value[2:]
    value = value.lstrip("/")
    if not value:
        continue
    parts = pathlib.PurePosixPath(value).parts
    if ".." in parts:
        raise SystemExit(f"unsafe package path in {package_path}: {entry!r}")
    normalized_path = "/".join(parts)
    if normalized_path not in allowed:
        raise SystemExit(f"unexpected package path in {package_path}: {entry!r}")
    normalized.append(normalized_path)
if normalized.count("usr/bin/minisky") != 1:
    raise SystemExit(f"{package_path} must contain exactly one /usr/bin/minisky")
PY
}

assert_installed_binary() {
  if [[ ! -f /usr/bin/minisky || ! -x /usr/bin/minisky || -L /usr/bin/minisky ]]; then
    echo "Installed /usr/bin/minisky is not a regular executable file." >&2
    exit 1
  fi
}

assert_removed() {
  local package_format=$1
  if [[ -e /usr/bin/minisky || -L /usr/bin/minisky ]]; then
    echo "${package_format} uninstall left /usr/bin/minisky behind." >&2
    exit 1
  fi
  if dpkg-query -W -f='${Status}\n' minisky 2>/dev/null | grep -Fqx 'install ok installed'; then
    echo "${package_format} uninstall left the dpkg package installed." >&2
    exit 1
  fi
  if rpm --quiet --query minisky; then
    echo "${package_format} uninstall left the rpm package installed." >&2
    exit 1
  fi
}

deb_package="${packages[0]}"
rpm_package="${packages[1]}"

deb_entries="${work_dir}/deb-entries.txt"
dpkg-deb --fsys-tarfile "${deb_package}" | tar -tf - >"${deb_entries}"
validate_paths "${deb_entries}" "${deb_package}"
installed_format=deb
as_root dpkg --install "${deb_package}"
assert_installed_binary
/usr/bin/minisky version
/usr/bin/minisky doctor bigquery
as_root dpkg --remove minisky
installed_format=""
assert_removed deb

rpm_entries="${work_dir}/rpm-entries.txt"
rpm --query --package --list "${rpm_package}" >"${rpm_entries}"
validate_paths "${rpm_entries}" "${rpm_package}"
installed_format=rpm
as_root rpm --install "${rpm_package}"
assert_installed_binary
/usr/bin/minisky version
/usr/bin/minisky doctor bigquery
as_root rpm --erase minisky
installed_format=""
assert_removed rpm

echo "Native linux/${goarch} deb and rpm install validation passed."
