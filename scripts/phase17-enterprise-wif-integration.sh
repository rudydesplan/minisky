#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE17_ENTERPRISE_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Phase-17 enterprise WIF integration without MINISKY_PHASE17_ENTERPRISE_INTEGRATION=1." >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "${script_dir}/.." && pwd)"
phase13_script="${repository_root}/scripts/phase13-wif-integration.sh"
if [[ ! -f "${phase13_script}" ]]; then
  echo "Phase-13 WIF integration harness was not found under the resolved repository root." >&2
  exit 1
fi

exec env \
  MINISKY_PHASE13_INTEGRATION=1 \
  MINISKY_PHASE13_ENTERPRISE_CONTROLS=1 \
  "${phase13_script}"
