#!/usr/bin/env bash

set -Eeuo pipefail

for command in go mktemp; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/minisky-install-pack.XXXXXX")"
trap 'rm -rf "${work_dir}"' EXIT

cat >"${work_dir}/main.go" <<'EOF'
package main

import (
	"context"
	"fmt"
	"os"

	"minisky/pkg/orchestrator"
)

func main() {
	if err := orchestrator.InstallToolDependency(context.Background(), "pack"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
EOF

(
  cd "${repo_root}"
  go run "${work_dir}/main.go"
)

pack_path="${HOME}/.minisky/bin/pack"
if [[ ! -x "${pack_path}" ]]; then
  echo "Pack installer did not create ${pack_path}" >&2
  exit 1
fi

printf '%s\n' "${pack_path}"
