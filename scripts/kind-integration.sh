#!/usr/bin/env bash

set -Eeuo pipefail

validate_cluster_name() {
  [[ "$1" =~ ^minisky-ci-[a-z0-9]([a-z0-9-]{0,50}[a-z0-9])?$ ]]
}

list_kind_clusters() {
  local binary=$1
  local clusters
  if ! clusters="$("${binary}" get clusters)"; then
    echo "Failed to list Kind clusters; cluster absence is unverified." >&2
    return 1
  fi
  printf '%s\n' "${clusters}"
}

resolved_cleanup_exit() {
  local original_exit=$1
  local cleanup_failed=$2
  if [[ "${original_exit}" -ne 0 ]]; then
    printf '%s\n' "${original_exit}"
  elif [[ "${cleanup_failed}" -ne 0 ]]; then
    printf '1\n'
  else
    printf '0\n'
  fi
}

if [[ "${MINISKY_KIND_SELF_TEST:-}" == "1" ]]; then
  validate_cluster_name "minisky-ci-123-1"
  if validate_cluster_name "developer-cluster"; then
    exit 1
  fi
  if validate_cluster_name "minisky-ci-UPPER"; then
    exit 1
  fi
  fake_dir="$(mktemp -d)"
  trap 'rm -rf "${fake_dir}"' EXIT
  printf '#!/usr/bin/env bash\nexit 7\n' > "${fake_dir}/kind-fail"
  chmod +x "${fake_dir}/kind-fail"
  if list_kind_clusters "${fake_dir}/kind-fail" >/dev/null 2>&1; then
    exit 1
  fi
  printf '#!/usr/bin/env bash\nprintf "minisky-ci-123-1\\n"\n' > "${fake_dir}/kind-ok"
  chmod +x "${fake_dir}/kind-ok"
  test "$(list_kind_clusters "${fake_dir}/kind-ok")" = "minisky-ci-123-1"
  test "$(resolved_cleanup_exit 7 1)" = "7"
  test "$(resolved_cleanup_exit 0 1)" = "1"
  test "$(resolved_cleanup_exit 0 0)" = "0"
  echo "Kind integration ownership guard self-test passed."
  exit 0
fi

if [[ "${MINISKY_KIND_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to create a Kind cluster without MINISKY_KIND_INTEGRATION=1." >&2
  exit 2
fi

for command in docker go kubectl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done
docker info >/dev/null

cluster_name="${MINISKY_KIND_CLUSTER:-}"
if ! validate_cluster_name "${cluster_name}"; then
  echo "MINISKY_KIND_CLUSTER must be an isolated minisky-ci-* name." >&2
  exit 2
fi

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
chmod 0700 "${work_dir}"
test_home="${work_dir}/home"
mkdir -m 0700 "${test_home}"
owns_cluster=0
kind_bin="${test_home}/.minisky/bin/kind"
kubeconfig="${work_dir}/kubeconfig"
node_image="kindest/node:v1.29.2@sha256:51a1434a5397193442f0be2a297b488b6c919ce8a3931be0ce822606ea5ca245"

cleanup() {
  local original_exit=$?
  local cleanup_failed=0
  local clusters
  trap - EXIT INT TERM
  if [[ "${owns_cluster}" == "1" ]]; then
    if ! "${kind_bin}" delete cluster --name "${cluster_name}"; then
      echo "Failed to delete owned Kind cluster ${cluster_name}." >&2
      cleanup_failed=1
    elif ! clusters="$(list_kind_clusters "${kind_bin}")"; then
      cleanup_failed=1
    elif grep -Fqx "${cluster_name}" <<< "${clusters}"; then
      echo "Owned Kind cluster ${cluster_name} remains after cleanup." >&2
      cleanup_failed=1
    fi
  fi
  rm -rf "${work_dir}"
  exit "$(resolved_cleanup_exit "${original_exit}" "${cleanup_failed}")"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

installer="${work_dir}/install-kind.go"
cat >"${installer}" <<'EOF'
package main

import (
	"context"
	"log"

	"minisky/pkg/orchestrator"
)

func main() {
	if err := orchestrator.InstallToolDependency(context.Background(), "kind"); err != nil {
		log.Fatal(err)
	}
}
EOF

go_cache="$(go env GOCACHE)"
go_mod_cache="$(go env GOMODCACHE)"
(
  cd "${repository_root}"
  HOME="${test_home}" GOCACHE="${go_cache}" GOMODCACHE="${go_mod_cache}" \
    go run "${installer}"
)
test -x "${kind_bin}"
"${kind_bin}" version | grep -F "kind v0.22.0"

if ! clusters="$(list_kind_clusters "${kind_bin}")"; then
  exit 1
fi
if grep -Fqx "${cluster_name}" <<< "${clusters}"; then
  echo "Refusing to adopt pre-existing Kind cluster ${cluster_name}." >&2
  exit 1
fi

# Absence was proven above, so cleanup may remove only this exact unique name
# even if creation fails after partially provisioning the cluster.
owns_cluster=1
"${kind_bin}" create cluster \
  --name "${cluster_name}" \
  --image "${node_image}" \
  --kubeconfig "${kubeconfig}" \
  --wait 120s

if ! clusters="$(list_kind_clusters "${kind_bin}")"; then
  exit 1
fi
if ! grep -Fqx "${cluster_name}" <<< "${clusters}"; then
  echo "Owned Kind cluster was not observable after creation." >&2
  exit 1
fi
KUBECONFIG="${kubeconfig}" kubectl cluster-info --context "kind-${cluster_name}"
KUBECONFIG="${kubeconfig}" kubectl wait \
  --for=condition=Ready nodes --all --timeout=60s
KUBECONFIG="${kubeconfig}" kubectl get nodes -o name | grep -Fqx "node/${cluster_name}-control-plane"

"${kind_bin}" delete cluster --name "${cluster_name}"
if ! clusters="$(list_kind_clusters "${kind_bin}")"; then
  exit 1
fi
if grep -Fqx "${cluster_name}" <<< "${clusters}"; then
  echo "Owned Kind cluster still exists after deletion." >&2
  exit 1
fi
owns_cluster=0

echo "Pinned Kind create, use, and delete lifecycle passed."
