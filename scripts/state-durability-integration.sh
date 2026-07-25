#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_STATE_DURABILITY_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start durability integration without MINISKY_STATE_DURABILITY_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3 terraform; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done
docker info >/dev/null

if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Refusing to disturb an existing MiniSky Docker network." >&2
  exit 1
fi
while IFS= read -r container_name; do
  case "${container_name}" in
    minisky-*)
      echo "Refusing to disturb existing MiniSky container: ${container_name}" >&2
      exit 1
      ;;
  esac
done < <(docker ps -a --format '{{.Names}}')

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="${repository_root}/terraform"
lock_dir="${TMPDIR:-/tmp}/minisky-state-durability-integration.lock"
if ! mkdir "${lock_dir}" 2>/dev/null; then
  echo "Another MiniSky durability integration run is active." >&2
  exit 1
fi

work_dir="$(mktemp -d)"
minisky_pid=""
active_profile=""
run_profile="state-durability-$$"
source_profile="${run_profile}-source"
imported_profile="${run_profile}-imported"

cleanup_profile_docker() {
  local profile=$1
  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f "${container}" >/dev/null 2>&1 || true
  done < <(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true)
  network_manager="$(docker network inspect --format '{{index .Labels "managed-by"}}' minisky-net 2>/dev/null || true)"
  network_profile="$(docker network inspect --format '{{index .Labels "minisky.profile"}}' minisky-net 2>/dev/null || true)"
  if [[ "${network_manager}" == "minisky" && "${network_profile}" == "${profile}" ]]; then
    docker network rm minisky-net >/dev/null 2>&1 || true
  fi
}

cleanup() {
  exit_code=$?
  trap - EXIT INT TERM
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}" 2>/dev/null || true
    wait "${minisky_pid}" 2>/dev/null || true
  fi
  cleanup_profile_docker "${source_profile}"
  cleanup_profile_docker "${imported_profile}"
  rm -rf "${work_dir}"
  rmdir "${lock_dir}" 2>/dev/null || true
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

free_port() {
  python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

api_port="${MINISKY_DURABILITY_API_PORT:-$(free_port)}"
ui_port="${MINISKY_DURABILITY_UI_PORT:-$(free_port)}"
gateway="http://127.0.0.1:${api_port}"
project_id="local-dev-project"
dataset_id="minisky_durability"
service_account_id="minisky-durability"
service_account_email="${service_account_id}@${project_id}.iam.gserviceaccount.com"
state_root="${work_dir}/state"
snapshot="${work_dir}/snapshot.json"

mkdir -p "${work_dir}/home"
go build -trimpath -o "${work_dir}/minisky" ./cmd/minisky

start_minisky() {
  active_profile=$1
  MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${active_profile}" HOME="${work_dir}/home" \
    "${work_dir}/minisky" start --port "${api_port}" --ui-port "${ui_port}" \
    >"${work_dir}/minisky-${active_profile}.log" 2>&1 &
  minisky_pid=$!

  ready_url="${gateway}/_minisky/bigquery/bigquery/v2/projects/${project_id}/datasets"
  for _ in {1..60}; do
    if curl --fail --silent --show-error "${ready_url}" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "${minisky_pid}" 2>/dev/null; then
      echo "MiniSky exited during ${active_profile} startup." >&2
      return 1
    fi
    sleep 1
  done
  curl --fail --silent --show-error "${ready_url}" >/dev/null
}

stop_minisky() {
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}"
    wait "${minisky_pid}"
  fi
  minisky_pid=""
}

export TF_DATA_DIR="${work_dir}/terraform-data"
terraform -chdir="${terraform_dir}" init \
  -backend-config="path=${work_dir}/terraform.tfstate" \
  -input=false \
  -lockfile=readonly
terraform -chdir="${terraform_dir}" validate

tf_targets=(
  -target=google_bigquery_dataset.compatibility
  -target=google_bigquery_table.events
  -target=google_service_account.compatibility
)
tf_vars=(
  -var="dataset_id=${dataset_id}"
  -var="service_account_id=${service_account_id}"
  -var="minisky_endpoint=${gateway}"
  -var="profile=local"
)

start_minisky "${source_profile}"
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"
stop_minisky

start_minisky "${source_profile}"
set +e
terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"
plan_exit=$?
set -e
if [[ "${plan_exit}" != "0" ]]; then
  echo "Expected source-profile restart plan exit 0, received ${plan_exit}." >&2
  exit "${plan_exit}"
fi
stop_minisky
cleanup_profile_docker "${source_profile}"

MINISKY_STATE_DIR="${state_root}" "${work_dir}/minisky" state --profile "${source_profile}" export "${snapshot}"
if [[ -e "${state_root}/profiles/${imported_profile}" ]]; then
  echo "Imported profile must be clean before state import." >&2
  exit 1
fi
MINISKY_STATE_DIR="${state_root}" "${work_dir}/minisky" state --profile "${imported_profile}" import "${snapshot}"

start_minisky "${imported_profile}"
set +e
terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"
plan_exit=$?
set -e
if [[ "${plan_exit}" != "0" ]]; then
  echo "Expected imported-profile plan exit 0, received ${plan_exit}." >&2
  exit "${plan_exit}"
fi

terraform -chdir="${terraform_dir}" destroy -auto-approve -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"

dataset_url="${gateway}/_minisky/bigquery/bigquery/v2/projects/${project_id}/datasets/${dataset_id}"
service_account_url="${gateway}/_minisky/iam/v1/projects/${project_id}/serviceAccounts/${service_account_email}"
for url in "${dataset_url}" "${service_account_url}"; do
  status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' "${url}")"
  if [[ "${status}" != "404" ]]; then
    echo "Expected destroyed resource ${url} to return HTTP 404, received ${status}." >&2
    exit 1
  fi
done
