#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE18_WORKFLOWS_TERRAFORM_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 18 Workflows Terraform integration without MINISKY_PHASE18_WORKFLOWS_TERRAFORM_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3 terraform; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done
if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is unavailable; the guarded MiniSky lifecycle requires Docker isolation." >&2
  exit 1
fi

shared_lock="${TMPDIR:-/tmp}/minisky-net-integration.lock"
phase_lock="${TMPDIR:-/tmp}/minisky-phase18-workflows-terraform-integration.lock"
if ! mkdir "${shared_lock}" 2>/dev/null; then
  echo "Another MiniSky Docker integration is active (${shared_lock})." >&2
  exit 1
fi
if ! mkdir "${phase_lock}" 2>/dev/null; then
  rmdir "${shared_lock}" 2>/dev/null || true
  echo "Another Phase 18 Workflows Terraform integration is active (${phase_lock})." >&2
  exit 1
fi

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="${repository_root}/terraform"
work="$(mktemp -d)"
chmod 700 "${work}"
home="${work}/home"
state_root="${work}/state"
tf_data_dir="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
profile="phase18-workflows-terraform-$$"
project="phase18-workflows-terraform-$$"
region="us-central1"
workflow_name="phase18-terraform-workflow"
canonical="projects/${project}/locations/${region}/workflows/${workflow_name}"
pid=""
watchdog_pid=""
gateway=""
current_log=""
started_at="${SECONDS}"
mkdir -p "${home}" "${state_root}" "${tf_data_dir}"

print_current_log() {
  if [[ -n "${current_log}" && -f "${current_log}" ]]; then
    echo "MiniSky Phase 18 Workflows Terraform daemon log (${current_log}):" >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"))' \
      "${current_log}" >&2
  fi
}

remove_owned_resources() {
  local container
  local network_id
  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f "${container}" >/dev/null 2>&1 || true
  done < <(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true)
  network_id="$(docker network inspect --format \
    '{{if and (eq (index .Labels "managed-by") "minisky") (eq (index .Labels "minisky.profile") "'"${profile}"'")}}{{.Id}}{{end}}' \
    minisky-net 2>/dev/null || true)"
  if [[ -n "${network_id}" ]]; then
    docker network rm "${network_id}" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    print_current_log
  fi
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  if [[ -n "${watchdog_pid}" ]] && kill -0 "${watchdog_pid}" 2>/dev/null; then
    kill -TERM "${watchdog_pid}" 2>/dev/null || true
    wait "${watchdog_pid}" 2>/dev/null || true
  fi
  remove_owned_resources
  rm -rf "${work}"
  rmdir "${phase_lock}" 2>/dev/null || true
  rmdir "${shared_lock}" 2>/dev/null || true
  exit "${status}"
}
trap cleanup EXIT INT TERM

(
  sleep 600
  echo "Phase 18 Workflows Terraform integration exceeded its 10 minute budget." >&2
  kill -TERM "$$"
) &
watchdog_pid=$!

if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Refusing to disturb an existing Docker network named minisky-net." >&2
  exit 1
fi
while IFS= read -r existing_container_name; do
  case "${existing_container_name}" in
    minisky-*)
      echo "Refusing to disturb existing MiniSky container: ${existing_container_name}" >&2
      exit 1
      ;;
  esac
done < <(docker ps -a --format '{{.Names}}')

free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

start_daemon() {
  local label=$1
  local attempt
  local api_port
  local ui_port
  for attempt in {1..3}; do
    api_port="$(free_port)"
    ui_port="$(free_port)"
    while [[ "${ui_port}" == "${api_port}" ]]; do
      ui_port="$(free_port)"
    done
    gateway="http://127.0.0.1:${api_port}"
    current_log="${work}/${label}-attempt-${attempt}.log"
    HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
      MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
      "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" \
      >"${current_log}" 2>&1 &
    pid=$!
    for _ in {1..80}; do
      if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
        return 0
      fi
      if ! kill -0 "${pid}" 2>/dev/null; then
        print_current_log
        return 1
      fi
      sleep 0.25
    done
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
    pid=""
  done
  echo "Timed out waiting for MiniSky readiness after three bounded attempts." >&2
  return 1
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}"
  fi
  pid=""
}

set_tf_vars() {
  tf_vars=(
    -var="enable_phase18_workflows_resource=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase18_workflow_name=${workflow_name}"
    -var="profile=local"
    -var="project_id=${project}"
    -var="region=${region}"
  )
}

assert_workflow() {
  local response="${work}/workflow.json"
  curl --globoff --fail --silent --show-error \
    "${gateway}/_minisky/workflows/v1/${canonical}" >"${response}"
  python3 - "${response}" "${canonical}" <<'PY'
import json
import sys
workflow = json.load(open(sys.argv[1], encoding="utf-8"))
expected_name = sys.argv[2]
if workflow.get("name") != expected_name:
    raise SystemExit(f"workflow name={workflow.get('name')!r} want={expected_name!r}")
if workflow.get("state") != "ACTIVE":
    raise SystemExit(f"workflow state={workflow.get('state')!r} want='ACTIVE'")
if workflow.get("description") != "MiniSky Phase-18 Terraform lifecycle evidence":
    raise SystemExit("workflow description did not survive provider lifecycle")
if "minisky-phase18" not in workflow.get("sourceContents", ""):
    raise SystemExit("workflow source contents did not survive provider lifecycle")
if not workflow.get("revisionId") or not workflow.get("createTime") or not workflow.get("updateTime"):
    raise SystemExit("workflow is missing stable provider-observed metadata")
PY
}

assert_workflow_missing() {
  local status
  status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' \
    "${gateway}/_minisky/workflows/v1/${canonical}")"
  if [[ "${status}" != "404" ]]; then
    echo "Expected destroyed workflow ${canonical} to return HTTP 404, received ${status}." >&2
    return 1
  fi
}

assert_no_drift() {
  local plan_exit
  set +e
  terraform -chdir="${terraform_dir}" plan -detailed-exitcode \
    -input=false \
    -target='google_workflows_workflow.phase18[0]' \
    "${tf_vars[@]}"
  plan_exit=$?
  set -e
  if [[ "${plan_exit}" != "0" ]]; then
    echo "Expected a Workflows no-drift plan (exit 0), received exit ${plan_exit}." >&2
    return "${plan_exit}"
  fi
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky

export HOME="${home}"
export TF_DATA_DIR="${tf_data_dir}"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT

start_daemon "apply"
set_tf_vars
terraform -chdir="${terraform_dir}" init \
  -backend-config="path=${tf_state}" \
  -input=false \
  -lockfile=readonly
terraform -chdir="${terraform_dir}" validate
terraform -chdir="${terraform_dir}" apply \
  -auto-approve \
  -input=false \
  -target='google_workflows_workflow.phase18[0]' \
  "${tf_vars[@]}"
assert_workflow
assert_no_drift
stop_daemon

start_daemon "restart"
set_tf_vars
assert_workflow
assert_no_drift
echo "Pinned google_workflows_workflow provider resource does not support import; import is not applicable."
terraform -chdir="${terraform_dir}" destroy \
  -auto-approve \
  -input=false \
  -target='google_workflows_workflow.phase18[0]' \
  "${tf_vars[@]}"
assert_workflow_missing
stop_daemon

start_daemon "cleanup-restart"
set_tf_vars
assert_workflow_missing
stop_daemon

if (( SECONDS - started_at > 600 )); then
  echo "Phase 18 Workflows Terraform integration exceeded its 10 minute budget." >&2
  exit 1
fi
echo "Phase 18 Workflows Terraform apply/restart/no-drift/destroy gate passed in $((SECONDS - started_at))s; import is unsupported."
