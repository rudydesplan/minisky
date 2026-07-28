#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1
[[ "${MINISKY_PHASE20_STORAGE_TRANSFER_TERRAFORM_INTEGRATION:-}" == "1" ]] || { echo "Explicit opt-in required." >&2; exit 2; }
for command in curl docker go python3 terraform; do command -v "${command}" >/dev/null || exit 1; done
shared_lock="${TMPDIR:-/tmp}/minisky-net-integration.lock"
phase_lock="${TMPDIR:-/tmp}/minisky-phase20-storage-transfer-terraform.lock"
shared_lock_acquired=0
phase_lock_acquired=0
baseline_ready=0
root=""; work=""; owned_work=""; home=""; state=""; tfdata=""; tfstate=""
profile="phase20-transfer-tf-$$"; project="phase20-transfer-tf-$$"; source_bucket="phase20-source-$$"; sink_bucket="phase20-sink-$$"
pid=""; watchdog=""; gateway=""; job=""
baseline_containers=""; baseline_volumes=""; baseline_networks=""

owned_containers(){ docker ps -aq --filter "label=managed-by=minisky" --filter "label=minisky.profile=${profile}"; }
owned_volumes(){ docker volume ls -q --filter "label=managed-by=minisky" --filter "label=minisky.profile=${profile}"; }
owned_networks(){ docker network ls -q --filter "label=managed-by=minisky" --filter "label=minisky.profile=${profile}"; }
is_baseline_resource(){
  awk -v resource="$1" '$0 == resource { found=1 } END { exit found ? 0 : 1 }' "$2"
}
capture_new_resources(){
  local current="$1" baseline="$2" output="$3" resource
  : >"${output}"
  while IFS= read -r resource; do
    if [[ -n "${resource}" ]] && ! is_baseline_resource "${resource}" "${baseline}"; then
      printf '%s\n' "${resource}" >>"${output}"
    fi
  done <"${current}"
}
remove_owned_work_directory(){
  if [[ -z "${owned_work}" || "${work}" != "${owned_work}" || ! -d "${work}" || -L "${work}" ||
    "${state}" != "${work}/state" || "${profile}" != phase20-transfer-tf-* ]]; then
    echo "Refusing Storage Transfer cleanup outside the exact owned work/profile root." >&2
    return 1
  fi
  python3 - "${work}" "${state}" "${profile}" <<'PY' || {
import os
import sys

work, state, profile = sys.argv[1:]
work_real = os.path.realpath(work)
state_real = os.path.realpath(state)
profile_real = os.path.realpath(os.path.join(state, "profiles", profile))
if state_real != os.path.join(work_real, "state"):
    raise SystemExit(1)
if os.path.commonpath((work_real, profile_real)) != work_real:
    raise SystemExit(1)
PY
    echo "Refusing Storage Transfer cleanup after path containment validation failed." >&2
    return 1
  }
  rm -rf -- "${work}"
}
cleanup(){
  local status=$? cleanup_failed=0 resource
  trap - EXIT INT TERM
  set +e
  if [[ -n "${pid}" ]]; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  if [[ -n "${watchdog}" ]]; then
    kill "${watchdog}" 2>/dev/null || true
    wait "${watchdog}" 2>/dev/null || true
  fi
  if [[ "${baseline_ready}" == "1" && -n "${work}" ]]; then
    local current_containers="${work}/cleanup-current-containers"
    local current_volumes="${work}/cleanup-current-volumes"
    local current_networks="${work}/cleanup-current-networks"
    local new_containers="${work}/cleanup-new-containers"
    local new_volumes="${work}/cleanup-new-volumes"
    local new_networks="${work}/cleanup-new-networks"
    if owned_containers >"${current_containers}" &&
      owned_volumes >"${current_volumes}" &&
      owned_networks >"${current_networks}"; then
      capture_new_resources "${current_containers}" "${baseline_containers}" "${new_containers}" || cleanup_failed=1
      capture_new_resources "${current_volumes}" "${baseline_volumes}" "${new_volumes}" || cleanup_failed=1
      capture_new_resources "${current_networks}" "${baseline_networks}" "${new_networks}" || cleanup_failed=1
      while IFS= read -r resource; do
        [[ -z "${resource}" ]] || docker rm -f "${resource}" >/dev/null 2>&1 || cleanup_failed=1
      done <"${new_containers}"
      while IFS= read -r resource; do
        [[ -z "${resource}" ]] || docker volume rm "${resource}" >/dev/null 2>&1 || cleanup_failed=1
      done <"${new_volumes}"
      while IFS= read -r resource; do
        [[ -z "${resource}" ]] || docker network rm "${resource}" >/dev/null 2>&1 || cleanup_failed=1
      done <"${new_networks}"
    else
      cleanup_failed=1
    fi
  fi
  if [[ -n "${work}" ]]; then
    remove_owned_work_directory || cleanup_failed=1
  fi
  if [[ "${phase_lock_acquired}" == "1" ]]; then
    rmdir "${phase_lock}" 2>/dev/null || cleanup_failed=1
    phase_lock_acquired=0
  fi
  if [[ "${shared_lock_acquired}" == "1" ]]; then
    rmdir "${shared_lock}" 2>/dev/null || cleanup_failed=1
    shared_lock_acquired=0
  fi
  if [[ "${cleanup_failed}" != "0" ]]; then
    echo "Storage Transfer cleanup incomplete for profile ${profile}." >&2
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

if ! mkdir "${shared_lock}" 2>/dev/null; then
  echo "Another MiniSky Docker integration is active (${shared_lock})." >&2
  exit 1
fi
shared_lock_acquired=1
if ! mkdir "${phase_lock}" 2>/dev/null; then
  echo "Another Phase 20 Storage Transfer Terraform integration is active (${phase_lock})." >&2
  exit 1
fi
phase_lock_acquired=1
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
owned_work="${work}"
home="${work}/home"; state="${work}/state"; tfdata="${work}/tf"; tfstate="${work}/terraform.tfstate"
mkdir -p "${home}" "${state}" "${tfdata}"
baseline_containers="${work}/baseline-containers"
baseline_volumes="${work}/baseline-volumes"
baseline_networks="${work}/baseline-networks"
if ! owned_containers >"${baseline_containers}" ||
  ! owned_volumes >"${baseline_volumes}" ||
  ! owned_networks >"${baseline_networks}"; then
  echo "Failed to capture baseline Docker inventory; refusing to start." >&2
  exit 1
fi
baseline_ready=1
preflight_owned_resources(){
  local containers="${work}/preflight-containers"
  local volumes="${work}/preflight-volumes"
  local networks="${work}/preflight-networks"
  if ! owned_containers >"${containers}" ||
    ! owned_volumes >"${volumes}" ||
    ! owned_networks >"${networks}"; then
    echo "Failed to inspect Docker resources during preflight; refusing to start." >&2
    return 1
  fi
  if [[ -s "${containers}" || -s "${volumes}" || -s "${networks}" ]] ||
    docker network inspect minisky-net >/dev/null 2>&1; then
    echo "Refusing live Phase 20 Storage Transfer run: colliding MiniSky Docker resources exist." >&2
    return 1
  fi
}
free_port(){ python3 - <<'PY'
import socket
s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
}
start(){
  p="$(free_port)"; u="$(free_port)"; gateway="http://127.0.0.1:${p}"
  HOME="${home}" MINISKY_STATE_DIR="${state}" MINISKY_PROFILE="${profile}" MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
    "${work}/minisky" start --port "${p}" --ui-port "${u}" >"${work}/minisky.log" 2>&1 & pid=$!
  for _ in {1..120}; do curl -fsS "${gateway}/healthz" >/dev/null && return; sleep .25; done; return 1
}
stop(){ kill -TERM "${pid}"; wait "${pid}"; pid=""; }
vars(){ tfvars=(-var="enable_phase20_storage_transfer_job=true" -var="minisky_endpoint=${gateway}" -var="profile=local" -var="project_id=${project}" -var="phase20_storage_transfer_source_bucket=${source_bucket}" -var="phase20_storage_transfer_sink_bucket=${sink_bucket}"); }
create_data(){
  curl -fsS -X POST "${gateway}/_minisky/storage/storage/v1/b?project=${project}" -H 'Content-Type: application/json' -d "{\"name\":\"${source_bucket}\"}" >/dev/null
  curl -fsS -X POST "${gateway}/_minisky/storage/storage/v1/b?project=${project}" -H 'Content-Type: application/json' -d "{\"name\":\"${sink_bucket}\"}" >/dev/null
  printf 'bounded-storage-transfer' | curl -fsS -X POST --data-binary @- "${gateway}/_minisky/storage/upload/storage/v1/b/${source_bucket}/o?uploadType=media&name=gate.txt" >/dev/null
}
discover(){
  job="$(terraform -chdir="${root}/terraform" output -raw phase20_storage_transfer_job_name)"
}
observe(){
  curl -fsS "${gateway}/_minisky/storagetransfer/v1/${job}?projectId=${project}" | python3 -c 'import json,sys; j=json.load(sys.stdin); assert j["status"]=="ENABLED"'
}
run_transfer(){
  curl -fsS -X POST "${gateway}/_minisky/storagetransfer/v1/${job}:run" -H 'Content-Type: application/json' -d "{\"projectId\":\"${project}\"}" | python3 -c 'import json,sys; o=json.load(sys.stdin); assert o["done"]'
  [[ "$(curl -fsS "${gateway}/_minisky/storage/download/storage/v1/b/${sink_bucket}/o/gate.txt?alt=media")" == "bounded-storage-transfer" ]]
}
nodrift(){ set +e; terraform -chdir="${root}/terraform" plan -detailed-exitcode -input=false -target='google_storage_transfer_job.phase20[0]' "${tfvars[@]}"; r=$?; set -e; [[ $r == 0 ]]; }
deleted(){ curl -fsS "${gateway}/_minisky/storagetransfer/v1/${job}?projectId=${project}" | python3 -c 'import json,sys; assert json.load(sys.stdin)["status"]=="DELETED"'; }

preflight_owned_resources
( sleep 420; kill -TERM "$$" ) & watchdog=$!
go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tfdata}"; unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT
start; vars; create_data
terraform -chdir="${root}/terraform" init -backend-config="path=${tfstate}" -input=false -lockfile=readonly
terraform -chdir="${root}/terraform" validate
terraform -chdir="${root}/terraform" apply -auto-approve -input=false -target='google_storage_transfer_job.phase20[0]' "${tfvars[@]}"
discover; observe; run_transfer; nodrift; stop
start; vars; observe; run_transfer; nodrift
terraform -chdir="${root}/terraform" state rm -backup="${work}/state-before-import.backup" 'google_storage_transfer_job.phase20[0]'
terraform -chdir="${root}/terraform" import -input=false "${tfvars[@]}" 'google_storage_transfer_job.phase20[0]' "${project}/${job#transferJobs/}"
nodrift
terraform -chdir="${root}/terraform" destroy -auto-approve -input=false -target='google_storage_transfer_job.phase20[0]' "${tfvars[@]}"
deleted; stop
start; deleted; stop
echo "Phase 20 Storage Transfer Terraform lifecycle passed with bounded local GCS data-plane evidence."
