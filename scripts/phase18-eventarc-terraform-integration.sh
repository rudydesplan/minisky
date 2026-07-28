#!/usr/bin/env bash

set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

if [[ "${MINISKY_PHASE18_EVENTARC_TERRAFORM_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 18 Eventarc Terraform integration without MINISKY_PHASE18_EVENTARC_TERRAFORM_INTEGRATION=1." >&2
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
phase_lock="${TMPDIR:-/tmp}/minisky-phase18-eventarc-terraform-integration.lock"
if ! mkdir "${shared_lock}" 2>/dev/null; then
  echo "Another MiniSky Docker integration is active (${shared_lock})." >&2
  exit 1
fi
if ! mkdir "${phase_lock}" 2>/dev/null; then
  rmdir "${shared_lock}" 2>/dev/null || true
  echo "Another Phase 18 Eventarc Terraform integration is active (${phase_lock})." >&2
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
profile="phase18-eventarc-terraform-$$"
project="phase18-eventarc-terraform-$$"
region="us-central1"
workflow_name="phase18-eventarc-workflow"
trigger_name="phase18-terraform-trigger"
topic_name="phase18-eventarc-transport"
workflow_canonical="projects/${project}/locations/${region}/workflows/${workflow_name}"
trigger_canonical="projects/${project}/locations/${region}/triggers/${trigger_name}"
topic_canonical="projects/${project}/topics/${topic_name}"
pid=""
watchdog_pid=""
gateway=""
current_log=""
started_at="${SECONDS}"
mkdir -p "${home}" "${state_root}" "${tf_data_dir}"

print_current_log() {
  if [[ -n "${current_log}" && -f "${current_log}" ]]; then
    echo "MiniSky Phase 18 Eventarc Terraform daemon log (${current_log}):" >&2
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
  echo "Phase 18 Eventarc Terraform integration exceeded its 10 minute budget." >&2
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
    -var="enable_phase18_eventarc_resource=true"
    -var="enable_phase18_workflows_resource=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase18_eventarc_transport_topic=${topic_name}"
    -var="phase18_eventarc_trigger_name=${trigger_name}"
    -var="phase18_workflow_name=${workflow_name}"
    -var="profile=local"
    -var="project_id=${project}"
    -var="region=${region}"
  )
}

assert_workflow() {
  curl --globoff --fail --silent --show-error \
    "${gateway}/_minisky/workflows/v1/${workflow_canonical}" >/dev/null
}

assert_trigger() {
  local response="${work}/trigger.json"
  curl --globoff --fail --silent --show-error \
    "${gateway}/_minisky/eventarc/v1/${trigger_canonical}" >"${response}"
  python3 - "${response}" "${trigger_canonical}" "${workflow_canonical}" "${topic_canonical}" <<'PY'
import json
import sys
trigger = json.load(open(sys.argv[1], encoding="utf-8"))
expected_name, expected_workflow, expected_topic = sys.argv[2:]
if trigger.get("name") != expected_name:
    raise SystemExit(f"trigger name={trigger.get('name')!r} want={expected_name!r}")
if trigger.get("eventFilters") != [{
    "attribute": "type",
    "value": "google.cloud.storage.object.v1.finalized",
}]:
    raise SystemExit(f"unexpected event filters: {trigger.get('eventFilters')!r}")
if trigger.get("destination", {}).get("workflow") != expected_workflow:
    raise SystemExit(f"unexpected workflow destination: {trigger.get('destination')!r}")
pubsub = trigger.get("transport", {}).get("pubsub", {})
if pubsub.get("topic") != expected_topic:
    raise SystemExit(f"unexpected Pub/Sub transport metadata: {trigger.get('transport')!r}")
if trigger.get("eventDataContentType") != "application/json":
    raise SystemExit("eventDataContentType did not survive provider lifecycle")
for field in ("uid", "createTime", "updateTime", "etag"):
    if not trigger.get(field):
        raise SystemExit(f"trigger is missing stable provider-observed {field}")
PY
}

assert_missing() {
  local kind=$1
  local url=$2
  local status
  status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' "${url}")"
  if [[ "${status}" != "404" ]]; then
    echo "Expected destroyed ${kind} to return HTTP 404, received ${status}." >&2
    return 1
  fi
}

assert_no_drift() {
  local plan_exit
  set +e
  terraform -chdir="${terraform_dir}" plan -detailed-exitcode \
    -input=false \
    -target='google_eventarc_trigger.phase18[0]' \
    "${tf_vars[@]}"
  plan_exit=$?
  set -e
  if [[ "${plan_exit}" != "0" ]]; then
    echo "Expected an Eventarc no-drift plan (exit 0), received exit ${plan_exit}." >&2
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
  -target='google_eventarc_trigger.phase18[0]' \
  "${tf_vars[@]}"
assert_workflow
assert_trigger
assert_no_drift
stop_daemon

start_daemon "restart"
set_tf_vars
assert_workflow
assert_trigger
assert_no_drift
terraform -chdir="${terraform_dir}" state rm -backup="${work}/state-before-import.backup" 'google_eventarc_trigger.phase18[0]'
terraform -chdir="${terraform_dir}" import \
  -input=false \
  "${tf_vars[@]}" \
  'google_eventarc_trigger.phase18[0]' \
  "${trigger_canonical}"
assert_trigger
terraform -chdir="${terraform_dir}" apply \
  -auto-approve \
  -input=false \
  -target='google_eventarc_trigger.phase18[0]' \
  "${tf_vars[@]}"
assert_no_drift
terraform -chdir="${terraform_dir}" destroy \
  -auto-approve \
  -input=false \
  -target='google_eventarc_trigger.phase18[0]' \
  -target='google_workflows_workflow.phase18[0]' \
  "${tf_vars[@]}"
assert_missing "Eventarc trigger ${trigger_canonical}" \
  "${gateway}/_minisky/eventarc/v1/${trigger_canonical}"
assert_missing "workflow ${workflow_canonical}" \
  "${gateway}/_minisky/workflows/v1/${workflow_canonical}"
stop_daemon

start_daemon "cleanup-restart"
set_tf_vars
assert_missing "Eventarc trigger ${trigger_canonical}" \
  "${gateway}/_minisky/eventarc/v1/${trigger_canonical}"
assert_missing "workflow ${workflow_canonical}" \
  "${gateway}/_minisky/workflows/v1/${workflow_canonical}"
stop_daemon

if (( SECONDS - started_at > 600 )); then
  echo "Phase 18 Eventarc Terraform integration exceeded its 10 minute budget." >&2
  exit 1
fi
echo "Phase 18 Eventarc Terraform apply/observe/restart/no-drift/import/destroy gate passed in $((SECONDS - started_at))s; this gate does not exercise event delivery."
