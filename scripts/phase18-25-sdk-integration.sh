#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE18_25_SDK_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 18-25 SDK integration without MINISKY_PHASE18_25_SDK_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock="${TMPDIR:-/tmp}/minisky-phase18-25-sdk-integration.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 18-25 SDK integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
state_root="${work}/state"
home="${work}/home"
profile="phase18-25-sdk-$$"
project="phase18-25-project-$$"
workflow_id="phase18-25-workflow-$$"
trigger_id="phase18-25-trigger-$$"
job_id="phase18-25-job-$$"
evidence_file="${work}/generated-client-evidence.json"
batch_image="busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
pid=""
pull_pid=""
started_at="${SECONDS}"
mkdir -p "${state_root}" "${home}"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  if [[ -n "${pull_pid}" ]] && kill -0 "${pull_pid}" 2>/dev/null; then
    kill -TERM "${pull_pid}" 2>/dev/null || true
    wait "${pull_pid}" 2>/dev/null || true
  fi
  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f "${container}" >/dev/null 2>&1 || true
  done < <(docker ps -aq \
    --filter "label=minisky.service=batch" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true)
  rm -rf "${work}" "${lock}"
  exit "${status}"
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

api_port="$(free_port)"
ui_port="$(free_port)"
gateway="http://127.0.0.1:${api_port}"

docker info >/dev/null
pull_image() {
  local image=$1
  local log_file="${work}/batch-image-pull.log"
  local deadline
  if docker image inspect "${image}" >/dev/null 2>&1; then
    return
  fi
  docker pull "${image}" >"${log_file}" 2>&1 &
  pull_pid=$!
  deadline=$((SECONDS + 300))
  while kill -0 "${pull_pid}" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      kill -TERM "${pull_pid}" 2>/dev/null || true
      wait "${pull_pid}" 2>/dev/null || true
      echo "Timed out pulling pinned Batch image after 300s." >&2
      return 1
    fi
    sleep 1
  done
  if ! wait "${pull_pid}"; then
    echo "Failed to pull pinned Batch image ${image}." >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' "${log_file}" >&2
    return 1
  fi
  pull_pid=""
}
pull_image "${batch_image}"

assert_no_owned_batch_containers() {
  local containers
  containers="$(docker ps -aq \
    --filter "label=minisky.service=batch" \
    --filter "label=minisky.profile=${profile}")"
  if [[ -n "${containers}" ]]; then
    echo "Exact-owned Batch container cleanup is incomplete: ${containers}" >&2
    return 1
  fi
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky

start_daemon() {
  local experimental=$1
  local log_file=$2
  if [[ "${experimental}" == "1" ]]; then
    HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
      MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
      "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  else
    HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
      env -u MINISKY_ENABLE_EXPERIMENTAL_SERVICES \
      "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  fi
  pid=$!
  for _ in {1..80}; do
    if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${log_file}" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "Timed out waiting for MiniSky readiness." >&2
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${log_file}" >&2
  return 1
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}"
  fi
  pid=""
}

export MINISKY_ENDPOINT="${gateway}"
export MINISKY_PROJECT_ID="${project}"
export MINISKY_PHASE18_25_LOCATION="us-central1"
export MINISKY_PHASE18_25_WORKFLOW_ID="${workflow_id}"
export MINISKY_PHASE18_25_TRIGGER_ID="${trigger_id}"
export MINISKY_PHASE18_25_JOB_ID="${job_id}"
export MINISKY_PHASE18_25_BATCH_IMAGE="${batch_image}"
export MINISKY_PHASE18_25_EVIDENCE="${evidence_file}"

start_daemon 0 "${work}/minisky-default-gated.log"
MINISKY_PHASE18_25_MODE=gate go run ./sdk-smoke/phase18-25
stop_daemon

start_daemon 1 "${work}/minisky-create.log"
MINISKY_PHASE18_25_MODE=create MINISKY_PHASE18_25_EXPERIMENTAL_OPT_IN=1 \
  go run ./sdk-smoke/phase18-25
assert_no_owned_batch_containers
echo "Exact-owned Batch container cleanup verified after terminal execution."
stop_daemon

start_daemon 1 "${work}/minisky-restarted.log"
MINISKY_PHASE18_25_MODE=verify MINISKY_PHASE18_25_EXPERIMENTAL_OPT_IN=1 \
  go run ./sdk-smoke/phase18-25
assert_no_owned_batch_containers
MINISKY_PHASE18_25_MODE=delete MINISKY_PHASE18_25_EXPERIMENTAL_OPT_IN=1 \
  go run ./sdk-smoke/phase18-25
assert_no_owned_batch_containers
echo "Batch delete 404 and exact-owned cleanup verified."
stop_daemon

duration=$((SECONDS - started_at))
echo "Phase 18-25 generated Go client integration passed in ${duration}s."
