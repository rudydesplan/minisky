#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_REDIS_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Redis integration without MINISKY_REDIS_INTEGRATION=1." >&2
  exit 2
fi

overall_timeout_seconds="${MINISKY_REDIS_TIMEOUT_SECONDS:-900}"
profile="${MINISKY_REDIS_PROFILE:-redis-integration-$$-$(date +%s)}"

if [[ ! "${overall_timeout_seconds}" =~ ^[1-9][0-9]*$ ]]; then
  echo "MINISKY_REDIS_TIMEOUT_SECONDS must be a positive integer." >&2
  exit 1
fi
if [[ ! "${profile}" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$ ]]; then
  echo "MINISKY_REDIS_PROFILE is invalid." >&2
  exit 1
fi

for command in curl docker go python3 terraform; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done

shared_lock="/tmp/minisky-net-integration.lock"
phase_lock="/tmp/minisky-redis-integration.lock"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir=""
minisky_runtime_dir=""
minisky_binary=""
minisky_pid=""
watchdog_pid=""
foreign_network_id=""
foreign_volume=""
foreign_volume_created=""
foreign_volume_mount=""
foreign_container_id=""
terraform_timeout_seconds=240
in_cleanup=0
shared_lock_acquired=0
phase_lock_acquired=0
tracked_container_ids=()
tracked_network_ids=()
tracked_volumes=()
initial_network_absence_proven=0
current_network_id=""
overall_deadline_epoch=$(($(date +%s) + overall_timeout_seconds))

run_bounded() {
  local seconds="$1"
  local remaining
  shift
  if [[ "${in_cleanup}" -eq 0 ]]; then
    remaining=$((overall_deadline_epoch - $(date +%s)))
    if [[ "${remaining}" -le 0 ]]; then
      echo "Redis integration exceeded ${overall_timeout_seconds} seconds." >&2
      return 124
    fi
    if [[ "${remaining}" -lt "${seconds}" ]]; then
      seconds="${remaining}"
    fi
  fi
  python3 -c '
import os
import signal
import subprocess
import sys
import time

class WrapperInterrupted(Exception):
    def __init__(self, signum):
        self.signum = signum

def interrupt(signum, _frame):
    global received_signal
    received_signal = signum
    if process is not None:
        raise WrapperInterrupted(signum)

def terminate_process_group(process, first_signal):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        os.killpg(process.pid, first_signal)
    except ProcessLookupError:
        process.wait()
        return
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        if process.poll() is None:
            try:
                process.wait(timeout=min(0.05, max(0.001, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
        else:
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()

watched_signals = {signal.SIGINT, signal.SIGTERM}
received_signal = None
process = None
for watched_signal in watched_signals:
    signal.signal(watched_signal, interrupt)
try:
    process = subprocess.Popen(sys.argv[2:], start_new_session=True)
    if received_signal is not None:
        raise WrapperInterrupted(received_signal)
    returncode = process.wait(timeout=float(sys.argv[1]))
except WrapperInterrupted as interrupted:
    if process is not None:
        terminate_process_group(process, signal.SIGTERM)
    raise SystemExit(128 + interrupted.signum)
except subprocess.TimeoutExpired:
    print("Command timed out after " + sys.argv[1] + " seconds: " + " ".join(sys.argv[2:]), file=sys.stderr)
    terminate_process_group(process, signal.SIGTERM)
    raise SystemExit(124)
except BaseException:
    if received_signal is not None:
        raise SystemExit(128 + received_signal)
    raise
raise SystemExit(128 - returncode if returncode < 0 else returncode)
' "${seconds}" "$@"
}

configure_docker_endpoint() {
  local configured_host="${DOCKER_HOST:-}"
  local configured_context="${DOCKER_CONTEXT:-}"
  local resolved
  if [[ -n "${configured_host}" && -n "${configured_context}" ]]; then
    echo "Refusing ambiguous DOCKER_HOST and DOCKER_CONTEXT configuration." >&2
    return 1
  fi
  if [[ -n "${configured_context}" ]]; then
    resolved="$(env -u DOCKER_HOST -u DOCKER_CONTEXT docker \
      --context "${configured_context}" context inspect "${configured_context}" \
      --format '{{ (index .Endpoints "docker").Host }}')"
  elif [[ -n "${configured_host}" ]]; then
    resolved="${configured_host}"
  else
    resolved="$(env -u DOCKER_HOST -u DOCKER_CONTEXT docker context inspect \
      --format '{{ (index .Endpoints "docker").Host }}')"
  fi
  case "${resolved}" in
    unix:///*) ;;
    *)
      echo "Redis integration requires a local Unix Docker endpoint, got ${resolved}." >&2
      return 1
      ;;
  esac
  resolved="$(python3 -c 'import os,sys; print("unix://" + os.path.realpath(sys.argv[1][7:]))' "${resolved}")"
  unset DOCKER_CONTEXT
  export DOCKER_HOST="${resolved}"
  run_bounded 15 docker info >/dev/null
}

assert_initial_network_absent() {
  local inspect
  local status
  set +e
  inspect="$(run_bounded 10 docker network inspect minisky-net 2>&1)"
  status=$?
  set -e
  if [[ "${status}" -eq 1 ]] &&
    [[ "${inspect}" == "Error response from daemon: network minisky-net not found" ||
      "${inspect}" == $'[]\nError response from daemon: network minisky-net not found' ||
      "${inspect}" == "network minisky-net not found" ]]; then
    initial_network_absence_proven=1
    return
  fi
  if [[ "${status}" -eq 0 ]]; then
    echo "Refusing to disturb an existing MiniSky Docker network." >&2
    return 1
  fi
  echo "Unable to prove initial minisky-net absence (status ${status}): ${inspect}" >&2
  return 1
}

release_locks() {
  local failed=0
  if [[ "${phase_lock_acquired}" -eq 1 ]]; then
    rmdir "${phase_lock}" 2>/dev/null || failed=1
    phase_lock_acquired=0
  fi
  if [[ "${shared_lock_acquired}" -eq 1 ]]; then
    rmdir "${shared_lock}" 2>/dev/null || failed=1
    shared_lock_acquired=0
  fi
  return "${failed}"
}

configure_docker_endpoint

if ! mkdir "${shared_lock}" 2>/dev/null; then
  echo "Another MiniSky Docker integration is active (${shared_lock})." >&2
  exit 1
fi
shared_lock_acquired=1
trap 'status=$?; release_locks || true; exit "${status}"' EXIT
trap 'release_locks || true; exit 130' INT
trap 'release_locks || true; exit 143' TERM
if ! mkdir "${phase_lock}" 2>/dev/null; then
  echo "Another MiniSky Redis integration is active (${phase_lock})." >&2
  exit 1
fi
phase_lock_acquired=1

terraform_bounded() {
  run_bounded "${terraform_timeout_seconds}" terraform "$@"
}

wait_for_pid_exit() {
  local pid="$1"
  local seconds="$2"
  local attempts=$((seconds * 10))
  for ((attempt = 0; attempt < attempts; attempt++)); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      wait "${pid}"
      return
    fi
    sleep 0.1
  done
  kill -KILL "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
  return 1
}

start_watchdog() {
  python3 - "$$" "${overall_timeout_seconds}" <<'PY' &
import os
import signal
import sys
import time

time.sleep(float(sys.argv[2]))
print(f"Redis integration exceeded {sys.argv[2]} seconds.", file=sys.stderr)
os.kill(int(sys.argv[1]), signal.SIGTERM)
PY
  watchdog_pid=$!
}

signal_exit() {
  exit "$1"
}

stop_minisky() {
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}"
    wait_for_pid_exit "${minisky_pid}" 15
  fi
  minisky_pid=""
}

remove_foreign_network() {
  [[ -n "${foreign_network_id}" ]] || return 0
  local inspect
  if ! inspect="$(run_bounded 10 docker network inspect "${foreign_network_id}")"; then
    echo "Unable to inspect tracked foreign Redis network ${foreign_network_id}; preserving unknown state." >&2
    return 1
  fi
  if ! run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_id, gate = sys.argv[2:]
if len(items) != 1 or items[0].get("Id") != expected_id:
    raise SystemExit("foreign network immutable identity changed")
labels = items[0].get("Labels") or {}
if labels.get("managed-by") != "redis-gate-foreign" or labels.get("minisky.redis-gate-run") != gate:
    raise SystemExit("foreign network marker changed")
' "${inspect}" "${foreign_network_id}" "${gate_id}"; then
    echo "Tracked foreign Redis network identity changed; preserving it." >&2
    return 1
  fi
  if ! run_bounded 15 docker network rm "${foreign_network_id}" >/dev/null; then
    echo "Unable to remove tracked foreign Redis network ${foreign_network_id}." >&2
    return 1
  fi
  foreign_network_id=""
}

remove_foreign_container() {
  [[ -n "${foreign_container_id}" ]] || return 0
  local inspect
  if ! inspect="$(run_bounded 10 docker inspect --type container "${foreign_container_id}")"; then
    echo "Unable to inspect tracked foreign Redis container ${foreign_container_id}; preserving unknown state." >&2
    return 1
  fi
  if ! run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_id, gate = sys.argv[2:]
if len(items) != 1 or items[0].get("Id") != expected_id:
    raise SystemExit("foreign container immutable identity changed")
labels = (items[0].get("Config") or {}).get("Labels") or {}
if labels.get("managed-by") != "redis-gate-foreign" or labels.get("minisky.redis-gate-run") != gate:
    raise SystemExit("foreign container marker changed")
' "${inspect}" "${foreign_container_id}" "${gate_id}"; then
    echo "Tracked foreign Redis container identity changed; preserving it." >&2
    return 1
  fi
  if ! run_bounded 15 docker rm -f "${foreign_container_id}" >/dev/null; then
    echo "Unable to remove tracked foreign Redis container ${foreign_container_id}." >&2
    return 1
  fi
  foreign_container_id=""
}

remove_foreign_volume() {
  [[ -n "${foreign_volume}" ]] || return 0
  local inspect
  if ! inspect="$(run_bounded 10 docker volume inspect "${foreign_volume}")"; then
    echo "Unable to inspect tracked foreign Redis volume ${foreign_volume}; preserving unknown state." >&2
    return 1
  fi
  if ! run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_name, expected_created, expected_mount, gate = sys.argv[2:]
if len(items) != 1 or items[0].get("Name") != expected_name:
    raise SystemExit("foreign volume immutable identity changed")
if items[0].get("CreatedAt") != expected_created or items[0].get("Mountpoint") != expected_mount:
    raise SystemExit("foreign volume creation identity changed")
labels = items[0].get("Labels") or {}
if labels.get("managed-by") != "redis-gate-foreign" or labels.get("minisky.redis-gate-run") != gate:
    raise SystemExit("foreign volume marker changed")
' "${inspect}" "${foreign_volume}" "${foreign_volume_created}" "${foreign_volume_mount}" "${gate_id}"; then
    echo "Tracked foreign Redis volume identity changed; preserving it." >&2
    return 1
  fi
  if ! run_bounded 15 docker volume rm "${foreign_volume}" >/dev/null; then
    echo "Unable to remove tracked foreign Redis volume ${foreign_volume}." >&2
    return 1
  fi
  foreign_volume=""
  foreign_volume_created=""
  foreign_volume_mount=""
}

assert_profile_empty() {
  local containers
  local volumes
  if ! containers="$(run_bounded 15 docker ps -aq --no-trunc \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-redis")"; then
    echo "Unable to inventory Redis containers for profile ${profile}." >&2
    return 1
  fi
  if ! volumes="$(run_bounded 15 docker volume ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-redis")"; then
    echo "Unable to inventory Redis volumes for profile ${profile}." >&2
    return 1
  fi
  if [[ -n "${containers}" || -n "${volumes}" ]]; then
    echo "Redis integration profile is not empty: containers=${containers} volumes=${volumes}" >&2
    return 1
  fi
}

cleanup_tracked_resources() {
  local inspect
  local status
  local container_id
  local network_id
  local volume_record
  local volume_name
  local volume_created
  local volume_mount
  local failed=0
  for container_id in "${tracked_container_ids[@]:-}"; do
    [[ -n "${container_id}" ]] || continue
    set +e
    inspect="$(run_bounded 10 docker inspect --type container "${container_id}" 2>&1)"
    status=$?
    set -e
    if [[ "${status}" -eq 0 ]]; then
      if ! run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_id, profile = sys.argv[2:]
if len(items) != 1 or items[0].get("Id") != expected_id:
    raise SystemExit("tracked Redis container immutable ID changed")
labels = (items[0].get("Config") or {}).get("Labels") or {}
if labels.get("managed-by") != "minisky" or labels.get("minisky.profile") != profile or labels.get("minisky.service") != "memorystore-redis":
    raise SystemExit("tracked Redis container ownership changed")
' "${inspect}" "${container_id}" "${profile}" ||
        ! run_bounded 15 docker rm -f "${container_id}" >/dev/null; then
        failed=1
      fi
    elif [[ "${status}" -eq 1 && "${inspect}" == *"No such"* ]]; then
      :
    else
      echo "Unable to inspect tracked Redis container ${container_id}: ${inspect}" >&2
      failed=1
    fi
  done
  for volume_record in "${tracked_volumes[@]:-}"; do
    [[ -n "${volume_record}" ]] || continue
    IFS='|' read -r volume_name volume_created volume_mount <<<"${volume_record}"
    set +e
    inspect="$(run_bounded 10 docker volume inspect "${volume_name}" 2>&1)"
    status=$?
    set -e
    if [[ "${status}" -eq 0 ]]; then
      if ! run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_name, expected_created, expected_mount, profile = sys.argv[2:]
if len(items) != 1:
    raise SystemExit("tracked Redis volume inventory is not singular")
volume = items[0]
if volume.get("Name") != expected_name or volume.get("CreatedAt") != expected_created or volume.get("Mountpoint") != expected_mount:
    raise SystemExit("tracked Redis volume immutable identity changed")
labels = items[0].get("Labels") or {}
if labels.get("managed-by") != "minisky" or labels.get("minisky.profile") != profile or labels.get("minisky.service") != "memorystore-redis":
    raise SystemExit("tracked Redis volume ownership changed")
' "${inspect}" "${volume_name}" "${volume_created}" "${volume_mount}" "${profile}" ||
        ! run_bounded 15 docker volume rm "${volume_name}" >/dev/null; then
        failed=1
      fi
    elif [[ "${status}" -eq 1 && "${inspect}" == *"no such volume"* ]]; then
      :
    else
      echo "Unable to inspect tracked Redis volume ${volume_name}: ${inspect}" >&2
      failed=1
    fi
  done
  for network_id in "${tracked_network_ids[@]:-}"; do
    [[ -n "${network_id}" ]] || continue
    set +e
    inspect="$(run_bounded 10 docker network inspect "${network_id}" 2>&1)"
    status=$?
    set -e
    if [[ "${status}" -eq 0 ]]; then
      if ! run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_id, profile = sys.argv[2:]
if len(items) != 1 or items[0].get("Id") != expected_id:
    raise SystemExit("tracked Redis network immutable ID changed")
labels = items[0].get("Labels") or {}
if labels.get("managed-by") != "minisky" or labels.get("minisky.profile") != profile:
    raise SystemExit("tracked Redis network ownership changed")
' "${inspect}" "${network_id}" "${profile}" ||
        ! run_bounded 15 docker network rm "${network_id}" >/dev/null; then
        failed=1
      fi
    elif [[ "${status}" -eq 1 ]] &&
      [[ "${inspect}" == "Error response from daemon: network ${network_id} not found" ||
        "${inspect}" == $'[]\n'"Error response from daemon: network ${network_id} not found" ||
        "${inspect}" == "network ${network_id} not found" ]]; then
      :
    else
      echo "Unable to inspect tracked Redis network ${network_id}: ${inspect}" >&2
      failed=1
    fi
  done
  return "${failed}"
}

track_current_network_if_present() {
  local inspect
  local status
  local network_id
  local tracked
  if [[ "${initial_network_absence_proven}" -ne 1 ]]; then
    echo "Refusing to track minisky-net without an initial absence proof." >&2
    return 1
  fi
  set +e
  inspect="$(run_bounded 10 docker network inspect minisky-net 2>&1)"
  status=$?
  set -e
  if [[ "${status}" -eq 1 ]] &&
    [[ "${inspect}" == "Error response from daemon: network minisky-net not found" ||
      "${inspect}" == $'[]\nError response from daemon: network minisky-net not found' ||
      "${inspect}" == "network minisky-net not found" ]]; then
    return
  fi
  if [[ "${status}" -ne 0 ]]; then
    echo "Unable to inspect a potentially created minisky-net (status ${status}): ${inspect}" >&2
    return 1
  fi
  if ! network_id="$(run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
profile = sys.argv[2]
if len(items) != 1:
    raise SystemExit("Redis network inventory is not singular")
network = items[0]
network_id = network.get("Id")
labels = network.get("Labels") or {}
if not network_id or labels.get("managed-by") != "minisky" or labels.get("minisky.profile") != profile:
    raise SystemExit("Redis network immutable ownership is incomplete")
print(network_id)
' "${inspect}" "${profile}")"; then
    return 1
  fi
  current_network_id="${network_id}"
  for tracked in "${tracked_network_ids[@]:-}"; do
    [[ -n "${tracked}" ]] || continue
    if [[ "${tracked}" == "${network_id}" ]]; then
      return
    fi
  done
  tracked_network_ids+=("${network_id}")
}

track_current_network() {
  track_current_network_if_present
  if [[ -z "${current_network_id}" ]]; then
    echo "Redis gateway became ready without an exact tracked minisky-net." >&2
    return 1
  fi
}

track_redis_binding() {
  local container_id="$1"
  local volume_name="$2"
  local volume_created="$3"
  local volume_mount="$4"
  tracked_container_ids+=("${container_id}")
  tracked_volumes+=("${volume_name}|${volume_created}|${volume_mount}")
}

track_container_id() {
  tracked_container_ids+=("$1")
}

run_minisky_bounded() {
  local seconds="$1"
  shift
  (
    cd "${minisky_runtime_dir}"
    run_bounded "${seconds}" "${minisky_binary}" "$@"
  )
}

cleanup() {
  local exit_code=$?
  local cleanup_failed=0
  in_cleanup=1
  trap - EXIT INT TERM
  if [[ -n "${watchdog_pid}" ]] && kill -0 "${watchdog_pid}" 2>/dev/null; then
    kill -TERM "${watchdog_pid}" 2>/dev/null || cleanup_failed=1
    wait "${watchdog_pid}" 2>/dev/null || true
  fi
  stop_minisky || cleanup_failed=1
  remove_foreign_container || cleanup_failed=1
  remove_foreign_volume || cleanup_failed=1
  remove_foreign_network || cleanup_failed=1
  cleanup_tracked_resources || cleanup_failed=1
  assert_profile_empty || cleanup_failed=1
  if [[ "${exit_code}" -ne 0 && -n "${work_dir}" && -f "${work_dir}/minisky.log" ]]; then
    echo "MiniSky Redis integration log (last 200 lines):" >&2
    python3 - "${work_dir}/minisky.log" <<'PY' >&2 || cleanup_failed=1
import pathlib
import sys
print("\n".join(pathlib.Path(sys.argv[1]).read_text(errors="replace").splitlines()[-200:]))
PY
  fi
  if [[ -n "${work_dir}" ]]; then
    chmod -R u+w "${work_dir}" 2>/dev/null || cleanup_failed=1
    rm -rf "${work_dir}" || cleanup_failed=1
  fi
  release_locks || cleanup_failed=1
  if [[ "${exit_code}" -eq 0 && "${cleanup_failed}" -ne 0 ]]; then
    exit_code=1
  fi
  exit "${exit_code}"
}

trap cleanup EXIT
trap 'signal_exit 130' INT
trap 'signal_exit 143' TERM
start_watchdog

assert_initial_network_absent
if ! existing_containers="$(run_bounded 15 docker ps -a --format '{{.Names}}' 2>&1)"; then
  echo "Unable to inventory Docker containers: ${existing_containers}" >&2
  exit 1
fi
while IFS= read -r existing; do
  case "${existing}" in
    minisky-*)
      echo "Refusing to disturb existing MiniSky container ${existing}." >&2
      exit 1
      ;;
  esac
done <<<"${existing_containers}"
assert_profile_empty

work_dir="$(mktemp -d)"
gate_id="redis-gate-$$-$(date +%s)"
minisky_runtime_dir="${work_dir}/runtime"

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
project="local-dev-project"
location="us-central1"
sdk_instance_id="minisky-sdk-redis"
valkey_image_ref="valkey/valkey:7.2.12-alpine@sha256:28ca383369c5497fb4d63092e852a1c9e23c5a0b5553bb8f0f54a0b7fa0ddd4b"
valkey_repo_digest="valkey/valkey@sha256:28ca383369c5497fb4d63092e852a1c9e23c5a0b5553bb8f0f54a0b7fa0ddd4b"
valkey_image_id=""

export HOME="${work_dir}/home"
export MINISKY_STATE_DIR="${work_dir}/state"
export MINISKY_PROFILE="${profile}"
export TF_DATA_DIR="${work_dir}/terraform-data"
mkdir -p "${HOME}" "${TF_DATA_DIR}" "${minisky_runtime_dir}"

go build -trimpath -o "${work_dir}/minisky" ./cmd/minisky
go build -trimpath -o "${work_dir}/redis-sdk" ./sdk-smoke/redis
minisky_binary="${work_dir}/minisky"

create_foreign_network() {
  foreign_network_id="$(run_bounded 15 docker network create \
    --label managed-by=redis-gate-foreign \
    --label "minisky.redis-gate-run=${gate_id}" \
    minisky-net)"
}

assert_foreign_network_preserved() {
  run_bounded 10 docker network inspect "${foreign_network_id}" >/dev/null
}

create_foreign_volume() {
  local inspect
  foreign_volume="$1"
  run_bounded 15 docker volume create \
    --label managed-by=redis-gate-foreign \
    --label "minisky.redis-gate-run=${gate_id}" \
    "${foreign_volume}" >/dev/null
  inspect="$(run_bounded 10 docker volume inspect "${foreign_volume}")"
  IFS=$'\t' read -r foreign_volume_created foreign_volume_mount \
    <<<"$(run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_name, gate = sys.argv[2:]
if len(items) != 1 or items[0].get("Name") != expected_name:
    raise SystemExit("created foreign Redis volume identity is invalid")
labels = items[0].get("Labels") or {}
created = items[0].get("CreatedAt")
mount = items[0].get("Mountpoint")
if not created or not mount or labels.get("managed-by") != "redis-gate-foreign" or labels.get("minisky.redis-gate-run") != gate:
    raise SystemExit("created foreign Redis volume identity is incomplete")
print(f"{created}\t{mount}")
' "${inspect}" "${foreign_volume}" "${gate_id}")"
}

assert_foreign_volume_preserved() {
  local inspect
  inspect="$(run_bounded 10 docker volume inspect "${foreign_volume}")"
  run_bounded 10 python3 -c '
import json
import sys
items = json.loads(sys.argv[1])
expected_name, expected_created, expected_mount, gate = sys.argv[2:]
if len(items) != 1 or items[0].get("Name") != expected_name:
    raise SystemExit("foreign volume immutable identity changed")
if items[0].get("CreatedAt") != expected_created or items[0].get("Mountpoint") != expected_mount:
    raise SystemExit("foreign volume creation identity changed")
labels = items[0].get("Labels") or {}
if labels.get("managed-by") != "redis-gate-foreign" or labels.get("minisky.redis-gate-run") != gate:
    raise SystemExit("foreign volume marker changed")
' "${inspect}" "${foreign_volume}" "${foreign_volume_created}" "${foreign_volume_mount}" "${gate_id}"
}

create_foreign_container() {
  local name="$1"
  local image_id="$2"
  foreign_container_id="$(run_bounded 15 docker create \
    --name "${name}" \
    --label managed-by=redis-gate-foreign \
    --label "minisky.redis-gate-run=${gate_id}" \
    "${image_id}")"
}

assert_foreign_container_preserved() {
  run_bounded 10 docker inspect --type container "${foreign_container_id}" >/dev/null
}

require_ownership_conflict() {
  local probe="$1"
  local status="$2"
  local expected="$3"
  local log_path="$4"
  local output
  output="$(<"${log_path}")"
  if [[ "${status}" -eq 124 ]]; then
    echo "${probe} ownership probe timed out." >&2
    return 1
  fi
  if [[ "${status}" -ne 1 ]]; then
    echo "${probe} ownership probe returned ${status}, want explicit conflict status 1." >&2
    return 1
  fi
  if [[ "${output}" != *"${expected}"* ]]; then
    echo "${probe} ownership probe failed without the expected ownership conflict." >&2
    return 1
  fi
}

create_foreign_network
set +e
run_minisky_bounded 20 start \
  --port "${api_port}" --ui-port "${ui_port}" --services redis.googleapis.com \
  >"${work_dir}/foreign-network.log" 2>&1
probe_status=$?
set -e
require_ownership_conflict \
  "foreign network" "${probe_status}" \
  'Docker resource ownership conflict: network "minisky-net" exists but is not owned' \
  "${work_dir}/foreign-network.log"
assert_foreign_network_preserved
remove_foreign_network

start_minisky() {
  : >"${work_dir}/minisky.log"
  current_network_id=""
  (
    cd "${minisky_runtime_dir}"
    exec "${minisky_binary}" start \
      --port "${api_port}" \
      --ui-port "${ui_port}" \
      --services redis.googleapis.com
  ) >"${work_dir}/minisky.log" 2>&1 &
  minisky_pid=$!
  for _ in $(seq 1 160); do
    if ! track_current_network_if_present; then
      return 1
    fi
    if ! kill -0 "${minisky_pid}" 2>/dev/null; then
      wait "${minisky_pid}" || true
      echo "MiniSky exited before Redis gateway readiness." >&2
      return 1
    fi
    if run_bounded 2 curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
      track_current_network
      return
    fi
    sleep 0.25
  done
  echo "Timed out waiting for MiniSky Redis gateway." >&2
  return 1
}

run_sdk() {
  local mode="$1"
  local instance_id="${2:-${sdk_instance_id}}"
  MINISKY_ENDPOINT="${gateway}" \
    MINISKY_PROJECT_ID="${project}" \
    MINISKY_REDIS_LOCATION="${location}" \
    MINISKY_REDIS_INSTANCE_ID="${instance_id}" \
    MINISKY_REDIS_MODE="${mode}" \
    run_bounded 180 "${work_dir}/redis-sdk"
}

redis_resource_names() {
  run_bounded 10 python3 - "$profile" "$project" "$location" "$1" <<'PY'
import hashlib
import sys

profile, project, location, instance = sys.argv[1:]
resource_hash = hashlib.sha256()
for part in (project, location, instance):
    encoded = part.encode()
    resource_hash.update(len(encoded).to_bytes(4, "big"))
    resource_hash.update(encoded)
resource_id = "redis-" + resource_hash.hexdigest()[:32]
docker_hash = hashlib.sha256((profile + "\0" + resource_id).encode()).hexdigest()[:16]
print(f"{resource_id}\tminisky-redis-{docker_hash}\tminisky-redis-data-{docker_hash}")
PY
}

discover_api_endpoint() {
  local instance_id="$1"
  local response="${work_dir}/${instance_id}-api.json"
  run_bounded 10 curl --fail --silent --show-error \
    "${gateway}/_minisky/redis.googleapis.com/v1/projects/${project}/locations/${location}/instances/${instance_id}" \
    --output "${response}"
  run_bounded 10 python3 - "${response}" "${project}" "${location}" "${instance_id}" <<'PY'
import ipaddress
import json
import pathlib
import sys

path, project, location, instance_id = sys.argv[1:]
raw = pathlib.Path(path).read_bytes()
if not raw or len(raw) > 1 << 20:
    raise SystemExit("Redis API response is empty or oversized")
document = json.loads(raw)
expected = f"projects/{project}/locations/{location}/instances/{instance_id}"
if document.get("name") != expected or document.get("state") != "READY":
    raise SystemExit("Redis API response is not the expected READY instance")
host = document.get("host")
port = document.get("port")
if not isinstance(host, str) or not isinstance(port, int) or isinstance(port, bool):
    raise SystemExit("Redis API endpoint is malformed")
address = ipaddress.ip_address(host)
if not address.is_loopback or not 1 <= port <= 65535:
    raise SystemExit("Redis API endpoint is not bounded to loopback")
print(f"[{host}]:{port}" if address.version == 6 else f"{host}:{port}")
PY
}

inspect_exact_redis_binding() {
  local resource_id="$1"
  local container_name="$2"
  local volume_name="$3"
  local expected_api_endpoint="$4"
  local container_json="${work_dir}/${container_name}-container.json"
  local volume_json="${work_dir}/${volume_name}-volume.json"
  run_bounded 10 docker inspect --type container "${container_name}" >"${container_json}"
  run_bounded 10 docker volume inspect "${volume_name}" >"${volume_json}"
  run_bounded 10 python3 - \
    "${container_json}" "${volume_json}" "${profile}" "${resource_id}" \
    "${container_name}" "${volume_name}" "${expected_api_endpoint}" "${valkey_image_id}" <<'PY'
import ipaddress
import hashlib
import json
import pathlib
import re
import sys

container_path, volume_path, profile, resource_id, container_name, volume_name, expected_endpoint, image_id = sys.argv[1:]
containers = json.loads(pathlib.Path(container_path).read_text())
volumes = json.loads(pathlib.Path(volume_path).read_text())
if len(containers) != 1 or len(volumes) != 1:
    raise SystemExit("exact Redis Docker inventory is not singular")
container = containers[0]
volume = volumes[0]
container_id = container.get("Id")
if not isinstance(container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", container_id):
    raise SystemExit("Redis container immutable ID is malformed")
if container.get("Image") != image_id or (container.get("Config") or {}).get("Image") != image_id:
    raise SystemExit("Redis container was not created by the immutable image ID")
if (container.get("Config") or {}).get("Cmd") != [
    "valkey-server", "--appendonly", "yes", "--appendfsync", "always", "--dir", "/data"
]:
    raise SystemExit("Redis AOF command configuration drifted")
if (container.get("HostConfig") or {}).get("NetworkMode") != "minisky-net":
    raise SystemExit("Redis container network drifted")
expected_labels = {
    "managed-by": "minisky",
    "minisky.profile": profile,
    "minisky.service": "memorystore-redis",
    "minisky.resource": resource_id,
}
if volume.get("Name") != volume_name or not volume.get("CreatedAt") or not volume.get("Mountpoint"):
    raise SystemExit("Redis volume immutable identity is incomplete")
volume_labels = volume.get("Labels") or {}
volume_provenance = volume_labels.get("minisky.volume-identity")
if not isinstance(volume_provenance, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", volume_provenance):
    raise SystemExit("Redis volume provenance label is malformed")
expected_volume_labels = {**expected_labels, "minisky.volume-identity": volume_provenance}
actual_volume_ownership = {
    key: value for key, value in volume_labels.items()
    if key == "managed-by" or key.lower().startswith("minisky.")
}
if actual_volume_ownership != expected_volume_labels:
    raise SystemExit("Redis volume ownership labels do not match")
volume_identity = "sha256:" + hashlib.sha256(
    (volume_name + "\0" + volume.get("CreatedAt") + "\0" + volume.get("Mountpoint")).encode()
).hexdigest()
container_labels = (container.get("Config") or {}).get("Labels") or {}
generation = container_labels.get("minisky.generation")
container_identity = container_labels.get("minisky.container-identity")
if not isinstance(generation, str) or not generation.isascii() or not generation.isdigit() or int(generation) < 1:
    raise SystemExit("Redis container generation label is malformed")
if not isinstance(container_identity, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", container_identity):
    raise SystemExit("Redis container provenance label is malformed")
expected_container_labels = {**expected_labels,
    "minisky.generation": generation,
    "minisky.container-identity": container_identity,
    "minisky.container-name": container_name,
    "minisky.image-id": image_id,
    "minisky.volume-name": volume_name,
    "minisky.volume-identity": volume_identity,
    "minisky.volume-provenance": volume_provenance,
}
actual_container_ownership = {
    key: value for key, value in container_labels.items()
    if key == "managed-by" or key.lower().startswith("minisky.")
}
if actual_container_ownership != expected_container_labels:
    raise SystemExit("Redis container ownership labels do not match")
mounts = container.get("Mounts") or []
if len(mounts) != 1 or mounts[0].get("Type") != "volume" or mounts[0].get("Name") != volume_name or mounts[0].get("Destination") != "/data" or mounts[0].get("RW") is not True:
    raise SystemExit("Redis container does not have the exact AOF volume")
ports = ((container.get("NetworkSettings") or {}).get("Ports") or {}).get("6379/tcp")
if not isinstance(ports, list) or len(ports) != 1:
    raise SystemExit("Redis port binding is not singular")
host = ports[0].get("HostIp")
port = ports[0].get("HostPort")
address = ipaddress.ip_address(host)
if not address.is_loopback or not isinstance(port, str) or not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535:
    raise SystemExit("Redis published endpoint is not loopback")
endpoint = f"[{host}]:{port}" if address.version == 6 else f"{host}:{port}"
if endpoint != expected_endpoint:
    raise SystemExit("Redis API endpoint does not match exact-owned Docker endpoint")
print(f"{container_id}\t{volume.get('CreatedAt')}\t{volume.get('Mountpoint')}")
PY
}

resolve_valkey_image_id() {
  local inspect
  inspect="$(run_bounded 10 docker image inspect "${valkey_image_ref}")"
  run_bounded 10 python3 - \
    "${inspect}" "${valkey_image_ref}" "${valkey_repo_digest}" <<'PY'
import json
import re
import sys

items = json.loads(sys.argv[1])
image, repo_digest = sys.argv[2:]
if len(items) != 1:
    raise SystemExit("Valkey image inventory is not singular")
identity = items[0]
image_id = identity.get("Id")
if not isinstance(image_id, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
    raise SystemExit("Valkey Docker image ID is malformed")
if not any(value in (image, repo_digest) for value in identity.get("RepoDigests") or []):
    raise SystemExit("Valkey RepoDigest was not confirmed")
if f"{identity.get('Os')}/{identity.get('Architecture')}" != "linux/amd64":
    raise SystemExit("Valkey image is not the verified linux/amd64 platform")
print(image_id)
PY
}

assert_single_exact_container() {
  local resource_id="$1"
  local expected_id="$2"
  local inventory
  inventory="$(run_bounded 10 docker ps -aq --no-trunc \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-redis" \
    --filter "label=minisky.resource=${resource_id}")"
  if [[ "${inventory}" != "${expected_id}" ]]; then
    echo "Exact Redis container inventory is not singular and stable: ${inventory}" >&2
    return 1
  fi
}

assert_redis_value() {
  local endpoint="$1"
  local key="$2"
  local value="$3"
  local mode="$4"
  run_bounded 10 python3 - "${endpoint}" "${key}" "${value}" "${mode}" <<'PY'
import ipaddress
import socket
import sys

endpoint, key, value, mode = sys.argv[1:]
host, separator, port_text = endpoint.rpartition(":")
host = host.removeprefix("[").removesuffix("]")
if not separator or not port_text.isascii() or not port_text.isdigit():
    raise SystemExit("Redis endpoint is malformed")
address = ipaddress.ip_address(host)
port = int(port_text)
if not address.is_loopback or not 1 <= port <= 65535:
    raise SystemExit("Redis endpoint is outside loopback bounds")
key_bytes = key.encode("ascii")
value_bytes = value.encode()
if not key_bytes or len(key_bytes) > 128 or len(value_bytes) > 1024:
    raise SystemExit("Redis evidence key or value is outside bounds")

def encode(parts):
    return b"*" + str(len(parts)).encode() + b"\r\n" + b"".join(
        b"$" + str(len(part)).encode() + b"\r\n" + part + b"\r\n" for part in parts
    )

def read_line(reader):
    line = reader.readline(4097)
    if len(line) > 4096 or not line.endswith(b"\r\n"):
        raise RuntimeError("Redis returned an invalid response line")
    return line[:-2]

def command(parts):
    with socket.create_connection((host, port), timeout=2) as connection:
        connection.settimeout(2)
        reader = connection.makefile("rb")
        connection.sendall(encode(parts))
        first = read_line(reader)
        if first.startswith(b"+"):
            return first[1:]
        if not first.startswith(b"$"):
            raise RuntimeError(f"unexpected Redis response {first!r}")
        length = int(first[1:])
        if length < 0 or length > 1024:
            raise RuntimeError("Redis bulk response length is outside bounds")
        payload = reader.read(length + 2)
        if len(payload) != length + 2 or not payload.endswith(b"\r\n"):
            raise RuntimeError("Redis bulk response is truncated")
        return payload[:-2]

if mode == "set":
    if command([b"SET", key_bytes, value_bytes]) != b"OK":
        raise RuntimeError("Redis SET failed")
if command([b"GET", key_bytes]) != value_bytes:
    raise RuntimeError("Redis GET returned a different value")
print(f"Redis RESP SET/GET passed: endpoint={endpoint} key={key}")
PY
}

remove_exact_redis_container() {
  local expected_container_id="$1"
  local resource_id="$2"
  local container_name="$3"
  local expected_volume="$4"
  local expected_endpoint="$5"
  local binding
  binding="$(inspect_exact_redis_binding "${resource_id}" "${container_name}" "${expected_volume}" "${expected_endpoint}")"
  if [[ "${binding%%$'\t'*}" != "${expected_container_id}" ]]; then
    echo "Redis immutable container ID changed before replacement." >&2
    return 1
  fi
  run_bounded 15 docker rm -f "${expected_container_id}" >/dev/null
  run_bounded 10 docker volume inspect "${expected_volume}" >/dev/null
}

assert_export_boundary() {
  local snapshot="$1"
  local secret_value="$2"
  run_bounded 10 python3 - "${snapshot}" "${secret_value}" <<'PY'
import pathlib
import sys

data = pathlib.Path(sys.argv[1]).read_bytes()
if not data or len(data) > 16 << 20:
    raise SystemExit("Redis metadata snapshot is empty or oversized")
for forbidden in (
    b"/var/lib/docker/volumes/",
    b"appendonly.aof",
    b"access_token",
    b"authString",
    b"minisky-local-only",
    sys.argv[2].encode(),
):
    if forbidden in data:
        raise SystemExit(f"Redis export crossed a non-portable boundary: {forbidden!r}")
PY
}

assert_no_exact_redis_resources() {
  local resource_id="$1"
  local container_name="$2"
  local volume_name="$3"
  local inventory
  inventory="$(run_bounded 10 docker ps -aq --no-trunc \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-redis" \
    --filter "label=minisky.resource=${resource_id}")"
  [[ -z "${inventory}" ]] || {
    echo "Exact Redis container remains: ${inventory}" >&2
    return 1
  }
  if run_bounded 10 docker inspect --type container "${container_name}" >/dev/null 2>&1; then
    echo "Deterministic Redis container name remains." >&2
    return 1
  fi
  if run_bounded 10 docker volume inspect "${volume_name}" >/dev/null 2>&1; then
    echo "Deterministic Redis volume remains." >&2
    return 1
  fi
}

assert_exact_cleanup() {
  local containers
  local volumes
  containers="$(run_bounded 10 docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-redis")"
  volumes="$(run_bounded 10 docker volume ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-redis")"
  if [[ -n "${containers}" || -n "${volumes}" ]]; then
    echo "Exact Redis resources remain: containers=${containers} volumes=${volumes}" >&2
    return 1
  fi
}

start_minisky

IFS=$'\t' read -r foreign_volume_resource foreign_volume_container foreign_volume_name \
  <<<"$(redis_resource_names "redis-foreign-volume")"
if [[ -z "${foreign_volume_resource}" || -z "${foreign_volume_container}" || -z "${foreign_volume_name}" ]]; then
  echo "Redis deterministic foreign-volume names are incomplete." >&2
  exit 1
fi
create_foreign_volume "${foreign_volume_name}"
set +e
run_sdk create "redis-foreign-volume" >"${work_dir}/foreign-volume.log" 2>&1
probe_status=$?
set -e
require_ownership_conflict \
  "foreign volume" "${probe_status}" \
  "refusing to adopt pre-existing Redis volume \"${foreign_volume_name}\"" \
  "${work_dir}/foreign-volume.log"
assert_foreign_volume_preserved
remove_foreign_volume
valkey_image_id="$(resolve_valkey_image_id)"

IFS=$'\t' read -r foreign_container_resource foreign_container_name foreign_container_volume \
  <<<"$(redis_resource_names "redis-foreign-container")"
if [[ -z "${foreign_container_resource}" || -z "${foreign_container_name}" || -z "${foreign_container_volume}" ]]; then
  echo "Redis deterministic foreign-container names are incomplete." >&2
  exit 1
fi
create_foreign_container "${foreign_container_name}" \
  "${valkey_image_id}"
set +e
run_sdk create "redis-foreign-container" >"${work_dir}/foreign-container.log" 2>&1
probe_status=$?
set -e
require_ownership_conflict \
  "foreign container" "${probe_status}" \
  "Redis container \"${foreign_container_name}\" already exists" \
  "${work_dir}/foreign-container.log"
assert_foreign_container_preserved
remove_foreign_container

run_sdk create
IFS=$'\t' read -r sdk_resource_id sdk_container sdk_volume <<<"$(redis_resource_names "${sdk_instance_id}")"
sdk_endpoint="$(discover_api_endpoint "${sdk_instance_id}")"
sdk_binding="$(inspect_exact_redis_binding "${sdk_resource_id}" "${sdk_container}" "${sdk_volume}" "${sdk_endpoint}")"
IFS=$'\t' read -r sdk_container_id sdk_volume_created sdk_volume_mount <<<"${sdk_binding}"
track_redis_binding "${sdk_container_id}" "${sdk_volume}" "${sdk_volume_created}" "${sdk_volume_mount}"
evidence_key="minisky-redis-${gate_id}"
evidence_value="redis-aof-survival-${gate_id}"
assert_redis_value "${sdk_endpoint}" "${evidence_key}" "${evidence_value}" set
assert_single_exact_container "${sdk_resource_id}" "${sdk_container_id}"

stop_minisky
snapshot="${work_dir}/redis-metadata-export.json"
(
  cd "${minisky_runtime_dir}"
  "${minisky_binary}" state --profile "${profile}" export "${snapshot}"
)
assert_export_boundary "${snapshot}" "${evidence_value}"
start_minisky
run_sdk verify
sdk_endpoint="$(discover_api_endpoint "${sdk_instance_id}")"
assert_redis_value "${sdk_endpoint}" "${evidence_key}" "${evidence_value}" get
restarted_binding="$(inspect_exact_redis_binding "${sdk_resource_id}" "${sdk_container}" "${sdk_volume}" "${sdk_endpoint}")"
IFS=$'\t' read -r restarted_container_id restarted_volume_created restarted_volume_mount <<<"${restarted_binding}"
if [[ "${restarted_container_id}" == "${sdk_container_id}" ||
  "${restarted_volume_created}" != "${sdk_volume_created}" ||
  "${restarted_volume_mount}" != "${sdk_volume_mount}" ]]; then
  echo "MiniSky restart did not reconcile a fresh container around the exact Redis volume." >&2
  exit 1
fi
track_container_id "${restarted_container_id}"
assert_single_exact_container "${sdk_resource_id}" "${restarted_container_id}"

remove_exact_redis_container \
  "${restarted_container_id}" "${sdk_resource_id}" "${sdk_container}" "${sdk_volume}" "${sdk_endpoint}"
stop_minisky
start_minisky
run_sdk verify
sdk_endpoint="$(discover_api_endpoint "${sdk_instance_id}")"
assert_redis_value "${sdk_endpoint}" "${evidence_key}" "${evidence_value}" get
replacement_binding="$(inspect_exact_redis_binding "${sdk_resource_id}" "${sdk_container}" "${sdk_volume}" "${sdk_endpoint}")"
IFS=$'\t' read -r replacement_container_id replacement_volume_created replacement_volume_mount <<<"${replacement_binding}"
if [[ "${replacement_container_id}" == "${sdk_container_id}" ||
  "${replacement_volume_created}" != "${sdk_volume_created}" ||
  "${replacement_volume_mount}" != "${sdk_volume_mount}" ]]; then
  echo "Redis container replacement did not retain the exact AOF volume." >&2
  exit 1
fi
track_container_id "${replacement_container_id}"
assert_single_exact_container "${sdk_resource_id}" "${replacement_container_id}"
run_sdk delete
assert_no_exact_redis_resources "${sdk_resource_id}" "${sdk_container}" "${sdk_volume}"

terraform_dir="${repository_root}/terraform/redis"
terraform_state="${work_dir}/terraform.tfstate"
tf_instance_id="minisky-terraform-redis"
tf_vars=(
  -var="endpoint=${gateway}"
  -var="project_id=${project}"
  -var="region=${location}"
  -var="instance_name=${tf_instance_id}"
)
IFS=$'\t' read -r tf_resource_id tf_container tf_volume <<<"$(redis_resource_names "${tf_instance_id}")"

assert_no_drift() {
  local plan_exit
  set +e
  terraform_bounded -chdir="${terraform_dir}" plan -detailed-exitcode -input=false "${tf_vars[@]}"
  plan_exit=$?
  set -e
  if [[ "${plan_exit}" -ne 0 ]]; then
    echo "Redis ${drift_phase} plan returned ${plan_exit}, want 0." >&2
    return 1
  fi
}

terraform_bounded -chdir="${terraform_dir}" init \
  -reconfigure \
  -backend-config="path=${terraform_state}" \
  -input=false \
  -lockfile=readonly
terraform_bounded -chdir="${terraform_dir}" validate
terraform_bounded -chdir="${terraform_dir}" apply -auto-approve -input=false "${tf_vars[@]}"
tf_host="$(terraform_bounded -chdir="${terraform_dir}" output -raw host)"
tf_port="$(terraform_bounded -chdir="${terraform_dir}" output -raw port)"
tf_endpoint="${tf_host}:${tf_port}"
IFS=$'\t' read -r tf_container_id tf_volume_created tf_volume_mount \
  <<<"$(inspect_exact_redis_binding "${tf_resource_id}" "${tf_container}" "${tf_volume}" "${tf_endpoint}")"
track_redis_binding "${tf_container_id}" "${tf_volume}" "${tf_volume_created}" "${tf_volume_mount}"
tf_key="minisky-terraform-${gate_id}"
tf_value="terraform-aof-survival-${gate_id}"
assert_redis_value "${tf_endpoint}" "${tf_key}" "${tf_value}" set
drift_phase="post-apply"
assert_no_drift

stop_minisky
start_minisky
drift_phase="post-restart"
assert_no_drift
assert_redis_value "${tf_endpoint}" "${tf_key}" "${tf_value}" get

terraform_bounded -chdir="${terraform_dir}" destroy -auto-approve -input=false "${tf_vars[@]}"
stop_minisky
start_minisky
deleted_url="${gateway}/_minisky/redis.googleapis.com/v1/projects/${project}/locations/${location}/instances/${tf_instance_id}"
deleted_status="$(run_bounded 10 curl --silent --output "${work_dir}/deleted.json" \
  --write-out '%{http_code}' "${deleted_url}")"
if [[ "${deleted_status}" != "404" ]]; then
  echo "Redis GET after Terraform destroy returned ${deleted_status}, want 404." >&2
  exit 1
fi
assert_no_exact_redis_resources "${tf_resource_id}" "${tf_container}" "${tf_volume}"
assert_exact_cleanup

echo "Redis generated SDK, AOF restart/replacement, foreign refusal, export boundary, and Terraform lifecycle passed."
