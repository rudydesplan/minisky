#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_MEMCACHE_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Memcached integration without MINISKY_MEMCACHE_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3 terraform; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done

shared_lock="/tmp/minisky-net-integration.lock"
phase_lock="/tmp/minisky-memcache-integration.lock"
if ! mkdir "${shared_lock}" 2>/dev/null; then
  echo "Another MiniSky Docker integration is active (${shared_lock})." >&2
  exit 1
fi
if ! mkdir "${phase_lock}" 2>/dev/null; then
  rmdir "${shared_lock}" 2>/dev/null || true
  echo "Another MiniSky Memcached integration run is active (${phase_lock})." >&2
  exit 1
fi

work_dir=""
profile=""
minisky_pid=""
watchdog_pid=""
overall_timeout_seconds="${MINISKY_MEMCACHE_TIMEOUT_SECONDS:-600}"
terraform_command_timeout_seconds=240
in_cleanup=0

if [[ ! "${overall_timeout_seconds}" =~ ^[1-9][0-9]*$ ]]; then
  echo "MINISKY_MEMCACHE_TIMEOUT_SECONDS must be a positive integer." >&2
  exit 1
fi
overall_deadline_epoch=$(($(date +%s) + overall_timeout_seconds))

run_bounded() {
  local seconds="$1"
  local remaining
  shift
  if [[ "${in_cleanup}" -eq 0 ]]; then
    remaining=$((overall_deadline_epoch - $(date +%s)))
    if [[ "${remaining}" -le 0 ]]; then
      echo "Memcached integration exceeded ${overall_timeout_seconds} seconds." >&2
      return 124
    fi
    if [[ "${remaining}" -lt "${seconds}" ]]; then
      seconds="${remaining}"
    fi
  fi
  python3 - "${seconds}" "$@" <<'PY'
import subprocess
import sys

try:
    result = subprocess.run(sys.argv[2:], timeout=float(sys.argv[1]), check=False)
except subprocess.TimeoutExpired:
    print(f"Command timed out after {sys.argv[1]} seconds: {' '.join(sys.argv[2:])}", file=sys.stderr)
    raise SystemExit(124)
raise SystemExit(result.returncode)
PY
}

terraform_bounded() {
  run_bounded "${terraform_command_timeout_seconds}" terraform "$@"
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
  echo "Process ${pid} did not exit within ${seconds} seconds; sending KILL." >&2
  kill -KILL "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
  return 1
}

inspect_minisky_network() {
  local output
  local status
  if output="$(run_bounded 10 docker network inspect minisky-net 2>&1)"; then
    printf '%s' "${output}"
    return 0
  else
    status=$?
  fi
  if [[ "${status}" -eq 1 ]] &&
    [[ "${output}" == *"No such network: minisky-net"* ||
      "${output}" == *"network minisky-net not found"* ]]; then
    return 1
  fi
  echo "MiniSky Docker network inspection failed: ${output}" >&2
  return 2
}

network_label() {
  local inventory="$1"
  local label="$2"
  python3 -c '
import json
import sys

inventory = json.loads(sys.argv[1])
if len(inventory) != 1:
    raise SystemExit("unexpected Docker network inventory")
print((inventory[0].get("Labels") or {}).get(sys.argv[2], ""))
' "${inventory}" "${label}"
}

start_watchdog() {
  python3 - "$$" "${overall_timeout_seconds}" <<'PY' &
import os
import signal
import sys
import time

time.sleep(float(sys.argv[2]))
print(f"Memcached integration exceeded {sys.argv[2]} seconds.", file=sys.stderr)
os.kill(int(sys.argv[1]), signal.SIGTERM)
PY
  watchdog_pid=$!
}

signal_exit() {
  exit "$1"
}

cleanup() {
  exit_code=$?
  cleanup_failed=0
  in_cleanup=1
  trap - EXIT INT TERM
  if [[ -n "${watchdog_pid}" ]] && kill -0 "${watchdog_pid}" 2>/dev/null; then
    kill -TERM "${watchdog_pid}" 2>/dev/null || cleanup_failed=1
    wait "${watchdog_pid}" 2>/dev/null || true
  fi
  if [[ "${exit_code}" -ne 0 && -n "${work_dir}" && -f "${work_dir}/minisky.log" ]]; then
    echo "MiniSky Memcached integration log (last 200 lines):" >&2
    if ! python3 - "${work_dir}/minisky.log" <<'PY' >&2
import pathlib
import sys

lines = pathlib.Path(sys.argv[1]).read_text(errors="replace").splitlines()
print("\n".join(lines[-200:]))
PY
    then
      cleanup_failed=1
    fi
  fi
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}" 2>/dev/null || cleanup_failed=1
    wait_for_pid_exit "${minisky_pid}" 15 2>/dev/null || cleanup_failed=1
  fi
  if [[ -n "${profile}" ]]; then
    owned_inventory=""
    if ! owned_inventory="$(run_bounded 15 docker ps -aq \
      --filter "label=managed-by=minisky" \
      --filter "label=minisky.profile=${profile}" 2>&1)"; then
      echo "Cleanup Docker inventory failed: ${owned_inventory}" >&2
      cleanup_failed=1
    else
      while IFS= read -r container; do
        if [[ -n "${container}" ]] && ! run_bounded 15 docker rm -f "${container}" >/dev/null 2>&1; then
          cleanup_failed=1
        fi
      done <<<"${owned_inventory}"
    fi
    if network_inventory="$(inspect_minisky_network)"; then
      network_status=0
    else
      network_status=$?
    fi
    if [[ "${network_status}" -eq 0 ]]; then
      network_manager="$(network_label "${network_inventory}" "managed-by" 2>/dev/null)" || cleanup_failed=1
      network_profile="$(network_label "${network_inventory}" "minisky.profile" 2>/dev/null)" || cleanup_failed=1
      if [[ "${network_manager:-}" == "minisky" && "${network_profile:-}" == "${profile}" ]] &&
        ! run_bounded 15 docker network rm minisky-net >/dev/null 2>&1; then
        cleanup_failed=1
      fi
    elif [[ "${network_status}" -ne 1 ]]; then
      cleanup_failed=1
    fi
  fi
  if [[ -n "${work_dir}" ]]; then
    chmod -R u+w "${work_dir}" 2>/dev/null || cleanup_failed=1
    rm -rf "${work_dir}" || cleanup_failed=1
  fi
  rmdir "${phase_lock}" 2>/dev/null || cleanup_failed=1
  rmdir "${shared_lock}" 2>/dev/null || cleanup_failed=1
  if [[ "${exit_code}" -eq 0 && "${cleanup_failed}" -ne 0 ]]; then
    exit_code=1
  fi
  exit "${exit_code}"
}
trap cleanup EXIT
trap 'signal_exit 130' INT
trap 'signal_exit 143' TERM
start_watchdog

run_bounded 15 docker info >/dev/null
if inspect_minisky_network >/dev/null; then
  initial_network_status=0
else
  initial_network_status=$?
fi
if [[ "${initial_network_status}" -eq 0 ]]; then
  echo "Refusing to disturb an existing MiniSky Docker network." >&2
  exit 1
elif [[ "${initial_network_status}" -ne 1 ]]; then
  exit 1
fi
if ! existing_containers="$(docker ps -a --format '{{.Names}}' 2>&1)"; then
  echo "Unable to inventory existing Docker containers: ${existing_containers}" >&2
  exit 1
fi
while IFS= read -r container_name; do
  case "${container_name}" in
    minisky-*)
      echo "Refusing to disturb existing MiniSky container: ${container_name}" >&2
      exit 1
      ;;
  esac
done <<<"${existing_containers}"

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
profile="memcache-integration-$$"

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

export HOME="${work_dir}/home"
export MINISKY_STATE_DIR="${work_dir}/state"
export MINISKY_PROFILE="${profile}"
export TF_DATA_DIR="${work_dir}/terraform-data"
mkdir -p "${HOME}" "${TF_DATA_DIR}"

go build -trimpath -o "${work_dir}/minisky" ./cmd/minisky

start_minisky() {
  : >"${work_dir}/minisky.log"
  "${work_dir}/minisky" start \
    --port "${api_port}" \
    --ui-port "${ui_port}" \
    --services memcache.googleapis.com \
    >"${work_dir}/minisky.log" 2>&1 &
  minisky_pid=$!
  for _ in $(seq 1 120); do
    if ! kill -0 "${minisky_pid}" 2>/dev/null; then
      wait "${minisky_pid}" || true
      echo "MiniSky exited before readiness." >&2
      return 1
    fi
    if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "Timed out waiting for MiniSky readiness." >&2
  return 1
}

stop_minisky() {
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}"
    wait_for_pid_exit "${minisky_pid}" 15
  fi
  minisky_pid=""
}

run_sdk() {
  MINISKY_ENDPOINT="${gateway}" \
    MINISKY_PROJECT_ID="${project}" \
    MINISKY_MEMCACHE_LOCATION="${location}" \
    MINISKY_MEMCACHE_INSTANCE_ID="minisky-sdk-memcached" \
    MINISKY_MEMCACHE_MODE="$1" \
    go run ./sdk-smoke/memcache
}

assert_no_memcache_container() {
  local inventory
  if ! inventory="$(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-memcached" \
    --filter "label=minisky.resource=${tf_import}" 2>&1)"; then
    echo "Memcached Docker inventory failed: ${inventory}" >&2
    return 1
  fi
  if [[ -n "${inventory}" ]]; then
    echo "Exact-owned Memcached container remains after destroy: ${inventory}" >&2
    return 1
  fi
}

start_minisky
run_sdk create
stop_minisky
start_minisky
run_sdk verify
run_sdk delete

terraform_dir="${repository_root}/terraform/memcache"
terraform_state="${work_dir}/terraform.tfstate"
tf_vars=(
  -var="minisky_endpoint=${gateway}"
  -var="project_id=${project}"
  -var="region=${location}"
  -var="instance_name=minisky-terraform-memcached"
)
tf_address='google_memcache_instance.compatibility'
tf_import="projects/${project}/locations/${location}/instances/minisky-terraform-memcached"

assert_no_drift() {
  local plan_exit
  set +e
  terraform_bounded -chdir="${terraform_dir}" plan \
    -detailed-exitcode -input=false "${tf_vars[@]}"
  plan_exit=$?
  set -e
  if [[ "${plan_exit}" -ne 0 ]]; then
    echo "Memcached ${drift_phase} plan returned ${plan_exit}, want 0." >&2
    return 1
  fi
}

validate_import_normalization_plan() {
  python3 - "$1" "${tf_address}" <<'PY'
import json
import sys

plan_path, expected_address = sys.argv[1:]
with open(plan_path, encoding="utf-8") as handle:
    plan = json.load(handle)

if plan.get("resource_drift"):
    raise SystemExit("import normalization includes resource drift")
changes = plan.get("resource_changes") or []
if len(changes) != 1 or changes[0].get("address") != expected_address:
    raise SystemExit(f"import normalization must affect only {expected_address}")
change = changes[0].get("change") or {}
if change.get("actions") != ["update"]:
    raise SystemExit(f"unexpected import normalization actions: {change.get('actions')}")
before = change.get("before") or {}
after = change.get("after") or {}
changed = {key for key in set(before) | set(after) if before.get(key) != after.get(key)}
allowed = {"deletion_protection", "terraform_labels", "timeouts"}
if changed != allowed:
    unexpected = sorted(changed - allowed)
    missing = sorted(allowed - changed)
    raise SystemExit(f"import normalization fields differ: unexpected={unexpected} missing={missing}")
if before.get("deletion_protection") is not None or after.get("deletion_protection") is not False:
    raise SystemExit("deletion_protection normalization is not null to false")
if before.get("terraform_labels") not in (None, {}) or after.get("terraform_labels") != {
    "goog-terraform-provisioned": "true"
}:
    raise SystemExit("terraform_labels normalization is not the provider label injection")
if before.get("timeouts") not in (None, {}) or after.get("timeouts") != {
    "create": "3m", "delete": "3m", "update": "3m"
}:
    raise SystemExit("timeouts normalization does not match the fixture")
PY
}

capture_api_snapshot() {
  curl --fail --silent --show-error \
    "${gateway}/_minisky/memcache.googleapis.com/v1/${tf_import}" >"$1"
}

assert_api_snapshot_unchanged() {
  python3 - "$1" "$2" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as before_file:
    before = json.load(before_file)
with open(sys.argv[2], encoding="utf-8") as after_file:
    after = json.load(after_file)
if before != after:
    raise SystemExit("provider-only import normalization mutated Memcached API state")
PY
}

normalize_imported_provider_state() {
  # CLI import reconstructs API fields but cannot restore optional provider-only
  # configuration (deletion protection and custom timeouts) or computed
  # terraform_labels. Save and structurally validate the plan before applying
  # it, then prove the provider-only reconciliation did not mutate API state.
  local before_snapshot="${work_dir}/import-normalization-before.json"
  local after_snapshot="${work_dir}/import-normalization-after.json"
  capture_api_snapshot "${before_snapshot}"
  terraform_bounded -chdir="${terraform_dir}" plan \
    -input=false -out="${import_normalization_plan}" "${tf_vars[@]}"
  terraform_bounded -chdir="${terraform_dir}" show -json "${import_normalization_plan}" \
    >"${import_normalization_json}"
  validate_import_normalization_plan "${import_normalization_json}"
  terraform_bounded -chdir="${terraform_dir}" apply -input=false "${import_normalization_plan}"
  capture_api_snapshot "${after_snapshot}"
  assert_api_snapshot_unchanged "${before_snapshot}" "${after_snapshot}"
}

terraform_bounded -chdir="${terraform_dir}" init \
  -reconfigure \
  -backend-config="path=${terraform_state}" \
  -input=false \
  -lockfile=readonly
terraform_bounded -chdir="${terraform_dir}" validate
terraform_bounded -chdir="${terraform_dir}" apply \
  -auto-approve -input=false "${tf_vars[@]}"

drift_phase="post-apply"
assert_no_drift

drift_phase="post-restart"
stop_minisky
start_minisky
assert_no_drift

terraform_bounded -chdir="${terraform_dir}" state rm "${tf_address}"
terraform_bounded -chdir="${terraform_dir}" import "${tf_vars[@]}" "${tf_address}" "${tf_import}"
import_normalization_plan="${work_dir}/import-normalization.tfplan"
import_normalization_json="${work_dir}/import-normalization-plan.json"
normalize_imported_provider_state
drift_phase="post-import-normalization"
assert_no_drift

terraform_bounded -chdir="${terraform_dir}" destroy \
  -auto-approve -input=false "${tf_vars[@]}"
stop_minisky
start_minisky
deleted_url="${gateway}/_minisky/memcache.googleapis.com/v1/${tf_import}"
deleted_status="$(curl --silent --output "${work_dir}/deleted.json" --write-out '%{http_code}' "${deleted_url}")"
if [[ "${deleted_status}" != "404" ]]; then
  echo "Memcached GET after Terraform destroy returned ${deleted_status}, want 404." >&2
  exit 1
fi
assert_no_memcache_container

echo "Memcached SDK and Terraform apply/restart/import-normalization/no-drift/destroy lifecycle passed."
