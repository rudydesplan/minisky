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

memcache_backend_id() {
  run_bounded 10 python3 -c '
import hashlib
import sys

hasher = hashlib.sha256()
for part in sys.argv[1:]:
    encoded = part.encode("utf-8")
    if not encoded or len(encoded) > 1024:
        raise SystemExit("Memcached backend identity segment is outside bounds")
    hasher.update(len(encoded).to_bytes(4, "big"))
    hasher.update(encoded)
print("memcache-" + hasher.hexdigest()[:32])
' "$@"
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
sdk_instance_id="minisky-sdk-memcached"
sdk_import="projects/${project}/locations/${location}/instances/${sdk_instance_id}"
sdk_backend_id="$(memcache_backend_id "${project}" "${location}" "${sdk_instance_id}")"

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
    MINISKY_MEMCACHE_INSTANCE_ID="${sdk_instance_id}" \
    MINISKY_MEMCACHE_MODE="$1" \
    go run ./sdk-smoke/memcache
}

inspect_exact_memcache_container() {
  local container_id="${1:?Memcached container ID is required}"
  local resource_id="${2:?Memcached resource ID is required}"
  local inspection
  if ! inspection="$(run_bounded 10 docker inspect --type container \
    --format '{{json .Id}}{{"\t"}}{{json .State.Running}}{{"\t"}}{{json .State.Status}}{{"\t"}}{{json .Config.Labels}}{{"\t"}}{{json (index .NetworkSettings.Ports "11211/tcp")}}' \
    "${container_id}" 2>&1)"; then
    echo "Exact-owned Memcached Docker inspect failed: ${inspection}" >&2
    return 1
  fi
  run_bounded 10 python3 -c '
import ipaddress
import json
import re
import sys

inspection, expected_id, profile, resource_id = sys.argv[1:]
if len(inspection.encode("utf-8")) > 16384:
    raise SystemExit("Memcached Docker inspect output exceeds 16 KiB")
parts = inspection.split("\t")
if len(parts) != 5:
    raise SystemExit("Memcached Docker inspect output is malformed")
container_id, running, status, labels, bindings = (json.loads(part) for part in parts)
if not isinstance(container_id, str) or not re.fullmatch(r"[0-9a-f]{64}", container_id):
    raise SystemExit("Memcached Docker inspect returned a malformed immutable ID")
if container_id != expected_id:
    raise SystemExit("Memcached immutable ID changed during protocol evidence")
expected_labels = dict([
    ("managed-by", "minisky"),
    ("minisky.profile", profile),
    ("minisky.service", "memorystore-memcached"),
    ("minisky.resource", resource_id),
])
if labels != expected_labels:
    raise SystemExit("Memcached container labels do not exactly match ownership")
if running is not True or status != "running":
    raise SystemExit("Exact-owned Memcached container is not running")
if not isinstance(bindings, list) or len(bindings) != 1 or not isinstance(bindings[0], dict):
    raise SystemExit("Memcached 11211/tcp must have exactly one published binding")
host = bindings[0].get("HostIp")
port_text = bindings[0].get("HostPort")
if not isinstance(host, str) or not isinstance(port_text, str) or not port_text.isascii() or not port_text.isdigit():
    raise SystemExit("Memcached published endpoint is malformed")
try:
    address = ipaddress.ip_address(host)
    port = int(port_text)
except ValueError as error:
    raise SystemExit("Memcached published endpoint is malformed") from error
if not address.is_loopback or not 1 <= port <= 65535:
    raise SystemExit("Memcached published endpoint is not loopback")
endpoint = f"[{host}]:{port}" if address.version == 6 else f"{host}:{port}"
print(f"{container_id}\t{endpoint}")
' "${inspection}" "${container_id}" "${profile}" "${resource_id}"
}

discover_exact_memcache_container() {
  local resource_id="${1:?Memcached resource ID is required}"
  local inventory
  local container_ids=()
  if ! inventory="$(run_bounded 10 docker ps -aq --no-trunc \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-memcached" \
    --filter "label=minisky.resource=${resource_id}" 2>&1)"; then
    echo "Exact-owned Memcached Docker inventory failed: ${inventory}" >&2
    return 1
  fi
  while IFS= read -r container_id; do
    if [[ -n "${container_id}" ]]; then
      container_ids+=("${container_id}")
    fi
  done <<<"${inventory}"
  if [[ "${#container_ids[@]}" -ne 1 ]]; then
    echo "Exact-owned Memcached Docker inventory found ${#container_ids[@]} containers, want 1." >&2
    return 1
  fi
  inspect_exact_memcache_container "${container_ids[0]}" "${resource_id}"
}

assert_exact_memcache_container_binding() {
  local expected_id="${1:?Memcached container ID is required}"
  local resource_id="${2:?Memcached resource ID is required}"
  local expected_endpoint="${3:?Memcached endpoint is required}"
  local current_binding
  local current_id
  local current_endpoint
  current_binding="$(inspect_exact_memcache_container "${expected_id}" "${resource_id}")"
  IFS=$'\t' read -r current_id current_endpoint <<<"${current_binding}"
  if [[ "${current_id}" != "${expected_id}" ]]; then
    echo "Memcached immutable ID changed during protocol evidence." >&2
    return 1
  fi
  if [[ "${current_endpoint}" != "${expected_endpoint}" ]]; then
    echo "Memcached published endpoint changed during protocol evidence." >&2
    return 1
  fi
}

discover_sdk_memcache_endpoint() {
  local expected_endpoint="${1:?Exact-owned Memcached endpoint is required}"
  local response_file="${work_dir}/sdk-memcache-instance.json"
  run_bounded 10 curl --fail --silent --show-error --max-time 5 \
    "${gateway}/_minisky/memcache.googleapis.com/v1/${sdk_import}" \
    --output "${response_file}"
  run_bounded 10 python3 -c '
import ipaddress
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected_name = sys.argv[2]
expected_endpoint = sys.argv[3]
raw = path.read_bytes()
if not raw or len(raw) > 1 << 20:
    raise SystemExit("Memcached API response is empty or exceeds 1 MiB")
document = json.loads(raw)
if document.get("name") != expected_name or document.get("state") != "READY":
    raise SystemExit("Memcached API response does not identify the ready SDK instance")
nodes = document.get("memcacheNodes")
if not isinstance(nodes, list) or len(nodes) != 1:
    raise SystemExit("Memcached API response must expose exactly one node")
host = nodes[0].get("host")
port = nodes[0].get("port")
if not isinstance(host, str) or not isinstance(port, int) or isinstance(port, bool):
    raise SystemExit("Memcached node endpoint is malformed")
try:
    address = ipaddress.ip_address(host)
except ValueError as error:
    raise SystemExit("Memcached node host is not an IP literal") from error
if not ipaddress.ip_address(host).is_loopback or not 1 <= port <= 65535:
    raise SystemExit("Memcached node endpoint is not bounded to loopback")
api_node_endpoint = f"[{host}]:{port}" if address.version == 6 else f"{host}:{port}"
if document.get("discoveryEndpoint") != api_node_endpoint:
    raise SystemExit("Memcached discovery endpoint does not match its exact node")
if document.get("discoveryEndpoint") != expected_endpoint:
    raise SystemExit("Memcached API endpoint does not match exact-owned Docker endpoint")
print(api_node_endpoint)
' "${response_file}" "${sdk_import}" "${expected_endpoint}"
}

assert_memcache_protocol() {
  local endpoint="$1"
  local key="$2"
  local value="$3"
  run_bounded 10 python3 -c '
import ipaddress
import socket
import sys

endpoint, key, value = sys.argv[1:]
host, separator, port_text = endpoint.rpartition(":")
host = host.removeprefix("[").removesuffix("]")
if not separator or not host or not port_text.isascii() or not port_text.isdigit():
    raise SystemExit("Memcached endpoint is malformed")
try:
    address = ipaddress.ip_address(host)
    port = int(port_text)
except ValueError as error:
    raise SystemExit("Memcached endpoint is malformed") from error
if not address.is_loopback or not 1 <= port <= 65535:
    raise SystemExit("Memcached endpoint must be loopback with a valid port")
try:
    key_bytes = key.encode("ascii")
except UnicodeEncodeError as error:
    raise SystemExit("Memcached evidence key must be ASCII") from error
value_bytes = value.encode("utf-8")
if not key_bytes or len(key_bytes) > 250 or any(byte <= 32 or byte == 127 for byte in key_bytes):
    raise SystemExit("Memcached evidence key is outside protocol bounds")
if len(value_bytes) > 1024 or b"\r" in value_bytes or b"\n" in value_bytes:
    raise SystemExit("Memcached evidence value is outside protocol bounds")

def read_line(reader):
    line = reader.readline(513)
    if len(line) > 512 or not line.endswith(b"\r\n"):
        raise RuntimeError("Memcached returned an invalid or oversized response line")
    return line

def read_exact(reader, size):
    result = bytearray()
    while len(result) < size:
        chunk = reader.read(size - len(result))
        if not chunk:
            raise RuntimeError("Memcached closed before the bounded response completed")
        result.extend(chunk)
    return bytes(result)

with socket.create_connection((host, port), timeout=2) as connection:
    connection.settimeout(2)
    reader = connection.makefile("rb")
    size = str(len(value_bytes)).encode("ascii")
    connection.sendall(b"set " + key_bytes + b" 0 30 " + size + b"\r\n" + value_bytes + b"\r\n")
    if read_line(reader) != b"STORED\r\n":
        raise RuntimeError("Memcached SET did not return exact STORED")
    connection.sendall(b"get " + key_bytes + b"\r\n")
    if read_line(reader) != b"VALUE " + key_bytes + b" 0 " + size + b"\r\n":
        raise RuntimeError("Memcached GET returned an unexpected VALUE header")
    if read_exact(reader, len(value_bytes) + 2) != value_bytes + b"\r\n":
        raise RuntimeError("Memcached GET returned a different value")
    if read_line(reader) != b"END\r\n":
        raise RuntimeError("Memcached GET did not return exact END")
print(f"Memcached protocol set/get passed: endpoint={endpoint} key={key} bytes={len(value_bytes)}")
' "${endpoint}" "${key}" "${value}"
}

assert_no_memcache_container() {
  local resource_id="${1:?Memcached resource ID is required}"
  local inventory
  if ! inventory="$(run_bounded 10 docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=memorystore-memcached" \
    --filter "label=minisky.resource=${resource_id}" 2>&1)"; then
    echo "Memcached Docker inventory failed: ${inventory}" >&2
    return 1
  fi
  if [[ -n "${inventory}" ]]; then
    echo "Exact-owned Memcached container remains for ${resource_id}: ${inventory}" >&2
    return 1
  fi
}

start_minisky
run_sdk create
sdk_binding="$(discover_exact_memcache_container "${sdk_backend_id}")"
IFS=$'\t' read -r sdk_container_id sdk_endpoint <<<"${sdk_binding}"
discover_sdk_memcache_endpoint "${sdk_endpoint}" >/dev/null
assert_exact_memcache_container_binding "${sdk_container_id}" "${sdk_backend_id}" "${sdk_endpoint}"
assert_memcache_protocol "${sdk_endpoint}" "minisky-protocol-evidence" "memcached-data-plane-ok"
assert_exact_memcache_container_binding "${sdk_container_id}" "${sdk_backend_id}" "${sdk_endpoint}"
stop_minisky
start_minisky
run_sdk verify
run_sdk delete
assert_no_memcache_container "${sdk_backend_id}"

terraform_dir="${repository_root}/terraform/memcache"
terraform_state="${work_dir}/terraform.tfstate"
tf_instance_id="minisky-terraform-memcached"
tf_vars=(
  -var="minisky_endpoint=${gateway}"
  -var="project_id=${project}"
  -var="region=${location}"
  -var="instance_name=${tf_instance_id}"
)
tf_address='google_memcache_instance.compatibility'
tf_import="projects/${project}/locations/${location}/instances/${tf_instance_id}"
tf_backend_id="$(memcache_backend_id "${project}" "${location}" "${tf_instance_id}")"

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
assert_no_memcache_container "${tf_backend_id}"

echo "Memcached SDK protocol and Terraform apply/restart/import-normalization/no-drift/destroy lifecycle passed."
