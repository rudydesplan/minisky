#!/usr/bin/env bash

set -Eeuo pipefail

lock_host="$(uname -n)"
lock_token="$$-${RANDOM}-${RANDOM}"
lock_owner_pid=""
lock_owner_host=""
lock_owner_token=""

read_lock_owner() {
  local lock=$1
  local key
  local value
  lock_owner_pid=""
  lock_owner_host=""
  lock_owner_token=""
  [[ -f "${lock}/owner" ]] || return 1
  while IFS='=' read -r key value; do
    case "${key}" in
      pid) lock_owner_pid="${value}" ;;
      host) lock_owner_host="${value}" ;;
      token) lock_owner_token="${value}" ;;
    esac
  done <"${lock}/owner"
  [[ "${lock_owner_pid}" =~ ^[0-9]+$ && -n "${lock_owner_host}" && -n "${lock_owner_token}" ]]
}

write_lock_owner() {
  local lock=$1
  local owner_pid=$2
  local owner_host=$3
  local owner_token=$4
  local temporary="${lock}/owner.tmp.$$"
  (
    umask 077
    printf 'pid=%s\nhost=%s\ntoken=%s\n' \
      "${owner_pid}" "${owner_host}" "${owner_token}" >"${temporary}"
  )
  mv "${temporary}" "${lock}/owner"
}

pid_is_alive() {
  python3 - "$1" <<'PY'
import os
import sys

try:
    os.kill(int(sys.argv[1]), 0)
except ProcessLookupError:
    raise SystemExit(1)
except PermissionError:
    raise SystemExit(0)
else:
    raise SystemExit(0)
PY
}

restore_quarantined_lock() {
  local lock=$1
  local quarantine=$2
  if [[ ! -e "${lock}" ]]; then
    mv "${quarantine}" "${lock}"
  else
    echo "Lock ownership changed during stale recovery; preserved ${quarantine} for inspection." >&2
  fi
}

acquire_lock() {
  local lock=$1
  local description=$2
  local attempt
  local observed_pid
  local observed_host
  local observed_token
  local quarantine
  for attempt in 1 2 3; do
    if mkdir "${lock}" 2>/dev/null; then
      if ! write_lock_owner "${lock}" "$$" "${lock_host}" "${lock_token}"; then
        rm -rf "${lock}"
        return 1
      fi
      return 0
    fi
    if ! read_lock_owner "${lock}"; then
      echo "${description} lock has no complete owner metadata (${lock})." >&2
      return 1
    fi
    observed_pid="${lock_owner_pid}"
    observed_host="${lock_owner_host}"
    observed_token="${lock_owner_token}"
    if [[ "${observed_host}" != "${lock_host}" ]]; then
      echo "${description} lock belongs to host ${observed_host} (${lock})." >&2
      return 1
    fi
    if pid_is_alive "${observed_pid}"; then
      echo "${description} lock is held by live PID ${observed_pid} on ${observed_host} (${lock})." >&2
      return 1
    fi
    quarantine="${lock}.stale.${observed_pid}.$$.$RANDOM"
    if ! mv "${lock}" "${quarantine}" 2>/dev/null; then
      continue
    fi
    if ! read_lock_owner "${quarantine}" ||
      [[ "${lock_owner_pid}" != "${observed_pid}" ||
        "${lock_owner_host}" != "${observed_host}" ||
        "${lock_owner_token}" != "${observed_token}" ]]; then
      restore_quarantined_lock "${lock}" "${quarantine}"
      return 1
    fi
    if pid_is_alive "${lock_owner_pid}"; then
      restore_quarantined_lock "${lock}" "${quarantine}"
      echo "${description} lock owner became live during stale recovery." >&2
      return 1
    fi
    rm -rf "${quarantine}"
  done
  echo "Could not acquire ${description} lock after stale-owner recovery (${lock})." >&2
  return 1
}

release_lock() {
  local lock=$1
  [[ -d "${lock}" ]] || return 0
  if ! read_lock_owner "${lock}" ||
    [[ "${lock_owner_pid}" != "$$" ||
      "${lock_owner_host}" != "${lock_host}" ||
      "${lock_owner_token}" != "${lock_token}" ]]; then
    echo "Refusing to release a lock not owned by this process (${lock})." >&2
    return 1
  fi
  rm -rf "${lock}"
}

select_subnetwork_cidr() {
  local inspection=$1
  python3 - "${inspection}" <<'PY'
import ipaddress
import json
import sys

existing = []
for network in json.load(open(sys.argv[1], encoding="utf-8")):
    for config in (network.get("IPAM") or {}).get("Config") or []:
        subnet = config.get("Subnet")
        if not subnet:
            continue
        try:
            existing.append(ipaddress.ip_network(subnet, strict=False))
        except ValueError:
            pass
for private in (
    ipaddress.ip_network("172.28.0.0/14"),
    ipaddress.ip_network("10.240.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("10.0.0.0/8"),
):
    for candidate in private.subnets(new_prefix=24):
        if not any(candidate.overlaps(current) for current in existing if current.version == 4):
            print(candidate)
            raise SystemExit(0)
raise SystemExit("no non-overlapping private IPv4 /24 is available")
PY
}

run_self_test() {
  command -v python3 >/dev/null 2>&1 || {
    echo "Required command not found: python3" >&2
    return 1
  }
  local root
  local lock
  local original_token="${lock_token}"
  local cidr_fixture
  root="$(mktemp -d)"
  lock="${root}/gate.lock"
  cidr_fixture="${root}/networks.json"
  trap "rm -rf '${root}'" EXIT INT TERM

  acquire_lock "${lock}" "self-test"
  if acquire_lock "${lock}" "self-test contender" 2>/dev/null; then
    echo "Lock self-test acquired a lock held by a live PID." >&2
    return 1
  fi
  lock_token="not-the-owner"
  if release_lock "${lock}" 2>/dev/null; then
    echo "Lock self-test released a lock with the wrong token." >&2
    return 1
  fi
  lock_token="${original_token}"
  release_lock "${lock}"

  mkdir "${lock}"
  write_lock_owner "${lock}" "99999999" "${lock_host}" "stale-owner"
  acquire_lock "${lock}" "stale self-test"
  release_lock "${lock}"

  mkdir "${lock}"
  write_lock_owner "${lock}" "99999999" "other-host.invalid" "foreign-owner"
  if acquire_lock "${lock}" "foreign self-test" 2>/dev/null; then
    echo "Lock self-test reclaimed a foreign-host lock." >&2
    return 1
  fi
  rm -rf "${lock}"

  cat >"${cidr_fixture}" <<'JSON'
[
  {"Name":"minisky-net","IPAM":{"Config":[{"Subnet":"172.28.0.0/16"}]}},
  {"Name":"other","IPAM":{"Config":[{"Subnet":"10.240.0.0/16"}]}}
]
JSON
  if [[ "$(select_subnetwork_cidr "${cidr_fixture}")" != "172.29.0.0/24" ]]; then
    echo "CIDR self-test did not account for all existing Docker ranges." >&2
    return 1
  fi
  rm -rf "${root}"
  trap - EXIT INT TERM
  echo "Phase 16 subnetwork Terraform lock and CIDR self-test passed."
}

if [[ "${MINISKY_PHASE16_SUBNETWORK_TERRAFORM_SELF_TEST:-}" == "1" ]]; then
  run_self_test
  exit 0
fi

if [[ "${MINISKY_PHASE16_SUBNETWORK_TERRAFORM_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Phase 16 subnetwork Terraform integration without MINISKY_PHASE16_SUBNETWORK_TERRAFORM_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3 terraform; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done
if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is unavailable; Phase 16 subnetwork Terraform integration requires a real Docker daemon." >&2
  exit 1
fi

shared_lock="${TMPDIR:-/tmp}/minisky-net-integration.lock"
phase_lock="${TMPDIR:-/tmp}/minisky-phase16-subnetwork-terraform-integration.lock"
if ! acquire_lock "${shared_lock}" "shared MiniSky Docker integration"; then
  exit 1
fi
if ! acquire_lock "${phase_lock}" "Phase 16 subnetwork Terraform integration"; then
  release_lock "${shared_lock}" || true
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
profile="phase16-subnetwork-terraform-$$"
project="phase16-subnetwork-terraform-$$"
region="us-central1"
network_name="phase16-terraform-network"
subnetwork_name="phase16-terraform-subnetwork"
instance_name="phase16-terraform-instance"
zone="${region}-a"
canonical="projects/${project}/global/networks/${network_name}"
instance_canonical="projects/${project}/zones/${zone}/instances/${instance_name}"
peer_name="minisky-phase16-peer-$$"
pid=""
current_log=""
bridge_id=""
container_id=""
instance_ip=""
subnetwork_cidr=""
started_at="${SECONDS}"
mkdir -p "${home}" "${state_root}" "${tf_data_dir}"

bridge_name="$(python3 - "${profile}" "${canonical}" "${network_name}" <<'PY'
import hashlib
import sys

profile, canonical, network = sys.argv[1:]
suffix = hashlib.sha256((profile + "\0" + canonical).encode()).hexdigest()[:16]
prefix = "minisky-vpc-"
readable = network[:63 - len(prefix) - 1 - len(suffix)].rstrip("-")
name = prefix + readable + "-" + suffix
if len(name) > 63:
    raise SystemExit("hashed Docker bridge name exceeds 63 characters")
print(name)
PY
)"

container_name="$(python3 - "${profile}" "${instance_canonical}" "${instance_name}" <<'PY'
import hashlib
import sys

profile, canonical, instance = sys.argv[1:]
suffix = hashlib.sha256((profile + "\0" + canonical).encode()).hexdigest()[:16]
prefix = "minisky-vm-"
readable = instance[:63 - len(prefix) - 1 - len(suffix)].rstrip("-")
print(prefix + readable + "-" + suffix)
PY
)"

print_current_log() {
  if [[ -n "${current_log}" && -f "${current_log}" ]]; then
    echo "MiniSky Phase 16 subnetwork Terraform daemon log (${current_log}):" >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"))' \
      "${current_log}" >&2
  fi
}

inspect_exact_bridge() {
  local reference=$1
  local expected_id=${2:-}
  local output=$3
  docker network inspect "${reference}" >"${output}"
  python3 - "${output}" "${bridge_name}" "${expected_id}" "${subnetwork_cidr}" \
    "${profile}" "${project}" "${network_name}" "${canonical}" <<'PY'
import json
import sys

path, expected_name, expected_id, expected_cidr, profile, project, network, canonical = sys.argv[1:]
values = json.load(open(path, encoding="utf-8"))
if len(values) != 1:
    raise SystemExit("expected exactly one Docker network inspection")
value = values[0]
if value.get("Name") != expected_name:
    raise SystemExit(f"bridge name={value.get('Name')!r} want={expected_name!r}")
if not value.get("Id"):
    raise SystemExit("bridge has no immutable Docker ID")
if expected_id and value["Id"] != expected_id:
    raise SystemExit(f"bridge ID churned: got={value['Id']} want={expected_id}")
if value.get("Driver") != "bridge":
    raise SystemExit(f"bridge driver={value.get('Driver')!r} want='bridge'")
configs = (value.get("IPAM") or {}).get("Config") or []
if len(configs) != 1 or configs[0].get("Subnet") != expected_cidr:
    raise SystemExit(f"bridge IPAM={configs!r} want one subnet {expected_cidr!r}")
expected_labels = {
    "managed-by": "minisky",
    "minisky.profile": profile,
    "minisky.service": "compute-network",
    "minisky.project": project,
    "minisky.network": network,
    "minisky.canonical-resource": canonical,
}
if (value.get("Labels") or {}) != expected_labels:
    raise SystemExit(f"bridge labels={value.get('Labels')!r} want={expected_labels!r}")
print(value["Id"])
PY
}

inspect_exact_instance() {
  local reference=$1
  local expected_id=${2:-}
  local expected_ip=${3:-}
  local output=$4
  docker inspect "${reference}" >"${output}"
  python3 - "${output}" "${container_name}" "${expected_id}" "${expected_ip}" \
    "${profile}" "${project}" "${zone}" "${instance_name}" "${instance_canonical}" \
    "${bridge_name}" "${bridge_id}" "${subnetwork_cidr}" <<'PY'
import ipaddress
import json
import sys

(path, expected_name, expected_id, expected_ip, profile, project, zone, instance,
 canonical, bridge_name, bridge_id, cidr) = sys.argv[1:]
values = json.load(open(path, encoding="utf-8"))
if len(values) != 1:
    raise SystemExit("expected exactly one Docker container inspection")
value = values[0]
if value.get("Name", "").lstrip("/") != expected_name:
    raise SystemExit(f"container name={value.get('Name')!r} want={expected_name!r}")
if not value.get("Id"):
    raise SystemExit("Compute container has no immutable Docker ID")
if expected_id and value["Id"] != expected_id:
    raise SystemExit(f"Compute container ID churned: got={value['Id']} want={expected_id}")
expected_labels = {
    "managed-by": "minisky",
    "minisky.profile": profile,
    "minisky.service": "compute-instance",
    "minisky.project": project,
    "minisky.zone": zone,
    "minisky.instance": instance,
    "minisky.canonical-resource": canonical,
}
labels = (value.get("Config") or {}).get("Labels") or {}
if any(labels.get(key) != expected for key, expected in expected_labels.items()):
    raise SystemExit(f"Compute ownership labels={labels!r} want at least {expected_labels!r}")
networks = (value.get("NetworkSettings") or {}).get("Networks") or {}
if list(networks) != [bridge_name]:
    raise SystemExit(f"Compute networks={list(networks)!r} want only {bridge_name!r}")
endpoint = networks[bridge_name]
if endpoint.get("NetworkID") != bridge_id:
    raise SystemExit(f"Compute endpoint bridge ID={endpoint.get('NetworkID')!r} want={bridge_id!r}")
address = endpoint.get("IPAddress", "")
if ipaddress.ip_address(address) not in ipaddress.ip_network(cidr):
    raise SystemExit(f"Compute IPv4={address!r} is outside {cidr!r}")
if expected_ip and address != expected_ip:
    raise SystemExit(f"Compute IPv4 churned: got={address!r} want={expected_ip!r}")
print(value["Id"], address)
PY
}

capture_bridge_if_exact() {
  local inspection="${work}/trap-bridge-inspect.json"
  local captured
  if captured="$(inspect_exact_bridge "${bridge_name}" "" "${inspection}" 2>/dev/null)"; then
    bridge_id="${captured}"
  fi
}

remove_captured_bridge_if_exact() {
  local inspection="${work}/cleanup-bridge-inspect.json"
  [[ -n "${bridge_id}" ]] || return 0
  if inspect_exact_bridge "${bridge_id}" "${bridge_id}" "${inspection}" >/dev/null 2>&1; then
    docker network rm "${bridge_id}" >/dev/null 2>&1 || return 1
  fi
}

remove_owned_minisky_net() {
  local inspection="${work}/cleanup-minisky-net.json"
  local id
  if ! docker network inspect minisky-net >"${inspection}" 2>/dev/null; then
    return 0
  fi
  id="$(python3 - "${inspection}" "${profile}" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], encoding="utf-8"))[0]
labels = value.get("Labels") or {}
if labels == {"managed-by": "minisky", "minisky.profile": sys.argv[2]}:
    print(value.get("Id", ""))
PY
)"
  if [[ -n "${id}" ]]; then
    docker network rm "${id}" >/dev/null 2>&1 || return 1
  fi
}

remove_owned_containers() {
  local containers="${work}/owned-containers.txt"
  local container
  local failed=0
  if ! docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" >"${containers}"; then
    return 1
  fi
  while IFS= read -r container; do
    if [[ -n "${container}" ]] && ! docker rm -f "${container}" >/dev/null 2>&1; then
      failed=1
    fi
  done <"${containers}"
  if docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" | python3 -c 'import sys; raise SystemExit(1 if sys.stdin.read().strip() else 0)'; then
    return "${failed}"
  fi
  return 1
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT INT TERM
  if (( status != 0 )); then
    print_current_log
  fi
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  capture_bridge_if_exact
  remove_owned_containers || cleanup_failed=1
  remove_captured_bridge_if_exact || cleanup_failed=1
  remove_owned_minisky_net || cleanup_failed=1
  rm -rf "${work}"
  release_lock "${phase_lock}" || cleanup_failed=1
  release_lock "${shared_lock}" || cleanup_failed=1
  if (( status == 0 && cleanup_failed != 0 )); then
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

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

assert_single_canonical_bridge() {
  local ids_file="${work}/canonical-network-ids.txt"
  local inspections="${work}/canonical-networks.json"
  docker network ls -q \
    --filter "label=minisky.canonical-resource=${canonical}" >"${ids_file}"
  python3 - "${ids_file}" <<'PY'
import pathlib
import sys

ids = [line for line in pathlib.Path(sys.argv[1]).read_text().splitlines() if line]
if len(ids) != 1:
    raise SystemExit(f"canonical label matched {len(ids)} Docker networks, want exactly one")
PY
  docker network inspect $(python3 -c 'import pathlib,sys; print(" ".join(pathlib.Path(sys.argv[1]).read_text().split()))' "${ids_file}") \
    >"${inspections}"
  python3 - "${inspections}" "${bridge_id}" "${bridge_name}" <<'PY'
import json
import sys

values = json.load(open(sys.argv[1], encoding="utf-8"))
if len(values) != 1 or values[0].get("Id") != sys.argv[2] or values[0].get("Name") != sys.argv[3]:
    raise SystemExit("canonical label resolved to an unexpected Docker network")
PY
}

free_tcp_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

bind_collision() {
  python3 - "${current_log}" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text(errors="replace").lower()
raise SystemExit(0 if "address already in use" in text or "bind:" in text else 1)
PY
}

start_daemon() {
  local label=$1
  local attempt
  local api_port
  local ui_port
  for attempt in {1..3}; do
    api_port="$(free_tcp_port)"
    ui_port="$(free_tcp_port)"
    while [[ "${ui_port}" == "${api_port}" ]]; do
      ui_port="$(free_tcp_port)"
    done
    gateway="http://127.0.0.1:${api_port}"
    current_log="${work}/${label}-attempt-${attempt}.log"
    HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
      MINISKY_PHASE16_SUBNETWORK_INTEGRATION=1 \
      "${work}/minisky" start --services compute --port "${api_port}" --ui-port "${ui_port}" \
      >"${current_log}" 2>&1 &
    pid=$!
    for _ in {1..80}; do
      if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
        return 0
      fi
      if bind_collision; then
        kill -TERM "${pid}" 2>/dev/null || true
        wait "${pid}" 2>/dev/null || true
        pid=""
        break
      fi
      if ! kill -0 "${pid}" 2>/dev/null; then
        print_current_log
        return 1
      fi
      sleep 0.25
    done
    if [[ -n "${pid}" ]]; then
      echo "Timed out waiting for MiniSky readiness." >&2
      print_current_log
      return 1
    fi
    echo "Retrying MiniSky start after loopback bind collision (${attempt}/3)." >&2
  done
  echo "MiniSky could not bind dynamic API/UI ports after 3 attempts." >&2
  return 1
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  pid=""
}

assert_bridge_absent() {
  local canonical_matches="${work}/absent-canonical-networks.txt"
  local profile_matches="${work}/absent-profile-networks.txt"
  if [[ -n "${bridge_id}" ]] && docker network inspect "${bridge_id}" >/dev/null 2>&1; then
    echo "Expected Docker bridge ID ${bridge_id} to be absent." >&2
    return 1
  fi
  if docker network inspect "${bridge_name}" >/dev/null 2>&1; then
    echo "Expected Docker bridge ${bridge_name} to be absent." >&2
    return 1
  fi
  docker network ls -q \
    --filter "label=minisky.canonical-resource=${canonical}" >"${canonical_matches}"
  if [[ -s "${canonical_matches}" ]]; then
    echo "Expected no Docker bridge with canonical resource ${canonical}." >&2
    return 1
  fi
  docker network ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=compute-network" >"${profile_matches}"
  if [[ -s "${profile_matches}" ]]; then
    echo "Expected no profile-owned Compute network bridges after destroy." >&2
    return 1
  fi
}

start_owned_test_peer() {
  docker run --detach \
    --name "${peer_name}" \
    --network "${bridge_id}" \
    --label managed-by=minisky \
    --label "minisky.profile=${profile}" \
    --label minisky.service=compute-instance-acceptance-peer \
    --label "minisky.resource=${instance_canonical}" \
    nginx:1.27-alpine >/dev/null
}

assert_peer_traffic() {
  local response
  response="$(docker exec "${peer_name}" wget -qO- "http://${instance_ip}/")"
  if [[ "${response}" != *"Welcome to nginx!"* ]]; then
    echo "Owned bridge peer did not receive the Compute instance HTTP response." >&2
    return 1
  fi
}

remove_owned_test_peer() {
  local inspection="${work}/peer-cleanup.json"
  local id
  if ! docker inspect "${peer_name}" >"${inspection}" 2>/dev/null; then
    return 0
  fi
  id="$(python3 - "${inspection}" "${profile}" "${instance_canonical}" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], encoding="utf-8"))[0]
expected = {
    "managed-by": "minisky",
    "minisky.profile": sys.argv[2],
    "minisky.service": "compute-instance-acceptance-peer",
    "minisky.resource": sys.argv[3],
}
labels = (value.get("Config") or {}).get("Labels") or {}
if all(labels.get(key) == expected_value for key, expected_value in expected.items()):
    print(value.get("Id", ""))
PY
)"
  if [[ -z "${id}" ]]; then
    echo "Refusing to remove a test peer without exact ownership labels." >&2
    return 1
  fi
  docker rm -f "${id}" >/dev/null
}

assert_instance_absent() {
  if [[ -n "${container_id}" ]] && docker inspect "${container_id}" >/dev/null 2>&1; then
    echo "Expected Compute container ID ${container_id} to be absent." >&2
    return 1
  fi
  if docker inspect "${container_name}" >/dev/null 2>&1; then
    echo "Expected Compute container ${container_name} to be absent." >&2
    return 1
  fi
}

assert_api_resources() {
  local network_response="${work}/network-response.json"
  local subnetwork_response="${work}/subnetwork-response.json"
  local instance_response="${work}/instance-response.json"
  local network_url="${gateway}/_minisky/compute/compute/v1/projects/${project}/global/networks/${network_name}"
  local subnetwork_url="${gateway}/_minisky/compute/compute/v1/projects/${project}/regions/${region}/subnetworks/${subnetwork_name}"
  local instance_url="${gateway}/_minisky/compute/compute/v1/projects/${project}/zones/${zone}/instances/${instance_name}"
  curl --globoff --fail --silent --show-error "${network_url}" >"${network_response}"
  curl --globoff --fail --silent --show-error "${subnetwork_url}" >"${subnetwork_response}"
  curl --globoff --fail --silent --show-error "${instance_url}" >"${instance_response}"
  python3 - "${network_response}" "${subnetwork_response}" "${instance_response}" "${project}" "${region}" \
    "${zone}" "${network_name}" "${subnetwork_name}" "${instance_name}" "${subnetwork_cidr}" <<'PY'
import ipaddress
import json
import sys
from datetime import datetime

network_path, subnetwork_path, instance_path, project, region, zone, network_name, subnetwork_name, instance_name, cidr = sys.argv[1:]
network = json.load(open(network_path, encoding="utf-8"))
subnetwork = json.load(open(subnetwork_path, encoding="utf-8"))
instance = json.load(open(instance_path, encoding="utf-8"))
network_link = f"https://www.googleapis.com/compute/v1/projects/{project}/global/networks/{network_name}"
region_link = f"https://www.googleapis.com/compute/v1/projects/{project}/regions/{region}"
network_expected = {
    "kind": "compute#network",
    "name": network_name,
    "selfLink": network_link,
    "autoCreateSubnetworks": False,
}
for key, expected in network_expected.items():
    if network.get(key) != expected:
        raise SystemExit(f"network {key}={network.get(key)!r} want={expected!r}")
subnetwork_expected = {
    "kind": "compute#subnetwork",
    "name": subnetwork_name,
    "ipCidrRange": cidr,
    "network": network_link,
    "region": region_link,
    "selfLink": f"{region_link}/subnetworks/{subnetwork_name}",
    "gatewayAddress": str(next(ipaddress.ip_network(cidr).hosts())),
    "privateIpGoogleAccess": False,
    "purpose": "PRIVATE",
    "stackType": "IPV4_ONLY",
    "state": "READY",
}
for key, expected in subnetwork_expected.items():
    if subnetwork.get(key) != expected:
        raise SystemExit(f"subnetwork {key}={subnetwork.get(key)!r} want={expected!r}")
for resource_name, resource in (("network", network), ("subnetwork", subnetwork)):
    if not str(resource.get("id", "")).isdigit() or int(resource["id"]) <= 0:
        raise SystemExit(f"{resource_name} id is not a positive numeric ID")
    try:
        datetime.fromisoformat(resource["creationTimestamp"].replace("Z", "+00:00"))
    except (KeyError, ValueError):
        raise SystemExit(f"{resource_name} creationTimestamp is not RFC3339")
if not subnetwork.get("fingerprint"):
    raise SystemExit("subnetwork fingerprint is empty")
interfaces = instance.get("networkInterfaces") or []
if instance.get("name") != instance_name or instance.get("status") != "RUNNING" or len(interfaces) != 1:
    raise SystemExit(f"unexpected Compute instance={instance!r}")
interface = interfaces[0]
expected_interface = {
    "kind": "compute#networkInterface",
    "name": "nic0",
    "network": network_link,
    "subnetwork": f"{region_link}/subnetworks/{subnetwork_name}",
}
for key, expected in expected_interface.items():
    if interface.get(key) != expected:
        raise SystemExit(f"instance network interface {key}={interface.get(key)!r} want={expected!r}")
address = ipaddress.ip_address(interface.get("networkIP", ""))
if address not in ipaddress.ip_network(cidr):
    raise SystemExit(f"instance networkIP={address} is outside {cidr}")
if interface.get("accessConfigs"):
    raise SystemExit("bounded Compute instance unexpectedly reports a NAT access config")
print(address)
PY
}

assert_api_missing() {
  local url
  local status
  for url in \
    "${gateway}/_minisky/compute/compute/v1/projects/${project}/zones/${zone}/instances/${instance_name}" \
    "${gateway}/_minisky/compute/compute/v1/projects/${project}/regions/${region}/subnetworks/${subnetwork_name}" \
    "${gateway}/_minisky/compute/compute/v1/projects/${project}/global/networks/${network_name}"; do
    status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' "${url}")"
    if [[ "${status}" != "404" ]]; then
      echo "Expected destroyed resource ${url} to return HTTP 404, received ${status}." >&2
      return 1
    fi
  done
}

assert_targeted_plan_clean() {
  local plan_exit
  set +e
  terraform -chdir="${terraform_dir}" plan \
    -detailed-exitcode \
    -input=false \
    -target='google_compute_network.phase16[0]' \
    -target='google_compute_subnetwork.phase16[0]' \
    -target='google_compute_instance.phase16[0]' \
    "${tf_vars[@]}"
  plan_exit=$?
  set -e
  if [[ "${plan_exit}" != "0" ]]; then
    echo "Expected a targeted no-drift plan (exit 0), received exit ${plan_exit}." >&2
    return "${plan_exit}"
  fi
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky

export HOME="${home}"
export TF_DATA_DIR="${tf_data_dir}"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT

start_daemon "first"
docker network inspect $(docker network ls -q) >"${work}/existing-networks.json"
subnetwork_cidr="$(select_subnetwork_cidr "${work}/existing-networks.json")"

tf_vars=(
  -var="enable_phase16_compute_instance=true"
  -var="enable_phase16_network_resources=true"
  -var="phase16_instance_name=${instance_name}"
  -var="phase16_instance_zone=${zone}"
  -var="minisky_endpoint=${gateway}"
  -var="phase16_network_name=${network_name}"
  -var="phase16_subnetwork_cidr=${subnetwork_cidr}"
  -var="phase16_subnetwork_name=${subnetwork_name}"
  -var="profile=local"
  -var="project_id=${project}"
  -var="region=${region}"
)

terraform -chdir="${terraform_dir}" init \
  -backend-config="path=${tf_state}" \
  -input=false \
  -lockfile=readonly
terraform -chdir="${terraform_dir}" validate
terraform -chdir="${terraform_dir}" apply \
  -auto-approve \
  -input=false \
  -target='google_compute_network.phase16[0]' \
  -target='google_compute_subnetwork.phase16[0]' \
  -target='google_compute_instance.phase16[0]' \
  "${tf_vars[@]}"

bridge_id="$(inspect_exact_bridge "${bridge_name}" "" "${work}/apply-bridge.json")"
instance_ip="$(assert_api_resources)"
read -r container_id inspected_ip < <(
  inspect_exact_instance "${container_name}" "" "${instance_ip}" "${work}/apply-instance.json"
)
if [[ "${inspected_ip}" != "${instance_ip}" ]]; then
  echo "API and Docker primary IPv4 addresses diverged." >&2
  exit 1
fi
start_owned_test_peer
assert_peer_traffic
assert_single_canonical_bridge
assert_targeted_plan_clean
stop_daemon

start_daemon "restarted"
tf_vars[4]="-var=minisky_endpoint=${gateway}"
assert_targeted_plan_clean
restarted_ip="$(assert_api_resources)"
if [[ "${restarted_ip}" != "${instance_ip}" ]]; then
  echo "Primary IPv4 address churned across MiniSky restart." >&2
  exit 1
fi
inspect_exact_bridge "${bridge_id}" "${bridge_id}" "${work}/restart-bridge.json" >/dev/null
inspect_exact_instance "${container_id}" "${container_id}" "${instance_ip}" "${work}/restart-instance.json" >/dev/null
assert_peer_traffic
assert_single_canonical_bridge

terraform -chdir="${terraform_dir}" state rm \
  -backup="${work}/state-before-instance-rm.backup" \
  'google_compute_instance.phase16[0]'
terraform -chdir="${terraform_dir}" state rm \
  -backup="${work}/state-before-subnetwork-rm.backup" \
  'google_compute_subnetwork.phase16[0]'
terraform -chdir="${terraform_dir}" state rm \
  -backup="${work}/state-before-network-rm.backup" \
  'google_compute_network.phase16[0]'
terraform -chdir="${terraform_dir}" import \
  -backup="${work}/state-before-network-import.backup" \
  -input=false \
  "${tf_vars[@]}" \
  'google_compute_network.phase16[0]' \
  "projects/${project}/global/networks/${network_name}"
terraform -chdir="${terraform_dir}" import \
  -backup="${work}/state-before-subnetwork-import.backup" \
  -input=false \
  "${tf_vars[@]}" \
  'google_compute_subnetwork.phase16[0]' \
  "projects/${project}/regions/${region}/subnetworks/${subnetwork_name}"
terraform -chdir="${terraform_dir}" import \
  -backup="${work}/state-before-instance-import.backup" \
  -input=false \
  "${tf_vars[@]}" \
  'google_compute_instance.phase16[0]' \
  "projects/${project}/zones/${zone}/instances/${instance_name}"
# The provider can import and refresh this bounded ID, but Compute GET cannot
# reconstruct the create-only boot_disk.initialize_params.image value. A
# post-import plan therefore proposes replacement and is not claimed drift-free.
stop_daemon

start_daemon "import-restarted"
tf_vars[4]="-var=minisky_endpoint=${gateway}"
imported_ip="$(assert_api_resources)"
if [[ "${imported_ip}" != "${instance_ip}" ]]; then
  echo "Primary IPv4 address churned across import restart." >&2
  exit 1
fi
inspect_exact_bridge "${bridge_id}" "${bridge_id}" "${work}/import-restart-bridge.json" >/dev/null
inspect_exact_instance "${container_id}" "${container_id}" "${instance_ip}" "${work}/import-restart-instance.json" >/dev/null
assert_peer_traffic
assert_single_canonical_bridge

terraform -chdir="${terraform_dir}" destroy \
  -auto-approve \
  -input=false \
  -target='google_compute_instance.phase16[0]' \
  "${tf_vars[@]}"
assert_instance_absent
remove_owned_test_peer
terraform -chdir="${terraform_dir}" destroy \
  -auto-approve \
  -input=false \
  -target='google_compute_subnetwork.phase16[0]' \
  "${tf_vars[@]}"
terraform -chdir="${terraform_dir}" destroy \
  -auto-approve \
  -input=false \
  -target='google_compute_network.phase16[0]' \
  "${tf_vars[@]}"
assert_api_missing
assert_bridge_absent
stop_daemon

start_daemon "cleanup-restarted"
tf_vars[4]="-var=minisky_endpoint=${gateway}"
assert_api_missing
assert_instance_absent
assert_bridge_absent
stop_daemon

duration=$((SECONDS - started_at))
if (( duration > 1200 )); then
  echo "Phase 16 subnetwork Terraform integration exceeded its 20 minute budget (${duration}s)." >&2
  exit 1
fi
echo "Phase 16 Terraform subnetwork/IPAM Docker restart integration passed in ${duration}s."
