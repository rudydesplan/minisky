#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE20_SDK_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 20 SDK integration without MINISKY_PHASE20_SDK_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock="${TMPDIR:-/tmp}/minisky-phase20-sdk-integration.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 20 SDK integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
state_root="${work}/state"
home="${work}/home"
profile="phase20-sdk-$$"
project="phase20-project-$$"
cluster_id="phase20-cluster-$$"
alloydb_instance_id="phase20-primary-$$"
filestore_instance_id="phase20-files-$$"
redis_instance_id="phase20-cache-$$"
source_bucket="phase20-source-$$"
sink_bucket="phase20-sink-$$"
evidence_file="${work}/generated-client-evidence.json"
postgres_image="postgres:15.8-bookworm@sha256:eb3747f5d0a92195ca486d2f15d9a4ee5e9461b0332fe87fbc59069490a5c659"
valkey_image="valkey/valkey:7.2.12-alpine@sha256:d0809f1d763f9fc3d77cd27e7c0393b1b0d69b6ad9269f932328b4793a620c78"
storage_image="fsouza/fake-gcs-server:latest@sha256:91afded49de804aa61b5f3eb6c7cd65205acf9e5c5e047cf0ba7d9507af806c8"
pid=""
pull_pid=""
started_at="${SECONDS}"
mkdir -p "${state_root}" "${home}"

owned_containers() {
  docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true
}

owned_volumes() {
  docker volume ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true
}

owned_networks() {
  docker network ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true
}

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
  done < <(owned_containers)
  while IFS= read -r volume; do
    [[ -n "${volume}" ]] && docker volume rm "${volume}" >/dev/null 2>&1 || true
  done < <(owned_volumes)
  while IFS= read -r network; do
    [[ -n "${network}" ]] && docker network rm "${network}" >/dev/null 2>&1 || true
  done < <(owned_networks)
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

pull_pinned_image() {
  local image=$1
  local log_file="${work}/image-pull.log"
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
      echo "Timed out pulling pinned image ${image} after 300s." >&2
      return 1
    fi
    sleep 1
  done
  if ! wait "${pull_pid}"; then
    echo "Failed to pull pinned image ${image}." >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' "${log_file}" >&2
    return 1
  fi
  pull_pid=""
}

assert_container_image() {
  local service=$1
  local expected=$2
  local containers container actual expected_id count
  containers="$(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=${service}")"
  count="$(printf '%s\n' "${containers}" | awk 'NF { count++ } END { print count+0 }')"
  if [[ "${count}" != "1" ]]; then
    echo "Expected one exact-owned ${service} container, found ${count}: ${containers}" >&2
    return 1
  fi
  container="${containers}"
  actual="$(docker inspect --format '{{.Image}}' "${container}")"
  expected_id="$(docker image inspect --format '{{.Id}}' "${expected}")"
  if [[ "${actual}" != "${expected_id}" ]]; then
    echo "${service} container image ${actual} does not match pinned ${expected} (${expected_id})." >&2
    return 1
  fi
}

assert_no_service_resources() {
  local service=$1
  local containers volumes
  containers="$(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=${service}")"
  volumes="$(docker volume ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=${service}")"
  if [[ -n "${containers}" || -n "${volumes}" ]]; then
    echo "Exact-owned ${service} cleanup incomplete; containers=${containers} volumes=${volumes}" >&2
    return 1
  fi
}

preflight_shared_resources() {
  if docker network inspect minisky-net >/dev/null 2>&1; then
    owner="$(docker network inspect --format '{{ index .Labels "minisky.profile" }}' minisky-net)"
    echo "Refusing live Phase 20 run: shared network minisky-net is owned by profile ${owner:-unknown}." >&2
    return 1
  fi
}

docker info >/dev/null
pull_pinned_image "${postgres_image}"
pull_pinned_image "${valkey_image}"
pull_pinned_image "${storage_image}"

api_port="$(free_port)"
ui_port="$(free_port)"
gateway="http://127.0.0.1:${api_port}"
go build -trimpath -o "${work}/minisky" ./cmd/minisky
preflight_shared_resources

start_daemon() {
  local log_file=$1
  HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
    MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
    "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  pid=$!
  for _ in {1..120}; do
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

print_owned_diagnostics() {
  local container
  while IFS= read -r container; do
    [[ -z "${container}" ]] && continue
    docker inspect --format \
      'container={{.Name}} status={{.State.Status}} error={{json .State.Error}} image={{.Image}} labels={{json .Config.Labels}} mounts={{json .Mounts}} ports={{json .NetworkSettings.Ports}}' \
      "${container}" >&2 || true
    docker logs --tail 80 "${container}" >&2 || true
  done < <(owned_containers)
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}" || true
  fi
  pid=""
}

export MINISKY_ENDPOINT="${gateway}"
export MINISKY_PROJECT_ID="${project}"
export MINISKY_PHASE20_LOCATION="us-central1"
export MINISKY_PHASE20_CLUSTER_ID="${cluster_id}"
export MINISKY_PHASE20_ALLOYDB_INSTANCE_ID="${alloydb_instance_id}"
export MINISKY_PHASE20_FILESTORE_INSTANCE_ID="${filestore_instance_id}"
export MINISKY_PHASE20_REDIS_INSTANCE_ID="${redis_instance_id}"
export MINISKY_PHASE20_SOURCE_BUCKET="${source_bucket}"
export MINISKY_PHASE20_SINK_BUCKET="${sink_bucket}"
export MINISKY_PHASE20_POSTGRES_IMAGE="${postgres_image}"
export MINISKY_PHASE20_VALKEY_IMAGE="${valkey_image}"
export MINISKY_PHASE20_EVIDENCE="${evidence_file}"
export MINISKY_PHASE20_OPT_IN=1

start_daemon "${work}/minisky-create.log"
if ! MINISKY_PHASE20_MODE=boundary go run ./sdk-smoke/phase20; then
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' \
    "${work}/minisky-create.log" >&2
  print_owned_diagnostics
  exit 1
fi
if ! MINISKY_PHASE20_MODE=create go run ./sdk-smoke/phase20; then
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' \
    "${work}/minisky-create.log" >&2
  print_owned_diagnostics
  exit 1
fi
assert_container_image "alloydb" "${postgres_image}"
assert_container_image "memorystore-redis" "${valkey_image}"
assert_container_image "storage.googleapis.com" "${storage_image}"
stop_daemon

start_daemon "${work}/minisky-restarted.log"
if ! MINISKY_PHASE20_MODE=verify go run ./sdk-smoke/phase20; then
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' \
    "${work}/minisky-restarted.log" >&2
  exit 1
fi
assert_container_image "alloydb" "${postgres_image}"
assert_container_image "memorystore-redis" "${valkey_image}"
if ! MINISKY_PHASE20_MODE=delete go run ./sdk-smoke/phase20; then
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' \
    "${work}/minisky-restarted.log" >&2
  exit 1
fi
assert_no_service_resources "alloydb"
assert_no_service_resources "memorystore-redis"
stop_daemon
while IFS= read -r network; do
  [[ -n "${network}" ]] && docker network rm "${network}" >/dev/null
done < <(owned_networks)

remaining_containers="$(owned_containers)"
remaining_volumes="$(owned_volumes)"
remaining_networks="$(owned_networks)"
if [[ -n "${remaining_containers}" || -n "${remaining_volumes}" || -n "${remaining_networks}" ]]; then
  echo "Exact-owned final cleanup incomplete; containers=${remaining_containers} volumes=${remaining_volumes} networks=${remaining_networks}" >&2
  exit 1
fi

duration=$((SECONDS - started_at))
echo "Phase 20 generated Go client integration passed in ${duration}s."
