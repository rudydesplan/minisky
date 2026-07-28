#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

kafka_image="apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f"
if [[ "${1:-}" == "--print-required-images" && "$#" -eq 1 ]]; then
  printf '%s\n' "${kafka_image}"
  exit 0
fi
[[ "$#" -eq 0 ]] || { echo "Usage: $0 [--print-required-images]" >&2; exit 2; }

if [[ "${MINISKY_PHASE19_MANAGED_KAFKA_TERRAFORM_INTEGRATION:-}" != "1" ||
      "${MINISKY_PHASE19_DOCKER_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Managed Kafka Terraform integration without both explicit Terraform and heavy Docker opt-ins." >&2
  exit 2
fi
for command in curl docker go python3 terraform; do
  command -v "${command}" >/dev/null 2>&1 || { echo "Required command not found: ${command}" >&2; exit 1; }
done
docker info >/dev/null
docker image inspect "${kafka_image}" >/dev/null

shared_lock="${TMPDIR:-/tmp}/minisky-net-integration.lock"
phase_lock="${TMPDIR:-/tmp}/minisky-phase19-managed-kafka-terraform-integration.lock"
mkdir "${shared_lock}" 2>/dev/null || { echo "Another MiniSky Docker integration is active." >&2; exit 1; }
mkdir "${phase_lock}" 2>/dev/null || { rmdir "${shared_lock}"; echo "Another Managed Kafka Terraform gate is active." >&2; exit 1; }

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="${root}/terraform"
work="$(mktemp -d)"
home="${work}/home"
state_root="${work}/state"
tf_data="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
volumes="${work}/volumes"
profile="phase19-kafka-tf-$$"
project="phase19-kafka-tf-$$"
region="us-central1"
cluster_id="phase19-terraform-kafka"
canonical="projects/${project}/locations/${region}/clusters/${cluster_id}"
pid=""
watchdog_pid=""
gateway=""
mkdir -p "${home}" "${state_root}" "${tf_data}"
: >"${volumes}"

capture_volumes() {
  docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{println .Name}}{{end}}{{end}}' "$1" >>"${volumes}"
}
remove_owned() {
  local container volume
  while IFS= read -r container; do
    [[ -n "${container}" ]] && { capture_volumes "${container}" || true; docker rm -f -v "${container}" >/dev/null 2>&1 || true; }
  done < <(docker ps -aq --filter "label=minisky.profile=${profile}" 2>/dev/null || true)
  while IFS= read -r volume; do
    [[ -n "${volume}" ]] && docker volume rm "${volume}" >/dev/null 2>&1 || true
  done <"${volumes}"
  docker network rm minisky-net >/dev/null 2>&1 || true
}
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  [[ -n "${pid}" ]] && kill -TERM "${pid}" >/dev/null 2>&1 || true
  [[ -n "${pid}" ]] && wait "${pid}" >/dev/null 2>&1 || true
  [[ -n "${watchdog_pid}" ]] && kill -TERM "${watchdog_pid}" >/dev/null 2>&1 || true
  remove_owned
  rm -rf "${work}"
  rmdir "${phase_lock}" 2>/dev/null || true
  rmdir "${shared_lock}" 2>/dev/null || true
  exit "${status}"
}
trap cleanup EXIT INT TERM
( sleep 600; echo "Managed Kafka Terraform gate exceeded 10 minutes." >&2; kill -TERM "$$" ) &
watchdog_pid=$!

if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Refusing to disturb existing minisky-net." >&2
  exit 1
fi
if [[ -n "$(docker ps -aq --filter 'label=managed-by=minisky')" ]]; then
  echo "Refusing to disturb existing MiniSky containers." >&2
  exit 1
fi

free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}
start_daemon() {
  local label=$1 api_port ui_port log_file
  api_port="$(free_port)"
  ui_port="$(free_port)"
  gateway="http://127.0.0.1:${api_port}"
  log_file="${work}/${label}.log"
  HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
    MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
    "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  pid=$!
  for _ in {1..120}; do
    curl --fail --silent "${gateway}/healthz" >/dev/null 2>&1 && return
    kill -0 "${pid}" 2>/dev/null || { python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${log_file}" >&2; return 1; }
    sleep 0.25
  done
  return 1
}
stop_daemon() {
  kill -TERM "${pid}"
  wait "${pid}"
  pid=""
}
set_vars() {
  tf_vars=(
    -var="enable_phase19_managed_kafka_resource=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase19_managed_kafka_cluster_id=${cluster_id}"
    -var="profile=local"
    -var="project_id=${project}"
    -var="region=${region}"
  )
}
container_id() {
  docker ps -q --filter "label=minisky.profile=${profile}" --filter "label=minisky.resource=${canonical}"
}
assert_cluster() {
  local response="${work}/cluster.json"
  curl --fail --silent --show-error "${gateway}/_minisky/managedkafka/v1/${canonical}" >"${response}"
  python3 - "${response}" "${canonical}" <<'PY'
import json,sys
c=json.load(open(sys.argv[1]))
assert c["name"] == sys.argv[2] and c["state"] == "ACTIVE" and c["bootstrapAddress"]
assert c["capacityConfig"] == {"vcpuCount":"3","memoryBytes":"3221225472"}
assert c["gcpConfig"]["accessConfig"]["networkConfigs"][0]["subnet"].endswith("/subnetworks/minisky-metadata-only")
assert c["labels"]["goog-terraform-provisioned"] == "true"
PY
}
assert_backend_protocol() {
  local container topic="terraform-gate" received
  container="$(container_id)"
  [[ -n "${container}" ]] || { echo "Exact-owned Kafka container missing." >&2; return 1; }
  [[ "$(docker inspect --format '{{.Image}}' "${container}")" == "sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f" ]]
  capture_volumes "${container}"
  docker exec "${container}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "${topic}" --partitions 1 --replication-factor 1 >/dev/null
  printf 'minisky-managed-kafka\n' | docker exec -i "${container}" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "${topic}" >/dev/null
  received="$(docker exec "${container}" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "${topic}" --partition 0 --offset earliest --max-messages 1 --timeout-ms 10000 2>/dev/null)"
  [[ "${received}" == *"minisky-managed-kafka"* ]]
}
assert_no_drift() {
  set +e
  terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
    -target='google_managed_kafka_cluster.phase19[0]' "${tf_vars[@]}"
  local result=$?
  set -e
  [[ "${result}" == "0" ]]
}
assert_missing() {
  [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' "${gateway}/_minisky/managedkafka/v1/${canonical}")" == "404" ]]
}
assert_cleanup() {
  local volume
  [[ -z "$(docker ps -aq --filter "label=minisky.profile=${profile}")" ]] || return 1
  while IFS= read -r volume; do
    [[ -z "${volume}" ]] || ! docker volume inspect "${volume}" >/dev/null 2>&1 || return 1
  done <"${volumes}"
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT

start_daemon apply
set_vars
terraform -chdir="${terraform_dir}" init -backend-config="path=${tf_state}" -input=false -lockfile=readonly
terraform -chdir="${terraform_dir}" validate
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false -target='google_managed_kafka_cluster.phase19[0]' "${tf_vars[@]}"
assert_cluster
assert_backend_protocol
assert_no_drift
stop_daemon

start_daemon restart
set_vars
assert_cluster
assert_backend_protocol
assert_no_drift
terraform -chdir="${terraform_dir}" state rm -backup="${work}/state-before-import.backup" 'google_managed_kafka_cluster.phase19[0]'
terraform -chdir="${terraform_dir}" import -input=false "${tf_vars[@]}" 'google_managed_kafka_cluster.phase19[0]' "${canonical}"
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false -target='google_managed_kafka_cluster.phase19[0]' "${tf_vars[@]}"
assert_no_drift
terraform -chdir="${terraform_dir}" destroy -auto-approve -input=false -target='google_managed_kafka_cluster.phase19[0]' "${tf_vars[@]}"
assert_missing
assert_cleanup
stop_daemon

start_daemon cleanup-restart
assert_missing
assert_cleanup
stop_daemon
echo "Phase 19 Managed Kafka Terraform lifecycle passed; capacity/subnet are metadata and the exact-owned loopback broker is plaintext."
