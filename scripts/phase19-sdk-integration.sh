#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE19_SDK_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 19 SDK integration without MINISKY_PHASE19_SDK_INTEGRATION=1." >&2
  exit 2
fi

for command in curl go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock="${TMPDIR:-/tmp}/minisky-phase19-sdk-integration.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 19 SDK integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
state_root="${work}/state"
home="${work}/home"
profile="phase19-sdk-$$"
project="phase19-project-$$"
evidence_file="${work}/generated-client-evidence.json"
pid=""
owned_volumes_file="${work}/owned-volumes"
mkdir -p "${state_root}" "${home}"
: >"${owned_volumes_file}"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  if command -v docker >/dev/null 2>&1; then
    while IFS= read -r container; do
      [[ -n "${container}" ]] && docker rm -f -v "${container}" >/dev/null 2>&1 || true
    done < <(docker ps -aq --filter "label=minisky.profile=${profile}" 2>/dev/null || true)
    while IFS= read -r volume; do
      [[ -n "${volume}" ]] && docker volume rm "${volume}" >/dev/null 2>&1 || true
    done <"${owned_volumes_file}"
  fi
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

go build -trimpath -o "${work}/minisky" ./cmd/minisky

start_daemon() {
  local experimental=$1
  local log_file=$2
  local attempt
  for attempt in {1..60}; do
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
    for _ in {1..120}; do
      if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
        return
      fi
      if ! kill -0 "${pid}" 2>/dev/null; then
        wait "${pid}" 2>/dev/null || true
        pid=""
        if python3 -c 'import pathlib,sys; raise SystemExit("Docker resource ownership conflict" not in pathlib.Path(sys.argv[1]).read_text())' "${log_file}"; then
          sleep 1
          break
        fi
        python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${log_file}" >&2
        return 1
      fi
      sleep 0.25
    done
    if [[ -n "${pid}" ]]; then
      echo "Timed out waiting for MiniSky readiness." >&2
      return 1
    fi
  done
  echo "Timed out waiting for the shared MiniSky Docker network to become available." >&2
  return 1
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}"
  fi
  pid=""
}

assert_no_owned_resources() {
  local containers
  containers="$(docker ps -aq --filter "label=minisky.profile=${profile}")"
  if [[ -n "${containers}" ]]; then
    echo "Exact-owned Phase 19 container cleanup is incomplete: ${containers}" >&2
    return 1
  fi
  while IFS= read -r volume; do
    if [[ -n "${volume}" ]] && docker volume inspect "${volume}" >/dev/null 2>&1; then
      echo "Exact-owned Phase 19 volume cleanup is incomplete: ${volume}" >&2
      return 1
    fi
  done <"${owned_volumes_file}"
}

capture_owned_volumes() {
  local container volume
  while IFS= read -r container; do
    while IFS= read -r volume; do
      [[ -n "${volume}" ]] && printf '%s\n' "${volume}" >>"${owned_volumes_file}"
    done < <(docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{println .Name}}{{end}}{{end}}' "${container}")
  done < <(docker ps -aq --filter "label=minisky.profile=${profile}")
  return 0
}

export MINISKY_ENDPOINT="${gateway}"
export MINISKY_PROJECT_ID="${project}"
export MINISKY_PHASE19_LOCATION="us-central1"
export MINISKY_PHASE19_DATAFLOW_NAME="phase19-dataflow"
export MINISKY_PHASE19_REPOSITORY_ID="phase19-repo"
export MINISKY_PHASE19_WORKSPACE_ID="phase19-workspace"
export MINISKY_PHASE19_CLUSTER_ID="phase19-kafka"
export MINISKY_PHASE19_TOPIC_ID="phase19-topic"
export MINISKY_PHASE19_ENVIRONMENT_ID="phase19-airflow"
export MINISKY_PHASE19_EVIDENCE="${evidence_file}"

start_daemon 0 "${work}/default-gate.log"
MINISKY_PHASE19_MODE=gate go run ./sdk-smoke/phase19
stop_daemon

start_daemon 1 "${work}/core-create.log"
MINISKY_PHASE19_MODE=create MINISKY_PHASE19_EXPERIMENTAL_OPT_IN=1 go run ./sdk-smoke/phase19
stop_daemon

start_daemon 1 "${work}/core-restart.log"
MINISKY_PHASE19_MODE=verify MINISKY_PHASE19_EXPERIMENTAL_OPT_IN=1 go run ./sdk-smoke/phase19
MINISKY_PHASE19_MODE=delete MINISKY_PHASE19_EXPERIMENTAL_OPT_IN=1 go run ./sdk-smoke/phase19
stop_daemon

if [[ "${MINISKY_PHASE19_DOCKER_INTEGRATION:-}" != "1" ]]; then
  echo "Phase 19 core generated-client integration passed."
  echo "Pinned Kafka/Airflow backend evidence skipped; set MINISKY_PHASE19_DOCKER_INTEGRATION=1 for the heavy opt-in gate."
  exit 0
fi

command -v docker >/dev/null 2>&1 || {
  echo "Docker integration requested, but docker is unavailable." >&2
  exit 1
}
docker info >/dev/null

start_daemon 1 "${work}/docker-create.log"
MINISKY_PHASE19_MODE=docker-create MINISKY_PHASE19_EXPERIMENTAL_OPT_IN=1 go run ./sdk-smoke/phase19
capture_owned_volumes

kafka_container="$(docker ps -q \
  --filter "label=minisky.profile=${profile}" \
  --filter "label=minisky.resource=projects/${project}/locations/us-central1/clusters/phase19-kafka")"
airflow_container="$(docker ps -q \
  --filter "label=minisky.profile=${profile}" \
  --filter "label=minisky.resource=projects/${project}/locations/us-central1/environments/phase19-airflow")"
[[ -n "${kafka_container}" && -n "${airflow_container}" ]] || {
  echo "Expected exact-owned Kafka and Airflow containers were not found." >&2
  exit 1
}

printf 'phase19-one\nphase19-two\n' | docker exec -i "${kafka_container}" \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic phase19-topic
kafka_messages="$(docker exec "${kafka_container}" \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic phase19-topic --partition 0 --offset earliest --max-messages 2 --timeout-ms 10000)"
[[ "${kafka_messages}" == *"phase19-one"* && "${kafka_messages}" == *"phase19-two"* ]] || {
  echo "Kafka standard-protocol producer/consumer evidence did not round-trip." >&2
  exit 1
}
echo "Kafka data-plane evidence used the image's standard protocol CLI, not a generated control client."

cat >"${work}/phase19.py" <<'PY'
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG("phase19", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    EmptyOperator(task_id="done")
PY
docker cp "${work}/phase19.py" "${airflow_container}:/opt/airflow/dags/phase19.py"
dag_ready=0
for _ in {1..120}; do
  if docker exec "${airflow_container}" airflow dags list 2>/dev/null | awk '$1 == "phase19" { found=1 } END { exit !found }'; then
    dag_ready=1
    break
  fi
  sleep 1
done
if [[ "${dag_ready}" != "1" ]]; then
  echo "Timed out waiting for the exact-owned Airflow container to register the Phase 19 DAG." >&2
  docker exec "${airflow_container}" airflow dags list-import-errors >&2 || true
  exit 1
fi
docker exec "${airflow_container}" airflow dags reserialize >/dev/null
trigger_ready=0
for _ in {1..60}; do
  if docker exec "${airflow_container}" airflow dags trigger --run-id phase19-sdk-run phase19 \
    >"${work}/airflow-trigger.log" 2>&1; then
    trigger_ready=1
    break
  fi
  sleep 1
done
if [[ "${trigger_ready}" != "1" ]]; then
  echo "Timed out triggering the Phase 19 DAG after it was parsed." >&2
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' \
    "${work}/airflow-trigger.log" >&2
  exit 1
fi
echo "Composer exact-owned Airflow DAG upload and trigger evidence passed via the backend CLI."
stop_daemon

start_daemon 1 "${work}/docker-restart.log"
MINISKY_PHASE19_MODE=docker-verify MINISKY_PHASE19_EXPERIMENTAL_OPT_IN=1 go run ./sdk-smoke/phase19
MINISKY_PHASE19_MODE=docker-delete MINISKY_PHASE19_EXPERIMENTAL_OPT_IN=1 go run ./sdk-smoke/phase19
while IFS= read -r volume; do
  [[ -n "${volume}" ]] && docker volume rm "${volume}" >/dev/null 2>&1 || true
done <"${owned_volumes_file}"
assert_no_owned_resources
stop_daemon

echo "Phase 19 generated-client, restart, backend, 404, container, and volume cleanup integration passed."
