#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
command -v python3 >/dev/null 2>&1 || { echo "Required command not found: python3" >&2; exit 1; }
airflow_image="$(
  python3 - "${root}/pkg/config/images.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["composer"]["airflow_image"])
PY
)"

verify_container_image() {
  local container=$1 configured_reference expected_repo_digest repo_digests
  configured_reference="$(docker inspect --format '{{.Config.Image}}' "${container}")"
  [[ "${configured_reference}" == "${airflow_image}" ]] || {
    echo "Airflow configured image mismatch: expected ${airflow_image}, got ${configured_reference}" >&2
    return 1
  }
  expected_repo_digest="$(
    python3 - "${airflow_image}" <<'PY'
import sys
tagged, digest = sys.argv[1].rsplit("@", 1)
slash = tagged.rfind("/")
colon = tagged.rfind(":")
if colon <= slash:
    raise SystemExit("image reference lacks an explicit tag")
print(f"{tagged[:colon]}@{digest}")
PY
  )"
  repo_digests="$(docker image inspect --format '{{json .RepoDigests}}' "${configured_reference}")"
  python3 - "${repo_digests}" "${expected_repo_digest}" <<'PY' || {
import json, sys
raise SystemExit(0 if sys.argv[2] in (json.loads(sys.argv[1]) or []) else 1)
PY
    echo "Airflow repository digest mismatch: expected ${expected_repo_digest}, got ${repo_digests}" >&2
    return 1
  }
}

if [[ "${1:-}" == "--print-required-images" && "$#" -eq 1 ]]; then
  printf '%s\n' "${airflow_image}"
  exit 0
fi
if [[ "${1:-}" == "--verify-container-image" && "$#" -eq 2 ]]; then
  verify_container_image "$2"
  exit
fi
[[ "$#" -eq 0 ]] || {
  echo "Usage: $0 [--print-required-images | --verify-container-image <container>]" >&2
  exit 2
}

if [[ "${MINISKY_PHASE19_COMPOSER_TERRAFORM_INTEGRATION:-}" != "1" ||
      "${MINISKY_PHASE19_DOCKER_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Composer Terraform integration without both explicit Terraform and heavy Docker opt-ins." >&2
  exit 2
fi
for command in curl docker go python3 terraform; do
  command -v "${command}" >/dev/null 2>&1 || { echo "Required command not found: ${command}" >&2; exit 1; }
done
docker info >/dev/null
docker image inspect "${airflow_image}" >/dev/null

shared_lock="${TMPDIR:-/tmp}/minisky-net-integration.lock"
phase_lock="${TMPDIR:-/tmp}/minisky-phase19-composer-terraform-integration.lock"
mkdir "${shared_lock}" 2>/dev/null || { echo "Another MiniSky Docker integration is active." >&2; exit 1; }
mkdir "${phase_lock}" 2>/dev/null || { rmdir "${shared_lock}"; echo "Another Composer Terraform gate is active." >&2; exit 1; }

terraform_dir="${root}/terraform"
work="$(mktemp -d)"
home="${work}/home"
state_root="${work}/state"
tf_data="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
volumes="${work}/volumes"
profile="phase19-composer-tf-$$"
project="phase19-composer-tf-$$"
region="us-central1"
environment_name="phase19-terraform-composer"
canonical="projects/${project}/locations/${region}/environments/${environment_name}"
pid=""
watchdog_pid=""
gateway=""
log_file=""
mkdir -p "${home}" "${state_root}" "${tf_data}"
: >"${volumes}"

capture_volumes() {
  local container=$1
  docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{println .Name}}{{end}}{{end}}' "${container}" >>"${volumes}"
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
( sleep 600; echo "Composer Terraform gate exceeded 10 minutes." >&2; kill -TERM "$$" ) &
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
  local label=$1 api_port ui_port
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
  echo "Timed out waiting for MiniSky." >&2
  return 1
}
stop_daemon() {
  kill -TERM "${pid}"
  wait "${pid}"
  pid=""
}
set_vars() {
  tf_vars=(
    -var="enable_phase19_composer_resource=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase19_composer_environment_name=${environment_name}"
    -var="profile=local"
    -var="project_id=${project}"
    -var="region=${region}"
  )
}
assert_environment() {
  local expected_state=$1 response="${work}/environment.json"
  curl --fail --silent --show-error "${gateway}/_minisky/composer/v1/${canonical}" >"${response}"
  python3 - "${response}" "${canonical}" "${expected_state}" <<'PY'
import json,sys
environment=json.load(open(sys.argv[1], encoding="utf-8"))
if environment.get("name") != sys.argv[2] or environment.get("state") != sys.argv[3]:
    raise SystemExit(f"unexpected Composer environment: {environment!r}")
if not environment.get("uuid") or not environment.get("createTime") or not environment.get("updateTime"):
    raise SystemExit("missing stable Composer metadata")
if environment.get("labels", {}).get("goog-terraform-provisioned") != "true":
    raise SystemExit("provider label was not persisted")
config=environment.get("config", {})
if sys.argv[3] == "RUNNING" and not config.get("airflowUri"):
    raise SystemExit("running environment lacks local Airflow endpoint")
if sys.argv[3] == "ERROR" and config.get("airflowUri"):
    raise SystemExit("restarted metadata exposed stale Airflow endpoint")
PY
}
assert_backend() {
  local container
  container="$(docker ps -q --filter "label=minisky.profile=${profile}" --filter "label=minisky.resource=${canonical}")"
  [[ -n "${container}" ]] || { echo "Exact-owned Airflow container missing." >&2; return 1; }
  verify_container_image "${container}"
  capture_volumes "${container}"
}
assert_dag_execution() {
  local container
  container="$(docker ps -q --filter "label=minisky.profile=${profile}" --filter "label=minisky.resource=${canonical}")"
  cat >"${work}/terraform_gate.py" <<'PY'
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG("terraform_gate", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    EmptyOperator(task_id="done")
PY
  docker cp "${work}/terraform_gate.py" "${container}:/opt/airflow/dags/terraform_gate.py"
  for _ in {1..120}; do
    docker exec "${container}" airflow dags list 2>/dev/null | awk '$1 == "terraform_gate" { found=1 } END { exit !found }' && break
    sleep 1
  done
  docker exec "${container}" airflow dags reserialize >/dev/null
  docker exec "${container}" airflow dags trigger --run-id terraform-gate-run terraform_gate >/dev/null
}
assert_no_drift() {
  set +e
  terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
    -target='google_composer_environment.phase19[0]' "${tf_vars[@]}"
  local result=$?
  set -e
  [[ "${result}" == "0" ]] || { echo "Composer plan drifted with exit ${result}." >&2; return "${result}"; }
}
assert_missing() {
  local status
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' "${gateway}/_minisky/composer/v1/${canonical}")"
  [[ "${status}" == "404" ]] || { echo "Expected destroyed Composer environment 404, got ${status}." >&2; return 1; }
}
assert_cleanup() {
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
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false \
  -target='google_composer_environment.phase19[0]' "${tf_vars[@]}"
assert_environment RUNNING
assert_backend
assert_dag_execution
assert_no_drift
stop_daemon

start_daemon restart
set_vars
assert_environment RUNNING
assert_backend
assert_no_drift
terraform -chdir="${terraform_dir}" state rm -backup="${work}/state-before-import.backup" 'google_composer_environment.phase19[0]'
terraform -chdir="${terraform_dir}" import -input=false "${tf_vars[@]}" \
  'google_composer_environment.phase19[0]' "${canonical}"
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false \
  -target='google_composer_environment.phase19[0]' "${tf_vars[@]}"
assert_no_drift
terraform -chdir="${terraform_dir}" destroy -auto-approve -input=false \
  -target='google_composer_environment.phase19[0]' "${tf_vars[@]}"
assert_missing
while IFS= read -r volume; do
  [[ -n "${volume}" ]] && docker volume rm "${volume}" >/dev/null 2>&1 || true
done <"${volumes}"
assert_cleanup
stop_daemon

start_daemon cleanup-restart
set_vars
assert_missing
assert_cleanup
stop_daemon
echo "Phase 19 Composer Terraform lifecycle passed; this does not claim Cloud Composer parity beyond exact local Airflow backend evidence."
