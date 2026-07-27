#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${MINISKY_PHASE21_SERVICE_DIRECTORY_TERRAFORM_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Service Directory Terraform integration without explicit opt-in." >&2
  exit 2
fi
for command in curl go python3 terraform; do
  command -v "${command}" >/dev/null || { echo "Required command not found: ${command}" >&2; exit 1; }
done

lock="${TMPDIR:-/tmp}/minisky-phase21-service-directory-terraform-integration.lock"
mkdir "${lock}" 2>/dev/null || { echo "Another Service Directory Terraform gate is active." >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
home="${work}/home"
state_root="${work}/state"
tf_data="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
profile="phase21-servicedirectory-tf-$$"
project="phase21-servicedirectory-tf-$$"
region="us-central1"
namespace_id="phase21-namespace"
service_id="phase21-service"
endpoint_id="phase21-endpoint"
namespace="projects/${project}/locations/${region}/namespaces/${namespace_id}"
service="${namespace}/services/${service_id}"
endpoint="${service}/endpoints/${endpoint_id}"
pid=""
watchdog_pid=""
gateway=""
mkdir -p "${home}" "${state_root}" "${tf_data}"

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  [[ -n "${pid}" ]] && kill -TERM "${pid}" >/dev/null 2>&1 || true
  [[ -n "${pid}" ]] && wait "${pid}" >/dev/null 2>&1 || true
  [[ -n "${watchdog_pid}" ]] && kill -TERM "${watchdog_pid}" >/dev/null 2>&1 || true
  rm -rf "${work}"
  rmdir "${lock}" 2>/dev/null || true
  exit "${status}"
}
trap cleanup EXIT INT TERM
( sleep 420; echo "Service Directory Terraform gate exceeded 7 minutes." >&2; kill -TERM "$$" ) &
watchdog_pid=$!

free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}
start_daemon() {
  local api_port ui_port log_file
  api_port="$(free_port)"
  ui_port="$(free_port)"
  gateway="http://127.0.0.1:${api_port}"
  log_file="${work}/minisky.log"
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
    -var="enable_phase21_service_directory_resources=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase21_service_directory_namespace_id=${namespace_id}"
    -var="phase21_service_directory_service_id=${service_id}"
    -var="phase21_service_directory_endpoint_id=${endpoint_id}"
    -var="profile=local"
    -var="project_id=${project}"
    -var="region=${region}"
  )
}
assert_hierarchy() {
  local ns_json="${work}/namespace.json" service_json="${work}/service.json" endpoint_json="${work}/endpoint.json"
  curl --fail --silent --show-error "${gateway}/_minisky/servicedirectory/v1/${namespace}" >"${ns_json}"
  curl --fail --silent --show-error "${gateway}/_minisky/servicedirectory/v1/${service}" >"${service_json}"
  curl --fail --silent --show-error "${gateway}/_minisky/servicedirectory/v1/${endpoint}" >"${endpoint_json}"
  python3 - "${ns_json}" "${service_json}" "${endpoint_json}" "${namespace}" "${service}" "${endpoint}" <<'PY'
import json,sys
n,s,e=(json.load(open(path, encoding="utf-8")) for path in sys.argv[1:4])
assert n["name"] == sys.argv[4] and n["labels"]["purpose"] == "metadata-only"
assert s["name"] == sys.argv[5] and s["annotations"]["protocol"] == "opaque"
assert e["name"] == sys.argv[6] and e["address"] == "127.0.0.1" and e["port"] == 8080
assert e["annotations"]["resolution"] == "unsupported"
for resource in (n,s,e):
    assert resource.get("uid") and resource.get("createTime") and resource.get("updateTime")
PY
}
assert_no_drift() {
  set +e
  terraform -chdir="${root}/terraform" plan -detailed-exitcode -input=false \
    -target='google_service_directory_endpoint.phase21[0]' "${tf_vars[@]}"
  local result=$?
  set -e
  [[ "${result}" == "0" ]] || { echo "Service Directory plan drifted with exit ${result}." >&2; return "${result}"; }
}
assert_missing() {
  local name status
  for name in "${endpoint}" "${service}" "${namespace}"; do
    status="$(curl --silent --output /dev/null --write-out '%{http_code}' "${gateway}/_minisky/servicedirectory/v1/${name}")"
    [[ "${status}" == "404" ]] || { echo "Expected ${name} to return 404, got ${status}." >&2; return 1; }
  done
  curl --fail --silent "${gateway}/_minisky/servicedirectory/v1/projects/${project}/locations/${region}/namespaces" |
    python3 -c 'import json,sys; assert not json.load(sys.stdin).get("namespaces", [])'
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT
start_daemon
set_vars
terraform -chdir="${root}/terraform" init -backend-config="path=${tf_state}" -input=false -lockfile=readonly
terraform -chdir="${root}/terraform" validate
terraform -chdir="${root}/terraform" apply -auto-approve -input=false \
  -target='google_service_directory_endpoint.phase21[0]' "${tf_vars[@]}"
assert_hierarchy
assert_no_drift
stop_daemon

start_daemon
set_vars
assert_hierarchy
assert_no_drift
terraform -chdir="${root}/terraform" state rm \
  -backup="${work}/state-before-import.backup" \
  'google_service_directory_endpoint.phase21[0]' \
  'google_service_directory_service.phase21[0]' \
  'google_service_directory_namespace.phase21[0]'
terraform -chdir="${root}/terraform" import -input=false "${tf_vars[@]}" \
  'google_service_directory_namespace.phase21[0]' "${namespace}"
terraform -chdir="${root}/terraform" import -input=false "${tf_vars[@]}" \
  'google_service_directory_service.phase21[0]' "${service}"
terraform -chdir="${root}/terraform" import -input=false "${tf_vars[@]}" \
  'google_service_directory_endpoint.phase21[0]' "${endpoint}"
terraform -chdir="${root}/terraform" apply -auto-approve -input=false \
  -target='google_service_directory_endpoint.phase21[0]' "${tf_vars[@]}"
assert_no_drift
terraform -chdir="${root}/terraform" destroy -auto-approve -input=false \
  -target='google_service_directory_endpoint.phase21[0]' \
  -target='google_service_directory_service.phase21[0]' \
  -target='google_service_directory_namespace.phase21[0]' "${tf_vars[@]}"
assert_missing
stop_daemon

start_daemon
set_vars
assert_missing
stop_daemon
echo "Phase 21 Service Directory Terraform lifecycle passed; endpoint address/port remain metadata with no DNS or network-resolution claim."
