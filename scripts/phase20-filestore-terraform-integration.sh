#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

[[ "${MINISKY_PHASE20_FILESTORE_TERRAFORM_INTEGRATION:-}" == "1" ]] || {
  echo "Refusing Filestore Terraform integration without explicit opt-in." >&2
  exit 2
}
for command in curl docker go python3 terraform; do
  command -v "${command}" >/dev/null 2>&1 || { echo "Required command not found: ${command}" >&2; exit 1; }
done
docker info >/dev/null

lock="${TMPDIR:-/tmp}/minisky-phase20-filestore-terraform-integration.lock"
mkdir "${lock}" 2>/dev/null || { echo "Another Filestore Terraform gate is active." >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="${root}/terraform"
work="$(mktemp -d)"
home="${work}/home"
state_root="${work}/state"
tf_data="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
profile="phase20-filestore-tf-$$"
project="phase20-filestore-tf-$$"
region="us-central1"
location="${region}-a"
instance="phase20-terraform-filestore"
canonical="projects/${project}/locations/${location}/instances/${instance}"
data_key="$(python3 - "${canonical}" <<'PY'
import hashlib,sys
print(hashlib.sha256(sys.argv[1].encode()).hexdigest()[:32])
PY
)"
share_root="${state_root}/profiles/${profile}/filestore-data/${data_key}/minisky"
data_file="${share_root}/terraform-gate.txt"
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
( sleep 420; echo "Filestore Terraform gate exceeded 7 minutes." >&2; kill -TERM "$$" ) &
watchdog_pid=$!

if [[ -n "$(docker ps -aq --filter "label=minisky.profile=${profile}")" ]]; then
  echo "Refusing profile-owned container collision." >&2
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
    -var="enable_phase20_filestore_resource=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase20_filestore_instance_name=${instance}"
    -var="profile=local"
    -var="project_id=${project}"
    -var="region=${region}"
  )
}
assert_instance() {
  local response="${work}/instance.json"
  curl --fail --silent --show-error "${gateway}/_minisky/file/v1/${canonical}" >"${response}"
  python3 - "${response}" "${canonical}" <<'PY'
import json,sys
i=json.load(open(sys.argv[1]))
assert i["name"] == sys.argv[2] and i["tier"] == "BASIC_HDD" and i["state"] == "READY"
assert i["fileShares"] == [{"name":"minisky","capacityGb":"1024"}]
assert i["networks"] == [{"network":"minisky-metadata-only","modes":["MODE_IPV4"]}]
assert i["labels"]["goog-terraform-provisioned"] == "true"
PY
}
assert_no_drift() {
  set +e
  terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
    -target='google_filestore_instance.phase20[0]' "${tf_vars[@]}"
  local result=$?
  set -e
  [[ "${result}" == "0" ]]
}
assert_missing() {
  local status
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' "${gateway}/_minisky/file/v1/${canonical}")"
  [[ "${status}" == "404" ]] || { echo "Expected durable 404, got ${status}." >&2; return 1; }
}
assert_clean() {
  [[ ! -e "${state_root}/profiles/${profile}/filestore-data/${data_key}" ]]
  [[ -z "$(docker ps -aq --filter "label=minisky.profile=${profile}")" ]]
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT

start_daemon apply
set_vars
terraform -chdir="${terraform_dir}" init -backend-config="path=${tf_state}" -input=false -lockfile=readonly
terraform -chdir="${terraform_dir}" validate
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false -target='google_filestore_instance.phase20[0]' "${tf_vars[@]}"
assert_instance
[[ -d "${share_root}" ]]
printf '%s\n' "bounded-local-filestore" >"${data_file}"
[[ "$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text().strip())' "${data_file}")" == "bounded-local-filestore" ]]
assert_no_drift
stop_daemon

start_daemon restart
set_vars
assert_instance
[[ "$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text().strip())' "${data_file}")" == "bounded-local-filestore" ]]
assert_no_drift
terraform -chdir="${terraform_dir}" state rm -backup="${work}/state-before-import.backup" 'google_filestore_instance.phase20[0]'
terraform -chdir="${terraform_dir}" import -input=false "${tf_vars[@]}" 'google_filestore_instance.phase20[0]' "${canonical}"
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false -target='google_filestore_instance.phase20[0]' "${tf_vars[@]}"
assert_no_drift
terraform -chdir="${terraform_dir}" destroy -auto-approve -input=false -target='google_filestore_instance.phase20[0]' "${tf_vars[@]}"
assert_missing
assert_clean
stop_daemon

start_daemon cleanup-restart
assert_missing
assert_clean
stop_daemon
echo "Phase 20 Filestore Terraform lifecycle passed; local profile files are not NFS or VPC parity."
