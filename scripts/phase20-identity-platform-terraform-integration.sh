#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

[[ "${MINISKY_PHASE20_IDENTITY_PLATFORM_TERRAFORM_INTEGRATION:-}" == "1" ]] || {
  echo "Refusing Identity Platform Terraform integration without explicit opt-in." >&2
  exit 2
}
for command in curl go python3 terraform; do
  command -v "${command}" >/dev/null 2>&1 || { echo "Required command not found: ${command}" >&2; exit 1; }
done

lock="${TMPDIR:-/tmp}/minisky-phase20-identity-platform-terraform-integration.lock"
mkdir "${lock}" 2>/dev/null || { echo "Another Identity Platform gate is active." >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="${root}/terraform"
work="$(mktemp -d)"
home="${work}/home"
state_root="${work}/state"
tf_data="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
profile="phase20-identity-tf-$$"
project="phase20-identity-tf-$$"
canonical="projects/${project}/config"
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
( sleep 300; echo "Identity Platform gate exceeded 5 minutes." >&2; kill -TERM "$$" ) &
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
  local domains=$1
  tf_vars=(
    -var="enable_phase20_identity_platform_config=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase20_identity_platform_authorized_domains=${domains}"
    -var="profile=local"
    -var="project_id=${project}"
  )
}
assert_domains() {
  local expected=$1 response="${work}/config.json"
  curl --fail --silent --show-error "${gateway}/_minisky/identityplatform/v2/${canonical}" >"${response}"
  python3 - "${response}" "${canonical}" "${expected}" <<'PY'
import json,sys
c=json.load(open(sys.argv[1]))
assert c["name"] == sys.argv[2]
want=[] if sys.argv[3] == "reset" else ["localhost"]
assert c.get("authorizedDomains", []) == want
PY
}
assert_no_drift() {
  set +e
  terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
    -target='google_identity_platform_config.phase20[0]' "${tf_vars[@]}"
  local result=$?
  set -e
  [[ "${result}" == "0" ]]
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT

start_daemon apply
set_vars '["localhost"]'
terraform -chdir="${terraform_dir}" init -backend-config="path=${tf_state}" -input=false -lockfile=readonly
terraform -chdir="${terraform_dir}" validate
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false -target='google_identity_platform_config.phase20[0]' "${tf_vars[@]}"
assert_domains configured
assert_no_drift
stop_daemon

start_daemon restart
set_vars '["localhost"]'
assert_domains configured
assert_no_drift
terraform -chdir="${terraform_dir}" state rm -backup="${work}/state-before-import.backup" 'google_identity_platform_config.phase20[0]'
terraform -chdir="${terraform_dir}" import -input=false "${tf_vars[@]}" 'google_identity_platform_config.phase20[0]' "${project}"
assert_no_drift

# The singleton cannot be deleted. Reconcile it to an empty bounded baseline,
# then destroy only removes Terraform ownership.
set_vars '[]'
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false -target='google_identity_platform_config.phase20[0]' "${tf_vars[@]}"
assert_domains reset
terraform -chdir="${terraform_dir}" destroy -auto-approve -input=false -target='google_identity_platform_config.phase20[0]' "${tf_vars[@]}"
assert_domains reset
stop_daemon

start_daemon cleanup-restart
assert_domains reset
stop_daemon
echo "Phase 20 Identity Platform config lifecycle passed with truthful singleton reset semantics."
