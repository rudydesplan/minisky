#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${MINISKY_PHASE23_DOCUMENT_AI_TERRAFORM_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Document AI Terraform integration without explicit opt-in." >&2
  exit 2
fi
for command in curl go python3 terraform; do
  command -v "${command}" >/dev/null || { echo "Required command not found: ${command}" >&2; exit 1; }
done

lock="${TMPDIR:-/tmp}/minisky-phase23-document-ai-terraform-integration.lock"
mkdir "${lock}" 2>/dev/null || { echo "Another Document AI Terraform gate is active." >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
home="${work}/home"
state_root="${work}/state"
tf_data="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
profile="phase23-documentai-tf-$$"
project="phase23-documentai-tf-$$"
region="us"
display_name="phase23-metadata-processor"
parent="projects/${project}/locations/${region}"
processor=""
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
( sleep 420; echo "Document AI Terraform gate exceeded 7 minutes." >&2; kill -TERM "$$" ) &
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
    -var="enable_phase23_document_ai_processor=true"
    -var="minisky_endpoint=${gateway}"
    -var="phase23_document_ai_processor_display_name=${display_name}"
    -var="profile=local"
    -var="project_id=${project}"
    -var="region=${region}"
  )
}
discover_processor() {
  processor="$(curl --fail --silent "${gateway}/_minisky/documentai/v1/${parent}/processors" |
    python3 -c 'import json,sys; values=json.load(sys.stdin).get("processors", []); assert len(values)==1; print(values[0]["name"])')"
  [[ "${processor}" == "${parent}/processors/"* ]] || { echo "Unexpected processor name: ${processor}" >&2; return 1; }
}
assert_processor() {
  local response="${work}/processor.json"
  curl --fail --silent --show-error "${gateway}/_minisky/documentai/v1/${processor}" >"${response}"
  python3 - "${response}" "${processor}" "${display_name}" <<'PY'
import json,sys
p=json.load(open(sys.argv[1], encoding="utf-8"))
assert p["name"] == sys.argv[2]
assert p["displayName"] == sys.argv[3]
assert p["type"] == "OCR_PROCESSOR" and p["state"] == "ENABLED"
assert p.get("createTime") and p.get("defaultProcessorVersion", "").startswith(sys.argv[2] + "/processorVersions/")
assert "document" not in p and "rawDocument" not in p
PY
}
assert_no_drift() {
  set +e
  terraform -chdir="${root}/terraform" plan -detailed-exitcode -input=false \
    -target='google_document_ai_processor.phase23[0]' "${tf_vars[@]}"
  local result=$?
  set -e
  [[ "${result}" == "0" ]] || { echo "Document AI plan drifted with exit ${result}." >&2; return "${result}"; }
}
assert_missing() {
  local status
  status="$(curl --silent --output /dev/null --write-out '%{http_code}' "${gateway}/_minisky/documentai/v1/${processor}")"
  [[ "${status}" == "404" ]] || { echo "Expected processor 404, got ${status}." >&2; return 1; }
  curl --fail --silent "${gateway}/_minisky/documentai/v1/${parent}/processors" |
    python3 -c 'import json,sys; assert not json.load(sys.stdin).get("processors", [])'
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}"
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT
start_daemon
set_vars
terraform -chdir="${root}/terraform" init -backend-config="path=${tf_state}" -input=false -lockfile=readonly
terraform -chdir="${root}/terraform" validate
terraform -chdir="${root}/terraform" apply -auto-approve -input=false \
  -target='google_document_ai_processor.phase23[0]' "${tf_vars[@]}"
discover_processor
assert_processor
assert_no_drift
stop_daemon

start_daemon
set_vars
assert_processor
assert_no_drift
terraform -chdir="${root}/terraform" state rm -backup="${work}/state-before-import.backup" 'google_document_ai_processor.phase23[0]'
terraform -chdir="${root}/terraform" import -input=false "${tf_vars[@]}" \
  'google_document_ai_processor.phase23[0]' "${processor}"
terraform -chdir="${root}/terraform" apply -auto-approve -input=false \
  -target='google_document_ai_processor.phase23[0]' "${tf_vars[@]}"
assert_no_drift
terraform -chdir="${root}/terraform" destroy -auto-approve -input=false \
  -target='google_document_ai_processor.phase23[0]' "${tf_vars[@]}"
assert_missing
stop_daemon

start_daemon
set_vars
assert_missing
stop_daemon
echo "Phase 23 Document AI Terraform lifecycle passed; document processing, OCR quality, model inference, and sensitive document handling remain unsupported."
