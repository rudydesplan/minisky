#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

if [[ "${MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Binary Authorization Terraform integration without explicit opt-in." >&2
  exit 2
fi
for command in curl go python3 terraform; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock=""
root=""
terraform_dir=""
work=""
home=""
state_root=""
tf_data=""
tf_state=""
terraform_log=""
diagnostics=""
profile=""
project=""
canonical=""
pid=""
watchdog_pid=""
gateway=""

cleanup() {
  local result=$?
  if [[ "$#" -gt 0 ]]; then
    result=$1
  fi
  trap - EXIT INT TERM
  [[ -n "${pid}" ]] && kill -TERM "${pid}" >/dev/null 2>&1 || true
  [[ -n "${pid}" ]] && wait "${pid}" >/dev/null 2>&1 || true
  [[ -n "${watchdog_pid}" ]] && kill -TERM "${watchdog_pid}" >/dev/null 2>&1 || true
  [[ -n "${watchdog_pid}" ]] && wait "${watchdog_pid}" >/dev/null 2>&1 || true
  if [[ "${result}" -ne 0 ]] && declare -F emit_diagnostics >/dev/null 2>&1; then
    emit_diagnostics || true
  fi
  [[ -n "${work}" ]] && rm -rf "${work}" >/dev/null 2>&1 || true
  [[ -n "${lock}" ]] && rmdir "${lock}" 2>/dev/null || true
  exit "${result}"
}

lock="${TMPDIR:-/tmp}/minisky-phase25-binary-authorization-terraform-integration.lock"
mkdir "${lock}" 2>/dev/null || {
  echo "Another Binary Authorization Terraform gate is active." >&2
  exit 1
}
trap cleanup EXIT
trap 'cleanup 130' INT
trap 'cleanup 143' TERM

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="${root}/terraform"
work="$(mktemp -d)"
home="${work}/home"
state_root="${work}/state"
tf_data="${work}/terraform-data"
tf_state="${work}/terraform.tfstate"
terraform_log="${work}/terraform.log"
diagnostics="${work}/diagnostics"
profile="phase25-binauthz-tf-$$"
project="phase25-binauthz-tf-$$"
canonical="projects/${project}/policy"
mkdir -p "${home}" "${state_root}" "${tf_data}"
[[ -z "$(ls -A "${state_root}")" ]] || {
  echo "Refusing to use a non-empty isolated state root." >&2
  exit 1
}

emit_diagnostics() {
  if ! python3 - "${work}" "${diagnostics}" "${terraform_log}" "${work}"/minisky-*.log <<'PY'
import pathlib, re, sys

work = pathlib.Path(sys.argv[1]).resolve()
destination = pathlib.Path(sys.argv[2])
try:
    destination.resolve().relative_to(work)
except (OSError, ValueError):
    sys.stderr.write("Phase 25 diagnostics destination escaped the temporary work directory; skipping diagnostics.\n")
    raise SystemExit(0)
try:
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
except OSError:
    sys.stderr.write("Phase 25 diagnostics destination is unavailable; continuing cleanup.\n")
    raise SystemExit(0)
for raw in sys.argv[3:]:
    source = pathlib.Path(raw)
    try:
        source.resolve().relative_to(work)
    except (OSError, ValueError):
        continue
    try:
        text = source.read_text(encoding="utf-8", errors="replace")
    except OSError:
        sys.stderr.write(f"Phase 25 diagnostic source {source.name!r} is unreadable; skipping it.\n")
        continue
    try:
        text = text.replace(str(work), "<temporary-workdir>")
        text = re.sub(r"(?i)(authorization:\s*bearer\s+)\S+", r"\1<redacted>", text)
        text = re.sub(
            r"(?i)((?:access|refresh)[_ -]?token|client[_ -]?secret|private[_ -]?key|credentials)"
            r"([\"'=:\s]+)\S+",
            r"\1\2<redacted>",
            text,
        )
        text = "\n".join(text.splitlines()[-400:]) + "\n"
    except Exception:
        sys.stderr.write(f"Phase 25 diagnostic source {source.name!r} could not be sanitized; skipping it.\n")
        continue
    try:
        (destination / source.name).write_text(text, encoding="utf-8")
    except OSError:
        sys.stderr.write(f"Phase 25 diagnostic destination rejected {source.name!r}; continuing cleanup.\n")
    sys.stderr.write(f"--- sanitized {source.name} ---\n{text}")
PY
  then
    printf '%s\n' "Phase 25 diagnostics unavailable; continuing isolated cleanup." >&2
  fi
  return 0
}
( sleep 480; echo "Binary Authorization Terraform gate exceeded 8 minutes." >&2; kill -TERM "$$" ) &
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
  while [[ "${ui_port}" == "${api_port}" ]]; do ui_port="$(free_port)"; done
  gateway="http://127.0.0.1:${api_port}"
  [[ "${gateway}" =~ ^http://127\.0\.0\.1:[0-9]+$ ]] || {
    echo "Refusing non-loopback MiniSky endpoint: ${gateway}" >&2
    return 1
  }
  log_file="${work}/minisky-${label}.log"
  HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
    MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
    "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  pid=$!
  for _ in {1..120}; do
    curl --fail --silent "${gateway}/healthz" >/dev/null 2>&1 && return
    kill -0 "${pid}" 2>/dev/null || {
      echo "MiniSky exited before becoming ready; sanitized logs follow on cleanup." >&2
      return 1
    }
    sleep 0.25
  done
  echo "Timed out waiting for MiniSky readiness." >&2
  return 1
}
stop_daemon() {
  kill -TERM "${pid}"
  wait "${pid}"
  pid=""
}
set_vars() {
  tf_vars=(
    -var="enable_phase25_binary_authorization_policy=true"
    -var="minisky_endpoint=${gateway}"
    -var="profile=local"
    -var="project_id=${project}"
  )
}
run_logged() {
  {
    printf '>>> '
    printf '%q ' "$@"
    printf '\n'
  } >>"${terraform_log}"
  "$@" 2>&1 | tee -a "${terraform_log}"
  return "${PIPESTATUS[0]}"
}
tf() {
  local terraform_cli="terraform"
  if [[ -n "${TERRAFORM_CLI_PATH:-}" ]]; then
    terraform_cli="${TERRAFORM_CLI_PATH}/terraform-bin"
    if [[ ! -x "${terraform_cli}" ]]; then
      echo "TERRAFORM_CLI_PATH does not contain an executable terraform-bin." >&2
      return 1
    fi
  fi
  run_logged "${terraform_cli}" -chdir="${terraform_dir}" "$@"
}
assert_provider_contract() {
  local schema="${work}/provider-schema.json" version="${work}/terraform-version.json"
  terraform -chdir="${terraform_dir}" providers schema -json >"${schema}" 2>>"${terraform_log}"
  terraform -chdir="${terraform_dir}" version -json >"${version}" 2>>"${terraform_log}"
  python3 - "${schema}" "${version}" <<'PY'
import json, sys

schema = json.load(open(sys.argv[1], encoding="utf-8"))
version = json.load(open(sys.argv[2], encoding="utf-8"))
provider = "registry.terraform.io/hashicorp/google"
assert version["provider_selections"][provider] == "7.41.0"
root = schema["provider_schemas"][provider]
assert root["provider"]["block"]["attributes"]["binary_authorization_custom_endpoint"]["optional"] is True
resource = root["resource_schemas"]["google_binary_authorization_policy"]["block"]
assert resource["block_types"]["admission_whitelist_patterns"]["nesting_mode"] == "list"
assert resource["block_types"]["default_admission_rule"]["min_items"] == 1
assert resource["block_types"]["default_admission_rule"]["max_items"] == 1
assert resource["attributes"]["project"]["computed"] is True
PY
}
assert_configured_policy() {
  local response="${work}/configured-policy.json"
  curl --fail --silent --show-error \
    "${gateway}/_minisky/binaryauthorization/v1/${canonical}" >"${response}"
  python3 - "${response}" "${canonical}" <<'PY'
import json, sys

policy = json.load(open(sys.argv[1], encoding="utf-8"))
assert policy["name"] == sys.argv[2]
assert policy.get("admissionWhitelistPatterns") == [
    {"namePattern": "gcr.io/minisky-phase25/allowed/*"}
]
assert policy["defaultAdmissionRule"] == {
    "evaluationMode": "ALWAYS_DENY",
    "enforcementMode": "ENFORCED_BLOCK_AND_AUDIT_LOG",
}
PY
}
assert_decision() {
  local image=$1 expected=$2 reason=$3 response="${work}/decision.json"
  curl --fail --silent --show-error -X POST -H 'Content-Type: application/json' \
    -d "{\"image\":\"${image}\"}" \
    "${gateway}/_minisky/binaryauthorization/v1/projects/${project}/policy:evaluate" >"${response}"
  python3 - "${response}" "${expected}" "${reason}" "${canonical}" <<'PY'
import json, sys

decision = json.load(open(sys.argv[1], encoding="utf-8"))
assert decision["allowed"] is (sys.argv[2] == "allow")
assert sys.argv[3] in decision["reason"]
assert decision["policy"] == sys.argv[4]
PY
}
assert_plan_exit() {
  local expected=$1 label=$2 result
  if tf plan -detailed-exitcode -input=false \
    -target='google_binary_authorization_policy.phase25[0]' "${tf_vars[@]}"; then
    result=0
  else
    result=$?
  fi
  if [[ "${result}" != "${expected}" ]]; then
    echo "Expected ${label} plan exit ${expected}, received ${result}." >&2
    return 1
  fi
}
assert_no_drift() {
  assert_plan_exit 0 "no-drift"
}
put_stale_policy() {
  local request="${work}/stale-policy-request.json"
  python3 - "${request}" "${canonical}" <<'PY'
import json, sys

with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump({
        "name": sys.argv[2],
        "description": "deliberately stale remote policy",
        "admissionWhitelistPatterns": [
            {"namePattern": "gcr.io/minisky-phase25/allowed/*"}
        ],
        "defaultAdmissionRule": {
            "evaluationMode": "ALWAYS_DENY",
            "enforcementMode": "ENFORCED_BLOCK_AND_AUDIT_LOG",
        },
    }, stream, separators=(",", ":"))
PY
  curl --fail --silent --show-error -X PUT -H 'Content-Type: application/json' \
    --data-binary "@${request}" \
    "${gateway}/_minisky/binaryauthorization/v1/${canonical}" >"${work}/stale-policy-response.json"
}
verify_matching_import() {
  tf state rm -backup="${work}/state-before-import.backup" \
    'google_binary_authorization_policy.phase25[0]'
  tf import -input=false "${tf_vars[@]}" \
    'google_binary_authorization_policy.phase25[0]' "projects/${project}"
  assert_plan_exit 0 "matching import refresh"
}
verify_stale_import_and_reconcile() {
  put_stale_policy
  tf state rm -backup="${work}/state-before-stale-import.backup" \
    'google_binary_authorization_policy.phase25[0]'
  tf import -input=false "${tf_vars[@]}" \
    'google_binary_authorization_policy.phase25[0]' "projects/${project}"
  assert_plan_exit 2 "stale import refresh"
  tf apply -auto-approve -input=false \
    -target='google_binary_authorization_policy.phase25[0]' "${tf_vars[@]}"
  assert_plan_exit 0 "stale import reconcile"
}
assert_provider_reset() {
  local response="${work}/reset-policy.json" status
  status="$(curl --silent --output "${response}" --write-out '%{http_code}' \
    "${gateway}/_minisky/binaryauthorization/v1/${canonical}")"
  [[ "${status}" == "200" ]] || {
    echo "Expected provider default policy after destroy, got HTTP ${status}." >&2
    return 1
  }
  python3 - "${response}" "${canonical}" <<'PY'
import json, sys

policy = json.load(open(sys.argv[1], encoding="utf-8"))
assert policy["name"] == sys.argv[2]
assert policy.get("admissionWhitelistPatterns") == [
    {"namePattern": "gcr.io/google_containers/*"}
]
assert policy["defaultAdmissionRule"] == {
    "evaluationMode": "ALWAYS_ALLOW",
    "enforcementMode": "ENFORCED_BLOCK_AND_AUDIT_LOG",
}
for stale in ("description", "globalPolicyEvaluationMode", "clusterAdmissionRules"):
    assert stale not in policy, f"stale field survived provider reset: {stale}"
PY
  assert_decision "us-docker.pkg.dev/minisky/unlisted/app@sha256:reset" allow \
    "default admission rule allows image"
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}" TF_IN_AUTOMATION=1
unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT
unset GOOGLE_CLOUD_QUOTA_PROJECT GOOGLE_CLOUD_UNIVERSE_DOMAIN GOOGLE_CREDENTIALS
unset GOOGLE_ACCESS_TOKEN GOOGLE_OAUTH_ACCESS_TOKEN CLOUDSDK_AUTH_ACCESS_TOKEN
unset GOOGLE_IMPERSONATE_SERVICE_ACCOUNT CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT
unset CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE CLOUDSDK_CORE_ACCOUNT CLOUDSDK_CORE_PROJECT
unset GOOGLE_GHA_CREDS_PATH GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES

start_daemon apply
set_vars
tf init -backend-config="path=${tf_state}" -input=false -lockfile=readonly
assert_provider_contract
tf validate
tf apply -auto-approve -input=false \
  -target='google_binary_authorization_policy.phase25[0]' "${tf_vars[@]}"
assert_configured_policy
assert_decision "gcr.io/minisky-phase25/allowed/app@sha256:allow" allow \
  "image matched admission whitelist"
assert_decision "us-docker.pkg.dev/minisky/blocked/app@sha256:deny" deny \
  "default admission rule denies image"
assert_no_drift
stop_daemon

start_daemon restart
set_vars
assert_configured_policy
assert_no_drift
verify_matching_import
verify_stale_import_and_reconcile
tf destroy -auto-approve -input=false \
  -target='google_binary_authorization_policy.phase25[0]' "${tf_vars[@]}"
assert_provider_reset
stop_daemon

start_daemon reset-restart
set_vars
assert_provider_reset
stop_daemon
echo "Phase 25 Binary Authorization Terraform lifecycle passed; local advisory evaluation can block MiniSky Cloud Deploy rollouts for enforced DENY and permits audit-only decisions, but is not production or GKE admission security."
