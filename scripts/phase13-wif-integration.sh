#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE13_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Phase-13 WIF integration without MINISKY_PHASE13_INTEGRATION=1." >&2
  exit 2
fi

enterprise_controls=0
if [[ "${MINISKY_PHASE13_ENTERPRISE_CONTROLS:-}" == "1" ]]; then
  enterprise_controls=1
fi

for command in curl docker go openssl python3 terraform; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done
docker info >/dev/null

if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Refusing to disturb an existing MiniSky Docker network." >&2
  exit 1
fi
while IFS= read -r container_name; do
  case "${container_name}" in
    minisky-*)
      echo "Refusing to disturb existing MiniSky container: ${container_name}" >&2
      exit 1
      ;;
  esac
done < <(docker ps -a --format '{{.Names}}')

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
terraform_dir="${repository_root}/terraform"
phase_label="phase13"
if [[ "${enterprise_controls}" == "1" ]]; then
  phase_label="phase17-enterprise"
fi
lock_dir="${TMPDIR:-/tmp}/minisky-wif-integration.lock"
if ! mkdir "${lock_dir}" 2>/dev/null; then
  echo "Another MiniSky ${phase_label} WIF integration run is active." >&2
  exit 1
fi

work_dir="$(mktemp -d)"
state_root="${work_dir}/state"
profile="${phase_label}-wif-$$"
minisky_pid=""
active_log=""
enterprise_audit_active=0
enterprise_quota_json='{"routes":{"bigquery.googleapis.com /bigquery/v2/projects/{id}/datasets":{"limit":2,"window":"10m"}}}'

cleanup_profile_docker() {
  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f "${container}" >/dev/null 2>&1 || true
  done < <(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true)

  local network_manager
  local network_profile
  network_manager="$(docker network inspect --format '{{index .Labels "managed-by"}}' minisky-net 2>/dev/null || true)"
  network_profile="$(docker network inspect --format '{{index .Labels "minisky.profile"}}' minisky-net 2>/dev/null || true)"
  if [[ "${network_manager}" == "minisky" && "${network_profile}" == "${profile}" ]]; then
    docker network rm minisky-net >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}" 2>/dev/null || true
    wait "${minisky_pid}" 2>/dev/null || true
  fi
  cleanup_profile_docker
  if [[ "${exit_code}" != "0" ]]; then
    for log_file in "${work_dir}"/minisky-*.log; do
      if [[ -f "${log_file}" ]]; then
        echo "MiniSky daemon log (${log_file##*/}):" >&2
        python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${log_file}" >&2
      fi
    done
  fi
  rm -rf "${work_dir}"
  rmdir "${lock_dir}" 2>/dev/null || true
  exit "${exit_code}"
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

if [[ "${enterprise_controls}" == "1" ]]; then
  api_port="${MINISKY_PHASE17_API_PORT:-$(free_port)}"
  ui_port="${MINISKY_PHASE17_UI_PORT:-$(free_port)}"
else
  api_port="${MINISKY_PHASE13_API_PORT:-$(free_port)}"
  ui_port="${MINISKY_PHASE13_UI_PORT:-$(free_port)}"
fi
gateway="http://127.0.0.1:${api_port}"
dashboard="http://127.0.0.1:${ui_port}"
project_id="local-dev-project"
pool_id="minisky-phase13"
provider_id="minisky-oidc"
subject="minisky-phase13-caller"
issuer_uri="https://issuer.minisky.invalid"
token_audience="minisky-phase13"
delegate_email="minisky-wif-delegate@${project_id}.iam.gserviceaccount.com"
target_email="minisky-wif-target@${project_id}.iam.gserviceaccount.com"
pool_name="projects/${project_id}/locations/global/workloadIdentityPools/${pool_id}"
provider_name="${pool_name}/providers/${provider_id}"
sts_audience="//iam.googleapis.com/${provider_name}"
scope="https://www.googleapis.com/auth/cloud-platform"

mkdir -p "${work_dir}/home" "${state_root}"
openssl genpkey -quiet -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "${work_dir}/subject-key.pem"
chmod 600 "${work_dir}/subject-key.pem"
modulus_hex="$(openssl rsa -in "${work_dir}/subject-key.pem" -noout -modulus | cut -d= -f2)"
python3 - "${modulus_hex}" "${work_dir}/jwks.json" <<'PY'
import base64
import json
import pathlib
import sys

modulus = bytes.fromhex(sys.argv[1])
jwks = {
    "keys": [{
        "kty": "RSA",
        "kid": "phase13-ephemeral",
        "alg": "RS256",
        "use": "sig",
        "n": base64.urlsafe_b64encode(modulus).rstrip(b"=").decode(),
        "e": "AQAB",
    }]
}
pathlib.Path(sys.argv[2]).write_text(json.dumps(jwks, separators=(",", ":")), encoding="utf-8")
PY
jwks_json="$(<"${work_dir}/jwks.json")"

go build -trimpath -o "${work_dir}/minisky" ./cmd/minisky

start_minisky() {
  local mode=$1
  active_log="${work_dir}/minisky-${mode}.log"
  if [[ "${mode}" == "strict" ]]; then
    if [[ "${enterprise_controls}" == "1" && "${enterprise_audit_active}" == "1" ]]; then
      MINISKY_IAM_MODE=strict MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
        MINISKY_AUDIT_ENABLED=true MINISKY_AUDIT_STRICT=true MINISKY_QUOTAS_JSON="${enterprise_quota_json}" \
        HOME="${work_dir}/home" "${work_dir}/minisky" start \
        --port "${api_port}" --ui-port "${ui_port}" >"${active_log}" 2>&1 &
    else
      MINISKY_IAM_MODE=strict MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
        HOME="${work_dir}/home" "${work_dir}/minisky" start \
        --port "${api_port}" --ui-port "${ui_port}" >"${active_log}" 2>&1 &
    fi
  else
    if [[ "${enterprise_controls}" == "1" && "${enterprise_audit_active}" == "1" ]]; then
      MINISKY_IAM_MODE='' MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
        MINISKY_AUDIT_ENABLED=true MINISKY_AUDIT_STRICT=true MINISKY_QUOTAS_JSON="${enterprise_quota_json}" \
        HOME="${work_dir}/home" "${work_dir}/minisky" start \
        --port "${api_port}" --ui-port "${ui_port}" >"${active_log}" 2>&1 &
    else
      MINISKY_IAM_MODE='' MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
        HOME="${work_dir}/home" "${work_dir}/minisky" start \
        --port "${api_port}" --ui-port "${ui_port}" >"${active_log}" 2>&1 &
    fi
  fi
  minisky_pid=$!

  for _ in {1..60}; do
    if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "${minisky_pid}" 2>/dev/null; then
      echo "MiniSky exited during ${mode} startup." >&2
      return 1
    fi
    sleep 1
  done
  curl --fail --silent --show-error "${gateway}/healthz" >/dev/null
}

stop_minisky() {
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}"
    wait "${minisky_pid}"
  fi
  minisky_pid=""
}

assert_json_value() {
  local file=$1
  local expression=$2
  local expected=$3
  python3 - "${file}" "${expression}" "${expected}" <<'PY'
import json
import pathlib
import sys

value = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
for component in sys.argv[2].split("."):
    value = value[component]
if str(value) != sys.argv[3]:
    raise SystemExit(f"{sys.argv[2]} was {value!r}, expected {sys.argv[3]!r}")
PY
}

assert_get_value() {
  local url=$1
  local expression=$2
  local expected=$3
  local response_file="${work_dir}/gateway-response.json"
  local status
  status="$(curl --globoff --silent --show-error --output "${response_file}" --write-out '%{http_code}' "${url}")"
  if [[ "${status}" != "200" ]]; then
    echo "Expected HTTP 200 from ${url}, received ${status}." >&2
    return 1
  fi
  assert_json_value "${response_file}" "${expression}" "${expected}"
}

assert_policy_binding() {
  local resource_url=$1
  local role=$2
  local member=$3
  local response_file="${work_dir}/policy-response.json"
  local status
  status="$(curl --silent --show-error --output "${response_file}" --write-out '%{http_code}' \
    -X POST -H 'Content-Type: application/json' --data '{}' "${resource_url}:getIamPolicy")"
  if [[ "${status}" != "200" ]]; then
    echo "Expected IAM policy HTTP 200 from ${resource_url}, received ${status}." >&2
    return 1
  fi
  python3 - "${response_file}" "${role}" "${member}" <<'PY'
import json
import pathlib
import sys
policy = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
if not any(
    binding.get("role") == sys.argv[2] and sys.argv[3] in binding.get("members", [])
    for binding in policy.get("bindings", [])
):
    raise SystemExit(f"missing {sys.argv[2]} binding for expected member")
PY
}

export TF_DATA_DIR="${work_dir}/terraform-data"
tf_targets=(
  -target=google_iam_workload_identity_pool.phase13
  -target=google_iam_workload_identity_pool_provider.phase13
  -target=google_service_account.phase13_delegate
  -target=google_service_account.phase13_target
  -target=google_service_account_iam_member.phase13_federated_caller
  -target=google_service_account_iam_member.phase13_delegation
)
tf_vars=(
  -var=enable_phase13_wif_resources=true
  -var="minisky_endpoint=${gateway}"
  -var="phase13_wif_issuer_uri=${issuer_uri}"
  -var="phase13_wif_public_jwks=${jwks_json}"
  -var="phase13_wif_subject=${subject}"
  -var="phase13_wif_token_audience=${token_audience}"
  -var=profile=local
)

start_minisky permissive
terraform -chdir="${terraform_dir}" init \
  -backend-config="path=${work_dir}/terraform.tfstate" \
  -input=false \
  -lockfile=readonly
terraform -chdir="${terraform_dir}" validate
terraform -chdir="${terraform_dir}" apply -auto-approve -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"

pool_url="${gateway}/_minisky/iam/v1/${pool_name}"
provider_url="${gateway}/_minisky/iam/v1/${provider_name}"
delegate_url="${gateway}/_minisky/iam/v1/projects/${project_id}/serviceAccounts/${delegate_email}"
target_url="${gateway}/_minisky/iam/v1/projects/${project_id}/serviceAccounts/${target_email}"
assert_get_value "${pool_url}" name "${pool_name}"
assert_get_value "${provider_url}" name "${provider_name}"
assert_get_value "${delegate_url}" email "${delegate_email}"
assert_get_value "${target_url}" email "${target_email}"
federated_principal="principal://iam.googleapis.com/${pool_name}/subject/${subject}"
assert_policy_binding "${delegate_url}" roles/iam.workloadIdentityUser "${federated_principal}"
assert_policy_binding "${target_url}" roles/iam.serviceAccountTokenCreator "serviceAccount:${delegate_email}"
project_policy_url="${gateway}/_minisky/iam/v1/projects/${project_id}"
if [[ "${enterprise_controls}" == "1" ]]; then
  project_policy_status="$(curl --silent --show-error --output "${work_dir}/project-policy-set.json" --write-out '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data "{\"policy\":{\"version\":1,\"bindings\":[{\"role\":\"roles/minisky.viewer\",\"members\":[\"${federated_principal}\"]}]}}" \
    "${project_policy_url}:setIamPolicy")"
  if [[ "${project_policy_status}" != "200" ]]; then
    echo "Expected project viewer policy HTTP 200, received ${project_policy_status}." >&2
    exit 1
  fi
  assert_policy_binding "${project_policy_url}" roles/minisky.viewer "${federated_principal}"
fi

set +e
terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"
plan_exit=$?
set -e
if [[ "${plan_exit}" != "0" ]]; then
  echo "Expected initial Phase-13 no-drift plan exit 0, received ${plan_exit}." >&2
  exit "${plan_exit}"
fi

stop_minisky
if [[ "${enterprise_controls}" == "1" ]]; then
  enterprise_audit_active=1
fi
start_minisky strict
curl --fail --silent --show-error -H "Authorization: Bearer invalid" "${pool_url}" >/dev/null 2>&1 || true

now="$(date +%s)"
expires="$((now + 600))"
python3 - "${issuer_uri}" "${subject}" "${token_audience}" "${now}" "${expires}" "${work_dir}/jwt-input" <<'PY'
import base64
import json
import pathlib
import sys

def encoded(value):
    raw = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

issuer, subject, audience, issued, expires, output = sys.argv[1:]
header = {"alg": "RS256", "kid": "phase13-ephemeral", "typ": "JWT"}
payload = {
    "iss": issuer,
    "sub": subject,
    "aud": audience,
    "iat": int(issued),
    "exp": int(expires),
}
pathlib.Path(output).write_text(f"{encoded(header)}.{encoded(payload)}", encoding="utf-8")
PY
openssl dgst -sha256 -sign "${work_dir}/subject-key.pem" \
  -out "${work_dir}/jwt-signature" "${work_dir}/jwt-input"
signature="$(python3 - "${work_dir}/jwt-signature" <<'PY'
import base64
import pathlib
import sys
print(base64.urlsafe_b64encode(pathlib.Path(sys.argv[1]).read_bytes()).rstrip(b"=").decode())
PY
)"
subject_token="$(<"${work_dir}/jwt-input").${signature}"
invalid_subject_token="$(python3 - "${subject_token}" <<'PY'
import sys
header, payload, signature = sys.argv[1].split(".")
replacement = "A" if signature[0] != "A" else "B"
print(f"{header}.{payload}.{replacement}{signature[1:]}")
PY
)"

sts_url="${gateway}/_minisky/sts/v1/token"
sts_response="${work_dir}/sts-response.json"
sts_status="$(curl --silent --show-error --output "${sts_response}" --write-out '%{http_code}' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange' \
  --data-urlencode 'requested_token_type=urn:ietf:params:oauth:token-type:access_token' \
  --data-urlencode 'subject_token_type=urn:ietf:params:oauth:token-type:jwt' \
  --data-urlencode "audience=${sts_audience}" \
  --data-urlencode "scope=${scope}" \
  --data-urlencode "subject_token=${subject_token}" \
  "${sts_url}")"
if [[ "${sts_status}" != "200" ]]; then
  echo "Expected valid WIF exchange HTTP 200, received ${sts_status}." >&2
  exit 1
fi
federated_token="$(python3 - "${sts_response}" <<'PY'
import json
import pathlib
import sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["access_token"])
PY
)"

invalid_response="${work_dir}/invalid-sts-response.json"
invalid_status="$(curl --silent --show-error --output "${invalid_response}" --write-out '%{http_code}' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=urn:ietf:params:oauth:grant-type:token-exchange' \
  --data-urlencode 'requested_token_type=urn:ietf:params:oauth:token-type:access_token' \
  --data-urlencode 'subject_token_type=urn:ietf:params:oauth:token-type:jwt' \
  --data-urlencode "audience=${sts_audience}" \
  --data-urlencode "scope=${scope}" \
  --data-urlencode "subject_token=${invalid_subject_token}" \
  "${sts_url}")"
if [[ "${invalid_status}" != "400" ]] || python3 - "${invalid_response}" "${invalid_subject_token}" <<'PY'
import pathlib
import sys
raise SystemExit(0 if sys.argv[2] in pathlib.Path(sys.argv[1]).read_text(encoding="utf-8") else 1)
PY
then
  echo "Invalid WIF signature was accepted or echoed." >&2
  exit 1
fi

credentials_url="${gateway}/_minisky/iamcredentials/v1/projects/-/serviceAccounts/${target_email}:generateAccessToken"
missing_chain_response="${work_dir}/missing-chain-response.json"
missing_chain_status="$(curl --silent --show-error --output "${missing_chain_response}" --write-out '%{http_code}' \
  -H "Authorization: Bearer ${federated_token}" \
  -H 'Content-Type: application/json' \
  --data "{\"scope\":[\"${scope}\"]}" \
  "${credentials_url}")"
if [[ "${missing_chain_status}" != "403" ]] || python3 - "${missing_chain_response}" "${federated_token}" <<'PY'
import json
import pathlib
import sys
body = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
parsed = json.loads(body)
leaked = sys.argv[2] in body or "accessToken" in parsed
raise SystemExit(0 if leaked else 1)
PY
then
  echo "Missing delegation chain was not denied safely." >&2
  exit 1
fi

credentials_response="${work_dir}/credentials-response.json"
credentials_status="$(curl --silent --show-error --output "${credentials_response}" --write-out '%{http_code}' \
  -H "Authorization: Bearer ${federated_token}" \
  -H 'Content-Type: application/json' \
  --data "{\"delegates\":[\"projects/-/serviceAccounts/${delegate_email}\"],\"scope\":[\"${scope}\"]}" \
  "${credentials_url}")"
if [[ "${credentials_status}" != "200" ]]; then
  echo "Expected delegated generateAccessToken HTTP 200, received ${credentials_status}." >&2
  exit 1
fi
target_token="$(python3 - "${credentials_response}" <<'PY'
import json
import pathlib
import sys
print(json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))["accessToken"])
PY
)"
python3 - "${target_token}" "serviceAccount:${target_email}" <<'PY'
import base64
import json
import sys
parts = sys.argv[1].split(".")
if len(parts) != 3:
    raise SystemExit("target token is not a JWT")
payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
if payload.get("sub") != sys.argv[2]:
    raise SystemExit(f"target token subject was {payload.get('sub')!r}")
PY

strict_read_status="$(curl --globoff --silent --show-error --output "${work_dir}/strict-read.json" --write-out '%{http_code}' \
  -H "Authorization: Bearer ${target_token}" "${target_url}")"
if [[ "${strict_read_status}" != "200" ]]; then
  echo "Target token could not authenticate an IAM read; received ${strict_read_status}." >&2
  exit 1
fi
assert_json_value "${work_dir}/strict-read.json" email "${target_email}"

if [[ "${enterprise_controls}" == "1" ]]; then
  dashboard_get_status="$(curl --silent --show-error --output "${work_dir}/dashboard-view.json" --write-out '%{http_code}' \
    -H "Authorization: Bearer ${federated_token}" \
    -H "X-MiniSky-Project: ${project_id}" \
    "${dashboard}/api/services")"
  if [[ "${dashboard_get_status}" != "200" ]]; then
    echo "Expected federated Dashboard view HTTP 200, received ${dashboard_get_status}." >&2
    exit 1
  fi

  sensitive_body_marker="phase17-sensitive-body-${profile}-${RANDOM}${RANDOM}"
  dashboard_manage_status="$(curl --silent --show-error --output "${work_dir}/dashboard-manage-denied.json" --write-out '%{http_code}' \
    -X POST -H "Authorization: Bearer ${federated_token}" \
    -H "X-MiniSky-Project: ${project_id}" \
    -H 'Content-Type: application/json' \
    --data "{\"marker\":\"${sensitive_body_marker}\"}" \
    "${dashboard}/api/settings")"
  if [[ "${dashboard_manage_status}" != "403" ]]; then
    echo "Expected federated Dashboard manage denial HTTP 403, received ${dashboard_manage_status}." >&2
    exit 1
  fi

  terminal_status="$(curl --silent --show-error --output "${work_dir}/dashboard-terminal-denied.json" --write-out '%{http_code}' \
    -H "Authorization: Bearer ${federated_token}" \
    -H "X-MiniSky-Project: ${project_id}" \
    "${dashboard}/api/manage/compute/terminal")"
  if [[ "${terminal_status}" != "403" ]]; then
    echo "Expected federated Dashboard terminal denial HTTP 403, received ${terminal_status}." >&2
    exit 1
  fi

  bigquery_url="${gateway}/_minisky/bigquery/bigquery/v2/projects/${project_id}/datasets"
  unauthorized_bq_status="$(curl --silent --show-error --output "${work_dir}/bigquery-unauthorized.json" --write-out '%{http_code}' \
    "${bigquery_url}")"
  if [[ "${unauthorized_bq_status}" != "401" ]]; then
    echo "Expected unauthenticated BigQuery request HTTP 401, received ${unauthorized_bq_status}." >&2
    exit 1
  fi
  for read_number in 1 2; do
    bigquery_status="$(curl --silent --show-error --output "${work_dir}/bigquery-read-${read_number}.json" --write-out '%{http_code}' \
      -H "Authorization: Bearer ${federated_token}" "${bigquery_url}")"
    if [[ "${bigquery_status}" != "200" ]]; then
      echo "Expected authorized BigQuery read ${read_number} HTTP 200, received ${bigquery_status}." >&2
      exit 1
    fi
  done
  quota_status="$(curl --silent --show-error --dump-header "${work_dir}/bigquery-quota.headers" \
    --output "${work_dir}/bigquery-quota.json" --write-out '%{http_code}' \
    -H "Authorization: Bearer ${federated_token}" "${bigquery_url}")"
  if [[ "${quota_status}" != "429" ]] || ! python3 - "${work_dir}/bigquery-quota.headers" "${work_dir}/bigquery-quota.json" <<'PY'
import json
import pathlib
import re
import sys

headers = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
body = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
retry = re.search(r"(?im)^Retry-After:\s*([0-9]+)\s*$", headers)
raise SystemExit(0 if retry and int(retry.group(1)) > 0 and body["error"]["status"] == "RESOURCE_EXHAUSTED" else 1)
PY
  then
    echo "Expected numeric Retry-After and RESOURCE_EXHAUSTED from the third authorized BigQuery read." >&2
    exit 1
  fi
fi

python3 - "${work_dir}" "${state_root}" "${repository_root}" "${subject_token}" <<'PY'
import os
import pathlib
import subprocess
import sys

work, state, repository, subject_token = sys.argv[1:]
private_key = pathlib.Path(work, "subject-key.pem").read_bytes()
subject_bytes = subject_token.encode()
excluded = {
    pathlib.Path(work, "subject-key.pem"),
    pathlib.Path(work, "jwt-input"),
    pathlib.Path(work, "jwt-signature"),
}
for root in (pathlib.Path(state), pathlib.Path(work)):
    for path in root.rglob("*"):
        if not path.is_file() or path in excluded:
            continue
        data = path.read_bytes()
        if private_key in data or subject_bytes in data:
            raise SystemExit(f"private signing material leaked into {path}")
for path in pathlib.Path(repository).rglob("*"):
    if ".git" in path.parts or not path.is_file():
        continue
    data = path.read_bytes()
    if private_key in data or subject_bytes in data:
        raise SystemExit(f"private signing material leaked into repository file {path}")
diff = subprocess.check_output(["git", "diff", "--no-ext-diff"], cwd=repository)
if private_key in diff or subject_bytes in diff:
    raise SystemExit("private signing material leaked into git diff")
terraform_state = pathlib.Path(work, "terraform.tfstate").read_text(encoding="utf-8")
json_modulus = __import__("json").loads(pathlib.Path(work, "jwks.json").read_text())["keys"][0]["n"]
if json_modulus not in terraform_state:
    raise SystemExit("expected public JWKS is absent from Terraform state")
if not any(
    json_modulus in path.read_text(encoding="utf-8")
    for path in pathlib.Path(state).rglob("*")
    if path.is_file()
):
    raise SystemExit("expected public JWKS is absent from MiniSky state")
PY

if [[ "${enterprise_controls}" == "1" ]]; then
  stop_minisky
  audit_export="${work_dir}/audit-export.json"
  MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" HOME="${work_dir}/home" \
    "${work_dir}/minisky" audit verify >/dev/null
  MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" HOME="${work_dir}/home" \
    "${work_dir}/minisky" audit export --limit 1000 "${audit_export}"
  python3 - "${audit_export}" "${federated_principal}" "${project_id}" <<'PY'
import json
import pathlib
import sys

records = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
principal, project = sys.argv[2:]
if not any(
    record.get("phase") == "complete"
    and record.get("status") == 403
    and record.get("principal") == principal
    and record.get("service") == "dashboard"
    and record.get("route") == "/{id}/{id}"
    and record.get("project") == project
    for record in records
):
    dashboard_denials = [
        {
            key: record.get(key)
            for key in ("phase", "status", "principal", "service", "route", "project")
        }
        for record in records
        if record.get("service") == "dashboard" and record.get("status") == 403
    ]
    raise SystemExit(f"missing complete federated Dashboard 403 audit record; observed {dashboard_denials!r}")
PY

  rm -f \
    "${work_dir}/subject-key.pem" \
    "${work_dir}/jwt-input" \
    "${work_dir}/jwt-signature" \
    "${work_dir}/sts-response.json" \
    "${work_dir}/credentials-response.json"
  python3 - "${work_dir}" "${state_root}" "${subject_token}" "${federated_token}" "${target_token}" "${sensitive_body_marker}" <<'PY'
import pathlib
import sys

work, state, subject, federated, target, marker = sys.argv[1:]
generated_secrets = [
    subject.encode(),
    federated.encode(),
    target.encode(),
    marker.encode(),
]
forbidden_text = [
    b"-----BEGIN PRIVATE KEY-----",
    b"Authorization",
    b"Cookie",
]
for root in (pathlib.Path(state), pathlib.Path(work)):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        data = path.read_bytes()
        for needle in generated_secrets:
            if needle and needle in data:
                raise SystemExit(f"sensitive value leaked into {path}")
        if b"\0" not in data[:8192]:
            for needle in forbidden_text:
                if needle in data:
                    raise SystemExit(f"sensitive text leaked into {path}")
PY

  tampered_state="${work_dir}/tampered-state"
  tampered_profile="${profile}-tampered"
  mkdir -p "${tampered_state}/profiles/${tampered_profile}"
  cp -R "${state_root}/profiles/${profile}/audit" "${tampered_state}/profiles/${tampered_profile}/audit"
  python3 - "${tampered_state}/profiles/${tampered_profile}/audit/mutations.jsonl" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines()
record = json.loads(lines[0])
record["method"] = "TAMPERED"
lines[0] = json.dumps(record, separators=(",", ":"))
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
  set +e
  MINISKY_STATE_DIR="${tampered_state}" MINISKY_PROFILE="${tampered_profile}" HOME="${work_dir}/home" \
    "${work_dir}/minisky" audit verify >/dev/null 2>&1
  tampered_verify_exit=$?
  set -e
  if [[ "${tampered_verify_exit}" == "0" ]]; then
    echo "Tampered isolated audit chain unexpectedly verified." >&2
    exit 1
  fi

  start_minisky permissive
  stop_minisky
  MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" HOME="${work_dir}/home" \
    "${work_dir}/minisky" audit verify >/dev/null
fi

stop_minisky
start_minisky permissive
set +e
terraform -chdir="${terraform_dir}" plan -detailed-exitcode -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"
restart_plan_exit=$?
set -e
if [[ "${restart_plan_exit}" != "0" ]]; then
  echo "Expected post-restart Phase-13 no-drift plan exit 0, received ${restart_plan_exit}." >&2
  exit "${restart_plan_exit}"
fi
terraform -chdir="${terraform_dir}" destroy -auto-approve -input=false \
  "${tf_targets[@]}" "${tf_vars[@]}"

if [[ "${enterprise_controls}" == "1" ]]; then
  project_policy_clear_status="$(curl --silent --show-error --output "${work_dir}/project-policy-clear.json" --write-out '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data '{"policy":{"version":1,"bindings":[]}}' \
    "${project_policy_url}:setIamPolicy")"
  if [[ "${project_policy_clear_status}" != "200" ]]; then
    echo "Expected project viewer policy cleanup HTTP 200, received ${project_policy_clear_status}." >&2
    exit 1
  fi
fi

for url in "${provider_url}" "${pool_url}" "${delegate_url}" "${target_url}"; do
  status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' "${url}")"
  if [[ "${status}" != "404" ]]; then
    echo "Expected destroyed resource ${url} to return HTTP 404, received ${status}." >&2
    exit 1
  fi
done

stop_minisky
cleanup_profile_docker
remaining_containers="$(docker ps -aq \
  --filter "label=managed-by=minisky" \
  --filter "label=minisky.profile=${profile}")"
if [[ -n "${remaining_containers}" ]]; then
  echo "Phase-13 integration left an owned Docker container behind." >&2
  exit 1
fi
if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
  echo "Phase-13 integration left the MiniSky process running." >&2
  exit 1
fi
network_manager="$(docker network inspect --format '{{index .Labels "managed-by"}}' minisky-net 2>/dev/null || true)"
network_profile="$(docker network inspect --format '{{index .Labels "minisky.profile"}}' minisky-net 2>/dev/null || true)"
if [[ "${network_manager}" == "minisky" && "${network_profile}" == "${profile}" ]]; then
  echo "Phase-13 integration left its Docker network behind." >&2
  exit 1
fi

if [[ "${enterprise_controls}" == "1" ]]; then
  echo "Phase-17 enterprise WIF integration passed with IAM, quota, Dashboard, and tamper-evident audit controls."
else
  echo "Phase-13 WIF integration passed with public JWKS only in Terraform and MiniSky state."
fi
