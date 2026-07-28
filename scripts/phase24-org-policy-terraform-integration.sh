#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

[[ "${MINISKY_PHASE24_ORG_POLICY_TERRAFORM_INTEGRATION:-}" == "1" ]] ||
  { echo "Refusing Organization Policy Terraform integration without explicit opt-in." >&2; exit 2; }
for command in curl go python3 terraform; do command -v "${command}" >/dev/null || exit 1; done
lock="${TMPDIR:-/tmp}/minisky-phase24-org-policy-terraform-integration.lock"
mkdir "${lock}" 2>/dev/null || { echo "Another Organization Policy gate is active." >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"; home="${work}/home"; state_root="${work}/state"; tf_data="${work}/tfdata"
profile="phase24-orgpolicy-tf-$$"; project="phase24-orgpolicy-tf-$$"
policy="projects/${project}/policies/compute.disableSerialPortAccess"
pid=""; watchdog_pid=""; gateway=""
mkdir -p "${home}" "${state_root}" "${tf_data}"
cleanup() {
  local status=$?; trap - EXIT INT TERM
  [[ -n "${pid}" ]] && kill -TERM "${pid}" >/dev/null 2>&1 || true
  [[ -n "${pid}" ]] && wait "${pid}" >/dev/null 2>&1 || true
  [[ -n "${watchdog_pid}" ]] && kill -TERM "${watchdog_pid}" >/dev/null 2>&1 || true
  rm -rf "${work}"; rmdir "${lock}" 2>/dev/null || true; exit "${status}"
}
trap cleanup EXIT INT TERM
( sleep 420; echo "Organization Policy gate exceeded 7 minutes." >&2; kill -TERM "$$" ) & watchdog_pid=$!
free_port() { python3 - <<'PY'
import socket
with socket.socket() as s: s.bind(("127.0.0.1",0)); print(s.getsockname()[1])
PY
}
start() {
  local p u; p="$(free_port)"; u="$(free_port)"; gateway="http://127.0.0.1:${p}"
  HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
    "${work}/minisky" start --port "${p}" --ui-port "${u}" >"${work}/minisky.log" 2>&1 & pid=$!
  for _ in {1..120}; do curl -fsS "${gateway}/healthz" >/dev/null 2>&1 && return; sleep .25; done
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${work}/minisky.log" >&2; return 1
}
stop() { kill -TERM "${pid}"; wait "${pid}"; pid=""; }
vars() { tfvars=(-var="enable_phase24_org_policy=true" -var="minisky_endpoint=${gateway}" -var="profile=local" -var="project_id=${project}"); }
observe() {
  curl -fsS "${gateway}/_minisky/orgpolicy/v2/${policy}" >"${work}/policy.json"
  python3 - "${work}/policy.json" "${policy}" <<'PY'
import json,sys
p=json.load(open(sys.argv[1])); assert p["name"]==sys.argv[2]
assert p["spec"]["rules"][0]["enforce"] is True and p["spec"].get("updateTime")
PY
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d "{\"resource\":\"projects/${project}\",\"constraint\":\"constraints/compute.disableSerialPortAccess\",\"ancestors\":[]}" \
    "${gateway}/_minisky/orgpolicy/v2/policies:evaluate" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["enforced"] is True and d["source"].startswith("projects/")'
}
nodrift() { set +e; terraform -chdir="${root}/terraform" plan -detailed-exitcode -input=false -target='google_org_policy_policy.phase24[0]' "${tfvars[@]}"; local r=$?; set -e; [[ "${r}" == 0 ]]; }
missing() {
  [[ "$(curl -s -o /dev/null -w '%{http_code}' "${gateway}/_minisky/orgpolicy/v2/${policy}")" == 404 ]]
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d "{\"resource\":\"projects/${project}\",\"constraint\":\"constraints/compute.disableSerialPortAccess\",\"ancestors\":[]}" \
    "${gateway}/_minisky/orgpolicy/v2/policies:evaluate" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["enforced"] is False and d["source"]=="constraintDefault"'
  curl -fsS "${gateway}/_minisky/orgpolicy/v2/projects/${project}/policies" |
    python3 -c 'import json,sys; assert not json.load(sys.stdin).get("policies", [])'
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}"; unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT
start; vars
terraform -chdir="${root}/terraform" init -backend-config="path=${work}/terraform.tfstate" -input=false -lockfile=readonly
terraform -chdir="${root}/terraform" apply -auto-approve -input=false -target='google_org_policy_policy.phase24[0]' "${tfvars[@]}"
observe; nodrift; stop
start; vars; observe; nodrift
terraform -chdir="${root}/terraform" state rm -backup="${work}/state-before-import.backup" 'google_org_policy_policy.phase24[0]'
terraform -chdir="${root}/terraform" import -input=false "${tfvars[@]}" 'google_org_policy_policy.phase24[0]' "${policy}"
terraform -chdir="${root}/terraform" apply -auto-approve -input=false -target='google_org_policy_policy.phase24[0]' "${tfvars[@]}"
nodrift
terraform -chdir="${root}/terraform" destroy -auto-approve -input=false -target='google_org_policy_policy.phase24[0]' "${tfvars[@]}"
missing; stop; start; vars; missing; stop
echo "Phase 24 Organization Policy Terraform lifecycle passed; enforcement is bounded local simulation, not production security or compliance."
