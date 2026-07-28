#!/usr/bin/env bash
set -Eeuo pipefail
export TF_IN_AUTOMATION=1 CHECKPOINT_DISABLE=1

postgres_image="postgres:15.8-bookworm@sha256:eb3747f5d0a92195ca486d2f15d9a4ee5e9461b0332fe87fbc59069490a5c659"
if [[ "${1:-}" == "--print-required-images" && "$#" -eq 1 ]]; then
  printf '%s\n' "${postgres_image}"
  exit 0
fi
[[ "$#" -eq 0 ]] || { echo "Usage: $0 [--print-required-images]" >&2; exit 2; }

if [[ "${MINISKY_PHASE20_ALLOYDB_TERRAFORM_INTEGRATION:-}" != "1" ||
      "${MINISKY_PHASE20_ALLOYDB_DOCKER_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing AlloyDB Terraform integration without both explicit opt-ins." >&2
  exit 2
fi
for command in curl docker go python3 terraform; do
  command -v "${command}" >/dev/null || { echo "Required command not found: ${command}" >&2; exit 1; }
done
docker info >/dev/null
docker image inspect "${postgres_image}" >/dev/null

shared_lock="${TMPDIR:-/tmp}/minisky-net-integration.lock"
phase_lock="${TMPDIR:-/tmp}/minisky-phase20-alloydb-terraform-integration.lock"
mkdir "${shared_lock}" 2>/dev/null || { echo "Another MiniSky Docker integration is active." >&2; exit 1; }
mkdir "${phase_lock}" 2>/dev/null || { rmdir "${shared_lock}"; echo "Another AlloyDB gate is active." >&2; exit 1; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"; home="${work}/home"; state_root="${work}/state"; tf_data="${work}/tfdata"
profile="phase20-alloydb-tf-$$"; project="phase20-alloydb-tf-$$"; region="us-central1"
cluster="phase20-alloydb"; instance="primary"
cluster_name="projects/${project}/locations/${region}/clusters/${cluster}"
instance_name="${cluster_name}/instances/${instance}"
pid=""; watchdog_pid=""; gateway=""; volumes="${work}/volumes"
mkdir -p "${home}" "${state_root}" "${tf_data}"; : >"${volumes}"

owned_containers() { docker ps -aq --filter "label=minisky.profile=${profile}"; }
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  [[ -n "${pid}" ]] && kill -TERM "${pid}" >/dev/null 2>&1 || true
  [[ -n "${pid}" ]] && wait "${pid}" >/dev/null 2>&1 || true
  [[ -n "${watchdog_pid}" ]] && kill -TERM "${watchdog_pid}" >/dev/null 2>&1 || true
  while IFS= read -r c; do [[ -n "${c}" ]] && docker rm -f -v "${c}" >/dev/null 2>&1 || true; done < <(owned_containers)
  while IFS= read -r v; do [[ -n "${v}" ]] && docker volume rm "${v}" >/dev/null 2>&1 || true; done <"${volumes}"
  docker network rm minisky-net >/dev/null 2>&1 || true
  rm -rf "${work}"; rmdir "${phase_lock}" 2>/dev/null || true; rmdir "${shared_lock}" 2>/dev/null || true
  exit "${status}"
}
trap cleanup EXIT INT TERM
( sleep 600; echo "AlloyDB gate exceeded 10 minutes." >&2; kill -TERM "$$" ) & watchdog_pid=$!
docker network inspect minisky-net >/dev/null 2>&1 && { echo "Refusing existing minisky-net." >&2; exit 1; }
[[ -z "$(docker ps -aq --filter 'label=managed-by=minisky')" ]] || { echo "Refusing existing MiniSky containers." >&2; exit 1; }
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
vars() { tfvars=(-var="enable_phase20_alloydb_resources=true" -var="minisky_endpoint=${gateway}" -var="profile=local" -var="project_id=${project}" -var="region=${region}" -var="phase20_alloydb_cluster_id=${cluster}" -var="phase20_alloydb_instance_id=${instance}"); }
observe() {
  curl -fsS "${gateway}/_minisky/alloydb/v1/${cluster_name}" >"${work}/cluster.json"
  curl -fsS "${gateway}/_minisky/alloydb/v1/${instance_name}" >"${work}/instance.json"
  python3 - "${work}/cluster.json" "${work}/instance.json" "${cluster_name}" "${instance_name}" <<'PY'
import json,sys
c=json.load(open(sys.argv[1])); i=json.load(open(sys.argv[2]))
assert c["name"]==sys.argv[3] and c["state"]=="READY" and c["networkConfig"]["network"]
assert i["name"]==sys.argv[4] and i["state"]=="READY" and i["instanceType"]=="PRIMARY" and i["ipAddress"]
PY
}
backend() {
  local c; c="$(docker ps -q --filter "label=minisky.profile=${profile}" --filter "label=minisky.service=alloydb")"
  [[ -n "${c}" ]]; docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{println .Name}}{{end}}{{end}}' "${c}" >>"${volumes}"
  docker exec "${c}" psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c 'create table if not exists minisky_gate(v text);' -c "insert into minisky_gate values ('alloydb-provider');" >/dev/null
  docker exec "${c}" psql -U postgres -d postgres -Atc "select count(*) from minisky_gate where v='alloydb-provider'" | awk '$1 >= 1 {ok=1} END {exit !ok}'
}
nodrift() { set +e; terraform -chdir="${root}/terraform" plan -detailed-exitcode -input=false -target='google_alloydb_instance.phase20[0]' -target='google_alloydb_cluster.phase20[0]' "${tfvars[@]}"; local r=$?; set -e; [[ "${r}" == 0 ]]; }

go build -trimpath -o "${work}/minisky" ./cmd/minisky
export HOME="${home}" TF_DATA_DIR="${tf_data}"; unset GOOGLE_APPLICATION_CREDENTIALS CLOUDSDK_CONFIG GOOGLE_CLOUD_PROJECT GCLOUD_PROJECT
start; vars
terraform -chdir="${root}/terraform" init -backend-config="path=${work}/terraform.tfstate" -input=false -lockfile=readonly
terraform -chdir="${root}/terraform" apply -auto-approve -input=false -target='google_alloydb_instance.phase20[0]' "${tfvars[@]}"
observe; backend; nodrift; endpoint_before="$(python3 -c 'import json; print(json.load(open("'"${work}/instance.json"'"))["ipAddress"])')"; stop
start; vars; observe; backend; [[ "$(python3 -c 'import json; print(json.load(open("'"${work}/instance.json"'"))["ipAddress"])')" == "${endpoint_before}" ]]; nodrift
terraform -chdir="${root}/terraform" state rm -backup="${work}/state-before-import.backup" 'google_alloydb_instance.phase20[0]' 'google_alloydb_cluster.phase20[0]'
terraform -chdir="${root}/terraform" import -input=false "${tfvars[@]}" 'google_alloydb_cluster.phase20[0]' "${cluster_name}"
terraform -chdir="${root}/terraform" import -input=false "${tfvars[@]}" 'google_alloydb_instance.phase20[0]' "${instance_name}"
terraform -chdir="${root}/terraform" apply -auto-approve -input=false -target='google_alloydb_instance.phase20[0]' "${tfvars[@]}"
nodrift
terraform -chdir="${root}/terraform" destroy -auto-approve -input=false -target='google_alloydb_instance.phase20[0]' -target='google_alloydb_cluster.phase20[0]' "${tfvars[@]}"
[[ "$(curl -s -o /dev/null -w '%{http_code}' "${gateway}/_minisky/alloydb/v1/${instance_name}")" == 404 ]]
[[ "$(curl -s -o /dev/null -w '%{http_code}' "${gateway}/_minisky/alloydb/v1/${cluster_name}")" == 404 ]]
[[ -z "$(owned_containers)" ]]; stop; start; vars
[[ "$(curl -s -o /dev/null -w '%{http_code}' "${gateway}/_minisky/alloydb/v1/${cluster_name}")" == 404 ]]
stop
echo "Phase 20 AlloyDB Terraform lifecycle passed; networking and production AlloyDB semantics remain unsupported."
