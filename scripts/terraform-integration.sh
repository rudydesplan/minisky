#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_TERRAFORM_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Docker-backed integration without MINISKY_TERRAFORM_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3 rg terraform; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done

enable_fidelity_cloudsql="${MINISKY_TERRAFORM_CLOUDSQL:-0}"
enable_fidelity_gke="${MINISKY_TERRAFORM_GKE:-0}"
enable_java_smoke="${MINISKY_TERRAFORM_JAVA_SMOKE:-0}"
for value in "${enable_fidelity_cloudsql}" "${enable_fidelity_gke}" "${enable_java_smoke}"; do
  if [[ "${value}" != "0" && "${value}" != "1" ]]; then
    echo "MINISKY_TERRAFORM_CLOUDSQL, MINISKY_TERRAFORM_GKE, and MINISKY_TERRAFORM_JAVA_SMOKE must be 0 or 1." >&2
    exit 2
  fi
done
if [[ "${enable_fidelity_gke}" == "1" ]]; then
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "Required GKE lifecycle command not found: kubectl" >&2
    exit 1
  fi
fi

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
lock_dir="${TMPDIR:-/tmp}/minisky-terraform-integration.lock"

if ! mkdir "${lock_dir}" 2>/dev/null; then
  echo "Another MiniSky Terraform integration run is active." >&2
  exit 1
fi

work_dir="$(mktemp -d)"
minisky_pid=""
profile="terraform-integration-$$"
kind_bin=""

owned_kind_backend() {
  python3 - "${work_dir}/home/.minisky/state/profiles/${profile}" "${profile}" <<'PY'
import glob
import json
import pathlib
import re
import sys

profile_dir = pathlib.Path(sys.argv[1])
profile = sys.argv[2]
expected = (profile, "local-dev-project", "us-central1-c", "minisky-fidelity")
names = set()

def consider(ownership):
    if not isinstance(ownership, dict):
        return
    identity = (
        ownership.get("profile"), ownership.get("project"),
        ownership.get("zone"), ownership.get("cluster"),
    )
    name = ownership.get("backendName", "")
    if identity == expected and re.fullmatch(r"minisky-owned-[0-9a-f]{32}", name):
        names.add(name)

state_path = profile_dir / "state.json"
if state_path.exists():
    document = json.loads(state_path.read_text())
    metadata = document.get("entries", {}).get("gke/metadata", {})
    for ownership in metadata.get("kubeconfigOwnerships", {}).values():
        consider(ownership)

for path in glob.glob(str(profile_dir / "runtime/gke/.intent-*")):
    envelope = json.loads(pathlib.Path(path).read_text())
    consider(envelope.get("intent", {}).get("ownership"))

if len(names) > 1:
    raise SystemExit("multiple nonce-owned Kind backends found for the integration identity")
if names:
    print(next(iter(names)))
PY
}

cleanup() {
  exit_code=$?
  cleanup_failed=0
  trap - EXIT INT TERM

  if [[ "${exit_code}" -ne 0 && -f "${work_dir}/minisky.log" ]]; then
    echo "MiniSky integration log (last 200 lines):" >&2
    python3 - "${work_dir}/minisky.log" <<'PY' >&2
import pathlib
import sys

lines = pathlib.Path(sys.argv[1]).read_text(errors="replace").splitlines()
print("\n".join(lines[-200:]))
PY
  fi

  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}" 2>/dev/null || true
    wait "${minisky_pid}" 2>/dev/null || true
  fi

  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f "${container}" >/dev/null 2>&1 || true
  done < <(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true)
  if [[ "${enable_fidelity_gke}" == "1" && -x "${kind_bin}" ]]; then
    if ! owned_cluster="$(owned_kind_backend)"; then
      echo "Failed to resolve exact nonce-owned Kind backend during cleanup." >&2
      cleanup_failed=1
    elif ! kind_clusters="$("${kind_bin}" get clusters)"; then
      echo "Failed to list Kind clusters during cleanup." >&2
      cleanup_failed=1
    elif [[ -n "${owned_cluster}" ]]; then
      if rg -Fx "${owned_cluster}" <<<"${kind_clusters}" >/dev/null; then
        if ! "${kind_bin}" delete cluster --name "${owned_cluster}"; then
          echo "Failed to delete exact owned Kind backend ${owned_cluster}." >&2
          cleanup_failed=1
        elif ! post_delete_clusters="$("${kind_bin}" get clusters)"; then
          echo "Failed to verify exact owned Kind backend deletion." >&2
          cleanup_failed=1
        elif rg -Fx "${owned_cluster}" <<<"${post_delete_clusters}" >/dev/null; then
          echo "Exact owned Kind backend still exists after deletion: ${owned_cluster}." >&2
          cleanup_failed=1
        fi
      fi
    elif rg '^minisky-owned-[0-9a-f]{32}$' <<<"${kind_clusters}" >/dev/null; then
      echo "Refusing ambiguous Kind cleanup without durable ownership identity." >&2
      cleanup_failed=1
    fi
  fi
  network_manager="$(docker network inspect --format '{{index .Labels "managed-by"}}' minisky-net 2>/dev/null || true)"
  network_profile="$(docker network inspect --format '{{index .Labels "minisky.profile"}}' minisky-net 2>/dev/null || true)"
  if [[ "${network_manager}" == "minisky" && "${network_profile}" == "${profile}" ]]; then
    docker network rm minisky-net >/dev/null 2>&1 || true
  fi

  chmod -R u+w "${work_dir}" 2>/dev/null || true
  rm -rf "${work_dir}" || true
  rmdir "${lock_dir}" 2>/dev/null || true
  if [[ "${cleanup_failed}" -ne 0 ]]; then
    exit_code=1
  fi
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

api_port="${MINISKY_TERRAFORM_API_PORT:-$(free_port)}"
ui_port="${MINISKY_TERRAFORM_UI_PORT:-$(free_port)}"
gateway="http://127.0.0.1:${api_port}"
project_id="local-dev-project"
secondary_project_id="local-secondary-project"
dataset_id="minisky_terraform"
table_id="events"
service_account_id="minisky-terraform"
service_account_email="${service_account_id}@${project_id}.iam.gserviceaccount.com"
storage_bucket_name="minisky-terraform-$$"
enable_phase15_resources="${MINISKY_TERRAFORM_PHASE15:-0}"
if [[ "${enable_phase15_resources}" != "0" && "${enable_phase15_resources}" != "1" ]]; then
  echo "MINISKY_TERRAFORM_PHASE15 must be 0 or 1." >&2
  exit 2
fi
tf_phase15_enabled=false
if [[ "${enable_phase15_resources}" == "1" ]]; then
  tf_phase15_enabled=true
fi
enable_phase10_resources="${MINISKY_TERRAFORM_PHASE10:-0}"
if [[ "${enable_phase10_resources}" != "0" && "${enable_phase10_resources}" != "1" ]]; then
  echo "MINISKY_TERRAFORM_PHASE10 must be 0 or 1." >&2
  exit 2
fi
tf_phase10_enabled=false
if [[ "${enable_phase10_resources}" == "1" ]]; then
  tf_phase10_enabled=true
fi
enable_phase10_artifact_resources="${MINISKY_TERRAFORM_PHASE10_ARTIFACT:-0}"
if [[ "${enable_phase10_artifact_resources}" != "0" && "${enable_phase10_artifact_resources}" != "1" ]]; then
  echo "MINISKY_TERRAFORM_PHASE10_ARTIFACT must be 0 or 1." >&2
  exit 2
fi
tf_phase10_artifact_enabled=false
if [[ "${enable_phase10_artifact_resources}" == "1" ]]; then
  tf_phase10_artifact_enabled=true
fi
tf_vars=(
  -var="enable_fidelity_cloudsql_resources=$([[ "${enable_fidelity_cloudsql}" == "1" ]] && echo true || echo false)"
  -var="enable_fidelity_gke_resources=$([[ "${enable_fidelity_gke}" == "1" ]] && echo true || echo false)"
  -var="enable_phase10_artifact_resources=${tf_phase10_artifact_enabled}"
  -var="enable_phase10_lb_resources=${tf_phase10_enabled}"
  -var="enable_phase15_resources=${tf_phase15_enabled}"
  -var="minisky_endpoint=${gateway}"
  -var="storage_bucket_name=${storage_bucket_name}"
  -var="profile=local"
)

mkdir -p "${work_dir}/home"
if [[ "${enable_fidelity_gke}" == "1" ]]; then
  installer="${work_dir}/install-kind.go"
  cat >"${installer}" <<'EOF'
package main

import (
	"context"
	"log"

	"minisky/pkg/orchestrator"
)

func main() {
	if err := orchestrator.InstallToolDependency(context.Background(), "kind"); err != nil {
		log.Fatal(err)
	}
}
EOF
  (
    cd "${repository_root}"
    HOME="${work_dir}/home" GOCACHE="$(go env GOCACHE)" GOMODCACHE="$(go env GOMODCACHE)" \
      go run "${installer}"
  )
  kind_bin="${work_dir}/home/.minisky/bin/kind"
  test -x "${kind_bin}"
  "${kind_bin}" version | rg -F 'kind v0.22.0'
  PATH="$(dirname "${kind_bin}"):${PATH}"
  export PATH
fi
go build -trimpath -o "${work_dir}/minisky" ./cmd/minisky

if [[ "${enable_fidelity_gke}" == "1" ]]; then
  export MINISKY_GKE_BACKEND=kind
fi
HOME="${work_dir}/home" MINISKY_PROFILE="${profile}" "${work_dir}/minisky" start \
  --port "${api_port}" \
  --ui-port "${ui_port}" >"${work_dir}/minisky.log" 2>&1 &
minisky_pid=$!

ready_url="${gateway}/_minisky/bigquery/bigquery/v2/projects/${project_id}/datasets"
for _ in {1..60}; do
  if curl --fail --silent --show-error "${ready_url}" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${minisky_pid}" 2>/dev/null; then
    echo "MiniSky exited during startup:" >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${work_dir}/minisky.log" >&2
    exit 1
  fi
  sleep 1
done
curl --fail --silent --show-error "${ready_url}" >/dev/null
curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"projectId\":\"${secondary_project_id}\",\"displayName\":\"MiniSky integration secondary\"}" \
  "${gateway}/_minisky/cloudresourcemanager/v3/projects" >/dev/null

export TF_DATA_DIR="${work_dir}/terraform-data"
terraform -chdir="${terraform_dir}" init \
  -backend-config="path=${work_dir}/terraform.tfstate" \
  -input=false \
  -lockfile=readonly
terraform -chdir="${terraform_dir}" validate
terraform -chdir="${terraform_dir}" apply \
  -auto-approve \
  -input=false \
  "${tf_vars[@]}"

assert_json_value() {
  local url=$1
  local expression=$2
  local expected=$3
  local response_file="${work_dir}/response.json"
  local status

  status="$(curl --globoff --silent --show-error --output "${response_file}" --write-out '%{http_code}' "${url}")"
  if [[ "${status}" != "200" ]]; then
    echo "Expected HTTP 200 from ${url}, received ${status}:" >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${response_file}" >&2
    return 1
  fi

  python3 - "${response_file}" "${expression}" "${expected}" <<'PY'
import json
import sys

path, expression, expected = sys.argv[1:]
value = json.loads(open(path, encoding="utf-8").read())
for component in expression.split("."):
    value = value[int(component)] if component.isdigit() else value[component]
if str(value) != expected:
    raise SystemExit(f"{expression} was {value!r}, expected {expected!r}")
PY
}

dataset_url="${gateway}/_minisky/bigquery/bigquery/v2/projects/${project_id}/datasets/${dataset_id}"
secondary_dataset_url="${gateway}/_minisky/bigquery/bigquery/v2/projects/${secondary_project_id}/datasets/${dataset_id}"
table_url="${gateway}/_minisky/bigquery/bigquery/v2/projects/${project_id}/datasets/${dataset_id}/tables/${table_id}"
service_account_url="${gateway}/_minisky/iam/v1/projects/${project_id}/serviceAccounts/${service_account_email}"
storage_bucket_url="${gateway}/_minisky/storage/storage/v1/b/${storage_bucket_name}"
pubsub_topic_name="projects/${project_id}/topics/minisky-cross-project"
pubsub_subscription_name="projects/${secondary_project_id}/subscriptions/minisky-cross-project"
pubsub_topic_url="${gateway}/_minisky/pubsub/v1/${pubsub_topic_name}"
pubsub_subscription_url="${gateway}/_minisky/pubsub/v1/${pubsub_subscription_name}"
redis_instance_url="${gateway}/_minisky/redis/v1/projects/${project_id}/locations/us-central1/instances/minisky-terraform"
spanner_instance_url="${gateway}/_minisky/spanner/v1/projects/${project_id}/instances/minisky-terraform"
spanner_database_url="${spanner_instance_url}/databases/minisky-terraform"
compute_base_url="${gateway}/_minisky/compute/compute/v1/projects/${project_id}"
phase10_firewall_url="${compute_base_url}/global/firewalls/minisky-phase10-http"
phase10_instance_url="${compute_base_url}/zones/us-central1-a/instances/minisky-phase10-http"
phase10_group_url="${compute_base_url}/zones/us-central1-a/instanceGroups/minisky-phase10-http"
phase10_health_url="${compute_base_url}/global/healthChecks/minisky-phase10-http"
phase10_backend_url="${compute_base_url}/global/backendServices/minisky-phase10-http"
phase10_url_map_url="${compute_base_url}/global/urlMaps/minisky-phase10-http"
phase10_proxy_url="${compute_base_url}/global/targetHttpProxies/minisky-phase10-http"
phase10_forwarding_url="${compute_base_url}/global/forwardingRules/minisky-phase10-http"
phase10_traffic_url="${phase10_forwarding_url}/proxy/"
phase10_artifact_url="${gateway}/_minisky/artifactregistry/v1/projects/${project_id}/locations/us-central1/repositories/minisky-phase10"
cloudsql_instance_url="${gateway}/_minisky/sqladmin/v1/projects/${project_id}/instances/minisky-fidelity"
cloudsql_database_url="${cloudsql_instance_url}/databases/app"
cloudsql_users_url="${cloudsql_instance_url}/users"
gke_cluster_url="${gateway}/_minisky/container/v1/projects/${project_id}/locations/us-central1-c/clusters/minisky-fidelity"

assert_json_value "${dataset_url}" "datasetReference.datasetId" "${dataset_id}"
assert_json_value "${secondary_dataset_url}" "datasetReference.projectId" "${secondary_project_id}"
assert_json_value "${table_url}" "tableReference.tableId" "${table_id}"
assert_json_value "${service_account_url}" "email" "${service_account_email}"
assert_json_value "${storage_bucket_url}" "name" "${storage_bucket_name}"
assert_json_value "${pubsub_topic_url}" "name" "${pubsub_topic_name}"
assert_json_value "${pubsub_subscription_url}" "topic" "${pubsub_topic_name}"
if [[ "${enable_phase15_resources}" == "1" ]]; then
  assert_json_value "${redis_instance_url}" "name" "projects/${project_id}/locations/us-central1/instances/minisky-terraform"
  assert_json_value "${spanner_instance_url}" "name" "projects/${project_id}/instances/minisky-terraform"
  assert_json_value "${spanner_database_url}" "name" "projects/${project_id}/instances/minisky-terraform/databases/minisky-terraform"
fi
if [[ "${enable_phase10_artifact_resources}" == "1" ]]; then
  assert_json_value "${phase10_artifact_url}" "name" "projects/${project_id}/locations/us-central1/repositories/minisky-phase10"
fi
if [[ "${enable_phase10_resources}" == "1" ]]; then
  assert_json_value "${phase10_firewall_url}" "name" "minisky-phase10-http"
  assert_json_value "${phase10_instance_url}" "status" "RUNNING"
  assert_json_value "${phase10_group_url}" "size" "1"
  assert_json_value "${phase10_health_url}" "name" "minisky-phase10-http"
  assert_json_value "${phase10_backend_url}" "name" "minisky-phase10-http"
  assert_json_value "${phase10_url_map_url}" "name" "minisky-phase10-http"
  assert_json_value "${phase10_proxy_url}" "name" "minisky-phase10-http"
  assert_json_value "${phase10_forwarding_url}" "name" "minisky-phase10-http"
  traffic_file="${work_dir}/phase10-traffic.txt"
  traffic_status="$(curl --silent --show-error --output "${traffic_file}" --write-out '%{http_code}' "${phase10_traffic_url}")"
  traffic_response="$(<"${traffic_file}")"
  if [[ "${traffic_status}" != "200" || "${traffic_response}" != *"Welcome to nginx!"* ]]; then
    echo "Expected HTTP 200 with real backend content through the Phase-10 forwarding-rule proxy; received ${traffic_status}." >&2
    printf '%s\n' "${traffic_response}" >&2
    exit 1
  fi
fi
if [[ "${enable_fidelity_cloudsql}" == "1" ]]; then
  assert_json_value "${cloudsql_instance_url}" "state" "RUNNABLE"
  assert_json_value "${cloudsql_database_url}" "name" "app"
  assert_json_value "${cloudsql_users_url}" "items.0.name" "app_user"
fi
if [[ "${enable_fidelity_gke}" == "1" ]]; then
  assert_json_value "${gke_cluster_url}" "status" "RUNNING"
fi

export MINISKY_ENDPOINT="${gateway}"
export MINISKY_PROJECT_ID="${project_id}"
export MINISKY_SECONDARY_PROJECT_ID="${secondary_project_id}"
export MINISKY_PUBSUB_PRIMARY_TOPIC="${pubsub_topic_name}"
export MINISKY_PUBSUB_SECONDARY_SUBSCRIPTION="${pubsub_subscription_name}"
(cd "${repository_root}" && go run ./sdk-smoke/go)
python3 -m venv "${work_dir}/python-venv"
"${work_dir}/python-venv/bin/python" -m pip install \
  --disable-pip-version-check \
  --quiet \
  -r "${repository_root}/sdk-smoke/python/requirements.txt"
"${work_dir}/python-venv/bin/python" "${repository_root}/sdk-smoke/python/smoke.py"
if [[ "${enable_java_smoke}" == "1" ]]; then
  MINISKY_JAVA_SDK_SMOKE=1 \
    MINISKY_JAVA_CONTAINER=1 \
    MINISKY_ENDPOINT="${gateway}" \
    MINISKY_PROJECT_ID="${project_id}" \
    MINISKY_JAVA_BUCKET="minisky-java-${profile}" \
    "${repository_root}/scripts/java-sdk-smoke.sh"
fi

set +e
terraform -chdir="${terraform_dir}" plan \
  -detailed-exitcode \
  -input=false \
  "${tf_vars[@]}"
plan_exit=$?
set -e
if [[ "${plan_exit}" != "0" ]]; then
  echo "Expected a no-drift plan (exit 0), received exit ${plan_exit}." >&2
  exit "${plan_exit}"
fi

terraform -chdir="${terraform_dir}" destroy \
  -auto-approve \
  -input=false \
  "${tf_vars[@]}"

destroyed_urls=(
  "${table_url}"
  "${dataset_url}"
  "${secondary_dataset_url}"
  "${service_account_url}"
  "${storage_bucket_url}"
  "${pubsub_subscription_url}"
  "${pubsub_topic_url}"
)
if [[ "${enable_phase15_resources}" == "1" ]]; then
  destroyed_urls+=("${redis_instance_url}" "${spanner_database_url}" "${spanner_instance_url}")
fi
if [[ "${enable_phase10_resources}" == "1" ]]; then
  destroyed_urls+=(
    "${phase10_firewall_url}"
    "${phase10_instance_url}"
    "${phase10_group_url}"
    "${phase10_health_url}"
    "${phase10_backend_url}"
    "${phase10_url_map_url}"
    "${phase10_proxy_url}"
    "${phase10_forwarding_url}"
  )
fi
if [[ "${enable_phase10_artifact_resources}" == "1" ]]; then
  destroyed_urls+=("${phase10_artifact_url}")
fi
if [[ "${enable_fidelity_cloudsql}" == "1" ]]; then
  destroyed_urls+=("${cloudsql_database_url}" "${cloudsql_instance_url}")
fi
if [[ "${enable_fidelity_gke}" == "1" ]]; then
  destroyed_urls+=("${gke_cluster_url}")
fi
for url in "${destroyed_urls[@]}"; do
  status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' "${url}")"
  if [[ "${status}" != "404" ]]; then
    echo "Expected destroyed resource ${url} to return HTTP 404, received ${status}." >&2
    exit 1
  fi
done

remaining_compute_containers="$(docker ps -aq \
  --filter "label=managed-by=minisky" \
  --filter "label=minisky.profile=${profile}" \
  --filter "label=minisky.service=compute-instance")"
if [[ "${enable_phase10_resources}" == "1" && -n "${remaining_compute_containers}" ]]; then
  echo "Phase-10 destroy left an owned Compute container behind." >&2
  exit 1
fi
