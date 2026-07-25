#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE10_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Docker-backed integration without MINISKY_PHASE10_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3; do
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

lock_dir="${TMPDIR:-/tmp}/minisky-phase10-artifact-integration.lock"
if ! mkdir "${lock_dir}" 2>/dev/null; then
  echo "Another MiniSky Phase-10 Artifact integration run is active." >&2
  exit 1
fi

work_dir="$(mktemp -d)"
profile="phase10-artifact-integration-$$"
minisky_pid=""

cleanup() {
  exit_code=$?
  trap - EXIT INT TERM
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}" 2>/dev/null || true
    wait "${minisky_pid}" 2>/dev/null || true
  fi
  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f "${container}" >/dev/null 2>&1 || true
  done < <(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true)
  network_manager="$(docker network inspect --format '{{index .Labels "managed-by"}}' minisky-net 2>/dev/null || true)"
  network_profile="$(docker network inspect --format '{{index .Labels "minisky.profile"}}' minisky-net 2>/dev/null || true)"
  if [[ "${network_manager}" == "minisky" && "${network_profile}" == "${profile}" ]]; then
    docker network rm minisky-net >/dev/null 2>&1 || true
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

api_port="${MINISKY_PHASE10_API_PORT:-$(free_port)}"
ui_port="${MINISKY_PHASE10_UI_PORT:-$(free_port)}"
gateway="http://127.0.0.1:${api_port}"
artifact_api="${gateway}/_minisky/artifactregistry/v1"
project_id="phase10-artifact-project"
location="us-central1"
repository_id="phase10-artifacts"
repository_name="projects/${project_id}/locations/${location}/repositories/${repository_id}"
repository_url="${artifact_api}/${repository_name}"

mkdir -p "${work_dir}/home"
go build -trimpath -o "${work_dir}/minisky" ./cmd/minisky
HOME="${work_dir}/home" MINISKY_PROFILE="${profile}" "${work_dir}/minisky" start \
  --port "${api_port}" \
  --ui-port "${ui_port}" >"${work_dir}/minisky.log" 2>&1 &
minisky_pid=$!

ready_url="${artifact_api}/projects/${project_id}/locations/${location}/repositories"
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

poll_operation() {
  local operation_name=$1
  local response_file="${work_dir}/operation.json"
  for _ in {1..80}; do
    curl --fail --silent --show-error \
      "${artifact_api}/${operation_name}" >"${response_file}"
    result="$(python3 - "${response_file}" <<'PY'
import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
if value.get("error"):
    raise SystemExit("operation failed: " + json.dumps(value["error"], sort_keys=True))
print("done" if value.get("done") else "pending")
PY
)"
    if [[ "${result}" == "done" ]]; then
      return
    fi
    sleep 0.1
  done
  echo "Operation did not complete: ${operation_name}" >&2
  return 1
}

create_file="${work_dir}/create.json"
curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d '{"format":"DOCKER","description":"MiniSky deterministic artifact gate","labels":{"managed_by":"phase10_gate"}}' \
  "${ready_url}?repositoryId=${repository_id}" >"${create_file}"
create_operation="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["name"])' "${create_file}")"
poll_operation "${create_operation}"

curl --fail --silent --show-error "${repository_url}" >"${work_dir}/repository.json"
python3 - "${work_dir}/repository.json" "${repository_name}" <<'PY'
import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
if value.get("name") != sys.argv[2] or value.get("format") != "DOCKER":
    raise SystemExit(f"unexpected repository: {value!r}")
PY

# Listing packages cold-starts the profile-owned registry:2 backend.
curl --fail --silent --show-error "${repository_url}/packages" >/dev/null
registry_profile="$(docker inspect --format '{{index .Config.Labels "minisky.profile"}}' minisky-artifact-registry)"
registry_manager="$(docker inspect --format '{{index .Config.Labels "managed-by"}}' minisky-artifact-registry)"
if [[ "${registry_profile}" != "${profile}" || "${registry_manager}" != "minisky" ]]; then
  echo "Artifact Registry backend ownership labels do not match this run." >&2
  exit 1
fi
registry_address="$(docker port minisky-artifact-registry 5000/tcp)"
case "${registry_address}" in
  127.0.0.1:*) ;;
  *)
    echo "Artifact Registry backend is not loopback-published: ${registry_address}" >&2
    exit 1
    ;;
esac
registry="http://${registry_address}"
image_path="${repository_id}/tiny"

python3 - "${work_dir}" <<'PY'
import pathlib,sys
root=pathlib.Path(sys.argv[1])
root.joinpath("config.json").write_bytes(b"{}")
root.joinpath("layer.tar").write_bytes(b"minisky phase10 artifact\n")
PY

upload_blob() {
  local file=$1
  local digest
  local location_header
  local upload_url
  digest="$(python3 -c 'import hashlib,sys; print("sha256:"+hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "${file}")"
  curl --silent --show-error --dump-header "${work_dir}/upload.headers" \
    --output /dev/null --request POST "${registry}/v2/${image_path}/blobs/uploads/"
  location_header="$(python3 - "${work_dir}/upload.headers" <<'PY'
import sys
for line in open(sys.argv[1], encoding="iso-8859-1"):
    if line.lower().startswith("location:"):
        print(line.split(":", 1)[1].strip())
        break
else:
    raise SystemExit("registry upload response omitted Location")
PY
)"
  if [[ "${location_header}" == /* ]]; then
    upload_url="${registry}${location_header}"
  else
    upload_url="${location_header}"
  fi
  if [[ "${upload_url}" == *\?* ]]; then
    upload_url="${upload_url}&digest=${digest}"
  else
    upload_url="${upload_url}?digest=${digest}"
  fi
  curl --fail --silent --show-error --request PUT \
    -H 'Content-Type: application/octet-stream' \
    --data-binary "@${file}" "${upload_url}" >/dev/null
  printf '%s' "${digest}"
}

config_digest="$(upload_blob "${work_dir}/config.json")"
layer_digest="$(upload_blob "${work_dir}/layer.tar")"
python3 - "${work_dir}/manifest.json" "${config_digest}" "${layer_digest}" <<'PY'
import json,sys
manifest={
    "schemaVersion":2,
    "mediaType":"application/vnd.docker.distribution.manifest.v2+json",
    "config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":2,"digest":sys.argv[2]},
    "layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":25,"digest":sys.argv[3]}],
}
open(sys.argv[1],"w",encoding="utf-8").write(json.dumps(manifest,separators=(",",":")))
PY
curl --fail --silent --show-error --request PUT \
  -H 'Content-Type: application/vnd.docker.distribution.manifest.v2+json' \
  --data-binary "@${work_dir}/manifest.json" \
  "${registry}/v2/${image_path}/manifests/gate" >/dev/null

curl --fail --silent --show-error "${repository_url}/packages" >"${work_dir}/packages.json"
curl --fail --silent --show-error "${repository_url}/packages/tiny/versions" >"${work_dir}/versions.json"
python3 - "${work_dir}/packages.json" "${work_dir}/versions.json" "${repository_name}" <<'PY'
import json,sys
packages=json.load(open(sys.argv[1], encoding="utf-8"))["packages"]
versions=json.load(open(sys.argv[2], encoding="utf-8"))["versions"]
package_name=sys.argv[3]+"/packages/tiny"
if [item.get("name") for item in packages] != [package_name]:
    raise SystemExit(f"unexpected packages: {packages!r}")
if [item.get("name") for item in versions] != [package_name+"/versions/gate"]:
    raise SystemExit(f"unexpected versions: {versions!r}")
PY

manifest_headers="${work_dir}/manifest.headers"
curl --fail --silent --show-error --max-time 10 --head --dump-header "${manifest_headers}" \
  -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
  "${registry}/v2/${image_path}/manifests/gate" >/dev/null
manifest_digest="$(python3 - "${manifest_headers}" <<'PY'
import sys
for line in open(sys.argv[1], encoding="iso-8859-1"):
    if line.lower().startswith("docker-content-digest:"):
        print(line.split(":", 1)[1].strip())
        break
else:
    raise SystemExit("registry manifest response omitted Docker-Content-Digest")
PY
)"
delete_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --request DELETE "${registry}/v2/${image_path}/manifests/${manifest_digest}")"
if [[ "${delete_status}" != "202" ]]; then
  echo "Registry v2 manifest delete returned HTTP ${delete_status}." >&2
  exit 1
fi
curl --fail --silent --show-error "${repository_url}/packages/tiny/versions" >"${work_dir}/versions-after-delete.json"
python3 - "${work_dir}/versions-after-delete.json" <<'PY'
import json,sys
if json.load(open(sys.argv[1], encoding="utf-8"))["versions"]:
    raise SystemExit("deleted manifest version is still listed")
PY

delete_file="${work_dir}/delete.json"
curl --fail --silent --show-error --request DELETE "${repository_url}" >"${delete_file}"
delete_operation="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["name"])' "${delete_file}")"
poll_operation "${delete_operation}"
repository_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' "${repository_url}")"
if [[ "${repository_status}" != "404" ]]; then
  echo "Deleted repository returned HTTP ${repository_status}, expected 404." >&2
  exit 1
fi

kill -TERM "${minisky_pid}"
wait "${minisky_pid}"
minisky_pid=""
remaining_containers="$(docker ps -aq \
  --filter "label=managed-by=minisky" \
  --filter "label=minisky.profile=${profile}")"
if [[ -n "${remaining_containers}" ]]; then
  echo "Artifact gate left an owned container behind." >&2
  exit 1
fi
if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Artifact gate left the owned MiniSky network behind." >&2
  exit 1
fi

echo "Phase-10 Artifact Registry integration passed with complete owned-resource cleanup."
