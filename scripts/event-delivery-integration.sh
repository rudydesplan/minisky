#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_EVENT_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Docker-backed event integration without MINISKY_EVENT_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go pack python3; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done
docker info >/dev/null

# New fixed-network harnesses share this lock. The profile-owned Docker network
# reservation below also excludes older harnesses that only check minisky-net.
lock_dir="${TMPDIR:-/tmp}/minisky-net-integration.lock"
if ! mkdir "${lock_dir}" 2>/dev/null; then
  echo "Another MiniSky integration using minisky-net is active." >&2
  exit 1
fi

work_dir="$(mktemp -d)"
profile="event-integration-$(basename "${work_dir}")"
daemon_pid=""
handler_pid=""
started_at="${SECONDS}"
pack_version="$(pack version 2>&1)"

owned_containers() {
  docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true
}

owned_container_names() {
  docker ps -a --format '{{.Names}}' \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true
}

cleanup() {
  local status
  local cleanup_failed=0
  local container
  local network_manager
  local network_profile
  status=$?
  trap - EXIT INT TERM

  if (( status != 0 )); then
    if [[ -f "${work_dir}/daemon.log" ]]; then
      echo "MiniSky event integration daemon log:" >&2
      python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"))' \
        "${work_dir}/daemon.log" >&2
    fi
    while IFS= read -r container; do
      if [[ -n "${container}" ]]; then
        echo "Container log (${container}):" >&2
        docker logs "${container}" >&2 || true
      fi
    done < <(owned_container_names)
  fi

  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill -TERM "${daemon_pid}" 2>/dev/null || true
    wait "${daemon_pid}" 2>/dev/null || true
  fi
  if [[ -n "${handler_pid}" ]] && kill -0 "${handler_pid}" 2>/dev/null; then
    kill -TERM "${handler_pid}" 2>/dev/null || true
    wait "${handler_pid}" 2>/dev/null || true
  fi

  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f "${container}" >/dev/null 2>&1 || true
  done < <(owned_containers)

  network_manager="$(docker network inspect --format '{{index .Labels "managed-by"}}' minisky-net 2>/dev/null || true)"
  network_profile="$(docker network inspect --format '{{index .Labels "minisky.profile"}}' minisky-net 2>/dev/null || true)"
  if [[ "${network_manager}" == "minisky" && "${network_profile}" == "${profile}" ]]; then
    docker network rm minisky-net >/dev/null 2>&1 || true
  fi

  if [[ -n "$(owned_containers)" ]]; then
    echo "Owned MiniSky containers remain after cleanup." >&2
    cleanup_failed=1
  fi
  network_manager="$(docker network inspect --format '{{index .Labels "managed-by"}}' minisky-net 2>/dev/null || true)"
  network_profile="$(docker network inspect --format '{{index .Labels "minisky.profile"}}' minisky-net 2>/dev/null || true)"
  if [[ "${network_manager}" == "minisky" && "${network_profile}" == "${profile}" ]]; then
    echo "Owned minisky-net remains after cleanup." >&2
    cleanup_failed=1
  fi
  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    echo "MiniSky process remains after cleanup." >&2
    cleanup_failed=1
  fi
  if [[ -n "${handler_pid}" ]] && kill -0 "${handler_pid}" 2>/dev/null; then
    echo "Handler process remains after cleanup." >&2
    cleanup_failed=1
  fi

  rm -rf "${work_dir}"
  rmdir "${lock_dir}" 2>/dev/null || cleanup_failed=1
  if (( cleanup_failed != 0 )); then
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

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

# Reserve the fixed network atomically before starting any process. MiniSky will
# reuse it only because the ownership labels exactly match this run's profile.
if ! docker network create --driver bridge \
  --label managed-by=minisky \
  --label "minisky.profile=${profile}" \
  minisky-net >/dev/null; then
  echo "Failed to reserve minisky-net for the event integration profile." >&2
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

api_port="$(free_port)"
ui_port="$(free_port)"
handler_port="$(free_port)"
gateway="http://127.0.0.1:${api_port}"
project="local-dev-project"
location="us-central1"
deliveries="${work_dir}/deliveries.jsonl"
mkdir -p "${work_dir}/home" "${work_dir}/state"

cat >"${work_dir}/handler.py" <<'PY'
import http.server
import json
import os
import threading

lock = threading.Lock()
attempts = {}

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length).decode("utf-8", "strict")
        with lock:
            attempt = attempts.get(self.path, 0) + 1
            attempts[self.path] = attempt
            if self.path == "/scheduler":
                status = 204
            elif self.path == "/tasks/success":
                status = 204
            elif self.path == "/tasks/retry":
                status = 503 if attempt == 1 else 204
            elif self.path == "/tasks/terminal":
                status = 500
            else:
                status = 404
            record = {"path": self.path, "body": body, "attempt": attempt, "status": status}
            with open(os.environ["DELIVERIES"], "a", encoding="utf-8") as output:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
                output.flush()
                os.fsync(output.fileno())
        self.send_response(status)
        self.end_headers()

    def log_message(self, *_):
        pass

http.server.ThreadingHTTPServer(
    ("127.0.0.1", int(os.environ["HANDLER_PORT"])), Handler
).serve_forever()
PY
DELIVERIES="${deliveries}" HANDLER_PORT="${handler_port}" \
  python3 "${work_dir}/handler.py" >"${work_dir}/handler.log" 2>&1 &
handler_pid=$!

poll() {
  local timeout=$1
  local description=$2
  local deadline=$((SECONDS + timeout))
  shift 2
  until "$@"; do
    if (( SECONDS >= deadline )); then
      echo "Timed out after ${timeout}s waiting for ${description}." >&2
      return 1
    fi
    sleep 0.25
  done
}

gateway_ready() {
  curl -fsS -H "Host: iam.googleapis.com" \
    "${gateway}/v1/projects/${project}/serviceAccounts" >/dev/null 2>&1
}

service_ready() {
  local name=$1
  local response
  response="$(curl -fsS -H "Host: run.googleapis.com" \
    "${gateway}/v2/projects/${project}/locations/${location}/services/${name}" 2>/dev/null)" || return 1
  python3 -c '
import json, sys
body = json.load(sys.stdin)
assert body["name"] == sys.argv[1]
assert body["reconciling"] is False
assert isinstance(body["uri"], str) and body["uri"]
' "projects/${project}/locations/${location}/services/${name}" <<<"${response}" 2>/dev/null
}

function_ready() {
  local name=$1
  local response
  response="$(curl -fsS -H "Host: cloudfunctions.googleapis.com" \
    "${gateway}/v2/projects/${project}/locations/${location}/functions/${name}" 2>/dev/null)" || return 1
  python3 -c '
import json, sys
body = json.load(sys.stdin)
assert body["name"] == sys.argv[1]
assert body["state"] == "ACTIVE"
assert isinstance(body["url"], str) and body["url"]
' "projects/${project}/locations/${location}/functions/${name}" <<<"${response}" 2>/dev/null
}

task_has_status() {
  local task_name=$1
  local expected_status=$2
  local response
  response="$(curl -fsS -H "Host: cloudtasks.googleapis.com" \
    "${gateway}/v2/${task_name}" 2>/dev/null)" || return 1
  python3 -c '
import json, sys
body = json.load(sys.stdin)
assert "task" not in body
assert body["name"] == sys.argv[1]
assert body["status"] == sys.argv[2]
' "${task_name}" "${expected_status}" <<<"${response}" 2>/dev/null
}

serverless_container() {
  local resource_type=$1
  local resource_name=$2
  local collection="${resource_type}s"
  local canonical="projects/${project}/locations/${location}/${collection}/${resource_name}"
  local containers
  containers="$(docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" \
    --filter "label=minisky.service=serverless" \
    --filter "label=minisky.resource-type=${resource_type}" \
    --filter "label=minisky.resource=${canonical}")"
  [[ "$(wc -w <<<"${containers}")" -eq 1 ]] || return 1
  docker inspect --format '{{.Name}}' "${containers}" | sed 's#^/##'
}

container_has_marker() {
  local resource_type=$1
  local resource_name=$2
  local marker=$3
  local container
  container="$(serverless_container "${resource_type}" "${resource_name}")" || return 1
  docker logs "${container}" 2>&1 | grep -Fq "${marker}"
}

serverless_container_absent() {
  local resource_type=$1
  local resource_name=$2
  ! serverless_container "${resource_type}" "${resource_name}" >/dev/null
}

resource_missing() {
  local host=$1
  local url=$2
  local status
  status="$(curl -sS -o /dev/null -w '%{http_code}' -H "Host: ${host}" "${url}")"
  [[ "${status}" == "404" ]]
}

go build -trimpath -o "${work_dir}/minisky" ./cmd/minisky
HOME="${work_dir}/home" MINISKY_STATE_DIR="${work_dir}/state" MINISKY_PROFILE="${profile}" \
  MINISKY_RUNTIME_PROFILE=full MINISKY_SERVERLESS_BACKEND=buildpacks \
  MINISKY_PORT="${api_port}" MINISKY_UI_PORT="${ui_port}" \
  "${work_dir}/minisky" start >"${work_dir}/daemon.log" 2>&1 &
daemon_pid=$!

poll 120 "gateway readiness" gateway_ready

scheduler_job="projects/${project}/locations/${location}/jobs/event-gate"
python3 - "${scheduler_job}" "${handler_port}" <<'PY' >"${work_dir}/scheduler.json"
import json, sys
name, port = sys.argv[1:]
print(json.dumps({
    "name": name,
    "schedule": "0 0 1 1 *",
    "httpTarget": {
        "uri": f"http://127.0.0.1:{port}/scheduler",
        "httpMethod": "POST",
        "body": "scheduler-body",
    },
}))
PY
curl -fsS -X POST -H "Content-Type: application/json" -H "Host: cloudscheduler.googleapis.com" \
  --data-binary "@${work_dir}/scheduler.json" \
  "${gateway}/v1/projects/${project}/locations/${location}/jobs" >/dev/null
curl -fsS -X POST -H "Host: cloudscheduler.googleapis.com" \
  "${gateway}/v1/${scheduler_job}:run" >/dev/null

queue="projects/${project}/locations/${location}/queues/event-gate"
curl -fsS -X POST -H "Content-Type: application/json" -H "Host: cloudtasks.googleapis.com" \
  -d '{"name":"'"${queue}"'","retryConfig":{"maxAttempts":2,"minBackoff":"50ms","maxBackoff":"50ms","maxDoublings":0}}' \
  "${gateway}/v2/projects/${project}/locations/${location}/queues" >"${work_dir}/queue.json"
python3 - "${queue}" "${work_dir}/queue.json" <<'PY'
import json, pathlib, sys
body = json.loads(pathlib.Path(sys.argv[2]).read_text())
assert body["name"] == sys.argv[1]
assert body["state"] == "RUNNING"
assert body["retryConfig"]["maxAttempts"] == 2
PY

create_task() {
  local task_id=$1
  local path=$2
  local body=$3
  local task_name="${queue}/tasks/${task_id}"
  python3 - "${task_name}" "${handler_port}" "${path}" "${body}" <<'PY' >"${work_dir}/${task_id}.json"
import base64, json, sys
name, port, path, body = sys.argv[1:]
print(json.dumps({"task": {
    "name": name,
    "httpRequest": {
        "url": f"http://127.0.0.1:{port}{path}",
        "httpMethod": "POST",
        "body": base64.b64encode(body.encode()).decode(),
    },
}}))
PY
  curl -fsS -X POST -H "Content-Type: application/json" -H "Host: cloudtasks.googleapis.com" \
    --data-binary "@${work_dir}/${task_id}.json" \
    "${gateway}/v2/${queue}/tasks" >"${work_dir}/${task_id}-created.json"
  python3 - "${task_name}" "${work_dir}/${task_id}-created.json" <<'PY'
import json, pathlib, sys
body = json.loads(pathlib.Path(sys.argv[2]).read_text())
assert body["name"] == sys.argv[1]
assert body["status"] == "PENDING"
PY
}

create_task success /tasks/success success-body
create_task retry /tasks/retry retry-body
create_task terminal /tasks/terminal terminal-body

success_task="${queue}/tasks/success"
retry_task="${queue}/tasks/retry"
terminal_task="${queue}/tasks/terminal"
poll 30 "successful Cloud Task completion" task_has_status "${success_task}" COMPLETED
poll 30 "retried Cloud Task completion" task_has_status "${retry_task}" COMPLETED
poll 30 "terminal Cloud Task failure" task_has_status "${terminal_task}" FAILED

curl -fsS -H "Host: cloudtasks.googleapis.com" "${gateway}/v2/${retry_task}" >"${work_dir}/retry-final.json"
curl -fsS -H "Host: cloudtasks.googleapis.com" "${gateway}/v2/${terminal_task}" >"${work_dir}/terminal-final.json"
curl -fsS -H "Host: cloudtasks.googleapis.com" "${gateway}/v2/${queue}/tasks" >"${work_dir}/tasks-list.json"
python3 - "${retry_task}" "${terminal_task}" "${work_dir}" <<'PY'
import json, pathlib, sys
retry_name, terminal_name, root = sys.argv[1:]
root = pathlib.Path(root)
retry = json.loads((root / "retry-final.json").read_text())
terminal = json.loads((root / "terminal-final.json").read_text())
listed = json.loads((root / "tasks-list.json").read_text())
assert retry["name"] == retry_name
assert retry["status"] == "COMPLETED"
assert retry["attemptCount"] == 2
assert retry["lastStatusCode"] == 204
assert retry.get("lastError", "") == ""
assert terminal["name"] == terminal_name
assert terminal["status"] == "FAILED"
assert terminal["attemptCount"] == 2
assert terminal["lastStatusCode"] == 500
assert terminal["lastError"] == "HTTP status 500"
assert len(terminal["lastError"]) <= 256
assert set(listed) == {"tasks"}
tasks = {task["name"]: task for task in listed["tasks"]}
assert set(tasks) == {
    retry_name,
    terminal_name,
    retry_name.rsplit("/", 1)[0] + "/success",
}
assert tasks[retry_name]["attemptCount"] == 2
assert tasks[terminal_name]["attemptCount"] == 2
PY

sleep 0.5
python3 - "${deliveries}" <<'PY'
import json, pathlib, sys
records = [json.loads(line) for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
expected = {
    "/scheduler": [("scheduler-body", 1, 204)],
    "/tasks/success": [("success-body", 1, 204)],
    "/tasks/retry": [("retry-body", 1, 503), ("retry-body", 2, 204)],
    "/tasks/terminal": [("terminal-body", 1, 500), ("terminal-body", 2, 500)],
}
for path, attempts in expected.items():
    actual = [
        (record["body"], record["attempt"], record["status"])
        for record in records if record["path"] == path
    ]
    assert actual == attempts, (path, actual, attempts)
assert len(records) == sum(map(len, expected.values())), records
PY

deploy_function() {
  local name=$1
  local event_type=$2
  local resource=$3
  local marker=$4
  python3 - "${name}" "${event_type}" "${resource}" "${marker}" "${project}" "${location}" \
    <<'PY' >"${work_dir}/${name}.json"
import json, sys
name, event_type, resource, marker, project, location = sys.argv[1:]
code = (
    "def handler(event, context=None):\n"
    f"    print({marker!r}, flush=True)\n"
    "    return ('ok', 200)\n"
)
print(json.dumps({
    "type": "function",
    "name": name,
    "runtime": "python312",
    "entryPoint": "handler",
    "project": project,
    "location": location,
    "code": code,
    "eventTrigger": {"eventType": event_type, "resource": resource},
}))
PY
  curl -fsS -X POST -H "Content-Type: application/json" -H "Host: cloudfunctions.googleapis.com" \
    --data-binary "@${work_dir}/${name}.json" "${gateway}/v2/deploy" >/dev/null
  # Google buildpacks may download the builder/runtime on a cold machine.
  poll 600 "${name} function activation" function_ready "${name}"
}

deploy_service() {
  local name=$1
  local event_type=$2
  local resource=$3
  local marker=$4
  local payload_kind=$5
  python3 - "${name}" "${event_type}" "${resource}" "${marker}" "${payload_kind}" \
    "${project}" "${location}" <<'PY' >"${work_dir}/${name}.json"
import json, sys
name, event_type, resource, marker, payload_kind, project, location = sys.argv[1:]
code = f'''import http.server
import json
import os

class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        payload = json.loads(self.rfile.read(length))
        valid = (
            payload == {{"bucket": "events-service-bucket", "name": "service-event.txt"}}
            if {payload_kind!r} == "storage"
            else payload == {{"messages": [{{"data": "c2VydmljZS1wYXlsb2Fk"}}]}}
        )
        if self.path != "/" or not valid:
            self.send_response(400)
            self.end_headers()
            return
        print({marker!r}, flush=True)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *_):
        pass

http.server.ThreadingHTTPServer(("0.0.0.0", int(os.environ["PORT"])), Handler).serve_forever()
'''
print(json.dumps({
    "type": "service",
    "name": name,
    "runtime": "python312",
    "project": project,
    "location": location,
    "code": code,
    "eventTrigger": {"eventType": event_type, "resource": resource},
}))
PY
  curl -fsS -X POST -H "Content-Type: application/json" -H "Host: run.googleapis.com" \
    --data-binary "@${work_dir}/${name}.json" "${gateway}/v2/deploy" >/dev/null
  # Service builds use the same bounded cold-build allowance as functions.
  poll 600 "${name} service activation" service_ready "${name}"
}

storage_function="event-storage-function"
storage_service="event-storage-service"
storage_bucket="events-service-bucket"
storage_function_marker="MINISKY_STORAGE_FUNCTION_EVENT_OK"
storage_service_marker="MINISKY_STORAGE_SERVICE_EVENT_OK"
deploy_function "${storage_function}" google.storage.object.finalize "${storage_bucket}" "${storage_function_marker}"
deploy_service "${storage_service}" google.storage.object.finalize "${storage_bucket}" "${storage_service_marker}" storage

curl -fsS -X POST -H "Content-Type: application/json" -H "Host: storage.googleapis.com" \
  -d '{"name":"'"${storage_bucket}"'"}' "${gateway}/storage/v1/b?project=${project}" >/dev/null
curl -fsS -X POST -H "Host: storage.googleapis.com" --data-binary 'service-payload' \
  "${gateway}/upload/storage/v1/b/${storage_bucket}/o?uploadType=media&name=service-event.txt" >/dev/null

pubsub_function="event-pubsub-function"
pubsub_service="event-pubsub-service"
pubsub_topic="events-service-topic"
pubsub_function_marker="MINISKY_PUBSUB_FUNCTION_EVENT_OK"
pubsub_service_marker="MINISKY_PUBSUB_SERVICE_EVENT_OK"
deploy_function "${pubsub_function}" google.cloud.pubsub.topic.v1.messagePublished "${pubsub_topic}" "${pubsub_function_marker}"
deploy_service "${pubsub_service}" google.cloud.pubsub.topic.v1.messagePublished "${pubsub_topic}" "${pubsub_service_marker}" pubsub

curl -fsS -X PUT -H "Content-Type: application/json" -H "Host: pubsub.googleapis.com" -d '{}' \
  "${gateway}/v1/projects/${project}/topics/${pubsub_topic}" >/dev/null
curl -fsS -X POST -H "Content-Type: application/json" -H "Host: pubsub.googleapis.com" \
  -d '{"messages":[{"data":"c2VydmljZS1wYXlsb2Fk"}]}' \
  "${gateway}/v1/projects/${project}/topics/${pubsub_topic}:publish" >/dev/null

poll 60 "Storage function marker" container_has_marker \
  function "${storage_function}" "${storage_function_marker}"
poll 60 "Storage service validated marker" container_has_marker \
  service "${storage_service}" "${storage_service_marker}"
poll 60 "Pub/Sub function marker" container_has_marker \
  function "${pubsub_function}" "${pubsub_function_marker}"
poll 60 "Pub/Sub service validated marker" container_has_marker \
  service "${pubsub_service}" "${pubsub_service_marker}"

for service in "${storage_service}" "${pubsub_service}"; do
  curl -fsS -X DELETE -H "Host: run.googleapis.com" \
    "${gateway}/v2/projects/${project}/locations/${location}/services/${service}" >/dev/null
  poll 30 "${service} API deletion" resource_missing run.googleapis.com \
    "${gateway}/v2/projects/${project}/locations/${location}/services/${service}"
  poll 30 "${service} container deletion" serverless_container_absent service "${service}"
done

# Service deletion must not disturb the independently deployed functions.
function_ready "${storage_function}"
function_ready "${pubsub_function}"
docker inspect "$(serverless_container function "${storage_function}")" >/dev/null
docker inspect "$(serverless_container function "${pubsub_function}")" >/dev/null

echo "Event delivery integration passed in $((SECONDS - started_at))s (Pack: ${pack_version})"
