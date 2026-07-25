#!/usr/bin/env bash
set -euo pipefail

if [[ "${MINISKY_EVENT_INTEGRATION:-}" != "1" ]]; then
  echo "Set MINISKY_EVENT_INTEGRATION=1 to run event delivery integration." >&2
  exit 1
fi

for tool in curl docker go pack python3; do
  command -v "${tool}" >/dev/null || { echo "Missing required tool: ${tool}" >&2; exit 1; }
done

if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Refusing to reuse an existing minisky-net network." >&2
  exit 1
fi

tmp="$(mktemp -d)"
profile="event-integration-$$"
api_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
ui_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
handler_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
gateway="http://127.0.0.1:${api_port}"
deliveries="${tmp}/deliveries"
daemon_pid=""
handler_pid=""

cleanup() {
  local status=$?
  if [[ "${status}" != "0" && -f "${tmp}/daemon.log" ]]; then
    echo "MiniSky event integration log:" >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${tmp}/daemon.log" >&2
  fi
  [[ -n "${daemon_pid}" ]] && kill -TERM "${daemon_pid}" >/dev/null 2>&1 || true
  [[ -n "${handler_pid}" ]] && kill -TERM "${handler_pid}" >/dev/null 2>&1 || true
  wait "${daemon_pid}" >/dev/null 2>&1 || true
  wait "${handler_pid}" >/dev/null 2>&1 || true
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
  rm -rf "${tmp}"
  exit "${status}"
}
trap cleanup EXIT INT TERM

cat >"${tmp}/handler.py" <<'PY'
import http.server, os
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length).decode("utf-8", "replace")
        with open(os.environ["DELIVERIES"], "a", encoding="utf-8") as output:
            output.write(self.path + "\t" + body + "\n")
        self.send_response(204)
        self.end_headers()
    def log_message(self, *_):
        pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(os.environ["HANDLER_PORT"])), Handler).serve_forever()
PY
DELIVERIES="${deliveries}" HANDLER_PORT="${handler_port}" python3 "${tmp}/handler.py" &
handler_pid=$!

go build -trimpath -o "${tmp}/minisky" ./cmd/minisky
HOME="${tmp}/home" MINISKY_STATE_DIR="${tmp}/state" MINISKY_PROFILE="${profile}" \
  MINISKY_RUNTIME_PROFILE=full MINISKY_PORT="${api_port}" MINISKY_UI_PORT="${ui_port}" \
  "${tmp}/minisky" start >"${tmp}/daemon.log" 2>&1 &
daemon_pid=$!

poll() {
  local description=$1 command=$2 deadline=$((SECONDS + 120))
  until eval "${command}"; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for ${description}" >&2
      return 1
    fi
    sleep 0.25
  done
}

poll "gateway readiness" "curl -fsS -H 'Host: iam.googleapis.com' '${gateway}/v1/projects/local-dev-project/serviceAccounts' >/dev/null"

curl -fsS -X POST -H "Content-Type: application/json" \
  -d '{"name":"projects/local-dev-project/locations/us-central1/jobs/http","schedule":"0 0 1 1 *","httpTarget":{"uri":"http://127.0.0.1:'"${handler_port}"'/scheduler","httpMethod":"POST","body":"scheduler"}}' \
  -H "Host: cloudscheduler.googleapis.com" \
  "${gateway}/v1/projects/local-dev-project/locations/us-central1/jobs" >/dev/null
curl -fsS -X POST -H "Host: cloudscheduler.googleapis.com" \
  "${gateway}/v1/projects/local-dev-project/locations/us-central1/jobs/http:run" >/dev/null

queue="projects/local-dev-project/locations/us-central1/queues/integration"
curl -fsS -X POST -H "Content-Type: application/json" -H "Host: cloudtasks.googleapis.com" \
  -d '{"name":"'"${queue}"'","retryConfig":{"maxAttempts":2,"minBackoff":"10ms"}}' \
  "${gateway}/v2/projects/local-dev-project/locations/us-central1/queues" >/dev/null
curl -fsS -X POST -H "Content-Type: application/json" -H "Host: cloudtasks.googleapis.com" \
  -d '{"task":{"httpRequest":{"url":"http://127.0.0.1:'"${handler_port}"'/tasks","httpMethod":"POST","body":"dGFza3M="}}}' \
  "${gateway}/v2/${queue}/tasks" >/dev/null

poll "Scheduler and Cloud Tasks delivery" "test \"\$(wc -l < '${deliveries}' 2>/dev/null || echo 0)\" -ge 2"

deploy_function() {
  local name=$1 event_type=$2 resource=$3 marker=$4
  python3 - "${name}" "${event_type}" "${resource}" "${marker}" <<'PY' >"${tmp}/${name}.json"
import json, sys
name, event_type, resource, marker = sys.argv[1:]
code = "def handler(event, context=None):\n    print(" + repr(marker) + ", flush=True)\n    return ('ok', 200)\n"
print(json.dumps({"type":"function","name":name,"runtime":"python312","entryPoint":"handler",
                  "project":"local-dev-project","location":"us-central1","code":code,
                  "eventTrigger":{"eventType":event_type,"resource":resource}}))
PY
  curl -fsS -X POST -H "Content-Type: application/json" -H "Host: cloudfunctions.googleapis.com" \
    --data-binary "@${tmp}/${name}.json" "${gateway}/v2/deploy" >/dev/null
  poll "${name} activation" "curl -fsS -H 'Host: cloudfunctions.googleapis.com' '${gateway}/v2/projects/local-dev-project/locations/us-central1/functions/${name}' | grep -q '\"state\":\"ACTIVE\"'"
}

deploy_function storage-handler google.storage.object.finalize events-bucket MINISKY_STORAGE_EVENT_OK
curl -fsS -X POST -H "Content-Type: application/json" -H "Host: storage.googleapis.com" \
  -d '{"name":"events-bucket"}' "${gateway}/storage/v1/b?project=local-dev-project" >/dev/null
curl -fsS -X POST -H "Host: storage.googleapis.com" --data-binary 'payload' \
  "${gateway}/upload/storage/v1/b/events-bucket/o?uploadType=media&name=event.txt" >/dev/null
poll "Storage event handler" "docker logs minisky-serverless-storage-handler 2>&1 | grep -q MINISKY_STORAGE_EVENT_OK"

deploy_function pubsub-handler google.cloud.pubsub.topic.v1.messagePublished events-topic MINISKY_PUBSUB_EVENT_OK
curl -fsS -X PUT -H "Content-Type: application/json" -H "Host: pubsub.googleapis.com" -d '{}' \
  "${gateway}/v1/projects/local-dev-project/topics/events-topic" >/dev/null
curl -fsS -X POST -H "Content-Type: application/json" -H "Host: pubsub.googleapis.com" \
  -d '{"messages":[{"data":"cGF5bG9hZA=="}]}' \
  "${gateway}/v1/projects/local-dev-project/topics/events-topic:publish" >/dev/null
poll "Pub/Sub event handler" "docker logs minisky-serverless-pubsub-handler 2>&1 | grep -q MINISKY_PUBSUB_EVENT_OK"

echo "Event delivery integration passed"
