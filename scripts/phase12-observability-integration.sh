#!/usr/bin/env bash

set -Eeuo pipefail

assert_isolated_loopback_harness() {
  local bind=$1
  local gateway=$2
  local dashboard=$3
  local collector=$4
  local candidate_profile=$5
  local candidate_work=$6
  local candidate_state=$7

  [[ "${bind}" == "127.0.0.1" ]] || return 1
  [[ "${candidate_profile}" == phase12-observability-* ]] || return 1
  [[ "${candidate_state}" == "${candidate_work}/state" ]] || return 1
  [[ -d "${candidate_work}" && -d "${candidate_state}" ]] || return 1
  python3 - "${gateway}" "${dashboard}" "${collector}" <<'PY' || return 1
import sys
import urllib.parse

ports = set()
for raw in sys.argv[1:]:
    parsed = urllib.parse.urlsplit(raw)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise SystemExit("Phase 12 integration endpoints must be explicit loopback HTTP origins")
    ports.add(parsed.port)
if len(ports) != 3:
    raise SystemExit("Phase 12 integration requires three distinct loopback ports")
PY
}

run_guard_self_test() (
  local root
  root="$(mktemp -d)"
  trap 'rm -rf "${root}"' EXIT
  mkdir "${root}/state"
  assert_isolated_loopback_harness \
    "127.0.0.1" \
    "http://127.0.0.1:18080" \
    "http://127.0.0.1:18081" \
    "http://127.0.0.1:14318" \
    "phase12-observability-self-test" \
    "${root}" \
    "${root}/state"
  if assert_isolated_loopback_harness \
    "0.0.0.0" \
    "http://127.0.0.1:18080" \
    "http://127.0.0.1:18081" \
    "http://127.0.0.1:14318" \
    "phase12-observability-self-test" \
    "${root}" \
    "${root}/state" 2>/dev/null; then
    echo "Phase 12 guard accepted a non-loopback gateway bind." >&2
    return 1
  fi
  if assert_isolated_loopback_harness \
    "127.0.0.1" \
    "http://127.0.0.1:18080" \
    "http://127.0.0.1:18081" \
    "http://192.0.2.10:14318" \
    "phase12-observability-self-test" \
    "${root}" \
    "${root}/state" 2>/dev/null; then
    echo "Phase 12 guard accepted a non-loopback collector." >&2
    return 1
  fi
  if assert_isolated_loopback_harness \
    "127.0.0.1" \
    "http://127.0.0.1:18080" \
    "http://127.0.0.1:18080" \
    "http://127.0.0.1:14318" \
    "phase12-observability-self-test" \
    "${root}" \
    "${root}/state" 2>/dev/null; then
    echo "Phase 12 guard accepted colliding listener ports." >&2
    return 1
  fi
  if assert_isolated_loopback_harness \
    "127.0.0.1" \
    "http://127.0.0.1:18080" \
    "http://127.0.0.1:18081" \
    "http://127.0.0.1:14318" \
    "foreign-profile" \
    "${root}" \
    "${root}/state" 2>/dev/null; then
    echo "Phase 12 guard accepted a foreign profile." >&2
    return 1
  fi
  echo "Phase 12 observability loopback guard self-test passed."
)

if [[ "${MINISKY_PHASE12_OBSERVABILITY_SELF_TEST:-}" == "1" ]]; then
  command -v python3 >/dev/null 2>&1 || {
    echo "Required command not found: python3" >&2
    exit 1
  }
  run_guard_self_test
  exit 0
fi

if [[ "${MINISKY_PHASE12_OBSERVABILITY_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 12 observability integration without MINISKY_PHASE12_OBSERVABILITY_INTEGRATION=1." >&2
  exit 2
fi

for command in curl go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lock="${TMPDIR:-/tmp}/minisky-phase12-observability-integration.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 12 observability integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
chmod 700 "${work}"
state_root="${work}/state"
home="${work}/home"
profile="phase12-observability-$(basename "${work}")"
project="phase12-project-$$"
other_project="${project}-other"
trace_id="4bf92f3577b34da6a3ce929d0e0e4736"
parent_id="00f067aa0ba902b7"
secret="phase12-must-not-appear-$$"
minisky_pid=""
collector_pid=""
started_at="${SECONDS}"
mkdir -p "${state_root}" "${home}"

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "${minisky_pid}" ]] && kill -0 "${minisky_pid}" 2>/dev/null; then
    kill -TERM "${minisky_pid}" 2>/dev/null || true
    wait "${minisky_pid}" 2>/dev/null || true
  fi
  if [[ -n "${collector_pid}" ]] && kill -0 "${collector_pid}" 2>/dev/null; then
    kill -TERM "${collector_pid}" 2>/dev/null || true
    wait "${collector_pid}" 2>/dev/null || true
  fi
  rm -rf "${work}" "${lock}"
  exit "${status}"
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

api_port="$(free_port)"
ui_port="$(free_port)"
collector_port="$(free_port)"
while [[ "${ui_port}" == "${api_port}" ]]; do ui_port="$(free_port)"; done
while [[ "${collector_port}" == "${api_port}" || "${collector_port}" == "${ui_port}" ]]; do
  collector_port="$(free_port)"
done
gateway="http://127.0.0.1:${api_port}"
dashboard="http://127.0.0.1:${ui_port}"
collector="http://127.0.0.1:${collector_port}"

assert_isolated_loopback_harness \
  "127.0.0.1" "${gateway}" "${dashboard}" "${collector}" \
  "${profile}" "${work}" "${state_root}"

cd "${repository_root}"
go build -trimpath -o "${work}/minisky" ./cmd/minisky

cat >"${work}/collector.py" <<'PY'
import http.server
import pathlib
import sys

count_file = pathlib.Path(sys.argv[2])
success_file = pathlib.Path(sys.argv[3])
count = 0

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(204)
        self.end_headers()

    def do_POST(self):
        global count
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        count += 1
        count_file.write_text(str(count))
        self.send_response(200 if success_file.exists() else 503)
        self.end_headers()

    def log_message(self, _format, *_args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler)
server.serve_forever()
PY
python3 "${work}/collector.py" \
  "${collector_port}" "${work}/collector-count" "${work}/collector-success" \
  >"${work}/collector.log" 2>&1 &
collector_pid=$!

for _ in {1..40}; do
  if curl --fail --silent --show-error "${collector}" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${collector_pid}" 2>/dev/null; then
    echo "Loopback OTLP failure collector exited during startup." >&2
    exit 1
  fi
  sleep 0.1
done
curl --fail --silent --show-error "${collector}" >/dev/null

# The existing native-only integration switch prevents this telemetry test from
# creating or inspecting Docker resources. The Phase 12 guard above remains the
# operator opt-in and constrains every listener to this temporary loopback setup.
HOME="${home}" \
MINISKY_STATE_DIR="${state_root}" \
MINISKY_PROFILE="${profile}" \
MINISKY_PHASE16_LOGGING_INTEGRATION=1 \
MINISKY_OTEL_ENABLED=true \
MINISKY_OTEL_ENDPOINT="${collector}" \
MINISKY_REQUEST_REPLAY_ENABLED=true \
MINISKY_REQUEST_REPLAY_MAX_BODY=1024 \
  "${work}/minisky" start \
    --bind 127.0.0.1 \
    --port "${api_port}" \
    --ui-port "${ui_port}" \
    --services cloudresourcemanager \
    >"${work}/minisky.log" 2>&1 &
minisky_pid=$!

for _ in {1..80}; do
  if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${minisky_pid}" 2>/dev/null; then
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${work}/minisky.log" >&2
    exit 1
  fi
  sleep 0.25
done
curl --fail --silent --show-error "${gateway}/healthz" >/dev/null

request_status="$(
  curl --silent --show-error \
    --output "${work}/safe-response.json" \
    --write-out '%{http_code}' \
    -H "Host: cloudresourcemanager.googleapis.com" \
    -H "X-MiniSky-Project: ${project}" \
    -H "X-MiniSky-Request-ID: phase12-safe-request" \
    -H "traceparent: 00-${trace_id}-${parent_id}-01" \
    "${gateway}/v3/projects/${project}"
)"
[[ "${request_status}" =~ ^[245][0-9][0-9]$ ]] || {
  echo "Unexpected gateway response status ${request_status}." >&2
  exit 1
}

curl --silent --show-error \
  --output "${work}/secret-response.json" \
  -H "Host: cloudresourcemanager.googleapis.com" \
  -H "X-MiniSky-Project: ${project}" \
  -H "X-MiniSky-Request-ID: phase12-secret-request" \
  -H "Authorization: Bearer ${secret}" \
  -H "Cookie: session=${secret}" \
  "${gateway}/v3/projects/${project}" >/dev/null

curl --fail --silent --show-error \
  -H "X-MiniSky-Project: ${project}" \
  "${dashboard}/api/diagnostics/traces?project=${project}&traceId=${trace_id}" \
  >"${work}/traces.json"
python3 - "${work}/traces.json" "${trace_id}" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1]))
matches = [record for record in payload["traces"] if record["requestId"] == "phase12-safe-request"]
if len(matches) != 1 or matches[0]["traceId"] != sys.argv[2]:
    raise SystemExit("gateway did not preserve the inbound W3C trace ID")
if matches[0]["spanId"] == "00f067aa0ba902b7":
    raise SystemExit("gateway record reused the remote parent instead of creating a server span")
PY

for _ in {1..80}; do
  if [[ -s "${work}/collector-count" ]] && [[ "$(<"${work}/collector-count")" -ge 1 ]]; then
    break
  fi
  sleep 0.25
done
[[ -s "${work}/collector-count" ]] && [[ "$(<"${work}/collector-count")" -ge 1 ]] || {
  echo "OTLP collector did not observe the configured failing export." >&2
  exit 1
}

after_failure_status="$(
  curl --silent --show-error \
    --output "${work}/after-failure-response.json" \
    --write-out '%{http_code}' \
    -H "Host: cloudresourcemanager.googleapis.com" \
    -H "X-MiniSky-Project: ${project}" \
    -H "X-MiniSky-Request-ID: phase12-after-exporter-failure" \
    "${gateway}/v3/projects/${project}"
)"
if [[ "${after_failure_status}" != "${request_status}" ]]; then
  echo "Exporter failure changed the API status from ${request_status} to ${after_failure_status}." >&2
  exit 1
fi
if ! python3 - "${work}/safe-response.json" "${work}/after-failure-response.json" <<'PY'
import pathlib
import sys
raise SystemExit(0 if pathlib.Path(sys.argv[1]).read_bytes() == pathlib.Path(sys.argv[2]).read_bytes() else 1)
PY
then
  echo "Exporter failure changed the API response payload." >&2
  exit 1
fi

cross_status="$(
  curl --silent --show-error \
    --output "${work}/cross-project-replay.json" \
    --write-out '%{http_code}' \
    -X POST \
    -H "X-MiniSky-Project: ${other_project}" \
    "${dashboard}/api/diagnostics/requests/phase12-safe-request/replay?project=${other_project}"
)"
[[ "${cross_status}" == "404" ]] || {
  echo "Cross-project replay returned ${cross_status}, want 404." >&2
  exit 1
}

curl --fail --silent --show-error \
  -X POST \
  -H "X-MiniSky-Project: ${project}" \
  "${dashboard}/api/diagnostics/requests/phase12-safe-request/replay?project=${project}" \
  >"${work}/replay.json"
python3 - "${work}/replay.json" "${request_status}" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1]))
if payload["status"] != int(sys.argv[2]) or not payload.get("requestId"):
    raise SystemExit("same-gateway replay did not preserve the original response status")
PY

curl --fail --silent --show-error \
  -H "X-MiniSky-Project: ${project}" \
  "${dashboard}/api/diagnostics/requests?project=${project}" \
  >"${work}/requests.json"
python3 - "${work}/requests.json" "${work}/replay.json" <<'PY'
import json
import sys
records = json.load(open(sys.argv[1]))["requests"]
replay_id = json.load(open(sys.argv[2]))["requestId"]
original = [record for record in records if record["requestId"] == "phase12-safe-request"]
replayed = [record for record in records if record["requestId"] == replay_id]
if len(original) != 1 or not original[0]["replayable"]:
    raise SystemExit("original request was not retained as an eligible replay")
if len(replayed) != 1 or replayed[0]["replayable"]:
    raise SystemExit("replayed request was recursively captured")
PY

curl --fail --silent --show-error \
  "${dashboard}/api/diagnostics/metrics" >"${work}/metrics.txt"
python3 - "${work}/metrics.txt" "${project}" <<'PY'
import re
import sys
text = open(sys.argv[1]).read()
required = (
    "minisky_gateway_requests_total",
    "minisky_gateway_request_duration_seconds",
    "minisky_resources",
)
if any(name not in text for name in required):
    raise SystemExit("Prometheus output omitted a required bounded metric family")
if sys.argv[2] in text or "phase12-safe-request" in text or "trace_id=" in text:
    raise SystemExit("Prometheus output leaked a project, request, or trace identifier")
allowed = {"service", "method", "route", "status_class"}
for line in text.splitlines():
    if not line.startswith("minisky_gateway_request"):
        continue
    start, end = line.find("{"), line.rfind("}")
    if start >= 0 and end > start:
        labels = set(re.findall(r"(?:^|,)([a-z_]+)=", line[start + 1:end]))
        if labels != allowed:
            raise SystemExit(f"unexpected gateway metric labels: {sorted(labels)}")
PY

python3 - "${work}/minisky.log" "${secret}" <<'PY'
import json
import sys
records = []
for line in open(sys.argv[1]):
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        continue
    if value.get("requestId") in {"phase12-safe-request", "phase12-secret-request"}:
        records.append(value)
if len(records) != 2:
    raise SystemExit("structured access records were not emitted exactly once")
encoded = json.dumps(records, sort_keys=True).lower()
if sys.argv[2].lower() in encoded:
    raise SystemExit("structured access records leaked a secret value")
for forbidden in ("authorization", "cookie", "body", "rawquery", "error"):
    if any(forbidden in {key.lower() for key in record} for record in records):
        raise SystemExit(f"structured access records exposed forbidden field {forbidden}")
PY

# Let shutdown flush to a healthy collector after proving a failed export did
# not affect later API responses.
touch "${work}/collector-success"
kill -TERM "${minisky_pid}"
for _ in {1..80}; do
  if ! kill -0 "${minisky_pid}" 2>/dev/null; then
    break
  fi
  sleep 0.25
done
if kill -0 "${minisky_pid}" 2>/dev/null; then
  echo "MiniSky did not complete graceful shutdown within 20 seconds." >&2
  exit 1
fi
set +e
wait "${minisky_pid}"
shutdown_status=$?
set -e
minisky_pid=""
if [[ "${shutdown_status}" != "0" ]]; then
  echo "MiniSky graceful shutdown exited with status ${shutdown_status}." >&2
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${work}/minisky.log" >&2
  exit 1
fi
if ! python3 -c 'import pathlib,sys; raise SystemExit(0 if "shutting down gracefully" in pathlib.Path(sys.argv[1]).read_text() else 1)' "${work}/minisky.log"; then
  echo "MiniSky did not report graceful shutdown." >&2
  exit 1
fi

duration=$((SECONDS - started_at))
echo "Phase 12 loopback observability integration passed in ${duration}s."
