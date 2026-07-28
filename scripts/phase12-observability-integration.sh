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

assert_loopback_listeners() {
  local api_port=$1
  local ui_port=$2
  local collector_port=$3

  python3 - "${api_port}" "${ui_port}" "${collector_port}" <<'PY'
import pathlib
import re
import subprocess
import sys

ports = {int(value) for value in sys.argv[1:]}
listeners = {port: [] for port in ports}
proc_tcp = pathlib.Path("/proc/net/tcp")
if proc_tcp.exists():
    for line in proc_tcp.read_text().splitlines()[1:]:
        columns = line.split()
        if len(columns) < 4 or columns[3] != "0A":
            continue
        address, raw_port = columns[1].split(":")
        port = int(raw_port, 16)
        if port in listeners:
            listeners[port].append(address)
    invalid = {
        port: addresses
        for port, addresses in listeners.items()
        if not addresses or any(address != "0100007F" for address in addresses)
    }
else:
    try:
        output = subprocess.check_output(
            ["lsof", "-nP", "-iTCP", "-sTCP:LISTEN"],
            text=True,
            stderr=subprocess.STDOUT,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"cannot inspect loopback listeners: {error}")
    for line in output.splitlines():
        match = re.search(r"TCP\s+(\S+):(\d+)\s+\(LISTEN\)", line)
        if match and int(match.group(2)) in listeners:
            listeners[int(match.group(2))].append(match.group(1))
    invalid = {
        port: addresses
        for port, addresses in listeners.items()
        if not addresses or any(address not in {"127.0.0.1", "localhost"} for address in addresses)
    }
if invalid:
    raise SystemExit(f"listeners are missing or not loopback-only: {invalid}")
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

readonly MAX_OTLP_BODY_BYTES=$((1 << 20))
readonly MAX_OTLP_CAPTURE_FILES=32
readonly MAX_OTLP_TOTAL_BYTES=$((4 << 20))
readonly MAX_DIAGNOSTIC_FILE_BYTES=$((64 << 10))
readonly MAX_DIAGNOSTIC_TOTAL_BYTES=$((512 << 10))

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
resource_id="phase12-resource-$$"
api_key="phase12-api-key-$$"
token="phase12-token-$$"
query_value="phase12-query-value-$$"
unknown_host="phase12-unknown-host-$$.example"
trace_id="00000000000000000000000000000001"
probe_trace_id="00000000000000000000000000000002"
unknown_trace_id="00000000000000000000000000000003"
parent_id="00f067aa0ba902b7"
probe_parent_id="1111111111111111"
secret="phase12-must-not-appear-$$"
probe_path="/v3/projects/${project}/resources/${resource_id}"
probe_query="api_key=${api_key}&access_token=${token}&pageToken=${query_value}"
probe_target="${probe_path}?${probe_query}"
minisky_pid=""
collector_pid=""
started_at="${SECONDS}"
diagnostics_dir="${MINISKY_PHASE12_DIAGNOSTICS_DIR:-}"
capture_dir="${work}/otlp-captures"
forbidden_file="${work}/forbidden-values.json"
mkdir -p "${state_root}" "${home}" "${capture_dir}"
python3 - "${forbidden_file}" \
  "${project}" "${resource_id}" "${api_key}" "${token}" "${query_value}" \
  "${secret}" "${probe_path}" "${probe_query}" "${unknown_host}" <<'PY'
import json
import pathlib
import sys

labels = (
    "project ID",
    "resource ID",
    "API key",
    "access token",
    "query value",
    "secret",
    "raw path",
    "raw query",
    "unknown host",
)
pathlib.Path(sys.argv[1]).write_text(json.dumps(dict(zip(labels, sys.argv[2:]))))
PY

stop_process() {
  local pid=$1
  local label=$2
  local attempt
  [[ -n "${pid}" ]] || return 0
  if kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    for attempt in {1..50}; do
      kill -0 "${pid}" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "Force-stopping ${label} after cleanup timeout." >&2
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  fi
  wait "${pid}" 2>/dev/null || true
}

preserve_failure_diagnostics() {
  local status=$1
  [[ -n "${diagnostics_dir}" ]] || return 0
  printf 'status=%s\nprofile=%s\nelapsed_seconds=%s\n' \
    "${status}" "${profile}" "$((SECONDS - started_at))" \
    >"${work}/harness-summary.txt"
  python3 "${repository_root}/scripts/phase12-sanitize-diagnostics.py" \
    --source-dir "${work}" \
    --destination-dir "${diagnostics_dir}" \
    --forbidden-file "${forbidden_file}" \
    --max-file-bytes "${MAX_DIAGNOSTIC_FILE_BYTES}" \
    --max-total-bytes "${MAX_DIAGNOSTIC_TOTAL_BYTES}"
  echo "Phase 12 failure diagnostics saved to ${diagnostics_dir}." >&2
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  stop_process "${minisky_pid}" "MiniSky"
  stop_process "${collector_pid}" "OTLP test collector"
  if [[ "${status}" -ne 0 ]]; then
    preserve_failure_diagnostics "${status}"
  fi
  rm -rf "${work}" "${lock}"
  if [[ -e "${work}" || -e "${lock}" ]]; then
    echo "Phase 12 cleanup left temporary paths behind." >&2
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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
collector_endpoint="${collector}/v1/traces"
python3 - "${forbidden_file}" \
  "${gateway}${probe_target}" \
  "http://cloudresourcemanager.googleapis.com${probe_target}" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
values = json.loads(path.read_text())
values["gateway full URL"] = sys.argv[2]
values["service full URL"] = sys.argv[3]
path.write_text(json.dumps(values))
PY

assert_isolated_loopback_harness \
  "127.0.0.1" "${gateway}" "${dashboard}" "${collector}" \
  "${profile}" "${work}" "${state_root}"

cd "${repository_root}"
go build -trimpath -o "${work}/minisky" ./cmd/minisky
go build -trimpath -o "${work}/phase12-otlp-inspect" ./scripts/phase12-otlp-inspect

cat >"${work}/collector.py" <<'PY'
import http.server
import pathlib
import signal
import sys
import threading

count_file = pathlib.Path(sys.argv[2])
success_file = pathlib.Path(sys.argv[3])
capture_dir = pathlib.Path(sys.argv[4])
max_body_bytes = int(sys.argv[5])
max_capture_files = int(sys.argv[6])
max_total_bytes = int(sys.argv[7])
lock = threading.RLock()
request_count = 0
capture_count = 0
captured_bytes = 0
diagnostic_count = 0

def note(message):
    global diagnostic_count
    with lock:
        if diagnostic_count < 32:
            print(message, flush=True)
            diagnostic_count += 1

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(204)
        self.send_header("content-length", "0")
        self.end_headers()

    def do_POST(self):
        global request_count, capture_count, captured_bytes
        if self.path != "/v1/traces":
            note("rejected OTLP request: unexpected path")
            self.send_response(404)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        if self.headers.get_content_type() != "application/x-protobuf":
            note(f"rejected OTLP request: content type {self.headers.get_content_type()!r}")
            self.send_response(415)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        try:
            length = int(self.headers.get("content-length", ""))
        except ValueError:
            length = -1
        if length < 1 or length > max_body_bytes:
            note("rejected OTLP request: missing or oversized content length")
            self.send_response(413)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        body = self.rfile.read(length)
        if len(body) != length:
            note("rejected OTLP request: incomplete body")
            self.send_response(400)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        with lock:
            request_count += 1
            count_file.write_text(str(request_count))
            if capture_count >= max_capture_files or captured_bytes + len(body) > max_total_bytes:
                note("rejected OTLP request: aggregate capture limit exceeded")
                status = 507
            else:
                capture_count += 1
                captured_bytes += len(body)
                destination = capture_dir / f"otlp-{capture_count:04d}.pb"
                temporary = destination.with_suffix(".tmp")
                temporary.write_bytes(body)
                temporary.replace(destination)
                status = 200 if success_file.exists() else 503
        self.send_response(status)
        self.send_header("content-type", "application/x-protobuf")
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, _format, *_args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler)
signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
server.serve_forever()
PY
python3 "${work}/collector.py" \
  "${collector_port}" "${work}/collector-count" "${work}/collector-success" \
  "${capture_dir}" "${MAX_OTLP_BODY_BYTES}" \
  "${MAX_OTLP_CAPTURE_FILES}" "${MAX_OTLP_TOTAL_BYTES}" \
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
MINISKY_OTEL_ENDPOINT="${collector_endpoint}" \
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

assert_loopback_listeners "${api_port}" "${ui_port}" "${collector_port}"

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

set +e
probe_status="$(
  curl --silent --show-error \
    --output "${work}/probe-response.json" \
    --write-out '%{http_code}' \
    -H "Host: cloudresourcemanager.googleapis.com" \
    -H "X-MiniSky-Project: ${project}" \
    -H "X-MiniSky-Request-ID: phase12-sensitive-telemetry-probe" \
    -H "traceparent: 00-${probe_trace_id}-${probe_parent_id}-01" \
    -H "Authorization: Bearer ${secret}" \
    -H "Cookie: session=${secret}" \
    "${gateway}${probe_target}" \
    2>"${work}/probe-curl.log"
)"
probe_curl_status=$?
set -e
if [[ "${probe_curl_status}" -ne 0 || ! "${probe_status}" =~ ^[245][0-9][0-9]$ ]]; then
  echo "Sensitive telemetry probe did not complete through the loopback gateway." >&2
  exit 1
fi
set +e
unknown_status="$(
  curl --silent --show-error \
    --output "${work}/unknown-response.json" \
    --write-out '%{http_code}' \
    -H "Host: ${unknown_host}" \
    -H "X-MiniSky-Project: ${project}" \
    -H "X-MiniSky-Request-ID: phase12-unknown-host-probe" \
    -H "traceparent: 00-${unknown_trace_id}-${probe_parent_id}-01" \
    "${gateway}/v3/projects/${project}" \
    2>"${work}/unknown-curl.log"
)"
unknown_curl_status=$?
set -e
if [[ "${unknown_curl_status}" -ne 0 || ! "${unknown_status}" =~ ^[245][0-9][0-9]$ ]]; then
  echo "Unknown-host telemetry probe did not complete through the loopback gateway." >&2
  exit 1
fi
if [[ "${MINISKY_PHASE12_TEST_FAIL_AFTER_PROBE:-}" == "1" ]]; then
  echo "Injected Phase 12 failure after the sensitive telemetry probe." >&2
  exit 97
fi

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
  echo "Cross-project replay lookup returned ${cross_status}, want 404." >&2
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
    raise SystemExit("same-project replay lookup did not preserve the original response status")
PY

curl --fail --silent --show-error \
  -H "X-MiniSky-Project: ${project}" \
  "${dashboard}/api/diagnostics/requests?project=${project}" \
  >"${work}/requests.json"
python3 - "${work}/requests.json" "${work}/replay.json" "${forbidden_file}" <<'PY'
import json
import sys
records = json.load(open(sys.argv[1]))["requests"]
replay_id = json.load(open(sys.argv[2]))["requestId"]
forbidden = json.load(open(sys.argv[3]))
original = [record for record in records if record["requestId"] == "phase12-safe-request"]
replayed = [record for record in records if record["requestId"] == replay_id]
if len(original) != 1 or not original[0]["replayable"]:
    raise SystemExit("original request was not retained as an eligible replay")
if len(replayed) != 1 or replayed[0]["replayable"]:
    raise SystemExit("replayed request was recursively captured")
encoded = json.dumps(records, sort_keys=True).lower()
for label, value in forbidden.items():
    if label not in {"API key", "access token", "query value", "secret", "raw query"}:
        continue
    if value.lower() in encoded:
        raise SystemExit(f"diagnostics request records leaked the {label} canary")
PY

curl --fail --silent --show-error \
  "${dashboard}/api/diagnostics/metrics" >"${work}/metrics.txt"
python3 - "${work}/metrics.txt" "${forbidden_file}" <<'PY'
import json
import re
import sys
text = open(sys.argv[1]).read()
forbidden = json.load(open(sys.argv[2]))
required = (
    "minisky_gateway_requests_total",
    "minisky_gateway_request_duration_seconds",
    "minisky_resources",
)
if any(name not in text for name in required):
    raise SystemExit("Prometheus output omitted a required bounded metric family")
for label, value in forbidden.items():
    if value in text:
        raise SystemExit(f"Prometheus output leaked the {label} canary")
if "phase12-safe-request" in text or "trace_id=" in text:
    raise SystemExit("Prometheus output leaked a request or trace identifier")
allowed = {"service", "method", "route", "status_class"}
for line in text.splitlines():
    start, end = line.find("{"), line.rfind("}")
    if start < 0 or end <= start:
        continue
    labels = set(re.findall(r'(?:^|,)([a-z_]+)=', line[start + 1:end]))
    if line.startswith("minisky_gateway_request") and labels != allowed:
        raise SystemExit(f"unexpected gateway metric labels: {sorted(labels)}")
    if line.startswith("minisky_resources") and labels != {"service", "resource_kind"}:
        raise SystemExit(f"unexpected resource metric labels: {sorted(labels)}")
PY

python3 - "${work}/minisky.log" "${forbidden_file}" <<'PY'
import json
import sys
records = []
text = open(sys.argv[1]).read()
forbidden = json.load(open(sys.argv[2]))
for label, value in forbidden.items():
    if label not in {"API key", "access token", "query value", "secret", "raw query"}:
        continue
    if value.lower() in text.lower():
        raise SystemExit(f"MiniSky logs leaked the {label} canary")
for line in text.splitlines():
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        continue
    if value.get("requestId") in {"phase12-safe-request", "phase12-secret-request"}:
        records.append(value)
if len(records) != 2:
    raise SystemExit("structured access records were not emitted exactly once")
encoded = json.dumps(records, sort_keys=True).lower()
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
  exit 1
fi
if ! python3 -c 'import pathlib,sys; raise SystemExit(0 if "shutting down gracefully" in pathlib.Path(sys.argv[1]).read_text() else 1)' "${work}/minisky.log"; then
  echo "MiniSky did not report graceful shutdown." >&2
  exit 1
fi

stop_process "${collector_pid}" "OTLP test collector"
collector_pid=""
"${work}/phase12-otlp-inspect" \
  --capture-dir "${capture_dir}" \
  --forbidden-file "${forbidden_file}" \
  --max-files "${MAX_OTLP_CAPTURE_FILES}" \
  --max-body-bytes "${MAX_OTLP_BODY_BYTES}" \
  --max-total-bytes "${MAX_OTLP_TOTAL_BYTES}" \
  --required-trace-id "${probe_trace_id}" \
  --required-service "cloudresourcemanager.googleapis.com" \
  --required-service "other" \
  --resource-service "minisky" \
  | tee "${work}/otlp-inspection.txt"

duration=$((SECONDS - started_at))
echo "Phase 12 loopback observability integration passed in ${duration}s."
