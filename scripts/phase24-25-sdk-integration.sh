#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE24_25_SDK_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 24-25 SDK integration without MINISKY_PHASE24_25_SDK_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock="${TMPDIR:-/tmp}/minisky-phase24-25-sdk-integration.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 24-25 SDK integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
state_root="${work}/state"
home="${work}/home"
profile="phase24-25-sdk-$$"
project="phase24-25-project-$$"
certificate_id="phase24-certificate-$$"
template_id="phase24-template-$$"
perimeter_id="phase24-perimeter-$$"
network_policy_id="phase25-deny-$$"
mesh_id="phase25-mesh-$$"
proxy_policy_id="phase25-proxy-deny-$$"
proxy_mesh_id="phase25-proxy-mesh-$$"
evidence_file="${work}/generated-client-evidence.json"
pid=""
backend_pid=""
started_at="${SECONDS}"
mkdir -p "${state_root}" "${home}"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  if [[ -n "${backend_pid}" ]] && kill -0 "${backend_pid}" 2>/dev/null; then
    kill -TERM "${backend_pid}" 2>/dev/null || true
    wait "${backend_pid}" 2>/dev/null || true
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
default_backend_port="$(free_port)"
routed_backend_port="$(free_port)"
gateway="http://127.0.0.1:${api_port}"
default_backend="http://127.0.0.1:${default_backend_port}"
routed_backend="http://127.0.0.1:${routed_backend_port}"

cat >"${work}/backends.py" <<'PY'
import http.server
import signal
import sys
import threading

def handler(label):
    class Backend(http.server.BaseHTTPRequestHandler):
        hits = 0

        def do_GET(self):
            if self.path == "/__hits":
                body = str(type(self).hits).encode()
            else:
                type(self).hits += 1
                body = label.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass
    return Backend

servers = [
    http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), handler("default")),
    http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[2])), handler("routed")),
]
for server in servers:
    threading.Thread(target=server.serve_forever, daemon=True).start()
signal.pause()
PY
python3 "${work}/backends.py" "${default_backend_port}" "${routed_backend_port}" \
  >"${work}/backends.log" 2>&1 &
backend_pid=$!
for _ in {1..40}; do
  if curl --fail --silent "${default_backend}/__hits" >/dev/null 2>&1 &&
    curl --fail --silent "${routed_backend}/__hits" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${backend_pid}" 2>/dev/null; then
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${work}/backends.log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! curl --fail --silent "${default_backend}/__hits" >/dev/null ||
  ! curl --fail --silent "${routed_backend}/__hits" >/dev/null; then
  echo "Timed out waiting for isolated local backends." >&2
  exit 1
fi

go build -trimpath -o "${work}/minisky" ./cmd/minisky

start_daemon() {
  local experimental=$1
  local log_file=$2
  if [[ "${experimental}" == "1" ]]; then
    HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
      MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
      "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  else
    HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
      env -u MINISKY_ENABLE_EXPERIMENTAL_SERVICES \
      "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  fi
  pid=$!
  for _ in {1..80}; do
    if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${log_file}" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "Timed out waiting for MiniSky readiness." >&2
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${log_file}" >&2
  return 1
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}"
  fi
  pid=""
  for _ in {1..40}; do
    if ! docker network inspect minisky-net >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  echo "Timed out waiting for MiniSky Docker network cleanup." >&2
  return 1
}

export MINISKY_ENDPOINT="${gateway}"
export MINISKY_PROJECT_ID="${project}"
export MINISKY_PHASE24_25_LOCATION="us-central1"
export MINISKY_PHASE24_25_CERTIFICATE_ID="${certificate_id}"
export MINISKY_PHASE24_25_TEMPLATE_ID="${template_id}"
export MINISKY_PHASE24_25_PERIMETER_ID="${perimeter_id}"
export MINISKY_PHASE24_25_NETWORK_POLICY_ID="${network_policy_id}"
export MINISKY_PHASE24_25_MESH_ID="${mesh_id}"
export MINISKY_PHASE24_25_PROXY_POLICY_ID="${proxy_policy_id}"
export MINISKY_PHASE24_25_PROXY_MESH_ID="${proxy_mesh_id}"
export MINISKY_PHASE24_25_DEFAULT_BACKEND="${default_backend}"
export MINISKY_PHASE24_25_ROUTED_BACKEND="${routed_backend}"
export MINISKY_PHASE24_25_EVIDENCE="${evidence_file}"

start_daemon 0 "${work}/minisky-default-gated.log"
MINISKY_PHASE24_25_MODE=gate go run ./sdk-smoke/phase24-25
stop_daemon

start_daemon 1 "${work}/minisky-create.log"
MINISKY_PHASE24_25_MODE=create MINISKY_PHASE24_25_EXPERIMENTAL_OPT_IN=1 \
  go run ./sdk-smoke/phase24-25
stop_daemon

start_daemon 1 "${work}/minisky-restarted.log"
MINISKY_PHASE24_25_MODE=verify MINISKY_PHASE24_25_EXPERIMENTAL_OPT_IN=1 \
  go run ./sdk-smoke/phase24-25
MINISKY_PHASE24_25_MODE=delete MINISKY_PHASE24_25_EXPERIMENTAL_OPT_IN=1 \
  go run ./sdk-smoke/phase24-25
stop_daemon

duration=$((SECONDS - started_at))
echo "Phase 24-25 generated Go client integration passed in ${duration}s."
