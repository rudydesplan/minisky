#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE16_DNS_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Phase 16 Cloud DNS integration without MINISKY_PHASE16_DNS_INTEGRATION=1." >&2
  exit 2
fi

for command in curl go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock="${TMPDIR:-/tmp}/minisky-phase16-dns.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 16 Cloud DNS integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
chmod 700 "${work}"
state_root="${work}/state"
home="${work}/home"
profile="phase16-dns-$$"
project="phase16-project-$$"
pid=""
current_log=""
started_at="${SECONDS}"
mkdir -p "${state_root}" "${home}"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  rm -rf "${work}" "${lock}"
  exit "${status}"
}
trap cleanup EXIT INT TERM

free_tcp_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

free_udp_port() {
  python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    if port < 1024:
        raise SystemExit("ephemeral UDP port was privileged")
    print(port)
PY
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky

allocate_ports() {
  api_port="$(free_tcp_port)"
  ui_port="$(free_tcp_port)"
  while [[ "${ui_port}" == "${api_port}" ]]; do
    ui_port="$(free_tcp_port)"
  done
  dns_port="$(free_udp_port)"
  gateway="http://127.0.0.1:${api_port}"
  dns_addr="127.0.0.1:${dns_port}"
  export MINISKY_ENDPOINT="${gateway}"
  export MINISKY_DNS_ADDR="${dns_addr}"
}

udp_ready() {
  python3 - "${dns_addr}" <<'PY'
import socket
import struct
import sys

host, port = sys.argv[1].rsplit(":", 1)
labels = b"readiness.example.test".split(b".")
question = b"".join(bytes([len(label)]) + label for label in labels) + b"\x00"
query = struct.pack("!HHHHHH", 16, 0, 1, 0, 0, 0) + question + struct.pack("!HH", 1, 1)
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
    sock.settimeout(0.25)
    sock.sendto(query, (host, int(port)))
    response, _ = sock.recvfrom(1500)
if len(response) < 12 or response[:2] != query[:2] or not (response[2] & 0x80):
    raise SystemExit("invalid UDP DNS readiness response")
PY
}

bind_collision() {
  python3 - "${current_log}" <<'PY'
import pathlib
import sys

text = pathlib.Path(sys.argv[1]).read_text(errors="replace").lower()
raise SystemExit(0 if "address already in use" in text or "bind:" in text else 1)
PY
}

print_current_log() {
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"))' "${current_log}" >&2
}

start_daemon() {
  local log_prefix=$1
  local attempt
  for attempt in {1..3}; do
    allocate_ports
    current_log="${work}/${log_prefix}-attempt-${attempt}.log"
    HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
      MINISKY_DNS_ADDR="${dns_addr}" MINISKY_DNS_PROJECT="${project}" \
      MINISKY_PHASE16_DNS_INTEGRATION=1 \
      "${work}/minisky" start --services dns --port "${api_port}" --ui-port "${ui_port}" >"${current_log}" 2>&1 &
    pid=$!
    for _ in {1..80}; do
      if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1 && udp_ready 2>/dev/null; then
        return
      fi
      if bind_collision; then
        if kill -0 "${pid}" 2>/dev/null; then
          kill -TERM "${pid}" 2>/dev/null || true
        fi
        wait "${pid}" 2>/dev/null || true
        pid=""
        break
      fi
      if ! kill -0 "${pid}" 2>/dev/null; then
        print_current_log
        return 1
      fi
      sleep 0.25
    done
    if [[ -n "${pid}" ]]; then
      echo "Timed out waiting for MiniSky HTTP and UDP DNS readiness." >&2
      print_current_log
      return 1
    fi
    echo "Retrying MiniSky start after bind collision (attempt ${attempt}/3)." >&2
  done
  echo "MiniSky could not bind dynamic ports after 3 attempts." >&2
  print_current_log
  return 1
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}"
  fi
  pid=""
}

export MINISKY_PROJECT_ID="${project}"

start_daemon "minisky-first"
MINISKY_PHASE16_DNS_MODE=seed go run ./sdk-smoke/phase16-dns
stop_daemon

start_daemon "minisky-restarted"
MINISKY_PHASE16_DNS_MODE=verify go run ./sdk-smoke/phase16-dns
MINISKY_PHASE16_DNS_MODE=cleanup go run ./sdk-smoke/phase16-dns
stop_daemon

start_daemon "minisky-cleanup-restarted"
MINISKY_PHASE16_DNS_MODE=verify-cleanup go run ./sdk-smoke/phase16-dns
stop_daemon

duration=$((SECONDS - started_at))
echo "Phase 16 Cloud DNS generated-SDK restart and UDP integration passed in ${duration}s."
