#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE16_MONITORING_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Phase 16 Monitoring integration without MINISKY_PHASE16_MONITORING_INTEGRATION=1." >&2
  exit 2
fi

for command in curl go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock="${TMPDIR:-/tmp}/minisky-phase16-monitoring.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 16 Monitoring integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
state_root="${work}/state"
home="${work}/home"
profile="phase16-monitoring-$$"
project="phase16-project-$$"
metric_type="custom.googleapis.com/minisky/phase16/value_$$"
sample="42.125"
pid=""
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
gateway="http://127.0.0.1:${api_port}"

go build -trimpath -o "${work}/minisky" ./cmd/minisky

start_daemon() {
  local log_file=$1
  HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
    "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  pid=$!
  for _ in {1..60}; do
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
}

export MINISKY_ENDPOINT="${gateway}"
export MINISKY_PROJECT_ID="${project}"
export MINISKY_PHASE16_METRIC_TYPE="${metric_type}"
export MINISKY_PHASE16_SAMPLE="${sample}"
export MINISKY_PHASE16_END_TIME="2026-07-25T10:00:00Z"

start_daemon "${work}/minisky-first.log"
MINISKY_PHASE16_MODE=seed go run ./sdk-smoke/phase16
stop_daemon

start_daemon "${work}/minisky-restarted.log"
MINISKY_PHASE16_MODE=query go run ./sdk-smoke/phase16
MINISKY_PHASE16_MODE=cleanup go run ./sdk-smoke/phase16
stop_daemon

duration=$((SECONDS - started_at))
echo "Phase 16 Monitoring restart integration passed in ${duration}s."
