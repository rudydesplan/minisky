#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE23_SDK_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 23 SDK integration without MINISKY_PHASE23_SDK_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done

lock="${TMPDIR:-/tmp}/minisky-phase23-sdk-integration.lock"
if ! mkdir "${lock}" 2>/dev/null; then
  echo "Another Phase 23 SDK integration is running (${lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
state_root="${work}/state"
home="${work}/home"
profile="phase23-sdk-$$"
project="phase23-project-$$"
evidence_file="${work}/generated-client-evidence.json"
sensitive_sentinel="phase23-sensitive-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
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
  network_id="$(docker network ls -q \
    --filter "name=^minisky-net$" \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true)"
  if [[ -n "${network_id}" ]]; then
    docker network rm "${network_id}" >/dev/null 2>&1 || true
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

docker info >/dev/null
if docker network inspect minisky-net >/dev/null 2>&1; then
  owner="$(docker network inspect --format '{{ index .Labels "minisky.profile" }}' minisky-net)"
  if [[ "${owner}" != "${profile}" ]]; then
    echo "Refusing Phase 23 run: shared network minisky-net is owned by profile ${owner:-unknown}." >&2
    exit 1
  fi
fi

go build -trimpath -o "${work}/minisky" ./cmd/minisky

start_daemon() {
  local log_file=$1
  local experimental=$2
  local -a env_args=(
    "HOME=${home}"
    "MINISKY_STATE_DIR=${state_root}"
    "MINISKY_PROFILE=${profile}"
  )
  if [[ "${experimental}" == "1" ]]; then
    env_args+=("MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1")
  fi
  env "${env_args[@]}" "${work}/minisky" start \
    --port "${api_port}" --ui-port "${ui_port}" >"${log_file}" 2>&1 &
  pid=$!
  for _ in {1..120}; do
    if curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' "${log_file}" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "Timed out waiting for MiniSky readiness." >&2
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' "${log_file}" >&2
  return 1
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}"
  fi
  pid=""
}

run_smoke() {
  local mode=$1
  local log_file=$2
  if ! MINISKY_PHASE23_MODE="${mode}" \
    MINISKY_PHASE23_SENSITIVE_SENTINEL="${sensitive_sentinel}" \
    go run ./sdk-smoke/phase23; then
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text()[-8192:])' "${log_file}" >&2
    return 1
  fi
}

assert_sensitive_probe_not_persisted() {
  python3 - "${sensitive_sentinel}" "${state_root}" "${evidence_file}" "${work}" <<'PY'
import pathlib
import sys

needle = sys.argv[1].encode()
state_root = pathlib.Path(sys.argv[2])
evidence = pathlib.Path(sys.argv[3])
work = pathlib.Path(sys.argv[4])
paths = []
if state_root.exists():
    paths.extend(path for path in state_root.rglob("*") if path.is_file())
if evidence.exists():
    paths.append(evidence)
paths.extend(work.glob("minisky-*.log"))
for path in paths:
    if needle in path.read_bytes():
        raise SystemExit(f"sensitive Phase 23 probe persisted in {path}")
PY
}

export MINISKY_ENDPOINT="${gateway}"
export MINISKY_PROJECT_ID="${project}"
export MINISKY_PHASE23_LOCATION="us-central1"
export MINISKY_PHASE23_EVIDENCE="${evidence_file}"

# Prove every Phase 23 public-gateway domain remains default-disabled.
start_daemon "${work}/minisky-default-gated.log" 0
run_smoke gate "${work}/minisky-default-gated.log"
stop_daemon

# Explicit opt-in is required for stateful and semantic-boundary evidence.
export MINISKY_PHASE23_EXPERIMENTAL_OPT_IN=1
start_daemon "${work}/minisky-create.log" 1
run_smoke create "${work}/minisky-create.log"
stop_daemon
assert_sensitive_probe_not_persisted

# Recreate the daemon against the same isolated profile, then clean up.
start_daemon "${work}/minisky-restarted.log" 1
run_smoke verify "${work}/minisky-restarted.log"
run_smoke delete "${work}/minisky-restarted.log"
stop_daemon
assert_sensitive_probe_not_persisted

network_id="$(docker network ls -q \
  --filter "name=^minisky-net$" \
  --filter "label=managed-by=minisky" \
  --filter "label=minisky.profile=${profile}")"
if [[ -n "${network_id}" ]]; then
  docker network rm "${network_id}" >/dev/null
fi
if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Phase 23 cleanup left a shared minisky-net network." >&2
  exit 1
fi

duration=$((SECONDS - started_at))
echo "Phase 23 generated Go client integration passed in ${duration}s."
