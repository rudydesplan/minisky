#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE15_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start emulator integration without MINISKY_PHASE15_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done
docker info >/dev/null

if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Refusing to disturb an existing MiniSky Docker network." >&2
  exit 1
fi
for container in minisky-firestore minisky-datastore minisky-spanner; do
  if docker container inspect "${container}" >/dev/null 2>&1; then
    echo "Refusing to disturb existing container ${container}." >&2
    exit 1
  fi
done

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
profile="phase15-$$"
pid=""

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  for container in minisky-firestore minisky-datastore minisky-spanner; do
    if [[ "$(docker inspect --format '{{ index .Config.Labels "managed-by" }}:{{ index .Config.Labels "minisky.profile" }}' "${container}" 2>/dev/null || true)" == "minisky:${profile}" ]]; then
      docker rm -f "${container}" >/dev/null 2>&1 || true
    fi
  done
  if [[ "$(docker network inspect --format '{{ index .Labels "managed-by" }}:{{ index .Labels "minisky.profile" }}' minisky-net 2>/dev/null || true)" == "minisky:${profile}" ]]; then
    docker network rm minisky-net >/dev/null 2>&1 || true
  fi
  rm -rf "${work}"
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
MINISKY_STATE_DIR="${work}/state" MINISKY_PROFILE="${profile}" \
  "${work}/minisky" start --port "${api_port}" --ui-port "${ui_port}" >"${work}/minisky.log" 2>&1 &
pid=$!

for _ in {1..60}; do
  if curl --silent --output /dev/null "${gateway}/_minisky/firestore/"; then
    break
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())' "${work}/minisky.log" >&2
    exit 1
  fi
  sleep 1
done
curl --silent --output /dev/null "${gateway}/_minisky/datastore/"
curl --silent --output /dev/null "${gateway}/_minisky/spanner/"

docker_port() {
  docker inspect --format "{{ (index (index .NetworkSettings.Ports \"$2\") 0).HostPort }}" "$1"
}

export FIRESTORE_EMULATOR_HOST="127.0.0.1:$(docker_port minisky-firestore 8082/tcp)"
export DATASTORE_EMULATOR_HOST="127.0.0.1:$(docker_port minisky-datastore 8081/tcp)"
export SPANNER_EMULATOR_HOST="127.0.0.1:$(docker_port minisky-spanner 9010/tcp)"
export MINISKY_PROJECT_ID="local-dev-project"
export MINISKY_SPANNER_INSTANCE="phase15-$$"
export MINISKY_SPANNER_DATABASE="phase15"

go run ./sdk-smoke/phase15
