#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_JAVA_SDK_SMOKE:-}" != "1" ]]; then
  echo "Refusing to run Java SDK smoke without MINISKY_JAVA_SDK_SMOKE=1." >&2
  exit 2
fi

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project_dir="${repository_root}/sdk-smoke/java"
classpath_file="${project_dir}/target/runtime-classpath.txt"
cleanup_proxy() { :; }

if [[ "${MINISKY_JAVA_CONTAINER:-}" == "1" ]]; then
  command -v docker >/dev/null 2>&1 || {
    echo "Required command not found: docker" >&2
    exit 1
  }
  image="maven:3.9.11-eclipse-temurin-21@sha256:6fdc855a6ed81d288ca7ca37ac6ff5e9308b612485c0801d70b25a858c83d237"
  # Docker tmpfs options intentionally use comma-separated mount flags.
  # shellcheck disable=SC2054
  docker_args=(
    run --rm --read-only
    --tmpfs /tmp:rw,exec,nosuid,size=64m
    --tmpfs /workspace:rw,noexec,nosuid,size=256m
    --tmpfs /root/.m2:rw,noexec,nosuid,size=512m
    --mount "type=bind,src=${project_dir}/pom.xml,dst=/workspace/pom.xml,readonly"
    --mount "type=bind,src=${project_dir}/src,dst=/workspace/src,readonly"
    -e "MINISKY_JAVA_COMPILE_ONLY=${MINISKY_JAVA_COMPILE_ONLY:-0}"
  )
  if [[ "${MINISKY_JAVA_COMPILE_ONLY:-0}" != "1" ]]; then
    : "${MINISKY_ENDPOINT:?MINISKY_ENDPOINT is required}"
    : "${MINISKY_PROJECT_ID:?MINISKY_PROJECT_ID is required}"
    : "${MINISKY_JAVA_BUCKET:?MINISKY_JAVA_BUCKET is required}"
    case "${MINISKY_ENDPOINT}" in
      http://127.0.0.1:*|http://localhost:*) ;;
      *) echo "MINISKY_ENDPOINT must be a loopback HTTP endpoint." >&2; exit 2 ;;
    esac
    container_endpoint="${MINISKY_ENDPOINT}"
    proxy_pid=""
    proxy_dir=""
    cleanup_proxy() {
      [[ -n "${proxy_pid}" ]] && kill "${proxy_pid}" 2>/dev/null || true
      [[ -n "${proxy_pid}" ]] && wait "${proxy_pid}" 2>/dev/null || true
      [[ -n "${proxy_dir}" ]] && rm -rf "${proxy_dir}" || true
    }
    trap cleanup_proxy EXIT INT TERM
    if [[ "$(uname -s)" == "Darwin" ]]; then
      command -v python3 >/dev/null 2>&1 || {
        echo "Required command not found: python3" >&2
        exit 1
      }
      proxy_port="$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)"
      proxy_dir="$(mktemp -d)"
      python3 - "${MINISKY_ENDPOINT}" "${proxy_port}" <<'PY' >"${proxy_dir}/proxy.log" 2>&1 &
import gzip
import http.client
import http.server
import sys
import urllib.parse

target = urllib.parse.urlsplit(sys.argv[1])
port = int(sys.argv[2])

class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def relay(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b";", 1)[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)
            body = b"".join(chunks)
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.headers.get("Content-Encoding", "").lower() == "gzip":
            body = gzip.decompress(body)
        connection = http.client.HTTPConnection(target.hostname, target.port)
        headers = {key: value for key, value in self.headers.items()
                   if key.lower() not in {
                       "connection", "content-encoding", "host", "transfer-encoding"
                   }}
        headers["Host"] = target.netloc
        headers["Content-Length"] = str(len(body))
        connection.request(self.command, self.path, body=body, headers=headers)
        response = connection.getresponse()
        payload = response.read()
        self.send_response(response.status)
        for key, value in response.getheaders():
            if key.lower() not in {"connection", "content-length", "transfer-encoding"}:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        connection.close()
    do_GET = relay
    do_POST = relay
    do_PUT = relay
    do_DELETE = relay

http.server.ThreadingHTTPServer(("127.0.0.1", port), Proxy).serve_forever()
PY
      proxy_pid=$!
      container_endpoint="http://host.docker.internal:${proxy_port}"
      docker_args+=(--add-host host.docker.internal:host-gateway -e MINISKY_ALLOW_DOCKER_HOST=1)
    else
      docker_args+=(--network host)
    fi
    docker_args+=(
      -e "MINISKY_ENDPOINT=${container_endpoint}"
      -e "MINISKY_PROJECT_ID=${MINISKY_PROJECT_ID}"
      -e "MINISKY_JAVA_BUCKET=${MINISKY_JAVA_BUCKET}"
    )
  fi
  docker "${docker_args[@]}" "${image}" bash -ceu '
    mvn -q -f /workspace/pom.xml package
    if [[ "${MINISKY_JAVA_COMPILE_ONLY}" == "1" ]]; then exit 0; fi
    mvn -q -f /workspace/pom.xml -DincludeScope=runtime \
      -Dmdep.outputFile=/workspace/target/runtime-classpath.txt dependency:build-classpath
    classpath="$(</workspace/target/runtime-classpath.txt)"
    java -cp "/workspace/target/classes:${classpath}" dev.minisky.StorageSmoke
  '
  cleanup_proxy
  trap - EXIT INT TERM
  exit
fi

for command in java mvn; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done
: "${MINISKY_ENDPOINT:?MINISKY_ENDPOINT is required}"
: "${MINISKY_PROJECT_ID:?MINISKY_PROJECT_ID is required}"
: "${MINISKY_JAVA_BUCKET:?MINISKY_JAVA_BUCKET is required}"

mvn -q -f "${project_dir}/pom.xml" package
mvn -q -f "${project_dir}/pom.xml" \
  -DincludeScope=runtime \
  -Dmdep.outputFile="${classpath_file}" \
  dependency:build-classpath

classpath="$(<"${classpath_file}")"
java -cp "${project_dir}/target/classes:${classpath}" dev.minisky.StorageSmoke
