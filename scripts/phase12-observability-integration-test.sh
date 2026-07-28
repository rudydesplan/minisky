#!/usr/bin/env bash

set -Eeuo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
script="${repository_root}/scripts/phase12-observability-integration.sh"
workflow="${repository_root}/.github/workflows/ci.yml"
makefile="${repository_root}/Makefile"
sanitizer="${repository_root}/scripts/phase12-sanitize-diagnostics.py"
sanitizer_test="${repository_root}/scripts/phase12-sanitize-diagnostics-test.py"

fail() {
  echo "Phase 12 harness contract failed: $*" >&2
  exit 1
}

require_text() {
  local file=$1
  local text=$2
  python3 - "${file}" "${text}" <<'PY' || fail "${file#"$repository_root"/} is missing: ${text}"
import pathlib
import sys

raise SystemExit(0 if sys.argv[2] in pathlib.Path(sys.argv[1]).read_text() else 1)
PY
}

for command in bash go python3; do
  command -v "${command}" >/dev/null 2>&1 || fail "required command not found: ${command}"
done

bash -n "${script}"

refusal_log="$(mktemp)"
contract_root="$(mktemp -d)"
trap 'rm -f "${refusal_log}"; rm -rf "${contract_root}"' EXIT
set +e
env -u MINISKY_PHASE12_OBSERVABILITY_INTEGRATION \
  -u MINISKY_PHASE12_OBSERVABILITY_SELF_TEST \
  "${script}" >"${refusal_log}" 2>&1
refusal_status=$?
set -e
[[ "${refusal_status}" -eq 2 ]] || fail "unguarded execution returned ${refusal_status}, want 2"
require_text "${refusal_log}" "Refusing Phase 12 observability integration"

self_test_output="$(
  MINISKY_PHASE12_OBSERVABILITY_SELF_TEST=1 "${script}"
)"
[[ "${self_test_output}" == *"loopback guard self-test passed"* ]] ||
  fail "loopback guard self-test did not report success"

for text in \
  'HOME="${home}"' \
  'MINISKY_STATE_DIR="${state_root}"' \
  'MINISKY_PROFILE="${profile}"' \
  '--bind 127.0.0.1' \
  'traceparent: 00-${trace_id}-${parent_id}-01' \
  'Authorization: Bearer ${secret}' \
  'Cookie: session=${secret}' \
  'Cross-project replay lookup returned' \
  'unexpected gateway metric labels' \
  'Exporter failure changed the API status' \
  'MiniSky did not complete graceful shutdown' \
  'assert_loopback_listeners' \
  'MINISKY_PHASE12_DIAGNOSTICS_DIR' \
  'scripts/phase12-otlp-inspect' \
  'scripts/phase12-sanitize-diagnostics.py' \
  'phase12-unknown-host-' \
  '--required-service "cloudresourcemanager.googleapis.com"' \
  '--required-service "other"' \
  '--resource-service "minisky"' \
  'otlp-captures' \
  'MAX_OTLP_BODY_BYTES' \
  'MAX_DIAGNOSTIC_FILE_BYTES' \
  'MINISKY_PHASE12_TEST_FAIL_AFTER_PROBE'; do
  require_text "${script}" "${text}"
done

(cd "${repository_root}" && go test ./scripts/phase12-otlp-inspect)

python3 - "${sanitizer}" "${sanitizer_test}" <<'PY'
import ast
import pathlib
import sys
for path in sys.argv[1:]:
    ast.parse(pathlib.Path(path).read_text())
PY
python3 "${sanitizer_test}"
mkdir -p "${contract_root}/source"
python3 - "${contract_root}/source" "${contract_root}/forbidden.json" <<'PY'
import json
import pathlib
import sys

source = pathlib.Path(sys.argv[1])
secret = "artifact-must-not-upload"
source.joinpath("minisky.log").write_text((secret + "\n") * 4096)
source.joinpath("requests.json").write_text(json.dumps({"secret": secret}))
source.joinpath("otlp-0001.pb").write_bytes(secret.encode())
pathlib.Path(sys.argv[2]).write_text(json.dumps({"artifact secret": secret}))
PY
python3 "${sanitizer}" \
  --source-dir "${contract_root}/source" \
  --destination-dir "${contract_root}/diagnostics" \
  --forbidden-file "${contract_root}/forbidden.json" \
  --max-file-bytes 1024 \
  --max-total-bytes 2048
python3 - "${contract_root}/diagnostics" <<'PY' || fail "failure diagnostics were not sanitized and bounded"
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
files = [path for path in root.iterdir() if path.is_file()]
if any(b"artifact-must-not-upload" in path.read_bytes() for path in files):
    raise SystemExit("artifact secret was retained")
if sum(path.stat().st_size for path in files) > 2048:
    raise SystemExit("artifact total exceeded its cap")
if any(path.name.startswith("otlp-") for path in files):
    raise SystemExit("raw OTLP capture was copied to diagnostics")
PY

require_text "${makefile}" 'test-phase12-observability-contract:'
require_text "${makefile}" './scripts/phase12-observability-integration-test.sh'
require_text "${makefile}" 'test-phase12-observability: test-phase12-observability-contract'

python3 - "${workflow}" <<'PY' || fail "CI does not require the guarded Phase 12 gate"
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
workflow_header = text.split("jobs:", 1)[0]
if "push:\n    branches: [main]" not in workflow_header or "pull_request:" not in workflow_header:
    raise SystemExit("Phase 12 required workflow is not triggered for pull requests and main pushes")
if "On manual dispatch, run required Phase 12 loopback observability acceptance" not in workflow_header:
    raise SystemExit("Phase 12 workflow_dispatch input does not describe manual opt-in behavior")
match = re.search(
    r"(?ms)^  phase12-observability-integration:\n(.*?)(?=^  [a-zA-Z0-9_-]+:\n|\Z)",
    text,
)
if not match:
    raise SystemExit("Phase 12 CI job is missing")
job = match.group(1)
required = (
    "github.event_name == 'pull_request'",
    "github.event_name == 'push'",
    "github.event_name == 'workflow_dispatch' && inputs.run_phase12_observability_integration",
    "make test-phase12-observability",
    "MINISKY_PHASE12_DIAGNOSTICS_DIR",
    "if: failure()",
    "phase12-observability-diagnostics",
)
missing = [item for item in required if item not in job]
if missing:
    raise SystemExit("Phase 12 CI job is missing: " + ", ".join(missing))
if "phase12-observability-integration.log" in job:
    raise SystemExit("Phase 12 CI job uploads an unbounded unsanitized tee log")
PY

echo "Phase 12 observability harness contract passed."
