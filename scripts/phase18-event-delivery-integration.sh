#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_PHASE18_EVENT_DELIVERY_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing Phase 18 event delivery integration without MINISKY_PHASE18_EVENT_DELIVERY_INTEGRATION=1." >&2
  exit 2
fi

for command in curl docker go python3; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "Required command not found: ${command}" >&2
    exit 1
  }
done
if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is unavailable; Pub/Sub gateway evidence requires its isolated emulator." >&2
  exit 1
fi

shared_lock="${TMPDIR:-/tmp}/minisky-net-integration.lock"
phase_lock="${TMPDIR:-/tmp}/minisky-phase18-event-delivery-integration.lock"
if ! mkdir "${shared_lock}" 2>/dev/null; then
  echo "Another MiniSky Docker integration is active (${shared_lock})." >&2
  exit 1
fi
if ! mkdir "${phase_lock}" 2>/dev/null; then
  rmdir "${shared_lock}" 2>/dev/null || true
  echo "Another Phase 18 event delivery integration is active (${phase_lock})." >&2
  exit 1
fi

work="$(mktemp -d)"
chmod 700 "${work}"
home="${work}/home"
state_root="${work}/state"
profile="phase18-event-delivery-$$"
project="phase18-event-delivery-$$"
region="us-central1"
workflow_id="event-delivery"
trigger_id="pubsub-to-workflow"
topic_id="event-delivery"
workflow="projects/${project}/locations/${region}/workflows/${workflow_id}"
topic="projects/${project}/topics/${topic_id}"
foreign_topic="projects/${project}/topics/foreign-event-delivery"
foreign_project="${project}-foreign"
foreign_project_topic="projects/${foreign_project}/topics/event-delivery"
pid=""
watchdog_pid=""
publish_pid=""
gateway=""
current_log=""
iam_mode=""
access_token=""
denied_token=""
auth_args=()
admission_pause_file=""
interrupted_execution_name=""
started_at="${SECONDS}"
mkdir -p "${home}" "${state_root}"

owned_containers() {
  docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true
}

owned_volumes() {
  docker volume ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${profile}" 2>/dev/null || true
}

owned_network() {
  docker network inspect --format \
    '{{if and (eq (index .Labels "managed-by") "minisky") (eq (index .Labels "minisky.profile") "'"${profile}"'")}}{{.Id}}{{end}}' \
    minisky-net 2>/dev/null || true
}

print_logs() {
  local log_file
  for log_file in "${work}"/daemon-*.log; do
    [[ -f "${log_file}" ]] || continue
    echo "MiniSky Phase 18 event delivery daemon log (${log_file}):" >&2
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"))' \
      "${log_file}" >&2
  done
  local container
  while IFS= read -r container; do
    [[ -n "${container}" ]] || continue
    echo "Owned container log (${container}):" >&2
    docker logs "${container}" >&2 || true
  done < <(owned_containers)
}

remove_owned_resources() {
  local container
  local volume
  local network
  while IFS= read -r container; do
    [[ -n "${container}" ]] && docker rm -f -v "${container}" >/dev/null 2>&1 || true
  done < <(owned_containers)
  while IFS= read -r volume; do
    [[ -n "${volume}" ]] && docker volume rm "${volume}" >/dev/null 2>&1 || true
  done < <(owned_volumes)
  network="$(owned_network)"
  [[ -z "${network}" ]] || docker network rm "${network}" >/dev/null 2>&1 || true
}

assert_no_owned_resources() {
  if [[ -n "$(owned_containers)" || -n "$(owned_volumes)" || -n "$(owned_network)" ]]; then
    echo "Exact profile-owned Docker resources remain after cleanup." >&2
    return 1
  fi
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "MiniSky daemon remains after cleanup." >&2
    return 1
  fi
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT INT TERM
  if (( status != 0 )); then
    print_logs
  fi
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  pid=""
  if [[ -n "${publish_pid}" ]] && kill -0 "${publish_pid}" 2>/dev/null; then
    kill -TERM "${publish_pid}" 2>/dev/null || true
    wait "${publish_pid}" 2>/dev/null || true
  fi
  publish_pid=""
  if [[ -n "${watchdog_pid}" ]] && kill -0 "${watchdog_pid}" 2>/dev/null; then
    kill -TERM "${watchdog_pid}" 2>/dev/null || true
    wait "${watchdog_pid}" 2>/dev/null || true
  fi
  remove_owned_resources
  assert_no_owned_resources || cleanup_failed=1
  rm -rf "${work}" || cleanup_failed=1
  rmdir "${phase_lock}" 2>/dev/null || cleanup_failed=1
  rmdir "${shared_lock}" 2>/dev/null || cleanup_failed=1
  if (( cleanup_failed != 0 )); then
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

(
  sleep 300
  echo "Phase 18 event delivery integration exceeded its 5 minute budget." >&2
  kill -TERM "$$"
) &
watchdog_pid=$!

if docker network inspect minisky-net >/dev/null 2>&1; then
  echo "Refusing to disturb an existing Docker network named minisky-net." >&2
  exit 1
fi
while IFS= read -r existing_container_name; do
  case "${existing_container_name}" in
    minisky-*)
      echo "Refusing to disturb existing MiniSky container: ${existing_container_name}" >&2
      exit 1
      ;;
  esac
done < <(docker ps -a --format '{{.Names}}')

free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

poll() {
  local timeout=$1
  local description=$2
  local deadline=$((SECONDS + timeout))
  shift 2
  until "$@"; do
    if (( SECONDS >= deadline )); then
      echo "Timed out after ${timeout}s waiting for ${description}." >&2
      return 1
    fi
    sleep 0.25
  done
}

api_curl() {
  curl "${auth_args[@]}" "$@"
}

start_daemon() {
  local label=$1
  local api_port
  local ui_port
  api_port="$(free_port)"
  ui_port="$(free_port)"
  while [[ "${ui_port}" == "${api_port}" ]]; do
    ui_port="$(free_port)"
  done
  gateway="http://127.0.0.1:${api_port}"
  current_log="${work}/daemon-${label}.log"
  HOME="${home}" MINISKY_STATE_DIR="${state_root}" MINISKY_PROFILE="${profile}" \
    MINISKY_IAM_MODE="${iam_mode}" \
    MINISKY_TEST_WORKFLOWS_ADMISSION_PAUSE_FILE="${admission_pause_file}" \
    MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1 \
    "${work}/minisky" start --bind 127.0.0.1 --port "${api_port}" --ui-port "${ui_port}" \
    >"${current_log}" 2>&1 &
  pid=$!
  poll 30 "${label} gateway readiness" gateway_ready
}

stop_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}"
    wait "${pid}"
  fi
  pid=""
}

interrupt_daemon() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -KILL "${pid}"
    wait "${pid}" 2>/dev/null || true
  fi
  pid=""
}

gateway_ready() {
  curl --fail --silent --show-error "${gateway}/healthz" >/dev/null 2>&1
}

assert_resource_ready() {
  local selector=$1
  local path=$2
  api_curl --globoff --fail --silent --show-error \
    "${gateway}/_minisky/${selector}/v1/${path}" >/dev/null 2>&1
}

assert_resource_missing() {
  local selector=$1
  local path=$2
  local status
  status="$(api_curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' \
    "${gateway}/_minisky/${selector}/v1/${path}")"
  [[ "${status}" == "404" ]]
}

create_workflow() {
  api_curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
    --data-binary "@${work}/workflow.json" \
    "${gateway}/_minisky/workflows.googleapis.com/v1/projects/${project}/locations/${region}/workflows?workflowId=${workflow_id}" \
    >"${work}/workflow-operation.json"
  poll 10 "workflow visibility" assert_resource_ready workflows.googleapis.com "${workflow}"
}

create_trigger() {
  api_curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
    --data-binary "@${work}/trigger.json" \
    "${gateway}/_minisky/eventarc.googleapis.com/v1/projects/${project}/locations/${region}/triggers?triggerId=${trigger_id}" \
    >"${work}/trigger-operation.json"
  poll 10 "Eventarc trigger visibility" assert_resource_ready eventarc.googleapis.com \
    "projects/${project}/locations/${region}/triggers/${trigger_id}"
}

ensure_topic() {
  local topic_resource=${1:-"${topic}"}
  local status
  status="$(api_curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' \
    "${gateway}/_minisky/pubsub.googleapis.com/v1/${topic_resource}")"
  case "${status}" in
    200)
      return 0
      ;;
    404)
      api_curl --globoff --fail --silent --show-error -X PUT \
        -H "Content-Type: application/json" -d '{}' \
        "${gateway}/_minisky/pubsub.googleapis.com/v1/${topic_resource}" >"${work}/topic.json"
      ;;
    *)
      echo "Unexpected Pub/Sub topic lookup status ${status}." >&2
      return 1
      ;;
  esac
}

execution_nonces_absent() {
  local response="${work}/executions-negative.json"
  api_curl --globoff --fail --silent --show-error \
    "${gateway}/_minisky/workflowexecutions.googleapis.com/v1/${workflow}/executions" \
    >"${response}" 2>/dev/null || return 1
  python3 - "${response}" "$@" <<'PY'
import json
import pathlib
import sys

response = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
forbidden = set(sys.argv[2:])
for execution in response.get("executions", []):
    try:
        argument = json.loads(execution.get("argument", ""))
    except (TypeError, json.JSONDecodeError):
        continue
    for message in argument.get("messages", []):
        nonce = message.get("attributes", {}).get("nonce")
        if nonce in forbidden:
            raise SystemExit(f"unexpected execution for isolated nonce {nonce}")
PY
}

assert_no_executions_for_nonces() {
  local seconds=$1
  local deadline=$((SECONDS + seconds))
  shift
  while (( SECONDS < deadline )); do
    execution_nonces_absent "$@"
    sleep 0.25
  done
}

executions_match() {
  local expected_count=$1
  shift
  local response="${work}/executions.json"
  api_curl --globoff --fail --silent --show-error \
    "${gateway}/_minisky/workflowexecutions.googleapis.com/v1/${workflow}/executions" \
    >"${response}" 2>/dev/null || return 1
  python3 - "${response}" "${expected_count}" "$@" <<'PY'
import json
import pathlib
import sys

response = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
expected_count = int(sys.argv[2])
expected_payloads = sys.argv[3:]
executions = response.get("executions", [])
if len(executions) > expected_count:
    raise SystemExit(
        f"unbounded duplicate delivery: got {len(executions)}, expected {expected_count}"
    )
if len(executions) != expected_count:
    raise SystemExit(1)
actual_payloads = sorted(execution.get("argument") for execution in executions)
if actual_payloads != sorted(expected_payloads):
    raise SystemExit(
        f"execution payloads={actual_payloads!r}, expected={sorted(expected_payloads)!r}"
    )
if any(execution.get("state") != "SUCCEEDED" for execution in executions):
    raise SystemExit(1)
if any(execution.get("result") != execution.get("argument") for execution in executions):
    raise SystemExit(
        "terminal Workflow result did not preserve the exact raw PublishRequest payload"
    )
PY
}

assert_no_executions_for() {
  local seconds=$1
  local deadline=$((SECONDS + seconds))
  while (( SECONDS < deadline )); do
    executions_match 0
    sleep 0.25
  done
}

assert_persisted_interrupted_deliveries() {
  python3 - "${state_root}/profiles/${profile}/state.json" "${admission_pause_file}" <<'PY'
import json
import pathlib
import sys

document = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
marker = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
eventarc = document.get("entries", {}).get("eventarc/metadata", {})
workflows = document.get("entries", {}).get("workflows/metadata", {})
delivery_id = marker.get("deliveryId")
execution_name = marker.get("executionName")
delivery = eventarc.get("deliveries", {}).get(delivery_id)
execution = workflows.get("executions", {}).get(execution_name)
admission = workflows.get("eventAdmissions", {}).get(execution_name)
if not delivery or delivery.get("state") != "ATTEMPTING":
    raise SystemExit("pause marker does not identify a persisted ATTEMPTING Eventarc intent")
if execution_name != f"{delivery.get('workflow')}/executions/event-{delivery_id}":
    raise SystemExit("Workflow execution name is not correlated to the Eventarc delivery ID")
if not execution or execution.get("state") != "ACTIVE":
    raise SystemExit("paused Workflow execution admission is not persisted ACTIVE")
if not admission or admission.get("deliveryId") != delivery_id or admission.get("phase") != "ADMITTED":
    raise SystemExit("Workflow admission is not durably paused before start")
PY
  interrupted_execution_name="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["executionName"])' \
    "${admission_pause_file}")"
}

assert_interrupted_execution_terminal() {
  local response="${work}/interrupted-executions.json"
  api_curl --globoff --fail --silent --show-error \
    "${gateway}/_minisky/workflowexecutions.googleapis.com/v1/${workflow}/executions" \
    >"${response}"
  python3 - "${response}" "${interrupted_execution_name}" "${payload_two}" <<'PY'
import json
import pathlib
import sys

response = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
execution_name, payload = sys.argv[2:]
matches = [
    execution for execution in response.get("executions", [])
    if execution.get("name") == execution_name
]
if len(matches) != 1:
    raise SystemExit(f"interrupted execution resources={len(matches)}, want exactly one")
execution = matches[0]
if execution.get("state") != "SUCCEEDED":
    raise SystemExit(f"interrupted execution state={execution.get('state')!r}, want SUCCEEDED")
if execution.get("argument") != payload or execution.get("result") != payload:
    raise SystemExit("interrupted execution did not reach the exact terminal result")
PY
}

go build -trimpath -o "${work}/minisky" ./cmd/minisky

nonce_one="phase18-live-${profile}"
nonce_two="phase18-after-restart-${profile}"
nonce_three="phase18-after-delete-${profile}"
foreign_topic_nonce="phase18-foreign-topic-${profile}"
foreign_project_nonce="phase18-foreign-project-${profile}"
payload_one='{"messages":[{"attributes":{"nonce":"'"${nonce_one}"'"},"data":"cGhhc2UxOC1saXZl"}]}'
payload_two='{"messages":[{"attributes":{"nonce":"'"${nonce_two}"'"},"data":"cGhhc2UxOC1hZnRlci1yZXN0YXJ0"}]}'
payload_three='{"messages":[{"attributes":{"nonce":"'"${nonce_three}"'"},"data":"cGhhc2UxOC1hZnRlci1kZWxldGU="}]}'
foreign_topic_payload='{"messages":[{"attributes":{"nonce":"'"${foreign_topic_nonce}"'"},"data":"cGhhc2UxOC1mb3JlaWduLXRvcGlj"}]}'
foreign_project_payload='{"messages":[{"attributes":{"nonce":"'"${foreign_project_nonce}"'"},"data":"cGhhc2UxOC1mb3JlaWduLXByb2plY3Q="}]}'
printf '%s' "${payload_one}" >"${work}/publish-one.json"
printf '%s' "${payload_two}" >"${work}/publish-two.json"
printf '%s' "${payload_three}" >"${work}/publish-three.json"
printf '%s' "${foreign_topic_payload}" >"${work}/publish-foreign-topic.json"
printf '%s' "${foreign_project_payload}" >"${work}/publish-foreign-project.json"
python3 - "${workflow}" <<'PY' >"${work}/workflow.json"
import json
import sys
print(json.dumps({"sourceContents": json.dumps([{"return": "${args}"}])}))
PY
python3 - "${workflow}" "${topic}" <<'PY' >"${work}/trigger.json"
import json
import sys
workflow, topic = sys.argv[1:]
print(json.dumps({
    "eventFilters": [{
        "attribute": "type",
        "value": "google.cloud.pubsub.topic.v1.messagePublished",
    }],
    "destination": {"workflow": workflow},
    "transport": {"pubsub": {"topic": topic}},
}))
PY

bootstrap_strict_iam() {
  local account_id="phase18-delivery"
  local denied_account_id="phase18-denied"
  local account_email="${account_id}@${project}.iam.gserviceaccount.com"
  local denied_email="${denied_account_id}@${project}.iam.gserviceaccount.com"
  local iam_base="${gateway}/_minisky/iam.googleapis.com/v1"

  curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
    -d '{"accountId":"'"${account_id}"'","serviceAccount":{"displayName":"Phase 18 delivery"}}' \
    "${iam_base}/projects/${project}/serviceAccounts" >"${work}/service-account.json"
  curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
    -d '{"accountId":"'"${denied_account_id}"'","serviceAccount":{"displayName":"Phase 18 denied"}}' \
    "${iam_base}/projects/${project}/serviceAccounts" >"${work}/denied-service-account.json"

  python3 - "${account_email}" <<'PY' >"${work}/project-policy.json"
import json
import sys

member = "serviceAccount:" + sys.argv[1]
permissions = [
    "eventarc.triggers.create",
    "eventarc.triggers.delete",
    "eventarc.triggers.get",
    "pubsub.topics.create",
    "pubsub.topics.delete",
    "pubsub.topics.get",
    "pubsub.topics.publish",
    "workflows.executions.list",
    "workflows.workflows.create",
    "workflows.workflows.delete",
    "workflows.workflows.get",
]
print(json.dumps({"policy": {"version": 1, "bindings": [
    {"role": "permission:" + permission, "members": [member]}
    for permission in permissions
]}}))
PY
  curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
    --data-binary "@${work}/project-policy.json" \
    "${iam_base}/projects/${project}:setIamPolicy" >"${work}/project-policy-response.json"
  curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
    --data-binary "@${work}/project-policy.json" \
    "${iam_base}/projects/${foreign_project}:setIamPolicy" >"${work}/foreign-project-policy-response.json"

  cat >"${work}/issue-token.go" <<'GO'
package main

import (
	"fmt"
	"os"
	"time"

	"minisky/pkg/security"
)

func main() {
	issuer, err := security.LoadIssuer(os.Args[1])
	if err != nil {
		panic(err)
	}
	token, _, err := issuer.Issue(security.TokenRequest{
		Subject: os.Args[2], Audience: "minisky-gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime: 10 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
	fmt.Print(token)
}
GO
  access_token="$(go run "${work}/issue-token.go" \
    "${state_root}/profiles/${profile}" "serviceAccount:${account_email}")"
  denied_token="$(go run "${work}/issue-token.go" \
    "${state_root}/profiles/${profile}" "serviceAccount:${denied_email}")"
}

start_daemon "iam-bootstrap"
bootstrap_strict_iam
stop_daemon
iam_mode="strict"
auth_args=(-H "Authorization: Bearer ${access_token}")
start_daemon "create"

unauthenticated_status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' \
  "${gateway}/_minisky/workflows.googleapis.com/v1/${workflow}")"
[[ "${unauthenticated_status}" == "401" ]] || {
  echo "Strict IAM unauthenticated request returned ${unauthenticated_status}, want 401." >&2
  exit 1
}
denied_status="$(curl --globoff --silent --show-error --output /dev/null --write-out '%{http_code}' \
  -H "Authorization: Bearer ${denied_token}" \
  "${gateway}/_minisky/workflows.googleapis.com/v1/${workflow}")"
[[ "${denied_status}" == "403" ]] || {
  echo "Strict IAM denied principal returned ${denied_status}, want 403." >&2
  exit 1
}

create_workflow
create_trigger
ensure_topic
api_curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
  --data-binary "@${work}/publish-one.json" \
  "${gateway}/_minisky/pubsub.googleapis.com/v1/${topic}:publish" >"${work}/publish-one-response.json"
poll 30 "pre-restart Workflow execution containing the Pub/Sub payload" \
  executions_match 1 "${payload_one}"

ensure_topic "${foreign_topic}"
ensure_topic "${foreign_project_topic}"
api_curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
  --data-binary "@${work}/publish-foreign-topic.json" \
  "${gateway}/_minisky/pubsub.googleapis.com/v1/${foreign_topic}:publish" \
  >"${work}/publish-foreign-topic-response.json"
api_curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
  --data-binary "@${work}/publish-foreign-project.json" \
  "${gateway}/_minisky/pubsub.googleapis.com/v1/${foreign_project_topic}:publish" \
  >"${work}/publish-foreign-project-response.json"
assert_no_executions_for_nonces 2 "${foreign_topic_nonce}" "${foreign_project_nonce}"
executions_match 1 "${payload_one}"

stop_daemon
admission_pause_file="${work}/event-admission-pause.json"
start_daemon "admission-pause"
ensure_topic
api_curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
  --data-binary "@${work}/publish-two.json" \
  "${gateway}/_minisky/pubsub.googleapis.com/v1/${topic}:publish" \
  >"${work}/publish-two-response.json" 2>"${work}/publish-two-error.log" &
publish_pid=$!
poll 30 "persisted Workflow admission pause marker" test -s "${admission_pause_file}"
assert_persisted_interrupted_deliveries
interrupt_daemon
wait "${publish_pid}" 2>/dev/null || true
publish_pid=""

admission_pause_file=""
start_daemon "interrupted-replay"
assert_resource_ready eventarc.googleapis.com \
  "projects/${project}/locations/${region}/triggers/${trigger_id}"
assert_resource_ready workflows.googleapis.com "${workflow}"
ensure_topic
poll 30 "interrupted Eventarc intent to resume its exact Workflow execution" \
  executions_match 2 "${payload_one}" "${payload_two}"
assert_interrupted_execution_terminal

api_curl --globoff --fail --silent --show-error -X DELETE \
  "${gateway}/_minisky/eventarc.googleapis.com/v1/projects/${project}/locations/${region}/triggers/${trigger_id}" \
  >"${work}/trigger-delete-operation.json"
poll 10 "trigger deletion" assert_resource_missing eventarc.googleapis.com \
  "projects/${project}/locations/${region}/triggers/${trigger_id}"
api_curl --globoff --fail --silent --show-error -X DELETE \
  "${gateway}/_minisky/pubsub.googleapis.com/v1/${topic}" >/dev/null
api_curl --globoff --fail --silent --show-error -X DELETE \
  "${gateway}/_minisky/workflows.googleapis.com/v1/${workflow}" \
  >"${work}/workflow-delete-operation.json"
poll 10 "workflow deletion" assert_resource_missing workflows.googleapis.com "${workflow}"

stop_daemon
start_daemon "delete-restart"
assert_resource_missing eventarc.googleapis.com \
  "projects/${project}/locations/${region}/triggers/${trigger_id}"
assert_resource_missing workflows.googleapis.com "${workflow}"

create_workflow
ensure_topic
api_curl --globoff --fail --silent --show-error -X POST -H "Content-Type: application/json" \
  --data-binary "@${work}/publish-three.json" \
  "${gateway}/_minisky/pubsub.googleapis.com/v1/${topic}:publish" >"${work}/publish-three-response.json"
assert_no_executions_for 2

api_curl --globoff --fail --silent --show-error -X DELETE \
  "${gateway}/_minisky/pubsub.googleapis.com/v1/${topic}" >/dev/null
api_curl --globoff --fail --silent --show-error -X DELETE \
  "${gateway}/_minisky/workflows.googleapis.com/v1/${workflow}" >/dev/null
stop_daemon
remove_owned_resources
assert_no_owned_resources

if (( SECONDS - started_at > 300 )); then
  echo "Phase 18 event delivery integration exceeded its 5 minute budget." >&2
  exit 1
fi
echo "Phase 18 strict-IAM public-gateway live dispatch, nonce isolation, exact persisted admission interruption/replay, one correlated Workflow execution resource with terminal result, ordered deletion, and exact profile-owned cleanup passed in $((SECONDS - started_at))s."
