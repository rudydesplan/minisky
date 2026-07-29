#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "${MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION:-}" != "1" ]]; then
  echo "Refusing to start Storage persistence and Pub/Sub session-boundary integration without MINISKY_STORAGE_PERSISTENCE_PUBSUB_SESSION_INTEGRATION=1." >&2
  exit 2
fi

for command in docker go python3; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Required command not found: ${command}" >&2
    exit 1
  fi
done
docker info >/dev/null

pinned_public_pubsub_image="gcr.io/google.com/cloudsdktool/cloud-sdk:emulators@sha256:f098583c0387cf0fe4c64068ed1e1cf0dbc73e04f0621642b9a7e1f8b26ca287"
storage_image="${MINISKY_STORAGE_TEST_IMAGE:-fsouza/fake-gcs-server:latest@sha256:91afded49de804aa61b5f3eb6c7cd65205acf9e5c5e047cf0ba7d9507af806c8}"
pubsub_image="${MINISKY_PUBSUB_TEST_IMAGE:-${pinned_public_pubsub_image}}"
pubsub_required_platform="linux/amd64"
image_pattern='^[A-Za-z0-9./:_-]+@sha256:[a-f0-9]{64}$'
for image in "${storage_image}" "${pubsub_image}"; do
  if [[ ! "${image}" =~ ${image_pattern} ]]; then
    echo "Boundary integration requires immutable image@sha256 references; received ${image}." >&2
    exit 2
  fi
done

work="$(mktemp -d)"
profile="${MINISKY_EMULATOR_BOUNDARY_PROFILE:-emulator-boundary-$(basename "${work}")}"
if [[ ! "${profile}" =~ ^emulator-boundary-[A-Za-z0-9._-]+$ ]]; then
  echo "MINISKY_EMULATOR_BOUNDARY_PROFILE must start with emulator-boundary- and contain only safe name characters." >&2
  rm -rf "${work}"
  exit 2
fi
state_root="${work}/state"
home="${work}/home"
binary="${work}/minisky"
diagnostics_dir="${MINISKY_EMULATOR_BOUNDARY_DIAGNOSTICS_DIR:-${work}/diagnostics}"
lock_dir="${TMPDIR:-/tmp}/minisky-${profile}.lock"
if ! mkdir -p "${state_root}" "${home}" "${diagnostics_dir}"; then
  rm -rf "${work}"
  exit 1
fi
if ! mkdir "${lock_dir}" 2>/dev/null; then
  echo "Another Storage persistence and Pub/Sub session-boundary run owns ${lock_dir}." >&2
  rm -rf "${work}"
  exit 1
fi

owned_containers() {
  local target_profile="${1:?profile is required}"
  docker ps -aq \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${target_profile}"
}

owned_networks() {
  local target_profile="${1:?profile is required}"
  docker network ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${target_profile}"
}

owned_volumes() {
  local target_profile="${1:?profile is required}"
  docker volume ls -q \
    --filter "label=managed-by=minisky" \
    --filter "label=minisky.profile=${target_profile}"
}

capture_diagnostics() {
  local target_profile
  {
    printf 'profile=%s\n' "${profile}"
    printf 'storage_image=%s\n' "${storage_image}"
    printf 'pubsub_image=%s\n' "${pubsub_image}"
    printf 'pubsub_required_platform=%s\n' "${pubsub_required_platform}"
    docker version 2>&1 || true
    docker ps -a --format '{{.ID}} {{.Image}} {{.Names}} {{.Labels}}' 2>&1 || true
  } >"${diagnostics_dir}/docker-inventory.txt"
  for target_profile in "${profile}" "${profile}-isolated"; do
    while IFS= read -r container; do
      [[ -n "${container}" ]] || continue
      docker inspect "${container}" >"${diagnostics_dir}/container-${container}-inspect.json" 2>&1 || true
      docker logs "${container}" >"${diagnostics_dir}/container-${container}.log" 2>&1 || true
    done < <(owned_containers "${target_profile}")
    while IFS= read -r network; do
      [[ -n "${network}" ]] || continue
      docker network inspect "${network}" >"${diagnostics_dir}/network-${network}-inspect.json" 2>&1 || true
    done < <(owned_networks "${target_profile}")
    while IFS= read -r volume; do
      [[ -n "${volume}" ]] || continue
      docker volume inspect "${volume}" >"${diagnostics_dir}/volume-${volume}-inspect.json" 2>&1 || true
    done < <(owned_volumes "${target_profile}")
  done
}

cleanup_exact_profile() {
  local target_profile="${1:?profile is required}"
  local inventory
  local resource
  local volume_identity
  local failed=0

  if inventory="$(owned_containers "${target_profile}")"; then
    while IFS= read -r resource; do
      [[ -n "${resource}" ]] || continue
      if ! docker rm -f -v "${resource}" >/dev/null; then
        echo "Unable to remove exact-owned container ${resource} for profile ${target_profile}." >&2
        failed=1
      fi
    done <<<"${inventory}"
  else
    echo "Unable to inventory exact-owned containers for profile ${target_profile}." >&2
    failed=1
  fi

  if inventory="$(owned_networks "${target_profile}")"; then
    while IFS= read -r resource; do
      [[ -n "${resource}" ]] || continue
      if ! docker network rm "${resource}" >/dev/null; then
        echo "Unable to remove exact-owned network ${resource} for profile ${target_profile}." >&2
        failed=1
      fi
    done <<<"${inventory}"
  else
    echo "Unable to inventory exact-owned networks for profile ${target_profile}." >&2
    failed=1
  fi

  if inventory="$(owned_volumes "${target_profile}")"; then
    while IFS= read -r resource; do
      [[ -n "${resource}" ]] || continue
      echo "Docker volume cleanup boundary: deletion is name-based with no conditional immutable-ID delete; correctness assumes no hostile or coincident external replacement in the final inspect-to-delete interval." >&2
      if ! volume_identity="$(docker volume inspect \
        --format '{{ index .Labels "managed-by" }}|{{ index .Labels "minisky.profile" }}' \
        "${resource}")"; then
        echo "Unable to re-inspect exact-owned volume ${resource} for profile ${target_profile}; refusing name-based removal." >&2
        failed=1
      elif [[ "${volume_identity}" != "minisky|${target_profile}" ]]; then
        echo "Exact ownership changed for volume ${resource}; expected minisky|${target_profile}, got ${volume_identity}; refusing name-based removal." >&2
        failed=1
      elif ! docker volume rm "${resource}" >/dev/null; then
        echo "Unable to remove exact-owned volume ${resource} for profile ${target_profile}." >&2
        failed=1
      fi
    done <<<"${inventory}"
  else
    echo "Unable to inventory exact-owned volumes for profile ${target_profile}." >&2
    failed=1
  fi

  if inventory="$(owned_containers "${target_profile}")"; then
    if [[ -n "${inventory}" ]]; then
      echo "Exact-owned containers remain after cleanup for profile ${target_profile}: ${inventory}" >&2
      failed=1
    fi
  else
    echo "Unable to verify exact-owned container cleanup for profile ${target_profile}." >&2
    failed=1
  fi
  if inventory="$(owned_networks "${target_profile}")"; then
    if [[ -n "${inventory}" ]]; then
      echo "Exact-owned networks remain after cleanup for profile ${target_profile}: ${inventory}" >&2
      failed=1
    fi
  else
    echo "Unable to verify exact-owned network cleanup for profile ${target_profile}." >&2
    failed=1
  fi
  if inventory="$(owned_volumes "${target_profile}")"; then
    if [[ -n "${inventory}" ]]; then
      echo "Exact-owned volumes remain after cleanup for profile ${target_profile}: ${inventory}" >&2
      failed=1
    fi
  else
    echo "Unable to verify exact-owned volume cleanup for profile ${target_profile}." >&2
    failed=1
  fi

  return "${failed}"
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  local cleanup_profile
  trap - EXIT INT TERM
  set +e

  if (( status != 0 )); then
    capture_diagnostics || true
    for log in "${diagnostics_dir}"/minisky-*.log; do
      if [[ -f "${log}" ]]; then
        printf 'MiniSky boundary daemon log (%s):\n' "${log}" >&2
        python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"))' \
          "${log}" >&2 || true
      fi
    done
  fi

  for cleanup_profile in "${profile}" "${profile}-isolated"; do
    if ! cleanup_exact_profile "${cleanup_profile}"; then
      cleanup_failed=1
    fi
  done
  if (( cleanup_failed != 0 )); then
    capture_diagnostics || true
  fi
  if ! rm -rf "${work}"; then
    cleanup_failed=1
  fi
  if ! rmdir "${lock_dir}" 2>/dev/null; then
    cleanup_failed=1
  fi
  if (( status == 0 && cleanup_failed != 0 )); then
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

pull_pubsub_image() {
  local anonymous_config="${work}/docker-anonymous"
  local docker_host

  if [[ "${pubsub_image}" != "${pinned_public_pubsub_image}" ]]; then
    echo "Acquiring configured Pub/Sub image override with the active Docker credential policy." >&2
    if ! docker pull --platform "${pubsub_required_platform}" "${pubsub_image}"; then
      echo "Configured Pub/Sub image override acquisition failed before capability checks." >&2
      return 1
    fi
    return 0
  fi

  if [[ -n "${DOCKER_HOST:-}" ]]; then
    docker_host="${DOCKER_HOST}"
  elif ! docker_host="$(docker context inspect --format '{{.Endpoints.docker.Host}}')"; then
    echo "Unable to resolve the active Docker daemon endpoint for anonymous public Pub/Sub image acquisition." >&2
    return 1
  fi
  if [[ -z "${docker_host}" ]]; then
    echo "The active Docker daemon endpoint is empty; refusing anonymous public Pub/Sub image acquisition." >&2
    return 1
  fi
  if ! mkdir -p "${anonymous_config}" ||
    ! chmod 0700 "${anonymous_config}" ||
    ! printf '%s\n' '{"auths":{}}' >"${anonymous_config}/config.json" ||
    ! chmod 0600 "${anonymous_config}/config.json"; then
    echo "Unable to create isolated anonymous Docker config for the pinned public Pub/Sub image." >&2
    return 1
  fi

  echo "Acquiring pinned public Pub/Sub image with an isolated anonymous Docker config against the active daemon endpoint." >&2
  if ! docker \
    --config "${anonymous_config}" \
    --host "${docker_host}" \
    pull --platform "${pubsub_required_platform}" "${pubsub_image}"; then
    echo "Anonymous pinned public Pub/Sub image pull failed with host credential helpers bypassed; this is a registry, network, or digest acquisition failure, not an emulator capability failure." >&2
    return 1
  fi
}

for boundary_profile in "${profile}" "${profile}-isolated"; do
  if ! containers="$(owned_containers "${boundary_profile}")" ||
    ! networks="$(owned_networks "${boundary_profile}")" ||
    ! volumes="$(owned_volumes "${boundary_profile}")"; then
    echo "Unable to inventory exact-owned Docker resources for profile ${boundary_profile}." >&2
    exit 1
  fi
  if [[ -n "${containers}${networks}${volumes}" ]]; then
    echo "Refusing to reuse exact-owned Docker resources for profile ${boundary_profile}." >&2
    exit 1
  fi
done

docker pull "${storage_image}"
pull_pubsub_image
if ! docker_engine_platform="$(docker info --format '{{.OSType}}/{{.Architecture}}')"; then
  echo "Unable to determine the Docker engine platform required for the Pub/Sub emulator preflight." >&2
  exit 1
fi
if ! pubsub_image_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "${pubsub_image}")"; then
  echo "Unable to inspect the pinned Pub/Sub emulator image platform." >&2
  exit 1
fi
if [[ "${pubsub_image_platform}" != "${pubsub_required_platform}" ]]; then
  echo "Pinned Pub/Sub emulator resolved to ${pubsub_image_platform}; expected ${pubsub_required_platform}." >&2
  exit 1
fi
preflight_name="minisky-pubsub-data-dir-$(python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest()[:12])' "${profile}")"
if [[ "${docker_engine_platform}" != "linux/amd64" && "${docker_engine_platform}" != "linux/x86_64" ]]; then
  if ! docker run --rm \
    --platform "${pubsub_required_platform}" \
    --name "${preflight_name}-platform" \
    --label managed-by=minisky \
    --label "minisky.profile=${profile}" \
    "${pubsub_image}" \
    sh -c 'exit 0' \
    >"${diagnostics_dir}/pubsub-platform-preflight.txt" 2>&1; then
    python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"), end="")' \
      "${diagnostics_dir}/pubsub-platform-preflight.txt" >&2 || true
    echo "Pinned Pub/Sub emulator requires ${pubsub_required_platform} execution; Docker engine ${docker_engine_platform} cannot run it. Configure amd64 emulation before running this gate." >&2
    exit 1
  fi
fi
if ! docker run --rm \
  --platform "${pubsub_required_platform}" \
  --name "${preflight_name}" \
  --label managed-by=minisky \
  --label "minisky.profile=${profile}" \
  "${pubsub_image}" \
  gcloud beta emulators pubsub start --help \
  >"${diagnostics_dir}/pubsub-emulator-help.txt" \
  2>"${diagnostics_dir}/pubsub-emulator-help.stderr"; then
  python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text(errors="replace"), end="")' \
    "${diagnostics_dir}/pubsub-emulator-help.stderr" >&2 || true
  echo "Unable to inspect pinned Pub/Sub emulator --data-dir support after the ${pubsub_required_platform} platform preflight." >&2
  exit 1
fi
if ! python3 -c 'import pathlib,sys; raise SystemExit(0 if "--data-dir" in pathlib.Path(sys.argv[1]).read_text() else 1)' \
  "${diagnostics_dir}/pubsub-emulator-help.txt"; then
  echo "Pinned Pub/Sub emulator does not advertise --data-dir configuration support." >&2
  exit 1
fi
go build -trimpath -o "${binary}" ./cmd/minisky

go test -race -count=1 ./pkg/orchestrator -run \
  '^(TestCleanupProfileSweepsOnlyExactOwnedDockerResources|TestEnsureDurableEmulatorAllowsVendorLabelsButRejectsOwnershipAndMountMismatch|TestRemoveDurableEmulatorRequiresExactOwnershipBeforeCleanup|TestDurableEmulatorMountCreationFailurePrecedesDocker|TestDurableEmulatorRuntimeDataIsExcludedFromMetadataExport)$'

MINISKY_DOCKER_EMULATOR_BOUNDARY_INTEGRATION=1 \
MINISKY_EMULATOR_BOUNDARY_BINARY="${binary}" \
MINISKY_EMULATOR_BOUNDARY_STATE_DIR="${state_root}" \
MINISKY_EMULATOR_BOUNDARY_PROFILE="${profile}" \
MINISKY_EMULATOR_BOUNDARY_DIAGNOSTICS_DIR="${diagnostics_dir}" \
MINISKY_STORAGE_TEST_IMAGE="${storage_image}" \
MINISKY_PUBSUB_TEST_IMAGE="${pubsub_image}" \
  go test -race -count=1 -v ./pkg/orchestrator -run \
  '^TestStoragePersistenceAndPubSubSessionBoundaries$'

echo "Storage persistence and Pub/Sub session-boundary integration passed for ${profile}."
