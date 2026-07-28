#!/usr/bin/env bash

set -Eeuo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <required-images-script>" >&2
  exit 2
fi

manifest_script="$1"
[[ -x "${manifest_script}" ]] || {
  echo "Required image manifest is not executable: ${manifest_script}" >&2
  exit 1
}

manifest_output="$("${manifest_script}" --print-required-images)" || {
  echo "Required image manifest failed: ${manifest_script}" >&2
  exit 1
}
[[ -n "${manifest_output}" ]] || {
  echo "No required image was declared by ${manifest_script}." >&2
  exit 1
}

images=()
while IFS= read -r image; do
  images+=("${image}")
done <<<"${manifest_output}"

for image in "${images[@]}"; do
  if [[ ! "${image}" =~ ^[^[:space:]@]+:[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]]; then
    echo "Refusing unpinned or malformed required image: ${image}" >&2
    exit 1
  fi
  echo "Pulling digest-pinned backend image ${image}"
  docker pull "${image}" || {
    echo "Failed to pull digest-pinned image: ${image}" >&2
    exit 1
  }
  docker image inspect "${image}" >/dev/null || {
    echo "Digest-pinned image reference is not available after pull: ${image}" >&2
    exit 1
  }
done
