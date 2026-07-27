#!/usr/bin/env bash

set -Eeuo pipefail

release_asset_set_decision() {
  local actual=$1
  shift
  local actual_sorted expected_sorted
  actual_sorted="$(printf '%s\n' "${actual}" | awk 'NF' | sort -u)"
  expected_sorted="$(printf '%s\n' "$@" | awk 'NF' | sort -u)"
  if [[ "${actual_sorted}" != "${expected_sorted}" ]]; then
    echo "Existing release asset set is missing, unexpected, or conflicting" >&2
    return 1
  fi
  printf 'identical-set\n'
}

release_boundary_decision() {
  local boundary=$1
  local observed=$2
  local expected=$3
  if [[ "${observed}" != "${expected}" ]]; then
    echo "Release tag moved before ${boundary}: ${observed} != ${expected}" >&2
    return 1
  fi
  printf 'unchanged\n'
}

verify_release_tag_sha() {
  local tag=$1
  local expected=$2
  local repository=$3
  local object type sha
  object="$(gh api "repos/${repository}/git/ref/tags/${tag}" --jq '.object')"
  type="$(jq -r '.type' <<< "${object}")"
  sha="$(jq -r '.sha' <<< "${object}")"
  for _ in {1..16}; do
    case "${type}" in
      commit)
        release_boundary_decision "external write" "${sha}" "${expected}" >/dev/null
        printf '%s\n' "${sha}"
        return
        ;;
      tag)
        object="$(gh api "repos/${repository}/git/tags/${sha}" --jq '.object')"
        type="$(jq -r '.type' <<< "${object}")"
        sha="$(jq -r '.sha' <<< "${object}")"
        ;;
      *)
        echo "Release tag ${tag} resolves to unsupported object type ${type}" >&2
        return 1
        ;;
    esac
  done
  echo "Release tag ${tag} annotation chain is too deep" >&2
  return 1
}

if [[ "${MINISKY_RELEASE_TAG_POLICY_SELF_TEST:-}" == "1" ]]; then
  workflow=.github/workflows/release.yml
  for forbidden in 'docker/metadata-action' 'type=semver' 'value=latest' 'pattern={{major}}' '--tag ' '--verify-tag-ruleset' '/rulesets'; do
    if grep -Fq -- "${forbidden}" "${workflow}"; then
      echo "Digest-only release workflow contains forbidden registry tag or ruleset policy: ${forbidden}" >&2
      exit 1
    fi
  done
  grep -Fq "manifests/\${tested_index_digest}" "${workflow}"
  grep -Fq -- '--request PUT' "${workflow}"
  grep -Fq 'push-by-digest=true' "${workflow}"
  grep -Fq -- "--target \"\${GITHUB_SHA}\"" "${workflow}"
  grep -Fq 'container-release-evidence' "${workflow}"
  grep -Fq 'container-digests.json' "${workflow}"
  grep -Fq 'cmp "release-assets/' "${workflow}"
  if grep -E 'secrets\.[A-Za-z0-9_]+' "${workflow}" | grep -Ev 'secrets\.GITHUB_TOKEN' >/dev/null; then
    echo "Release workflow must deploy with only the standard GITHUB_TOKEN" >&2
    exit 1
  fi
  test "$(grep -Fc -- '--verify-tag-sha' "${workflow}")" -ge 5
  grep -Fq 'group: release-promotion-${{ github.repository }}' "${workflow}"
  grep -Fq 'cancel-in-progress: false' "${workflow}"
  if grep -Fq 'queue:' "${workflow}"; then
    echo "Release concurrency must use only supported GitHub Actions keys" >&2
    exit 1
  fi
  expected_assets=(
    checksums.txt
    container-digests.json
    minisky_darwin_arm64.tar.gz
    minisky_linux_amd64.tar.gz
    minisky_linux_arm64.tar.gz
    minisky_windows_amd64.zip
  )
  actual_assets="$(printf '%s\n' "${expected_assets[@]}")"
  test "$(release_asset_set_decision "${actual_assets}" "${expected_assets[@]}")" = "identical-set"
  if release_asset_set_decision "${actual_assets}" "${expected_assets[@]}" extra.deb >/dev/null 2>&1; then
    exit 1
  fi
  if release_asset_set_decision "${actual_assets}" "${expected_assets[@]:1}" >/dev/null 2>&1; then
    exit 1
  fi
  for boundary in platform-digest-push digest-index-push release-upload retry-acceptance evidence-upload; do
    test "$(release_boundary_decision "${boundary}" abc abc)" = "unchanged"
    if release_boundary_decision "${boundary}" moved original >/dev/null 2>&1; then
      exit 1
    fi
  done
  fixture_dir="$(mktemp -d)"
  printf '%s\n' '{"indexDigest":"sha256:test"}' > "${fixture_dir}/container-digests.json"
  printf 'archive\n' > "${fixture_dir}/archive.tar.gz"
  (
    cd "${fixture_dir}"
    sha256sum archive.tar.gz container-digests.json > checksums.txt
    test "$(awk '$2 == "container-digests.json" { count++ } END { print count+0 }' checksums.txt)" -eq 1
    awk '$2 == "container-digests.json"' checksums.txt | sha256sum --check --strict -
    awk '$2 == "container-digests.json"' checksums.txt | shasum -a 256 --check -
  )
  rm -rf "${fixture_dir}"
  grep -Fq 'publishes no GHCR tags' docs/distribution.md
  grep -Fq 'records that source SHA' docs/distribution.md
  grep -Fq 'publishes no GHCR tags' README.md
  echo "Release tag policy self-test passed."
  exit 0
fi

if [[ "${1:-}" == "--release-assets" ]]; then
  if [[ "$#" -lt 3 ]]; then
    echo "Usage: $0 --release-assets ACTUAL_NEWLINE_LIST EXPECTED_ASSET..." >&2
    exit 2
  fi
  actual=$2
  shift 2
  release_asset_set_decision "${actual}" "$@"
  exit
fi

if [[ "${1:-}" == "--verify-tag-sha" ]]; then
  if [[ "$#" -ne 4 ]]; then
    echo "Usage: $0 --verify-tag-sha TAG EXPECTED_SHA OWNER/REPOSITORY" >&2
    exit 2
  fi
  verify_release_tag_sha "$2" "$3" "$4"
  exit
fi

echo "Usage: $0 --release-assets ... | --verify-tag-sha ..." >&2
exit 2
