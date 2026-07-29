# Distribution

MiniSky's release workflow runs only for stable `vMAJOR.MINOR.PATCH` tags. It
first requires successful mandatory checks attached to the exact tagged commit;
relevant runtime changes also require the critical provider, durability, and
Kind checks. Before publishing, it builds and tests the native archives and runs
`minisky doctor bigquery` from both the `linux/amd64` and `linux/arm64`
container images.

## GitHub releases

Each release contains the supported native archives, `container-digests.json`,
and a `checksums.txt` file that covers every archive plus the container digest
evidence. The evidence records the immutable multi-platform index digest, both
tested platform digests, and the source commit SHA. The workflow validates each
archive's integrity, rejects unsafe paths, checks for the binary, license,
documentation, and readme files, and verifies the generated SHA-256 checksums
before creating the release.

Download the archive and `checksums.txt` as separate files, require exactly one
checksum line for the selected archive, and pipe only that line to
`sha256sum --check --strict -` on Linux or `shasum -a 256 --check -` on macOS
before extraction. MiniSky does not require executing a downloaded installer
script.

The repository-local action at `.github/actions/setup-minisky` applies the same
release checksum requirement or accepts a caller-supplied binary. It is tested
from a built artifact in CI but is not published as a separate
`minisky/setup-minisky@v1` action. Release downloads require an explicit stable
`vMAJOR.MINOR.PATCH` input; moving `latest` selection is rejected.

For disconnected transfer, `scripts/airgap-bundle.sh` packages a supplied
binary and optionally an already-present image, emits SHA-256 metadata, and
verifies all files before optional `docker load`. It never publishes or pulls.

## Local Phase 11 distribution gate

`make test-phase11-distribution` is the non-destructive local and CI entry
point. It runs `goreleaser check`, verifies the GoReleaser v2 snapshot artifact
contract against the supported release archives and local action targets,
checks the local action JavaScript syntax, exercises the air-gap bundle, and
renders the Compose configuration. It does not create a release, push an image,
or update any package repository. The harness itself has a dependency-light
static self-test:

```bash
./scripts/phase11-distribution-test.sh --self-test
make test-phase11-distribution
```

The native `deb`/`rpm` build, install, smoke-test, and uninstall lifecycle
changes the host package database and builds CGO artifacts. It is therefore
disabled by default and runs only on native Linux `x86_64` or `aarch64` when
explicitly requested:

```bash
MINISKY_PHASE11_DISTRIBUTION_BUILD=1 make test-phase11-distribution
```

The guarded path retains the package test's refusal to replace an existing
MiniSky installation, `/usr/bin/minisky`, or GoReleaser `dist` directory, and
requires root or passwordless `sudo`.

On 2026-07-27,
[CI run 30287887431](https://github.com/rudydesplan/minisky/actions/runs/30287887431)
passed on commit `d657e4b0b77a34ddb615124db2d82da810238502`.
The read-only jobs installed pinned GoReleaser 2.17.0, passed the static
distribution contract, built the Linux AMD64 release snapshot, and completed
native AMD64 and ARM64 deb/rpm build → install → smoke → uninstall. The same
run passed Linux ARM64, macOS ARM64, and Windows AMD64 DuckDB/CGO conformance;
the Windows job also passed its MinGW runtime DLL audit. This is CI evidence,
not publication evidence: no release, image, package repository, tap, or bucket
was published.

That historical Windows AMD64 CGO run does not verify the newer
`windows-state-markers` lifecycle. PR #23's exact-head
[general CI run 30431422742](https://github.com/rudydesplan/minisky/actions/runs/30431422742)
passed the native
[`windows-state-markers` job](https://github.com/rudydesplan/minisky/actions/runs/30431422742/job/90509292114)
and its authoritative
[`quality` aggregate](https://github.com/rudydesplan/minisky/actions/runs/30431422742/job/90510621655)
on commit `794b68439c59bfa0dd35b37962049a1a3e510ea1`. Windows
cross-compilation and workflow-contract tests also passed locally at source SHA-256
`328b4cb13c6ca1705ca51d0e3fb543a830cd6a4af2be8aa8ef3ebda456873a25`
and diff SHA-256
`25318c4dffcf6f04931fe84d1b7cb27218cc0c3a4f8cb63e46f8ff1f90469033`;
those local prerequisites remain distinct from the immutable native CI pass.

## GHCR

MiniSky publishes no GHCR tags: not exact semantic versions and not moving
`latest`, major, or minor aliases. The exact container identity is the
checksummed `container-digests.json` GitHub Release asset. Its index is pushed
and referenced only by digest, and is assembled from two platform digests that
were individually vulnerability-scanned and started through the shipped
entrypoint until `/healthz` was ready.

Download and verify the exact release evidence, then pull by digest:

```bash
VERSION=v1.2.3
gh release download "${VERSION}" \
  --repo qamarudeenm/minisky \
  --pattern checksums.txt \
  --pattern container-digests.json
test "$(awk '$2 == "container-digests.json" { count++ } END { print count+0 }' checksums.txt)" -eq 1
awk '$2 == "container-digests.json"' checksums.txt |
  sha256sum --check --strict -
IMAGE="$(jq -r .image container-digests.json)"
DIGEST="$(jq -r .indexDigest container-digests.json)"
docker pull "${IMAGE}@${DIGEST}"
```

On macOS, use the same exact-line filter with
`shasum -a 256 --check -` instead of `sha256sum --check --strict -`. The
manifest includes `linux/amd64` and `linux/arm64` images plus BuildKit
provenance and SBOM attestations. Publishing uses only the workflow-scoped
`GITHUB_TOKEN`; no PAT, GitHub App credential, repository ruleset, or registry
secret is required.

Immediately before each registry digest push and GitHub Release asset write,
the workflow dereferences the annotated release tag through the GitHub API and
requires it to equal the original workflow `GITHUB_SHA`. The checksummed
evidence records that source SHA. Existing-release retries accept only the
exact complete expected asset set when every asset is byte-identical. New
release creation specifies the target SHA and verifies that the remote tag
already exists. These visible checks do not provide impossible atomic
protection against a malicious authorized tag force-move between API calls;
maintainers must not move published release tags. The source SHA and
content-addressed digests make the published evidence independently
verifiable.

Promotions are serialized FIFO with GitHub Actions'
`concurrency.queue: max`; the workflow intentionally omits
`cancel-in-progress`. Separate stable tags are therefore not cancelled while
waiting. That queue schema is authoritative for GitHub-hosted validation.
Older local `actionlint` versions may reject only the `queue` key before their
schema catches up; local validation must use a narrowly scoped ignore for that
single schema-lag diagnostic and still fail on every other workflow error.

## Homebrew and Scoop

Homebrew tap and Scoop bucket publishing are not automated. Adding either
publisher requires all of the following before a workflow should be enabled:

1. A maintainer-owned destination repository (for example, a Homebrew tap or
   Scoop bucket) with branch protection.
2. A fine-grained token or GitHub App credential limited to contents write on
   that destination repository, stored as `HOMEBREW_TAP_TOKEN` or
   `SCOOP_BUCKET_TOKEN`.
3. A protected `package-manager-publish` GitHub environment with required
   reviewers and access restricted to stable version tags.
4. A job that runs only after the GitHub release exists, consumes its published
   checksums, and installs and smoke-tests the generated formula or manifest on
   a native runner before updating the destination repository.

Until those repositories, credentials, and approval gates exist, releases must
not claim to update a Homebrew tap or Scoop bucket automatically.

The Compose example binds gateway and Dashboard ports to `127.0.0.1` by
default. Exposing either listener remotely requires an explicit bind override,
TLS, strict gateway authentication, host firewall policy, and a trusted reverse
proxy. The mounted Docker socket must never be exposed remotely.

## deb and rpm

GoReleaser declares nFPM `deb` and `rpm` packages, but the native tag workflow
does not invoke `goreleaser release` and therefore does not publish those
packages. CI now configures native amd64 and arm64 jobs that use GoReleaser v2
to build one host-architecture snapshot, inspect package contents, install and
smoke-test each format independently, and verify uninstallation. The jobs have
read-only repository permissions and explicitly skip publishing and
announcements. Both architecture jobs passed on 2026-07-25, including
`minisky version`, `minisky doctor bigquery`, and post-uninstall removal
checks. This is package-build evidence, not publication evidence.

Local evidence from 2026-07-25 validates the GoReleaser v2 configuration,
builds the macOS ARM64 snapshot, and runs both `minisky version` and
`minisky doctor bigquery` from that artifact. The bundled Buildpacks CLI is
Pack 0.40.8, which is compatible with Docker daemons that reject the legacy
API level used by Pack 0.34.2. This evidence does not replace native Linux
install-from-repository tests or credentialed package publication.

External `deb` and `rpm` repository publication remains blocked for the same
operational reasons as Homebrew and Scoop: maintainers must first provision
the destination repositories, narrowly scoped publishing tokens, and protected
environments with required approval. Until those controls and
install-from-repository tests exist, the project has local package-build
evidence only and must not claim repository publication.
