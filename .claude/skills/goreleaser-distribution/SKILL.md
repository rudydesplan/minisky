---
name: goreleaser-distribution
description: "Maintain MiniSky's GoReleaser v2 configuration and its contract with GitHub Actions release validation, native CGO artifacts, checksums, Homebrew casks, Scoop, and nFPM packages. Use for `.goreleaser.yaml`, release artifact names/contents, native runner feasibility, or GoReleaser check/snapshot failures. Do not use for ordinary CI, Docker image publishing, or application builds unless they affect the release artifact contract."
---

# GoReleaser Distribution

Keep `.goreleaser.yaml` valid and aligned with the release workflow without assuming GoReleaser publishes every artifact.

## Establish the source of truth

Read:

- `.goreleaser.yaml`
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `Dockerfile`
- `go.mod`, `pkg/version/version.go`, UI lockfile/scripts, and package files referenced by archives

Current split of responsibility:

- CI runs `goreleaser check` and a Linux-amd64 snapshot build.
- Tag releases build four CGO binaries natively in a GitHub Actions matrix and package/publish them manually.
- The release workflow generates and verifies checksums before creating the GitHub release.
- Container images are built, smoke-tested, published with provenance and SBOM attestations, and pushed by Docker actions, not GoReleaser.
- `.goreleaser.yaml` defines archives, Scoop, nFPM, and Homebrew cask configuration, but the tag workflow does not currently invoke `goreleaser release`.

Do not describe configured publishers as deployed until the workflow actually runs them with required credentials.

## Preserve the artifact contract

The current native target set is:

- Linux amd64
- Linux arm64 on a native ARM runner
- macOS arm64
- Windows amd64 with MSYS2 UCRT64 GCC

CGO is required for DuckDB conformance. Prefer native runners because cross-compiling CGO requires target sysroots, compilers, and compatible runtime libraries. QEMU is currently used for container smoke tests, not native binary production.

Artifact names and archive contents must match consumers in the release workflow. If either side changes, update and test both:

```text
minisky_<os>_<arch>.tar.gz
minisky_windows_amd64.zip
checksums.txt
```

Archives currently include the binary, README, CONTRIBUTING guide, license, and docs. Prevent absolute paths and `..` entries.

## GoReleaser v2 configuration

- Keep `version: 2`.
- Use current plural archive fields (`formats`, including in `format_overrides`).
- Pin build IDs used by `nfpms` or CI.
- Build UI assets with `npm ci && npm run build` before compiling Go because `ui/dist` is embedded.
- Use `-trimpath`; inject only variables that exist in `pkg/version`.
- Use `mod_timestamp: "{{ .CommitTimestamp }}"` when reproducibility is in scope, while acknowledging CGO/native toolchains can still vary.
- Keep Windows archives ZIP-compatible for Scoop.
- Verify current GoReleaser docs before changing `homebrew_casks`, `scoops`, or `nfpms`; these sections evolve across v2 releases.

Never run `go mod tidy` as a release hook. Release builds should consume the committed module graph, not mutate it.

## CI and publishing security

- Keep default workflow permissions read-only and grant `contents: write` or `packages: write` only to publishing jobs.
- Use `${{ github.token }}` for the repository release where possible.
- Store cross-repository publisher tokens in scoped secrets; never place token templates in generated artifacts or logs.
- Pin tool major versions as the repository does and use `go-version-file: go.mod`.
- Publish only from validated stable tags (`vMAJOR.MINOR.PATCH`) unless prerelease behavior is explicitly added.
- Use concurrency that does not cancel an in-progress release.
- Verify the tag before publishing and avoid replacing an existing release silently.
- Preserve SBOM/provenance settings for containers; do not claim native archives are attested unless an attestation step exists.

Homebrew casks, Scoop manifests, and nFPM packages need install-time tests on their actual platforms before they become release gates. A syntactically valid GoReleaser config is not proof that a tap/bucket/package works.

## Test-first change workflow

For a release bug, first add or tighten the cheapest deterministic check that would have caught it:

- config validation;
- snapshot build for the affected build ID;
- archive-name/content assertion;
- runtime DLL audit;
- installed-binary smoke test;
- package-manager dry run.

Then make the smallest config/workflow change.

Local/CI validation:

```bash
cd ui && npm ci && npm run build
goreleaser check
goreleaser build --snapshot --clean --id linux-amd64
./dist/linux-amd64_linux_amd64*/minisky version
```

Do not guess the `dist` path in automation; inspect GoReleaser output metadata or use the configured artifact path. Use `goreleaser release --snapshot --clean` only when all configured targets can build on the current host or are intentionally skipped with current `--skip` syntax.

For native CGO targets, reproduce the workflow commands on the matching OS/architecture. Smoke-test the extracted archive, not merely the build output:

```text
minisky version
minisky doctor bigquery
```

On Windows, retain the runtime DLL audit and fail if non-system MinGW runtime DLLs are required but not shipped.

## Cleanup and failure behavior

- Build snapshots in a clean temporary/dist directory.
- Do not delete user artifacts outside GoReleaser's configured output.
- Upload diagnostics with `if: always()` only when they contain no secrets.
- Publishing must depend on every required native artifact; partial target success must not produce a partial GitHub release.
- Container publishing must remain gated on smoke tests and binary release success.

## Acceptance gates

- `goreleaser check` passes with the CI-supported v2 line.
- The changed build ID succeeds as a clean snapshot.
- UI is built from the lockfile before Go compilation.
- Native platform tests and `doctor bigquery` pass for every affected CGO target.
- Extracted archives contain exactly the required safe paths and executable name.
- SHA-256 checksums cover and verify the full artifact set.
- Windows runtime dependencies are audited.
- Package-manager configuration is validated without publishing.
- No release, package, tap, bucket, or image is published during validation.
- Workflow permissions and secrets remain least-privilege.

Report which path was validated: GoReleaser snapshot, native tag workflow, package manager, or container pipeline.
