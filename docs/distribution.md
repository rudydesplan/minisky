# Distribution

MiniSky's release workflow runs only for stable `vMAJOR.MINOR.PATCH` tags. Before
publishing, it builds and tests the native archives and runs
`minisky doctor bigquery` from both the `linux/amd64` and `linux/arm64`
container images.

## GitHub releases

Each release contains the supported native archives and a `checksums.txt` file.
The workflow validates each archive's integrity, rejects unsafe paths, checks
for the binary, license, documentation, and readme files, and verifies the
generated SHA-256 checksums before creating the release.

## GHCR

The workflow publishes `ghcr.io/<owner>/<repository>` with these tags:

- the immutable release version, such as `1.2.3`
- the moving minor and major tags, such as `1.2` and `1`
- `latest`

The manifest includes `linux/amd64` and `linux/arm64` images plus BuildKit
provenance and SBOM attestations. Publishing uses the workflow-scoped
`GITHUB_TOKEN` with only `contents: read` and `packages: write`; no registry
secret is required. Pull requests cannot publish because the release workflow
is triggered only by version-tag pushes and the publish job repeats that gate.

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
