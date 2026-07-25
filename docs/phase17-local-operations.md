# Phase 17 local operations

Phase 17 provides bounded local CI, source-compiled plugin scaffolding,
benchmarks, quotas, audit evidence, RBAC, and offline bundles. It does not
provide a published setup action, runtime plugin loader, remote marketplace, or
immutable compliance storage.

## GitHub Actions

The repository-local action accepts either a trusted built binary or downloads
a release archive together with `checksums.txt` and verifies SHA-256 before
extraction. It isolates HOME, state, profile, and ports, waits on `/healthz`,
exports `MINISKY_ENDPOINT`, and stops the process in its post step.

```yaml
steps:
  - uses: actions/checkout@v6
  - uses: actions/setup-go@v6
    with:
      go-version-file: go.mod
  - run: go build -trimpath -o minisky ./cmd/minisky
  - id: minisky
    uses: ./.github/actions/setup-minisky
    with:
      binary: ./minisky
      api-port: "18080"
      ui-port: "18081"
      profile: test
      services: compute,storage
  - run: curl --fail --silent "${{ steps.minisky.outputs.endpoint }}/healthz"
```

Using `version: vMAJOR.MINOR.PATCH` instead of `binary` selects a checksummed
GitHub release. The action rejects omitted or moving release versions.
`uses: minisky/setup-minisky@v1` is not published by this repository.

## GitLab CI and Compose

Include `.gitlab/ci/minisky.yml`, then extend the hidden job:

```yaml
include:
  - local: .gitlab/ci/minisky.yml

integration:
  extends: .minisky
  script:
    - curl --fail "$MINISKY_ENDPOINT/healthz"
```

Set `MINISKY_GO_IMAGE` and `MINISKY_DIND_IMAGE` to organization-approved,
immutable image digests before running the job; the template intentionally has
no moving image defaults.

The template builds the current checkout and uses Docker-in-Docker. For the
common Compute/Storage/Pub/Sub Compose topology, supply a release image that
you have verified:

```bash
MINISKY_IMAGE='ghcr.io/qamarudeenm/minisky@sha256:<verified-digest>' \
  docker compose -f deployments/docker-compose.yml up
```

The Docker socket is host-root-equivalent. Use this stack only on a trusted
local or isolated CI host. Compose publishes both listeners on `127.0.0.1` by
default. Remote exposure requires an explicit host-binding override plus TLS,
gateway authentication, firewalling, and a trusted reverse proxy; never expose
the Docker socket.

## Contributor checks

```bash
make test
make test-phase17
make benchmark
MINISKY_INTEGRATION=1 make test-integration
```

`test-phase17` checks the plugin contract, controls, action JavaScript, shell
workflow, and Compose expansion. Docker-backed integration remains explicitly
guarded. `minisky doctor` also validates configured quota JSON and an enabled
audit chain.

## In-tree plugin SDK v0

Generate a source contribution:

```bash
minisky plugin scaffold ./pkg/shims/example \
  --name example \
  --domain example.googleapis.com
go test ./pkg/shims/example
```

The scaffold compiles against `pkg/pluginsdk`, starts with an explicit
`501 UNIMPLEMENTED`, and implements post-boot and shutdown hooks. A contributor
must still add the blank import, registry metadata, compatibility docs, and
tests. The SDK does not load third-party binaries or processes.

`plugin-catalog.schema.json` and `plugin-catalog.example.json` define local
discovery metadata. The example all-zero digest is a placeholder and must be
replaced with the actual source archive SHA-256. There are intentionally no
`plugin install`, `list`, or `remove` commands because no secure runtime loader
exists.

## Benchmarks

Microbenchmarks verify routing, state save/load, and a representative metadata
shim without enforcing invented thresholds:

```bash
make benchmark
```

For a running local daemon, the API harness is loopback-only, duration- and
concurrency-bounded, records environment metadata and sampled errors, and exits
nonzero for transport or non-2xx responses:

```bash
go run ./scripts/api_benchmark.go \
  --endpoint http://127.0.0.1:8080 \
  --path /healthz \
  --duration 5s \
  --concurrency 4 \
  --output /tmp/minisky-benchmark.json
```

CI uploads raw Go benchmark output as an artifact and does not gate on latency
or throughput.

## Quotas

Quotas are disabled by default. Rules use fixed local windows and may apply at
route, service, project, and default scopes:

```bash
export MINISKY_QUOTAS_JSON='{
  "services":{"compute.googleapis.com":{"limit":100,"window":"1m"}},
  "projects":{"team-project":{"limit":20,"window":"1s"}},
  "routes":{"compute.googleapis.com /compute/v1/projects/{id}/zones/{id}/instances":{"limit":10,"window":"1s"}}
}'
minisky start
```

A rejected request returns HTTP `429` with `RESOURCE_EXHAUSTED` and
`Retry-After`. Metrics use only normalized service, route, and scope labels.
These defaults do not alter Terraform behavior unless quotas are explicitly
enabled.

## Audit and local RBAC

```bash
MINISKY_AUDIT_ENABLED=true minisky start
minisky audit verify
minisky audit export --limit 100 audit.json
```

`MINISKY_AUDIT_STRICT=true` writes an attempt before dispatch and rejects a
mutation if that write fails. Records are profile-scoped, append-only JSONL
with a SHA-256 hash chain, and exclude bodies, raw queries, authorization,
cookies, and arbitrary headers. Verification proves internal hash-chain
consistency only. Without an externally anchored checkpoint, a host user who
can rewrite the complete file can also construct a new valid chain; this is not
immutable or compliance-certified storage.

When `MINISKY_IAM_MODE=strict`, the existing verified local bearer principal is
required by both gateway and Dashboard APIs. Project policies can bind:

- `roles/minisky.viewer`: Dashboard and supported gateway reads
- `roles/minisky.editor`: viewer plus bounded mutations
- `roles/minisky.admin`: editor plus bounded destructive operations and the
  dedicated `minisky.dashboard.terminal` permission

Bindings inherit through the Phase 14 project/folder/organization hierarchy.
The role list covers only documented local permissions and is not GCP IAM.

## Air-gapped bundle

Build or obtain the binary and pre-pull any image on a connected staging host:

```bash
go build -trimpath -o /tmp/minisky ./cmd/minisky
./scripts/airgap-bundle.sh create \
  --output /tmp/minisky-airgap \
  --binary /tmp/minisky \
  --image 'ghcr.io/qamarudeenm/minisky@sha256:<verified-digest>'
```

Transfer the directory, then verify it offline before use:

```bash
./scripts/airgap-bundle.sh verify --bundle /media/minisky-airgap
./scripts/airgap-bundle.sh verify --bundle /media/minisky-airgap --load-image
/media/minisky-airgap/minisky version
```

The script never publishes or pulls. Image creation requires the exact image to
already exist locally. Verification requires the manifest's exact file set,
rejects duplicate, missing, and unexpected entries, and checks every digest.
Loading is optional and cannot occur unless the image archive itself verifies.

## Explicitly deferred

- a separately published `setup-minisky` action repository/tag
- remote plugin marketplace and runtime install/list/remove
- loader protocol negotiation, process isolation, signatures, supervision, and
  third-party failure containment
- immutable/WORM audit storage, external identity providers, SSO, SCIM, policy
  administration UI, and compliance certification
- distributed quotas or claims that local benchmark data proves production
  capacity
