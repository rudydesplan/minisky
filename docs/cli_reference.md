# MiniSky CLI reference

The `minisky` binary controls the local daemon and talks directly to the public
API gateway. Service commands do not require the Dashboard to be running.

## API endpoint

All gateway-backed service commands use this precedence:

1. `--endpoint`
2. `MINISKY_ENDPOINT`
3. `http://localhost:8080`

The value is the API gateway base URL, not a Dashboard URL. MiniSky appends a
canonical route in the form `/_minisky/{service}/{service-path}`.

```bash
export MINISKY_ENDPOINT=http://127.0.0.1:8080
minisky storage buckets list --project local-dev-project

# A flag overrides the environment variable.
minisky --endpoint http://minisky.internal:8080 tasks queues list
```

Non-2xx responses are reported as errors, including the HTTP status and any
response message. `MINISKY_UI_PORT` remains a Dashboard/startup setting and is
not used by headless service commands.

## Lifecycle and diagnostics

- `minisky start` starts the API gateway and Dashboard.
  - `--port` sets the gateway port (default `8080`).
  - `--ui-port` sets the Dashboard port (default `8081`).
  - `--tls auto` generates profile-local TLS material; `--tls files` uses
    `--tls-cert` and `--tls-key`.
  - `--tls-client-ca` enables client-certificate verification on the gateway
    only.
  - `--enforce-projects` rejects API paths naming an unknown project.
  - `--token-audience` sets the strict-mode local bearer audience.
  - `--services` exposes only comma-separated service aliases or domains.
  - `--quotas` accepts the documented JSON local quota configuration; empty
    keeps quotas disabled.
  - `--audit` enables profile-scoped mutation records.
  - `--audit-strict` rejects a mutation when its pre-dispatch audit attempt
    cannot be appended.
- `minisky stop` sends the daemon a graceful stop signal.
- `minisky restart` stops and starts the daemon.
- `minisky doctor` checks local requirements and MiniSky dependencies.
  - Includes disk-space and required-image diagnostics.
  - `--fix` installs optional tools and pulls only images declared by MiniSky.
    It does not prune containers, images, volumes, or networks.
- `minisky version` prints the installed version.
- `minisky uninstall` removes managed containers, networks, and local data.
- `minisky diagnostics requests` prints bounded public gateway access records.
- `minisky diagnostics traces` prints records that carry valid trace IDs.
- `minisky diagnostics metrics` prints Prometheus-compatible gateway metrics.
- `minisky diagnostics replay REQUEST_ID` replays an eligible request when
  replay capture was enabled at daemon startup.
- `minisky audit verify [--profile PROFILE]` verifies the complete local hash
  chain.
- `minisky audit export [FILE] [--limit N]` exports at most 10,000 recent
  records.
- `minisky plugin scaffold DIRECTORY --name NAME --domain DOMAIN` creates a
  source-compiled SDK v0 contribution. Runtime plugin install/list/remove are
  not implemented.

Diagnostics use `--diagnostics-endpoint`, then
`MINISKY_DIAGNOSTICS_ENDPOINT`, then `http://127.0.0.1:8081`. This is the
loopback Dashboard listener, not the public API gateway. Replay is disabled by
default and rejects omitted, oversized, non-JSON, or sensitive payloads.

## Headless service commands

### Projects

- `minisky project create PROJECT_ID [--display-name NAME] [--parent RESOURCE]`
- `minisky project list`
- `minisky project switch PROJECT_ID`
- `minisky project delete PROJECT_ID`

`switch` writes only the profile-local CLI default to `cli.json` with `0600`
permissions. `MINISKY_PROJECT_ID` and service command `--project` flags override
that default.

These commands use the public API gateway and canonical routes:

- `minisky artifact-registry repositories list`
  - `--project` (default `local-dev-project`)
  - `--location` (default `us-central1`)
  - Route: `/_minisky/artifactregistry/v1/projects/{project}/locations/{location}/repositories`
  - Related gateway commands:
    `repository-create REPOSITORY_ID`, `repository-delete REPOSITORY_ID`, and
    `packages REPOSITORY_ID`.
- `minisky cloud-build builds list`
  - `--project` (default `local-dev-project`)
  - Route: `/_minisky/cloudbuild/v1/projects/{project}/builds`
- `minisky vertex-ai models list`
  - Route: `/_minisky/aiplatform/v1/internal/models`
- `minisky kms keyrings list`
  - `--project` (default `local-dev-project`)
  - `--location` (default `global`)
  - Route: `/_minisky/cloudkms/v1/projects/{project}/locations/{location}/keyRings`
- `minisky secrets list`
  - `--project` (default `local-dev-project`)
  - Route: `/_minisky/secretmanager/v1/projects/{project}/secrets`
- `minisky tasks queues list`
  - `--project` (default `local-dev-project`)
  - `--location` (default `us-central1`)
  - Route: `/_minisky/cloudtasks/v2/projects/{project}/locations/{location}/queues`
  - `queue-create QUEUE_ID` creates a queue.
  - `enqueue QUEUE_ID URL [BODY]` creates an HTTP task; BODY is base64 encoded
    by the CLI.
- `minisky scheduler jobs list` and `minisky scheduler jobs run JOB_ID`
  use the Cloud Scheduler gateway route and configured project/location.
- `minisky compute instances list`
  - `--project` (default `local-dev-project`)
  - `--zone` (default `us-central1-a`)
  - Route: `/_minisky/compute/compute/v1/projects/{project}/zones/{zone}/instances`
- `minisky storage buckets list`
  - `--project` (default `local-dev-project`)
  - Route: `/_minisky/storage/storage/v1/b?project={project}`
- `minisky pubsub topics list`
  - `--project` (default `local-dev-project`)
  - Route: `/_minisky/pubsub/v1/projects/{project}/topics`
- `minisky gke clusters list`
  - `--project` (default `local-dev-project`)
  - `--location` (default `us-central1-a`)
  - Route: `/_minisky/container/v1/projects/{project}/locations/{location}/clusters`
- `minisky sql instances list`
  - `--project` (default `local-dev-project`)
  - Route: `/_minisky/sqladmin/v1/projects/{project}/instances`
- `minisky dataproc clusters list`
  - `--project` (default `local-dev-project`)
  - `--region` (default `us-central1`)
  - Route: `/_minisky/dataproc/v1/projects/{project}/regions/{region}/clusters`
- `minisky bigtable instances list`
  - `--project` (default `local-dev-project`)
  - Route: `/_minisky/bigtableadmin/v2/projects/{project}/instances`
- `minisky logs tail`
  - Polls `POST /_minisky/logging/v2/entries:list`.

### Unsupported headless commands

The Spanner emulator is registered as a lazy Docker backend and has no public
MiniSky HTTP shim route. Consequently, these commands return a clear
unsupported message instead of contacting the Dashboard:

- `minisky spanner instances list`
- `minisky spanner instances create [instance-id]`

The existing Spanner `--project` flag is retained for compatibility.

## Serverless

`minisky deploy` deploys a Cloud Function or Cloud Run service.

```bash
minisky deploy \
  --name my-function \
  --runtime python312 \
  --entry-point handler \
  --source main.py \
  --type function
```

Required flags are `--name` and `--source`. `--runtime` defaults to
`python312`, `--entry-point` to `handler`, and `--type` to `function`. Deploy
uses `POST /_minisky/cloudfunctions/v2/deploy`.

`minisky list` lists active Cloud Functions and Cloud Run services through
`/_minisky/cloudfunctions/v2/functions` and `/_minisky/run/v2/services`.

## State snapshots

- `minisky state export [FILE]` exports portable metadata to a file, or stdout
  when `FILE` is omitted.
- `minisky state import [FILE]` atomically imports metadata from a file, or
  stdin when `FILE` is omitted.
- `--profile` selects a state profile; otherwise `MINISKY_PROFILE` or `default`
  is used.

Snapshots intentionally exclude DuckDB files and other binary service data.

## Environment variables

- `MINISKY_ENDPOINT`: API gateway base URL for headless service commands.
- `MINISKY_PORT`: API gateway listen port used by `minisky start`.
- `MINISKY_UI_PORT`: Dashboard listen port only.
- `MINISKY_PROFILE`: state profile used by snapshot commands.
- `MINISKY_PROJECT_ID`: project override for project-aware SDK smoke and CLI
  default selection.
- `MINISKY_TLS_MODE`, `MINISKY_TLS_CERT`, `MINISKY_TLS_KEY`, and
  `MINISKY_TLS_CLIENT_CA`: transport configuration.
- `MINISKY_ENFORCE_PROJECTS`: optional gateway project-existence enforcement.
- `MINISKY_TOKEN_AUDIENCE`: strict-mode local token audience.
- `MINISKY_SERVICES`: comma-separated exposed service aliases or domains.
- `MINISKY_QUOTAS_JSON`: route/service/project/default quota JSON; unset means
  unlimited.
- `MINISKY_AUDIT_ENABLED` and `MINISKY_AUDIT_STRICT`: opt-in profile mutation
  audit modes.
- `MINISKY_IAM_MODE=strict`: enables the documented bounded mutation checks;
  gateway and Dashboard callers use a verified local bearer token.
- `STORAGE_EMULATOR_HOST`: SDK endpoint, typically `http://localhost:8080`.
- `PUBSUB_EMULATOR_HOST`: SDK endpoint, typically `http://localhost:8080`.
