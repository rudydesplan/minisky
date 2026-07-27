# MiniSky: The Complete User Guide

**Website:** [minisky.bmics.com.ng](https://minisky.bmics.com.ng)

MiniSky is a local emulator for bounded Google Cloud development and CI
workflows. One gateway exposes custom Go shims and selected Docker-backed
emulators; it is not a full local GCP environment.

TLS, mTLS, local credentials, strict IAM, and their non-production boundaries
are documented in [Security simulation](security-simulation.md).
CI templates, plugin SDK v0, quotas, audit/RBAC, benchmarks, and offline bundles
are documented in [Phase 17 local operations](phase17-local-operations.md).

## Table of Contents
1. [Core Configuration](#core-configuration)
2. [Serverless (Functions & Cloud Run)](#serverless)
3. [Cloud Storage (GCS)](#cloud-storage)
4. [Pub/Sub](#pubsub)
5. [Databases (Firestore, SQL, Bigtable)](#databases)
6. [Observability (Logging & Monitoring)](#observability)
7. [BigQuery](#bigquery)
8. [Lazy Loading & State](#lazy-loading--state)

---

## Core Configuration

To use MiniSky, you must point your Google Cloud SDKs to the local gateway.

- **API Gateway**: `http://localhost:8080`
- **Dashboard**: `http://localhost:8081`
- **Project ID**: `local-dev-project` (Default)

### Environment Variables
Set these in your terminal or `.env` file:
```bash
export STORAGE_EMULATOR_HOST=http://localhost:8080
export PUBSUB_EMULATOR_HOST=http://localhost:8080
export FIRESTORE_EMULATOR_HOST=localhost:8080
export BIGQUERY_EMULATOR_HOST=http://localhost:8080
```

---

## Serverless

The default simulation profile stores Serverless metadata but does not execute
user code. Use the `full` runtime profile or
`MINISKY_SERVERLESS_BACKEND=buildpacks` to request executable local containers.
That mode requires Docker and `pack`; missing dependencies, source, or a Cloud
Run image fail explicitly.

### Deploying via Dashboard
1. Go to **Compute Engine Instances** -> **Serverless Console**.
2. Click **Deploy New**.
3. Select **Cloud Functions v2** or **Cloud Run**.
4. Enter your code and click **Deploy**.

### Event Triggers (GCS)
You can trigger functions automatically on GCS uploads.
```python
def handler(event, context):
    print(f"Triggered by file: {event['name']}")
```

## Strict local IAM

IAM enforcement is permissive by default. Set `MINISKY_IAM_MODE=strict` to
enforce the bounded method-aware route map documented in the service
compatibility reference. Callers must send a MiniSky-issued local bearer token;
the gateway derives the principal from the verified token and ignores
caller-supplied principal headers. For the Dashboard, paste a dashboard-audience
token into **Project → Strict IAM session**. It is retained only in that tab's
session storage and sent to same-origin Dashboard APIs. Terminal access also
requires the dedicated admin terminal permission. This local policy engine is
not production GCP IAM.

## Doctor and integration targets

`minisky doctor` checks writable state, free disk space, Docker, optional tools,
and configured required images. `minisky doctor --fix` may install MiniSky's
optional tools and pull only images declared by MiniSky; it never prunes global
Docker resources. Those downloads require network access; after the required
tools and images are present, the corresponding checks can run offline.

Use `make dev`, `make test`, `make test-phase17`, `make benchmark`, or the explicitly guarded
`MINISKY_INTEGRATION=1 make test-integration`.

If Docker is unavailable at startup, MiniSky keeps the public gateway,
dashboard, diagnostics, and in-process shims available. Docker-backed services
then fail explicitly instead of preventing the daemon from starting.
Configuration or ownership conflicts still fail closed because continuing could
adopt or modify the wrong resources.

---

## Cloud Storage

Emulated via `fake-gcs-server`.

### Python Example
```python
from google.cloud import storage

# MiniSky automatically handles the emulator host if env var is set
client = storage.Client()
bucket = client.bucket("my-bucket")
blob = bucket.blob("test.txt")
blob.upload_from_string("Hello MiniSky!")
```

---

## Pub/Sub

The Docker passthrough supports the bounded topic/subscription publish, pull,
and acknowledge workflows used by the acceptance gates. Push subscriptions and
general Pub/Sub parity are not claimed.

### Node.js Example (Publisher)
```javascript
const {PubSub} = require('@google-cloud/pubsub');
const pubsub = new PubSub();

async function publishMessage() {
  const dataBuffer = Buffer.from('Hello World');
  await pubsub.topic('my-topic').publishMessage({data: dataBuffer});
}
```

---

## Databases

### Firestore
Standard Firestore emulator integration.
```python
from google.cloud import firestore
db = firestore.Client()
doc_ref = db.collection("users").document("alace")
doc_ref.set({"name": "Alice", "active": True})
```

### Cloud SQL
MiniSky supports a bounded MySQL/PostgreSQL instance lifecycle plus persisted
instance, database, and user control-plane metadata. The guarded Terraform gate
proves create/read/no-drift/destroy for one PostgreSQL instance, database, and
user. Restored instances are `SUSPENDED`/`METADATA_ONLY` with stale endpoints
removed until explicitly reconciled; container database files are not included
in state export.
- **Access**: While the owned backend is running, use the loopback port shown in
  the **Database Topology** dashboard.

---

## Observability

### Gateway requests

Every public gateway request emits one JSON access record with a sanitized
correlation ID, normalized service and route, status, and latency. The returned
ID is in `X-MiniSky-Request-ID`. Access records do not contain request or
response bodies, authorization, cookies, arbitrary headers, or raw query
strings, and they are separate from the emulated Cloud Logging service.

Open **Gateway Requests** in the Dashboard to filter the bounded in-memory
history, or use:

```bash
minisky diagnostics requests
minisky diagnostics traces
minisky diagnostics metrics
```

The diagnostics endpoint is served only on the loopback Dashboard listener.
Its exact Prometheus-compatible metrics route is
`http://127.0.0.1:8081/api/diagnostics/metrics`. Set
`MINISKY_DIAGNOSTICS_ENDPOINT` when using a non-default Dashboard port.

OpenTelemetry export is disabled by default. Enable W3C trace propagation and
OTLP/HTTP export explicitly:

```bash
minisky start --otel --otel-endpoint http://127.0.0.1:4318
```

The equivalent variables are `MINISKY_OTEL_ENABLED=true` and
`MINISKY_OTEL_ENDPOINT`. Exporter failures are warnings and do not change
gateway responses.

Request replay is also disabled by default. Enable it with
`--request-replay` or `MINISKY_REQUEST_REPLAY_ENABLED=true`, then use the
Dashboard action or `minisky diagnostics replay REQUEST_ID`. Only eligible
same-gateway requests can be replayed. Oversized, non-JSON, or sensitive JSON
bodies are omitted and produce an explicit replay error. The body cap defaults
to 65536 bytes and can be lowered with `--request-replay-max-body` or
`MINISKY_REQUEST_REPLAY_MAX_BODY`.

See [ADR 0012](adr/0012-local-observability-and-request-replay.md) for the
redaction, cardinality, binding, and replay-safety decisions.

### Cloud Logging
The bounded Logging API persists entries, sinks, and unacknowledged sink
deliveries. Pending file/Pub/Sub deliveries are retried after restart and are
cancelled when their sink is deleted. Delivery is at-least-once across the
external-acceptance/persisted-acknowledgement window, so duplicates remain
possible.
- **Search**: Filter persisted entries by the supported project/resource/text
  fields in the dark operational Log Explorer.
- The container harvester polls logs from currently managed containers; it is
  not a guaranteed lossless live stream.

### Cloud Monitoring
The dark operational Monitoring view displays bounded managed-container
CPU/memory samples and persisted custom metric data. The supported PromQL API
is an exact instant selector, not full Cloud Monitoring or PromQL parity.

---

## BigQuery

On CGO builds, the `full` profile or `MINISKY_BQ_BACKEND=duckdb` enables local
DuckDB query execution. The default `simulation` profile keeps dataset/table
metadata behavior and simulated queries; it does not silently enable DuckDB.

```python
from google.cloud import bigquery
client = bigquery.Client()
query = "SELECT count(*) FROM `my-project.my-dataset.my-table`"
results = client.query(query)
```

---

## Lazy Loading & State

### Lazy Loading
MiniSky is "Lazy" by default. If you run a command like `gcloud pubsub topics create ...`, MiniSky will detect that Pub/Sub is not running, pull the image (if missing), start the container, and then execute your command. Docker image pulls honor request cancellation and are bounded to two minutes; timeout and registry failures are returned to the caller instead of leaving the request pending indefinitely.

### Delivery lifecycle boundaries
Cloud Tasks persists stable task IDs, attempts, schedules, and outcomes.
Nonterminal tasks are resumed once per API lifetime after restart with their
remaining retry budget. This is at-least-once delivery: a crash after target
acceptance but before the terminal result is saved can cause a duplicate.
Cloud Scheduler manual and cron deliveries run under the Scheduler API lifetime,
not the incoming request lifetime; shutdown cancels and bounded-waits for active
deliveries. Missed schedules are not replayed.

### State lifecycle
Adopted Go shims persist metadata under
`~/.minisky/state/profiles/<profile>/state.json`; other shims remain in memory.
Docker-backed services retain only data provided by their configured container,
volume, or profile runtime mount. Use `minisky state export` and
`minisky state import` for portable JSON metadata snapshots. These snapshots do
not include Docker volumes, runtime directories, DuckDB files, or object data.

---

## System Diagnostics

Use the **System Diagnostics** page to check if required tools like `docker`, `pack`, and `kind` are installed. MiniSky can automatically fix many missing dependencies for you.
