# MiniSky: The Complete User Guide

**Website:** [minisky.bmics.com.ng](https://minisky.bmics.com.ng)

MiniSky is a high-fidelity GCP emulator designed for local development, testing, and CI/CD. It allows you to run a full GCP cloud environment locally using Docker and a custom API Gateway.

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
Docker resources.

Use `make dev`, `make test`, `make test-phase17`, `make benchmark`, or the explicitly guarded
`MINISKY_INTEGRATION=1 make test-integration`.

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

Full support for Topics, Subscriptions (Push & Pull).

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
MiniSky spins up high-performance Docker containers for MySQL/PostgreSQL.
- **Access**: Via the local port shown in the **Database Topology** dashboard.

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
All container logs (Serverless, Compute, etc.) are automatically harvested into the **Cloud Logging** dashboard.
- **Search**: Filter by resource name or text content.
- **Live Stream**: Real-time log tailing.

### Cloud Monitoring
Real-time CPU and Memory metrics are collected from all managed containers.
- View charts in the **Cloud Monitoring** tab.

---

## BigQuery

Powered by **DuckDB** for lightning-fast local analytical queries without the heavy overhead of the official BQ emulator.

```python
from google.cloud import bigquery
client = bigquery.Client()
query = "SELECT count(*) FROM `my-project.my-dataset.my-table`"
results = client.query(query)
```

---

## Lazy Loading & State

### Lazy Loading
MiniSky is "Lazy" by default. If you run a command like `gcloud pubsub topics create ...`, MiniSky will detect that Pub/Sub is not running, pull the image (if missing), start the container, and then execute your command.

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
