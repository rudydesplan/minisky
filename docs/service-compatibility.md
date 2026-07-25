# Service compatibility

This matrix is the human-readable view of the executable manifest in
`pkg/registry`. The contract gate derives its service list from runtime
registrations, so adding or removing a registered domain requires updating the
manifest and this document.

Fidelity tiers:

- **high**: broad protocol and behavior compatibility for the documented API.
- **standard**: selected resource operations with GCP-shaped responses.
- **passthrough**: requests are delegated to a Docker-backed emulator.

Persistence categories describe where service state lives: **memory**, **file**,
**Docker**, **hybrid** (control-plane metadata plus a data-plane backend), or
**static**.

| Domain | Fidelity | Persistence |
| --- | --- | --- |
| `aiplatform.googleapis.com` | standard | memory |
| `appengine.googleapis.com` | standard | hybrid |
| `artifactregistry.googleapis.com` | standard | memory |
| `bigquery.googleapis.com` | standard | file |
| `bigtable.googleapis.com` | standard | hybrid |
| `bigtableadmin.googleapis.com` | standard | hybrid |
| `cloudbuild.googleapis.com` | standard | hybrid |
| `cloudfunctions.googleapis.com` | standard | hybrid |
| `cloudkms.googleapis.com` | standard | file |
| `cloudscheduler.googleapis.com` | standard | file |
| `cloudtasks.googleapis.com` | standard | memory |
| `compute.googleapis.com` | standard | hybrid |
| `container.googleapis.com` | standard | file |
| `dataproc.googleapis.com` | standard | hybrid |
| `datastore.googleapis.com` | passthrough | Docker |
| `dns.googleapis.com` | standard | file |
| `firebasehosting.googleapis.com` | passthrough | Docker |
| `firebaseio.com` | passthrough | Docker |
| `firestore.googleapis.com` | passthrough | Docker |
| `iam.googleapis.com` | standard | file |
| `identitytoolkit.googleapis.com` | passthrough | Docker |
| `logging.googleapis.com` | standard | file |
| `memcache.googleapis.com` | standard | hybrid |
| `metadata.google.internal` | high | static |
| `monitoring.googleapis.com` | standard | memory |
| `pubsub.googleapis.com` | passthrough | Docker |
| `redis.googleapis.com` | standard | hybrid |
| `run.googleapis.com` | standard | hybrid |
| `secretmanager.googleapis.com` | standard | file |
| `spanner.googleapis.com` | passthrough | Docker |
| `sqladmin.googleapis.com` | standard | hybrid |
| `storage.googleapis.com` | passthrough | Docker |

The unsupported-route contract uses
`/__minisky_contract__/unsupported`. Probe-safe registered handlers must return
HTTP 501 with a GCP JSON error envelope and `UNIMPLEMENTED` status. Services
whose request path necessarily starts a lazy Docker backend are skipped through
explicit manifest metadata; they remain covered by registration and
documentation checks.
# Service compatibility

This document describes the services that are registered by the current
MiniSky binary. It is an implementation inventory, not a claim that every API
method exposed by Google Cloud is supported.

## Fidelity tiers

- **Executable**: MiniSky runs a local workload or performs the core operation,
  although the surrounding control plane can still be incomplete.
- **Emulator-backed**: requests are proxied to a purpose-built third-party or
  Google emulator.
- **Metadata-only**: resource metadata is tracked, but there is no equivalent
  service workload.
- **Experimental**: a partial, opt-in, simulated, or known-incomplete path that
  should not be treated as a compatibility guarantee.

`Host route` below means that the request must retain the registered domain in
the HTTP `Host` header. The localhost gateway has explicit path routing only
for Storage, BigQuery, Pub/Sub, Compute, and the shared Serverless handler.
`No smoke CI` means there is no Google SDK smoke suite in the current CI
workflow. Dashboard status means the service appears in `/api/services`; it
does not imply complete resource management.

## Registered domains

| Registered API domain | Implementation | Tier | Current persistence | Terraform | SDK | Dashboard | Evidence-based caveat |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `appengine.googleapis.com` | Go control-plane shim plus Docker deployment | Experimental | Metadata in memory; deployed containers may outlive the process | Host route; unverified | Host override; no smoke CI | Listed | The v1 API is explicitly described as a mock; deployment primarily uses a MiniSky-specific `/deploy` extension. |
| `artifactregistry.googleapis.com` | Go metadata shim | Experimental | Repositories in memory | Host route; unverified | Host override; no smoke CI | Listed | Create/list repository metadata exists, but package and version lists are hard-coded and no registry backend is connected. |
| `bigquery.googleapis.com` | Go control plane with optional embedded DuckDB | Executable | Dataset/table/job metadata in memory; DuckDB file at `~/.minisky/data/bigquery.duckdb` when enabled | `/bigquery/` path route; unverified | Endpoint override; no smoke CI | Listed | SQL execution requires a CGO build and `MINISKY_BQ_BACKEND=duckdb`; otherwise jobs use simulated execution. |
| `bigtableadmin.googleapis.com` | Go Admin shim sharing the Bigtable emulator bridge | Emulator-backed | Instance/cluster/table metadata in memory; emulator container state is not volume-backed | Host route; unverified | Host override; no smoke CI | Combined Bigtable entry | Admin CRUD is implemented in Go; creating metadata only cold-starts the emulator and does not prove complete Admin API parity. |
| `bigtable.googleapis.com` | Go REST bridge to the Google Bigtable emulator | Emulator-backed | Emulator container filesystem only; no configured volume | Host route; unverified | Emulator endpoint/host override; no smoke CI | Listed | Only the implemented data bridge is covered; the same singleton also serves Admin traffic. |
| `cloudbuild.googleapis.com` | Go control plane executing build steps in Docker | Executable | Builds/LROs/triggers in memory; temporary Docker workspace volumes | Host route; unverified | Host override; no smoke CI | Listed | Steps run in containers, but timing and trigger behavior are simulated and build history disappears on restart. |
| `cloudkms.googleapis.com` | Go shim using local AES-256-GCM | Executable | Key rings, keys, versions, and key material in memory | Host route; unverified | Host override; no smoke CI | Listed | Encryption is real local cryptography, but keys vanish on restart and CRC32C verification is not implemented. |
| `sqladmin.googleapis.com` | Go control plane plus PostgreSQL/MySQL containers | Executable | Control-plane metadata in memory; database container filesystem has no configured durable volume | Host route; unverified | Host override; no smoke CI | Listed | Instance/database/user CRUD and LROs are partial; restart rehydration and durable database volumes are absent. |
| `cloudtasks.googleapis.com` | Go queue/task shim with background HTTP delivery | Experimental | Queues and tasks in memory | Host route; unverified | Host override; no smoke CI | Listed | HTTP tasks execute after a fixed delay; retry, backoff, attempts, and terminal delivery state are not modeled. |
| `compute.googleapis.com` | Go control plane plus Docker VM/network operations | Executable | Resource metadata and LROs in memory; Docker containers/networks may survive process restart | `/compute/` path route; unverified | Endpoint override; no smoke CI | Listed | VM lifecycle is executable, but load-balancer routes are stateless accepted-LRO stubs and metadata is not rehydrated. |
| `dataproc.googleapis.com` | Go control plane plus Spark-labeled Docker containers | Experimental | Cluster/job metadata and LROs in memory; containers may survive process restart | Host route; unverified | Host override; no smoke CI | Listed | Supported Spark/PySpark jobs call `spark-submit`; unsupported job types complete as timed no-ops. |
| `dns.googleapis.com` | Go metadata shim | Metadata-only | Zones, record sets, and changes in memory | Host route; unverified | Host override; no smoke CI | Listed | CRUD and change records are modeled, but MiniSky does not provide an authoritative DNS server. |
| `identitytoolkit.googleapis.com` | Reverse proxy to Firebase Auth emulator | Emulator-backed | Emulator container filesystem only; no configured export/import volume | Host route; unverified | Firebase emulator host/endpoint; no smoke CI | Listed | Auth is started as a dedicated Firebase process without configured persistence. |
| `firebasehosting.googleapis.com` | Reverse proxy to Firebase Hosting emulator | Emulator-backed | Emulator container filesystem only; no configured host volume | Host route; unverified | Firebase emulator host/endpoint; no smoke CI | Listed | The configured command starts Hosting only; deployment/source lifecycle is delegated entirely to that emulator. |
| `firebaseio.com` | Reverse proxy to Firebase Realtime Database emulator | Emulator-backed | Emulator container filesystem only; no configured export/import volume | Not a Google provider resource path | Firebase emulator host/URL; no smoke CI | Listed | Project subdomains are flattened, but no durable import/export directory is mounted. |
| `container.googleapis.com` | Go GKE shim with optional Kind backend | Experimental | Cluster metadata/LROs in memory; Kind containers and temporary kubeconfig files when enabled | Host route; unverified | Host override; no smoke CI | Listed | Kind requires `MINISKY_GKE_BACKEND=kind` and the `kind` CLI; otherwise provisioning is simulated. |
| `iam.googleapis.com` | Go metadata/policy shim | Metadata-only | Service accounts, fake keys, and policies in memory | Host route; unverified | Host override; no smoke CI | Listed | Generated private keys are non-functional and stored policies are not enforced as authorization decisions. |
| `logging.googleapis.com` | Go log ingestion/list shim | Metadata-only | Bounded JSON file at `~/.minisky/cloud_logs.json` | Host route; unverified | Host override; no smoke CI | Not listed as a service | Supports entry write/list plus internal harvesting, not the complete Logging API. |
| `redis.googleapis.com` | Shared Go Memorystore control plane plus Redis/Valkey container | Executable | Instance metadata/LROs in memory; container filesystem has no configured volume | Host route; unverified | Host override; no smoke CI | Combined Memorystore entry | The data service runs, but restart reconciliation and full Memorystore control-plane semantics are absent. |
| `memcache.googleapis.com` | Shared Go Memorystore control plane plus Memcached container | Executable | Instance metadata/LROs in memory; Memcached data is ephemeral by design | Host route; unverified | Host override; no smoke CI | Combined Memorystore entry | Only the common instance lifecycle subset is modeled. |
| `metadata.google.internal` | Static Go metadata server | Experimental | Compiled defaults in memory; no per-VM persisted state | Not applicable | Metadata clients can use a host override; no smoke CI | Not listed | Tokens and identity values are fake; the identity token is not cryptographically valid. |
| `monitoring.googleapis.com` | Go collector over Docker stats | Experimental | Last 60 CPU/memory points per resource in memory | Host route; unverified | Host override; no smoke CI | Not listed as a service | `/timeSeries` returns the internal stats map rather than full Cloud Monitoring request/response semantics. |
| `pubsub.googleapis.com` | Reverse proxy to Google Pub/Sub emulator with event interception | Emulator-backed | Emulator container filesystem only; no configured volume | Root/path route; unverified | Emulator endpoint; no smoke CI | Listed | Publish events are forwarded to Serverless observers before proxy completion; durable emulator state is not configured. |
| `cloudscheduler.googleapis.com` | Go cron scheduler with HTTP/Pub/Sub/App Engine delivery | Executable | Jobs and cron registrations in memory | Host route; unverified | Host override; no smoke CI | Listed | Jobs execute locally, but restart loses schedules and Pub/Sub/App Engine delivery uses fixed gateway assumptions. |
| `secretmanager.googleapis.com` | Go secret/version shim | Metadata-only | Secret metadata and plaintext payloads in memory | Host route; unverified | Host override; no smoke CI | Listed | Version access works, but there is no encryption-at-rest, IAM enforcement, replication, or restart persistence. |
| `cloudfunctions.googleapis.com` | Shared Go Serverless control plane plus Docker; optional Buildpacks | Experimental | Function metadata/LROs in memory; built images/containers may survive | `/v2/` and location-path route; unverified | Endpoint override; no smoke CI | Combined Serverless entry | Without `MINISKY_SERVERLESS_BACKEND=buildpacks`, source deployments can fall back to a Cloud SDK image rather than execute user code. |
| `run.googleapis.com` | Shared Go Serverless control plane plus Docker image execution | Experimental | Service metadata/LROs in memory; images/containers may survive | Shared `/v2/` handler; unverified | Endpoint override; no smoke CI | Combined Serverless entry | Cloud Run and Functions share one handler; routing is path-based and only a subset of service/revision behavior exists. |
| `storage.googleapis.com` | Reverse proxy to `fake-gcs-server` with event interception | Emulator-backed | Emulator container filesystem only; no configured host volume | `/storage/` and `/upload/storage/` routes; one example, not CI-tested | Endpoint override; no smoke CI | Listed | Bucket/object behavior comes from `fake-gcs-server`; restart durability is not guaranteed by MiniSky configuration. |
| `aiplatform.googleapis.com` | Vertex request translator to Ollama or an OpenAI-compatible endpoint | Experimental | Provider/model configuration in memory; model files belong to the external backend | Host route; unverified | Host override; no smoke CI | Listed | Ollama `generateContent` is implemented; the OpenAI proxy returns `501`, and streaming is handled as a non-streaming request. |
| `firestore.googleapis.com` | Lazy direct proxy to Google Firestore emulator | Emulator-backed | Emulator container filesystem only; no configured volume | Host route only; unverified | Emulator endpoint; no smoke CI | Listed | There is no MiniSky Firestore shim or path mapping; the raw emulator API defines compatibility. |
| `datastore.googleapis.com` | Lazy direct proxy to Google Datastore emulator | Emulator-backed | Bind mount `./data/datastore:/data` | Host route only; unverified | Emulator endpoint; no smoke CI | Listed | This is the only configured emulator bind mount; the relative host path depends on the daemon working directory. |
| `spanner.googleapis.com` | Lazy direct proxy to Cloud Spanner emulator | Emulator-backed | Emulator container filesystem only; no configured volume | Host route only; unverified | Emulator endpoint; no smoke CI | Listed | No REST adaptation or persistence layer is added by MiniSky. |

## What is actually verified

The repository has focused BigQuery DuckDB conformance tests (query execution,
nested schema mapping, streaming inserts, CSV loads, persistence, and no-CGO
behavior). General Go tests, race tests, UI lint/build, and release builds run
in CI. There are currently no per-domain create/get/delete contract tests, no
Google SDK smoke suites, and no Terraform apply/plan/destroy CI job. Treat all
other compatibility statements above as implementation evidence, not an
acceptance-test guarantee.
