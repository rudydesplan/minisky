# State model

MiniSky has a shared, versioned, profile-scoped state store. Adoption is
incremental: shims without an adapter still hold maps in process memory, while
Docker-backed services keep whatever survives in their containers or configured
mounts.

BigQuery, IAM, Cloud DNS, Scheduler, Secret Manager, Cloud KMS, GKE, Compute
Engine, Cloud SQL, and Serverless (Cloud Functions and Cloud Run) persist
resource metadata through this store. Docker-backed resources restore as
metadata without implicitly creating missing containers. Docker adoption and
cleanup require both MiniSky ownership and active-profile labels.

> **Sensitive data warning:** profile state files and exported snapshots can
> contain local Secret Manager payloads, Cloud KMS AES key material, Serverless
> source code, environment variables, and other sensitive values. The active
> state file is atomically written with `0600` permissions and profile
> directories use `0700`, but exported snapshots inherit the permissions and
> handling of their destination. Store, copy, and delete snapshots as secrets.

## Current storage classes

- **Memory**: lost whenever the MiniSky process exits.
- **File**: written below `~/.minisky` and available after restart.
- **Container**: data may remain while the same Docker container exists, but
  `minisky uninstall` removes managed containers and data.
- **Bind mount**: data is stored at a host path configured for the emulator.
- **External**: state belongs to another local or remote process.

The shared LRO manager persists polling metadata. On restart, any saved
`PENDING` or `RUNNING` operation becomes a stable terminal error stating that
the operation was interrupted; MiniSky never replays its side effects.
Durability-dependent handlers reject a new operation when its initial
`PENDING` snapshot cannot be saved, before starting asynchronous work. If a
terminal save fails after work has run, the live manager logs and exposes a
persistence-degraded error and polling reports a terminal in-process error.
The exact restart limitation is that only the last successful snapshot is
available: restart polling therefore reports the operation as interrupted and
cannot determine whether the already-executed side effect succeeded. MiniSky
does not replay that side effect.

## Per-service behavior

| Service/domain | Current state | Planned adapter behavior |
| :--- | :--- | :--- |
| App Engine (`appengine.googleapis.com`) | Apps, services, versions, and LROs in memory; deployment containers may survive | Persist the resource graph and deployment identity; reconcile each version with its container/image and mark missing workloads failed rather than silently recreating them. |
| Artifact Registry (`artifactregistry.googleapis.com`) | Repository map in memory; LRO polling metadata uses the shared operation store; package/version results are repository-scoped views of the profile-owned Registry v2 backend | Persist repository metadata before including it in snapshots; Registry v2 blobs and manifests remain outside metadata export/import. |
| BigQuery (`bigquery.googleapis.com`) | Dataset/table/job metadata in profile state; optional DuckDB data in `~/.minisky/data/bigquery.duckdb`; uploads in `~/.minisky/uploads` | Metadata survives restart and metadata-only export/import. DuckDB rows and uploaded files are deliberately excluded. |
| Bigtable Admin (`bigtableadmin.googleapis.com`) | Instance, cluster, and table metadata in memory | Persist Admin resources and reconcile them with the emulator; do not claim table data durability unless the emulator data directory is also mounted and exported. |
| Bigtable Data (`bigtable.googleapis.com`) | Data belongs to an unmounted emulator container | Add a profile-scoped emulator data mount and lifecycle hooks for clean export/import; share identity with the Bigtable Admin adapter. |
| Cloud Build (`cloudbuild.googleapis.com`) | Build and trigger state is held through the in-memory LRO manager; temporary Docker workspace volumes | Persist build metadata, trigger definitions, and final logs/status; treat active builds as interrupted after restart and garbage-collect orphan workspaces. |
| Cloud KMS (`cloudkms.googleapis.com`) | Key hierarchy and AES key bytes in profile state | Key material and version metadata survive restart. Snapshots contain local AES key bytes and must be handled as sensitive files. |
| Cloud SQL (`sqladmin.googleapis.com`) | Instance, database, and user metadata in profile state; LROs remain in memory; database files live only in containers | Rehydrated instances report `SUSPENDED` with `backendStatus: METADATA_ONLY` and omit stale endpoints until explicitly reconciled. Add profile-scoped named volumes for database data before claiming data durability. |
| Cloud Tasks (`cloudtasks.googleapis.com`) | Queues, tasks, retry attempts, and terminal delivery state in profile state | Terminal tasks are never replayed. Tasks persisted as `PENDING` or `RETRYING` are marked `FAILED` with an interruption diagnostic after restart; automatic at-least-once resumption is not implemented. |
| Compute Engine (`compute.googleapis.com`) | Instances, networks, firewall rules, and load-balancer metadata in profile state; security policies and LROs remain in memory; Docker containers/networks may survive | Rehydrated VMs report `METADATA_ONLY`, clear stale host-port mappings, and are never recreated implicitly. Explicit-URL load-balancer backends continue to proxy after restart; Docker-backed instance endpoints require reconciliation. |
| Dataproc (`dataproc.googleapis.com`) | Cluster/job metadata and LROs in memory; master/worker containers may survive | Persist clusters and terminal jobs, reconcile expected node containers, and mark in-flight jobs interrupted unless backend evidence proves completion. |
| Cloud DNS (`dns.googleapis.com`) | Zones, records, changes, and stable counters under `dns/metadata`; the UDP listener is transient process state | Persisted metadata survives restart without replaying changes. Rehydration lowercases absolute DNS names, rebuilds RRSet keys from resource fields, and clamps legacy TTLs to the UDP wire range; new out-of-range TTLs are rejected. The optional loopback resolver is rebound from `MINISKY_DNS_ADDR` at startup and closed through plugin shutdown; resolver startup failure is surfaced instead of being treated as a listening data plane. |
| Firebase Auth (`identitytoolkit.googleapis.com`) | Data belongs to an unmounted Firebase emulator container | Configure profile-scoped Firebase export/import storage and let the adapter coordinate emulator shutdown/startup rather than duplicating user state. |
| Firebase Hosting (`firebasehosting.googleapis.com`) | Files/configuration belong to an unmounted Firebase emulator container | Mount a profile-scoped hosting workspace and snapshot deploy metadata plus assets; validate that restored files match the recorded release. |
| Firebase Realtime Database (`firebaseio.com`) | Data belongs to an unmounted Firebase emulator container | Use Firebase emulator export/import in a profile directory and persist the project-to-emulator mapping. |
| GKE (`container.googleapis.com`) | Cluster metadata and backend mode in profile state; LROs in memory; optional Kind containers and kubeconfig under `/tmp` | Rehydrate metadata only. Missing Kind containers are not silently recreated; Docker reconciliation remains explicit. |
| IAM (`iam.googleapis.com`) | Service accounts, key lifecycle metadata, policies, and workload identity pool/provider metadata in profile state; static inline public JWKS is persisted, while the local token HMAC secret is a separate `0600` profile file | IAM and bounded WIF metadata survive restart and metadata-only export/import. Public JWKS may appear in snapshots and Terraform state; private WIF keys, signed subject JWTs, and local credential signing material are deliberately excluded. |
| Resource Manager (`cloudresourcemanager.googleapis.com`) | Projects plus the minimal organization/folder hierarchy in profile state | Registry metadata survives restart and export/import; `local-dev-project` is seeded when absent. |
| Cloud Logging (`logging.googleapis.com`) | Up to 5,000 entries and project-keyed sink metadata in profile state; rehydration does not replay sink delivery | The legacy `~/.minisky/cloud_logs.json` file is imported once and renamed with a `.migrated` suffix. File sink output lives in the profile runtime tree and is excluded from metadata export. Pagination and sink-delivery checkpoints are not persisted. |
| Memorystore for Redis (`redis.googleapis.com`) | Instance metadata in profile state; Redis AOF data in an owned named Docker volume | Startup reconciles only exact profile/resource ownership labels. Missing or unowned backends become `REPAIRING` with no stale endpoint. Docker volumes are excluded from metadata export. |
| Memorystore for Memcached (`memcache.googleapis.com`) | Not implemented; requests return `501 UNIMPLEMENTED` | Cache persistence and control-plane behavior remain deferred. |
| Metadata server (`metadata.google.internal`) | Static defaults compiled into the process | Derive per-workload metadata from persisted Compute/Serverless identity; no independent snapshot should be required beyond optional local overrides. |
| Cloud Monitoring (`monitoring.googleapis.com`) | Metric descriptors and bounded time-series samples in profile state; exact-selector PromQL instant queries read persisted project-local samples after restart | Samples are local test data and are never authoritative production telemetry. PromQL queries do not add a second persistence store. |
| Pub/Sub (`pubsub.googleapis.com`) | Topics, subscriptions, and messages belong to an unmounted emulator container | Add a profile-scoped emulator data/export path if supported, persist event-bridge configuration separately, and avoid replaying already delivered observer events. |
| Cloud Scheduler (`cloudscheduler.googleapis.com`) | Job definitions and delivery metadata in profile state | Persisted jobs survive restart; missed executions are not replayed implicitly. |
| Secret Manager (`secretmanager.googleapis.com`) | Secret metadata, version state, and base64 payloads in profile state | Rehydrate complete local secrets. State and snapshots are sensitive even though the active file is protected by `0600` permissions. |
| Cloud Functions (`cloudfunctions.googleapis.com`) | Function/build/trigger metadata and local source in profile state; LROs in memory; built images and containers may survive | Rehydrate metadata only and never start a missing container during load. |
| Cloud Run (`run.googleapis.com`) | Service/template/traffic metadata in profile state; LROs in memory; image containers may survive | Rehydrate metadata only and keep container reconciliation explicit. |
| Cloud Storage (`storage.googleapis.com`) | Buckets/objects live in an unmounted `fake-gcs-server` container | Add a profile-scoped data mount, snapshot emulator data with object metadata, and persist event-delivery checkpoints separately. |
| Vertex AI (`aiplatform.googleapis.com`) | Non-secret mock/Ollama `generateContent` provider settings in profile state; API keys in process memory only; deterministic endpoint predictions are computed statelessly and are not persisted | External model data and prediction responses remain outside snapshots. Ollama endpoints are restricted to loopback HTTP origins. |
| Firestore (`firestore.googleapis.com`) | Emulator data bind-mounted from `<state-root>/profiles/<profile>/runtime/firestore` | Runtime data is profile-scoped and excluded from metadata export/import. Listener and security-rule fidelity is not claimed. |
| Datastore (`datastore.googleapis.com`) | Emulator data bind-mounted from `<state-root>/profiles/<profile>/runtime/datastore` | Runtime data is profile-scoped and independent of the daemon working directory. It is not included in metadata export/import. |
| Spanner (`spanner.googleapis.com`) | Data belongs to the official in-memory emulator container | The REST/admin and gRPC ports are loopback-published. Emulator data does not survive container deletion and is excluded from export/import. |

## Adapter lifecycle contract

A future adapter should expose these behaviors consistently:

1. **Load** a recognized schema version before the public gateway accepts
   requests.
2. **Save atomically** after a successful mutation; partially written state
   must never replace the last valid snapshot.
3. **Reconcile** saved control-plane records only with Docker resources whose
   ownership and profile labels are proven; otherwise remain metadata-only.
4. **Export** a portable JSON metadata snapshot. Runtime directories, Docker
   volumes, containers, DuckDB files, and uploaded objects are excluded.
5. **Import** into an empty, isolated profile, validating paths and versions
   before any side effect.
6. **Migrate** older schemas explicitly and fail safely on newer unknown
   versions.

Profile paths are rooted under `~/.minisky/state/profiles/<name>/` by default
(or beneath `MINISKY_STATE_DIR`); adapters must not depend on the daemon's
working directory. Resource and operation IDs must remain stable across restart
and export/import.
