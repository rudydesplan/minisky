# State model

MiniSky has a shared, versioned, profile-scoped state store. Adoption is
incremental: shims without an adapter still hold maps in process memory, while
Docker-backed services keep whatever survives in their containers or configured
mounts.

App Engine, Artifact Registry, BigQuery, Bigtable Admin, Cloud Logging, Cloud
Monitoring, Cloud Tasks, Dataproc, IAM, Cloud DNS, Scheduler, Secret Manager,
Cloud KMS, GKE, Compute Engine, Cloud SQL, and Serverless (Cloud Functions and
Cloud Run) persist resource metadata through this store.
Docker-backed resources restore as metadata without implicitly creating missing
containers. Docker adoption and cleanup require both MiniSky ownership and
active-profile labels.

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
`PENDING` snapshot cannot be saved, before starting asynchronous work. Terminal
candidates remain hidden from pollers and observers until persistence succeeds.
If `Save` reports an error after committing, the manager publishes the
candidate only when an exact readback—including response, error, scope, and
losslessly normalized JSON metadata—matches. A pre-commit, unreadable, or
mismatched result remains unpublished and fails closed. A matching terminal
retry is idempotent; a conflicting retry is rejected without changing the
first result. Persistence degradation remains sticky for health reporting.
These guarantees apply to operation-result publication, not to exactly-once
external side effects; interrupted nonterminal work is still never replayed.

## Per-service behavior

| Service/domain | Current state | Planned adapter behavior |
| :--- | :--- | :--- |
| App Engine (`appengine.googleapis.com`) | App, service, version, and pending-deletion metadata in profile state; LROs remain in memory and deployment containers may survive | Restart normalization marks non-terminal versions stopped and does not create an app on GET. Missing workloads are not silently recreated; container/image data is outside metadata export/import. |
| Artifact Registry (`artifactregistry.googleapis.com`) | Repository metadata and compact terminal operation outcomes in profile state; LRO polling metadata uses the shared operation store; package/version results are repository-scoped views of the profile-owned Registry v2 backend | Repository metadata survives restart and metadata export/import. Registry v2 blobs and manifests remain outside metadata export/import. |
| BigQuery (`bigquery.googleapis.com`) | Dataset/table/job metadata in profile state; optional DuckDB data in `~/.minisky/data/bigquery.duckdb`; uploads in `~/.minisky/uploads` | Metadata survives restart and metadata-only export/import. DuckDB rows and uploaded files are deliberately excluded. |
| Bigtable Admin (`bigtableadmin.googleapis.com`) | Instance, table, metadata-only cluster, and compact terminal cluster-operation metadata in profile state; restored instances and clusters report `STATE_NOT_KNOWN` until emulator availability is reconciled | Cluster create/get/list/delete is a scoped metadata-only control-plane slice. Operations are already terminal, include typed request/response metadata, and can be polled only through their exact instance/cluster scope. No cluster nodes or table-data durability is implied. |
| Bigtable Data (`bigtable.googleapis.com`) | Data belongs to an unmounted emulator container | Add a profile-scoped emulator data mount and lifecycle hooks for clean export/import; share identity with the Bigtable Admin adapter. |
| Cloud Build (`cloudbuild.googleapis.com`) | Build and trigger state is held through the in-memory LRO manager; temporary Docker workspace volumes | Persist build metadata, trigger definitions, and final logs/status; treat active builds as interrupted after restart and garbage-collect orphan workspaces. |
| Cloud KMS (`cloudkms.googleapis.com`) | Key hierarchy and AES key bytes in profile state | Key material and version metadata survive restart. Snapshots contain local AES key bytes and must be handled as sensitive files. |
| Cloud SQL (`sqladmin.googleapis.com`) | Instance, database, and user metadata in profile state; LROs remain in memory; database files live only in containers | Rehydrated instances report `SUSPENDED` with `backendStatus: METADATA_ONLY` and omit stale endpoints until explicitly reconciled. Add profile-scoped named volumes for database data before claiming data durability. |
| Cloud Tasks (`cloudtasks.googleapis.com`) | Queues, stable task names, request payloads, schedule/retry timestamps, attempt counters, and terminal delivery state in profile state | Terminal tasks are never replayed. On each API lifetime, persisted `PENDING` or `RETRYING` tasks in running queues are scheduled once, preserve their stable ID and consumed retry budget, and record `RETRYING` before dispatch. This is at-least-once restart replay: a crash after the target accepts a request but before the terminal acknowledgement is saved can duplicate delivery. Exactly-once delivery and target-side deduplication are not provided. |
| Compute Engine (`compute.googleapis.com`) | Instances, networks, subnetworks, firewall rules, security policies, load-balancer metadata, the stable subnetwork counter, and shared operation polling metadata in profile state; Docker containers and bridges may survive | Covered mutations register before changing resource state and publish terminal results only after durable save or exact committed readback. Metadata rollback occurs only when the terminal candidate is not known durable; an immutable conflicting terminal result preserves the committed resource truth and fails closed. Rehydrated VMs remain `METADATA_ONLY`, clear stale host-port mappings, and are never recreated implicitly. A supported custom VPC reconciles only its exact project/profile-owned bridge identity and labels; Docker bridge state remains outside export/import. |
| Dataproc (`dataproc.googleapis.com`) | Cluster and job metadata in profile state; LROs remain in memory and owned master/worker containers may survive | Rehydration preserves exact project/region/name/UUID identity, marks non-terminal jobs `ERROR` as interrupted, and does not claim backend reconciliation. Job execution requires the persisted cluster identity and an exactly owned container; Spark and PySpark are the bounded executable job types. |
| Cloud DNS (`dns.googleapis.com`) | Zones, records, changes, and stable counters under `dns/metadata`; the UDP listener is transient process state | Persisted metadata survives restart without replaying changes. Rehydration lowercases absolute DNS names, rebuilds RRSet keys from resource fields, and clamps legacy TTLs to the UDP wire range; new out-of-range TTLs are rejected. The optional loopback resolver is rebound from `MINISKY_DNS_ADDR` at startup and closed through plugin shutdown; resolver startup failure is surfaced instead of being treated as a listening data plane. |
| Firebase Auth (`identitytoolkit.googleapis.com`) | Data belongs to an unmounted Firebase emulator container | Configure profile-scoped Firebase export/import storage and let the adapter coordinate emulator shutdown/startup rather than duplicating user state. |
| Firebase Hosting (`firebasehosting.googleapis.com`) | Files/configuration belong to an unmounted Firebase emulator container | Mount a profile-scoped hosting workspace and snapshot deploy metadata plus assets; validate that restored files match the recorded release. |
| Firebase Realtime Database (`firebaseio.com`) | Data belongs to an unmounted Firebase emulator container | Use Firebase emulator export/import in a profile directory and persist the project-to-emulator mapping. |
| GKE (`container.googleapis.com`) | Cluster metadata and backend mode in profile state; LROs in memory; optional Kind containers and kubeconfig under `/tmp` | Rehydrate metadata only. Missing Kind containers are not silently recreated; Docker reconciliation remains explicit. |
| IAM (`iam.googleapis.com`) | Service accounts, key lifecycle metadata, policies, and workload identity pool/provider metadata in profile state; static inline public JWKS is persisted, while the local token HMAC secret is a separate `0600` profile file | IAM and bounded WIF metadata survive restart and metadata-only export/import. Public JWKS may appear in snapshots and Terraform state; private WIF keys, signed subject JWTs, and local credential signing material are deliberately excluded. |
| Resource Manager (`cloudresourcemanager.googleapis.com`) | Projects plus the minimal organization/folder hierarchy in profile state | Registry metadata survives restart and export/import; `local-dev-project` is seeded when absent. |
| Cloud Logging (`logging.googleapis.com`) | Up to 5,000 entries, project-keyed sink metadata, and unacknowledged sink deliveries in profile state | On post-boot, durable pending deliveries are retried and removed only after successful delivery is persisted. Deleting a sink atomically cancels its pending deliveries, so they are not replayed after restart; loop labels prevent Pub/Sub-to-Logging recursion. A crash after an external sink accepts an entry but before acknowledgement persistence can duplicate delivery. The legacy `~/.minisky/cloud_logs.json` file is imported once and renamed with a `.migrated` suffix. File sink output lives in the profile runtime tree and is excluded from metadata export. |
| Memorystore for Redis (`redis.googleapis.com`) | Instance metadata in profile state; Redis AOF data in an owned named Docker volume | Startup reconciles only exact profile/resource ownership labels. Missing or unowned backends become `REPAIRING` with no stale endpoint. Docker volumes are excluded from metadata export. |
| Memorystore for Memcached (`memcache.googleapis.com`) | Memcached metadata is profile-persisted, including instances, admitted create/update/delete associations, and shared operation polling metadata; nodes use exact-owned Memcached containers | Cache contents are intentionally ephemeral and excluded from metadata export. Startup reconciles only exact profile/service/resource ownership labels and observable backend state, never replays create/update side effects, preserves an existing terminal result exactly, and clears an association only after authoritative reconciliation. During sticky persistence degradation, exact known operation polls remain available while unknown/cross-scope polls, resource reads, and mutations fail closed. Delete is retried only for an exact-owned backend. |
| Metadata server (`metadata.google.internal`) | Static defaults compiled into the process | Derive per-workload metadata from persisted Compute/Serverless identity; no independent snapshot should be required beyond optional local overrides. |
| Cloud Monitoring (`monitoring.googleapis.com`) | Metric descriptors and bounded time-series samples in profile state; exact-selector PromQL instant queries read persisted project-local samples after restart | Samples are local test data and are never authoritative production telemetry. PromQL queries do not add a second persistence store. |
| Pub/Sub (`pubsub.googleapis.com`) | Topics, subscriptions, and messages belong to an unmounted emulator container | Add a profile-scoped emulator data/export path if supported, persist event-bridge configuration separately, and avoid replaying already delivered observer events. |
| Cloud Scheduler (`cloudscheduler.googleapis.com`) | Job definitions and delivery metadata in profile state | Persisted jobs survive restart; missed executions are not replayed implicitly. Cron and manual deliveries run under the Scheduler API lifetime rather than the triggering HTTP request lifetime. Shutdown stops cron registration, cancels active deliveries, and waits only until the caller's deadline. |
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
5. **Import** into an empty, isolated profile, validating paths, versions, and
   every registered durable entry schema before ownership or replacement.
6. **Migrate** older schemas explicitly and fail safely on newer unknown
   versions.

Profile paths are rooted under `~/.minisky/state/profiles/<name>/` by default
(or beneath `MINISKY_STATE_DIR`); adapters must not depend on the daemon's
working directory. Resource and operation IDs must remain stable across restart
and export/import.
