# State model

MiniSky has a shared, versioned, profile-scoped state store. Adoption is
incremental: shims without an adapter still hold maps in process memory, while
Docker-backed services keep whatever survives in their containers or configured
mounts.

App Engine, Artifact Registry, BigQuery, Bigtable Admin, Cloud Build, Cloud
Logging, Cloud Monitoring, Cloud Tasks, Dataproc, IAM, Cloud DNS, Scheduler,
Secret Manager, Cloud KMS, GKE, Compute Engine, Cloud SQL, and Serverless
(Cloud Functions and Cloud Run) persist resource metadata through this store.
Docker-backed resources restore as metadata without implicitly creating missing
containers. Docker adoption and cleanup require both MiniSky ownership and
active-profile labels.

> **Sensitive data warning:** profile state files and exported snapshots can
> contain local Secret Manager payloads, Cloud KMS AES key material, Serverless
> source code, environment variables, and other sensitive values. The active
> state file is atomically written with `0600` permissions and profile
> directories use `0700`, but exported snapshots inherit the permissions and
> handling of their destination. Store, copy, and delete snapshots as secrets.

State transforms use bounded optimistic compare-and-swap. They read a document
and the sorted validation pipeline under short locks, run caller and validator
code without profile/file mutation locks, then commit only if both the document
version and hook-registry generation are unchanged. Reentrant loads are safe;
nested writes force a retry or a typed conflict instead of being overwritten.
Hook names are single-assignment process identities: every duplicate name is a
deterministic conflict, regardless of callback provenance. Production hooks
register during package initialization, which Go executes once per process;
only a successful new registration advances the pipeline generation.

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
| Cloud Build (`cloudbuild.googleapis.com`) | Build and trigger metadata in profile state; operation polling metadata in the shared durable operation store; temporary Docker workspace volumes | Terminal build and operation outcomes survive restart. Rehydration changes saved `QUEUED` or `WORKING` builds to `INTERNAL_ERROR`, persists the interrupted outcome, and never replays execution. Shared nonterminal operations likewise become stable terminal interruption errors. Workspace data remains outside metadata export/import. |
| Cloud KMS (`cloudkms.googleapis.com`) | Key hierarchy and AES key bytes in profile state | Key material and version metadata survive restart. Snapshots contain local AES key bytes and must be handled as sensitive files. |
| Cloud SQL (`sqladmin.googleapis.com`) | Instance, database, user, and non-secret runtime metadata in profile state; database files live in an exact-owned named volume; LROs remain in memory | Runtime metadata records the profile/project/instance contract, ownership fingerprint, bootstrap policy, exact configured image, and immutable image and volume identities. Rehydration clears stale endpoints and initially reports `SUSPENDED`. Exact local provenance can restore the same exact-compatible container on same-container restart or create a volume-only replacement around the same proven volume. The configured PostgreSQL recovery boundary covers `POSTGRES_16`, `POSTGRES_17`, and `POSTGRES_18`; the guarded live gate executes `POSTGRES_16`. A loopback endpoint is published only after protocol and authenticated readiness. Imported snapshots cannot supply the profile-local provenance marker, credentials, Docker volume, or database contents, which remain outside metadata export/import. Legacy metadata without complete runtime provenance fails closed as `METADATA_ONLY`; delete returns a precondition failure without touching Docker, and recreate conflicts with the retained metadata. |
| Cloud Tasks (`cloudtasks.googleapis.com`) | Queues, stable task names, request payloads, schedule/retry timestamps, attempt counters, and terminal delivery state in profile state | Terminal tasks are never replayed. On each API lifetime, persisted `PENDING` or `RETRYING` tasks in running queues are scheduled once, preserve their stable ID and consumed retry budget, and record `RETRYING` before dispatch. This is at-least-once restart replay: a crash after the target accepts a request but before the terminal acknowledgement is saved can duplicate delivery. Exactly-once delivery and target-side deduplication are not provided. |
| Compute Engine (`compute.googleapis.com`) | Instances, networks, subnetworks, firewall rules, security policies, load-balancer metadata, the stable subnetwork counter, and shared operation polling metadata in profile state; Docker containers and bridges may survive | Covered mutations register before changing resource state and publish terminal results only after durable save or exact committed readback. Metadata rollback occurs only when the terminal candidate is not known durable; an immutable conflicting terminal result preserves the committed resource truth and fails closed. Rehydrated VMs remain `METADATA_ONLY`, clear stale host-port mappings, and are never recreated implicitly. A supported custom VPC reconciles only its exact project/profile-owned bridge identity and labels; Docker bridge state remains outside export/import. |
| Dataproc (`dataproc.googleapis.com`) | Cluster, job, and bounded runtime-intent metadata in profile state; operation polling metadata in the shared durable operation store; exact-owned master/worker containers | Rehydration preserves exact project/region/name/UUID identity, cleans exact-owned interrupted backends without replay, and marks nonterminal clusters/jobs `ERROR`. Shared nonterminal operations become stable terminal interruption errors. Job execution requires persisted cluster identity and an exactly owned container; Spark and PySpark are the bounded executable job types. Container data remains outside metadata export/import. |
| Cloud DNS (`dns.googleapis.com`) | Zones, records, changes, and stable counters under `dns/metadata`; the UDP listener is transient process state | Persisted metadata survives restart without replaying changes. Rehydration lowercases absolute DNS names, rebuilds RRSet keys from resource fields, and clamps legacy TTLs to the UDP wire range; new out-of-range TTLs are rejected. The optional loopback resolver is rebound from `MINISKY_DNS_ADDR` at startup and closed through plugin shutdown; resolver startup failure is surfaced instead of being treated as a listening data plane. |
| Firebase Auth (`identitytoolkit.googleapis.com`) | Data belongs to an unmounted Firebase emulator container | Configure profile-scoped Firebase export/import storage and let the adapter coordinate emulator shutdown/startup rather than duplicating user state. |
| Firebase Hosting (`firebasehosting.googleapis.com`) | Files/configuration belong to an unmounted Firebase emulator container | Mount a profile-scoped hosting workspace and snapshot deploy metadata plus assets; validate that restored files match the recorded release. |
| Firebase Realtime Database (`firebaseio.com`) | Data belongs to an unmounted Firebase emulator container | Use Firebase emulator export/import in a profile directory and persist the project-to-emulator mapping. |
| GKE (`container.googleapis.com`) | Cluster metadata, backend mode, scoped operation outcomes, kubeconfig ownership metadata, and an ownership checksum in profile state; optional Kind containers and kubeconfig files | Terminal operations remain pollable after restart. Rehydration persists each nonterminal operation as `DONE` with an interruption error and never replays side effects. Missing or mismatched Kind backends are degraded rather than silently recreated; kubeconfig cleanup requires matching ownership metadata and checksum. Kind runtime data and kubeconfig files remain outside metadata export/import. |
| IAM (`iam.googleapis.com`) | Service accounts, key lifecycle metadata, policies, and workload identity pool/provider metadata in profile state; static inline public JWKS is persisted, while the local token HMAC secret is a separate `0600` profile file | IAM and bounded WIF metadata survive restart and metadata-only export/import. Public JWKS may appear in snapshots and Terraform state; private WIF keys, signed subject JWTs, and local credential signing material are deliberately excluded. |
| Resource Manager (`cloudresourcemanager.googleapis.com`) | Projects plus the minimal organization/folder hierarchy in profile state | Registry metadata survives restart and export/import; `local-dev-project` is seeded when absent. |
| Cloud Logging (`logging.googleapis.com`) | Up to 5,000 entries, project-keyed sink metadata, and unacknowledged sink deliveries in profile state | On post-boot, durable pending deliveries are retried and removed only after successful delivery is persisted. Deleting a sink atomically cancels its pending deliveries, so they are not replayed after restart; loop labels prevent Pub/Sub-to-Logging recursion. A crash after an external sink accepts an entry but before acknowledgement persistence can duplicate delivery. The legacy `~/.minisky/cloud_logs.json` file is imported once and renamed with a `.migrated` suffix. File sink output lives in the profile runtime tree and is excluded from metadata export. |
| Memorystore for Redis (`redis.googleapis.com`) | Private Redis backend metadata includes the exact local endpoint and Docker image/container/volume provenance needed for same-profile restart; Redis AOF data remains in an exact-owned named Docker volume | Redis durable mutations are serialized through save success or rollback, so a rejected create/delete cannot be captured by a concurrent Redis snapshot. The Redis metadata entry and the shared durable operation entry are validated as one snapshot: portable entry normalization and sorted snapshot-wide normalization complete before sorted entry and snapshot validators run; an absent sibling differs from an explicit empty operation set; references and operations must match exactly; and operation names, IDs, timestamps, progress, terminal results, verbs, and targets are canonical. Portable export strips the local host/port, image ID, container/volume identities, and generation; import preserves only semantically valid control-plane metadata and operation relationships in `REPAIRING` state and never adopts the source profile's Docker resources. A portable snapshot safely terminates a valid pending Redis operation with a deterministic interruption error before semantic validation instead of replaying it. Recognized pre-schema metadata is exactly the historical `{instance, backendId}` value shape. Migration runs as a locked transactional transform: it reloads the latest whole document, preserves the exact operation sibling committed by any operation manager, inserts an empty sibling only when absent, validates the transformed relationships, atomically replaces once, and initializes API memory only from committed readback. Hybrid legacy/current backend objects, unknown fields, malformed or ambiguous legacy data, CAS conflicts, and failed transforms leave the old snapshot wholly intact without backend calls. Same-profile current-schema startup still reconciles persisted immutable identities and the stable loopback port. AOF volume contents are not portable; volume paths and runtime credentials are also excluded. Cleanup assumes a cooperative Docker daemon: MiniSky re-inspects exact ownership and volume identity immediately before name-based deletion, but Docker provides no atomic compare-and-delete for volumes. A process or host crash after a Docker create side effect but before resolved provenance is persisted can leave exact-labelled resources. Deterministic names and labels aid explicit cleanup, but restart neither adopts nor automatically removes these pre-persistence resources; exact automatic cleanup is claimed only for completed or successfully compensated operations. |
| Memorystore for Memcached (`memcache.googleapis.com`) | Memcached metadata is profile-persisted, including instances, admitted create/update/delete associations, and shared operation polling metadata; nodes use exact-owned Memcached containers | Cache contents are intentionally ephemeral and excluded from metadata export. Startup reconciles only exact profile/service/resource ownership labels and observable backend state, never replays create/update side effects, preserves an existing terminal result exactly, and clears an association only after authoritative reconciliation. During sticky persistence degradation, exact known operation polls remain available while unknown/cross-scope polls, resource reads, and mutations fail closed. Delete is retried only for an exact-owned backend. |
| Metadata server (`metadata.google.internal`) | Static defaults compiled into the process | Derive per-workload metadata from persisted Compute/Serverless identity; no independent snapshot should be required beyond optional local overrides. |
| Cloud Monitoring (`monitoring.googleapis.com`) | Metric descriptors and bounded time-series samples in profile state; exact-selector PromQL instant queries read persisted project-local samples after restart | Samples are local test data and are never authoritative production telemetry. PromQL queries do not add a second persistence store. |
| Pub/Sub (`pubsub.googleapis.com`) | Topics, subscriptions, and queued messages last only for one official emulator session | MiniSky process crash/restart continuity is supported only while the same exact-owned Pub/Sub container remains alive. Backend/container replacement loses topics, subscriptions, and queued messages. Graceful MiniSky shutdown tears down managed Docker resources, so it is not a continuity path. Pub/Sub runtime data is outside metadata export/import; exactly-once delivery and portable data export are not claimed. |
| Cloud Scheduler (`cloudscheduler.googleapis.com`) | Job definitions and delivery metadata in profile state | Persisted jobs survive restart; missed executions are not replayed implicitly. Cron and manual deliveries run under the Scheduler API lifetime rather than the triggering HTTP request lifetime. Shutdown stops cron registration, cancels active deliveries, and waits only until the caller's deadline. |
| Secret Manager (`secretmanager.googleapis.com`) | Secret metadata, version state, and base64 payloads in profile state | Rehydrate complete local secrets. State and snapshots are sensitive even though the active file is protected by `0600` permissions. |
| Cloud Functions (`cloudfunctions.googleapis.com`) | Function/build/trigger metadata and local source in profile state; LROs in memory; built images and containers may survive | Rehydrate metadata only and never start a missing container during load. |
| Cloud Run (`run.googleapis.com`) | Service/template/traffic metadata in profile state; LROs in memory; image containers may survive | Rehydrate metadata only and keep container reconciliation explicit. |
| Cloud Storage (`storage.googleapis.com`) | Buckets and objects use a profile-scoped runtime bind mount for `fake-gcs-server` | Runtime data survives exact-owned Storage emulator-container replacement and is isolated by profile. Storage runtime data is outside metadata export/import; portable data export, IAM, HA, security, and full GCP parity are not claimed. |
| Vertex AI (`aiplatform.googleapis.com`) | Non-secret mock/Ollama `generateContent` provider settings in profile state; API keys in process memory only; deterministic endpoint predictions are computed statelessly and are not persisted | External model data and prediction responses remain outside snapshots. Ollama endpoints are restricted to loopback HTTP origins. |
| Firestore (`firestore.googleapis.com`) | Emulator data bind-mounted from `<state-root>/profiles/<profile>/runtime/firestore` | Runtime data is profile-scoped and excluded from metadata export/import. Listener and security-rule fidelity is not claimed. |
| Datastore (`datastore.googleapis.com`) | Emulator data bind-mounted from `<state-root>/profiles/<profile>/runtime/datastore` | Runtime data is profile-scoped and independent of the daemon working directory. It is not included in metadata export/import. |
| Spanner (`spanner.googleapis.com`) | Data belongs to the official in-memory emulator container | The REST/admin and gRPC ports are loopback-published. Emulator data does not survive container deletion and is excluded from export/import. |

Storage and Pub/Sub runtime data remain outside metadata export/import. Their
shared bounded gate does not turn emulator runtime data into portable profile
metadata.

Cloud SQL named-volume deletion and the Storage/Pub/Sub gate's Docker cleanup
evidence assume cooperative, exclusive use of MiniSky's managed resource names
on the Docker daemon. Docker's volume-delete API accepts a mutable name and has
no conditional immutable-identity parameter. MiniSky revalidates exact labels
and its derived immutable identity immediately before deletion and fails closed
on absence, mismatch, or inspection error, but a foreign replacement in the
final inspect-to-delete interval cannot be made atomic. This limitation applies
to cleanup evidence; Storage bucket/object durability still comes from its
profile-scoped bind mount, not a named volume. These checks are collision and
ownership guards, not a security boundary against a hostile or concurrently
mutating daemon.

The guarded `make test-cloudsql-restart-integration` target and
`cloudsql-restart-integration` critical-integration job cover creation,
same-container restart, SQL row recovery through volume-only replacement,
portable-snapshot exclusions, legacy fail-closed behavior, readiness, and exact
cleanup. The stable uncommitted snapshot passed this gate and its bounded
Terraform apply/no-drift/destroy lifecycle at source SHA-256
`328b4cb13c6ca1705ca51d0e3fb543a830cd6a4af2be8aa8ef3ebda456873a25`
and diff SHA-256
`25318c4dffcf6f04931fe84d1b7cb27218cc0c3a4f8cb63e46f8ff1f90469033`.
Local status remains `local-passed-uncommitted`. PR #23's exact-head
[critical run 30431422780](https://github.com/rudydesplan/minisky/actions/runs/30431422780)
and
[Cloud SQL job](https://github.com/rudydesplan/minisky/actions/runs/30431422780/job/90509291797)
are `ci-passed` on commit
`794b68439c59bfa0dd35b37962049a1a3e510ea1`; this immutable CI provenance is
separate from the stable local fingerprints.

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
