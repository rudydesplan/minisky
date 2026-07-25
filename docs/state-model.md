# State model

MiniSky has a shared, versioned, profile-scoped state store. Adoption is
incremental: shims without an adapter still hold maps in process memory, the
operation manager remains in memory, and Docker-backed services keep whatever
survives in their containers or configured mounts.

Secret Manager, Cloud KMS, GKE, Compute Engine, Cloud SQL, and Serverless
(Cloud Functions and Cloud Run) now persist resource metadata through this
store. Docker-backed Compute, Cloud SQL, GKE, and Serverless resources restore
as metadata without implicitly creating missing containers; reconciliation
remains an explicit lifecycle action.

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

The shared LRO manager is memory-only for every service that uses it. A durable
adapter must save enough operation identity and terminal state for clients to
resume polling after a restart; it must not replay completed side effects.

## Per-service behavior

| Service/domain | Current state | Planned adapter behavior |
| :--- | :--- | :--- |
| App Engine (`appengine.googleapis.com`) | Apps, services, versions, and LROs in memory; deployment containers may survive | Persist the resource graph and deployment identity; reconcile each version with its container/image and mark missing workloads failed rather than silently recreating them. |
| Artifact Registry (`artifactregistry.googleapis.com`) | Repository map and LROs in memory; package/version responses are static | Persist repositories first; replace static package data with a backend-derived index before including packages and versions in snapshots. |
| BigQuery (`bigquery.googleapis.com`) | Dataset/table/job metadata in memory; optional DuckDB data in `~/.minisky/data/bigquery.duckdb`; uploads in `~/.minisky/uploads` | Persist metadata and stable job results beside the DuckDB file, validate schema/version on load, and include both metadata and database data in export/import. |
| Bigtable Admin (`bigtableadmin.googleapis.com`) | Instance, cluster, and table metadata in memory | Persist Admin resources and reconcile them with the emulator; do not claim table data durability unless the emulator data directory is also mounted and exported. |
| Bigtable Data (`bigtable.googleapis.com`) | Data belongs to an unmounted emulator container | Add a profile-scoped emulator data mount and lifecycle hooks for clean export/import; share identity with the Bigtable Admin adapter. |
| Cloud Build (`cloudbuild.googleapis.com`) | Build and trigger state is held through the in-memory LRO manager; temporary Docker workspace volumes | Persist build metadata, trigger definitions, and final logs/status; treat active builds as interrupted after restart and garbage-collect orphan workspaces. |
| Cloud KMS (`cloudkms.googleapis.com`) | Key hierarchy and AES key bytes in profile state | Key material and version metadata survive restart. Snapshots contain local AES key bytes and must be handled as sensitive files. |
| Cloud SQL (`sqladmin.googleapis.com`) | Instance, database, and user metadata in profile state; LROs remain in memory; database files live only in containers | Rehydrated instances report `SUSPENDED` with `backendStatus: METADATA_ONLY` and omit stale endpoints until explicitly reconciled. Add profile-scoped named volumes for database data before claiming data durability. |
| Cloud Tasks (`cloudtasks.googleapis.com`) | Queues, tasks, and delivery state in memory | Persist queues and tasks with schedule/attempt timestamps; on rehydrate, resume only eligible unfinished tasks with idempotent attempt accounting. |
| Compute Engine (`compute.googleapis.com`) | Instances, networks, firewall rules, and load-balancer metadata in profile state; security policies and LROs remain in memory; Docker containers/networks may survive | Rehydrated VMs report `METADATA_ONLY`, clear stale host-port mappings, and are never recreated implicitly. Explicit-URL load-balancer backends continue to proxy after restart; Docker-backed instance endpoints require reconciliation. |
| Dataproc (`dataproc.googleapis.com`) | Cluster/job metadata and LROs in memory; master/worker containers may survive | Persist clusters and terminal jobs, reconcile expected node containers, and mark in-flight jobs interrupted unless backend evidence proves completion. |
| Cloud DNS (`dns.googleapis.com`) | Zones, records, and changes in memory | Persist zones and record sets atomically; reconstruct derived SOA/NS records and retain completed change IDs without replaying changes. |
| Firebase Auth (`identitytoolkit.googleapis.com`) | Data belongs to an unmounted Firebase emulator container | Configure profile-scoped Firebase export/import storage and let the adapter coordinate emulator shutdown/startup rather than duplicating user state. |
| Firebase Hosting (`firebasehosting.googleapis.com`) | Files/configuration belong to an unmounted Firebase emulator container | Mount a profile-scoped hosting workspace and snapshot deploy metadata plus assets; validate that restored files match the recorded release. |
| Firebase Realtime Database (`firebaseio.com`) | Data belongs to an unmounted Firebase emulator container | Use Firebase emulator export/import in a profile directory and persist the project-to-emulator mapping. |
| GKE (`container.googleapis.com`) | Cluster metadata and backend mode in profile state; LROs in memory; optional Kind containers and kubeconfig under `/tmp` | Rehydrate metadata only. Missing Kind containers are not silently recreated; Docker reconciliation remains explicit. |
| IAM (`iam.googleapis.com`) | Service accounts, fake keys, and policies in memory | Persist accounts and policies; regenerate or securely wrap local key material, and version policy representation for future enforcement semantics. |
| Cloud Logging (`logging.googleapis.com`) | Up to 5,000 entries in `~/.minisky/cloud_logs.json` | Move the existing file behind a versioned adapter with atomic writes, profile isolation, validation, and bounded export behavior. |
| Memorystore for Redis (`redis.googleapis.com`) | Instance metadata/LROs in memory; Redis/Valkey data in an unmounted container | Persist instance metadata and use a profile-scoped data volume where the selected engine supports it; reconcile endpoint/port changes on startup. |
| Memorystore for Memcached (`memcache.googleapis.com`) | Instance metadata/LROs in memory; cache data is ephemeral | Persist only control-plane metadata and explicitly restore an empty cache; reconcile the container and publish a new endpoint if necessary. |
| Metadata server (`metadata.google.internal`) | Static defaults compiled into the process | Derive per-workload metadata from persisted Compute/Serverless identity; no independent snapshot should be required beyond optional local overrides. |
| Cloud Monitoring (`monitoring.googleapis.com`) | Rolling CPU/memory samples in memory | Keep metrics ephemeral by default; optionally persist bounded profile-scoped samples, never treat them as authoritative resource state. |
| Pub/Sub (`pubsub.googleapis.com`) | Topics, subscriptions, and messages belong to an unmounted emulator container | Add a profile-scoped emulator data/export path if supported, persist event-bridge configuration separately, and avoid replaying already delivered observer events. |
| Cloud Scheduler (`cloudscheduler.googleapis.com`) | Jobs and cron entry IDs in memory | Persist job definitions and delivery metadata; rebuild cron registrations on startup without firing missed jobs unless an explicit catch-up policy allows it. |
| Secret Manager (`secretmanager.googleapis.com`) | Secret metadata, version state, and base64 payloads in profile state | Rehydrate complete local secrets. State and snapshots are sensitive even though the active file is protected by `0600` permissions. |
| Cloud Functions (`cloudfunctions.googleapis.com`) | Function/build/trigger metadata and local source in profile state; LROs in memory; built images and containers may survive | Rehydrate metadata only and never start a missing container during load. |
| Cloud Run (`run.googleapis.com`) | Service/template/traffic metadata in profile state; LROs in memory; image containers may survive | Rehydrate metadata only and keep container reconciliation explicit. |
| Cloud Storage (`storage.googleapis.com`) | Buckets/objects live in an unmounted `fake-gcs-server` container | Add a profile-scoped data mount, snapshot emulator data with object metadata, and persist event-delivery checkpoints separately. |
| Vertex AI (`aiplatform.googleapis.com`) | Provider, endpoint, API key, and model selection in memory; model data is external | Persist non-secret provider/model settings; store credentials through a secret facility or omit them from export, and only reference external model stores. |
| Firestore (`firestore.googleapis.com`) | Data lives in an unmounted Google emulator container | Configure a profile-scoped emulator data/export directory and persist project/database routing metadata. |
| Datastore (`datastore.googleapis.com`) | Emulator data bind-mounted from `./data/datastore` | Replace the working-directory-relative mount with a profile-scoped absolute path, then version and include that directory in export/import. |
| Spanner (`spanner.googleapis.com`) | Data lives in an unmounted emulator container | Add a profile-scoped durable backend where supported; otherwise explicitly classify restart/import as destructive and persist only routing metadata. |

## Adapter lifecycle contract

A future adapter should expose these behaviors consistently:

1. **Load** a recognized schema version before the public gateway accepts
   requests.
2. **Save atomically** after a successful mutation; partially written state
   must never replace the last valid snapshot.
3. **Reconcile** saved control-plane records with Docker containers, volumes,
   files, and external backends.
4. **Export** a portable manifest plus service-owned data, excluding or
   encrypting secrets.
5. **Import** into an empty, isolated profile, validating paths and versions
   before any side effect.
6. **Migrate** older schemas explicitly and fail safely on newer unknown
   versions.

Profile paths are rooted under `~/.minisky/state/profiles/<name>/` by default
(or beneath `MINISKY_STATE_DIR`); adapters must not depend on the daemon's
working directory. Resource and operation IDs must remain stable across restart
and export/import.
