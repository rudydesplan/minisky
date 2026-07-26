# Service Compatibility

This document lists every GCP service domain registered in MiniSky with its
fidelity tier, persistence model, and current implementation status.

## Fidelity Tiers

| Tier | Meaning |
|------|---------|
| **High** | The registered local contract has stronger fidelity for its deliberately narrow surface; it does not mean full GCP parity |
| **Standard** | A bounded custom API surface is implemented; consult the service notes for executable versus metadata-only behavior |
| **Passthrough** | Requests are proxied to a Docker-backed emulator; compatibility is limited by that backend and MiniSky routing |

## Persistence Models

| Model | Meaning |
|-------|---------|
| **File** | Versioned JSON state survives restarts |
| **Memory** | In-memory only; state lost on restart (persistence planned) |
| **Docker** | Runtime data belongs to a Docker-backed emulator; it may be ephemeral unless a profile runtime mount is documented |
| **Hybrid** | Metadata in JSON + data in Docker containers/volumes |
| **Static** | No mutable state; configuration-only |

## Service Catalog

### Core Infrastructure (Phases 6–11)

| Domain | Service | Fidelity | Persistence | LRO | Terraform |
|--------|---------|----------|-------------|-----|-----------|
| compute.googleapis.com | Compute Engine | Standard | Hybrid | ✅ | ✅ |
| container.googleapis.com | GKE | Standard | File | ✅ | Guarded opt-in |
| storage.googleapis.com | Cloud Storage | Passthrough | Docker | — | ✅ |
| bigquery.googleapis.com | BigQuery | Standard | File | — | ✅ |
| pubsub.googleapis.com | Pub/Sub | Passthrough | Docker | — | ✅ |
| sqladmin.googleapis.com | Cloud SQL | Standard | Hybrid | ✅ | Guarded opt-in |
| cloudfunctions.googleapis.com | Cloud Functions | Standard | Hybrid | ✅ | — |
| run.googleapis.com | Cloud Run | Standard | Hybrid | ✅ | — |
| iam.googleapis.com | IAM | Standard | File | — | ✅ |
| iamcredentials.googleapis.com | IAM Credentials | Standard | Static | — | — |
| dns.googleapis.com | Cloud DNS | Standard | File | — | ✅ |
| cloudkms.googleapis.com | Cloud KMS | Standard | File | — | ✅ |
| secretmanager.googleapis.com | Secret Manager | Standard | File | — | ✅ |
| cloudscheduler.googleapis.com | Cloud Scheduler | Standard | File | — | ✅ |
| cloudtasks.googleapis.com | Cloud Tasks | Standard | File | — | ✅ |
| cloudbuild.googleapis.com | Cloud Build | Standard | Hybrid | ✅ | ✅ |
| artifactregistry.googleapis.com | Artifact Registry | Standard | File | ✅ | Guarded opt-in |
| bigtableadmin.googleapis.com | Bigtable Admin | Standard | File | Terminal scoped cluster operations | — |
| bigtable.googleapis.com | Bigtable Data | Standard | Hybrid | — | Backend-limited |
| dataproc.googleapis.com | Dataproc | Standard | Hybrid | ✅ | — |
| appengine.googleapis.com | App Engine | Standard | Hybrid | ✅ | — |
| redis.googleapis.com | Memorystore (Redis) | Standard | Hybrid | ✅ | ✅ |
| aiplatform.googleapis.com | Vertex AI | Standard | File | — | ✅ |
| monitoring.googleapis.com | Cloud Monitoring | Standard | File | — | ✅ |
| logging.googleapis.com | Cloud Logging | Standard | File | — | ✅ |
| cloudresourcemanager.googleapis.com | Resource Manager | Standard | File | — | ✅ |
| sts.googleapis.com | Security Token Service | Standard | Static | — | — |
| metadata.google.internal | Metadata Server | High | Static | — | — |

The checkmarks and guarded labels above identify only a tested slice, not an
entire service API. Important boundaries:

- `Example only` in the Terraform column means optional HCL exists but no
  provider apply/no-drift/destroy claim is made. The authoritative accepted
  resource list is `docs/terraform-compatibility.md`.

- App Engine and Dataproc resource metadata now survives restart. App Engine
  does not create missing apps on observation; Dataproc marks interrupted jobs
  terminal and executes only supported Spark/PySpark jobs against exactly owned
  cluster containers.
- Artifact Registry persists repository metadata and bounded terminal operation
  outcomes. Package/version views come from the profile-owned Registry v2
  backend; blobs and manifests are not in metadata snapshots.
- Compute covers bounded instance CRUD, one custom network/subnetwork/bridge
  slice, and a classic global HTTP load-balancer graph using one unmanaged zonal
  instance group and default-service routing. Managed/regional groups,
  host/path routing, HTTPS and non-HTTP proxies, IPv6, NAT, peering, PSC, and
  general VPC/load-balancer parity remain unsupported.
- Bigtable cluster create/get/list/delete is metadata-only. Its scoped terminal
  operations carry typed metadata and responses but create no cluster nodes.
- Cloud SQL persists instance/database/user control-plane metadata. Database and
  user lifecycle is tested only through the guarded provider gate; database
  files remain container runtime data and restored instances have no stale
  endpoint.
- Cloud Tasks replays persisted nonterminal tasks once per API lifetime with the
  same task ID and retry budget. This is at-least-once: a crash between target
  acceptance and acknowledgement persistence can duplicate a delivery.
- Logging persists unacknowledged sink deliveries, retries them after restart,
  and cancels pending work when a sink is deleted. The same acknowledgement
  window permits duplicates.
- Scheduler deliveries are tied to the Scheduler API lifetime, not the incoming
  `:run` request context; shutdown cancels and bounded-waits for active work.
- Native Windows binaries support the in-process GKE metadata surface, but the
  secure Kind kubeconfig ownership/publish path is fail-safe unsupported on
  Windows. The guarded Kind lifecycle evidence is Unix/Linux.
- The gateway validator is a curated allow-by-default set of selected mutating
  method/path rules. It is not full Discovery Document schema validation.

### Docker-Backed Emulators

| Domain | Service | Fidelity | Persistence | Notes |
|--------|---------|----------|-------------|-------|
| firestore.googleapis.com | Firestore | Passthrough | Docker | Official emulator |
| datastore.googleapis.com | Datastore | Passthrough | Docker | Official emulator |
| spanner.googleapis.com | Spanner | Passthrough | Docker | Official emulator |
| identitytoolkit.googleapis.com | Firebase Auth | Passthrough | Docker | Firebase emulator |
| firebasehosting.googleapis.com | Firebase Hosting | Passthrough | Docker | Firebase emulator |
| firebaseio.com | Firebase RTDB | Passthrough | Docker | Firebase emulator |

## Deferred Services

| Domain | Reason |
|--------|--------|
| memcache.googleapis.com | Returns 501 UNIMPLEMENTED for all operations |

## Machine-Readable Manifest

<!-- This section is parsed by pkg/registry/manifest_test.go — do not change the format -->

| `aiplatform.googleapis.com` | standard | file |
| `appengine.googleapis.com` | standard | hybrid |
| `artifactregistry.googleapis.com` | standard | file |
| `bigquery.googleapis.com` | standard | file |
| `bigtable.googleapis.com` | standard | hybrid |
| `bigtableadmin.googleapis.com` | standard | file |
| `cloudbuild.googleapis.com` | standard | hybrid |
| `cloudfunctions.googleapis.com` | standard | hybrid |
| `cloudkms.googleapis.com` | standard | file |
| `cloudresourcemanager.googleapis.com` | standard | file |
| `cloudscheduler.googleapis.com` | standard | file |
| `cloudtasks.googleapis.com` | standard | file |
| `compute.googleapis.com` | standard | hybrid |
| `container.googleapis.com` | standard | file |
| `dataproc.googleapis.com` | standard | hybrid |
| `datastore.googleapis.com` | passthrough | docker |
| `dns.googleapis.com` | standard | file |
| `firebasehosting.googleapis.com` | passthrough | docker |
| `firebaseio.com` | passthrough | docker |
| `firestore.googleapis.com` | passthrough | docker |
| `iam.googleapis.com` | standard | file |
| `iamcredentials.googleapis.com` | standard | static |
| `identitytoolkit.googleapis.com` | passthrough | docker |
| `logging.googleapis.com` | standard | file |
| `memcache.googleapis.com` | deferred | static |
| `metadata.google.internal` | high | static |
| `monitoring.googleapis.com` | standard | file |
| `pubsub.googleapis.com` | passthrough | docker |
| `redis.googleapis.com` | standard | hybrid |
| `run.googleapis.com` | standard | hybrid |
| `secretmanager.googleapis.com` | standard | file |
| `spanner.googleapis.com` | passthrough | docker |
| `sqladmin.googleapis.com` | standard | hybrid |
| `storage.googleapis.com` | passthrough | docker |
| `sts.googleapis.com` | standard | static |
