# Service Compatibility

This document lists every GCP service domain registered in MiniSky with its
fidelity tier, persistence model, and current implementation status.

## Support Status

- **Implemented** domains expose their documented bounded runtime contract by
  default and carry a fidelity tier.
- **Experimental** domains remain registered but have no promoted fidelity
  tier because promotion evidence remains incomplete. They return canonical JSON
  `501 UNIMPLEMENTED` by default. Set
  `MINISKY_ENABLE_EXPERIMENTAL_SERVICES=1` for one process to expose the
  prototype handlers; opt-in does not promote their status.
- **Deferred** domains have no available implementation.

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
| memcache.googleapis.com | Memorystore (Memcached) | Standard | Hybrid | ✅ | Generated service gate |
| aiplatform.googleapis.com | Vertex AI | Standard | File | — | ✅ |
| monitoring.googleapis.com | Cloud Monitoring | Standard | File | — | ✅ |
| logging.googleapis.com | Cloud Logging | Standard | File | — | ✅ |
| cloudresourcemanager.googleapis.com | Resource Manager | Standard | File | — | ✅ |
| sts.googleapis.com | Security Token Service | Standard | Static | — | — |
| metadata.google.internal | Metadata Server | High | Static | — | — |

<!-- BEGIN GENERATED MEMCACHED SERVICE GATE -->
**Generated Memcached service-gate truth:** The bounded lifecycle is `local-passed` at immutable source commit `8e16d147b0127bd3120eae106aa0da1fb59a52c9` with Google provider `7.41.0`, `make test-memcache-integration`, and `scripts/memcache-integration.sh`.

Lifecycle dimensions (16): `sdk-create`, `sdk-update`, `sdk-read`, `sdk-list`, `sdk-delete`, `data-plane-set`, `data-plane-get`, `daemon-restart`, `terraform-apply`, `terraform-no-drift`, `terraform-restart`, `terraform-import-normalization`, `terraform-post-import-no-drift`, `terraform-destroy`, `durable-404`, `exact-docker-cleanup`.

CI is `ci-passed` in [GitHub Actions run 30416460134](https://github.com/rudydesplan/minisky/actions/runs/30416460134) ([job](https://github.com/rudydesplan/minisky/actions/runs/30416460134/job/90463936974)) on commit `8e16d147b0127bd3120eae106aa0da1fb59a52c9`. This evidence does not claim broad GCP parity or promote service fidelity.
<!-- END GENERATED MEMCACHED SERVICE GATE -->
<!-- BEGIN GENERATED REDIS SERVICE GATE -->
**Generated Redis durability-gate truth:** Gate `phase15-redis` is `local-passed-uncommitted` at source SHA-256 `ecf23998d307c1d0383dd0d424b667c4277b0091757ab00a795502db64a4def7` and diff SHA-256 `ccedf19e3aa4b0d825e3204fdfff632ae8f33d128a7754d89f61deb18e54426e` via `scripts/redis-integration.sh` (`make test-redis-integration`). Google provider `7.41.0`.

Lifecycle dimensions (13): `immutable-image-identity`, `sdk-create-read-list-delete-lro`, `data-plane-set-get`, `process-restart-survival`, `container-replacement-survival`, `foreign-resource-refusal`, `export-boundary`, `terraform-apply`, `terraform-no-drift`, `terraform-restart`, `terraform-destroy`, `durable-404`, `exact-docker-cleanup`.

CI is `configured-unverified`; no external run URL or commit is recorded. Boundaries: no portable AOF export, HA/failover, TLS/IAM/VPC/PSC, Redis Cluster, or hostile-daemon guarantee. Exact volume deletion remains a cooperative Docker daemon, non-atomic re-inspect/delete boundary. A process or host crash after a Docker create side effect but before resolved provenance is persisted can leave exact-labelled resources; deterministic names and labels aid explicit cleanup, but restart neither adopts nor automatically removes them.
<!-- END GENERATED REDIS SERVICE GATE -->
<!-- BEGIN GENERATED STORAGE PUBSUB BOUNDARY -->
**Generated Storage/Pub/Sub boundary truth:** The `local-passed-uncommitted` working-tree gate runs `make test-storage-persistence-pubsub-session`, `scripts/storage-persistence-pubsub-session-integration.sh`, and `TestStoragePersistenceAndPubSubSessionBoundaries`. Stable snapshot source SHA-256: `328b4cb13c6ca1705ca51d0e3fb543a830cd6a4af2be8aa8ef3ebda456873a25`; diff SHA-256: `25318c4dffcf6f04931fe84d1b7cb27218cc0c3a4f8cb63e46f8ff1f90469033`. CI is `ci-passed` in [GitHub Actions run 30431422780](https://github.com/rudydesplan/minisky/actions/runs/30431422780) ([job](https://github.com/rudydesplan/minisky/actions/runs/30431422780/job/90509291773)) on exact PR #23 head commit `794b68439c59bfa0dd35b37962049a1a3e510ea1`; the stable local fingerprints remain separate from immutable CI provenance.

The exact pinned public Pub/Sub image is acquired against the active daemon with an isolated anonymous Docker configuration, then checked for immutable digest syntax, `linux/amd64` platform execution, and advertised `--data-dir` capability. Storage uses a profile-scoped runtime bind mount. Buckets and objects survive exact-owned Storage emulator-container replacement. Pub/Sub resources and messages last only for one official emulator session: MiniSky process crash/restart continuity is supported only while the same exact-owned Pub/Sub container remains alive. Replacing the Pub/Sub backend/container loses topics, subscriptions, and queued messages. Graceful MiniSky shutdown tears down managed Docker resources and is not a Pub/Sub continuity path.

Storage and Pub/Sub runtime data remain outside metadata export/import. This gate does not claim exactly-once delivery, portable data export, IAM, HA, security, or full GCP parity. Its Docker cleanup evidence assumes cooperative, exclusive use of the managed resource names. Docker volume deletion accepts only a mutable name, not a conditional immutable identity: MiniSky revalidates exact ownership and identity immediately before deletion and fails closed, but a foreign replacement in the final inspect-to-delete interval cannot be excluded atomically. This is a bounded cleanup invariant, not a hostile-daemon security boundary. Public registry/network access remains required; the global unowned image cache may retain an authorized pull; Pub/Sub remains amd64/emulation/session-only. Five unrelated local volumes and a pre-existing lock observed during certification are not product evidence.
<!-- END GENERATED STORAGE PUBSUB BOUNDARY -->
<!-- BEGIN GENERATED STABLE SNAPSHOT CERTIFICATION -->
**Generated stable-snapshot certification:** The stable local certification remains identified by source SHA-256 `328b4cb13c6ca1705ca51d0e3fb543a830cd6a4af2be8aa8ef3ebda456873a25` and diff SHA-256 `25318c4dffcf6f04931fe84d1b7cb27218cc0c3a4f8cb63e46f8ff1f90469033`. PR #23 exact-head commit `794b68439c59bfa0dd35b37962049a1a3e510ea1` has immutable CI evidence; historical PR #22 evidence remains separate.

- Cloud SQL restart recovery is `local-passed-uncommitted`: live `POSTGRES_16` row survival passed through same-container restart and volume-only recovery, followed by exact cleanup; the bounded Terraform apply/no-drift/destroy lifecycle also passed. CI is `ci-passed` in [critical run 30431422780](https://github.com/rudydesplan/minisky/actions/runs/30431422780) ([Cloud SQL job](https://github.com/rudydesplan/minisky/actions/runs/30431422780/job/90509291797)).
- Storage/Pub/Sub is `local-passed-uncommitted`: anonymous acquisition and immutable digest/platform/capability checks passed before Storage replacement persistence, Pub/Sub session-loss boundaries, and exact cleanup. CI is `ci-passed` in the same [critical run 30431422780](https://github.com/rudydesplan/minisky/actions/runs/30431422780) ([Storage/Pub/Sub job](https://github.com/rudydesplan/minisky/actions/runs/30431422780/job/90509291773)).
- Native `windows-state-markers` is `ci-passed` in [general CI run 30431422742](https://github.com/rudydesplan/minisky/actions/runs/30431422742) ([native job](https://github.com/rudydesplan/minisky/actions/runs/30431422742/job/90509292114)); the authoritative `quality` aggregate is also `ci-passed` ([quality job](https://github.com/rudydesplan/minisky/actions/runs/30431422742/job/90510621655)) in that exact run.

These exact-head PR #23 passes verify only the three listed gates and their documented boundaries. PR #22 URLs apply only to their exact historical commit.
<!-- END GENERATED STABLE SNAPSHOT CERTIFICATION -->

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
- Cloud SQL persists instance/database/user control-plane metadata plus
  non-secret runtime provenance. Database files use an exact-owned named volume.
  Complete local provenance permits same-container restart and volume-only
  replacement around that same proven volume; restored instances publish no
  stale endpoint and become reachable only after protocol and authenticated
  readiness. Passwords, local provenance authorization, and Docker data remain
  outside metadata export/import. PostgreSQL 16 live recovery and the bounded
  Terraform lifecycle are `local-passed-uncommitted` at the source/diff
  fingerprints in the generated stable-snapshot section. PR #23's exact-head
  [Cloud SQL job](https://github.com/rudydesplan/minisky/actions/runs/30431422780/job/90509291797)
  is `ci-passed` on commit
  `794b68439c59bfa0dd35b37962049a1a3e510ea1`.
  Exact deletion guards and their cooperative-daemon,
  non-atomic inspect-to-delete boundary are documented in the state model.
- Memorystore for Memcached persists bounded instance and admitted-operation
  metadata, but not cache entries. The generated service gate above is
  authoritative for lifecycle dimensions, local evidence status and revision
  provenance, provider/tool entry points, and CI status. The bounded claim does
  not include GCP networking, HA, maintenance, security, or broad API parity.
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

## Experimental Services

<!-- BEGIN GENERATED PHASE 18-25 SUMMARY -->
**Generated truth:** 36 experimental; 36 default-off; 12 Terraform claims. Persistence inventory: file=22, hybrid=4, memory=4, static=6.

Machine-readable promotion matrix: 6 batch gates and 12 per-domain Terraform checks. Package-unit gates passed locally: 6/6; strict-IAM gates passed locally: 6/6. Generated-client lifecycle gates passed locally: 6/6; configured but unverified: 0/6. Restart gates passed locally: 6/6; cleanup gates passed locally: 6/6; CI gates passed: 6/6; configured but unverified: 0/6. Heavy backend CI gates passed: 1/1; configured but unverified: 0/1. Terraform CI gates passed: 12/12; configured but unverified: 0/12. Admission replay gates passed locally: 1/1. Package and IAM passes do not promote compatibility; every inventoried service remains experimental until its required integration gates pass.
<!-- END GENERATED PHASE 18-25 SUMMARY -->
<!-- BEGIN GENERATED PR22 PROMOTION EVIDENCE -->
**Generated PR #22 promotion truth:** Exact source revision `8e16d147b0127bd3120eae106aa0da1fb59a52c9` passed [general CI run 30416460163](https://github.com/rudydesplan/minisky/actions/runs/30416460163) ([job](https://github.com/rudydesplan/minisky/actions/runs/30416460163/job/90463937205)), [critical reliability run 30416460134](https://github.com/rudydesplan/minisky/actions/runs/30416460134) ([job](https://github.com/rudydesplan/minisky/actions/runs/30416460134/job/90464017548)), and [the bounded promotion run 30416460053](https://github.com/rudydesplan/minisky/actions/runs/30416460053). The Memcached lifecycle passed in the critical run ([job](https://github.com/rudydesplan/minisky/actions/runs/30416460134/job/90463936974)).

All 12 Terraform jobs passed: [alloydb job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961998), [binary-authorization job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962058), [composer job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962071), [document-ai job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962053), [eventarc job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962021), [filestore job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961994), [identity-platform job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962019), [managed-kafka job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961989), [org-policy job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962085), [service-directory job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962025), [storage-transfer job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962027), [workflows job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464962052).

All seven SDK/backend jobs passed: [phase18-25 SDK job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961861), [phase19 SDK job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961867), [phase19 backend job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961865), [phase20 SDK job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961883), [phase21-22 SDK job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961884), [phase23 SDK job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961893), [phase24-25 SDK job](https://github.com/rudydesplan/minisky/actions/runs/30416460053/job/90464961869).

These immutable records apply only to that source revision. The current working-tree promotion workflow does not retain a duplicate full-quality job: authoritative quality checks remain in the separate general CI workflow, while `promotion-assets` builds and shares `ui/dist` for the integration jobs. PR #22's URLs do not verify those current workflow changes. The uncommitted Storage/Pub/Sub boundary gate is not attributed to these historical runs.
<!-- END GENERATED PR22 PROMOTION EVIDENCE -->

The Phase 18–25 domains marked `experimental` in the machine-readable manifest
below are package-registered prototypes. The offline
`make test-phase18-25-evidence` gate covers their canonical public selector,
default-off/opt-in behavior, validation ordering, scoped IAM where applicable,
and the package-local evidence referenced by
`pkg/evidence/phase18_25.json`. All six generated-client gates pass locally;
the generated PR #22 section above records the matching immutable external
promotion evidence.
Phase 18–25 covered Workflows, Workflow Executions, Eventarc, and
exact-owned Docker-backed Batch execution. Phase 19 covered terminal/cancelled
Dataflow, the Dataform hierarchy, explicit Pub/Sub Lite 501 behavior, Kafka
protocol round-trip, and Airflow DAG triggering. Both gates passed restart and
cleanup, including exact-owned containers and anonymous volumes where relevant.
Phase 20 covered AlloyDB, Filestore, Identity Platform, Redis-domain Valkey,
Storage Transfer, canonical Storage upload and transfer custom actions, restart
truth, deletion, and exact-owned digest-pinned PostgreSQL, Valkey, and Storage
backend cleanup. Phase 21–22 covered telemetry, API Gateway, Service Directory,
Service Management/Control, Binary Authorization, Cloud Deploy, strict errors,
restart truth, generated deletion, executable loopback routing, and exact-owned
isolated network cleanup; its real-backend dimension remains not applicable.
Phase 23 covered deterministic bounded translation, Vision, and Language
behavior; explicit Speech/TTS and semantic/inference boundaries; GCP-shaped
errors; Document AI, Dialogflow, and Vertex control-plane lifecycles; restart
truth; project isolation; sensitive-data absence; and exact-owned network
cleanup. Its real-backend dimension also remains not applicable. The final
Phase 24–25 gate covered security/network resource lifecycles, perimeter and
proxy policy deny/allow enforcement, strict IAM and GCP-shaped errors, canonical
Compute/network routing, restart truth, generated deletion, and exact cleanup.
Its backend gate exercised two isolated loopback HTTP backends without claiming
a Docker service backend. Bounded Terraform provider evidence is tracked per
domain in the generated catalog below; all experimental domains remain
default-off and do not claim Standard fidelity.

The path-aware, weekly, and manually runnable promotion workflow owns the
12 Terraform legs and seven SDK/backend gates exactly once after removing 20
general-CI manual shadows. The Binary Authorization Terraform claim remains an
independent Phase 24–25 bounded check. Its recorded promotion pass does not
promote the service beyond experimental/default-off support.

aiplatform.googleapis.com remains implemented for the existing Vertex
prediction/configuration slice. Its newly integrated Index and Model Registry
control-plane routes are separately default-off behind the same experimental
opt-in and do not change that existing evidence claim.

## Machine-Readable Manifest

<!-- This section is parsed by pkg/registry/manifest_test.go — do not change the format -->

<!-- BEGIN GENERATED SERVICE CATALOG -->
| Domain | Registry classification | Persistence | Support | Fidelity | Default-off | Method note / evidence tests | Terraform claim |
|--------|-------------------------|-------------|---------|----------|-------------|------------------------------|-----------------|
| `accesscontextmanager.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | bounded access-policy, access-level, perimeter metadata, and advisory access decisions<br>Package tests only: `pkg/shims/accesscontextmanager.TestCreateAccessPolicy`<br>`pkg/shims/accesscontextmanager.TestGetAccessPolicy`<br>`pkg/shims/accesscontextmanager.TestListAccessPolicies`<br>`pkg/shims/accesscontextmanager.TestDeleteAccessPolicyCascade`<br>`pkg/shims/accesscontextmanager.TestPersistAndReload`<br>`pkg/shims/accesscontextmanager.TestCheckAccessRequestUsesPersistedAccessLevelConditions` | No |
| `aiplatform.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `alloydb.googleapis.com` | experimental | hybrid | experimental | — (not promoted) | Yes | persisted cluster metadata plus exact-owned PostgreSQL primary-instance backend; provider 7.41.0 coupled apply/restart/no-drift/import/destroy lifecycle<br>Package tests only: `pkg/shims/alloydb.TestCreateCluster`<br>`pkg/shims/alloydb.TestClusterToPrimaryInstanceLifecycleUsesBackend`<br>`pkg/shims/alloydb.TestRestartReconcilesWithoutProvisioningDuplicate`<br>`pkg/shims/alloydb.TestPersistAndReload`<br>`pkg/shims/alloydb.TestCreateInstanceIsCanonicalUnsupportedBeforeMutation` | Yes |
| `apigateway.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | API/config/gateway metadata and loopback-only proxy<br>Package tests only: `pkg/shims/apigateway.TestApiConfigHierarchyAndLoopbackProxy`<br>`pkg/shims/apigateway.TestApiConfigRejectsSSRFAndUnsupportedMode`<br>`pkg/shims/apigateway.TestApiGatewayHierarchySurvivesRestart` | No |
| `appengine.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `artifactregistry.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `batch.googleapis.com` | experimental | hybrid | experimental | — (not promoted) | Yes | persisted job metadata plus bounded exact-owned Docker runnable execution and cleanup<br>Package tests only: `pkg/shims/batch.TestCreateExecutableJobRunsAndCapturesTerminalState`<br>`pkg/shims/batch.TestRestartCleansOwnedContainerAndFailsInterruptedExecutableJob`<br>`pkg/shims/batch.TestDockerExecutableJobIntegration`<br>`pkg/shims/batch.TestCancelJobReturnsLROAndPersistsCancelledState`<br>`pkg/shims/batch.TestPersistAndReload` | No |
| `bigquery.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `bigtable.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `bigtableadmin.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `binaryauthorization.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | project policy persistence and bounded local enforcement; provider 7.41.0 apply with pre-restart allow/deny observation, restart persistence/no-drift, matching-import/stale-import-reconcile/destroy-reset, plus package-proven Cloud Deploy deny, dry-run AUDIT permit, and explicit unsupported outcomes without durable audit logging or GKE/production admission security<br>Package tests only: `pkg/shims/binaryauthorization.TestPolicyRestartSaveFailureAndAuthorization`<br>`pkg/shims/binaryauthorization.TestEvaluateRequestUsesActivePolicy`<br>`pkg/shims/binaryauthorization.TestInvalidPersistedPolicyIsRejected`<br>`pkg/shims/binaryauthorization.TestUnsupportedEvaluationModesRemainExplicit`<br>`pkg/shims/binaryauthorization.TestDryRunRuleReportsDenialWithoutBlockingDeployment` | Yes |
| `cloudasset.googleapis.com` | experimental | memory | experimental | — (not promoted) | Yes | in-memory inventory projection; no restart claim<br>Package tests only: `pkg/shims/cloudasset.TestListAssetsPagination` | No |
| `cloudbuild.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `clouddeploy.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | delivery hierarchy metadata and loopback-only rollout<br>Package tests only: `pkg/shims/clouddeploy.TestFullHierarchy`<br>`pkg/shims/clouddeploy.TestRolloutRejectsSSRFAndUnsupportedStrategy`<br>`pkg/shims/clouddeploy.TestCloudDeployHierarchySurvivesRestart` | No |
| `clouderrorreporting.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | error-event and group metadata<br>Package tests only: `pkg/shims/errorreporting.TestPersistAndReload`<br>`pkg/shims/errorreporting.TestGroupStatsPageTokenIsProjectBound` | No |
| `cloudfunctions.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `cloudkms.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `cloudprofiler.googleapis.com` | experimental | memory | experimental | — (not promoted) | Yes | in-memory profile lifecycle; no restart claim<br>Package tests only: `pkg/shims/cloudprofiler.TestCreateProfile` | No |
| `cloudresourcemanager.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `cloudscheduler.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `cloudtasks.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `cloudtrace.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | trace batch-write and scoped list metadata<br>Package tests only: `pkg/shims/cloudtrace.TestPersistAndReload`<br>`pkg/shims/cloudtrace.TestBatchWriteRejectsCrossProjectSpan`<br>`pkg/shims/cloudtrace.TestTracePageTokenIsScopeBound` | No |
| `composer.googleapis.com` | experimental | hybrid | experimental | — (not promoted) | Yes | persisted environment metadata plus exact-owned Airflow backend lifecycle; provider 7.41.0 apply/restart/no-drift/import/destroy lifecycle<br>Package tests only: `pkg/shims/composer.TestPersistAndReload`<br>`pkg/shims/composer.TestCreateEnvironmentProvidesReadableConfigForMinimalProviderRequest` | Yes |
| `compute.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `container.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `dataflow.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | metadata-only job lifecycle and cancellation<br>Package tests only: `pkg/shims/dataflow.TestCancelJob`<br>`pkg/shims/dataflow.TestPersistAndReload` | No |
| `dataform.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | repository and workspace metadata<br>Package tests only: `pkg/shims/dataform.TestPersistAndReload` | No |
| `dataproc.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `datastore.googleapis.com` | passthrough | docker | implemented | passthrough | No | Google Cloud Datastore emulator; cold-start and backend errors are deterministic, CRUD requires Docker | Not inventoried |
| `dialogflow.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | agent metadata and bounded local detect-intent<br>Package tests only: `pkg/shims/dialogflowcx.TestAgentRestartSaveFailureAndConcurrency` | No |
| `dlp.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | template metadata and bounded local content transforms<br>Package tests only: `pkg/shims/dlp.TestPersistAndReload` | No |
| `dns.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `documentai.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | processor metadata and bounded processing; provider 7.41.0 apply/restart/no-drift/import/delete lifecycle without processing or inference parity<br>Package tests only: `pkg/shims/documentai.TestDocumentAICreatedProcessorSurvivesRestart`<br>`pkg/shims/documentai.TestDocumentAIDeleteOperationSurvivesRestart`<br>`pkg/shims/documentai.TestPersistAndReload`<br>`pkg/shims/documentai.TestProcessDocumentEnforcesDecodedPayloadLimit` | Yes |
| `eventarc.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | trigger metadata with project/topic/strict-IAM-isolated delivery; commit 852d9e3 public-gateway live evidence proves deterministic post-admission interruption, same stable Workflow execution identity across restart, exact terminal result, no duplicate execution resources, and owned cleanup; this is idempotent admission/resource identity, not exactly-once external side effects, CloudEvents parity, OIDC, ordering, or transport provisioning; provider 7.41.0 apply/restart/no-drift/import/destroy remains bounded to trigger metadata<br>Package tests only: `pkg/shims/eventarc.TestCreateTrigger`<br>`pkg/shims/eventarc.TestDeleteTrigger`<br>`pkg/shims/eventarc.TestPersistAndReload`<br>`pkg/shims/eventarc.TestFailedWorkflowDeliveryReplaysAfterRestart`<br>`pkg/shims/eventarc.TestPubSubDeliveryRequiresTriggerProjectAndTransportTopic` | Yes |
| `file.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | persisted instance metadata plus bounded profile-local share data; provider 7.41.0 apply/restart/no-drift/import/destroy lifecycle without NFS/VPC parity<br>Package tests only: `pkg/shims/filestore.TestLocalShareDataSurvivesMetadataRestart`<br>`pkg/shims/filestore.TestPersistAndReload`<br>`pkg/shims/filestore.TestLocalShareReadWriteIsBoundedAndPrivate`<br>`pkg/shims/filestore.TestShareIORejectsSymlinkTraversal` | Yes |
| `firebasehosting.googleapis.com` | passthrough | docker | implemented | passthrough | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `firebaseio.com` | passthrough | docker | implemented | passthrough | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `firestore.googleapis.com` | passthrough | docker | implemented | passthrough | No | Google Cloud Firestore emulator; cold-start and backend errors are deterministic, CRUD requires Docker | Not inventoried |
| `iam.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `iamcredentials.googleapis.com` | standard | static | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `identityplatform.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | project/tenant/IdP configuration metadata; provider 7.41.0 initialize/update/restart/import/reset lifecycle for project authorized domains<br>Package tests only: `pkg/shims/identityplatform.TestPersistAndReload`<br>`pkg/shims/identityplatform.TestProjectAndTenantConfigUseInjectedAuthBackend`<br>`pkg/shims/identityplatform.TestInitializeProjectConfigForProvider` | Yes |
| `identitytoolkit.googleapis.com` | passthrough | docker | implemented | passthrough | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `language.googleapis.com` | experimental | static | experimental | — (not promoted) | Yes | deterministic stateless analysis only<br>Package tests only: `pkg/shims/language.TestAnalyzeSentimentBoundaries` | No |
| `logging.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `managedkafka.googleapis.com` | experimental | hybrid | experimental | — (not promoted) | Yes | persisted cluster/topic metadata plus exact-owned plaintext loopback Kafka backend; provider 7.41.0 apply/restart/no-drift/import/destroy lifecycle<br>Package tests only: `pkg/shims/managedkafka.TestPersistAndReload`<br>`pkg/shims/managedkafka.TestCreateCluster` | Yes |
| `memcache.googleapis.com` | standard | hybrid | implemented | standard | No | Memcached metadata is profile-persisted; instances use exact-owned Memcached containers without durable cache contents | Not inventoried |
| `metadata.google.internal` | high | static | implemented | high | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `monitoring.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `networksecurity.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | authorization and TLS policy metadata with advisory policy evaluation<br>Package tests only: `pkg/shims/networksecurity.TestPersistAndReload`<br>`pkg/shims/networksecurity.TestAuthorizationPolicyRequestUsesStoredDenyRule` | No |
| `networkservices.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | mesh and HTTP route metadata with advisory route resolution<br>Package tests only: `pkg/shims/servicemesh.TestPersistAndReload`<br>`pkg/shims/servicemesh.TestValidateReferencesRequiresOfficialHierarchy`<br>`pkg/shims/servicemesh.TestResolveRequestUsesStoredRouteHierarchy` | No |
| `orgpolicy.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | project policy metadata and bounded local hierarchy evaluation; provider 7.41.0 apply/restart/no-drift/import/delete lifecycle<br>Package tests only: `pkg/shims/orgpolicy.TestPolicyCRUDLifecycle`<br>`pkg/shims/orgpolicy.TestPolicyCreateRestartListDeleteLifecycle`<br>`pkg/shims/orgpolicy.TestEvaluatePolicyUsesResourceBeforeAncestors` | Yes |
| `privateca.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | certificate metadata and revocation without persisted private keys<br>Package tests only: `pkg/shims/privateca.TestIssuePersistsCertificateWithoutPrivateKeyMaterial`<br>`pkg/shims/privateca.TestRevokePersistsWithoutExposingKeyMaterial`<br>`pkg/shims/privateca.TestRevokeCertificateRequestUpdatesPersistedDecisionPath`<br>`pkg/shims/privateca.TestCorruptPersistedCertificateIsRejected`<br>`pkg/shims/privateca.TestCreateCertificateUsesOptionalQueryIDAndDirectCertificateBody` | No |
| `pubsub.googleapis.com` | passthrough | docker | implemented | passthrough | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `pubsublite.googleapis.com` | experimental | static | experimental | — (not promoted) | Yes | explicit unsupported boundary; no fabricated broker<br>Package tests only: `pkg/shims/pubsublite.TestDeferredHandlerNeverAcknowledgesMutations` | No |
| `redis.googleapis.com` | standard | hybrid | implemented | standard | No | Memorystore metadata is profile-persisted; instances use exact-owned Valkey containers and volumes | Not inventoried |
| `run.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `secretmanager.googleapis.com` | standard | file | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `servicecontrol.googleapis.com` | experimental | static | experimental | — (not promoted) | Yes | bounded local check/report against active service rollouts; quota allocation stays unsupported<br>Package tests only: `pkg/shims/cloudendpoints.TestConfigRolloutCheckAndReport`<br>`pkg/shims/cloudendpoints.TestUnsupportedControlFeatureReturnsUnimplemented` | No |
| `servicedirectory.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | namespace/service/endpoint hierarchy metadata; provider 7.41.0 apply/restart/no-drift/import/ordered-destroy lifecycle without DNS resolution<br>Package tests only: `pkg/shims/servicedirectory.TestFullHierarchy`<br>`pkg/shims/servicedirectory.TestServiceDirectoryHierarchySurvivesRestart`<br>`pkg/shims/servicedirectory.TestServiceDirectoryPageTokenIsParentBound`<br>`pkg/shims/servicedirectory.TestDeleteHierarchyRequiresEmptyParent` | Yes |
| `servicemanagement.googleapis.com` | experimental | static | experimental | — (not promoted) | Yes | bounded local service config and rollout control plane<br>Package tests only: `pkg/shims/cloudendpoints.TestConfigRolloutCheckAndReport`<br>`pkg/shims/cloudendpoints.TestUnsupportedControlFeatureReturnsUnimplemented` | No |
| `spanner.googleapis.com` | passthrough | docker | implemented | passthrough | No | Cloud Spanner emulator; cold-start and backend errors are deterministic, database behavior requires Docker | Not inventoried |
| `speech.googleapis.com` | experimental | static | experimental | — (not promoted) | Yes | explicit unsupported recognition boundary<br>Package tests only: `pkg/shims/speech.TestRecognizeRejectsMalformedAndFailsHonestly` | No |
| `sqladmin.googleapis.com` | standard | hybrid | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `storage.googleapis.com` | passthrough | docker | implemented | passthrough | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `storagetransfer.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | bounded local GCS-to-GCS transfer metadata/outcome; provider 7.41.0 apply/run/restart/import/soft-delete lifecycle<br>Package tests only: `pkg/shims/storagetransfer.TestRunTransferJobCopiesObjectsAndPersistsOutcome`<br>`pkg/shims/storagetransfer.TestPersistAndReload`<br>`pkg/shims/storagetransfer.TestListTransferJobsPagination` | Yes |
| `sts.googleapis.com` | standard | static | implemented | standard | No | Registry metadata only; see the hand-written compatibility boundaries above | Not inventoried |
| `texttospeech.googleapis.com` | experimental | static | experimental | — (not promoted) | Yes | explicit unsupported synthesis boundary<br>Package tests only: `pkg/shims/texttospeech.TestSynthesizeValidationPrecedesUnsupported` | No |
| `translate.googleapis.com` | experimental | memory | experimental | — (not promoted) | Yes | in-memory deterministic translation subset; no restart claim<br>Package tests only: `pkg/shims/translate.TestTranslateText` | No |
| `vision.googleapis.com` | experimental | memory | experimental | — (not promoted) | Yes | in-memory deterministic annotation subset; projectless annotate requires X-Goog-User-Project<br>Package tests only: `pkg/shims/vision.TestAnnotateLabels` | No |
| `workflowexecutions.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | shared persisted workflow execution state and cancellation<br>Package tests only: `pkg/shims/workflows.TestCancelExecution`<br>`pkg/shims/workflows.TestCancelExecutionCancelsInflightHTTPCall`<br>`pkg/shims/workflows.TestPersistAndReload` | No |
| `workflows.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | workflow metadata and bounded local execution; provider 7.41.0 apply/restart/no-drift/destroy lifecycle<br>Package tests only: `pkg/shims/workflows.TestCreateWorkflow`<br>`pkg/shims/workflows.TestListWorkflows`<br>`pkg/shims/workflows.TestDeleteWorkflow`<br>`pkg/shims/workflows.TestPersistAndReload`<br>`pkg/shims/workflows.TestRestartMarksActiveExecutionFailed` | Yes |
<!-- END GENERATED SERVICE CATALOG -->
