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

## Experimental Services

<!-- BEGIN GENERATED PHASE 18-25 SUMMARY -->
**Generated truth:** 36 experimental; 36 default-off; 11 Terraform claims. Persistence inventory: file=22, hybrid=4, memory=4, static=6.

Machine-readable promotion matrix: 6 batch gates and 11 per-domain Terraform checks. Package-unit gates passed locally: 6/6; strict-IAM gates passed locally: 6/6. Generated-client lifecycle gates passed locally: 6/6; configured but unverified: 0/6. Restart gates passed locally: 6/6; cleanup gates passed locally: 6/6; CI gates passed: 6/6; configured but unverified: 0/6. Heavy backend CI gates passed: 1/1; configured but unverified: 0/1. Package and IAM passes do not promote compatibility; every inventoried service remains experimental until its required integration gates pass.
<!-- END GENERATED PHASE 18-25 SUMMARY -->

The Phase 18–25 domains marked `experimental` in the machine-readable
manifest below are package-registered prototypes. The offline
`make test-phase18-25-evidence` gate covers their canonical public selector,
default-off/opt-in behavior, validation ordering, scoped IAM where applicable,
and the package-local evidence referenced by
`pkg/evidence/phase18_25.json`. All six generated-client gates passed
locally. Phase 18–25 covered Workflows, Workflow Executions, Eventarc, and
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
a Docker service backend. All six generated-client CI jobs passed on commit
`62d6fa245774f3ff3bdd9b82e19d1c617650d448` in
[run 30285572232](https://github.com/rudydesplan/minisky/actions/runs/30285572232).
The separate Phase 19 Kafka/Airflow backend job passed on commit
`d657e4b0b77a34ddb615124db2d82da810238502` in
[run 30287887431](https://github.com/rudydesplan/minisky/actions/runs/30287887431),
including its final exact-owned cleanup step. Bounded Terraform provider
evidence is tracked per domain in the generated catalog below; all experimental
domains remain default-off and do not claim Standard fidelity.

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
| `binaryauthorization.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | project policy persistence and advisory local evaluation<br>Package tests only: `pkg/shims/binaryauthorization.TestPolicyRestartSaveFailureAndAuthorization`<br>`pkg/shims/binaryauthorization.TestEvaluateRequestUsesActivePolicy`<br>`pkg/shims/binaryauthorization.TestInvalidPersistedPolicyIsRejected` | No |
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
| `eventarc.googleapis.com` | experimental | file | experimental | — (not promoted) | Yes | trigger metadata with project/topic-isolated delivery; public-gateway live dispatch and terminal execution persistence are gated, while deterministic crash-window intent replay is not gateway-proven; provider 7.41.0 apply/restart/no-drift/import/destroy lifecycle remains bounded to trigger metadata<br>Package tests only: `pkg/shims/eventarc.TestCreateTrigger`<br>`pkg/shims/eventarc.TestDeleteTrigger`<br>`pkg/shims/eventarc.TestPersistAndReload`<br>`pkg/shims/eventarc.TestFailedWorkflowDeliveryReplaysAfterRestart`<br>`pkg/shims/eventarc.TestPubSubDeliveryRequiresTriggerProjectAndTransportTopic` | Yes |
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
| `memcache.googleapis.com` | deferred | static | deferred | — (not promoted) | No | Memorystore for Memcached is not implemented; every request returns 501 UNIMPLEMENTED | Not inventoried |
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
