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
**docker**, **hybrid** (control-plane metadata plus a data-plane backend), or
**static**.

| Domain | Fidelity | Persistence |
| --- | --- | --- |
| `aiplatform.googleapis.com` | standard | file |
| `appengine.googleapis.com` | standard | hybrid |
| `artifactregistry.googleapis.com` | standard | memory |
| `bigquery.googleapis.com` | standard | file |
| `bigtable.googleapis.com` | standard | hybrid |
| `bigtableadmin.googleapis.com` | standard | hybrid |
| `cloudbuild.googleapis.com` | standard | hybrid |
| `cloudresourcemanager.googleapis.com` | standard | file |
| `cloudfunctions.googleapis.com` | standard | hybrid |
| `cloudkms.googleapis.com` | standard | file |
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
| `memcache.googleapis.com` | standard | hybrid |
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

## Security and project boundaries

- IAM Credentials supports direct and ordered delegated, one-hour-or-shorter
  local `generateAccessToken` with at most four delegates. Longer chains,
  `generateIdToken`, `signJwt`, and `signBlob` are unsupported.
- Security Token Service supports MiniSky local-token exchange plus one bounded
  workload identity path: a local project-ID canonical audience (not a project
  number), an OIDC provider with static inline JWKS, RS256 JWT verification,
  exact issuer/audience matching and temporal checks, and only
  `google.subject=assertion.sub`. The bounded `sub` value is preserved exactly
  in the escaped federated principal, whose IAM binding must match. The result
  is a local `ms1` token, not a Google credential.
- AWS, SAML, X.509, workforce federation, remote OIDC discovery/JWKS, CEL
  conditions or arbitrary mappings, non-RS256 signatures, Google trust roots,
  credential portability/revocation, and WIF undelete/soft-delete recovery
  remain unsupported. WIF exchange performs no network or discovery calls.
- Resource Manager persists project metadata and the
  minimal local organization/folder parent chain. It does not claim complete
  Resource Manager LRO, search, undelete, tag, lien, or org-policy parity.
- Representative in-process state such as BigQuery datasets is keyed by
  project. Docker passthrough services remain shared profile backends unless
  their upstream emulator provides and the integration suite proves project
  isolation.

The unsupported-route contract uses
`/__minisky_contract__/unsupported`. Probe-safe registered handlers must return
HTTP 501 with a GCP JSON error envelope and `UNIMPLEMENTED` status. Docker
wrappers can still execute that reserved probe without starting a container.
The pure lazy domains—Datastore, Firestore, and Spanner—have explicit manifest
rationale and a deterministic router contract: without an available backend
manager they return HTTP 503 `UNAVAILABLE`; a real cold-start failure returns
the same envelope. CRUD/data-plane claims remain backend-gated:

- Datastore and Firestore require their Google emulators.
- Firebase Auth, Realtime Database, and Hosting require Firebase emulators.
- Pub/Sub requires the Google Pub/Sub emulator.
- Spanner requires the Cloud Spanner emulator.
- Storage requires `fake-gcs-server`.

## Evidence boundaries

The manifest is a registration and unsupported-route contract, not a claim of
complete method parity. BigQuery has the deepest native conformance coverage.
The tracked Terraform and SDK smoke suites exercise BigQuery dataset/table
resources, IAM service accounts, and a Docker-backed Storage bucket. The
separate durability gate covers only persisted BigQuery/IAM metadata; Storage
emulator data is outside metadata export/import. Broader compatibility must be
supported by focused executable tests before its manifest fidelity is raised.
The separate guarded Phase-13 gate passed Google provider 7.41.0 apply,
restart/no-drift, static-JWKS JWT exchange, one-delegate impersonation,
authenticated target-token use, destroy, and post-destroy `404` checks. Public
JWKS appeared in Terraform and MiniSky state as expected; the harness verified
that its private key and signed subject JWT were not persisted or logged.

## Phase 9–11 boundaries

- Scheduler cross-shim delivery uses the daemon's configured gateway port.
  Its HTTP end-to-end evidence is a manual `:run` invocation, not scheduled
  clock execution.
- Cloud Tasks queue and task metadata, including terminal attempts, persists.
  After restart, persisted `PENDING` or `RETRYING` work becomes a terminal
  interruption failure and is not replayed. This is metadata durability, not a
  durable delivery queue or payload replay guarantee.
- Simulation stores Serverless metadata without claiming to run code. Requested
  Buildpacks execution fails explicitly when dependencies, source, or an image
  are missing; no Cloud SDK utility image is substituted.
- The guarded Pack v0.40.8 gate uses MiniSky's local `POST /v2/deploy` source
  helper for both functions and Cloud Run-style `type=service` handlers. That
  path is not the Cloud Run v2 image API and does not establish full Cloud Run
  v2 source-build or Terraform serverless compatibility.
- Event delivery is a synchronous local bridge with native MiniSky payloads.
  Eventarc/CloudEvents envelopes, Pub/Sub push, a durable event queue, ordering,
  exactly-once delivery, and production serverless operation are unsupported.
- Cloud Tasks does not claim OIDC, task-header, redirect, or dead-letter-queue
  parity, and interrupted deliveries are not replayed.
- Strict IAM (`MINISKY_IAM_MODE=strict`) covers only Storage bucket/object,
  Pub/Sub topic/subscription/publish, and Compute instance mutations. Callers
  provide `X-MiniSky-Principal`; default mode remains permissive.
- Artifact Registry package/version listing comes from a lazily started,
  profile-owned `registry:2` container on a dynamic loopback port. GCP package
  and version listing is scoped to the repository prefix. Registry v2 manifest
  deletion is enabled and requires a digest; GCP package/version deletion stays
  `501 UNIMPLEMENTED`.
- The pinned Terraform fixture optionally exercises Artifact Registry repository
  create/read/no-drift/destroy and the bounded Compute load-balancer graph.

See [State model](state-model.md) for restart and export behavior and
[Terraform compatibility](terraform-compatibility.md) for tested provider and
SDK coverage.

## Phase 15–16 bounded slices

- Memorystore for Redis validates create requests, exposes GCP-shaped
  create/delete operations, persists profile metadata, reconciles only
  profile-owned Docker containers, and publishes Redis on a Docker-assigned
  loopback port. Redis uses an owned named volume with AOF enabled. Memcached,
  failover, import/export, and upgrade APIs remain `501 UNIMPLEMENTED`.
- Firestore and Datastore remain official Google emulator passthroughs.
  Their data directories are profile-scoped under the non-portable runtime
  tree. The guarded phase 15 SDK smoke covers Firestore document CRUD/query and
  Datastore entity CRUD/ancestor query when Docker collision checks pass.
  Firestore listeners and security rules are not claimed.
- Spanner remains official emulator passthrough. MiniSky publishes both the
  emulator REST/admin port and its gRPC data port on dynamic loopback ports.
  The guarded SDK smoke covers instance/database creation, DDL, insert, read,
  row delete, and database/instance cleanup. The emulator does not provide
  production IAM, TLS, backups, multi-region replication, or production query
  performance, and its data is not included in MiniSky state export.
- Monitoring implements profile-persisted metric descriptor CRUD and a bounded
  time-series write/list subset with `metric.type` equality filters. Monitoring
  Query Language is `501 UNIMPLEMENTED`.
- Logging migrates the legacy global log file into profile state, supports
  bounded `severity`, `logName`, and `resource.type` filters, and supports safe
  relative file sinks plus Pub/Sub sinks with a delivery-loop marker.
  Alerting and log-based metrics are `501 UNIMPLEMENTED`.
- Vertex AI defaults to deterministic profile-configurable mock
  `generateContent` and `predict` behavior. Optional Ollama calls are restricted
  to loopback HTTP endpoints; API keys remain process-memory only. Streaming,
  batch prediction, and feature stores are `501 UNIMPLEMENTED`.
- Cloud DNS retains its persisted managed-zone/RRSet control plane and can run
  an opt-in loopback-only UDP resolver through `MINISKY_DNS_ADDR`. Ports below
  1024 and non-loopback binds are rejected. The resolver currently serves
  A, AAAA, and CNAME records with stored TTL/update/delete/restart behavior.
- Advanced networking is limited to profile-owned Docker bridges and optional
  IPv4 IPAM. MiniSky never installs host-global iptables rules. Cloud NAT,
  peering, PSC service attachments, VPN, and interconnect data planes return
  `501 UNIMPLEMENTED`.
