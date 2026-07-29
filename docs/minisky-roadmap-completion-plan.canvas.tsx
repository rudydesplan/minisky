import {
  Callout,
  Card,
  CardBody,
  CardHeader,
  Code,
  Divider,
  Grid,
  H1,
  H2,
  H3,
  Pill,
  Row,
  Stack,
  Table,
  Text,
  useHostTheme,
} from "cursor/canvas";

const phases = [
  ["6", "Fidelity baseline", "Implemented", "Domain contracts and canonical routing pass locally"],
  ["7", "Terraform and SDK", "Verified foundation", "Default apply/assert/no-drift/destroy passes; bounded Phase-15 provider slices are local-only"],
  ["8", "Durable state", "CI-verified history · new local pass", "PR #22 proves the historical operation contract; stable snapshot 328b4cb… / diff 25318c4… locally passes Cloud SQL same-container and volume-only recovery, with external CI still unverified"],
  ["9", "Serverless and events", "CI-verified bounded slice", "Linux CI proves Pack-backed Storage/Pub/Sub function and service delivery plus Tasks retry outcomes; Scheduler remains manual :run E2E"],
  ["10", "Networking and artifacts", "Verified bounded slice", "Terraform-managed HTTP traffic and repository-scoped push/list/delete pass locally"],
  ["11", "DX and distribution", "CI-verified distribution · marker pending", "Historical Windows AMD64 CGO evidence remains valid; the new required native windows-state-markers job is configured-unverified, although cross-compilation and workflow contracts pass locally"],
  ["12", "Platform diagnostics", "CI-verified bounded platform slice", "Recorded CI run 30337314745 proves W3C traces, structured logs, low-cardinality metrics, sanitized OTLP inspection, exporter degradation, replay bounds, and shutdown on commit 60e8215"],
  ["13", "Security simulation", "Verified local slice", "Static-JWKS RS256 WIF and up to four ordered delegates pass locally; production federation remains unsupported"],
  ["14", "Multi-tenancy", "Verified bounded local slice", "Cross-project Pub/Sub Terraform and Go SDK publish/pull/ack pass; shared backends remain bounded"],
  ["15", "Data services", "CI-verified bounded slices", "Firestore, Datastore, and Spanner SDK data-plane behavior passes locally; Redis has a guarded provider lifecycle, and PR #22 records immutable Memcached lifecycle evidence"],
  ["16", "ML, monitoring, networking", "CI-verified bounded slices", "Linux critical CI proves persisted PromQL, Logging, DNS/UDP, Subnetwork/IPAM SDK + Terraform, and deterministic Vertex restart gates"],
  ["17", "CI/CD, plugins, enterprise", "CI-verified bounded slice", "Linux critical CI proves federated RBAC/quota/audit integration; production controls and external publication remain"],
  ["18", "Event workflows", "Experimental · promotion passed", "PR #22 passed the bounded Terraform and SDK jobs; deterministic replay remains local evidence and exactly-once external side effects remain unsupported"],
  ["19", "Pipelines and streaming", "Experimental · promotion passed", "PR #22 passed Composer and Managed Kafka provider jobs plus SDK and Kafka/Airflow backend jobs; production service parity remains unsupported"],
  ["20", "Extended databases", "Experimental · promotion passed", "PR #22 passed four bounded provider jobs and the Docker-backed SDK job; production networking remains outside the claims"],
  ["21–22", "Observability and API management", "Experimental · promotion passed", "PR #22 passed the Service Directory provider job and SDK job; deployment and DNS parity remain unsupported"],
  ["23", "AI and ML services", "Experimental · promotion passed", "PR #22 passed the metadata-only Document AI provider job and SDK job; processing and inference remain unsupported"],
  ["24–25", "Security and networking", "Experimental · promotion passed", "PR #22 passed Organization Policy and Binary Authorization provider jobs plus the SDK job without claiming production enforcement"],
];

const priorities = [
  ["1", "Deliver the stable local certification", "Commit and push source 328b4cb… / diff 25318c4…, then require exact-head Cloud SQL, Storage/PubSub, and native Windows CI", "PR #22 predates these working-tree gates and must not be attributed to them"],
  ["2", "Promote services individually", "Retain all 36 Phase 18–25 domains as experimental and default-off until each promotion boundary passes", "Code existence and batch success do not establish fidelity"],
  ["3", "Prove a publishable release", "Run stable-tag archives, container digest evidence, and installed-artifact checks before publication claims", "GHCR tags, package repositories, taps, buckets, and the external action remain unprovisioned"],
];

const waves = [
  {
    title: "Wave 1 — Close the truth and durability gaps",
    phases: "6–8",
    deliverables: [
      "Align README, manifest, compatibility, and state terminology.",
      "Require one executable contract or explicit passthrough failure contract for every registered domain.",
      "Expand canonical endpoint coverage and the live Terraform/Go/Python stack.",
      "Add profile-scoped emulator storage, explicit Docker reconciliation, durable LRO terminal state, and restart/export/import CI.",
    ],
    gate: "All 71 registered domains stay aligned with generated compatibility truth; durability remains bounded to documented metadata and owned backends.",
  },
  {
    title: "Wave 2 — Complete existing executable workflows",
    phases: "9–11",
    deliverables: [
      "Run actual Functions and Cloud Run handlers; wire Scheduler to the configured gateway.",
      "Prove Storage, Pub/Sub, Scheduler, and Tasks invoke handlers end to end; persist unfinished Task state safely.",
      "Start and own the local registry, make push/list/delete observable, and enforce strict IAM in selected shims.",
      "Add doctor image/disk checks, gateway-first CLI parity, Make targets, and tested GHCR/package-manager releases.",
    ],
    gate: "Event, load-balancer, artifact, IAM, CLI, and installed-package acceptance jobs pass on owned isolated resources.",
  },
  {
    title: "Wave 3 — Platform diagnostics",
    phases: "12",
    deliverables: [
      "Keep structured access logs, W3C propagation, optional OTLP/HTTP export, and Prometheus metrics bounded and sanitized.",
      "Inspect bounded OTLP captures without retaining raw capture files in CI failure diagnostics.",
      "Keep diagnostics on the loopback Dashboard listener, separate from Cloud Logging and the Phase 21–22 service domains.",
      "Treat replay as opt-in in-process execution with project-keyed lookup scoping, not cross-project authorization.",
    ],
    gate: "Three local-only gates pass, and required CI passed in run 30337314745 on full commit 60e82159d7fd80cf6472327b2cd14c2ae1465f23.",
  },
  {
    title: "Wave 4 — Identity and tenant boundaries",
    phases: "13–14",
    deliverables: [
      "Document simulation boundaries, then add TLS and optional client-certificate verification.",
      "Implement short-lived local tokens, key expiry/revocation, impersonation, and route-level strict authorization.",
      "Create a persisted project registry; align CLI and dashboard project selection.",
      "Isolate in-process shims first, then add minimal org/folder inheritance and authorized cross-project resolution.",
    ],
    gate: "Permissive mode remains compatible; strict mode returns GCP-shaped 401/403; two-project Terraform tests prove namespace isolation.",
  },
  {
    title: "Wave 5 — Deepen data services",
    phases: "15",
    deliverables: [
      "15.1 Memorystore: Redis and Memcached now have bounded persisted control planes, exact-owned Docker reconciliation, generated-client evidence, and guarded provider lifecycles.",
      "15.2 Spanner: reuse the official emulator; add only the admin/LRO slice required by SDK and Terraform.",
      "15.3 Firestore: CRUD and queries with profile-scoped emulator export.",
      "15.4 Datastore: fix the working-directory mount and verify entity/index workflows.",
    ],
    gate: "Accepted Phase 15 slices have local generated-client/backend evidence; claimed Redis and Memcached provider resources have guarded restart/no-drift/destroy lifecycles.",
  },
  {
    title: "Wave 6 — Split the oversized advanced phase",
    phases: "16",
    deliverables: [
      "16.1 Monitoring write plus bounded PromQL instant-query subset and profile-scoped Logging filters/sinks; keep deprecated MQL unsupported.",
      "16.2 Vertex prediction MVP using deterministic mocks; optional Ollama remains a separate backend.",
      "16.3 DNS local resolution backed by the existing zone store.",
      "16.4 Bounded subnetwork/IPv4 IPAM plus provider apply/import/no-drift/destroy is complete; workload connectivity, NAT, peering, and PSC remain future work.",
    ],
    gate: "No fidelity promotion without SDK, provider, persistence, and data-plane evidence; all six critical Phase 16 Linux jobs passed in run 30333855843.",
  },
  {
    title: "Wave 7 — Delivery ecosystem before enterprise",
    phases: "17",
    deliverables: [
      "17.1 Keep the tested repository-local setup action, GitLab template, and Compose topology distinct from external publication.",
      "17.2 Maintain scaffold tooling and the in-tree plugin API v0 without claiming an out-of-process runtime loader.",
      "17.3 Maintain reproducible benchmarks and fixed-window, process-local quota middleware.",
      "17.4 Preserve the guarded federated RBAC/quota/audit cross-gate and checksummed air-gap evidence.",
    ],
    gate: "The bounded enterprise cross-gate passed in critical run 30333855843; external identity, distributed quotas, immutable audit storage, package publication, and runtime loading remain separate gates.",
  },
  {
    title: "Wave 8 — Promote Phase 18–25 services by evidence",
    phases: "18–25",
    deliverables: [
      "Keep every Phase 18–25 service domain default-off and experimental until its individual promotion gate passes.",
      "Reuse semantic import validation, transactional persistence, typed scoped LROs, opaque pagination, explicit routing, and strict IAM foundations.",
      "Promote only bounded package workflows with public-gateway, restart, generated-client, failure, and cleanup evidence.",
      "Keep unsupported methods explicit while exact-owned Batch, Composer, Kafka, AlloyDB, and Valkey backends retain collision-safe cleanup and truthful failure behavior.",
      "Refresh the committed CodeGraph database whenever structural service or routing changes make the index stale.",
    ],
    gate: "PR #22 passed general CI, critical reliability, Memcached, all 12 Terraform jobs, and all seven SDK/backend jobs at exact source revision 8e16d147b0127bd3120eae106aa0da1fb59a52c9.",
  },
];

export default function MiniSkyRoadmapCompletionPlan() {
  const theme = useHostTheme();

  return (
    <Stack gap={20} style={{ padding: 24, background: theme.bg.editor, color: theme.text.primary }}>
      <Stack gap={8}>
        <Row align="center" justify="space-between" wrap>
          <H1>MiniSky roadmap completion plan</H1>
          <Pill active>Repository audit · 2026-07-29</Pill>
        </Row>
        <Text tone="secondary">
          A dependency-ordered plan grounded in <Code>README.md</Code>, <Code>PRODUCT.md</Code>,
          compatibility documents, implementation, tests, and CI.
        </Text>
      </Stack>

      <Callout tone="info" title="Implementation status">
        The registry verifies 71 domains: 35 implemented, 36 experimental, and 0 deferred. The current uncommitted
        reliability milestone makes the shared durable operation contract persist-before-publish, accepts an
        ambiguous post-commit save only after exact readback, preserves JSON metadata without numeric loss, and keeps
        matching terminal retries idempotent while rejecting conflicts. Covered Compute and Memcached mutation,
        reconciliation, restart, and polling paths align with that contract. This is exactly-once operation-result
        publication, not exactly-once external side effects. Phase 18–25 services remain default-off experimental.
        Their package, strict-IAM, generated-client, restart, backend, cleanup, docs-truth, and race evidence passes in
        the current working tree. At exact source revision 8e16d147b0127bd3120eae106aa0da1fb59a52c9, PR #22&apos;s
        dedicated path-aware, weekly, and manually runnable promotion workflow passed all 12 Terraform legs and seven
        SDK/backend gates while general CI, critical reliability, and Memcached also passed.
        Commit 852d9e3 adds isolated public-gateway Pub/Sub → Eventarc → Workflows evidence with exact raw request
        nonce/argument/result checks, foreign-topic/project and strict-IAM bounds, and a deterministic post-admission
        pause. The live gate interrupts MiniSky after durable admission, then resumes the same stable Workflow
        execution identity to its exact terminal result with no duplicate execution resource and exact-owned cleanup.
        This proves idempotent admission/resource identity, not exactly-once external side effects, CloudEvents parity,
        OIDC, ordering, or transport provisioning.
        Separately, the machine-readable Memcached service gate records its immutable local revision and critical CI
        job URL. It remains authoritative for the bounded dimensions, tool entry points, and CI state, and makes no
        broad GCP parity claim. Stable snapshot source SHA-256
        328b4cb13c6ca1705ca51d0e3fb543a830cd6a4af2be8aa8ef3ebda456873a25 and diff SHA-256
        25318c4dffcf6f04931fe84d1b7cb27218cc0c3a4f8cb63e46f8ff1f90469033 now record
        <Code>local-passed-uncommitted</Code> Cloud SQL recovery. The uncommitted Storage/Pub/Sub boundary gate is
        also <Code>local-passed-uncommitted</Code>. Native
        <Code>windows-state-markers</Code> remains <Code>configured-unverified</Code>; cross-compilation and workflow
        contracts are not a native Windows test pass.
      </Callout>

      <Callout tone="info" title="Recorded external evidence">
        PR #22 passed general CI run 30416460163, critical reliability and Memcached in run 30416460134, and all 12
        Terraform plus seven SDK/backend jobs in promotion run 30416460053 at exact source revision
        8e16d147b0127bd3120eae106aa0da1fb59a52c9. These records apply only to that revision and do not verify the
        uncommitted Cloud SQL, Storage/PubSub, or native Windows gates.
      </Callout>

      <Grid columns="1fr 1fr 1fr" gap={12}>
        <Card>
          <CardHeader>Registry contract</CardHeader>
          <CardBody><H2>71 domains</H2><Text tone="secondary">35 implemented · 36 experimental · 0 deferred</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Phase 18–25 gate</CardHeader>
          <CardBody><H2>Bounded evidence</H2><Text tone="secondary">Local matrices plus exact-revision PR #22 CI provenance</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Promotion status</CardHeader>
          <CardBody><H2>19/19 passed</H2><Text tone="secondary">Twelve Terraform jobs and seven SDK/backend jobs at one exact revision</Text></CardBody>
        </Card>
      </Grid>

      <Grid columns="2fr 1fr" gap={20}>
        <Stack gap={8}>
          <H2>Promotion evidence state</H2>
          <Text>
            <Code>pkg/evidence/batch_gates.json</Code> records six batches across package, generated-client,
            restart, backend/Docker, strict-IAM, Terraform, cleanup, and CI dimensions. The complete Go race suite,
            documentation truth gate, package references, strict-IAM routes, and isolated anonymous-volume cleanup
            pass locally.
          </Text>
          <Text tone="secondary">
            All six Phase 18–25 SDK/restart/cleanup workflows pass locally, including terminal exact-owned Batch
            cleanup, Phase 19&apos;s heavy Kafka/Airflow path, pinned Phase 20 backends, and isolated Phase 24–25
            routing backends. PR #22 promotion run 30416460053 passed the matching 12 Terraform and seven SDK/backend
            jobs at full commit 8e16d147b0127bd3120eae106aa0da1fb59a52c9.
          </Text>
          <Text tone="secondary">
            Commit 852d9e3&apos;s guarded Phase 18 public-gateway gate deterministically pauses after durable Eventarc
            intent and Workflow execution admission, interrupts MiniSky, and resumes the same stable execution
            identity to the exact terminal result without creating a duplicate execution resource. It also proves
            foreign-project/topic and strict-IAM non-delivery plus exact-owned cleanup. Idempotent admission/resource
            identity is proven; exactly-once external side effects, CloudEvents parity, OIDC, ordering, transport
            provisioning, and Eventarc promotion remain explicit non-goals.
          </Text>
        </Stack>
        <Stack gap={8}>
          <H3>Terraform status</H3>
          <Text tone="secondary">
            Twelve bounded provider lifecycles now pass with provider 7.41.0, including the coupled
            <Code>google_alloydb_cluster</Code> and <Code>google_alloydb_instance</Code> gate. AlloyDB proves
            real PostgreSQL protocol persistence, restart reconciliation, no drift, canonical imports, ordered
            destroy, durable 404s, and exact-owned cleanup without claiming production networking or HA parity.
            Service Directory additionally proves a complete namespace/service/endpoint metadata hierarchy without
            claiming DNS or network resolution. Document AI proves one processor control-plane lifecycle without
            claiming document processing, OCR quality, or model inference.
            Organization Policy additionally proves one project boolean policy and advisory decision without claiming
            production enforcement, IAM, or compliance.
            Binary Authorization adds only the default-off <Code>google_binary_authorization_policy</Code> singleton:
            apply observes allow/deny before restart; after restart, the gate proves policy persistence and no drift
            without repeating those decisions; matching import returns 0, stale import returns 2 before reconciliation
            reaches zero drift, and destroy restores and persists the exact default policy.
          </Text>
          <Text tone="secondary">
            Independent package evidence proves enforced DENY locally blocks MiniSky Cloud Deploy rollouts.
            DRYRUN_AUDIT_LOG_ONLY permits rollout and returns AUDIT; no durable audit record or log is created.
            Unsupported attestation, global-policy, and cluster-rule evaluation returns explicit UNSUPPORTED.
            This is local emulation, not GKE or production admission security. The Binary Authorization Terraform
            check passed in PR #22&apos;s exact-revision promotion run. Like the other eleven domain checks, that CI
            pass remains bounded and does not promote service fidelity.
          </Text>
          <Text tone="secondary">
            Outside the Phase 18–25 matrix, <Code>pkg/evidence/service_gates.json</Code> is the source of truth for
            dedicated Memcached and Cloud SQL lifecycle dimensions. Memcached&apos;s local and critical CI evidence
            remains pinned to PR #22&apos;s exact commit. Cloud SQL recovery is
            <Code>local-passed-uncommitted</Code> at the stable source/diff fingerprints, while its required external
            job remains <Code>configured-unverified</Code>. Cache contents, production Cloud SQL HA/security, and
            broad GCP parity remain unclaimed.
          </Text>
        </Stack>
      </Grid>

      <Stack gap={10}>
        <H2>Phase reality check</H2>
        <Table
          headers={["Phase", "Scope", "Reality", "Remaining completion condition"]}
          rows={phases}
          rowTone={phases.map((row) => row[2] === "Implemented" || row[2].startsWith("Verified") ? "success" : "warning")}
          striped
          stickyHeader
        />
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Next evidence milestones</H2>
        <Text tone="secondary">
          Ordered by what currently blocks truthful promotion or release claims.
        </Text>
        <Table
          headers={["Order", "Milestone", "Required evidence", "Do not overclaim"]}
          rows={priorities}
          striped
          stickyHeader
        />
      </Stack>

      <Divider />

      <Grid columns="2fr 1fr" gap={20}>
        <Stack gap={8}>
          <H2>Phase 9, 13, 14, 16, 17, and 18 verified boundaries</H2>
          <Text>
            The real Pack v0.40.8 gate passed locally and in critical Linux run 30333855843. Storage and Pub/Sub each invoked an
            existing function and a Cloud Run-style service through MiniSky&apos;s local <Code>/v2/deploy</Code>
            helper; service readiness and owned container deletion passed. Tasks proved two-attempt
            <Code>503 → 204</Code> completion and two-attempt terminal <Code>500</Code> failure.
          </Text>
          <Text tone="secondary">
            This does not verify the Cloud Run v2 image API, full source builds, Terraform serverless,
            Eventarc CloudEvents parity or transport provisioning, Pub/Sub push, Eventarc/Cloud Tasks
            OIDC/task-header/redirect/dead-letter-queue parity, exactly-once task replay, production serverless,
            ordering, or exactly-once delivery. The Phase 18 path remains default-off experimental and Scheduler
            remains a manual <Code>:run</Code> HTTP E2E check.
          </Text>
          <Text>
            Google provider 7.41.0 passed apply → restart → no-drift → destroy for local project-ID WIF
            resources, static inline public JWKS, RS256 exchange, and one-delegate impersonation.
          </Text>
          <Text tone="secondary">
            Issuer and allowed audience match exactly, temporal claims are checked, and the bounded subject is
            preserved exactly for IAM matching; only <Code>google.subject=assertion.sub</Code> executes.
            Returned <Code>ms1</Code> tokens are local, not Google credentials.
          </Text>
          <Text>
            Google provider 7.41.0 and the discovery-based Go SDK passed a two-project Pub/Sub reference flow:
            apply, canonical topic/subscription reads, unique publish, secondary-project pull and acknowledgement,
            zero drift, destroy, and post-destroy 404 checks.
          </Text>
          <Text tone="secondary">
            Resource-scoped strict authorization checks exact attach permission and rejects malformed topic names,
            but the shared emulator backend is not a production tenant-isolation boundary.
          </Text>
          <Text>
            The official generated Monitoring REST client wrote a DOUBLE sample, MiniSky restarted against the
            same isolated profile, and the canonical project-scoped PromQL endpoint returned the persisted value.
          </Text>
          <Text tone="secondary">
            Only exact <Code>{`{__name__="<metric-type>"}`}</Code> instant selectors are supported. Label matchers,
            operators, functions, aggregation, ranges, Boolean samples, and full metric/resource translation remain
            unsupported. MQL stays 501 because Google no longer recommends it for new queries.
          </Text>
          <Text>
            The generated Logging REST client wrote fixed entries for two projects and created a filtered sink.
            Restart verified exact project-scoped entry and sink metadata; deletion remained absent after a third
            daemon start while the entries remained available.
          </Text>
          <Text tone="secondary">
            The bounded slice supports entries write/list and sink create/get/list/delete. Pagination,
            partial-success writes, sink updates, per-entry errors, exactly-once delivery guarantees, log-based
            metrics, and alerting remain unsupported.
          </Text>
          <Text>
            The generated Cloud DNS REST client created a public zone and A record. Restart validated the supported
            persisted fields plus a real loopback UDP answer; a third start verified durable API 404 and UDP
            NXDOMAIN cleanup.
          </Text>
          <Text tone="secondary">
            The bounded resolver is loopback-only and supports A, AAAA, and CNAME. Pagination, DNSSEC signing,
            forwarding and peering, recursion, TCP and encrypted DNS transports, broader records, CNAME chaining,
            EDNS0, and private-network enforcement remain unsupported.
          </Text>
          <Text>
            On 2026-07-26, <Code>make test-phase16-subnetwork</Code> passed locally on Docker Desktop/macOS in 17
            seconds. The generated <Code>google.golang.org/api/compute/v1</Code> client created a custom-mode global
            network and one regional IPv4 subnetwork, polled global/regional operations, captured stable supported
            fields, restarted against the same profile, and verified exact metadata. The exact project/profile-owned
            Docker bridge retained its immutable Docker ID, labels, bridge driver, and single CIDR/IPAM. Deleting the
            subnetwork and then the network removed the bridge; a third start verified API 404 and list cleanup.
          </Text>
          <Text>
            The pinned Google provider 7.41.0 then applied <Code>google_compute_network</Code> and
            <Code>google_compute_subnetwork</Code>, reached zero drift, restarted without bridge-ID churn, detached
            and imported both canonical IDs without deleting API or Docker state, reached zero drift before and after
            another restart, and destroyed in dependency order. A final restart confirmed API 404 and exact bridge
            cleanup. The guarded local run passed in 40 seconds.
          </Text>
          <Text tone="secondary">
            Unit, race, and failure tests cover strict 1 MiB ingress, canonical /8–/29 IPv4, one primary subnet per
            custom VPC, project overlap rejection, page-token binding, save/LRO failure, parent and instance reference
            guards, exact project-scoped VM/network Docker identities, unowned/mismatch refusal, ambiguous-create
            recovery, attached-endpoint delete failure, fail-closed compensation, and state-graph validation. This does not
            establish broad Terraform compatibility, auto-mode networks, multiple/secondary ranges, IPv6, routes, workload
            connectivity, firewall packet isolation, Shared VPC, host routing/iptables, cross-host semantics, NAT,
            peering, PSC, VPN/interconnect, or full GCP VPC parity. Both bounded subnet gates passed in critical Linux
            run 30333855843.
          </Text>
          <Text>
            The generated AI Platform REST client recorded two deterministic endpoint predictions, MiniSky
            restarted, and the same ordered inputs, framed scores, deployed-model metadata, and canonical model
            resource were verified.
          </Text>
          <Text tone="secondary">
            This is a bounded local simulation, not persisted prediction output or model-semantic parity. Endpoint
            deployment, real inference, streaming/raw/batch prediction, and feature stores remain unsupported.
          </Text>
          <Text>
            The Phase-17 cross-gate passed locally and in critical Linux run 30333855843 with the federated principal exercising Dashboard RBAC,
            gateway authorization, route quota rejection, audit verification, redaction checks, and tamper
            detection. This verifies a bounded local slice, not production-ready enterprise controls.
          </Text>
        </Stack>
        <Stack gap={8}>
          <H3>Still unsupported</H3>
          <Text tone="secondary">
            External IdPs, SSO, SCIM, distributed quotas, immutable/WORM/compliance audit storage, package
            publication, and a remote marketplace or runtime plugin loader. Static-JWKS WIF limits and local
            <Code>ms1</Code> credentials remain; quotas are fixed-window and process-local.
          </Text>
        </Stack>
      </Grid>

      <Divider />

      <Stack gap={16}>
        <H2>Dependency-ordered execution</H2>
        {waves.map((wave, index) => (
          <div key={wave.title}>
            <Card collapsible defaultOpen={index < 2}>
              <CardHeader trailing={<Pill size="sm">Phases {wave.phases}</Pill>}>{wave.title}</CardHeader>
              <CardBody>
                <Grid columns="2fr 1fr" gap={20}>
                  <Stack gap={8}>
                    <H3>Deliverables</H3>
                    {wave.deliverables.map((item, itemIndex) => (
                      <div key={item}>
                        <Row gap={10} align="start">
                          <Text tone="tertiary" as="span">{itemIndex + 1}.</Text>
                          <Text as="span">{item}</Text>
                        </Row>
                      </div>
                    ))}
                  </Stack>
                  <Stack gap={8}>
                    <H3>Exit gate</H3>
                    <Text tone="secondary">{wave.gate}</Text>
                  </Stack>
                </Grid>
              </CardBody>
            </Card>
          </div>
        ))}
      </Stack>

      <Divider />

      <Grid columns="1fr 1fr" gap={20}>
        <Stack gap={8}>
          <H2>Cross-cutting TDD gate</H2>
          <Text>1. Add the smallest failing contract or workflow test.</Text>
          <Text>2. Implement the minimum production behavior.</Text>
          <Text>3. Run focused race tests and shared router/validator/manifest checks.</Text>
          <Text>4. Prove the public gateway with SDK and Terraform clients.</Text>
          <Text>5. Add restart, destroy, failure-injection, and platform evidence where relevant.</Text>
        </Stack>
        <Stack gap={8}>
          <H2>Recorded decisions and remaining boundaries</H2>
          <Text>Profile versus GCP project semantics and Docker multi-project strategy.</Text>
          <Text>ADR 0012 fixes bounded in-memory telemetry, redaction, low-cardinality labels, and a loopback metrics listener.</Text>
          <Text>ADR 0012 fixes bounded in-process replay; persistent traces, a remote listener, Cloud Logging parity, and RBAC replay isolation remain deferred.</Text>
          <Text>TLS trust model and local token format.</Text>
          <Text>Dynamic plugin process boundary, compatibility, signing, and sandboxing.</Text>
        </Stack>
      </Grid>

      <Callout tone="info" title="Latest bounded provider evidence">
        Workflows and Eventarc trigger control-plane gates pass with provider 7.41.0. A separate guarded
        public-gateway gate proves live Pub/Sub → Eventarc → Workflows dispatch plus completed execution persistence
        across restart; deterministic interrupted-intent replay, CloudEvents parity, transport provisioning, OIDC,
        ordering, exactly-once delivery, and Eventarc promotion remain outside that claim. Provider 7.41.0 exposes no
        Batch job resource or Batch custom endpoint.
        The Phase 25 Binary Authorization singleton now has an independent local-passed provider 7.41.0 record,
        exact script and Make target, import/no-drift/reset semantics, and temporary backup/cleanup references.
        This is separate from the already recorded Phase 24–25 generated-client CI gate: no Binary Authorization
        Terraform CI pass is claimed. Keep every domain experimental and default-off; fidelity promotion,
        production enforcement, and distribution publication remain separate workflows.
      </Callout>
    </Stack>
  );
}
