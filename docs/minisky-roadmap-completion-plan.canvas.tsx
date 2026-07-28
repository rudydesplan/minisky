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
  ["7", "Terraform and SDK", "Verified foundation", "Default apply/assert/no-drift/destroy passes; optional Phase-15 Terraform remains"],
  ["8", "Durable state", "Verified foundation", "Guarded restart/export/import/destroy passes; Docker data snapshots remain bounded"],
  ["9", "Serverless and events", "CI-verified bounded slice", "Linux CI proves Pack-backed Storage/Pub/Sub function and service delivery plus Tasks retry outcomes; Scheduler remains manual :run E2E"],
  ["10", "Networking and artifacts", "Verified bounded slice", "Terraform-managed HTTP traffic and repository-scoped push/list/delete pass locally"],
  ["11", "DX and distribution", "CI-verified distribution slice", "GoReleaser Linux AMD64 snapshot, native amd64/arm64 deb/rpm, and Linux ARM64/macOS ARM64/Windows AMD64 CGO gates pass; publication remains external"],
  ["12", "Platform diagnostics", "CI-verified bounded platform slice", "Required CI run 30337314745 proves W3C traces, structured logs, low-cardinality metrics, sanitized OTLP inspection, exporter degradation, replay bounds, and shutdown on the current PR head"],
  ["13", "Security simulation", "Verified local slice", "Static-JWKS RS256 WIF and up to four ordered delegates pass locally; production federation remains unsupported"],
  ["14", "Multi-tenancy", "Verified bounded local slice", "Cross-project Pub/Sub Terraform and Go SDK publish/pull/ack pass; shared backends remain bounded"],
  ["15", "Data services", "Verified bounded slice", "Firestore, Datastore, and Spanner SDK data-plane gate passes locally"],
  ["16", "ML, monitoring, networking", "CI-verified bounded slices", "Linux critical CI proves persisted PromQL, Logging, DNS/UDP, Subnetwork/IPAM SDK + Terraform, and deterministic Vertex restart gates"],
  ["17", "CI/CD, plugins, enterprise", "CI-verified bounded slice", "Linux critical CI proves federated RBAC/quota/audit integration; production controls and external publication remain"],
  ["18", "Event workflows", "Experimental · delivery + two Terraform gates passed", "The public gateway proves dispatch, negative isolation, and terminal persistence; one package test replays FAILED → SUCCEEDED, but public-gateway crash-window replay remains unproven"],
  ["19", "Pipelines and streaming", "Experimental · core + heavy CI + two Terraform gates passed", "Dataflow/Dataform lifecycle plus exact-owned Kafka protocol and Airflow DAG execution pass; provider 7.41.0 proves bounded Composer and Managed Kafka lifecycles/imports; Pub/Sub Lite stays explicit 501"],
  ["20", "Extended databases", "Experimental · local + CI + four Terraform gates passed", "Provider 7.41.0 proves bounded AlloyDB cluster/primary, Filestore, Identity Platform singleton, and local GCS-to-GCS Storage Transfer lifecycles; production networking remains outside the claims"],
  ["21–22", "Observability and API management", "Experimental · local + CI + one Terraform gate passed", "Provider 7.41.0 proves the Service Directory namespace/service/endpoint hierarchy; durable ingestion, gateway, rollout, policy, restart, and cleanup gates pass while external deployment and DNS parity remain unsupported"],
  ["23", "AI and ML services", "Experimental · local + CI + one Terraform gate passed", "Provider 7.41.0 proves one metadata-only Document AI processor lifecycle; bounded client contracts, restart, sensitive-data absence, and cleanup pass while processing and inference parity remain unsupported"],
  ["24–25", "Security and networking", "Experimental · batch CI + two local Terraform gates", "Provider 7.41.0 proves bounded Organization Policy and Binary Authorization project-policy lifecycles locally; both remain local emulation without production enforcement claims"],
];

const priorities = [
  ["1", "Close Eventarc crash-window evidence", "Interrupt a persisted intent before execution, restart through the public gateway, and prove bounded replay", "The package FAILED → SUCCEEDED test is necessary but not gateway evidence"],
  ["2", "Qualify Terraform claims individually", "Keep the existing 12 domain claims; add another only after provider restart/import/no-drift/destroy evidence", "API routes and generated-client gates do not establish provider compatibility"],
  ["3", "Promote services individually", "Retain all 36 Phase 18–25 domains as experimental and default-off until each promotion boundary passes", "Code existence and batch success do not establish fidelity"],
  ["4", "Prove a publishable release", "Run stable-tag archives, container digest evidence, and installed-artifact checks before publication claims", "GHCR tags, package repositories, taps, buckets, and the external action remain unprovisioned"],
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
      "15.1 Memorystore: persistence, reconciliation, Redis data volume, SDK and Terraform lifecycle.",
      "15.2 Spanner: reuse the official emulator; add only the admin/LRO slice required by SDK and Terraform.",
      "15.3 Firestore: CRUD and queries with profile-scoped emulator export.",
      "15.4 Datastore: fix the working-directory mount and verify entity/index workflows.",
    ],
    gate: "Each service has an SDK data-plane proof plus Terraform create/read/no-drift/destroy and restart behavior.",
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
    gate: "Offline evidence, docs truth, the full race suite, and all six generated-client/restart/cleanup workflows pass locally and in CI. Twelve domain-scoped Terraform checks pass locally; Binary Authorization has no separate CI pass record.",
  },
];

export default function MiniSkyRoadmapCompletionPlan() {
  const theme = useHostTheme();

  return (
    <Stack gap={20} style={{ padding: 24, background: theme.bg.editor, color: theme.text.primary }}>
      <Stack gap={8}>
        <Row align="center" justify="space-between" wrap>
          <H1>MiniSky roadmap completion plan</H1>
          <Pill active>Repository audit · 2026-07-28</Pill>
        </Row>
        <Text tone="secondary">
          A dependency-ordered plan grounded in <Code>README.md</Code>, <Code>PRODUCT.md</Code>,
          compatibility documents, implementation, tests, and CI.
        </Text>
      </Stack>

      <Callout tone="info" title="Implementation status">
        The registry now verifies 71 domains. Guarded Terraform-managed HTTP load balancing, Artifact Registry push/list/delete, and the Phase-13
        static-JWKS WIF → delegated impersonation path now pass locally, alongside state durability, Buildpacks
        delivery, Phase-14 cross-project Pub/Sub, Phase-15 emulator, Phase-16 Monitoring/PromQL, Logging, DNS/UDP,
        Subnetwork/IPAM SDK and Terraform, and Vertex prediction restart gates, and Phase-17 federated
        RBAC/quota/audit integration. Phase 18–25 services now share fail-closed state, scoped operations,
        pagination, routing, strict IAM, exact-owned Docker cleanup, and machine-readable evidence while remaining
        default-off experimental. All six package-unit, strict-IAM, generated-client, restart, and cleanup batch gates
        pass locally, including the applicable Docker and loopback backend boundaries. All six external CI gates
        passed on commit 62d6fa2. On commit d657e4b, Phase 19 Kafka/Airflow, the corrected GoReleaser validation,
        Linux AMD64 release snapshot, native AMD64/ARM64 packages, and Linux ARM64, macOS ARM64, and Windows AMD64
        CGO jobs all passed. On the current PR head, required CI run 30337314745 and critical run 30337314786
        both passed on full commit 60e82159d7fd80cf6472327b2cd14c2ae1465f23, including Phase 12 diagnostics,
        Phase 9, all six Phase 16 jobs, and Phase 17. Provider 7.41.0 now passes twelve independently recorded, domain-scoped Terraform
        lifecycles while every Phase 18–25 service remains default-off and bounded by its documented local contract.
        Phase 18 now also has isolated public-gateway Pub/Sub → Eventarc → Workflows live-dispatch evidence with
        exact raw request nonce/argument/result checks, foreign-topic/project negative publishes, and separate
        terminal execution persistence across restart. A package race test independently isolates trigger-project
        mismatch and configured transport-topic mismatch non-delivery. One package restart test replays a persisted
        FAILED delivery to SUCCEEDED; deterministic public-gateway crash-window replay remains unproven.
        Production-grade semantics remain explicit external boundaries.
      </Callout>

      <Callout tone="info" title="Current PR evidence">
        PR #21 targets 60e82159d7fd80cf6472327b2cd14c2ae1465f23. Required CI run 30337314745 and
        critical-integration run 30337314786 both completed successfully. Optional opt-in integration jobs skipped by
        the pull-request workflow are not treated as passes. The Binary Authorization Terraform lifecycle remains
        local-passed only, with no dedicated Terraform CI pass.
      </Callout>

      <Grid columns="1fr 1fr 1fr" gap={12}>
        <Card>
          <CardHeader>Registry contract</CardHeader>
          <CardBody><H2>71 domains</H2><Text tone="secondary">Manifest, docs, IAM, and routing checked</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Phase 18–25 gate</CardHeader>
          <CardBody><H2>Passed</H2><Text tone="secondary">Offline evidence plus full Go race suite</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Promotion status</CardHeader>
          <CardBody><H2>Default-off</H2><Text tone="secondary">Twelve bounded provider lifecycles pass without batch-wide production promotion</Text></CardBody>
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
            All six Phase 18–25 SDK/restart/cleanup workflows passed
            locally on 2026-07-27, including terminal exact-owned Batch cleanup, Phase 19&apos;s heavy Kafka/Airflow
            path, pinned Phase 20 backends, and isolated Phase 24–25 routing backends. The six GitHub jobs passed in
            run 30285572232 on commit 62d6fa2. Run 30287887431 on commit d657e4b then passed the explicit Phase 19
            Kafka/Airflow workflow, exact cleanup, corrected GoReleaser validation, Linux AMD64 snapshot, native
            Linux package jobs, and Linux ARM64, macOS ARM64, and Windows AMD64 CGO jobs.
          </Text>
          <Text tone="secondary">
            The guarded Phase 18 public-gateway gate separately proves that Pub/Sub publish creates an
            Eventarc-triggered Workflow execution containing the exact raw PublishRequest nonce, argument, and
            terminal result, and that the completed execution remains visible after daemon restart. The request is
            not a CloudEvent envelope. A package restart test separately replays one persisted FAILED delivery exactly
            once to SUCCEEDED. The public-gateway gate does not interrupt a persisted intent before execution, so
            deterministic crash-window replay remains unproven; transport provisioning, OIDC, ordering, exactly-once
            delivery, and Eventarc promotion remain explicit non-goals.
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
            check is local-passed only; it has no invented CI run URL or commit, and it does not promote service fidelity.
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
