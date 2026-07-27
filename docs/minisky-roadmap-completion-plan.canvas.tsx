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
  ["9", "Serverless and events", "Verified bounded local slice", "Pack-backed Storage/Pub/Sub function and service delivery plus Tasks retry outcomes pass; Scheduler remains manual :run E2E"],
  ["10", "Networking and artifacts", "Verified bounded slice", "Terraform-managed HTTP traffic and repository-scoped push/list/delete pass locally"],
  ["11", "DX and distribution", "Verified local slice", "Native amd64/arm64 deb/rpm install gates pass; publication remains external"],
  ["12", "Observability", "Local slice", "Persistent span backend intentionally deferred"],
  ["13", "Security simulation", "Verified local slice", "Static-JWKS RS256 WIF and up to four ordered delegates pass locally; production federation remains unsupported"],
  ["14", "Multi-tenancy", "Verified bounded local slice", "Cross-project Pub/Sub Terraform and Go SDK publish/pull/ack pass; shared backends remain bounded"],
  ["15", "Data services", "Verified bounded slice", "Firestore, Datastore, and Spanner SDK data-plane gate passes locally"],
  ["16", "ML, monitoring, networking", "Monitoring + Logging + DNS + Subnetwork/IPAM + Vertex", "Persisted PromQL, Logging, DNS/UDP, Terraform-importable custom-VPC IPv4 subnet/bridge, and deterministic predictions pass restart gates"],
  ["17", "CI/CD, plugins, enterprise", "Verified bounded local slice", "Federated RBAC/quota/audit integration passes locally; CI pass evidence and production controls remain"],
  ["18", "Event workflows", "Experimental · local package gate passed", "Eventarc → Workflows and exact-owned Batch execution have failure, cancellation, scoped-LRO, cleanup, and restart coverage; guarded SDK lifecycle remains unverified"],
  ["19", "Pipelines and streaming", "Experimental · local package gate passed", "Dataflow/Dataform plus owned Composer and Kafka backends are bounded; Pub/Sub Lite stays explicit 501 and the heavy Kafka/Airflow gate is optional-unverified"],
  ["20", "Extended databases", "Experimental · local package gate passed", "AlloyDB, Valkey, Identity Platform, Filestore, and Storage Transfer have bounded lifecycle/cleanup coverage; guarded SDK and restart integration remains unverified"],
  ["21–22", "Observability and API management", "Experimental · local package gate passed", "Durable ingestion, hierarchy, local gateway, directory, rollout, and policy tests pass; external deployment parity remains unsupported"],
  ["23", "AI and ML services", "Experimental · local package gate passed", "Bounded deterministic and explicit-unsupported client contracts pass locally; real model-semantic inference is not claimed"],
  ["24–25", "Security and networking", "Experimental · local package gate passed", "CA, Binary Authorization, DLP, asset, org-policy, perimeter, Armor, CDN, and service-routing boundaries have local enforcement evidence"],
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
    gate: "32/32 domains covered; apply → restart → no-drift → export → clean import → destroy passes in CI.",
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
      "Introduce request-context and middleware seams with a shared redaction policy.",
      "Add structured gateway access logs, optional OTel traces, and low-cardinality Prometheus metrics.",
      "Expose local-only query APIs and dashboard views.",
      "Add opt-in, bounded, same-gateway replay only after privacy and SSRF decisions are recorded.",
    ],
    gate: "Trace propagation, redaction, metric cardinality, exporter failure, shutdown, and replay isolation tests pass under race detection.",
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
    gate: "No fidelity promotion without SDK, provider, persistence, and data-plane evidence; both subnet gates passed on local Docker Desktop/macOS and have opt-in Linux CI configured.",
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
    gate: "The bounded local slice is verified; CI pass evidence, external identity, distributed quotas, immutable audit storage, package publication, and runtime loading remain separate gates.",
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
    gate: "Offline evidence, docs truth, and the full race suite pass. Six generated-client/restart/cleanup CI gates remain configured-unverified; no Phase 18–25 Terraform claim exists.",
  },
];

export default function MiniSkyRoadmapCompletionPlan() {
  const theme = useHostTheme();

  return (
    <Stack gap={20} style={{ padding: 24, background: theme.bg.editor, color: theme.text.primary }}>
      <Stack gap={8}>
        <Row align="center" justify="space-between" wrap>
          <H1>MiniSky roadmap completion plan</H1>
          <Pill active>Repository audit · 2026-07-27</Pill>
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
        default-off experimental. All six package-unit and strict-IAM batch gates pass locally. Guarded
        generated-client, restart, backend/cleanup CI, native Linux/Windows CGO, and Phase 18–25 Terraform evidence
        remain unverified or absent. Production-grade semantics remain explicit external boundaries.
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
          <CardBody><H2>Default-off</H2><Text tone="secondary">Experimental until per-service evidence passes</Text></CardBody>
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
            Configured-unverified is not a pass claim. The six guarded SDK/restart workflows, external GitHub jobs,
            and native Linux/Windows CGO release-equivalent builds still need recorded runs. Phase 19&apos;s heavy
            Kafka/Airflow backend path remains optional-unverified.
          </Text>
        </Stack>
        <Stack gap={8}>
          <H3>Terraform status</H3>
          <Text tone="secondary">
            Zero Phase 18–25 provider claims. Add one only after isolated apply, restart, no-drift, import where
            relevant, destroy, and cleanup evidence passes.
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

      <Grid columns="2fr 1fr" gap={20}>
        <Stack gap={8}>
          <H2>Phase 9, 13, 14, 16, and 17 verified boundaries</H2>
          <Text>
            The real Pack v0.40.8 gate passed locally in about 235 seconds. Storage and Pub/Sub each invoked an
            existing function and a Cloud Run-style service through MiniSky&apos;s local <Code>/v2/deploy</Code>
            helper; service readiness and owned container deletion passed. Tasks proved two-attempt
            <Code>503 → 204</Code> completion and two-attempt terminal <Code>500</Code> failure.
          </Text>
          <Text tone="secondary">
            This does not verify the Cloud Run v2 image API, full source builds, Terraform serverless,
            Eventarc/CloudEvents, Pub/Sub push, Cloud Tasks OIDC/task-header/redirect/dead-letter-queue parity,
            interrupted task replay, production serverless, or durable/ordered/exactly-once delivery. Scheduler
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
            partial-success writes, sink updates, per-entry errors, delivery replay, log-based metrics, and alerting
            remain unsupported.
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
            establish Terraform compatibility, auto-mode networks, multiple/secondary ranges, IPv6, routes, workload
            connectivity, firewall packet isolation, Shared VPC, host routing/iptables, cross-host semantics, NAT,
            peering, PSC, VPN/interconnect, or full GCP VPC parity. Opt-in Linux CI is configured without pass evidence.
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
            The Phase-17 cross-gate passed locally with the federated principal exercising Dashboard RBAC,
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
          <H2>Decisions to record before coding</H2>
          <Text>Profile versus GCP project semantics and Docker multi-project strategy.</Text>
          <Text>Telemetry storage, cardinality, redaction, and metrics listener.</Text>
          <Text>Replay capture/privacy/SSRF boundary.</Text>
          <Text>TLS trust model and local token format.</Text>
          <Text>Dynamic plugin process boundary, compatibility, signing, and sandboxing.</Text>
        </Stack>
      </Grid>

      <Callout tone="info" title="Next evidence milestone">
        Run and record the six guarded generated-client create → restart → observe → delete workflows, including
        exact-owned backend and terminal cleanup assertions. Then run the optional Phase 19 Kafka/Airflow path and
        native Linux/Windows CGO release gates. Keep every domain experimental until its own matrix is complete;
        add Terraform claims only after provider lifecycle evidence exists.
      </Callout>
    </Stack>
  );
}
