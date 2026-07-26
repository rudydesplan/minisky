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
  ["16", "ML, monitoring, networking", "Verified Monitoring + Logging + Vertex", "Persisted PromQL, Logging entries/sinks, and deterministic predictions pass restart gates"],
  ["17", "CI/CD, plugins, enterprise", "Verified bounded local slice", "Federated RBAC/quota/audit integration passes locally; CI pass evidence and production controls remain"],
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
      "16.4 Subnetworks, then narrowly scoped NAT, peering, and PSC with explicit Docker-platform limits.",
    ],
    gate: "No fidelity promotion without SDK, persistence, and data-plane evidence on supported host platforms.",
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
];

export default function MiniSkyRoadmapCompletionPlan() {
  const theme = useHostTheme();

  return (
    <Stack gap={20} style={{ padding: 24, background: theme.bg.editor, color: theme.text.primary }}>
      <Stack gap={8}>
        <Row align="center" justify="space-between" wrap>
          <H1>MiniSky roadmap completion plan</H1>
          <Pill active>Repository audit · 2026-07-25</Pill>
        </Row>
        <Text tone="secondary">
          A dependency-ordered plan grounded in <Code>README.md</Code>, <Code>PRODUCT.md</Code>,
          compatibility documents, implementation, tests, and CI.
        </Text>
      </Stack>

      <Callout tone="info" title="Implementation status">
        Guarded Terraform-managed HTTP load balancing, Artifact Registry push/list/delete, and the Phase-13
        static-JWKS WIF → delegated impersonation path now pass locally, alongside state durability, Buildpacks
        delivery, Phase-14 cross-project Pub/Sub, Phase-15 emulator, Phase-16 Monitoring/PromQL, Logging, and
        Vertex prediction restart gates, and Phase-17 federated RBAC/quota/audit integration. Twelve guarded
        local gates have passed. Native amd64 and arm64 deb/rpm build-install-smoke-uninstall jobs also pass in
        read-only CI; the opt-in Phase-9 event-delivery and Phase-17 CI jobs are configured but have no CI pass
        evidence yet. Production-grade semantics remain explicit external boundaries.
      </Callout>

      <Grid columns="1fr 1fr 1fr" gap={12}>
        <Card>
          <CardHeader>Guarded local gates</CardHeader>
          <CardBody><H2>12 passed</H2><Text tone="secondary">Including Phase-16 Logging persistence</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Native release smoke</CardHeader>
          <CardBody><H2>Passed</H2><Text tone="secondary">macOS ARM64 snapshot and DuckDB doctor</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Native package gates</CardHeader>
          <CardBody><H2>Passed</H2><Text tone="secondary">amd64 and arm64 deb/rpm lifecycle</Text></CardBody>
        </Card>
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
        Next internal executable milestone: add generated-SDK restart and loopback-resolution evidence for the
        bounded Phase-16 Cloud DNS zone and record-set slice. Homebrew, Scoop, deb, and rpm publication remains
        externally blocked until maintainer-owned repositories, scoped credentials, protected approval
        environments, and native install-from-repository tests exist.
      </Callout>
    </Stack>
  );
}
