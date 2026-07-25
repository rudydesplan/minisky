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
  ["9", "Serverless and events", "Verified local slice", "Pack-backed Scheduler, Tasks, Storage, and Pub/Sub delivery passes locally"],
  ["10", "Networking and artifacts", "Verified bounded slice", "Terraform-managed HTTP traffic and repository-scoped push/list/delete pass locally"],
  ["11", "DX and distribution", "CI gate pending", "Native Linux deb/rpm install jobs are configured; publication remains external"],
  ["12", "Observability", "Local slice", "Persistent span backend intentionally deferred"],
  ["13", "Security simulation", "Local slice", "Provider-backed WIF and delegated impersonation remain 501"],
  ["14", "Multi-tenancy", "Metadata slice", "Shared Docker backends are not project-isolated"],
  ["15", "Data services", "Verified bounded slice", "Firestore, Datastore, and Spanner SDK data-plane gate passes locally"],
  ["16", "ML, monitoring, networking", "Bounded slices", "Advanced peering/NAT/PSC and broad query engines remain 501"],
  ["17", "CI/CD, plugins, enterprise", "Bounded local slice", "Remote marketplace and compliance storage deferred"],
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
      "16.1 Monitoring write/query subset and profile-scoped Logging with bounded filters/sinks.",
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
      "17.1 Publish and test setup-minisky, a GitLab template, and one Compose topology.",
      "17.2 Add scaffold tooling and freeze an in-tree plugin API v0 before designing out-of-process loading.",
      "17.3 Add reproducible benchmarks and configurable quota/rate-limit middleware.",
      "17.4 Start audit/RBAC/air-gap work only after phases 12–14 define identity, tenancy, and audit foundations.",
    ],
    gate: "Fresh CI runners can install, start, test, and stop MiniSky; plugin and enterprise claims remain separate until independently gated.",
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
        Guarded Terraform-managed HTTP load balancing and Artifact Registry push/list/delete now pass locally,
        alongside state durability, Buildpacks delivery, and Phase-15 emulator gates. Native Linux package
        installation is configured for CI but does not become verified evidence until both architecture jobs pass.
        Credentialed publication and production-grade semantics remain explicit external boundaries.
      </Callout>

      <Grid columns="1fr 1fr 1fr" gap={12}>
        <Card>
          <CardHeader>Guarded local gates</CardHeader>
          <CardBody><H2>5 passed</H2><Text tone="secondary">Including Phase-10 traffic and artifacts</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Native release smoke</CardHeader>
          <CardBody><H2>Passed</H2><Text tone="secondary">macOS ARM64 snapshot and DuckDB doctor</Text></CardBody>
        </Card>
        <Card>
          <CardHeader>Pending CI evidence</CardHeader>
          <CardBody><H2>Linux packages</H2><Text tone="secondary">Native amd64 and arm64 install jobs</Text></CardBody>
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
        Run the native amd64 and arm64 deb/rpm build-install-smoke-uninstall jobs. Homebrew and Scoop
        publication remains blocked until maintainer-owned repositories, scoped credentials, protected
        approval environments, and install-from-repository tests exist.
      </Callout>
    </Stack>
  );
}
