# ADR 0012: Local gateway observability and bounded replay

## Status

Accepted for the Phase 12 vertical slice.

## Decision

MiniSky records one structured JSON access event for each request entering the
public API gateway. The record contains a bounded request ID, optional valid
W3C trace/span IDs, HTTP method, normalized route, normalized service domain,
status, and latency. The request ID is returned as
`X-MiniSky-Request-ID`. Invalid inbound IDs are replaced rather than echoed.

Access records live in a concurrency-safe, oldest-first bounded in-memory
store. They are distinct from the emulated Cloud Logging API. MiniSky does not
persist these records in this slice because profile state can contain sensitive
resources and an access-log retention policy has not been defined.

OpenTelemetry export is off by default. `--otel` (or
`MINISKY_OTEL_ENABLED=true`) enables OTLP/HTTP export to the explicit
`--otel-endpoint`/`MINISKY_OTEL_ENDPOINT`. The gateway extracts W3C
Trace Context and Baggage, and instrumented reverse-proxy transports inject
that context downstream. Export and shutdown failures are warnings and do not
alter GCP response payloads.

Prometheus-compatible request count and latency metrics are exposed at exactly
`GET /api/diagnostics/metrics` on the loopback-bound Dashboard listener.
Diagnostics are mounted separately from the public gateway, so they bypass GCP
request validation. Metrics labels are restricted to normalized service,
bounded method, normalized route, and status class.

Request replay is opt-in with `--request-replay` or
`MINISKY_REQUEST_REPLAY_ENABLED=true`. A replay:

- targets only the in-process public gateway handler;
- preserves only method, path, query, host classification, and the allowlisted
  `Accept`, `Content-Type`, `If-Match`, and `If-None-Match` headers;
- never preserves authorization, cookies, or arbitrary headers;
- captures a body only when it is JSON, below the configured strict byte cap,
  and contains no key that looks credential- or secret-bearing;
- is rejected explicitly when capture was omitted or redacted; and
- carries an internal marker so the replay is observed but not recursively
  captured as another replay candidate.

## Consequences

The Dashboard and `minisky diagnostics` command query the same bounded local
store. The Gateway Requests page remains separate from the Cloud Logging page.
Trace queries show gateway records correlated by trace ID; MiniSky does not
provide a full local span backend in this slice. Metrics contain request
latency summaries, not resource-usage metrics.

The Dashboard listener is loopback-only. This makes diagnostics and replay
local by construction, but remote Dashboard access now requires an explicit
operator-controlled tunnel or proxy.

## Security and cardinality constraints

Access events and telemetry never include bodies, authorization, cookies,
arbitrary headers, raw query strings, or error text. Route labels replace known
resource identifiers and collapse oversized routes. Request IDs and service
domains have strict character and length validation. Replay remains a local
developer feature, not an authentication or audit mechanism.
