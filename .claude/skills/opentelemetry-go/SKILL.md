---
name: opentelemetry-go
description: "Add or repair OpenTelemetry instrumentation in MiniSky's Go gateway, shims, orchestrator, or outbound HTTP paths. Use for tracer/meter provider setup, W3C propagation, HTTP spans, low-cardinality metrics, OTLP or Prometheus export, trace-log correlation, and bounded shutdown. Do not use for request replay, dashboard feature design, generic logging cleanup, or observability work that does not use OpenTelemetry."
---

# OpenTelemetry Go

Add telemetry as an optional, testable cross-cutting concern without changing API behavior.

## Verify the baseline

Read `go.mod`, startup/shutdown code, `pkg/router/proxy.go`, relevant shim/orchestrator paths, and logging/dashboard APIs before designing instrumentation.

OpenTelemetry packages currently appear only as indirect dependencies and MiniSky has no telemetry provider initialization, `/metrics` endpoint, trace store, or trace dashboard contract. Treat all of those as new infrastructure, not existing behavior.

Use the versions selected by `go get`/`go mod tidy`; do not hard-code a semantic-convention import version from this skill. Keep all `go.opentelemetry.io/otel*` modules version-compatible.

## Design before dependencies

Define:

- signals needed: traces, metrics, and/or log correlation;
- exporter: OTLP, Prometheus, test exporter, or none;
- enablement and endpoint configuration;
- behavior when telemetry configuration/export fails;
- ownership of global providers and propagators;
- shutdown order and timeout;
- cardinality and sensitive-data policy.

Instrumentation must be disabled or no-op by default unless the product requirement says otherwise. Exporter failure must not corrupt a GCP response.

## Provider lifecycle

Construct providers explicitly and return an idempotent shutdown function:

```go
func setupTelemetry(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error)
```

- Build a resource with stable service name/version and useful environment attributes.
- Use a batch span processor for production exporters and a synchronous/in-memory exporter in tests.
- Register `TraceContext` plus `Baggage` propagation when HTTP boundaries need distributed context.
- Install globals once during startup; avoid package `init` side effects.
- Shut down with a bounded context and combine errors from all providers.
- Ensure partially initialized exporters are closed when a later step fails.

The Prometheus OpenTelemetry exporter is a metric reader/collector. Exposing it requires registration with the Prometheus HTTP handler; do not assume creating the exporter automatically creates `/metrics`.

## HTTP instrumentation

Prefer maintained instrumentation such as `otelhttp` around server/client boundaries where it fits the router. Manual spans are appropriate for MiniSky-specific operations.

- Extract remote context before starting a server span.
- Inject context into outbound requests.
- Use stable route/template names when available; raw resource paths create cardinality and privacy problems.
- Set server/client span kind correctly.
- Record errors and set error status for actual failures. Do not mark every 4xx as an internal server error without defining policy.
- Use current HTTP semantic-convention helpers instead of legacy attributes such as `http.method`, `http.url`, and `http.status_code`.
- Preserve optional interfaces when wrapping `http.ResponseWriter` (`Flusher`, `Hijacker`, `Pusher`, `ReaderFrom`) or use a tested middleware that does.
- Do not record request/response bodies, authorization, cookies, tokens, SQL passwords, object contents, or arbitrary headers.

Pass the derived `context.Context` through router, shim, Docker, state, and outbound HTTP calls. Starting a child span with the old parent context breaks hierarchy.

## Metrics

Define instruments once after the meter provider is installed; handle constructor errors.

Good labels are bounded:

- service/domain;
- HTTP method;
- normalized route;
- status-code class;
- operation kind or terminal state.

Never label by project, resource name, URL, trace/request ID, error text, container ID, task name, or user input.

Follow OpenTelemetry naming conventions: units and names must not duplicate Prometheus suffixes accidentally. Choose counters, histograms, up/down counters, or observable gauges according to the measured value. Document histogram units and boundaries.

If `/metrics` is added:

- bind it to the existing intended listener or a separately configured loopback listener;
- prevent it from falling through GCP request validation/routing;
- define whether it is local-only;
- test content type, representative metric names, and absence of sensitive labels.

## Logging correlation

Use the existing logger where possible. Add `trace_id` and `span_id` only when the span context is valid. Correlation fields do not make logs OpenTelemetry logs.

- Do not trust an arbitrary inbound request ID without length/character validation.
- Generate a bounded ID when absent and echo only the sanitized value.
- Keep structured fields typed and stable.
- Avoid duplicate logs from middleware and handlers.

Request capture/replay is intentionally out of scope: it has separate storage, authorization, redaction, and SSRF risks.

## Test-first workflow

1. Add a failing test using an in-memory/manual-reader exporter or propagator.
2. Assert spans/metrics by semantic fields, not timestamps or generated IDs.
3. Implement the smallest instrumentation seam.
4. Run focused tests and the race detector.
5. Exercise startup and shutdown with export success, failure, and timeout.

```bash
gofmt -w <changed-go-files>
go test -race ./pkg/router ./pkg/orchestrator ./pkg/shims/<service>
go mod tidy
go test -race ./...
```

Use `httptest` to verify incoming `traceparent` extraction and outgoing injection. Use deterministic clocks/readers when metric timing matters; do not add sleeps.

## Acceptance gates

- No telemetry packages are direct dependencies unless imported by MiniSky.
- Disabled telemetry preserves existing behavior and starts no exporter.
- Parent/child relationships and remote-parent extraction are tested.
- Outbound HTTP carries W3C context.
- Error spans and status attributes match the documented policy.
- Metrics are low-cardinality and have correct units.
- No secret, body, raw URL, or resource identifier appears in telemetry.
- Exporter failure is observable but does not crash request handling.
- Shutdown flushes within a deadline and does not leak goroutines.
- `go test -race ./...`, `go vet ./cmd/... ./pkg/... ./ui`, and the normal build pass.

Report enabled signals/exporters, configuration surface, cardinality choices, and shutdown behavior.
