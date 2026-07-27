# Product

## Platform

GCP emulator (CLI + embedded web dashboard)

## Users

Backend engineers, DevOps/platform engineers, and full-stack developers who build on Google Cloud Platform. They use Terraform, Google Cloud SDKs, and serverless frameworks and need one local gateway with explicit compatibility boundaries. Go and Python SDK smoke suites cover the core fixture; a guarded Java Storage smoke is available for the tested local slice. First use and Docker-backed services can require network access to obtain dependencies and images.

## Product Purpose

MiniSky gives GCP developers a bounded local environment for testing selected Google Cloud workflows. Success means a documented Terraform or SDK lifecycle passes against the public gateway with the supported paths, errors, polling, persistence, and backend behavior. It does not mean general Google Cloud API or provider parity.

## Brand Personality

Technical, honest, precise. MiniSky speaks with the voice of a senior infrastructure engineer who values correctness over convenience. The tone is **direct** (no "might work"), **honest** (clearly states what's emulated vs. stubbed), and **fidelity-first** (never fakes success for operations it can't actually perform).

Three-word personality: **precise, honest, reliable**.

## Anti-references

MiniSky must avoid:

- **Fake success responses**: Returning 200 OK for operations that aren't actually implemented. Stubbed operations return 501 UNIMPLEMENTED with a clear message.
- **Shallow mocks**: Skipping LRO state transitions, ignoring intermediate resource states, returning instant results for async operations.
- **Feature-count marketing**: Claiming "29+ services" without documenting which are executable vs. metadata-only. Every service has a documented fidelity tier.
- **Fragmented experience**: Separate ports per service, separate configs, no shared state. One gateway, one config, one binary.
- **Magic behavior**: Hidden defaults, undocumented shortcuts, behavior that differs from real GCP without explanation.

## Design Principles

1. **Fidelity before breadth.** A service that works correctly is worth more than ten that return fake 200s. Deepen existing services before adding new names.
2. **Honest compatibility.** Document what works, what's simulated, and what's stubbed. Never hide limitations behind successful HTTP status codes.
3. **Contract fidelity within a named slice.** Match paths, errors, polling, and resource shapes only where executable evidence exists; state unsupported boundaries explicitly.
4. **Durable local environments where adopted.** Profile-scoped metadata adapters survive restarts. Export/import is deterministic for JSON metadata, not Docker volumes, runtime directories, object data, or DuckDB files.
5. **Test complete workflows.** Acceptance gates cover create → observe → update → restart → destroy — not only successful responses.

## Architecture Principles

1. **Single binary, lazy-loaded.** Registered services activate on demand; startup performance is measured rather than promised.
2. **Docker for selected data planes.** Owned containers back bounded Compute, Cloud SQL, optional Kind/GKE, passthrough, and other executable slices. Without Docker, the gateway, dashboard, diagnostics, and in-process shims continue while Docker-backed requests fail explicitly.
3. **Versioned state.** JSON state store with atomic writes, profile scoping, and export/import portability.
4. **Curated request validation.** A maintained allow-by-default subset validates selected mutating method/path pairs before dispatch. It is not full Discovery Document validation.
5. **Scoped operations.** Async APIs expose only their service-specific, scope-checked operation routes. Some bounded metadata operations complete immediately; the shared operation manager is not proof of Google LRO parity.

## Accessibility & Inclusion

Dashboard accessibility target: WCAG 2.1 AA for the tested flows. The application shell and management surfaces use the light Google-style theme; Logging, Monitoring, terminal, and log-output views intentionally use dark operational palettes. Key commitments:
- All interactive elements keyboard-navigable with visible focus states.
- Color contrast and status meaning are checked on both the light shell and dark operational views.
- `prefers-reduced-motion` respected for dashboard animations.
- CLI output works in screen readers and high-contrast terminals.
- Error messages are actionable — state what went wrong AND how to fix it.
