---
name: benchmark
description: Reproducible MiniSky performance baselines for Go packages, API gateway/shims, state load/save, Docker-backed services, startup, and UI builds. Use for regressions or explicit performance questions; not for invented SLA targets or automatic external load tests.
license: MIT
metadata:
  origin: ECC
---

# Benchmark MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC), under its preserved MIT
license and attribution.

## Applicability

Use when the user asks for a baseline, regression comparison, profiling, or
capacity evidence. Do not run load against an external deployment without
explicit authorization, store baselines in a made-up directory, or apply generic
Core Web Vitals/API SLA thresholds as MiniSky requirements.

## Define the Question

Choose one surface and metric before measuring:

- pure Go hot path: `ns/op`, `B/op`, allocations;
- API/shim: throughput plus p50/p95/p99, errors, and response size;
- startup/doctor: wall time and dependency/backend state;
- state: save/load/import/export time, bytes, and resource count;
- Docker/Kind/Buildpacks: provisioning latency and cleanup success;
- UI/release: lint/build/image build time and artifact/image size.

Record commit/tree state if allowed, OS/architecture, Go version, CGO/compiler,
CPU/memory, Docker version, runtime profile, backend overrides, warm/cold state,
dataset size, concurrency, duration, and exact command. Never compare simulation
with DuckDB/Kind/Buildpacks as if they were equivalent.

## Method

1. Establish correctness first; a fast incorrect response is not a result.
2. Isolate ports, HOME/state, Terraform state, and Docker resource names.
3. Warm up when appropriate and run enough repeated samples to show variance.
4. Change one variable.
5. Compare distributions, errors, allocations, and resource cleanup—not only an
   average.
6. Keep raw evidence in a user-approved path; do not create tracked artifacts
   automatically.

## Go Benchmarks

Prefer package benchmarks close to code:

```bash
go test -run='^$' -bench='BenchmarkName' -benchmem -count=10 ./pkg/<package>
```

Use `pprof` only after a repeatable regression exists. Ensure compiler
optimization does not eliminate benchmark work and move setup outside the timed
region. For concurrent code, validate with `go test -race` separately; race mode
is not a performance baseline.

## API and Backend Benchmarks

Use a fixed payload/corpus and bounded concurrency. Capture non-2xx responses and
verify response contracts. Docker-backed tests require explicit Docker access,
unique managed resources, cleanup, and a note that host contention affects
results. Kind is optional. Native BigQuery comparisons must state
`CGO_ENABLED=1` and the DuckDB backend; no-CGO simulation is a separate mode.

## Report

For each metric provide baseline, candidate, absolute/percent delta, variability,
and confidence caveats. State whether the change exceeds a threshold agreed
before measurement; if no threshold exists, report the delta without labeling it
a regression. Include commands, environment, correctness checks, and missing
measurements. Do not claim production capacity from a local microbenchmark.
