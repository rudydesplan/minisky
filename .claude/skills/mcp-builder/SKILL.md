---
name: mcp-builder
description: Design, implement, or review Model Context Protocol (MCP) servers and their tools, resources, prompts, transports, schemas, safety annotations, and tests. Use when a user explicitly wants an MCP integration or server. Do not apply this workflow to ordinary MiniSky GCP shim or REST API work merely because it exposes tools or APIs.
license: Apache-2.0; upstream terms at https://github.com/anthropics/skills/blob/main/skills/mcp-builder/LICENSE.txt
---

# MCP Server Development Guide

Build MCP servers around real user workflows with narrow schemas, bounded
results, clear side effects, and actionable errors.

> Adapted for MiniSky from Anthropic's `mcp-builder` skill:
> https://github.com/anthropics/skills/tree/main/skills/mcp-builder

## Scope and project fit

- MiniSky is a Go application that emulates GCP APIs. A new service shim under
  `pkg/shims/` is not automatically an MCP server; use the shim-building skills
  for that work.
- Use this skill only for an explicit MCP surface, adapter, or standalone MCP
  project.
- Do not place generated TypeScript or Python packages in the MiniSky repository
  unless the user chooses the location and integration design.
- This local skill contains no bundled `reference/` or `scripts/` files. Do not
  rely on paths from the upstream distribution.

## 1. Confirm requirements

Establish:

- users and client hosts;
- external service and highest-value workflows;
- local stdio or remote HTTP deployment;
- authentication and secret storage;
- read, write, destructive, and open-world operations;
- expected result volume, pagination, and latency;
- target language and existing project constraints.

Ask only questions whose answers change the design.

## 2. Load current documentation

MCP and SDK APIs evolve. Before generating implementation code:

1. Resolve the relevant MCP SDK with Context7 and query current documentation
   for the exact transport, registration, schema, and response APIs needed.
2. Consult the current MCP specification for protocol behavior not covered by
   the SDK docs.
3. Read the target service's current API documentation.

Do not copy SDK calls from this skill or assume upstream `main` examples match
the installed version. Prefer stable releases over draft protocol features
unless the user explicitly targets a draft.

## 3. Design the surface

Choose the smallest surface that supports the requested workflows.

### Tools

- Use action-oriented, unambiguous names.
- Make descriptions state behavior, side effects, and important prerequisites.
- Keep inputs typed, constrained, and documented with examples only where useful.
- Prefer explicit pagination, filtering, and field selection over unbounded
  responses.
- Return structured content when supported by the negotiated SDK and client;
  provide a concise text representation when compatibility requires it.
- Avoid mirroring every REST endpoint when a smaller composable set covers the
  user workflows. Do not hide essential control inside one opaque mega-tool.

### Resources and prompts

Use resources for addressable, primarily read-oriented context. Use prompts for
reusable user-invoked templates. Do not model every capability as a tool by
default.

### Annotations and confirmation

Set available safety annotations accurately, including read-only, destructive,
idempotent, and open-world hints. Treat annotations as guidance, not access
control. Enforce authorization server-side.

For consequential writes:

- expose the intended target and change in arguments or a preview;
- reject ambiguous scope;
- support idempotency where the backing API permits it;
- require client/user confirmation rather than claiming annotations provide it.

## 4. Choose transport and runtime

- Use stdio for a local child process when appropriate. Keep protocol output on
  stdout clean; send diagnostics to stderr.
- Use the current specification's recommended HTTP transport for remote
  deployments, with authentication, origin validation, request limits, and
  deployment-aware session behavior.
- Choose TypeScript, Python, Go, or another supported SDK based on the target
  project. Do not force TypeScript into MiniSky solely because upstream examples
  use it.

## 5. Implement safely

Separate:

1. SDK registration and transport;
2. service client and authentication;
3. validation and domain operations;
4. response shaping and pagination;
5. error translation and observability.

Security requirements:

- read secrets from approved environment or secret stores; never return or log
  them;
- validate all external inputs at the boundary;
- enforce authorization independently of model-provided arguments;
- bound timeouts, retries, payloads, and pagination;
- sanitize errors while retaining a useful recovery action;
- protect against SSRF and arbitrary path/command execution;
- use least-privilege credentials;
- never execute user-provided shell fragments as an implementation shortcut.

Return errors that identify the failed operation, whether retry is reasonable,
and which input or prerequisite the caller should correct. Do not leak tokens,
raw internal exceptions, or sensitive response bodies.

## 6. Test

Test at three levels:

1. Unit-test validation, response shaping, and error mapping.
2. Integration-test tools against a fake, sandbox, or approved development
   service.
3. Exercise the built server through an MCP client or current Inspector.

Cover:

- initialization and capability negotiation;
- valid calls and structured outputs;
- invalid inputs and actionable errors;
- auth failure and upstream timeout;
- pagination and result-size limits;
- read-only and mutating operations;
- cancellation and cleanup where supported;
- clean stdio output or HTTP security behavior.

Use package scripts and SDK commands that actually exist in the generated
project. If this work intentionally adds Go code to MiniSky, also run:

```bash
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
```

Do not install or run the Inspector through an unpinned network download without
checking current documentation and obtaining approval when dependency changes
are outside scope.

## 7. Evaluate usability

Create a small set of realistic tasks that represent how an agent will use the
server. Prefer stable, independently verifiable, read-only scenarios for
automated evaluations. Add approved write scenarios only in an isolated
environment.

Evaluate whether:

- the correct tool is discoverable from its name and description;
- required arguments are inferable;
- responses contain enough information without flooding context;
- errors lead to recovery;
- multi-step workflows compose cleanly;
- prohibited or ambiguous mutations fail safely.

Do not force a fixed count, XML format, or benchmark harness unless the target
project already requires one.

## 8. Deliver

Document:

- runtime and installation;
- required environment variables without secret values;
- client configuration for the chosen transport;
- available tools/resources/prompts and side effects;
- local test commands;
- security boundaries and known limitations.

Report the files changed, documentation sources and versions consulted, tests
run, and any behavior not verified against a real client or service.
