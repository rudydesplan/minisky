---
name: security-review
description: Security review guidance for MiniSky API handlers, Docker-socket access, profile state, secret/KMS shims, imports, Terraform credentials, logs, dashboard, and dependencies. Use for sensitive changes or explicit security review; not as proof of production GCP security or compliance.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# Security Review for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability and Boundary

Use for code that accepts untrusted HTTP/JSON/files/archives, handles secrets or
key material, invokes Docker/Kind/Buildpacks, writes profile state, proxies to
backends, emits logs, or changes Terraform credentials. This is engineering
review, not compliance certification or a claim that MiniSky enforces GCP IAM.
Current emulation may be permissive or experimental; report that honestly.
Do not use this skill as evidence of production GCP security or compliance.

## Review Priorities

### Input and API contracts

- Bound request/body sizes and validate methods, paths, content type, fields,
  enum values, identifiers, URLs, and update masks before side effects.
- Prevent path traversal and SSRF in imports, source paths, proxy targets,
  callbacks, task URLs, and build contexts.
- Return GCP-shaped errors without Go errors, host paths, backend topology, or
  secret values. Unsupported behavior must be explicit, not successful.

### State and secrets

- Profile state and exports may contain Secret Manager values, KMS key bytes,
  source, and environment variables. Preserve `0700` directories, `0600` files,
  atomic writes, and profile isolation.
- Reject symlink/path escapes and unknown state versions before mutation.
- Do not log request bodies, auth headers, service-account keys, kubeconfigs,
  environment values, Terraform state, or snapshot content.
- Do not claim local AES simulation provides Cloud KMS durability, HSM, IAM,
  audit, or key-management guarantees.

### Docker and subprocesses

- The Docker socket is effectively host-root-equivalent. Never expose it over
  the network or pass untrusted arguments through a shell.
- Use argument arrays, pinned/checksummed tools/images, bounded contexts, and
  ownership checks before deleting containers/networks/volumes/Kind clusters.
- Buildpacks and user code are untrusted execution. Document isolation limits;
  avoid host mounts, credentials, and broad network access.

### Terraform and imports

- Terraform state and generated service-account files are sensitive. Keep them
  ignored, never print them, and never run acceptance tests against existing
  state or cloud credentials.
- Preserve the integration script's explicit opt-in, collision checks, temporary
  HOME/TF state, lock, and destroy cleanup.

### Dashboard and browser surface

- Validate and authorize mutating dashboard actions according to the actual
  threat model; do not assume localhost is always trusted.
- Avoid unsafe HTML, permissive CORS, secret-bearing URLs, and credential data in
  WebSocket messages. Add CSP/auth/rate limits only where requirements support
  them; do not claim controls that are absent.

## Verification

Review diffs and nearby call paths, then run relevant tests. Add negative tests
for malformed/oversized input, traversal, SSRF allow/deny behavior, secret
redaction, ownership checks, corrupt snapshots, and unsupported routes. Run:

```bash
go vet ./cmd/... ./pkg/... ./ui
go test -race ./cmd/... ./pkg/... ./ui
cd ui && npm audit --audit-level=high
```

Dependency audit output is evidence, not an automatic remediation command. Do
not run broad auto-fixes or external scanners without authorization. Report
findings by severity with file/evidence, exploit preconditions, impact, and a
minimal fix; explicitly list unverified boundaries.
