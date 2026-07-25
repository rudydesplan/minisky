---
name: api-design
description: GCP-compatible HTTP API design for MiniSky shims, gateway routes, validation, pagination, errors, unsupported methods, and long-running operations. Use when adding or reviewing service endpoints; not for inventing generic REST envelopes or claiming full GCP parity.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# API Design for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

Use for public gateway, validator, router, dashboard API, and shim endpoint
contracts. For GCP endpoints, Google Discovery documents and observed provider/
SDK behavior outrank generic REST conventions. Do not force `/api/v1`, plural
kebab-case resources, a `{data: ...}` envelope, authentication, rate limiting,
or cursor pagination when the emulated API specifies otherwise.

## Contract Sources

Check, in order:

1. the service's Discovery document/schema and canonical GCP resource names;
2. existing MiniSky routing and service registrations;
3. Terraform provider/SDK request behavior covered by tests;
4. nearby shim conventions and documented fidelity tier.

Keep host-based and explicit localhost path routes unambiguous. A dashboard-only
route is not automatically a public GCP endpoint.

## Response Rules

- Match the GCP method, path, query names, JSON field names, status code, and
  response body. Omit fields only when the real API permits omission.
- Validate content type, required query/body fields, enum/type constraints, and
  resource-parent consistency before mutation.
- Return `Content-Type: application/json` for JSON responses.
- Errors use a Google-style envelope:

```json
{
  "error": {
    "code": 400,
    "message": "Field 'name' is required.",
    "status": "INVALID_ARGUMENT",
    "details": []
  }
}
```

- Use canonical statuses such as `INVALID_ARGUMENT`, `NOT_FOUND`,
  `ALREADY_EXISTS`, `FAILED_PRECONDITION`, and `UNIMPLEMENTED` as applicable.
- Unsupported operations return an explicit unsupported response (the contract
  gate expects HTTP 501 with `UNIMPLEMENTED`); never return accepted fake work.
- Do not leak Go errors, filesystem paths, Docker details, secrets, or stack
  traces in client messages.

## Long-Running Operations

When the GCP method is asynchronous, return a correctly shaped operation and
model observable state transitions. Preserve operation identity. Terminal
success and error are mutually exclusive. A restart must not replay completed
side effects; if durable operation recovery is not implemented, document and
honestly test that limitation.

## Lists and Updates

- Follow service-defined `pageSize`, `pageToken`, ordering, filtering, update
  masks, and delete semantics; do not substitute a generic pagination scheme.
- Generate opaque, stable page tokens only if the service supports pagination.
- Reject malformed tokens rather than silently starting over.
- PATCH/UPDATE must honor the API's update mask and immutable fields.
- DELETE should be idempotent only if the upstream contract is idempotent.

## Fidelity Discipline

Distinguish executable, emulator-backed, metadata-only, and experimental paths.
A stored resource or successful HTTP response is not proof of workload
execution, IAM enforcement, durability, or full API support. Update compatibility
claims only when tests support them.

## Verification

Add focused handler tests for success, malformed input, not found/conflict,
unsupported behavior, and exact error fields. For registered domains, use the
contract-test guidance. If Terraform-facing, verify the isolated apply → read →
no-drift plan → destroy workflow; never use a developer's existing state.
