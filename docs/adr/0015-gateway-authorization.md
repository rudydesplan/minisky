# ADR 0015: Gateway authorization placement

## Status

Accepted for the Phase 13 vertical slice.

## Decision

Strict-mode bearer verification and the bounded route-to-permission mapping run
in the gateway after route resolution and validation but before dispatch.
Current IAM policies remain the authorization source. Only a verified local
principal is propagated to shims. Permissive mode bypasses this gate.

Independent token parsing in each shim was rejected because it duplicates
security-sensitive behavior and produces inconsistent failures.

## Consequences

Strict mode has consistent Google-shaped `401` and `403` responses and optional
project validation. The permission map intentionally covers only tested
methods; authenticated unmapped methods are not described as complete GCP IAM
enforcement. Strict mode remains opt-in for compatibility.
