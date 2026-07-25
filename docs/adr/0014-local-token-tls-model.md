# ADR 0014: Local token and TLS model

## Status

Accepted for the Phase 13 vertical slice.

## Decision

HTTP remains the default. Optional TLS uses user material or a profile-local
self-signed ECDSA certificate. Optional client-CA verification applies only to
the gateway. Metadata, IAM Credentials, and STS share profile-local
HMAC-authenticated tokens with a one-hour maximum. Private material is `0600`
and excluded from portable metadata snapshots.

Unsigned JWT-shaped values were rejected because they cannot exercise tamper,
expiry, audience, or scope failures.

## Consequences

The behavior is cryptographically real on the local machine but does not use
Google trust roots or provide production key custody. Generated certificates
require explicit client trust. Disabling TLS and strict IAM restores the
existing HTTP and permissive defaults.
