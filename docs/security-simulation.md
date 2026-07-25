# Security simulation

MiniSky provides cryptographically real **local simulation** for transport and
short-lived credentials. These controls help exercise application behavior on a
developer machine. They are not Google Cloud IAM, a public certificate
authority, a security boundary against the host user, or a production identity
provider.

## Transport security

HTTP remains the default for compatibility. `minisky start --tls auto` creates a
self-signed ECDSA certificate and private key under the active profile's
`tls/` directory. Profile directories are `0700`; generated private material is
`0600`. The certificate is valid for `localhost`, `127.0.0.1`, and `::1`.

For user-managed material, use:

```text
minisky start --tls files --tls-cert ./server.pem --tls-key ./server-key.pem
```

The private key must not be accessible by group or other users. Invalid,
incomplete, or unreadable TLS configuration stops startup. `minisky doctor`
checks the same configuration.

TLS applies to the gateway and loopback dashboard. `--tls-client-ca ca.pem`
additionally requires and verifies client certificates **only on the public
gateway listener**; the dashboard does not require a client certificate. This
is local mTLS transport authentication. Certificate subjects are not mapped to
GCP IAM principals.

## Local credentials

Metadata and IAM Credentials issue bounded, HMAC-SHA256-authenticated local
tokens. The signing secret is generated under the active profile's `security/`
directory with `0600` permissions and is excluded from metadata export/import.
Tokens carry:

- service-account or local principal identity;
- issue and expiry times (at most one hour);
- audience;
- explicit OAuth scopes;
- a random token identifier.

Strict IAM mode validates token signature, expiry, configured audience, and a
bounded service scope before route authorization. Authentication failures are
Google-shaped `401 UNAUTHENTICATED`; authenticated permission failures are
`403 PERMISSION_DENIED`. Responses and request logs never include token
material.

`iamcredentials.googleapis.com` supports direct
`projects/-/serviceAccounts/{email}:generateAccessToken`. Delegation chains are
explicitly `501 UNIMPLEMENTED`. The STS endpoint accepts only MiniSky local
access tokens; JWT, SAML, AWS, OIDC provider discovery, and arbitrary workforce
or workload identity provider flows return `501 UNIMPLEMENTED`.

Service-account key metadata records disable, delete, and expiry lifecycle.
Keys and local tokens are not Google credentials and cannot authenticate to
Google Cloud.

## Authorization and projects

Permissive mode remains the default. In strict mode, the gateway authenticates
once and applies a bounded route-to-permission map using current IAM policies.
Project, folder, and organization policies can be consulted through the local
Resource Manager hierarchy; nested child resources normalize to their project
ancestor for inherited role lookup. Unmapped mutating routes fail closed.
Unmapped reads are authenticated but are not claimed as complete GCP IAM
enforcement.

Dashboard APIs derive authorization targets from canonical resource paths or
JSON bodies and reject conflicting query targets. In strict mode, the UI uses
an explicit tab-scoped bearer-token session. Interactive container terminals
require a separate admin-only permission, same-origin WebSocket handshakes, and
an active-profile ownership check before Docker exec.

Optional project-existence enforcement rejects requests naming an unknown
project. In-process metadata shims that key resources by project are isolated.
Docker passthrough emulators still share one profile-local backend unless their
upstream emulator provides project isolation. MiniSky does not claim process,
kernel, network, or storage isolation between projects.

## Unsupported production boundaries

MiniSky does not provide:

- Google trust roots, OAuth authorization servers, token introspection, or
  revocation propagation;
- HSM-backed key custody, managed key rotation, audit-log guarantees, VPC
  Service Controls, Access Context Manager, or organization-policy parity;
- production-grade tenant isolation or protection from a user who can read the
  profile, control the process, or access the Docker socket;
- Shared VPC packet routing, Google front-end behavior, public DNS, certificate
  transparency, or managed certificate renewal;
- complete IAM permission, condition, deny-policy, principal-set, WIF, or STS
  provider semantics.

Unsupported recognized flows fail explicitly instead of returning successful
placeholder credentials.
