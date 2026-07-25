# IAM shim modes

The IAM shim is permissive by default: `testIamPermissions` returns every
requested permission. This preserves the local-development behavior of earlier
MiniSky versions.

Set `MINISKY_IAM_MODE=strict` before starting MiniSky to evaluate
`testIamPermissions` against the policy stored for the requested resource. In
strict mode, each request must include:

```text
X-MiniSky-Principal: user:alice@example.com
```

The value must be a complete IAM member string and must exactly match a member
in a policy binding, for example `user:...`, `serviceAccount:...`, or
`group:...`. A missing or blank header receives a GCP-shaped
`PERMISSION_DENIED` response. A known caller without a requested permission
receives a successful response containing only the allowed subset, matching
`testIamPermissions` semantics.

Strict mode includes a deliberately small permission mapping for:

- `roles/storage.admin`, `roles/storage.objectAdmin`, and
  `roles/storage.objectViewer`
- `roles/compute.admin` and `roles/compute.viewer`
- `roles/pubsub.admin` and `roles/pubsub.viewer`

Local tests can grant one permission without adding a role mapping by using a
role named `permission:<permission>`, such as
`permission:pubsub.topics.publish`. This is a MiniSky-only convention, not a
Google Cloud role format.

Strict mode currently affects only the IAM shim's `testIamPermissions`
endpoint. Enforcement by Storage, Compute, Pub/Sub, or other shims is a
separate integration and is not implemented here.
