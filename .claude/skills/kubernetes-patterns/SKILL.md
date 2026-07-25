---
name: kubernetes-patterns
description: Kubernetes guidance for MiniSky's optional Kind-backed GKE emulation and any explicitly requested manifests. Use for Kind lifecycle, kubeconfig, Kubernetes YAML, RBAC, probes, or cluster debugging; not for assuming Kubernetes is required to run or deploy MiniSky.
license: MIT; see ../THIRD_PARTY_NOTICES.md
metadata:
  origin: ECC
---

# Kubernetes and Kind Patterns for MiniSky

Adapted from `affaan-m/everything-claude-code` (ECC). Preserve upstream license
and attribution when redistributing this material.

## Applicability

MiniSky does not require Kubernetes for its default simulation profile. The GKE
shim can use Kind only when `MINISKY_GKE_BACKEND=kind` is selected (or the full
profile requests it) and both `kind` and Docker are available. Use this skill
for that optional backend or for user-requested Kubernetes manifests. Do not
turn Kind availability into a global build/test requirement or claim GKE parity.

## Kind Backend Rules

- Detect dependencies before provisioning and expose the effective backend plus
  diagnostic consistently in constructors, doctor, and dashboard state.
- Use unique, profile/resource-derived cluster names; never adopt or delete an
  unrelated Kind cluster.
- Treat kubeconfig paths, Docker container IDs, IPs, and ports as ephemeral.
- Persist control-plane metadata only through the state adapter. On rehydrate,
  do not silently recreate missing Kind clusters; report metadata-only state
  until explicit reconciliation.
- Bound create/delete operations with context and surface partial cleanup.
- Avoid fixed `/tmp` filenames; use secure temporary files with restrictive
  permissions and cleanup.
- Never log kubeconfig credentials or embed them in exported state without an
  explicit security design.

## Manifest Guidance

When manifests are actually in scope:

- Pin images by immutable tag/digest; avoid `latest`.
- Run non-root where the image supports it, disable privilege escalation, and
  drop capabilities unless justified.
- Set resource requests based on measurements; set limits deliberately rather
  than copying universal values.
- Use startup, readiness, and liveness probes only when corresponding endpoints
  have verified semantics. A liveness probe must not depend on optional
  downstream services.
- Disable service-account token automount unless Kubernetes API access is
  required; grant namespace-scoped least privilege.
- Kubernetes Secrets are base64, not encryption. Do not commit plaintext secret
  manifests.
- HPA requires resource requests and a working metrics source; PDBs protect only
  against voluntary disruptions and can block maintenance if misconfigured.

## Safe Debugging

Start read-only:

```bash
kind get clusters
kubectl cluster-info --context <expected-context>
kubectl get pods -A
kubectl describe pod <pod> -n <namespace>
kubectl logs <pod> -n <namespace> --previous
```

Before delete, apply, rollout undo, or scale, verify the context and resource
ownership. Never run broad cluster mutations against an unspecified current
context.

## Verification

Unit-test backend selection and missing-dependency fallback without requiring
Kind. Run Kind integration only when explicitly enabled in an isolated Docker
environment. Verify create → observe → delete, cleanup after partial failure,
restart metadata behavior, and no collision with pre-existing clusters. Label
all unrun Kind checks as optional, not passed.
