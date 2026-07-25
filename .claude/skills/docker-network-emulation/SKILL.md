---
name: docker-network-emulation
description: "Implement or repair MiniSky networking behavior backed by the Docker Engine API: VPC bridge lifecycle, compute container attachment, localhost port publication, firewall enforcement, Docker socket portability, and owned-resource cleanup. Also use when connecting existing Compute load-balancer or DNS control-plane behavior to a Docker data plane. Do not use for ordinary Dockerfiles, generic container deployment, or metadata-only networking CRUD."
---

# Docker Network Emulation

Map only the required GCP behavior onto Docker, and state where Docker cannot provide equivalent semantics.

## Read the real implementation

Before editing, inspect:

- `pkg/orchestrator/manager.go` and OS-specific Docker dialers.
- `pkg/shims/compute/` resource handlers and tests.
- `pkg/shims/dns/` before claiming DNS affects container resolution.
- `pkg/config/` image registry and runtime-profile behavior.
- `Dockerfile`, CI native/container jobs, and teardown commands.

Current implementation facts:

- MiniSky talks directly to the Docker Engine HTTP API; it does not use a Go Docker SDK.
- Managed emulator containers use `minisky-net`; named VPCs use `minisky-vpc-<name>`.
- Published ports bind to `127.0.0.1` with Docker-assigned host ports for Docker Desktop portability.
- Firewall handling stores simplified rules and can recreate compute containers to change published ports.
- Compute load-balancer control-plane resources feed an in-process health-aware HTTP proxy; no Nginx/Envoy proxy container is provisioned.
- Cloud DNS currently provides API metadata/record behavior, not CoreDNS or `/etc/hosts` injection.
- VPC peering, Private Service Connect, Cloud NAT fidelity, and general iptables programming are not present.

Do not present absent data planes as implemented.

## Decide the emulation boundary

For each change, write down:

1. The GCP behavior Terraform/SDK callers can observe.
2. The Docker primitive used.
3. The unsupported semantics.
4. The host platforms to support: Linux Docker Engine, Docker Desktop on macOS, and Windows named pipes where relevant.
5. Which resources MiniSky owns and may delete.

Prefer control-plane fidelity plus a narrow data-plane path over a broad, inaccurate network simulation.

## Docker Engine API rules

- Create requests with caller contexts and bounded HTTP clients.
- Check every HTTP status, including conflict/not-found cases that are intentionally idempotent.
- Drain and close response bodies so transports can reuse connections.
- URL-escape container, network, image, and volume identifiers.
- Validate user-derived names before using them in query strings or Docker object names.
- Bind host ports to loopback by default. Expanding to `0.0.0.0` is a security-sensitive product decision.
- Never mount arbitrary host paths, the Docker socket, or privileged capabilities from untrusted API input.
- Label every created object with stable ownership and resource identity labels.
- Do not prune global Docker resources. Cleanup must select only objects proven to be owned by this MiniSky run/resource.

Docker socket access is host-level control. Keep it explicit, avoid logging socket paths as credentials, and do not imply that a container with the mounted socket is sandboxed from the host.

## VPC lifecycle

For create:

- Use a bridge network with an ownership label.
- Treat an existing owned network as idempotent.
- Do not accept an unowned same-name network silently.
- If custom IPAM is added, validate CIDRs and prove they do not collide with tracked MiniSky networks; Docker may still reject host conflicts.

For attach:

- Set the intended network at container creation or connect explicitly.
- Preserve the shared emulator network only when cross-service access requires it.
- Verify Docker Desktop reachability through published localhost ports rather than bridge IP assumptions.

For delete:

- Define behavior when endpoints remain attached.
- Disconnect/remove only owned endpoints.
- Accept not-found as already deleted.
- Verify the network is gone.

## Firewall emulation

Published host ports are not a complete GCP firewall model. They affect host ingress, not arbitrary east-west traffic, priorities, implied rules, target service accounts, or all egress.

When changing the current simplified model:

- Test rule precedence, direction, protocol, port ranges, source CIDRs, target tags, and default behavior that are actually supported.
- Parse CIDRs with `net/netip`; string equality is not CIDR membership.
- Make deny/allow precedence explicit and deterministic.
- Recreate a container only after snapshotting image, command, environment, networks, mounts, and published ports needed for faithful restoration.
- Do not destroy application data or change a random host port without updating observable state.
- Roll back or report a terminal error if replacement fails.

Do not claim "real isolation" from separate bridges alone. Host-published ports, multi-network containers, Docker daemon access, and host networking can bypass that boundary.

## Load balancing and DNS

Extend the existing in-process load-balancer path unless the requirement explicitly calls for a proxy container. Keep resource graph resolution, health checks, backend filtering, deterministic round-robin behavior, and 503 failures covered by `pkg/shims/compute` tests.

For DNS, distinguish:

- control plane: managed zones, record sets, and atomic changes;
- data plane: queries from host or containers.

Only add a DNS data plane with an explicit resolver architecture, network attachment, TTL/update semantics, startup readiness, and cleanup tests. Editing `/etc/hosts` cannot represent TTL, wildcard, SRV, or dynamic DNS behavior.

## Test-first workflow

Start with unit/handler tests using a fake Docker HTTP server. Assert method, path, encoded query, payload, labels, and status handling before touching a real daemon.

Then run:

```bash
gofmt -w <changed-go-files>
go test -race ./pkg/orchestrator ./pkg/shims/compute ./pkg/shims/dns
```

Run Docker integration tests only behind an explicit environment opt-in. Use a unique run label/name prefix and `t.Cleanup`; never reuse or remove a developer's existing `minisky-*` objects.

## Acceptance gates

- Focused tests fail before and pass after implementation.
- Concurrent creates/deletes and repeated cleanup are idempotent under `go test -race`.
- Failure injection covers daemon unavailable, timeout, malformed JSON, 409, 404, and 5xx.
- Published ports remain loopback-only unless explicitly approved.
- Network/firewall behavior is tested from the relevant vantage points: host, same VPC, and different VPC.
- Container recreation preserves required runtime configuration and data.
- Cleanup removes every object created by the test and no unowned object.
- Linux behavior passes; Docker Desktop and Windows limitations are documented when not exercised.

Report which semantics are metadata-only, in-process, or Docker-enforced.
