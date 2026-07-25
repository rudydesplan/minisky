---
name: gcp-api-fidelity
description: Implement or review MiniSky HTTP behavior against a specific Google Cloud REST contract, including status codes, google.rpc.Status-style errors, method/path schemas, list shapes, long-running operation polling, and explicit unsupported semantics. Use when Terraform, gcloud, or a Google client rejects a response; when adding validation or LROs; or when matching a service’s request/response fields. Do not use to claim broad GCP parity from metadata-only behavior.
---

# GCP API Fidelity

Treat fidelity as service- and method-specific. Verify the current GCP REST
reference or Discovery document, the client request being supported, and
MiniSky's existing behavior before editing. Google APIs differ in paths,
operation resources, list keys, delete responses, and error details.

## Define the contract before coding

Record the exact slice:

- API domain, version, method, path, and path/query identifiers;
- required and optional request fields and JSON types;
- success HTTP code and response/list shape;
- duplicate, missing, malformed, and precondition behavior;
- synchronous response versus service-specific operation;
- pagination parameter and token names;
- behavior that is metadata simulation, executable local behavior, or emulator
  passthrough.

Do not add headers, etags, pagination, intermediate resource states, or
`google.rpc` details merely because another GCP service uses them.

## Emit JSON errors, never plain text

MiniSky's shared baseline is an outer error envelope:

```json
{
  "error": {
    "code": 400,
    "message": "field 'name' is required",
    "status": "INVALID_ARGUMENT",
    "details": []
  }
}
```

Set `Content-Type: application/json` before `WriteHeader`, then encode the
envelope. Do not use `http.Error`. Keep HTTP status, numeric `error.code`, and
symbolic `error.status` coherent. Common mappings include:

- 400 `INVALID_ARGUMENT`;
- 401 `UNAUTHENTICATED`;
- 403 `PERMISSION_DENIED`;
- 404 `NOT_FOUND`;
- 409 `ALREADY_EXISTS` or service-specific conflict status;
- 412 `FAILED_PRECONDITION`;
- 429 `RESOURCE_EXHAUSTED`;
- 500 `INTERNAL`;
- 501 `UNIMPLEMENTED`;
- 503 `UNAVAILABLE`.

Use only mappings confirmed for the method. A 405 response for an unsupported
HTTP verb is different from a recognized but unimplemented operation.

The gateway validator currently emits one detail for request failures:

```json
{
  "@type": "type.googleapis.com/google.rpc.BadRequest",
  "message": "field 'name' is required"
}
```

That is the repository contract in `pkg/validator/contract.go`; it is not a
`fieldViolations` structure. Handler helpers vary by service, so improve them
only within the requested service scope and test the exact chosen shape.

## Preserve unsupported semantics

Never return 2xx, an empty object, or a completed operation for behavior MiniSky
does not implement.

Classify the request:

- unregistered domain or unknown canonical service selector: router returns
  HTTP 501 `UNIMPLEMENTED`;
- recognized MiniSky stub with no implementation: return HTTP 501 in a GCP
  envelope;
- unsupported verb on a known resource: return the real service behavior,
  commonly 405;
- syntactically valid path for a missing resource: 404 `NOT_FOUND`;
- lazy Docker cold-start failure: 503, not 501.

The executable registry baseline uses
`registry.UnsupportedContractPath`
(`/__minisky_contract__/unsupported`). `registry.ContractHandler` intercepts it
for `ProbeUnsupported` services and returns 501 with the domain in the message.
This reserved probe proves the boundary but does not replace tests for real
unimplemented methods.

## Match MiniSky routing

`pkg/router.ProxyRouter` normalizes the host and supports canonical local URLs:

```text
/_minisky/<first-domain-label>/<service-path>
/_minisky/<full-domain>/<service-path>
```

It rewrites the path, resolves the domain, then validates before dispatch.
Ambiguous first-label aliases are disabled; full domains remain valid. Legacy
bare paths are recognized only for explicitly coded service families. Test a
request through the router when Terraform/client endpoint compatibility is part
of the claim.

## Add validator rules precisely

MiniSky embeds a targeted subset of Discovery-derived mutation rules in
`pkg/validator/discovery.go`:

```go
{
	Domain: "myservice.googleapis.com",
	Methods: []MethodSchema{{
		HTTPMethod:    "POST",
		PathGlob:      "/v1/projects/*/locations/*/resources",
		ContentType:   "application/json",
		RequiredQuery: []string{"resourceId"},
		RequiredBody: []BodyField{
			{Path: "config.name", Type: "string", Message: "field 'config.name' is required"},
		},
	}},
}
```

Use the exact field names: `HTTPMethod`, `PathGlob`, `RequiredQuery`,
`RequiredBody`, `BodyField.Path`, `BodyField.Type`, and `BodyField.Message`.
A `*` matches one non-empty path segment. Paths are not version-normalized.
Nested body fields use dot notation. Types are `string`, `integer`, `boolean`,
`object`, and `array`.

Rules are allow-by-default: an unlisted domain or unmatched method/path passes.
Therefore, a validator rule proves only the listed mutation requirements, not
full schema conformance. Keep service-level semantic validation in the handler.

The validator reads and restores request bodies. It accepts content-type
parameters by prefix match. Current content-type failures use HTTP 415 with
symbolic status `INVALID_ARGUMENT`; test that existing contract unless the task
explicitly changes it.

## Use MiniSky's actual operation model

`pkg/orchestrator.OperationManager` stores in-memory operations with:

- `id`, `name`, `kind`, `operationType`;
- `status`: `PENDING`, `RUNNING`, or `DONE`;
- `progress` and `done`;
- timestamps, target link, zone/region, optional metadata;
- optional `{code,message}` error.

Register and start work through the injected manager:

```go
op := api.opMgr.Register("myservice#operation", "CREATE", targetLink, zone, region)
api.opMgr.RunAsync(op.Name, func() error {
	return api.provision(...)
})
_ = json.NewEncoder(w).Encode(op)
```

`RunAsync` deliberately makes `PENDING` and `RUNNING` observable, then sets
`DONE`, `done=true`, and progress 100. A work error calls `Fail` with code 500.
The manager does not persist operations and does not add a generic `response`
field. Its map access is synchronized, but `Register`, `Get`, and `List` expose
live operation pointers. Do not read those pointers concurrently as if they
were immutable snapshots; add a copy or synchronization boundary when needed.

Expose the operation through the exact service polling path and use
`api.opMgr.Get(opName)`. Return a GCP-shaped 404 for an unknown name. Do not
invent `operations/<id>` naming or a universal Google LRO shape when the
service's REST API uses zonal, regional, or service-specific operations.

Resource state transitions are separate from operation status. Add states such
as `PROVISIONING`, `RUNNABLE`, or `ACTIVE` only when the specific resource
contract and implementation require them.

## Match collections and pagination truthfully

Use the service's actual collection key (`items`, `managedZones`, `instances`,
and so on), kind fields, omitted/empty collection behavior, and page parameter
names. Return `nextPageToken` only when pagination is implemented or the real
response contract requires an empty token. Never advertise pagination while
ignoring tokens in a way that causes client loops or data loss.

## Prove fidelity test-first

For one method at a time:

1. Add a failing test for the exact request and response.
2. Add malformed, missing, duplicate, and unsupported cases that matter.
3. For async methods, poll through initial, running, terminal success, terminal
   error, and unknown-operation cases.
4. Exercise the public router when endpoint rewriting or validation matters.
5. Run focused tests with `-race`, then relevant shared packages.

```bash
go test -race ./pkg/shims/<service>
go test -race ./pkg/validator ./pkg/router ./pkg/registry
```

Full repository validation depends on CGO because MiniSky imports
`go-duckdb`; CI/releases use `CGO_ENABLED=1`, Clang on Darwin, and GCC/UCRT on
Windows. Executable Docker, Kind, Buildpacks, or Terraform claims require those
environments. Report what was not run and qualify the resulting fidelity claim.
