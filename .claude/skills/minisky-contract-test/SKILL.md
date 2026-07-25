---
name: minisky-contract-test
description: Write or repair MiniSky service contract tests for registered domains, gateway routing, mutation validation, GCP-shaped errors, CRUD/LRO lifecycles, and explicit unsupported behavior. Use for requests such as “add a Phase 6 validator case,” “prove this shim works through the gateway,” “test create/get/delete,” or “ensure stubs return UNIMPLEMENTED.” Do not use for implementation without a testing request or for generic Go unit-test advice.
---

# MiniSky Contract Tests

Test the contract MiniSky actually exposes. Do not require every service to use
the same status code, resource shape, or synchronous lifecycle; derive those
from the real GCP API and the supported client workflow.

## Select the correct test layer

- `pkg/shims/<service>/*_test.go`: handler parsing, resource semantics, exact
  JSON, LRO polling, concurrency, persistence, and backend behavior.
- `pkg/validator/phase6_test.go`: table-driven mutation rules from
  `embeddedRules`.
- `pkg/validator/contract_test.go`: glob matching, body restoration, field
  types, and validator error envelopes.
- `pkg/router/proxy_test.go`: host/canonical endpoint resolution, path rewrite,
  resolved-domain validation, aliases, and unknown-domain behavior.
- `pkg/registry/manifest_test.go`: registration/manifest/docs coherence and the
  deterministic unsupported-route contract.

Prefer a public router test when the risk is integration between layers. A
direct handler test alone does not prove `/_minisky/<selector>/...` routing or
pre-dispatch validation.

## Use a red-green loop

For each behavior:

1. Write one focused test and run it.
2. Confirm it fails because the behavior is absent or wrong, not because the
   fixture is malformed.
3. Implement the minimum fix if implementation is in scope.
4. Re-run the focused test, then its package with `-race`.
5. Add the next contract.

Do not write a large suite before observing the first useful failure.

## Test mutation validation exactly

`pkg/validator/discovery.go` defines `embeddedRules []ServiceSchema`. A rule is:

```go
{
	Domain: "myservice.googleapis.com",
	Methods: []MethodSchema{{
		HTTPMethod:  http.MethodPost,
		PathGlob:    "/v1/projects/*/locations/*/resources",
		ContentType: "application/json",
		RequiredQuery: []string{"resourceId"},
		RequiredBody: []BodyField{
			{Path: "config.name", Type: "string", Message: "field 'config.name' is required"},
		},
	}},
}
```

Use `NewValidator().ValidateRequestForDomain(rec, req, domain)`. The validator:

- matches the first exact method and path glob;
- treats `*` as one non-empty segment and does not strip version prefixes;
- accepts `application/json` parameters via prefix matching;
- restores a consumed JSON body for the downstream shim;
- validates required query values, nested dot-path fields, and the types
  `string`, `integer`, `boolean`, `object`, and `array`;
- passes requests with no domain rule or no matching method/path rule.

Each Phase 6 case should prove a valid request passes and a minimally invalid
request fails with the expected message. Also cover malformed JSON, wrong type,
missing content type, or missing query only when relevant to the new rule.

## Assert the current validator error shape

Validation failures set `Content-Type: application/json` and encode:

```json
{
  "error": {
    "code": 400,
    "message": "field 'name' is required",
    "status": "INVALID_ARGUMENT",
    "details": [{
      "@type": "type.googleapis.com/google.rpc.BadRequest",
      "message": "field 'name' is required"
    }]
  }
}
```

The content-type mismatch path currently uses HTTP 415 with
`status: "INVALID_ARGUMENT"`. Assert the repository contract unless the task is
explicitly changing it. For handler errors, verify the service-specific GCP
shape rather than assuming validator details are universal.

## Test supported lifecycle behavior

For every new resource slice, test the methods actually claimed:

1. create with valid identifiers and required fields;
2. get and decode the returned resource;
3. duplicate create if GCP defines `ALREADY_EXISTS`;
4. list shape and pagination fields if supported;
5. update semantics if supported;
6. delete using the real success response;
7. get after delete returns 404 `NOT_FOUND`.

Use exact GCP field names and JSON encodings, including string-encoded numeric
IDs where applicable. For asynchronous APIs, create/delete should return an
operation, not the final resource. Poll the service's operation route and assert
the current manager lifecycle:

`PENDING, done=false -> RUNNING -> DONE, done=true, progress=100`

Also test 404 for an unknown operation and a terminal operation error when the
work function fails. Avoid fixed sleeps where the manager can be polled with a
bounded deadline.

## Test unsupported behavior without fake success

There are two distinct contracts:

1. `registry.ContractHandler` intercepts exactly
   `registry.UnsupportedContractPath`
   (`/__minisky_contract__/unsupported`) for probeable in-process services and
   returns HTTP 501 with `code: 501`, `status: "UNIMPLEMENTED"`, and a message
   containing the domain.
2. A real recognized but unimplemented service method must return its explicit
   service-correct unsupported response. It must never return 2xx or fabricate
   a resource/operation.

`registry.ContractHandlers(opMgr, svcMgr)` skips services whose manifest has
`ProbeUnsupported == false`, because those require Docker. It also omits
`PostBoot`; contract fixtures must not depend on cross-shim wiring.

Do not confuse an unsupported method with:

- unknown resource: normally 404;
- unsupported HTTP verb on a supported resource: often 405;
- unknown router domain or canonical selector: router-level 501;
- lazy Docker startup failure: 503.

## Test registration and routing

Import `_ "minisky/pkg/shims"` in external registry tests so all package
`init` functions run. `registry.Services()` must report no missing or stale
manifest entries. Its metadata must use the current fidelity and persistence
enums, and every domain must appear in `docs/service-compatibility.md`.

Router tests should register a handler with `RegisterShim` and exercise either
the actual host or:

```text
http://localhost:8080/_minisky/<selector>/<service-path>
```

Assert path/query preservation and that validation uses the resolved service
domain. First-label aliases can be ambiguous; the full domain remains the
unambiguous selector.

## Test persistence and concurrency when claimed

For `file` or `hybrid` metadata:

- create `store, err := state.New(t.TempDir(), "restart")`;
- construct through an injectable `NewAPIWithStore`;
- mutate via HTTP, construct a fresh API, and read the resource;
- prove `state.ErrNotFound` starts empty;
- prove corrupt state returns an error and is not overwritten;
- run concurrent mutation/read coverage under `-race`;
- where persistence calls external code, prove the API mutex is not held while
  `Save` executes.

For Docker-backed metadata, verify rehydration does not silently recreate or
adopt containers unless that behavior is explicitly designed and tested.

## Run proportionate checks

```bash
go test -race ./pkg/shims/<service>
go test -race ./pkg/validator ./pkg/router ./pkg/registry
```

Run `go test -race ./...` when CGO is available. The repository imports
`go-duckdb`; CI and releases build with `CGO_ENABLED=1`, using Clang on Darwin
and GCC/UCRT on Windows. Docker, Kind, Buildpacks, and Terraform integration
tests require separate prerequisites. Report skipped checks and never infer
backend fidelity from metadata-only tests.
