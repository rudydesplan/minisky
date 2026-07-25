---
name: minisky-state-persistence
description: Add or repair durable, profile-scoped MiniSky shim metadata using pkg/state, including injectable rehydration, atomic saves, corrupt-state handling, restart tests, export/import boundaries, and Docker reconciliation. Use for requests such as “persist this shim,” “survive restart,” “fix state import/export,” “add a profile state entry,” or “rehydrate metadata.” Do not use for generic database migrations or transient operation state.
---

# MiniSky State Persistence

Persist only the metadata a shim promises to restore. MiniSky state snapshots do
not include arbitrary files, DuckDB databases, Docker volumes, containers, or
networks.

## Use the actual store contract

`pkg/state.Store` is profile-scoped at:

```text
<root>/profiles/<profile>/state.json
```

Create it by handling both return values:

```go
store, err := state.New(config.GetStateDir(), config.GetProfile())
```

`registry.Context` does not contain a state store. It exposes only `OpMgr`,
`SvcMgr`, and `GetShim`.

The current API is:

```go
store.Save("myservice/metadata", value)
store.Load("myservice/metadata", &target)
store.Delete("myservice/metadata")
store.Export(writer)
store.Import(reader)
```

`Save` marshals `value` itself. `Load` wraps missing entries with
`state.ErrNotFound`; use `errors.Is`. `Delete` is idempotent. `Export` and
`Import` stream through `io.Writer`/`io.Reader`; they do not return or accept a
snapshot value directly.

Entry paths may contain safe slash-separated components. Use one stable
`<service>/metadata` key unless the service already has a different established
contract.

## Know the on-disk and portable formats

The private store document is:

```json
{"format":"minisky-state-store","version":1,"entries":{}}
```

The portable snapshot is:

```json
{"format":"minisky-state","version":1,"entries":{}}
```

The snapshot contains no `profile` or `timestamp` fields. Import rejects unknown
fields, trailing JSON, invalid entry names, invalid payload JSON, unsupported
versions, and inputs over the store limit. It validates the complete snapshot
before atomically replacing the active profile document.

Writes create profile directories with restrictive permissions, write and sync
a temporary file, rename it over `state.json`, and sync the directory. Do not
reimplement that sequence in a shim.

## Construct persistent shims for production and tests

Keep production construction convenient and test construction explicit:

```go
func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: My Service] state disabled: %v", err)
		return newAPI(nil)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		log.Printf("[Shim: My Service] state rehydration failed: %v", err)
		return newAPI(store)
	}
	return api
}

func NewAPIWithStore(store *state.Store) (*API, error) {
	api := newAPI(store)
	if store == nil {
		return api, nil
	}
	var saved metadata
	if err := store.Load(myServiceStateEntry, &saved); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load My Service metadata: %w", err)
	}
	api.restore(saved)
	return api, nil
}
```

A narrow `Load`/`Save` interface is also valid and helps locking tests. Preserve
the shim's established constructor signatures and dependencies. Do not silently
overwrite malformed or unsupported state during rehydration.

## Snapshot safely

Persist resource data, identifiers needed to rebuild private indexes, and
monotonic counters needed to avoid collisions. Exclude:

- mutexes, clients, managers, handlers, channels, and function values;
- operation-manager progress and other transient request state;
- reconstructable caches;
- secrets or binary data not already part of the shim's explicit state model;
- external container, volume, network, or DuckDB contents.

Use a serializing persistence mutex when concurrent mutations could save
snapshots out of order. Under the API read lock, copy or marshal a consistent
snapshot; release the API lock before calling `Store.Save`:

```go
func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	payload, err := json.Marshal(metadata{Resources: api.resources})
	api.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("snapshot My Service metadata: %w", err)
	}
	return api.stateStore.Save(myServiceStateEntry, json.RawMessage(payload))
}
```

If copying pointer-rich maps instead of marshaling under the lock, deep-copy
nested maps, slices, and pointed values. A shallow map copy still races.

## Make mutation and persistence failure semantics explicit

Persist after every successful state mutation, including update and delete.
Follow the existing service's error model. Current durable shims commonly turn
a save failure into HTTP 500 rather than acknowledge a mutation that will be
lost after restart. Set the response header before writing status and return a
GCP-shaped `INTERNAL` error.

Avoid holding the API mutex while filesystem or injected store code executes.
If a save fails after memory changed, document and test whether the shim keeps
the in-memory mutation, rolls it back, or becomes degraded. Do not claim
crash-safe resource semantics merely because the metadata file write is atomic.

## Rehydrate external backends truthfully

Loading metadata must not accidentally create or adopt external resources.
For Docker-backed Compute and Cloud SQL, the current pattern restores resources
as disclosed metadata-only/suspended state and clears stale ports/addresses.

Choose and test one explicit strategy:

- restore metadata only and mark the backend unavailable;
- reconcile against owned Docker resources and update status;
- recreate a backend only when the service contract explicitly promises it.

Check ownership before adopting or deleting containers, networks, or volumes.
Keep metadata export/import separate from external data backup.

## Work test-first

Add one failing test for each relevant guarantee:

1. missing entry starts empty;
2. create/update/delete survives construction of a fresh API;
3. corrupt or unsupported state returns an error and remains untouched;
4. nil maps/slices from older valid state are normalized;
5. save failure follows the documented HTTP/error behavior;
6. `Save` runs without the API mutex held;
7. concurrent mutation/persistence passes `-race`;
8. Docker-backed rehydration does not fabricate a running backend;
9. export/import round-trips metadata and excludes external data.

Use the exact constructor signature:

```go
store, err := state.New(t.TempDir(), "restart")
```

Run:

```bash
go test -race ./pkg/state ./pkg/shims/<service>
```

Run broader tests only when relevant. Full repository tests require the CGO
toolchain because MiniSky imports `go-duckdb`; Docker reconciliation tests also
require a reachable Docker daemon. Report unavailable checks rather than
weakening the persistence claim.
