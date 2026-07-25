---
name: event-driven-delivery
description: "Implement or fix delivery behavior between MiniSky services: Storage or Pub/Sub events to serverless handlers, Cloud Tasks HTTP execution and retries, or Cloud Scheduler HTTP/Pub/Sub/App Engine targets. Use for observer wiring, payload fidelity, scheduling, cancellation, backoff, delivery outcomes, and event lifecycle tests. Do not use for generic Go concurrency, standalone CRUD, or event systems not connected to these shims."
---

# Event-Driven Delivery

Extend the delivery paths that MiniSky actually has; do not replace them with a new event bus.

## Inspect the existing path first

Read the source and tests for every producer and consumer involved:

- `pkg/shims/storage/`
- `pkg/shims/pubsub/`
- `pkg/shims/serverless/`
- `pkg/shims/cloudtasks/`
- `pkg/shims/scheduler/`
- `pkg/registry/` for construction and `OnPostBoot` wiring
- the target shim's state adapter when delivery state persists

Current boundaries:

- Storage and Pub/Sub maintain observer lists and wire the serverless shim during `OnPostBoot`.
- Their observer callback is `HandleEvent(eventType, resource, payload string)`.
- Observer notification is currently synchronous; do not describe it as guaranteed non-blocking.
- Cloud Tasks owns cancellable background delivery jobs, bounded HTTP timeouts, bounded retries, and in-memory task outcome fields.
- Cloud Scheduler uses `robfig/cron/v3` with the default five-field parser, persists job metadata, and dispatches HTTP, Pub/Sub, or App Engine targets.
- There is no general durable event queue or dead-letter subsystem.

## Test-first workflow

1. Write a failing focused test around an observable contract.
2. Use injectable `httptest.Server`, HTTP clients, clocks, stores, and short retry intervals.
3. Implement the minimum behavior without `time.Sleep` in tests.
4. Run the focused test under the race detector.
5. Run producer, consumer, and registry integration tests.

```bash
gofmt -w <changed-go-files>
go test -race ./pkg/shims/<producer> ./pkg/shims/<consumer>
go test -race ./pkg/registry ./pkg/shims/serverless
```

## Storage and Pub/Sub observers

Preserve these properties:

- Snapshot observers under a read lock, then invoke callbacks without holding the producer lock.
- Ignore nil observers and prevent duplicate registration.
- Preserve the original proxied request body after reading it.
- Emit the existing CloudEvents-style event type strings expected by serverless tests.
- Notify only for a successfully recognized producer operation; decide and test whether notification happens before or after upstream success.
- A slow or panicking observer must not corrupt producer state. If asynchronous delivery is introduced, define ownership, shutdown, backpressure, ordering, and test synchronization first.

Do not invent Storage event enums or Pub/Sub push-envelope support that the code does not expose. Add them only with API-fidelity tests and a real consumer path.

## Cloud Tasks delivery

Keep delivery jobs tied to the API lifetime:

- Derive request context from the API context.
- Cancel jobs when their task or queue is deleted and in `Close`.
- Pair every started goroutine with `WaitGroup` accounting.
- Drain and close every HTTP response body.
- Bound client timeouts, attempts, backoff, and retry duration.
- Record each attempt and a deterministic terminal state under the API lock.
- Use timers selectable on cancellation, not unconditional sleeps.

Current Task bodies are base64 strings. Invalid base64 must fail deterministically without issuing a request.

Treat 2xx as success. Before changing retry classification for 3xx/4xx/5xx, verify Cloud Tasks semantics and encode the decision in tests. The current client follows redirects unless configured otherwise, and the current implementation does not inject Cloud Tasks headers; do not claim either behavior is implemented.

Validate outbound targets before expanding delivery:

- allow only intended HTTP(S) schemes;
- reject malformed URLs and embedded credentials;
- prevent uncontrolled access to host metadata, loopback, Unix sockets, or private networks unless local-emulator behavior explicitly requires a narrowly scoped exception;
- cap request/response sizes and never log authorization headers or bodies containing secrets.

## Cloud Scheduler delivery

Use the current `Config` injection points for HTTP client, clock, gateway base URL, and store.

- Keep `ENABLED`, `PAUSED`, and deletion synchronized with cron entry creation/removal.
- Remove an old cron entry before rescheduling the same job.
- Treat the default cron parser as five-field. Do not add `WithSeconds` without an API decision and tests.
- Preserve the no-redirect client behavior.
- Require `MINISKY_GATEWAY_URL` or configured gateway base URL for Pub/Sub and App Engine targets.
- Record last-attempt time, HTTP status, error, and status object after every execution.
- Persist a cloned snapshot outside the write lock; never serialize mutable maps while another goroutine can edit them.
- Stop the cron scheduler in test cleanup and application shutdown.

Scheduled callbacks currently reference job pointers. Any update/delete work must prove with race tests that callbacks cannot mutate stale or concurrently edited state.

## Persistence and restart

Only claim durability where a shim has a state adapter. Scheduler metadata is persisted; Cloud Tasks delivery state currently is not.

When adding persistence:

1. Rehydrate resources before scheduling work.
2. Define what happens to due, running, retrying, canceled, and completed items after restart.
3. Avoid replaying terminal work.
4. Make schedule registration idempotent.
5. Return persistence failures instead of acknowledging writes that were not saved.

## Acceptance gates

Cover the behavior in scope:

- exact payload, method, path, and safe headers;
- fan-out once per distinct observer;
- first-attempt success;
- transient failure then success with deterministic attempt count;
- terminal failure at attempt or duration limit;
- malformed body/URL without outbound request;
- timeout and cancellation during request and backoff;
- delete/close leaves no live job or cron entry;
- paused jobs do not fire and resumed jobs fire once;
- non-2xx outcomes are recorded;
- restart behavior for persisted scheduler jobs;
- `go test -race` passes without leaked test servers or goroutines.

Report supported delivery guarantees honestly: ordering, durability, retry classification, and at-most/at-least-once behavior must come from tests, not assumptions.
