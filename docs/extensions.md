# Runtime extensions

Kuasar's conductor supports statically linked runtime extensions for
deployments that need process-local integration without carrying a long-lived
fork. An extension is trusted code compiled into `xconductor`. It shares the
core process address space, UID, lifetime, filesystem, and network capabilities.
The API organizes ownership and concurrency; it is not a security sandbox.

There is one extension object per conductor process. The framework does not
discover or load plugins at runtime, maintain an extension registry, assign
priorities, provide a dependency-injection container, or reserve URL
namespaces. A private project can compose any modules it needs behind its one
object.

## Conductor object sources

Set `conductor.Runtime.Extension` during the startup `Configure` hook. The
object implements the stable base contract:

```go
type Extension interface {
    Start(context.Context, Host) error
}

type Host interface {
    Sandboxes() SandboxSource
    Builds() BuildSource
}
```

`Start` is called exactly once after the store, launcher, and conductor core
exist, but before systemd units are installed, durable state is reconciled,
pools or node-link start, or an external listener is exposed. A returned error
aborts startup and closes the resources already created. Cancellation of the
provided context tells extension-owned goroutines to exit. `Start` should not
wait for those long-running goroutines.

`SandboxSource` and `BuildSource` each provide a point `Get` and a callback
`Watch`. Returned views are independent deep copies. They contain rich durable
and runtime state plus irreversible credential fingerprints, while omitting raw
credentials, access tokens, sandbox environment values, MMDS secret values, and
build cleanup/runtime-preparation internals. This minimizes routine event data
and accidental logging; it does not restrict what trusted in-process code could
otherwise access.

`SandboxSource.Watch` covers all durable sandbox rows. `BuildSource.Watch`
covers the current `registered`, `waiting`, `building`, and `ready` set. A live
transition to build `error` produces `BuildRemove` with the final view and
reason. That removal is not durable: after a resync the failed build is simply
absent from the current set, while `BuildSource.Get` can still read its durable
error row.

### Watch generations

A Watch is an eventually convergent state stream, not a durable event log:

1. `sync_begin` starts a generation.
2. Snapshot `upsert` events enumerate durable rows in stable order.
3. `sync_end` marks that generation as a complete snapshot.
4. Subsequent live object events are delivered in publication order.

The core subscribes before querying the SQLite snapshot, so a mutation cannot
fall permanently into the snapshot/live boundary. Each subscriber has its own
bounded queue. If it falls behind, its queue overflows, or its source generation
becomes invalid, only that subscriber's generation is abandoned and a new
`sync_begin` plus full snapshot starts automatically. A generation without
`sync_end` is incomplete and must never replace authoritative consumer state.

Consumers should replace local state with the newest complete generation and
then apply its live events. Duplicates are allowed. Intermediate changes are not
guaranteed to be observed, but a later full resync converges to durable state.
Callbacks run serially in the goroutine that called `Watch`, never on a core
commit thread. Returning an error stops the Watch and returns that error;
context cancellation returns `ctx.Err()`.

Do not use Watch as an audit, billing, or exactly-once delivery mechanism. Those
requirements need a separately designed durable outbox.

## Boundaries

A nil extension preserves the built-in startup and request behavior: no event
hub or watcher goroutine is created. Cluster router, registry, and placer do not
have extension objects; node-local observation does not change node-link ACK,
exact replay, idempotency, or stable SandboxID to NodeSandboxID authority.

This API has no namespace, fixed extension URI, dynamic loading, hot reload, or
component compatibility version. WebSocket transport is outside Issue #256 and
is tracked independently by Issue #269.

See [`examples/custom-conductor`](../examples/custom-conductor) for a complete,
buildable program.
