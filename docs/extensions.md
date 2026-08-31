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

After `Start` succeeds, the conductor checks the same object once for optional
`SandboxHook`, `BuildHook`, and `APIWrapper` capabilities. That result is frozen
for the process lifetime; these interfaces are not registrations and cannot be
installed, replaced, or reordered at runtime.

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

## Conductor lifecycle hooks

An extension may implement either or both admission callbacks over public,
operation-specific copies:

```go
type SandboxHook interface {
    PrepareSandbox(context.Context, *SandboxOperation) error
}

type BuildHook interface {
    PrepareBuild(context.Context, *BuildOperation) error
}
```

The operation is an independent mutable copy. Exactly one request field is
non-nil. `ID`, `Kind`, `Origin`, `SandboxID` or `BuildID`, and already allocated
protocol identities remain core-owned. The conductor rejects changes to that
envelope, then normalizes and validates every mutable field again. `Current`,
when present, is the same non-secret deep-copy projection returned by the object
source.

Sandbox operations are admitted as follows:

- `create`: after caller authentication, strict parsing, preliminary template
  resolution, metadata normalization, and ID allocation; before launch claims,
  durable `starting`, directories, network attachment, route publication, or
  runner assignment. A Hook may change the template reference, timeout,
  metadata, environment, secure flag, and supported request MMDS input. The
  final template is resolved again and must retain the core-owned profile.
- `pause`: after ownership, running incarnation, and checkpoint preconditions
  are captured; before snapshot or durable state changes. The Hook may change
  action-scoped merge-ref and drop-cache policy.
- `resume`: only for a real `paused → starting` admission shared by API Connect,
  proxy Wake, native exec, and canonical cluster commands. Running/starting
  idempotent joins do not invoke it. The Hook may change the requested deadline.
- `delete`: only ordinary explicit API or canonical cluster Delete. It can reject
  that request. Failed-create/failed-resume rollback, reconciliation, shutdown,
  recovery, and other mandatory cleanup never call the Hook and cannot be
  blocked by extension availability.

Build `register` runs after caller identity and IDs are resolved, but before the
registration capacity transaction. It may change names, aliases, profile, kind,
resources, metadata (including the routes-only MMDS declaration), and builder
options; `BuildID` and `TemplateID` remain core-owned. Raw MMDS secret values,
registry credentials, and pull tokens are not copied into the operation. The
core rebinds retained initial MMDS values to the final route declaration and
resolves registry credentials only after the Hook. A canonical cluster
BuildRegister runs the Hook only for new ownership. Exact replay of an existing
durable `BuildID` verifies the original tenant-keyed request identity and
credential fingerprints without invoking the mutable Hook again, so policy
changes cannot break an ACK-lost replay.

Build `trigger` runs after ownership/state checks and preliminary pure parsing,
but before source resolution, registry credential resolution, and the
`registered → waiting` CAS. The final `fromImage`/`fromTemplate`, steps,
commands, and resource assertion are validated again; credentials are resolved
from that final source. There is deliberately no Hook at the
`waiting → building` execution claim.

Callbacks never run while holding the sandbox lifecycle lock, inside a SQLite
transaction, or from a CAS callback. For sandbox pause/resume/delete and build
trigger, the core captures a precondition, unlocks, invokes the Hook, then
authoritatively re-reads under its mutation fence before committing. A stale
result is rejected and is never applied to a newer run, snapshot, ownership, or
Build state. Context deadlines are cooperative cancellation only: the core does
not abandon an in-process Go callback in a detached goroutine.

Returning an error matching `extension.ErrRejected` produces a fixed policy
rejection (`403` for the direct API and the corresponding rejected cluster ACK).
Any other error is logged with its private detail and exposed only as a fixed
temporary-unavailable `503`. Core validation of a modified candidate retains
the ordinary `400`/`409` contracts.

## Conductor API wrapper

The same object may implement:

```go
type APIWrapper interface {
    WrapAPI(http.Handler) http.Handler
}
```

`WrapAPI` is called once after `Start`; returning nil aborts startup. The wrapper
may add a private route, rewrite a request before calling `next`, override a core
route, proxy elsewhere, or return a local response. The framework does not
recover panics, reserve a prefix, detect route conflicts, prescribe auth, or
require that `next` be called.

The exact wrapped handler is used by both the public `api.<domain>` listener and
the API fallback on the conductor config socket. Config-socket task, run, admin,
plugin, and secret-management routes remain outside the wrapper. The data-plane
handler is also outside it. A nil extension or an extension without
`APIWrapper` uses the existing core handler object directly.

## Boundaries

A nil extension preserves the built-in startup and request behavior: no event
hub, watcher goroutine, lifecycle callback, or wrapper is created. Cluster
router, registry, and placer do not have extension objects; node-local Hooks and
observation do not change node-link ACK, exact replay, idempotency, or stable
SandboxID to NodeSandboxID authority.

This API has no namespace, fixed extension URI, dynamic loading, hot reload, or
component compatibility version. WebSocket transport is outside Issue #256 and
is tracked independently by Issue #269.

See [`examples/custom-conductor`](../examples/custom-conductor) for a complete,
buildable program.
