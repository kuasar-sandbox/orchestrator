# Runtime extensions

Create identity is selected before the Hook. `SandboxOperation.SandboxID` stays immutable, and reintroducing `kuasar-sandbox.identity` into the cleaned mutable metadata is rejected. See [Sandbox identity on Create](sandbox-identity.md).

Kuasar's conductor and independent Proxy support statically linked runtime
extensions for deployments that need process-local integration without carrying
a long-lived fork. An extension is trusted code compiled into `xconductor` or
`xproxy`. It shares the core process address space, UID, lifetime, filesystem,
and network capabilities. The API organizes ownership and concurrency; it is
not a security sandbox.

There is one extension object per conductor or Proxy master process,
and one fresh object per Proxy worker epoch. The framework does not
discover or load plugins at runtime, maintain an extension registry, assign
priorities, provide a dependency-injection container, or reserve URL
namespaces. A private project can compose any modules it needs behind the one
object for that role and lifetime.

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

`SandboxView` exposes the typed lifecycle projection directly:
`ResumeSourceKind`, `ResumeSourceRef`, `ArtifactLocation`,
`AutoPauseMemory`, and `LaunchMode`. `ArtifactLocation` is orthogonal to E/S
kind, and a running row may retain a source for node-local artifact ownership.
`LaunchMode` is non-empty only while `starting`; it is the durable, already
resolved launch decision rather than the trigger that requested it. The
internal `deleting` state is durable cleanup ownership: it is never a route or
activation state. It retains exact runner, RunDir, and BaseDir fields until the
core finalizer succeeds. Its network tuple remains exact until connector Detach
and the durable clear both succeed under the allocation fence; the clear removes
all four network fields atomically.

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

An explicit delete removes the sandbox from the node cache and publishes a
route delete as soon as the durable `deleting` transition succeeds. That route
delete is projection withdrawal only; it does not prove that the unit, network,
paths, or durable row have been finalized. The conductor object source emits its
terminal sandbox removal only after the exact local cleanup and hard delete
succeed. A restart-time snapshot may therefore contain a cleanup-pending
`deleting` view with either a pending or already cleared network tuple.
Extensions must treat it as diagnostic state and must not try to resume, route,
or independently clean it.

For route consumers, both the live delete and omission from a reconnecting full
snapshot withdraw an older projection. Durable local cleanup continues from the
retained row independently of either route convergence path.

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
  Hook may also change `AutoPauseMemory`; the final template is resolved again
  and must retain the core-owned profile.
- `pause`: after ownership, running incarnation, and checkpoint preconditions
  are captured; before snapshot or durable state changes. The Hook may change
  action-scoped merge-ref and drop-cache policy. `CaptureKind` is observable
  but core-owned and cannot be changed.
- `resume`: only for a real `paused → starting` admission shared by API Connect,
  proxy Wake, native exec, and canonical cluster commands. Running/starting
  idempotent joins do not invoke it. The Hook may change the requested deadline;
  `Mode` and `Trigger` are observable but core-owned.
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
plugin, and secret-management routes remain outside the wrapper. Conductor has
no data-plane handler. A nil extension or an extension without
`APIWrapper` uses the existing core handler object directly.

## Proxy master extension

An independent Proxy may set `proxy.Runtime.MasterExtension` from the master
invocation of `BindRuntime`. It uses a public leaf contract that does not depend
on `internal/*`:

```go
type MasterExtension interface {
    Start(context.Context, MasterHost) error
}

type MasterHost interface {
    Routes() RouteSource
    Traffic() TrafficSource
}
```

There is one object for the master process. The master creates its shared route
table, separate cross-worker admission arena, and in-process traffic aggregate,
calls `Start` exactly once, and only
then binds listeners or starts route synchronization and workers. A Start error
aborts startup and removes the shared-memory and socket artifacts already
created. The supplied context is canceled at master shutdown. As with the
conductor contract, `Start` should launch rather than wait for long-running
extension goroutines.

After a successful Start, the same object is checked once for the optional
management capability:

```go
type ManagementWrapper interface {
    WrapManagement(http.Handler) http.Handler
}
```

`WrapManagement` wraps the existing handler on `stats_socket`. It may add or
override any local path, proxy elsewhere, or call the built-in traffic handler.
There is no reserved namespace or conflict registry. Returning nil aborts
startup; panics are not recovered. A nil extension or one without this optional
interface uses the original handler directly.

### Master route source

`RouteSource.Get` returns an independent, non-secret projection of the route
that the master successfully applied to its core table. It includes sandbox and
authorization identities, profile, template, state, RunID, current local
endpoints, E/S artifact location, credential fingerprints, and the core route
revision. It omits raw secrets, access tokens, MMDS values, and internal SHM
records. This minimizes routine event data and accidental logging; it is not a
permission boundary for trusted in-process code. No metadata field or new SHM
schema is introduced for extensions. The proxy projection deliberately exposes
only `ArtifactLocation`; it has no ResumeSource kind or launch-mode gate and
cannot decide whether a paused route may Wake.

`SyncState` reports `initializing`, `syncing`, `synced`, or `stale`. A routesync
disconnect moves the source to `stale` and produces `sync_lost`; the serving SHM
retains its existing reconnect behavior. A subsequent sync replaces the
projection at its bookmark and returns the source to `synced`.

`RouteSource.Watch` follows the generation rules described above. A complete
consumer generation is `sync_begin`, the sorted applied-route snapshot, then
`sync_end`; ordered live `upsert` and `delete` events follow it. A source reset
or lagging bounded queue abandons only that watcher's incomplete generation and
starts a new full snapshot after the source is synced. Duplicates are allowed,
intermediate states are not guaranteed, and this is not a durable audit stream.

The observing sink updates the extension projection only after the core SHM
operation succeeds. Publication is bounded and non-blocking, and callbacks run
only in the goroutine calling Watch. Extension lag, callback errors, or resyncs
cannot terminate routesync, block SHM changes or worker notification, delay a
RouteBarrier ACK, or affect Wake/activation.

### Master traffic source

`TrafficSource.Get` combines the current applied route identity and its effective
per-Sandbox `maxInflight` policy with `MasterStats` directly in process; it does
not query the stats UDS or derive limits from the conductor Sandbox row. Returned
`TrafficView.MaxInflight` uses the canonical `config.MaxInflight` shape; maps and
time pointers are independent copies. V1 intentionally has no traffic Watch. A
route retained during reconnect remains queryable, so callers that require
freshness also inspect `Routes().SyncState()`.

## Proxy worker extension

`BindRuntime` runs in every worker re-execution with a fresh `Runtime` value. A
custom proxy may construct that epoch's one extension object there:

```go
type WorkerExtension interface {
    Start(context.Context, WorkerHost) error
}

type WorkerHost interface {
    Process() Process
    GetRoute(string) (RouteView, bool)
    ForwardAuthorized(http.ResponseWriter, *http.Request, ForwardRequest)
}
```

The worker reconstructs its SHM table, listeners, update/wake channels, and
process-local clients, constructs the Host, and waits for the required initial
route sync before calling `Start` exactly once. A Start error prevents the data
ingress listener from serving and lets the existing master supervisor restart
that worker epoch. The supplied context is canceled when the epoch exits.

After Start succeeds, the same object is checked once for the optional raw
ingress capability:

```go
type IngressWrapper interface {
    WrapIngress(http.Handler) http.Handler
}
```

The resulting handler is frozen for the epoch and used unchanged by the node's
`data_listen`. It receives
requests before `ParseSandbox`, `ParseConnect`, or core token admission, so it
may define a private Header/path/authentication contract, rewrite canonical
`E2b-Sandbox-*` input and call `next`, answer locally, contact another upstream,
or intentionally override a standard request. Unmatched requests should call
`next` when built-in behavior is desired. MMDS does not use this wrapper.
Returning nil aborts the worker epoch; panics are not recovered. A nil worker
extension or one without `IngressWrapper` uses the original core handler object
directly.

### Worker route point lookup

`GetRoute` returns an independent public `RouteView` copied from the worker's
current SHM point lookup. It exposes the same existing non-secret route fields
and revision used by the master projection, not the raw SHM record, router,
tokens, or mutable pointers. Worker V1 deliberately has no route Watch; dynamic
route observation remains master-only. This small surface is API discipline,
not a security boundary for trusted same-process code.

### Authorized core forwarding

`ForwardAuthorized` is for a wrapper that has already completed its private
authentication but wants to reuse the core route and transport pipeline:

```go
type ForwardRequest struct {
    SandboxID string
    Target    ConnectTarget
    Revalidate func(context.Context) error
    Rewrite    func(*http.Request) error
}
```

It owns `http.ResponseWriter` and completes the response on every success or
failure path; callers must return without writing another error after it does.
It intentionally does not verify Kuasar's `X-Access-Token`. A wrapper that needs
that token contract should canonicalize the request and call `next` instead.

The fixed sequence is side-effect-free route lookup and target validation,
shared per-Sandbox admission plus traffic parking, `ActivateRoute`/Wake and
admission/route binding revalidation, optional
`Revalidate`, backend dial, ordinary HTTP or CONNECT transport, and traffic
close/idle accounting. `Revalidate` runs after activation but before dial, so a
private registration, policy generation, or lease revision can fence a resumed
route. Its private error detail is logged and the client receives only a fixed
stale-policy response; no old backend is dialed.

Admission happens exactly once whether the wrapper calls `ForwardAuthorized`
directly or canonicalizes a request and calls `next`; wrappers must not nest the
two paths for one logical request. Rejection occurs before Wake, activation, or
dial and uses the core `429 max_inflight_reached` response. The admission lease
and parking/egress accounting share one lifecycle, including ordinary response,
CONNECT relay, cancellation, and failure cleanup. The generic helper still
rejects native exec, so it cannot bypass the KAT/CEL/first-frame gate or move exec
admission ahead of that gate.

For ordinary HTTP, `Rewrite` receives an independent guest-facing request clone.
The core performs final hop-header and transport normalization after the
callback and writes no guest request bytes if it fails. CONNECT has no guest
HTTP request and never invokes `Rewrite`. Native exec targets are rejected by
this generic helper: standard exec continues through `next` and its KAT plus
per-command CEL path. The helper uses the repository's current ordinary HTTP
and CONNECT transport and does not implement WebSocket support.

## Ingress boundary

The Worker `IngressWrapper` is reachable only at the Proxy `data_listen`.
Conductor's public listener and config-socket API fallback both use the wrapped
control API handler; they never relay ordinary HTTP or CONNECT to a worker.
Cluster router, registry, and placer do not gain extensions or private ingress:
a private deployment must canonicalize a cluster request at its outer boundary
before using the existing canonical cluster path.

## Boundaries

A nil conductor, proxy-master, or proxy-worker extension preserves the built-in
startup and request behavior: no event hub, watcher goroutine, lifecycle
callback, or wrapper is created. Cluster router, registry, and placer do not
have extension objects; node-local Hooks and observation do not change
node-link ACK, exact replay, idempotency, or stable SandboxID to NodeSandboxID
authority.

This API has no namespace, fixed extension URI, dynamic loading, hot reload, or
component compatibility version. WebSocket transport is outside Issue #256 and
is tracked independently by Issue #269.

See [`examples/custom-conductor`](../examples/custom-conductor) and
[`examples/custom-proxy`](../examples/custom-proxy) for buildable programs.
