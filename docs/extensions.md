[English](extensions.md) | [简体中文](extensions_zh.md)

# Runtime extensions

Create identity is selected before the Hook. `SandboxOperation.SandboxID` stays immutable, and reintroducing `kuasar-sandbox.identity` into the cleaned mutable metadata is rejected. See [Sandbox identity on Create](node.md#412-create-identity).

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

## Component bootstrap and process-local material

### Public configuration entry points

The public `config` package exposes `LoadConductor(path string) (*Conductor, error)` and `DecodeConductor(io.Reader) (*Conductor, error)`, with `LoadProxy`/`DecodeProxy` equivalents returning `*Proxy`. Load opens a file; Decode accepts one strict YAML document up to 4 MiB, applies defaults and checks provided values. It does not resolve providers, inspect executable/material files or require all final runtime declarations. `ValidateConductorFinal(*Conductor) error` and `ValidateProxyFinal(*Proxy) error` perform final declarative validation without reapplying defaults or invoking providers.

`(*Conductor).Clone() *Conductor` and `(*Proxy).Clone() *Proxy` deep-copy declarations and preserve nil. Both expose `ParkTimeoutDur() time.Duration`. `(*ResourceAllocatable).SetMemory(string)` marks an explicit memory value; `InheritMemory()` restores nil, meaning inherited default/clamping rather than an explicitly configured `256MiB`. Configuration field semantics remain in [Node](node.md), [Build](node-build.md), [Proxy](node-proxy.md) and [resource policy](node-resource.md); exported types and helpers are in [config](../config/config.go).

`app/conductor` and `app/proxy` re-export their respective leaf `app/*/extension` types, constants and sentinels. Consumers may use either public import surface; no internal package is required. In each App package, `New(Hooks) *App`, `(*App).Run() error` and `(*App).RunContext(context.Context) error` have the lifecycle defined below. `Runtime.MarshalJSON() ([]byte, error)` and `(*Runtime).UnmarshalJSON([]byte) error` always reject serialization/deserialization.

### Conductor bootstrap

Operations still starts `node-ctl conductor serve --config ...`, which first performs environment-independent strict decoding/defaulting/declarative validation. Empty `paths.conductor_executable` selects explicit final validation and built-in runtime key/TLS/credential resolution. Otherwise node-ctl opens a protected absolute executable, validates runtime owner/mode/file identity against that FD, and executes the same file through `/proc/self/fd`, without resolving a replaceable pathname again. Sealed bootstrap records its device/inode; xconductor compares them only with `/proc/self/exe`. Deployment-time pathname replacement/deletion cannot alter the validated identity. node-ctl replaces itself with xconductor through exec.

Bootstrap environment contains FD numbers only; config bytes/digest live in a memfd sealed against writes, growth, shrinkage and further seal changes. Direct xconductor execution or missing/truncated/oversized/version/digest/component-mismatched bootstrap fails closed. This organizes processes and prevents misuse; it does not defend against a malicious same-UID process.

A custom main needs public packages only; the complete compilable example is [examples/custom-conductor](../examples/custom-conductor/README.md):

```go
app := conductor.New(conductor.Hooks{
    Configure: func(ctx context.Context, cfg *conductor.Config, rt *conductor.Runtime) error {
        // Adjust declarative Config; bind startup Runtime providers.
        rt.Extension = myExtension
        return nil
    },
})
if err := app.Run(); err != nil {
    log.Fatal(err)
}
```

`New` has no side effects. `Run` is one-shot and handles SIGINT/SIGTERM; embedders may use RunContext. App does not call os.Exit. Ordering is fixed: decode bootstrap → clone Config → Configure exactly once → verify unchanged conductor executable → final declarative validation → clone/freeze again → resolve Runtime materials → start shared core. Configure, startup material-provider and final-validation failures precede opening durable storage, listeners, systemd launchers/units or node-link. A hook may replace the whole Config but must preserve the originally frozen executable. Defaults are not reapplied after the hook.

App must come from conductor.New. A zero value or nil receiver returns an explicit error from Run/RunContext before signal handlers, bootstrap reads or goroutines. `node-ctl config conductor` performs declarative/bootstrap and executable-metadata diagnosis only; it neither executes custom App/providers nor substitutes its diagnostic EUID for the actual service owner policy. Custom-mode output explicitly defers runtime ownership/final validation to component startup.

Config contains only serializable declarations. Runtime is a process object that rejects JSON serialization and exposes logging, TLS material, an ordered AES-256 key set, neutral builder-files-storage credentials and one trusted statically compiled Extension. TLS providers return DER certificate chains, crypto.Signer and root/client CA pools, never arbitrary tls.Config; core retains minimum TLS version, ALPN and mTLS/client verification. TLS and encryption providers are resolved at startup. Object-store credentials are also refreshed on demand as described below; this is request-path work, not a background-only callback. A non-nil provider is authoritative; errors never fall back to files/environment/static credentials. V1 has no configuration or TLS/encryption-material hot reload.

Bootstrap retains original node-ctl's exact path. Generated runner/builder units execute that node-ctl. Adjacent sandbox-ctl/connector-ctl/flatten-ctl/manifest-ctl resolution also uses its release directory, not xconductor's. Custom component and node-ctl must be compatible versions. Public API exposes no store/launcher/vswitch/orch/Router internals; a trusted API wrapper receives only http.Handler next. There is no Go plugin, runtime discovery, global registry, dynamic middleware registration or DI container. Conductor serves control API only.

Conductor's exact public material surface is defined in [app/conductor](../app/conductor/conductor.go):

| Type | Fields or method |
|---|---|
| `Config` | Alias of `config.Conductor` |
| `Runtime` | `Logger *slog.Logger`, `TLS TLSMaterialProvider`, `EncryptionKeys EncryptionKeyProvider`, `ObjectStoreCredentials ObjectStoreCredentialsProvider`, `Extension extension.Extension` |
| `TLSMaterial` | `CertificateChain [][]byte`, `PrivateKey crypto.Signer`, `RootCAs *x509.CertPool`, `ClientCAs *x509.CertPool` |
| `TLSMaterialProvider` | `TLSMaterial(context.Context, TLSPurpose) (TLSMaterial, error)` |
| `EncryptionKeyProvider` | `EncryptionKeys(context.Context) ([][]byte, error)` |
| `ObjectStoreCredentials` | `AccessKeyID`, `SecretAccessKey`, `SessionToken` are strings; `Expires time.Time`, `CanExpire bool` |
| `ObjectStoreCredentialsProvider` | `RetrieveObjectStoreCredentials(context.Context) (ObjectStoreCredentials, error)` |

`TLSMaterialProviderFunc`, `EncryptionKeyProviderFunc` and `ObjectStoreCredentialsProviderFunc` adapt functions with the exact corresponding method arguments/results. `TLSPurposeAPI = "api"` is resolved first; `TLSPurposeNodeLinkClient = "node-link-client"` is used only when `Cluster.NodeLink.Endpoint` is nonempty. Certificates are leaf-first DER with a matching signer; chain and signer must be supplied together. Empty material disables that purpose. API `ClientCAs` requires a server certificate and enables required verified client certificates; node-link uses `RootCAs`. Core fixes minimum TLS 1.2 and ALPN (`h2`/`http/1.1` for API, `h2` for node-link).

The encryption provider must return a nonempty ordered set of raw 32-byte AES keys. Index zero encrypts new records; the key set permits decrypting earlier records. This process encryption key set is distinct from updating tenant credential-distribution records. A custom object-store provider requires configured `Builder.FilesStorage`, nonempty access/secret keys and, when `CanExpire`, a nonzero future `Expires`. It is primed before core startup. Later, the AWS SDK credentials cache invokes it when credentials need renewal during a Build S3 presign or HEAD operation; refresh can block or fail that operation. Implementations must account for request latency and must not assume startup-only execution. Provider errors do not fall back to YAML or the ambient credential chain. See the owning [filestore adapter](../internal/filestore/filestore.go).

### Proxy bootstrap

The sole operator entry point remains `node-ctl proxy serve --config ...`. Public Load/Decode performs only environment-independent strict decoding, defaults, and validation of provided values. node-ctl explicitly performs final validation for the built-in path; the custom path defers it until after master `Configure`. With a nonempty `paths.proxy_executable`, node-ctl validates the protected absolute executable's runtime owner, mode, and identity. It writes a bootstrap snapshot of public `config.Proxy` into a size-bounded sealed memfd that forbids writes, growth, and shrinking. Only the FD number enters the environment; configuration bodies and TLS material enter neither argv nor environment variables. node-ctl replaces itself with xproxy through the same validated open file and never falls back to the built-in implementation on failure. Direct xproxy execution, missing/corrupt bootstrap, and component/file identity mismatches fail closed. These checks organize processes and prevent misuse; they are not cryptographic authentication against a malicious process with the same UID.

See the buildable [custom-proxy example](../examples/custom-proxy/README.md):

```go
app := proxy.New(proxy.Hooks{
    Configure: func(ctx context.Context, cfg *proxy.Config) error {
        // master-only declarative override
        return nil
    },
    BindRuntime: func(ctx context.Context, process proxy.Process, rt *proxy.Runtime) error {
        // bind process-local logger / TLS provider
        if process.Role == proxy.RoleMaster {
            rt.MasterExtension = newMasterExtension()
        } else if process.Role == proxy.RoleWorker {
            // Construct a fresh instance for this worker epoch.
            rt.WorkerExtension = newWorkerExtension()
        }
        return nil
    },
})
if err := app.Run(); err != nil {
    log.Fatal(err)
}
```

`New` has no side effects. `Run` is one-shot, handles SIGINT/SIGTERM, and does not call `os.Exit`; an embedding host can use `RunContext`. A zero-value/nil App returns an explicit error requiring construction through `proxy.New` before processing signals or component/worker bootstrap. The master's order is fixed: decode bootstrap → clone → call `Configure` exactly once → verify that `paths.proxy_executable` is unchanged → final validation → deep-clone, canonical serialization, and digest to freeze EffectiveConfig → `BindRuntime(master)` → start the core. Configure-hook, provider, or final-validation failure occurs before SHM, listeners, routesync sessions, or workers are created. If `MasterExtension` is set, the core creates the shared route table and in-process traffic aggregate, then calls `Start(ctx, MasterHost)` exactly once. Start failure occurs before listener binding, routesync, or workers, and cleans up the created SHM. After successful Start, the same object's optional `ManagementWrapper` can wrap the `stats_socket` handler. Capability detection happens once and is frozen; a nil returned handler aborts startup.

Workers always reexecute the master's `/proc/self/exe`: a built-in master creates a node-ctl worker, and a custom master creates an xproxy worker. The configured executable does not select workers. Another sealed memfd carries frozen EffectiveConfig, its digest, worker ID/epoch, FD protocol/mapping, and current executable identity. Listeners, wake/notify, stats, MMDS RPC, and the independent admission arena are inherited FDs; worker index/epoch and arena version/size/layout are also validated. After strict validation, the worker calls `BindRuntime(worker)`. Each worker epoch receives a new `Runtime` and must not reuse the previous epoch's `WorkerExtension`. The worker completes stats hello/ready, constructs its Host, waits for the initial route-table sync, calls `WorkerExtension.Start` exactly once, and freezes the same object's optional `IngressWrapper`. Only then does it Serve. A Start error or nil wrapper keeps the data listener closed to serving; the existing master supervisor restarts the worker. Workers never read `proxy.yaml` or call `Configure`, so replacing or deleting that file does not affect replacement workers.

Public `Config` holds only serializable declarations. `Runtime` is a process object that rejects JSON encoding/decoding. It exposes a logger, a startup TLS-material provider, and trusted statically compiled `MasterExtension`/`WorkerExtension` objects selected by process role. The provider returns a certificate chain, `crypto.Signer`, and optional client CA pool; it cannot replace an arbitrary `*tls.Config`. A non-nil provider is authoritative and errors never fall back to cert/key files. The core still fixes the minimum TLS version, HTTP/2 ALPN, and client-auth policy. V1 supports no hot updates of configuration, material, or extensions. A custom component and node-ctl must use compatible versions.

This API applies only to the independent Proxy and adds no conductor data-plane factory. It exposes no raw Router, SHM, listener, routesync, stats, dial target, or credential records. Beyond the optional management wrapper of the same master extension and ingress wrapper of the same worker extension, it introduces no Go plugins, runtime discovery, multi-extension registry, generic lifecycle hooks, secret resolver, or DI container. `node-ctl config proxy` diagnoses only declarative/bootstrap configuration and executable metadata. It never executes xproxy, invokes Runtime providers, or substitutes the diagnostic command's EUID for actual startup ownership checks. The complete extension contract continues below.

Proxy's [public material types](../app/proxy/proxy.go) are deliberately different from Conductor's:

| Type | Fields or method |
|---|---|
| `Config` | Alias of `config.Proxy` |
| `Runtime` | `Logger *slog.Logger`, `TLS TLSMaterialProvider`, `MasterExtension extension.MasterExtension`, `WorkerExtension extension.WorkerExtension`; the opposite role's extension field is ignored |
| `TLSMaterial` | `CertificateChain [][]byte`, `PrivateKey crypto.Signer`, `ClientCAs *x509.CertPool`; no `RootCAs` |
| `TLSMaterialProvider` | `TLSMaterial(context.Context) (TLSMaterial, error)`; no purpose argument |
| `TLSMaterialProviderFunc` | Function adapter with that exact signature |
| `Process` | `Role Role`, `WorkerID string`, `WorkerEpoch uint64`; worker fields are zero-valued for master |

`RoleMaster = "master"` and `RoleWorker = "worker"`. Certificate/signer matching, paired presence, empty-material TLS disablement and required verified client certificates with `ClientCAs` follow the same server-material constraints. Core retains minimum TLS 1.2 and `h2`/`http/1.1` ALPN. `Hooks.Configure` takes `(context.Context, *Config) error`; `Hooks.BindRuntime` takes `(context.Context, Process, *Runtime) error`, as shown above.

Process-owned mapping and transport cleanup are defined once in the [Proxy worker lifetime contract](node-proxy.md#91-worker-lifetime-and-transport-cancellation). Public source, Hook and wrapper interfaces follow below; those are not another data-plane implementation.

## Conductor object sources

The point-read and watch signatures are:

```go
type SandboxSource interface {
    Get(context.Context, string) (SandboxView, bool, error)
    Watch(context.Context, func(SandboxEvent) error) error
}
type BuildSource interface {
    Get(context.Context, string) (BuildView, bool, error)
    Watch(context.Context, func(BuildEvent) error) error
}
```

Missing objects return `false, nil`. A nil Watch callback is invalid. Events carry `Generation uint64`, a typed `Kind`, `SandboxID`/`BuildID string` and `View *SandboxView`/`*BuildView`; `BuildEvent` also carries `Reason string`. Sandbox Delete may lack a View; Build Upsert/Remove include it. The complete nonsecret field definitions are [SandboxView and its enums](../app/conductor/extension/sandbox.go) and [BuildView, BuildResources and BuildOptions](../app/conductor/extension/build.go). `BuildView` includes immutable resources, requested Builder target, source/steps/commands, registration/queue/execution timestamps, current RunID/claim/enforcement/phase/network, cluster group and credential fingerprints. Neither source exposes raw credentials, MMDS secret values or mutable core objects; the snapshot and convergence rules below apply to every field.

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

The public [Sandbox request types and enums](../app/conductor/extension/sandbox_hook.go) define the complete mutable input:

| Request | Fields and constraints |
|---|---|
| `SandboxCreateRequest` | `TemplateID string`, immutable `Profile Profile`, `TimeoutSeconds int`, `Metadata`/`Env map[string]string`, `Secure bool`, `AutoPauseMemory *bool`, `MMDS *string`; MMDS preserves top-level input presence. Request-scoped MMDS/metadata may contain secrets or credentials: do not log them as an object-source projection |
| `SandboxPauseRequest` | Immutable `CaptureKind CaptureKind` (`snapshot`/`sandbox`); `CheckpointMergeRef`/`CheckpointDropCaches *bool`, where nil inherits Sandbox/node policy |
| `SandboxResumeRequest` | `RequestedDeadlineUnix *int64`, where nil uses core resume-deadline policy; immutable `Mode ResumeMode` (`auto`/`memory`/`cold`) and `Trigger ResumeTrigger` (`connect`/`wake`/`route`/`exec`/`exec-session`) |
| `SandboxDeleteRequest` | `Reason string` is diagnostic; editing it does not alter cleanup behavior |

Direct Create may change the template within its immutable profile. Canonical cluster Create must retain its template and cluster metadata and leave top-level `MMDS` nil; it does not acquire direct-Create override authority.

Build `register` runs after caller identity and IDs are resolved, but before the
registration capacity transaction. It may change `Names`, `Aliases`, `Profile`,
`Resources`, `Metadata` (including the routes-only MMDS declaration), `Env`, `Secure` and
supported `Builder` options; `BuildID` and `TemplateID` remain core-owned. There is no mutable request `kind`: terminal `BuildKind` (`img`/`sbx`/`snp`) is derived only on success, and remains empty before then. Raw MMDS secret values,
registry credentials, and pull tokens are not copied into the operation. The
core rebinds retained initial MMDS values to the final route declaration and
resolves registry credentials only after the Hook. A canonical cluster
BuildRegister runs the Hook only for new ownership. Exact replay of an existing
durable `BuildID` verifies the original tenant-keyed request identity and
credential fingerprints without invoking the mutable Hook again, so policy
changes cannot break an ACK-lost replay.

`Register.Resources` is the sole registration execution vector (`CPU` in milli-CPU, `Memory`/`Storage` in bytes). The Hook must leave `Builder.Resources` nil; reintroducing that duplicate authority is rejected. Mutable Builder controls are `Target *BuildTarget {Kind BuildTargetKind; Memory bool}`, `Referer *BuildRefererOptions {Enabled, Writeback *bool}`, and `Registry *BuildRegistryOptions` whose `TLS *BuildRegistryTLSOptions` contains `CABundlePEM string` and `InsecureSkipVerify bool`. Target kinds are `image` and `sandbox`; nil means auto resolution from final effective start/ready commands under the [Build target contract](node-build.md#31-request-scoped-builder-input), not from arbitrary configuration presence.

The public [Build request types](../app/conductor/extension/build_hook.go) define `BuildTriggerRequest` as `FromImage`, `FromTemplate`, `StartCommand`, `ReadyCommand` strings, `Steps []BuildStep`, and `ResourceAssertion BuildResourcePatch`. Each [BuildStep](../app/conductor/extension/build.go) contains `Type string`, `Args []string`, `FilesHash string` and `Force bool`. `BuildResourcePatch` contains `CPU`, `Memory`, `Storage *int64` in the same execution units as registration; nil leaves assert nothing, while present leaves must equal immutable registered resources.

Build `trigger` runs after ownership/state checks and preliminary pure parsing,
but before source resolution, registry credential resolution, and the
`registered → waiting` CAS. The final `fromImage`/`fromTemplate`, steps,
commands, and resource assertion are validated again; credentials are resolved
from that final source. There is deliberately no Hook at the
`waiting → building` execution claim.

Mandatory ownership cleanup may precede or bypass Hooks: Resume first cleans the previous paused owner before admitting the new operation.

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

```go
type RouteSource interface {
    Get(context.Context, string) (RouteView, bool, error)
    Watch(context.Context, func(RouteEvent) error) error
    SyncState() RouteSyncState
}
```

Missing point reads return `false, nil`; nil Watch callbacks fail. `RouteEvent` carries `Generation uint64`, `Kind RouteEventKind`, `SandboxID string` and `View *RouteView`, present for Upsert/Delete. [RouteView and enums](../app/proxy/extension/route.go) define node-local and stable identity, profile/template/lifecycle/RunID, Envd/CI sockets, FloatingIP, artifact location, full credential fingerprints and `Revision uint64`.

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

`TrafficSource.Get(context.Context, string) (TrafficView, error)` has no found boolean or Watch. `ErrTrafficUnavailable` means route identity or complete worker contributions are unavailable; `ErrTrafficConflict` means the current route state cannot produce the requested observation. [TrafficView](../app/proxy/extension/traffic.go) carries `SandboxID`, `RunID`, `Profile`, `State`, effective `MaxInflight config.MaxInflight`, `Inflight TrafficInflight`, `IdleSince *time.Time` and `Services map[string]ServiceTrafficView`. Inflight has `Parking`/`Egress uint64`; each service adds `IdleSince *time.Time`. `config.MaxInflight` has `Total`, `Forward`, `E2BEnvd`, `E2BCodeInterpreter`, `Exec uint32` (JSON `total`, `forward`, `e2b:envd`, `e2b:code-interpreter`, `exec`); zero is unlimited and `Unlimited() bool` checks the whole vector.

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

`ConnectTarget {Service ConnectService; Port int}` selects the backend. Service constants are `ConnectServiceLegacy = ""`, `ConnectServiceForward = "forward"`, `ConnectServiceE2BEnvd = "e2b:envd"`, `ConnectServiceE2BInterpreter = "e2b:code-interpreter"`, and `ConnectServiceExec = "exec"`. Legacy/forward require port `1..65535`; `ForwardAuthorized` rejects Exec, which must use the built-in KAT/per-command CEL path. See the [public target types](../app/proxy/extension/worker.go).

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
per-command CEL path. The helper shares the core HTTP/CONNECT transport, including
[HTTP/1.1 WebSocket forwarding](node-proxy.md#51-http11-websocket-forwarding).
It returns only after the upgraded relay finishes; wrappers do not own a separate
WebSocket transport or traffic lifecycle.

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
component compatibility version. WebSocket support belongs to the shared core
transport and adds no extension-specific API.

See [`examples/custom-conductor`](../examples/custom-conductor) and
[`examples/custom-proxy`](../examples/custom-proxy) for buildable programs.
