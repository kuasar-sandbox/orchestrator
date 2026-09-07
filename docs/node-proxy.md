[English](node-proxy.md) | [简体中文](node-proxy_zh.md)

<a id="node-proxy--节点数据面转发层"></a>
# node-proxy — Node data-plane forwarding

<a id="1-概述"></a>
## 1. Overview

The data-plane proxy is the L7 forwarding layer for sandbox traffic. It routes requests from external e2b SDKs/CLIs, port forwarding, or the cluster router by `(sid, target)` to guest envd/CI UDS endpoints, user ports at a sandbox floating IP, or the native exec `ctl.sock`. Ordinary HTTP retains the legacy port target; CONNECT can explicitly select a logical service with `E2b-Sandbox-Service`. The control-plane API, lifecycle, keys, and builds belong to `node-ctl conductor serve`; see [node.md](node.md). This document covers the data-plane forwarding layer.

```text
client / cluster-router
  │ Host: <port>-<sid>.<domain>
  │ or E2b-Sandbox-Id + E2b-Sandbox-Port
  │ CONNECT may add E2b-Sandbox-Service + X-Access-Token
  ▼
node proxy worker
  │ shared route view (read-only mmap)
  ├─ e2b legacy 49983/49999 ─► envd / ci UDS
  ├─ forward:port ───────────► floatingip:port
  └─ exec ──────────────────► <run_root>/sandboxes/<sid>/ctl.sock
```

<a id="11-设计原则"></a>
### 1.1 Design principles

- **Separate forwarding and control planes:** the proxy decides routes, applies authentication policy, and forwards bytes. The conductor owns sandbox lifecycle authority.
- **One subscribing master, multiple forwarding workers:** only the proxy master registers a config-socket plugin. Workers neither connect to the conductor nor hold independent routesync subscriptions.
- **Separate route views:** fixed-length data-plane fields enter shared memory, which workers mmap read-only. Variable-length `mmds_routes` and `mmds_route_secret_values` remain in the master's bounded heap. Workers query an exact path through inherited local socketpair RPC. Secret-value plaintext never enters mmap.
- **Inherited listener FDs:** the master binds the data/MMDS listeners and passes the same FD to all workers, which accept and forward connections. A future systemd socket-activation implementation could replace master-side binding without changing this worker model.
- **Configurable forwarding network namespace:** with `proxy_netns` pointing to the connector management namespace, workers run there; their `floatingip:port` access and the MMDS listener also use that namespace.
- **No upstream connection pool:** ordinary HTTP dials and closes a backend for each request. Each CONNECT binds one request to one TCP/UDS connection. Connections are never reused across sandboxes or ports.
- **Authentication policy precedes lifecycle effects:** ordinary HTTP and non-exec CONNECT follow `LookupRoute → authorize(RouteBinding) → TryBeginParking → ActivateRoute → fresh Route → dial`. Lookup never wakes, resumes, parks, or dials; Activate revalidates the binding before and after lifecycle effects. Invalid ordinary credentials cannot wake a sandbox or consume traffic parking/admission when the node's effective policy is `enforce`. Node `log`/`off` modes admit ordinary requests according to §6.
- **Logical service selection belongs to CONNECT:** ordinary HTTP does not use `E2b-Sandbox-Service` to choose a backend and otherwise forwards it as an application header. The explicit exception is the exact value `exec`, which returns 405 before activation. For CONNECT, an explicit service is authoritative for backend selection.
- **Exec authenticates before activation:** `service=exec` always validates a KAT bound to `StableID`. After CONNECT 200, it must also authorize the complete first ExecRequest. Failure cannot trigger parking, Wake/resume, or a backend dial. The final node proxy gives the ctl tunnel helper only requests that pass both gates; it exposes neither arbitrary UDS endpoints nor other ctl capabilities to tenants.
- **Deterministic MMDS keys:** `MmdsSecret = HMAC-SHA256(manifest_key, "kuasar-mmds-v1:" + sid)`, with the hex manifest key decoded to bytes. PUT and GET therefore agree even when different workers serve them.

<a id="2-cli"></a>
## 2. CLI

Start the node data plane as one proxy master process:

```bash
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

`node-ctl` runs the built-in master or replaces itself with a statically customized master selected by `paths.proxy_executable`. The master always reexecutes its own current executable to start workers. The internal worker mode is not an operator interface.

`proxy.yaml` fields:

| Field | Default | Meaning |
|---|---|---|
| `config_socket` | `/run/sandbox/node-ctl.socket` | Conductor config-socket; the master registers on its plugin plane and synchronizes routes |
| `paths.proxy_executable` | Empty | Absolute executable for a statically customized Proxy master; empty uses the built-in implementation. Static diagnostics check regular/executable status, absence of group/world writability, and same-file identity, without inferring ownership from the diagnostic command's EUID. Actual root dispatch requires root ownership; non-root dispatch accepts root or its own EUID. Used only for node-ctl → master dispatch, never to select a worker executable |
| `paths.run_root` | Required | Node RunRoot; workers construct `<run_root>/sandboxes/<NodeSandboxID>/ctl.sock` locally. This path is not transmitted in routesync `Policy` or shared route records |
| `data_listen` | Required | The node's only sandbox data ingress; the master binds once and gives workers the same listener FD |
| `proxy_netns` | Empty | Forwarding namespace; empty uses the current namespace. Otherwise workers run there and the conductor-projected MMDS address is bound there; `data_listen` stays in the master's current namespace |
| `stats_socket` | `<dir(config_socket)>/proxy-stats.sock` | Traffic-stats UDS owned and registered with the conductor by the master. Must be absolute, cannot conflict with config/SHM paths, and has mode 0600 |
| `shm_path` | `<dir(config_socket)>/proxy-routes.shm` | Shared route-table mmap file |
| `route_capacity` | `65536` | Fixed route-slot capacity; exhaustion fails Upsert and terminates the current routesync session. A Create waiting for that route returns 503 |
| `workers` | `1` | Number of worker processes |
| `tls` | Empty | Data-plane TLS `{cert,key}`; empty means h2c |
| `auth` | `enforce` | Data-plane authentication fallback before routesync policy arrives |
| `park_timeout` | `30s` | Parking fallback before routesync policy arrives |
| `metrics_listen` | Empty | Master Prometheus text endpoint aggregating worker data-plane counters |
| `traffic.max_inflight.total` | `0` | Proxy-wide inflight limit across all applicable services of each sandbox; `0` means unlimited |
| `traffic.max_inflight.forward` | `0` | Per-sandbox `forward` inflight limit; `0` means unlimited |
| `traffic.max_inflight."e2b:envd"` | `0` | Per-e2b-sandbox envd inflight limit; `0` means unlimited |
| `traffic.max_inflight."e2b:code-interpreter"` | `0` | Per-e2b-sandbox code-interpreter inflight limit; `0` means unlimited |
| `traffic.max_inflight.exec` | `0` | Per-sandbox native exec inflight limit; `0` means unlimited |

`proxy.yaml` contains neither `mmds_listen` nor `services`. Their sole sources are conductor `mmds.listen` and `mmds.services`, delivered in `Hello{Policy}` through trusted plugin registration.

<a id="21-静态定制-proxy"></a>
### 2.1 Statically customized Proxy

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

`MasterHost.Routes()` exposes applied-route `Get`, generation-based `Watch`, and `SyncState(initializing|syncing|synced|stale)`. A full generation is `sync_begin → snapshot upsert* → sync_end`, followed by live upsert/delete in publication order; disconnect emits `sync_lost`. A slow watcher invalidates only its own generation and automatically receives a full resync. Duplicates are allowed, every intermediate change is not guaranteed, and this is not a durable audit stream. Views copy identity, profile/template/state/RunID, current endpoints, artifact location, fingerprints, and route revision. They copy no raw secrets/tokens and add neither route metadata nor an SHM schema. Observers publish nonblockingly only after successful core SHM application, never affecting routesync, barrier ACK, Wake, or worker notification.

`MasterHost.Traffic().Get` reads the master's in-process worker aggregate directly against current route identity, without looping through the stats UDS, and returns map/pointer copies. V1 has no Traffic Watch. Retained routes remain queryable during disconnection; callers requiring freshness must also check Route `SyncState`. A management wrapper may add, override, or pass through any local route. The framework reserves no namespace, detects no route conflicts, and prescribes no authentication.

`WorkerHost.Process()` returns the current worker ID/epoch. `GetRoute(sid)` performs only a current-SHM point lookup and returns an independent, non-secret `RouteView` copy. It exposes no raw record, Router, or mutable pointer, and offers no worker Route Watch. `IngressWrapper` receives raw requests before canonical Host/Header and CONNECT parsing. The wrapped handler serves only node `data_listen`, never the MMDS listener. An extension may define its own headers, paths, or authentication, override behavior, or respond locally. Calling `next` for unmatched requests preserves core token and native exec semantics.

After private authentication, `WorkerHost.ForwardAuthorized` can reuse the core path `LookupRoute → TryBeginParking → ActivateRoute/Wake/binding revalidation → optional Revalidate → dial → ordinary HTTP/CONNECT → traffic close`. This helper owns `ResponseWriter`; its caller must not write another error after return. It does not validate Kuasar `X-Access-Token`. `Revalidate` runs after activation and before dial. Ordinary HTTP `Rewrite` receives only the guest-facing clone, and failure writes no guest-request bytes. CONNECT never calls Rewrite. The generic helper rejects native exec, which must continue through `next` and the KAT plus per-command CEL gates. This interface adds no WebSocket transport; [#269](https://github.com/kuasar-sandbox/orchestrator/issues/269) tracks that separately.

This API applies only to the independent Proxy and adds no conductor data-plane factory. It exposes no raw Router, SHM, listener, routesync, stats, dial target, or credential records. Beyond the optional management wrapper of the same master extension and ingress wrapper of the same worker extension, it introduces no Go plugins, runtime discovery, multi-extension registry, generic lifecycle hooks, secret resolver, or DI container. `node-ctl config proxy` diagnoses only declarative/bootstrap configuration and executable metadata. It never executes xproxy, invokes Runtime providers, or substitutes the diagnostic command's EUID for actual startup ownership checks. See [extensions.md](extensions.md) for the full extension contract.

<a id="3-部署拓扑"></a>
## 3. Deployment topology

Every sandbox-capable node runs both conductor and Proxy. The conductor API listener carries only the control plane; the Proxy's required `data_listen` is the node's only sandbox data ingress. The two advertised endpoints point to these separate listeners, with no cross-plane fallback.

```text
client / cluster-router ── control ─► conductor APIEndpoint
                              │
                              └─ config_socket plugin stream
                                 Wake / BarrierAck ▲ │ route/policy/MMDS
                                                   │ ▼
client / cluster-router ── data ───► Proxy DataEndpoint
                                      master ─► shared route mmap
                                        │       shared admission mmap
                                        │       MMDS bounded heap
                                        │       stats UDS
                                        └─ inherited data/MMDS listener
                                                   ▼
                                             worker[0..N)
```

Key points:

- The conductor sees one fixed plugin ID, `proxy`. Registration's `Proxy` field is the trusted Proxy marker; `StatsSocket` is a separate optional traffic-stats address.
- Only the master listens on `stats_socket`. The conductor's public traffic GET reads the master's aggregate cache through this UDS, without querying workers on demand. A missing `StatsSocket` does not remove the route-barrier participant.
- Workers do not register plugins or hold independent complete route tables. Route mmap remains master-write/worker-read-only; in admission mmap, each worker writes only its own absolute-counter column. The master restarts crashed workers, and replacements read the current route/admission view directly.
- Master exit takes its workers down. After systemd restarts the master, it registers again and rebuilds shared tables.
- The plugin ID must be exactly `proxy`, and registration must satisfy `subscribe.kind=route_wake`, `proxy!=nil`, and `mmds=true` before the conductor projects MMDS policy, routes, and secret values. Ordinary observers and node-link never receive these confidential values.

<a id="4-routesync-与共享路由视图"></a>
## 4. routesync and shared route views

routesync remains framed JSON over h2c; the proxy master dials the conductor:

```text
master → conductor : register{subscribe: route_wake, proxy{stats_socket?}, mmds}
master → conductor : wake{sid} / route_barrier_ack{barrier_id}
conductor → master : hello{policy}
conductor → master : upsert* → bookmark → upsert/delete/route_barrier...
```

The master projects the downstream route stream into shared memory:

- `BeginSync` starts a new synchronization generation.
- `Upsert` inserts or updates the `sid` slot. An identical replay only refreshes its synchronization generation, without advancing the lifecycle revision.
- `Delete` reclaims live slots through backshift deletion and records credential-free `(sid, revision)` in a separate bounded terminal-state cache.
- `Bookmark` removes old records absent from this generation and marks initial sync complete.
- `Policy` writes the shared header. Each worker request reads current `auth_mode` and `park_timeout_ms`.

Every Create establishes a route-applied barrier on the same ordered stream:

```text
conductor: Upsert(initial starting) → route_barrier{id}
master:    traffic patch validate/merge → admission apply → route SHM Upsert
           → notify workers → route_barrier_ack{id}
```

`ApplyUpsert` treats admission and route changes as one rollback-capable transaction. Failure in validation, arena allocation, MMDS projection, or route SHM Upsert restores the previous admission/route view. The master's ACK only means that the `starting RouteBinding` and effective limits needed for parking have entered its serving view. It does not mean the sandbox is running, a backend is dialable, envd has completed `/init`, or any worker is healthy/ready. Zero serving workers, worker restarts, and stats-stream faults do not enter the Create condition. One upstream writer serializes Wake and BarrierAck frames. If an earlier Upsert fails to apply, the subscriber sends no ACK and terminates the session; the conductor rolls back the waiting Create and returns 503. Barrier IDs exist only in this in-memory coordination, not the route changelog, shared table, Sandbox schema, or log fields.

Currently there is one master participant, fixed plugin ID `proxy`. After ACK, Create rechecks that the registration epoch is still the current lease. Disconnect, same-ID replacement, and late or old-session ACKs cannot complete the barrier. Internally completion requires all members of the participant set, not a quorum. If multiple traffic-serving masters are explicitly configured in the future, all must ACK before 201 can return.

The MMDS extension is not stored in fixed records. For each active sandbox, the master replaces routes and values under one lock; `route_capacity` bounds the heap-entry count. `BeginSync` and routesync disconnection immediately clear the heap and mark it unavailable. A new master starts with an empty view; a full `Bookmark` is required before queries reopen. A worker-only restart does not clear the surviving master's heap; its replacement reconnects through inherited RPC and waits for the shared view to be synchronized before serving. Each `Hello{Policy}` atomically replaces the whole service registry. Running/starting Upsert updates the heap before publishing the SHM route; paused/Delete revokes the heap before updating SHM. Thus workers that sampled an old active row also fail closed, preventing a new lifecycle from being combined with old secrets. `RunID` enters fixed records to bind MMDSv2 tokens to an incarnation. MMDS route declarations and secret-value plaintext never enter fixed records, metrics, or logs.

Here `RouteEntry.SandboxID`, `sid`, and shared-table keys are node-local SandboxIDs. In cluster operation they are Registry-allocated NodeSandboxIDs; the cluster Router translates the public stable SandboxID before reaching the node. `RouteEntry.StableID` preserves sandbox identity across NodeSandboxID changes for KAT/credential binding and is not the shared-table lookup key. routesync V3 made the hard switch to `stable_id`; V4 replaced Snapshot-specific location with `artifact_location`, orthogonal to E/S kind; V5 split node registration into `api_endpoint` and `data_endpoint` and narrowed Proxy registration. Subscribers and node-link clients validate the version in the first Hello and terminate mismatched sessions before handling routes or commands.

The shared table is a fixed-capacity open-addressed hash table with a single master writer. Each record has a seqlock. Workers retry when a write is in progress or the version changes, never observing a partial route. Workers depend only on local shared-table reads:

```text
sid hash ─► record slot ─► RouteEntry (read-only Lookup)
                         ├─ running → policy admission, Activate recheck, then forward
                         ├─ starting → policy admission, park without Wake; end on rollback
                         └─ paused → wake pipe and park only after policy admission
```

Ordinary route Lookup never writes the wake pipe for missing, paused, or starting routes. Only after a request passes the returned `RouteBinding`'s authentication policy may Activate write a wake for a paused SID. A starting sandbox already has a conductor launch owner; Activate only waits for running/delete/paused updates and must not send another Wake. The latter two rollback updates immediately terminate the starting request. The master deduplicates requests and sends upstream routesync `Wake`. After each table write it wakes worker-local parking waiters through a notify pipe. Global revision/notification merely triggers rechecking; workers use that SID's live or terminal revision to determine whether Wake received a terminal response. Live route, terminal cache, and revision are read in one table-seqlock snapshot, preventing an old missing/paused route and new revision from forming a false terminal result. Identical paused Upserts do not advance per-SID revision, so subscription replay cannot impersonate Wake completion. Live-hash deletion backshifts and immediately reclaims slots under the same table seqlock. The terminal cache holds at most 4096 entries and no credentials. Under extreme churn, collisions only evict older terminal correlations; affected waiters conservatively keep parking until a later state or timeout, without misrouting or treating an unrelated SID as the Wake result.

On the normal single-Wake path, terminal revision still exposes rollback promptly even when asynchronous convergence coalesces intermediate starting and subsequent paused/Delete updates. Only extreme cache eviction degrades to a conservative timeout. A request may send at most one Wake from an initial missing/paused state; after observing starting, a return to paused must not trigger another Wake. Ordinary Lookup itself never sends this Wake.

The shared view is an asynchronously converging route cache. Default creation uses UUIDs; cluster NodeSandboxIDs use `<stableSandboxID>-g<SandboxGeneration>`. Normal operation does not reuse one node-local ID for distinct logical sandboxes. If an external system immediately assigns a just-deleted NodeSandboxID to a different logical sandbox, workers can briefly retain the previous instance's projected credentials and same-name run directory before Delete/new Upsert arrives. Nodes must not deliberately perform this immediate cross-sandbox ID reuse. Explicit migration targets for different logical sandboxes must use a fresh NodeSandboxID or first confirm route-view convergence.

Protected `RouteEntry` states are `starting|running|paused|dead`. Entries explicitly carry `StableID`, `APISecret`, `APISecretFingerprint`, `ManifestKeyFingerprint`, `ServiceSecret`, `EnvdAccessToken`, `TrafficAccessToken`, and `ForwardAccessToken`. Raw ManifestKey never enters routes. Node forwarding selects only EnvdAccessToken or ForwardAccessToken for its target. TrafficAccessToken is projected in the protected view for external gateways and e2b data-plane components, but the node platform layer does not consume it. `StableID + ServiceSecret` verifies exec KATs; shared-table keys and local run directories still use NodeSandboxID exclusively. Existing `MmdsSecret` independently signs MMDS tokens and is not a route secret value; route secret values enter only the master's heap through the trusted projection described above.

On the routesync wire, `MaxInflightPatch` carries only explicit sandbox leaves. The master merges it with destination-node `proxy.yaml` defaults and writes fixed effective values plus `{admission slot,generation}` into route SHM. Pointers enter neither SHM nor route equality. Current internal boundaries are routesync version 7, route SHM schema 7, worker bootstrap/config/FD protocol version 2, and admission arena version 1. These are hard compatibility cuts: mismatched conductor/proxy/registry/router protocol versions or old SHM/bootstrap formats fail closed.

The initial durable starting Upsert may lack a FloatingIP, UDS, or any other backend endpoint. Workers park by state and never attempt those empty fields. After the node persists network ownership and completes YAML/ready.sock preparation, it publishes enriched starting; only then can MMDS identify it by FloatingIP. Ordinary data traffic still waits for running.

<a id="5-转发路径"></a>
## 5. Forwarding paths

Ordinary HTTP parses `(sid, port)` from `Host: <port>-<sid>.<domain>` or `E2b-Sandbox-Id` / `E2b-Sandbox-Port`. It does not select a backend from `E2b-Sandbox-Service`; that header is forwarded unchanged as an application header, except that the exact value `exec` returns 405 before activation. CONNECT parses `(sid, service?, port?)`. The second cluster hop must use the current NodeSandboxID as `sid`.

Without an explicit service, the node applies the legacy mapping from its trusted local profile:

```text
profile=e2b  and port ∈ {49983,49999} → envd / ci UDS
otherwise                              → floatingip:port
unknown or not running before timeout   → 404
```

Thus bare ports 49983/49999 forward to `floatingip:port` just like any other valid port. They have no envd/CI logical meaning and do not return 501.

An explicit CONNECT `E2b-Sandbox-Service` replaces legacy port inference:

| Service | Supported profile | Backend | Port semantics |
|---|---|---|---|
| `forward` | e2b / bare | `floatingip:port` | Required from `E2b-Sandbox-Port`, legacy Host, or CONNECT authority |
| `e2b:envd` | e2b | envd UDS | May be present but does not select the backend |
| `e2b:code-interpreter` | e2b | CI UDS | May be present but does not select the backend |
| `exec` | e2b / bare | `<run_root>/sandboxes/<NodeSandboxID>/ctl.sock` | May be present but does not select the backend |

Explicit `e2b:envd` or `e2b:code-interpreter` on bare returns 501. An unknown or empty service returns 400. Service and port may coexist; the node never uses 49983/49999 to override an explicit service.

Ordinary HTTP:

1. The worker reads the shared table and obtains a `RouteBinding` without a backend.
2. It selects EnvdAccessToken or ForwardAccessToken for the target and applies the node's effective authentication policy to `X-Access-Token`. Eligible `/files` requests can also verify a signature with EnvdAccessToken; §6 describes mode-specific enforcement.
3. After admission by that policy, `TryBeginParking` checks sandbox total and target-service limits together. Success publishes the shared count and enters parking. Exhaustion returns 429 without Wake, Activate, or dial.
4. `ActivateRoute` revalidates the binding, including admission generation/effective policy, before Wake/waiting and again after lifecycle work. It constructs the final backend from the latest running route and fails closed if the binding changes.
5. It dials envd UDS or `floatingip:port` once. With `proxy_netns`, the floating-IP dial occurs in that namespace.
6. It writes one HTTP request and streams the response. Response completion or final relay completion closes the flow and releases its quota exactly once.

CONNECT:

- Sandbox ID comes from `E2b-Sandbox-Id` or the legacy authority label.
- Legacy/`forward` may obtain the actual port from CONNECT authority. For portless logical services, authority is only a transport placeholder and does not produce `E2b-Sandbox-Port`.
- After ordinary forward/envd/CI targets pass the effective authentication policy, the client and backend connections are spliced bidirectionally.
- `service=exec` accepts CONNECT only. Ordinary HTTP carrying this service returns 405 without triggering resume.

Node exec has three ordered stages:

1. Before CONNECT 200, side-effect-free local `LookupExec` strictly verifies the `X-Access-Token` KAT using route `StableID + ServiceSecret`. Only after successful HMAC verification does it compile CEL programs or retrieve them from the bounded cache. It does not park, activate, or dial `ctl.sock`. Token/identity/expiry/condition failures end with HTTP 400/401/404/501.
2. After returning and flushing CONNECT 200, it reads the complete first ctl frame within a fixed 10-second first-request timeout. Strict parsing covers top-level `exec_request`, `ExecSpec`, and `StdioSpec`, preserving the client's original four-byte little-endian length plus JSON bytes. It rechecks expiry, constructs the normalized request view, and ANDs all conditions. False, error, unknown, cost exhaustion, and cancellation all fail closed.
3. Only successful request admission permits `TryBeginParking(exec) → ActivateExec → exact identity recheck → ctl.sock dial → AttachBackend`. It then writes the first Raw frame unchanged exactly once and starts bidirectional relay. Once that frame reaches the backend, it never retries, reroutes, or replays it.

The CEL view normalizes nil argv/env to `[]`/`{}`, and empty cwd or `/` to `/`; user retains its requested value. In TTY mode, stdin/stdout/stderr flags retain the ctl wire's ignored semantics. The view exposes normalized effective semantics and does not reject valid requests merely because those flags coexist. Failed condition or structural gates change neither parking nor activity and start no guest child, so they cannot resume a paused sandbox.

Workers obtain required `paths.run_root` from the master's frozen EffectiveConfig rather than rereading `proxy.yaml`. It must agree with the same node conductor's `paths.run_root`. routesync `Policy` and shared views project routes, credentials, and authentication policy, never the `ctl.sock` path.

The shared sandboxer tunnel helper understands no KAT, CEL, route, or lifecycle. It fixes callback ordering, strict first-frame reading, one-time Raw forwarding, and half-close relay. H1 continues from Hijack's buffered reader; H2 reads the request body and promptly flushes responses. Both preserve half-close and wait for both relay directions. After CONNECT 200, a fully recognized request denial or backend failure returns the same sanitized ctl frame, `{"type":"error","msg":"exec request rejected"}`. Unrecoverable framing closes the tunnel directly. `max_inflight.exec` is checked only after the first frame and CEL pass. Since HTTP 200 is already committed, exhaustion uses that generic ctl error and closes, records `data_requests_total{result="max_inflight_reached"}`, and neither fabricates HTTP 429 / `X-Kuasar-Proxy-Error` nor activates or dials `ctl.sock`.

KAT validation occurs at CONNECT admission and expiry is rechecked during first-frame authorization. Expiry after backend relay begins does not forcibly close an established tunnel. One unexpired KAT can open multiple independent CONNECTs. Each tunnel carries exactly one ctl exec session and never reuses a backend connection. New CONNECTs use the current NodeSandboxID after a route change; established tunnels do not migrate.

The worker performs the complete token, request, and backend gates in its own process, constructing `sandboxes/<NodeSandboxID>/ctl.sock` under frozen EffectiveConfig `paths.run_root`. Neither this path nor CEL programs enter routesync `Policy` or SHM records. The conductor does not parse, select, or forward ordinary HTTP, CONNECT, or exec bytes. Data requests sent to APIEndpoint receive only the API handler's natural response. The cluster router's canonical chained CONNECT is a relay; traffic accounting occurs only at the node worker establishing the final sandbox backend.

<a id="6-数据面鉴权"></a>
## 6. Data-plane authentication

All data-plane requests use `X-Access-Token`, with the expected value selected by target:

- e2b legacy ports 49983/49999 and explicit `e2b:envd` / `e2b:code-interpreter` use the create response's `envdAccessToken`.
- Every bare legacy port, other e2b legacy ports, and explicit `forward` use `forwardAccessToken`.
- `trafficAccessToken` is for verification by external gateways and e2b data-plane components; the node proxy does not consume it.
- `exec` accepts only a `kat1` ExecAccessToken directly HMAC-signed with ServiceSecret, bound to `StableID`, with `aud=exec`. Envd/Forward/Traffic tokens cannot replace it.

Opaque Envd/Forward tokens use their respective wire-format validation. Exec KATs strictly validate format, signature, SID, audience, and optional expiry.

`auth` / policy `auth_mode`:

| Mode | Behavior |
|---|---|
| `enforce` | A mismatch returns 401 |
| `log` | Log mismatches but allow forwarding |
| `off` | Skip validation |

This table applies only to ordinary data traffic. Exec always enforces authentication, regardless of `auth_mode`. The node uses its own effective policy; router `enforce` does not change it. In node `log`/`off`, invalid ordinary credentials can pass into parking, activation, and backend dial.

For e2b legacy 49983 `GET/POST /files` without `X-Access-Token`, the node can verify envd's signature query with EnvdAccessToken. In `enforce`, the proxy verifies before forwarding, and envd independently verifies the same original request. A nonempty wrong `X-Access-Token` never falls back to a signature. These checks follow the ordinary node policy: `log` can forward a mismatch and `off` skips verification. Explicit logical services and bare port 49983 do not inherit the legacy signed-file exception.

<a id="7-mmds"></a>
## 7. MMDS

With `mmds.enabled=true`, envd in FC mode obtains the current identity's access-token hash through Firecracker MMDS v2. `mmds.routes.enabled=true` additionally exposes explicitly declared static/secret/service exact routes. Only Proxy workers serve HTTP; the master supplies the bounded route view. With `proxy_netns`, the master binds the conductor-projected `mmds.listen` in that namespace and passes the same listener FD to all workers. Workers read no proxy/MMDS YAML.

```text
guest envd
  │ 169.254.169.254:80
  ▼
connector mgmt-extract
  │ conductor mmds.listen
  ▼
proxy worker ─┬─► shared route view(token identity + RunID)
              └─► master socketpair RPC(routes + values + service socket)
```

The two-stage protocol mints a token and then uses it for either root or declared-path GET:

1. `PUT /latest/api/token` requires exactly one `X-metadata-token-ttl-seconds`, in `1..21600`. It finds the enriched starting/running route's FloatingIP using the request source IP. Before initial starting has a FloatingIP, only this minting path may wait for `park_timeout`. The returned HMAC token binds SID, source IP, current `RunID`, `aud=mmds`, and expiry.
2. `GET /` revalidates signature, source, expiry, audience, and current `RunID`, then returns `{instanceID, envID, address, accessTokenHash}` (`address` is currently empty). Pause/resume changes the incarnation, invalidating previous tokens.
3. `GET <declared-path>` performs the same authentication and then exact lookup. Undeclared routes or declared secrets without a configured value return 404; unavailable storage/sync returns 503. Static/secret Content-Type defaults to `text/plain` only at response time, without rewriting configuration.

The deterministic per-sandbox `mmds_secret` lets different PUT/GET workers validate each other's session tokens. Request paths are neither cleaned nor redirected. Query, fragment, percent escapes, characters requiring percent encoding, empty/dot segments, backslashes, wildcards, and trailing slashes except root are rejected. GET with nonzero/unknown Content-Length, any Transfer-Encoding, or an unknown body returns 400; the handler does not probe by reading even one body byte. Built-in root matches only `/`. Every guest response includes `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`.

Three custom route types exist:

- `static` directly returns declared UTF-8 `data`.
- `secret` retrieves current opaque bytes by the route's `secret` name. PUT replaces the whole value; DELETE immediately makes GET return 404. There is no waiting or TTL, and Content-Type always comes from the route.
- `service` resolves a local Unix socket from the master's sole registry. The worker/handler creates a new `GET <exact-path> HTTP/1.1` with `Host: mmds-service` and only the additional headers `E2b-Sandbox-Id: <sid>` and `E2b-Sandbox-Service: <service>`. It sends no port and forwards no guest Host, query, body, token, Authorization, Cookie, or other guest headers. V1 relays only a valid status, bounded body, and valid Content-Type (default `text/plain`), without following redirects. Timeout maps to 504; oversized/invalid responses to 502; missing services or unreachable sockets to 503.

The security boundary distinguishes portable route declarations from secret values held only as SQLite ciphertext or in the trusted Proxy master's bounded heap. Values do not enter ordinary metadata, shared mmap, logs, metrics, migration tokens, templates, or build artifacts, and are never sent to observers/node-link. Once a guest actively GETs a value, however, it enters guest/application memory. A later Pause/snapshot including memory may capture that copy as ordinary guest working set. The platform cannot erase it from arbitrary guest memory from the host. Callers should limit application residency and protect snapshots containing consumed secrets as sensitive artifacts.

<a id="8-per-sandbox-traffic-admission-与-stats"></a>
## 8. Per-sandbox traffic admission and stats

<a id="81-配置与合并"></a>
### 8.1 Configuration and merging

`traffic.max_inflight` specifies each sandbox's admitted logical inflight concurrency across the whole node Proxy. It is not QPS, bandwidth, worker capacity, or global Proxy capacity. A configured `M` is neither independently applied to each worker nor statically split into `ceil(M/N)`:

```yaml
traffic:
  max_inflight:
    total: 128
    forward: 96
    "e2b:envd": 16
    "e2b:code-interpreter": 8
    exec: 8
```

`0` makes that dimension unlimited. Total and target service are checked independently in one critical section; service limits need not sum to less than total. `forward` is not split by port. Reaching one sandbox's limit consumes or rejects no other sandbox's quota, preventing global failure amplification.

A sandbox stores only its explicit patch in metadata key `kuasar-sandbox.traffic`. Create/build may also provide the same JSON shape in exactly one `X-Kuasar-Sandbox-Traffic` header. Merge precedence is:

```text
template/group/default explicit metadata
  < create metadata
  < X-Kuasar-Sandbox-Traffic
  < target Proxy resolution only for absent leaves
```

The first three layers merge by `max_inflight` leaf and marshal canonically. An absent leaf inherits lower-priority explicit patches and finally current destination `proxy.yaml`; explicit `0` clears lower-priority or node-default limits. Empty or duplicate headers, `null`, unknown fields, negative/non-integer values, and uint32 overflow are rejected. Bare sandboxes also reject explicit `e2b:envd` or `e2b:code-interpreter`. Node defaults may contain every service; bare consumes only `total/forward/exec`.

Destination-node defaults are not written into sandbox metadata/structs/SQLite, Registry records, or MigrationToken. MigrationToken retains existing Metadata: absent traffic remains absent after migration and resolves against destination Proxy defaults; explicit patches migrate unchanged and override those defaults. V1 cannot modify metadata at runtime. Lowering a limit does not evict existing connections and affects only later acquire attempts.

<a id="82-共享-admission-arena-与误差证明"></a>
### 8.2 Shared admission arena and error-bound proof

Route mmap is separate from the mutable admission arena. For a route whose effective limits are all zero, the master publishes a zero binding. Workers take the existing `BeginParking` path without scanning the arena or IPC. Other routes receive stable `{slot,generation,effective limits}`. Each entry holds identity/state, generation, fixed limits, and `counters[worker][forward/envd/CI/exec]`; a worker writes only its own absolute cells.

Acquire runs under the existing worker-local per-sandbox entry lock:

```text
verify active generation and effective limits
→ atomically load every worker's four cells exactly once
→ check total and target service together
→ increment this worker's target-service cell
→ enter local parking
→ unlock and return success
```

All services of one sandbox in one worker share this lock, so each worker can have at most one acquire critical section in progress. First consider a monotone execution with no releases. Take the successful atomic increment that first brings the published count to `M` as the boundary publication. At that boundary, each other worker can have at most one acquire already started but not yet published: at most `N-1` altogether. The boundary worker can begin its next check only after unlocking. Any check starting after the boundary reads each column at a value no smaller than at the boundary. Even though these reads are not an atomic snapshot, their sum is at least `M`, so the check must reject. The increment is published before unlocking and returning the grant. Thus only those other `N-1` acquires already in progress at the boundary may still succeed, bounding the peak by `M + N - 1`.

Concurrent releases do not enlarge this bound. Fix any observation time `T`. Remove from the execution history every complete acquire→release interval of flows released before `T`. Removing positive-count intervals leaves the column values seen by remaining acquires unchanged or lower. Consequently every acquire that succeeded originally and remains active at `T` would still pass in the reduced history; same-worker serial order is also preserved. The reduced history has no release through `T`, and its published count equals the original execution's actual active count at `T`. The monotone-execution `M + N - 1` bound therefore applies. This argument requires no atomic snapshot across column reads and applies separately to total and target service checked within the same critical section. For configured `M` and `N` workers, the contract is:

```text
actual admitted inflight <= M + N - 1
```

The parking→egress transition does not change shared counts. Activation/dial/HTTP-forward failure, context cancellation, an ordinary response, and final Close of complete CONNECT/exec relay all release through the same flow/lease exactly once. Half-close does not release it.

Delete or identity replacement first makes the old generation unacquirable, then clears/reuses the slot. Old flows retain their old generation; a mismatched release must not decrement the new route's cell. Starting/running/paused updates in the same sandbox lifecycle retain generation. Activate rereads route identity and the complete binding around route/policy publication, and directly revalidates arena state for a drained limited generation. An old lookup therefore cannot bypass a newly published binding.

A worker stats-stream fault first terminates the worker. Only after supervisor `cmd.Wait` proves the old process exited and the kernel closed its connections may the master take over a leftover row guard and clear that index. Until then, stale-high counts can only conservatively reject, never undercount. Replacements reuse the index with a new epoch. Master exit terminates every child and connection; a new master rebuilds the arena without inheriting old counts. A full routesync on a surviving master instead preserves counts for replayed, unchanged bindings and retires absent bindings at Bookmark; it does not reset active flows' counts.

For `route_capacity=65536,workers=2`, counters alone occupy `65536 × 2 × 4 × 8 = 4194304` bytes. Including entry headers, row guards, and one transaction spare entry, the mmap is `8388800` bytes. Master and worker mapping validate size, stride, worker count/index, and eight-byte atomic alignment. Current support is limited to the project's Linux `amd64`/`arm64` scope.

Exhaustion for ordinary HTTP/non-exec CONNECT returns 429, `X-Kuasar-Proxy-Error: max_inflight_reached`, and a fixed body. It sets no `Retry-After`, performs no Wake/Activate/dial, emits no per-rejection log, and increments low-cardinality `data_requests_total{result="max_inflight_reached"}`. See §5 for exec's behavior after CONNECT 200.

<a id="83-traffic-stats-与统一-worker-stream"></a>
### 8.3 Traffic stats and the unified worker stream

The public API is `GET /sandboxes/{sid}/stats/traffic`. It counts logical ingress admitted by the final node proxy's authentication policy, not physical client TCP connections:

```text
ingress = parking + egress

parking: after authentication-policy and ExecRequest admission; ActivateRoute/ActivateExec and final backend dial are incomplete
egress:  final node proxy → sandbox backend is established and has not reached final Close
```

Services are fixed to `forward`, `e2b:envd`, `e2b:code-interpreter`, and `exec`. e2b returns all four; bare returns only forward/exec. Each ordinary HTTP request and each CONNECT/exec tunnel counts as one logical ingress. Successful dial atomically performs `parking--/egress++` under the same worker-local entry lock. Activation/dial failure only ends parking. `CloseWrite` propagates half-close without ending egress; only the tracked backend's final `Close`, guarded by `sync.Once`, ends egress. Token or ExecRequest rejection does not enter parking/egress or refresh sandbox activity/`idleSince`.

Example idle response for a bare sandbox (inapplicable e2b limits are zero):

```json
{
  "state": "running",
  "maxInflight": {
    "total": 128,
    "forward": 96,
    "e2b:envd": 0,
    "e2b:code-interpreter": 0,
    "exec": 8
  },
  "inflight": {
    "parking": 0,
    "egress": 0
  },
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "forward": {
      "parking": 0,
      "egress": 0,
      "idleSince": "2026-08-12T14:03:21.123456789Z"
    },
    "exec": {
      "parking": 0,
      "egress": 0,
      "idleSince": "2026-08-12T14:00:00Z"
    }
  }
}
```

The Proxy master injects `maxInflight` from the current applied route's effective policy, not the conductor Sandbox row. An all-zero object explicitly means unlimited. Unavailable worker stats can still make this API return 503, without affecting master route/admission authority or the Create barrier.

Top-level `idleSince` appears only for state=running with all inflight counts zero. Starting/paused omits it even with zero connections. A service's `idleSince` likewise appears only when both of its counts are zero. The API returns no `idle`, `idleForSeconds`, last-open/close, cumulative connection counts, bytes, latency, port breakdown, or worker identity, and sets `Cache-Control: no-store`. Unsynchronized Proxy routes, RunID/profile/state mismatches, or an untrusted worker set return 503. State participates in the conductor→master query identity, preventing a stale top-level `idleSince` after Pause commits while the asynchronous route view still says running.

Each worker reports through one Unix socketpair:

```text
worker hot path
  update sharded local absolute state + revision
  mark SID/counter dirty + nonblocking notify
        │
        ▼ async single sender
[4-byte LE length][JSON hello/ready/update/remove/goodbye]
        │ absolute Prometheus counters + absolute per-SID traffic
        ▼
master: workerID/epoch/sequence/contribution → per-SID aggregate cache
        ├─ counter absolute delta → existing metrics.M
        └─ stats UDS batchGet → conductor public GET
```

The hot path never writes the socket. One sender may coalesce arbitrary intermediate changes and clears dirty only if revision has not changed again after a successful write. Coalesced notifications and backpressure therefore cannot lose final absolute state. Frames have a 1 MiB limit plus bounds on SID/counter counts and identifier lengths. Within an epoch, backward/skipped sequence numbers, sequence reuse with different content, counter regression/omission, and malformed frames are protocol errors.

Master queries read a continuously maintained aggregate cache, without GET-time fan-out to workers. It compares `idleSince` using Linux boottime and emits only UTC wall time. The effective timestamp is the maximum of worker idle time, the trust start of the current master/worker set, and the first observation of current RunID as running. A disconnected worker stats stream immediately starts a 503 window and terminates that worker. Only after `Wait` confirms exit and kernel closure of backend FDs does the master remove all of its contributions. A replacement sends hello+ready with a new epoch. Stats remains unavailable until ready, and the worker starts serving data listeners only after stats readiness.

Workers send absolute state to the master over socketpairs; the conductor queries only the currently registered `stats_socket`. Route SHM remains master-write/worker-read-only, without a stats area or worker writes. Mutable worker admission columns exist only in the separate arena, never in route records that can move during backshift.

<a id="9-可靠性"></a>
## 9. Reliability

- **Worker crash:** a stats fault first terminates the old worker. The master waits for `cmd.Wait`, clears its admission column, and restarts the same index with a new epoch. Stale-high counts only conservatively reject while waiting. Other workers keep accepting on the same listener FD. Routes, sandbox state, and Create barriers do not change. The kernel closes existing connections on the crashed worker.
- **Master crash:** the plugin lease disconnects, new Create cannot pass its barrier, and DataEndpoint becomes unavailable. systemd restart registers a new master, rebuilds shared tables, and starts workers. Already running sandboxes themselves are unaffected.
- **routesync disconnect:** the master reconnects with exponential backoff. Fixed data-plane routes retain their existing retention/Bookmark convergence semantics, but MMDS route/value/service authority clears immediately and returns 503. Old secrets and service routes are not served before a full-sync Bookmark. Disconnect also fails pending Create barriers. A disconnect after ACK and completed 201 commit is an ordinary runtime availability failure.
- **Park/wake:** Lookup sends no Wake. Only Activate admitted by the effective authentication policy may wake a paused SID and wait for shared-table updates. Starting only parks, never wakes; paused/Delete immediately ends that wait. Resume ownership and current-launch progression remain with the conductor.
- **Stats stream:** EOF, timeout, or protocol error on any worker stream stops that worker. Traffic GET returns 503 until confirmed exit; then contributions are removed and readiness waits for the replacement. Prometheus counters remain monotone throughout the master's lifetime and do not regress on worker epoch changes.
- **Failure codes:** unavailable Create proxy stream, barrier timeout/disconnect, or route-apply failure = 503; invalid target = 400; non-CONNECT exec = 405; unknown/deleted SID = 404; rejected authentication = 401; recognized service unsupported by the profile = 501; unregistered/unreachable backend or proxy = 502; authorized exec resume failure = 503; ordinary admission exhaustion = 429 plus `max_inflight_reached`; exec exhaustion = generic ctl error after CONNECT 200.

<a id="10-性能"></a>
## 10. Performance

- Ordinary data-plane lookup is a worker-local mmap hash lookup, with no conductor call or cross-process RPC. Only custom guest MMDS paths use local worker→master socketpair RPC.
- Unlimited traffic never accesses the admission arena. Limited flows scan fixed `N × 4` absolute cells without per-flow master RPC. Different sandboxes use different worker-local mutexes, avoiding a process-global lock; same-SID concurrency serializes only within that worker's entry and row.
- The master alone writes route SHM; workers only read it. Admission workers write their own columns, and the master is absent from the per-flow hot path.
- Neither ordinary HTTP nor CONNECT uses upstream connection pools, avoiding reuse across sandboxes/ports.
- `route_capacity` is a fixed protective capacity. Increase it and restart the proxy master when more capacity is needed.
- Worker metrics and traffic report absolute snapshots asynchronously over the unified stats socketpair. At `metrics_listen`, the master differences absolute counters and continues exposing existing `data_requests_total{result=...}` metrics. Backpressure coalesces intermediate snapshots without permanently losing counts or current traffic.
- MMDS source-IP reverse lookup uses one reconstructible fixed slot indexed by IPv4 modulo connector `MaxPorts`, then validates the candidate SID against the authoritative primary route table, including active state and exact IP. It is no longer a linear table scan. Missing, mismatched, or stale hints fail closed; a modulo collision displaces the previous source hint rather than authorizing it as the new source. This hint holds no credentials and is not an ordinary data-plane route authority. Token minting uses the source lookup; authenticated GET validates the token's source/IP/incarnation binding, and custom GET additionally uses master RPC. MMDS is not limited to envd initialization.

Here running proves only that the orchestrator readiness wire and mandatory e2b `/init` succeeded. It does not guarantee that the code interpreter, forwarded business port, or user application is listening/healthy. [#125](https://github.com/kuasar-sandbox/orchestrator/issues/125) separately tracks business-backend readiness; the proxy adds no generic dial retry in this phase.

<a id="11-see-also"></a>
## 11. See Also

- [node.md](node.md) — conductor control plane, Proxy deployment, lifecycle, and key model.
- [cluster-router.md](cluster-router.md) — how cluster ingress forwards to this node's data plane.
- [Connector vSwitch](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch.md) — mgmt-extract and MMDS VIP translation.
- [Deployment](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md) — topology, ports, and failure domains.
