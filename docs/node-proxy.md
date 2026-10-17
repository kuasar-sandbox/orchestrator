[English](node-proxy.md) | [简体中文](node-proxy_zh.md)

# node-proxy — Node data-plane forwarding

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

### 1.1 Design principles

Sandbox metric collection/history is owned by the separate
[Telemetry component](telemetry.md), not Proxy's traffic counters or MMDS.
Telemetry reuses the management namespace/packet path and MMDS FloatingIP
lookup rules, but directly accepts guest OTLP on its own listeners. Do not
forward OTLP through the application data proxy; Conductor's metrics query uses
the live telemetry registration's separate API UDS, not Proxy StatsSocket.

- **Separate forwarding and control planes:** the proxy decides routes, applies authentication policy, and forwards bytes. The conductor owns sandbox lifecycle authority.
- **One subscribing master, multiple forwarding workers:** only the proxy master registers a config-socket plugin. Workers neither connect to the conductor nor hold independent routesync subscriptions.
- **Separate route views:** fixed-length data-plane fields enter shared memory, which workers mmap read-only. Variable-length `mmds_routes` and `mmds_route_secret_values` remain in the master's bounded heap. Workers query an exact path through inherited local socketpair RPC. Secret-value plaintext never enters mmap.
- **Inherited listener FDs:** configuration selects a TCP address or a named activated listener for data and MMDS independently. The master gives each worker its own descriptor for the same listening socket. Service-owned copies preserve listeners across InplaceRestart.
- **Configurable forwarding network namespace:** with `proxy_netns` pointing to the connector management namespace, workers run there; their `floatingip:port` access and the MMDS listener also use that namespace.
- **No upstream connection pool:** ordinary HTTP dials and closes a backend for each request. Each CONNECT binds one request to one TCP/UDS connection. Connections are never reused across sandboxes or ports.
- **Authentication policy precedes lifecycle effects:** ordinary HTTP and non-exec CONNECT follow `LookupRoute → authorize(RouteBinding) → TryBeginParking → ActivateRoute → fresh Route → dial`. Lookup never wakes, resumes, parks, or dials; Activate revalidates the binding before and after lifecycle effects. Invalid ordinary credentials cannot wake a sandbox or consume traffic parking/admission when the node's effective policy is `enforce`. Node `log`/`off` modes admit ordinary requests according to §6.
- **Logical service selection belongs to CONNECT:** ordinary HTTP does not use `E2b-Sandbox-Service` to choose a backend and otherwise forwards it as an application header. The explicit exception is the exact value `exec`, which returns 405 before activation. For CONNECT, an explicit service is authoritative for backend selection.
- **Exec authenticates before activation:** `service=exec` always validates a KAT bound to `StableID`. After CONNECT 200, it must also authorize the complete first ExecRequest. Failure cannot trigger parking, Wake/resume, or a backend dial. The final node proxy gives the ctl tunnel helper only requests that pass both gates; it exposes neither arbitrary UDS endpoints nor other ctl capabilities to tenants.
- **Deterministic MMDS keys:** `MmdsSecret = HMAC-SHA256(manifest_key, "kuasar-mmds-v1:" + sid)`, with the hex manifest key decoded to bytes. PUT and GET therefore agree even when different workers serve them.

## 2. CLI

Start the node data plane as one proxy master process:

```bash
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
```

`node-ctl` runs the built-in master or replaces itself with a statically customized master selected by `paths.proxy_executable`. The master always reexecutes its own current executable to start workers. The internal worker mode is not an operator interface.

Static customization interfaces, configuration hooks, runtime binding, private route sources and authorized forwarding are defined in [Extensions](extensions.md#proxy-bootstrap).

`proxy.yaml` fields:

| Field | Default | Meaning |
|---|---|---|
| `config_socket` | `/run/sandbox/node-ctl.socket` | Conductor config-socket; the master registers on its plugin plane and synchronizes routes |
| `paths.proxy_executable` | Empty | Absolute executable for a statically customized Proxy master; empty uses the built-in implementation. Static diagnostics check regular/executable status, absence of group/world writability, and same-file identity, without inferring ownership from the diagnostic command's EUID. Actual root dispatch requires root ownership; non-root dispatch accepts root or its own EUID. It selects node-ctl → master dispatch and the target opened when InplaceRestart is accepted; workers use their own master's image |
| `paths.run_root` | Required | Node RunRoot; workers construct `<run_root>/sandboxes/<NodeSandboxID>/ctl.sock` locally. This path is not transmitted in routesync `Policy` or shared route records |
| `data_listen` | Required | The node's sandbox data ingress: TCP `host:port`, or `fd=<name>` for an explicitly supplied named TCP listener |
| `shutdown_timeout` | `0` | Non-negative duration; zero disables InplaceRestart and makes automatic termination immediate. Explicit App.Shutdown still drains gracefully. A positive budget is shared by data/MMDS and, for full stop, all instances |
| `inplace_restart` | `false` | Register SIGUSR2 in the original master entry when true; direct `App.InplaceRestart` works with either value |
| `proxy_netns` | Empty | Forwarding namespace; empty uses the current namespace. Otherwise workers run there and the conductor-projected MMDS address is bound there; `data_listen` stays in the master's current namespace |
| `stats_socket` | `<dir(config_socket)>/proxy-stats.sock` | Traffic-stats UDS owned and registered with the conductor by the master. Must be absolute, cannot conflict with config/SHM paths, and has mode 0600 |
| `shm_path` | `<dir(config_socket)>/proxy-routes.shm` | Shared route-table mmap file |
| `route_capacity` | `65536` | Fixed route-slot capacity; exhaustion fails Upsert and terminates the current routesync session. A Create waiting for that route returns 503 |
| `workers` | `1` | Number of worker processes |
| `tls` | Empty | Data-plane TLS `{cert,key}`; empty means h2c |
| `auth` | `enforce` | Data-plane authentication fallback before routesync policy arrives |
| `park_timeout` | `30s` | Parking fallback before routesync policy arrives |
| `metrics_listen` | Empty | Master Prometheus text endpoint aggregating worker data-plane counters |
| `traffic.max_inflight.total` | `0` | Node-wide per-sandbox inflight limit across all applicable services; `0` means unlimited |
| `traffic.max_inflight.forward` | `0` | Per-sandbox `forward` inflight limit; `0` means unlimited |
| `traffic.max_inflight."e2b:envd"` | `0` | Per-e2b-sandbox envd inflight limit; `0` means unlimited |
| `traffic.max_inflight."e2b:code-interpreter"` | `0` | Per-e2b-sandbox code-interpreter inflight limit; `0` means unlimited |
| `traffic.max_inflight.exec` | `0` | Per-sandbox native exec inflight limit; `0` means unlimited |

`proxy.yaml` contains neither `mmds_listen` nor `services`. Their sole sources are conductor `mmds.listen` and `mmds.services`, delivered in `Hello{Policy}` through trusted plugin registration.

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

- A/A′ register as `proxy`; temporary B uses a private instance ID. IDs identify ownership and same-name replacement. The admitted registration's `Proxy` capability identifies its role; `StatsSocket` is a separate optional traffic-stats address.
- Only the master listens on `stats_socket`. The conductor's public traffic GET reads the master's aggregate cache through this UDS, without querying workers on demand. A missing `StatsSocket` does not remove the route-barrier participant.
- Workers do not register plugins or hold independent complete route tables. Route mmap remains master-write/worker-read-only; in admission mmap, each worker writes only its own absolute-counter column. The master restarts crashed workers, and replacements read the current route/admission view directly.
- Master exit takes its workers down. After systemd restarts the master, it registers again and rebuilds shared tables.
- Admitted registrations must satisfy `subscribe.kind=route_wake`, `proxy!=nil`, and `mmds=true` before the conductor projects MMDS policy, routes, and secret values. Connection admission, valid MMDS state and registration epochs remain enforced. Ordinary observers and node-link never receive these confidential values.

## 4. routesync and shared route views

routesync remains framed JSON over h2c; the proxy master dials the conductor:

```text
master → conductor : register{subscribe: route_wake, proxy{stats_socket?}, mmds}
master → conductor : wake{sid} / route_barrier_ack{barrier_id}
conductor → master : hello{policy, proxy_capabilities}
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

The barrier includes all valid Proxy participants, including A and B during replacement. After ACK, Create rechecks registration epochs and the participant set. Disconnect, same-ID replacement, and late or old-session ACKs cannot complete the barrier. All members must ACK before 201 can return. `Hello.proxy_capabilities` includes `inplace_restart_v1` when conductor supports these multi-instance semantics; its absence permits compatible single-instance service but rejects InplaceRestart before draining A.

With conductor `paths.plugin_pidfile` configured, the PID allowlist remains enforced. The original coordinator opens B's dedicated conductor connection and passes its descriptor over private control, including on reconnect. The connection retains the authorized coordinator PID. B still registers separately and receives its own route stream; other processes gain no new admission rule.

The MMDS extension is not stored in fixed records. For each active sandbox, the master replaces routes and values under one lock; `route_capacity` bounds the heap-entry count. `BeginSync` and routesync disconnection immediately clear the heap and mark it unavailable. A new master starts with an empty view; a full `Bookmark` is required before queries reopen. A worker-only restart does not clear the surviving master's heap; its replacement reconnects through inherited RPC and waits for the shared view to be synchronized before serving. Each `Hello{Policy}` atomically replaces the whole service registry. Running/starting Upsert updates the heap before publishing the SHM route; paused/Delete revokes the heap before updating SHM. Thus workers that sampled an old active row also fail closed, preventing a new lifecycle from being combined with old secrets. `RunID` enters fixed records to bind MMDSv2 tokens to an incarnation. MMDS route declarations and secret-value plaintext never enter fixed records, metrics, or logs.

Here `RouteEntry.SandboxID`, `sid`, and shared-table keys are node-local SandboxIDs. In cluster operation they are Registry-allocated NodeSandboxIDs; the cluster Router translates the public stable SandboxID before reaching the node. `RouteEntry.StableID` preserves sandbox identity across NodeSandboxID changes for KAT/credential binding and is not the shared-table lookup key. routesync V3 made the hard switch to `stable_id`; V4 replaced Snapshot-specific location with `artifact_location`, orthogonal to E/S kind; V5 split node registration into `api_endpoint` and `data_endpoint` and narrowed Proxy registration. Subscribers and node-link clients validate the version in the first Hello and terminate mismatched sessions before handling routes or commands.

The shared table is a fixed-capacity open-addressed hash table with a single master writer. Each record has a seqlock. Workers retry when a write is in progress or the version changes, never observing a partial route. Workers depend only on local shared-table reads:

```text
sid hash ─► record slot ─► RouteEntry (read-only Lookup)
                         ├─ running → policy admission, Activate recheck, then forward
                         ├─ starting → policy admission, park without Wake; end on rollback
                         └─ paused → wake pipe and park only after policy admission
```

Ordinary route Lookup never writes the wake pipe for missing, paused, or starting routes. Only after a request passes the returned `RouteBinding`'s authentication policy may Activate write a wake for a paused SID. A starting sandbox already has a conductor launch owner; Activate only waits for running/delete/paused updates and must not send another Wake. The latter two rollback updates immediately terminate the starting request. The master deduplicates requests and sends upstream routesync `Wake`. After each table write it wakes worker-local parking waiters through a notify pipe. Global revision/notification merely triggers rechecking; workers use that SID's live or terminal revision to determine whether Wake received a terminal response. Live route, terminal cache, and revision are read in one table-seqlock snapshot, preventing an old missing/paused route and new revision from forming a false terminal result. Identical paused Upserts do not advance per-SID revision, so subscription replay cannot impersonate Wake completion. A change only to pause reason, recovery obligation, pressure version or running interval updates SHM and global notification while retaining the activation revision; explicit resource-pause adoption must reach all readers without impersonating a failed launch. Live-hash deletion backshifts and immediately reclaims slots under the same table seqlock. The terminal cache holds at most 4096 entries and no credentials. Under extreme churn, collisions only evict older terminal correlations; affected waiters conservatively keep parking until a later state or timeout, without misrouting or treating an unrelated SID as the Wake result.

On the normal activation path, terminal revision still exposes rollback promptly even when asynchronous convergence coalesces intermediate starting and subsequent paused/Delete updates. Only extreme cache eviction degrades to a conservative timeout. While an authorized request remains undelivered and the same binding is still paused, activation may retry Wake at most once per second within its original park deadline. This lets an ordinary paused request re-evaluate node eligibility after critical relief. The master deduplicates queued Wakes and conductor retains one launch owner. Once starting is observed, or a lifecycle revision proves a coalesced starting→paused rollback, that request ends without another Wake. Ordinary Lookup itself never sends Wake.

The shared view is an asynchronously converging route cache. Default creation uses UUIDs; cluster NodeSandboxIDs use `<stableSandboxID>-g<SandboxGeneration>`. Normal operation does not reuse one node-local ID for distinct logical sandboxes. If an external system immediately assigns a just-deleted NodeSandboxID to a different logical sandbox, workers can briefly retain the previous instance's projected credentials and same-name run directory before Delete/new Upsert arrives. Nodes must not deliberately perform this immediate cross-sandbox ID reuse. Explicit migration targets for different logical sandboxes must use a fresh NodeSandboxID or first confirm route-view convergence.

Protected `RouteEntry` states are `starting|running|paused|dead`. Entries explicitly carry `StableID`, `APISecret`, `APISecretFingerprint`, `ManifestKeyFingerprint`, `ServiceSecret`, `EnvdAccessToken`, `TrafficAccessToken`, and `ForwardAccessToken`. Raw ManifestKey never enters routes. Node forwarding selects only EnvdAccessToken or ForwardAccessToken for its target. TrafficAccessToken is projected in the protected view for external gateways and e2b data-plane components, but the node platform layer does not consume it. `StableID + ServiceSecret` verifies exec KATs; shared-table keys and local run directories still use NodeSandboxID exclusively. Existing `MmdsSecret` independently signs MMDS tokens and is not a route secret value; route secret values enter only the master's heap through the trusted projection described above.

On the routesync wire, `MaxInflightPatch` carries only explicit sandbox leaves. The master merges it with destination-node `proxy.yaml` defaults and writes fixed effective values plus `{admission slot,generation}` into route SHM. Pointers enter neither SHM nor route equality. Current internal boundaries are routesync version 8, route SHM schema 9, worker bootstrap/config/FD protocol version 3, and admission arena version 2. These are hard compatibility cuts: mismatched conductor/proxy/registry/router protocol versions or old SHM/bootstrap formats fail closed.

Resource-paused and explicitly paused routes both park after the existing authentication policy. The node decides eligibility from current durable intent: adoption removes resource auto-recovery and its critical exemption, but a real still-parked request or later Wake can use ordinary rules. Starting continues to wait for the new running endpoint. No delivered nonidempotent request is replayed, and previously killed native exec children are not preserved. Pressure facts also survive full route synchronization/Bookmark and extension projection; they do not enter NodeList or Cluster placement decisions.

The initial durable starting Upsert may lack a FloatingIP, UDS, or any other backend endpoint. Workers park by state and never attempt those empty fields. After the node persists network ownership and completes YAML/ready.sock preparation, it publishes enriched starting; only then can MMDS identify it by FloatingIP. Ordinary data traffic still waits for running.

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
5. It dials envd UDS or `floatingip:port` from that running route. With `proxy_netns`, the floating-IP dial occurs in that namespace.
6. It writes one HTTP request and streams the response, or relays a negotiated HTTP/1.1 WebSocket upgrade. Response completion or final relay completion closes the flow and releases its quota exactly once.

Ordinary HTTP observes the selected route's running state, RunID, backend address and authorization identity for its current target until the exchange ends. A withdrawn route cancels its transport without waiting for TCP retransmission timeouts. Before any guest HTTP bytes are written, the same admitted request may activate its original binding again and dial the latest endpoint; it retains its one admission/traffic flow and the client's context/deadline. Once forwarding starts, cancellation closes the backend and uses the existing upstream-error handling; it never replays HTTP bytes. Policy updates, including limited/unlimited transitions and changes to other services' credentials, do not evict an already accepted transport; any later reactivation still checks the original full binding. This is local transport availability behavior, not a packet-isolation condition; connector generation remains independent. CONNECT keeps its existing stream and half-close behavior.

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

### 5.1 HTTP/1.1 WebSocket forwarding

Standard ingress and privately authenticated `ForwardAuthorized` use the same
HTTP exchange. The latter applies `Rewrite` to a clone before final transport
normalization; rejection writes no guest request bytes. An HTTP/1.1 GET carrying
`Connection: Upgrade` and `Upgrade: websocket` keeps the upgrade handshake.
Connection tokens may be mixed-case, comma-separated, or split across header
values. Unrelated hop-by-hop headers, including fields nominated by Connection,
are removed. Endpoint handshake headers, cookies, Origin and application
authorization remain unless nominated as hop-by-hop fields. The endpoints
validate the key, version, origin and subprotocol.

A regular upstream response, including handshake rejection, is streamed normally.
Only a matching HTTP/1.1 WebSocket 101 transfers the response to a bidirectional
relay. An unexpected or mismatched 101 returns 502 before hijack; a writer that
cannot hijack, including through `Unwrap`, returns 500 `upgrade unsupported`.
After hijack, failures close the transport without writing another HTTP error.
Both readers' prefetched bytes survive. A clean EOF half-closes the peer and
drains its tail; request cancellation closes both sides and joins the relay.

One upgraded connection retains the original tracked backend, admission lease
and connected count until relay completion. Frames do not create new ingress or
request-result counts. The cluster router uses this same HTTP exchange over its
existing node CONNECT tunnel, retaining its buffered reader and identity rewrite;
there is no second WebSocket implementation or replay of the inner request.
No new configuration, extension API, frame processing, or HTTP/2 extended CONNECT
is introduced.

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
- `secret` retrieves current opaque bytes by the route's `secret` name. PUT replaces the whole value. DELETE commits removal before publishing an asynchronous route update; guest GET returns 404 after the Proxy projection applies it. There is no deletion barrier, cancellation of an already-resolved response, value wait or TTL. Content-Type always comes from the route.
- `service` resolves a local Unix socket from the master's sole registry. The worker/handler creates a new `GET <exact-path> HTTP/1.1` with `Host: mmds-service` and only the additional headers `E2b-Sandbox-Id: <sid>` and `E2b-Sandbox-Service: <service>`. It sends no port and forwards no guest Host, query, body, token, Authorization, Cookie, or other guest headers. V1 relays only a valid status, bounded body, and valid Content-Type (default `text/plain`), without following redirects. Timeout maps to 504; oversized/invalid responses to 502; missing services or unreachable sockets to 503.

The security boundary distinguishes portable route declarations from secret values: control-plane persistence is SQLite ciphertext, with bounded trusted conductor/Proxy-master heap use at runtime. The platform does not automatically project values into ordinary metadata, shared mmap, logs, metrics, migration tokens, templates or Build artifacts, or send them to observers/node-link. Once a guest actively GETs a value, it can copy it into application memory or files. Later Pause/snapshot or image export may retain those guest copies. Host DELETE cannot erase already-consumed copies. Callers should limit application residency and protect any memory, disk or image artifacts containing them as sensitive data.

Sandbox-local MMDS is a local object/route mechanism, not a cluster Secret API, generic CONNECT configuration surface or placement/admission feature. Standalone CONNECT parses secrets-only import only when the target is absent and actually imported; an existing target skips it. The complete migration boundary is in [Node §8.1.4](node.md#814-publication-templates-and-migration). Request-scoped Build Register MMDS remains encrypted under Build ownership on the selected node ([Build §3.1](node-build.md#31-request-scoped-builder-input)). Incidental ordinary metadata propagation is not a cluster-wide MMDS support or E2E acceptance claim.

## 8. Per-sandbox traffic admission and stats

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
cluster group explicit metadata (when present)
  < create metadata
  < X-Kuasar-Sandbox-Traffic
  < target Proxy resolution only for absent leaves
```

The first three layers merge by `max_inflight` leaf and marshal canonically. An absent leaf inherits lower-priority explicit patches and finally current destination `proxy.yaml`; explicit `0` clears lower-priority or node-default limits. Empty or duplicate headers, `null`, unknown fields, negative/non-integer values, and uint32 overflow are rejected. Bare sandboxes also reject explicit `e2b:envd` or `e2b:code-interpreter`. Node defaults may contain every service; bare consumes only `total/forward/exec`.

Build registration traffic applies only to the Build runtime and its synthetic route. It does not become a default for later Sandboxes created from the output artifact. Canonical TemplateID Create does not load retained Build rows, a Template catalog, or artifact metadata to recover traffic policy. Standalone Create starts with its explicit request patch; cluster Create may additionally carry explicit group defaults.

Destination-node defaults are not written into sandbox metadata/structs/SQLite, Registry records, or MigrationToken. MigrationToken retains existing Metadata: absent traffic remains absent after migration and resolves against destination Proxy defaults; explicit patches migrate unchanged and override those defaults. V1 cannot modify metadata at runtime. Lowering a limit does not evict existing connections and affects only later acquire attempts.

### 8.2 Shared admission arena and error-bound proof

Route mmap is separate from the mutable admission arena. For a valid route whose effective limits are all zero, the master publishes a zero binding. Workers take the existing `BeginParking` path without scanning the arena or IPC. Limited routes receive `{slot,generation,effective limits}` in route SHM. Arena v2 stores only a master-written active generation per slot and generation-tagged `counters[worker][forward/envd/CI/exec]`. Each live worker alone writes its rows. Its process-local per-slot mutex serializes row initialization, acquire and release; master never acquires that mutex or clears a live row. There is no shared guard, identity hash, draining state, or duplicate mutable limit policy. Local locks are indexed by slot, not merely by SandboxID.

Acquire runs under the process-local per-slot mutex (shared by all services for that slot):

```text
verify active generation and effective limits
→ initialize own row for a new generation: tag=0, reset four cells, publish new tag
→ read each row's tag, four cells, then tag again; include only unchanged matching tags
→ check total and target service together
→ recheck active generation, increment own target-service cell, recheck again
→ on revocation undo increment under the same local slot mutex and reject
→ unlock the slot mutex and return the admission lease
→ enter local parking under the distinct per-Sandbox traffic-entry mutex
```

All services of one sandbox in one worker share this lock, so each worker can have at most one acquire critical section in progress. First consider a monotone execution with no releases. Take the successful atomic increment that first brings the published count to `M` as the boundary publication. At that boundary, each other worker can have at most one acquire already started but not yet published: at most `N-1` altogether. The boundary worker can begin its next check only after unlocking. Any check starting after the boundary reads each column at a value no smaller than at the boundary. Even though these reads are not an atomic snapshot, their sum is at least `M`, so the check must reject. The increment is published before unlocking and returning the grant. Thus only those other `N-1` acquires already in progress at the boundary may still succeed, bounding the peak by `M + N - 1`.

Concurrent releases do not enlarge this bound. Fix any observation time `T`. Remove from the execution history every complete acquire→release interval of flows released before `T`. Removing positive-count intervals leaves the column values seen by remaining acquires unchanged or lower. Consequently every acquire that succeeded originally and remains active at `T` would still pass in the reduced history; same-worker serial order is also preserved. The reduced history has no release through `T`, and its published count equals the original execution's actual active count at `T`. The monotone-execution `M + N - 1` bound therefore applies. This argument requires no atomic snapshot across column reads and applies separately to total and target service checked within the same critical section. For the current generation, stable positive limit `M` and `N` workers, the contract is:

```text
actual admitted inflight <= M + N - 1
```

The parking→connected transition does not change shared counts. Activation/dial/HTTP-forward failure, context cancellation, an ordinary response, and final Close of complete CONNECT/exec relay all release through the same flow/lease exactly once. Half-close does not release it.

Delete or identity replacement revokes the old active generation without waiting for a live worker, including one stopped inside acquire. The master's allocation transaction retains one spare slot: revoke the old binding and allocate the new binding before publishing the route; commit retires the old slot, while rollback retires the unpublished slot and reactivates the old generation without clearing outstanding counts. On first use of a new generation, each worker initializes only its own row: store tag=0, reset all four counters, then publish the new tag. Scans read tag/counters/tag and include only unchanged tags matching the current generation. A late release either precedes that worker's initialization or sees the new tag and does nothing; it cannot decrement the new identity. Acquire rechecks generation before and after publishing its increment and undoes a revoked increment under the same local slot mutex. Complete RouteBinding/ExecIdentity equality still fences activation credentials and policy; arena Valid checks generation liveness only. Ordinary starting/running/paused updates and unchanged full-sync replay preserve generation and counts.

This bound does not combine old and new identities into one quota or promise to evict existing flows when a limit decreases. Worker placement is not a static M/N quota: one worker may use all M, but multiple workers cannot each independently use M. No watchdog, lock timeout, kill-on-contention policy, per-flow master RPC or global limiter is introduced.

A worker stats-stream fault first terminates the worker. Only after supervisor `cmd.Wait` proves the old process exited and the kernel closed its connections may the master clear that index, without any shared lock. Until then, stale-high counts can only conservatively reject, never undercount. Replacements reuse the index with a new epoch. Master exit terminates every child and connection; a new master rebuilds the arena without inheriting old counts. A full routesync on a surviving master instead preserves counts for replayed, unchanged bindings and retires absent bindings at Bookmark; it does not reset active flows' counts.

For `route_capacity=65536,workers=2`, counter payload occupies `65536 × 2 × 4 × 8 = 4194304` bytes. Including generation tags, headers and one transaction spare entry, arena v2 mmap is `5767320` bytes. Each worker separately holds 65537 local mutexes, or `524296` bytes when `sync.Mutex` is eight bytes. These mutexes are not shared mappings. Master and worker mapping validate size, layout version, stride, worker count/index, and eight-byte atomic alignment. Current support is limited to the project's Linux `amd64`/`arm64` scope. Only the admission arena changes from v1 to v2, not user configuration or routesync; master and workers must share the same executable/bootstrap source set.

Tests retain 1/2/4/8-worker threshold interleavings, service/total checks, skew, release waves, rollback and real child-process crash cleanup. SIGSTOP tests stop a real child inside acquire and require master route changes and a surviving worker to progress without first waking or killing it.

Worker shutdown retains both mappings until process exit, including when asynchronous extension code or hijacked handlers outlive `Run`. Only `cmd.Wait` permits clearing the exact worker epoch and starting its replacement; see [worker lifetime](#91-worker-lifetime-and-transport-cancellation).

If already-persisted traffic metadata is invalid, conductor keeps the actual Sandbox lifecycle and credentials and projects only `traffic_policy_invalid`. Master publishes no effective policy or enabled admission binding. This never fabricates `dead`/Delete, removes Registry ownership, disables MMDS, or interrupts full synchronization for other routes. Existing request and migration input validation remains strict.

Ordinary HTTP/non-exec CONNECT first follows the existing credential policy, then returns HTTP 503 with `route_error` before parking/acquire/Wake/Activate/dial. Privately authenticated `ForwardAuthorized` uses the same gate. Native exec retains KAT → CONNECT 200 → strict first frame/expiry/CEL → generic ctl rejection, without backend side effects. Traffic stats and extension `TrafficSource.Get` return unavailable rather than reporting zero limits; route lookups, `RouteSource.Get` and `WorkerHost.GetRoute` retain true facts. An invalid batch member makes the entire stats batch unavailable.

Exhaustion for ordinary HTTP/non-exec CONNECT returns 429, `X-Kuasar-Proxy-Error: max_inflight_reached`, and a fixed body. It sets no `Retry-After`, performs no Wake/Activate/dial, emits no per-rejection log, and increments low-cardinality `data_requests_total{result="max_inflight_reached"}`. See §5 for exec's behavior after CONNECT 200.

### 8.3 Traffic stats and the unified worker stream

The public API is `GET /sandboxes/{sid}/stats/traffic`. It counts logical ingress admitted by the final node proxy's authentication policy, not physical client TCP connections:

```text
ingress = parking + connected

parking: after authentication-policy and ExecRequest admission; ActivateRoute/ActivateExec and final backend dial are incomplete
connected:  final node proxy → sandbox backend is established and has not reached final Close
```

Services are fixed to `forward`, `e2b:envd`, `e2b:code-interpreter`, and `exec`. e2b returns all four; bare returns only forward/exec. Each ordinary HTTP request and each CONNECT/exec tunnel counts as one logical ingress. Successful dial atomically performs `parking--/connected++` under the same worker-local entry lock. Activation/dial failure only ends parking. `CloseWrite` propagates half-close without ending connected; only the tracked backend's final `Close`, guarded by `sync.Once`, ends connected. Token or ExecRequest rejection does not enter parking/connected or refresh sandbox activity/`idleSince`.

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
    "connected": 0
  },
  "idleSince": "2026-08-12T14:03:21.123456789Z",
  "services": {
    "forward": {
      "parking": 0,
      "connected": 0,
      "idleSince": "2026-08-12T14:03:21.123456789Z"
    },
    "exec": {
      "parking": 0,
      "connected": 0,
      "idleSince": "2026-08-12T14:00:00Z"
    }
  },
  "platform": {
    "rxPackets": 57,
    "rxBytes": 5108,
    "txPackets": 39,
    "txBytes": 8042
  },
  "transit": {
    "rxPackets": 2,
    "rxBytes": 196,
    "txPackets": 3,
    "txBytes": 294
  },
  "egress": {}
}
```

The Proxy master injects `maxInflight` from the current applied route's effective policy, not the conductor Sandbox row. An all-zero object explicitly means unlimited. Unavailable worker stats can still make this API return 503, without affecting master route/admission authority or the Create barrier.

Top-level `idleSince` appears only for state=running with all inflight counts zero. Starting/paused omits it even with zero connections. A service's `idleSince` likewise appears only when both of its counts are zero. The API returns no `idle`, `idleForSeconds`, last-open/close, cumulative connection counts, rates, latency, port breakdown, or worker identity, and sets `Cache-Control: no-store`. Unsynchronized Proxy routes, RunID/profile/state mismatches, or an untrusted worker set return 503. State participates in the conductor→master query identity, preventing a stale top-level `idleSince` after Pause commits while the asynchronous route view still says running.

The response is flat: `state`, `maxInflight`, `inflight`, `idleSince`, `services`, `platform`, `transit`, and `egress`. Conductor owns this native API and composes the existing Proxy observation with the current Sandbox-to-switch/port binding. `platform` maps connector Mgmt RX/TX packets/bytes; `transit` maps Transit RX/TX. Both use the sandbox viewpoint. Each configured current port supplies all four unsigned integer counters, including valid zero. A sandbox without an attached port has empty `platform`/`transit` objects only when its FloatingIP, inner IP and port MAC are also absent, meaning no applicable current observation. An attached port requires all three identity fields; a partial binding returns 503 before querying either source. `egress: {}` always means no publishable egress statistics; it does not mean observed zero traffic. There are no per-service packet counters, source groups or network API alias.

`connected` replaces the former native `inflight.egress` and `services[*].egress`, preserving the established-backend-until-final-Close meaning. Top-level `idleSince` describes only admitted Proxy ingress; management monitoring packets do not refresh it and it makes no claim about sandbox computation or network idleness. Packets are packet counts, not application request counts; bytes are observed frame bytes, not throughput. Management uses the existing port/management ingress frame length; transit uses the frame before encapsulation or after removal of outer GENEVE headers, retaining Ethernet. Different observation points are not added into a traffic total. The cumulative counters belong to the current attachment and can reset on reuse; the API creates no network history.

Conductor batches at most 64 current ports per switch through connector's Go `Stats(ports)` API, with no per-sandbox CLI processes or second lifecycle authority. It reuses its allocation/detach fence; connector reuses the pin-directory shared lock, current pinned-map ID and reset-confirmation flag. The existing Proxy stats socket also accepts a bounded batch. Configured-source read errors, unconfirmed reset, control contention, incomplete results or changed bindings return 503 for the whole read, never zero or an older complete-looking response. The same domain reads serve the public API, trusted local batch and conductor extension. Stats remains available independently of telemetry and does not participate in Create/Resume readiness.

Traffic collection requires the matching connector control binary, TC programs and current counter-map ABI. Recreate older PERCPU_ARRAY/64-byte counter maps before enabling collection; the current map is a locked ARRAY/80-byte value. A failed reset affects stats availability only. See [connector operations](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch-operations.md) for switch lifecycle and deployment.

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

Workers send absolute state to their master over socketpairs; the conductor queries one live admitted Proxy. During InplaceRestart, `inplaceRestart` marks its instance-local statistics as incomplete service traffic. Failed reads remain unavailable and do not count as zero. Route SHM remains master-write/worker-read-only, without a stats area or worker writes. Mutable worker admission columns exist only in the separate arena, never in route records that can move during backshift.

## 9. Reliability

- **Worker crash:** a stats fault first terminates the old worker. The master waits for `cmd.Wait`, clears its admission column, and restarts the same index with a new epoch. Stale-high counts only conservatively reject while waiting. Other workers keep accepting on the same listener FD. Routes, sandbox state, and Create barriers do not change. The kernel closes existing connections on the crashed worker.
- **Master crash:** the plugin lease disconnects, new Create cannot pass its barrier, and DataEndpoint becomes unavailable. systemd restart registers a new master, rebuilds shared tables, and starts workers. Already running sandboxes themselves are unaffected.
- **routesync disconnect:** the master reconnects with exponential backoff. Fixed data-plane routes retain their existing retention/Bookmark convergence semantics, but MMDS route/value/service authority clears immediately and returns 503. Old secrets and service routes are not served before a full-sync Bookmark. Disconnect also fails pending Create barriers. A disconnect after ACK and completed 201 commit is an ordinary runtime availability failure.
- **Park/wake:** Lookup sends no Wake. Only Activate admitted by the effective authentication policy may wake a paused SID and wait for shared-table updates. Starting only parks, never wakes; paused/Delete immediately ends that wait. Resume ownership and current-launch progression remain with the conductor.
- **Stats stream:** EOF, timeout, or protocol error on any worker stream stops that worker. Traffic GET returns 503 until confirmed exit; then contributions are removed and readiness waits for the replacement. Prometheus counters remain monotone throughout the master's lifetime and do not regress on worker epoch changes.
- **Failure codes:** unavailable Create proxy stream, barrier timeout/disconnect, or route-apply failure = 503; invalid target = 400; non-CONNECT exec = 405; unknown/deleted SID = 404; rejected authentication = 401; recognized service unsupported by the profile = 501; unregistered/unreachable backend or proxy = 502; authorized exec resume failure = 503; ordinary admission exhaustion = 429 plus `max_inflight_reached`; exec exhaustion = generic ctl error after CONNECT 200.

### 9.1 Worker lifetime and transport cancellation

A Proxy worker is a dedicated one-shot subprocess. The built-in and custom App
entry points must exit that process after `Run` returns; they are not an API for
restarting workers inside an otherwise long-lived process.

Successful route SHM and admission arena mappings belong to the worker process.
`PreparedWorker.Close` closes inherited descriptors but does not unmap either
mapping. Existing handlers, extensions and traffic GC may still hold references
when a forced `Run` completion returns. Graceful draining waits for business and
extension completion first; process exit remains the final mapping ownership
boundary. The kernel reclaims mappings when the subprocess exits. Constructors still clean
up their own unsuccessful operations; low-level mapping tests can explicitly
unmap only after every reader has stopped. Worker lifecycle tests use child
processes instead of requiring production teardown to serve test-only reuse.

This does not change normal request cleanup. A finished request or complete tunnel
still closes its backend and releases its inflight contribution exactly once.
The supervisor must still wait for process exit before clearing that worker's
shared counters and stats contribution: unmapping a process does not zero shared
memory used by surviving processes. Master route apply and Create barriers do not
wait for worker health or handler shutdown.

Traffic limits decide whether a new flow is admitted, not how an admitted flow
is transported. Ordinary HTTP cancellation and HTTP/2 CONNECT stream cancellation
close the associated backend with or without an effective limit. HTTP/1 hijacked
CONNECT retains half-close semantics; its original HTTP request context is not a
universal tunnel-close signal. Native exec keeps its existing KAT / first-frame /
CEL / traffic-admission ordering.


### 9.2 Business readiness and service status

Each master reserves `GET /readyz` and `GET /status` on its own 0600 `stats_socket`.
Both return JSON with `Cache-Control: no-store`; other methods return 405.
`/readyz` returns 200 only after initial Bookmark, local initialization and every
configured worker's actual data and enabled-MMDS acceptance; otherwise it returns
503. Zero sandboxes is valid; disabled MMDS does not block readiness. Later
worker or route-state faults remove readiness until recovery. These reads never
call a Guest. Stats-stream readiness and an open TCP port are insufficient.

```sh
curl --fail --silent --show-error --max-time 2 \
  --unix-socket /run/sandbox/proxy-stats.sock http://localhost/readyz
curl --silent --show-error --max-time 2 \
  --unix-socket /run/sandbox/proxy-stats.sock http://localhost/status
```

Status exposes local `ready`, `instance_id`, `boot_id`, `pid`, `phase`,
`routes_synced`, `workers.{ready,expected}`, `data`, `mmds` and `reasons`.
Phases are `starting/prepared/serving/draining/stopped/failed`; an unconfirmed
peer is `unknown`. Reasons include `initial_sync_pending`, `worker_not_ready`,
`route_state_unavailable` and `draining`. `service_ready` requires at least one
known ready instance and no complete-service stop. `instances` includes actual
management and metrics addresses. `inplace_restart` holds the current or last
operation's ID, phase, pinned target identity, attempts, latest error, next
attempt, result and forced-close count. Transition/result logs retain history.
The shared data port is never used to confirm a particular candidate's readiness.

### 9.3 Bounded shutdown

Shutdown independently stops data and MMDS acceptance and waits for each
endpoint's accepted work. A worker exits after both have ended; dependencies
remain available while its work still uses them. This does not guarantee request
sequences such as data calling MMDS or consecutive data/MMDS requests.
Ordinary responses, streaming bodies, HTTP/1 WebSocket and CONNECT tunnels, and
the supported HTTP/2/h2c paths participate. Existing keep-alive/H2 connections
cannot begin new business after drain starts; established tunnels can finish.
Listener Serve returning, HTTP Shutdown finishing and hijacked tunnels ending
are separate events. No new transport protocol is implied.

For HTTP/1, draining half-closes the response direction and consumes pending
input until the peer closes its write direction or the shared deadline expires.
This preserves a completed response when another request was already pipelined.
A keep-alive client that reads the complete response but leaves its write
direction open can therefore consume the remaining drain budget. Closing that
connection at the deadline is reported as forced even when the response body
was delivered in full. That result describes connection cleanup and does not
mean that an already completed response failed.

`shutdown_timeout` defaults to zero. Automatic termination (SIGINT/SIGTERM in
Run, or RunContext cancellation) then cleans up immediately; explicit
`App.Shutdown(ctx)` still drains accepted work without a configured deadline.
InplaceRestart requires a positive timeout. With a positive value, one instance
shares the budget across data and MMDS. Each of the two restart drains gets one
budget; full Shutdown shares one deadline across A/A′ and B. Repeated calls never
extend it, and caller cancellation ends only the wait. Deadline expiry closes
remaining work and reaps processes;
`ErrForcedClose` and `completed_with_forced_close` distinguish this outcome.
An extension's optional `Shutdown(context.Context) error` joins its work after
the core cancels its Start context. See [extension lifecycle](extensions.md#proxy-lifecycle).

### 9.4 InplaceRestart

Both built-in Proxy and xproxy use **A → ready B → drained A → original-PID exec
→ ready A′ → drained/reaped B → A′ alone**. Each takeover requires business
readiness from dedicated local control. Initial Bookmark suffices for route
initialization; there is no old-instance catch-up condition. At most two
instances may retain business. B and workers cannot start nested operations.

Deploy a compatible binary as a separate protected file and atomically replace
the configured executable path. An accepted operation opens and pins that
target's identity and digest plus the original declarative input. B, A′ and
retries use those inputs even if the deployment path or YAML later changes.
Each master reruns Configure from that original input and checks takeover
compatibility. For initial adoption from an older version, use ordinary service
startup first. Conductor must advertise `inplace_restart_v1`, and configured
extensions must provide Shutdown; these checks also apply to programmatic calls.

With a positive `shutdown_timeout` and `inplace_restart: true`, trigger the original master:

```sh
systemctl kill --kill-who=main --signal=SIGUSR2 kuasar-proxy.service
```

The default false leaves SIGUSR2 unregistered; it does not disable
`App.InplaceRestart(ctx)` when the timeout is positive. Enabling the signal with
a zero timeout is a configuration error. `App.Run()` retains SIGINT/SIGTERM handling. A custom
entry uses `RunContext()` and calls App actions itself; see the
[public lifecycle API](extensions.md#proxy-lifecycle). Acceptance returns an
operation ID, not final completion; the original Go stack cannot survive exec.
Inspect status throughout the operation, including B's private socket while the
original master's management endpoint is temporarily unavailable.

Before A stops accepting, a failed B is drained/reaped and A keeps serving.
After that boundary the coordinator recovers automatically: it replaces failed
B, retries failed exec, retries A′ initialization, repairs A′ during B drain,
and retries B reclamation. Candidate preparation is bounded by 30 seconds;
retry delays are 1/2/4/8/10 seconds, capped at 10 with no fixed attempt limit.
Preparation, drain and backoff have separate clocks. Stop cancels future
attempts. Persistent faults remain visible; removing their cause resumes the
same operation. Runtime-fatal main-process faults require deployment supervision.
Recovery never replays user requests or restores connections lost in a crash.

A/A′ retain configured `shm_path` and `stats_socket`; B creates checked private
names in the same parent directories. Only owned resources are removed after
actual process exit. Address listeners and activated listeners both retain
service copies; they are not rebound during replacement. Closing an instance's
listener never performs a socket-wide shutdown on another holder.

For metrics, empty disables all instances; fixed `host:p` uses p for A/A′ and
p+1 for B; port 0 remains dynamically assigned. Port 65535 rejects the operation.
B binds and retains its metrics listener before A drains; a conflict fails that
preparation without scanning further ports. IPv6 follows the same rule. Counters
may reset per instance and scraping may have a short gap. Ordinary startup logs
a metrics bind failure and continues serving; restart requires the configured
metrics resources to be available before A drains.

Traffic statistics remain instance-local. The existing traffic response exposes
`inplaceRestart: true` from acceptance through operation cleanup, including when
B answers during the exec gap. Idle consumers must treat these observations as
incomplete and read normal single-instance statistics again after completion.
An unavailable read is still an error, never zero or evidence that restart ended.
There are no persistent statistics records, completion files or cross-instance
counter aggregation. Each generation keeps existing admission limits and
single-generation concurrency slack. Reserve capacity for two instances, two
quota sets, FD/memory peaks and the adjacent metrics port.

### 9.5 Named listeners and systemd deployment

`data_listen: "fd=proxy-data"` and conductor `mmds.listen: "fd=proxy-mmds"`
select names exactly; either may independently remain a TCP address. Names are
case-sensitive, nonempty, and contain no whitespace, colon or NUL. Invalid,
missing, duplicate, unused, non-listening or non-TCP inputs fail startup; two
names cannot alias one socket. Environment inputs never override configuration
or cause an address to fall back to an inherited listener.

`app/activation.Receive()` adapts `LISTEN_FDS` and `LISTEN_FDNAMES` beginning at
FD 3. Absent input yields an empty map; zero accepts an empty name set. An
absent LISTEN_PID is allowed; a supplied empty, invalid or mismatched PID fails
without taking or closing its FDs. Processing clears activation environment;
owned descriptors are close-on-exec until explicitly passed to successors.
The final master consumes activation across node-ctl → xproxy; worker bootstrap
and logging descriptors remain separate. For direct runtime file ownership,
see [Runtime.ListenerFiles](extensions.md#proxy-lifecycle).

The following units are deployment templates. Create the connector management
namespace and forwarding/address configuration first, then choose the MMDS
address that matches it. For example, with `proxy_netns: sw0_mgmt` and connector
forwarding to `127.0.0.1:19254` in that namespace:

```ini
# kuasar-proxy-data.socket
[Socket]
ListenStream=0.0.0.0:3443
Accept=no
FileDescriptorName=proxy-data
Service=kuasar-proxy.service

[Install]
WantedBy=sockets.target
```

```ini
# kuasar-proxy-mmds.socket
[Socket]
ListenStream=127.0.0.1:19254
NetworkNamespacePath=/run/netns/sw0_mgmt
Accept=no
FileDescriptorName=proxy-mmds
Service=kuasar-proxy.service

[Install]
WantedBy=sockets.target
```

```ini
# kuasar-proxy.service
[Unit]
Requires=kuasar-proxy-data.socket kuasar-proxy-mmds.socket
After=kuasar-proxy-data.socket kuasar-proxy-mmds.socket

[Service]
Type=simple
ExecStart=/opt/kuasar/bin/node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
Sockets=kuasar-proxy-data.socket kuasar-proxy-mmds.socket
KillMode=mixed
TimeoutStopSec=45s
Restart=on-failure
```

Set `shutdown_timeout: 30s` and `inplace_restart: true` in proxy.yaml for this
signal example, plus `data_listen: fd=proxy-data`. Conductor owns
`mmds: {enabled: true, listen: fd=proxy-mmds}`. Omit the MMDS unit and its service
references when MMDS is disabled. Keep runtime directory permissions, binary
ownership, namespace access and control-socket admission consistent with the
service identity. Add ordering for the deployment's actual conductor and network
setup units. Put `NetworkNamespacePath` in the MMDS unit's `[Socket]` section,
create that namespace first, and verify its listener with
`ip netns exec sw0_mgmt ss -ltn`. Putting only the service in a namespace does
not move existing sockets. See
[systemd namespace semantics](https://github.com/systemd/systemd/blob/v255/man/systemd.exec.xml).

`KillMode=mixed` lets the main process coordinate graceful stop before systemd's
final cgroup cleanup. Keep `TimeoutStopSec` above the application budget with
reaping margin. Ordinary `systemctl restart kuasar-proxy.service` retains active
socket units; queued connections still depend on backlog and client timeouts.
For complete shutdown, stop both socket units and the service together so
incoming traffic cannot activate it again. Install or alter production units
only through the deployment process.

## 10. Performance

- Ordinary data-plane lookup is a worker-local mmap hash lookup, with no conductor call or cross-process RPC. Only custom guest MMDS paths use local worker→master socketpair RPC.
- Unlimited traffic never accesses the admission arena. Limited flows scan fixed `N × 4` absolute cells without per-flow master RPC. Different sandboxes use different worker-local mutexes, avoiding a process-global lock; same-SID concurrency serializes only within that worker's entry and row.
- The master alone writes route SHM; workers only read it. Admission workers write their own columns, and the master is absent from the per-flow hot path.
- Neither ordinary HTTP nor CONNECT uses upstream connection pools, avoiding reuse across sandboxes/ports.
- `route_capacity` is a fixed protective capacity. Increase it and restart the proxy master when more capacity is needed.
- Worker metrics and traffic report absolute snapshots asynchronously over the unified stats socketpair. At `metrics_listen`, the master differences absolute counters and continues exposing existing `data_requests_total{result=...}` metrics. Backpressure coalesces intermediate snapshots without permanently losing counts or current traffic.
- MMDS source-IP reverse lookup uses one reconstructible fixed slot indexed by IPv4 modulo connector `MaxPorts`, then validates the candidate SID against the authoritative primary route table, including active state and exact IP. It is no longer a linear table scan. Missing, mismatched, or stale hints fail closed; a modulo collision displaces the previous source hint rather than authorizing it as the new source. This hint holds no credentials and is not an ordinary data-plane route authority. Token minting uses the source lookup; authenticated GET validates the token's source/IP/incarnation binding, and custom GET additionally uses master RPC. MMDS is not limited to envd initialization.

Here running proves only that the orchestrator readiness wire and mandatory e2b `/init` succeeded. It does not guarantee that the code interpreter, forwarded business port, or user application is listening/healthy. [#125](https://github.com/kuasar-sandbox/orchestrator/issues/125) separately tracks business-backend readiness; the proxy adds no generic dial retry in this phase.

## 11. See Also

- [node.md](node.md) — conductor control plane, Proxy deployment, lifecycle, and key model.
- [cluster-router.md](cluster-router.md) — how cluster ingress forwards to this node's data plane.
- [Connector vSwitch](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch.md) — mgmt-extract and MMDS VIP translation.
- [Deployment](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/deployment.md) — topology, ports, and failure domains.
