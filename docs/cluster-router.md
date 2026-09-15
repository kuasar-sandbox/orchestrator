[English](cluster-router.md) | [简体中文](cluster-router_zh.md)

# cluster-router — unified e2b ingress and route cache

The standalone `kuasar-sandbox.identity` / `X-Kuasar-Sandbox-Identity` extension is rejected by public cluster Create and Build registration; Registry keeps allocation authority. See [Sandbox identity on Create](node.md#412-create-identity).

`cluster-ctl router` is the cluster's northbound ingress for both the e2b control plane and data plane. It is not a routing authority and subscribes to neither routes nor node_list. It locates route owners by group. Explicit create/connect/exec-session calls use their corresponding Reserve operation; a data-plane cache miss starts with Resolve. RouteResolve returns both `APIEndpoint` and `DataEndpoint`, each with a fixed purpose. Once a route has `NodeSandboxID + DataEndpoint`, ordinary data traffic connects directly to the final node proxy even in paused/starting states; the node handles authorized parking, Wake, and backend connection. Exec CONNECT is the exception: after public 200, the router reads and authorizes the first ctl frame before connecting to the final node. `Reserve(operation=data)` remains only a fallback for a missing target after request admission, a typed stale target before sending Raw, or the ordinary data-plane compatibility path.

## 1. Overview

### 1.1 Request path

```text
client
  │ HTTPS e2b control/data
  ▼
router
  │ sandbox/build control: APIEndpoint
  ├────────────────────────────────────► node conductor
  │ ordinary data: known NodeSandboxID + DataEndpoint (ready/paused/starting)
  ├────────────────────────────────────► node Proxy
  │                                      auth → per-Sandbox admission → parking → Activate/Wake → backend
  │ exec: token gate → public 200 → first-frame gate → node proxy
  │
  │ data cache miss / missing target / typed stale
  ▼
route_link owner
  │ Resolve
  │ fallback only ──────────► operation=data Reserve
  ▼
node owner / placer / node
```

### 1.2 Principles

1. **Every request requires a group**: `X-Kuasar-Sandbox-Group` shards both the cluster control plane and data plane.
2. **Reserve operations are explicit**: create/data return only at READY. connect/exec-session complete after synchronous node preparation and a typed result, without waiting for asynchronous resume. The router does not subscribe to route watches.
3. **The hot path prefers the route cache**: a recently resolved route with a node target lets new requests avoid Resolve/Reserve in ready, paused, and starting states. In-flight requests are not a routing source for new requests.
4. **Fail-fast invalidation**: if the node returns typed not-found/unauthorized before accepting the inner request, the router evicts the old target, revalidates the same credential through ReserveData, refreshes the route, and retries once. Exec permits this retry only before Raw has been sent to any node. Connection failure also evicts the target; subsequent requests Resolve again.
5. **Data-plane bytes do not enter the registry**: the registry participates only in explicit create/connect/exec-session, cache-miss Resolve, and data Reserve fallback for a missing target or typed stale response.
6. **Root credentials have separate purposes**: Reserve requires the original API key for create/connect/exec-session. Create group admission uses a ready placer's provider APISecret; connect/exec-session use the APISecret already bound to the Sandbox record. For other control operations, the router calls the route owner's verify-key, and the registry fails over to ready placers for verification. Protected READY route results also project the sandbox's bound APISecret, ServiceSecret, and purpose-specific access tokens to trusted routers. Raw ManifestKey never enters the registry/router routing chain.

## 2. Command line

```text
cluster-ctl router --config /etc/cluster-ctl/router.yaml
```

## 3. Configuration

```yaml
domain: sandboxes.example.com

registry:
  bootstrap: registry-1.example:7700

ingress:
  listen: ":443"

auth:
  data_plane: enforce
  cache_ttl: 60s

cache:
  route_ttl: 5m
  idle_timeout: 2m
```

| Field | Meaning |
|---|---|
| `domain` | Cluster service domain. |
| `ingress.listen` | Control- and data-plane ingress listener. |
| `ingress.tls` | Wildcard certificate configuration. |
| `registry.bootstrap` | Registry bootstrap endpoint for fetching membership. |
| `registry.tls` | mTLS to the registry control plane. |
| `auth.data_plane` | Data-plane credential verification: `enforce` / `log` / `off`. |
| `auth.cache_ttl` | API-key verification cache TTL. |
| `cache.route_ttl` | Route-resolution cache TTL. |
| `cache.idle_timeout` | Idle age for route-cache eviction; it is not a tunnel idle-close timer. |
| `metrics_listen` | Prometheus endpoint. |

Public router ingress can terminate TLS; registry control connections can use independent mTLS. Current router-to-node `APIEndpoint` and `DataEndpoint` connections use plaintext HTTP/CONNECT. Node registration must supply reachable plaintext internal listeners for these two different purposes; a TLS-enabled node listener cannot be placed directly in either field.

## 4. Registry membership

After startup, the router fetches registry membership through bootstrap:

```text
GET /cluster/membership
```

The response includes active / next / old_grace registry members, the membership label, and owner count. For each group, the router locates route owners with `LocateN(group, active.members, route_link.owner_count)`.

During refresh, it tries bootstrap and known members and selects the result with the newest active version. A route_link 5xx/409 response triggers membership refresh and one retry.

The router does not participate in registry member health detection and subscribes to neither routes nor node_list.

## 5. Addressing

| Request | Required identity | Behavior |
|---|---|---|
| create | group + route_key (generated if omitted) | Extract and validate the resource patch and this request's restore/credentials/checkpoint; locate the route owner and pass them with `ReserveSandbox`. |
| kill | group + route_key + sandbox_id | Locate the route owner; the registry dispatches `CmdDelete` through node-link. |
| connect | group + route_key + stable sandbox_id | Call Reserve with `operation=connect`; the registry dispatches CmdConnect through node-link. No Resolve or forwarding to node HTTP `/connect`. |
| exec session | group + route_key + stable sandbox_id | Call Reserve with `operation=exec-session`; the registry verifies the API key and dispatches CmdExecSession through node-link. Only ExecAccessToken is returned publicly. |
| get/stats/pause/timeout/export | group + route_key + stable sandbox_id | Resolve the current NodeSandboxID and APIEndpoint through the route owner; rewrite the path and forward to the node conductor. Stats bodies have no SID and need no response identity adaptation. Missing APIEndpoint fails closed. |
| list/get | group | Read the group shard; individual sandbox lookup also uses the identity described above. |
| data plane | group + route_key + stable sandbox_id + target | With known NodeSandboxID/DataEndpoint, open a one-use node CONNECT, including paused/starting states. Fall back to `operation=data` only for a missing target or typed stale response; a miss starts with Resolve. |
| build register | group + build_id | Normalize the body/Builder header into Build.Resources, parse the target Sandbox ResourcePatch independently, then call `ReserveBuild`. The selected node performs final registration admission. |
| build cancel | group + build_id | ResolveBuild afresh for each call and forward through the current APIEndpoint without selecting another node. |
| build trigger/status/files | group + build_id | ResolveBuild returns APIEndpoint; cache it and forward to the node conductor. Never fall back to DataEndpoint if it is missing. |
| build delete | group + transient TemplateID | Resolve the original node APIEndpoint from the existing projection; forward Query/Header unchanged for final node authorization and execution |

`route_key` locates a route within a group; `sandbox_id` is the stable public identity. The registry generates SandboxID on the first create. Same-node resume, cross-node migration, and re-placement do not change it. Protected routes additionally carry the current `node_sandbox_id=<sandbox_id>-g<N>`. The router addresses sandboxes by stable SandboxID and substitutes NodeSandboxID only at the node boundary; public responses do not expose NodeSandboxID.

The protected route's `stable_id` is the same sandbox's StableID across NodeSandboxID changes. The cluster always requires `StableID == SandboxID`. The router retains this field as the existing credential projection and uses it to validate KAT `sid`, while cache/route lookup remains keyed by `(group, route_key, SandboxID)`. It neither uses StableID as a node-local lookup key nor builds a unique index on it.

Create body metadata can carry `kuasar-sandbox.resource`, traffic, restore, credentials, and checkpoint. `X-Kuasar-Sandbox-Resource` and `X-Kuasar-Sandbox-Traffic` override only explicitly present leaves in their respective patches. The public resource surface is strictly limited to capacity/allocatable/startup and uses the same merge helper as group defaults; traffic uses the shared max-inflight patch validation and leaf merge. Restore/credentials headers replace the complete object of the same name; the checkpoint header overrides individual fields. The router always parses and strictly validates the body, so a valid header cannot conceal an invalid lower-priority resource/traffic/checkpoint. Omitted restore, `{}`, and explicit `off` all disable restore; only an explicit `memory` on this create enables it. The create-body limit is 16 MiB; larger bodies return **413**.

The router constructs requests to the shared `POST /route-link/reserve` endpoint according to the [Registry Reserve protocol](cluster.md#83-reserve). It forwards operation-specific authenticated context and preserves the requested stable identity. Create is strictly normalized before admission; Connect preserves the optional memory selection, including absence; exec-session first strictly parses public TTL/conditions and converts them to the typed Registry request. Data uses the existing access-token/service/port context.

The Registry specification owns the exact query/header/body schemas and repeats validation at its boundary. Conditions are not carried in unrelated metadata, query parameters or configuration Headers. Keeping this transformation boundary separate avoids a second, potentially stale schema table here.

For connect/exec-session/data, `sid` is the customer's expected stable SandboxID. It prevents requests from crossing lineage when a route_key is deleted and recreated.

## 6. Cache model

### 6.1 Decision order

```text
request
  │
  ├─ cache hit with NodeSandboxID + DataEndpoint
  │     └─► forward to node (ready/paused/starting)
  ├─ cache hit without a complete node target
  │     └─► operation=data Reserve, then forward
  │
  └─ miss
        └─► Resolve via route owner
              ├─ complete node target ──► cache route, then forward
              └─ missing node target ───► Reserve, cache route, then forward
```

The route-resolution cache is a hot-path optimization that avoids sending every new HTTP request to the registry. Direct forwarding depends only on a complete `NodeSandboxID + DataEndpoint`, not on ready/paused/starting state. The final node performs parking, Wake, and backend connection. The router does not maintain consistency through a full route stream.

### 6.2 Route-resolution cache

Key:

```text
(group, route_key, sandbox_id)
```

Value:

```text
{node_id, api_endpoint, data_endpoint, sandbox_id, node_sandbox_id, profile, stable_id,
 api_secret, api_secret_fingerprint, manifest_key_fingerprint, service_secret,
 envd_access_token, traffic_access_token, forward_access_token,
 target_port, route_revision, expires, last_used}
```

This cache is protected state inside the router process. Root credentials and tokens must not be emitted in public responses, logs, or observability interfaces. `envd_access_token` serves e2b envd ports; `forward_access_token` serves other forwarding targets. `traffic_access_token` is retained with the protected route and returned in create responses; the cluster router does not use it for data-plane authentication.

`route_revision` is the committed revision of the group's route recordSet. A lower revision cannot overwrite the cache. An equal revision with a different NodeSandboxID is also rejected. Late Resolve/Reserve results from an old generation therefore cannot roll back a switched node target.

Ports are not parsed from route_key. A legacy port comes from `E2b-Sandbox-Port`, the `<port>-<sandbox_id>` Host, CONNECT authority, or route_link's `target_port`. `target_port>0` enforces that port: use it when the request omits a port, and reject an explicitly conflicting port with 400. If all sources are missing, legacy requests return 400; the router does not default to 49983. `exec` is a portless logical service. A request may carry a port, but the router does not use it to select the backend and does not reject an omitted port. Port contracts for other explicit services remain within the [#63](https://github.com/kuasar-sandbox/orchestrator/issues/63) scope.

### 6.3 In-flight requests

The in-flight count only indicates that `(group, route_key, stable sandbox_id)` is being forwarded. Once forwarding starts, each request owns a route copy and no longer depends on the cache entry. The count is not a routing source for new requests, does not retain/reuse/share data-plane TCP connections, and does not prevent a higher RouteRevision from replacing the current NodeSandboxID.

```text
client A ── data request ──┐
client B ── data request ──┼─ same resolved route ─► each request opens its own CONNECT to node proxy
client C ── CONNECT ───────┘
```

New requests for a route take their current decision only from the route cache and always create a fresh one-use CONNECT to the node proxy. Ordinary HTTP requests also start with CONNECT to the node proxy, then send one HTTP request inside the tunnel. External CONNECT requests receive the tunnel directly. A router-to-node TCP connection is therefore never shared across sandboxes or ports.

### 6.4 Reserve concurrency

The router does not coalesce Reserve requests, avoiding a shared call across different operations, credentials, timeouts, or migration tokens. The registry coalesces the creation state machine only for concurrent create. Data converges on one activation through route CAS and stable-lineage waits. Each connect independently dispatches CmdConnect to the current NodeSandboxID. Each exec-session API call also has its own CmdID and ExecSessionResult and does not share a token. Data requests for unknown routes only Resolve; they never implicitly create a sandbox.

### 6.5 Invalidation

Evict a cached route when:

- The node proxy returns 404/401 during the CONNECT handshake with `X-Kuasar-Proxy-Error: not_found|unauthorized`.
- Connecting to node data_endpoint fails.
- Route TTL or cache idle timeout expires.

After a successful handshake, HTTP 401/403/404 comes from the sandbox application or envd and does not evict the route. A handshake failure with the typed errors above evicts the old target, calls `ReserveData` with the same request credential to revalidate and obtain a fresh route, then retries once. Ordinary connection failure only evicts the cache; subsequent requests Resolve again. No router route subscription is maintained for this purpose.

## 7. Control plane

API-key authentication cannot be disabled. Create/connect/exec-session pass the client's original API key to Reserve, where the registry verifies it before any lifecycle side effect. Create group admission uses a ready placer's provider APISecret; connect/exec-session use the APISecret already bound to the Sandbox business record. For other control operations, the router calls the route owner's `verify-key`, which fails over to ready placers. ManifestKey serves content paths only and does not authenticate API calls.

| Operation | Behavior |
|---|---|
| create | Call `operation=create` Reserve with leaf-merged resources and request-scoped restore/credentials/checkpoint. Separate credentials before writing ordinary metadata. |
| kill | The route owner matches group + route_key + sandbox_id exactly and dispatches `CmdDelete` through node-link. |
| connect | Call `operation=connect` Reserve. The registry dispatches CmdConnect to the current NodeSandboxID and returns after validating typed ConnectResult. The router neither Resolves nor forwards a second request to node HTTP `/connect`. |
| exec session | Strictly parse the body with a 64 KiB limit and call `operation=exec-session` Reserve. The registry verifies the original API key and dispatches CmdExecSession; the router projects only ExecAccessToken. |
| get/pause/timeout/export | Resolve the current route, use APIEndpoint only, and replace public SandboxID with NodeSandboxID in the forwarded path. Rewrite typed get responses to stable SandboxID. |
| get/list | Read the group's route_link records. |
| build register | Generate stable build_id/template_id, strictly merge registration resource body/header/E2B capacity leaves, and call ReserveBuild. The node validates again before saving the build. |
| build cancel | ResolveBuild afresh by group+build_id and forward through the current original-node APIEndpoint without using a stale cache entry. |
| build trigger/status/files | Locate the node by group+build_id; cache and forward through BuildReserveResult.APIEndpoint only. |
| build delete | Transient TemplateID only; resolve the original node from the existing projection and preserve Query/Header |

DELETE `/templates/{transientID}?cancel=true` and `X-Kuasar-Sandbox-Builder: {"cancel":true}` share the node action parser. Each input is strictly validated independently. Explicit Header.cancel > Query.cancel > false; `{}` preserves Query and explicit false overrides true. Router does not parse this as registration BuildOptions. Lookup failure or incomplete ownership returns 503; an unreachable node returns 502, never successful deletion or an invented 404. DELETE 202 preserves the node relative Status Location; only final node hard deletion completes the operation. See [Build actions](node-build.md#11-cancel-and-delete-a-build-record).

Sandbox control and Build follow-up use a different node endpoint from ordinary data/CONNECT/native exec. Missing APIEndpoint for control/build never falls back to DataEndpoint; missing DataEndpoint for data/exec never falls back to APIEndpoint.

A connect Node Ack projects only NodeSandboxID, TemplateID, Profile, and Envd/Traffic/ForwardAccessToken; it contains neither root credentials nor fingerprints. The router checks consistency between the Ack and protected Route, then builds an e2b response containing only stable SandboxID and the profile's public tokens. The node has synchronously completed validation, optional migration import, deadline persistence, and credential reads. Resume proceeds asynchronously after Ack.

The public exec-session endpoint is `POST /sandboxes/{stableSandboxID}/exec-sessions`. Requests require group, route-key, and the original `X-API-KEY`, with optional `X-Kuasar-Migration-Token`. The body permits only an empty body, `{}`, or strict `ttlSeconds` plus `conditions:[{"expr":"..."}]`. The entire original body, including trailing whitespace, is limited to 64 KiB. Missing conditions and `[]` are unrestricted and normalize to nil. Explicit `null`, unknown/duplicate fields, empty expr, a second JSON value, negative values, or exceeded TTL/condition bounds return 400.

The router passes the API key to the registry verifier without writing it into the command. CmdExecSession explicitly carries APISecretFingerprint, expected Profile, TTLSeconds, ExecConditions, and optional MigrationToken/cluster context. The node is the sole signer: before any activation mutation, it authoritatively compiles conditions and signs. Ack returns only `ExecSessionResult`, followed by asynchronous resume under the existing eager contract. Conditions do not enter Route/record/event/metadata/log. Deferred activation in [#240](https://github.com/kuasar-sandbox/orchestrator/issues/240) remains separate work. The registry joins the result with the current stable-lineage Route; after checking consistency, the router returns:

```http
HTTP/1.1 201 Created
Cache-Control: no-store
Content-Type: application/json

{"execAccessToken":"kat1..."}
```

The response contains no session ID, exp, NodeSandboxID, ServiceSecret, or route. Runtime failures return fixed, sanitized errors rather than raw node Ack reasons or internal paths. An asynchronous resume failure after Ack does not retroactively alter the returned token; later connections fail according to current route/state.

## 8. Data plane

The router first separates ordinary legacy targets from the native exec logical service:

- Ordinary HTTP uses only the legacy port from Host/`E2b-Sandbox-Id + E2b-Sandbox-Port`. Except for the exact value `exec`, it does not interpret `E2b-Sandbox-Service`; that header remains an application header in the inner request. Non-CONNECT requests selecting `exec` return **405** with `Allow: CONNECT` before route lookup or activation.
- CONNECT without a service also uses a legacy raw port.
- `E2b-Sandbox-Service: exec` is the integrated portless CONNECT target. A port may coexist but does not select the backend.
- The complete cluster service-aware forwarding contract for `forward|e2b:envd|e2b:code-interpreter` is tracked in [#63](https://github.com/kuasar-sandbox/orchestrator/issues/63). Current cluster ingress does not advertise those three header values as supported backend selectors. The final node chooses its backend from its locally trusted profile.

Legacy sid-host data-plane credential rules are:

- e2b profile ports 49983/49999 require `X-Access-Token: <envd_access_token>`.
- Other e2b ports and every bare port require `X-Access-Token: <forward_access_token>`. Bare ports 49983/49999 are ordinary TCP forwarding targets, not 501 control ports.
- `traffic_access_token` is for external gateways and e2b data-plane components; the cluster router does not consume it.
- A presigned envd file URL can omit the token only for `port=49983`, `GET/POST /files`, and a nonempty `signature` query.
- A nonempty but incorrect `X-Access-Token` never falls back to signature authentication.

Ordinary token forwarding reuses the client's `X-Access-Token`:

```text
CONNECT sandbox:<port> HTTP/1.1
E2b-Sandbox-Id: <node_sandbox_id>
E2b-Sandbox-Port: <port>
X-Access-Token: <client X-Access-Token>
```

The client-facing Host/path uses stable SandboxID. The router uses current NodeSandboxID in the outer CONNECT to the node proxy and rewrites ordinary HTTP's inner Host to `<port>-<node_sandbox_id>.<domain>`. If the inner request already carries `E2b-Sandbox-Id`, it replaces that value with NodeSandboxID while preserving the header so user applications can see the current node-local ID.

For tokenless signed envd `/files` requests, the router first validates the signature with the protected route's `EnvdAccessToken`. This occurs before any node CONNECT or Reserve/activation. Failure returns 401 immediately and must not wake the sandbox. After successful verification, the router adds that `EnvdAccessToken` only to the outer CONNECT for the node proxy's final-hop authentication. The tunneled HTTP request keeps its original signature and receives no injected `X-Access-Token`, so envd validates the URL independently. Ordinary token requests retain the client's original `X-Access-Token` in both outer CONNECT and tunneled HTTP.

After the router's local authentication gate, a route with `NodeSandboxID + DataEndpoint` does not wait for READY in the registry: paused/starting opens a one-use node CONNECT immediately, just like ready. The final node proxy follows `LookupRoute → authorize → BeginParking → ActivateRoute/Wake → fresh Route → dial`. Admitted parking is therefore counted only at the final node, without duplicate router/registry accounting. In ordinary router `enforce` mode, invalid tokens fail before node CONNECT/Reserve. The router's file-signature validation and explicit token checks on `/files` remain enforced in every router mode. Exec is always enforced. For ordinary requests, router `log` records a local mismatch and continues, while `off` skips that local token check. The node proxy applies its own effective authentication policy independently: preventing an invalid ordinary credential from Wake/Resume/parking/backend dial requires node proxies to remain in `enforce`. If the target node also uses `log` or `off`, it can admit that request and activate the backend. Router configuration does not override the node's policy.

Only a missing target first invokes the `operation=data` fallback. Ordinary requests pass the customer token to the registry; signed `/files` passes the protected EnvdAccessToken already used to validate the signature. If a known node returns typed `not_found`/`unauthorized` before accepting the inner request, the router evicts that node-local target and invokes the same ReserveData to revalidate the credential against current lineage and get a fresh route, then retries once. A fallback returning another stable identity or still lacking a complete target fails closed. `auth.data_plane=off` only skips the router's local token gate for ordinary requests; an explicit wrong token on `/files` never falls back to signature in any mode.

Exec CONNECT uses its own three-stage KAT + request-admission contract:

```text
client CONNECT
  stable SandboxID + service=exec + X-Access-Token=KAT
    ↓ side-effect-free cache/Resolve
Router verifies StableID + ServiceSecret, then compiles CEL
    ↓ flush public 200; read strict first ctl frame; recheck expiry; evaluate
    ↓ admission passed (no node/Reserve before this point)
node CONNECT
  current NodeSandboxID + service=exec + optional original port + same KAT
    ↓ node independently verifies KAT + CEL and reads/evaluates first frame
    ↓ parking → ActivateExec/Wake → ctl.sock; Raw written exactly once

missing/stale target, only after public request admission and before Raw send:
  Reserve(operation=data, same KAT + service=exec)
    ↓ Registry re-verifies and returns fresh current Route
  retry one node CONNECT
```

Exec is always enforced regardless of `auth.data_plane=off|log|enforce`. EnvdAccessToken, ForwardAccessToken, and TrafficAccessToken cannot substitute for an exec KAT. A missing, expired, incorrectly signed, or wrong-SID/audience KAT returns 401 before public 200 and cannot trigger router Reserve, node CONNECT, registry CmdConnect, node parking, or resume. ExecRequest/condition failures after 200 return the uniform `{"type":"error","msg":"exec request rejected"}` with none of those side effects. A paused/starting route with a known node target adds no registry calls. Router and node both execute the complete gate; a pure transport relay does not parse tokens/CEL/ExecRequest. The registry revalidates only on missing/stale fallback. After Reserve, the router must recheck stable lineage/credentials and use current NodeSandboxID, never the old target.

After public 200, the router runs the first-frame gate before opening the next hop. The final node independently checks `StableID + ServiceSecret` from its local route, replies with node CONNECT 200, and repeats the first-frame gate before parking, activation, or connecting to `<run_root>/sandboxes/<NodeSandboxID>/ctl.sock`. The router does not dial `ctl.sock`.

KAT binds StableID, not NodeSandboxID or its generation. Same-lineage resume, migration, or replacement therefore does not itself require re-signing the token; expiry, claims, and current credential validation still apply. Each new node CONNECT uses the current NodeSandboxID.

When ordinary HTTP/non-exec CONNECT reaches the final node proxy's per-Sandbox limit, that proxy returns `429` plus `X-Kuasar-Proxy-Error: max_inflight_reached`. The router passes through status, header, and fixed body unchanged. This typed response is not a stale route: it causes no cache eviction, Reserve, or retry on a different node. The router implements no second limiter; the target proxy enforces the same Sandbox limit across all its workers, not as cluster-wide or per-router capacity. Native exec admission occurs at the node after CONNECT 200 and CEL/first-frame checks. At the limit, the node writes the existing generic ctl error and closes. The router transparently relays the frame without synthesizing HTTP 429.

Long-lived CONNECT uses the same route resolution, but tunnels are never reused. After disconnect, the route cache remains until idle/TTL expiry or fail-fast invalidation. Bytes prefetched by the router-to-node CONNECT response reader remain available in the tunnel. One-way EOF propagates a half-close only; the relay waits for the opposite direction's complete `exec_ack`, stdout/stderr, and exit status rather than truncating the tunnel on the first `io.Copy` return. One typed stale retry is allowed before Raw has been written to any node. Once Raw reaches a node, retry/reroute/replay is prohibited; node ctl errors pass through transparently without a second synthesized error.

## 9. Reliability

| Event | Behavior |
|---|---|
| Router crash | Local caches disappear; the load balancer redirects traffic; cache misses Resolve again after restart. |
| Registry moved | Refresh membership and retry. |
| Registry-owner failure | Fail over in owner-set order. |
| Reserve timeout | Return 503/504 and finish the current request; the router retains no Reserve flight. |
| Typed stale cache | Evict the old target, revalidate/refresh through ReserveData, and retry once. |
| Typed `max_inflight_reached` | Pass 429 through unchanged; no route eviction, Reserve, or retry on another node. |
| Node API endpoint failure | Fail the current control/build request; subsequent requests Resolve again without switching to DataEndpoint. |
| Node data endpoint failure | Evict the route cache; the next request Resolves again without switching to APIEndpoint. |
| Late old-generation result | A lower RouteRevision or equal revision with another NodeSandboxID cannot overwrite the cache; an old request's failure cannot evict the new generation. |

## 10. Performance

- A cached known node target uses local route resolution plus one new router-to-node CONNECT in ready/paused/starting states. Parking/Wake runs at the node.
- A data miss adds one registry Resolve. A missing target or typed stale response alone adds data Reserve.
- Growth in group count does not make the router maintain a full route stream.
- Per-Sandbox max inflight is enforced only at the final node proxy; the router adds no global limiter, shared counter, or retry amplification.
- Control-plane forwarding may use HTTP transport connection pools isolated by `api_endpoint`; data-plane forwarding uses no pooled transport.

## 11. See also

- [cluster.md](cluster.md) — registry membership, route_link, Reserve, and the data model.
- [cluster-placer.md](cluster-placer.md) — verify-key, Place, and key-distribution sources.
- [node-proxy.md](node-proxy.md) — node-internal data-plane forwarding, CONNECT, and envd signatures.
- [Router implementation](../internal/router/router.go) — authentication modes, cache lifetime, endpoint selection, and exec admission.
