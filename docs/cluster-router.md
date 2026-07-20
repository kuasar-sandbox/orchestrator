# Cluster Router

`cluster-ctl router` 是 caller-facing e2b API 和 Sandbox data-plane ingress。它认证 caller/GROUP，通过可信
mTLS 调用 Registry Route/Build API，并仅转发当前 Serve Permit 授权的 READY execution。

## Authority boundary

Router 负责 caller/GROUP authentication、HTTP contract、request single-flight、短期 Route/Build cache 和
数据面转发。Router 不拥有 Route 状态，不参与 Admission，不维护 Node Catalog，也不根据节点不可见创建
replacement。

Registry 不接收 caller credential。Router 从 Provider 验证 caller 后构造可信内部请求：

```text
cluster_id, registry_generation, system_epoch, registry_layout_digest, shard_id
group, route_key or build_id
min_revision
operation input
```

## Registry Layout and Permit

Router 启动时验证 signed Registry Layout chain 和 anti-rollback guard，并为 Registry Layout 中每个 Registry member 创建
认证客户端。Router 定期从 System Group 刷新 bounded Serve Permit，使用 monotonic deadline 保存本地
有效期。

读和 cache forwarding 要求：

```text
ServeGate && CutoverGate && RecoveryClosed
```

mutation 额外要求 `WriteGate`。Permit 失效后 Router 立即 fail closed，已有 cache 也不能继续转发。

## Route resolution

Route key 映射由 `ShardHashV1` 和 Registry Layout 固定参数确定。本地正读使用按 `(group, route_key)` 的 replica
rendezvous 排序，从不同 key 分散到三个副本；leader hint 仅用于 mutation 和 strong read。

读取流程：

```text
try replica-local positive read
-> accept only exact generation, route key and min_revision
-> local miss/behind follows leader hint
-> strong read may return final NOT_FOUND
```

Router 为每个 route key 保存最高已观察 revision。cache entry 不能满足该 fence 时必须重新 resolve。

ListRoutes 逐个读取 16 个 route bucket，每个 bucket 返回独立 snapshot revision。WatchRoutes 使用相同 bucket
identity 和 durable changefeed cursor；收到 reset 后先重建该 bucket snapshot，再从 head 继续。

## Reserve and mutation

同一 `(group, route_key)` 的 cache miss 使用进程内 single-flight，但业务幂等由 Registry workflow 保证。
`(group, route_key)` 是唯一北向 Sandbox identity；Router 不接受 concrete SID 作为独立定位键，也不创建
execution identity。`ReserveSandbox` 返回：

```text
READY      exact forwarding projection
PENDING    committed workflow revision; caller may retry
CONFLICT   immutable request mismatch
UNAVAILABLE / NEED_LEADER
```

Resume/Delete 只携带 `(group, route_key)` 和最低 revision；Registry 从当前 Route 内部取得 concrete SID。
Build 注册以 `(group, build_id)` 为键，Registry 只返回 `BUILD_REGISTERED` 的 immutable node binding，或
pre-accept placement tombstone。

## Data-plane forwarding

READY Route 至少包含：

```text
SandboxID, NodeID, NodeEpoch, data_endpoint
registry_generation, Binding digest, route revision
template reference, target port
AccessToken and TrafficAccessToken
```

Router 向 node proxy 发起一次 fenced connect，携带预期 SandboxID、NodeID、NodeEpoch、generation 和 Binding
digest。node 必须逐项匹配当前 protected object metadata。

控制面路径中的 northbound route-key 先解析 Route，再由 Router 改写为 concrete SID；node JSON 响应中的
concrete SID 改回 route-key。Build 注册后的 trigger/files/status/logs 直接转发到 registration binding 指定的
node，携带同样的 generation/Binding fence 和 `build_id`，不经过 Registry lifecycle API、不重新 Placement。
Router 到 node 必须使用 Router-role mTLS。

以下 typed proxy error 表示 cache execution 已过期：

```text
NOT_FOUND
WRONG_NODE_EPOCH
WRONG_BINDING
ROUTE_INACTIVE
```

Router 删除该 cache entry，把 `min_revision` 提高到旧 revision + 1，并 resolve 当前 route key。原始数据请求
直接失败，不重放到新 execution；因此 stale Follower READY 最多触达错误 node 一次，且在 node fence 处
被拒绝。

`UNAUTHORIZED` 只清 cache，不证明存在更新 revision。普通 upstream transport error 也不能触发 replacement。

## Authentication

北向控制请求使用 `X-API-KEY`。数据面根据配置验证 e2b signed token 或 `X-Access-Token`，再将 execution
capability 注入 node 请求。caller API key 和 execution capability 不可互换，也不会写入 Registry。

```yaml
auth:
  api_key: enforce
  data_plane: enforce
  cache_ttl: 60s
```

`off` 和 `log` 仅用于受控环境。生产必须使用 `enforce`。

## Cache behavior

Route cache key 仅包含 group 和 route key，entry 内保存当前 concrete SID、generation 和 revision；不存在 SID
反向索引。以下情况删除 entry：

```text
Permit/generation no longer authorized
route TTL or idle timeout elapsed
node returned a typed stale fence error
strong resolution observed a newer revision
```

Build cache同样绑定 generation 和 build revision，但不参与 Sandbox data-plane forwarding。

## Configuration

```yaml
domain: sandboxes.example.com
registry_layout:
  chain: /etc/kuasar/registry-layout-chain.json
  keys: /etc/kuasar/Registry Layout-keys.json
  guard: /var/lib/kuasar/router-registry-layout-guard.json
registry_tls:
  cert: /etc/kuasar/tls/router.crt
  key: /etc/kuasar/tls/router.key
  ca: /etc/kuasar/tls/ca.crt
node_tls:
  cert: /etc/kuasar/tls/router.crt
  key: /etc/kuasar/tls/router.key
  ca: /etc/kuasar/tls/ca.crt
providers:
  endpoints:
    - { name: provider-1, endpoint: "https://provider-1.example:7900" }
  tls: { cert: /etc/kuasar/tls/router.crt, key: /etc/kuasar/tls/router.key, ca: /etc/kuasar/tls/ca.crt }
ingress:
  listen: ":443"
  tls: { cert: /etc/kuasar/tls/wildcard.crt, key: /etc/kuasar/tls/wildcard.key }
auth: { api_key: enforce, data_plane: enforce, cache_ttl: 60s }
cache: { route_ttl: 5m, idle_timeout: 2m }
```

Router 不使用 bootstrap member endpoint；准确 Registry endpoint 来自 verified Registry Layout。
