# cluster-router — e2b 统一入口与活动连接缓存

`cluster-ctl router` 是 cluster 的北向入口,同时承载 e2b 控制面和数据面。它不持路由权威,
不订阅 route 或 node_list;它通过 group 定位 route owner,在 miss / fail-fast 时调用 Reserve,
热路径直接转发到 node。

## 1. 概述

### 1.1 请求路径

```text
client
  │ HTTPS e2b control/data
  ▼
router
  │ active cache hit / route cache hit
  ├────────────────────────────────────► node proxy
  │
  │ miss / stale / fail-fast
  ▼
route_link owner
  │ Reserve / Resolve
  ▼
node owner / placer / node
```

### 1.2 原则

1. **每个请求必须有 group**:`X-Kuasar-Sandbox-Group` 是 cluster 控制面和数据面的分片依据。
2. **Reserve 返回即 READY 或失败**:router 不在 Reserve 后订阅 watch 等结果。
3. **热路径优先复用活动连接**:同一路由已有活动连接或近期解析缓存时,新请求不触发 Reserve。
4. **fail-fast 失效**:node 返回 sandbox 不存在、token 不匹配、连接失败时,router 淘汰本地缓存并重新 Reserve。
5. **数据面字节不进 registry**:registry 只参与 cold/miss/fail-fast 控制面。
6. **鉴权材料来自 placer/provider 侧**:router 调 route owner 的 verify-key;registry failover 到 ready placer,
   不读取 auth_key;router 不接触 manifest_key。

## 2. 命令行

```text
cluster-ctl router --config /etc/cluster-ctl/router.yaml
```

## 3. 配置

```yaml
domain: sandboxes.example.com

registry:
  bootstrap: registry-1.example:7700

ingress:
  listen: ":443"

auth:
  api_key: enforce
  data_plane: enforce
  cache_ttl: 60s

cache:
  route_ttl: 5m
  idle_timeout: 2m
```

| 字段 | 说明 |
|---|---|
| `domain` | cluster 服务域 |
| `ingress.listen` | 控制面和数据面入口 |
| `ingress.tls` | 通配证书 |
| `registry.bootstrap` | registry bootstrap endpoint,用于拉取 membership |
| `registry.tls` | 到 registry 控制面的 mTLS |
| `auth.api_key` | `enforce` / `log` / `off` |
| `auth.data_plane` | 数据面凭证校验:`enforce` / `log` / `off` |
| `auth.cache_ttl` | API key 校验缓存 |
| `cache.route_ttl` | 路由解析缓存 TTL |
| `cache.idle_timeout` | 活动连接空闲淘汰 |
| `metrics_listen` | Prometheus 端点 |

## 4. Registry Membership

router 启动后通过 bootstrap 拉取 registry membership:

```text
GET /cluster/membership
```

返回 active / next / old_grace registry members、membership label 和 owner count。router 对每个 group 用
`LocateN(group, active.members, route_link.owner_count)` 定位 route owner。

刷新 membership 时,router 尝试 bootstrap 和已知成员,选择 active version 最新的结果。route_link 请求遇到
5xx/409 时刷新并重试一次。

router 不参与 registry 成员健康检测,不订阅 route,也不订阅 node_list。

## 5. 寻址

| 请求 | 必需身份 | 行为 |
|---|---|---|
| create | group + route_key(可缺省生成) | 提取并校验本次请求的 `kuasar-sandbox.restore`,定位 route owner 后随 `ReserveSandbox` 传递 |
| connect/resume | group + route_key / sandbox_id | 定位 route owner 后调用 `ReserveSandbox` 恢复 |
| kill | group + route_key + sandbox_id | 定位 route owner 后由 registry 经 node-link 下发 `CmdDelete` |
| get/connect/pause/timeout/export | group + route_key + sandbox_id | route owner 解析 node 后转发到 node 控制面 |
| list/get | group | 读取 group 分片 |
| data plane | group + route_key + sandbox_id + port | cache 命中后建立一次性 CONNECT;miss Reserve |
| build register | group + build_id | 生成稳定 id 后调用 `ReserveBuild` |
| build status/files | group + build_id | 定位 build node 后转发 |

`route_key` 是稳定会话身份,`sandbox_id` 是当前实例身份。cluster 内部总是同时维护二者。

create 可在 body metadata 中携 `kuasar-sandbox.restore`,或使用
`X-Kuasar-Sandbox-Restore`;同一请求 Header 胜出。router 只提取该命名空间,不会把
Prefetch 变成 template、group 或节点统一策略。未提供、`{}` 与显式 `off` 均为关闭;
只有本次 create 显式提供 `memory` 才启用。Header 存在时 router 不读取 create body;
未提供 Header 时 create body 上限为 16 MiB,超限返回 **413**。

## 6. 缓存模型

### 6.1 决策顺序

```text
request
  │
  ├─ active connection cache hit
  │     └─► forward to node
  │
  ├─ route resolution cache hit
  │     └─► forward to node
  │
  └─ miss
        └─► Reserve/Resolve via route owner
              └─► cache route, then forward
```

active connection cache 是热路径主优化。route resolution cache 是短期兜底,避免每个新 HTTP request 都打
registry。router 不靠全量 route stream 保持一致。

### 6.2 route resolution cache

key:

```text
(group, route_key, sandbox_id)
```

value:

```text
{node_id, data_endpoint, sandbox_id, access_token, traffic_access_token, target_port, route_rev, expires, last_used}
```

端口不从 route_key 解析。数据面端口来自请求显式端口(`E2b-Sandbox-Port`、`<port>-<sandbox_id>` Host
或 CONNECT authority)或 route_link 返回的 `target_port`。`target_port>0` 表示强制端口:请求未显式带端口时
使用它;请求显式端口与它不一致时拒绝 400。两者都缺失时拒绝 400,router 不默认 49983。

### 6.3 active route cache

active cache 只表示某个 `(group, route_key, sandbox_id)` 正有请求在途,用于避免 route cache 被普通
TTL/idle 清理时让同一路由的新请求重新 Reserve。它不保存、复用或共享任何数据面 TCP 连接。

```text
client A ── data request ──┐
client B ── data request ──┼─ same active route ─► each request opens its own CONNECT to node proxy
client C ── CONNECT ───────┘
```

同一路由已有活动请求时,新请求可复用本地 route decision,但转发到 node proxy 时总是重新建立一次性
CONNECT。普通 HTTP 请求也先对 node proxy 发 CONNECT,再在隧道内发送一条 HTTP 请求;外部 CONNECT
请求则把该隧道直接交给客户端。这样不会出现不同沙箱或不同端口复用同一条 router→node TCP 连接。

### 6.4 singleflight

同一 `(group, route_key)` 的并发 miss 只允许一个 Reserve 在途。其他请求等待结果或共享失败。

### 6.5 失效

以下情况淘汰缓存:

- node proxy 在 CONNECT 握手阶段返回带 `X-Kuasar-Proxy-Error: not_found|unauthorized` 的 404/401。
- node data_endpoint 连接失败。
- route TTL 或 idle timeout 到期。

握手成功后的 HTTP 401/403/404 是沙箱内应用或 envd 的响应,不淘汰 route。淘汰后下一次请求重新
Reserve。迁移/恢复时允许首个请求付出一次 fail-fast 代价,不为此维护 router route 订阅。

## 7. 控制面

router 校验 API key 与 group 关系时调用 route owner `verify-key`;route owner failover 到 ready placer,
实际校验使用 placer/provider 侧 `auth_key` 或等价 verify 能力。router 不接触 `manifest_key`。

| 操作 | 行为 |
|---|---|
| create/connect | 新建 create 随 Reserve 传递请求级 restore policy;已有实例的 connect 读取其持久化 metadata;READY 后返回 |
| kill | route owner 精确匹配 group + route_key + sandbox_id,经 node-link 下发 `CmdDelete` |
| get/connect/pause/timeout/export | route owner 解析 node 后转发 |
| get/list | 读 group route_link |
| build register | 生成稳定 build_id/template_id 后调 ReserveBuild |
| build status/files | 按 group+build_id 定位 node 后转发 |

## 8. 数据面

router 在 sid-host 数据面路径先校验凭证:

- 普通数据面请求必须带 `X-Access-Token: <route_link.access_token>`。
- envd 预签名文件 URL 可不带 token,但仅限 `port=49983`、`GET/POST /files`、非空 `signature` query。
- 同一请求若带了非空但错误的 `X-Access-Token`,不回退到 signature。

普通 token 请求转发时注入:

```text
CONNECT sandbox:<port> HTTP/1.1
E2b-Sandbox-Id: <sandbox_id>
E2b-Sandbox-Port: <port>
X-Access-Token: <route_link.access_token>
```

router 对 node proxy 的 CONNECT 握手总携带 `X-Access-Token`,由 node proxy 完成最后一跳鉴权。握手成功后,
普通 token 请求在隧道内写入 HTTP 请求并继续带 `X-Access-Token`;signature 请求的隧道内 HTTP 请求不注入
`X-Access-Token`,让 envd 继续按同一 URL 验签。

CONNECT 长连接使用同一 route resolution,但 tunnel 自身不复用。连接断开后保留 route cache 至 idle/TTL
或 fail-fast 失效。

## 9. 可靠性

| 事件 | 行为 |
|---|---|
| router 崩溃 | 本地缓存丢失;LB 切走;重启后 miss Reserve |
| registry moved | 刷新 membership 并重试 |
| registry owner 故障 | owner set 内按顺序 failover |
| Reserve timeout | 返回 503/504;本地 singleflight 释放 |
| stale cache | fail-fast 淘汰并重试 |
| node data endpoint 失败 | 淘汰 route cache,重新 Reserve |

## 10. 性能

- active cache 命中:一次本地查表 + 一条新的 router→node CONNECT。
- route cache 命中:本地 route resolution + 一条新的 router→node CONNECT。
- miss:一次 registry Reserve/Resolve + node 快照恢复/启动。
- router 不因 group 总量增长而维护全量 route 流。
- 控制面转发可使用 HTTP transport 连接池,连接池按 `data_endpoint` 隔离;数据面转发不使用 pooled transport。

## 11. See Also

- [cluster.md](cluster.md) — registry membership、route_link、Reserve 与数据模型。
- [cluster-placer.md](cluster-placer.md) — verify-key、Place 和 key distribution 来源。
- [node-proxy.md](node-proxy.md) — node 内部数据面转发、CONNECT 和 envd signature。
