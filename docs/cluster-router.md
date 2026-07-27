# cluster-router — e2b 统一入口与路由缓存

`cluster-ctl router` 是 cluster 的北向入口,同时承载 e2b 控制面和数据面。它不持路由权威,
不订阅 route 或 node_list;它通过 group 定位 route owner。显式 create/connect 调用对应
Reserve operation,数据面 cache miss / fail-fast 先 Resolve,已知非 READY sandbox 再调用 data
Reserve;热路径通过本地 route cache 直接转发到 node。

## 1. 概述

### 1.1 请求路径

```text
client
  │ HTTPS e2b control/data
  ▼
router
  │ READY route cache hit
  ├────────────────────────────────────► node proxy
  │
  │ data cache miss / stale / fail-fast
  ▼
route_link owner
  │ Resolve
  │ known non-READY route ──► operation=data Reserve
  ▼
node owner / placer / node
```

### 1.2 原则

1. **每个请求必须有 group**:`X-Kuasar-Sandbox-Group` 是 cluster 控制面和数据面的分片依据。
2. **Reserve 操作显式分类**:create/data 只在 READY 时返回;connect 在 node 同步准备并返回
   typed ConnectResult 后完成,不等待异步 resume。router 不订阅 route watch。
3. **热路径优先使用 route cache**:近期解析的 READY route 命中时,新请求不触发 Resolve/Reserve;
   在途请求不作为新请求的路由来源。
4. **fail-fast 失效**:node 返回 sandbox 不存在、token 不匹配、连接失败时,router 淘汰本地缓存并重新 Resolve;
   只有已存在但非 READY 的 route 才继续 Reserve。
5. **数据面字节不进 registry**:registry 只参与显式 create/connect、已知 route 激活和
   miss/fail-fast Resolve。
6. **根凭据用途分离**:create/connect 由 Reserve 强制验证原始 API key;其它控制操作由 router 调
   route owner 的 verify-key,registry failover 到 ready placer,由 placer 使用 provider 的 APISecret 验证。
   READY route 的受保护返回同时投影该 sandbox 已绑定的 APISecret、ServiceSecret 和用途明确的 access tokens
   给可信 router;ManifestKey 原文不进入 registry/router 路由链路。

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
| create | group + route_key(可缺省生成) | 提取并校验本次请求的 `kuasar-sandbox.restore` 与 `kuasar-sandbox.credentials`,定位 route owner 后随 `ReserveSandbox` 传递 |
| kill | group + route_key + sandbox_id | 定位 route owner 后由 registry 经 node-link 下发 `CmdDelete` |
| connect | group + route_key + stable sandbox_id | 调用 `operation=connect` Reserve;registry 经 node-link 下发 CmdConnect,不 Resolve 或转发 node HTTP `/connect` |
| get/pause/timeout/export | group + route_key + stable sandbox_id | route owner 解析当前 NodeSandboxID,Router 重写路径后转发到 node 控制面 |
| list/get | group | 读取 group 分片 |
| data plane | group + route_key + stable sandbox_id + port | READY cache 命中后建立一次性 CONNECT;cache 中已知非 READY route 调用 `operation=data` Reserve;miss 先 Resolve |
| build register | group + build_id | 生成稳定 id 后调用 `ReserveBuild` |
| build status/files | group + build_id | 定位 build node 后转发 |

`route_key` 是 group 内 route 定位键,`sandbox_id` 是稳定公开身份。Registry 在首次 create 时生成
SandboxID;同节点 resume、跨节点迁移和 re-place 不改变它。受保护 route 另携当前
`node_sandbox_id=<sandbox_id>-g<N>`。Router 使用稳定 SandboxID 寻址,只在 node 边界替换为
NodeSandboxID;公开响应不暴露 NodeSandboxID。

create 可在 body metadata 中携 `kuasar-sandbox.restore` / `kuasar-sandbox.credentials`,或使用
对应的 `X-Kuasar-Sandbox-Restore` / `X-Kuasar-Sandbox-Credentials`;每个 Header 只覆盖同名
metadata object,credentials 不做字段级 merge。router 只提取并严格校验这两个请求级
namespace。未提供 restore、`{}` 与显式 `off` 均为关闭;只有本次 create 显式提供
`memory` 才启用。两个 namespace 都由 Header 提供时无需读取 body;否则 create body
上限为 16 MiB,超限返回 **413**。

Router 调用同一 `POST /route-link/reserve` 时按 operation 组装不同请求:

- `create`:query 为 `operation=create&group=&route_key=`,Header 携 `X-API-KEY`,body 只携上述 create config。
- `connect`:query 为 `operation=connect&group=&route_key=&sid=[&timeout=]`,Header 携 `X-API-KEY`
  和可选 `X-Kuasar-Migration-Token`,body 为空。
- `data`:query 为 `operation=data&group=&route_key=&sid=[&port=]`,Header 携 `X-Access-Token`,body 为空。

connect/data 中的 `sid` 均是客户期望的稳定 SandboxID,用于防止 route_key 被删除重建后请求
跨越 lineage。

## 6. 缓存模型

### 6.1 决策顺序

```text
request
  │
  ├─ READY route resolution cache hit
  │     └─► forward to node
  ├─ non-READY route resolution cache hit
  │     └─► operation=data Reserve, then forward
  │
  └─ miss
        └─► Resolve via route owner
              ├─ READY ──► cache route, then forward
              └─ known non-READY ──► Reserve, cache route, then forward
```

READY route resolution cache 是热路径优化,避免每个新 HTTP request 都打 registry。缓存命中
非 READY route 时仍执行 data Reserve。router 不靠全量 route stream 保持一致。

### 6.2 route resolution cache

key:

```text
(group, route_key, sandbox_id)
```

value:

```text
{node_id, data_endpoint, sandbox_id, node_sandbox_id, profile, auth_sandbox_id,
 api_secret, api_secret_fingerprint, manifest_key_fingerprint, service_secret,
 envd_access_token, traffic_access_token, forward_access_token,
 target_port, route_revision, expires, last_used}
```

该 cache 是 router 进程内的受保护状态,不得通过公开响应、日志或观测接口输出 root/token。
`envd_access_token` 用于 e2b envd 端口,`forward_access_token` 用于其他 forward 目标。
`traffic_access_token` 随受保护 route 保存并在 create 响应中返回,cluster router 不用它执行数据面鉴权。

`route_revision` 是 group route recordSet 的已提交 revision。新结果的 revision 更低时不覆盖 cache;
revision 相同但 NodeSandboxID 不同时也拒绝覆盖。这使迟到的旧代际 Resolve/Reserve 结果不能
回退已切换的新节点目标。

端口不从 route_key 解析。数据面端口来自请求显式端口(`E2b-Sandbox-Port`、`<port>-<sandbox_id>` Host
或 CONNECT authority)或 route_link 返回的 `target_port`。`target_port>0` 表示强制端口:请求未显式带端口时
使用它;请求显式端口与它不一致时拒绝 400。两者都缺失时拒绝 400,router 不默认 49983。

### 6.3 在途请求

在途请求计数只表示某个 `(group, route_key, stable sandbox_id)` 正在转发。每个请求在开始转发后
持有自身的 route 副本,不再依赖 cache 条目。该计数不作为新请求的路由来源,不保存、复用或
共享任何数据面 TCP 连接,也不阻止更高 RouteRevision 替换当前 NodeSandboxID。

```text
client A ── data request ──┐
client B ── data request ──┼─ same resolved route ─► each request opens its own CONNECT to node proxy
client C ── CONNECT ───────┘
```

同一路由的新请求只能从 route cache 取当前 decision,转发到 node proxy 时总是重新建立一次性
CONNECT。普通 HTTP 请求也先对 node proxy 发 CONNECT,再在隧道内发送一条 HTTP 请求;外部 CONNECT
请求则把该隧道直接交给客户端。这样不会出现不同沙箱或不同端口复用同一条 router→node TCP 连接。

### 6.4 Reserve 并发

Router 不合并 Reserve 请求,避免把不同 operation、凭据、timeout 或 migration token 合并到同一次调用。
Registry 只对并发 create 合并创建状态机;data 通过 route CAS 和稳定 lineage wait 收敛同一次激活;
connect 的每个请求均针对当前 NodeSandboxID 独立下发 CmdConnect。未知 route 的数据面请求只
Resolve,不会隐式创建 sandbox。

### 6.5 失效

以下情况淘汰缓存:

- node proxy 在 CONNECT 握手阶段返回带 `X-Kuasar-Proxy-Error: not_found|unauthorized` 的 404/401。
- node data_endpoint 连接失败。
- route TTL 或 idle timeout 到期。

握手成功后的 HTTP 401/403/404 是沙箱内应用或 envd 的响应,不淘汰 route。淘汰后下一次请求重新
Resolve;若解析到已存在但非 READY 的 route,再 Reserve 激活。迁移/恢复时允许首个请求付出一次
fail-fast 代价,不为此维护 router route 订阅。

## 7. 控制面

API key 鉴权不可关闭。create/connect 把客户端原始 API key 交给 Reserve,由 Registry 在任何生命周期
副作用前完成验证;其它控制操作由 router 调 route owner `verify-key`,route owner failover 到 ready placer,
实际校验由 placer 使用 provider 侧 APISecret 完成。ManifestKey 只用于内容路径,不参与该校验。

| 操作 | 行为 |
|---|---|
| create | 调用 `operation=create` Reserve,传递请求级 restore policy 与 credentials object;credentials 在写普通 metadata 前分离 |
| kill | route owner 精确匹配 group + route_key + sandbox_id,经 node-link 下发 `CmdDelete` |
| connect | 调用 `operation=connect` Reserve;Registry 对当前 NodeSandboxID 下发 CmdConnect,验证 typed ConnectResult 后返回;Router 不 Resolve,不二次转发 node HTTP `/connect` |
| get/pause/timeout/export | Resolve 当前 route,把公开 SandboxID 路径替换为 NodeSandboxID 后转发;typed get 响应重写回稳定 SandboxID |
| get/list | 读 group route_link |
| build register | 生成稳定 build_id/template_id 后调 ReserveBuild |
| build status/files | 按 group+build_id 定位 node 后转发 |

connect 的 Node Ack 只投影 NodeSandboxID、TemplateID、Profile 和 Envd/Traffic/ForwardAccessToken;不返回
root 凭据或 fingerprint。Router 校验 Ack 与受保护 Route 一致,再组装仅包含稳定 SandboxID 和
profile 对应公开 token 的 e2b 响应。node 已同步完成校验、可选迁移导入、deadline 持久化和
凭据读取;resume 在 Ack 后异步进行。

## 8. 数据面

router 在 sid-host 数据面路径先校验凭证:

- e2b profile 的 49983/49999 端口必须带 `X-Access-Token: <envd_access_token>`。
- 其他 forward 目标必须带 `X-Access-Token: <forward_access_token>`。
- `traffic_access_token` 供外部网关及 e2b 数据面组件使用,cluster router 不消费。
- envd 预签名文件 URL 可不带 token,但仅限 `port=49983`、`GET/POST /files`、非空 `signature` query。
- 同一请求若带了非空但错误的 `X-Access-Token`,不回退到 signature。

普通 token 请求转发时复用客户端提供的 `X-Access-Token`:

```text
CONNECT sandbox:<port> HTTP/1.1
E2b-Sandbox-Id: <node_sandbox_id>
E2b-Sandbox-Port: <port>
X-Access-Token: <client X-Access-Token>
```

客户请求的 Host/路径使用稳定 SandboxID。Router 向 node proxy 建立外层 CONNECT 时使用当前
NodeSandboxID;普通 HTTP 的内层 Host 也改为 `<port>-<node_sandbox_id>.<domain>`。内层请求已有
`E2b-Sandbox-Id` 时把值替换为 NodeSandboxID,保留该 Header 供用户应用感知当前 node-local ID。

对于不带 token 的 envd `/files` signature 请求,router 先使用受保护 route 中的 `EnvdAccessToken` 验签。
该校验发生在已知非 READY sandbox 的 Reserve/激活之前;验签失败直接返回 401,不得唤醒 sandbox。
验证通过后,router 仅在到 node proxy 的外层 CONNECT 携带该 `EnvdAccessToken`,由 node proxy 完成最后一跳
鉴权;隧道内 HTTP 请求保持原样且不注入 `X-Access-Token`,让 envd 使用原 URL 中的 signature 再次验证。
普通 token 请求在外层 CONNECT 和隧道内 HTTP 请求中都保留客户端原始 `X-Access-Token`。

已知 route 非 READY 时,Router 只在上述本地鉴权成功后调用 `operation=data` Reserve。普通请求
把客户 token 传给 Registry;签名 `/files` 把已用于验签的受保护 EnvdAccessToken 传给 Registry。
Registry 按当前 profile/端口再验证凭据,通过后才允许 CmdConnect 激活并等待 READY。
`auth.data_plane=off` 只跳过 Router 对普通 READY 转发的本地 token 闸门,不取消 Reserve 激活的鉴权;
带显式错误 token 的 `/files` 在任何 Router 鉴权模式下都不回退 signature。

CONNECT 长连接使用同一 route resolution,但 tunnel 自身不复用。连接断开后保留 route cache 至 idle/TTL
或 fail-fast 失效。

## 9. 可靠性

| 事件 | 行为 |
|---|---|
| router 崩溃 | 本地缓存丢失;LB 切走;重启后 cache miss 重新 Resolve |
| registry moved | 刷新 membership 并重试 |
| registry owner 故障 | owner set 内按顺序 failover |
| Reserve timeout | 返回 503/504;当前请求结束,Router 不保留 Reserve flight |
| stale cache | fail-fast 淘汰并重试 |
| node data endpoint 失败 | 淘汰 route cache,下一次请求重新 Resolve |
| 旧代际迟到 | 低 RouteRevision 或同 revision 但不同 NodeSandboxID 不覆盖 cache;旧请求失败不驱逐新代际 |

## 10. 性能

- READY route cache 命中:本地 route resolution + 一条新的 router→node CONNECT。
- data miss:一次 registry Resolve;只有解析到已存在但非 READY 的 route 才增加 data Reserve
  与 node 快照恢复/启动。
- router 不因 group 总量增长而维护全量 route 流。
- 控制面转发可使用 HTTP transport 连接池,连接池按 `data_endpoint` 隔离;数据面转发不使用 pooled transport。

## 11. See Also

- [cluster.md](cluster.md) — registry membership、route_link、Reserve 与数据模型。
- [cluster-placer.md](cluster-placer.md) — verify-key、Place 和 key distribution 来源。
- [node-proxy.md](node-proxy.md) — node 内部数据面转发、CONNECT 和 envd signature。
