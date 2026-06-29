# cluster-router — e2b 统一入口与活动连接缓存

router 是 cluster 的北向入口,同时承载 e2b 控制面和数据面。它不持路由权威,也不订阅全量 route
watch;它通过 group 定位 route owner,在 miss 时调用 Reserve,在热路径通过活动连接缓存直接转发到
node。

## 1. 概述

### 1.1 原则

1. **每请求必须有 group**:`X-Kuasar-Sandbox-Group` 是 cluster 数据面和控制面的分片依据。当前不
   设计无 group 的 sid-only 入口。
2. **Reserve 返回即 READY 或失败**:router 不在 Reserve 后再订阅 watch 等结果。
3. **热路径优先复用活动连接**:同一路由已有活动连接或近期解析缓存时,新请求不触发 Reserve。
4. **fail-fast 失效**:node 返回 sandbox 不存在、token 不匹配、连接失败时,router 淘汰本地缓存并
   重新 Reserve。
5. **数据面字节不进 registry**:router 只在 cold/miss/fail 时调用 registry。
6. **鉴权材料来自 scaler/provider 侧**:router 调 route owner 的 verify-key;registry 只转发到 ready scaler,
   不读取 auth_key;router 不接触 manifest_key。

## 2. 命令行

```text
cluster-ctl router --config /etc/cluster-ctl/router.yaml
```

## 3. 配置

| 字段 | 说明 |
|---|---|
| `domain` | cluster 服务域 |
| `ingress.listen` | 控制面和数据面入口 |
| `ingress.tls` | 通配证书 |
| `registry.bootstrap` | registry bootstrap endpoint,用于拉取 membership |
| `registry.tls` | 到 registry 控制面的 mTLS |
| `auth.api_key` | `enforce` / `log` / `off` |
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
`LocateN(group,active.members,route_link.owner_count)` 定位 route owner。刷新 membership 时,router 会尝试
bootstrap 和已知成员,选择 active version 最新的结果;route_link 请求遇到 5xx/409 时会刷新并重试一次。

router 不参与 registry 成员健康检测,也不订阅 route 或 node_list。

## 5. 寻址

| 请求 | 必需身份 | 行为 |
|---|---|---|
| create | group + route_key(可缺省生成) | 定位 route owner 后调用 `ReserveSandbox` |
| connect/resume | group + route_key / sandbox_id | 定位 route owner 后调用 `ReserveSandbox` 恢复 |
| pause/kill/timeout | group + route_key/sandbox_id | 定位 route owner 后转发 node |
| list/get | group | 读 group 分片 |
| data plane | group + route_key + sandbox_id + port | cache 命中直转;miss Reserve |
| build | group + build_id | group 分片解析 build node 后转发 |

`route_key` 是稳定会话身份,`sandbox_id` 是当前实例身份。cluster 内部总是同时维护二者。

## 6. 缓存模型

### 6.1 route resolution cache

key:

```text
(group, route_key, sandbox_id)
```

value:

```text
{node_id, data_endpoint, sandbox_id, route_version, expires, last_used}
```

端口和协议不参与 route owner 解析;它们保留在 `<port>-<sandbox_id>.<domain>` Host 和请求方法中,由
node proxy 执行最后一跳。

`access_token` 存储在 registry route 记录中,由 scaler 在 Place 时使用 `auth_key` 生成:

```text
access_token = MAC(auth_key, sandbox_id)
```

### 6.2 active connection cache

HTTP 请求复用到 node 的 pooled transport。CONNECT/WebSocket 不能复用同一 TCP tunnel,但会维持
route active 标记和 resolution cache。active cache 是热路径主优化;它比维护全量 route 流更符合
会话流量模型。

### 6.3 singleflight

同一 `(group, route_key)` 的并发 miss 只允许一个 Reserve 在途。其他请求等待结果或共享失败。

### 6.4 失效

以下情况淘汰缓存:

- node proxy 返回 401/403:token 或鉴权材料陈旧。
- node 返回 404: sandbox 不存在。
- node 连接失败/502/connection reset。
- route TTL/idle timeout 到期。

淘汰后下一次请求重新 Reserve。迁移/恢复时允许首个请求付出一次 fail-fast 代价,不为此维护 router
route 订阅。

## 7. 控制面

router 校验 API key 与 group 关系时调用 route owner `verify-key`;route owner 只转发到 ready scaler,
实际校验使用 scaler/provider 侧的 `auth_key` 或等价 verify 能力。router 不接触 `manifest_key`。

| 操作 | 行为 |
|---|---|
| create/connect | 调 Reserve;READY 后返回 |
| pause/kill/timeout | route owner 解析 node 后转发 |
| get/list | 读 group route_link |
| build register | 调 ReserveBuild |
| build status/files | 按 group+build_id 定位 node 后转发 |

## 8. 数据面

router 转发时注入:

- `E2b-Sandbox-Id: <sandbox_id>`
- `X-Access-Token: <route_link.access_token>`

node proxy 执行最后一跳 `(sandbox_id,port) -> guest envd/floatingip` 并校验 token。bare 与 e2b 在
cluster router 看来一致,差别在 node 最后一跳。

## 9. 可靠性与性能

| 事件 | 行为 |
|---|---|
| router 崩溃 | 缓存丢失;LB 切走;重启后 miss Reserve |
| registry moved | 刷新成员表并重试 |
| registry owner 故障 | owner set 内按顺序 failover |
| Reserve timeout | 返回 503/504;本地 singleflight 释放 |
| stale cache | fail-fast 淘汰并重试 |

性能目标:

- active cache 命中:一次本地查表 + pooled transport 转发。
- miss:一次 registry Reserve + node 快照恢复/启动。
- router 不因 group 总量增长而维护全量 route 流。
