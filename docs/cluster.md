# cluster — registry 自聚簇、会话路由与沙箱编排

`cluster-ctl` 是面向大规模部署的集群控制面,把数千个 `node-ctl serve` 节点聚合成一个
按 **sandbox-group** 分片的逻辑沙箱池。它对外提供 e2b 兼容控制面与数据面入口,对内维护
会话路由、放置、构建、密钥分发和节点故障收敛。

cluster 的核心目标是:稳态数据面请求不经过 registry,冷路径通过 registry 自聚簇完成放置 /
恢复 / 构建预留;Registry 成员故障只影响其参与的小逻辑集群,不触发全局重分片;成员变更由
运维版本化触发,以 learner 预同步和 per-shard 短栅栏完成业务无损切换。

## 1. 概述

### 1.1 设计原则

1. **sandbox-group 是唯一业务分片键**:路由、构建、placement、group 配置均按 group 定位。
   每个北向请求必须携带 `X-Kuasar-Sandbox-Group`;当前不支持无 group 的 cluster 数据面入口。
2. **registry 自身是可靠状态集群**:状态由 registry 成员直接复制,不依赖外部存储后端。
   Registry 容忍单成员故障。整套 registry 完全下电后不要求自动恢复沙箱。
3. **节点是存活态真相之源**:sandbox/build 是否还在运行以 node 上报为准;registry 是路由权威和
   可重建聚合。node 整机重启直接清空,不重拉旧 sandbox。
4. **热路径旁路控制面**:router 对活动连接和近期路由做本地缓存;命中时直接转发到 node,不调用
   `Reserve`。miss / fail-fast 后才走 registry。
5. **SWIM 不参与分片计算**:Registry 成员故障只影响 owner set 内的可达性判断;`LocateN` 的输入
   仅为版本化成员表。成员变更只由运维更新 `membership_version` 触发。
6. **放置与执行分层**:scaler 负责 group 导入、WATCH_LIST、shuffle-sharding 与 Place;node owner
   负责节点连接、命令投递、运行态与资源 admission。

### 1.2 角色

| 角色 | 职责 |
|---|---|
| registry member | 组成 registry 小集群,承载 `route_link` / `node_link` / `node_list` / `scale_link` 命名空间 |
| router | e2b 统一入口;按 group 定位 route owner;活动连接缓存;miss 时调用 Reserve |
| scaler | 消费 node_list WATCH_LIST;导入 group;维护 placement / key allocation;响应 PlaceSandbox / PlaceBuild |
| node | 运行 sandbox/build;经 node_link 上报全量清单与事件;接收 create/connect/delete/key/build 命令 |

## 2. Registry 命名空间

Registry 成员集由运维配置和 `membership_version` 定义。每个命名空间用 `pkg/maglev.LocateN`
从版本化成员表定位 owner set。

| 命名空间 | key | owner set | 内容 |
|---|---|---|---|
| `route_link` | group | `LocateN(group,K)` | route 记录、build 记录、placement 记录;每个 owner 持完整 group 视图 |
| `node_link` | node_id | `LocateN(node_id,N)` | node 连接、sandbox/build 清单、labels、水位、build 预算;每个 owner 持完整 node 视图 |
| `node_list` | `__NODE_LIST__#shard` | `LocateN(key,M)` | 低频节点目录与 labels,向 scaler 提供 WATCH_LIST |
| `scale_link` | scaler_id | 全复制 | scaler 实例目录和能力 |

`node_link` 中只有当前连接 owner 持有实际 node TCP/h2c 连接;其余 owner 通过复制持有完整视图。
`route_link` 的所有 owner 均可响应查询和订阅。写入达到 W quorum 后提交,随后继续复制到全
owner set,用于完整视图和本地 waiter 唤醒。

## 3. 成员与分片变更

### 3.1 成员故障

成员故障由 SWIM 探测,但不改变 `LocateN` 结果。某成员不可达时:

- 它参与的 owner set 降一格,只要可达 owner 数 ≥ W 即继续服务。
- 若可达 owner 数 < W,该 shard 停写(CP),不降级乱写。
- 故障不触发 reshard;恢复成员须 catch-up 后才能重新参与 quorum。

### 3.2 成员变更

成员变更采用 **版本化成员表 + learner 预同步 + per-shard 短栅栏 + moved/grace 转发**:

```text
stable(v)
  -> prepare(v+1)
  -> catchup(v+1)
  -> switching(v+1, barrier_seq)
  -> stable(v+1)
  -> old_grace(v, expires)
```

1. 运维提交新成员表,生成 `membership_version=v+1`。
2. Registry 计算受影响的 route/node/node_list shard。
3. 新 owner 先作为 learner,从旧 owner 拉 snapshot,再接收 delta。
4. learner 追到 barrier 后,仅对该 shard 进入短 `switching` 栅栏:新写短排队,超时返回
   `Retry-After`。
5. 新 owner set 确认应用到 `barrier_seq` 后 flip 到 `v+1`。
6. 旧 owner 进入 `old_grace`,只返回 `moved(v+1,new_owners)` 或代转发,不接受本地写。

新增成员先 learner 后 flip;删除成员先 leaving,由 replacement learner 追平后再退出。node_link
owner 变化不能误判 node dead:旧 owner 在 grace 期继续转发 node 事件/命令,并在 ack 中提示 node
后续重连到新 owner。

## 4. 状态复制与恢复

`route_link` 和后续 `node_link` 采用无主 quorum CAS。协议以唯一 ballot 定序写入:
`(round, writer_id)` 按字典序比较,同一 key 的两个并发写不会撞同一个 version。写入分
prepare/accept 两阶段;读 quorum 时必须把读到的最高 ballot/version 回写到落后 owner,完成
read-repair。这样不需要 per-group primary,也不把 SWIM 活性判断引入数据路径。

复制协议必须满足:

- 写入必须经 owner set 的 W quorum 提交。
- 提交后继续 best-effort 复制到全 K/N/M owner。
- 每条记录有单调 rev 和唯一 ballot;写条件必须校验 expected rev。
- 旧成员表写入必须被 `membership_version` 拒绝。
- 成员冷重启后进入 joining,catch-up 完成前不参与写 quorum。

size-1 模式中 `LocateN` 只返回本成员,quorum 退化为本地内存写;同样使用 expected rev + ballot
接口,因此扩展到多成员时不改变上层状态机。size-1 用于开发和小规模部署,不提供 registry 成员
故障 HA。

## 5. Route 模型

### 5.1 身份

- 稳定会话身份:`(group, route_key)`。
- 当前运行实例:`sandbox_id`。它是不透明字符串,内部可包含保存/恢复代际,外部不解析。
- `sid` 如继续使用,应与 `sandbox_id` 等价或作为其别名。
- cluster 内部总是同时维护 `group`、`route_key`、`sandbox_id`。

### 5.2 Route 记录

| 字段 | 说明 |
|---|---|
| `group` | 分片键 |
| `route_key` | 会话键 |
| `state` | `reserved` / `placed` / `ready` / `paused` / `dead` |
| `node_id` | 当前承载节点 |
| `sandbox_id` | 当前实例 id |
| `version` | CAS 版本 |
| `updated_at` | reconcile / timeout 使用 |

### 5.3 状态机

```text
none -> reserved -> placed -> ready
ready -> paused -> reserved -> ready
ready/paused/placed -> dead/tombstone
```

整机清空、单沙箱 killed、node 重启后的缺失沙箱都收敛为 dead route 清理;下次 Reserve
重新放置。

`ReserveSandbox` 返回时必须已经 READY 或失败。若已有 `reserved/placed`,新请求 join 等待同一状态机。
客户端超时不等价于取消已提交的 placed;node 后续上报 running 时记录仍可进入 READY。

孤儿清理由 route owner 判定:node 上报的 `(group, route_key, sandbox_id)` 在 route_link 中不存在
或已被替换,则下发 delete/kill 到该 node。整机清空、单 sandbox killed、node dead 最终都走 dead
sandbox 清理 route 的统一路径。

## 6. Node-link

Node 不进 SWIM。Node 连接 registry 后按 `LocateN(node_id,N)` 归属 node owner set。node 上报:

- 全量 sandbox 清单和增量事件。
- build 状态事件。
- labels、runtime_digest、build_capacity、draining、粗粒度 liveness。
- 高频水位保留在 node_link owner 本地;node_list 是独立低频投影,普通 heartbeat 不触发
  WATCH_LIST 扇出。draining 变化和粗粒度 liveness 刷新才更新 node_list。

增量订阅的 rev 是字符串,格式由 node owner/node 私有约定,推荐编码为 `source_fingerprint:seq`。
node owner 在订阅时把上次 token 传给 node;node 校验 fingerprint,匹配时 replay `seq` 之后的
事件,不匹配或 changelog 不可用时强制全量 resync。node owner 还应以 1h-6h 随机打散周期做全量
resync,防止长期增量漂移。

## 7. Router

Router 是无状态北向入口,但持本地缓存:

- **route resolution cache**:key 为 `(group, route_key, sandbox_id, port, protocol)`。
- **active connection cache**:已有活动 HTTP/CONNECT 路由时,同一路由新请求不调用 Reserve。
- **singleflight reserve**:同 route 并发 miss 只发起一次 Reserve。

所有请求必须带 `X-Kuasar-Sandbox-Group`。当前不设计 sid 编 group 的无头入口。router 不订阅全量
全量 route 订阅;转发失败时 fail-fast 淘汰缓存,下次重新 Reserve。

## 8. Scaler 与 node_list

Scaler 消费 `node_list` 的 WATCH_LIST。WATCH_LIST 只包含低频字段:

- node_id
- labels
- runtime_digest
- build_capacity / capacity class
- draining
- liveness timestamp

`allocated`、`build_alloc`、counts、zone 等高频水位字段不进 WATCH_LIST;Place 时向 node owner
按需 GET 或使用短 TTL 缓存。

Scaler 通过 `SandboxGroupImporter.Range` 导入 group,通过 `SandboxGroupProvider` 取得 placement hint、
auth-key、manifest-key、registry_auth 等配置。shuffle-sharding 使用 `pkg/maglev.LocateN`,结果编码进
最终 placement selectors。

## 9. 密钥与鉴权

group 有两个密钥域:

| 名称 | 持有者 | 用途 |
|---|---|---|
| `auth_key` | router / node / provider | 验 API key,派生 access token |
| `manifest_key` | node / provider | 解密镜像/快照内容;router 不接触 |

`access_token = MAC(auth_key, sandbox_id)`。access token 不需要存储在 route 记录中。

`manifest_key` 和 `registry_auth` 都是 typed secret,支持 inline 或 ref 带外交付。密钥分发遵循
scaler/Tier-1 的 allocation 结果:scaler 决定哪些 node 应有 key,node owner 负责实际 `key_put/key_drop`
和 lease/ack 重试。密钥分发是 create/build 前置条件,不影响已运行 sandbox。

## 10. Build

Build 记录按 group 存在 route_link;执行态和实时预算归 node owner。

流程:

1. route owner 收到 `ReserveBuild(group, resources)`。
2. 调 scaler `PlaceBuild`。
3. 向 node owner 请求 `AdmitBuild(build_id, resources, ttl)`。
4. admission 成功后写 group build record。
5. 下发 `build_register`。
6. 失败路径 release admission;终态 build_event 释放预算。

`build_id` 查询必须带 group。若 node owner 发现预算不足,直接拒绝,route owner 重调度。

## 11. Provider

新 provider 模型:

```text
SandboxGroupProvider:
  Get(group)              -> sandbox config, image repo, registry_auth, metadata
  GetPlacementHint(group) -> raw selectors / shuffle labels
  GetKey(group)           -> typed manifest_key
  GetAuthKey(group)       -> auth_key or verification material

SandboxGroupImporter:
  Range(cursor, limit)    -> groups with ttl/generation
```

实现阶段可用现有 `groupcfg` 做 adapter,但新 cluster 内核只依赖该统一接口。

## 12. 可靠性

| 事件 | 行为 |
|---|---|
| router 崩溃 | 丢缓存;重启后 miss Reserve |
| scaler 崩溃 | 冷放置受影响;热连接不受影响 |
| node 崩溃/清空 | node owner / route owner 清理 route;下次 Reserve 重新放置 |
| registry 单成员故障 | owner set quorum 足够时继续服务;故障成员恢复后 catch-up |
| registry 双成员故障 | 对应 shard 少于 W 时停写 |
| membership 变更 | learner 预同步 + per-shard 短栅栏 + old grace |
| 整集群下电 | 不要求自动恢复沙箱 |
