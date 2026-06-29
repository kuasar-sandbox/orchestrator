# cluster — registry 自聚簇、会话路由与沙箱编排

`cluster-ctl` 是面向大规模部署的集群控制面,把数千个 `node-ctl serve` 节点聚合成一个
按 **sandbox-group** 分片的逻辑沙箱池。它对外提供 e2b 兼容控制面与数据面入口,对内维护
会话路由、放置、构建、密钥分发、节点故障收敛和 registry 成员变更。

cluster 的核心目标是:稳态数据面请求不经过 registry;冷路径通过 registry 自聚簇完成放置 / 恢复 /
构建预留;Registry 成员故障只影响其参与的小逻辑分片;成员变更由运维版本化触发,通过
joint owner set 与 namespace 自身收敛驱动完成业务无损切换。

## 1. 概述

### 1.1 设计原则

1. **sandbox-group 是唯一业务分片键**:路由、构建和 placement 均按 group 定位。每个北向请求必须
   携带 `X-Kuasar-Sandbox-Group`;当前不支持无 group 的 cluster 数据面入口。
2. **registry 自身是可靠状态集群**:执行态由 registry 成员直接复制,不依赖外部存储后端。Registry
   逻辑分片容忍单成员故障。整套 registry 完全下电后不要求自动恢复运行中 sandbox。
3. **registry 不实现 sandbox-group provider**:`SandboxGroupProvider` 和 `SandboxGroupImporter` 只属于
   scaler 或外部 provider。registry 不保存 group 配置、placement hint、auth_key 或 manifest_key 的
   provider store。
4. **节点是存活态真相之源**:sandbox/build 是否还在运行以 node 上报为准;registry 是路由权威和
   可重建聚合。node 整机重启直接清空,不重拉旧 sandbox。
5. **热路径旁路控制面**:router 对活动连接和近期路由做本地缓存;命中时直接转发到 node,不调用
   `Reserve`。miss / fail-fast 后才走 registry。
6. **成员健康不参与分片计算**:Registry 成员表由配置版本定义;`LocateN` 的输入仅为版本化成员表。
   HTTP replica health 只影响 retry/cooldown,不改变 owner set。
7. **放置与执行分层**:scaler 负责 group 导入、WATCH_LIST、shuffle-sharding、key allocation 与
   `Place`;node owner 负责节点连接、命令投递、运行态与资源 admission。

### 1.2 角色

| 角色 | 职责 |
|---|---|
| registry member | 组成 registry 自聚簇,承载 `route_link` / `node_link` / `node_list` 执行态和 membership |
| router | e2b 统一入口;按 group 定位 route owner;活动连接缓存;miss 时调用 Reserve |
| scaler | 消费 node_list WATCH_LIST;通过 provider/importer 导入 group;维护 placement / key allocation;提供 Place API |
| node | 运行 sandbox/build;经 node_link 上报全量清单与事件;接收 create/connect/delete/key/build 命令 |

## 2. 配置与监听

registry 默认只有一个控制面监听,所有协议按 path 区分:

```yaml
member:
  id: A
  listen: "0.0.0.0:7700"
  advertise: "https://A:7700"

membership:
  active: 1
  # next: 2       # joint 阶段: active + next owner set 同时提交
  # old_grace: 1  # cutover 后:旧成员只作为 peer/node_link 接入保留,不进入 owner set
  versions:
    - version: 1
      members:
        - { id: A, advertise: "https://A:7700" }
        - { id: B, advertise: "https://B:7700" }
        - { id: C, advertise: "https://C:7700" }
  owners:
    route_link: 3
    node_link: 3
    node_list: 3

node_link:
  heartbeat_interval: 10s
  node_dead_after: 30s
  revision_retention: 10000
  # listen: ""      # 空 = 复用 member.listen;非空 = 独立 node 长连接监听
  # advertise: ""

route_link:
  park_timeout: 30s

node_list:
  watch_retention: 10000

scale_link:
  scaler_replica_count: 3
  min_ready_scalers: 1
  place_timeout: 2s
```

默认 path:

```text
/node-link/*
/route-link/*
/scale-link/watch-node-list
/cluster/membership
/internal/registry-member/route-replica
/internal/registry-member/node-replica
/internal/registry-member/node-list-replica
/internal/node-owner
```

`member_link` 不作为单独配置项存在,它就是统一监听上的 `/internal/*` registry 成员 RPC。`node_link.listen`
只用于生产上隔离 node 长连接流量,不改变 owner 规则和协议语义。

## 3. Registry 命名空间

Registry 成员集由运维配置和 membership version 定义。每个命名空间用 `pkg/maglev.LocateN` 从版本化
成员表定位 owner set。

| 命名空间 | shard key | owner set | 内容 |
|---|---|---|---|
| `route_link` | group | `LocateN(group,K)` | route 记录、build 执行态;每个 owner 持完整 group 执行态视图 |
| `node_link` | node_id | `LocateN(node_id,N)` | node 连接、sandbox/build 清单、labels、水位、build 预算;每个 owner 持完整 node 视图 |
| `node_list` | `node_list` | `LocateN("node_list",M)` | 低频节点目录与 labels;每个 node_list owner 持完整目录和 WATCH_LIST log |
| `scale_link` | group | `LocateN(group,ready_scalers,R)` | scaler 动态成员域和 Place 故障转移候选 |

`node_link` 中只有当前连接 owner 持有实际 node h2 stream;其余 owner 通过复制持有完整视图。
node 记录携带 `link_owner`,route owner 需要下发 `create/key/build/delete` 时,通过 node-owner RPC 转发到
持有该 h2 stream 的 registry 成员。`route_link` 的所有 owner 均可响应查询。写入达到要求 quorum 后提交,
随后继续复制到全 owner set,用于完整视图和本地 waiter 唤醒。
若某个 route owner 本地 group 视图尚未 ready,它只能按该 group 的 owner set 做 group-scoped catch-up,
从分片内副本拉取同 group 记录并 read-repair 到本地;不能跨 group 扫描或把多个无关分片结果合并。

## 4. Membership 与成员健康

### 4.1 成员表

registry 成员表只来自运维分发的配置文件。reload 通过信号或 API 触发。每个版本有稳定 label:

```text
registry.<version>.<sha256(sort(member_ids))>
```

registry 可以同时持有 `active`、`next` 和 `old_grace`:

- `active`:客户端定位 route/node/node_list owner 的版本。
- `next`:joint 阶段的目标版本;写入必须同时满足 active quorum 与 next quorum。
- `old_grace`:cutover 后保留的旧版本;只用于 registry 成员互访、node-owner RPC 和仍接在旧成员上的
  node_link,不参与 owner set,也不接受旧 owner 本地提交。

router、scaler 通过 `GET /cluster/membership` 获取成员表。刷新时会从 bootstrap 和已知成员中选择 active
version 最新的 membership,避免单个 bootstrap 滞后影响切换。

### 4.2 成员健康

registry 成员之间的 route/node replica RPC 使用 HTTP 控制面。客户端维护每个 peer 的短周期 health/cooldown:
RPC 失败后该 peer 暂时 fail-fast,冷路径 fanout 仍按完整 owner set 并发尝试。成员健康必须遵守以下边界:

- 不维护 registry 成员清单。
- 不改变 `LocateN` 输入。
- 不作为数据复制通道。
- 不把 suspect/dead 事件转化为 reshard。

scaler ready 通过 register loop 表达。scaler 向每个 active / next registry 成员推送:

```json
{"role":"scaler","id":"s1","advertise":"https://s1:7800","ready_label":"registry.2.hash"}
```

### 4.3 成员故障

成员故障由 replica RPC 失败和 health/cooldown 体现,但不改变 owner set。某成员不可达时:

- 它参与的 owner set 降一格,只要可达 owner 数满足 quorum 即继续服务。
- 若 quorum 不足,该 shard 停写(CP),不降级乱写。
- 恢复成员不扫描全局 key 空间;后续同 shard key 的读写、node-link 上报或 group-scoped 操作触达该 key 时,
  quorum read-repair 补齐本地副本。

## 5. Joint Owner Set

跨版本时,对同一个 shard key,逻辑 owner set 是 old/new 并集:

```text
oldOwners   = LocateN(shard_key,V1)
newOwners   = LocateN(shard_key,V2)
jointOwners = oldOwners ∪ newOwners
```

提交条件不是并集 majority,而是 joint quorum:

```text
prepare/read:  quorum(oldOwners) + quorum(newOwners)
accept/write:  quorum(oldOwners) + quorum(newOwners)
repair:        best-effort 写满 jointOwners
```

重叠成员可同时计入 old quorum 和 new quorum。普通并集 majority 不安全,因为它可能读不到旧版本已提交
但只落在旧 quorum 上的值。

状态机:

```text
stable(v1)
  -> load_config(v2)
  -> joint_node_link(v1,v2)
  -> node_list_refresh(v2)
  -> wait_scaler_ready(v2)
  -> joint_route_link(v1,v2)
  -> cutover_barrier
  -> stable(v2, old_grace=v1)
  -> retire(v1)
```

reload 只允许两类 membership active 变化:同 active 下加载/取消同一个 `next`,或从已配置的 `next`
切到新的 `active`。不能从 stable(v1) 直接跳到 stable(v2)。cutover 配置可以带 `old_grace=v1`,让旧成员
继续作为 node_link 接入和 node-owner RPC 目标,但 owner views 只包含 v2。

## 6. 状态复制

`route_link`、`node_link` 和 `node_list` 使用无主 quorum CAS。协议以唯一 ballot 定序写入:
`(round, writer_id)` 按字典序比较,同一 key 的两个并发写不会撞同一个 version。写入分
prepare/accept 两阶段;读 quorum 时必须把读到的最高 ballot/version 回写到落后 owner,完成
read-repair。

复制协议必须满足:

- stable 模式写入经当前 owner set quorum 提交。
- joint 模式写入经 old quorum + new quorum 提交。
- 提交后继续 best-effort 复制到同 shard key 的 joint owner set。
- 每条记录有单调 rev 和唯一 ballot;写条件必须校验 expected rev。
- cutover 后旧成员若仍在 `old_grace`,必须暴露新 active membership;旧版本不进入 owner view,旧 membership
  写入不能在旧 owner 本地提交。
- 成员冷重启后本地副本为空;不通过跨片扫描发现 key。只有带 shard key 的访问、node-link 上报或
  group-scoped 操作触达该 key 时才执行 quorum read-repair。

size-1 模式中 `LocateN` 只返回本成员,quorum 退化为本地内存写;同样使用 expected rev + ballot
接口。size-1 用于开发和小规模部署,不提供 registry 成员故障 HA。

## 7. Namespace 收敛

registry 不支持跨 group/node shard 扫描,也不通过枚举 key 做后台 promoter。收敛由 namespace 自己的事实源
驱动:route/build 由同 group 请求、node 上报或 group-scoped list/export 触达;group import 驱动 group/key
allocation;node_link 由 node 连接、心跳和事件触达;node_list 由 node-link 持有者的低频投影触达。

### 7.1 node_link

活动 node 连接持有者在进入 joint/cutover 后:

1. register 和后续低频 liveness refresh 把 node_list 投影写入当前 owner set。
2. heartbeat、sandbox/build event、清单变化继续写入当前 node_link owner set。
3. 记录中的 `link_owner` 保留实际 h2 stream 持有者;新 route/node owner 通过 node-owner RPC 向该成员
   下发 create/connect/delete/key/build 命令。
4. 旧接入成员在 `old_grace` 期间不进入 owner set,但仍可作为 `link_owner` 接收转发命令。

已断开 node 不阻塞切换。它重连时按当前 membership 做全量 resync;不重连则按 dead/reconcile 收敛。

### 7.2 route_link / group

scaler 的 group provider/importer 是 group/key allocation 的事实源。导入是 upsert + TTL/tombstone 语义,
缺失 group 不构成删除;过期或 tombstone 才会停止参与 Place 和 key allocation。scaler 不复制 route 数据。
route/build 执行态不做跨 group promoter;后续同 group 请求、node 上报和 group-scoped list/export 会通过
quorum read-repair 补齐该 group 的 owner 副本。

provider 删除 group 时不能直接从 import 中消失;必须以 tombstone/draining group 形式继续出现,直到 registry
确认该 group 没有活动 route/build 执行态。

route owner 内部维护按 group 分开的本地 watch log。这个 watch 只服务 registry 内部:

- node owner 写入 READY/PAUSED/DEAD 后,group owner 的本地副本 accept/repair 触发 group event。
- `ReserveSandbox` park 期间同时等待本进程 singleflight、该 group event,并用短周期 quorum read 兜底。
- router 不订阅 route watch;router 只依赖 Reserve 返回 READY 或失败。
- group list/export 在返回前必须确认本地 group 视图 ready;未 ready 时先执行 group-scoped catch-up,
  quorum 不足则返回 503。

### 7.3 node_list

node_list 是固定 namespace 的逻辑分片:`LocateN("node_list",M)` 得到 owner set,分片内每个 owner 持完整目录。
node-link 连接持有者在 register、draining 变化和低频 liveness refresh 时,把 node_link 的低频投影以
无主 CAS 写入当前 node_list owner set。owner 本地副本 accept/repair 会触发 WATCH_LIST 事件;本地视图未
ready 时,只能从同一 node_list owner set 做 list + repair,不能跨 node shard 扫描,也不从 node_link
重建第二条事实传播路径。watch token 编入 membership label;label 变化时 scaler 重新订阅 node_list owner
set 并 reset + full snapshot。

## 8. Route 模型

### 8.1 身份

- 稳定会话身份:`(group, route_key)`。
- 当前运行实例:`sandbox_id`。它是不透明字符串,内部可包含保存/恢复代际,外部不解析。
- `sid` 如继续使用,应与 `sandbox_id` 等价或作为其别名。
- cluster 内部总是同时维护 `group`、`route_key`、`sandbox_id`。

### 8.2 Route 记录

| 字段 | 说明 |
|---|---|
| `group` | 分片键 |
| `route_key` | 会话键 |
| `state` | `reserved` / `ready` / `paused` / `dead` |
| `node_id` | 当前承载节点 |
| `sandbox_id` | 当前实例 id |
| `access_token` | 当前实例的数据面 token,由 scaler 按 `MAC(auth_key,sandbox_id)` 生成 |
| `version` | CAS 版本 |
| `updated_at` | reconcile / timeout 使用 |

### 8.3 状态机

```text
none -> reserved -> ready
ready -> paused -> reserved -> ready
ready/paused/reserved -> dead/tombstone
```

整机清空、单沙箱 killed、node 重启后的缺失沙箱都收敛为 dead route 清理;下次 Reserve 重新放置。

`ReserveSandbox` 返回时必须已经 READY 或失败。若已有 `reserved`,新请求 join 等待同一状态机。
create/connect 命令被 node owner ack 接受后,route owner 等待 node 上报 READY;等待超时会把仍停在
`reserved` 的记录回滚或恢复为原 PAUSED 记录。若 node 的 READY 上报先到达,READY 写入胜出。

孤儿清理由 route owner 判定:node 上报的 `(group, route_key, sandbox_id)` 在 route_link 中不存在或已被替换,
则下发 delete/kill 到该 node。这个过程不经过数据面,也不依赖 access token。

## 9. Node-link

Node 连接 registry 后按 `LocateN(node_id,N)` 归属 node owner set。node 上报:

- 全量 sandbox 清单和增量事件。
- build 状态事件。
- labels、runtime_digest、build_capacity、draining、粗粒度 liveness。
- `manifest_keys` desired cache,由 key allocation 写入 node_link owner set;实际下发由 node-link 心跳维系刷新。
- 高频水位保留在 node_link owner 本地;node_list 是独立低频投影,普通 heartbeat 不触发 WATCH_LIST 扇出。

增量订阅的 rev 是字符串,格式由 node owner/node 私有约定,推荐编码为 `source_fingerprint:seq`。
node owner 在订阅时把上次 token 传给 node;node 校验 fingerprint,匹配时 replay `seq` 之后的事件,不匹配
或 changelog 不可用时强制全量 resync。node owner 还应以 1h-6h 随机打散周期做全量 resync。

node-link bookmark 标记本轮同步结束。若 bookmark 来自全量 Range,registry 会把本 node 记录中本轮未出现
的 sandbox refs 按精确 `(group,route_key,sandbox_id)` 校验后删除对应 route;若 bookmark 来自增量 replay,
只更新 resume token,不做缺失清理。build 终态仍由 build_event 收敛,不从 sandbox 全量清单推断。

## 10. Router

Router 是无状态北向入口,但持本地缓存:

- **route resolution cache**:key 为 `(group, route_key, sandbox_id, port, protocol)`。
- **active connection cache**:已有活动 HTTP/CONNECT 路由时,同一路由新请求不调用 Reserve。
- **singleflight reserve**:同 route 并发 miss 只发起一次 Reserve。

所有请求必须带 `X-Kuasar-Sandbox-Group`。router 通过 bootstrap 获取 registry membership,并在刷新时尝试
bootstrap 与已知 active/next/old_grace 成员,选择 active version 最新的结果;按 active group owner 定位
route owner。router 不订阅全量 route 或 node_list;转发失败时 fail-fast 淘汰缓存,下次重新 Reserve。

router 调 route owner 的 `verify-key`;registry 只按 group failover 到 ready scaler 校验,不读取 auth_key。
router 与 registry 都不接触 manifest_key。

## 11. Scaler 与 Place

scaler 是 placement 和 group provider/importer 的消费者。registry 不实现 provider。

scaler 负责:

- `SandboxGroupImporter.Range` 全量/增量导入 group。
- `SandboxGroupProvider.GetPlacementHint` 构建 group placement cache。
- `SandboxGroupProvider.GetKey` / `GetAuthKey` 生成 key allocation / auth material intent。
- 按 active / next membership 得到 node_list owner 候选,一次只订阅一个 owner;断线后 reset 并切换下一个。
- 周期性向每个 active / next registry owner 成员 `POST /scale-link/register` 发布 ready 状态。
- 向每个 active / next registry owner 成员推送 selector/key allocation patch。
- 对 registry 暴露 `POST /scale-link/place`。

registry 对 group 做 scaler 选择:

```text
readyScalers = alive scalers where ready_label == active_registry_label
candidates   = LocateN(group, readyScalers, scaler_replica_count)
try candidates in order until success
```

registry 只做故障转移,不做 P2C。P2C/负载选择属于 scaler 内部对 node 的 placement 策略。最终 node owner
admission 仍是资源确认点。

`PlaceSandbox` 返回:

```text
node_id
create_spec / sandbox_config
key intent / secret refs
access_token = MAC(auth_key, sandbox_id)
runtime/template hints
```

registry 根据返回值做 node owner admission、node_link key cache 更新、create/connect。registry 不解析 provider。

## 12. 密钥与鉴权

group 有两个密钥域:

| 名称 | 持有者 | 用途 |
|---|---|---|
| `auth_key` | router / node / provider | 验 API key,派生 access token |
| `manifest_key` | node / provider | 解密镜像/快照内容;router 不接触 |

`access_token = MAC(auth_key, sandbox_id)`。scaler 在 Place 时生成当前 sandbox_id 对应 token,registry 将其
随 route 记录保存,READY/ResolveSID 只读 route_link,不回查 provider。

`manifest_key` 和 `registry_auth` 都是 typed secret,支持 inline 或 ref 带外交付。`manifest_key` 使用 ref
时必须同时给出 fingerprint,供 create/build precheck 使用。密钥分发遵循 scaler 的 allocation 结果:
scaler 决定哪些 node 应有 key,registry/node owner 将 desired key list 写入对应 node_link 记录并在 owner
set 内 CAS 复制。实际 `key_put` 是 node-link 心跳维系的定期刷新;`key_drop` 不作为正确性依赖,节点侧租约
按 TTL 淘汰未续租 key。密钥分发是 create/build 前置条件,不影响已运行 sandbox。

## 13. Build

Build 记录按 group 存在 route_link;执行态和实时预算归 node owner。

流程:

1. route owner 收到 `ReserveBuild(group, resources)`。
2. 调 scaler `PlaceBuild`。
3. 向 node owner 请求 `AdmitBuild(build_id, resources, ttl)`。
4. admission 成功后写 group build record。
5. 下发 `build_register`。
6. 失败路径 release admission;终态 build_event 释放预算。

`build_id` 查询必须带 group。若 node owner 发现预算不足,直接拒绝,route owner 重调度。

## 14. 导入导出

registry export/import 只覆盖 registry 执行态灾备数据:

- route records
- build execution records

registry export/import 不覆盖 sandbox-group provider 数据。group 配置、placement hint、auth_key、manifest_key
由 scaler/provider 侧导入导出。

## 15. 可靠性

| 事件 | 行为 |
|---|---|
| router 崩溃 | 丢缓存;重启后 miss Reserve |
| scaler 崩溃 | 冷放置受影响;热连接不受影响;registry failover 到同 group 的下一个 ready scaler |
| node 崩溃/清空 | node owner / route owner 清理 route;下次 Reserve 重新放置 |
| registry 单成员故障 | owner set quorum 足够时继续服务;故障成员由后续同 key 访问或事实源上报触发 read-repair |
| registry 双成员故障 | 对应 shard 少于 quorum 时停写 |
| membership 变更 | joint owner set + namespace 收敛 + old grace |
| 整集群下电 | 不要求自动恢复运行中 sandbox |

## 16. 集群 stub e2e

`node-stub-ctl` 是集群功能的本仓 e2e 节点桩。它使用真实 `node_link` 协议接入 registry,一个进程可
模拟多个节点,但不启动 microVM。除 microVM/应用进程外,它完整模拟节点控制面行为:

- 注册 node、心跳、drain、水位和 build 预算。
- 接收 `key_put/key_drop/create/connect/delete/build_register` 命令并返回 ack。
- 按 sandbox 行为配置延迟发布 READY/dead route event。
- 发布 build event。
- 提供 admin API / 子命令查询节点、沙箱、build、key、command 和 data hit。
- 支持 `restart-link`、`reboot-empty`、`crash/start` 等节点动作。

本仓 `make test-e2e` 先 `make build`,再用产物真实启动 `cluster-ctl registry/router/scaler` 与
`node-stub-ctl`,覆盖 group 导入、key 分发、Reserve、数据面转发、活动路由缓存、BuildRegister、
孤儿 route 清理和节点清空收敛。`test/e2e/e2e_cluster_stub.sh` 默认跑 `registry-n1`、`registry-n3`
和 `registry-joint` 三个 case;joint case 验证 next-only 成员复制可见、active v2 + old_grace v1 cutover、
consumer membership refresh、cutover 后新 Reserve/BuildRegister 以及旧 node_link owner 转发。
