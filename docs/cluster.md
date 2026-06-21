# cluster — 集群级注册表、控制面与沙箱编排

`cluster-ctl` 是面向大规模部署的集群控制面,把一个机群里数千个 `node-ctl serve` 节点(node.md)
聚合成一个逻辑沙箱池:对外提供 **e2b 兼容控制面 + 数据面**(均限定在 **sandbox-group** 范围内),
按业务身份(sandbox-group + **route-key**)在机群范围把请求路由 / 拉起到正确的沙箱。一身兼三角色:
**registry**(持久化状态权威 + 通道枢纽 + 异步操作 / watch 接口,本文)、**router**(e2b 兼容**统一
入口**:控制面 + 数据面,cluster-router.md)、**scaler**(放置**调度器**:给出放置建议,
cluster-scaler.md)。

**最小化启动延迟是第一约束**(平台以亚秒级冷启动 / 快照恢复为目标):热路径(已建立会话)
经 router 本地路由缓存直转,**零控制面往返**;冷 / 恢复路径以**远程快照 + 快速恢复**(平台原生
能力)拉起,经 shuffle-sharding 的缓存局部性加速——cluster 自身只贡献可忽略的编排开销(§9)。

cluster **以 sandbox-group 为唯一分区单位**组织一切(沙箱 / 构建 / 配置),操作严格限定单个 group、
**无跨 group 操作**,由此支持规模化分片。控制面**校验后转发节点**执行(节点以既有 e2b 生命周期
原语运行,node.md §4 / §8),cluster 不新增沙箱机制、只做编排与路由。

产物一个二进制 **`cluster-ctl`**,三角色 registry / router / scaler:当前 registry(含进程内放置器)与
router 为运行守护进程,router 经 UDS / mTLS 连 registry;大规模可把放置(scaler)拆为独立进程、各自横向
扩展(§4 / §12)。同机共置走 UDS 低延迟。

## 1. 概述

### 1.1 业务问题

平台目标是把大量 Agent / Serverless 工作负载承载在机群上。单机 node(node.md)解决不了:

- **会话亲和**:同一用户 / 会话的连续请求应落到**同一个**热沙箱,而它可能在机群任意节点——需要
  机群范围的 (业务身份 → 沙箱 → 节点) 映射,且查得快(热路径零控制面往返,§9)。
- **缩到零 + 快启**:**沙箱 / sandbox-group 数量远超节点数**,不可能在节点上常驻预热实例;空闲沙箱
  下沉为节点本机快照(PAUSED)乃至远程快照(SAVED,可移植、释放节点),按需以**快照恢复**拉起
  (亚秒级,§9)。"预热池"是**虚拟的**:即去重的远程快照存储 + 缓存,按需恢复。
- **放置、爆炸半径与密度**:新沙箱落在哪个节点取决于标签(zone/pool/slot)、实时水位与**爆炸半径**
  (一个 group 只落在受限的节点子集);租户密钥须先到达目标节点。
- **统一北向面**:e2b SDK / CLI 与业务流量指向集群一个入口,而非逐节点寻址。

### 1.2 设计原则

1. **启动延迟第一**:热路径 router 本地缓存直转;冷 / 恢复路径靠远程快照 + 快速恢复 + 缓存局部性;
   控制面编排开销可忽略(持久长连、UDS 共置、密钥预置,§9)。
2. **sandbox-group 是唯一分区键、操作不跨 group**:沙箱 / 构建 / 配置按 group 组织;每个操作点名
   一个 group(`X-Kuasar-Sandbox-Group`)。节点注册表是**唯一的全局(不分片)表**(§6)。
3. **控制面 + 数据面,均经节点执行**:cluster 提供完整 e2b 控制面(§8)与数据面转发,但校验后
   **转发节点**——节点以既有 e2b 原语执行(node.md §8 / §8.1)。
4. **持久可靠、无纯内存模式**:registry 任何后端都持久(sqlite / etcd / raft,§10),进程重启可从
   存储 + 节点重连恢复业务态。
5. **节点事件为真相之源、最终一致、Reserve 总等 READY**:沙箱 / 构建状态由节点经统一通道上报,
   registry 据此收敛;`Reserve` 永远等待节点**自动上报的 running 事件**才返回(§7)。
6. **调度与状态分层(利于规模化)**:scaler 是**调度器**,给出放置**建议**;registry **提交**预留
   (原子 CAS)。scaler 不联系节点、不持密钥、不碰生命周期(§4 / cluster-scaler.md)。

### 1.3 边界与依赖

- **下游是 node-ctl serve**:经统一通道(§5)注册 / 心跳 / 事件 / 命令 / 密钥;控制面操作经 router
  转发到节点 e2b 控制面(node.md §4),create/connect 经 Reserve(走节点命令)。不直连 guest。
- **数据面字节不经 registry**:router → node 数据端点 → guest,两跳;通道只承载控制,无业务字节。
- **数据面两种寻址并存**:create 后**按 sid**(e2b 原生 SDK 流)、或业务流量**按 (group, route-key) 头**
  (无 create 直触发 Reserve)(cluster-router.md §4)。
- **实现于 sandbox-orchestrator `internal/*`**(与 node-ctl 共仓,无新导出包);纯 Go、`CGO_ENABLED=0`;
  无 gRPC / protobuf——统一通道是帧化 JSON over h2c,沿用 routesync 线格式(node-proxy.md §6)。
  shuffle-sharding 复用 sandbox-accelerator 导出的 `pkg/maglev`;快照恢复 / 去重靠 accelerator 远程
  store + 缓存(cluster-scaler.md §4 / §9)。

### 1.4 架构与数据通路

```
   e2b SDK / CLI (api.<domain> 控制面)            业务流量 (数据面)
   X-API-KEY + X-Kuasar-Sandbox-Group            X-Kuasar-Sandbox-Group + X-Kuasar-Route-Key
            │                                     (或 create 后 by sid)
            ▼                                          ▼
   ┌──────── cluster-ctl router (N replicas, 无状态, 统一 e2b 入口) ──────────┐
   │  本地路由缓存(热路径直转, 零控制往返, §9)                              │
   │  控制面: 校验(api_key↔group) → create/connect=ReserveSandbox ·          │
   │          pause/kill/timeout=转发节点 · list/get=group 分片 · build=…     │
   │  数据面: 缓存命中→直转;未命中→registry.ReserveSandbox → 注入 token 转发  │
   └──────────────┬───────────────────────────────────────┬─────────────────┘
        异步 op / watch (UDS 本机 | mTLS)                   │ data-plane forward
                  ▼                                         │ (router→node→guest)
   ┌──────── cluster-ctl registry (持久状态权威 + 通道枢纽) ──┐  │
   │  NodeStore(全局)· GroupConfigProvider(细粒度可外置)   │  │
   │  SandboxStore / BuildStore(按 group 分片)             │  │
   │  Reserve 提交(CAS)· 控制面 · 密钥分发 · §5 通道枢纽    │  │
   └────────┬──────────────────────────────┬───────────────┘  │
   PlaceSandbox/PlaceBuild 建议(同步)       │ 统一通道(per node, mTLS):
   ┌── scaler(调度器: 选节点建议)──┐         │  ▲ 节点上报: routes(sandbox)·build·heartbeat
   │  订阅 node/group 态;maglev      │         │  ▼ registry 下发: create·connect·delete·key_put·key_drop
   │  shuffle 选择器 patch          │         │
   └────────────────────────────────┘         ▼
   ┌──────── node-ctl serve (per node, node.md) ─────────────────────────┐
   │  e2b 控制面(转发目标)· data-plane proxy → guest envd / floatingip    │◄── router forward
   │  e2b lifecycle: create/restore/snapshot/migrate(快照恢复, §9)        │
   │  通道客户端(node.md §10): dials registry, 反向注册为路由权威          │
   └──────────────┬──────────────────────────────────────────────────────┘
                  ▼  sandbox-ctl run / --restore / snapshot
            cloud-hypervisor microVM (guest: sandbox-init + envd)
```

## 2. 命令行接口

| 子命令 | 用途 |
|---|---|
| `registry` | 持久状态权威 + 通道枢纽 + 异步 op / watch 接口(§4 / §5) |
| `router` | e2b 兼容统一入口:控制面 + 数据面(cluster-router.md) |
| `scaler` | 放置调度器:给出 PlaceSandbox 建议(沙箱与 build 均经此;cluster-scaler.md) |
| `group` | sandbox-group 配置管理(`upsert`/`get`/`list`/`remove`,§6.2);registry admin 瘦客户端 |
| `config` | 配置规范化 / 校验,或输出带注释骨架 |
| `version` | 版本 |

```
cluster-ctl registry [--config …] [--store sqlite:/var/lib/cluster/registry.db]
                     [--channel-listen :7700] [--op-listen /run/cluster/registry.sock]
cluster-ctl router   [--config …] [--registry <addr>] [--listen :443] [--tls-cert/--tls-key]
cluster-ctl scaler   [--config …] [--registry <addr>]
```

registry(持久)与 router 为运行守护进程,router 经 `--registry`(UDS 本机 / mTLS 远端)连 registry
的 op-listen(§4.2)。**放置(scaler 角色)当前在 registry 进程内**——registry 直接持有放置器并调用
(§7.5);`cluster-ctl scaler` 仅打印其放置配置,独立 scaler 进程是大规模拆分形态(§4.2 右列)。

## 3. 配置

权威结构 `internal/clustercfg/config.go`。按角色分组:`store`、`group_config`、`channel`、`reserve`、
`router`(cluster-router.md §3)、`scaler`(cluster-scaler.md §3)。**必填仅 `domain` 与 `store`**。

| 字段 | 默认 | 说明 |
|---|---|---|
| `domain` | (必填) | 服务域;router 据此分流控制面 / 数据面 |
| `store.kind` | `sqlite` | 注册表后端:`sqlite`(单机持久,最简)/ `etcd` / `raft`(§10);**无 `memory`** |
| `store.dsn` | `/var/lib/cluster/registry.db` | sqlite 路径 / etcd 端点 / raft 配置 |
| `group_config.providers` | `store` | 细粒度 provider 选择(§6.2):每个接口(key / sandbox-config / placement)可 `store` 或 `external:<addr>` |
| `group_config.encryption_key` | 空 | `store` 自存 manifest_key 时的 AES-256 落盘密钥(同 node.md §3,可 env 覆盖);全 `external` 时不需 |
| `channel.listen` | `:7700` | 节点拨入的统一通道监听(§5);registry 持有 |
| `channel.tls` | 空 | 通道 mTLS 证书 / CA(生产必配,§5.4) |
| `channel.heartbeat_interval` | `10s` | 下发心跳周期 |
| `channel.node_dead_after` | `30s` | 连续未收心跳判失联(§11) |
| `channel.revision_retention` | `10000` | 每分片保留的变更日志条数,供断线增量重放(§5.3);超出则客户端全量重同步 |
| `op.listen` | `/run/cluster/registry.sock` | 异步 op / watch 监听(UDS 本机;跨机拆分的 op-mTLS 见 §10)|
| `reserve.park_timeout` | `30s` | `Reserve` 等待 READY 的预算上限(§7);超时回错由 router 转 503 |

## 4. 架构与角色

### 4.1 三角色职责

- **registry**——**持久**状态权威 + 通道枢纽。持有每节点一条统一通道长连(§5),维护四张注册表
  (§6),提供 `Reserve`(§7)与控制面后端逻辑(§8),**提交**放置预留(CAS),**主管密钥分发**
  (§7.6),对 router / scaler 提供**异步操作 + watch** 接口(§4.3)。registry **内部**,不对客户端
  直接开 e2b 口。
- **router**——无状态 **e2b 兼容统一入口**(控制面 + 数据面)。持**本地路由缓存**(订阅 registry
  路由流):热路径命中直转(零控制往返,§9);未命中 / 控制操作 → registry。N 副本置 LB 后。
- **scaler**——放置**调度器**。当前在 registry 进程内,直接读其节点 / group 注册表,被 registry 调用
  `PlaceSandbox`(沙箱与 build 均经此,§7.5)时**给出节点建议**(本地计算)。**不联系节点、不持密钥、不碰
  沙箱生命周期、无 drain**;维护 shuffle-sharding 选择器(cluster-scaler.md §4)。大规模可拆为订阅 registry
  视图的独立进程。

### 4.2 部署形态

registry 与 router 为运行守护进程;放置(scaler 角色)当前在 registry 进程内计算,大规模可拆为独立
scaler 进程(下表右列)。

| | 小规模(同机共置) | 大规模(拆分) |
|---|---|---|
| 进程 | registry(sqlite)+ router + scaler,同机,**op 接口走 UDS** | registry 多副本(按 group 分片,etcd/raft)+ router N 副本(LB)+ scaler 多副本(按 group 分片) |
| 启动延迟 | 冷路径控制往返 = UDS(亚毫秒) | 冷路径控制往返 = 同 AZ mTLS;热路径仍 router 本地缓存(§9) |
| 可靠性 | sqlite 持久,registry 重启对账(§11) | KV / raft 复制 |

### 4.3 异步操作 + watch 接口

registry 对 router / scaler 提供**异步风格**接口(`op.listen`,帧化 JSON,与 §5 通道同族):

- **watch 订阅**:router 订阅路由流(喂本地缓存),scaler 订阅节点 / group 状态;增量 + 可断线
  恢复(§5.3)。
- **操作**:router 发 `ReserveSandbox` / `ReserveBuild` / 控制转发,经结果事件得到结果(§7);
  registry 调用放置器 `PlaceSandbox`(沙箱与 build 均经此,§7.5;当前进程内,大规模可拆同步请求 / 响应)。
- **调度器 / 提交分层**:scaler **建议**节点(P2C over 本地视图),registry **原子提交**预留
  (CAS;并发冲突 / 视图滞后则重问一次)——Kubernetes scheduler / apiserver 式分层,利于
  placement 层独立扩展(§1.2 原则 6)。

## 5. 统一通道协议

**一套帧化 JSON over h2c 的 pub/sub + 上行命令协议**承载所有控制连接,角色化复用 routesync 线格式
(node-proxy.md §6)——零 gRPC / protobuf。统一对象:

| 连接 | 拨号方 | 权威(下行流) | 订阅 + 上行 |
|---|---|---|---|
| proxy worker ↔ node | worker | node | worker(`route`/`route_wake`)——既有 |
| **node ↔ registry**(§5.1) | **node** | **node**(其 routes + `build` 事件) | **registry**(`kind=registry`):上行下发 `create`/`connect`/`delete`/`key_put`/`key_drop` |
| router ↔ registry | router | registry(集群路由 + op 结果) | router:上行发 `ReserveSandbox` 等 op(§4.3) |
| scaler ↔ registry | scaler | registry(节点 / group 状态) | scaler:registry 反向请求 `Place*`(§7.5) |
| 外部观察者 ↔ registry | watcher | registry | watcher(`kind=watch`) |

- **帧**:`[4 字节 LE 长度][JSON]`,单帧 ≤ 1 MiB,每帧一条;每条带**分片单调版本号 `rev`**(§5.3)。
- **传输**:生产 mTLS;持挂连接即在线租约,断连即失联。

### 5.1 node ↔ registry(反向注册)

节点接入复用其既有 routesync **权威**侧(node-proxy.md §6),只是连接由节点拨出:

1. node 拨 registry 的 `channel.listen`,首帧发 `register{node_id, labels, capacity, build_capacity,
   data_endpoint, runtime_digest}`,registry 回 ack。
2. node **反向监听**;registry 在该连接发 `register{subscribe:{kind:registry}}`——即 registry 作为
   node 的一个路由订阅者。
3. 此后 node 作**路由 / 构建权威**下行流式 `upsert/delete`(`sandbox`/`build` 路由)+ `bookmark`;
   registry 作**订阅者 + 命令方**上行发命令。

故节点侧**无独立 node-link 实现**——一套 routesync 引擎同时服务本机 proxy worker 与远端 registry
(node.md §10)。

**节点上报**(下行,node→registry):`sandbox{sid, group, route_key, state(running|paused|dead),
snap_loc, access_token, migration_token?, template_id}`、`build{build_id, group, state, template_id?,
reason?}`、`delete{sid|build_id}`、`bookmark`;`heartbeat{zone, allocated, pool, build_alloc, counts, draining}`(`draining` 由节点侧 drain 置位,
node-resource.md §2.5,放置据此排除该节点)。

**registry 下发**(上行命令,registry→node):

| 命令 | 用途 |
|---|---|
| `create{cmd_id, sid, group, route_key, template_ref, key_fp, config, migration_token?}` | 拉起沙箱:冷启 `template_ref`(快照模板=快速恢复,§9)或迁移(migration_token 一步导入 + restore)|
| `connect{cmd_id, sid}` | 恢复本机 PAUSED 沙箱(node.md §8) |
| `delete{cmd_id, sid|build_id}` | 销毁(kill,或 SAVED 两阶段回收步,§7.4) |
| `key_put`/`key_drop{fingerprint, manifest_key?, expires_unix}` | 密钥租约分发 / 续租(续租=重发 `key_put`)/ 撤销(registry owns,§7.6) |

命令携 `cmd_id`,node 立即回 `cmd_ack{cmd_id, status, reason?}`(accepted/rejected);**终态经上述
事件上报**,`Reserve` 等该事件(§7)。命令以 sid / build_id 幂等。

### 5.2 router / scaler ↔ registry

同族帧协议(`op.listen`):router / scaler 拨 registry,订阅事件流(下行)+ 发操作(上行);registry
对 scaler 反向发 `Place*` 请求(§7.5)。UDS 本机 / mTLS 远端(§4.2)。

### 5.3 断线快速重连(分片版本号增量重放)

每分片维护**单调递增 `rev`**;每条下行事件带其 `rev`。订阅者记最后应用的 `rev`,**重连时带入**
`resume_from`:

- registry 若仍有自该 `rev` 起的变更日志(`channel.revision_retention` 窗口内)→ 只重放增量 +
  新 `bookmark`;
- 否则(`rev` 过旧 / 已压实)→ 退回逐条全量 + `bookmark` 世代清扫(node-proxy.md §6)。

**opt-in**:不支持 `resume_from` 的订阅者照常全量重同步。此机制对三类订阅(节点 / 本机 plugin /
外部 watch)一致,避免高密度下重连风暴(§11 / §12)。

### 5.4 安全

mTLS;节点证书 SAN / 指纹背书 node_id。`access_token` / `migration_token` 上行、`manifest_key`
下行皆在 mTLS 内;registry 若自存(provider=store)按 secretbox 加密(node.md §7),external provider
则 manifest_key 仅过路。同 node_id 重连顶替旧连。

## 6. 注册表模型

四张表,**sandbox-group 是唯一分区键**;NodeStore 全局,Sandbox/Build 按 group 分片;group 配置经
细粒度 provider。

### 6.1 四张表

| 表 | 分区 | 内容 | 来源 |
|---|---|---|---|
| **node** | **全局(不分片)** | labels、liveness、watermark(zone/allocated/pool)、**build_capacity{cpu,mem,storage}** + build_alloc、capacity、data_endpoint、runtime_digest、`draining` 标记 | 通道 `register`/`heartbeat`(§5);失联判定 §11 |
| **sandbox-group config** | (经 provider,§6.2) | tenant(project_id + manifest_key)、沙箱初始化配置、镜像仓库、模板 ref、nodeSelectors / shuffle 标签 | 细粒度 `GroupConfigProvider`:store 自存或外部 cloud provider |
| **sandbox** | **按 group 分片** | 每 `(group, route_key)`:state、sid、node_id、access_token、migration_token、last_active、snap_loc、template_id | 通道 `sandbox`/`delete` 收敛(§5);Reserve 提交 RESERVED(§7) |
| **build**(预留) | **按 group 分片** | 每 `(group, build_id)`:state、node_id、resources、template_id | 设计中;当前 ReserveBuild 仅放置节点,build→node 由 router 在内存跟踪(§7.5),此表尚未落地 |

- **group 唯一分片键**:节点表全局且小;沙箱 / 构建表按 group 分片。每操作点名 group → 唯一分片 →
  **无跨分片 / 跨 group 操作**。`list` = Range 该 group 的 sandbox 分片。
- **sid 索引 group 局部**:每请求带 `X-Kuasar-Sandbox-Group`,`(group, sid)` 在该分片内反查——无全局
  sid 索引。
- 一个 (group, route_key) 至多一条 sandbox 记录(会话亲和)。

### 6.2 GroupConfigProvider(细粒度、可外置)

group 关联配置按功能拆成**细粒度接口**,provider 可按接口选择子集实现(例如密钥走外部 cloud
provider、沙箱配置走 store):

```
GroupKeyProvider:           Get(group) → {project_id, manifest_key}        // 鉴权 + 密钥分发, 敏感
GroupSandboxConfigProvider: Get(group) → {sandbox_config, image_repo, template_ref}
GroupPlacementProvider:     Get(group) → {nodeSelectors, shuffle_labels}   // 静态部分; shuffle 由 scaler 维护
```

- 每接口经 `group_config.providers`(§3)独立选 `store`(registry 自存,`cluster-ctl group upsert` 写,
  manifest_key secretbox 加密)或 `external:<addr>`(向 cloud provider 取,不落 registry——manifest_key
  仅在 api_key 校验 / 密钥分发时过路)。这是云厂商接管 group 目录 / 多租户的扩展点;沙箱数据流只
  依赖接口可达。
- `template_ref` 须**远程可移植快照模板**(`manifest://` 持久 snp id,§9 快启);`sandbox_config` 缺失
  则数据面直触发的 Reserve 失败(§8)。

## 7. Reserve 与放置

`Reserve` 是核心:**ReserveSandbox**(按 route_key,有亲和 / 暂停态)与 **ReserveBuild**(按 build_id,
资源感知,§7.5)**显式分开**。scaler **建议**放置,registry **提交**(§4.3)。

### 7.1 沙箱状态

| state | 含义 | node_id | migration_token |
|---|---|---|---|
| `NONE` | 无记录 | — | — |
| `RESERVED` | 处理中(放置 / 创建 / 恢复在途;单飞) | 提交后置 | — |
| `READY` | 在 node_id 上运行、可服务 | 有 | — |
| `PAUSED` | 节点本机快照(快恢复、绑节点) | 有 | — |
| `SAVED` | 远程快照(可移植、不绑节点) | **空** | 有 |

无 `WARM` 态——预热靠远程快照 + 快速恢复,非节点常驻实例(§9)。

### 7.2 ReserveSandbox 流程

```
ReserveSandbox(group, route_key, create_config?) → (node_id, sid, reserve_token{access_token,sid,node_id,exp}, err)

cfg = merge(节点默认, GroupSandboxConfigProvider.Get(group).sandbox_config, create_config)
      // 数据面直触发(无 create_config)只用 节点默认 ⊕ group 配置;group 无沙箱配置 → 失败(§8)
lookup (group, route_key):
  READY     → 直接返回
  RESERVED  → 加入该键单飞,等 READY 或 park_timeout
  PAUSED    → 单飞{ 提交 RESERVED; connect(sid)[同节点恢复, 免放置]; 等 running → READY }
  SAVED|NONE→ 单飞{ node = scaler.PlaceSandbox(group, route_key) 建议; registry 原子提交(CAS);
                    create(sid, …, cfg, migration_token?)   // SAVED 带 token=一步迁移; NONE 冷启 template_ref(快照恢复)
                    等 running → READY }
```

- **单飞按 (group, route_key)**;**总等 running 事件**才返回(§5.1)。
- **快启**:`template_ref` 为 snp 快照模板时,"冷启"实为**快照恢复**(亚秒级,§9);PAUSED 同节点
  恢复免放置;SAVED 远程快照恢复。
- **reserve_token** = `{access_token, sid, node_id, exp}`——router 注入 `X-Access-Token` 转发
  `node_id.data_endpoint`(cluster-router.md §7)。

### 7.3 状态转移与驱动方

| 转移 | 驱动 | 机制 |
|---|---|---|
| NONE / SAVED → RESERVED → READY | ReserveSandbox | PlaceSandbox 建议 + 提交 → `create` → 等 running |
| PAUSED → RESERVED → READY | ReserveSandbox | `connect{sid}`(同节点,免放置)→ 等 running |
| READY → PAUSED | **节点**(TTL auto-suspend,node.md §8)| 节点 pause + `sandbox`(snap_loc=local) |
| PAUSED → SAVED | **节点**(深空闲自提升)/ 显式控制面 | 节点上送远程 + 铸 token + `sandbox`(saved,§7.4) |
| READY/PAUSED → NONE | **节点失联**(§11)/ kill | 死节点清扫;kill = `delete` + 事件 |

沙箱 **pause / 提升只由节点侧策略(TTL / 深空闲)或控制面显式调用发起**;scaler 不碰生命周期。

### 7.4 失败、回滚与 SAVED 两阶段

- **park 超时**:`park_timeout` 内未等到 running → 回错(router 转 503/504);RESERVED **回滚**到先前态
  (SAVED / NONE / PAUSED),可重入。
- **部分失败**:`create` 被 `rejected` / 超时 → 回滚;`rejected` 时 registry 改投(re-Place + 重新提交)
  重试一次再回滚。
- **SAVED 两阶段(不丢态)**:节点上送远程 + 铸 `migration_token`、报 `saved(token)` 但**保留本机
  快照**;registry 持久化 token、清 `node_id`,再下发 `delete{sid}`;节点收到才回收本机。registry 在
  token 持久化前崩溃则本机仍在、重连重报自愈。SAVED 的 migration_token 指向**内容寻址远程快照**,
  re-Place 可重复消费。

### 7.5 ReserveBuild

`POST /v3/templates`(register)经 router 触发 **ReserveBuild**:

```
ReserveBuild(group) → (node_id, err)
  node = PlaceSandbox(group, "build")        // 复用沙箱放置器:selector + shuffle 分片 + 计数 P2C(§4.2)
  返回该 node 的 data_endpoint
```

此后该 build 的 register/trigger/status/files 由 router 按 build_id **转发到该 node 的 e2b 构建 API**
(§8;**经数据面 HTTP 转发,非 node-link 命令**);节点跑三阶段构建(node.md §12),产物持久模板经远程
store 可达,任意节点快照恢复(§6 `template_ref`)。

**资源感知构建**(独立 build 资源池 `build_capacity` / `build_alloc`、按余量 P2C、build 注册表持久跟踪、
`RESERVED` 占用 build 预算)为设计中能力:节点已在 `register` / `heartbeat` 上报 `build_capacity` /
`build_alloc`,但当前放置按沙箱计数,尚未单列 build 余量。

### 7.6 密钥分发(registry 主管)

**registry**(非 scaler)负责把 group 的 manifest_key 以 TTL 租约**预分发**到其分配节点集并续租
(`key_put` 写 / 重发续租 / `key_drop` 撤,§5.1;reconcile 周期重发 `key_put` 即续租)。**预分发是基本能力**(非 opt-in):放置前密钥已在节点;
`Place` 不做即时 key_put,**所选节点缺密钥则 `create` 直接失败**(→ re-Place / 错)。`KeyDistributor`
两实现:**registry 经通道推送**(默认),或 **registry 委托 provider**(云厂商密钥服务带外推到节点,
registry 只交付"分配集")。manifest_key 由 `GroupKeyProvider` 取。lease 默认 **3h、每 1h 续租**(留 ≥2 次续租裕度抗抖动);
分配节点集 = scaler 维护的 group 有效 nodeSelectors 命中集(cluster-scaler.md §4.4),选择器变更 →
registry 撤旧节点租约 / 铺新节点;隔离节点续租断流即 ≤ 3h 后过期失效(失败闭合)。

## 8. 控制面 API(e2b 兼容,sandbox-group 范围)

router 在 `api.<domain>` 暴露 e2b 兼容控制面(实现见 cluster-router.md;本节定义契约)。**每请求必带
`X-Kuasar-Sandbox-Group`**,操作严格限定该 group——**无跨 group 操作**。鉴权:`X-API-KEY` 经 MAC 校验
该 group 的租户 manifest_key(`GroupKeyProvider` 取,复用 node.md §7),不符回 403(不泄露存在性回 404)。

| 操作 | 方法 + 路径 | 集群语义 |
|---|---|---|
| create | `POST /sandboxes` | 取 route_key + create 配置 → 合并 `节点默认 ⊕ group 配置 ⊕ create`(create 胜)→ ReserveSandbox → 回 `{sandboxID, tokens}`(node.md §4.1) |
| connect(resume) | `POST /sandboxes/{id}/connect` | 本 group 取记录(含 route_key)→ ReserveSandbox 恢复 / 迁移(可携 `X-Kuasar-Migration-Token` + route_key 一步迁移)|
| pause / kill / timeout | `POST .../pause` 等 | 取记录 node_id → **转发该节点 e2b 控制面**执行;kill 另删记录 + `delete` |
| get | `GET /sandboxes/{id}` | 本 group 分片读 +(可选)转发节点取活信息 |
| list | `GET /v2/sandboxes` | **仅本 group**:Range 该 group sandbox 分片;不跨 group |
| build | `POST /v3/templates`、`/v2/templates/{tid}/builds/{bid}`、`/status`、`/files` | register 走 ReserveBuild(§7.5)→ 其余转发被 pin 节点(node.md §12)|

- **校验后转发**:create/connect 经 Reserve(走节点命令);pause/kill/timeout/build-trigger 转发节点
  执行;list/get 从 group 分片 store 服务。
- **配置合并**:create 初始化配置与 group 关联配置合并;**数据面直触发的 Reserve(无 create)只用
  `节点默认 ⊕ group 配置`**,group 未关联沙箱配置则 Reserve 失败、拒绝请求。
- **api_key 与单机一致**:manifest_key 派生 api_key(node.md §7);cluster 按 group 的 manifest_key 校验。

## 9. 启动延迟与数据通路

最小化启动延迟是第一约束。延迟拆成两类路径(PAUSED 唤醒是介于二者的快档,见下):

```
热(已建立会话, READY):  client ─TLS─► router ─(本地路由缓存)─► node ─UDS─► guest
                          = 2 网络跳, 零控制面往返。

冷/恢复(首触 / 唤醒):    client ─► router ─► registry.ReserveSandbox ─►[PlaceSandbox 建议+提交]─►
                          node.create/connect(快照恢复) ─► running 事件 ─► 结果 ─► router ─► node
                          = 控制往返(共置时 UDS, 亚毫秒)+ 快照恢复(亚秒, 平台原生)主导。
```

- **热路径零控制往返**:router 订阅 registry 路由流、持本地缓存(`(group,route-key)/sid → node,token`),
  命中直转(cluster-router.md §5)。已建立会话不碰控制面。
- **快照恢复即"预热"**:沙箱 / group 数量远超节点,不在节点常驻实例;group 模板取 **snp 快照** →
  首启即快速恢复;空闲 PAUSED(本机)/ SAVED(远程),唤醒即恢复。"预热池"=去重远程快照存储 + 缓存,
  按需恢复(亚秒)。
- **shuffle-sharding 顺带缓存局部性**:group 钉死 n 个 slot(cluster-scaler.md §4),其快照 chunk 与
  本机 bundle 在那几个节点上缓存常热——恢复免远程拉取。一举三得:爆炸半径、均衡分布、**恢复局部性**。
- **同节点 PAUSED 优于 SAVED 迁移**:PAUSED 本机恢复免放置、免远程拉取,远快于 SAVED 迁移;调高
  节点 idle→SAVED 阈值让会话久留 PAUSED,SAVED 留给真正回收 / 跨机。
- **编排开销可忽略**:控制连接持久(无握手)、密钥预置(无 key 推送,§7.6)、共置 UDS、调用方鉴权
  缓存(cluster-router.md §8);唯一不可省的网络跳是 `registry↔node`。

## 10. Store 接口与可扩展性

四张表统一在一组 Store 接口之后,**持久后端从 sqlite 起、接口为 etcd / raft 预留,扩容不改接口**;
**sandbox-group 是唯一分片键**。

```
NodeStore(全局)               Get/Put/Delete/Range/Watch/Lease
GroupConfigProvider(细粒度)    见 §6.2,可外置
SandboxStore / BuildStore      Get/Put/Delete/Range(group,…)/Watch(group)/Lease/CAS(group,…)
  (按 group 分片)
```

- **后端**:`sqlite`(单机持久,最简,复用 node 的纯 Go sqlite);`etcd`(单 / 多节点,Watch/Lease/txn,
  分片版本号即 etcd revision);`raft`(按 group 分片,每片一组 multi-raft)。**无 memory**——任何模式
  进程重启可恢复(§11)。
- **调度器 / 提交**:放置经 `CAS(group, …)` 原子提交(§4.3),scaler 建议、registry 提交。
- **分片版本号**:每分片单调 `rev` + 有界变更日志,支撑断线增量重放(§5.3)与 watch。
- **流式 `Range`**:list / 计数 / 重同步 / GC 一律回调式迭代,不物化整表。
- **Lease**:节点 liveness、manifest_key 节点租约(§7.6)、空闲 (group,route_key) 记录 GC(SAVED / 长
  不活跃 TTL 回收,控制 route-key 基数)。

## 11. 可靠性

| 故障 | 影响 | 自愈 |
|---|---|---|
| registry 崩溃 / 重启 | 通道全断;Reserve 暂不可用 | **从持久 Store 恢复**(group 配置 / 记录)+ 节点重连重报沙箱 / 构建集(§5.3 增量)→ 机群态重建。**无纯内存**故重启不丢业务态(§1.2) |
| **节点失联(> `node_dead_after`)** | 该节点单元不可达 | 排除放置;**死节点清扫**:其 `READY`/`PAUSED`(本机快照随节点已失)、及**无在途 Reserve 持有**的 `RESERVED` 记录 → 删除(下次 Reserve 快照恢复 / re-Place);有在途 Reserve 的 `RESERVED` 不动(由单飞自管),`SAVED`(不绑节点)不动。避免 READY 记录长指死节点反复 502(cluster-router.md §9) |
| 节点抖动后重连 | 短暂 stale | 增量重连重报(§5.3);期间该节点请求 502 / park 超时 |
| router / scaler 崩溃 | 无状态 | 重启重连 registry,增量重同步缓存 / 视图 |
| Reserve 在途失败 / 超时 | 该请求 | 回滚 RESERVED,可重入(§7.4) |
| scaler 不可用 | 冷放置停滞 | 热路径不受影响(router 缓存);park 超时;scaler 重连即恢复 |
| 节点密钥租约过期(被隔离) | 不能解密 | 失败闭合:create 失败 → re-Place(§7.6) |

**关键不变量**:**节点是沙箱 / 构建存活态的真相之源,registry 是持久聚合 + 路由权威**。

## 12. 性能

- **热路径**:router 本地缓存命中 = 一次查表 + 即时刷流反代,零控制往返(§9)。
- **冷路径**:控制往返(共置 UDS)+ 快照恢复;PlaceSandbox 建议 = scaler 本地计算(订阅视图);
  单飞抑制同 (group,route_key) 冷启惊群。
- **横向扩**:router 无状态(N 副本 LB);registry 按 group 分片(Sandbox/Build),NodeStore + 通道枢纽
  为全局节点层;scaler 按 group 分片(调度 / 提交分层利于此,§4.3)。
- **断线快恢复**:分片版本号增量重放(§5.3),高密度下重连不发全量,内存 / 带宽有界。
- **route-key 基数**:一 (group,route_key) 一记录,会话维度大——Lease 对 SAVED / 长不活跃记录 TTL
  回收(§10),全量扫描流式。

## 13. See Also

- [cluster-router.md](cluster-router.md) —— e2b 兼容统一入口:控制面 + 数据面、本地路由缓存、头解析、
  调用方鉴权、Reserve 消费、两跳转发与 CONNECT、token 注入。
- [cluster-scaler.md](cluster-scaler.md) —— 放置调度器(建议)、shuffle-sharding 选择器 patch / maglev、
  PlaceBuild 资源感知、爆炸半径不变量;不联系节点、不碰生命周期。
- [node.md](node.md) §10 —— 节点侧统一通道(反向注册为路由权威);§4 —— cluster 控制面转发到的节点
  e2b 契约;§8 / §8.1 —— 沙箱生命周期与快照 / 迁移原语(§9 快启);§7 —— 密钥模型;§12 —— 三阶段构建。
- [node-proxy.md](node-proxy.md) §6 —— routesync 线格式(本统一通道的基座,含 `kind=registry` + 命令
  上行 + 版本号重放)。
- [node-resource.md](node-resource.md) —— 节点水位来源(P2C 信号)。
- `sandbox-accelerator/docs/cache.md` / `manifest.md` —— `pkg/maglev`(shuffle-sharding 复用)、远程
  快照 store 与缓存(§9 快启 / 去重)。
- `kuasar-sandbox/docs/kuasar-sandbox.md` / `deployment.md` —— 平台总体定位与集群部署拓扑。
