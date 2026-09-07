[English](cluster-placer.md) | [简体中文](cluster-placer_zh.md)

# cluster-placer — group 导入、WATCH_LIST 与放置

`cluster-ctl placer` 是独立放置调度器。它不持 node 连接,不拥有 sandbox 生命周期,也不实现 registry
状态复制。它通过 `SandboxGroupProvider` / `SandboxGroupImporter` 获取 group 配置与密钥材料,消费
registry 的 `node_list` WATCH_LIST,并向 registry 提供 `PlaceSandbox` / `PlaceBuild` / `verify-key`。

## 1. 概述

### 1.1 职责

```text
SandboxGroupProvider / Importer
          │
          ▼
      placer
          │  selector patch / Place / verify-key
          ▼
      registry
          │ node owner commands
          ▼
        node
```

placer 负责:

- group provider/importer 接入。
- `node_list` WATCH_LIST 消费。
- selector patch 与 APISecret/ManifestKey pair cache refresh。
- shuffle-sharding、静态 selector、runtime match、P2C。
- `PlaceSandbox` / `PlaceBuild` 建议。
- API key verify 的 provider 侧校验。

placer 不负责:

- 连接 node。
- 下发 `key_put` / create / delete / build 命令。
- 维护 route/build 执行态。
- 保存 registry 的 group/route 记录。

### 1.2 原则

1. **registry 不实现 group provider**:Provider/Importer 属于 placer 或外部平台。
2. **WATCH_LIST 仅给 placer**:router 不消费 node_list。
3. **高频负载不走 WATCH_LIST**:WATCH_LIST 只承载低频目录字段;最终资源确认由 node owner admission 完成。
4. **source_id 是 import 执行单元**:多个 placer 配置相同 `source_id` 时,竞争同一条 source lease。
5. **shuffle 只影响新建**:不迁移正在运行的 sandbox。
6. **凭据对分发只影响 create/build 前置条件**:drop、租约过期或 provider 更新不修改已经复制到
   现有 sandbox/build 记录的凭据对。

## 2. 命令行

```text
cluster-ctl placer --config /etc/cluster-ctl/placer.yaml
```

standalone placer 必须配置至少一个 `import_groups` source。嵌入式或生产集成可以直接注入自定义
`SandboxGroupProvider` / `SandboxGroupImporter`。

## 3. 配置

```yaml
placer:
  id: placer-1
  listen: ":7800"
  advertise: "https://placer-1.example:7800"
  memberlist_label: placer.default

registry:
  bootstrap: registry-1.example:7700

import_groups:
  - source_id: example-file-source
    source_type: file
    path: /var/lib/kuasar/groups

placement:
  candidates: 2
  zone_admit_max: yellow
  import_source_owner_count: 3
  import_source_lease_ttl: 15s
  selector_patch_refresh_interval: 1m
  # shuffle_sharding:
  #   - selector: { pool: gpu }
  #     shard_by: zone
  #     n: 2
```

| 字段 | 说明 |
|---|---|
| `registry.bootstrap` | registry bootstrap endpoint,用于拉取 membership |
| `placer.id` | placer 实例 id |
| `placer.listen` | placer HTTP API 监听 |
| `placer.advertise` | registry 调用 placer 的地址,同时作为 placer memberlist HTTP transport 地址 |
| `placer.memberlist_label` | placer memberlist label,默认 `placer.default` |
| `import_groups[]` | standalone placer 的 group source;内置 `source_type=file` |
| `placement.candidates` | placer 内部 P2C 候选数量 |
| `placement.zone_admit_max` | 可放置最高水位 |
| `placement.import_source_owner_count` | 每个 source 可参与 lease 竞争的 placer 候选数 |
| `placement.import_source_lease_ttl` | registry 侧 source lease TTL |
| `placement.selector_patch_refresh_interval` | unchanged selector patch 刷新周期 |
| `placement.shuffle_sharding` | shuffle-sharding 规则 |

registry 侧 `placer_link.placer_label` 指定 placer memberlist 域;`placer_link.placer_replica_count` 指定 registry
对一个 group 调 placer 的 failover 候选数。它不是 registry 内部 `placer_link` namespace 的 owner count。

## 4. Provider / Importer

placer 使用统一接口:

```text
SandboxGroupProvider:
  Get(group)
  GetPlacementHint(group)
  GetKey(group)       # typed manifest_key
  GetAPISecret(group) # typed APISecret

SandboxGroupImporter:
  Range(cursor, limit)
```

内置 file source 只用于本地开发和 e2e:

```yaml
import_groups:
  - source_id: example-file-source
    source_type: file
    path: /path/to/dir
```

该 source 枚举目录下的 `*.json` 文件。每个文件是一个 `SandboxGroupRecord` JSON。文件数量预期较小,
`Get/GetPlacementHint/GetKey/GetAPISecret` 可直接扫描目录解析。生产环境应通过接口接入实际 group 源。

示例:

```json
{
  "group": "/cell/project/app/group",
  "template_ref": "tmpl-1",
  "target_port": 49983,
  "api_secret": {"type": "inline", "value": "<64-lowercase-hex>"},
  "manifest_key": {"type": "inline", "value": "<64-lowercase-hex>"},
  "node_selectors": [{"pool": "default"}]
}
```

显式 `api_secret` 优先。inline `manifest_key` 缺省 `api_secret` 时,placer 按
`HMAC-SHA256(decodeHex(ManifestKey), "kuasar-api-secret-v1")` 派生默认值;
ref ManifestKey 无法在 placer 本地物化,必须显式提供 APISecret。两项 root 及 ref 的完整指纹
都必须是 64-lowercase-hex。

`target_port` 是数据面强制端口,随 Place 结果返回给 route_link/router。它不参与节点筛选;
节点筛选仍只由 `node_selectors` 与 shuffle-sharding 配置决定。

多个 source 的语义需要区分:

- Provider 点查可以按 source 顺序查找 group,若同一个 group 被多个 source 定义则报错。
- Importer Range 按 `source_id` 独立执行,不把多个 source 合并成一个 Range 视图。
- 每个 `source_id` 的 cursor/lease 独立维护。

group 从 provider 消失后,新的 Place 找不到该 group,verify-key 拒绝缺失凭据,两者均不能
继续使用已移除的 group。已经写入 node_link 的凭据对 cache 不主动删除,由 registry/node
侧 TTL 淘汰;现有 sandbox/build 的持久凭据副本保持不变。

## 5. node_list WATCH_LIST

node_list 由 registry 的 node owner 投影低频节点字段:

- node_id
- labels
- runtime_digest
- api_endpoint
- data_endpoint
- registration/execution Build configured capacity(usage/headroom 不进入低频视图)
- draining
- liveness timestamp

```text
registry node owners
  │ low-frequency projection
  ▼
node_list owner set
  │ reset + delta + bookmark
  ▼
placer local node view
```

WATCH_LIST 语义:

1. placer 按 active membership 得到 node_list owner 候选,任一时刻只消费其中一个 owner 的完整 WATCH_LIST。
2. 首帧为 reset,随后 delta,bookmark 标记初始视图完整。
3. watch token 编入 registry 本地 epoch、node_list shard view label 和 Rev。
4. epoch/label 不匹配或 changelog 已压缩时,placer 全量重订。
5. 普通 heartbeat 不触发 WATCH_LIST;draining 变化和粗粒度 liveness refresh 触发 node_list 更新。

node_list owner 分片内全复制。placer 不需要也不能把多个 node_list owner 的结果合并。当前 owner 断线时,
placer 清空该源视图并 failover 到另一个候选 owner,重新 reset + bookmark。完成 bookmark 前 placer
不应声明 ready。

## 6. placer memberlist 域

placer 启动后加入 `placer.memberlist_label` 指定的 placer memberlist,并周期性向 active / next registry
成员注册 seed:

```json
{"id":"s1","advertise":"https://s1:7800","memberlist_label":"placer.default"}
```

```text
placer S1 ── register seed ──► registry A
    │                            │
    └──── placer.default memberlist ◄──── registry observer
```

placer ready 条件:

- 已拉取当前 registry membership。
- 一个 active node_list WATCH_LIST 已完成 reset/bookmark。

group import 不构成全局 ready 门槛。每个 Place 只解析请求中的 group;该 group 未命中时返回 NoNode /
不可用,不阻塞其他 group。

registry 只根据 memberlist meta 选择 ready placer:

```json
{"role":"placer","id":"s1","advertise":"https://s1:7800","ready":true,"ready_label":"registry.2.hash"}
```

`placer_link/register` 不是 ready 状态源,也不是 placer 目录存储。

## 7. Import Reconcile

### 7.1 source owner 选择

每个 source 独立执行:

```text
readyPlacers = placer memberlist nodes where ready_label == active_registry_label
sourceOwners = LocateN(source_id, readyPlacers, import_source_owner_count)

sourceOwners race:
  POST /placer-link/import-source-lease
```

source lease 存在 registry shardkv:

```text
namespace = placer_link
shardKey  = import/source/<source_id>
recordSet = import
recordKey = state
```

lease 请求携带 `source_id`、`owner_id`、`run_id` 和 `ttl_ms`。只有 lease 胜者执行该 source 的
`Importer.Range(cursor, limit)`。

### 7.2 page 处理

```text
lease winner
  │ Range(cursor, limit)
  ▼
groups in page
  │ for each group:
  │   GetPlacementHint
  │   GetKey / GetAPISecret
  │   calculate effective selectors
  ▼
selector patch with fencing
  │
  ▼
registry refreshes node_link credential-pair cache
  │
  ▼
cursor checkpoint after whole page succeeds
```

selector patch 必须携带 `import_source_id/import_owner_id/import_run_id/import_term`。registry 只接受与当前
source lease 匹配的 patch。缺失 fencing 字段或 term/run_id 不匹配时拒绝。

`NextCursor==""` 表示本轮结束:cursor 清空并递增 round。失败或 owner 崩溃时 cursor 不推进,后续 lease
owner 从上次成功 cursor 继续或重放同一页。

### 7.3 selector patch refresh

placer 按 `placement.selector_patch_refresh_interval` 续推 unchanged selector patch,用于维持 node_link
credential-pair cache。group 从 provider 消失时不主动删除 cache;TTL 到期后自动淘汰。

## 8. Placement

### 8.1 Registry 选择 placer

registry 对 group 只做确定性 failover:

```text
readyPlacers = placer memberlist nodes where role=placer and alive and ready=true
               and ready_label == active_registry_label
candidates   = LocateN(group, readyPlacers, placer_link.placer_replica_count)
try candidates in order until success
```

registry 不对 placer 做 P2C。P2C 只用于 placer 内部从 node 候选中选择放置目标。

### 8.2 PlaceSandbox

输入:

```text
group, route_key, sandbox_id, target_runtime_digest?, config?, exclude_node_ids?
```

`sandbox_id` 是 Registry 持有的稳定公开 SandboxID;placer 不分配、解析或接收 NodeSandboxID
和 SandboxGeneration.

流程:

```text
provider.GetPlacementHint(group)
  │
  ▼
filter node_list:
  selector match
  not draining
  not in exclude_node_ids
  zone <= admit max
  runtime digest match
  shuffle slot match
  │
  ▼
P2C over candidates
  │
  ▼
return node_id + create_spec + APISecret fingerprint + runtime/template hints
```

`node_list` 只提供低频目录,不判定 node 是否在线。route owner 在提交前向 node owner 查询当前
node-link 连接；断线、admission 拒绝或命令拒绝的候选加入 `exclude_node_ids`,随后重新 Place,直到选中
在线候选或 placer 返回无候选。node owner 是唯一存活权威。

### 8.3 PlaceBuild

输入:

```text
group, build_id, template_id, resources
```

Build placement 与 sandbox 类似,但 Placer 的低频视图只按 configured registration capacity
排除永远装不下单个 Build 的节点,不接收 heartbeat usage。当前 headroom 只在 Holder/节点边界读取:

1. placer 返回静态 capacity 可容纳的建议 node。
2. Registry 通过当前 Holder 确认 node-link 在线并读取最新 durable registration usage。
3. headroom 已不足时 Registry 排除该候选并重调度,尚不创建注册 intent。
4. Registry 持久化选中 node/build 的 STARTING intent,再发送 `build_register`。
5. node 在 SQLite 事务中作最终 registration admission；明确无副作用拒绝才允许排除候选,
   timeout/断连/ACK 丢失则固定该 node/BuildID 查询或重试。

## 9. Key Distribution

placer 主管 selector patch 和 APISecret/ManifestKey pair cache refresh:

1. 获取 group 的 typed `manifest_key` 和 `api_secret`;缺省 APISecret 只可由 inline ManifestKey 固定派生。
2. 根据 placement selectors 和 shuffle 结果得到目标 node set。
3. selector patch 要么携带完整 pair,要么完全不携带凭据字段。两项各带 typed carrier 和完整
   64-hex SHA-256 指纹;半对拒绝。
4. registry/node owner 以完整 `APISecretFingerprint` 为 record key,把 desired pair 写入 node_link
   `key_pair` recordSet,并在 owner set 内 CAS 复制。
5. node_link heartbeat 只对未获 `AckAccepted` 或已进入续租窗口的条目执行原子 `key_put`;只有匹配
   当前完整 desired pair 和 lease 的 ACK 才推进已交付状态。
6. 同一 APISecret 完整指纹只能绑定完全相同的 pair material;冲突不得覆盖。
7. 未续租 pair 由节点 TTL 淘汰,`key_drop` 以完整 APISecret 指纹定位,不作为正确性依赖。

凭据对是 create/build 前置条件。cache 删除、key_drop、租约过期或后续 provider 更新不影响已经复制
到现有 sandbox/build 业务记录的凭据对。

## 10. 可靠性

| 事件 | 行为 |
|---|---|
| placer 崩溃 | registry failover 到同 group 的下一个 ready placer;热路径不受影响 |
| WATCH_LIST 断线 | placer 清空 node_list 视图并 failover 到另一个 owner 全量重订;完成 bookmark 前 not-ready |
| provider 不可用 | 受影响 group 的 Place/verify-key 返回不可用 |
| node labels 旧 | node owner admission/create 兜底拒绝 |
| source owner 崩溃 | source lease 到期后其他候选从已提交 cursor 接管;旧 owner patch/cursor 被 fencing 拒绝 |
| key 续租投递失败 | timeout、断线或 reject 都不推进已交付状态；create/build 在 node 侧 reject，下一次 heartbeat refresh 重试 |
| Build 注册超时或 ACK 丢失 | 保留已持久化的选定 node/BuildID intent,只查询或重试同一节点。持久 registration usage 由节点负责,placer 没有靠 admission lease 计时器释放它的机制;节点记录删除决定额度释放及 registry 投影删除。 |

权威实现见 [注册下发与投影恢复](../internal/registry/build.go) 和
[放置谓词](../internal/placer/scaler.go)。尤其不能把投递结果不明当作无副作用拒绝的证据。

## 11. 性能

- WATCH_LIST 只传低频字段,避免高频水位扇出。
- Place QPS 通过 ready placer 集合按 group `LocateN` 分散。
- placer 内部仅按当前本地目录视图比较抽样的 P2C 候选,不向节点同步轮询实时负载。
- import/source 独立分页,不同 source 之间不做全量合并。
- selector patch 是慢路径,可限速;unchanged patch 只用于刷新 TTL。

## 12. See Also

- [cluster.md](cluster_zh.md) — registry membership、shardkv、node_link、route_link、placer_link 总设计。
- [cluster-router.md](cluster-router_zh.md) — router Reserve 消费、route cache 和数据面转发.
- [node.md](node.md) — node-link 节点侧注册、心跳、命令执行和 key TTL。
