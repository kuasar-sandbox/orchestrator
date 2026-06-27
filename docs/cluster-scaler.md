# cluster-scaler — group 导入、WATCH_LIST 与放置

scaler 是 cluster 的放置执行方。它导入 sandbox-group,维护 placement 与密钥分配集合,消费
`node_list` 的 WATCH_LIST,并在 Reserve 冷路径中响应 `PlaceSandbox` / `PlaceBuild`。

## 1. 原则

1. **scaler 不持 node 连接**:实际 `key_put`、`build_register`、create/connect/delete 均由 node owner
   经 node_link 下发。
2. **scaler 主管 placement 与 key allocation 决策**:它通过 Provider/Importer 取得 group 配置与 key,
   算出哪些 node 应接收 key,再交 node owner 投递。
3. **WATCH_LIST 仅给 scaler**:router 不消费 node_list。
4. **高频负载按需获取**:WATCH_LIST 不承载 allocated/build_alloc/counts 等高频字段;Place 时向 node
   owner GET 实时水位或使用短 TTL cache。
5. **shuffle 影响新建,不迁移在跑 sandbox**。

## 2. 命令行

```text
cluster-ctl scaler --config /etc/cluster-ctl/scaler.yaml
```

## 3. 配置

| 字段 | 说明 |
|---|---|
| `scale_link.endpoint` | registry scale_link 地址 |
| `scale_link.tls` | 到 registry scale_link 的 mTLS |
| `placement.candidates` | P2C 候选数量 |
| `placement.zone_admit_max` | 可放置最高水位 |
| `placement.shuffle_sharding` | shuffle 规则 |
| `watch_list.resync_interval` | WATCH_LIST 断线/定期重订间隔 |
| `group_import.interval` | Importer.Range 周期 |

## 4. Provider / Importer

scaler 使用统一 group provider:

```text
SandboxGroupProvider:
  Get(group)
  GetPlacementHint(group)
  GetKey(group)       // typed manifest_key
  GetAuthKey(group)   // auth_key or verification material

SandboxGroupImporter:
  Range(cursor, limit)
```

Importer 提供 group 列表、TTL 和 generation。scaler 周期导入,对缺失/过期 group 撤销 placement 和
key allocation。

## 5. node_list WATCH_LIST

node_list 由 registry 的 node owner 汇总低频节点字段:

- node_id
- labels
- runtime_digest
- build_capacity / capacity class
- draining
- liveness timestamp

WATCH_LIST 语义:

1. scaler 订阅所有 node_list shard。
2. 首帧为 snapshot,随后 delta。
3. 无持久 changelog;落后、断线、owner moved 后全量重订。
4. 高频负载不在该流中传播;普通 heartbeat 只更新 node_link,不会触发 node_list 事件。
5. draining 变化和粗粒度 liveness 刷新更新 node_list。

## 6. Placement 计算

### 6.1 Tier-1: group placement reconcile

周期任务:

1. `Importer.Range` 得 group 集。
2. `GetPlacementHint` 得静态 selectors/shuffle labels。
3. 用 `pkg/maglev.LocateN` 计算 shuffle 结果。
4. 将 shuffle 约束合并进最终 selectors。
5. 写 group placement 记录到 route_link。
6. 计算 key allocation set,交 node owner 投递 key。

### 6.2 Tier-2: PlaceSandbox

输入:`group, route_key, target_runtime_digest?`

流程:

1. 读取 group placement selectors。
2. 在 WATCH_LIST 本地索引中过滤 labels/runtime/draining。
3. 从候选集中抽 P2C。
4. 对候选 node owner GET 实时负载。
5. 返回较优 node。

若 node owner admission 或 create 后续拒绝,route owner 可重调度。

### 6.3 PlaceBuild

输入:`group, resources`

流程与 sandbox 类似,但候选需要 build_capacity 满足请求。最终预算权威在 node owner:

1. scaler 返回建议 node。
2. route owner 调 node owner `AdmitBuild(build_id,resources,ttl)`。
3. node owner 若余量不足直接拒绝,route owner 重调度。

## 7. 密钥分发

scaler 主管 key allocation 决策:

1. 获取 group 的 typed `manifest_key` 和 `auth_key`。
2. 根据 placement selectors 得到 allocation set。
3. 将目标 node set 与 key material/ref 交给 node owner。
4. node owner 执行 `key_put/key_drop`、ack、lease、重试。

密钥是 create/build 前置条件;key_drop 或租约过期不影响已经运行的 sandbox。

## 8. 可靠性

| 事件 | 行为 |
|---|---|
| scaler 崩溃 | 冷放置暂停;热路径不受影响;重启后重订 WATCH_LIST 和重新导入 group |
| WATCH_LIST 断线 | 全量重订 |
| node labels 旧 | node owner admission/create 兜底拒绝 |
| key 投递失败 | create/build 在 node 侧 reject,route owner 重调度或返回失败 |
| build 预算泄漏 | admission lease 超时释放 |

## 9. 性能

- WATCH_LIST 只传低频字段,避免 5000 node 高频水位扇出。
- Place 只对 P2C 候选做实时 GET。
- group 导入和 shuffle reconcile 是慢路径,可分批限速。
