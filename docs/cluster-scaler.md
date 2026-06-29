# cluster-scaler — group 导入、WATCH_LIST 与放置

scaler 是 cluster 的放置执行方。它通过 `SandboxGroupProvider` / `SandboxGroupImporter` 获取
sandbox-group 配置,维护 placement 与密钥分配集合,消费 `node_list` 的 WATCH_LIST,并向 registry 提供
`PlaceSandbox` / `PlaceBuild`。

## 1. 原则

1. **scaler 不持 node 连接**:实际 `key_put`、`build_register`、create/connect/delete 均由 node owner
   经 node_link 下发。
2. **scaler 主管 placement 与 key allocation 决策**:它通过 Provider/Importer 取得 group 配置与 key,
   算出哪些 node 应接收 key,再把 intent 交给 registry/node owner 执行。
3. **registry 不实现 group provider**:内置 file source 只服务开发和 e2e;生产通过接口注入 provider/importer。
4. **WATCH_LIST 仅给 scaler**:router 不消费 node_list。
5. **高频负载不走 WATCH_LIST**:WATCH_LIST 不承载 allocated/build_alloc/counts 等高频字段;scaler 用
   低频目录做候选过滤,最终资源确认由 registry/node owner admission 完成。
6. **shuffle 影响新建,不迁移在跑 sandbox**。

## 2. 命令行

```text
cluster-ctl scaler --config /etc/cluster-ctl/scaler.yaml
```

## 3. 配置

| 字段 | 说明 |
|---|---|
| `registry.bootstrap` | registry bootstrap endpoint,用于拉取 membership |
| `member.id` | scaler 实例 id |
| `member.listen` | scaler HTTP API 监听 |
| `member.advertise` | registry 调用 scaler 的地址 |
| `memberlist.label` | scaler memberlist label,默认 `scaler.default` |
| `import_groups[]` | standalone scaler 的 group source;首版内置 `source_type=file` |
| `placement.candidates` | scaler 内部 node P2C 候选数量 |
| `placement.zone_admit_max` | 可放置最高水位 |
| `placement.import_owner_count` | 每个 import task 可参与 lease 竞争的 scaler 候选数,默认 3 |
| `placement.import_task_lease_ttl` | registry 侧 import task owner lease TTL,默认 15s |
| `placement.allocation_refresh_interval` | unchanged key allocation patch 刷新周期,默认 1m |
| `placement.shuffle_sharding` | shuffle 规则 |

registry 侧的 `scale_link.scaler_label` 指定 scaler memberlist 域,默认 `scaler.default`;
`scale_link.scaler_replica_count` 控制每个 group 的 scaler failover 候选数量,`scale_link.allocation_ttl`
控制 registry 侧 scaler key allocation intent 的自动过期时间。`scale_link` 不再是 registry reverse
session,也不保存 scaler 目录。scaler 通过 registry membership 得到 active / next registry owner 成员、
node_list owner 候选与当前 registry label,向这些 owner 成员注册 memberlist seed,并始终只从一个
node_list owner 订阅 WATCH_LIST;断线或 membership 变化后切换到下一候选。membership refresh 会尝试
bootstrap 与已知成员并选择 active version 最新的结果。registry 收到 scaler register 后以 `role=observer`
加入 scaler memberlist;ready scaler 视图来自 memberlist meta。scaler 在本地 API 提供:

```text
POST /scale-link/place
GET  /scale-link/verify-key
POST /scale-link/import-task-lease
```

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

Importer 只提供当前可见的 group 列表。Provider 对 group 的点查 miss 后,新的 Place/verify-key 返回
不可用。registry 已经收到的 key allocation intent 由
`scale_link.allocation_ttl` 自动过期,并在下一轮 key distributor reconcile 中清理 node_link key cache。

内置 `file` source 只是辅助实现,用于本地开发和 e2e。`cluster-ctl scaler` 独立运行时必须至少配置一个
`import_groups` source;生产环境也可以在嵌入式模式直接注入自定义 `SandboxGroupProvider` /
`SandboxGroupImporter`:

```yaml
import_groups:
  - source_id: example-file-source
    source_type: file
    path: /path/to/dir
```

该 source 枚举目录下的 `*.json` 文件。每个文件是一个 `SandboxGroupRecord` JSON。文件数量预期较小,
`Get/GetPlacementHint/GetKey/GetAuthKey` 可直接扫描目录解析;生产环境应通过实现
`SandboxGroupProvider` / `SandboxGroupImporter` 接口接入实际 group 源。

## 5. node_list WATCH_LIST

node_list 由 registry 的 node owner 汇总低频节点字段:

- node_id
- labels
- runtime_digest
- build_capacity / capacity class
- draining
- liveness timestamp

WATCH_LIST 语义:

1. scaler 按 active / next registry membership 得到 node_list owner 候选,但任一时刻只消费其中一个 owner
   的完整 WATCH_LIST。
2. 首帧为 snapshot/reset,随后 delta,最后以 bookmark 标记初始视图完整。
3. watch token 编入 membership label;label 变化或 token fingerprint 不匹配时全量重订。
4. 高频负载不在该流中传播;普通 heartbeat 只更新 node_link,不会触发 node_list 事件。
5. draining 变化和粗粒度 liveness 刷新更新 node_list。

node_list owner 分片内全复制,所以 scaler 不需要也不能把多个 owner 的结果做片间合并。scaler ready 只要求
一个 active node_list WATCH_LIST 完成 reset/bookmark;当前 owner 断线时,scaler 清空该源视图并 failover
到另一个候选 owner 重新 snapshot。group 由 Provider 按请求点查,不参与全局 ready。

## 6. Scale-link 成员域

scaler 启动后加入 `memberlist.label` 指定的 scaler memberlist,并周期性向每个 active / next registry
owner 成员注册 seed:

```json
{"id":"s1","advertise":"https://s1:7800","memberlist_label":"scaler.default","memberlist_advertise":"https://s1:7800"}
```

scaler ready 的条件:

- 已拉取当前 registry membership。
- 一个 active node_list WATCH_LIST 已完成 reset/bookmark。

group import 不构成全局 ready 门槛。每个 Place 只解析请求中的 group;该 group 未命中时返回
NoNode/不可用,不会阻塞其他 group。

registry 只把 scaler memberlist 中 `role=scaler`、alive、`ready=true` 且
`ready_label == active_registry_label` 的成员作为 Place 候选。ready 信息不来自 `scale_link/register`,
而来自 scaler memberlist meta:

```json
{"role":"scaler","id":"s1","api_advertise":"https://s1:7800","memberlist_advertise":"https://s1:7800","ready":true,"ready_label":"registry.2.hash"}
```

## 7. Placement 计算

### 7.1 group placement reconcile

每个 source 暴露一个或多个 import task。内置 file source 的 task id 等于 `source_id`;自定义 importer
未显式暴露 task 时作为 `default` task 处理。

每轮 reconcile 对每个 task 执行:

1. 从 scaler memberlist 取 `role=scaler && ready=true && ready_label==active_registry_label` 的成员 id。
2. 用 `pkg/maglev.LocateN(task_id,ready_scalers,placement.import_owner_count)` 得到 task owner 候选。
3. 候选按 `task\0<task_id>` 定位 scale_link owner set,向 owner endpoint
   `POST /scale-link/import-task-lease` 抢 task lease。请求携带
   `task_id`、`owner_id`、进程 `run_id` 和 `ttl_ms`。
   task lease 是 `scale_link` 命名空间记录,record key 为 `task\0<task_id>`,按
   `NamespaceScaleLink + record_key` 定位 registry owner set,并在分片内用无主 CAS 全复制维护。
4. 只有 lease 胜者执行该 task 的 `Importer.Range`。
5. 胜者用 `GetPlacementHint` 得静态 selectors/shuffle labels,用 `pkg/maglev.LocateN` 计算 shuffle 结果,
   将 shuffle 约束合并进最终 selectors。
6. 胜者计算 key allocation set,按 `allocation\0<group>` 定位 scale_link owner set,生成 selector/key
   allocation patch。patch 携带 `import_task_id/import_owner_id/import_run_id/import_term`;registry 只接受与
   当前 task lease 匹配的 patch,并把 allocation intent 写入 scale_link CAS 记录。
7. membership 切换期间保持 group/key allocation 视图在新 owner 可用;route/build 执行态仍由同 group
   请求或 node 事件触发 read-repair。

scaler 会按 `placement.allocation_refresh_interval` 续推 unchanged allocation patch,该值必须短于 registry
侧 `scale_link.allocation_ttl`。group 从 provider 消失时不主动删除 allocation intent;registry TTL 到期后
自动清理。

### 7.2 Registry 选择 scaler

registry 对 group 只做确定性 failover:

```text
readyScalers = scaler memberlist nodes where role=scaler and alive and ready=true and ready_label == active_registry_label
candidates   = LocateN(group, readyScalers, scale_link.replica_count)
try candidates in order until success
```

registry 不对 scaler 做 P2C。P2C 只用于 scaler 内部从 node 候选中选择放置目标。

### 7.3 PlaceSandbox

输入:`group, route_key, target_runtime_digest?`

流程:

1. 读取 group Provider 和 placement hint。
2. 在 WATCH_LIST 本地索引中过滤 labels/runtime/draining。
3. 从候选集中抽 P2C。
4. 返回 `node_id + create_spec + key intent + runtime/template hints`。

若 node owner admission 或 create 后续拒绝,route owner 可换下一个 scaler 或重新 Place。

### 7.4 PlaceBuild

输入:`group, resources`

流程与 sandbox 类似,但候选需要 build_capacity 满足请求。最终预算权威在 node owner:

1. scaler 返回建议 node。
2. route owner 调 node owner `AdmitBuild(build_id,resources,ttl)`。
3. node owner 若余量不足直接拒绝,route owner 重调度。

## 8. 密钥分发

scaler 主管 key allocation 决策:

1. 获取 group 的 typed `manifest_key` 和 `auth_key`。
2. 根据 placement selectors 得到 allocation set。
3. 将目标 node set 与 key material/ref 作为 allocation patch 推给 registry。
4. registry/node owner 把每个 node 的 desired key list 写入 node_link 记录并在 owner set 内 CAS 复制。
5. node_link key cache 记录已成功下发的 lease 到期时间;node-link 心跳维系只对未下发或 TTL 已到期的
   条目执行 `key_put` 续租。未续租 key 由节点 TTL 淘汰,`key_drop` 不作为正确性依赖。

密钥是 create/build 前置条件;key cache 删除、key_drop 或租约过期不影响已经运行的 sandbox。

## 9. 可靠性

| 事件 | 行为 |
|---|---|
| scaler 崩溃 | registry 对该 group failover 到下一个 ready scaler;热路径不受影响 |
| WATCH_LIST 断线 | scaler 清空 node_list 视图并 failover 到另一个 owner 全量重订;完成 bookmark 前 not-ready |
| provider 不可用 | 对受影响 group 的 Place/verify-key 返回不可用 |
| node labels 旧 | node owner admission/create 兜底拒绝 |
| task owner 崩溃 | import task lease 到期后其他候选接管;旧 owner 后续 patch 被 term/run_id fencing 拒绝 |
| key 续租投递失败 | create/build 在 node 侧 reject,route owner 重调度或返回失败;下一次 heartbeat refresh 重试 |
| build 预算泄漏 | admission lease 超时释放 |

## 10. 性能

- WATCH_LIST 只传低频字段,避免 5000 node 高频水位扇出。
- registry 到 scaler 按 group `LocateN` 分散 Place QPS。
- Place 只对 P2C 候选做实时 GET。
- group 导入和 shuffle reconcile 是慢路径,可分批限速。
