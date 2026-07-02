# cluster — registry 自聚簇、路由与放置控制面

`cluster-ctl` 是大规模部署的集群控制面,由三个独立角色组成:

- `registry`:有状态可靠集群,维护 node / route / scaler import 等执行态。
- `router`:e2b 兼容统一入口,按 sandbox-group 定位 route owner,热路径直转 node。
- `scaler`:group provider/importer 与放置调度器,消费 `node_list`,向 registry 提供 Place / verify-key。

Registry 的基础能力是按 `namespace + shard key + recordSet + record key` 组织的一致性 KV。所有
`node_link`、`route_link`、`node_list`、`scale_link` 记录都复用这套模型:分片间不遍历,分片内全复制,
通过 quorum 读写、CAS、WATCH 和 read-repair 收敛。

## 1. 概述

### 1.1 总体拓扑

```text
                         client / e2b SDK
                                │
                                ▼
                         ┌────────────┐
                         │   router   │
                         │ ingress +  │
                         │ route cache│
                         └─────┬──────┘
                               │ route_link Reserve/Resolve
                               ▼
┌────────────┐          ┌──────────────────────┐          ┌────────────┐
│ node-ctl   │ node_link│      registry        │ scale_link│   scaler   │
│ serve      │◄────────►│   state cluster      │◄────────►│ placement  │
│ sandbox VM │          │ shardkv namespaces   │          │ importer   │
└────────────┘          └──────────────────────┘          └────────────┘
                               ▲
                               │ node_list WATCH_LIST
                               └────────────── scaler consumes one owner
```

稳态数据面不经过 registry。只有 cold/miss/fail-fast 路径需要 registry:

- router miss 时调用 `ReserveSandbox` / `ReserveBuild`。
- registry 调 scaler `PlaceSandbox` / `PlaceBuild`。
- registry 经 node owner 下发 create/connect/delete/build/key 命令。
- node 经 node_link 上报 sandbox/build 状态和低频节点目录。

### 1.2 设计原则

1. **group 是业务分片键**:所有 cluster 北向请求必须携带 `X-Kuasar-Sandbox-Group` 或等价 group
   身份。当前不支持无 group 的 cluster 数据面入口。
2. **registry 是可靠状态集群**:执行态由 registry 成员直接复制。逻辑 owner set 容忍单成员故障;
   整套 registry 完全下电后不要求自动恢复运行中 sandbox。
3. **分片间不遍历**:registry 不能跨 group/node shard 扫描,不能把多个无关 shard 的局部结果合并成事实。
4. **分片内全复制**:同一 `namespace + shard key + recordSet` 的 owner 均持有完整视图,可响应点读、
   CAS 和 WATCH。非 owner 可作为协调者转发点读/CAS,但不能提供本地完整 Snapshot/WATCH。
5. **成员健康不参与分片计算**:成员表来自版本化配置;`LocateN` 输入只使用 membership members。
   memberlist 只做健康检测和 meta 传播。
6. **node 是运行态真相之源**:sandbox/build 是否仍存在以 node 上报为准。node 整机重启直接清空,
   不重拉旧 sandbox。
7. **scaler 不拥有生命周期**:scaler 只做 group 导入、selector patch、shuffle-sharding、P2C 与 Place
   建议。最终资源确认在 node owner admission。
8. **router 不订阅海量 group**:Reserve 返回 READY 或失败;router 只维护 route cache 和 active connection
   cache。

### 1.3 角色边界

| 角色 | 职责 |
|---|---|
| registry member | 组成 registry 自聚簇,承载 `route_link` / `node_link` / `node_list` / `scale_link` 执行态和 membership |
| router | e2b 统一入口;按 group 定位 route owner;miss 时 Reserve;热路径复用活动连接 |
| scaler | 消费 `node_list` WATCH_LIST;通过 provider/importer 导入 group;维护 placement 与 selector patch;提供 Place / verify-key |
| node | 运行 sandbox/build;通过 node_link 上报全量清单和事件;接收 create/connect/delete/build/key 命令 |

## 2. 配置与监听

registry 默认只有一个控制面监听。node_link 可以配置独立监听用于隔离 node 长连接流量,但不改变 owner
规则。

```yaml
member:
  id: A
  listen: "0.0.0.0:7700"       # registry 统一控制面监听

membership:
  active: 1
  # next: 2                    # joint 阶段目标版本
  # old_grace: 1               # cutover 后保留旧成员作为 read-only shardkv 证书/快照来源
  reload_ready_timeout: 10s
  versions:
    - version: 1
      members:
        - { id: A, advertise: "https://A:7700", node_advertise: "A:7700" }
        - { id: B, advertise: "https://B:7700", node_advertise: "B:7700" }
        - { id: C, advertise: "https://C:7700", node_advertise: "C:7700" }
  owners:
    route_link: 3
    node_link: 3
    scale_link: 3
    node_list: 3

node_link:
  # listen: ""                 # 空 = 复用 member.listen;非空 = 独立 node 长连接监听
  heartbeat_interval: 10s
  node_dead_after: 30s

route_link:
  park_timeout: 30s

node_list:
  watch_retention: 10000

scale_link:
  scaler_label: scaler.default
  scaler_replica_count: 3      # registry 调 scaler 的 failover 候选数
  min_ready_scalers: 1
  place_timeout: 2s
```

默认 path:

```text
/cluster/membership
/node-link/*
/route-link/*
/scale-link/*
/internal/registry-member/shardkv
/internal/node-owner
/internal/node-link/relay
/internal/memberlist/packet
/internal/memberlist/stream
```

registry 对外地址写在 `membership.versions[].members[].advertise`;
redirect-capable node 使用 `membership.versions[].members[].node_advertise`。

## 3. Membership 与健康检测

### 3.1 版本化成员表

registry 成员表只来自运维分发的配置文件。每个版本有稳定 label:

```text
registry.<version>.<sha256(sort(member_ids))>
```

registry 可同时持有三个视图:

- `active`:客户端定位 route/node/node_list/scale_link owner 的版本。
- `next`:joint 阶段的目标版本。写入必须同时满足 active quorum 和 next quorum。
- `old_grace`:cutover 后保留的旧版本。旧成员可作为 peer/node_link 接入或 node-owner RPC 目标,
  并作为 shardkv read-only set 提供旧 head 的 snapshot/certificate;但不参与写 owner set,也不能用旧视图提交写入。

```text
stable(v1)
  -> load_config(active=v1,next=v2)
  -> wait_memberlist_ready(v2)
  -> joint owner set: owners(v1) ∪ owners(v2)
  -> cutover active=v2, old_grace=v1
  -> retire(v1)
```

reload 只能从 stable 加载/取消 `next`,或从已配置的 `next` 切换为新的 `active`。不能从
stable(v1) 直接跳到 stable(v2)。

### 3.2 memberlist 边界

```text
membership config                       memberlist
  defines member ids                      observes liveness
  defines advertise URLs                  propagates tiny meta
  input to LocateN                        does not reshard
  controlled by reload                    does not own member list
```

每个 registry membership label 对应独立 memberlist 域。memberlist transport 复用控制面 HTTP:

```text
registry A /internal/memberlist/*  ◄────►  registry B /internal/memberlist/*
label=registry.1.hash(A,B,C)
```

memberlist 只用于:

- 判断配置成员是否运行期可达。
- 发布 registry/scaler 的 ready meta。
- 给 RPC fail-fast / cooldown 提供信号。

memberlist 不用于:

- 维护 registry 成员清单。
- 改变 `LocateN` 输入。
- 做数据复制。
- 把 suspect/dead 转化为 reshard。

### 3.3 scaler memberlist 域

scaler 使用独立 label,默认 `scaler.default`。registry 不配置 scaler 列表,而是作为
`role=observer` 加入 scaler memberlist。`POST /scale-link/register` 只提供 seed,用于 registry 初始 join。
ready scaler 由 scaler memberlist meta 表达:

```json
{"role":"scaler","id":"s1","advertise":"https://s1:7800","ready":true,"ready_label":"registry.2.hash"}
```

registry 只把 `role=scaler && alive && ready=true && ready_label==active_registry_label` 的成员作为
Place / verify-key 候选。

## 4. Registry 状态模型

### 4.1 数据层级

```text
namespace
  └── shardKey
        └── recordSet
              ├── commit Rev
              ├── recordKey -> value
              └── tombstone(recordKey)
```

- `namespace`:逻辑域,例如 `route_link`、`node_link`。
- `shardKey`:成员分片键,例如 group 或 node_id。
- `recordSet`:数据复制、Rev 和 WATCH 域。
- `recordKey`:recordSet 内的记录键。
- `Rev`:recordSet commit version。record 的 `rev` 是该 record 最后修改时的 recordSet Rev。

shard 是成员分片单位;recordSet 是数据复制单位。Rev 不放在 shard 层,否则会把一个 shard 内原本可独立
演进的 profile/sandbox/build/key 等数据域强行绑定。

### 4.2 owner 解析

```text
(namespace, shardKey)
        │
        ▼
ShardResolver
        │  LocateN(shardKey, versioned members, owner_count)
        ▼
owner set
   ┌──────────┬──────────┬──────────┐
   ▼          ▼          ▼
 registry A  registry B  registry C
 full copy   full copy   full copy
```

`membership.owners.route_link/node_link/scale_link/node_list` 分别控制各 namespace 的 owner 数量。
owner count 是 registry 内部复制因子。`scale_link.scaler_replica_count` 只控制 registry 调 ready scaler
的 failover 候选数,不是 `scale_link` namespace 的 owner count。

### 4.3 namespace schema

| namespace | shard key | recordSet | record key | 内容 |
|---|---|---|---|---|
| `route_link` | group | `sandbox` | route_key | route 记录 |
| `route_link` | group | `build` | build_id | build 执行态 |
| `node_link` | node_id | `profile` | `profile` | node profile、labels、liveness、link_owner、低频容量 |
| `node_link` | node_id | `sandbox` | group + route_key | node 维度 sandbox 清单 |
| `node_link` | node_id | `build` | group + build_id | node 维度 build 清单 |
| `node_link` | node_id | `manifest_key` | fingerprint | node key cache |
| `node_list` | `node_list` | `nodes` | node_id | 低频节点目录和 WATCH_LIST |
| `scale_link` | `import/source/<source_id>` | `import` | `state` | import source lease/cursor |

当前 recordSet 集合由固定 schema 定义。如果未来某 namespace 引入动态 recordSet 名称,recordSet 目录也必须
作为同 shard 下的保留 recordSet 维护,并遵守同一套 CAS/WATCH 规则。

### 4.4 读写协议

shardkv 要解决的问题是:registry 不引入每 group/node 的 primary,但任意接入成员都能在目标 shard owner set
内完成 CAS、读取和 WATCH;同时在单成员故障、membership joint view、局部 repair、tombstone 回收时仍保持
recordSet committed history 单调一致。

解决思路是把“成员分片”和“数据复制”分开:

- `shardKey` 只决定 owner set。
- `recordSet` 是 CAS、Rev、WATCH 和提交证书的复制单元。
- 写入使用唯一 ballot `(round, writer_id)` 和两阶段 prepare/accept。
- accepted state 先不可见;只有被 quorum 选择出的 committed snapshot 才能安装到 committed view。
- committed snapshot 携带 `CommitCertificate`,让后续读写即使只看到一个最新副本也能证明该 Rev 已被 quorum
  决定。

shardkv 在每个 shard view 内区分两个集合:

- `WriteSets`:允许 prepare/accept/install 的集合。stable 为 active;joint 为 active+next;
  cutover old_grace 阶段为 active。
- `ReadSets`:允许 read/snapshot/certificate 验证的集合。stable 为 active;joint 为 active+next;
  cutover old_grace 阶段为 active+old_grace。

stable 模式提交条件是当前 `WriteSets` quorum;joint 模式提交条件是 active quorum + next quorum。
old_grace 只参与读取和证明旧 head,不参与新写提交。

```text
coordinator
   │ prepare(ballot)
   ├──────────────► owner A
   ├──────────────► owner B
   └──────────────► owner C
          quorum promise
   │ accept(record, new_rev)
   ├──────────────► owner A
   ├──────────────► owner B
   └──────────────► owner C
          quorum accepted
   │ install(snapshot, commit_certificate)
   ├──────────────► owner A
   ├──────────────► owner B
   └──────────────► owner C
          quorum installed, laggards repaired best-effort
```

#### 4.4.1 不变量

shardkv 的正确性建立在以下不变量上:

1. **复制单元是 recordSet**。同一 `(namespace, shardKey, recordSet)` 下只有一条单调递增的 `Rev`
   序列。一次 commit 最多改变一个 `recordKey`,但它占用整个 recordSet 的下一个 `Rev`。
2. **成员分片和数据复制分离**。`shardKey` 只决定 owner set;`recordSet` 决定 Rev、CAS 和 WATCH 域。
   同一 shard 下不同 recordSet 的 Rev 独立递增。
3. **ballot 全局作用于 recordSet**。`prepare` 会提升 recordSet 级 promise。这样不同 key 的并发写也会
   在同一 recordSet Rev 序列上定序,不会各自分配相同 `Rev+1`。
4. **accept 不可见**。owner 收到 `accept` 后只保存 pending accepted record,不写入 committed records,
   不触发 WATCH,不让普通读返回。只有带提交证书的 install 或 quorum 可验证的 snapshot 才进入
   committed view。
5. **每个 owner 持有完整 committed view**。ready owner 可以在本地提供 Snapshot/WATCH;未 ready 的本地
   view 只能参与 quorum 协议,不能作为完整本地读源。

系统假设 registry 成员是 crash/fail-stop 模型,不会伪造对端响应;通信可能超时、断开、重复,但请求体不被
拜占庭篡改。`UpdatedAt` 只服务 TTL/GC,不参与一致性排序。

#### 4.4.2 提交证书

accept quorum 已经决定了某个 `Rev` 的值,但只把 accepted state 留在内存里会带来一个可用性问题:如果
随后一个 accepted 成员故障,剩余 quorum 可能只看到一个最新副本和一个旧副本,无法通过“相同 snapshot
达到 quorum”恢复最新提交。

因此 coordinator 在 accept quorum 后生成 `CommitCertificate`:

```text
CommitCertificate {
  rev      = committed recordSet Rev
  digest   = hash(rev + canonical live records)
  ballot   = accepted ballot
  labels   = membership labels whose owner set accepted quorum
  members  = accept quorum member ids
}
```

`canonical live records` 只包含未删除记录。删除操作仍推进 recordSet `Rev`,因此 delete commit 会改变
`digest`;但过期 tombstone 是否仍被某个 owner 本地保留,不影响该 `Rev` 的逻辑 committed state。

install 把 full committed snapshot 和 certificate 一起写到 owner。之后 quorum 读 snapshot 时,可用两种方式
确认 committed head:

```text
case A: same (rev,digest) snapshot is returned by quorum
case B: one snapshot carries valid certificate, and certificate.members proves one ReadSet quorum
```

`case B` 允许“m1/m2 已提交, m3 当时故障;随后 m1 故障, m2+m3 仍可继续写”:m2 携带的 certificate 证明
`Rev` 已被 m1/m2 accept quorum 决定,coordinator 可把该 snapshot repair 到 m3 后继续分配 `Rev+1`。

`labels` 解决 membership 变更阶段的旧 head 识别问题。V1 稳定阶段提交的 certificate 带 `labels=[V1]`。
进入 V1+V2 joint 后,第一次触达某个冷 recordSet 时,V2 owner 可能还没有该 head;只要某个 snapshot 携带的
certificate 对 V1 owner set 满足 quorum,它仍然是已提交 head。coordinator 先把这个 head install/repair 到
joint owner set,再执行下一次写。joint 阶段产生的新 certificate 会同时带 V1/V2 labels,因为新写必须满足
old quorum + new quorum。

cutover 到 `active=V2,old_grace=V1` 后,V1 不再进入 `WriteSets`,但仍进入 `ReadSets`。冷 recordSet 第一次
由 V2 访问时,V1 certificate 仍可证明旧 head;registry 会把旧 head repair 到 V2 write owners 后继续读写。
一旦 V2 write owners 已持有带 certificate 的 committed head,后续读写只要求 V2 write quorum 在线;不再要求
V1 old_grace quorum 同时在线。
只有 `old_grace` 退出后,V1 certificate 才不再作为当前 shard view 的证明来源。切换期间仍禁止绕过 joint view
的 old-only 写和 new-only 写并发执行。

如果 coordinator 在 accept quorum 之后、install quorum 之前失败,下一次写的 prepare 会读到 pending accepted
record。新 coordinator 必须先用新 ballot 完成该 pending record 并安装 certificate,再处理自己的写。对调用方而言,
这种阶段性失败的 CAS 返回 `ErrQuorum` 时结果是 unknown:调用方必须按 `(recordKey, expectRev)` 重新读/重试,
不能假设该写一定未发生。

install 还必须遵守本地单调规则:

- 本地 view 已持有提交证书时,不能被更低 `Rev` 覆盖。
- 同一 `Rev` 下,只有 canonical digest 相同的 snapshot 才能互相替换;这用于 tombstone 保留/压缩形态转换。
- 同一 `Rev` 下 canonical digest 不同,代表两个不同 committed histories,必须拒绝并让上层重试/报冲突。
- 无证书本地 view 只视为待修复缓存,可被 quorum 选择出的 committed snapshot 覆盖。

#### 4.4.3 写正确性

一次成功 CAS 的线性化点是 accept quorum 决定该 `Rev` 的时刻;成功返回则额外要求 install quorum 已保存
committed snapshot/certificate,保证后续即使另一个成员故障也能恢复该提交。

为什么两个不同值不能同时以同一 Rev 提交:

- 每次写使用唯一 ballot `(round, writer_id)`。
- 任意两个 quorum 在同一个 member set 内相交;joint view 要求 old quorum 和 new quorum 都满足,因此与
  old-only / new-only 操作也保持交集。membership 变更期间不能允许绕过 joint view 的 old-only 写和
  new-only 写并发执行。
- 相交 owner 在 prepare 后会拒绝更低 ballot 的 accept。
- 如果相交 owner 已保存 pending accepted record,后续更高 ballot 的 writer 会在 prepare 响应中看到它,
  并先完成该 record。新写不会跳过已 accepted 的 `head+1`。
- owner 拒绝 `rec.rev > local_rev+1` 的 accept,防止 coordinator 跳过中间 Rev。

因此 recordSet 的 committed history 是一条线性序列。CAS 的 `expectRev` 匹配的是目标 record 的最后修改
Rev;当目标 key 未变化而其他 key 推进了 recordSet Rev 时,该 key 的 `expectRev` 不会被无关写破坏。

#### 4.4.4 读正确性

点读默认走 quorum,但不必每次都拉取 full snapshot:

```text
read(key) from all ReadSet members
  ├─ if same record version is visible on WriteSet quorum: return it and repair write laggards
  ├─ if ReadSet quorum reports not found and no ReadSet member reports a record: return not found
  └─ otherwise fetch committed snapshot, choose committed head, install/repair, then read key
```

返回某个 record version 的条件是该版本本身在 quorum 中可见。若该 key 在更高 Rev 被修改/删除,成功返回的
写已把新版本安装到 `WriteSets` quorum;读到的 `ReadSets` 与当前/旧提交证书集合相交,不会把旧版本误判为
quorum-visible。not found 也必须由 `ReadSets` quorum 证明;old_grace 阶段不能只凭 active 空副本判定旧
recordSet 不存在。若存在单副本高版本、或不同副本冲突,点读必须退回 committed snapshot 选择逻辑。

Snapshot/EnsureReady 总是先选择 committed head,再 install 到本地并标记 `(label, Rev)` ready。
若本地 recordSet 视图已经 ready,调用方可使用 `ReadOptions`:

| 选项 | 行为 |
|---|---|
| `ReadDefault` | quorum 读 |
| `ReadDefault + MinRev` | 本地 ready view 的 Rev 满足时直接读本地,否则回退 quorum |
| `ReadLocal` | 只读本地 ready view;未 ready 返回 local-view-behind |
| `ReadLocal + MinRev` | 本地 ready view 的 Rev 不足时返回 local-view-behind |

本地完整视图只由 `EnsureReady` / `Snapshot` / `Watch` 建立。普通 accept 只保存 pending accepted;
read-repair 可安装单条 committed record,但不会把该 recordSet 标记为完整 ready。

#### 4.4.5 WATCH 正确性

WATCH 只基于 committed records:

- accept pending 不入 watch log,也不会唤醒订阅者。
- 连续单条 commit 以 put/delete delta 追加 watch log。
- install snapshot 若不是本地 `rev+1` 的单条提交,会 reset 订阅者,要求消费者重新接收完整 snapshot。
- token 包含 epoch、membership label 和 recordSet Rev。epoch/label 不匹配或 log 被压缩时,消费者必须
  重新订阅 reset。

因此 WATCH 是 committed view 的增量缓存,不是复制协议本身。复制和修复仍由 quorum read/CAS/install 保证。

#### 4.4.6 效率边界

当前实现优先优化“海量 shard、每个 recordSet 小到中等规模”的场景:

| 操作 | RPC 轮次 | 载荷 | 说明 |
|---|---:|---|---|
| 点读命中 quorum-visible record | 1 | O(1) record | 热路径;可顺带 repair laggard |
| 点读全 quorum miss | 1 | O(1) record | 没有 ReadSet 成员返回该 key 时直接 not found |
| 点读冲突/落后 | read + snapshot/install | O(recordSet) snapshot | 用于确认 committed head |
| CAS | prepare + snapshot + accept + install | snapshot/install 为 O(recordSet) | 冷路径;并行打 owner set |
| Snapshot/Watch 初始 | snapshot + install | O(recordSet) | 建立本地完整 ready view |
| WATCH delta | 0 额外 RPC | O(1) event | 仅本地 committed install 后广播 |

`N=3` 时稳定写通常是 4 个并行 RPC round。Reserve/create/build/import lease 都是冷路径,相对沙箱启动和构建
耗时可接受;router 数据面热路径不写 shardkv。成本主要随单个 recordSet 的记录数增长,不随全局 group/node
数量增长,因为 registry 不跨 shard 扫描。

设计约束:

- recordSet 不应承载无界大表。group 下 sandbox/build、node 下 sandbox/build/key、node_list 低频目录都应
  保持可分页/可淘汰/可按事实源重投影。
- 如果未来某 recordSet 需要高频大表写,应把提交证书改成基于前一 digest 的 delta certificate,或拆分
  recordSet;不能继续依赖 full snapshot install。
- member readiness 只做 fail-fast 和 liveness,不改变 quorum 计算;owner count=3 时运行期只承诺逻辑分片视角
  的单成员故障容忍。

### 4.5 WATCH

WATCH 是 recordSet 层能力。初始帧是 reset/snapshot,随后是 delta,最后 bookmark 表示初始视图完整。

```text
watch(from token)
  -> reset(records, rev, token)
  -> bookmark(rev, token)
  -> put/delete(..., rev, token)
```

watch token 编码本地 epoch、shard view label 和 Rev。epoch/label 不匹配或 changelog 已压缩时,消费者必须
重新订阅并获取 reset + full snapshot。

route owner 内部也使用 shardkv watch log 唤醒本进程 waiter,但这不是 router watch。router 不订阅 route。

### 4.6 GC

GC 要解决的是本地存储和 WATCH reset snapshot 膨胀,不是数据迁移。它必须保持两个边界:

- 不跨 shard 枚举。
- 不改变 recordSet committed history。

tombstone 是 delete 后的保留记录,用于短期 `GetRecord`、WATCH reset 和调试;逻辑读写只关心“该 key 当前是否
存在”。因此 tombstone 保留期到期后可以只在本地删除。提交证书的 canonical digest 排除 deleted records,
所以同一 `Rev` 下“仍保留 tombstone”和“已压缩 tombstone”的 owner 是等价 committed snapshot,不会产生
同 Rev 冲突。

compactor 的执行规则:

- tombstone 保留期到期且 owner 成员 ready 时,删除本地 tombstone。
- compact 前先通过 recordSet 的 committed-head 选择修复本地视图,避免在落后副本上回收。
- 非 pinned namespace 的空闲本地 shard 到期后释放。

GC 不触发数据迁移。活跃记录仍由对应 namespace 的事实源和同 shard 读写触达。

## 5. Membership 变更

对同一个 shard key:

```text
oldOwners   = LocateN(shardKey, V1)
newOwners   = LocateN(shardKey, V2)
jointOwners = oldOwners ∪ newOwners

write/read quorum = quorum(oldOwners) + quorum(newOwners)
repair            = best-effort to jointOwners
```

示例:

```text
V1 owners for group G: A,B,C
V2 owners for group G: B,D,E

joint write sets: A,B,C + B,D,E
commit requires: quorum(A,B,C) + quorum(B,D,E)
```

普通并集 majority 不安全,因为它可能读不到只落在旧 quorum 的已提交值。重叠成员可同时计入 old quorum
和 new quorum。

membership 切换不是全局迁移任务。registry 不扫描所有 group/node。数据通过以下事实源自然进入新 owner:

- 活动 node 连接、心跳、sandbox/build 事件持续写入当前 node_link owner set。
- node owner 按当前 membership 持续把低频 profile 投影到 node_list。
- group 请求、node 上报、group-scoped list/export 触达对应 route_link recordSet 时做 catch-up/read-repair。
- scaler import/source lease、cursor、selector patch 通过 `scale_link` 当前 owner set 维护。

首次触达旧 recordSet 时,旧 membership certificate 可证明旧 head,随后由同一次读写 repair 到当前 `WriteSets`。
因此 joint/cutover 变更不需要跨 shard 扫描。cutover 后的 `old_grace` 继续作为 `ReadSets` 的 read-only 来源,
保证冷 recordSet 不会被 active 空副本误判为不存在。但 cutover 必须是受控动作:在 active/next joint 期间,
所有新写都必须使用 joint view;不能让部分成员提前只按 next view 写,否则不同 membership quorum 之间不再有协议保证。

## 6. node_link

### 6.1 接入、redirect 与 relay

node 可以连接任意 registry 成员。接入成员先按 `LocateN(node_id,N)` 找到 node owner set。

```text
node X connects registry A

hash(node X) -> node owners = B,C,D

case 1: A ∈ owners
node X ─────► A
              │ subscribe node stream
              ├── replicate profile/sandbox/build/key ─► C
              └── replicate profile/sandbox/build/key ─► D

case 2: A ∉ owners, node supports redirect
node X ─────► A
              │ hello{redirect:[B,C,D]}
              ▼
node X reconnects B/C/D

case 3: A ∉ owners, no redirect target
node X ─────► A ── relay ──► first successful owner in B,C,D
```

relay 必须按 owner 顺序逐个尝试,首个成功响应者承接订阅。node profile 记录携带 `link_owner`,表示实际持有
h2 stream 的 registry 成员。route owner 下发 create/connect/delete/build/key 命令时,经 node-owner RPC
转发到 `link_owner`。

`old_grace` 阶段旧成员不进入 owner set,但可继续作为 `link_owner` 接收转发命令;node 断开后重连时按当前
membership 重新解析 owner。

### 6.2 node 记录

node_link 维护以下 recordSet:

- `profile`:node_id、labels、runtime_digest、data_endpoint、build_capacity、draining、liveness、link_owner。
- `sandbox`:该 node 上 sandbox 的 group/route_key/sandbox_id/state。
- `build`:该 node 上 build 的 group/build_id/state。
- `manifest_key`:selector patch 刷新的 key cache。

心跳只更新 `profile` recordSet 中的 runtime/liveness 字段,不得重写 `sandbox`、`build`、`manifest_key`
recordSet。后三个 recordSet 只能由对应事实事件或 selector patch 更新,避免高频心跳把无关 recordSet 的 CAS
队列拖慢。

node_link 流按事件重要性处理:

- `upsert/delete` route event、`cmd_ack` 和 build event 是收敛关键事件,必须在读循环中立即处理。
- heartbeat 是最新值语义。registry 读循环只把最新 heartbeat 投递给每 node 一个异步合并 updater;updater 慢时
  旧 heartbeat 可被覆盖。
- node 侧发送也分优先级:command ack/build event 先进 high-priority outbox;heartbeat 只保留最新一条。
- `StreamAuthority` 在写侧优先刷新 route event,避免 route READY/DEAD 排在心跳后面。

这样 Reserve 的 READY route report 不会被心跳持久化阻塞。若 READY 晚于 park timeout 到达,route owner 会按
当前 `(group,route_key,sandbox_id)` 判定为 orphan 并删除 node 上孤儿 sandbox;但这应是异常退避路径,不是常态。

高频水位不通过 node_list 高频扇出。低频 liveness / draining / profile 变化才投影到 node_list。
node_link profile 写入失败会拒绝订阅;node_list 投影失败不应断开 node_link,后续 register/heartbeat/resync 会再次
投影。

### 6.3 增量订阅

node owner 发起订阅时可传 opaque rev 字符串。推荐编码 `source_fingerprint:seq`,由 node 私有解析。
fingerprint 匹配且 changelog 可用时 replay 增量;否则全量 resync。node owner 还应以 1h-6h 随机打散周期
做全量 resync。

全量 bookmark 表示本轮同步结束。node owner 可用 `(group,route_key,sandbox_id)` 精确比对缺失 sandbox,
并让 route owner 判定孤儿清理。增量 replay 的 bookmark 只推进 resume token,不做缺失清理。

## 7. node_list

node_list 是固定 shard key 的特殊 namespace:

```text
namespace = node_list
shardKey  = node_list
recordSet = nodes
recordKey = node_id
```

```text
node_link owner
  │ profile/liveness/draining low-frequency projection
  ▼
node_list owner set: LocateN("node_list", M)
  ┌─────────────┬─────────────┬─────────────┐
  ▼             ▼             ▼
 owner A       owner B       owner C
 full view     full view     full view
  ▲
  │ WATCH_LIST
  ▼
scaler consumes one owner at a time
```

node_list owner 分片内全复制,所以 scaler 不需要也不能把多个 owner 的结果做片间合并。若当前 owner 断线,
scaler 清空该源视图并切换到另一个 owner 重新 reset + bookmark。

node_list 未 ready 时,只能从同一 node_list owner set 做 list + repair。不能跨 node shard 扫描,也不能从
node_link 重建第二条事实传播路径。

## 8. route_link

### 8.1 身份

- 稳定会话身份:`(group, route_key)`。
- 当前运行实例:`sandbox_id`。它是不透明字符串,可包含保存/恢复代际,外部不解析。
- cluster 内部总是同时维护 `group`、`route_key`、`sandbox_id`。

### 8.2 route 记录

| 字段 | 说明 |
|---|---|
| `group` | 分片键 |
| `route_key` | 稳定会话键 |
| `sandbox_id` | 当前实例 |
| `state` | `reserved` / `ready` / `paused` / `dead` |
| `node_id` | 当前承载节点 |
| `access_token` | 当前实例数据面 token |
| `updated_at` | timeout/reconcile 使用 |

状态机:

```text
none -> reserved -> ready
ready -> paused -> reserved -> ready
ready/paused/reserved -> dead/tombstone
```

整机清空、单沙箱 killed、node 重启后的缺失 sandbox 都收敛为 dead route 清理;下次 Reserve 重新放置。

### 8.3 Reserve

```text
router
  │ ReserveSandbox(group, route_key)
  ▼
route_link owner
  │ existing ready? return
  │ none/paused? CAS reserved
  ▼
scaler PlaceSandbox
  │ choose node + access_token
  ▼
node owner
  │ admit + create/connect
  ▼
node reports RUNNING/READY
  │
  ▼
route_link CAS ready
  │
  ▼
Reserve returns READY
```

`ReserveSandbox` 返回时必须 READY 或失败。若已有 `reserved`,新请求 join 同一 in-flight 状态机。
node READY 事件到达后,route owner 通过本地 group WATCH 或短周期 quorum read 唤醒 waiter。router 不订阅
route_link 更新。

孤儿清理由 route owner 判定:node 上报的 `(group,route_key,sandbox_id)` 在 route_link 中不存在或已被替换,
则下发 delete/kill 到该 node。该过程不经过数据面,也不依赖 access token。

## 9. scale_link 与 scaler

### 9.1 scaler 发现

```text
scaler S1
  │ POST /scale-link/register {id, advertise, memberlist_label}
  ▼
registry observer joins scaler.default memberlist
  │
  ▼
ready scaler view from memberlist meta
```

registry 对 group 做确定性 failover:

```text
readyScalers = scaler memberlist nodes where role=scaler and alive and ready=true
               and ready_label == active_registry_label
candidates   = LocateN(group, readyScalers, scale_link.scaler_replica_count)
try candidates in order until success
```

registry 不对 scaler 做 P2C。P2C 属于 scaler 内部从 node 候选中选择目标。

### 9.2 import/source

```text
source_id = file-prod-a

ready scalers ── LocateN(source_id, readyScalers, import_source_owner_count)
       │
       ▼
candidates race CAS lease:
  namespace = scale_link
  shardKey  = import/source/file-prod-a
  recordSet = import
  recordKey = state

lease winner
  │ Range(cursor, limit)
  │ GetPlacementHint/GetKey/GetAuthKey(group)
  │ selector patch with lease fencing
  │ cursor checkpoint after page success
  ▼
node_link manifest_key cache refreshed
```

`source_id` 是 importer 的唯一执行单元。多个 scaler 配置相同 `source_id` 时,它们竞争同一条
`scale_link` source execution record。Provider 的点查可以跨多个 source 去重;Importer 的 Range 不把多个
source 合并成一个视图。

### 9.3 group provider 边界

registry 不实现 `SandboxGroupProvider` / `SandboxGroupImporter`。group 配置、placement hint、auth_key、
manifest_key 属于 scaler/provider。registry 只保存执行态和 node key cache。

group 从 provider 消失后,新的 Place/verify-key 按 group 不存在处理。已经进入 node_link 的 manifest key
cache 不主动删除,由 registry/node 侧 TTL 淘汰。

## 10. router

router 是无状态北向入口,但持本地缓存:

- route resolution cache:`(group, route_key, sandbox_id)` -> node endpoint / access token / route Rev。
- active connection cache:同一路由已有活动 HTTP/CONNECT/WebSocket 时,新请求不调用 Reserve。
- singleflight:同一 `(group, route_key)` 并发 miss 只发起一次 Reserve。

```text
request(group, route_key, sandbox_id)
  │
  ├─ active connection cache hit ──► node proxy
  │
  ├─ route cache hit ──────────────► node proxy
  │
  └─ miss/fail-fast ───────────────► route owner Reserve/Resolve
                                      │
                                      ▼
                                    node proxy
```

所有请求必须带 group。router 通过 bootstrap 拉取 `/cluster/membership`,再按 active membership 定位
route owner。membership refresh 会尝试 bootstrap 和已知 active/next/old_grace 成员,选择 active version
最新的结果。

router 调 route owner 的 verify-key;route owner 只 failover 到 ready scaler 校验。router 和 registry 都不
读取 `manifest_key`。

## 11. 密钥与鉴权

group 有两个密钥域:

| 名称 | 持有者 | 用途 |
|---|---|---|
| `auth_key` | provider/scaler;router 通过 verify-key 间接使用 | 验 API key,派生 access token |
| `manifest_key` | provider/scaler/node | 解密镜像/快照内容;router 不接触 |

数据面 token:

```text
access_token = MAC(auth_key, sandbox_id)
```

scaler 在 Place 时生成当前 sandbox_id 的 token,registry 保存到 route_link。READY/ResolveSID 只读 route_link,
不回查 provider。

`manifest_key` 和 `registry_auth` 都是 typed secret,支持 inline 或 ref 带外交付。密钥分发只发生在
shuffle-sharding/import/selector patch 路径:

```text
scaler selector patch
  │ target node set + manifest_key material/ref
  ▼
registry/node owner
  │ CAS node_link manifest_key cache
  ▼
node_link heartbeat refresh
  │ key_put only when missing or near lease expiry
  ▼
node encrypted local key store
```

`key_drop` 不是正确性依赖。节点侧 key 租约按 TTL 淘汰未续租条目。密钥分发是 create/build 前置条件,
不影响已经运行的 sandbox。

## 12. Build

Build 记录按 group 存在 `route_link` 的 `build` recordSet;执行态和实时预算归 node owner。

```text
router build register
  │ stable build_id/template_id
  ▼
route owner ReserveBuild
  │ PlaceBuild
  ▼
scaler suggests node
  │
  ▼
node owner AdmitBuild(build_id, resources, ttl)
  │
  ▼
route_link build record CAS
  │
  ▼
node_link build_register command
  │
  ▼
node build_event releases/adapts state
```

node owner 若资源余量不足直接拒绝,route owner 重新调度。`build_id` 查询必须带 group。

## 13. 导入导出

registry export/import 覆盖 registry 执行态灾备数据:

- `kind=route_link`:导出指定 group 下的 `route_link/sandbox` route records 和 `route_link/build` build execution records

JSONL 行使用显式类型:

```json
{"type":"route","route":{}}
{"type":"build","build":{}}
```

registry export/import 不覆盖 sandbox-group provider 数据。group 配置、placement hint、auth_key、
manifest_key 由 scaler/provider 侧负责导入导出。

整套 registry 完全下电后不要求自动恢复运行中 sandbox。灾难场景可通过外部持久化的 export/import 做手动恢复。

## 14. 可靠性

| 事件 | 行为 |
|---|---|
| router 崩溃 | 丢本地缓存;重启后 miss 重新 Reserve |
| scaler 崩溃 | registry 对同 group failover 到下一个 ready scaler;热路径不受影响 |
| node_link 断线 | node owner 保留最后视图,node_dead_after 后 node_list 失效;node 重连后全量/增量重报 |
| node 整机重启 | node 清空运行态;缺失 sandbox 经 node 上报/清理收敛为 dead route |
| registry 单成员故障 | owner set quorum 足够时继续服务;恢复后由同 shard 访问或事实源上报触发 read-repair |
| registry 多成员故障导致 quorum 不足 | 对应 shard 停写,不降级乱写 |
| membership 变更 | active/next joint 写 quorum + old_grace read-only 证书/快照来源 |
| 整集群下电 | 不自动恢复运行中 sandbox;可手动导入外部持久化执行态 |

## 15. 性能

- 数据面热路径:router active cache / route cache 命中后直转 node,不访问 registry。
- Place 冷路径:group 经 route_link owner -> ready scaler failover -> node owner admission。
- 海量 group:router 不订阅 group;registry 不跨 group 扫描;scaler import 按 source_id 独立分页。
- 海量 node:node_link 按 node_id 分片;node_list 只承载低频目录,不承载高频水位。
- membership 变更:不做全局 promoter;由 node 上报、group 请求、source import、read-repair 自然收敛。

## 16. 集群 stub e2e

`node-stub-ctl` 是本仓 cluster e2e 节点桩。它使用真实 node_link 协议接入 registry,一个进程可模拟多个
node,但不启动 microVM。除 microVM/应用进程外,它模拟节点控制面行为:

- 注册 node、心跳、drain、水位和 build 预算。
- 接收 `key_put/key_drop/create/connect/delete/build_register` 命令并返回 ack。
- 按 sandbox 行为配置发布 READY/dead route event。
- 发布 build event。
- 提供 admin/data API 查询节点、sandbox、build、key、command 和 data hit。
- 支持 `restart-link`、`reboot-empty`、`crash/start` 等节点动作。

`make test-e2e` 先 `make build`,再用产物真实启动 `cluster-ctl registry/router/scaler` 与 `node-stub-ctl`。
`test/e2e/e2e_cluster_stub.sh` 覆盖 N=1 registry、多 registry、membership joint/old_grace cutover、group
import、key 分发、Reserve、数据面转发、active cache、BuildRegister、孤儿 route 清理和节点清空收敛。

## 17. See Also

- [cluster-router.md](cluster-router.md) — router 入口、活动连接缓存和数据面转发。
- [cluster-scaler.md](cluster-scaler.md) — group provider/importer、WATCH_LIST、Place 与 key distribution。
- [node.md](node.md) — node-ctl 单机主机与 node-link 节点侧行为。
- [node-proxy.md](node-proxy.md) — node 数据面 proxy、routesync 与 CONNECT。
- `kuasar-sandbox/docs/deployment.md` — 部署拓扑、端口、启停与故障域。
