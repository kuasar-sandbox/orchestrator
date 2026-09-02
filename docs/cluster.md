# cluster — registry 自聚簇、路由与放置控制面

`cluster-ctl` 是大规模部署的集群控制面,由三个独立角色组成:

- `registry`:有状态可靠集群,维护 node / route / placer import 等执行态。
- `router`:e2b 兼容统一入口,按 sandbox-group 定位 route owner,热路径直转 node。
- `placer`:group provider/importer 与放置调度器,消费 `node_list`,向 registry 提供 Place / verify-key。

Registry 的基础能力是按 `namespace + shard key + recordSet + record key` 组织的一致性 KV。所有
`node_link`、`route_link`、`node_list`、`placer_link` 记录都复用这套模型:分片间不遍历,分片内全复制,
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
│ node-ctl   │ node_link│      registry        │ placer_link│   placer   │
│ serve      │◄────────►│   state cluster      │◄────────►│ placement  │
│ sandbox VM │          │ shardkv namespaces   │          │ importer   │
└────────────┘          └──────────────────────┘          └────────────┘
                               ▲
                               │ node_list WATCH_LIST
                               └────────────── placer consumes one owner
```

稳态数据面不经过 registry.只有显式 create/connect/exec-session,以及数据面 target 缺失或
typed stale fallback 路径需要 registry:

- `POST /route-link/reserve` 以 `operation=create|connect|exec-session|data` 区分四种操作.create 直接
  Reserve;connect/exec-session 由 registry 经 node-link 完成;data 只在 route 缺少完整 node target 或
  node proxy 返回 typed stale 时 Reserve。未知 route 不会隐式创建 sandbox。显式 build register 调用
  `ReserveBuild`。
- registry 调 placer `PlaceSandbox` / `PlaceBuild`。
- registry 经 node owner 下发 create/connect/exec_session/delete/build/key 命令.
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
7. **placer 不拥有生命周期**:placer 只做 group 导入、selector patch、shuffle-sharding、P2C 与 Place
   建议。最终资源确认在 node owner admission。
8. **router 不订阅海量 group**:create/data Reserve 返回 READY 或失败;connect/exec-session
   Reserve 在 node 同步准备完成后返回,不等待异步 resume.router 只维护有界 route cache;
   在途请求不作为新请求的路由来源.

### 1.3 角色边界

| 角色 | 职责 |
|---|---|
| registry member | 组成 registry 自聚簇,承载 `route_link` / `node_link` / `node_list` / `placer_link` 执行态和 membership |
| router | e2b 统一入口;按 group 定位 route owner;cache miss 时 Resolve,按 create/connect/exec-session/data 调用 Reserve;热路径使用本地 route cache |
| placer | 消费 `node_list` WATCH_LIST;通过 provider/importer 导入 group;维护 placement 与 selector patch;提供 Place / verify-key |
| node | 运行 sandbox/build;通过 node_link 上报全量清单和事件;接收 create/connect/exec_session/delete/build/key 命令 |

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
    placer_link: 3
    node_list: 3

node_link:
  # listen: ""                 # 空 = 复用 member.listen;非空 = 独立 node 长连接监听
  heartbeat_interval: 10s
  node_dead_after: 30s          # node-link 断线后清理持久状态的等待时间

route_link:
  park_timeout: 30s

node_list:
  watch_retention: 10000

placer_link:
  placer_label: placer.default
  placer_replica_count: 3      # registry 调 placer 的 failover 候选数
  min_ready_placers: 1
  place_timeout: 2s
```

默认 path:

```text
/cluster/membership
/node-link/*
/route-link/*
/placer-link/*
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

- `active`:客户端定位 route/node/node_list/placer_link owner 的版本。
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
- 发布 registry/placer 的 ready meta。
- 给 RPC fail-fast / cooldown 提供信号。

memberlist 不用于:

- 维护 registry 成员清单。
- 改变 `LocateN` 输入。
- 做数据复制。
- 把 suspect/dead 转化为 reshard。

### 3.3 placer memberlist 域

placer 使用独立 label,默认 `placer.default`。registry 不配置 placer 列表,而是作为
`role=observer` 加入 placer memberlist。`POST /placer-link/register` 只提供 seed,用于 registry 初始 join。
ready placer 由 placer memberlist meta 表达:

```json
{"role":"placer","id":"s1","advertise":"https://s1:7800","ready":true,"ready_label":"registry.2.hash"}
```

registry 只把 `role=placer && alive && ready=true && ready_label==active_registry_label` 的成员作为
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

`membership.owners.route_link/node_link/placer_link/node_list` 分别控制各 namespace 的 owner 数量。
owner count 是 registry 内部复制因子。`placer_link.placer_replica_count` 只控制 registry 调 ready placer
的 failover 候选数,不是 `placer_link` namespace 的 owner count。

### 4.3 namespace schema

| namespace | shard key | recordSet | record key | 内容 |
|---|---|---|---|---|
| `route_link` | group | `sandbox` | route_key | route 记录 |
| `route_link` | group | `build` | build_id | build 执行态 |
| `node_link` | node_id | `profile` | `profile` | node profile、labels、liveness、link_owner、低频容量 |
| `node_link` | node_id | `sandbox` | node_sandbox_id | node 维度 sandbox 归属表,值含 sandbox_id + sandbox_generation + group + route_key + profile + api_secret_fingerprint |
| `node_link` | node_id | `build` | build_id | node 维度 build 归属表,值含 group |
| `node_link` | node_id | `key_pair` | api_secret_fingerprint | node APISecret+ManifestKey pair cache |
| `node_list` | `node_list` | `nodes` | node_id | 低频节点目录和 WATCH_LIST |
| `placer_link` | `import/source/<source_id>` | `import` | `state` | import source lease/cursor |

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
- placer import/source lease、cursor、selector patch 通过 `placer_link` 当前 owner set 维护。

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

- `profile`:node_id,labels,runtime_digest,api_endpoint,data_endpoint,Build registration/execution capacity 与 durable usage,draining,liveness,link_owner;heartbeat 的 `allocated` memory 是本节点全部 sandbox 的 NodeReservation 之和,`pool` 是 node allocatable pool,不是 host `memory.current`,VMM charge 或 guest demand;其中低频 `node_list` 投影包含两个 endpoint 与 capacity,usage 保留在 node owner 的实时 profile 中.
- `sandbox`:该 node 上 sandbox 的
  `node_sandbox_id -> {sandbox_id,sandbox_generation,group,route_key,profile,api_secret_fingerprint}`
  完整归属表。
- `build`:该 node 上 build 的 `build_id -> group` 完整归属表。
- `key_pair`:selector patch 刷新的 APISecret+ManifestKey pair cache。

心跳只更新 `profile` recordSet 中的 runtime/liveness 字段,不得重写 `sandbox`、`build`、`key_pair`
recordSet。sandbox/build 表由 cluster 在任务下发前写入。build 终态只释放容量,归属记录保留到对应
build record 删除;key_pair 由 selector patch 更新。
node 不生成 group/route-key,但会校验并独立持久化 node-link 下发的 sandbox system context;
build 的 cluster group 是节点 Build 行的独立系统字段,不进入 portable metadata。这样高频心跳不会把无关 recordSet 的 CAS 队列拖慢。

同一 node 内 `node_sandbox_id` 归属以 CAS 写入:相同完整归属重放为幂等刷新,不同归属返回
冲突且不得覆盖旧值。create 在下发 node 命令前遇到该冲突时,仅回滚本次 RESERVED
record,保持稳定 `sandbox_id`,消费下一个 `sandbox_generation` 并生成新 `node_sandbox_id`
后重试。node 端在异步 launch 前同步拒绝已有或正在创建的 node-local ID。

node_link 流按事件重要性处理:

- `upsert/delete` route event、`cmd_ack` 和 build event 是收敛关键事件,必须在读循环中立即处理。
- heartbeat 是最新值语义。registry 读循环只把最新 heartbeat 投递给每 node 一个异步合并 updater;updater 慢时
  旧 heartbeat 可被覆盖。
- node 侧发送也分优先级:command ack/build event 先进 high-priority outbox;heartbeat 只保留最新一条。
- `StreamAuthority` 在写侧优先刷新 route event,避免 route READY/DEAD 排在心跳后面。

这样 Reserve 的 READY route report 不会被心跳持久化阻塞。node-local `starting` upsert
只为节点 proxy/MMDS 暴露 launch 身份:node_link owner 在全量同步时将其计入 seen set,
但不把它增加为 route_link 业务状态,也不覆盖既有 RESERVED/PAUSED;后续 READY/PAUSED/Delete
才推进 route_link。create 候选失败的 Delete 若仍命中当前 in-flight RESERVED fence,
Registry 通过该 fence 恢复 Reserve 前 route(全新 create 则删除 reservation),不能先删
RESERVED 行使旧 route 失去回滚锚点。sandbox 事件携带 NodeSandboxID、profile
与 node-owned 执行态;nodelink owner 以 `(node_id,NodeSandboxID)` 查本节点归属表得到稳定
SandboxID、SandboxGeneration 和 group/route_key,再更新 route_link。若 READY 晚于
park timeout 到达,归属表已删除,该事件被判定为 orphan 并触发 node 上孤儿 sandbox 清理。

node 的 CmdCreate `cmd_ack` 只在其已 claim 唯一 launch attempt、insert durable
`starting,run_id=""` 并 cache/publish starting 后返回;Ack 是 node-local launch acceptance,
不是 READY。后续资源准备/runner/runtime failure 以 matching Delete 驱动上述 reservation
rollback。CmdConnect Ack 前则完成 paused→starting、清空旧 run/network ownership、提交 deadline
并发布 starting;其 restore failure 发布 paused Upsert,不得进入 fresh-create Delete 分支。

高频水位和 liveness 不投影到 node_list。node_list 只承载注册时的 labels/capacity/endpoint/runtime 等目录字段
以及 draining 变化。node owner 持有的当前 node-link 连接是唯一存活权威；route owner 在 create/build 提交前
验证连接，失败候选加入本次 placement 的排除集合并重选。node_link profile 写入失败会拒绝订阅；node_list
投影失败不应断开 node_link，后续 register/heartbeat/resync 会重试待完成的目录投影。

### 6.3 增量订阅

node owner 发起订阅时可传 opaque rev 字符串。推荐编码 `source_fingerprint:seq`,由 node 私有解析。
fingerprint 匹配且 changelog 可用时 replay 增量;否则全量 resync。node owner 还应以 1h-6h 随机打散周期
做全量 resync。

全量订阅开始前,nodelink owner 捕获本节点 sandbox 归属表基线。bookmark 表示本轮同步结束时,只清理
基线中未按 node_sandbox_id 出现的条目;清理前再次读取并确认当前完整稳定/节点代际归属
仍等于基线,从而保护
同步期间新下发或重新绑定的任务。增量 replay 的 bookmark 只推进 resume token,不做缺失清理。

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
placer consumes one owner at a time
```

node_list owner 分片内全复制,所以 placer 不需要也不能把多个 owner 的结果做片间合并。若当前 owner 断线,
placer 清空该源视图并切换到另一个 owner 重新 reset + bookmark。

node_list 未 ready 时,只能从同一 node_list owner set 做 list + repair。不能跨 node shard 扫描,也不能从
node_link 重建第二条事实传播路径。

## 8. route_link

### 8.1 身份

- route 定位键:`(group, route_key)`。
- 稳定公开身份:`sandbox_id`。Registry 在首次 create 时生成,同节点 resume、跨节点迁移和
  re-place 均不改变;公开 API、Host 和 router cache key 使用该 ID。
- 稳定 sandbox 身份:`stable_id`。cluster invariant 固定为 `stable_id == sandbox_id`；它在
  NodeSandboxID 变化时保持不变，并绑定 ServiceSecret 与 KAT `sid`。该重复投影为现有受保护
  credential state，本次不去重。StableID 不是 node-local lookup key，也不增加唯一索引；
  身份保持型 migration/copy 可以让多个 node-local sandbox 共享同一 StableID。
- Registry durable JSON state 直接使用 `stable_id`，不保留旧字段 fallback；pre-release
  部署必须清理并重建旧 preview state。
- 节点执行身份:`node_sandbox_id = <sandbox_id>-g<sandbox_generation>`。首个候选为 g0;候选
  冲突/失败或跨节点迁移消费下一个 generation,同节点 resume 保持当前 NodeSandboxID。
  NodeSandboxID 是不透明的 node-local ID,权威映射在 Registry 归属表,组件不从字符串反向解析。
- node 事件不携带 group/route_key/SandboxGeneration。Registry 以 `(node_id,node_sandbox_id)` 查归属表
  恢复稳定身份和 group 上下文;不存在跨 group 的 SandboxID 索引。

### 8.2 route 记录

| 字段 | 说明 |
|---|---|
| `group` | 分片键 |
| `route_key` | group 内 route 定位键 |
| `sandbox_id` | 稳定公开 SandboxID |
| `node_sandbox_id` | 当前 node-local 执行 ID |
| `sandbox_generation` | 当前 NodeSandboxID 的 Registry-owned 代际 |
| `next_sandbox_generation` | 下一可分配代际;只由 Registry 持久化,不下发 node |
| `state` | `reserved` / `ready` / `paused` / `dead` |
| `node_id` | 当前承载节点 |
| `profile` | 创建意图确定的 sandbox profile,与 node 归属及事件事实一致 |
| `api_secret_fingerprint` | sandbox 业务记录绑定的完整 APISecret 指纹;生命周期命令和归属清理据此防止跨 binding 操作 |
| `manifest_key_fingerprint` | 与 APISecret 配对的 ManifestKey 完整指纹;route 不保存或投影 ManifestKey 原文 |
| `stable_id` | 跨 NodeSandboxID 变化保持的 sandbox identity；cluster 中等于 `sandbox_id`，并绑定 ServiceSecret/KAT |
| `api_secret` | 当前 sandbox 已绑定的 APISecret;仅存在于受保护 route 存储和可信 router/proxy 投影 |
| `service_secret` | 当前 sandbox 持久化的 service credential;用于签发和验证用途明确的 KAT token |
| `envd_access_token` | e2b envd 端口使用的数据面 token |
| `traffic_access_token` | 外部网关及 e2b 数据面组件使用的 token,cluster 平台层不消费 |
| `forward_access_token` | bare/e2b 的其他 forward 目标使用的数据面 token |
| `target_port` | group/provider 返回的强制数据面端口;为 0 时请求必须显式携带端口 |
| `updated_at` | timeout/reconcile 使用 |

状态机:

```text
none -> reserved -> ready
ready -> paused -> reserved -> ready
ready/paused/reserved -> dead/tombstone
```

整机清空、单沙箱 killed、node 重启后的缺失 sandbox 都收敛为 dead route 清理;下次显式 create/恢复
Reserve 才重新放置。

### 8.3 Reserve

```text
POST /route-link/reserve
  ?operation=create|connect|exec-session|data
  &group=<group>&route_key=<route-key>
  [&sid=<stable-sandbox-id>][&port=<effective-port>]
  [&timeout=<seconds>]
```

Reserve body 按 operation 使用独立 typed schema:create 携 create config,exec-session 携
`{"ttl_seconds":N,"conditions":["..."]}`,connect/data body 为空.四种 operation 的凭据和
完成条件不同;Registry 对 exec-session body 再做严格 schema/bounds 校验,不接受旧的
`ttl_seconds` query 或把 conditions 塞入 Header/metadata/config map:

- `create`:query 只携 group/route_key,Header 携 `X-API-KEY`,body 只允许 restore/credentials
  config。Registry 在 placement 和 route 写入前通过 group provider 验证 API key,生成稳定
  SandboxID 和首个 NodeSandboxID,下发 CmdCreate。node Ack 只表示 durable starting + active
  attempt;Registry 仍等待 node READY 事件后才向北向 create 返回 `Route`。并发 create 在
  Registry 内合并。
- `connect`:query 必须携期望的稳定 `sid`,可选 `timeout`;Header 携 `X-API-KEY`,可选
  `X-Kuasar-Migration-Token`。Registry 使用 route 业务记录已绑定的 APISecret 验证 API key,
  对精确 NodeSandboxID 下发 CmdConnect。目标节点不可用且已提供 migration token 时,Registry
  排除原节点、分配新 generation 并向新节点下发 CmdConnect。node 同步完成校验、可选
  import、paused→starting、旧 run/network ownership 清理、deadline 持久化和凭据读取,Ack
  返回 typed `ConnectResult`;Registry 校验其
  NodeSandboxID/TemplateID/Profile/三项公开 token 与 route 一致后返回 `Route + Connect`。
  resume 异步进行,connect 不等待 READY,也不在 Router 合并不同请求。
- `exec-session`:query 必须携期望的稳定 `sid`;typed body 携非负 int64 `ttl_seconds` 和
  已规范化的 CEL source string array;
  Header 携原始 `X-API-KEY` 和可选 `X-Kuasar-Migration-Token`.Registry 以 route 业务
  记录已绑定的 APISecret 验证 API key,并校验 expected stable SID,之后原始 key 在
  Registry verifier 终止,不进入 command,Ack,route,日志或错误文本.Registry 向
  当前/新候选 NodeSandboxID 下发 `CmdExecSession{APISecretFingerprint,Profile,
  TTLSeconds,ExecConditions,MigrationToken?,Cluster?}`.其它 command kind 携
  `ExecConditions` 时必须拒绝.Node 同步完成可选 import,对象/凭据/context 校验,权威 CEL
  编译和 KAT 签名,Ack 仅携 `ExecSessionResult{ExecAccessToken}`,然后按既有合同异步 resume.
  编译/mint 失败发生在任何 resume mutation 之前.Conditions 只在本次 public request、
  Reserve body、Command 和 token 中存在,不持久化到 Route、SandboxRecord、event 或 metadata,
  日志也不输出 source.
  MigrationToken 只在本次 Reserve/command wire 内存活,不写 route/SandboxRecord,不进入日志或
  错误文本,Node 在同步 import 消费后丢弃.
  Registry 验证 typed result 后重读当前 route,返回 `Route + ExecSession`,不等待 READY;
  Router 只向客户端投影 `execAccessToken`.已是 READY/RESERVED 的 route 不因 token 签发改写
  其 revision 或占用其它 workflow 的 rollback fence;PAUSED 才按现有激活语义进入
  RESERVED.每个 API 调用使用独立 CmdID 并签发新 KAT,不与其它 exec-session
  Reserve 合并.
- `data`:query 必须携期望的稳定 `sid`;legacy 目标可选携有效 `port`,Registry 会将它
  与 route `target_port` 合并为鉴权目标;exec 逻辑服务不要求 port.Header 携
  `X-Access-Token`,exec 另携
  `E2b-Sandbox-Service: exec`.Registry 对普通目标按 profile/端口选择 EnvdAccessToken
  或 ForwardAccessToken;exec 则以 `StableID + ServiceSecret` 验证 KAT.
  鉴权在任何 route CAS/CmdConnect 之前完成.READY 直接返回;
  PAUSED 先 CAS RESERVED 并下发 CmdConnect;RESERVED 等待当前稳定 lineage 的事件。只在获得
  READY 且 DataEndpoint 有效时返回 `Route`。不存在的 route 直接返回 not found,不创建
  sandbox。

`Route` 是受保护结果,同时携稳定 SandboxID,当前 NodeSandboxID,`APIEndpoint`,
`DataEndpoint` 和 `route_revision`.两个 endpoint 都来自按 NodeID 查询的当前 node runtime/profile,
不复制进 SandboxRecord;control/build 固定使用 APIEndpoint,data/exec 固定使用 DataEndpoint.
`route_revision` 取当前 group route recordSet 的已提交 revision,供 Router 拒绝迟到的旧节点
结果。Router 不订阅 route_link 更新。

CmdConnect/CmdExecSession 的 `CmdID` 只关联当前 Command 与 Ack waiter,不是持久幂等键.
Registry 不在 Ack 超时、链路中断或 node 重启后自动重投同一个 Command/`CmdID`;本次调用
返回临时失败,API 重试创建新的 operation 和 `CmdID`.Connect 依靠 target insert-only、对象
绑定校验和 node-local launch ownership 保持可重试;Exec Session 重试可以签发新的 KAT.系统不持久化
command digest、Ack/result 或临时去重状态.

孤儿清理由 nodelink owner 和 route owner 共同收敛:先以 `(node_id,node_sandbox_id)` 查归属表;表项不存在,
或表项指向的 `(group,route_key)` 已不存在/被其他实例替换,则下发 delete/kill 到该 node。该过程不经过
数据面,不依赖 access token 或全局 sandbox ID 查询。全量同步、孤儿清理和节点回收均比较
`(group,route_key,sandbox_id,node_sandbox_id,sandbox_generation,profile,api_secret_fingerprint)` 完整归属,
避免迟到事件跨代际或凭据 binding 删除新记录。

## 9. placer_link 与 placer

### 9.1 placer 发现

```text
placer S1
  │ POST /placer-link/register {id, advertise, memberlist_label}
  ▼
registry observer joins placer.default memberlist
  │
  ▼
ready placer view from memberlist meta
```

registry 对 group 做确定性 failover:

```text
readyPlacers = placer memberlist nodes where role=placer and alive and ready=true
               and ready_label == active_registry_label
candidates   = LocateN(group, readyPlacers, placer_link.placer_replica_count)
try candidates in order until success
```

registry 不对 placer 做 P2C。P2C 属于 placer 内部从 node 候选中选择目标。

### 9.2 import/source

```text
source_id = file-prod-a

ready placers ── LocateN(source_id, readyPlacers, import_source_owner_count)
       │
       ▼
candidates race CAS lease:
  namespace = placer_link
  shardKey  = import/source/file-prod-a
  recordSet = import
  recordKey = state

lease winner
  │ Range(cursor, limit)
  │ GetPlacementHint/GetKey/GetAPISecret(group)
  │ selector patch with lease fencing
  │ cursor checkpoint after page success
  ▼
node_link key_pair cache refreshed
```

`source_id` 是 importer 的唯一执行单元。多个 placer 配置相同 `source_id` 时,它们竞争同一条
`placer_link` source execution record。Provider 的点查可以跨多个 source 去重;Importer 的 Range 不把多个
source 合并成一个视图。

### 9.3 group provider 边界

registry 不实现 `SandboxGroupProvider` / `SandboxGroupImporter`。group 配置、placement hint、APISecret、
ManifestKey 属于 placer/provider。registry 只保存执行态和各 node 所需的凭据对 cache。

group 从 provider 消失后,新的 Place/verify-key 按 group 不存在处理。已经进入 node_link 的凭据对
cache 不主动删除,由 registry/node 侧 TTL 淘汰;已复制到现有 sandbox/build 记录的凭据对不受影响。

## 10. router

router 是无状态北向入口,但持本地缓存:

- route resolution cache:`(group, route_key, stable sandbox_id)` -> NodeSandboxID / APIEndpoint / DataEndpoint / profile /
  StableID / APISecret / 两项 root fingerprint / ServiceSecret /
  EnvdAccessToken / TrafficAccessToken / ForwardAccessToken / RouteRevision。
- build forwarding cache:`(group, build_id)` -> APIEndpoint;不能只以 build_id 为键.
- 在途请求只持有自身的 route 副本和计数,不作为新请求的路由 cache,也不阻止新
  RouteRevision 替换旧 NodeSandboxID。

```text
request(group, route_key, sandbox_id)
  │
  ├─ cache hit with node target ───► node proxy (ready/paused/starting)
  ├─ cache hit without target ─────► data Reserve ──► node proxy
  │
  └─ miss ─────────────────────────► route owner Resolve
                                      │
                                      ├─ complete target ───► node proxy
                                      └─ missing target ────► Reserve ──► node proxy
```

node proxy 在 CONNECT 握手返回 typed `not_found`/`unauthorized` 时,Router 淘汰旧 target,以同一
credential 调用一次 `ReserveData` 复验并刷新 route,然后只重试一次。普通连接失败只淘汰 cache。

未知 route 的数据面请求在 Resolve 后返回 not found,不会触发 sandbox 创建。
命中 route 后,Router 在 node 边界把公开 SandboxID 转换为 NodeSandboxID:控制面重写路径并只拨
APIEndpoint,数据面外层 CONNECT,Host 和已有 sandbox identity Header 使用 NodeSandboxID 并只拨
DataEndpoint.任一 endpoint 缺失都 fail closed,不跨平面回退.公开 create/connect/list/get
结果仍只呈现稳定 SandboxID。

Router 接收新 route 时,不允许更低 RouteRevision 覆盖 cache,也不允许相同 RouteRevision 以不同
NodeSandboxID 覆盖当前值。旧代际在途请求失败时,仅在 cache 仍指向该 NodeSandboxID 时才能
驱逐,避免删除已切换的新代际。

所有请求必须带 group。router 通过 bootstrap 拉取 `/cluster/membership`,再按 active membership 定位
route owner。membership refresh 会尝试 bootstrap 和已知 active/next/old_grace 成员,选择 active version
最新的结果。

create/connect/exec-session 由 Reserve 强制验证客户端原始 API key:create 的 group admission
经 ready placer 使用 provider APISecret,connect/exec-session 使用 Sandbox 业务记录已绑定的
APISecret.其它控制操作由 router 调 route owner 的 verify-key,route owner 只 failover 到
ready placer 验证.sandbox READY 后,node 把该业务记录已绑定的 APISecret,ServiceSecret 及用途
明确的 access tokens 投影到受保护 route,供可信 registry/router/proxy 使用;ManifestKey 原文
不进入该链路,也不用于 API 认证.

Cluster native exec 的控制面不把 public node API reverse-proxy 到当前 node:

```text
POST /sandboxes/<stableSID>/exec-sessions
  X-Kuasar-Sandbox-Group + X-Kuasar-Route-Key + X-API-KEY
  empty / {} / {"ttlSeconds":N,"conditions":[{"expr":"..."}]} + optional MigrationToken
    ↓ Router strict 64 KiB decode
Reserve(operation=exec-session, expected stableSID, typed TTL + conditions body)
    ↓ Registry verifies API key and sends CmdExecSession
Node validates/imports, compiles conditions, signs, accepts async resume
    ↓ Route + ExecSessionResult
201 Cache-Control:no-store {"execAccessToken":"kat1..."}
```

Router 不签发 token,不向 node-link 传原始 API key,也不对外返回 NodeSandboxID.
body 与 direct Node 共用同一严格解码合同:完整原始 body 上限 64 KiB,空 body 合法;
`conditions` 缺失和 `[]` 都规范化为 unrestricted nil,显式 `null`、unknown/duplicate 字段、
空 expr、负数、尾随第二个 JSON value 和越界 TTL 拒绝.
Registry/node 内部失败对外映射为固定,脱敏的 exec-session 错误,不包含 NodeSandboxID,
socket path,fingerprint,ServiceSecret 或 token payload.

数据面只在 CONNECT 中解析 `E2b-Sandbox-Service`.普通 HTTP 始终按 legacy
port 转发,应用层 service Header 原样保留.CONNECT 未携 service 时保持 raw port;
Node 负责以本地受信 profile 完成最终 backend 选择;特别地,bare 的 legacy
49983/49999 是普通 TCP forward,不返回 501.Cluster Router 当前
只对 `service=exec` 实现 service-aware 分支,不将其它三个显式 service 值描述为已支持;
完整的 cluster 透传边界由 [#63](https://github.com/kuasar-sandbox/orchestrator/issues/63) 跟踪.

`service=exec` 只接受 CONNECT 并始终 enforce KAT.Router 先做无副作用 Resolve/cache lookup,
以 `StableID + ServiceSecret` 验证原始 `X-Access-Token`,并在 HMAC 成功后编译
conditions.返回 public CONNECT 200 后,Router 严格读取首个 ExecRequest、重查 expiry 并执行
conditions;失败不调用 `Reserve(operation=data)`、不连接 node.只有 request admission 成功后,
已有完整 node target 才直连 node proxy;target 缺失才调用 `Reserve(operation=data)`并在 fresh
route 上重新核验 stable lineage/credential.随后构造第二跳 CONNECT:

```text
E2b-Sandbox-Id:      <current NodeSandboxID>
E2b-Sandbox-Service: exec
E2b-Sandbox-Port:    <original port, if present>
X-Access-Token:      <same KAT>
```

两跳之间只重写 stable SID 为 NodeSandboxID,service/port/token 值和 token Header 都不变;
Router 将首帧 Raw 原样发送一次.最终 node 不信任 Router,以本地 route 再次验证同一 KAT,
重新读取并执行完整 ExecRequest gate,之后才能 parking、resume 和连接 `ctl.sock`.
完整 target 的 cache hot path 不增加 Registry RPC,但 Router/node 两层验证仍保留.
KAT 绑定 StableID 而不绑定 NodeSandboxID/generation,因此同一逻辑沙箱的同节点
resume,跨节点迁移或 re-place 不要求客户端重签;新 CONNECT 始终进入当前 NodeSandboxID.
typed stale retry 只允许发生在 Raw 尚未写给任何 node 时;node CONNECT 200 且 Raw 已发送后
禁止 retry/reroute/replay,node 返回的 ctl error 原样中继.

## 11. 密钥与鉴权

同一 group/租户范围内有两个用途分离的根凭据域:

| 名称 | 持有者 | 用途 |
|---|---|---|
| `APISecret` | provider/placer/node/registry/router/proxy | 签发并验证 API key及后续用途明确的认证材料;完整 SHA-256 指纹标识凭据对 |
| `ManifestKey` | provider/placer/node | 解密 manifest/镜像/快照内容,封装 pull token;router 不接触 |

两者都是 32B / 64-lowercase-hex。APISecret 缺省时,placer 在物化 inline ManifestKey 时使用固定 KDF:

```text
APISecret = HMAC-SHA256(decodeHex(ManifestKey), "kuasar-api-secret-v1")
```

API key 只由 APISecret 签发/验证。API key 内 `SHA256(APISecret)[:12]` 仅作候选预筛;
node-link、生命周期命令和业务记录使用完整 64-hex `SHA256(APISecret)`。ManifestKey 也携带
自己的完整 64-hex SHA-256 指纹,用于校验成对交付的内容键。

APISecret 和 ManifestKey 都是 typed secret,支持 inline 或 ref 带外交付。selector patch 要么携带
完整 pair(两项 typed carrier + 两项完整指纹),要么完全不携带凭据字段;半对必须拒绝。凭据分发只发生在
shuffle-sharding/import/selector patch 路径:

```text
placer selector patch
  │ target node set + APISecret/ManifestKey material/ref
  ▼
registry/node owner
  │ CAS node_link key_pair cache (record key = APISecretFingerprint)
  ▼
node_link heartbeat refresh
  │ key_put complete pair only when missing or near lease expiry
  ▼
node encrypted local key store
```

`key_put` 在 node 侧原子校验并安装完整 pair;同一 APISecret 完整指纹不得绑定不同 pair material。
`key_drop` 以完整 APISecret 指纹定位 pair,但不是正确性依赖。节点侧租约按 TTL 淘汰未续租条目。
凭据分发是 create/build 前置条件;drop、TTL 或 provider 更新都不修改已复制进现有 sandbox/build
业务记录的凭据对。

ServiceSecret 不是第三个 group root,而是每个 node Sandbox 业务记录的独立 service credential。缺省值由
该 Sandbox 已绑定的 APISecret 和 `StableID()` 以固定 domain 派生;也可由本次 create 的
`kuasar-sandbox.credentials` object 显式指定。Registry 在 route/ref/command 副作用前按 placement
Profile 校验并规范化该 object,node 再次校验、分离后把 ServiceSecret 与 Envd/Traffic/Forward token
加密写入 Sandbox 业务行。普通 metadata、guest 配置和 node-stub 观测面均不保留 credentials object。

ForwardAccessToken 和 ExecAccessToken 都使用 `kat1`,但 audience 明确分离.
ExecAccessToken 的 canonical payload 是 `v,session_id,sid,aud[,exp]`,其中 session ID 是
UUIDv7,`sid=StableID`,`aud=exec`,不包含 `iat`;ServiceSecret 解码为 32-byte key 后
直接执行 HMAC-SHA256,不增加 exec-specific 派生层.create/get/list 不返回缺省
ExecAccessToken;每个 exec-session API 调用独立签发,服务端不建 session row,revoke
或 single-use/replay 状态.

Registry 从 node route event 物化受保护 route 时,只采纳 APISecret、两项 root fingerprint、
StableID、ServiceSecret 和 Envd/Traffic/Forward tokens;不采纳 ManifestKey 原文或既有
node-link wire 中供节点 proxy/MMDS 使用的 MmdsSecret。Registry 本阶段以明文结构化字段保存
这些受保护 route 凭据,不增加额外加密层;它们只可由受保护 Reserve/Resolve 返回给可信 router,
不得进入普通 route watch/list、公开 create/get/list 响应、日志或观测接口。创建请求中的
`kuasar-sandbox.credentials` 在 Reserve 入口从普通 config 分离,仅随 RESERVED 记录冻结并在 CmdCreate 前临时
编码,READY 后清除。

## 12. Build

Build 记录按 group 存在 `route_link` 的 `build` recordSet;执行态和实时预算归 node owner。

```text
router build register
  │ stable build_id/template_id + explicit profile
  ▼
route owner ReserveBuild
  │ PlaceBuild
  ▼
placer suggests node
  │
  ▼
node owner AdmitBuild(node_id, build_id, resources)
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

`ReserveBuild` 返回当前 node 的 `APIEndpoint`;Router 的 build status/trigger/files/log 等后续
control HTTP 只缓存并拨该地址.结果不保留旧 `DataEndpoint` alias,BuildRecord 也不复制 endpoint.

北向 `/v3/templates` 将省略的 profile 按 e2b 端点语义解析为 `e2b`;进入集群内部后 profile 必须
显式存在。route owner 将其与 placement 返回的 `APISecretFingerprint` 持久化进 BuildRecord,
并随 `build_register` 下发,节点按该完整指纹从同一凭据对写入本地 build,同时将 profile 写入
BuildSpec;缺失或非法值直接拒绝,不得静默改写。bare build 只允许 image 产物,不接受
start/ready 命令。

node owner 的 admission 以 `(node_id,build_id)` 记账;同一 build_id 出现在不同 node 时互不影响。若资源
余量不足则直接拒绝,route owner 重新调度。build event 只携带 build_id,nodelink owner 查本节点归属表
得到 group;终态调用 `ReleaseBuild(node_id,build_id)`。北向查询和 router cache 始终带 group。

## 13. 状态所有权与灾备边界

node 是 sandbox/build 执行状态的事实源。`route_link` 中的 sandbox/build record、`node_link` 中的
反向 ownership 和 admission 都是由节点事实派生的路由、查询或调度投影,不能由 operator 文件创建或
转移。terminal build 仍是一次节点执行的查询投影;可持久复用的是 build 产出的 template/manifest,
不是 BuildRecord。

registry 正常控制面不提供执行态 import/export。尤其禁止导入现有 SID、NodeID、READY/PAUSED 状态、
build execution、node ref、admission 或本机 checkpoint。节点虽持久化 cluster sandbox 的独立系统上下文,
但当前 node-link full report 不回传 Registry-owned identity;registry 执行 shard 完全丢失时不能仅靠 route event
重建 ownership,也不能预装 ownership 让旧事件看似合法。这条显式 recovery/bootstrap 协议尚未实现,由
[issue #34](https://github.com/kuasar-sandbox/orchestrator/issues/34) 跟踪;当前不得用手工写入执行 row 规避该限制。

可移植 paused sandbox 的灾备对象是带 migration token 的**未绑定持久 route**,不是 SandboxRecord。
它不携带旧 SID/NodeID/admission,恢复时重新 placement,由目标 node 校验 remote snapshot 后创建新身份,
READY 上报才建立运行态 route。该 recovery-only workflow 由
[issue #33](https://github.com/kuasar-sandbox/orchestrator/issues/33) 跟踪,不挂载在正常 route_link API。

Manifest Bundle不改变migration token/KMT wire:token仍只携根Bundle ref。目标node的task-local
preflight只读根Bundle的连续metadata prefix,从平面 `bundle/refs` 收集located来源并用
`checkpoint.remote.ref_location_parent`确定性派生完整mapping;同目录sibling无需mapping,
被引用Bundle不会在编排层打开或递归扫描。export/promote继续委托sandboxer原样发布根Bundle及
其无location sibling,带location的外部依赖保持原地址。

sandbox-group 配置、placement hint、APISecret、ManifestKey 仍由 placer/provider 自己的持久化和灾备流程负责。
在 #33/#34 完成前,完整 registry 执行态丢失没有 operator runtime import 兜底;系统必须明确报告不可恢复,
而不是构造可能与节点冲突的 route/build ownership。

## 14. 可靠性

| 事件 | 行为 |
|---|---|
| router 崩溃 | 丢本地缓存;重启后 cache miss 重新 Resolve |
| placer 崩溃 | registry 对同 group failover 到下一个 ready placer;热路径不受影响 |
| node_link 断线 | node owner 立即拒绝该 node 的新提交；route owner 排除并重选；node_dead_after 后清理 node profile/node_list 和关联执行态；node 重连后全量/增量重报 |
| node 整机重启 | node 清空运行态;缺失 sandbox 经 node 上报/清理收敛为 dead route |
| registry 单成员故障 | owner set quorum 足够时继续服务;恢复后由同 shard 访问或事实源上报触发 read-repair |
| registry 多成员故障导致 quorum 不足 | 对应 shard 停写,不降级乱写 |
| registry 执行 shard 全失但 node 存活 | 不导入备份执行 row;进入显式 recovery mode,由 node 持久事实重建投影(#34;当前未实现) |
| membership 变更 | active/next joint 写 quorum + old_grace read-only 证书/快照来源 |
| 整集群下电 | 运行中和本机 checkpoint 不可恢复;仅 provider 持久数据、template/manifest 及未来 migration-token route(#33)可参与灾备 |

## 15. 性能

- 数据面热路径:router 命中完整 node target 后直转 node,包括 paused/starting,不访问 registry。
- Place 冷路径:group 经 route_link owner -> ready placer failover -> node owner 在线校验/admission；失败候选排除后重选。
- 海量 group:router 不订阅 group;registry 不跨 group 扫描;placer import 按 source_id 独立分页。
- 海量 node:node_link 按 node_id 分片;node_list 只承载低频目录,不承载高频水位。
- membership 变更:不做全局 promoter;由 node 上报、group 请求、source import、read-repair 自然收敛。

## 16. 集群 stub e2e

`node-stub-ctl` 是本仓 cluster e2e 节点桩。它使用真实 node_link 协议接入 registry,一个进程可模拟多个
node,但不启动 microVM.每个进程使用彼此不同的 admin,API 和 Data listener;ready JSON 返回
`admin`,`api`,`data`,每个 node 注册 APIEndpoint 与 DataEndpoint.除 microVM/应用进程外,它模拟节点控制面行为:

- 注册 node、心跳、drain、水位和 build 预算。
- 接收 `key_put/key_drop/create/connect/exec_session/delete/build_register` 命令并返回 ack.
- 按 sandbox 行为配置发布 READY/dead route event。
- 发布 build event。
- admin listener 只提供 node-stub-ctl 管理查询;API listener 只模拟 conductor control API;
  Data listener 只模拟 ordinary data,CONNECT 与 exec.
- 支持 `restart-link`、`reboot-empty`、`crash/start` 等节点动作。

`make test-e2e` 先 `make build`,再用产物真实启动 `cluster-ctl registry/router/placer` 与 `node-stub-ctl`。
`test/e2e/e2e_cluster_stub.sh` 覆盖 N=1 registry、多 registry、membership joint/old_grace cutover、group
import、key 分发、显式 create/Reserve、稳定 SandboxID 的 CmdConnect、SandboxID 与 NodeSandboxID
转换,control/build 命中 API listener,data/exec 命中 Data listener,ExecSession Reserve/CmdExecSession 签发,
`service=exec` KAT 拒绝/双层校验与第二跳 buffered tunnel,route cache,BuildRegister,
孤儿 route 清理和节点清空收敛.

## 17. See Also

- [cluster-router.md](cluster-router.md) — router 入口、route cache 和数据面转发。
- [cluster-placer.md](cluster-placer.md) — group provider/importer、WATCH_LIST、Place 与 key distribution。
- [node.md](node.md) — node-ctl 单机主机与 node-link 节点侧行为。
- [node-proxy.md](node-proxy.md) — node 数据面 proxy、routesync 与 CONNECT。
- `kuasar-sandbox/docs/deployment.md` — 部署拓扑、端口、启停与故障域。
