# Cluster Control Plane

本文描述 `cluster-ctl registry/router/placer` 与集群模式 `node-ctl` 的最终架构。正确性契约以
[orchestrator#46](https://github.com/kuasar-sandbox/orchestrator/issues/46) 正文为准；本文是实现和运维映射，
不定义历史兼容层、中间发布态或双写迁移。

## 1. Authority and trust boundary

调用链固定为：

```text
caller -> Router -> Registry -> Session Holder -> node
```

权威边界如下：

| 数据 | 权威 |
| --- | --- |
| caller / GROUP authentication | Router + Provider |
| Route workflow、Build registration binding 与 revision | Registry Multi-Raft |
| node session tuple | 当前 Session Holder，受 System Group enrollment 约束 |
| execution 存活状态、资源占用、Admission、command dedupe、event outbox | node |
| placement policy 与 immutable dispatch spec | Placer + Provider |
| forwarding capability | 当前 READY execution |

Router 到 Registry 是经过 mTLS 认证的可信内部连接。Registry 不接收 caller credential，不维护
AuthCatalog。`AccessToken` 和 `TrafficAccessToken` 是具体 execution 的转发 capability，不是 caller 或
GROUP 凭据。

Build 注册成功后的调用链不同：

```text
caller -> Router -> exact bound node
```

Router 只从 Registry 读取 immutable registration binding；trigger/files/status/logs/terminal 均由 node 本地处理。
Router 到 node 的控制面和数据面均使用 Router-role mTLS，并在任何读取或副作用前校验 NodeID、NodeEpoch、
Registry History Generation、Binding digest 和对象 ID。

### Node key authority

`AuthKey` 是 node API 鉴权根，`ManifestKey` 是 Content Manifest、快照、MMDS 与 pull token
的内容加密根；两者必须使用不同材料。Router/Provider 用 AuthKey 验证 caller，节点对直接转发请求再次用
对象自身的 AuthKey 验证。ManifestKey 不能用于 API 鉴权；READY Route 的 AccessToken 也不能替代 AuthKey。

Provider/Placer 生成完整、带 TTL 的双密钥 lease。Registry 不把秘密写入 Raft，只经当前认证 Session Holder
发送 `key_put`；节点加密持久化并 ACK 准确的 AuthKey/ManifestKey fingerprints 后才允许 Admission/dispatch。
`key_put` 重试刷新 TTL；`key_drop` 仅是 best-effort 提前清理，TTL 到期是权威老化机制。Lease 到期阻止新对象，
但对象生命周期内保存的 AuthKey/ManifestKey 不被改写，已有对象仍由其创建时 AuthKey 鉴权。

### Node request preservation

Sandbox Create 与 Build Register 进入 Registry 前先形成有界、可重放的 node request envelope。Router 拒绝
重复 JSON key、冲突 alias、伪造 internal/fencing header 和保留 system metadata；覆盖 group/route identity、
node-local object identity、effective template、Build resource ceiling 与 protected Binding 等 cluster-owned 字段。
其余 JSON 字段、query、content type 和普通 end-to-end header 原样保留在 immutable dispatch spec 中。
Leader 恢复只能重放该 envelope，不能重新调用 Provider 或把请求重新编码为较窄结构。

正常运行期间不提供 execution import。灾难恢复只从 node durable workflow 和受保护 Binding 投影；持久
Route 导入必须使用独立 migration-token 协议。现有 Sandbox/Build 不能由 operator 元数据提升为权威状态。

## 2. Registry History Generation and fencing

每套 Registry 存储历史由以下 identity 唯一标识：

```text
cluster_id
registry_generation
system_epoch
Registry Layout version/digest
shard_id
```

Registry 请求必须携带完整 Registry History Generation identity。副本仅在请求与本地已提交 identity 完全一致时服务。
Registry Layout guard 和本地 enrollment 防止 Registry History Generation、Registry Layout 或副本身份回退。

只有 operator 显式创建的空 Registry History Generation 可以 bootstrap。Leader 不可见、memberlist 判死、超时或 quorum
丢失均不能触发 re-bootstrap。任意 Data Shard 的 committed history 不可恢复时，禁止在原 Registry History Generation
内空 bootstrap；必须 rollover 整个 Registry History Generation，并执行第 10 节恢复。

新 Registry History Generation 必须具备以下证明之一：

1. 旧 System Group 对准确 successor Registry Layout intent 的共识关闭证明。
2. 对旧 Router、Registry、服务端点、凭据和存储的不可逆外部硬 fencing 证明。

不可见、超时和 memberlist dead 均不是关闭证明。

## 3. System Group and Serve Permit

每个 Registry History Generation 有独立三副本 System Group，保存：

```text
Registry History Generation and active Registry Layout
Serve/Write/Cutover gates
bounded Serve Permit state
node enrollment and static catalog
Registry Layout transition progress
schema/protocol version
recovery epoch and completion
Registry History Generation closure and final cutover gate
```

System Group 签发有界 Serve Permit。Permit 使用进程本机 monotonic time 计时，过期后 fail closed，并同时
约束：

```text
Registry local/strong reads and writes
Session Holder Probe, dispatch, event ACK and recovery
Router Registry History Generation cache and data-plane forwarding
```

Router 只有在 `ServeGate && CutoverGate && RecoveryClosed` 时可读或转发；写入还要求 `WriteGate`。新
Registry History Generation 激活前必须等待旧 Permit 最大生命周期结束，除非已提交完整硬 fencing。

System Group 或任意 Data Shard 丢失 quorum 时停止 mutation。已有本地 READY 数据不因此创建 replacement。

## 4. Fixed keyspace

首个 Registry History Generation 的固定参数为：

```text
virtual_shard_count = 4096
route_bucket_count  = 16
build_bucket_count  = 16
replication_factor  = 3
PlaceN candidates   = 4
```

`ShardHashV1` 使用：

```text
fixed magic
four domain bytes
uint32 big-endian field lengths
raw UTF-8 bytes without Unicode normalization
uint32 big-endian numeric fields
xxhash64 seed=0
```

固定测试向量位于 `internal/cluster/shardhash_test.go`。Registry Layout 保存 hash 版本、bucket 数量及每个 shard
准确的三副本集合。Registry History Generation 创建后不得在线改变 hash、bucket 或不兼容 schema；这类变化要求新
Registry History Generation。

## 5. Multi-Raft store and read safety

实现使用一个 Dragonboat `NodeHost` 承载 System Group 和本机 Data Group replicas，使用一个共享 Pebble
state engine，并以 `(raft_shard_id, replica_id)` 隔离 keyspace。具体选择和门槛见
[`adr/0001-dragonboat-pebble-multiraft.md`](adr/0001-dragonboat-pebble-multiraft.md)。

`route_revision` / `build_revision` 是该行最后一次 mutation 的 committed shard log index，必须与
`registry_generation + shard_id` 一起解释。Leader 只在 quorum commit 且本机 apply 后返回成功；Follower
只返回本机已 apply 状态。

读取分为：

| 路径 | 语义 |
| --- | --- |
| local positive read | replica-local；只返回满足 identity、READY/positive state 和 `min_revision` 的投影 |
| local miss / behind | 返回 `NEED_LEADER` 或 `REPLICA_BEHIND`，绝不返回最终 `NOT_FOUND` |
| strong read | Leader/read-index 路径，可返回最终 `NOT_FOUND` |
| ListRoutes | 每个 `(group, route_bucket)` 使用单个 Pebble snapshot，并返回该 bucket 的 shard revision |
| WatchRoutes | durable compact changefeed；返回 floor/head/cursor，cursor 落后时明确 `reset` |

Router 对 local reads 使用按 key 的 replica-spread rendezvous；leader hint 只用于 strong read 和 mutation。
Router 已观察到的 `min_revision` 不可回退。node proxy 返回 stale Binding/NodeEpoch 时，本次数据请求最多
fail-fast 一次；Router 提高 revision fence 并强读刷新缓存，但不把原请求重放到另一个 execution。

## 6. Route and Build workflows

Route 状态固定为：

```text
STARTING -> READY <-> PAUSED
                    -> RESUMING -> READY
READY/PAUSED/RESUMING -> DELETING -> TOMBSTONE
STARTING placement failure -> TOMBSTONE -> new round with a new SID
```

`STARTING` 持久化足以在 Leader 切换后继续相同 workflow 的全部信息：

```text
sandbox_id and monotonically increasing placement_round within the Route workflow
complete candidate pool
selected candidate/index
definitively rejected candidates
normalized demand + digest
typed immutable dispatch spec + digest
provider/policy version
current node Binding
last event_seq
pending node finalization intents
```

只允许 `DEFINITIVE_REJECT` 证明未产生副作用后尝试同 pool 的下一候选。ACK 丢失、断链、Holder 变化和
Directory 新 NodeEpoch 都不能当作该证明；已选择 Binding 保持 pinned。System Group 提交严格更新的
NodeEpoch 后，Registry 才可 fence 旧 execution。Sandbox replacement 必须使用新 SID；Build ID 不得绑定到
第二节点。

Build registration 状态固定为：

```text
BUILD_STARTING -> BUILD_REGISTERED
BUILD_STARTING -> BUILD_TOMBSTONE  // candidate pool 全部 definitive reject
```

`ACCEPTED_QUEUED` 与 `ACCEPTED_ADMITTED` 都立即提交 `BUILD_REGISTERED`，其含义仅是 `(group, build_id)`
已绑定 exact node。trigger dedupe、waiting/building/ready/error、artifact、logs 和资源释放全部保存在该 node
的 SQLite Build/workflow transaction 中。Registry 没有 Build lifecycle event、ACK 或 terminal
finalization；仅未接受候选的 definitive rejection marker 由 Registry 显式 finalize。Route 在
`DELETING` 时可以推进 Sandbox durable event watermark，但不能被迟到 READY/PAUSED 拉回非删除状态。

## 7. Session and placement

每次 node-link 建连前，node 必须递增并 fsync `session_seq`；仅在 `node_epoch` 增加后归零。NodeEpoch 是
持久单调 64 位值，以下情况必须在 `node-ctl` 启动前递增并 fsync：

```text
host reboot
local execution state reset
data_endpoint change
```

接受新 NodeEpoch 表示旧 epoch 的所有 execution 已不可能继续。durable NodeEpoch 丢失时必须注册新
`node_id`。node 只接受当前 `(node_epoch, session_seq)` 的 command。

Session Directory 是 Holder 路由提示，不是 execution authority，也不是 no-side-effect proof。Holder 不迁移
已建立连接；node 断线后按 Registry Layout rendezvous 重新连接。静态 enrollment/catalog 完全相同时，SessionSeq
重连只做 System strong read，不追加 System log。

Placement 流程：

```text
System committed Node Catalog
-> Placer applies Provider/Policy and returns up to four RandomN candidates
-> Registry probes a fresh pair through current Holders
-> Probe validates tuple, load model and sample age
-> P2C selects lower rate candidate
-> Registry commits selected Binding
-> Holder sends node-local Admission command
```

候选不足四个时允许返回 1..4 个，不制造不可用节点。PlacementLoadSnapshot 周期不得超过 500ms，并在
Admission、launch 和 completion 等重要变化时立即唤醒；memberlist 只可提供故障提示。

## 8. Node-local execution authority

Sandbox/Build Admission、queued/reserved resources 和 command dedupe 都持久化在 node；Sandbox 另有
durable event outbox，Build lifecycle 只保存在本地 Build/workflow 表。本地
Admission 以 SID 或 `build_id` 为唯一幂等键：

```text
same ID + same immutable digest -> return durable result
same ID + different digest      -> CONFLICT
sent but no ACK                 -> UNKNOWN; do not try another node
```

Build queue 是 durable async queue。`ACCEPTED_QUEUED` 与 `ACCEPTED_ADMITTED` 明确区分。Sandbox terminal
resource release、workflow state 和 outbox 更新必须原子提交；Build state、artifact/error 与 resource
release 必须原子提交，且不产生 cluster outbox event。

ExecutionBinding 没有引入独立 node 数据模型。它是 system-owned、受保护的 opaque object metadata，由系统
自动插入；user metadata 不能写该 key。Binding 固定：

```text
registry_generation, kind, object ID, group, route key
node ID, NodeEpoch
demand digest, dispatch-spec digest
```

node proxy 和 command 同时校验当前 Binding digest、NodeEpoch 与 `registry_generation`。

## 9. Sandbox event, fence and compaction

node 维护每个 Sandbox execution 的 latest durable event 和 ACK watermark。Registry 只在对应 Route
mutation committed 后 ACK。Build 不进入该流。重放受消息数、字节数和周期限制，command ACK 优先，但不能
永久饿死 event 或 placement snapshot。

同一 shard 还保存 `(group, route_key, sandbox_id) -> execution fence`。Fence 删除必须同时满足：

```text
terminal/fencing proof committed
final node outbox watermark ACKed, or NodeEpoch permanently fenced
all current replicas applied beyond fence revision
minimum operator retention elapsed
```

TTL 单独不构成删除证明。

## 10. Registry History Generation recovery

Data Shard history 不可恢复时执行 whole-Registry-History-Generation rollover 和 node-authoritative recovery：

```text
operator begins one RecoveryEpoch(source_registry_generation -> target_registry_generation)
-> collect bounded, ordered node durable reports
-> detect duplicate logical claims and quarantine conflicts
-> construct target Binding by changing registry_generation only
-> node digest-CAS replaces protected opaque metadata
-> node returns the exact durable target object in rebind ACK
-> Registry commits refreshed target projection as REBOUND
-> Sandbox: Registry fsync-ACKs exact target event, then ACTIVATE Route
-> Build: ACTIVATE registration binding directly; lifecycle state remains node-local
-> all shards finalize and System Group closes recovery
-> cutover gates may open
```

Sandbox source Registry History Generation event ACK 不能复用于 target Registry History Generation。若 rebind 与 ACK 之间 node 产生更高
event_seq，Registry 先更新共识投影并再次 ACK 最新 watermark。Sandbox READY 在 target ACK 前不可见。
Build snapshot 的 `event_seq` 必须为 0；恢复只重建 registration binding，不复制或解释 node-local lifecycle。

报告按对象数、页面字节、单节点总字节和 bytes/s 限流。丢失节点只能由 operator 提交外部证明后标记
MISSING 或 QUARANTINED；不可见和超时不会自动放弃 execution。

## 11. Registry Layout transition

扩缩容不创建第二套 Route 历史：

```text
publish signed next_registry_layout
-> System Group commits one transition
-> add learner/new replica and catch up
-> promote after exact applied proof
-> remove old replica
-> persist each shard progress
-> activate Registry Layout/system_epoch atomically
-> drain old Permit lifetime
-> finalize transition and delete removed local ranges
```

一次只允许一个 transition。新旧节点必须验证同一 signed artifact。memberlist 不修改 Raft membership。

## 12. Bounded operations

生产配置必须显式限制：

```text
Raft initialize and snapshot workers
Raft operation timeout and snapshot thresholds
Registry workflow/recovery/compaction workers
aggregate Sandbox and Build launch rates/bursts
node reconnect rate and event workers
node event replay batch/bytes/interval
recovery page/total bytes and bytes/s
node Sandbox/Build launch workers and durable queue depth
```

Registry aggregate launch rate按 Registry Layout member 数量等分，因此所有成员同时工作时总速率不超过配置值；
成员故障只降低吞吐，不放宽安全上限。

Dragonboat 的 per-replica queue 必须使用仓库中的
`deploy/dragonboat-soft-settings.json`。该依赖只在进程初始化时从当前工作目录读取固定文件名，因此 Registry
配置、该文件和进程工作目录必须是同一目录；标准部署为 `/etc/cluster-ctl`，对应 systemd unit 已固定
`WorkingDirectory=/etc/cluster-ctl`。Registry 会在创建 NodeHost 前校验文件内容、权限和工作目录并 fail closed，
避免静默使用在 4097-group 门槛中超过 8 GiB RSS 的上游默认队列。

## 13. Required verification

合入和切流前必须通过：

```text
control-plane partition does not create replacement while old data plane remains reachable
Admission ACK loss and Holder change do not create a second execution
local miss never returns final NOT_FOUND
stale Follower READY fails once without reaching a wrong execution
Leader restart resumes only from consensus intent
node restart preserves queued/admitted state
duplicate/out-of-order events never regress state
recovery restart, conflict, delayed epoch and target-event ACK windows
fence/tombstone cannot compact early
4097 live Raft groups and 1,000,000 READY rows meet latency/RSS gates
five-repository exact-head canonical make test-e2e completes within 30 minutes
```

`make test-e2e` 直接运行仓库内 canonical target，不依赖 `/opt` 下的外部脚本。
